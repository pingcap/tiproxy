#!/usr/bin/env bash
# Copyright 2026 PingCAP, Inc.
# SPDX-License-Identifier: Apache-2.0

# Linux-only, privileged two-node VIP smoke. Each Rust process owns its own
# network namespace, so both may bind the same SQL port and only the elected
# process may bind the shared VIP on its eth0. The bridge gives both processes
# a real PD/TiDB and gives the host a route to query the elected VIP.
set -euo pipefail

if [[ $(uname -s) != Linux ]]; then
	echo 'VIP acceptance requires Linux network namespaces' >&2
	exit 2
fi
for command in ip arping tiup mysql curl; do
	command -v "$command" >/dev/null || { echo "missing $command" >&2; exit 2; }
done

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../../.." && pwd)
source "$script_dir/versions.env"
rust_binary="$repo_root/rust/target/debug/tiproxy-rs"
[[ -x $rust_binary ]] || { echo "missing $rust_binary" >&2; exit 2; }

run_id=${GITHUB_RUN_ID:-local-$$}
run_dir=${DATAPLANE_ARTIFACT_ROOT:-$script_dir/artifacts}/vip-$run_id
mkdir -p "$run_dir/diagnostics"
run_dir=$(cd "$run_dir" && pwd)
tag=vip-$run_id
bridge=br-vip-test
ns_a=vip-a-$run_id
ns_b=vip-b-$run_id
link_id=${run_id: -6}
host_a=vha-$link_id
host_b=vhb-$link_id
host_ip=198.19.99.1
vip_ip=198.19.99.100
node_a_ip=198.19.99.11
node_b_ip=198.19.99.12
offset=13000
pd_port=$((2379 + offset))
tidb_port=$((4000 + offset))
tiup_pid=
monitor_pid=

owned_rust_pid() {
	local node=$1 pid=$2
	[[ $pid =~ ^[0-9]+$ ]] || return 1
	sudo ps -ww -p "$pid" -o args= 2>/dev/null |
		grep -Fq -- "$run_dir/$node.toml"
}

cleanup() {
	local status=$?
	trap - EXIT
	set +e
	[[ -n $monitor_pid ]] && kill "$monitor_pid" 2>/dev/null
	[[ -s $run_dir/diagnostics/overlap.log ]] && status=1
	for node in a b; do
		pidfile=$run_dir/$node.pid
		if [[ -f $pidfile ]]; then
			pid=$(cat "$pidfile")
			owned_rust_pid "$node" "$pid" && sudo kill -INT "$pid" 2>/dev/null
		fi
	done
	sleep 2
	for node in a b; do
		pidfile=$run_dir/$node.pid
		if [[ -f $pidfile ]]; then
			pid=$(cat "$pidfile")
			owned_rust_pid "$node" "$pid" && sudo kill -KILL "$pid" 2>/dev/null
		fi
	done
	if [[ -n $tiup_pid ]]; then
		tiup clean "$tag" >>"$run_dir/diagnostics/cleanup.log" 2>&1
		kill -INT "$tiup_pid" 2>/dev/null
	fi
	sudo ip netns del "$ns_a" 2>/dev/null
	sudo ip netns del "$ns_b" 2>/dev/null
	sudo ip link del "$bridge" 2>/dev/null
	echo "exit=$status" >>"$run_dir/diagnostics/result.txt"
	exit "$status"
}
trap cleanup EXIT

sudo ip link add "$bridge" type bridge
sudo ip addr add "$host_ip/24" dev "$bridge"
sudo ip link set "$bridge" up

attach_node() {
	local namespace=$1 host_link=$2 address=$3 mac_suffix=$4 peer=peer0
	sudo ip link add "$host_link" type veth peer name "$peer"
	sudo ip link set "$peer" netns "$namespace"
	# GitHub's runner assigned the same generated MAC to both peers in the
	# first acceptance runs. A shared MAC lets the bridge learn the wrong port
	# for the VIP even though exactly one namespace owns its IP address.
	sudo ip link set "$host_link" address "02:00:00:99:10:$mac_suffix"
	sudo ip link set "$host_link" master "$bridge"
	sudo ip link set "$host_link" up
	sudo ip -n "$namespace" link set "$peer" name eth0
	sudo ip -n "$namespace" link set eth0 address "02:00:00:99:20:$mac_suffix"
	sudo ip -n "$namespace" link set eth0 up
	sudo ip -n "$namespace" addr add "$address/24" dev eth0
	sudo ip -n "$namespace" route add default via "$host_ip"
}

sudo ip netns add "$ns_a"
sudo ip netns add "$ns_b"
sudo ip -n "$ns_a" link set lo up
sudo ip -n "$ns_b" link set lo up
attach_node "$ns_a" "$host_a" "$node_a_ip" 11
attach_node "$ns_b" "$host_b" "$node_b_ip" 12

# The backend playground is deliberately on the host's bridge address so a
# namespace's 127.0.0.1 can never accidentally bypass the veth network.
tiup "playground:v${TIUP_VERSION}" "$TIDB_VERSION" --tag "$tag" \
	--without-monitor --host "$host_ip" --port-offset "$offset" \
	--pd 1 --kv 1 --db 1 --tiflash 0 --tiproxy 0 \
	>"$run_dir/diagnostics/tiup.log" 2>&1 &
tiup_pid=$!

wait_mysql() {
	local host=$1 port=$2 label=$3
	local result
	for _ in {1..120}; do
		if result=$(mysql --protocol=TCP --connect-timeout=1 --ssl-mode=DISABLED \
			-h "$host" -P "$port" -u root --batch --skip-column-names \
			-e 'SELECT 1' 2>&1) && [[ $result == 1 ]]; then
			echo "$(date +%s.%N) $label SELECT 1 PASS" >>"$run_dir/diagnostics/events.log"
			return 0
		fi
		printf '%s\n' "$result" >"$run_dir/diagnostics/$label-mysql-last-error.txt"
		kill -0 "$tiup_pid" 2>/dev/null || break
		sleep 1
	done
	if declare -F snapshot >/dev/null; then
		snapshot "$label-failed"
	fi
	echo "$label SQL query failed" >&2
	return 1
}
wait_mysql "$host_ip" "$tidb_port" backend

# The runner must prove each network namespace can reach the same backend
# before a failed VIP query is attributed to routing in the Rust processes.
for namespace in "$ns_a" "$ns_b"; do
	if ! sudo ip netns exec "$namespace" mysql --protocol=TCP \
		--connect-timeout=2 --ssl-mode=DISABLED -h "$host_ip" -P "$tidb_port" \
		-u root --batch --skip-column-names -e 'SELECT 1' \
		>"$run_dir/diagnostics/$namespace-backend-preflight.txt" 2>&1; then
		echo "$namespace cannot query TiDB from its network namespace" >&2
		exit 1
	fi
done

write_config() {
	local node=$1 address=$2
	mkdir -p "$run_dir/$node-work"
	cat >"$run_dir/$node.toml" <<CONFIG
workdir = "$run_dir/$node-work"
enable-traffic-replay = false
[proxy]
addr = "0.0.0.0:6000"
advertise-addr = "$address"
pd-addrs = "$host_ip:$pd_port"
graceful-wait-before-shutdown = 0
graceful-close-conn-timeout = 1
[[proxy.backend-clusters]]
name = "vip-cluster"
pd-addrs = "$host_ip:$pd_port"
[api]
addr = "0.0.0.0:3080"
[ha]
virtual-ip = "$vip_ip/24"
interface = "eth0"
garp-burst-count = 1
garp-refresh-count = 3
[security.sql-tls]
skip-ca = true
CONFIG
}
write_config a "$node_a_ip"
write_config b "$node_b_ip"

start_node() {
	local node=$1 namespace=$2
	# exec preserves the PID written inside the namespace; the outer sudo PID
	# would not be a reliable signal target for the Rust shutdown path.
	sudo ip netns exec "$namespace" sh -c \
		'echo "$$" >"$1"; shift; exec "$@"' \
		sh "$run_dir/$node.pid" "$rust_binary" --standalone \
		--config "$run_dir/$node.toml" --health-port 8080 \
		>"$run_dir/diagnostics/$node.log" 2>&1 &
}
has_vip() {
	local namespace=$1
	sudo ip -n "$namespace" -o addr show dev eth0 2>/dev/null |
		grep -Fq "inet $vip_ip/24 "
}

monitor_overlap() {
	local a_bound b_bound
	while :; do
		a_bound=0
		b_bound=0
		has_vip "$ns_a" && a_bound=1
		has_vip "$ns_b" && b_bound=1
		if ((a_bound + b_bound > 1)); then
			echo "$(date +%s.%N) overlap" >>"$run_dir/diagnostics/overlap.log"
			return 1
		fi
		sleep 0.1
	done
}

assert_no_overlap() {
	if [[ -s $run_dir/diagnostics/overlap.log ]]; then
		echo 'two VIP holders observed by background sampler' >&2
		return 1
	fi
}

monitor_overlap &
monitor_pid=$!
start_node a "$ns_a"
start_node b "$ns_b"

wait_ready() {
	local node=$1 namespace=$2
	for _ in {1..60}; do
		if sudo ip netns exec "$namespace" curl --fail --silent --max-time 1 \
			'http://127.0.0.1:8080/health' \
			>"$run_dir/diagnostics/$node-health-ready.json"; then
			echo "$(date +%s.%N) $node ready" >>"$run_dir/diagnostics/events.log"
			return 0
		fi
		if [[ -f $run_dir/$node.pid ]]; then
			sudo kill -0 "$(cat "$run_dir/$node.pid")" 2>/dev/null || break
		fi
		sleep 1
	done
	echo "$node Rust process did not become ready" >&2
	return 1
}
wait_ready a "$ns_a"
wait_ready b "$ns_b"

snapshot() {
	local label=$1
	sudo ip -j -n "$ns_a" addr show >"$run_dir/diagnostics/$label-a-addr.json"
	sudo ip -j -n "$ns_b" addr show >"$run_dir/diagnostics/$label-b-addr.json"
	sudo ip netns exec "$ns_a" curl --silent --max-time 2 \
		'http://127.0.0.1:8080/health' \
		>"$run_dir/diagnostics/$label-a-health.json" || true
	sudo ip netns exec "$ns_b" curl --silent --max-time 2 \
		'http://127.0.0.1:8080/health' \
		>"$run_dir/diagnostics/$label-b-health.json" || true
	local ctl=${TIUP_HOME:-$HOME/.tiup}/components/ctl/$TIDB_VERSION/etcdctl
	if [[ -x $ctl ]]; then
		ETCDCTL_API=3 "$ctl" --endpoints "http://$host_ip:$pd_port" \
			get "/tiproxy/vip/$vip_ip/owner" --prefix -w json \
			>"$run_dir/diagnostics/$label-election.json" 2>&1 || true
		ETCDCTL_API=3 "$ctl" --endpoints "http://$host_ip:$pd_port" \
			get '/topology/tidb/' --prefix -w json \
			>"$run_dir/diagnostics/$label-tidb-topology.json" 2>&1 || true
	fi
}

wait_single_owner() {
	local label=$1 expected=${2:-either} a_bound b_bound
	for _ in {1..45}; do
		a_bound=0
		b_bound=0
		has_vip "$ns_a" && a_bound=1
		has_vip "$ns_b" && b_bound=1
		if ((a_bound + b_bound == 1)); then
			if [[ $expected == either || ($expected == a && $a_bound == 1) ||
				($expected == b && $b_bound == 1) ]]; then
				snapshot "$label"
				echo "$(date +%s.%N) $label owner=$([[ $a_bound == 1 ]] && echo a || echo b)" \
					>>"$run_dir/diagnostics/events.log"
				[[ $a_bound == 1 ]] && echo a || echo b
				return 0
			fi
		fi
		if ((a_bound + b_bound > 1)); then
			snapshot "$label-overlap"
			echo 'two VIP holders observed' >&2
			return 1
		fi
		sleep 1
	done
	snapshot "$label-timeout"
	echo "single VIP owner not observed at $label" >&2
	return 1
}

owner=$(wait_single_owner initial)
assert_no_overlap
wait_mysql "$vip_ip" 6000 initial-vip
if [[ $owner == a ]]; then
	former=a
	new=b
else
	former=b
	new=a
fi
former_pid=$(cat "$run_dir/$former.pid")
sudo kill -INT "$former_pid"
wait_single_owner after-controlled-close "$new" >/dev/null
assert_no_overlap
wait_mysql "$vip_ip" 6000 controlled-failover
for _ in {1..30}; do
	sudo kill -0 "$former_pid" 2>/dev/null || break
	sleep 1
done
if sudo kill -0 "$former_pid" 2>/dev/null; then
	echo 'former VIP owner did not exit after SIGINT' >&2
	exit 1
fi

# A restarted former owner removes any stale local address before campaigning.
if [[ $former == a ]]; then
	start_node a "$ns_a"
	wait_ready a "$ns_a"
else
	start_node b "$ns_b"
	wait_ready b "$ns_b"
fi
wait_single_owner after-restart "$new" >/dev/null
assert_no_overlap
wait_mysql "$vip_ip" 6000 restart

# Model a whole-node crash: terminate its process and remove its network link.
# Process-only SIGKILL leaves an IP on a live Linux interface and is separately
# documented as a remaining Go/Rust operational fencing gap.
sudo kill -KILL "$(cat "$run_dir/$new.pid")"
if [[ $new == a ]]; then
	sudo ip link del "$host_a"
else
	sudo ip link del "$host_b"
fi
wait_single_owner after-node-link-loss "$former" >/dev/null
assert_no_overlap
wait_mysql "$vip_ip" 6000 node-failover
kill "$monitor_pid" 2>/dev/null || true
wait "$monitor_pid" 2>/dev/null || true
monitor_pid=
assert_no_overlap
echo 'PASS: one VIP holder and SQL continuity through controlled close, restart and node/link loss'
