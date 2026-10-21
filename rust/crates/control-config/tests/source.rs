// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! CP-CFG immutable-source, parity, and failure tests.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use control_config::{
    CandidateValidator, ConfigError, ConfigModule, ConfigModuleOptions, ConfigNamespaceSource,
    ConfigNamespaceStore, EffectiveConfig, NamespaceConfig, PersistentConfigSnapshot,
    PreparedArtifact, RoutingBalancePolicy, RoutingRule, RoutingSelectionPolicy, SourceRevision,
    StoreError, decode_persistent_entries,
};
use control_etcd::ElectionConfig;
use control_external::EtcdClientConfig;

struct RejectableValidator(Arc<AtomicBool>);

impl CandidateValidator for RejectableValidator {
    fn validate(
        &self,
        _effective: &EffectiveConfig,
        _namespaces: &[NamespaceConfig],
    ) -> Result<PreparedArtifact, &'static str> {
        if self.0.load(Ordering::SeqCst) {
            Err("test_material_rejected")
        } else {
            Ok(PreparedArtifact::empty())
        }
    }
}

/// A concrete prepared type so `downcast_ref` has something to recover.
#[derive(Debug, PartialEq)]
struct SerialArtifact(u64);

/// Prepares a fresh, distinctly serial-numbered artifact per validation and
/// records the exact handle it returned, so a test can assert the store carries
/// that same handle without re-preparing.
struct RecordingValidator {
    next: AtomicU64,
    reject: AtomicBool,
    last: Mutex<Option<PreparedArtifact>>,
}

impl RecordingValidator {
    fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
            reject: AtomicBool::new(false),
            last: Mutex::new(None),
        }
    }

    fn set_reject(&self, reject: bool) {
        self.reject.store(reject, Ordering::SeqCst);
    }

    fn last(&self) -> PreparedArtifact {
        self.last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| unreachable!("a validation must have recorded an artifact"))
    }
}

impl CandidateValidator for RecordingValidator {
    fn validate(
        &self,
        _effective: &EffectiveConfig,
        _namespaces: &[NamespaceConfig],
    ) -> Result<PreparedArtifact, &'static str> {
        // Prepare and record even on rejection, so a test can prove the failed
        // product is not the one the store keeps or publishes.
        let serial = self.next.fetch_add(1, Ordering::SeqCst);
        let artifact = PreparedArtifact::new(Arc::new(SerialArtifact(serial)));
        *self
            .last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(artifact.clone());
        if self.reject.load(Ordering::SeqCst) {
            return Err("recording_rejected");
        }
        Ok(artifact)
    }
}

fn current_dir() -> &'static Path {
    Path::new("/var/lib/tiproxy-test")
}

fn assert_same_float(actual: f64, expected: f64) {
    assert_eq!(actual.to_bits(), expected.to_bits());
}

#[test]
fn routing_projection_has_go_compatible_defaults() {
    let store = ConfigNamespaceStore::from_toml(&[], None, current_dir())
        .unwrap_or_else(|error| unreachable!("default config: {error}"));
    let snapshot = store.current();
    let routing = snapshot
        .effective()
        .routing()
        .unwrap_or_else(|error| unreachable!("routing projection: {error}"));

    assert_eq!(routing.routing_rule, RoutingRule::MatchAll);
    assert_eq!(routing.balance_policy, RoutingBalancePolicy::Resource);
    assert_eq!(routing.selection_policy, RoutingSelectionPolicy::PreferIdle);
    assert!(routing.label_name.is_empty());
    assert!(routing.proxy_labels.is_empty());
    assert!(routing.failed_backends.is_empty());
    assert_eq!(routing.failover_timeout_seconds, 60);
    for rate in [
        routing.status.migrations_per_second,
        routing.health.migrations_per_second,
        routing.memory.migrations_per_second,
        routing.cpu.migrations_per_second,
        routing.location.migrations_per_second,
        routing.connection.migrations_per_second,
    ] {
        assert_same_float(rate, 0.0);
    }
    assert_same_float(routing.connection.count_ratio_threshold, 0.0);
}

#[test]
fn routing_projection_preserves_normalized_policy_and_namespace_binding() {
    let store = ConfigNamespaceStore::from_toml(
        br#"
[proxy]
fail-backend-list = [" tidb-1:4000 ", "tidb-2", "tidb-1:4000"]
failover-timeout = 17

[balance]
label-name = "tenant"
routing-rule = "port"
policy = "location"
routing-policy = "random"

[balance.status]
migrations-per-second = 1.5
[balance.health]
migrations-per-second = 2.5
[balance.memory]
migrations-per-second = 3.5
[balance.cpu]
migrations-per-second = 4.5
[balance.location]
migrations-per-second = 5.5
[balance.conn-count]
migrations-per-second = 6.5
count-ratio-threshold = 1.25

[labels]
zone = "z1"
region = "r1"
"#,
        None,
        current_dir(),
    )
    .unwrap_or_else(|error| unreachable!("custom config: {error}"));
    let routing = store
        .current()
        .effective()
        .routing()
        .unwrap_or_else(|error| unreachable!("routing projection: {error}"));

    assert_eq!(routing.routing_rule, RoutingRule::ListenerPort);
    assert_eq!(routing.balance_policy, RoutingBalancePolicy::Location);
    assert_eq!(routing.selection_policy, RoutingSelectionPolicy::Random);
    assert_eq!(routing.label_name.as_ref(), "tenant");
    assert_eq!(
        routing
            .proxy_labels
            .iter()
            .map(|(name, value)| (name.as_ref(), value.as_ref()))
            .collect::<Vec<_>>(),
        vec![("region", "r1"), ("zone", "z1")]
    );
    assert_eq!(
        routing
            .failed_backends
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&str>>(),
        vec!["tidb-1:4000", "tidb-2"]
    );
    assert_eq!(routing.failover_timeout_seconds, 17);
    assert_same_float(routing.status.migrations_per_second, 1.5);
    assert_same_float(routing.health.migrations_per_second, 2.5);
    assert_same_float(routing.memory.migrations_per_second, 3.5);
    assert_same_float(routing.cpu.migrations_per_second, 4.5);
    assert_same_float(routing.location.migrations_per_second, 5.5);
    assert_same_float(routing.connection.migrations_per_second, 6.5);
    assert_same_float(routing.connection.count_ratio_threshold, 1.25);

    let namespace: NamespaceConfig = serde_json::from_str(
        r#"{
            "namespace":"sales",
            "frontend":{"user":"alice"},
            "backend":{"instances":["10.0.0.1:4000","10.0.0.2:4000"]}
        }"#,
    )
    .unwrap_or_else(|error| unreachable!("namespace: {error}"));
    let binding = namespace.routing();
    assert_eq!(binding.name.as_ref(), "sales");
    assert_eq!(
        binding
            .users
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&str>>(),
        vec!["alice"]
    );
    assert_eq!(
        binding
            .backend_instances
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&str>>(),
        vec!["10.0.0.1:4000", "10.0.0.2:4000"]
    );
    assert!(binding.backend_tls.ca_path.is_none());
}

#[test]
fn invalid_or_overflowed_routing_inputs_retain_the_exact_last_good_generation() {
    let store = ConfigNamespaceStore::from_toml(&[], None, current_dir())
        .unwrap_or_else(|error| unreachable!("default config: {error}"));
    let last_good = store.current();
    let invalid = [
        "[balance.health]\nmigrations-per-second = -1\n",
        "[balance.conn-count]\ncount-ratio-threshold = 1\n",
        "[proxy]\nfailover-timeout = -1\n",
        "[proxy]\nfailover-timeout = 9223372036854775808\n",
    ];

    for (index, candidate) in invalid.into_iter().enumerate() {
        let revision = u64::try_from(index + 2)
            .unwrap_or_else(|_| unreachable!("small test revision fits u64"));
        assert!(
            store
                .apply_toml(candidate.as_bytes(), None, revision, current_dir())
                .is_err(),
            "candidate {index} must fail closed"
        );
        let retained = store.current();
        assert!(Arc::ptr_eq(&retained, &last_good));
        assert_eq!(retained.generation(), 1);
        assert_eq!(
            retained
                .effective()
                .routing()
                .unwrap_or_else(|error| unreachable!("last-good projection: {error}")),
            last_good
                .effective()
                .routing()
                .unwrap_or_else(|error| unreachable!("last-good projection: {error}"))
        );
    }
}

#[test]
fn prepared_artifact_is_the_exact_validator_handle_and_is_fresh_per_generation() {
    let validator = Arc::new(RecordingValidator::new());
    let store = ConfigNamespaceStore::from_toml_with_validator(
        &[],
        None,
        current_dir(),
        Arc::clone(&validator) as Arc<dyn CandidateValidator>,
    )
    .unwrap_or_else(|error| unreachable!("initial config: {error}"));

    // Generation one carries the exact handle the validator prepared — the
    // store never re-prepares it — and it downcasts to the concrete type.
    let gen_one = store.current();
    let prepared_one = validator.last();
    assert!(gen_one.prepared().is_same_handle(&prepared_one));
    assert_eq!(
        gen_one.prepared().downcast_ref::<SerialArtifact>(),
        Some(&SerialArtifact(0))
    );
    // A wrong downcast target is a miss, never a panic.
    assert!(gen_one.prepared().downcast_ref::<u64>().is_none());
    // The artifact never leaks its contents through Debug.
    assert_eq!(format!("{:?}", gen_one.prepared()), "<redacted>");

    // A real change publishes a new generation carrying a distinct handle.
    let gen_two = store
        .apply_toml(b"[proxy]\nmax-connections = 20\n", None, 2, current_dir())
        .unwrap_or_else(|error| unreachable!("changed candidate: {error}"))
        .unwrap_or_else(|| unreachable!("a changed candidate publishes"));
    assert!(gen_two.prepared().is_same_handle(&validator.last()));
    assert!(!gen_two.prepared().is_same_handle(gen_one.prepared()));

    // External-material refresh advances the artifact even though the config
    // and namespace checksums are unchanged: a re-prepared handle rides it.
    let refreshed = store
        .refresh_external_material()
        .unwrap_or_else(|error| unreachable!("material refresh: {error}"));
    assert_eq!(refreshed.config_checksum(), gen_two.config_checksum());
    assert_eq!(refreshed.namespace_checksum(), gen_two.namespace_checksum());
    assert!(refreshed.prepared().is_same_handle(&validator.last()));
    assert!(!refreshed.prepared().is_same_handle(gen_two.prepared()));
}

#[test]
fn a_rejected_candidate_retains_the_prior_artifact_handle_and_does_not_publish_its_own() {
    let validator = Arc::new(RecordingValidator::new());
    let store = ConfigNamespaceStore::from_toml_with_validator(
        &[],
        None,
        current_dir(),
        Arc::clone(&validator) as Arc<dyn CandidateValidator>,
    )
    .unwrap_or_else(|error| unreachable!("initial config: {error}"));
    let before = store.current();
    let updates = store.subscribe();

    // Now reject. Snapshot equality excludes the artifact, so it can no longer
    // prove the prior artifact survived a rejection; assert the exact handle.
    validator.set_reject(true);
    let rejected = store.apply_toml(b"[proxy]\nmax-connections = 20\n", None, 2, current_dir());
    assert!(matches!(
        rejected,
        Err(StoreError::CandidateRejected {
            class: "recording_rejected"
        })
    ));
    let after = store.current();
    // The published snapshot is untouched: same generation and the exact same
    // artifact Arc as before the rejection.
    assert_eq!(after.generation(), before.generation());
    assert!(after.prepared().is_same_handle(before.prepared()));
    // The failed validation did prepare an artifact, but it was neither kept nor
    // published — no half-published handle leaks out.
    assert!(!after.prepared().is_same_handle(&validator.last()));
    assert!(!updates.has_changed().unwrap_or(false));
}

#[test]
fn generation_one_is_real_and_partial_dynamic_updates_are_atomic() {
    let store = ConfigNamespaceStore::from_toml(
        br"
[proxy]
max-connections = 10
",
        Some("proxy.example"),
        current_dir(),
    )
    .unwrap_or_else(|error| unreachable!("initial config: {error}"));
    let initial = store.current();
    assert_eq!(initial.generation(), 1);
    assert_eq!(initial.source_revision().file_revision, 1);
    assert_eq!(initial.effective().workdir(), "/var/lib/tiproxy-test/work");
    assert_eq!(
        initial
            .topology()
            .unwrap_or_else(|error| unreachable!("topology: {error}"))
            .advertise_host_override
            .as_deref(),
        Some("proxy.example")
    );

    let updates = store.subscribe();
    let accepted = store
        .apply_toml(
            br"
[proxy]
max-connections = 20
",
            Some("proxy.example"),
            2,
            current_dir(),
        )
        .unwrap_or_else(|error| unreachable!("dynamic update: {error}"))
        .unwrap_or_else(|| unreachable!("changed candidate publishes"));
    assert_eq!(accepted.generation(), 2);
    assert!(updates.has_changed().unwrap_or(false));

    let before_reject = store.current();
    let result = store.apply_toml(
        br#"
[proxy]
proxy-protocol = "v1"
"#,
        Some("proxy.example"),
        3,
        current_dir(),
    );
    assert!(matches!(
        result,
        Err(StoreError::Config(ConfigError::InvalidField {
            field: "proxy.proxy-protocol",
            class: "unsupported"
        }))
    ));
    assert_eq!(store.current(), before_reject);
    assert_eq!(store.observed_source_revision().file_revision, 3);
}

#[test]
fn external_material_refresh_advances_generation_and_retains_last_good_on_rejection() {
    let reject = Arc::new(AtomicBool::new(false));
    let store = ConfigNamespaceStore::from_toml_with_validator(
        &[],
        None,
        current_dir(),
        Arc::new(RejectableValidator(Arc::clone(&reject))),
    )
    .unwrap_or_else(|error| unreachable!("initial config: {error}"));
    let initial = store.current();
    let updates = store.subscribe();

    let refreshed = store
        .refresh_external_material()
        .unwrap_or_else(|error| unreachable!("material refresh: {error}"));
    assert_eq!(refreshed.generation(), 2);
    assert_eq!(refreshed.source_revision(), initial.source_revision());
    assert_eq!(refreshed.config_checksum(), initial.config_checksum());
    assert_eq!(refreshed.namespace_checksum(), initial.namespace_checksum());
    assert!(updates.has_changed().unwrap_or(false));
    assert_eq!(initial.generation(), 1);

    reject.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.refresh_external_material(),
        Err(StoreError::CandidateRejected {
            class: "test_material_rejected"
        })
    ));
    assert_eq!(store.current(), refreshed);
}

#[test]
fn restart_required_reload_fails_closed_and_noop_does_not_publish() {
    let store = ConfigNamespaceStore::from_toml(&[], None, current_dir())
        .unwrap_or_else(|error| unreachable!("initial config: {error}"));
    let updates = store.subscribe();

    let no_change = store
        .apply_toml(&[], None, 2, current_dir())
        .unwrap_or_else(|error| unreachable!("no-op config: {error}"));
    assert!(no_change.is_none());
    assert!(!updates.has_changed().unwrap_or(false));
    assert_eq!(store.observed_source_revision().file_revision, 2);

    let result = store.apply_toml(
        br#"
[proxy]
addr = "127.0.0.1:7000"
"#,
        None,
        3,
        current_dir(),
    );
    assert!(matches!(
        result,
        Err(StoreError::Config(ConfigError::RestartRequired {
            field: "proxy.addr"
        }))
    ));
    assert_eq!(store.current().generation(), 1);
    assert_eq!(store.observed_source_revision().file_revision, 3);
}

#[test]
fn routing_rule_update_is_accepted_for_future_router_construction() {
    let store = ConfigNamespaceStore::from_toml(&[], None, current_dir())
        .unwrap_or_else(|error| unreachable!("initial config: {error}"));
    let initial = store.current();
    store
        .apply_toml(
            b"[balance]\nrouting-rule = 'proxy_cidr'\n",
            None,
            2,
            current_dir(),
        )
        .unwrap_or_else(|error| unreachable!("accepted rule: {error}"));
    let accepted = store.current();
    assert!(accepted.generation() > initial.generation());
    assert_eq!(
        accepted
            .effective()
            .routing()
            .map(|policy| policy.routing_rule),
        Ok(RoutingRule::ProxyCidr)
    );
    assert!(
        store
            .apply_toml(
                b"[balance]\nrouting-rule = 'invalid'\n",
                None,
                3,
                current_dir()
            )
            .is_err()
    );
    assert_eq!(store.current(), accepted);
}

#[test]
fn ns_servers_are_normalized_sorted_stably_with_duplicates_preserved() {
    let build = |list: &str| {
        let toml = format!(
            "[proxy]\npd-addrs = \"pd-a:2379\"\n\n[[proxy.backend-clusters]]\nname = \"c\"\npd-addrs = \"pd-a:2379\"\nns-servers = {list}\n"
        );
        let store = ConfigNamespaceStore::from_toml(toml.as_bytes(), None, current_dir())
            .unwrap_or_else(|error| unreachable!("config: {error}"));
        let topology = store
            .current()
            .topology()
            .unwrap_or_else(|error| unreachable!("topology: {error}"));
        topology.backend_clusters[0]
            .ns_servers
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    };
    // Go `normalizeCluster` sorts the RAW input strings, THEN normalizes each in
    // that order (`:53` appended), preserving duplicates.
    let expected = vec![
        "dns-a:53".to_owned(),
        "dns-a:53".to_owned(),
        "dns-b:53".to_owned(),
        "dns-c:53".to_owned(),
    ];
    assert_eq!(build(r#"["dns-c", "dns-a", "dns-a", "dns-b"]"#), expected);
    // A reversed input of the same set yields the identical projection, so an
    // order-only config change cannot churn the artifact or shift the resolver's
    // round-robin start.
    assert_eq!(build(r#"["dns-b", "dns-a", "dns-c", "dns-a"]"#), expected);

    // A bare IPv6 literal with no port normalizes to `[<ip>]:53` (Go's
    // `net.JoinHostPort` fallback); an explicit bracketed port is preserved.
    assert_eq!(
        build(r#"["2001:db8::1"]"#),
        vec!["[2001:db8::1]:53".to_owned()]
    );
    assert_eq!(
        build(r#"["[2001:db8::1]:2379"]"#),
        vec!["[2001:db8::1]:2379".to_owned()]
    );
    // A bracketed hostname is legal Go input (`net.SplitHostPort`) and normalizes
    // to the UNBRACKETED form (brackets are kept only for IPv6 literals).
    assert_eq!(
        build(r#"["[dns.example]:53"]"#),
        vec!["dns.example:53".to_owned()]
    );

    // Mixed families: the RAW strings sort first (so the bare IPv6 sorts by its
    // raw `2...` key, not its normalized `[...` key), then normalize in place.
    assert_eq!(
        build(r#"["dns-b", "2001:db8::1", "10.0.0.1", "dns-a", "2001:db8::1"]"#),
        vec![
            "10.0.0.1:53".to_owned(),
            "[2001:db8::1]:53".to_owned(),
            "[2001:db8::1]:53".to_owned(),
            "dns-a:53".to_owned(),
            "dns-b:53".to_owned(),
        ]
    );
    // A distinguishing case: raw sort places `2001:db8::1` before `5.5.5.5`
    // before `aaa`; a normalize-then-sort would instead order the bracketed
    // `[2001:db8::1]:53` after `5.5.5.5:53`, so this asserts raw-sort parity.
    assert_eq!(
        build(r#"["aaa", "5.5.5.5", "2001:db8::1"]"#),
        vec![
            "[2001:db8::1]:53".to_owned(),
            "5.5.5.5:53".to_owned(),
            "aaa:53".to_owned(),
        ]
    );
}

#[test]
fn topology_projection_normalizes_clusters_and_tls() {
    let store = ConfigNamespaceStore::from_toml(
        br#"
[proxy]
pd-addrs = " pd-b:2379, pd-a:2379 "

[[proxy.backend-clusters]]
name = " cluster-b "
pd-addrs = "pd-b:2379"
ns-servers = ["dns-b"]

[[proxy.backend-clusters]]
name = "cluster-a"
pd-addrs = "pd-a:2379"
ns-servers = ["127.0.0.1:5353"]

[security.cluster-tls]
ca = "/etc/tiproxy/ca.pem"
cert = "/etc/tiproxy/client.pem"
key = "/etc/tiproxy/client-key.pem"
"#,
        None,
        current_dir(),
    )
    .unwrap_or_else(|error| unreachable!("initial config: {error}"));
    let topology = store
        .current()
        .topology()
        .unwrap_or_else(|error| unreachable!("topology: {error}"));
    assert_eq!(topology.backend_clusters.len(), 2);
    assert_eq!(topology.backend_clusters[0].name.as_ref(), "cluster-a");
    assert_eq!(
        topology.backend_clusters[1].ns_servers[0].as_ref(),
        "dns-b:53"
    );
    assert_eq!(
        topology.cluster_tls.ca_path.as_deref(),
        Some(Path::new("/etc/tiproxy/ca.pem"))
    );

    let persistence = store
        .current()
        .effective()
        .config_persistence()
        .unwrap_or_else(|| unreachable!("legacy PD transport is configured"));
    assert_eq!(
        persistence
            .pd_addrs
            .iter()
            .map(AsRef::<str>::as_ref)
            .collect::<Vec<_>>(),
        ["pd-b:2379", "pd-a:2379"]
    );
    assert_eq!(
        persistence.cluster_tls.ca_path.as_deref(),
        Some(Path::new("/etc/tiproxy/ca.pem"))
    );
}

#[test]
fn config_persistence_ignores_dynamic_backend_cluster_order() {
    let store = ConfigNamespaceStore::from_toml(
        br#"
[proxy]
pd-addrs = "owner-pd:2379"

[[proxy.backend-clusters]]
name = "a-first"
pd-addrs = "routing-pd:2379"
"#,
        None,
        current_dir(),
    )
    .unwrap_or_else(|error| unreachable!("initial config: {error}"));
    let persistence = store
        .current()
        .effective()
        .config_persistence()
        .unwrap_or_else(|| unreachable!("legacy PD transport is configured"));
    assert_eq!(persistence.pd_addrs[0].as_ref(), "owner-pd:2379");
}

#[test]
fn vip_requires_complete_ha_pair_and_one_backend_cluster() {
    let load = |source: &str| {
        ConfigNamespaceStore::from_toml(source.as_bytes(), None, current_dir())
            .unwrap_or_else(|error| unreachable!("VIP config: {error}"))
    };
    let basic = r#"
[proxy]
pd-addrs = "pd:2379"
[ha]
virtual-ip = "192.0.2.5/24"
interface = "eth0"
garp-burst-count = 0
garp-refresh-count = 2
"#;
    let store = load(basic);
    let vip = store
        .current()
        .effective()
        .vip()
        .unwrap_or_else(|error| unreachable!("VIP projection: {error}"))
        .unwrap_or_else(|| unreachable!("complete single-cluster VIP is enabled"));
    assert_eq!(vip.address.as_ref(), "192.0.2.5/24");
    assert_eq!(vip.interface.as_ref(), "eth0");
    assert_eq!(vip.election_name(), "/tiproxy/vip/192.0.2.5/owner");
    assert_eq!(vip.cidr(), "192.0.2.5/24");
    assert_eq!(vip.garp_burst_count, 1);
    assert_eq!(vip.garp_refresh_count, 2);

    let incomplete = load(&basic.replace("interface = \"eth0\"", "interface = \"\""));
    assert!(matches!(incomplete.current().effective().vip(), Ok(None)));
    let no_pd = load(&basic.replace("pd-addrs = \"pd:2379\"", "pd-addrs = \"\""));
    assert!(matches!(no_pd.current().effective().vip(), Ok(None)));
    let explicit_cluster = load(&basic.replace(
        "[ha]",
        "[[proxy.backend-clusters]]\nname = \"only\"\npd-addrs = \"cluster-pd:2379\"\n[ha]",
    ));
    let cluster_vip = explicit_cluster
        .current()
        .effective()
        .vip()
        .unwrap_or_else(|error| unreachable!("VIP projection: {error}"))
        .unwrap_or_else(|| unreachable!("single explicit backend cluster enables VIP"));
    assert_eq!(cluster_vip.pd_addrs[0].as_ref(), "cluster-pd:2379");
    let two_clusters = load(
        &basic.replace(
            "[ha]",
            "[[proxy.backend-clusters]]\nname = \"a\"\npd-addrs = \"pd-a:2379\"\n[[proxy.backend-clusters]]\nname = \"b\"\npd-addrs = \"pd-b:2379\"\n[ha]",
        ),
    );
    assert!(matches!(two_clusters.current().effective().vip(), Ok(None)));

    let invalid_address = load(&basic.replace("192.0.2.5/24", "192.0.2.5/33"));
    assert!(matches!(
        invalid_address.current().effective().vip(),
        Err(ConfigError::InvalidField {
            field: "ha.virtual-ip",
            class: "invalid_ip_or_cidr"
        })
    ));
}

#[test]
fn config_module_rejects_a_factory_that_disagrees_with_initial_transport() {
    let initial = EtcdClientConfig::new(["initial-pd:2379".to_owned()], None)
        .unwrap_or_else(|error| unreachable!("initial transport: {error}"));
    let replacement = EtcdClientConfig::new(["different-pd:2379".to_owned()], None)
        .unwrap_or_else(|error| unreachable!("replacement transport: {error}"));
    let result = ConfigModule::load(ConfigModuleOptions {
        config_file: None,
        advertise_addr: None,
        current_dir: current_dir().to_owned(),
        etcd: Some(initial),
        election: Some(
            ElectionConfig::new("/election", "member", "/session/member", 3)
                .unwrap_or_else(|error| unreachable!("election: {error}")),
        ),
        persistence_factory: Some(Arc::new(move |_effective: &EffectiveConfig| {
            Ok(Some(replacement.clone()))
        })),
    });
    assert!(matches!(
        result,
        Err(StoreError::PersistentEntry {
            class: "persistence_transport_mismatch"
        })
    ));
}

#[test]
fn config_module_validates_persistence_transport_before_publishing_generation_one() {
    let initial = EtcdClientConfig::new(["initial-pd:2379".to_owned()], None)
        .unwrap_or_else(|error| unreachable!("initial transport: {error}"));
    let result = ConfigModule::load(ConfigModuleOptions {
        config_file: None,
        advertise_addr: None,
        current_dir: current_dir().to_owned(),
        etcd: Some(initial),
        election: Some(
            ElectionConfig::new("/election", "member", "/session/member", 3)
                .unwrap_or_else(|error| unreachable!("election: {error}")),
        ),
        persistence_factory: Some(Arc::new(|_effective: &EffectiveConfig| {
            Err("rejected transport".to_owned())
        })),
    });
    assert!(matches!(
        result,
        Err(StoreError::CandidateRejected {
            class: "persistence_transport_rejected"
        })
    ));
}

#[test]
fn legacy_pd_addrs_fall_back_to_default_cluster() {
    let store = ConfigNamespaceStore::from_toml(&[], None, current_dir())
        .unwrap_or_else(|error| unreachable!("initial config: {error}"));
    let topology = store
        .current()
        .topology()
        .unwrap_or_else(|error| unreachable!("topology: {error}"));
    assert_eq!(topology.backend_clusters.len(), 1);
    assert_eq!(topology.backend_clusters[0].name.as_ref(), "default");
    assert_eq!(
        topology.backend_clusters[0].pd_addrs[0].as_ref(),
        "127.0.0.1:2379"
    );
}

#[test]
fn legacy_namespace_json_is_name_checked_and_sorted() {
    let namespace_b = control_config::source::decode_namespace(
        "b",
        br#"{"namespace":"b","frontend":{"user":"user-b"},"backend":{"instances":["b:4000"]}}"#,
    )
    .unwrap_or_else(|error| unreachable!("namespace b: {error}"));
    let namespace_a = control_config::source::decode_namespace(
        "a",
        br#"{"namespace":"a","frontend":{"user":"user-a"},"backend":{"instances":["a:4000"]}}"#,
    )
    .unwrap_or_else(|error| unreachable!("namespace a: {error}"));
    assert!(matches!(
        control_config::source::decode_namespace("wrong", br#"{"namespace":"actual"}"#,),
        Err(StoreError::Namespace {
            class: "key_value_name_mismatch"
        })
    ));

    let store = ConfigNamespaceStore::new(
        EffectiveConfig::default(),
        vec![namespace_b, namespace_a],
        SourceRevision::default(),
        current_dir(),
    )
    .unwrap_or_else(|error| unreachable!("namespace store: {error}"));
    assert_eq!(store.current().namespaces()[0].namespace, "a");
    assert_eq!(store.current().namespaces()[1].namespace, "b");
}

#[test]
fn debug_output_redacts_metering_credentials() {
    let store = ConfigNamespaceStore::from_toml(
        br#"
[metering]
type = "s3"
endpoint = "https://store.invalid/?sig=endpoint-secret"
[metering.aws]
access-key = "AKIA-DO-NOT-LOG"
secret-access-key = "super-secret"
session-token = "temporary-secret"
"#,
        None,
        current_dir(),
    )
    .unwrap_or_else(|error| unreachable!("metering config: {error}"));
    let debug = format!("{:?}", store.current().effective());
    assert!(!debug.contains("AKIA-DO-NOT-LOG"));
    assert!(!debug.contains("super-secret"));
    assert!(!debug.contains("temporary-secret"));
    assert!(!debug.contains("endpoint-secret"));
}

#[test]
fn persistent_overlay_masks_file_updates_then_reveals_latest_file_base() {
    let store = ConfigNamespaceStore::from_toml(
        br"
[proxy]
max-connections = 10
",
        None,
        current_dir(),
    )
    .unwrap_or_else(|error| unreachable!("initial config: {error}"));
    let proxy = control_config::source::decode_proxy_online(br#"{"max-connections":20}"#)
        .unwrap_or_else(|error| unreachable!("proxy overlay: {error}"));
    let overlay = PersistentConfigSnapshot {
        proxy: Some(proxy),
        ..PersistentConfigSnapshot::default()
    };
    let accepted = store
        .apply_persistent(overlay, 4, current_dir())
        .unwrap_or_else(|error| unreachable!("persistent update: {error}"))
        .unwrap_or_else(|| unreachable!("changed overlay publishes"));
    assert_eq!(proxy_max_connections(&accepted), 20);

    let masked = store
        .apply_toml(
            br"
[proxy]
max-connections = 30
",
            None,
            2,
            current_dir(),
        )
        .unwrap_or_else(|error| unreachable!("masked file update: {error}"));
    assert!(masked.is_none());
    assert_eq!(proxy_max_connections(&store.current()), 20);
    assert_eq!(store.observed_source_revision().file_revision, 2);

    let revealed = store
        .apply_persistent(PersistentConfigSnapshot::default(), 5, current_dir())
        .unwrap_or_else(|error| unreachable!("overlay removal: {error}"))
        .unwrap_or_else(|| unreachable!("revealed file value publishes"));
    assert_eq!(revealed.generation(), 3);
    assert_eq!(proxy_max_connections(&revealed), 30);
    assert_eq!(revealed.source_revision().file_revision, 2);
    assert_eq!(revealed.source_revision().etcd_revision, 5);
}

#[test]
fn stale_persistent_revision_cannot_roll_back_an_optimistic_owner_apply() {
    let store = ConfigNamespaceStore::from_toml(&[], None, current_dir())
        .unwrap_or_else(|error| unreachable!("initial config: {error}"));
    let mut newest = PersistentConfigSnapshot::default();
    let proxy = control_config::source::decode_proxy_online(br#"{"max-connections":20}"#)
        .unwrap_or_else(|error| unreachable!("newest proxy: {error}"));
    newest.proxy = Some(proxy);
    store
        .apply_persistent(newest, 10, current_dir())
        .unwrap_or_else(|error| unreachable!("newest persistent view: {error}"));

    let proxy = control_config::source::decode_proxy_online(br#"{"max-connections":30}"#)
        .unwrap_or_else(|error| unreachable!("stale proxy: {error}"));
    let stale = PersistentConfigSnapshot {
        proxy: Some(proxy),
        ..PersistentConfigSnapshot::default()
    };
    let ignored = store
        .apply_persistent(stale, 9, current_dir())
        .unwrap_or_else(|error| unreachable!("stale view is ignored: {error}"));
    assert!(ignored.is_none());
    assert_eq!(
        store
            .current()
            .effective()
            .serving()
            .unwrap_or_else(|error| unreachable!("serving config: {error}"))
            .max_connections,
        20
    );
}

#[test]
fn malformed_revision_is_rejected_without_blocking_the_next_revision() {
    let store = ConfigNamespaceStore::from_toml(&[], None, current_dir())
        .unwrap_or_else(|error| unreachable!("initial config: {error}"));
    store.observe_etcd_revision(8);
    let malformed = decode_persistent_entries([(
        b"/config/ns/broken".as_slice(),
        br#"{"namespace":"other"}"#.as_slice(),
    )]);
    assert!(matches!(
        malformed,
        Err(StoreError::PersistentNamespace {
            ref key,
            class: "key_value_name_mismatch"
        }) if key == "/config/ns/broken"
    ));
    assert_eq!(store.current().generation(), 1);
    assert_eq!(store.observed_source_revision().etcd_revision, 8);

    let valid = decode_persistent_entries([(
        b"/config/ns/valid".as_slice(),
        br#"{"namespace":"valid","frontend":{"user":"alice"}}"#.as_slice(),
    )])
    .unwrap_or_else(|error| unreachable!("valid revision: {error}"));
    let accepted = store
        .apply_persistent(valid, 9, current_dir())
        .unwrap_or_else(|error| unreachable!("valid apply: {error}"))
        .unwrap_or_else(|| unreachable!("valid namespace publishes"));
    assert_eq!(accepted.generation(), 2);
    assert_eq!(accepted.source_revision().etcd_revision, 9);
    assert_eq!(accepted.namespaces()[0].namespace, "valid");
}

#[test]
fn persistent_decode_is_bounded_atomic_and_ignores_unknown_compatible_keys() {
    let decoded = decode_persistent_entries([
        (
            b"/config/future".as_slice(),
            b"opaque-future-value".as_slice(),
        ),
        (
            b"/config/ns/alpha".as_slice(),
            br#"{"namespace":"alpha"}"#.as_slice(),
        ),
    ])
    .unwrap_or_else(|error| unreachable!("compatible decode: {error}"));
    assert_eq!(decoded.namespaces.len(), 1);
    assert_eq!(decoded.namespaces[0].namespace, "alpha");

    let oversized = vec![b'x'; 64 * 1_024 + 1];
    assert!(matches!(
        decode_persistent_entries([(b"/config/proxy".as_slice(), oversized.as_slice())]),
        Err(StoreError::PersistentEntry {
            class: "value_too_large"
        })
    ));
    assert!(matches!(
        decode_persistent_entries([
            (b"/config/log".as_slice(), br#"{"level":"warn"}"#.as_slice()),
            (
                b"/config/log".as_slice(),
                br#"{"level":"debug"}"#.as_slice()
            ),
        ]),
        Err(StoreError::PersistentEntry {
            class: "duplicate_key"
        })
    ));
}

#[test]
fn go_duration_strings_and_legacy_json_nanoseconds_are_both_accepted() {
    let store = ConfigNamespaceStore::from_toml(
        br#"
[proxy.frontend-keepalive]
enabled = true
idle = "1h2m3.004s"
cnt = 7
intvl = "2500us"
timeout = "4.5s"
"#,
        None,
        current_dir(),
    )
    .unwrap_or_else(|error| unreachable!("duration TOML: {error}"));
    let serving = store
        .current()
        .effective()
        .serving()
        .unwrap_or_else(|error| unreachable!("serving projection: {error}"));
    assert_eq!(serving.frontend_keepalive.idle_millis, 3_723_004);
    assert_eq!(serving.frontend_keepalive.probe_count, 7);
    assert_eq!(serving.frontend_keepalive.interval_millis, 2);
    assert_eq!(serving.frontend_keepalive.user_timeout_millis, 4_500);

    let proxy = control_config::source::decode_proxy_online(
        br#"{"frontend-keepalive":{"enabled":true,"idle":2000000000,"cnt":3,"intvl":1000000000,"timeout":5000000000}}"#,
    )
    .unwrap_or_else(|error| unreachable!("duration JSON: {error}"));
    let applied = store
        .apply_persistent(
            PersistentConfigSnapshot {
                proxy: Some(proxy),
                ..PersistentConfigSnapshot::default()
            },
            2,
            current_dir(),
        )
        .unwrap_or_else(|error| unreachable!("persistent duration: {error}"))
        .unwrap_or_else(|| unreachable!("duration overlay changed"));
    let serving = applied
        .effective()
        .serving()
        .unwrap_or_else(|error| unreachable!("persistent serving projection: {error}"));
    assert_eq!(serving.frontend_keepalive.idle_millis, 2_000);
    assert_eq!(serving.frontend_keepalive.probe_count, 3);
    assert_eq!(serving.frontend_keepalive.interval_millis, 1_000);
    assert_eq!(serving.frontend_keepalive.user_timeout_millis, 5_000);
}

#[test]
fn malformed_or_sub_millisecond_go_duration_rejects_atomically() {
    assert!(matches!(
        ConfigNamespaceStore::from_toml(
            br#"
[proxy.frontend-keepalive]
idle = "1fortnight"
"#,
            None,
            current_dir(),
        ),
        Err(StoreError::TomlDecode(_))
    ));
    assert!(matches!(
        ConfigNamespaceStore::from_toml(
            br#"
[proxy.frontend-keepalive]
idle = "999us"
"#,
            None,
            current_dir(),
        ),
        Err(StoreError::Config(ConfigError::InvalidField {
            field: "proxy.frontend-keepalive",
            class: "sub_millisecond"
        }))
    ));
}

#[test]
fn canonical_full_config_checksum_matches_production_go_encoder() {
    let data = include_bytes!("../../../../tests/controlplane/cp004/testdata/full.toml");
    let store = ConfigNamespaceStore::from_toml(data, None, current_dir())
        .unwrap_or_else(|error| unreachable!("full parity fixture: {error}"));
    // Generated from lib/config.Config.ToBytes using the same full fixture.
    assert_eq!(store.current().config_checksum(), 2_137_169_756);
}

fn proxy_max_connections(snapshot: &control_config::ConfigNamespaceSnapshot) -> u64 {
    serde_json::to_value(snapshot.effective().as_ref())
        .unwrap_or_else(|error| unreachable!("serialize effective config: {error}"))["proxy"]
        ["max-connections"]
        .as_u64()
        .unwrap_or_else(|| unreachable!("max-connections is an unsigned integer"))
}

#[test]
fn namespace_incarnation_tracks_continuity_without_changing_content_identity()
-> Result<(), StoreError> {
    let store = ConfigNamespaceStore::new(
        EffectiveConfig::default(),
        vec![NamespaceConfig {
            namespace: "default".into(),
            ..NamespaceConfig::default()
        }],
        SourceRevision::default(),
        Path::new("/tmp"),
    )?;
    let first = store.current();
    store.apply_toml(
        b"[balance]\nrouting-policy = \"random\"",
        None,
        1,
        Path::new("/tmp"),
    )?;
    let policy = store.current();
    assert!(first.same_namespace_incarnation(&policy, "default"));
    let refreshed = store.refresh_external_material()?;
    assert!(policy.same_namespace_incarnation(&refreshed, "default"));
    assert_eq!(policy.namespace_checksum(), refreshed.namespace_checksum());
    assert!(
        store
            .apply(
                (**refreshed.effective()).clone(),
                refreshed.namespaces().to_vec(),
                SourceRevision::default(),
                Path::new("/tmp")
            )?
            .is_none()
    );
    let mut replaced = first.namespaces().to_vec();
    replaced[0].frontend.user = "replacement".into();
    store.apply(
        (**refreshed.effective()).clone(),
        replaced,
        SourceRevision::default(),
        Path::new("/tmp"),
    )?;
    let changed = store.current();
    assert!(!first.same_namespace_incarnation(&changed, "default"));
    store.apply(
        (**refreshed.effective()).clone(),
        first.namespaces().to_vec(),
        SourceRevision::default(),
        Path::new("/tmp"),
    )?;
    let restored = store.current();
    assert!(!first.same_namespace_incarnation(&restored, "default"));
    assert!(!changed.same_namespace_incarnation(&restored, "default"));
    store.apply(
        (**refreshed.effective()).clone(),
        Vec::new(),
        SourceRevision::default(),
        Path::new("/tmp"),
    )?;
    store.apply(
        (**refreshed.effective()).clone(),
        first.namespaces().to_vec(),
        SourceRevision::default(),
        Path::new("/tmp"),
    )?;
    assert!(!restored.same_namespace_incarnation(&store.current(), "default"));
    let foreign = ConfigNamespaceStore::new(
        (**first.effective()).clone(),
        first.namespaces().to_vec(),
        first.source_revision(),
        Path::new("/tmp"),
    )?;
    assert_eq!(first, foreign.current());
    assert!(!first.same_namespace_incarnation(&foreign.current(), "default"));
    Ok(())
}

#[test]
fn startup_namespace_seed_is_one_shot_and_explicit_empty_can_remove_it() -> Result<(), StoreError> {
    let store = ConfigNamespaceStore::from_toml(b"", None, Path::new("/tmp"))?;
    let default = NamespaceConfig {
        namespace: "default".to_owned(),
        ..NamespaceConfig::default()
    };
    let seeded = store.bootstrap_namespaces_if_empty(vec![default.clone()], Path::new("/tmp"))?;
    assert_eq!(
        seeded.as_ref().map(|snapshot| snapshot.generation()),
        Some(2)
    );
    let incarnation = store
        .current()
        .namespace_incarnation("default")
        .unwrap_or_else(|| unreachable!("the bootstrap namespace has an incarnation"));
    assert!(
        store
            .bootstrap_namespaces_if_empty(
                vec![NamespaceConfig {
                    namespace: "replacement".to_owned(),
                    ..NamespaceConfig::default()
                }],
                Path::new("/tmp"),
            )?
            .is_none()
    );
    assert!(
        store
            .current()
            .namespace_incarnation("default")
            .is_some_and(|current| current.same_as(&incarnation))
    );

    assert!(
        store
            .apply_persistent(
                PersistentConfigSnapshot {
                    namespaces: vec![default.clone()],
                    ..PersistentConfigSnapshot::default()
                },
                1,
                Path::new("/tmp"),
            )?
            .is_none(),
        "an equal completed relist preserves the seeded incarnation"
    );
    assert!(
        store
            .current()
            .namespace_incarnation("default")
            .is_some_and(|current| current.same_as(&incarnation))
    );

    let mut replacement = default;
    replacement.frontend.user = "replacement".to_owned();
    store.apply_persistent(
        PersistentConfigSnapshot {
            namespaces: vec![replacement],
            ..PersistentConfigSnapshot::default()
        },
        2,
        Path::new("/tmp"),
    )?;
    assert!(
        store
            .current()
            .namespace_incarnation("default")
            .is_some_and(|current| !current.same_as(&incarnation)),
        "a real replacement mints a fresh namespace incarnation"
    );

    store.apply_persistent(PersistentConfigSnapshot::default(), 3, Path::new("/tmp"))?;
    assert!(store.current().namespaces().is_empty());
    assert!(
        store
            .bootstrap_namespaces_if_empty(
                vec![NamespaceConfig {
                    namespace: "default".to_owned(),
                    ..NamespaceConfig::default()
                }],
                Path::new("/tmp"),
            )?
            .is_none(),
        "an explicit empty relist cannot revive the startup-only seed"
    );
    assert!(store.current().namespaces().is_empty());

    assert_preexisting_namespace_consumes_startup_seed()?;
    Ok(())
}

fn assert_preexisting_namespace_consumes_startup_seed() -> Result<(), StoreError> {
    let store = ConfigNamespaceStore::new(
        EffectiveConfig::default(),
        vec![NamespaceConfig {
            namespace: "tenant".to_owned(),
            ..NamespaceConfig::default()
        }],
        SourceRevision::default(),
        Path::new("/tmp"),
    )?;
    assert!(
        store
            .bootstrap_namespaces_if_empty(
                vec![NamespaceConfig {
                    namespace: "default".to_owned(),
                    ..NamespaceConfig::default()
                }],
                Path::new("/tmp"),
            )?
            .is_none(),
        "a nonempty startup relist consumes the one startup attempt without seeding"
    );
    store.apply_persistent(PersistentConfigSnapshot::default(), 1, Path::new("/tmp"))?;
    assert!(
        store
            .bootstrap_namespaces_if_empty(
                vec![NamespaceConfig {
                    namespace: "default".to_owned(),
                    ..NamespaceConfig::default()
                }],
                Path::new("/tmp"),
            )?
            .is_none()
    );
    assert!(store.current().namespaces().is_empty());
    Ok(())
}

#[test]
fn resource_incarnation_records_coalesced_policy_lifetimes() {
    fn must<T, E: std::fmt::Debug>(result: Result<T, E>) -> T {
        result.unwrap_or_else(|error| unreachable!("config fixture: {error:?}"))
    }
    let root = Path::new("/tmp");
    let store = must(ConfigNamespaceStore::from_toml(
        b"[balance]\npolicy=\"resource\"",
        None,
        root,
    ));
    let mut watch = store.subscribe();
    let original = store.current().resource_incarnation();
    assert!(original.enabled());
    must(store.apply_toml(b"[balance]\npolicy=\"location\"", None, 2, root));
    assert!(
        original.same_as(&store.current().resource_incarnation()),
        "COMPOSE_RESOURCE_LOCATION_CONTINUITY"
    );
    must(store.apply_toml(b"[log]\nlevel=\"debug\"", None, 3, root));
    assert!(
        original.same_as(&store.current().resource_incarnation()),
        "COMPOSE_UNRELATED_CONFIG_CONTINUITY"
    );
    assert!(
        store
            .apply_toml(b"[balance]\npolicy=\"invalid\"", None, 4, root)
            .is_err()
    );
    assert!(
        original.same_as(&store.current().resource_incarnation()),
        "COMPOSE_REJECTED_CONFIG_CONTINUITY"
    );
    must(store.apply_toml(b"[balance]\npolicy=\"connection\"", None, 5, root));
    assert!(!store.current().resource_incarnation().enabled());
    must(store.apply_toml(b"[balance]\npolicy=\"resource\"", None, 6, root));
    let last = watch.borrow_and_update().resource_incarnation();
    assert!(last.enabled());
    assert!(!original.same_as(&last), "COMPOSE_COALESCED_CONNECTION_ABA");
    let foreign = must(ConfigNamespaceStore::from_toml(
        b"[balance]\npolicy=\"resource\"",
        None,
        root,
    ));
    assert!(
        !last.same_as(&foreign.current().resource_incarnation()),
        "COMPOSE_FOREIGN_POLICY_IDENTITY"
    );
}

#[test]
fn routing_observer_socket_is_disabled_by_default_validated_and_restart_pinned() {
    let load = |text: &[u8]| ConfigNamespaceStore::from_toml(text, None, current_dir());
    let disabled = load(&[]).unwrap_or_else(|error| unreachable!("default: {error}"));
    assert!(
        disabled
            .current()
            .effective()
            .routing_shadow_socket()
            .is_none()
    );
    let input = br#"enable-traffic-replay=false
[rust-dataplane]
enabled=true
control-socket="/tmp/control.sock"
routing-shadow-socket="/tmp/observer.sock"
"#;
    let enabled = load(input).unwrap_or_else(|error| unreachable!("observer: {error}"));
    assert_eq!(
        enabled.current().effective().routing_shadow_socket(),
        Some(Path::new("/tmp/observer.sock"))
    );
    assert!(matches!(
        enabled.apply_toml(
            &String::from_utf8_lossy(input)
                .replace("observer.sock", "next.sock")
                .into_bytes(),
            None,
            2,
            current_dir()
        ),
        Err(StoreError::Config(ConfigError::RestartRequired {
            field: "rust-dataplane"
        }))
    ));
    assert_eq!(disabled.current().generation(), 1);
    for invalid in [
        "[rust-dataplane]\nrouting-shadow-socket='/tmp/observer.sock'",
        "[rust-dataplane]\nenabled=true\nrouting-shadow-socket='relative.sock'",
        "[rust-dataplane]\nenabled=true\ncontrol-socket='/tmp/control.sock'\nrouting-shadow-socket='/tmp/control.sock'",
    ] {
        assert!(
            load(format!("enable-traffic-replay=false\n{invalid}").as_bytes()).is_err(),
            "routing observer validation: {invalid}"
        );
    }
}

#[test]
fn metrics_owner_port_is_explicit_restart_pinned_and_collision_free() {
    let load = |text: &[u8]| ConfigNamespaceStore::from_toml(text, None, current_dir());
    let disabled = load(&[]).unwrap_or_else(|error| unreachable!("default: {error}"));
    assert_eq!(
        disabled
            .current()
            .effective()
            .topology()
            .unwrap_or_else(|error| unreachable!("default topology: {error}"))
            .metrics_owner_port,
        0
    );

    let input = b"[rust-dataplane]\nmetrics-owner-port=7443\n";
    let fixed = load(input).unwrap_or_else(|error| unreachable!("fixed port: {error}"));
    assert_eq!(
        fixed
            .current()
            .effective()
            .topology()
            .unwrap_or_else(|error| unreachable!("fixed topology: {error}"))
            .metrics_owner_port,
        7443
    );
    assert!(matches!(
        fixed.apply_toml(
            b"[rust-dataplane]\nmetrics-owner-port=7444\n",
            None,
            2,
            current_dir()
        ),
        Err(StoreError::Config(ConfigError::RestartRequired {
            field: "rust-dataplane"
        }))
    ));
    assert_eq!(
        fixed.current().generation(),
        1,
        "a restart-level change must not publish or invalidate the retained generation"
    );
    assert_eq!(
        fixed
            .current()
            .effective()
            .topology()
            .unwrap_or_else(|error| unreachable!("retained topology: {error}"))
            .metrics_owner_port,
        7443
    );

    for invalid in [
        "[rust-dataplane]\nmetrics-owner-port=-1",
        "[rust-dataplane]\nmetrics-owner-port=65536",
        "[rust-dataplane]\nmetrics-owner-port=6000",
        "[api]\naddr='0.0.0.0:7443'\n[rust-dataplane]\nmetrics-owner-port=7443",
        "[proxy]\nport-range=[6001,6003]\n[rust-dataplane]\nmetrics-owner-port=6002",
    ] {
        assert!(
            load(invalid.as_bytes()).is_err(),
            "invalid metrics owner port candidate: {invalid}"
        );
    }
}
