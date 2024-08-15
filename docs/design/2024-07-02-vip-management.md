# Proposal: VIP-Management

- Author(s): [djshow832](https://github.com/djshow832)
- Tracking Issue: https://github.com/pingcap/tiproxy/issues/583

## Abstract

This proposes a design of managing VIP on TiProxy clusters to achieve high availability of TiProxy without deploying third-party tools.

## Terms

- VIP: Virtual IP
- NIC: Network Interface Card
- ARP: Address Resolution Protocol
- VRRP: Virtual Router Redundancy Protocol
- MMM: Multi-Master Replication Manager for MySQL
- MHA: Master High Availability

## Background

In a self-hosted TiDB cluster with TiProxy, TiProxy is typically the endpoint for clients. To achieve high availability, users may deploy multiple TiProxy instances and only one serves requests so that the client can configure only one database address. When the active TiProxy is down, the cluster should elect another TiProxy automatically and the client doesn't need to update the database address.

So we need a VIP solution. The VIP is always bound to an available TiProxy node. When the active node is down, VIP floats to another node.

<img src="./imgs/vip-arch.png" alt="vip architecture" width="600">

Currently, typical solutions include:

- Deploy keepalived together with TiProxy. Keepalived is capable of both health checks and VIP management.
- Deploy a crontab job to check the health of TiProxy and set VIP

Both ways are not easy to use. This design proposes a solution to enable the TiProxy cluster to manage VIP by itself.

## Goals

- Bind a VIP to an available TiProxy node and switch the VIP when the node becomes unavailable
- Support VIP management on self-hosted TiDB clusters that run on bare metal with Linux

## Non-Goals

- Support configuring weights of TiProxy nodes
- Support configuring multiple VIPs for a TiProxy cluster
- Support VIP management on Docker, Kubernetes, or cloud
- Support VIP management on non-Linux operating systems

## Proposal

### Active Node Election

Firstly, the TiProxy cluster needs to elect an available instance to be the active node. Etcd is built in PD and is capable of leader election, so we can just use the Etcd election. The first instance booted will be the leader for the first election round.

When an instance is chosen to be active, it binds VIP to itself. It unbinds the VIP when:

- It finds that it's no longer the leader, maybe because its network is unstable and Etcd evicts it
- It's shutting down, maybe because TiProxy instances are rolling upgrade

### Failover

The Etcd session TTL determines the RTO. Longer TTL makes the RTO longer, while shorter TTL makes the leader switch frequently in a bad network. We set it to 3 seconds, thus the RTO should be nearly 3 seconds.

During the shutdown of the active node, the active node's unbinding and the standby node's binding happen concurrently. If the unbinding comes first, the clients may fail to connect. To ensure the binding comes first, the active node resigns the leader before graceful waiting and unbinds after graceful waiting.

When the PD leader is down and before a new leader is elected, all TiProxy nodes can't connect to the Etcd server. If the owner unbinds the VIP, the clients can't connect to TiProxy with the VIP. Thus, the owner doesn't unbind the VIP until the next active node is elected.

### Adding and Deleting VIP

Once a node is chosen to be active, it binds the VIP to itself through 2 steps:

1. Attach a secondary IP to the specified NIC through netlink
2. Notify the whole subnet through ARP about the IP and MAC address so that the clients update the ARP cache

There may be some time when the previous active node doesn't unbind the VIP in time and the new active node binds the VIP. The second step ensures that the clients connect to the new node because the ARP cache is updated. The connections to the previous node continue until the clients disconnect them.

These steps are equal to the Linux commands:

```shell
ip addr add 192.168.148.100/24 dev eth0
arping -q -c 1 -U -I eth0 192.168.148.100
```

The secondary IP is used in MySQL HA clusters such as MMM and MHA. The limitation is that the secondary IP should be reserved in the subnet and it only works in the same subnet.

The TiProxy user must be privileged to run `ip addr add`, `ip addr del` and `arping`, meaning that it should be the `root`. However, TiProxy is typically deployed by TiUP and TiUP only needs the `sudo` permission, so TiProxy should retry with `sudo` if the permission is denied, but it requires `ip` and `arping` to be installed.

## Configuration

All TiProxy instances have the same configuration:

```yaml
[ha]
  vip="192.168.148.100"
  interface="eth0"
```

`vip` declares the VIP and `interface` declares the network interface (or NIC device) to which the VIP is bound. If any of them is not configured, the instance won't preempt VIP.

It's possible to update configurations online but it's unnecessary. The clients need to update the database address and it interrupts the business anyway, so we don't support update configurations online.

## Observability

Besides logs, we can show the current active node on Grafana.

## Alternatives

### Consensus Algorithms

Some products use consensus algorithms such as Raft and Paxos to elect the active node. It's straightforward but has some disadvantages:

- The consensus algorithms need at least 3 nodes, while users usually need only 2.
- If there's a network partition, the elected node must be able to connect to the PD leader, while the active node elected by the consensus algorithm may be in another partition with the PD leader. If so, the node will route to the TiDB instances that are unable to connect to the PD leader either.

### VRRP

VRRP is another VIP solution and is applied by Keepalived, a tool widely used by proxies, including HAProxy.

The problem is that VRRP is too complicated to troubleshoot.

## Future works

### Weight Configuration

Node weights may be useful when users have preferences for active nodes. If the node with the highest weight is available, it holds the VIP until it's down. On top of this, some products also have a preempt mode. That is, when the preferred node recovers, it should take back the VIP even if the current active node is still available.

Although some products support configuring node weights, it's not so straightforward to implement on etcd and may not be necessary. We'll consider it in the future if users require it. Currently, all the nodes share the same possibility of being active.

### Multiple VIPs

Some MySQL clusters use one VIP for write nodes and multiple VIPs for read nodes. Similarly, TiProxy can have multiple VIPs to expose multiple endpoints for resource isolation. It needs to partition TiProxy and TiDB instances into node groups and each TiProxy only routes to the TiDB in the same group.

It changes the election procedure and TiProxy configuration. We'll consider it if users require it.
