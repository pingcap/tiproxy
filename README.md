## What is TiProxy?

TiProxy is a database proxy that is based on TiDB. It keeps client connections alive while the TiDB server upgrades, restarts, scales in, and scales out.

TiProxy is forked from [Weir](https://github.com/tidb-incubator/weir).

## Features

### Connection Management

When a TiDB instance restarts or shuts down, TiProxy migrates backend connections on this instance to other instances. In this way, the clients won't be disconnected.

For more details, please refer to the blogs [Achieving Zero-Downtime Upgrades with TiDB](https://www.pingcap.com/blog/achieving-zero-downtime-upgrades-tidb/) and [Maintaining Database Connectivity in Serverless Infrastructure with TiProxy](https://www.pingcap.com/blog/maintaining-database-connectivity-in-serverless-infra-with-tiproxy/).

### Load Balance

TiProxy routes new connections to backends based on their scores to keep load balanced. The score is basically calculated from the connections on each backend.

Besides, when the clients create or close connections, TiProxy also migrates backend connections to keep the backends balanced.

### Service Discovery

When a new TiDB instance starts, the TiProxy detects the new TiDB instance and migrates backend connections to the instance.

The TiProxy also checks health on TiDB instances to ensure they are alive, and migrates the backend connections to other TiDB instances if any instance is down.

## Architecture

For more details, see [Design Doc](https://github.com/pingcap/tidb/blob/master/docs/design/2022-07-20-session-manager.md).

## Future Plans

TiProxy's role as a versatile database proxy is continuously evolving to meet the diverse needs of self-hosting users. Here are some of the key expectations that TiProxy is poised to fulfill:

### Tenant Isolation

In a multi-tenant database environment that supports database consolidation, TiProxy offers the ability to route connections based on usernames or client addresses. This ensures the effective isolation of TiDB resources, safeguarding data and performance for different tenants.

### Traffic Management

Sudden traffic spikes can catch any system off guard. TiProxy steps in with features like rate limiting and query refusal in extreme cases, enabling you to better manage and control incoming traffic to TiDB.

### Post-Upgrade Validation

Ensuring the smooth operation of TiDB after an upgrade is crucial. TiProxy can play a vital role in this process by replicating traffic and replaying it on a new TiDB cluster. This comprehensive testing helps verify that the upgraded system works as expected.

## Build

Build the binary locally:

```shell
$ make
```

Build a docker image:

```shell
$ make docker
```

## Deployment

### Deploy with TiUP

Refer to https://docs.pingcap.com/tidb/dev/tiproxy-overview#installation-and-usage.

### Deploy with TiDB-Operator

Refer to https://docs.pingcap.com/tidb-in-kubernetes/stable/deploy-tiproxy.

### Deploy locally

1. Generate a self-signed certificate, which is used for the token-based authentication between TiDB and TiProxy.

For example, if you use openssl:

```shell
openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes \
  -keyout key.pem -out cert.pem -subj "/CN=example.com"
```

Put the certs and keys to all the TiDB servers. Make sure all the TiDB instances use the same certificate.

2. Update the `config.toml` of TiDB instances:

```toml
security.auto-tls=true
security.session-token-signing-cert={path/to/cert.pem}
security.session-token-signing-key={path/to/key.pem}
graceful-wait-before-shutdown=10
```

Where the `session-token-signing-cert` and `session-token-signing-key` are the paths to the certs generated in the 1st step.

And then start the TiDB cluster with the config.toml.

3. Update the [`proxy.toml`](/conf/proxy.toml) of TiProxy:

```toml
[proxy]
    pd-addrs = "127.0.0.1:2379"
```

Where the `pd-addrs` contains the addresses of all PD instances.

And then start TiProxy:

```shell
bin/tiproxy --config=conf/proxy.toml
```

4. Connect to TiProxy with your client. The default port is 6000:

```shell
mysql -h127.0.0.1 -uroot -P6000
```

## Interaction Latency Observability

TiProxy can expose per-interaction latency:

- Interaction latency: from forwarding one MySQL command to TiDB, until receiving the first response packet from TiDB.
- Command duration (`tiproxy_session_query_duration_seconds`) still exists and keeps the original meaning.
- Interaction metric (`tiproxy_session_query_interaction_duration_seconds`) includes labels: `backend`, `cmd_type`, `sql_type`.
  - `sql_type` is fine-grained only for `COM_QUERY` (for example `select`, `update`, `begin`, `commit`); other commands use `other`.

Configure it in `proxy.toml`:

```toml
[advance]
query-interaction-metrics = true
query-interaction-slow-log-threshold-ms = 200
query-interaction-user-patterns = "app_*,readonly"
backend-metrics-gc-interval-seconds = 300
backend-metrics-gc-idle-seconds = 3600
```

- `query-interaction-slow-log-threshold-ms`:
  - `0` disables slow interaction logs.
  - positive values log interactions slower than threshold.
- `query-interaction-user-patterns`:
  - comma-separated username glob patterns (`*`, `?`), case-sensitive.
  - empty value means collecting interaction metrics for all users.
- `backend-metrics-gc-idle-seconds`:
  - removes idle backend label series to control in-memory metric cache growth.
  - `0` disables metric GC.
- `backend-metrics-gc-interval-seconds`:
  - controls GC sweep frequency.
  - `0` disables metric GC.

These options support dynamic update through `PUT /api/admin/config`, so no restart is required.

Detailed docs:

- Design: [`docs/query-interaction-latency-design.md`](docs/query-interaction-latency-design.md)
- Usage: [`docs/query-interaction-latency-usage.md`](docs/query-interaction-latency-usage.md)

### Resource Sizing Notes

Enabling interaction latency metrics increases CPU and memory usage because each interaction adds extra histogram observation and optional slow-log checks.

- Recommended initial production reservation after enabling:
  - CPU: +15%
  - Memory: +10%
- Re-check and tune by workload. Use your own benchmark and metrics data before full rollout.

## Code of Conduct

This project is for everyone. We ask that our users and contributors take a few minutes to review our [Code of Conduct](code-of-conduct.md).

## License

TiProxy is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
