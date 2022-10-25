## What is TiProxy?

TiProxy is a database proxy that is based on TiDB. It keeps client connections alive while the TiDB server upgrades, restarts, scales in, and scales out.

TiProxy is forked from [Weir](https://github.com/tidb-incubator/weir).

## Features

### Connection Management

When a TiDB instance restarts or shuts down, TiProxy migrates backend connections on this instance to other instances. In this way, the clients won't be disconnected.

### Load Balance

TiProxy routes new connections to backends based on their scores to keep load balanced. The score is basically calculated from the connections on each backend.

Besides, when the clients create or close connections, TiProxy also migrates backend connections to keep the backends balanced.

### Service Discovery

When a new TiDB instance starts, the TiProxy detects the new TiDB instance and migrates backend connections to the instance.

The TiProxy also checks health on TiDB instances to ensure they are alive, and migrates the backend connections to other TiDB instances if any instance is down.

## Architecture

For more details, see [Design Doc](https://github.com/pingcap/tidb/blob/master/docs/design/2022-07-20-session-manager.md).

## Build

Build the binary in local:

```shell
$ make
```

Build a docker image:

```shell
$ make docker
```

## Usage

1. Generate a self-signed certificate, which is used for the token-based authentication between TiDB and TiProxy.

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

3. Update the `proxy.yaml` of TiProxy:

```yaml
proxy:
    pd-addrs: "127.0.0.1:2379"
```

Where the `pd-addrs` contains the addresses of all PD instances.

And then start TiProxy:

```shell
$ bin/tiproxy --config=conf/proxy.yaml
```

4. Connect to TiProxy with your client. The default port is 6000:

```shell
$ mysql -h127.0.0.1 -uroot -P6000
```

## Code of Conduct

This project is for everyone. We ask that our users and contributors take a few minutes to review our [Code of Conduct](code-of-conduct.md).

## License

TiProxy is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
