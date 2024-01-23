# Change Log

## [v0.2.0] 2024.1.16

### Improvements

- Support online reload for `require-backend-tls` and remove unnecessary configs [#396](https://github.com/pingcap/tiproxy/pull/396)
- Add config `graceful-close-conn-timeout` [#400](https://github.com/pingcap/tiproxy/pull/400)
- Add config `server-http-tls` to specify HTTP port TLS config [#403](https://github.com/pingcap/tiproxy/pull/403)
- Do not report errors when the sequence mismatches [#410](https://github.com/pingcap/tiproxy/pull/410)
- Double-check the target backend health before session migration [#412](https://github.com/pingcap/tiproxy/pull/412)
- Add more metrics for connections, queries and health checks [#416](https://github.com/pingcap/tiproxy/pull/416)
- Move config `require-backend-tls` from `proxy` to `security` and change the default value to `false` [#419](https://github.com/pingcap/tiproxy/pull/419)
- Add HTTP API `/debug` and `/metrics` to be consistent with TiDB [#426](https://github.com/pingcap/tiproxy/pull/426)
- Remove pushing metrics to Prometheus to avoid authentication [#431](https://github.com/pingcap/tiproxy/pull/431)
- Change the default minimum TLS version from v1.1 to v1.2 [#437](https://github.com/pingcap/tiproxy/pull/437)

### Fixes

- Fix `COM_STMT_EXECUTE` may hang when the client disables `ClientDeprecateEOF` and uses cursors [#440](https://github.com/pingcap/tiproxy/pull/440)
- Fix the config file may not be reloaded when the config directory is removed and created again [#446](https://github.com/pingcap/tiproxy/pull/446)
- Fix TiDB may be unbalanced after session migrations are interrupted [#451](https://github.com/pingcap/tiproxy/pull/451)
