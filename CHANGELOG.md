# Change Log

## [v1.0.0] 2024.3.25

### Improvements

- Replace `time.Time` with mono-time to optimize duration calculation [#461](https://github.com/pingcap/tiproxy/pull/461)
- Change the log level of some logs from info to debug [#463](https://github.com/pingcap/tiproxy/pull/463)
- Optimize updating metrics to improve the performance by around 3% [#467](https://github.com/pingcap/tiproxy/pull/467)
- Reduce GC CPU usage by 1% in the case of plenty of connections [#474](https://github.com/pingcap/tiproxy/pull/474)
- Add metrics for traffic and handshake [#477](https://github.com/pingcap/tiproxy/pull/477)
- Add alert rules [#481](https://github.com/pingcap/tiproxy/pull/481)
- Support get JSON format config for `/api/admin/config` by setting `Accept='application/json'` in the request header [#484](https://github.com/pingcap/tiproxy/pull/484)

### Fixes

- Fix that welcome logs may not printed [#454](https://github.com/pingcap/tiproxy/pull/454)
- Fix that rebalance may not work after session migrations are interrupted [#459](https://github.com/pingcap/tiproxy/pull/459)
- Fix the bug that the config file may not be reloaded if the directory is removed and recreated immediately [#475](https://github.com/pingcap/tiproxy/pull/475)
- Fix panic when a TiDB fails during session migration [#486](https://github.com/pingcap/tiproxy/pull/486)
- Fix the bug that some goroutines are not recovered after they panic [#488](https://github.com/pingcap/tiproxy/pull/488)

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
