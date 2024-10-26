# Change Log

## [v1.3.0] 2024.10.26

### Features

- Support traffic replay as an experimental feature [#642](https://github.com/pingcap/tiproxy/issues/642)

### Improvements

- Make the API `/api/debug/health` return error when no backends are available [#692](https://github.com/pingcap/tiproxy/pull/692)
- Do not log config when the config is not changed [#694](https://github.com/pingcap/tiproxy/pull/694)

### Bug Fixes

- Fix that the configuration `proxy` is reset when the command line flag `--advertise-addr` is set [#691](https://github.com/pingcap/tiproxy/pull/691)
- Fix that an unhealthy backend may never be removed [#697](https://github.com/pingcap/tiproxy/pull/697)

### Compatibility Breakers

- Change the tiproxyctl flag `--curls` to `--host` and `--port` [#664](https://github.com/pingcap/tiproxy/pull/664)

## [v1.2.0] 2024.8.15

### Features

- Support virtual IP management [#583](https://github.com/pingcap/tiproxy/issues/583)

### Improvements

- Read metrics from backends for load balance when Prometheus is unavailable [#465](https://github.com/pingcap/tiproxy/issues/465)

### Bug Fixes

- Fix the bug that backend metrics are not cleared after backends are down [#585](https://github.com/pingcap/tiproxy/pull/585)
- Fix vulnerability issues scanned by third-party tools [#623](https://github.com/pingcap/tiproxy/pull/623)

## [v1.1.0] 2024.6.28

### Features

- Support multi-factor-based balance [#465](https://github.com/pingcap/tiproxy/issues/465)

### Improvements

- Add configuration `proxy.advertise-addr` [#495](https://github.com/pingcap/tiproxy/pull/495)
- Add command line argument `--advertise-addr` [#497](https://github.com/pingcap/tiproxy/pull/497)
- Speed up health check by checking in parallel [#498](https://github.com/pingcap/tiproxy/pull/498)
- Make sure the Welcome TiProxy info is always printed [#507](https://github.com/pingcap/tiproxy/pull/507)
- Do not reject new connections during `graceful-wait-before-shutdown` [#525](https://github.com/pingcap/tiproxy/pull/525)
- Add configuration `labels` [#536](https://github.com/pingcap/tiproxy/pull/536)
- Set the Y axis of some metrics to `logBase=2` [#561](https://github.com/pingcap/tiproxy/pull/561)

### Compatibility Breakers

- Deprecate command line arguments `log_level` and `log_encoder` [#504](https://github.com/pingcap/tiproxy/pull/504)

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
