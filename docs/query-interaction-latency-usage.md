# TiProxy Query Interaction Latency 使用手册

## 1. 功能说明

本功能用于观测 TiProxy 到 TiDB 的“首包响应延迟”，并支持：

- Prometheus 聚合指标分析
- 慢交互日志定位
- backend metrics label 自动 GC
- 运行中动态调参，无需重启

## 2. 快速启用

在 `proxy.toml` 中添加：

```toml
[advance]
query-interaction-metrics = true
query-interaction-slow-log-threshold-ms = 200
query-interaction-slow-log-only-digest = false
query-interaction-user-patterns = "app_*,readonly"
backend-metrics-gc-interval-seconds = 300
backend-metrics-gc-idle-seconds = 3600
```

说明：

- `query-interaction-metrics=false`：关闭交互延迟指标（默认）。
- `query-interaction-slow-log-threshold-ms=0`：关闭慢交互日志。
- `query-interaction-slow-log-only-digest=true`：慢交互日志仅输出 `sql_digest`，不输出 `query`。
- `query-interaction-user-patterns=""`：采集所有用户名；设置后仅采集匹配 username 的交互指标。
- `backend-metrics-gc-interval-seconds=0` 或 `backend-metrics-gc-idle-seconds=0`：关闭 metrics GC。

## 3. 动态修改（不中断服务）

通过管理接口：

```bash
curl -X PUT http://127.0.0.1:3080/api/admin/config -d '
advance.query-interaction-metrics = true
advance.query-interaction-slow-log-threshold-ms = 1000
advance.query-interaction-slow-log-only-digest = true
advance.query-interaction-user-patterns = "app_*"
advance.backend-metrics-gc-interval-seconds = 120
advance.backend-metrics-gc-idle-seconds = 1800
'
```

常见动态操作：

- 临时排障：将阈值从 `200` 调到 `50` 或 `20`，快速捕获慢交互日志。
- 排障完成：将阈值恢复，或直接关掉慢日志（设为 `0`）。

## 4. 指标查看

新增指标：

- `tiproxy_session_query_interaction_duration_seconds`

关键标签：

- `backend`
- `cmd_type`
- `sql_type`（仅 `COM_QUERY` 会细分到 `select/update/begin/commit/...`，其他命令为 `other`）

PromQL 示例：

```promql
# 全局 P99 interaction latency
histogram_quantile(
  0.99,
  sum(rate(tiproxy_session_query_interaction_duration_seconds_bucket[1m])) by (le)
)
```

```promql
# 按 backend 看 P99 interaction latency
histogram_quantile(
  0.99,
  sum(rate(tiproxy_session_query_interaction_duration_seconds_bucket[1m])) by (le, backend)
)
```

```promql
# 按 sql_type 看 P99 interaction latency（例如 select/update/commit/begin）
histogram_quantile(
  0.99,
  sum(rate(tiproxy_session_query_interaction_duration_seconds_bucket[1m])) by (le, sql_type)
)
```

```promql
# 对比 command duration 与 interaction duration 的均值差
(
  sum(rate(tiproxy_session_query_duration_seconds_sum[1m])) /
  sum(rate(tiproxy_session_query_duration_seconds_count[1m]))
)
-
(
  sum(rate(tiproxy_session_query_interaction_duration_seconds_sum[1m])) /
  sum(rate(tiproxy_session_query_interaction_duration_seconds_count[1m]))
)
```

## 5. 慢交互日志

日志名：

- `slow mysql interaction`
- `slow mysql interaction matched username pattern`（当慢交互且命中 username pattern）

字段：

- `interaction_time`
- `interaction_duration`
- `connection_id`
- `cmd`
- `sql_type`
- `username`
- `username_pattern_matched`
- `username_matched_pattern`
- `backend_addr`
- `sql_digest`（仅 `COM_QUERY`）
- `query`（仅 `COM_QUERY`；当 `advance.query-interaction-slow-log-only-digest=true` 时不输出）
- `stmt_id`（适用 `COM_STMT_*`）

建议：

- 生产默认 `200ms` 或更高。
- 高峰期若日志量偏大，可临时提高阈值。

## 6. 内存与 CPU 容量建议

启用交互指标后，建议初始资源预留：

- CPU：+15%
- 内存：+10%

原因：

- 每次交互多一次 histogram observe。
- 若设置 `query-interaction-user-patterns`，每次交互会做一次用户名 glob 匹配（与 pattern 数量线性相关）。
- 慢日志阈值判断和可能的日志写入。
- backend metrics label cache 维护与周期性 GC。

上线前建议：

1. 在预发环境以真实流量压测 30~60 分钟。
2. 观察 `process_resident_memory_bytes`、`go_gc_duration_seconds`、CPU 使用率。
3. 调整阈值与 GC 参数到稳定区间后再全量上线。

## 7. 故障排查

### 7.1 看不到 interaction 指标

- 确认 `advance.query-interaction-metrics = true`。
- 若配置了 `advance.query-interaction-user-patterns`，确认当前连接 username 能匹配其中至少一个 pattern。
- 确认 `/api/admin/config` 返回已生效值。
- 确认有真实 SQL 流量经过 TiProxy。

### 7.2 backend labels 增长过快

- 缩短 `backend-metrics-gc-idle-seconds`。
- 缩短 `backend-metrics-gc-interval-seconds`。
- 检查 backend 地址是否高频变化（例如短周期扩缩容）。

### 7.3 慢日志太多

- 提高 `query-interaction-slow-log-threshold-ms`。
- 排障窗口之外可设置为 `0` 关闭慢日志。
