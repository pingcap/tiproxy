# TiProxy Query Interaction Latency Design

## 1. 背景

TiProxy 作为 TiDB gateway，原有指标可以看到命令级别的总耗时，但无法直接回答以下问题：

- 单次 MySQL 交互（request -> backend first response）的延迟是否异常。
- 出现慢请求时，是 TiDB 后端慢，还是 TiProxy 自身引入额外时延。
- backend 数量或地址变化后，metrics label 是否会长期累积占用内存。

为此，本设计新增交互延迟观测、慢交互日志、以及 backend metrics label GC。

## 2. 目标与非目标

### 2.1 目标

- 提供每次交互的聚合延迟指标，支持按 `backend`、`cmd_type`、`sql_type` 维度分析。
- 支持慢交互日志阈值动态修改，无需重启 TiProxy。
- 支持按 MySQL `username` 模式过滤交互指标，降低排障期指标压力。
- 控制 metrics label 内存增长，支持 TTL 回收。
- 保持线上稳定：默认关闭交互指标，开关与阈值可热更新。

### 2.2 非目标

- 不提供完整 SQL tracing/span。
- 不改变已有 `query_duration_seconds` 指标语义。
- 不对外暴露每一条请求的全量明细存储。

## 3. 术语定义

- Interaction：一轮 MySQL 命令交互，从 TiProxy 把 request 转发到 TiDB 后开始，直到收到 TiDB 的第一个 response packet 结束。
- Command Duration：现有命令完整处理耗时（已有指标，不变）。
- Interaction Duration：本次新增的首包响应延迟。

## 4. 配置设计

新增 `[advance]` 配置项：

- `query-interaction-metrics` (bool)
  - 是否开启交互延迟观测。
  - 默认：`false`。
- `query-interaction-slow-log-threshold-ms` (int)
  - 慢交互日志阈值，单位毫秒。
  - `0` 表示关闭慢日志。
  - 默认：`200`。
- `query-interaction-user-patterns` (string)
  - 交互指标按用户名过滤（glob 模式，逗号分隔，大小写敏感）。
  - 例如：`app_*`, `readonly`, `tenant_??`。
  - 空字符串表示不过滤（采集所有用户）。
  - 默认：`""`。
- `backend-metrics-gc-interval-seconds` (int)
  - backend metrics GC 扫描周期。
  - `0` 表示关闭 GC。
  - 默认：`300`。
- `backend-metrics-gc-idle-seconds` (int)
  - backend idle TTL，超过 TTL 未更新则回收其 metrics labels。
  - `0` 表示关闭 GC。
  - 默认：`3600`。

所有配置支持通过 `PUT /api/admin/config` 动态更新。

## 5. 指标与日志设计

### 5.1 新增指标

- `tiproxy_session_query_interaction_duration_seconds` (HistogramVec)
  - Labels: `backend`, `cmd_type`, `sql_type`
  - Bucket：与 `query_duration_seconds` 对齐。
  - 仅对匹配 `query-interaction-user-patterns` 的连接采集。
  - `sql_type` 取值固定：`select|insert|update|delete|replace|begin|commit|rollback|set|use|other`。

### 5.2 慢交互日志

当 `interaction_duration >= threshold` 时记录 `Warn` 日志：

- 固定字段：`interaction_time`, `interaction_duration`, `connection_id`, `cmd`, `sql_type`, `username`, `backend_addr`
- 过滤字段：`username_pattern_matched`, `username_matched_pattern`
- 条件字段：
  - `query`：仅 `COM_QUERY`，经过 normalize 并截断
  - `stmt_id`：`COM_STMT_*` 且包体含 statement id 时

当慢交互同时命中 username pattern 时，额外输出：

- `slow mysql interaction matched username pattern`

## 6. 数据路径与埋点位置

埋点位于 `CmdProcessor` 转发路径，按“每一轮交互”采集：

- 单响应命令：收到首包时采集一次。
- `COM_QUERY`/`COM_STMT_EXECUTE` 多结果：每轮结果单独采集。
- `COM_QUERY` 额外做轻量首关键字分类，写入固定集合 `sql_type`，未识别归类为 `other`。
- `LOAD DATA LOCAL INFILE`：包含本地文件阶段后的最终返回轮次。
- `COM_CHANGE_USER`：包含 auth switch 多轮交互。
- 无响应命令（如 `COM_QUIT`）不采集。

## 7. Backend Metrics GC 设计

在 backend metrics cache 维护 `lastSeen`：

1. 每次 metrics 更新刷新 `lastSeen`。
2. 到达 GC interval 时触发 sweep。
3. 若 `now - lastSeen > idleTTL`：
   - 删除缓存节点。
   - 调用 Prometheus `DeleteLabelValues` 回收对应 labels。

回收范围：

- `query_total`
- `query_duration_seconds`
- `query_interaction_duration_seconds`
- `handshake_duration_seconds`
- traffic counters

## 8. 性能与稳定性考虑

- 默认关闭交互指标，避免默认增量开销。
- 开启后增量主要是：
  - monotonic time 计算
  - histogram observe
  - 慢日志阈值判断
- GC 采用“低频 + TTL”策略，避免每次请求都做全量扫描。
- username 过滤采用预解析 glob 列表匹配，开销与 pattern 数量线性相关。

建议上线预留资源：

- CPU: +15%
- Memory: +10%

实际值需结合业务流量压测复核。

## 9. 测试策略

- 配置测试：新字段默认值、序列化、负值校验。
- 热更新测试：`WatchConfig` 下参数动态生效。
- 指标测试：
  - interaction histogram sample count 增长
  - TTL GC 可删除 stale backend labels
- 协议路径回归：`pkg/proxy/backend` 全量测试通过。

## 10. 兼容性

- 原有指标与语义保持不变。
- 新配置均有默认值，升级兼容旧配置文件。
- 若需完全关闭回收，将 `backend-metrics-gc-interval-seconds=0` 或 `backend-metrics-gc-idle-seconds=0`。
