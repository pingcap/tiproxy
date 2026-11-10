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

//! Route table and middleware mirroring Go `pkg/server/api`.
//!
//! Middleware runs in the Go order: the global blocking rate limit
//! (`DefAPILimit` = 100/s, never `429`), the readiness gate (`500
//! "service not ready"` until the process marks itself ready), then the
//! handler; the access log observes the final status. Unknown paths and
//! methods answer gin's default `404 page not found`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Request, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, StatusCode};
use http_body_util::BodyExt;

use crate::config::{CommitError, NAMESPACE_SCHEMA, SharedConfigAdmin, go_json_body};
use crate::drain::{
    DRAIN_SCHEMA, DrainRequest, DrainStartError, MAX_DRAIN_BUDGET_MS, SharedDrainAdmin,
};
use crate::health::{HealthInputs, HealthState, go_json_document, go_json_string};
use control_config::NamespaceConfig;

/// Go `DefAPILimit`: requests per second admitted by the global limiter.
pub const DEFAULT_RATE_LIMIT_PER_SECOND: u32 = 100;
/// uber-go/ratelimit default slack: how many unused slots may accumulate.
const RATE_LIMIT_SLACK: u32 = 10;
/// Largest request body any admin handler reads (Go reads unbounded).
const MAX_BODY_BYTES: usize = 1024 * 1024;
/// gin's default JSON content type.
const JSON_CONTENT_TYPE: &str = "application/json; charset=utf-8";
/// gin's `c.TOML` content type.
const TOML_CONTENT_TYPE: &str = "application/toml; charset=utf-8";
/// gin's `c.String` content type.
const TEXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";
/// gin's built-in 404 body is written through `http.Error`-style plain text
/// without a charset parameter.
const NOT_FOUND_CONTENT_TYPE: &str = "text/plain";
/// Prometheus text exposition content type (`promhttp` with the pinned Go
/// client, which appends the escaping scheme).
const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8; escaping=underscores";

/// Go `controlbridge.SnapshotStatus` projection, key order preserved.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DataplaneStatus {
    /// Whether a dataplane status source exists at all.
    pub enabled: bool,
    /// Latest generation the owner wants applied.
    pub desired_generation: u64,
    /// Latest generation handed to the serving side.
    pub sent_generation: u64,
    /// Latest generation applied to SQL serving.
    pub applied_generation: u64,
    /// Latest rejected generation.
    pub rejected_generation: u64,
    /// Proto enum name of the last apply outcome.
    pub last_result_code: String,
    /// Bounded detail of the last outcome (never a payload).
    pub detail: String,
    /// Age of the last successful apply in milliseconds.
    pub last_good_age_ms: i64,
}

impl DataplaneStatus {
    /// Renders the exact Go JSON: `{"enabled":false}` when disabled, otherwise
    /// gin's `H` map encoding, whose keys are emitted in sorted order.
    #[must_use]
    pub fn to_json(&self) -> String {
        if !self.enabled {
            return "{\"enabled\":false}".to_owned();
        }
        format!(
            "{{\"applied_generation\":{},\"desired_generation\":{},\"detail\":{},\"enabled\":true,\"last_good_age_ms\":{},\"last_result_code\":{},\"rejected_generation\":{},\"sent_generation\":{}}}",
            self.applied_generation,
            self.desired_generation,
            go_json_string(&self.detail),
            self.last_good_age_ms,
            go_json_string(&self.last_result_code),
            self.rejected_generation,
            self.sent_generation,
        )
    }
}

/// Process-state accessors supplied by the executable wiring.
#[derive(Clone)]
pub struct AdminHooks {
    /// Live health inputs for one probe.
    pub health_inputs: Arc<dyn Fn() -> HealthInputs + Send + Sync>,
    /// Prometheus text exposition of the process registry.
    pub metrics_text: Arc<dyn Fn() -> String + Send + Sync>,
    /// Current dataplane generation status.
    pub dataplane_status: Arc<dyn Fn() -> DataplaneStatus + Send + Sync>,
    /// Namespace and configuration storage.
    pub config: SharedConfigAdmin,
    /// Local drain seam; `None` answers Go's `{"enabled":false}`.
    pub drain: Option<SharedDrainAdmin>,
    /// The process log file the diagnostics `SearchLog` scans (Go's
    /// `log.log-file.filename`); `None` is Go's empty configuration.
    pub log_file: Option<std::path::PathBuf>,
    /// Go `BackendReader.GetBackendMetricsByCluster`: the owner-filtered
    /// metric history JSON for a cluster name (empty for a missing cluster,
    /// for the empty name when there is not exactly one cluster, or when
    /// this process is not serving the owner endpoint).
    pub backend_metrics: BackendMetricsHook,
    /// Go `NamespaceManager.RedirectConnections`: offers every connection a
    /// redirect to its own backend; `Err` is a router-level failure (Go's
    /// `[]error`), refused offers are not errors.
    pub redirect: RedirectHook,
}

/// The `AdminHooks::redirect` entry: Go's management redirect sweep.
pub type RedirectHook = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

/// The `AdminHooks::backend_metrics` reader: history bytes for a cluster name.
pub type BackendMetricsHook = Arc<dyn Fn(&str) -> Vec<u8> + Send + Sync>;

/// uber-go/ratelimit "leaky bucket with slack", the algorithm behind the Go
/// API's `ratelimit.New(DefAPILimit)`. `take` never rejects; it delays.
///
/// The port keeps uber's signed `sleepFor` accumulator: every reservation
/// adds one slot and subtracts the time elapsed since the previous
/// reservation, the accumulator is floored at minus the slack so idle time
/// cannot bank more than ten slots, and a positive accumulator becomes the
/// caller's delay while `last` moves to that future instant so later
/// callers queue behind it.
#[derive(Debug)]
pub struct RateLimiter {
    per_request_nanos: i128,
    max_slack_nanos: i128,
    state: Mutex<LimiterState>,
}

#[derive(Debug, Default)]
struct LimiterState {
    last: Option<Instant>,
    /// Signed debt (positive) or banked slack (negative) in nanoseconds.
    sleep_for_nanos: i128,
}

impl RateLimiter {
    /// Builds a limiter admitting `per_second` requests per second.
    #[must_use]
    pub fn new(per_second: u32) -> Self {
        let per_request = Duration::from_secs(1) / per_second.max(1);
        let per_request_nanos = nanos(per_request);
        Self {
            per_request_nanos,
            max_slack_nanos: per_request_nanos * i128::from(RATE_LIMIT_SLACK),
            state: Mutex::new(LimiterState::default()),
        }
    }

    /// Returns the delay the caller must observe before proceeding, updating
    /// the schedule as if the caller will do so.
    fn reserve(&self, now: Instant) -> Duration {
        let Ok(mut state) = self.state.lock() else {
            return Duration::ZERO;
        };
        let Some(last) = state.last else {
            state.last = Some(now);
            return Duration::ZERO;
        };
        // `last` may be in the future when the previous caller is still
        // sleeping; the signed difference then adds that caller's wait.
        let since_last = if now >= last {
            nanos(now - last)
        } else {
            -nanos(last - now)
        };
        state.sleep_for_nanos += self.per_request_nanos - since_last;
        if state.sleep_for_nanos < -self.max_slack_nanos {
            state.sleep_for_nanos = -self.max_slack_nanos;
        }
        if state.sleep_for_nanos > 0 {
            let sleep =
                Duration::from_nanos(u64::try_from(state.sleep_for_nanos).unwrap_or(u64::MAX));
            state.last = Some(now + sleep);
            state.sleep_for_nanos = 0;
            return sleep;
        }
        state.last = Some(now);
        Duration::ZERO
    }

    /// Waits for the caller's slot.
    pub async fn take(&self) {
        let delay = self.reserve(Instant::now());
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

/// Signed nanoseconds of a duration (saturating; durations here are tiny).
fn nanos(duration: Duration) -> i128 {
    i128::try_from(duration.as_nanos()).unwrap_or(i128::MAX)
}

/// Shared handler state.
pub struct AdminApp {
    hooks: AdminHooks,
    health: HealthState,
    ready: AtomicBool,
    limiter: RateLimiter,
    grpc: crate::grpc::SharedGrpc,
}

impl AdminApp {
    /// Creates the app; the readiness gate starts closed like Go's `ready`.
    #[must_use]
    pub fn new(hooks: AdminHooks, health: HealthState) -> Self {
        let grpc = Arc::new(crate::grpc::DiagnosticsService::new(hooks.log_file.clone()).server());
        Self {
            hooks,
            health,
            ready: AtomicBool::new(false),
            limiter: RateLimiter::new(DEFAULT_RATE_LIMIT_PER_SECOND),
            grpc,
        }
    }

    /// Opens the readiness gate (Go toggles it at the end of `NewServer`).
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    /// Returns the manual health override slot.
    #[must_use]
    pub const fn health(&self) -> &HealthState {
        &self.health
    }
}

/// Builds the complete route table served on the API listener.
pub fn full_router(app: Arc<AdminApp>) -> Router {
    // gin registers only the listed methods; axum would otherwise answer
    // HEAD through the GET handler, so HEAD is pinned to the 404 body.
    let health = get(health_get)
        .put(health_put)
        .delete(health_delete)
        .head(not_found);
    let router = Router::new()
        .route("/api/debug/health", health.clone())
        .route("/debug/health", health)
        .route("/metrics", get(metrics).head(not_found))
        .route("/metrics/", get(metrics).head(not_found))
        .route("/api/metrics", get(metrics).head(not_found))
        .route("/api/metrics/", get(metrics).head(not_found))
        .route(
            "/api/dataplane/status",
            get(dataplane_status).head(not_found),
        )
        .route("/api/dataplane/drain", post(drain_start).head(not_found))
        .route(
            "/api/dataplane/drain/{id}",
            get(drain_status).head(not_found),
        )
        // gin registers the group roots with a trailing slash and redirects
        // the bare path; both forms are served directly here (declared).
        .route(
            "/api/admin/namespace/",
            get(namespace_list).put(namespace_put_root).head(not_found),
        )
        .route(
            "/api/admin/namespace",
            get(namespace_list).put(namespace_put_root).head(not_found),
        )
        .route(
            "/api/admin/namespace/commit",
            post(namespace_commit).head(not_found),
        )
        .route(
            "/api/admin/namespace/{namespace}",
            get(namespace_get)
                .put(namespace_put)
                .delete(namespace_delete)
                .head(not_found),
        )
        .route(
            "/api/admin/config/",
            get(config_get).put(config_put).head(not_found),
        )
        .route(
            "/api/admin/config",
            get(config_get).put(config_put).head(not_found),
        )
        .route("/api/traffic/capture", post(traffic_disabled("capture")))
        .route("/api/traffic/replay", post(traffic_disabled("replay")))
        .route("/api/traffic/cancel", post(traffic_disabled("cancel")))
        .route("/api/traffic/show", get(traffic_disabled("show")))
        .route("/api/backend/metrics", get(backend_metrics).head(not_found))
        .route("/api/debug/redirect", post(debug_redirect).head(not_found));
    with_middleware(router, app)
}

/// Builds the plaintext branch used when HTTP TLS is configured: only the
/// readiness probe, only `GET` (Go `server.go` cmux `HTTP1Fast` branch).
pub fn plaintext_router(app: Arc<AdminApp>) -> Router {
    let router = Router::new()
        .route("/api/debug/health", get(health_get).head(not_found))
        .route("/debug/health", get(health_get).head(not_found));
    with_middleware(router, app)
}

fn with_middleware(router: Router<Arc<AdminApp>>, app: Arc<AdminApp>) -> Router {
    router
        .fallback(not_found)
        .method_not_allowed_fallback(not_found)
        // Layers run outermost-last: access log sees the final response,
        // then the rate limit admits the request, then the readiness gate,
        // then (gin's `grpcServer`, after the gate) HTTP/2 `application/grpc`
        // requests leave for the diagnostics service before any route.
        .layer(middleware::from_fn_with_state(Arc::clone(&app), grpc_split))
        .layer(middleware::from_fn_with_state(Arc::clone(&app), ready_gate))
        .layer(middleware::from_fn_with_state(Arc::clone(&app), rate_limit))
        .layer(middleware::from_fn(access_log))
        .with_state(app)
}

/// gin `grpcServer`: an HTTP/2 request whose `Content-Type` starts with
/// `application/grpc` is served by the gRPC server and never reaches the
/// HTTP routes (or the access log, which gin attaches after this point).
async fn grpc_split(State(app): State<Arc<AdminApp>>, request: Request, next: Next) -> Response {
    if crate::grpc::is_grpc(&request) {
        use tower::ServiceExt;
        let service = (*app.grpc).clone();
        return match service.oneshot(request).await {
            Ok(response) => response.map(Body::new),
            Err(never) => match never {},
        };
    }
    next.run(request).await
}

async fn rate_limit(State(app): State<Arc<AdminApp>>, request: Request, next: Next) -> Response {
    app.limiter.take().await;
    next.run(request).await
}

async fn ready_gate(State(app): State<Arc<AdminApp>>, request: Request, next: Next) -> Response {
    if !app.ready.load(Ordering::Acquire) {
        return json(StatusCode::INTERNAL_SERVER_ERROR, "\"service not ready\"");
    }
    next.run(request).await
}

/// Payload-free access record for non-success responses only, at `WARN`
/// like gin's error branch; Go logs successes at debug level, which the Rust
/// process does not emit.
async fn access_log(request: Request, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let started = Instant::now();
    let response = next.run(request).await;
    let status = response.status().as_u16();
    if status >= 400 {
        let line = serde_json::json!({
            "component": "control-admin",
            "event": "http_request",
            "method": method.as_str(),
            "path": path,
            "status": status,
            "latency_ms": started.elapsed().as_millis(),
        });
        control_plane::logging::emit(control_plane::logging::Level::Warn, &line.to_string());
    }
    response
}

async fn not_found() -> Response {
    with_content_type(
        StatusCode::NOT_FOUND,
        NOT_FOUND_CONTENT_TYPE,
        "404 page not found".to_owned(),
    )
}

async fn health_get(State(app): State<Arc<AdminApp>>) -> Response {
    let inputs = (app.hooks.health_inputs)();
    let evaluated = app.health.evaluate(&inputs);
    json(
        StatusCode::from_u16(evaluated.status).unwrap_or(StatusCode::BAD_GATEWAY),
        &evaluated.body,
    )
}

async fn health_put(State(app): State<Arc<AdminApp>>, request: Request) -> Response {
    let Some(body) = read_body(request).await else {
        return json(StatusCode::BAD_REQUEST, "\"bad health override json\"");
    };
    let Ok(body) = OverrideBody::decode(&body) else {
        return json(StatusCode::BAD_REQUEST, "\"bad health override json\"");
    };
    app.health.set_override(body.healthy, &body.reason);
    json(StatusCode::OK, "\"\"")
}

/// The Go `manualHealthOverrideRequest` decoded with `encoding/json`
/// semantics as gin's `ShouldBindJSON` applies them: exactly one JSON value
/// is read and trailing bytes are ignored; `null` yields the zero value;
/// keys match a field exactly or, failing that, case-insensitively; unknown
/// keys are skipped; every occurrence of a key is decoded in order, so a
/// wrong type anywhere is an error and the last well-typed value wins; a
/// JSON `null` leaves the field unchanged; a non-object value is an error.
#[derive(Debug, Default, PartialEq, Eq)]
struct OverrideBody {
    healthy: bool,
    reason: String,
}

impl OverrideBody {
    fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        // No `end()` call: `json.Decoder.Decode` reads one value only.
        serde::Deserializer::deserialize_any(&mut deserializer, OverrideVisitor)
    }
}

struct OverrideVisitor;

impl<'de> serde::de::Visitor<'de> for OverrideVisitor {
    type Value = OverrideBody;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON object or null")
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(OverrideBody::default())
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut body = OverrideBody::default();
        while let Some(key) = map.next_key::<String>()? {
            let field = if key == "healthy" || key == "reason" {
                key.as_str()
            } else if key.eq_ignore_ascii_case("healthy") {
                "healthy"
            } else if key.eq_ignore_ascii_case("reason") {
                "reason"
            } else {
                map.next_value::<serde::de::IgnoredAny>()?;
                continue;
            };
            let value = map.next_value::<serde_json::Value>()?;
            match (field, value) {
                (_, serde_json::Value::Null) => {}
                ("healthy", serde_json::Value::Bool(value)) => body.healthy = value,
                ("reason", serde_json::Value::String(value)) => body.reason = value,
                (field, other) => {
                    return Err(serde::de::Error::custom(format!(
                        "cannot unmarshal {} into field {field}",
                        json_kind(&other)
                    )));
                }
            }
        }
        Ok(body)
    }
}

fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

async fn health_delete(State(app): State<Arc<AdminApp>>) -> Response {
    app.health.clear_override();
    json(StatusCode::OK, "\"\"")
}

async fn metrics(State(app): State<Arc<AdminApp>>) -> Response {
    let body = (app.hooks.metrics_text)();
    with_content_type(StatusCode::OK, METRICS_CONTENT_TYPE, body)
}

async fn dataplane_status(State(app): State<Arc<AdminApp>>) -> Response {
    json(StatusCode::OK, &(app.hooks.dataplane_status)().to_json())
}

// ---- debug redirect (Go pkg/server/api/debug.go) ----

/// Go `DebugRedirect`: `NsMgr.RedirectConnections()`; any router-level
/// error answers `500 "redirect connections error"`, otherwise `200 ""`.
async fn debug_redirect(State(app): State<Arc<AdminApp>>) -> Response {
    match (app.hooks.redirect)() {
        Ok(()) => json(StatusCode::OK, "\"\""),
        Err(error) => {
            let line = serde_json::json!({
                "component": "control-admin",
                "event": "redirect_connections_error",
                "error": error,
            });
            control_plane::logging::emit(control_plane::logging::Level::Warn, &line.to_string());
            json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "\"redirect connections error\"",
            )
        }
    }
}

// ---- backend metrics (Go pkg/server/api/backend.go) ----

/// Go `BackendMetrics`: `c.Query("cluster")` (the first value, Go
/// `url.ParseQuery` decoding) selects the cluster; the answer is always
/// `200` with `Content-Type: application/json` and the reader's bytes, which
/// are empty when Go's reader returns nil.
async fn backend_metrics(State(app): State<Arc<AdminApp>>, request: Request) -> Response {
    let cluster = query_values(request.uri().query().unwrap_or_default(), "cluster")
        .into_iter()
        .next()
        .unwrap_or_default();
    let body = (app.hooks.backend_metrics)(&cluster);
    let mut response = (StatusCode::OK, body).into_response();
    // Go writes the bare media type here (no gin `c.JSON` charset).
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

// ---- operator drain (Go pkg/server/api/dataplane.go) ----

/// gin's validator message for the required `drain_id`.
const DRAIN_ID_REQUIRED: &str = "Key: 'drainRequestBody.DrainID' Error:Field validation for 'DrainID' failed on the 'required' tag";

async fn drain_start(State(app): State<Arc<AdminApp>>, request: Request) -> Response {
    let Some(drain) = app.hooks.drain.as_ref() else {
        return json(StatusCode::NOT_FOUND, "{\"enabled\":false}");
    };
    let Some(body) = read_body(request).await else {
        return json(
            StatusCode::BAD_REQUEST,
            &error_body("request body too large"),
        );
    };
    // gin ShouldBindJSON: decode errors carry the decoder's message; the
    // `required` tag on drain_id is reported through the validator.
    let decoded = match go_json_body(&body, DRAIN_SCHEMA) {
        Ok(Some(value)) => value,
        Ok(None) => serde_json::json!({}),
        Err(error) => return json(StatusCode::BAD_REQUEST, &error_body(&error.to_string())),
    };
    let field_str = |name: &str| {
        decoded
            .get(name)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let field_list = |name: &str| -> Vec<String> {
        decoded
            .get(name)
            .and_then(serde_json::Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    };
    let field_int = |name: &str| {
        decoded
            .get(name)
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0)
    };
    let drain_id = field_str("drain_id");
    if drain_id.is_empty() {
        return json(StatusCode::BAD_REQUEST, &error_body(DRAIN_ID_REQUIRED));
    }
    // Validate the raw millisecond inputs before any conversion, exactly
    // as Go does: negative is a client error, each value and the sum must
    // fit the shared 30-day cap so the conversion can never overflow.
    let (graceful, force) = (field_int("graceful_wait_ms"), field_int("force_timeout_ms"));
    if graceful < 0
        || force < 0
        || graceful > MAX_DRAIN_BUDGET_MS
        || force > MAX_DRAIN_BUDGET_MS
        || graceful > MAX_DRAIN_BUDGET_MS - force
    {
        return json(
            StatusCode::BAD_REQUEST,
            &error_body(&format!(
                "graceful_wait_ms and force_timeout_ms must be within [0, {MAX_DRAIN_BUDGET_MS}] (30 days)"
            )),
        );
    }
    let request = DrainRequest {
        drain_id: drain_id.clone(),
        listener_names: field_list("listener_names"),
        backend_ids: field_list("backend_ids"),
        graceful_wait: Duration::from_millis(graceful.unsigned_abs()),
        force_timeout: Duration::from_millis(force.unsigned_abs()),
    };
    match drain.start(request).await {
        Ok(()) => json(
            StatusCode::ACCEPTED,
            &format!("{{\"drain_id\":{}}}", go_json_string(&drain_id)),
        ),
        Err(error @ DrainStartError::InvalidBudget) => {
            json(StatusCode::BAD_REQUEST, &error_body(&error.message()))
        }
        Err(error @ (DrainStartError::NoSession | DrainStartError::SnapshotNotReady)) => json(
            StatusCode::SERVICE_UNAVAILABLE,
            &error_body(&error.message()),
        ),
        Err(error @ (DrainStartError::InProgress | DrainStartError::ForeignActive)) => {
            json(StatusCode::CONFLICT, &error_body(&error.message()))
        }
        Err(error @ DrainStartError::Other(_)) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &error_body(&error.message()),
        ),
    }
}

async fn drain_status(State(app): State<Arc<AdminApp>>, Path(id): Path<String>) -> Response {
    let Some(drain) = app.hooks.drain.as_ref() else {
        return json(StatusCode::NOT_FOUND, "{\"enabled\":false}");
    };
    let Some(progress) = drain.status(id.clone()).await else {
        return json(StatusCode::NOT_FOUND, "{\"known\":false}");
    };
    // gin.H is a map: keys are emitted in sorted order.
    json(
        StatusCode::OK,
        &format!(
            "{{\"active_connections\":{},\"code\":{},\"complete\":{},\"detail\":{},\"drain_id\":{},\"force_closed\":{},\"gracefully_closed\":{}}}",
            progress.active_connections,
            go_json_string(&progress.code),
            progress.complete,
            go_json_string(&progress.detail),
            go_json_string(&id),
            progress.force_closed,
            progress.gracefully_closed,
        ),
    )
}

fn error_body(message: &str) -> String {
    format!("{{\"error\":{}}}", go_json_string(message))
}

// ---- namespaces (Go pkg/server/api/namespace.go) ----

async fn namespace_list(State(app): State<Arc<AdminApp>>) -> Response {
    let namespaces = app.hooks.config.list_namespaces();
    if namespaces.is_empty() {
        // Go answers the JSON empty string for an empty list.
        return json(StatusCode::OK, "\"\"");
    }
    match serde_json::to_string(&namespaces) {
        Ok(body) => json(StatusCode::OK, &go_json_document(&body)),
        Err(_) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "\"failed to list namespaces\"",
        ),
    }
}

async fn namespace_get(State(app): State<Arc<AdminApp>>, Path(name): Path<String>) -> Response {
    if name.is_empty() {
        return json(StatusCode::BAD_REQUEST, "\"bad namespace parameter\"");
    }
    match app
        .hooks
        .config
        .get_namespace(&name)
        .and_then(|value| serde_json::to_string(&value).ok())
    {
        Some(body) => json(StatusCode::OK, &go_json_document(&body)),
        // Go reports "not found" through the same 500 as a store failure.
        None => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "\"can not get namespace\"",
        ),
    }
}

async fn namespace_put_root(State(app): State<Arc<AdminApp>>, request: Request) -> Response {
    namespace_upsert(app, String::new(), request).await
}

async fn namespace_put(
    State(app): State<Arc<AdminApp>>,
    Path(name): Path<String>,
    request: Request,
) -> Response {
    namespace_upsert(app, name, request).await
}

/// Go pre-fills the namespace name from the path, binds the body over it and
/// stores the value under the resulting `namespace` field: a body name wins
/// over the path, and an empty name is a store error (`500`).
async fn namespace_upsert(app: Arc<AdminApp>, path_name: String, request: Request) -> Response {
    let Some(body) = read_body(request).await else {
        return json(StatusCode::BAD_REQUEST, "\"bad namespace json\"");
    };
    let mut value = match go_json_body(&body, NAMESPACE_SCHEMA) {
        Ok(None) => NamespaceConfig::default(),
        Ok(Some(value)) => match serde_json::from_value::<NamespaceConfig>(value) {
            Ok(value) => value,
            Err(_) => return json(StatusCode::BAD_REQUEST, "\"bad namespace json\""),
        },
        Err(_) => return json(StatusCode::BAD_REQUEST, "\"bad namespace json\""),
    };
    if value.namespace.is_empty() {
        value.namespace = path_name;
    }
    if value.namespace.is_empty() {
        return json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "\"can not update config\"",
        );
    }
    match app.hooks.config.set_namespace(value).await {
        Ok(()) => json(StatusCode::OK, "\"\""),
        Err(_) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "\"can not update config\"",
        ),
    }
}

async fn namespace_delete(State(app): State<Arc<AdminApp>>, Path(name): Path<String>) -> Response {
    if name.is_empty() {
        return json(StatusCode::BAD_REQUEST, "\"bad namespace parameter\"");
    }
    match app.hooks.config.delete_namespace(name).await {
        Ok(()) => json(StatusCode::OK, "\"\""),
        Err(_) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "\"can not update config\"",
        ),
    }
}

async fn namespace_commit(State(app): State<Arc<AdminApp>>, request: Request) -> Response {
    // gin `QueryArray("namespace")`: every repeated `namespace=` value.
    let names = query_values(request.uri().query().unwrap_or_default(), "namespace");
    match app.hooks.config.commit_namespaces(names).await {
        Ok(()) => json(StatusCode::OK, "\"\""),
        Err(CommitError::Missing) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "\"failed to get namespace\"",
        ),
        Err(CommitError::Reload) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "\"failed to reload namespaces\"",
        ),
    }
}

/// Repeated query values for `key`, decoded like Go's `url.ParseQuery`
/// (`+` is a space, `%XX` is a byte); a pair with a malformed escape in its
/// key or value is dropped, as `ParseQuery` skips it.
fn query_values(query: &str, key: &str) -> Vec<String> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            let name = url_decode(name)?;
            let value = url_decode(value)?;
            (name == key).then_some(value)
        })
        .collect()
}

/// `None` for a malformed percent escape.
fn url_decode(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => output.push(b' '),
            b'%' => {
                let hex = text.get(index + 1..index + 3)?;
                output.push(u8::from_str_radix(hex, 16).ok()?);
                index += 3;
                continue;
            }
            byte => output.push(byte),
        }
        index += 1;
    }
    Some(String::from_utf8_lossy(&output).into_owned())
}

// ---- configuration (Go pkg/server/api/config.go) ----

async fn config_get(State(app): State<Arc<AdminApp>>, request: Request) -> Response {
    // TiDB Dashboard asks for JSON with `?format=json` (case-insensitive,
    // first value only like gin's `c.Query`) or an exact
    // `Accept: application/json`; tiproxyctl expects TOML.
    let wants_json = query_values(request.uri().query().unwrap_or_default(), "format")
        .first()
        .is_some_and(|value| value.eq_ignore_ascii_case("json"))
        || request
            .headers()
            .get(http::header::ACCEPT)
            .and_then(|value| value.to_str().ok())
            == Some("application/json");
    if wants_json {
        match app.hooks.config.config_json() {
            Some(body) => json(StatusCode::OK, &go_json_document(&body)),
            None => json(StatusCode::INTERNAL_SERVER_ERROR, "\"can not get config\""),
        }
    } else {
        match app.hooks.config.config_toml() {
            Some(body) => with_content_type(StatusCode::OK, TOML_CONTENT_TYPE, body),
            None => json(StatusCode::INTERNAL_SERVER_ERROR, "\"can not get config\""),
        }
    }
}

async fn config_put(State(app): State<Arc<AdminApp>>, request: Request) -> Response {
    let Some(body) = read_body(request).await else {
        return json(StatusCode::INTERNAL_SERVER_ERROR, "\"fail to read config\"");
    };
    match app.hooks.config.put_config_toml(body).await {
        Ok(()) => json(StatusCode::OK, "\"\""),
        Err(_) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "\"can not update config\"",
        ),
    }
}

fn traffic_disabled(
    verb: &'static str,
) -> impl Fn(State<Arc<AdminApp>>) -> std::future::Ready<Response> + Clone + Send + 'static {
    move |State(app): State<Arc<AdminApp>>| {
        let status = if app.hooks.config.traffic_replay_enabled() {
            "unsupported"
        } else {
            "disabled"
        };
        std::future::ready(text(
            StatusCode::BAD_REQUEST,
            &format!("traffic {verb} is {status}"),
        ))
    }
}

async fn read_body(request: Request) -> Option<Vec<u8>> {
    let limited = http_body_util::Limited::new(request.into_body(), MAX_BODY_BYTES);
    limited
        .collect()
        .await
        .ok()
        .map(|body| body.to_bytes().to_vec())
}

fn json(status: StatusCode, body: &str) -> Response {
    with_content_type(status, JSON_CONTENT_TYPE, body.to_owned())
}

fn text(status: StatusCode, body: &str) -> Response {
    with_content_type(status, TEXT_CONTENT_TYPE, body.to_owned())
}

fn with_content_type(status: StatusCode, content_type: &'static str, body: String) -> Response {
    let mut response = (status, body).into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

impl AdminHooks {
    /// Hooks returning fixed values over `config`, for tests and the replay.
    #[must_use]
    pub fn fixed(
        inputs: HealthInputs,
        metrics: String,
        status: DataplaneStatus,
        config: SharedConfigAdmin,
    ) -> Self {
        Self {
            health_inputs: Arc::new(move || inputs.clone()),
            metrics_text: Arc::new(move || metrics.clone()),
            dataplane_status: Arc::new(move || status.clone()),
            config,
            drain: None,
            log_file: None,
            backend_metrics: Arc::new(|_| Vec::new()),
            redirect: Arc::new(|| Ok(())),
        }
    }
}

/// Convenience for in-process tests: runs one request through a router.
#[doc(hidden)]
pub async fn oneshot(router: Router, request: Request<Body>) -> (StatusCode, String, String) {
    use tower::ServiceExt;
    let response = match router.oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    };
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let body = response
        .into_body()
        .collect()
        .await
        .map(|body| String::from_utf8_lossy(&body.to_bytes()).into_owned())
        .unwrap_or_default();
    (status, content_type, body)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::config::ConfigAdmin;

    fn memory() -> Arc<crate::config::MemoryConfigAdmin> {
        Arc::new(crate::config::MemoryConfigAdmin::new(
            control_config::EffectiveConfig::default()
                .validated(std::path::Path::new("/tmp"))
                .unwrap(),
            std::path::PathBuf::from("/tmp"),
        ))
    }

    fn app(ready: bool) -> Arc<AdminApp> {
        app_with(ready, memory())
    }

    fn app_with(ready: bool, config: Arc<crate::config::MemoryConfigAdmin>) -> Arc<AdminApp> {
        let hooks = AdminHooks::fixed(
            HealthInputs {
                closing: false,
                namespaces_ready: true,
                applied_generation: 3,
                config_checksum: 42,
            },
            "# HELP x y\n".to_owned(),
            DataplaneStatus {
                enabled: true,
                desired_generation: 3,
                sent_generation: 3,
                applied_generation: 3,
                rejected_generation: 0,
                last_result_code: "ERROR_CODE_OK".to_owned(),
                detail: String::new(),
                last_good_age_ms: 1500,
            },
            config,
        );
        let app = Arc::new(AdminApp::new(hooks, HealthState::new()));
        if ready {
            app.mark_ready();
        }
        app
    }

    fn request(method: &str, path: &str, body: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(path)
            .body(Body::from(body.to_owned()))
            .unwrap()
    }

    #[tokio::test]
    async fn readiness_gate_answers_500_json_before_ready() {
        let router = full_router(app(false));
        let (status, content_type, body) =
            oneshot(router, request("GET", "/api/debug/health", "")).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(content_type, JSON_CONTENT_TYPE);
        assert_eq!(body, "\"service not ready\"");
    }

    #[tokio::test]
    async fn health_routes_and_override_match_go() {
        let app = app(true);
        let router = full_router(Arc::clone(&app));
        for path in ["/api/debug/health", "/debug/health"] {
            let (status, content_type, body) =
                oneshot(router.clone(), request("GET", path, "")).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(content_type, JSON_CONTENT_TYPE);
            assert_eq!(body, "{\"config_checksum\":42}");
        }
        let (status, _, body) = oneshot(
            router.clone(),
            request(
                "PUT",
                "/api/debug/health",
                "{\"healthy\":false,\"reason\":\" down \"}",
            ),
        )
        .await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "\"\""));
        let (status, _, body) = oneshot(router.clone(), request("GET", "/debug/health", "")).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(
            body,
            "{\"config_checksum\":42,\"unhealthy_reason\":\"down\"}"
        );
        let (status, _, body) = oneshot(
            router.clone(),
            request("PUT", "/api/debug/health", "{\"healthy\":\"yes\"}"),
        )
        .await;
        assert_eq!(
            (status, body.as_str()),
            (StatusCode::BAD_REQUEST, "\"bad health override json\"")
        );
        let (status, _, body) = oneshot(
            router.clone(),
            request("PUT", "/api/debug/health", "{\"extra\":1}"),
        )
        .await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "\"\""));
        let (status, content_type, body) =
            oneshot(router.clone(), request("HEAD", "/api/debug/health", "")).await;
        assert_eq!(
            (status, content_type.as_str(), body.as_str()),
            (StatusCode::NOT_FOUND, NOT_FOUND_CONTENT_TYPE, "")
        );
        let (status, _, body) = oneshot(router.clone(), request("GET", "/debug/health", "")).await;
        assert_eq!(
            (status, body.as_str()),
            (StatusCode::BAD_GATEWAY, "{\"config_checksum\":42}")
        );
        let (status, _, body) =
            oneshot(router.clone(), request("DELETE", "/debug/health", "")).await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "\"\""));
        let (status, _, _) = oneshot(router.clone(), request("GET", "/debug/health", "")).await;
        assert_eq!(status, StatusCode::OK);
        let (status, content_type, body) =
            oneshot(router, request("POST", "/api/debug/health", "")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(content_type, NOT_FOUND_CONTENT_TYPE);
        assert_eq!(body, "404 page not found");
    }

    #[tokio::test]
    async fn metrics_status_traffic_and_unknown_paths() {
        let router = full_router(app(true));
        for path in ["/metrics", "/metrics/", "/api/metrics", "/api/metrics/"] {
            let (status, content_type, body) =
                oneshot(router.clone(), request("GET", path, "")).await;
            assert_eq!(status, StatusCode::OK, "{path}");
            assert_eq!(content_type, METRICS_CONTENT_TYPE);
            assert_eq!(body, "# HELP x y\n");
        }
        let (status, _, body) = oneshot(router.clone(), request("HEAD", "/metrics", "")).await;
        assert_eq!((status, body.as_str()), (StatusCode::NOT_FOUND, ""));
        let (status, _, _) =
            oneshot(router.clone(), request("HEAD", "/api/dataplane/status", "")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, content_type, body) =
            oneshot(router.clone(), request("GET", "/api/dataplane/status", "")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type, JSON_CONTENT_TYPE);
        assert_eq!(
            body,
            "{\"applied_generation\":3,\"desired_generation\":3,\"detail\":\"\",\"enabled\":true,\"last_good_age_ms\":1500,\"last_result_code\":\"ERROR_CODE_OK\",\"rejected_generation\":0,\"sent_generation\":3}"
        );
        for (method, path, verb) in [
            ("POST", "/api/traffic/capture", "capture"),
            ("POST", "/api/traffic/replay", "replay"),
            ("POST", "/api/traffic/cancel", "cancel"),
            ("GET", "/api/traffic/show", "show"),
        ] {
            let (status, content_type, body) =
                oneshot(router.clone(), request(method, path, "")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(content_type, TEXT_CONTENT_TYPE);
            assert_eq!(
                body,
                format!("traffic {verb} is unsupported"),
                "the default configuration leaves enable-traffic-replay true"
            );
        }
        let (status, _, body) =
            oneshot(router.clone(), request("GET", "/api/debug/pprof/heap", "")).await;
        assert_eq!(
            (status, body.as_str()),
            (StatusCode::NOT_FOUND, "404 page not found")
        );
        let (status, _, _) = oneshot(router, request("POST", "/metrics", "")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn override_body_follows_go_decoder_semantics() {
        let ok = |text: &str| OverrideBody::decode(text.as_bytes()).unwrap();
        assert_eq!(ok("null"), OverrideBody::default());
        assert!(ok("{\"healthy\":true} garbage").healthy);
        assert_eq!(
            ok("{\"Healthy\":true,\"Reason\":\"maint\"}"),
            OverrideBody {
                healthy: true,
                reason: "maint".to_owned()
            }
        );
        assert!(ok("{\"healthy\":false,\"healthy\":true}").healthy);
        assert!(!ok("{\"healthy\":true,\"HEALTHY\":false}").healthy);
        assert_eq!(
            ok("{\"healthy\":null,\"reason\":null,\"x\":[1,{}]}"),
            OverrideBody::default()
        );
        for bad in [
            "",
            "{\"healthy\":\"bad\",\"healthy\":true}",
            "{\"healthy\":1}",
            "{\"reason\":5}",
            "[]",
            "\"text\"",
            "{\"healthy\":true",
            "{healthy:true}",
        ] {
            assert!(OverrideBody::decode(bad.as_bytes()).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn namespace_endpoints_match_go_bodies_and_codes() {
        let store = memory();
        let router = full_router(app_with(true, Arc::clone(&store)));
        let (status, content_type, body) =
            oneshot(router.clone(), request("GET", "/api/admin/namespace/", "")).await;
        assert_eq!(
            (status, content_type.as_str(), body.as_str()),
            (StatusCode::OK, JSON_CONTENT_TYPE, "\"\"")
        );
        let (status, _, body) = oneshot(
            router.clone(),
            request("GET", "/api/admin/namespace/dge", ""),
        )
        .await;
        assert_eq!(
            (status, body.as_str()),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "\"can not get namespace\""
            )
        );
        let (status, _, body) = oneshot(
            router.clone(),
            request(
                "PUT",
                "/api/admin/namespace/dge",
                "{\"Frontend\":{\"user\":\"<u>\"},\"namespace\":\"dge\"} trailing",
            ),
        )
        .await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "\"\""));
        let (status, _, body) = oneshot(
            router.clone(),
            request("GET", "/api/admin/namespace/dge", ""),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            "{\"namespace\":\"dge\",\"frontend\":{\"user\":\"\\u003cu\\u003e\",\"security\":{}},\"backend\":{\"instances\":[],\"security\":{}}}"
        );
        let (status, _, body) =
            oneshot(router.clone(), request("GET", "/api/admin/namespace", "")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.starts_with("[{\"namespace\":\"dge\""));
        for bad in [
            "{\"frontend\":5}",
            "{\"frontend\":{\"user\":1,\"user\":\"x\"}}",
            "not json",
        ] {
            let (status, _, body) = oneshot(
                router.clone(),
                request("PUT", "/api/admin/namespace/x", bad),
            )
            .await;
            assert_eq!(
                (status, body.as_str()),
                (StatusCode::BAD_REQUEST, "\"bad namespace json\""),
                "{bad}"
            );
        }
        let (status, _, body) = oneshot(
            router.clone(),
            request("PUT", "/api/admin/namespace/", "{\"namespace\":\"root\"}"),
        )
        .await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "\"\""));
        assert!(store.get_namespace("root").is_some());
        // The body name wins over the path and is the stored key (Go
        // SetNamespace(nsc.Namespace)); an empty name is a store error.
        let (status, _, _) = oneshot(
            router.clone(),
            request(
                "PUT",
                "/api/admin/namespace/path",
                "{\"namespace\":\"body\"}",
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(store.get_namespace("body").is_some() && store.get_namespace("path").is_none());
        let (status, _, body) = oneshot(
            router.clone(),
            request("PUT", "/api/admin/namespace/", "{}"),
        )
        .await;
        assert_eq!(
            (status, body.as_str()),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "\"can not update config\""
            )
        );
        let (status, _, body) = oneshot(
            router.clone(),
            request(
                "POST",
                "/api/admin/namespace/commit?namespace=dge&namespace=missing",
                "",
            ),
        )
        .await;
        assert_eq!(
            (status, body.as_str()),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "\"failed to get namespace\""
            )
        );
        let (status, _, body) = oneshot(
            router.clone(),
            request(
                "POST",
                "/api/admin/namespace/commit?namespace=dge&namespace=root",
                "",
            ),
        )
        .await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "\"\""));
        assert_eq!(store.committed(), vec!["dge".to_owned(), "root".to_owned()]);
        let (status, _, _) = oneshot(
            router.clone(),
            request("POST", "/api/admin/namespace/commit", ""),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(store.committed().is_empty());
        let (status, _, body) = oneshot(
            router.clone(),
            request("DELETE", "/api/admin/namespace/dge", ""),
        )
        .await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "\"\""));
        let (status, _, _) = oneshot(
            router.clone(),
            request("GET", "/api/admin/namespace/dge", ""),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        let (status, _, _) = oneshot(router, request("HEAD", "/api/admin/namespace/", "")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn config_endpoints_render_toml_and_json_and_apply_partial_updates() {
        let store = memory();
        let router = full_router(app_with(true, Arc::clone(&store)));
        let (status, content_type, body) =
            oneshot(router.clone(), request("GET", "/api/admin/config/", "")).await;
        assert_eq!(
            (status, content_type.as_str()),
            (StatusCode::OK, TOML_CONTENT_TYPE)
        );
        assert!(body.contains("max-connections = 0"), "{body}");
        for (path, accept) in [
            ("/api/admin/config/?format=JSON", None),
            ("/api/admin/config", Some("application/json")),
        ] {
            let mut req = request("GET", path, "");
            if let Some(accept) = accept {
                req.headers_mut()
                    .insert(http::header::ACCEPT, HeaderValue::from_static(accept));
            }
            let (status, content_type, body) = oneshot(router.clone(), req).await;
            assert_eq!(
                (status, content_type.as_str()),
                (StatusCode::OK, JSON_CONTENT_TYPE)
            );
            assert!(body.contains("\"max-connections\":0"), "{body}");
        }
        let (status, _, body) = oneshot(
            router.clone(),
            request(
                "PUT",
                "/api/admin/config/",
                "[proxy]\nmax-connections = 123\n",
            ),
        )
        .await;
        assert_eq!((status, body.as_str()), (StatusCode::OK, "\"\""));
        let (_, _, body) = oneshot(
            router.clone(),
            request("GET", "/api/admin/config/?format=json", ""),
        )
        .await;
        assert!(body.contains("\"max-connections\":123"), "{body}");
        for bad in [
            "[proxy]\naddr = \"0.0.0.0:6001\"\n",
            "[proxy",
            "[proxy]\nconn-buffer-size = 1\n",
        ] {
            let (status, _, body) =
                oneshot(router.clone(), request("PUT", "/api/admin/config/", bad)).await;
            assert_eq!(
                (status, body.as_str()),
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "\"can not update config\""
                ),
                "{bad}"
            );
        }
        let (_, _, body) =
            oneshot(router, request("GET", "/api/admin/config/?format=json", "")).await;
        assert!(body.contains("\"max-connections\":123"), "{body}");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn drain_endpoints_follow_the_go_status_mapping() {
        use crate::drain::{DrainProgress, DrainStartError, ScriptedDrainAdmin};
        let router = full_router(app(true));
        let (status, _, body) = oneshot(
            router.clone(),
            request("POST", "/api/dataplane/drain", "{\"drain_id\":\"d\"}"),
        )
        .await;
        assert_eq!(
            (status, body.as_str()),
            (StatusCode::NOT_FOUND, "{\"enabled\":false}")
        );
        let (status, _, body) = oneshot(router, request("GET", "/api/dataplane/drain/d", "")).await;
        assert_eq!(
            (status, body.as_str()),
            (StatusCode::NOT_FOUND, "{\"enabled\":false}")
        );

        let drain = Arc::new(ScriptedDrainAdmin::default());
        let mut hooks = AdminHooks::fixed(
            HealthInputs::default(),
            String::new(),
            DataplaneStatus::default(),
            memory(),
        );
        hooks.drain = Some(Arc::clone(&drain) as SharedDrainAdmin);
        let app = Arc::new(AdminApp::new(hooks, HealthState::new()));
        app.mark_ready();
        let router = full_router(app);
        for (body, expected) in [
            ("{}", DRAIN_ID_REQUIRED.to_owned()),
            (
                "{\"drain_id\":\"d\",\"graceful_wait_ms\":-1}",
                format!(
                    "graceful_wait_ms and force_timeout_ms must be within [0, {MAX_DRAIN_BUDGET_MS}] (30 days)"
                ),
            ),
            (
                "{\"drain_id\":\"d\",\"graceful_wait_ms\":9223372036854775807}",
                format!(
                    "graceful_wait_ms and force_timeout_ms must be within [0, {MAX_DRAIN_BUDGET_MS}] (30 days)"
                ),
            ),
            (
                "{\"drain_id\":\"d\",\"graceful_wait_ms\":1728000000,\"force_timeout_ms\":1728000000}",
                format!(
                    "graceful_wait_ms and force_timeout_ms must be within [0, {MAX_DRAIN_BUDGET_MS}] (30 days)"
                ),
            ),
        ] {
            let (status, content_type, actual) = oneshot(
                router.clone(),
                request("POST", "/api/dataplane/drain", body),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(content_type, JSON_CONTENT_TYPE);
            assert_eq!(
                actual,
                format!("{{\"error\":{}}}", go_json_string(&expected)),
                "{body}"
            );
        }
        assert!(
            drain.requests().is_empty(),
            "invalid budgets never reach the seam"
        );
        let (status, _, body) = oneshot(
            router.clone(),
            request(
                "POST",
                "/api/dataplane/drain",
                "{\"drain_id\":\"d\",\"listener_names\":[\"sql-0\"],\"graceful_wait_ms\":2591999000,\"force_timeout_ms\":1000}",
            ),
        )
        .await;
        assert_eq!(
            (status, body.as_str()),
            (StatusCode::ACCEPTED, "{\"drain_id\":\"d\"}")
        );
        assert_eq!(
            drain.requests()[0].graceful_wait,
            Duration::from_millis(2_591_999_000)
        );
        for (outcome, code) in [
            (DrainStartError::InvalidBudget, StatusCode::BAD_REQUEST),
            (DrainStartError::NoSession, StatusCode::SERVICE_UNAVAILABLE),
            (
                DrainStartError::SnapshotNotReady,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (DrainStartError::InProgress, StatusCode::CONFLICT),
            (DrainStartError::ForeignActive, StatusCode::CONFLICT),
            (
                DrainStartError::Other("boom".to_owned()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ] {
            drain.push_start(Err(outcome.clone()));
            let (status, _, body) = oneshot(
                router.clone(),
                request("POST", "/api/dataplane/drain", "{\"drain_id\":\"d\"}"),
            )
            .await;
            assert_eq!(status, code, "{outcome:?}");
            assert_eq!(
                body,
                format!("{{\"error\":{}}}", go_json_string(&outcome.message()))
            );
        }
        let (status, _, body) =
            oneshot(router.clone(), request("GET", "/api/dataplane/drain/d", "")).await;
        assert_eq!(
            (status, body.as_str()),
            (StatusCode::NOT_FOUND, "{\"known\":false}")
        );
        drain.set_status(
            "d",
            DrainProgress {
                active_connections: 2,
                gracefully_closed: 1,
                force_closed: 1,
                complete: true,
                code: "ERROR_CODE_OK".to_owned(),
                detail: String::new(),
            },
        );
        let (status, _, body) = oneshot(router, request("GET", "/api/dataplane/drain/d", "")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            "{\"active_connections\":2,\"code\":\"ERROR_CODE_OK\",\"complete\":true,\"detail\":\"\",\"drain_id\":\"d\",\"force_closed\":1,\"gracefully_closed\":1}"
        );
    }

    #[test]
    fn query_values_decode_like_go() {
        assert_eq!(
            query_values("namespace=a&namespace=b%2Bc&x=1&namespace=d+e", "namespace"),
            vec!["a".to_owned(), "b+c".to_owned(), "d e".to_owned()]
        );
        assert!(query_values("", "namespace").is_empty());
        assert_eq!(query_values("namespace", "namespace"), vec![String::new()]);
        // A malformed escape drops that pair only (Go url.ParseQuery).
        assert_eq!(
            query_values("namespace=%GG&namespace=ok", "namespace"),
            vec!["ok".to_owned()]
        );
    }

    #[tokio::test]
    async fn plaintext_branch_serves_only_get_health() {
        let router = plaintext_router(app(true));
        let (status, _, _) = oneshot(router.clone(), request("GET", "/api/debug/health", "")).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _, _) =
            oneshot(router.clone(), request("PUT", "/api/debug/health", "{}")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _, _) = oneshot(router, request("GET", "/metrics", "")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn disabled_status_is_the_exact_go_body() {
        assert_eq!(DataplaneStatus::default().to_json(), "{\"enabled\":false}");
    }

    #[test]
    fn rate_limiter_queues_same_instant_callers_and_caps_slack() {
        let limiter = RateLimiter::new(100);
        let start = Instant::now();
        // The very first reservation never waits.
        assert_eq!(limiter.reserve(start), Duration::ZERO);
        // Same instant: the second caller waits one slot, the third waits
        // behind it (uber keeps the queued debt when `last` is in the future).
        assert_eq!(limiter.reserve(start), Duration::from_millis(10));
        assert_eq!(limiter.reserve(start), Duration::from_millis(20));
        assert_eq!(limiter.reserve(start), Duration::from_millis(30));
        // A caller arriving exactly when the queue drains proceeds at once.
        assert_eq!(
            limiter.reserve(start + Duration::from_millis(40)),
            Duration::ZERO
        );
        // Idling for long banks at most the slack (ten slots): eleven
        // back-to-back callers pass, the twelfth waits one slot.
        let later = start + Duration::from_secs(5);
        for _ in 0..11 {
            assert_eq!(limiter.reserve(later), Duration::ZERO);
        }
        assert_eq!(limiter.reserve(later), Duration::from_millis(10));
    }
    /// Reviewer regression (`CodexM5`, `e30148d9`): queued debt must survive a
    /// `last` that already sits in the future.
    #[test]
    fn review_concurrent_rate_reservations_keep_future_debt() {
        let limiter = RateLimiter::new(100);
        let now = Instant::now();
        assert_eq!(limiter.reserve(now), Duration::ZERO);
        assert_eq!(limiter.reserve(now), Duration::from_millis(10));
        assert_eq!(limiter.reserve(now), Duration::from_millis(20));
    }

    /// Go `BackendMetrics`: always `200 application/json`, the reader's bytes
    /// (empty for nil), `c.Query("cluster")` = the first decoded value and a
    /// pair with a bad escape dropped; other methods are gin's 404.
    #[tokio::test]
    async fn backend_metrics_follows_go_backend_handler() {
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let recorder = Arc::clone(&seen);
        let mut hooks = AdminHooks::fixed(
            HealthInputs {
                closing: false,
                namespaces_ready: true,
                applied_generation: 1,
                config_checksum: 7,
            },
            String::new(),
            DataplaneStatus::default(),
            Arc::new(crate::config::MemoryConfigAdmin::default()),
        );
        hooks.backend_metrics = Arc::new(move |cluster| {
            recorder.lock().unwrap().push(cluster.to_owned());
            if cluster == "missing" {
                Vec::new()
            } else {
                b"{\"cpu\":{}}".to_vec()
            }
        });
        let app = Arc::new(AdminApp::new(hooks, HealthState::new()));
        app.mark_ready();
        for (path, status, content_type, body) in [
            (
                "/api/backend/metrics?cluster=a",
                StatusCode::OK,
                "application/json",
                "{\"cpu\":{}}",
            ),
            (
                "/api/backend/metrics?cluster=missing",
                StatusCode::OK,
                "application/json",
                "",
            ),
            (
                "/api/backend/metrics",
                StatusCode::OK,
                "application/json",
                "{\"cpu\":{}}",
            ),
            (
                "/api/backend/metrics?cluster=a%20b&cluster=c",
                StatusCode::OK,
                "application/json",
                "{\"cpu\":{}}",
            ),
            (
                "/api/backend/metrics?cluster=%zz",
                StatusCode::OK,
                "application/json",
                "{\"cpu\":{}}",
            ),
            (
                "/api/backend/metrics/",
                StatusCode::NOT_FOUND,
                "text/plain",
                "404 page not found",
            ),
        ] {
            let request = Request::builder()
                .method("GET")
                .uri(path)
                .body(Body::empty())
                .unwrap();
            let (got_status, got_type, got_body) =
                oneshot(full_router(Arc::clone(&app)), request).await;
            assert_eq!(
                (got_status, got_type.as_str(), got_body.as_str()),
                (status, content_type, body),
                "{path}"
            );
        }
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["a", "missing", "", "a b", ""],
            "the first decoded cluster value reaches the reader; a bad escape drops its pair"
        );
        let request = Request::builder()
            .method("POST")
            .uri("/api/backend/metrics")
            .body(Body::empty())
            .unwrap();
        let (status, _, body) = oneshot(full_router(app), request).await;
        assert_eq!(
            (status, body.as_str()),
            (StatusCode::NOT_FOUND, "404 page not found")
        );
    }

    /// Go `DebugRedirect`: `200 ""` when every router's sweep returns nil,
    /// `500 "redirect connections error"` on a router-level error; GET is 404.
    #[tokio::test]
    async fn debug_redirect_follows_go_debug_handler() {
        let fail = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&fail);
        let mut hooks = AdminHooks::fixed(
            HealthInputs {
                closing: false,
                namespaces_ready: true,
                applied_generation: 1,
                config_checksum: 7,
            },
            String::new(),
            DataplaneStatus::default(),
            Arc::new(crate::config::MemoryConfigAdmin::default()),
        );
        hooks.redirect = Arc::new(move || {
            if flag.load(Ordering::SeqCst) {
                Err("route plane terminated".to_owned())
            } else {
                Ok(())
            }
        });
        let app = Arc::new(AdminApp::new(hooks, HealthState::new()));
        app.mark_ready();
        let post = || {
            Request::builder()
                .method("POST")
                .uri("/api/debug/redirect")
                .body(Body::empty())
                .unwrap()
        };
        let (status, content_type, body) = oneshot(full_router(Arc::clone(&app)), post()).await;
        assert_eq!(
            (status, content_type.as_str(), body.as_str()),
            (StatusCode::OK, JSON_CONTENT_TYPE, "\"\"")
        );
        fail.store(true, Ordering::SeqCst);
        let (status, content_type, body) = oneshot(full_router(Arc::clone(&app)), post()).await;
        assert_eq!(
            (status, content_type.as_str(), body.as_str()),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                JSON_CONTENT_TYPE,
                "\"redirect connections error\""
            )
        );
        let get = Request::builder()
            .method("GET")
            .uri("/api/debug/redirect")
            .body(Body::empty())
            .unwrap();
        let (status, _, body) = oneshot(full_router(app), get).await;
        assert_eq!(
            (status, body.as_str()),
            (StatusCode::NOT_FOUND, "404 page not found")
        );
    }
}
