mod cells;

use anyhow::{Context as _, Result, anyhow, bail};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use cells::{CellPool, CellPoolConfig, CellStatus};
use flags2env::BundledFlags2Env;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    env,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const MAX_TIMEOUT_MS: u64 = 20 * 60 * 1_000;
const MAX_BODY_BYTES: usize = 256 * 1024;
const MAX_PARALLELISM: usize = 4096;
const MAX_CELL_INVOCATIONS: u64 = 10_000_000;
const MAX_LIVE_CELLS: usize = 4096;
const MAX_CELLS_PER_GENERATION: usize = 64;
const MAX_CELL_CONCURRENCY: usize = 1024;
const MAX_STATELESS_ISOLATES: usize = 4096;
const MAX_CONTEXTS_PER_ISOLATE: usize = 1024;
const MAX_AFFINITY_ISOLATES: usize = 1_000_000;
const MAX_CELL_IDLE_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
const MAX_CELL_AGE_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const MAX_AFFINITY_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
struct CliConfig {
    GS_DESKTOP_ADDR: String,
    GS_JAVA_COMMAND: String,
    GS_CELL_MAIN_CLASS: String,
    GS_ARTIFACT_ROOT: Option<String>,
    GS_MAX_PARALLEL_INVOCATIONS: i64,
    GS_MAX_CELL_INVOCATIONS: i64,
    GS_MAX_LIVE_CELLS: i64,
    GS_MAX_CELLS_PER_GENERATION: i64,
    GS_MAX_CELL_CONCURRENCY: i64,
    GS_CELL_IDLE_TTL_MS: i64,
    GS_MAX_CELL_AGE_MS: i64,
    GS_MAX_STATELESS_ISOLATES: i64,
    GS_MAX_CONTEXTS_PER_ISOLATE: i64,
    GS_MAX_ROUTE_ISOLATES: i64,
    GS_MAX_SESSION_ISOLATES: i64,
    GS_MAX_ROUTE_SESSION_ISOLATES: i64,
    GS_SESSION_ISOLATE_TTL_MS: i64,
    GS_ROUTE_ISOLATE_TTL_MS: i64,
    GS_MAX_ISOLATE_MEMORY: String,
    GS_MAX_GUEST_HEAP_MEMORY: String,
    GS_MAX_GUEST_CPU_TIME_MS: i64,
    GS_MAX_AST_DEPTH: i64,
    GS_MAX_GUEST_THREADS: i64,
    GS_MAX_GUEST_STDOUT: String,
    GS_MAX_GUEST_STDERR: String,
    GS_ISOLATE_MODE: String,
    GS_DESKTOP_TOKEN_FILE: Option<String>,
    GS_DESKTOP_LOG: String,
}

#[derive(Debug)]
struct RuntimeConfig {
    addr: SocketAddr,
    parallelism: usize,
    cells: CellPoolConfig,
    token_path: PathBuf,
    log_filter: String,
}

#[derive(Clone)]
struct AppState {
    token: Arc<str>,
    permits: Arc<Semaphore>,
    cells: CellPool,
    started_at: Instant,
    accepted: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvocationRequest {
    invocation_id: String,
    tenant_id: String,
    deployment_id: String,
    route_id: Option<String>,
    session_id: Option<String>,
    affinity: Option<String>,
    payload_json: Value,
    timeout_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct InvocationResponse {
    invocation_id: String,
    deployment_id: String,
    ok: bool,
    payload_json: Option<Value>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    runtime: &'static str,
    logical_actor_reusable: bool,
    context_reusable: bool,
    engine_cell_reusable: bool,
    cell_reuse_scope: &'static str,
    security_boundary: &'static str,
    affinity_modes: [&'static str; 4],
    request_protocol: &'static str,
    config_source: &'static str,
    uptime_ms: u128,
    accepted: u64,
    completed: u64,
    failed: u64,
    live_cells: usize,
    available_invocation_slots: usize,
    available_cell_slots: usize,
    max_cells_per_generation: usize,
    max_cell_concurrency: usize,
    max_cell_invocations: u64,
    max_stateless_isolates: usize,
    max_route_isolates: usize,
    max_session_isolates: usize,
    max_route_session_isolates: usize,
    session_isolate_ttl_ms: u128,
    route_isolate_ttl_ms: u128,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&config.log_filter).context("invalid tracing filter")?)
        .init();

    let token = load_or_create_token(&config.token_path)?;
    let cells = CellPool::new(config.cells);
    cells.start_reaper();
    let state = AppState {
        token: Arc::from(token),
        permits: Arc::new(Semaphore::new(config.parallelism)),
        cells,
        started_at: Instant::now(),
        accepted: Arc::new(AtomicU64::new(0)),
        completed: Arc::new(AtomicU64::new(0)),
        failed: Arc::new(AtomicU64::new(0)),
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/status", get(status))
        .route("/v1/doctor", get(status))
        .route("/v1/cells", get(list_cells))
        .route(
            "/v1/cells/{tenant_id}/{deployment_id}/retire",
            post(retire_generation_route),
        )
        .route(
            "/v1/cells/{tenant_id}/{deployment_id}/{cell_index}/drain",
            post(drain_cell_route),
        )
        .route(
            "/v1/cells/{tenant_id}/{deployment_id}/{cell_index}/retire",
            post(retire_cell_route),
        )
        .route("/v1/invoke", post(invoke))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(config.addr).await?;
    tracing::info!(addr = %config.addr, "graal-show desktop daemon listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    state.cells.shutdown().await;
    return Ok(());
}

fn load_config() -> Result<RuntimeConfig> {
    let config_path = resolve_config_path()?;
    let config_path_text = config_path
        .to_str()
        .ok_or_else(|| anyhow!(".cli-flags.toml path is not UTF-8"))?;
    let parser = BundledFlags2Env::new();
    parser
        .audit_config(Some(config_path_text))
        .map_err(|error| anyhow!(error.to_string()))?;

    let argv = env::args().collect::<Vec<_>>();
    let parsed = parser
        .parse_structured(&argv, Some(config_path_text))
        .map_err(|error| anyhow!(error.to_string()))?;
    if !parsed.unknown_options.is_empty() {
        bail!(
            "unknown command-line options: {}",
            parsed.unknown_options.len()
        );
    }
    if !parsed.errors.is_empty() {
        bail!("invalid command-line values: {}", parsed.errors.join("; "));
    }
    if !parsed.extras.is_empty() {
        bail!("unexpected positional arguments: {}", parsed.extras.len());
    }

    let mut raw = env::vars().collect::<HashMap<_, _>>();
    raw.extend(parsed.provided_flags);
    let raw_config = parser
        .coerce::<CliConfig, _>(&raw, Some(config_path_text))
        .map_err(|error| anyhow!(error.to_string()))?;

    let addr = parse_loopback_addr(&raw_config.GS_DESKTOP_ADDR)?;
    let java_command = raw_config.GS_JAVA_COMMAND.trim().to_owned();
    if java_command.is_empty() {
        bail!("GS_JAVA_COMMAND may not be empty");
    }
    let main_class = validate_main_class(&raw_config.GS_CELL_MAIN_CLASS)?;
    let artifact_root = match raw_config.GS_ARTIFACT_ROOT {
        Some(path) if !path.trim().is_empty() => expand_home(Path::new(&path))?,
        _ => default_artifact_root()?,
    };
    let parallelism = bounded_usize(
        "GS_MAX_PARALLEL_INVOCATIONS",
        raw_config.GS_MAX_PARALLEL_INVOCATIONS,
        MAX_PARALLELISM,
    )?;
    let max_live_cells = bounded_usize(
        "GS_MAX_LIVE_CELLS",
        raw_config.GS_MAX_LIVE_CELLS,
        MAX_LIVE_CELLS,
    )?;
    let max_cells_per_generation = bounded_usize(
        "GS_MAX_CELLS_PER_GENERATION",
        raw_config.GS_MAX_CELLS_PER_GENERATION,
        MAX_CELLS_PER_GENERATION,
    )?;
    let max_cell_concurrency = bounded_usize(
        "GS_MAX_CELL_CONCURRENCY",
        raw_config.GS_MAX_CELL_CONCURRENCY,
        MAX_CELL_CONCURRENCY,
    )?;
    let max_cell_invocations = bounded_u64(
        "GS_MAX_CELL_INVOCATIONS",
        raw_config.GS_MAX_CELL_INVOCATIONS,
        MAX_CELL_INVOCATIONS,
    )?;
    let cell_idle_ttl_ms = bounded_u64(
        "GS_CELL_IDLE_TTL_MS",
        raw_config.GS_CELL_IDLE_TTL_MS,
        MAX_CELL_IDLE_TTL_MS,
    )?;
    let max_cell_age_ms = bounded_u64(
        "GS_MAX_CELL_AGE_MS",
        raw_config.GS_MAX_CELL_AGE_MS,
        MAX_CELL_AGE_MS,
    )?;
    let max_stateless_isolates = bounded_usize(
        "GS_MAX_STATELESS_ISOLATES",
        raw_config.GS_MAX_STATELESS_ISOLATES,
        MAX_STATELESS_ISOLATES,
    )?;
    let max_contexts_per_isolate = bounded_usize(
        "GS_MAX_CONTEXTS_PER_ISOLATE",
        raw_config.GS_MAX_CONTEXTS_PER_ISOLATE,
        MAX_CONTEXTS_PER_ISOLATE,
    )?;
    let max_route_isolates = bounded_usize(
        "GS_MAX_ROUTE_ISOLATES",
        raw_config.GS_MAX_ROUTE_ISOLATES,
        MAX_AFFINITY_ISOLATES,
    )?;
    let max_session_isolates = bounded_usize(
        "GS_MAX_SESSION_ISOLATES",
        raw_config.GS_MAX_SESSION_ISOLATES,
        MAX_AFFINITY_ISOLATES,
    )?;
    let max_route_session_isolates = bounded_usize(
        "GS_MAX_ROUTE_SESSION_ISOLATES",
        raw_config.GS_MAX_ROUTE_SESSION_ISOLATES,
        MAX_AFFINITY_ISOLATES,
    )?;
    let session_isolate_ttl_ms = bounded_u64(
        "GS_SESSION_ISOLATE_TTL_MS",
        raw_config.GS_SESSION_ISOLATE_TTL_MS,
        MAX_AFFINITY_TTL_MS,
    )?;
    let route_isolate_ttl_ms = bounded_u64(
        "GS_ROUTE_ISOLATE_TTL_MS",
        raw_config.GS_ROUTE_ISOLATE_TTL_MS,
        MAX_AFFINITY_TTL_MS,
    )?;
    let max_guest_cpu_time_ms = bounded_u64(
        "GS_MAX_GUEST_CPU_TIME_MS",
        raw_config.GS_MAX_GUEST_CPU_TIME_MS,
        MAX_TIMEOUT_MS,
    )?;
    let max_ast_depth = bounded_u32("GS_MAX_AST_DEPTH", raw_config.GS_MAX_AST_DEPTH, 100_000)?;
    let max_guest_threads =
        bounded_u32("GS_MAX_GUEST_THREADS", raw_config.GS_MAX_GUEST_THREADS, 64)?;
    let max_isolate_memory =
        validate_memory_limit("GS_MAX_ISOLATE_MEMORY", &raw_config.GS_MAX_ISOLATE_MEMORY)?;
    let max_guest_heap_memory = validate_memory_limit(
        "GS_MAX_GUEST_HEAP_MEMORY",
        &raw_config.GS_MAX_GUEST_HEAP_MEMORY,
    )?;
    let max_guest_stdout =
        validate_memory_limit("GS_MAX_GUEST_STDOUT", &raw_config.GS_MAX_GUEST_STDOUT)?;
    let max_guest_stderr =
        validate_memory_limit("GS_MAX_GUEST_STDERR", &raw_config.GS_MAX_GUEST_STDERR)?;
    let isolate_mode = raw_config.GS_ISOLATE_MODE.trim().to_ascii_lowercase();
    if isolate_mode != "internal" && isolate_mode != "external" {
        bail!("GS_ISOLATE_MODE must be internal or external");
    }
    let token_path = match raw_config.GS_DESKTOP_TOKEN_FILE {
        Some(path) if !path.trim().is_empty() => expand_home(Path::new(&path))?,
        _ => default_token_path()?,
    };

    return Ok(RuntimeConfig {
        addr,
        parallelism,
        cells: CellPoolConfig {
            java_command,
            main_class,
            artifact_root,
            max_live_cells,
            max_cells_per_generation,
            max_cell_concurrency,
            max_cell_invocations,
            cell_idle_ttl: Duration::from_millis(cell_idle_ttl_ms),
            max_cell_age: Duration::from_millis(max_cell_age_ms),
            max_stateless_isolates,
            max_contexts_per_isolate,
            max_route_isolates,
            max_session_isolates,
            max_route_session_isolates,
            session_isolate_ttl: Duration::from_millis(session_isolate_ttl_ms),
            route_isolate_ttl: Duration::from_millis(route_isolate_ttl_ms),
            max_isolate_memory,
            max_guest_heap_memory,
            max_guest_cpu_time_ms,
            max_ast_depth,
            max_guest_threads,
            max_guest_stdout,
            max_guest_stderr,
            isolate_mode,
        },
        token_path,
        log_filter: raw_config.GS_DESKTOP_LOG,
    });
}

fn validate_main_class(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 512
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'$'))
    {
        bail!("GS_CELL_MAIN_CLASS is invalid");
    }
    return Ok(value.to_owned());
}

fn bounded_usize(name: &str, value: i64, maximum: usize) -> Result<usize> {
    let value = usize::try_from(value)
        .ok()
        .filter(|value| *value > 0 && *value <= maximum)
        .ok_or_else(|| anyhow!("{name} must be between 1 and {maximum}"))?;
    return Ok(value);
}

fn bounded_u64(name: &str, value: i64, maximum: u64) -> Result<u64> {
    let value = u64::try_from(value)
        .ok()
        .filter(|value| *value > 0 && *value <= maximum)
        .ok_or_else(|| anyhow!("{name} must be between 1 and {maximum}"))?;
    return Ok(value);
}

fn bounded_u32(name: &str, value: i64, maximum: u32) -> Result<u32> {
    let value = u32::try_from(value)
        .ok()
        .filter(|value| *value > 0 && *value <= maximum)
        .ok_or_else(|| anyhow!("{name} must be between 1 and {maximum}"))?;
    return Ok(value);
}

fn validate_memory_limit(name: &str, value: &str) -> Result<String> {
    let value = value.trim();
    let suffix = ["KB", "MB", "GB"]
        .into_iter()
        .find(|suffix| value.ends_with(suffix))
        .ok_or_else(|| anyhow!("{name} must use KB, MB, or GB units"))?;
    let number = value.trim_end_matches(suffix);
    if number.is_empty()
        || number.starts_with('0')
        || !number.bytes().all(|byte| byte.is_ascii_digit())
    {
        bail!("{name} must be a positive integer followed by {suffix}");
    }
    return Ok(value.to_owned());
}

async fn health() -> &'static str {
    return "ok";
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    return Ok(Json(StatusResponse {
        runtime: "graalvm_polyglot",
        logical_actor_reusable: true,
        context_reusable: true,
        engine_cell_reusable: true,
        cell_reuse_scope: "same_tenant_generation",
        security_boundary: "os_process_per_tenant_generation",
        affinity_modes: ["stateless", "route", "session", "route_session"],
        request_protocol: "u32be_length_prefixed_json_v1",
        config_source: "flags-2-env",
        uptime_ms: state.started_at.elapsed().as_millis(),
        accepted: state.accepted.load(Ordering::Relaxed),
        completed: state.completed.load(Ordering::Relaxed),
        failed: state.failed.load(Ordering::Relaxed),
        live_cells: state.cells.live_cells().await,
        available_invocation_slots: state.permits.available_permits(),
        available_cell_slots: state.cells.available_cell_slots(),
        max_cells_per_generation: state.cells.max_cells_per_generation(),
        max_cell_concurrency: state.cells.max_cell_concurrency(),
        max_cell_invocations: state.cells.max_cell_invocations(),
        max_stateless_isolates: state.cells.max_stateless_isolates(),
        max_route_isolates: state.cells.max_route_isolates(),
        max_session_isolates: state.cells.max_session_isolates(),
        max_route_session_isolates: state.cells.max_route_session_isolates(),
        session_isolate_ttl_ms: state.cells.session_isolate_ttl_ms(),
        route_isolate_ttl_ms: state.cells.route_isolate_ttl_ms(),
    }));
}

async fn list_cells(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<CellStatus>>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    return Ok(Json(state.cells.statuses().await));
}

async fn retire_generation_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((tenant_id, deployment_id)): AxumPath<(String, String)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("tenant_id", &tenant_id)?;
    validate_identifier("deployment_id", &deployment_id)?;
    let indices = state
        .cells
        .statuses()
        .await
        .into_iter()
        .filter(|cell| cell.tenant_id == tenant_id && cell.deployment_id == deployment_id)
        .map(|cell| cell.cell_index)
        .collect::<Vec<_>>();
    let mut retired = 0_usize;
    for index in indices {
        if state.cells.retire(&tenant_id, &deployment_id, index).await {
            retired = retired.saturating_add(1);
        }
    }
    return Ok(Json(
        json!({ "retired": retired > 0, "retired_cells": retired }),
    ));
}

async fn drain_cell_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((tenant_id, deployment_id, cell_index)): AxumPath<(String, String, u32)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("tenant_id", &tenant_id)?;
    validate_identifier("deployment_id", &deployment_id)?;
    let draining = state
        .cells
        .drain(&tenant_id, &deployment_id, cell_index)
        .await;
    return Ok(Json(json!({ "draining": draining })));
}

async fn retire_cell_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((tenant_id, deployment_id, cell_index)): AxumPath<(String, String, u32)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("tenant_id", &tenant_id)?;
    validate_identifier("deployment_id", &deployment_id)?;
    let retired = state
        .cells
        .retire(&tenant_id, &deployment_id, cell_index)
        .await;
    return Ok(Json(json!({ "retired": retired })));
}

async fn invoke(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<InvocationRequest>,
) -> Result<Json<InvocationResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("invocation_id", &request.invocation_id)?;
    validate_identifier("tenant_id", &request.tenant_id)?;
    validate_identifier("deployment_id", &request.deployment_id)?;
    if let Some(route_id) = request.route_id.as_deref() {
        validate_identifier("route_id", route_id)?;
    }
    if let Some(session_id) = request.session_id.as_deref() {
        validate_identifier("session_id", session_id)?;
    }
    let affinity = validate_affinity(
        request.affinity.as_deref().unwrap_or("stateless"),
        request.route_id.as_deref(),
        request.session_id.as_deref(),
    )?;

    let timeout_ms = request.timeout_ms.unwrap_or(30_000);
    if timeout_ms == 0 || timeout_ms > MAX_TIMEOUT_MS {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("timeout_ms must be between 1 and {MAX_TIMEOUT_MS}"),
        ));
    }

    let _permit = state.permits.clone().try_acquire_owned().map_err(|_| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Graal invocation capacity is exhausted".to_owned(),
        )
    })?;
    state.accepted.fetch_add(1, Ordering::Relaxed);

    let result = state
        .cells
        .invoke(
            &request.tenant_id,
            &request.deployment_id,
            &request.invocation_id,
            request.route_id.as_deref(),
            request.session_id.as_deref(),
            affinity,
            &request.payload_json,
            timeout_ms,
        )
        .await;
    state.completed.fetch_add(1, Ordering::Relaxed);
    if result.is_err() {
        state.failed.fetch_add(1, Ordering::Relaxed);
    }

    let response = match result {
        Ok(payload_json) => InvocationResponse {
            invocation_id: request.invocation_id,
            deployment_id: request.deployment_id,
            ok: true,
            payload_json: Some(payload_json),
            error: None,
        },
        Err(error) => InvocationResponse {
            invocation_id: request.invocation_id,
            deployment_id: request.deployment_id,
            ok: false,
            payload_json: None,
            error: Some(error.to_string()),
        },
    };
    return Ok(Json(response));
}

fn validate_affinity<'a>(
    affinity: &'a str,
    route_id: Option<&str>,
    session_id: Option<&str>,
) -> Result<&'a str, (StatusCode, String)> {
    match affinity {
        "stateless" => Ok(affinity),
        "route" if route_id.is_some() => Ok(affinity),
        "session" if session_id.is_some() => Ok(affinity),
        "route_session" if route_id.is_some() && session_id.is_some() => Ok(affinity),
        "route" => Err((StatusCode::BAD_REQUEST, "route affinity requires route_id".to_owned())),
        "session" => Err((
            StatusCode::BAD_REQUEST,
            "session affinity requires session_id".to_owned(),
        )),
        "route_session" => Err((
            StatusCode::BAD_REQUEST,
            "route_session affinity requires route_id and session_id".to_owned(),
        )),
        _ => Err((StatusCode::BAD_REQUEST, "unsupported affinity".to_owned())),
    }
}

fn authorize(headers: &HeaderMap, state: &AppState) -> Result<(), (StatusCode, String)> {
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if provided.is_some_and(|token| constant_time_eq(token.as_bytes(), state.token.as_bytes())) {
        return Ok(());
    }
    return Err((StatusCode::UNAUTHORIZED, "unauthorized".to_owned()));
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..max_len {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    return difference == 0;
}

fn validate_identifier(name: &str, value: &str) -> Result<(), (StatusCode, String)> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && value != "."
        && value != "..";
    if valid {
        return Ok(());
    }
    return Err((StatusCode::BAD_REQUEST, format!("invalid {name}")));
}

fn parse_loopback_addr(value: &str) -> Result<SocketAddr> {
    let addr: SocketAddr = value
        .parse()
        .context("GS_DESKTOP_ADDR is not a socket address")?;
    if !is_loopback(addr.ip()) {
        bail!("GS_DESKTOP_ADDR must bind to loopback");
    }
    return Ok(addr);
}

fn is_loopback(ip: IpAddr) -> bool {
    return ip.is_loopback();
}

fn resolve_config_path() -> Result<PathBuf> {
    if let Some(path) = env::var_os("GS_DESKTOP_FLAGS_CONFIG") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        bail!("GS_DESKTOP_FLAGS_CONFIG is not a readable file");
    }
    let current = env::current_dir()?.join(".cli-flags.toml");
    if current.is_file() {
        return Ok(current);
    }
    let executable = env::current_exe()?;
    if let Some(parent) = executable.parent() {
        let adjacent = parent.join(".cli-flags.toml");
        if adjacent.is_file() {
            return Ok(adjacent);
        }
    }
    bail!("cannot locate .cli-flags.toml");
}

fn default_artifact_root() -> Result<PathBuf> {
    let home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .ok_or_else(|| anyhow!("HOME or USERPROFILE is required"))?;
    return Ok(PathBuf::from(home).join(".graal-show/artifacts"));
}

fn default_token_path() -> Result<PathBuf> {
    let home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .ok_or_else(|| anyhow!("HOME or USERPROFILE is required"))?;
    return Ok(PathBuf::from(home).join(".graal-show/daemon/token"));
}

fn expand_home(path: &Path) -> Result<PathBuf> {
    let text = path.to_string_lossy();
    if text == "~" || text.starts_with("~/") {
        let home = env::var_os("HOME")
            .or_else(|| env::var_os("USERPROFILE"))
            .ok_or_else(|| anyhow!("HOME or USERPROFILE is required"))?;
        let suffix = text.trim_start_matches('~').trim_start_matches('/');
        return Ok(PathBuf::from(home).join(suffix));
    }
    return Ok(path.to_path_buf());
}

fn validate_token_file_metadata(metadata: &std::fs::Metadata) -> Result<()> {
    if metadata.file_type().is_symlink() {
        bail!("desktop daemon token path may not be a symlink");
    }
    if !metadata.is_file() {
        bail!("desktop daemon token path must be a regular file");
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        if mode != 0o600 {
            bail!("desktop daemon token file must use owner-only mode 0600");
        }
    }
    return Ok(());
}

fn read_existing_token(path: &Path) -> Result<Option<String>> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "cannot inspect desktop daemon token file {}",
                    path.display()
                )
            });
        }
    };
    validate_token_file_metadata(&metadata)?;

    let token = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read desktop daemon token file {}", path.display()))?;
    let token = token.trim();
    if token.len() < 32 || token.chars().any(char::is_whitespace) {
        bail!("desktop daemon token file is malformed");
    }
    return Ok(Some(token.to_owned()));
}

fn load_or_create_token(path: &Path) -> Result<String> {
    if let Some(token) = read_existing_token(path)? {
        return Ok(token);
    }

    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("token path has no parent"))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("cannot create token directory {}", parent.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("cannot secure token directory {}", parent.display()))?;
    }

    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    use std::io::Write as _;
    match options.open(path) {
        Ok(mut file) => {
            file.write_all(format!("{token}\n").as_bytes())
                .with_context(|| {
                    format!("cannot write desktop daemon token file {}", path.display())
                })?;
            file.sync_all().with_context(|| {
                format!("cannot sync desktop daemon token file {}", path.display())
            })?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return read_existing_token(path)?.ok_or_else(|| {
                anyhow!("desktop daemon token file appeared but could not be read")
            });
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("cannot create desktop daemon token file {}", path.display())
            });
        }
    }

    let metadata = std::fs::symlink_metadata(path).with_context(|| {
        format!(
            "cannot inspect new desktop daemon token file {}",
            path.display()
        )
    })?;
    validate_token_file_metadata(&metadata)?;
    return Ok(token);
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_loopback_bind() {
        assert!(parse_loopback_addr("127.0.0.1:8764").is_ok());
        assert!(parse_loopback_addr("0.0.0.0:8764").is_err());
    }

    #[test]
    fn deployment_key_rejects_path_traversal() {
        assert!(validate_identifier("deployment_id", "deployment-1").is_ok());
        assert!(validate_identifier("deployment_id", "..").is_err());
        assert!(validate_identifier("deployment_id", "tenant/escape").is_err());
    }

    #[test]
    fn affinity_requires_matching_keys() {
        assert_eq!(validate_affinity("stateless", None, None).ok(), Some("stateless"));
        assert_eq!(
            validate_affinity("route", Some("orders.get"), None).ok(),
            Some("route")
        );
        assert!(validate_affinity("route", None, None).is_err());
        assert_eq!(
            validate_affinity("session", None, Some("session-1")).ok(),
            Some("session")
        );
        assert_eq!(
            validate_affinity(
                "route_session",
                Some("orders.get"),
                Some("session-1")
            )
            .ok(),
            Some("route_session")
        );
    }

    #[test]
    fn memory_limits_require_units() {
        assert_eq!(
            validate_memory_limit("memory", "256MB").ok(),
            Some("256MB".to_owned())
        );
        assert!(validate_memory_limit("memory", "0MB").is_err());
        assert!(validate_memory_limit("memory", "256").is_err());
    }
}

#[cfg(test)]
mod constant_time_auth_tests {
    use super::constant_time_eq;

    #[test]
    fn constant_time_token_comparison_matches_only_exact_bytes() {
        assert!(constant_time_eq(
            b"abcdefghijklmnopqrstuvwxyz012345",
            b"abcdefghijklmnopqrstuvwxyz012345"
        ));
        assert!(!constant_time_eq(
            b"abcdefghijklmnopqrstuvwxyz012345",
            b"abcdefghijklmnopqrstuvwxyz012346"
        ));
        assert!(!constant_time_eq(b"short", b"shorter"));
        assert!(!constant_time_eq(b"longer", b"long"));
    }
}
