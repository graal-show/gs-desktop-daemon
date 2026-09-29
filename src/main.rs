use anyhow::{Context as _, Result, anyhow, bail};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
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
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    time::timeout,
};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const MAX_TIMEOUT_MS: u64 = 20 * 60 * 1_000;
const MAX_BODY_BYTES: usize = 256 * 1024;
const MAX_CELL_INVOCATIONS: u64 = 1_000_000;
const MAX_LIVE_CELLS: usize = 2048;
const MAX_CELL_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
struct CliConfig {
    GS_DESKTOP_ADDR: String,
    GS_JAVA_COMMAND: String,
    GS_CELL_MAIN_CLASS: String,
    GS_ARTIFACT_ROOT: Option<String>,
    GS_MAX_CELL_INVOCATIONS: i64,
    GS_MAX_LIVE_CELLS: i64,
    GS_TENANT_ISOLATE_POOL_SIZE: i64,
    GS_DESKTOP_TOKEN_FILE: Option<String>,
    GS_DESKTOP_LOG: String,
}

#[derive(Debug)]
struct RuntimeConfig {
    addr: SocketAddr,
    java_command: String,
    main_class: String,
    artifact_root: PathBuf,
    max_cell_invocations: u64,
    max_live_cells: usize,
    pool_size: usize,
    token_path: PathBuf,
    log_filter: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CellKey {
    tenant_id: String,
    deployment_id: String,
}

struct Cell {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    invocation_count: u64,
    _live_permit: OwnedSemaphorePermit,
}

struct CellHandle {
    cell: Mutex<Cell>,
    slot: Arc<Semaphore>,
}

struct CellPool {
    cells: Vec<Arc<CellHandle>>,
    next: AtomicU64,
}

#[derive(Clone)]
struct AppState {
    token: Arc<str>,
    java_command: Arc<str>,
    main_class: Arc<str>,
    artifact_root: Arc<PathBuf>,
    max_cell_invocations: u64,
    max_live_cells: usize,
    cell_slots: Arc<Semaphore>,
    pools: Arc<Mutex<HashMap<CellKey, Arc<CellPool>>>>,
    pool_size: usize,
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
    request_context_reusable: bool,
    engine_cell_reusable: bool,
    cell_reuse_scope: &'static str,
    config_source: &'static str,
    uptime_ms: u128,
    accepted: u64,
    completed: u64,
    failed: u64,
    live_cells: usize,
    live_generation_pools: usize,
    max_cell_invocations: u64,
    max_live_cells: usize,
    available_cell_slots: usize,
    tenant_isolate_pool_size: usize,
}

#[derive(Debug, Serialize)]
struct CellStatus {
    tenant_id: String,
    deployment_id: String,
    invocation_count: u64,
    running: bool,
    busy: bool,
    cell_index: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&config.log_filter).context("invalid tracing filter")?)
        .init();

    let token = load_or_create_token(&config.token_path)?;
    let state = AppState {
        token: Arc::from(token),
        java_command: Arc::from(config.java_command),
        main_class: Arc::from(config.main_class),
        artifact_root: Arc::new(config.artifact_root),
        max_cell_invocations: config.max_cell_invocations,
        max_live_cells: config.max_live_cells,
        cell_slots: Arc::new(Semaphore::new(config.max_live_cells)),
        pools: Arc::new(Mutex::new(HashMap::new())),
        pool_size: config.pool_size,
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
    terminate_all_cells(&state).await;
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
        bail!("unknown command-line options: {}", parsed.unknown_options.len());
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
    let main_class = raw_config.GS_CELL_MAIN_CLASS.trim().to_owned();
    if main_class.is_empty()
        || main_class.len() > 512
        || !main_class
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'$'))
    {
        bail!("GS_CELL_MAIN_CLASS is invalid");
    }
    let artifact_root = match raw_config.GS_ARTIFACT_ROOT {
        Some(path) if !path.trim().is_empty() => expand_home(Path::new(&path))?,
        _ => default_artifact_root()?,
    };
    let max_cell_invocations = u64::try_from(raw_config.GS_MAX_CELL_INVOCATIONS)
        .ok()
        .filter(|value| *value > 0 && *value <= MAX_CELL_INVOCATIONS)
        .ok_or_else(|| {
            anyhow!("GS_MAX_CELL_INVOCATIONS must be between 1 and {MAX_CELL_INVOCATIONS}")
        })?;
    let max_live_cells = usize::try_from(raw_config.GS_MAX_LIVE_CELLS)
        .ok()
        .filter(|value| *value > 0 && *value <= MAX_LIVE_CELLS)
        .ok_or_else(|| anyhow!("GS_MAX_LIVE_CELLS must be between 1 and {MAX_LIVE_CELLS}"))?;
    let pool_size = usize::try_from(raw_config.GS_TENANT_ISOLATE_POOL_SIZE)
        .ok()
        .filter(|value| *value > 0 && *value <= 16)
        .ok_or_else(|| anyhow!("GS_TENANT_ISOLATE_POOL_SIZE must be between 1 and 16"))?;
    if pool_size > max_live_cells {
        bail!("GS_TENANT_ISOLATE_POOL_SIZE may not exceed GS_MAX_LIVE_CELLS");
    }
    let token_path = match raw_config.GS_DESKTOP_TOKEN_FILE {
        Some(path) if !path.trim().is_empty() => expand_home(Path::new(&path))?,
        _ => default_token_path()?,
    };

    return Ok(RuntimeConfig {
        addr,
        java_command,
        main_class,
        artifact_root,
        max_cell_invocations,
        max_live_cells,
        pool_size,
        token_path,
        log_filter: raw_config.GS_DESKTOP_LOG,
    });
}

async fn health() -> &'static str {
    return "ok";
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let pools = state.pools.lock().await;
    let live_generation_pools = pools.len();
    let live_cells = pools.values().map(|pool| pool.cells.len()).sum();
    drop(pools);

    return Ok(Json(StatusResponse {
        runtime: "graalvm_polyglot",
        request_context_reusable: false,
        engine_cell_reusable: true,
        cell_reuse_scope: "same_tenant_generation",
        config_source: "flags-2-env+deployment-manifest",
        uptime_ms: state.started_at.elapsed().as_millis(),
        accepted: state.accepted.load(Ordering::Relaxed),
        completed: state.completed.load(Ordering::Relaxed),
        failed: state.failed.load(Ordering::Relaxed),
        live_cells,
        live_generation_pools,
        max_cell_invocations: state.max_cell_invocations,
        max_live_cells: state.max_live_cells,
        available_cell_slots: state.cell_slots.available_permits(),
        tenant_isolate_pool_size: state.pool_size,
    }));
}

async fn list_cells(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<CellStatus>>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let pools = {
        let pools = state.pools.lock().await;
        pools
            .iter()
            .map(|(key, pool)| (key.clone(), pool.clone()))
            .collect::<Vec<_>>()
    };

    let mut statuses = Vec::new();
    for (key, pool) in pools {
        for (cell_index, handle) in pool.cells.iter().enumerate() {
            let busy = handle.slot.available_permits() == 0;
            let mut cell = handle.cell.lock().await;
            let running = cell.child.try_wait().map_err(internal_error)?.is_none();
            statuses.push(CellStatus {
                tenant_id: key.tenant_id.clone(),
                deployment_id: key.deployment_id.clone(),
                invocation_count: cell.invocation_count,
                running,
                busy,
                cell_index,
            });
        }
    }
    return Ok(Json(statuses));
}

async fn retire_cell_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath((tenant_id, deployment_id)): AxumPath<(String, String)>,
) -> Result<Json<Value>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    validate_identifier("tenant_id", &tenant_id)?;
    validate_identifier("deployment_id", &deployment_id)?;
    let key = CellKey {
        tenant_id,
        deployment_id,
    };
    let retired = retire_pool(&state, &key).await;
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

    let timeout_ms = request.timeout_ms.unwrap_or(30_000);
    if timeout_ms == 0 || timeout_ms > MAX_TIMEOUT_MS {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("timeout_ms must be between 1 and {MAX_TIMEOUT_MS}"),
        ));
    }

    state.accepted.fetch_add(1, Ordering::Relaxed);
    let key = CellKey {
        tenant_id: request.tenant_id.clone(),
        deployment_id: request.deployment_id.clone(),
    };

    let result = async {
        let pool = ensure_pool(&state, &key).await?;
        let (handle, permit) = acquire_cell(&pool)?;
        let result = invoke_cell(&handle.cell, &request, Duration::from_millis(timeout_ms)).await;
        drop(permit);
        return result;
    }
    .await;

    state.completed.fetch_add(1, Ordering::Relaxed);
    if result.is_err() {
        state.failed.fetch_add(1, Ordering::Relaxed);
    }

    let retire = match &result {
        Ok(_) => {
            let pool = state.pools.lock().await.get(&key).cloned();
            if let Some(pool) = pool {
                cell_limit_reached(&pool, state.max_cell_invocations).await
            } else {
                false
            }
        }
        Err(_) => true,
    };
    if retire {
        let _ = retire_pool(&state, &key).await;
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

fn acquire_cell(pool: &Arc<CellPool>) -> Result<(Arc<CellHandle>, OwnedSemaphorePermit)> {
    let len = pool.cells.len();
    if len == 0 {
        bail!("tenant generation pool has no cells");
    }

    let start = (pool.next.fetch_add(1, Ordering::Relaxed) as usize) % len;
    for offset in 0..len {
        let index = (start + offset) % len;
        let handle = pool.cells[index].clone();
        if let Ok(permit) = handle.slot.clone().try_acquire_owned() {
            return Ok((handle, permit));
        }
    }

    bail!("tenant generation concurrency is exhausted");
}

async fn ensure_pool(state: &AppState, key: &CellKey) -> Result<Arc<CellPool>> {
    if let Some(pool) = state.pools.lock().await.get(key).cloned() {
        return Ok(pool);
    }

    let candidate = spawn_pool(state, key).await?;
    let mut pools = state.pools.lock().await;
    if let Some(existing) = pools.get(key).cloned() {
        drop(pools);
        terminate_pool(&candidate).await;
        return Ok(existing);
    }
    pools.insert(key.clone(), candidate.clone());
    return Ok(candidate);
}

async fn spawn_pool(state: &AppState, key: &CellKey) -> Result<Arc<CellPool>> {
    let mut cells = Vec::with_capacity(state.pool_size);
    for _ in 0..state.pool_size {
        match spawn_cell(state, key).await {
            Ok(cell) => cells.push(Arc::new(CellHandle {
                cell: Mutex::new(cell),
                slot: Arc::new(Semaphore::new(1)),
            })),
            Err(error) => {
                let pool = Arc::new(CellPool {
                    cells,
                    next: AtomicU64::new(0),
                });
                terminate_pool(&pool).await;
                return Err(error);
            }
        }
    }

    return Ok(Arc::new(CellPool {
        cells,
        next: AtomicU64::new(0),
    }));
}

async fn spawn_cell(state: &AppState, key: &CellKey) -> Result<Cell> {
    let deployment_root = deployment_path(&state.artifact_root, key)?;
    let artifact = secure_regular_file(&deployment_root.join("gs-lambda-cell.jar"))?;
    let _manifest = secure_regular_file(&deployment_root.join("manifest.json"))?;
    let live_permit = state.cell_slots.clone().try_acquire_owned().map_err(|_| {
        anyhow!("live JVM cell limit reached; retire an idle generation before cold start")
    })?;

    let mut child = Command::new(state.java_command.as_ref())
        .arg("-cp")
        .arg(&artifact)
        .arg(state.main_class.as_ref())
        .env("GS_TENANT_ID", &key.tenant_id)
        .env("GS_DEPLOYMENT_ID", &key.deployment_id)
        .env("GS_DEPLOYMENT_ROOT", &deployment_root)
        .env(
            "GS_MAX_CELL_INVOCATIONS",
            state.max_cell_invocations.to_string(),
        )
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start {}", state.java_command))?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("JVM cell stdin unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("JVM cell stdout unavailable"))?;
    return Ok(Cell {
        child,
        stdin,
        stdout: BufReader::new(stdout),
        invocation_count: 0,
        _live_permit: live_permit,
    });
}

async fn invoke_cell(
    cell: &Mutex<Cell>,
    request: &InvocationRequest,
    deadline: Duration,
) -> Result<Value> {
    let mut cell = cell.lock().await;
    if cell.child.try_wait()?.is_some() {
        bail!("JVM isolate cell exited before invocation");
    }

    let envelope = json!({
        "invocation_id": request.invocation_id,
        "tenant_id": request.tenant_id,
        "deployment_id": request.deployment_id,
        "payload": request.payload_json,
    });
    let mut line = serde_json::to_vec(&envelope)?;
    line.push(b'\n');
    cell.stdin.write_all(&line).await?;
    cell.stdin.flush().await?;

    let response_line = timeout(
        deadline,
        read_bounded_line(&mut cell.stdout, MAX_CELL_RESPONSE_BYTES),
    )
    .await
    .map_err(|_| anyhow!("logical invocation timed out; isolate generation will be retired"))??;
    if response_line.is_empty() {
        bail!("JVM isolate cell closed its output");
    }

    let response: Value =
        serde_json::from_slice(&response_line).context("JVM isolate cell returned invalid JSON")?;
    if response.get("ok").and_then(Value::as_bool) != Some(true) {
        let message = response
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("JVM isolate invocation failed");
        bail!("{message}");
    }

    cell.invocation_count = cell.invocation_count.saturating_add(1);
    return Ok(response.get("payload").cloned().unwrap_or(Value::Null));
}

async fn read_bounded_line(
    reader: &mut BufReader<ChildStdout>,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    loop {
        let (chunk, consumed, found_newline) = {
            let buffer = reader.fill_buf().await?;
            if buffer.is_empty() {
                if output.is_empty() {
                    bail!("JVM cell closed its output");
                }
                return Ok(output);
            }
            let newline = buffer.iter().position(|byte| *byte == b'\n');
            let consumed = newline.map(|index| index + 1).unwrap_or(buffer.len());
            if output.len().saturating_add(consumed) > max_bytes {
                bail!("JVM cell response exceeds {max_bytes} bytes");
            }
            (buffer[..consumed].to_vec(), consumed, newline.is_some())
        };
        output.extend_from_slice(&chunk);
        reader.consume(consumed);
        if found_newline {
            if output.last() == Some(&b'\n') {
                output.pop();
            }
            if output.last() == Some(&b'\r') {
                output.pop();
            }
            return Ok(output);
        }
    }
}

async fn cell_limit_reached(pool: &Arc<CellPool>, max_invocations: u64) -> bool {
    for handle in &pool.cells {
        let cell = handle.cell.lock().await;
        if cell.invocation_count >= max_invocations {
            return true;
        }
    }
    return false;
}

async fn retire_pool(state: &AppState, key: &CellKey) -> bool {
    let pool = state.pools.lock().await.remove(key);
    if let Some(pool) = pool {
        terminate_pool(&pool).await;
        return true;
    }
    return false;
}

async fn terminate_pool(pool: &Arc<CellPool>) {
    for handle in &pool.cells {
        let mut cell = handle.cell.lock().await;
        let _ = cell.child.kill().await;
        let _ = cell.child.wait().await;
    }
}

async fn terminate_all_cells(state: &AppState) {
    let pools = {
        let mut map = state.pools.lock().await;
        map.drain().map(|(_, pool)| pool).collect::<Vec<_>>()
    };
    for pool in pools {
        terminate_pool(&pool).await;
    }
}

fn deployment_path(root: &Path, key: &CellKey) -> Result<PathBuf> {
    validate_path_component(&key.tenant_id)?;
    validate_path_component(&key.deployment_id)?;
    return Ok(root.join(&key.tenant_id).join(&key.deployment_id));
}

fn artifact_path(root: &Path, key: &CellKey, filename: &str) -> Result<PathBuf> {
    return Ok(deployment_path(root, key)?.join(filename));
}

fn secure_regular_file(path: &Path) -> Result<PathBuf> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("cannot inspect deployment artifact {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("deployment artifact must be a regular non-symlink file");
    }
    return Ok(path.to_path_buf());
}

fn validate_path_component(value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && value != "."
        && value != "..";
    if !valid {
        bail!("invalid artifact path component");
    }
    return Ok(());
}

fn authorize(headers: &HeaderMap, state: &AppState) -> Result<(), (StatusCode, String)> {
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if provided == Some(state.token.as_ref()) {
        return Ok(());
    }
    return Err((StatusCode::UNAUTHORIZED, "unauthorized".to_owned()));
}

fn validate_identifier(name: &str, value: &str) -> Result<(), (StatusCode, String)> {
    if validate_path_component(value).is_ok() {
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

fn internal_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
}

fn service_unavailable(error: impl std::fmt::Display) -> (StatusCode, String) {
    let message = error.to_string();
    if message.contains("limit reached") || message.contains("concurrency is exhausted") {
        return (StatusCode::SERVICE_UNAVAILABLE, message);
    }
    return internal_error(message);
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
        assert!(validate_path_component("deployment-1").is_ok());
        assert!(validate_path_component("..").is_err());
        assert!(validate_path_component("tenant/escape").is_err());
    }

    #[test]
    fn artifact_path_is_tenant_and_generation_scoped() {
        let root = PathBuf::from("/tmp/graal-show");
        let key = CellKey {
            tenant_id: "tenant-a".to_owned(),
            deployment_id: "deploy-123".to_owned(),
        };
        let valid = artifact_path(&root, &key, "gs-lambda-cell.jar")
            .map(|path| path.ends_with("tenant-a/deploy-123/gs-lambda-cell.jar"))
            .unwrap_or(false);
        assert!(valid);
    }

    #[tokio::test]
    async fn cell_slot_limit_is_strict() {
        let slots = Arc::new(Semaphore::new(1));
        let first = slots.clone().try_acquire_owned();
        assert!(first.is_ok());
        assert!(slots.clone().try_acquire_owned().is_err());
        drop(first);
        assert!(slots.try_acquire_owned().is_ok());
    }
}
