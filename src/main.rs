use anyhow::{Context as _, Result, anyhow, bail};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
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
    sync::Mutex,
    time::timeout,
};
use uuid::Uuid;

const DEFAULT_ADDR: &str = "127.0.0.1:8764";
const DEFAULT_JAVA: &str = "java";
const DEFAULT_MAIN_CLASS: &str = "dev.graalshow.lambda.CellMain";
const MAX_TIMEOUT_MS: u64 = 20 * 60 * 1_000;

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
}

#[derive(Clone)]
struct AppState {
    token: Arc<str>,
    java_command: Arc<str>,
    main_class: Arc<str>,
    artifact_root: Arc<PathBuf>,
    max_cell_invocations: u64,
    cells: Arc<Mutex<HashMap<CellKey, Arc<Mutex<Cell>>>>>,
    started_at: Instant,
    accepted: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
}

#[derive(Debug, Deserialize)]
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
    actor_reusable: bool,
    cell_reuse_scope: &'static str,
    uptime_ms: u128,
    accepted: u64,
    completed: u64,
    live_cells: usize,
    max_cell_invocations: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "gs_desktop_daemon=info".into()),
        )
        .init();

    let addr = parse_loopback_addr(env::var("GS_DESKTOP_ADDR").as_deref().unwrap_or(DEFAULT_ADDR))?;
    let token = load_or_create_token(&token_path()?)?;
    let java_command = env::var("GS_JAVA_COMMAND").unwrap_or_else(|_| DEFAULT_JAVA.to_owned());
    let main_class = env::var("GS_CELL_MAIN_CLASS").unwrap_or_else(|_| DEFAULT_MAIN_CLASS.to_owned());
    let artifact_root = artifact_root()?;
    let max_cell_invocations = positive_u64_env("GS_MAX_CELL_INVOCATIONS", 1_000)?;

    let state = AppState {
        token: Arc::from(token),
        java_command: Arc::from(java_command),
        main_class: Arc::from(main_class),
        artifact_root: Arc::new(artifact_root),
        max_cell_invocations,
        cells: Arc::new(Mutex::new(HashMap::new())),
        started_at: Instant::now(),
        accepted: Arc::new(AtomicU64::new(0)),
        completed: Arc::new(AtomicU64::new(0)),
    };

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/status", get(status))
        .route("/v1/invoke", post(invoke))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "graal-show desktop daemon listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    terminate_all_cells(&state).await;
    return Ok(());
}

async fn health() -> &'static str {
    return "ok";
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let live_cells = state.cells.lock().await.len();
    return Ok(Json(StatusResponse {
        runtime: "graalvm_jvm",
        actor_reusable: false,
        cell_reuse_scope: "same_tenant_generation",
        uptime_ms: state.started_at.elapsed().as_millis(),
        accepted: state.accepted.load(Ordering::Relaxed),
        completed: state.completed.load(Ordering::Relaxed),
        live_cells,
        max_cell_invocations: state.max_cell_invocations,
    }));
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
    let cell = ensure_cell(&state, &key).await.map_err(internal_error)?;
    let result = invoke_cell(&cell, &request, Duration::from_millis(timeout_ms)).await;
    state.completed.fetch_add(1, Ordering::Relaxed);

    let retire = match &result {
        Ok(_) => {
            let guard = cell.lock().await;
            guard.invocation_count >= state.max_cell_invocations
        }
        Err(_) => true,
    };
    if retire {
        retire_cell(&state, &key).await;
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

async fn ensure_cell(state: &AppState, key: &CellKey) -> Result<Arc<Mutex<Cell>>> {
    if let Some(cell) = state.cells.lock().await.get(key).cloned() {
        return Ok(cell);
    }

    let artifact = artifact_path(&state.artifact_root, key, "gs-lambda-cell.jar")?;
    if !artifact.is_file() {
        bail!("JVM cell artifact is missing for the requested deployment");
    }

    let mut child = Command::new(state.java_command.as_ref())
        .arg("-cp")
        .arg(&artifact)
        .arg(state.main_class.as_ref())
        .env("GS_TENANT_ID", &key.tenant_id)
        .env("GS_DEPLOYMENT_ID", &key.deployment_id)
        .env("GS_MAX_CELL_INVOCATIONS", state.max_cell_invocations.to_string())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to start {}", state.java_command))?;

    let stdin = child.stdin.take().ok_or_else(|| anyhow!("JVM cell stdin unavailable"))?;
    let stdout = child.stdout.take().ok_or_else(|| anyhow!("JVM cell stdout unavailable"))?;
    let cell = Arc::new(Mutex::new(Cell {
        child,
        stdin,
        stdout: BufReader::new(stdout),
        invocation_count: 0,
    }));

    let mut cells = state.cells.lock().await;
    if let Some(existing) = cells.get(key).cloned() {
        drop(cells);
        let mut unused = cell.lock().await;
        let _ = unused.child.kill().await;
        return Ok(existing);
    }
    cells.insert(key.clone(), cell.clone());
    return Ok(cell);
}

async fn invoke_cell(
    cell: &Arc<Mutex<Cell>>,
    request: &InvocationRequest,
    deadline: Duration,
) -> Result<Value> {
    let mut cell = cell.lock().await;
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

    let mut response_line = String::new();
    let bytes_read = timeout(deadline, cell.stdout.read_line(&mut response_line))
        .await
        .map_err(|_| anyhow!("invocation timed out"))??;
    if bytes_read == 0 {
        bail!("JVM cell closed its output");
    }

    let response: Value = serde_json::from_str(response_line.trim())
        .context("JVM cell returned invalid JSON")?;
    if response.get("ok").and_then(Value::as_bool) != Some(true) {
        let message = response
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("JVM actor invocation failed");
        bail!("{message}");
    }

    cell.invocation_count = cell.invocation_count.saturating_add(1);
    return Ok(response.get("payload").cloned().unwrap_or(Value::Null));
}

async fn retire_cell(state: &AppState, key: &CellKey) {
    let cell = state.cells.lock().await.remove(key);
    if let Some(cell) = cell {
        let mut cell = cell.lock().await;
        let _ = cell.child.kill().await;
    }
}

async fn terminate_all_cells(state: &AppState) {
    let cells = {
        let mut map = state.cells.lock().await;
        map.drain().map(|(_, cell)| cell).collect::<Vec<_>>()
    };
    for cell in cells {
        let mut cell = cell.lock().await;
        let _ = cell.child.kill().await;
    }
}

fn artifact_path(root: &Path, key: &CellKey, filename: &str) -> Result<PathBuf> {
    validate_path_component(&key.tenant_id)?;
    validate_path_component(&key.deployment_id)?;
    return Ok(root.join(&key.tenant_id).join(&key.deployment_id).join(filename));
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
    let addr: SocketAddr = value.parse().context("GS_DESKTOP_ADDR is not a socket address")?;
    if !is_loopback(addr.ip()) {
        bail!("GS_DESKTOP_ADDR must bind to loopback");
    }
    return Ok(addr);
}

fn is_loopback(ip: IpAddr) -> bool {
    return ip.is_loopback();
}

fn positive_u64_env(name: &str, default_value: u64) -> Result<u64> {
    let Some(raw) = env::var(name).ok() else {
        return Ok(default_value);
    };
    let value = raw.parse::<u64>().with_context(|| format!("{name} must be an integer"))?;
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    return Ok(value);
}

fn artifact_root() -> Result<PathBuf> {
    if let Ok(path) = env::var("GS_ARTIFACT_ROOT") {
        return expand_home(Path::new(&path));
    }
    let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is required"))?;
    return Ok(PathBuf::from(home).join(".graal-show/artifacts"));
}

fn token_path() -> Result<PathBuf> {
    if let Ok(path) = env::var("GS_DESKTOP_TOKEN_FILE") {
        return expand_home(Path::new(&path));
    }
    let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is required"))?;
    return Ok(PathBuf::from(home).join(".graal-show/daemon/token"));
}

fn expand_home(path: &Path) -> Result<PathBuf> {
    let text = path.to_string_lossy();
    if text == "~" || text.starts_with("~/") {
        let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is required"))?;
        let suffix = text.trim_start_matches('~').trim_start_matches('/');
        return Ok(PathBuf::from(home).join(suffix));
    }
    return Ok(path.to_path_buf());
}

fn load_or_create_token(path: &Path) -> Result<String> {
    if let Ok(token) = std::fs::read_to_string(path) {
        let token = token.trim();
        if token.len() >= 32 {
            return Ok(token.to_owned());
        }
        bail!("desktop daemon token file is too short");
    }

    let parent = path.parent().ok_or_else(|| anyhow!("token path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    std::fs::write(path, format!("{token}\n"))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }

    return Ok(token);
}

fn internal_error(error: impl std::fmt::Display) -> (StatusCode, String) {
    return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string());
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
