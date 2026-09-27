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
    sync::Mutex,
    time::timeout,
};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const MAX_TIMEOUT_MS: u64 = 20 * 60 * 1_000;
const MAX_BODY_BYTES: usize = 256 * 1024;
const MAX_CELL_INVOCATIONS: u64 = 1_000_000;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
struct CliConfig {
    GS_DESKTOP_ADDR: String,
    GS_JAVA_COMMAND: String,
    GS_CELL_MAIN_CLASS: String,
    GS_ARTIFACT_ROOT: Option<String>,
    GS_MAX_CELL_INVOCATIONS: i64,
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
    failed: Arc<AtomicU64>,
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
    logical_actor_reusable: bool,
    jvm_cell_reusable: bool,
    cell_reuse_scope: &'static str,
    config_source: &'static str,
    uptime_ms: u128,
    accepted: u64,
    completed: u64,
    failed: u64,
    live_cells: usize,
    max_cell_invocations: u64,
}

#[derive(Debug, Serialize)]
struct CellStatus {
    tenant_id: String,
    deployment_id: String,
    invocation_count: u64,
    running: bool,
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
        cells: Arc::new(Mutex::new(HashMap::new())),
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
    let live_cells = state.cells.lock().await.len();
    return Ok(Json(StatusResponse {
        runtime: "graalvm_jvm",
        logical_actor_reusable: false,
        jvm_cell_reusable: true,
        cell_reuse_scope: "same_tenant_generation",
        config_source: "flags-2-env",
        uptime_ms: state.started_at.elapsed().as_millis(),
        accepted: state.accepted.load(Ordering::Relaxed),
        completed: state.completed.load(Ordering::Relaxed),
        failed: state.failed.load(Ordering::Relaxed),
        live_cells,
        max_cell_invocations: state.max_cell_invocations,
    }));
}

async fn list_cells(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<CellStatus>>, (StatusCode, String)> {
    authorize(&headers, &state)?;
    let cells = {
        let cells = state.cells.lock().await;
        cells
            .iter()
            .map(|(key, cell)| (key.clone(), cell.clone()))
            .collect::<Vec<_>>()
    };

    let mut statuses = Vec::with_capacity(cells.len());
    for (key, cell) in cells {
        let mut cell = cell.lock().await;
        let running = cell.child.try_wait().map_err(internal_error)?.is_none();
        statuses.push(CellStatus {
            tenant_id: key.tenant_id,
            deployment_id: key.deployment_id,
            invocation_count: cell.invocation_count,
            running,
        });
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
    let retired = retire_cell(&state, &key).await;
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
    let cell = ensure_cell(&state, &key).await.map_err(internal_error)?;
    let result = invoke_cell(&cell, &request, Duration::from_millis(timeout_ms)).await;
    state.completed.fetch_add(1, Ordering::Relaxed);
    if result.is_err() {
        state.failed.fetch_add(1, Ordering::Relaxed);
    }

    let retire = match &result {
        Ok(_) => {
            let guard = cell.lock().await;
            guard.invocation_count >= state.max_cell_invocations
        }
        Err(_) => true,
    };
    if retire {
        let _ = retire_cell(&state, &key).await;
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
        let running = {
            let mut guard = cell.lock().await;
            guard.child.try_wait()?.is_none()
        };
        if running {
            return Ok(cell);
        }
        let _ = retire_cell(state, key).await;
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
        .map_err(|_| anyhow!("logical actor invocation timed out; JVM cell will be retired"))??;
    if bytes_read == 0 {
        bail!("JVM cell closed its output");
    }

    let response: Value =
        serde_json::from_str(response_line.trim()).context("JVM cell returned invalid JSON")?;
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

async fn retire_cell(state: &AppState, key: &CellKey) -> bool {
    let cell = state.cells.lock().await.remove(key);
    if let Some(cell) = cell {
        let mut cell = cell.lock().await;
        let _ = cell.child.kill().await;
        return true;
    }
    return false;
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
    return Ok(root
        .join(&key.tenant_id)
        .join(&key.deployment_id)
        .join(filename));
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

fn load_or_create_token(path: &Path) -> Result<String> {
    if let Ok(token) = std::fs::read_to_string(path) {
        let token = token.trim();
        if token.len() >= 32 && !token.chars().any(char::is_whitespace) {
            return Ok(token.to_owned());
        }
        bail!("desktop daemon token file is malformed");
    }

    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("token path has no parent"))?;
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
}
