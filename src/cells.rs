use anyhow::{Context as _, Result, anyhow, bail};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, oneshot},
    time::{interval, timeout},
};

const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

type WorkerResult = std::result::Result<Value, String>;
type PendingMap = HashMap<String, oneshot::Sender<WorkerResult>>;

#[derive(Clone, Debug)]
pub struct CellPoolConfig {
    pub java_command: String,
    pub main_class: String,
    pub artifact_root: PathBuf,
    pub max_live_cells: usize,
    pub max_cells_per_generation: usize,
    pub max_cell_concurrency: usize,
    pub max_cell_invocations: u64,
    pub cell_idle_ttl: Duration,
    pub max_cell_age: Duration,
    pub max_stateless_isolates: usize,
    pub max_contexts_per_isolate: usize,
    pub max_route_isolates: usize,
    pub max_session_isolates: usize,
    pub max_route_session_isolates: usize,
    pub session_isolate_ttl: Duration,
    pub route_isolate_ttl: Duration,
    pub max_isolate_memory: String,
    pub max_guest_heap_memory: String,
    pub max_guest_cpu_time_ms: u64,
    pub max_ast_depth: u32,
    pub max_guest_threads: u32,
    pub max_guest_stdout: String,
    pub max_guest_stderr: String,
    pub isolate_mode: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CellKey {
    tenant_id: String,
    deployment_id: String,
}

struct Cell {
    key: CellKey,
    artifact_sha256: String,
    index: u32,
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    pending: Arc<Mutex<PendingMap>>,
    capacity: Arc<Semaphore>,
    invocation_count: AtomicU64,
    active: AtomicU64,
    failed: Arc<AtomicBool>,
    draining: AtomicBool,
    started_at: Instant,
    last_used: Mutex<Instant>,
    _live_permit: OwnedSemaphorePermit,
}

struct Inner {
    config: CellPoolConfig,
    cell_slots: Arc<Semaphore>,
    cells: Mutex<HashMap<CellKey, Vec<Arc<Cell>>>>,
}

#[derive(Clone)]
pub struct CellPool {
    inner: Arc<Inner>,
}

#[derive(Debug, Serialize)]
pub struct CellStatus {
    pub tenant_id: String,
    pub deployment_id: String,
    pub artifact_sha256: String,
    pub cell_index: u32,
    pub invocation_count: u64,
    pub active_invocations: u64,
    pub available_invocation_slots: usize,
    pub draining: bool,
    pub failed: bool,
    pub age_ms: u128,
    pub idle_ms: u128,
}

impl CellPool {
    pub fn new(config: CellPoolConfig) -> Self {
        let max_live_cells = config.max_live_cells;
        return Self {
            inner: Arc::new(Inner {
                config,
                cell_slots: Arc::new(Semaphore::new(max_live_cells)),
                cells: Mutex::new(HashMap::new()),
            }),
        };
    }

    pub fn start_reaper(&self) {
        let pool = self.clone();
        tokio::spawn(async move {
            pool.reaper_loop().await;
        });
    }

    pub fn max_cells_per_generation(&self) -> usize {
        return self.inner.config.max_cells_per_generation;
    }

    pub fn max_cell_concurrency(&self) -> usize {
        return self.inner.config.max_cell_concurrency;
    }

    pub fn max_cell_invocations(&self) -> u64 {
        return self.inner.config.max_cell_invocations;
    }

    pub fn max_stateless_isolates(&self) -> usize {
        return self.inner.config.max_stateless_isolates;
    }

    pub fn max_route_isolates(&self) -> usize {
        return self.inner.config.max_route_isolates;
    }

    pub fn max_session_isolates(&self) -> usize {
        return self.inner.config.max_session_isolates;
    }

    pub fn max_route_session_isolates(&self) -> usize {
        return self.inner.config.max_route_session_isolates;
    }

    pub fn session_isolate_ttl_ms(&self) -> u128 {
        return self.inner.config.session_isolate_ttl.as_millis();
    }

    pub fn route_isolate_ttl_ms(&self) -> u128 {
        return self.inner.config.route_isolate_ttl.as_millis();
    }

    pub fn available_cell_slots(&self) -> usize {
        return self.inner.cell_slots.available_permits();
    }

    pub async fn live_cells(&self) -> usize {
        return self.inner.cells.lock().await.values().map(Vec::len).sum();
    }

    pub async fn statuses(&self) -> Vec<CellStatus> {
        let cells = self
            .inner
            .cells
            .lock()
            .await
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        let mut statuses = Vec::with_capacity(cells.len());
        for cell in cells {
            let idle_ms = cell.last_used.lock().await.elapsed().as_millis();
            statuses.push(CellStatus {
                tenant_id: cell.key.tenant_id.clone(),
                deployment_id: cell.key.deployment_id.clone(),
                artifact_sha256: cell.artifact_sha256.clone(),
                cell_index: cell.index,
                invocation_count: cell.invocation_count.load(Ordering::Relaxed),
                active_invocations: cell.active.load(Ordering::Relaxed),
                available_invocation_slots: cell.capacity.available_permits(),
                draining: cell.draining.load(Ordering::Acquire),
                failed: cell.failed.load(Ordering::Acquire),
                age_ms: cell.started_at.elapsed().as_millis(),
                idle_ms,
            });
        }
        return statuses;
    }

    pub async fn invoke(
        &self,
        tenant_id: &str,
        deployment_id: &str,
        invocation_id: &str,
        route_id: Option<&str>,
        session_id: Option<&str>,
        affinity: &str,
        payload: &Value,
        timeout_ms: u64,
    ) -> Result<Value> {
        let key = CellKey {
            tenant_id: tenant_id.to_owned(),
            deployment_id: deployment_id.to_owned(),
        };
        let (cell, _permit) = self.lease_cell(&key).await?;
        let result = invoke_cell(
            &cell,
            invocation_id,
            route_id,
            session_id,
            affinity,
            payload,
            timeout_ms,
            Duration::from_millis(timeout_ms),
        )
        .await;

        if result.is_err() {
            cell.failed.store(true, Ordering::Release);
            let _ = self
                .retire(&key.tenant_id, &key.deployment_id, cell.index)
                .await;
        } else if cell.invocation_count.load(Ordering::Relaxed)
            >= self.inner.config.max_cell_invocations
        {
            cell.draining.store(true, Ordering::Release);
        }

        if cell.draining.load(Ordering::Acquire) && cell.active.load(Ordering::Acquire) == 0 {
            let _ = self
                .retire(&key.tenant_id, &key.deployment_id, cell.index)
                .await;
        }

        return result;
    }

    pub async fn drain(&self, tenant_id: &str, deployment_id: &str, index: u32) -> bool {
        let key = CellKey {
            tenant_id: tenant_id.to_owned(),
            deployment_id: deployment_id.to_owned(),
        };
        let cell = {
            let cells = self.inner.cells.lock().await;
            cells
                .get(&key)
                .and_then(|group| group.iter().find(|cell| cell.index == index))
                .cloned()
        };
        let Some(cell) = cell else {
            return false;
        };
        cell.draining.store(true, Ordering::Release);
        if cell.active.load(Ordering::Acquire) == 0 {
            return self.retire(tenant_id, deployment_id, index).await;
        }
        return true;
    }

    pub async fn retire(&self, tenant_id: &str, deployment_id: &str, index: u32) -> bool {
        let key = CellKey {
            tenant_id: tenant_id.to_owned(),
            deployment_id: deployment_id.to_owned(),
        };
        let cell = {
            let mut cells = self.inner.cells.lock().await;
            let Some(group) = cells.get_mut(&key) else {
                return false;
            };
            let Some(position) = group.iter().position(|cell| cell.index == index) else {
                return false;
            };
            let cell = group.remove(position);
            if group.is_empty() {
                cells.remove(&key);
            }
            cell
        };

        cell.draining.store(true, Ordering::Release);
        fail_all_pending(&cell.pending, "Graal execution cell retired").await;
        let mut child = cell.child.lock().await;
        let _ = child.kill().await;
        let _ = child.wait().await;
        return true;
    }

    pub async fn shutdown(&self) {
        let cells = {
            let mut guard = self.inner.cells.lock().await;
            let cells = guard.values().flatten().cloned().collect::<Vec<_>>();
            guard.clear();
            cells
        };
        for cell in cells {
            cell.draining.store(true, Ordering::Release);
            fail_all_pending(&cell.pending, "Graal desktop daemon shutting down").await;
            let mut child = cell.child.lock().await;
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }

    async fn lease_cell(&self, key: &CellKey) -> Result<(Arc<Cell>, OwnedSemaphorePermit)> {
        let artifact_sha256 = generation_sha256(&self.inner.config.artifact_root, key)?;
        let mut cells = self.inner.cells.lock().await;
        let group = cells.entry(key.clone()).or_default();
        group.retain(|cell| !cell.failed.load(Ordering::Acquire));

        for cell in group.iter() {
            if cell.artifact_sha256 != artifact_sha256 {
                cell.draining.store(true, Ordering::Release);
            }
        }

        for cell in group.iter() {
            if cell.draining.load(Ordering::Acquire) {
                continue;
            }
            if cell.invocation_count.load(Ordering::Relaxed)
                >= self.inner.config.max_cell_invocations
            {
                cell.draining.store(true, Ordering::Release);
                continue;
            }
            if let Ok(permit) = cell.capacity.clone().try_acquire_owned() {
                return Ok((cell.clone(), permit));
            }
        }

        let current_generation_cells = group
            .iter()
            .filter(|cell| {
                !cell.failed.load(Ordering::Acquire) && cell.artifact_sha256 == artifact_sha256
            })
            .count();
        if current_generation_cells >= self.inner.config.max_cells_per_generation {
            bail!("all warm Graal cells for this immutable tenant generation are busy");
        }

        let index = next_cell_index(group);
        let cell = self.spawn_cell(key.clone(), index, &artifact_sha256)?;
        let permit = cell
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow!("new Graal cell had no invocation capacity"))?;
        group.push(cell.clone());
        return Ok((cell, permit));
    }

    fn spawn_cell(&self, key: CellKey, index: u32, artifact_sha256: &str) -> Result<Arc<Cell>> {
        let live_permit = self
            .inner
            .cell_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow!("live Graal cell limit reached"))?;
        let artifact_dir = artifact_dir(&self.inner.config.artifact_root, &key)?;
        let jar = artifact_dir.join("gs-lambda-cell.jar");
        require_regular_file(&jar, "Graal cell JAR")?;
        require_regular_file(
            &artifact_dir.join("manifest.json"),
            "Graal deployment manifest",
        )?;
        let actual_sha256 = sha256_file(&jar)?;
        if actual_sha256 != artifact_sha256 {
            bail!("Graal deployment artifact changed while preparing a warm cell");
        }

        let mut child = Command::new(&self.inner.config.java_command)
            .arg("-cp")
            .arg(&jar)
            .arg(&self.inner.config.main_class)
            .env("GS_TENANT_ID", &key.tenant_id)
            .env("GS_DEPLOYMENT_ID", &key.deployment_id)
            .env("GS_CELL_INDEX", index.to_string())
            .env("GS_ARTIFACT_DIR", &artifact_dir)
            .env("GS_ARTIFACT_SHA256", artifact_sha256)
            .env(
                "GS_MAX_CELL_INVOCATIONS",
                self.inner.config.max_cell_invocations.to_string(),
            )
            .env(
                "GS_MAX_CELL_CONCURRENCY",
                self.inner.config.max_cell_concurrency.to_string(),
            )
            .env(
                "GS_MAX_STATELESS_ISOLATES",
                self.inner.config.max_stateless_isolates.to_string(),
            )
            .env(
                "GS_MAX_CONTEXTS_PER_ISOLATE",
                self.inner.config.max_contexts_per_isolate.to_string(),
            )
            .env(
                "GS_MAX_ROUTE_ISOLATES",
                self.inner.config.max_route_isolates.to_string(),
            )
            .env(
                "GS_MAX_SESSION_ISOLATES",
                self.inner.config.max_session_isolates.to_string(),
            )
            .env(
                "GS_MAX_ROUTE_SESSION_ISOLATES",
                self.inner.config.max_route_session_isolates.to_string(),
            )
            .env(
                "GS_SESSION_ISOLATE_TTL_MS",
                self.inner.config.session_isolate_ttl.as_millis().to_string(),
            )
            .env(
                "GS_ROUTE_ISOLATE_TTL_MS",
                self.inner.config.route_isolate_ttl.as_millis().to_string(),
            )
            .env(
                "GS_MAX_ISOLATE_MEMORY",
                &self.inner.config.max_isolate_memory,
            )
            .env(
                "GS_MAX_GUEST_HEAP_MEMORY",
                &self.inner.config.max_guest_heap_memory,
            )
            .env(
                "GS_MAX_GUEST_CPU_TIME_MS",
                self.inner.config.max_guest_cpu_time_ms.to_string(),
            )
            .env(
                "GS_MAX_AST_DEPTH",
                self.inner.config.max_ast_depth.to_string(),
            )
            .env(
                "GS_MAX_GUEST_THREADS",
                self.inner.config.max_guest_threads.to_string(),
            )
            .env("GS_MAX_GUEST_STDOUT", &self.inner.config.max_guest_stdout)
            .env("GS_MAX_GUEST_STDERR", &self.inner.config.max_guest_stderr)
            .env("GS_ISOLATE_MODE", &self.inner.config.isolate_mode)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| {
                format!(
                    "failed to start Graal tenant cell with {}",
                    self.inner.config.java_command
                )
            })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("Graal cell stdin unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow!("Graal cell stdout unavailable"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow!("Graal cell stderr unavailable"))?;
        let pending = Arc::new(Mutex::new(PendingMap::new()));
        let failed = Arc::new(AtomicBool::new(false));

        tokio::spawn(read_worker_stdout(
            key.clone(),
            index,
            stdout,
            pending.clone(),
            failed.clone(),
        ));
        tokio::spawn(read_worker_stderr(key.clone(), index, stderr));

        return Ok(Arc::new(Cell {
            key,
            artifact_sha256: artifact_sha256.to_owned(),
            index,
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            pending,
            capacity: Arc::new(Semaphore::new(self.inner.config.max_cell_concurrency)),
            invocation_count: AtomicU64::new(0),
            active: AtomicU64::new(0),
            failed,
            draining: AtomicBool::new(false),
            started_at: Instant::now(),
            last_used: Mutex::new(Instant::now()),
            _live_permit: live_permit,
        }));
    }

    async fn reaper_loop(self) {
        let mut ticker = interval(CLEANUP_INTERVAL);
        loop {
            ticker.tick().await;
            let cells = self
                .inner
                .cells
                .lock()
                .await
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<_>>();
            for cell in cells {
                if cell.active.load(Ordering::Acquire) != 0 {
                    continue;
                }
                let idle = cell.last_used.lock().await.elapsed();
                let should_retire = cell.failed.load(Ordering::Acquire)
                    || cell.draining.load(Ordering::Acquire)
                    || idle >= self.inner.config.cell_idle_ttl
                    || cell.started_at.elapsed() >= self.inner.config.max_cell_age;
                if should_retire {
                    let _ = self
                        .retire(&cell.key.tenant_id, &cell.key.deployment_id, cell.index)
                        .await;
                }
            }
        }
    }
}

fn next_cell_index(cells: &[Arc<Cell>]) -> u32 {
    let mut index = 0_u32;
    loop {
        if cells.iter().all(|cell| cell.index != index) {
            return index;
        }
        index = index.saturating_add(1);
    }
}

fn artifact_dir(root: &Path, key: &CellKey) -> Result<PathBuf> {
    validate_path_component(&key.tenant_id)?;
    validate_path_component(&key.deployment_id)?;
    return Ok(root.join(&key.tenant_id).join(&key.deployment_id));
}

fn generation_sha256(root: &Path, key: &CellKey) -> Result<String> {
    let artifact_dir = artifact_dir(root, key)?;
    let jar = artifact_dir.join("gs-lambda-cell.jar");
    require_regular_file(&jar, "Graal cell JAR")?;
    require_regular_file(
        &artifact_dir.join("manifest.json"),
        "Graal deployment manifest",
    )?;
    return sha256_file(&jar);
}

fn sha256_file(path: &Path) -> Result<String> {
    let file = fs::File::open(path)
        .with_context(|| format!("cannot open deployment artifact {}", path.display()))?;
    return sha256_reader(file);
}

fn sha256_reader<R: Read>(mut reader: R) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    return Ok(format!("{:x}", hasher.finalize()));
}

fn require_regular_file(path: &Path, description: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("cannot inspect {description} {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("{description} must be a regular non-symlink file");
    }
    return Ok(());
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

async fn invoke_cell(
    cell: &Arc<Cell>,
    invocation_id: &str,
    route_id: Option<&str>,
    session_id: Option<&str>,
    affinity: &str,
    payload: &Value,
    timeout_ms: u64,
    deadline: Duration,
) -> Result<Value> {
    if cell.failed.load(Ordering::Acquire) || cell.draining.load(Ordering::Acquire) {
        bail!("Graal cell is not accepting new invocations");
    }

    let (sender, receiver) = oneshot::channel();
    {
        let mut pending = cell.pending.lock().await;
        if pending.contains_key(invocation_id) {
            bail!("duplicate invocation_id in Graal cell");
        }
        pending.insert(invocation_id.to_owned(), sender);
    }

    cell.active.fetch_add(1, Ordering::AcqRel);
    cell.invocation_count.fetch_add(1, Ordering::Relaxed);
    *cell.last_used.lock().await = Instant::now();

    let frame = json!({
        "frame_type": "invoke",
        "invocation_id": invocation_id,
        "tenant_id": cell.key.tenant_id,
        "deployment_id": cell.key.deployment_id,
        "route_id": route_id,
        "session_id": session_id,
        "affinity": affinity,
        "timeout_ms": timeout_ms,
        "payload": payload,
    });
    let encoded = serde_json::to_vec(&frame)?;
    if encoded.is_empty() || encoded.len() > MAX_FRAME_BYTES {
        cell.pending.lock().await.remove(invocation_id);
        cell.active.fetch_sub(1, Ordering::AcqRel);
        bail!("Graal invocation frame exceeded limit");
    }

    let write_result = async {
        let mut stdin = cell.stdin.lock().await;
        let length = u32::try_from(encoded.len())
            .map_err(|_| anyhow!("Graal invocation frame length overflow"))?;
        stdin.write_all(&length.to_be_bytes()).await?;
        stdin.write_all(&encoded).await?;
        stdin.flush().await?;
        return Ok::<_, anyhow::Error>(());
    }
    .await;
    if let Err(error) = write_result {
        cell.pending.lock().await.remove(invocation_id);
        cell.active.fetch_sub(1, Ordering::AcqRel);
        return Err(error).context("failed to write Graal invocation frame");
    }

    let received = timeout(deadline, receiver).await;
    cell.active.fetch_sub(1, Ordering::AcqRel);
    *cell.last_used.lock().await = Instant::now();

    match received {
        Ok(Ok(Ok(payload))) => return Ok(payload),
        Ok(Ok(Err(message))) => bail!(message),
        Ok(Err(_)) => bail!("Graal cell response channel closed"),
        Err(_) => {
            cell.pending.lock().await.remove(invocation_id);
            bail!("Graal invocation timed out; cell will be retired");
        }
    }
}

async fn read_worker_stdout(
    key: CellKey,
    index: u32,
    mut stdout: tokio::process::ChildStdout,
    pending: Arc<Mutex<PendingMap>>,
    failed: Arc<AtomicBool>,
) {
    loop {
        let mut header = [0_u8; 4];
        if let Err(error) = stdout.read_exact(&mut header).await {
            failed.store(true, Ordering::Release);
            fail_all_pending(&pending, &format!("Graal cell stdout closed: {error}")).await;
            return;
        }
        let length = u32::from_be_bytes(header) as usize;
        if length == 0 || length > MAX_FRAME_BYTES {
            failed.store(true, Ordering::Release);
            fail_all_pending(&pending, "Graal cell response frame length is invalid").await;
            return;
        }
        let mut bytes = vec![0_u8; length];
        if let Err(error) = stdout.read_exact(&mut bytes).await {
            failed.store(true, Ordering::Release);
            fail_all_pending(
                &pending,
                &format!("Graal cell response was truncated: {error}"),
            )
            .await;
            return;
        }
        let frame = match serde_json::from_slice::<Value>(&bytes) {
            Ok(frame) => frame,
            Err(error) => {
                failed.store(true, Ordering::Release);
                fail_all_pending(
                    &pending,
                    &format!("Graal cell returned invalid JSON: {error}"),
                )
                .await;
                return;
            }
        };
        let invocation_id = match frame.get("invocation_id").and_then(Value::as_str) {
            Some(value) => value.to_owned(),
            None => {
                failed.store(true, Ordering::Release);
                fail_all_pending(&pending, "Graal cell response omitted invocation_id").await;
                return;
            }
        };
        if let Some(sender) = pending.lock().await.remove(&invocation_id) {
            let _ = sender.send(decode_worker_frame(frame));
        } else {
            tracing::warn!(
                tenant_id = %key.tenant_id,
                deployment_id = %key.deployment_id,
                cell_index = index,
                %invocation_id,
                "discarding response for unknown Graal invocation"
            );
        }
    }
}

async fn read_worker_stderr(key: CellKey, index: u32, stderr: tokio::process::ChildStderr) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::warn!(
            tenant_id = %key.tenant_id,
            deployment_id = %key.deployment_id,
            cell_index = index,
            worker_stderr = %truncate(&line, 2048),
            "Graal cell stderr"
        );
    }
}

fn decode_worker_frame(frame: Value) -> WorkerResult {
    if frame.get("ok").and_then(Value::as_bool) == Some(false) {
        return Err(frame
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("Graal invocation failed")
            .to_owned());
    }
    return Ok(frame.get("payload").cloned().unwrap_or(Value::Null));
}

async fn fail_all_pending(pending: &Arc<Mutex<PendingMap>>, message: &str) {
    let senders = pending
        .lock()
        .await
        .drain()
        .map(|(_, sender)| sender)
        .collect::<Vec<_>>();
    for sender in senders {
        let _ = sender.send(Err(message.to_owned()));
    }
}

fn truncate(value: &str, max_chars: usize) -> String {
    return value.chars().take(max_chars).collect();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_frame_extracts_payload() {
        let frame = json!({
            "invocation_id": "inv-1",
            "ok": true,
            "payload": {"hello": "world"}
        });
        let result = decode_worker_frame(frame);
        assert_eq!(result.ok(), Some(json!({"hello": "world"})));
    }

    #[test]
    fn response_frame_propagates_guest_error() {
        let frame = json!({
            "invocation_id": "inv-1",
            "ok": false,
            "error": "boom"
        });
        assert!(decode_worker_frame(frame).is_err());
    }

    #[test]
    fn path_components_reject_traversal() {
        assert!(validate_path_component("deploy-1").is_ok());
        assert!(validate_path_component("..").is_err());
        assert!(validate_path_component("tenant/escape").is_err());
    }
}

#[cfg(test)]
mod generation_digest_tests {
    use super::sha256_reader;
    use std::io::Cursor;

    #[test]
    fn sha256_reader_matches_known_vector() -> anyhow::Result<()> {
        let digest = sha256_reader(Cursor::new(b"abc"))?;
        assert_eq!(
            digest,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        return Ok(());
    }
}
