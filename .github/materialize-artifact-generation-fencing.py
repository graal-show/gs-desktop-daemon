from pathlib import Path

path = Path("src/cells.rs")
text = path.read_text()


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match, found {count}")
    text = text.replace(old, new, 1)


replace_once(
    "use serde_json::{Value, json};\nuse std::{\n    collections::HashMap,\n    fs,\n    path::{Path, PathBuf},",
    "use serde_json::{Value, json};\nuse sha2::{Digest, Sha256};\nuse std::{\n    collections::HashMap,\n    fs,\n    io::Read,\n    path::{Path, PathBuf},",
    "imports",
)
replace_once(
    "struct Cell {\n    key: CellKey,\n    index: u32,",
    "struct Cell {\n    key: CellKey,\n    artifact_sha256: String,\n    index: u32,",
    "cell digest field",
)
replace_once(
    "pub struct CellStatus {\n    pub tenant_id: String,\n    pub deployment_id: String,\n    pub cell_index: u32,",
    "pub struct CellStatus {\n    pub tenant_id: String,\n    pub deployment_id: String,\n    pub artifact_sha256: String,\n    pub cell_index: u32,",
    "status digest field",
)
replace_once(
    "                tenant_id: cell.key.tenant_id.clone(),\n                deployment_id: cell.key.deployment_id.clone(),\n                cell_index: cell.index,",
    "                tenant_id: cell.key.tenant_id.clone(),\n                deployment_id: cell.key.deployment_id.clone(),\n                artifact_sha256: cell.artifact_sha256.clone(),\n                cell_index: cell.index,",
    "status digest value",
)
replace_once(
    "    async fn lease_cell(&self, key: &CellKey) -> Result<(Arc<Cell>, OwnedSemaphorePermit)> {\n        let mut cells = self.inner.cells.lock().await;\n        let group = cells.entry(key.clone()).or_default();\n        group.retain(|cell| !cell.failed.load(Ordering::Acquire));\n\n        for cell in group.iter() {",
    "    async fn lease_cell(&self, key: &CellKey) -> Result<(Arc<Cell>, OwnedSemaphorePermit)> {\n        let artifact_sha256 = generation_sha256(&self.inner.config.artifact_root, key)?;\n        let mut cells = self.inner.cells.lock().await;\n        let group = cells.entry(key.clone()).or_default();\n        group.retain(|cell| !cell.failed.load(Ordering::Acquire));\n\n        for cell in group.iter() {\n            if cell.artifact_sha256 != artifact_sha256 {\n                cell.draining.store(true, Ordering::Release);\n            }\n        }\n\n        for cell in group.iter() {",
    "lease digest fence",
)
replace_once(
    "        if group.len() >= self.inner.config.max_cells_per_generation {\n            bail!(\"all warm Graal cells for this tenant generation are busy\");\n        }\n\n        let index = next_cell_index(group);\n        let cell = self.spawn_cell(key.clone(), index)?;",
    "        let current_generation_cells = group\n            .iter()\n            .filter(|cell| {\n                !cell.failed.load(Ordering::Acquire)\n                    && cell.artifact_sha256 == artifact_sha256\n            })\n            .count();\n        if current_generation_cells >= self.inner.config.max_cells_per_generation {\n            bail!(\"all warm Graal cells for this immutable tenant generation are busy\");\n        }\n\n        let index = next_cell_index(group);\n        let cell = self.spawn_cell(key.clone(), index, &artifact_sha256)?;",
    "generation cell count",
)
replace_once(
    "    fn spawn_cell(&self, key: CellKey, index: u32) -> Result<Arc<Cell>> {",
    "    fn spawn_cell(\n        &self,\n        key: CellKey,\n        index: u32,\n        artifact_sha256: &str,\n    ) -> Result<Arc<Cell>> {",
    "spawn signature",
)
replace_once(
    "        require_regular_file(\n            &artifact_dir.join(\"manifest.json\"),\n            \"Graal deployment manifest\",\n        )?;\n\n        let mut child = Command::new(&self.inner.config.java_command)",
    "        require_regular_file(\n            &artifact_dir.join(\"manifest.json\"),\n            \"Graal deployment manifest\",\n        )?;\n        let actual_sha256 = sha256_file(&jar)?;\n        if actual_sha256 != artifact_sha256 {\n            bail!(\"Graal deployment artifact changed while preparing a warm cell\");\n        }\n\n        let mut child = Command::new(&self.inner.config.java_command)",
    "spawn digest verification",
)
replace_once(
    "            .env(\"GS_ARTIFACT_DIR\", &artifact_dir)\n            .env(",
    "            .env(\"GS_ARTIFACT_DIR\", &artifact_dir)\n            .env(\"GS_ARTIFACT_SHA256\", artifact_sha256)\n            .env(",
    "child digest environment",
)
replace_once(
    "        return Ok(Arc::new(Cell {\n            key,\n            index,",
    "        return Ok(Arc::new(Cell {\n            key,\n            artifact_sha256: artifact_sha256.to_owned(),\n            index,",
    "cell digest initialization",
)
replace_once(
    "fn require_regular_file(path: &Path, description: &str) -> Result<()> {",
    """fn generation_sha256(root: &Path, key: &CellKey) -> Result<String> {
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

fn require_regular_file(path: &Path, description: &str) -> Result<()> {""",
    "sha helpers",
)

text += """

#[cfg(test)]
mod generation_digest_tests {
    use super::sha256_reader;
    use std::io::Cursor;

    #[test]
    fn sha256_reader_matches_known_vector() {
        let digest = sha256_reader(Cursor::new(b"abc")).expect("hash test vector");
        assert_eq!(
            digest,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
"""
path.write_text(text)
