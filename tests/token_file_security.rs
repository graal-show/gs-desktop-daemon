#![cfg(unix)]

use std::{
    error::Error,
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn temp_root(case: &str) -> Result<PathBuf, Box<dyn Error>> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = std::env::temp_dir().join(format!(
        "gs-desktop-token-test-{case}-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&path)?;
    return Ok(path);
}

fn occupied_loopback() -> Result<(TcpListener, String), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?.to_string();
    return Ok((listener, address));
}

fn daemon_output(token_path: &Path) -> Result<std::process::Output, Box<dyn Error>> {
    let (_listener, address) = occupied_loopback()?;
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let artifact_root = token_path
        .parent()
        .ok_or("token path must have a parent")?
        .join("artifacts");

    let output = Command::new(env!("CARGO_BIN_EXE_gs-desktop-daemon"))
        .env(
            "GS_DESKTOP_FLAGS_CONFIG",
            manifest_dir.join(".cli-flags.toml"),
        )
        .env("GS_DESKTOP_ADDR", address)
        .env("GS_DESKTOP_TOKEN_FILE", token_path)
        .env("GS_ARTIFACT_ROOT", artifact_root)
        .output()?;
    return Ok(output);
}

fn stderr_text(output: &std::process::Output) -> String {
    return String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
}

#[test]
fn rejects_existing_token_with_group_or_world_access() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt;

    let root = temp_root("weak-mode")?;
    let token_path = root.join("token");
    fs::write(&token_path, format!("{}\n", "a".repeat(64)))?;
    fs::set_permissions(&token_path, fs::Permissions::from_mode(0o644))?;

    let output = daemon_output(&token_path)?;
    let stderr = stderr_text(&output);
    assert!(!output.status.success());
    assert!(stderr.contains("token"));
    assert!(
        stderr.contains("0600")
            || stderr.contains("permission")
            || stderr.contains("mode")
            || stderr.contains("owner-only")
    );

    fs::remove_dir_all(root)?;
    return Ok(());
}

#[test]
fn rejects_symlink_token_path() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let root = temp_root("symlink")?;
    let target = root.join("real-token");
    let token_path = root.join("token-link");
    fs::write(&target, format!("{}\n", "b".repeat(64)))?;
    fs::set_permissions(&target, fs::Permissions::from_mode(0o600))?;
    symlink(&target, &token_path)?;

    let output = daemon_output(&token_path)?;
    let stderr = stderr_text(&output);
    assert!(!output.status.success());
    assert!(stderr.contains("token"));
    assert!(
        stderr.contains("symlink") || stderr.contains("regular") || stderr.contains("file type")
    );

    fs::remove_dir_all(root)?;
    return Ok(());
}
