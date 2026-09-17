//! A stop that arrives while the runner is hashing manifests must exit the
//! process. Its own test binary, like the other signal tests, so no sibling has
//! already installed a handler.
#![cfg(unix)]

use std::{process::Stdio, time::Duration};

use anyhow::{Context, Result};
use tokio::process::Command;

#[tokio::test]
async fn a_stop_while_hashing_manifests_exits_instead_of_waiting_for_sigkill() -> Result<()> {
    // A FIFO with no writer blocks the profile hash inside `read_to_string`, which is
    // the stalled-filesystem case: the work never returns on its own.
    let manifests = std::env::temp_dir().join(format!("bigname-hash-stall-{}", std::process::id()));
    std::fs::create_dir_all(&manifests)?;
    let fifo = manifests.join("stalled.toml");
    let _ = std::fs::remove_file(&fifo);
    let made = Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .await
        .context("mkfifo")?;
    assert!(made.success(), "mkfifo failed: {made}");

    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_phase-runner"));
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("BIGNAME_") {
            command.env_remove(key);
        }
    }
    let mut child = command
        .current_dir(&root)
        .arg("run")
        .args([
            "--chain",
            "ethereum-sepolia",
            "--source",
            "ethereum-sepolia:hashstall:drpc:ethereum_head:0=HASH_STALL_RPC_URL",
            "--metrics-bind-addr",
            "127.0.0.1:0",
        ])
        .arg("--manifests-root")
        .arg(&manifests)
        .env(
            "BIGNAME_DATABASE_URL",
            "postgres://unused:unused@127.0.0.1:1/unused",
        )
        .env(
            "BIGNAME_PHASE_RUNNER_VERIFICATION_DATABASE_URL",
            "postgres://unused:unused@127.0.0.1:1/unused",
        )
        .env("HASH_STALL_RPC_URL", "http://127.0.0.1:1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("spawn phase-runner")?;

    // A non-blocking write-open of a FIFO fails with ENXIO until a reader holds
    // it, and the only reader is the hash's `read_to_string`, so the first open
    // that succeeds proves the runner is inside the hash — without a blocking
    // call the test could not abandon if the runner never got there. The handle
    // is held, never written or closed, until the runner has exited: closing it
    // would hand the reader an EOF and let the hash finish, which is the case
    // this test is not about.
    let _writer = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match open_write_end_nonblocking(&fifo) {
                Ok(writer) => break Ok::<_, anyhow::Error>(writer),
                Err(error) if error.raw_os_error() == Some(ENXIO) => {
                    if let Some(status) = child.try_wait()? {
                        anyhow::bail!("the runner exited ({status}) before it opened the manifest");
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => break Err(error).context("open the FIFO for writing"),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("the runner never opened the stalled manifest"))??;
    assert!(
        child.try_wait()?.is_none(),
        "the runner exited before it reached the stalled hash"
    );

    let pid = child.id().context("the runner has no pid")?;
    let killed = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .await
        .context("raise SIGTERM")?;
    assert!(killed.success(), "kill -TERM failed: {killed}");

    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "the runner did not exit while manifest hashing was blocked; a stop must not \
                 wait on the hash"
            )
        })??;
    assert!(
        status.success(),
        "a stop during hashing must exit cleanly, got {status}"
    );

    let _ = std::fs::remove_file(&fifo);
    let _ = std::fs::remove_dir(&manifests);
    Ok(())
}

const ENXIO: i32 = 6;
#[cfg(target_os = "linux")]
const O_NONBLOCK: i32 = 0o4000;
#[cfg(not(target_os = "linux"))]
const O_NONBLOCK: i32 = 0x4;

fn open_write_end_nonblocking(fifo: &std::path::Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .write(true)
        .custom_flags(O_NONBLOCK)
        .open(fifo)
}
