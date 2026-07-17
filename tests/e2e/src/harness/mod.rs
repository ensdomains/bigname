pub mod anvil;
pub mod artifacts;
pub mod basenames;
pub mod db;
pub mod ens_v1;
pub mod ens_v2;
pub mod manifests;
pub mod perturb;
pub mod pipeline;
pub mod responses;
pub mod rpc;

use std::path::PathBuf;
use std::sync::OnceLock;

static LOCAL_SERVER_START_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

/// Keep the bind-after-probe window exclusive across local Anvil and API
/// processes. The selected listener is necessarily released before the child
/// can bind it, so no other server started by this harness may probe a port in
/// that interval.
pub(super) async fn lock_local_server_start() -> tokio::sync::MutexGuard<'static, ()> {
    LOCAL_SERVER_START_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::lock_local_server_start;

    #[tokio::test]
    async fn local_server_start_lock_serializes_waiters() {
        let first = lock_local_server_start().await;
        let mut waiter = tokio::spawn(async {
            let _second = lock_local_server_start().await;
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut waiter)
                .await
                .is_err(),
            "a second local-server startup must wait for the first"
        );
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the second startup must acquire the released lock")
            .expect("startup-lock waiter must not panic");
    }
}
