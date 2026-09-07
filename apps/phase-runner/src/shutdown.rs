//! Docker stops a container with SIGTERM and `tini` forwards it unchanged, so a
//! handler that waits only for SIGINT never runs the way this service is
//! actually operated: the in-flight batch is killed instead of cancelled, and
//! `chain_phase_state` is left `running` behind a stale heartbeat.

/// Resolve when the process is asked to stop. Returns whether a signal was
/// actually observed, so a failed listener does not read as a stop request.
pub async fn requested() -> bool {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                return tokio::select! {
                    result = tokio::signal::ctrl_c() => result.is_ok(),
                    received = terminate.recv() => received.is_some(),
                };
            }
            Err(error) => tracing::warn!(
                error = ?error,
                "failed to install a SIGTERM handler; only SIGINT will stop this process cleanly"
            ),
        }
    }

    tokio::signal::ctrl_c().await.is_ok()
}

#[cfg(all(test, unix))]
mod tests {
    use std::{process::Command, time::Duration};

    use tokio::signal::unix::{SignalKind, signal};

    #[tokio::test]
    async fn a_sigterm_is_observed_as_a_stop_request() {
        // Register first: an unhandled SIGTERM would kill the whole test binary,
        // and tokio delivers the signal to every stream registered for the kind.
        let _installed = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let requested = tokio::spawn(super::requested());
        tokio::time::sleep(Duration::from_millis(50)).await;

        let killed = Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status()
            .expect("raise SIGTERM");
        assert!(killed.success(), "kill -TERM failed: {killed}");

        let observed = tokio::time::timeout(Duration::from_secs(5), requested)
            .await
            .expect("SIGTERM was not observed within the timeout")
            .expect("shutdown listener panicked");
        assert!(observed, "SIGTERM did not read as a stop request");
    }
}
