//! Docker stops a container with SIGTERM and `tini` forwards it unchanged, so a
//! handler that waits only for SIGINT never runs the way this service is
//! actually operated: the in-flight batch is killed instead of cancelled, and
//! `chain_phase_state` is left `running` behind a stale heartbeat.

/// The stop signals this process listens on, registered up front.
#[cfg(unix)]
struct StopSignals(Option<tokio::signal::unix::Signal>);
#[cfg(not(unix))]
struct StopSignals;

/// Register the stop signals with the runtime. This has to finish before the
/// caller returns to its own work: until the SIGTERM stream exists the signal
/// keeps its default disposition and terminates the process, so registering
/// inside a spawned task would leave the whole start-up window unprotected.
fn register() -> StopSignals {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(terminate) => StopSignals(Some(terminate)),
            Err(error) => {
                tracing::warn!(
                    error = ?error,
                    "failed to install a SIGTERM handler; only SIGINT will stop this process cleanly"
                );
                StopSignals(None)
            }
        }
    }
    #[cfg(not(unix))]
    StopSignals
}

/// Resolve when one of the registered signals arrives. Returns whether a signal
/// was actually observed, so a failed listener does not read as a stop request.
async fn wait(signals: StopSignals) -> bool {
    #[cfg(unix)]
    {
        let StopSignals(terminate) = signals;
        if let Some(mut terminate) = terminate {
            return tokio::select! {
                result = tokio::signal::ctrl_c() => result.is_ok(),
                received = terminate.recv() => received.is_some(),
            };
        }
    }
    #[cfg(not(unix))]
    let StopSignals = signals;

    tokio::signal::ctrl_c().await.is_ok()
}

/// Cancel `cancellation` when the process is asked to stop. Install this only
/// for commands that actually poll the token: registering a SIGTERM listener
/// replaces the default disposition for the whole process, so a one-shot
/// command that never reads the token would absorb the signal and keep running
/// until its supervisor escalates to SIGKILL.
pub fn cancel_on_signal(cancellation: &tokio_util::sync::CancellationToken) {
    let signals = register();
    let cancellation = cancellation.clone();
    tokio::spawn(async move {
        if wait(signals).await {
            cancellation.cancel();
        }
    });
}

/// Resolve when the process is asked to stop. Registration happens when this is
/// called rather than when the returned future is first polled, so a caller that
/// spawns or selects over it is covered from the call onwards.
pub fn requested() -> impl std::future::Future<Output = bool> {
    let signals = register();
    async move { wait(signals).await }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{process::Command, time::Duration};

    use tokio::signal::unix::{SignalKind, signal};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn cancel_on_signal_cancels_the_token_on_sigterm() {
        let _installed = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let cancellation = CancellationToken::new();
        super::cancel_on_signal(&cancellation);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let killed = Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status()
            .expect("raise SIGTERM");
        assert!(killed.success(), "kill -TERM failed: {killed}");

        tokio::time::timeout(Duration::from_secs(5), cancellation.cancelled())
            .await
            .expect("token was not cancelled within the timeout");
    }

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
