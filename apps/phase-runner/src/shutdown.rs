//! Docker stops a container with SIGTERM and `tini` forwards it unchanged, so a
//! handler that waits only for SIGINT never runs the way this service is
//! actually operated: the in-flight batch is killed instead of cancelled, and
//! `chain_phase_state` is left `running` behind a stale heartbeat.

/// The stop signals this process listens on, registered up front.
#[cfg(unix)]
struct StopSignals {
    terminate: Option<tokio::signal::unix::Signal>,
    interrupt: Option<tokio::signal::unix::Signal>,
}
#[cfg(not(unix))]
struct StopSignals;

/// Register the stop signals with the runtime. Both streams have to exist before
/// the caller returns to its own work: until one does, that signal keeps its
/// default disposition and terminates the process, so registering inside a
/// spawned task would leave the whole start-up window unprotected.
fn register() -> StopSignals {
    #[cfg(unix)]
    {
        use tokio::signal::unix::SignalKind;

        StopSignals {
            terminate: install(SignalKind::terminate(), "SIGTERM"),
            interrupt: install(SignalKind::interrupt(), "SIGINT"),
        }
    }
    // Non-Unix is a compile fallback, not a supported deployment: the runner ships
    // in Docker with `tini` forwarding SIGTERM, and CI and deployment are Linux.
    // Ctrl-C there is still registered on first poll of the waiter rather than here.
    #[cfg(not(unix))]
    StopSignals
}

#[cfg(unix)]
fn install(
    kind: tokio::signal::unix::SignalKind,
    name: &str,
) -> Option<tokio::signal::unix::Signal> {
    match tokio::signal::unix::signal(kind) {
        Ok(stream) => Some(stream),
        Err(error) => {
            tracing::warn!(
                error = ?error,
                signal = name,
                "failed to install a stop-signal handler; this signal will not stop the process cleanly"
            );
            None
        }
    }
}

/// Resolve when one of the registered signals arrives. Returns whether a signal
/// was actually observed, so a failed listener does not read as a stop request.
async fn wait(signals: StopSignals) -> bool {
    #[cfg(unix)]
    {
        let StopSignals {
            terminate,
            interrupt,
        } = signals;
        return match (terminate, interrupt) {
            (Some(mut terminate), Some(mut interrupt)) => tokio::select! {
                received = terminate.recv() => received.is_some(),
                received = interrupt.recv() => received.is_some(),
            },
            (Some(mut only), None) | (None, Some(mut only)) => only.recv().await.is_some(),
            (None, None) => tokio::signal::ctrl_c().await.is_ok(),
        };
    }
    #[cfg(not(unix))]
    {
        let StopSignals = signals;
        tokio::signal::ctrl_c().await.is_ok()
    }
}

/// Cancel `cancellation` when the process is asked to stop. Install this only
/// for commands that actually poll the token: registering a stop-signal listener
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

/// Run `startup` unless the process is asked to stop first, returning `None`
/// when it was. Start-up does work that never reads the token — hashing the
/// manifest repository, and a blocking `pg_advisory_lock` in manifest
/// synchronization that a concurrent runner can hold indefinitely — so without
/// this a stop during start-up is absorbed and the process waits for its
/// supervisor to escalate to SIGKILL instead of exiting.
pub async fn until_cancelled<T, E>(
    cancellation: &tokio_util::sync::CancellationToken,
    work: impl std::future::Future<Output = Result<T, E>>,
) -> Result<Option<T>, E> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(None),
        done = work => done.map(Some),
    }
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
