use super::*;
use tracing_subscriber::EnvFilter;

/// Docker stops a container with SIGTERM and `tini` forwards it unchanged, so
/// waiting only for SIGINT leaves `with_graceful_shutdown` unreachable in
/// production and every deploy severs in-flight requests instead of draining
/// them.
pub(super) async fn shutdown_signal(service: &'static str) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                let signal = tokio::select! {
                    result = tokio::signal::ctrl_c() => result.map(|()| "SIGINT"),
                    received = terminate.recv() => {
                        received.ok_or_else(|| std::io::Error::other("SIGTERM stream closed"))
                            .map(|()| "SIGTERM")
                    }
                };
                report_shutdown_signal(service, signal);
                return;
            }
            Err(error) => tracing::warn!(
                service = service,
                error = ?error,
                "failed to install a SIGTERM handler; only SIGINT will drain this service"
            ),
        }
    }

    report_shutdown_signal(service, tokio::signal::ctrl_c().await.map(|()| "SIGINT"));
}

fn report_shutdown_signal(service: &'static str, signal: std::io::Result<&'static str>) {
    match signal {
        Ok(signal) => info!(
            service = service,
            signal = signal,
            "shutdown signal received"
        ),
        Err(error) => tracing::warn!(
            service = service,
            error = ?error,
            "failed to listen for shutdown signal"
        ),
    }
}

/// Presence alone is not enough: Compose passes an unset variable through as an
/// empty string, so a bare `var_os(..).is_some()` check would pin every
/// deployment that forwards the variable to JSON regardless of its value.
fn json_logging_requested() -> bool {
    std::env::var("BIGNAME_LOG_JSON").is_ok_and(|value| {
        !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "" | "0" | "false" | "no" | "off"
        )
    })
}

pub(super) fn init_tracing(service: &'static str) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if json_logging_requested() {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_target(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .with_target(false)
            .init();
    }

    info!(
        service = service,
        version = SOFTWARE_VERSION,
        build_sha = BUILD_SHA,
        "logging configured"
    );
}

#[cfg(all(test, unix))]
mod tests {
    use std::{process::Command, time::Duration};

    use tokio::signal::unix::{SignalKind, signal};

    #[tokio::test]
    async fn a_sigterm_releases_the_shutdown_signal() {
        // Register first: an unhandled SIGTERM would kill the whole test binary,
        // and tokio delivers the signal to every stream registered for the kind.
        let _installed = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let signalled = tokio::spawn(super::shutdown_signal("test"));
        tokio::time::sleep(Duration::from_millis(50)).await;

        let killed = Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status()
            .expect("raise SIGTERM");
        assert!(killed.success(), "kill -TERM failed: {killed}");

        tokio::time::timeout(Duration::from_secs(5), signalled)
            .await
            .expect("SIGTERM did not release the shutdown signal within the timeout")
            .expect("shutdown listener panicked");
    }
}
