use super::*;
use tracing_subscriber::EnvFilter;

use std::future::Future;

pub(super) fn shutdown_signal(service: &'static str) -> anyhow::Result<impl Future<Output = ()>> {
    #[cfg(unix)]
    let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map(|mut signals| async move { signals.recv().await });
    #[cfg(not(unix))]
    let terminate = Ok(std::future::pending::<Option<()>>());
    registered_shutdown(service, terminate)
}

fn registered_shutdown(
    service: &'static str,
    terminate: std::io::Result<impl Future<Output = Option<()>>>,
) -> anyhow::Result<impl Future<Output = ()>> {
    use anyhow::Context;
    let terminate = terminate.context("failed to register API SIGTERM listener")?;
    let interrupt = tokio::signal::ctrl_c();
    Ok(wait_for_signals(service, interrupt, terminate))
}

async fn wait_for_signals(
    service: &'static str,
    interrupt: impl Future<Output = std::io::Result<()>>,
    terminate: impl Future<Output = Option<()>>,
) {
    tokio::pin!(interrupt, terminate);
    let (mut int_open, mut term_open) = (true, true);
    loop {
        let signal = tokio::select! {
            result = &mut interrupt, if int_open => match result {
                Ok(()) => Some("sigint"),
                Err(error) => { tracing::error!(service, %error, "Ctrl-C listener failed"); int_open = false; None }
            },
            result = &mut terminate, if term_open => match result {
                Some(()) => Some("sigterm"),
                None => { tracing::error!(service, "SIGTERM stream closed"); term_open = false; None }
            },
            else => {
                tracing::error!(service, "all shutdown listeners failed");
                return std::future::pending::<()>().await;
            }
        };
        if let Some(signal) = signal {
            info!(service, signal, action = "graceful_shutdown");
            return;
        }
    }
}

pub(super) fn init_tracing(service: &'static str) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if std::env::var_os("BIGNAME_LOG_JSON").is_some() {
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

#[cfg(test)]
mod shutdown_tests {
    use super::*;
    use std::future::{Pending, pending, ready};
    use std::{io, time::Duration};
    use tokio::time::timeout;

    #[test]
    fn registration_failure_is_a_startup_error() {
        let failure: io::Result<Pending<Option<()>>> = Err(io::Error::other("registration"));
        let error = registered_shutdown("api", failure).err().unwrap();
        assert!(format!("{error:#}").contains("register API SIGTERM listener"));
    }

    #[tokio::test]
    async fn listener_errors_preserve_pending_and_surviving_sources() {
        let subscriber = tracing_subscriber::fmt().with_test_writer().finish();
        let _diagnostics = tracing::subscriber::set_default(subscriber);
        let error = || ready(Err(io::Error::other("Ctrl-C")));
        let short = Duration::from_millis(1);
        let long = Duration::from_secs(1);
        let both_failed = wait_for_signals("api", error(), ready(None));
        assert!(timeout(short, both_failed).await.is_err());
        let ctrl_c_only_failed = wait_for_signals("api", error(), pending());
        assert!(timeout(short, ctrl_c_only_failed).await.is_err());
        let ctrl_c_only = wait_for_signals("api", ready(Ok(())), pending());
        timeout(long, ctrl_c_only).await.unwrap();
        for failed_interrupt in [true, false] {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let interrupt = async {
                if failed_interrupt {
                    Err(io::Error::other("Ctrl-C"))
                } else {
                    receiver.await.map_err(io::Error::other)
                }
            };
            let (term_sender, term_receiver) = tokio::sync::oneshot::channel();
            let terminate = async {
                if failed_interrupt {
                    term_receiver.await.ok()
                } else {
                    None
                }
            };
            let future = wait_for_signals("api", interrupt, terminate);
            tokio::pin!(future);
            assert!(timeout(short, &mut future).await.is_err());
            if failed_interrupt {
                term_sender.send(()).unwrap();
            } else {
                sender.send(()).unwrap();
            }
            timeout(long, future).await.unwrap();
        }
    }
}
