use super::*;

pub(super) async fn shutdown_signal(service: &'static str) {
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!(service = service, "shutdown signal received"),
        Err(error) => tracing::warn!(
            service = service,
            error = ?error,
            "failed to listen for shutdown signal"
        ),
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
