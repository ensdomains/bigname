use tracing::info;
use tracing_subscriber::EnvFilter;

pub(crate) fn init_tracing(service: &'static str, emit_logs_to_stderr: bool) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if std::env::var_os("BIGNAME_LOG_JSON").is_some() {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_target(false);
        if emit_logs_to_stderr {
            subscriber.with_writer(std::io::stderr).init();
        } else {
            subscriber.init();
        }
    } else {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .with_target(false);
        if emit_logs_to_stderr {
            subscriber.with_writer(std::io::stderr).init();
        } else {
            subscriber.init();
        }
    }

    info!(
        service = service,
        phase = bigname_domain::bootstrap_phase(),
        "logging configured"
    );
}
