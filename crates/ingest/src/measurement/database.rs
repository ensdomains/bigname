//! Actual connection witnesses share the ordinary observation sequence.
use super::*;

pub fn observe(role: &str, witness: Option<&str>) {
    let Some(c) = capture() else { return };
    let run = &c.session.run;
    let Ok(_guard) = run.emission.lock() else {
        run.invalid.store(true, Ordering::Relaxed);
        return;
    };
    if witness.is_none() || increment(&run.database_connections, 1, run) > 128 {
        run.invalid.store(true, Ordering::Relaxed);
    }
    let sequence = increment(&run.sequence, 1, run);
    tracing::info!(target: "bigname_memory", schema = 1, run = %c.session.id, sequence,
        elapsed_us = c.session.started.elapsed().as_micros() as u64,
        chain = "", from = 0, to = 0, attempt = 0, query_kind = "none", query_ordinal = 0,
        stage = "postgres_connection", database_role = role, database = witness.unwrap_or("null"),
        runner_pid = std::process::id(), rows = 0, logical_bytes = 0, max_item_bytes = 0,
        stored_rows = run.stored_rows.load(Ordering::Relaxed), stored_bytes = run.stored_bytes.load(Ordering::Relaxed),
        provider_rows = run.provider_rows.load(Ordering::Relaxed), provider_bytes = run.provider_bytes.load(Ordering::Relaxed),
        valid = c.session.valid(), "verify database connection observation");
}
