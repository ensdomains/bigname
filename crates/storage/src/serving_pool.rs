//! Red variant: the pool and its sessions are built as the base builds them, with no acquire
//! check and no idle-in-transaction timeout.

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

#[cfg_attr(not(test), allow(dead_code))]
const IN_TRANSACTION_BLOCK: &str = "SELECT statement_timestamp() <> transaction_timestamp()";

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const ROLLED_BACK_LEFT_OPEN: &str =
    "rolled back a transaction left open on a pooled read connection";

pub(crate) fn serving_pool_options(max_connections: u32) -> PgPoolOptions {
    PgPoolOptions::new().max_connections(max_connections)
}

/// `0` is PostgreSQL's own default: no timeout.
pub(crate) fn idle_in_transaction_setting(_request_deadline: Duration) -> (&'static str, String) {
    ("idle_in_transaction_session_timeout", "0".to_owned())
}

#[cfg(test)]
mod tests;
