//! Red variant: the pool is built as the base builds it, without the acquire check.

use sqlx::postgres::PgPoolOptions;

#[cfg_attr(not(test), allow(dead_code))]
const IN_TRANSACTION_BLOCK: &str = "SELECT statement_timestamp() <> transaction_timestamp()";

pub(crate) fn serving_pool_options(max_connections: u32) -> PgPoolOptions {
    PgPoolOptions::new().max_connections(max_connections)
}

#[cfg(test)]
mod tests;
