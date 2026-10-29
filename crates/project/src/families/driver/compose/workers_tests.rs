//! The budget the workers share and the pool a pass needs, without a database.
use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::postgres::PgPoolOptions;

use super::{require_pool, take};

#[test]
fn the_workers_spend_the_budget_down_to_zero_and_no_further() {
    let budget = AtomicU64::new(2);
    assert!(take(&budget) && take(&budget));
    assert!(!take(&budget));
    assert_eq!(budget.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_pool_needs_a_connection_more_than_its_workers() {
    let pool = |connections| {
        PgPoolOptions::new()
            .max_connections(connections)
            .connect_lazy("postgres://localhost/unused")
            .expect("a lazy pool")
    };
    assert!(require_pool("chain", &pool(1), 1).is_ok());
    assert!(require_pool("chain", &pool(4), 3).is_ok());
    let refused = require_pool("chain", &pool(3), 3)
        .expect_err("three workers need four connections")
        .to_string();
    assert!(
        refused.contains("3 workers") && refused.contains("needs 4") && refused.contains("has 3"),
        "{refused}"
    );
    assert!(require_pool("chain", &pool(1), 2).is_err());
}
