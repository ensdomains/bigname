//! PostgreSQL JSON timestamps and selected request positions describe the same instant.
use anyhow::{Context, Result};
use bigname_storage::{
    ChainPositions, SnapshotSelectionErrorKind, ensure_projection_chain_positions_match,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};

const CHAIN: &str = "ethereum-mainnet";
const HASH: &str = "0x4a98000000000000000000000000000000000000000000000000000000000000";

#[tokio::test]
async fn postgres_timestamp_offset_matches_the_selected_snapshot() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("snapshot_selection_timestamp").pool_max_connections(1),
    )
    .await?;
    sqlx::query("SET TIME ZONE 'UTC'")
        .execute(database.pool())
        .await?;
    let positions: Value = sqlx::query_scalar("SELECT jsonb_build_object('ethereum',jsonb_build_object('chain_id',$1::text,'block_number',27::bigint,'block_hash',$2::text,'timestamp',TIMESTAMPTZ '2025-06-15 15:07:42+00'))")
        .bind(CHAIN).bind(HASH).fetch_one(database.pool()).await?;
    let timestamp = positions
        .pointer("/ethereum/timestamp")
        .and_then(Value::as_str)
        .context("PostgreSQL omitted its timestamp")?;
    assert_eq!(timestamp, "2025-06-15T15:07:42+00:00");
    let selected = |timestamp: &str| {
        ChainPositions::from_value(
            &json!({"ethereum": {"chain_id":CHAIN,"block_number":27,"block_hash":HASH,"timestamp":timestamp}}),
        )
    };
    ensure_projection_chain_positions_match(
        "names",
        &positions,
        &selected("2025-06-15T15:07:42Z")?,
    )?;
    let error = ensure_projection_chain_positions_match(
        "names",
        &positions,
        &selected("2025-06-15T15:07:43Z")?,
    )
    .expect_err("a different instant must still be stale");
    assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale);
    database.cleanup().await
}
