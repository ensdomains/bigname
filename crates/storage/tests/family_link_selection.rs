//! `load_family_link_selection`: the link at the name's own node wins unless it is absent or a
//! clear (record id `0`); then the link at the empty-name node, the resolver's default record,
//! serves, unless it is a clear too. The table comes from its own migration.
use anyhow::Result;
use bigname_storage::families::topology::{LinkSelection, load_family_link_selection};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const RESOLVER: &str = "0x00000000000000000000000000000000000000d1";
const DEFAULT_NODE: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const NAME: &str = "0x1000000000000000000000000000000000000000000000000000000000000001";
const OTHER: &str = "0x1000000000000000000000000000000000000000000000000000000000000002";

async fn install(pool: &PgPool) -> Result<()> {
    // The migration installs its tables only once the phase schema exists.
    raw_sql(
        "CREATE SCHEMA IF NOT EXISTS bigname_phase;
         CREATE TABLE bigname_phase.name_current (logical_name_id text PRIMARY KEY);",
    )
    .execute(pool)
    .await?;
    raw_sql(include_str!(
        "../../../migrations/20260926100300_project_families_records.sql"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn link(pool: &PgPool, node: &str, record_id: &str, block: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_resolver_link (chain_id, resolver_address, node,
             block_number, transaction_index, log_index, event_identity, record_id,
             storage_model)
         VALUES ($1, $2, $3, $4, 0, 0, $5, $6, 'resolver_record_id')
         ON CONFLICT (chain_id, resolver_address, node) DO UPDATE
         SET block_number = EXCLUDED.block_number, event_identity = EXCLUDED.event_identity,
             record_id = EXCLUDED.record_id",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .bind(node)
    .bind(block)
    .bind(format!("link:{node}:{block}"))
    .bind(record_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn selection(pool: &PgPool, node: &str) -> Result<Option<LinkSelection>> {
    load_family_link_selection(pool, CHAIN, &RESOLVER.to_ascii_uppercase(), node).await
}

fn served(selection: &Option<LinkSelection>) -> Option<&str> {
    selection
        .as_ref()
        .and_then(LinkSelection::selected)
        .map(|link| link.record_id.as_str())
}

async fn exercise(pool: &PgPool) -> Result<()> {
    install(pool).await?;

    // Nothing linked: no selection.
    assert_eq!(selection(pool, NAME).await?, None);

    // Only a default record: it serves every node, and the exact probe found nothing.
    link(pool, DEFAULT_NODE, "7", 2).await?;
    let only_default = selection(pool, NAME).await?;
    assert_eq!(served(&only_default), Some("7"));
    assert!(only_default.as_ref().is_some_and(|s| s.exact.is_none()));

    // An exact link wins, and the default is not read.
    link(pool, NAME, "5", 3).await?;
    let exact = selection(pool, NAME).await?;
    assert_eq!(served(&exact), Some("5"));
    assert!(exact.as_ref().is_some_and(|s| s.default.is_none()));
    // Another node still falls back to the default.
    assert_eq!(served(&selection(pool, OTHER).await?), Some("7"));

    // Record 0 clears the exact link: the default serves again, and the clear is still reported.
    link(pool, NAME, "0", 4).await?;
    let cleared = selection(pool, NAME).await?;
    assert_eq!(served(&cleared), Some("7"));
    assert_eq!(
        cleared
            .as_ref()
            .and_then(|s| s.exact.as_ref())
            .map(|link| link.record_id.as_str()),
        Some("0")
    );

    // A cleared default leaves a cleared exact link with nothing to serve.
    link(pool, DEFAULT_NODE, "0", 5).await?;
    let both_cleared = selection(pool, NAME).await?;
    assert!(both_cleared.is_some());
    assert_eq!(served(&both_cleared), None);

    // A new exact link serves again over the cleared default.
    link(pool, NAME, "9", 6).await?;
    assert_eq!(served(&selection(pool, NAME).await?), Some("9"));
    Ok(())
}

#[tokio::test]
async fn exact_link_then_default_with_record_zero_as_a_clear() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("family_link_selection").pool_max_connections(1),
    )
    .await?;
    let pool = database.pool().clone();
    let result = exercise(&pool).await;
    drop(pool);
    database.cleanup().await?;
    result
}
