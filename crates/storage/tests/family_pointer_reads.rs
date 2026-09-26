//! The pointer reads of the family shadow readers, over tables installed by their own
//! migrations:
//! - `load_family_link_selection`: the latest link per (resolver, node); the link at the name's
//!   own node wins unless it is absent or a clear (record id `0`); then the link at the empty-name
//!   node, the resolver's default record, serves, unless it is a clear too.
//! - `load_family_alias_source_pointer`: the resource's current pointer, rejected when null, zero
//!   or empty, never an older pointer.
//! - `load_family_wildcard_source`: the latest non-zero pointer, with the latest pointer or
//!   version event as its boundary.
use anyhow::Result;
use bigname_storage::families::topology::{
    LinkSelection, load_family_alias_source_pointer, load_family_link_selection,
    load_family_wildcard_source,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::{PgPool, raw_sql};
use uuid::Uuid;

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
    for migration in [
        include_str!("../../../migrations/20260926100200_project_families_resolvers.sql"),
        include_str!("../../../migrations/20260926100300_project_families_records.sql"),
    ] {
        raw_sql(migration).execute(pool).await?;
    }
    Ok(())
}

async fn link(pool: &PgPool, node: &str, record_id: &str, block: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_resolver_link (chain_id, resolver_address, node,
             block_number, transaction_index, log_index, event_identity, record_id,
             storage_model)
         VALUES ($1, $2, $3, $4, 0, 0, $5, $6, $7)
         ON CONFLICT (chain_id, resolver_address, node) DO UPDATE
         SET block_number = EXCLUDED.block_number, event_identity = EXCLUDED.event_identity,
             record_id = EXCLUDED.record_id, storage_model = EXCLUDED.storage_model",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .bind(node)
    .bind(block)
    .bind(format!("link:{node}:{block}"))
    .bind(record_id)
    .bind("resolver_record_id")
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

async fn with_database<F, Fut>(name: &str, body: F) -> Result<()>
where
    F: FnOnce(PgPool) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let database =
        TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(1)).await?;
    let pool = database.pool().clone();
    let result = async {
        install(&pool).await?;
        body(pool.clone()).await
    }
    .await;
    drop(pool);
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn exact_link_then_default_with_record_zero_as_a_clear() -> Result<()> {
    with_database("family_link_selection", |pool| async move {
        exercise(&pool).await
    })
    .await
}

// A cleared default with no exact link is reported, and selects nothing.
#[tokio::test]
async fn an_isolated_cleared_default_selects_nothing() -> Result<()> {
    with_database("family_link_cleared_default", |pool| async move {
        link(&pool, DEFAULT_NODE, "0", 2).await?;
        let cleared = selection(&pool, NAME).await?;
        assert!(
            cleared.as_ref().is_some_and(|s| s.exact.is_none()
                && s.default.as_ref().is_some_and(|link| link.record_id == "0"))
        );
        assert_eq!(served(&cleared), None);
        Ok(())
    })
    .await
}

const ZERO: &str = "0x0000000000000000000000000000000000000000";
const OLDER: &str = "0x00000000000000000000000000000000000000c1";

/// A resource pointer row: the current pointer `current` at block 5, the latest non-zero pointer
/// `nonzero` at block 3 (or at block 5 when the current pointer is that one), and the current
/// pointer as the boundary.
async fn pointer(
    pool: &PgPool,
    resource: Uuid,
    current: Option<&str>,
    nonzero: Option<Option<&str>>,
) -> Result<()> {
    let position = |block: i64| {
        json!({"block_number": block, "transaction_index": 0, "log_index": 0,
               "event_identity": format!("pointer:{resource}:{block}")})
    };
    let nonzero_block = if nonzero == Some(current) { 5 } else { 3 };
    sqlx::query(
        "INSERT INTO bigname_phase.project_resource_pointer (chain_id, resource_id, block_number,
             transaction_index, log_index, event_identity, resolver_address, pointer_position,
             namespace, source_family, namehash, nonzero_resolver_address, nonzero_position,
             boundary_kind, boundary_position, boundary_block_timestamp)
         VALUES ($1, $2, 5, 0, 0, $3, $4, $5, 'ens', 'ens_v2_registry_l1', $6, $7, $8,
             'ResolverChanged', $5, to_timestamp(1700000005))",
    )
    .bind(CHAIN)
    .bind(resource)
    .bind(format!("pointer:{resource}:5"))
    .bind(current)
    .bind(position(5))
    .bind(NAME)
    .bind(nonzero.flatten())
    .bind(nonzero.map(|_| position(nonzero_block)))
    .execute(pool)
    .await?;
    Ok(())
}

// The alias read takes the current pointer and then rejects a null, zero or empty resolver, so a
// clear never exposes an older pointer. An empty resolver means no alias (Tate, 2026-09-26): it is
// the same "no resolver" rule the record pointer applies, and the ENSv2 registry adapter writes
// the resolver through `nullable_address`, so only a fixture can hold an empty one.
#[tokio::test]
async fn alias_pointer_rejects_null_zero_and_empty() -> Result<()> {
    with_database("family_alias_pointer", |pool| async move {
        let cases = [
            (Uuid::from_u128(1), Some(RESOLVER), Some(RESOLVER)),
            (Uuid::from_u128(2), None, None),
            (Uuid::from_u128(3), Some(ZERO), None),
            (Uuid::from_u128(4), Some(""), None),
        ];
        for (resource, current, _) in cases {
            pointer(&pool, resource, current, Some(Some(OLDER))).await?;
        }
        for (resource, _, expected) in cases {
            let read = load_family_alias_source_pointer(&pool, CHAIN, resource).await?;
            assert_eq!(
                read.as_ref()
                    .map(|pointer| pointer.resolver_address.as_str()),
                expected,
                "{resource}"
            );
        }
        Ok(())
    })
    .await
}

// The wildcard read keeps the latest non-zero pointer through a later zero clear, with the clear
// as its boundary. The F5 reducer records a non-zero pointer only for a non-empty, non-zero
// resolver (crates/project/src/families/resolver.rs), so no row it writes pairs a populated
// position with a null or empty address; docs/projections.md lists the served wildcard lateral's
// admission of those under F5.
#[tokio::test]
async fn wildcard_source_keeps_the_historical_pointer_through_a_clear() -> Result<()> {
    with_database("family_wildcard_source", |pool| async move {
        let cleared = Uuid::from_u128(1);
        pointer(&pool, cleared, Some(ZERO), Some(Some(OLDER))).await?;
        let never = Uuid::from_u128(2);
        pointer(&pool, never, Some(ZERO), None).await?;

        let source = load_family_wildcard_source(&pool, CHAIN, cleared)
            .await?
            .expect("a cleared pointer keeps its historical source");
        assert_eq!(source.nonzero_resolver_address.as_deref(), Some(OLDER));
        assert_eq!(source.nonzero_position["block_number"], json!(3));
        assert_eq!(source.boundary_kind, "ResolverChanged");
        assert_eq!(source.boundary_position["block_number"], json!(5));
        assert_eq!(
            load_family_wildcard_source(&pool, CHAIN, never).await?,
            None
        );
        Ok(())
    })
    .await
}
