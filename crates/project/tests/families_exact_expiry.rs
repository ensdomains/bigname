//! Exact expiry through the family writer, undo/rebuild, and the old-column schema upgrade.
#[path = "families_support/mod.rs"]
mod support;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use bigname_project::families::{self, FamilyMode};
use bigname_storage::families::name::load_family_name;
use serde_json::{Value, json};
use support::{CHAIN, Event, Fixture, uuid};

const MIGRATION: &str = include_str!("../../../migrations/20260930210000_exact_expiry_seconds.sql");
const FAMILY: &str = "ens_v2_registry_l1";
const REGISTRY: &str = "0x0000000000000000000000000000000000000667";
const OWNER: &str = "0x0000000000000000000000000000000000000067";
const NAME: &str = "ens:0x0000000000000000000000000000000000000000000000000000000000000667";
const TOKEN: &str = "0x0000000000000000000000000000000000000000000000000000066700000000";
const CONTROL_TABLES: [&str; 3] = [
    "project_family_marker",
    "project_family_undo",
    "project_repair_record",
];

fn reset_tables() -> BTreeSet<&'static str> {
    families::family_tables()
        .filter(|table| {
            !matches!(
                *table,
                "project_text_hydration_work"
                    | "project_reverse_hydration_work"
                    // Added later, by 20261005140000_project_ens_v2_registry_entries.sql.
                    | "project_ens_v2_entry_owner"
                    | "project_ens_v2_registry_parent"
            )
        })
        .chain(CONTROL_TABLES)
        .collect()
}

#[test]
fn exact_expiry_migration_reset_inventory_matches_owned_families() {
    let list = MIGRATION
        .split_once("ARRAY ARRAY[")
        .expect("literal reset inventory")
        .1
        .split_once(']')
        .expect("reset inventory end")
        .0;
    let literal: Vec<_> = list.split('\'').skip(1).step_by(2).collect();
    let unique: BTreeSet<_> = literal.iter().copied().collect();
    assert_eq!(literal.len(), unique.len(), "no duplicate reset entries");
    let expected: BTreeSet<_> = reset_tables()
        .into_iter()
        .chain(support::RETIRED_FAMILY_TABLES)
        .collect();
    assert_eq!(unique, expected);
}

/// The pending LabelRegistered grant followed by TokenResource's bound grant, owner and expiry,
/// matching the producer shape in protocol/v2_registry/{transfer.rs,v2_registry.rs}.
async fn register(fixture: &Fixture, expiry: u64) -> Result<()> {
    let resource = uuid(0x6671);
    let binding = uuid(0x6672);
    let instance = uuid(0x6673);
    fixture
        .binding(&binding, NAME, &resource, "ens_v2", 2, 2, None)
        .await?;
    let linked = json!({"token_id": TOKEN, "current_token_id": TOKEN, "upstream_resource": TOKEN});
    let mut bound = linked.clone();
    bound["source_event"] = json!("TokenResource");
    bound["binding_kind"] = json!("declared_registry_path");
    bound["surface_binding_id"] = json!(binding);
    let mut granted = linked.clone();
    for (key, value) in [
        ("source_event", json!("LabelRegistered")),
        ("registrant", json!(OWNER)),
        ("expiry", json!(expiry)),
        ("status", json!("registered")),
        ("authority_kind", json!("ens_v2_registry")),
        (
            "authority_key",
            json!(format!("ens-v2-registry:{CHAIN}:{instance}:{TOKEN}")),
        ),
        ("resource_pending", json!(false)),
        ("registry_contract_instance_id", json!(instance)),
    ] {
        granted[key] = value;
    }
    let mut transferred = linked.clone();
    transferred["source_event"] = json!("LabelRegistered");
    transferred["owner"] = json!(OWNER);
    let mut changed = linked;
    changed["source_event"] = json!("LabelRegistered");
    changed["expiry"] = json!(expiry);
    let pending = json!({"source_event": "LabelRegistered", "registrant": OWNER,
        "expiry": expiry, "token_id": TOKEN, "resource_pending": true,
        "status": "registered", "registry_contract_instance_id": instance});
    for (log, kind, with_resource, after) in [
        (0, "RegistrationGranted", false, pending),
        (2, "SurfaceBound", true, bound),
        (2, "RegistrationGranted", true, granted),
        (2, "AuthorityTransferred", true, transferred),
        (2, "ExpiryChanged", true, changed),
    ] {
        let identity = format!("exact-expiry-{kind}-2-{log}");
        let mut event = Event::new(&identity, 2, log, kind, FAMILY)
            .name(NAME)
            .after(after)
            .raw(json!({"emitting_address": REGISTRY}));
        if with_resource {
            event = event.resource(&resource);
        }
        fixture.event(event).await?;
    }
    Ok(())
}

async fn renew(fixture: &Fixture, old: u64, new: u64) -> Result<()> {
    assert!(new > old, "a renewal increases the retained uint64 expiry");
    let resource = uuid(0x6671);
    for kind in ["ExpiryChanged", "RegistrationRenewed"] {
        let identity = format!("exact-expiry-{kind}-3");
        fixture
            .event(
                Event::new(&identity, 3, 0, kind, FAMILY)
                    .name(NAME)
                    .resource(&resource)
                    .before(json!({"expiry": old}))
                    .after(json!({"source_event": "ExpiryUpdated", "token_id": TOKEN,
                "registry_contract_instance_id": uuid(0x6673), "expiry": new,
                "sender": OWNER, "revived_from_expiry": false}))
                    .raw(json!({"emitting_address": REGISTRY})),
            )
            .await?;
    }
    Ok(())
}

async fn assert_column_types(fixture: &Fixture, lifecycle: &str, summary: &str) -> Result<()> {
    for (table, column, expected) in [
        ("project_lifecycle_event", "expiry_seconds", lifecycle),
        ("project_name_summary", "expires_at", summary),
    ] {
        let actual: String = sqlx::query_scalar(
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute
             WHERE attrelid = $1::regclass AND attname = $2 AND NOT attisdropped",
        )
        .bind(table)
        .bind(column)
        .fetch_one(&fixture.pool)
        .await?;
        assert_eq!(actual, expected, "{table}.{column}");
    }
    Ok(())
}

async fn assert_expiry(fixture: &Fixture, expected: u64, retained: &[u64]) -> Result<()> {
    assert_column_types(fixture, "numeric", "numeric").await?;
    let actual: Vec<String> = sqlx::query_scalar(
        "SELECT retained.expiry_seconds::text FROM (
             SELECT DISTINCT expiry_seconds FROM project_lifecycle_event
             WHERE chain_id = $1 AND expiry_seconds IS NOT NULL
         ) retained ORDER BY retained.expiry_seconds",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        actual,
        retained.iter().map(u64::to_string).collect::<Vec<_>>()
    );
    let summary: String = sqlx::query_scalar(
        "SELECT expires_at::text FROM project_name_summary
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(NAME)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(summary, expected.to_string());
    let served = load_family_name(&fixture.pool, NAME)
        .await?
        .expect("composed name");
    assert_eq!(
        served.declared_summary["registration"]["expiry"],
        json!(expected.to_string())
    );
    assert!(
        served.declared_summary["registration"]
            .get("expires_at_reason")
            .is_none()
    );
    Ok(())
}

async fn publication_rows(fixture: &Fixture) -> Result<BTreeMap<&'static str, Vec<Value>>> {
    let mut rows = BTreeMap::new();
    for table in reset_tables() {
        rows.insert(table, fixture.rows(table).await?);
    }
    Ok(rows)
}

#[tokio::test]
async fn finite_expiry_above_i64_survives_incremental_renewal_undo_and_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_exact_expiry_replay", 4).await?;
    let granted = i64::MAX as u64 + 1;
    let renewed = u64::MAX - 1;
    register(&fixture, granted).await?;
    renew(&fixture, granted, renewed).await?;
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_expiry(&fixture, granted, &[granted]).await?;
    let before = fixture.exact().await?;
    let applied = fixture.apply(3, FamilyMode::Normal).await?;
    assert!(
        !applied.reset,
        "the renewal follows the existing publication"
    );
    assert_eq!(applied.blocks, 1);
    assert_expiry(&fixture, renewed, &[granted, renewed]).await?;
    assert_eq!(families::undo_to(&fixture.pool, CHAIN, 2).await?, 1);
    assert_eq!(
        fixture.exact().await?,
        before,
        "undo restores every family and the marker"
    );
    assert_expiry(&fixture, granted, &[granted]).await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    fixture.assert_rebuild_equal(3).await?;
    assert_expiry(&fixture, renewed, &[granted, renewed]).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn prior_expiry_types_upgrade_resets_once_and_rebuilds_exactly_like_fresh_baseline()
-> Result<()> {
    let fixture = Fixture::new("families_exact_expiry_upgrade", 4).await?;
    let ordinary = 2_000_000_000;
    let large = u64::MAX - 1;
    register(&fixture, ordinary).await?;
    renew(&fixture, ordinary, large).await?;
    // Capture the fresh numeric baseline's ordinary and later large publications first.
    fixture.apply(2, FamilyMode::Normal).await?;
    assert_expiry(&fixture, ordinary, &[ordinary]).await?;
    let ordinary_baseline = fixture.snapshot().await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    assert_expiry(&fixture, large, &[ordinary, large]).await?;
    let large_baseline = fixture.snapshot().await?;
    assert_eq!(families::undo_to(&fixture.pool, CHAIN, 2).await?, 1);
    assert_eq!(fixture.snapshot().await?, ordinary_baseline);
    // Only ordinary values remain published, so recreating the prior types is lossless.
    sqlx::raw_sql(
        "ALTER TABLE project_lifecycle_event ALTER COLUMN expiry_seconds TYPE bigint
             USING expiry_seconds::bigint;
         ALTER TABLE project_name_summary ALTER COLUMN expires_at TYPE timestamptz
             USING to_timestamp(expires_at::double precision)",
    )
    .execute(&fixture.pool)
    .await?;
    assert_column_types(&fixture, "bigint", "timestamp with time zone").await?;
    for table in [
        "project_lifecycle_event",
        "project_name_summary",
        "project_binding_candidate",
        "project_family_marker",
        "project_family_undo",
        "project_repair_record",
    ] {
        assert!(
            !fixture.rows(table).await?.is_empty(),
            "real input must populate {table}"
        );
    }
    let retained: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(retained, 7, "five grant rows and two renewal rows");
    sqlx::raw_sql(MIGRATION).execute(&fixture.pool).await?;
    assert_column_types(&fixture, "numeric", "numeric").await?;
    for (table, rows) in publication_rows(&fixture).await? {
        assert!(rows.is_empty(), "migration must reset {table}: {rows:?}");
    }
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events")
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(after, retained, "migration preserves normalized inputs");
    let rebuilt = fixture.apply(2, FamilyMode::Normal).await?;
    assert!(
        rebuilt.reset,
        "a removed marker triggers a fresh publication"
    );
    assert_eq!(fixture.snapshot().await?, ordinary_baseline);
    assert_expiry(&fixture, ordinary, &[ordinary]).await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    assert_eq!(fixture.snapshot().await?, large_baseline);
    assert_expiry(&fixture, large, &[ordinary, large]).await?;
    let published = publication_rows(&fixture).await?;
    sqlx::raw_sql(MIGRATION).execute(&fixture.pool).await?;
    assert_eq!(
        publication_rows(&fixture).await?,
        published,
        "a repeated migration preserves families, marker sequence, undo and repair state"
    );
    fixture.cleanup().await
}
