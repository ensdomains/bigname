//! F5's named resource keys retain each name's own latest pointer through clears, unnamed
//! pointer changes, undo and rebuild. These are owned event facts, not proof of a live binding.
mod families_support;

use anyhow::Result;
use bigname_project::families::{FamilyMode, FamilyOptions};
use families_support::{CONTENT_HASH, Event, FAMILY_TABLES, Fixture, uuid};
use serde_json::{Value, json};
use sqlx::raw_sql;

const TABLE: &str = "project_named_resource_pointer";
const R1: &str = "0x00000000000000000000000000000000000000a1";
const R2: &str = "0x00000000000000000000000000000000000000a2";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const MIGRATION: &str =
    include_str!("../../../migrations/20260929140000_named_resource_pointer.sql");

async fn seed(fixture: &Fixture) -> Result<()> {
    let resource = uuid(1);
    fixture.resource(&resource).await?;
    let first = format!("ens:0x{:064x}", 1);
    let second = format!("ens:0x{:064x}", 2);
    for name in [&first, &second] {
        fixture
            .surface(name, name.strip_prefix("ens:").unwrap())
            .await?;
    }
    // Insert ordinal 10 before ordinal 9: canonical emission order, not generated ID or
    // lexicographic identity order, must leave the ordinal-10 pointer in the row.
    for (identity, block, name, resolver) in [
        ("initial:0", 10, Some(first.as_str()), Some(R1)),
        ("same-log:10", 11, Some(first.as_str()), Some(R2)),
        ("same-log:9", 11, Some(first.as_str()), Some(R1)),
        ("clear:0", 12, Some(first.as_str()), Some(ZERO)),
        ("unnamed:1", 12, None, Some(R1)),
        ("new-name:0", 13, Some(second.as_str()), Some(R2)),
        ("null-clear:0", 14, Some(second.as_str()), None),
    ] {
        let mut event = Event::new(identity, block, 0, "ResolverChanged", "ens_v2_registry_l1")
            .resource(&resource)
            .after(json!({"resolver": resolver}));
        event.name = name;
        fixture.event(event).await?;
    }
    Ok(())
}

#[tokio::test]
async fn named_keys_keep_clears_and_canonical_order_through_undo_reset_and_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_named_pointer_replay", 14).await?;
    seed(&fixture).await?;
    for block in [10, 11] {
        let outcome = fixture.apply(block, FamilyMode::Normal).await?;
        assert_eq!(
            outcome.marker.as_ref().map(|marker| marker.number),
            Some(block),
            "{outcome:?}"
        );
    }
    let named = fixture.rows(TABLE).await?;
    assert_eq!(named.len(), 1);
    assert_eq!(named[0]["resolver_address"], R2);
    assert_eq!(named[0]["event_identity"], "same-log:10");

    fixture.assert_undo_restores(12).await?;
    assert_eq!(fixture.rows(TABLE).await?[0]["resolver_address"], ZERO);
    assert_eq!(
        fixture.rows("project_resource_pointer").await?[0]["resolver_address"],
        R1
    );
    fixture.assert_undo_restores(13).await?;
    assert_eq!(
        fixture.rows(TABLE).await?.len(),
        2,
        "undo restores an absent key too"
    );
    fixture.assert_undo_restores(14).await?;
    let named = fixture.rows(TABLE).await?;
    assert!(named.iter().any(|row| row["event_identity"] == "null-clear:0"
        && row["resolver_address"].is_null()));
    fixture.assert_rebuild_equal(14).await?;

    let published = fixture.snapshot().await?;
    let mut options = FamilyOptions::new(CONTENT_HASH);
    options.max_blocks_per_run = 0;
    fixture
        .apply_with(14, FamilyMode::Rebuild, &options)
        .await?;
    assert!(fixture.marker().await?.0.is_none());
    for table in FAMILY_TABLES {
        assert!(fixture.rows(table).await?.is_empty(), "reset kept {table}");
    }
    let replayed = fixture.apply(14, FamilyMode::Normal).await?;
    assert_eq!(
        replayed.marker.as_ref().map(|marker| marker.number),
        Some(14),
        "{replayed:?}"
    );
    assert_eq!(fixture.snapshot().await?, published);
    fixture.cleanup().await
}

/// Compare the migrated table's columns, constraints, indexes and comments to the baseline.
async fn shape(fixture: &Fixture) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(shape) FROM (
             SELECT 'column' AS kind, attname::text AS name,
                    format_type(atttypid, atttypmod) || CASE WHEN attnotnull THEN ' not null' ELSE '' END
                        AS definition
             FROM pg_attribute WHERE attrelid = 'project_named_resource_pointer'::regclass
               AND attnum > 0 AND NOT attisdropped
             UNION ALL
             SELECT 'constraint', conname::text, pg_get_constraintdef(oid)
             FROM pg_constraint WHERE conrelid = 'project_named_resource_pointer'::regclass
             UNION ALL
             SELECT 'index', indexrelid::regclass::text, pg_get_indexdef(indexrelid)
             FROM pg_index WHERE indrelid = 'project_named_resource_pointer'::regclass
             UNION ALL
             SELECT 'comment', objsubid::text, description FROM pg_description
             WHERE classoid = 'pg_class'::regclass AND objoid = 'project_named_resource_pointer'::regclass
         ) shape ORDER BY kind, name",
    ).fetch_all(&fixture.pool).await?)
}

#[tokio::test]
async fn migration_matches_baseline_resets_old_publication_and_is_idempotent() -> Result<()> {
    let fixture = Fixture::new("families_named_pointer_migration", 14).await?;
    // This historical upgrade starts from the real installed pre-removal schema, whose child
    // registration history still carries its maintenance stamps.
    raw_sql("DROP TABLE child_registration_events")
        .execute(&fixture.pool)
        .await?;
    raw_sql(include_str!(
        "../../../schema-v2/fixtures/pre-7c/06_projections.sql"
    ))
    .execute(&fixture.pool)
    .await?;
    seed(&fixture).await?;
    let outcome = fixture.apply(14, FamilyMode::Normal).await?;
    assert_eq!(
        outcome.marker.as_ref().map(|marker| marker.number),
        Some(14),
        "{outcome:?}"
    );
    let baseline = shape(&fixture).await?;
    let published = fixture.snapshot().await?;
    assert_eq!(fixture.rows(TABLE).await?.len(), 2);
    raw_sql("DROP TABLE project_named_resource_pointer")
        .execute(&fixture.pool)
        .await?;
    raw_sql(MIGRATION).execute(&fixture.pool).await?;
    assert_eq!(shape(&fixture).await?, baseline);
    for table in FAMILY_TABLES.iter().copied().chain([
        "project_family_marker",
        "project_family_undo",
        "project_repair_record",
    ]) {
        assert!(
            fixture.rows(table).await?.is_empty(),
            "migration kept {table}"
        );
    }
    let rebuilt = fixture.apply(14, FamilyMode::Normal).await?;
    assert_eq!(
        rebuilt.marker.as_ref().map(|marker| marker.number),
        Some(14),
        "{rebuilt:?}"
    );
    assert_eq!(
        fixture.snapshot().await?,
        published,
        "input survived the upgrade"
    );
    let before_rerun = fixture.exact().await?;
    raw_sql(MIGRATION).execute(&fixture.pool).await?;
    assert_eq!(
        fixture.exact().await?,
        before_rerun,
        "rerun must not reset publication"
    );
    fixture.cleanup().await
}
