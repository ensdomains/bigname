//! The schema-migration that gives `project_repair_record` the composition pass of a rebuild in
//! ranges (migrations/20261016130000_project_repair_record_composing.sql): the state `composing`
//! and the columns `composed_block`, `compose_stream` and `compose_cursor`. Without it the
//! record refuses `composing`, and a family run of this build fails before it writes a pass. On
//! the record as it stood before, the migration keeps every row and leaves the table as the
//! baseline defines it, and applied again it changes nothing.
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use bigname_project::families::{FamilyMode, FamilyOptions, RebuildRanges};
use support::{CHAIN, CONTENT_HASH, Fixture};

const MIGRATION: &str =
    include_str!("../../../migrations/20261016130000_project_repair_record_composing.sql");
/// The record as the release before this one defines it.
const PREDECESSOR: &str = "ALTER TABLE project_repair_record
         DROP CONSTRAINT project_repair_record_compose_check,
         DROP CONSTRAINT project_repair_record_rebuild_base_check,
         DROP CONSTRAINT project_repair_record_state_check,
         DROP COLUMN composed_block, DROP COLUMN compose_stream, DROP COLUMN compose_cursor,
         ADD CHECK (state IN ('undoing', 'replaying', 'rebuilding', 'complete')),
         ADD CHECK (state <> 'rebuilding' OR trusted_base_number IS NULL)";
const R1: &str = "0x00000000000000000000000000000000000000a1";

/// The record's columns, constraint definitions, named constraints and comments.
async fn shape(fixture: &Fixture) -> Result<Vec<Vec<String>>> {
    let columns = sqlx::query_scalar(
        "SELECT attribute.attname || ' ' || format_type(attribute.atttypid, attribute.atttypmod)
                || CASE WHEN attribute.attnotnull THEN ' not null' ELSE '' END
                || coalesce(' -- ' || col_description(attribute.attrelid, attribute.attnum), '')
         FROM pg_attribute attribute
         WHERE attribute.attrelid = 'project_repair_record'::regclass
           AND attribute.attnum > 0 AND NOT attribute.attisdropped
         ORDER BY 1",
    )
    .fetch_all(&fixture.pool)
    .await?;
    let checks = sqlx::query_scalar(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint
         WHERE conrelid = 'project_repair_record'::regclass ORDER BY 1",
    )
    .fetch_all(&fixture.pool)
    .await?;
    let named = sqlx::query_scalar(
        "SELECT conname || ' ' || pg_get_constraintdef(oid) FROM pg_constraint
         WHERE conrelid = 'project_repair_record'::regclass
           AND conname IN ('project_repair_record_state_check',
                           'project_repair_record_rebuild_base_check',
                           'project_repair_record_compose_check',
                           'project_repair_record_prefix_recorded_check')
         ORDER BY 1",
    )
    .fetch_all(&fixture.pool)
    .await?;
    Ok(vec![columns, checks, named])
}

/// A ranged rebuild of a few pointer blocks to 20, which composes in a pass.
async fn rebuild(
    fixture: &Fixture,
) -> bigname_project::Result<bigname_project::families::FamilyOutcome> {
    fixture
        .apply_with(
            20,
            FamilyMode::Rebuild,
            &FamilyOptions::new(CONTENT_HASH).with_rebuild_ranges(RebuildRanges::Through(20)),
        )
        .await
}

#[tokio::test]
async fn the_migration_gives_the_record_its_composition_pass() -> Result<()> {
    let fixture = Fixture::new("families_compose_migration", 30).await?;
    for block in 10..=14 {
        fixture.resolver_changed(block, 1, 1, R1).await?;
    }
    let baseline = shape(&fixture).await?;
    sqlx::raw_sql(PREDECESSOR).execute(&fixture.pool).await?;
    sqlx::query(
        "INSERT INTO project_repair_record (chain_id, attempt, reason, replay_target_number,
             replay_target_hash, state)
         VALUES ('ethereum-holesky', 3, 'content_hash_rebuild', 9, 'target', 'rebuilding')",
    )
    .execute(&fixture.pool)
    .await?;

    // Without the migration the record refuses the pass, and a run of this build fails.
    let refused = sqlx::query(
        "UPDATE project_repair_record SET state = 'composing' WHERE chain_id = 'ethereum-holesky'",
    )
    .execute(&fixture.pool)
    .await;
    ensure!(
        refused
            .as_ref()
            .is_err_and(|error| error.to_string().contains("check constraint")),
        "the predecessor refuses composing: {refused:?}"
    );
    ensure!(
        rebuild(&fixture).await.is_err(),
        "a family run fails on the predecessor record"
    );

    sqlx::raw_sql(MIGRATION).execute(&fixture.pool).await?;
    ensure!(
        shape(&fixture).await? == baseline,
        "the migrated record is the baseline's: {:?} against {baseline:?}",
        shape(&fixture).await?
    );
    let kept: (String, Option<i64>) = sqlx::query_as(
        "SELECT state, composed_block FROM project_repair_record
         WHERE chain_id = 'ethereum-holesky'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(
        kept == ("rebuilding".to_owned(), None),
        "the row is kept: {kept:?}"
    );
    sqlx::raw_sql(MIGRATION).execute(&fixture.pool).await?;
    ensure!(
        shape(&fixture).await? == baseline,
        "applied again, the migration changes nothing"
    );

    let outcome = rebuild(&fixture).await?;
    ensure!(
        outcome.compose_chunks > 0,
        "the migrated record holds a pass: {outcome:?}"
    );
    let composed: Option<i64> =
        sqlx::query_scalar("SELECT composed_block FROM project_repair_record WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(&fixture.pool)
            .await?;
    ensure!(composed == Some(14), "{composed:?}");
    fixture.cleanup().await
}
