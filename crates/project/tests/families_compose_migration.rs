//! The schema-migration that gives `project_repair_record` the composition pass of a rebuild in
//! ranges (migrations/20261016130000_project_repair_record_composing.sql): the state `composing`,
//! the columns `composed_block`, `compose_stream`, `compose_cursor` and `compose_chunks`, and the
//! pass's work list `project_compose_name_work`. Without it the record refuses `composing`, and a
//! family run of this build fails as it reads the record. On the record as it stood before, the
//! migration keeps every row and leaves the tables as the baseline defines them, and applied again
//! it changes nothing. A runner of the release before, which knows none of the new columns, can
//! still move a record off composing.
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use bigname_project::families::{FamilyMode, FamilyOptions, RebuildRanges};
use support::{CHAIN, CONTENT_HASH, Fixture};

const MIGRATION: &str =
    include_str!("../../../migrations/20261016130000_project_repair_record_composing.sql");
/// The later migration that gives the work list its catalogue build columns
/// (families_compose_builder_migration.rs), which a database of the release before applies after
/// this one.
const BUILDER: &str =
    include_str!("../../../migrations/20261027120000_project_compose_history_builder.sql");
/// The comment-only migration after it, which names the lookup inventories stream.
const STREAM_COMMENTS: &str =
    include_str!("../../../migrations/20261027130000_project_compose_stream_comments.sql");
/// The record as the release before this one defines it.
const PREDECESSOR: &str = "DROP TABLE IF EXISTS project_compose_history_work;
     DROP TABLE IF EXISTS project_compose_name_work;
     ALTER TABLE project_repair_record
         DROP CONSTRAINT project_repair_record_compose_check,
         DROP CONSTRAINT project_repair_record_rebuild_base_check,
         DROP CONSTRAINT project_repair_record_state_check,
         DROP COLUMN composed_block, DROP COLUMN compose_stream, DROP COLUMN compose_cursor,
         DROP COLUMN IF EXISTS compose_chunks,
         ADD CHECK (state IN ('undoing', 'replaying', 'rebuilding', 'complete')),
         ADD CHECK (state <> 'rebuilding' OR trusted_base_number IS NULL)";
const R1: &str = "0x00000000000000000000000000000000000000a1";

/// The record's and the work list's columns, defaults, constraint definitions and comments, and
/// the record's named constraints.
async fn shape(fixture: &Fixture) -> Result<Vec<Vec<String>>> {
    let columns = sqlx::query_scalar(
        "SELECT relation.relname || '.' || attribute.attname || ' '
                || format_type(attribute.atttypid, attribute.atttypmod)
                || CASE WHEN attribute.attnotnull THEN ' not null' ELSE '' END
                || coalesce(' default ' || pg_get_expr(fallback.adbin, fallback.adrelid), '')
                || coalesce(' -- ' || col_description(attribute.attrelid, attribute.attnum), '')
         FROM pg_attribute attribute
         JOIN pg_class relation ON relation.oid = attribute.attrelid
         LEFT JOIN pg_attrdef fallback
           ON fallback.adrelid = attribute.attrelid AND fallback.adnum = attribute.attnum
         WHERE relation.relname IN ('project_repair_record', 'project_compose_name_work')
           AND relation.relnamespace = 'bigname_phase'::regnamespace
           AND attribute.attnum > 0 AND NOT attribute.attisdropped
         UNION ALL
         SELECT relation.relname || ' -- ' || coalesce(obj_description(relation.oid, 'pg_class'), '')
         FROM pg_class relation
         WHERE relation.relname = 'project_compose_name_work'
           AND relation.relnamespace = 'bigname_phase'::regnamespace
         ORDER BY 1",
    )
    .fetch_all(&fixture.pool)
    .await?;
    let checks = sqlx::query_scalar(
        "SELECT relation.relname || ' ' || pg_get_constraintdef(constraint_row.oid)
         FROM pg_constraint constraint_row
         JOIN pg_class relation ON relation.oid = constraint_row.conrelid
         WHERE relation.relname IN ('project_repair_record', 'project_compose_name_work')
           AND relation.relnamespace = 'bigname_phase'::regnamespace
         ORDER BY 1",
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
    sqlx::raw_sql(BUILDER).execute(&fixture.pool).await?;
    sqlx::raw_sql(STREAM_COMMENTS)
        .execute(&fixture.pool)
        .await?;
    ensure!(
        shape(&fixture).await? == baseline,
        "the migrated record is the baseline's: {:?} against {baseline:?}",
        shape(&fixture).await?
    );
    // A runner of the release before moves a record off composing and leaves the pass columns
    // it does not know.
    sqlx::query(
        "INSERT INTO project_repair_record (chain_id, attempt, reason, replay_target_number,
             replay_target_hash, state, compose_stream, compose_cursor)
         VALUES ('ethereum-sepolia', 2, 'content_hash_rebuild', 9, 'target', 'composing', 0,
                 '7')",
    )
    .execute(&fixture.pool)
    .await?;
    let moved = sqlx::query(
        "UPDATE project_repair_record SET state = 'rebuilding', attempt = 3
         WHERE chain_id = 'ethereum-sepolia'",
    )
    .execute(&fixture.pool)
    .await;
    ensure!(
        moved.is_ok(),
        "the record leaves composing with its pass columns: {moved:?}"
    );
    let refused = sqlx::query(
        "UPDATE project_repair_record SET state = 'composing', compose_stream = NULL
         WHERE chain_id = 'ethereum-sepolia'",
    )
    .execute(&fixture.pool)
    .await;
    ensure!(
        refused
            .as_ref()
            .is_err_and(|error| error.to_string().contains("check constraint")),
        "composing still needs a stream: {refused:?}"
    );
    sqlx::query("DELETE FROM project_repair_record WHERE chain_id = 'ethereum-sepolia'")
        .execute(&fixture.pool)
        .await?;
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
    sqlx::raw_sql(BUILDER).execute(&fixture.pool).await?;
    sqlx::raw_sql(STREAM_COMMENTS)
        .execute(&fixture.pool)
        .await?;
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
