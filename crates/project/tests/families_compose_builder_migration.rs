//! The schema-migration that gives the composition pass its history catalogue build
//! (migrations/20261027120000_project_compose_history_builder.sql): the work list's `composed`
//! and `relations` columns with the index over the names still to compose, the build's work table
//! `project_compose_history_work`, and the new meaning of the record's stream and cursor. Without
//! it a family run of this build fails in the names stream. On the schema as it stood before, the
//! migration keeps every row, and with the comment-only migration after it
//! (migrations/20261027130000_project_compose_stream_comments.sql), which names the lookup
//! inventories stream and the lookup name's serving resource window, leaves the tables as the
//! baseline defines them. Applied again they change nothing, and on a database with no phase
//! schema they do nothing.
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use bigname_project::families::{FamilyMode, FamilyOptions, RebuildRanges};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use support::{CHAIN, CONTENT_HASH, Fixture};

const MIGRATION: &str =
    include_str!("../../../migrations/20261027120000_project_compose_history_builder.sql");
const STREAM_COMMENTS: &str =
    include_str!("../../../migrations/20261027130000_project_compose_stream_comments.sql");
/// The work list and the record's comments as the release before this one defines them.
const PREDECESSOR: &str = "DROP TABLE project_compose_history_work;
     DROP INDEX project_compose_name_work_uncomposed_idx;
     ALTER TABLE project_compose_name_work DROP COLUMN composed, DROP COLUMN relations;
     COMMENT ON COLUMN project_repair_record.compose_stream IS
         'This value is the composition pass stream under way: 0 names, 1 record nodes, 2 record ids, 3 history windows; null outside a pass.';
     COMMENT ON COLUMN project_repair_record.compose_cursor IS
         'This value is the last key the composition pass committed in its stream, a work list serial in the names stream; null at a stream''s start and outside a pass.';
     COMMENT ON COLUMN project_lookup_name.record_serving_resource_id IS
         'Resource whose factored record inventory serves this name, or null when absent.';";
/// The lookup name's serving resource comment, which the comment-only migration gives the window
/// in which the names stream leaves it null.
const SERVING_COMMENT: &str = "SELECT col_description('project_lookup_name'::regclass, attnum)
     FROM pg_attribute WHERE attrelid = 'project_lookup_name'::regclass
       AND attname = 'record_serving_resource_id'";
const R1: &str = "0x00000000000000000000000000000000000000a1";
const TABLES: &str = "'project_repair_record', 'project_compose_name_work', \
                      'project_compose_history_work'";

/// The three tables' columns with their defaults and comments, their comments, constraints and
/// indexes.
async fn shape(fixture: &Fixture) -> Result<Vec<Vec<String>>> {
    let mut shape = Vec::new();
    for query in [
        format!(
            "SELECT relation.relname || '.' || attribute.attname || ' '
                    || format_type(attribute.atttypid, attribute.atttypmod)
                    || CASE WHEN attribute.attnotnull THEN ' not null' ELSE '' END
                    || coalesce(' default ' || pg_get_expr(fallback.adbin, fallback.adrelid), '')
                    || coalesce(' -- ' || col_description(attribute.attrelid, attribute.attnum), '')
             FROM pg_attribute attribute
             JOIN pg_class relation ON relation.oid = attribute.attrelid
             LEFT JOIN pg_attrdef fallback
               ON fallback.adrelid = attribute.attrelid AND fallback.adnum = attribute.attnum
             WHERE relation.relname IN ({TABLES})
               AND relation.relnamespace = 'bigname_phase'::regnamespace
               AND attribute.attnum > 0 AND NOT attribute.attisdropped
             ORDER BY 1"
        ),
        format!(
            "SELECT relation.relname || ' -- ' || coalesce(obj_description(relation.oid, 'pg_class'), '')
             FROM pg_class relation
             WHERE relation.relname IN ({TABLES})
               AND relation.relnamespace = 'bigname_phase'::regnamespace
             ORDER BY 1"
        ),
        format!(
            "SELECT relation.relname || ' ' || pg_get_constraintdef(constraint_row.oid)
             FROM pg_constraint constraint_row
             JOIN pg_class relation ON relation.oid = constraint_row.conrelid
             WHERE relation.relname IN ({TABLES})
               AND relation.relnamespace = 'bigname_phase'::regnamespace
             ORDER BY 1"
        ),
        format!(
            "SELECT indexname || ' ' || indexdef FROM pg_indexes
             WHERE schemaname = 'bigname_phase' AND tablename IN ({TABLES})
             ORDER BY 1"
        ),
    ] {
        shape.push(
            sqlx::query_scalar(&query)
                .fetch_all(&fixture.pool)
                .await?,
        );
    }
    Ok(shape)
}

/// A ranged rebuild of a few pointer blocks to 20, which composes in a pass.
async fn rebuild(
    fixture: &Fixture,
) -> bigname_project::Result<bigname_project::families::FamilyOutcome> {
    fixture
        .apply_with(
            20,
            FamilyMode::Rebuild,
            &FamilyOptions::new(CONTENT_HASH)
                .with_rebuild_ranges(RebuildRanges::Through(20))
                .with_compose_builder_partitions(2, 2),
        )
        .await
}

#[tokio::test]
async fn the_migration_gives_the_pass_its_catalogue_build() -> Result<()> {
    let fixture = Fixture::new("families_compose_builder_migration", 30).await?;
    for block in 10..=14 {
        fixture.resolver_changed(block, 1, 1, R1).await?;
    }
    let baseline = shape(&fixture).await?;
    let serving: String = sqlx::query_scalar(SERVING_COMMENT)
        .fetch_one(&fixture.pool)
        .await?;
    sqlx::raw_sql(PREDECESSOR).execute(&fixture.pool).await?;
    sqlx::query(
        "INSERT INTO project_compose_name_work (chain_id, serial, logical_name_id, reach)
         VALUES ('ethereum-holesky', 1, 'ens:0x01', 2)",
    )
    .execute(&fixture.pool)
    .await?;
    ensure!(
        rebuild(&fixture).await.is_err(),
        "a family run fails on the predecessor work list"
    );

    sqlx::raw_sql(MIGRATION).execute(&fixture.pool).await?;
    let stream_comment = "SELECT col_description('project_repair_record'::regclass, attnum)
         FROM pg_attribute WHERE attrelid = 'project_repair_record'::regclass
           AND attname = 'compose_stream'";
    let before: String = sqlx::query_scalar(stream_comment)
        .fetch_one(&fixture.pool)
        .await?;
    ensure!(
        !before.contains("4 lookup inventories"),
        "the catalogue build's migration alone leaves the stream comment without the \
         inventories stream: {before}"
    );
    sqlx::raw_sql(STREAM_COMMENTS)
        .execute(&fixture.pool)
        .await?;
    ensure!(
        shape(&fixture).await? == baseline,
        "the migrated tables are the baseline's: {:?} against {baseline:?}",
        shape(&fixture).await?
    );
    let migrated: String = sqlx::query_scalar(SERVING_COMMENT)
        .fetch_one(&fixture.pool)
        .await?;
    ensure!(
        migrated == serving && serving.contains("lookup inventories stream"),
        "the serving resource comment is the baseline's: {migrated} against {serving}"
    );
    let kept: (bool, Option<serde_json::Value>) = sqlx::query_as(
        "SELECT composed, relations FROM project_compose_name_work
         WHERE chain_id = 'ethereum-holesky' AND serial = 1",
    )
    .fetch_one(&fixture.pool)
    .await?;
    ensure!(kept == (false, None), "the row is kept: {kept:?}");
    sqlx::raw_sql(MIGRATION).execute(&fixture.pool).await?;
    sqlx::raw_sql(STREAM_COMMENTS)
        .execute(&fixture.pool)
        .await?;
    let again: String = sqlx::query_scalar(SERVING_COMMENT)
        .fetch_one(&fixture.pool)
        .await?;
    ensure!(
        shape(&fixture).await? == baseline && again == serving,
        "applied again, the migrations change nothing"
    );
    sqlx::query("DELETE FROM project_compose_name_work WHERE chain_id = 'ethereum-holesky'")
        .execute(&fixture.pool)
        .await?;

    let outcome = rebuild(&fixture).await?;
    ensure!(
        outcome.compose_chunks > 1 + 2 + 5 * 2 + 1,
        "the migrated schema holds a pass and its build: {outcome:?}"
    );
    let composed: Option<i64> =
        sqlx::query_scalar("SELECT composed_block FROM project_repair_record WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(&fixture.pool)
            .await?;
    ensure!(composed == Some(14), "{composed:?}");
    fixture.cleanup().await?;

    // An empty schema-migration database has no phase schema: the migration does nothing.
    let empty =
        TestDatabase::create(TestDatabaseConfig::new("families_compose_builder_empty")).await?;
    let applied = sqlx::raw_sql(MIGRATION).execute(empty.pool()).await;
    let commented = sqlx::raw_sql(STREAM_COMMENTS).execute(empty.pool()).await;
    let present: Option<String> = sqlx::query_scalar(
        "SELECT to_regclass('bigname_phase.project_compose_history_work')::text",
    )
    .fetch_one(empty.pool())
    .await?;
    empty.cleanup().await?;
    ensure!(
        applied.is_ok() && commented.is_ok() && present.is_none(),
        "a no-op on an empty database: {applied:?} {commented:?} {present:?}"
    );
    Ok(())
}
