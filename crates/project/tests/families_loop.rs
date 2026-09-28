//! The owned key family loop (docs/projections.md, "Owned key families"): one transaction per
//! block after the served batch commits, a compare-and-swap on the shadow marker, a marker journal
//! row on every block, catch-up from a lagging marker, and a failure that stops the loop with an
//! error without touching anything the served batch published.
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use bigname_project::{
    Marker,
    families::{self, FamilyMode},
};
use support::{CHAIN, Fixture, hash, marker};

async fn bootstrapped(prefix: &str) -> Result<Fixture> {
    let fixture = Fixture::new(prefix, 20).await?;
    fixture.apply(10, FamilyMode::Rebuild).await?;
    assert_eq!(fixture.marker().await?.0, Some(10));
    Ok(fixture)
}

#[tokio::test]
async fn catch_up_applies_and_journals_every_block_to_the_served_marker() -> Result<()> {
    let fixture = bootstrapped("families_loop_catch_up").await?;
    let (_, _, sequence) = fixture.marker().await?;
    let outcome = fixture.apply(14, FamilyMode::Normal).await?;
    assert_eq!(outcome.blocks, 4);
    assert_eq!(outcome.lag_blocks(), 0);
    assert_eq!(
        fixture.marker().await?,
        (Some(14), Some(hash(14)), sequence + 4)
    );
    assert_eq!(fixture.journalled_blocks().await?, [10, 11, 12, 13, 14]);
    fixture.cleanup().await
}

#[tokio::test]
async fn a_second_run_at_the_same_head_changes_nothing() -> Result<()> {
    let fixture = bootstrapped("families_loop_same_head").await?;
    fixture.apply(14, FamilyMode::Normal).await?;
    let marker = fixture.marker().await?;
    let snapshot = fixture.snapshot().await?;
    let outcome = fixture.apply(14, FamilyMode::Normal).await?;
    assert_eq!(outcome.blocks, 0);
    assert_eq!(fixture.marker().await?, marker);
    assert_eq!(fixture.snapshot().await?, snapshot);
    fixture.cleanup().await
}

#[tokio::test]
async fn a_lagging_marker_catches_up_from_where_it_stands() -> Result<()> {
    let fixture = bootstrapped("families_loop_lagging").await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    let outcome = fixture.apply(14, FamilyMode::Normal).await?;
    assert_eq!(outcome.blocks, 3, "blocks 12 to 14, from the family marker");
    assert_eq!(fixture.marker().await?.0, Some(14));
    fixture.cleanup().await
}

#[tokio::test]
async fn a_failing_block_stops_the_loop_and_the_next_run_resumes_there() -> Result<()> {
    let fixture = bootstrapped("families_loop_failure").await?;
    // A failure inside block 13's transaction: its first journal write raises.
    sqlx::raw_sql(
        "CREATE FUNCTION fail_block_13() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.block_number = 13 THEN RAISE EXCEPTION 'injected family failure'; END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER fail_block_13 BEFORE INSERT ON project_family_undo
         FOR EACH ROW EXECUTE FUNCTION fail_block_13();",
    )
    .execute(&fixture.pool)
    .await?;
    let error = fixture
        .apply(14, FamilyMode::Normal)
        .await
        .expect_err("the failing block stops the loop")
        .to_string();
    assert!(error.contains("block 13"), "{error}");
    assert!(error.contains("injected family failure"), "{error}");
    assert_eq!(fixture.marker().await?.0, Some(12));
    assert_eq!(fixture.journalled_blocks().await?, [10, 11, 12]);

    sqlx::raw_sql("DROP TRIGGER fail_block_13 ON project_family_undo")
        .execute(&fixture.pool)
        .await?;
    let outcome = fixture.apply(14, FamilyMode::Normal).await?;
    assert_eq!(outcome.blocks, 2);
    assert_eq!(fixture.marker().await?.0, Some(14));
    fixture.cleanup().await
}

// Every block reads the input token inside its own transaction and records it on the marker. No
// block applies while Interpret is in redo: the run stops with an error and the next run resumes.
#[tokio::test]
async fn each_block_records_the_input_token_it_read_and_waits_out_an_interpret_redo() -> Result<()>
{
    let fixture = Fixture::new("families_loop_revision", 20).await?;
    fixture.interpret_row("interpret-hash-a", 3, false).await?;
    fixture.apply(10, FamilyMode::Normal).await?;
    assert_eq!(
        fixture.marker_revision().await?,
        (Some("interpret-hash-a".to_owned()), Some(3))
    );

    fixture.interpret_row("interpret-hash-b", 4, true).await?;
    let error = fixture
        .apply(11, FamilyMode::Normal)
        .await
        .expect_err("no block applies while Interpret is in redo")
        .to_string();
    assert!(error.contains("Interpret is in redo"), "{error}");
    assert_eq!(fixture.marker().await?.0, Some(10));

    fixture.interpret_row("interpret-hash-b", 4, false).await?;
    let resumed = fixture.apply(12, FamilyMode::Normal).await?;
    assert!(
        resumed.revision_adopted,
        "no Project redo followed the rewrite"
    );
    assert_eq!(fixture.marker().await?.0, Some(12));
    assert_eq!(
        fixture.marker_revision().await?,
        (Some("interpret-hash-b".to_owned()), Some(4))
    );
    let token: (Option<bool>, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT interpret_redo_in_progress, project_redo_attempt, admission_manifests
         FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(token, (Some(false), Some(0), Some(String::new())));
    fixture.cleanup().await
}

#[tokio::test]
async fn the_lag_counts_a_marker_off_the_served_branch_or_above_the_target() -> Result<()> {
    let fixture = bootstrapped("families_loop_lag_branch").await?;
    fixture.apply(14, FamilyMode::Normal).await?;

    // The served target drops below the family marker; the families hold two blocks too many.
    let lowered = families::standing(&fixture.pool, CHAIN, &marker(12)).await;
    assert_eq!(lowered.lag_blocks(), 2);

    // Block 14 is replaced by 14' at the same height before the loop runs: every family row is
    // still for the orphaned 14, one block past the branch point.
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 14",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state)
         VALUES ($1, '0xreplacement14', $2, 14, to_timestamp(1800000168), 'canonical')",
    )
    .bind(CHAIN)
    .bind(hash(13))
    .execute(&fixture.pool)
    .await?;
    let replacement = Marker {
        number: 14,
        hash: "0xreplacement14".to_owned(),
    };
    let forked = families::standing(&fixture.pool, CHAIN, &replacement).await;
    assert_eq!(
        forked.marker.as_ref().map(|marker| marker.hash.clone()),
        Some(hash(14))
    );
    assert_eq!(forked.lag_blocks(), 1);
    fixture.cleanup().await
}

// A rebuild does not start while Interpret is in redo: the run returns the wait as an error,
// resets nothing and leaves no marker. The next run after the redo clears rebuilds to the served marker.
#[tokio::test]
async fn a_rebuild_waits_out_an_interpret_redo_and_the_next_run_completes_it() -> Result<()> {
    let fixture = Fixture::new("families_loop_rebuild_wait", 20).await?;
    fixture.interpret_row("interpret-hash-a", 3, true).await?;
    let error = fixture
        .apply(10, FamilyMode::Rebuild)
        .await
        .expect_err("no rebuild starts while Interpret is in redo")
        .to_string();
    assert!(error.contains("Interpret is in redo"), "{error}");
    let applied: Option<i64> = sqlx::query_scalar(
        "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_optional(&fixture.pool)
    .await?
    .flatten();
    assert_eq!(applied, None, "no family block applied");

    fixture.interpret_row("interpret-hash-a", 3, false).await?;
    let rebuilt = fixture.apply(10, FamilyMode::Normal).await?;
    assert!(
        rebuilt.reset,
        "the next run rebuilds the families it never started"
    );
    assert_eq!(fixture.marker().await?.0, Some(10));
    assert_eq!(rebuilt.lag_blocks(), 0);
    fixture.cleanup().await
}
