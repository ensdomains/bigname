//! Undo and the repair record through the family loop: a redo undoes the families to the range's
//! predecessor and replays, a retried redo changes nothing, a redo below the journal's depth
//! rebuilds, a marker left on an orphaned block is undone, and an interrupted replay resumes.
mod families_support;

use anyhow::Result;
use bigname_project::families::{FamilyMode, FamilyOptions};
use families_support::{CHAIN, CONTENT_HASH, Fixture, hash};
use serde_json::{Value, json};

const RESOLVER_A: &str = "0x00000000000000000000000000000000000000a1";
const RESOLVER_B: &str = "0x00000000000000000000000000000000000000b2";

/// Events on blocks 11 to 14; two of them on 13 and 14 are the ones a redo later drops.
async fn seed(fixture: &Fixture) -> Result<Vec<i64>> {
    let mut dropped = Vec::new();
    for block in 11..=14 {
        fixture.resolver_changed(block, 1, 1, RESOLVER_A).await?;
        let id = fixture
            .resolver_changed(block, 2, block.unsigned_abs(), RESOLVER_B)
            .await?;
        if block >= 13 {
            dropped.push(id);
        }
    }
    Ok(dropped)
}

async fn fresh_rebuild(prefix: &str, dropped: bool) -> Result<Value> {
    let fixture = Fixture::new(prefix, 20).await?;
    let ids = seed(&fixture).await?;
    if dropped {
        drop_events(&fixture, &ids).await?;
    }
    fixture.apply(14, FamilyMode::Rebuild).await;
    let snapshot = fixture.snapshot().await?;
    fixture.cleanup().await?;
    Ok(snapshot)
}

async fn drop_events(fixture: &Fixture, ids: &[i64]) -> Result<()> {
    sqlx::query("DELETE FROM normalized_events WHERE normalized_event_id = ANY($1)")
        .bind(ids)
        .execute(&fixture.pool)
        .await?;
    Ok(())
}

#[tokio::test]
async fn a_redo_undoes_to_the_range_predecessor_and_replays_to_a_complete_record() -> Result<()> {
    let fixture = Fixture::new("families_undo_redo", 20).await?;
    let dropped = seed(&fixture).await?;
    let first = fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!(first.skipped, None);
    assert!(first.reset, "the first run populates from scratch");
    assert_eq!(
        fixture
            .repair_record()
            .await?
            .map(|record| record["state"].clone()),
        Some(json!("complete"))
    );

    // Deletion-only redo of 13..=14 under the Project row's next attempt.
    drop_events(&fixture, &dropped).await?;
    fixture.interpret_row("interpret-hash", 2, false).await?;
    fixture
        .project_row(
            1,
            Some((
                13,
                14,
                "required downstream redo: interpret changed 13..=14",
            )),
        )
        .await?;
    let redo = fixture
        .apply(14, FamilyMode::Redo { from: 13, to: 14 })
        .await;
    assert_eq!(redo.skipped, None);
    assert!(!redo.reset);
    assert_eq!(redo.undone_blocks, 2, "blocks 14 and 13 are undone");
    assert_eq!(redo.blocks, 2, "blocks 13 and 14 are replayed");
    assert_eq!(
        fixture.snapshot().await?,
        fresh_rebuild("families_undo_redo_fresh", true).await?,
        "the redone families equal a fresh rebuild at 14"
    );
    let (number, marker_hash, sequence) = fixture.marker().await?;
    assert_eq!(
        (number, marker_hash.as_deref()),
        (Some(14), Some(hash(14).as_str()))
    );
    assert_eq!(
        fixture.repair_record().await?,
        Some(json!({
            "attempt": 1,
            "reason": "required_redo_range",
            "state": "complete",
            "trusted_base_number": 12,
            "trusted_base_hash": hash(12),
            "replay_target_number": 14,
            "replay_target_hash": hash(14),
            "prefix_interpret_input_content_hash": "interpret-hash",
            "prefix_interpret_redo_attempt": 2,
            "prefix_recorded": true,
            "invalidation_from": null,
            "pending_undo_target": null,
            "completed_sequence": sequence,
            "completed_marker_number": 14,
            "completed_marker_hash": hash(14),
            "completed_input_hash": CONTENT_HASH,
        }))
    );

    // The same redo again finds the record complete at the target and changes nothing.
    let before = (fixture.snapshot().await?, fixture.repair_record().await?);
    let retry = fixture
        .apply(14, FamilyMode::Redo { from: 13, to: 14 })
        .await;
    assert_eq!(
        (retry.blocks, retry.undone_blocks, retry.reset),
        (0, 0, false)
    );
    assert_eq!(
        (fixture.snapshot().await?, fixture.repair_record().await?),
        before
    );
    assert_eq!(
        fixture.marker().await?.2,
        sequence,
        "no block and no undo ran"
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn a_redo_below_the_retained_journal_rebuilds_from_scratch() -> Result<()> {
    let fixture = Fixture::new("families_undo_depth", 20).await?;
    let dropped = seed(&fixture).await?;
    let shallow = FamilyOptions::new(CONTENT_HASH).with_retained_undo_depth(2);
    fixture.heads(20, 20, 20).await?;
    fixture.apply_with(14, FamilyMode::Normal, &shallow).await;
    assert_eq!(fixture.journalled_blocks().await?, vec![12, 13, 14]);

    drop_events(&fixture, &dropped).await?;
    fixture.project_row(1, Some((11, 14, "operator"))).await?;
    let redo = fixture
        .apply_with(14, FamilyMode::Redo { from: 11, to: 14 }, &shallow)
        .await;
    assert_eq!(redo.skipped, None);
    assert!(redo.reset, "block 11's journal was pruned");
    assert_eq!(redo.undone_blocks, 0);
    assert_eq!(
        fixture.snapshot().await?,
        fresh_rebuild("families_undo_depth_fresh", true).await?
    );
    let record = fixture.repair_record().await?.unwrap_or_default();
    assert_eq!(
        (
            &record["state"],
            &record["reason"],
            &record["attempt"],
            &record["trusted_base_number"]
        ),
        (
            &json!("complete"),
            &json!("operator_redo"),
            &json!(1),
            &Value::Null
        )
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn a_redo_attempt_the_families_never_saw_rebuilds_them() -> Result<()> {
    let fixture = Fixture::new("families_undo_missed", 20).await?;
    seed(&fixture).await?;
    fixture.apply(14, FamilyMode::Normal).await;
    // The Project row moved on to attempt 1 while the families were not running.
    fixture.project_row(1, None).await?;
    let normal = fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!(normal.skipped, None);
    assert!(normal.reset);
    assert_eq!(
        fixture
            .repair_record()
            .await?
            .map(|record| record["attempt"].clone()),
        Some(json!(1))
    );
    // With the attempt recorded, the next run just follows.
    let next = fixture.apply(15, FamilyMode::Normal).await;
    assert_eq!((next.reset, next.blocks), (false, 1));
    fixture.cleanup().await
}

#[tokio::test]
async fn a_marker_on_an_orphaned_block_is_undone_and_the_new_branch_replayed() -> Result<()> {
    let fixture = Fixture::new("families_undo_orphan", 20).await?;
    seed(&fixture).await?;
    fixture.apply(14, FamilyMode::Normal).await;
    // Block 14 is replaced by 14' on the readable lineage.
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
    let target = bigname_project::Marker {
        number: 14,
        hash: "0xreplacement14".to_owned(),
    };
    let token = bigname_project::families::input_token(&fixture.pool, CHAIN).await?;
    let outcome = bigname_project::families::apply(
        &fixture.pool,
        CHAIN,
        &target,
        FamilyMode::Normal,
        &token,
        &FamilyOptions::new(CONTENT_HASH),
    )
    .await;
    assert_eq!(outcome.skipped, None);
    assert_eq!(
        (outcome.undone_blocks, outcome.blocks, outcome.reset),
        (1, 1, false)
    );
    let (number, marker_hash, _) = fixture.marker().await?;
    assert_eq!(
        (number, marker_hash.as_deref()),
        (Some(14), Some("0xreplacement14"))
    );
    let record = fixture.repair_record().await?.unwrap_or_default();
    assert_eq!(
        (
            &record["state"],
            &record["reason"],
            &record["trusted_base_number"]
        ),
        (&json!("complete"), &json!("orphaned_lineage"), &json!(13))
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn an_interrupted_replay_resumes_on_the_next_run() -> Result<()> {
    let fixture = Fixture::new("families_undo_resume", 20).await?;
    seed(&fixture).await?;
    fixture.apply(14, FamilyMode::Normal).await;
    // The replay of block 14 fails once.
    sqlx::raw_sql(
        "CREATE FUNCTION refuse_fourteen() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.current_block_number = 14 THEN RAISE EXCEPTION 'injected'; END IF;
             RETURN NEW;
         END $$;
         CREATE TRIGGER refuse_fourteen BEFORE UPDATE ON project_family_marker
         FOR EACH ROW EXECUTE FUNCTION refuse_fourteen();",
    )
    .execute(&fixture.pool)
    .await?;
    fixture.project_row(1, Some((13, 14, "operator"))).await?;
    let stopped = fixture
        .apply(14, FamilyMode::Redo { from: 13, to: 14 })
        .await;
    assert!(stopped.skipped.is_some());
    assert_eq!(fixture.marker().await?.0, Some(13));
    assert_eq!(
        fixture
            .repair_record()
            .await?
            .map(|record| record["state"].clone()),
        Some(json!("replaying"))
    );

    sqlx::query("DROP TRIGGER refuse_fourteen ON project_family_marker")
        .execute(&fixture.pool)
        .await?;
    let resumed = fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!(resumed.skipped, None);
    assert_eq!(
        (resumed.blocks, resumed.undone_blocks, resumed.reset),
        (1, 0, false)
    );
    let record = fixture.repair_record().await?.unwrap_or_default();
    assert_eq!(
        (
            &record["state"],
            &record["attempt"],
            &record["completed_marker_number"]
        ),
        (&json!("complete"), &json!(1), &json!(14))
    );
    fixture.cleanup().await
}
