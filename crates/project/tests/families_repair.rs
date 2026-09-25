//! The durable boundaries of the family loop (docs/projections.md, "Owned key families"): the
//! input revision each block reads, the repair transitions that share a transaction with the
//! reset, the last undo and the final block, retention against the chain's finality heads, the
//! per-run block budget, a reorg of two blocks, and a key written twice in one block.
mod families_support;

use anyhow::Result;
use bigname_project::{
    Marker,
    families::{self, FamilyMode, FamilyOptions},
};
use families_support::{CHAIN, CONTENT_HASH, Fixture, hash};
use serde_json::{Value, json};

const RESOLVER_A: &str = "0x00000000000000000000000000000000000000a1";
const RESOLVER_B: &str = "0x00000000000000000000000000000000000000b2";

async fn seed(fixture: &Fixture, blocks: std::ops::RangeInclusive<i64>) -> Result<()> {
    for block in blocks {
        fixture.resolver_changed(block, 1, 1, RESOLVER_A).await?;
        fixture
            .resolver_changed(block, 2, block.unsigned_abs(), RESOLVER_B)
            .await?;
    }
    Ok(())
}

async fn sql(fixture: &Fixture, statement: &str) -> Result<()> {
    sqlx::raw_sql(statement).execute(&fixture.pool).await?;
    Ok(())
}

async fn record_state(fixture: &Fixture) -> Result<Option<Value>> {
    Ok(fixture
        .repair_record()
        .await?
        .map(|record| record["state"].clone()))
}

// Each block reads the input token in its own transaction. A revision that changes between two
// blocks of one run stops the run at the block that saw it, counted as a skip; the next run
// adopts it under the same attempt and continues.
#[tokio::test]
async fn a_revision_that_changes_between_two_blocks_stops_the_run() -> Result<()> {
    let fixture = Fixture::new("families_repair_revision", 20).await?;
    seed(&fixture, 11..=14).await?;
    fixture.interpret_row("h1", 1, false).await?;
    fixture.apply(10, FamilyMode::Normal).await;
    sql(
        &fixture,
        "CREATE FUNCTION move_revision() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             UPDATE chain_phase_state SET input_content_hash = 'h2'
             WHERE phase_name = 'interpret';
             RETURN NEW;
         END $$;
         CREATE TRIGGER move_revision AFTER UPDATE ON project_family_marker
         FOR EACH ROW WHEN (NEW.current_block_number = 12)
         EXECUTE FUNCTION move_revision();",
    )
    .await?;
    let stopped = fixture.apply(14, FamilyMode::Normal).await;
    let reason = stopped.skipped.clone().unwrap_or_default();
    assert!(
        reason.contains("revision changed"),
        "the run stops at block 13: {reason}"
    );
    assert_eq!(fixture.marker().await?.0, Some(12));
    assert_eq!(
        fixture.marker_revision().await?,
        (Some("h1".to_owned()), Some(1)),
        "block 12 recorded the revision it read before the change committed with it"
    );

    sql(
        &fixture,
        "DROP TRIGGER move_revision ON project_family_marker",
    )
    .await?;
    let resumed = fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!(resumed.skipped, None);
    assert!(resumed.revision_adopted);
    assert_eq!(fixture.marker().await?.0, Some(14));
    assert_eq!(
        fixture.marker_revision().await?,
        (Some("h2".to_owned()), Some(1))
    );
    fixture.cleanup().await
}

// The same change in the middle of a replay stops it; the next run reopens the repair, undoes
// the replayed prefix, replays under the new revision and completes with it.
#[tokio::test]
async fn a_revision_that_changes_during_a_replay_replays_again_under_it() -> Result<()> {
    let fixture = Fixture::new("families_repair_replay_revision", 20).await?;
    seed(&fixture, 11..=14).await?;
    fixture.interpret_row("h1", 1, false).await?;
    fixture.apply(14, FamilyMode::Normal).await;
    sql(
        &fixture,
        "CREATE FUNCTION move_revision() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             UPDATE chain_phase_state SET input_content_hash = 'h2'
             WHERE phase_name = 'interpret';
             RETURN NEW;
         END $$;
         CREATE TRIGGER move_revision AFTER UPDATE ON project_family_marker
         FOR EACH ROW WHEN (OLD.current_block_number = 12 AND NEW.current_block_number = 13)
         EXECUTE FUNCTION move_revision();",
    )
    .await?;
    fixture.project_row(1, Some((13, 14, "operator"))).await?;
    let stopped = fixture
        .apply(14, FamilyMode::Redo { from: 13, to: 14 })
        .await;
    assert!(stopped.skipped.is_some());
    assert_eq!(fixture.marker().await?.0, Some(13));
    assert_eq!(record_state(&fixture).await?, Some(json!("replaying")));

    sql(
        &fixture,
        "DROP TRIGGER move_revision ON project_family_marker",
    )
    .await?;
    let resumed = fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!(resumed.skipped, None);
    assert_eq!(fixture.marker().await?.0, Some(14));
    let record = fixture.repair_record().await?.unwrap_or_default();
    assert_eq!(
        (
            &record["state"],
            &record["prefix_interpret_input_content_hash"]
        ),
        (&json!("complete"), &json!("h2"))
    );
    let incremental = fixture.snapshot().await?;
    fixture.apply(14, FamilyMode::Rebuild).await;
    assert_eq!(fixture.snapshot().await?, incremental);
    fixture.cleanup().await
}

/// A trigger that fails the transaction that writes `condition` to the repair record.
async fn refuse_record(fixture: &Fixture, condition: &str) -> Result<()> {
    sql(
        fixture,
        &format!(
            "CREATE FUNCTION refuse_record() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'injected'; END $$;
             CREATE TRIGGER refuse_record BEFORE INSERT OR UPDATE ON project_repair_record
             FOR EACH ROW WHEN ({condition}) EXECUTE FUNCTION refuse_record();"
        ),
    )
    .await
}

async fn allow_record(fixture: &Fixture) -> Result<()> {
    sql(
        fixture,
        "DROP TRIGGER refuse_record ON project_repair_record;
         DROP FUNCTION refuse_record();",
    )
    .await
}

// The reset and the rebuild's intent commit together, the last undo commits with the move to
// replaying, and the final block commits with the completion: when the record write fails, the
// families stay exactly as they were before that transaction.
#[tokio::test]
async fn each_repair_transition_commits_with_the_work_it_describes() -> Result<()> {
    let fixture = Fixture::new("families_repair_transitions", 20).await?;
    seed(&fixture, 11..=14).await?;
    fixture.apply(14, FamilyMode::Normal).await;

    // A missed attempt rebuilds; the reset fails with its intent.
    let before = fixture.snapshot().await?;
    fixture.project_row(1, None).await?;
    refuse_record(&fixture, "NEW.state = 'rebuilding'").await?;
    let failed = fixture.apply(14, FamilyMode::Normal).await;
    assert!(failed.skipped.is_some());
    assert_eq!(fixture.snapshot().await?, before, "nothing was reset");
    assert_eq!(fixture.marker().await?.0, Some(14));
    allow_record(&fixture).await?;
    let rebuilt = fixture.apply(14, FamilyMode::Normal).await;
    assert!(rebuilt.reset);
    assert_eq!(record_state(&fixture).await?, Some(json!("complete")));

    // A redo whose last undo fails with the move to replaying keeps that block applied.
    fixture.project_row(2, Some((13, 14, "operator"))).await?;
    refuse_record(&fixture, "NEW.state = 'replaying'").await?;
    let failed = fixture
        .apply(14, FamilyMode::Redo { from: 13, to: 14 })
        .await;
    assert!(failed.skipped.is_some());
    assert_eq!(
        fixture.marker().await?.0,
        Some(13),
        "block 14 is undone; block 13's undo rolled back with the transition"
    );
    assert_eq!(record_state(&fixture).await?, Some(json!("undoing")));
    allow_record(&fixture).await?;

    // The final replayed block fails with the completion.
    refuse_record(&fixture, "NEW.state = 'complete'").await?;
    let failed = fixture.apply(14, FamilyMode::Normal).await;
    assert!(failed.skipped.is_some());
    assert_eq!(fixture.marker().await?.0, Some(13));
    assert_eq!(record_state(&fixture).await?, Some(json!("replaying")));
    allow_record(&fixture).await?;
    let done = fixture.apply(14, FamilyMode::Normal).await;
    assert_eq!(done.skipped, None);
    assert_eq!(fixture.marker().await?.0, Some(14));
    assert_eq!(record_state(&fixture).await?, Some(json!("complete")));
    assert_eq!(fixture.snapshot().await?, before);
    fixture.cleanup().await
}

// A completed redo is recognised only while the marker generation and the input content hash
// it recorded still stand: the same redo under another binary's content hash, or after the
// marker generation moved, runs again.
#[tokio::test]
async fn a_completed_redo_is_recognised_only_under_its_content_hash() -> Result<()> {
    let fixture = Fixture::new("families_repair_completed", 20).await?;
    seed(&fixture, 11..=14).await?;
    fixture.apply(14, FamilyMode::Normal).await;
    fixture.project_row(1, Some((13, 14, "operator"))).await?;
    let redo = fixture
        .apply(14, FamilyMode::Redo { from: 13, to: 14 })
        .await;
    assert_eq!((redo.undone_blocks, redo.blocks), (2, 2));
    let again = fixture
        .apply(14, FamilyMode::Redo { from: 13, to: 14 })
        .await;
    assert_eq!(
        (again.undone_blocks, again.blocks, again.reset),
        (0, 0, false)
    );
    let other = fixture
        .apply_with(
            14,
            FamilyMode::Redo { from: 13, to: 14 },
            &FamilyOptions::new("another-content-hash"),
        )
        .await;
    assert_eq!(other.skipped, None);
    assert!(
        other.blocks > 0,
        "the recorded completion names another content hash"
    );

    // Complete again, then move the marker generation as a block or an undo would.
    fixture
        .apply(14, FamilyMode::Redo { from: 13, to: 14 })
        .await;
    sql(
        &fixture,
        "UPDATE project_family_marker SET sequence = sequence + 1",
    )
    .await?;
    let moved = fixture
        .apply(14, FamilyMode::Redo { from: 13, to: 14 })
        .await;
    assert_eq!(moved.skipped, None);
    assert!(
        moved.blocks > 0,
        "the recorded completion names another marker generation"
    );
    fixture.cleanup().await
}

// Undo rows stay back to the lowest of the depth floor, the finalized head and the safe head,
// here 400 and 300 blocks below a marker of 700; a redo below them rebuilds.
#[tokio::test]
async fn retention_follows_the_finality_heads_and_a_deeper_redo_rebuilds() -> Result<()> {
    let fixture = Fixture::new("families_repair_retention", 700).await?;
    seed(&fixture, 101..=102).await?;
    fixture.heads(700, 400, 300).await?;
    fixture.apply(1, FamilyMode::Normal).await;
    let mut runs = 0;
    while fixture.marker().await?.0 != Some(700) {
        let outcome = fixture.apply(700, FamilyMode::Normal).await;
        assert_eq!(outcome.skipped, None);
        runs += 1;
        assert!(runs < 10, "the follow does not converge");
    }
    assert!(runs >= 3, "one run applies at most 256 blocks");
    let journalled = fixture.journalled_blocks().await?;
    assert_eq!(
        journalled.first(),
        Some(&300),
        "the finalized head, 400 blocks below the marker, bounds the pruning"
    );

    fixture.project_row(1, Some((100, 700, "operator"))).await?;
    let redo = fixture
        .apply(700, FamilyMode::Redo { from: 100, to: 700 })
        .await;
    assert_eq!(redo.skipped, None);
    assert!(redo.reset, "block 100's undo rows were pruned");
    assert_eq!(record_state(&fixture).await?, Some(json!("complete")));
    fixture.cleanup().await
}

// Without a finalized and safe head the loop prunes nothing.
#[tokio::test]
async fn without_finality_heads_no_undo_row_is_pruned() -> Result<()> {
    let fixture = Fixture::new("families_repair_no_heads", 300).await?;
    fixture.apply(1, FamilyMode::Normal).await;
    while fixture.marker().await?.0 != Some(300) {
        let outcome = fixture.apply(300, FamilyMode::Normal).await;
        assert_eq!(outcome.skipped, None);
    }
    let journalled = fixture.journalled_blocks().await?;
    assert!(
        journalled.first().is_some_and(|first| *first <= 2),
        "every followed block keeps its undo rows: {:?}",
        journalled.first()
    );
    fixture.cleanup().await
}

// A rebuild longer than the per-run budget completes over several runs while the served marker
// keeps moving, and equals a rebuild in one run.
#[tokio::test]
async fn a_rebuild_spans_several_runs_while_the_served_marker_moves() -> Result<()> {
    let fixture = Fixture::new("families_repair_budget", 80).await?;
    seed(&fixture, 11..=50).await?;
    let small = FamilyOptions::new(CONTENT_HASH).with_max_blocks_per_run(10);
    let first = fixture.apply_with(50, FamilyMode::Normal, &small).await;
    assert!(first.reset && first.budget_exhausted);
    assert_eq!(record_state(&fixture).await?, Some(json!("rebuilding")));
    let mut target = 50;
    let mut runs = 1;
    while fixture.marker().await?.0 != Some(target) {
        target += 1;
        let outcome = fixture.apply_with(target, FamilyMode::Normal, &small).await;
        assert_eq!(outcome.skipped, None);
        assert!(outcome.blocks <= 10);
        runs += 1;
        assert!(runs < 20, "the rebuild does not converge");
    }
    assert!(runs >= 4, "forty event blocks at ten a run");
    assert_eq!(record_state(&fixture).await?, Some(json!("complete")));
    let incremental = fixture.snapshot().await?;
    fixture.apply(target, FamilyMode::Rebuild).await;
    assert_eq!(fixture.snapshot().await?, incremental);
    fixture.cleanup().await
}

/// A ResolverChanged at a block of hash `block_hash`.
async fn pointer_at(
    fixture: &Fixture,
    block: i64,
    block_hash: &str,
    name: u64,
    resolver: &str,
) -> Result<()> {
    let node = format!("0x{name:064x}");
    let logical_name_id = format!("ens:{node}");
    fixture.surface(&logical_name_id, &node).await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, logical_name_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, before_state, after_state, raw_fact_ref)
         VALUES ($1, 'ens', $2, 'ResolverChanged', 'ens_v1_registry_l1', 1, $3, $4, $5, $6, 0, 1,
                 'ens_v2_registry_resource_surface', 'canonical', '{}'::jsonb,
                 jsonb_build_object('resolver', $7::text, 'node', $8::text),
                 '{\"emitting_address\": \"0x00000000000000000000000000000000000000e1\"}')",
    )
    .bind(format!("replacement-{block}"))
    .bind(&logical_name_id)
    .bind(CHAIN)
    .bind(block)
    .bind(block_hash)
    .bind(format!("0xreplacement-tx{block}"))
    .bind(resolver)
    .bind(&node)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// Blocks 13 and 14 replaced by 13' and 14', each with its own pointer.
async fn reorg(fixture: &Fixture) -> Result<Marker> {
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number IN (13, 14)",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    for (block, parent) in [(13, hash(12)), (14, "0xreplacement13".to_owned())] {
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state)
             VALUES ($1, $2, $3, $4, to_timestamp(1800000000 + $4 * 12), 'canonical')",
        )
        .bind(CHAIN)
        .bind(format!("0xreplacement{block}"))
        .bind(parent)
        .bind(block)
        .execute(&fixture.pool)
        .await?;
        pointer_at(
            fixture,
            block,
            &format!("0xreplacement{block}"),
            30 + block.unsigned_abs(),
            RESOLVER_A,
        )
        .await?;
    }
    Ok(Marker {
        number: 14,
        hash: "0xreplacement14".to_owned(),
    })
}

async fn apply_to(fixture: &Fixture, target: &Marker, mode: FamilyMode) -> families::FamilyOutcome {
    let token = families::input_token(&fixture.pool, CHAIN)
        .await
        .expect("the input token reads");
    families::apply(
        &fixture.pool,
        CHAIN,
        target,
        mode,
        &token,
        &FamilyOptions::new(CONTENT_HASH),
    )
    .await
}

// A reorg that replaces two consecutive blocks undoes them newest first, replays the new branch
// and equals a fresh rebuild of it.
#[tokio::test]
async fn a_two_block_reorg_undoes_newest_first_and_equals_a_fresh_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_repair_reorg", 20).await?;
    seed(&fixture, 11..=14).await?;
    fixture.apply(14, FamilyMode::Normal).await;
    sql(
        &fixture,
        "CREATE TABLE marker_moves (at bigserial, number bigint);
         CREATE FUNCTION log_marker() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN INSERT INTO marker_moves (number) VALUES (NEW.current_block_number); RETURN NEW; END $$;
         CREATE TRIGGER log_marker AFTER UPDATE ON project_family_marker
         FOR EACH ROW EXECUTE FUNCTION log_marker();",
    )
    .await?;
    let target = reorg(&fixture).await?;
    let outcome = apply_to(&fixture, &target, FamilyMode::Normal).await;
    assert_eq!(outcome.skipped, None);
    assert_eq!((outcome.undone_blocks, outcome.blocks), (2, 2));
    let moves: Vec<i64> = sqlx::query_scalar("SELECT number FROM marker_moves ORDER BY at")
        .fetch_all(&fixture.pool)
        .await?;
    assert_eq!(
        moves,
        vec![13, 12, 13, 14],
        "14 then 13 undone, then 13' and 14'"
    );
    let incremental = fixture.snapshot().await?;

    let fresh = Fixture::new("families_repair_reorg_fresh", 20).await?;
    seed(&fresh, 11..=14).await?;
    let target = reorg(&fresh).await?;
    apply_to(&fresh, &target, FamilyMode::Rebuild).await;
    assert_eq!(incremental, fresh.snapshot().await?);
    fresh.cleanup().await?;
    fixture.cleanup().await
}

// One block inserts a key and updates it again, and updates an existing key twice: its undo
// restores the absent key as absent and the existing key as it was.
#[tokio::test]
async fn a_key_written_twice_in_one_block_undoes_to_its_state_before_the_block() -> Result<()> {
    let fixture = Fixture::new("families_repair_twice", 20).await?;
    fixture.resolver_changed(11, 1, 1, RESOLVER_A).await?;
    fixture.resolver_changed(12, 1, 1, RESOLVER_B).await?;
    fixture.resolver_changed(12, 2, 1, RESOLVER_A).await?;
    fixture.resolver_changed(12, 3, 5, RESOLVER_A).await?;
    fixture.resolver_changed(12, 4, 5, RESOLVER_B).await?;
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// A rebuild refreshes the family statistics after 1, 2, 4, 8, ... blocks since its reset, counted
// across runs: a rebuild split over two runs refreshes at each of those points once.
#[tokio::test]
async fn a_rebuild_over_two_runs_refreshes_the_statistics_once_per_threshold() -> Result<()> {
    let fixture = Fixture::new("families_repair_statistics", 40).await?;
    seed(&fixture, 11..=30).await?;
    let small = FamilyOptions::new(CONTENT_HASH).with_max_blocks_per_run(5);
    let first = fixture.apply_with(30, FamilyMode::Rebuild, &small).await;
    assert_eq!((first.skipped.as_deref(), first.blocks), (None, 5));
    assert_eq!(
        first.statistics_refreshes, 3,
        "after 1, 2 and 4 rebuilt blocks"
    );
    let second = fixture.apply_with(30, FamilyMode::Normal, &small).await;
    assert_eq!((second.skipped.as_deref(), second.blocks), (None, 5));
    assert_eq!(
        second.statistics_refreshes, 1,
        "after 8 rebuilt blocks; 1, 2 and 4 were the first run's"
    );
    fixture.cleanup().await
}

// A served rebuild under a new binary whose family run was skipped (a late or failed token read)
// leaves families written under the old content hash. The next run, even a plain follow,
// rebuilds them under its own hash.
#[tokio::test]
async fn families_from_another_content_hash_rebuild_after_a_skipped_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_repair_hash_fence", 20).await?;
    seed(&fixture, 11..=15).await?;
    fixture.apply(14, FamilyMode::Normal).await;
    // The served rebuild's family run under the new binary never ran.
    let rotated = FamilyOptions::new("rotated-content-hash");
    let next = fixture.apply_with(15, FamilyMode::Normal, &rotated).await;
    assert_eq!(next.skipped, None);
    assert!(next.reset, "the families were written by another binary");
    let record = fixture.repair_record().await?.unwrap_or_default();
    assert_eq!(
        (
            &record["state"],
            &record["reason"],
            &record["completed_input_hash"]
        ),
        (
            &json!("complete"),
            &json!("content_hash_rebuild"),
            &json!("rotated-content-hash")
        )
    );
    let hash: Option<String> = sqlx::query_scalar(
        "SELECT input_content_hash FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(hash.as_deref(), Some("rotated-content-hash"));
    let incremental = fixture.snapshot().await?;
    fixture.apply_with(15, FamilyMode::Rebuild, &rotated).await;
    assert_eq!(fixture.snapshot().await?, incremental);
    fixture.cleanup().await
}

/// A resolver discovery edge that starts at `block`, a second work source for that block.
async fn edge_starting_at(fixture: &Fixture, block: i64) -> Result<()> {
    let origin: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v1_registry_l1', $1, 'fixture', 'active', 'fixture',
                 'fixture/registry.yaml', '{\"contracts\": []}'::jsonb)
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    let (from, to) = (
        "00000000-0000-0000-0000-00000000f001",
        "00000000-0000-0000-0000-00000000f002",
    );
    for instance in [from, to] {
        sqlx::query(
            "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
             VALUES ($1::uuid, $2, 'contract')",
        )
        .bind(instance)
        .bind(CHAIN)
        .execute(&fixture.pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address)
         VALUES ($1::uuid, $2, '0x00000000000000000000000000000000000000c7')",
    )
    .bind(to)
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id,
             to_contract_instance_id, discovery_source, admission_basis, source_manifest_id,
             active_from_block_number, active_from_block_hash, canonicality_state)
         VALUES ($1, 'resolver', $2::uuid, $3::uuid, 'NewResolver', 'fixture', $4, $5, $6,
                 'canonical')",
    )
    .bind(CHAIN)
    .bind(from)
    .bind(to)
    .bind(origin)
    .bind(block)
    .bind(hash(block))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

// A rebuild run reads one work block past its budget of three. With fewer work blocks than that
// read it has the whole remainder and adds the target when no work falls on it; with more, it
// applies its budget, reports it spent, and leaves the target to the next run. Every block carries
// two events, and one case adds a discovery edge that starts on an event block: each block is
// work once.
#[tokio::test]
async fn a_rebuild_run_reads_one_work_block_past_its_budget() -> Result<()> {
    let small = FamilyOptions::new(CONTENT_HASH).with_max_blocks_per_run(3);
    // (work blocks, a discovery edge starting at, per run: marker, blocks applied, budget spent)
    type Run = (i64, u64, bool);
    let cases: [(&[i64], Option<i64>, &[Run]); 7] = [
        (&[5, 7], None, &[(12, 3, false)]),
        (&[5, 7, 9], None, &[(9, 3, true), (12, 1, false)]),
        (&[5, 7, 9, 11], None, &[(9, 3, true), (12, 2, false)]),
        (&[5, 12], None, &[(12, 2, false)]),
        (&[5, 7, 12], None, &[(12, 3, false)]),
        (&[5, 7, 9, 12], None, &[(9, 3, true), (12, 1, false)]),
        (&[5, 7, 9], Some(7), &[(9, 3, true), (12, 1, false)]),
    ];
    for (work, edge, runs) in cases {
        let fixture = Fixture::new("families_repair_work_budget", 20).await?;
        for &block in work {
            seed(&fixture, block..=block).await?;
        }
        if let Some(block) = edge {
            edge_starting_at(&fixture, block).await?;
        }
        let mut seen = Vec::new();
        for run in 0..runs.len() {
            let mode = if run == 0 {
                FamilyMode::Rebuild
            } else {
                FamilyMode::Normal
            };
            let outcome = fixture.apply_with(12, mode, &small).await;
            assert_eq!(outcome.skipped, None, "{work:?}");
            seen.push((
                fixture.marker().await?.0.unwrap_or(-1),
                outcome.blocks,
                outcome.budget_exhausted,
            ));
        }
        assert_eq!(seen, runs, "work {work:?}, edge {edge:?}");
        fixture.cleanup().await?;
    }
    Ok(())
}
