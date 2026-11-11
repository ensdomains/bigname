//! `docs/phase-state.md` § Transitions, the required-redo stamps: `CREATE_STAMP` and
//! `EXTEND_STAMP` (`redo_stamp.rs:16, :37`) and the hand copy in
//! `crates/manifests/src/lib/required_ingest.rs:95, :118`.
use anyhow::{Result, ensure};
use bigname_manifests::RequiredIngestCause;
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{
    EARLIER_FAILURE, PENDING, RANGE, Row, Scratch, State, completed, expect_check_refusal,
    expect_state, expect_unchanged, failed, idle, marker, operator_redo, paused, read, redo,
    refusal, required_active, running, seed, unchanged_except, with_redo_progress, writes,
};
use crate::phase::{BlockRange, PhaseName, PhaseProgress, VerificationLevel};

const REASON: &str = "test stamp";
const STAMPED: &str = "required downstream redo: test stamp";
const MANIFEST_REASON: &str =
    "required downstream redo: manifest watch plan widened over an already-ingested range";
const MIXED_REASON: &str =
    "required downstream redo: discovery demand merged with operator-owned Ingest work";

/// `stamp_required_in_transaction`, the function every runner stamp goes through: it picks
/// `CREATE_STAMP` or `EXTEND_STAMP` and clips the range to the row's cursor.
async fn stamp(pool: &PgPool, chain: &str, phase: PhaseName, from: i64) -> Result<bool> {
    let mut transaction = pool.begin().await?;
    let stamped = crate::redo_stamp::stamp_required_in_transaction(
        &mut transaction,
        chain,
        phase,
        BlockRange { from, to: i64::MAX },
        REASON,
    )
    .await?;
    transaction.commit().await?;
    Ok(stamped)
}

async fn seeded(pool: &PgPool, chain: &str, row: Row) -> Result<Value> {
    let phase: PhaseName = row.get("phase_name").as_str().unwrap_or_default().parse()?;
    seed(pool, chain, &[row]).await?;
    read(pool, chain, phase).await
}

fn range(row: &Value) -> (Value, Value) {
    (
        row["redo_from_block_number"].clone(),
        row["redo_to_block_number"].clone(),
    )
}

#[tokio::test]
async fn create_stamp_saves_the_lifecycle_it_finds() -> Result<()> {
    let scratch = Scratch::create("phase_state_create_stamp").await?;
    let pool = scratch.pool();
    let interpret = PhaseName::Interpret;

    // Completed and failed rows: the saved lifecycle is the one a redo finish restores.
    for (chain, row, saved_status, saved_error, to) in [
        (
            "create-completed",
            completed("create-completed", interpret),
            "completed",
            Value::Null,
            100,
        ),
        (
            "create-failed",
            failed("create-failed", interpret),
            "failed",
            json!(EARLIER_FAILURE),
            60,
        ),
    ] {
        let before = seeded(pool, chain, row).await?;
        ensure!(stamp(pool, chain, interpret, 20).await?, "{chain}");
        let after = read(pool, chain, interpret).await?;
        expect_state(&after, State::RequiredPending)?;
        ensure!(after["last_error"] == json!(STAMPED));
        ensure!(range(&after) == (json!(20), json!(to)), "{chain}: {after}");
        ensure!(after["redo_previous_phase_status"] == json!(saved_status));
        ensure!(after["redo_previous_last_error"] == saved_error);
        ensure!(after["redo_previous_started_at"] == before["started_at"]);
        ensure!(after["redo_previous_finished_at"] == before["finished_at"]);
        ensure!(after["redo_attempt_generation"] == json!(1));
        unchanged_except(&before, &after, writes::CREATE_STAMP)?;
    }

    // Project records the stamped range as its requested range too.
    let chain = "create-project";
    let before = seeded(pool, chain, completed(chain, PhaseName::Project)).await?;
    ensure!(stamp(pool, chain, PhaseName::Project, 20).await?);
    let after = read(pool, chain, PhaseName::Project).await?;
    expect_state(&after, State::RequiredPending)?;
    ensure!(after["redo_requested_from_block_number"] == json!(20));
    ensure!(after["redo_requested_to_block_number"] == json!(100));
    unchanged_except(&before, &after, writes::CREATE_STAMP)?;

    // Pinned finding (TYR-511): neither the statement nor `stamp` reads `phase_status`, so a
    // running or paused normal run is stamped. The CHECKs accept the row; its writer cannot then
    // complete it.
    let verify = PhaseName::Verify;
    for (chain, row) in [
        ("create-running", running("create-running", verify)),
        ("create-paused", paused("create-paused", verify)),
    ] {
        let before = seeded(pool, chain, row).await?;
        ensure!(stamp(pool, chain, verify, 20).await?, "{chain}");
        let after = read(pool, chain, verify).await?;
        expect_state(&after, State::RequiredPending)?;
        ensure!(after["redo_previous_phase_status"] == before["phase_status"]);
        ensure!(after["phase_status"] == json!("running"));
        unchanged_except(&before, &after, writes::CREATE_STAMP)?;
    }
    let stamped = read(pool, "create-running", verify).await?;
    let mut connection = pool.acquire().await?;
    let result = scratch
        .store()
        .complete_phase_on_connection(
            &mut connection,
            "create-running",
            verify,
            &PhaseProgress {
                current: Some(marker(100)),
                target: Some(marker(100)),
                verification_level: Some(VerificationLevel::QuickSynced),
                ..PhaseProgress::default()
            },
        )
        .await;
    drop(connection);
    let error = refusal(result, "create-running completion")?;
    expect_check_refusal(&error.to_string(), "chain_phase_state_check4")?;
    expect_unchanged(&stamped, &read(pool, "create-running", verify).await?)?;

    // No stamp: a row with no cursor, or a cursor below the range.
    for (chain, row) in [
        ("create-idle", idle("create-idle", interpret)),
        (
            "create-below",
            completed("create-below", interpret)
                .set("current_block_number", json!(10))
                .set("current_block_hash", json!("h10")),
        ),
    ] {
        let before = seeded(pool, chain, row).await?;
        ensure!(!stamp(pool, chain, interpret, 20).await?, "{chain}");
        expect_unchanged(&before, &read(pool, chain, interpret).await?)?;
    }

    // Ingest and Live are never stamped by the runner.
    for phase in [PhaseName::Ingest, PhaseName::Live] {
        let chain = format!("create-{phase}");
        let before = seeded(pool, &chain, completed(&chain, phase)).await?;
        let mut transaction = pool.begin().await?;
        let result = crate::redo_stamp::stamp_required_in_transaction(
            &mut transaction,
            &chain,
            phase,
            BlockRange {
                from: 20,
                to: i64::MAX,
            },
            REASON,
        )
        .await;
        transaction.rollback().await?;
        refusal(result, &chain)?;
        expect_unchanged(&before, &read(pool, &chain, phase).await?)?;
    }

    // The statement's own guards, without `stamp`'s checks in front of it.
    for (chain, row) in [
        (
            "create-direct-active",
            operator_redo("create-direct-active", interpret),
        ),
        ("create-direct-idle", idle("create-direct-idle", interpret)),
    ] {
        let before = seeded(pool, chain, row).await?;
        let result = sqlx::query(crate::redo_stamp::CREATE_STAMP)
            .bind(chain)
            .bind(interpret.as_str())
            .bind(20_i64)
            .bind(100_i64)
            .bind(STAMPED)
            .execute(pool)
            .await?;
        ensure!(result.rows_affected() == 0, "{chain}");
        expect_unchanged(&before, &read(pool, chain, interpret).await?)?;
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn extend_stamp_widens_and_keeps_ownership() -> Result<()> {
    let scratch = Scratch::create("phase_state_extend_stamp").await?;
    let pool = scratch.pool();
    let interpret = PhaseName::Interpret;

    // The merged range is the union, clipped to the cursor (100). Progress clears; the owner,
    // mode, status and start time stay.
    for (chain, row, expected) in [
        (
            "extend-operator",
            with_redo_progress(operator_redo("extend-operator", interpret)),
            State::OperatorRedo,
        ),
        (
            "extend-operator-paused",
            operator_redo("extend-operator-paused", interpret).set("phase_status", json!("paused")),
            State::OperatorRedo,
        ),
        (
            "extend-active",
            with_redo_progress(required_active("extend-active", interpret)),
            State::RequiredActive,
        ),
        // Pinned finding: a required range merged into a flag recomputation keeps its mode.
        (
            "extend-recompute",
            redo(
                completed("extend-recompute", interpret),
                "recompute_flags",
                None,
            ),
            State::RecomputeRedo,
        ),
    ] {
        let before = seeded(pool, chain, row).await?;
        ensure!(stamp(pool, chain, interpret, 50).await?, "{chain}");
        let after = read(pool, chain, interpret).await?;
        expect_state(&after, expected)?;
        ensure!(
            range(&after) == (json!(RANGE.from), json!(100)),
            "{chain}: {after}"
        );
        ensure!(after["redo_current_block_number"].is_null());
        ensure!(after["redo_attempt_generation"] == json!(2));
        unchanged_except(&before, &after, writes::EXTEND_STAMP)?;
    }

    // A stamp inside the range moves only the generation and the progress.
    let chain = "extend-inside";
    let before = seeded(
        pool,
        chain,
        with_redo_progress(operator_redo(chain, PhaseName::Verify)),
    )
    .await?;
    let mut transaction = pool.begin().await?;
    ensure!(
        crate::redo_stamp::stamp_required_in_transaction(
            &mut transaction,
            chain,
            PhaseName::Verify,
            BlockRange { from: 20, to: 30 },
            REASON,
        )
        .await?
    );
    transaction.commit().await?;
    let after = read(pool, chain, PhaseName::Verify).await?;
    expect_state(&after, State::OperatorRedo)?;
    ensure!(range(&after) == range(&before));
    unchanged_except(&before, &after, writes::EXTEND_STAMP)?;

    // Project widens its requested range by LEAST and GREATEST.
    let chain = "extend-project";
    let before = seeded(
        pool,
        chain,
        operator_redo(chain, PhaseName::Project)
            .set("redo_requested_from_block_number", json!(30))
            .set("redo_requested_to_block_number", json!(60)),
    )
    .await?;
    ensure!(stamp(pool, chain, PhaseName::Project, 50).await?);
    let after = read(pool, chain, PhaseName::Project).await?;
    ensure!(after["redo_requested_from_block_number"] == json!(30));
    ensure!(after["redo_requested_to_block_number"] == json!(100));
    ensure!(range(&after) == (json!(RANGE.from), json!(100)));
    unchanged_except(&before, &after, writes::EXTEND_STAMP)?;

    // The statement's own guard: no marker, no row.
    let chain = "extend-direct-completed";
    let before = seeded(pool, chain, completed(chain, interpret)).await?;
    let result = sqlx::query(crate::redo_stamp::EXTEND_STAMP)
        .bind(chain)
        .bind(interpret.as_str())
        .bind(10_i64)
        .bind(100_i64)
        .bind(50_i64)
        .bind(100_i64)
        .execute(pool)
        .await?;
    ensure!(result.rows_affected() == 0);
    expect_unchanged(&before, &read(pool, chain, interpret).await?)?;
    scratch.cleanup().await
}

async fn install(
    pool: &PgPool,
    chain: &str,
    widened_from: i64,
    cause: RequiredIngestCause,
) -> Result<bool> {
    sqlx::query(
        "INSERT INTO ingest_cursors
             (chain_id, source_key, source_kind, seed_basis, start_block_number,
              next_block_number)
         VALUES ($1, 'rpc', 'rpc', 'ethereum_head', 0, 101)
         ON CONFLICT (chain_id, source_key) DO NOTHING",
    )
    .bind(chain)
    .execute(pool)
    .await?;
    let mut transaction = pool.begin().await?;
    let installed =
        bigname_manifests::install_required_ingest(&mut transaction, chain, widened_from, cause)
            .await?;
    transaction.commit().await?;
    Ok(installed)
}

#[tokio::test]
async fn required_ingest_copy_stamps_and_rewrites_ownership() -> Result<()> {
    let scratch = Scratch::create("phase_state_required_ingest").await?;
    let pool = scratch.pool();
    let ingest = PhaseName::Ingest;
    let manifest = RequiredIngestCause::ManifestWatchWidening;

    // The copy of `CREATE_STAMP`. Like it, it reads no `phase_status`, so a running Ingest run is
    // stamped as well (TYR-511's predicate, in the copy).
    for (chain, row, to) in [
        (
            "ingest-create-completed",
            completed("ingest-create-completed", ingest),
            100,
        ),
        (
            "ingest-create-running",
            running("ingest-create-running", ingest),
            60,
        ),
    ] {
        let before = seeded(pool, chain, row).await?;
        ensure!(install(pool, chain, 40, manifest).await?, "{chain}");
        let after = read(pool, chain, ingest).await?;
        expect_state(&after, State::RequiredPending)?;
        ensure!(after["last_error"] == json!(MANIFEST_REASON));
        ensure!(range(&after) == (json!(40), json!(to)), "{chain}: {after}");
        ensure!(after["redo_previous_phase_status"] == before["phase_status"]);
        unchanged_except(&before, &after, writes::INGEST_CREATE)?;
    }

    // The widen. Unlike `EXTEND_STAMP`, which keeps `last_error`, it writes the requested owner:
    // an operator's Ingest redo becomes required work, and an active required redo becomes
    // pending (pinned findings).
    for (chain, row, cause, owner, expected) in [
        (
            "ingest-widen-operator",
            with_redo_progress(operator_redo("ingest-widen-operator", ingest)),
            manifest,
            MANIFEST_REASON,
            State::RequiredPending,
        ),
        (
            "ingest-widen-active",
            required_active("ingest-widen-active", ingest),
            manifest,
            MANIFEST_REASON,
            State::RequiredPending,
        ),
        (
            "ingest-widen-active-discovery",
            required_active("ingest-widen-active-discovery", ingest),
            RequiredIngestCause::DiscoveryWatchAdmission,
            MIXED_REASON,
            State::RequiredPending,
        ),
        (
            "ingest-widen-pending",
            redo(
                completed("ingest-widen-pending", ingest),
                "redo",
                Some(PENDING),
            ),
            RequiredIngestCause::DiscoveryWatchAdmission,
            PENDING,
            State::RequiredPending,
        ),
    ] {
        let before = seeded(pool, chain, row).await?;
        ensure!(install(pool, chain, 40, cause).await?, "{chain}");
        let after = read(pool, chain, ingest).await?;
        expect_state(&after, expected)?;
        ensure!(after["last_error"] == json!(owner), "{chain}: {after}");
        ensure!(
            range(&after) == (json!(RANGE.from), json!(100)),
            "{chain}: {after}"
        );
        ensure!(after["redo_current_block_number"].is_null());
        ensure!(after["redo_attempt_generation"] == json!(2));
        unchanged_except(&before, &after, writes::INGEST_WIDEN)?;
    }

    // No cursor, no stamp.
    let chain = "ingest-idle";
    let before = seeded(pool, chain, idle(chain, ingest)).await?;
    ensure!(!install(pool, chain, 40, manifest).await?);
    expect_unchanged(&before, &read(pool, chain, ingest).await?)?;
    scratch.cleanup().await
}

#[tokio::test]
async fn the_project_stamp_after_an_interpret_redo_is_unclipped_and_extend_keeps_a_higher_end()
-> Result<()> {
    let scratch = Scratch::create("phase_state_unclipped_stamp").await?;
    let pool = scratch.pool();
    let project = PhaseName::Project;

    // Project at 60 on another projection content hash, Ingest complete through 100, the first
    // ingested block 10: an Interpret redo's completion stamps Project over 10..=100, above its
    // cursor, through `stamp_unclipped_in_transaction` (`redo_hash_adoption.rs:150-160`).
    let chain = "unclipped-project";
    seed(
        pool,
        chain,
        &[
            completed(chain, PhaseName::Ingest),
            completed(chain, PhaseName::Interpret),
            completed(chain, project)
                .set("current_block_number", json!(60))
                .set("current_block_hash", json!("h60"))
                .set("target_block_number", json!(60))
                .set("target_block_hash", json!("h60"))
                .set("input_content_hash", json!("old-projection-hash")),
        ],
    )
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (chain_id, source_key, source_kind, seed_basis,
                                     start_block_number, next_block_number)
         VALUES ($1, 'rpc', 'rpc', 'ethereum_head', 10, 101)",
    )
    .bind(chain)
    .execute(pool)
    .await?;
    let before = read(pool, chain, project).await?;
    let mut transaction = pool.begin().await?;
    ensure!(
        crate::redo_hash_adoption::stamp_project_after_interpret(
            &mut transaction,
            chain,
            BlockRange { from: 20, to: 30 },
            None,
        )
        .await?
    );
    transaction.commit().await?;
    let after = read(pool, chain, project).await?;
    expect_state(&after, State::RequiredPending)?;
    ensure!(range(&after) == (json!(10), json!(100)), "{after}");
    ensure!(after["redo_requested_from_block_number"] == json!(10));
    ensure!(after["redo_requested_to_block_number"] == json!(100));
    ensure!(after["current_block_number"] == json!(60));
    unchanged_except(&before, &after, writes::CREATE_STAMP)?;

    // A later clipped stamp on that redo: the new end is clipped to 60, and the recorded end
    // above the cursor stays (`redo_stamp.rs:222-228`).
    let before = after;
    ensure!(stamp(pool, chain, project, 20).await?);
    let after = read(pool, chain, project).await?;
    expect_state(&after, State::RequiredPending)?;
    ensure!(range(&after) == (json!(10), json!(100)), "{after}");
    ensure!(after["redo_requested_to_block_number"] == json!(100));
    ensure!(after["redo_attempt_generation"] == json!(2));
    unchanged_except(&before, &after, writes::EXTEND_STAMP)?;
    scratch.cleanup().await
}
