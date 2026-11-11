//! The `chain_phase_state` tables in `docs/phase-state.md`, held against the schema's CHECKs and
//! the statements that move a row. Each state test inserts a row and reads the CHECKs' verdict.
//! Each transition test seeds a row in one "from" state, runs the site's own statement through
//! the narrowest function that reaches it, and checks the row it leaves: the state it is in and
//! that no column outside the statement's write set moved.
use std::str::FromStr;

use anyhow::{Context, Result, bail, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, postgres::PgConnectOptions};

use crate::{
    database::RunnerDatabase,
    error::{RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{BlockRange, PhaseName},
    state::PhaseStore,
};

mod normal_tests;
mod redo_tests;
mod stamp_tests;
mod state_tests;

pub(crate) const T0: &str = "2026-01-01T00:00:00+00:00";
pub(crate) const T1: &str = "2026-01-01T01:00:00+00:00";
pub(crate) const T2: &str = "2026-01-01T02:00:00+00:00";
pub(crate) const PENDING: &str = "required downstream redo: test stamp";
pub(crate) const ACTIVE: &str = "required downstream redo active: test stamp";
pub(crate) const EARLIER_FAILURE: &str = "earlier failure";
pub(crate) const RANGE: BlockRange = BlockRange { from: 10, to: 90 };

/// The states of `docs/phase-state.md` § States, as `state` reads them from a row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum State {
    Idle,
    Running,
    Paused,
    Completed,
    CompletedUnconfigured,
    Failed,
    RetainedCompletion,
    OperatorRedo,
    RequiredPending,
    RequiredActive,
    RecomputeRedo,
}

/// Reads a row's state the way the runner reads it: the redo marker and the ownership prefix of
/// `last_error` first (`redo_stamp.rs`), then `phase_status`, with the retained completion of
/// `completed_phase_recovery.rs` (`is_retained_completion`) told apart from other failures.
pub(crate) fn state(row: &Value) -> State {
    if row["redo_in_progress"].as_bool() == Some(true) {
        if row["redo_mode"].as_str() == Some("recompute_flags") {
            return State::RecomputeRedo;
        }
        return match row["last_error"].as_str() {
            Some(error) if error.starts_with(crate::redo_stamp::REQUIRED_REDO_ACTIVE_PREFIX) => {
                State::RequiredActive
            }
            Some(error) if error.starts_with(crate::redo_stamp::REQUIRED_REDO_PREFIX) => {
                State::RequiredPending
            }
            _ => State::OperatorRedo,
        };
    }
    match row["phase_status"].as_str() {
        Some("idle") => State::Idle,
        Some("running") => State::Running,
        Some("paused") => State::Paused,
        Some("completed") if row["settled_while_unconfigured"].as_bool() == Some(true) => {
            State::CompletedUnconfigured
        }
        Some("completed") => State::Completed,
        _ if is_retained_completion(row) => State::RetainedCompletion,
        _ => State::Failed,
    }
}

fn is_retained_completion(row: &Value) -> bool {
    let completed_extent = !row["current_block_number"].is_null()
        && row["current_block_number"] == row["target_block_number"]
        && !row["current_block_hash"].is_null()
        && row["current_block_hash"] == row["target_block_hash"];
    match row["phase_name"].as_str() {
        Some("ingest") => {
            row["last_error"].as_str().is_some_and(|error| {
                error.starts_with(crate::error::COMPLETED_VALIDATION_FAILURE_PREFIX)
            }) && completed_extent
                && row["live_handoff_block_number"] == row["target_block_number"]
                && row["live_handoff_block_hash"] == row["target_block_hash"]
        }
        Some("verify") => completed_extent && !row["verification_level"].is_null(),
        _ => false,
    }
}

pub(crate) struct Scratch {
    database: TestDatabase,
    runner: RunnerDatabase,
}

impl Scratch {
    pub(crate) async fn create(prefix: &str) -> Result<Self> {
        let database =
            TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(10)).await?;
        crate::schema::initialize_schema_v2(database.pool()).await?;
        let options = PgConnectOptions::from_str(&database_url_from_env())?
            .database(database.database_name());
        let runner = RunnerDatabase::connect_with_options(options, 10).await?;
        Ok(Self { database, runner })
    }

    pub(crate) fn pool(&self) -> &PgPool {
        self.runner.pool()
    }

    pub(crate) fn store(&self) -> PhaseStore {
        PhaseStore::new(self.pool().clone())
    }

    pub(crate) async fn cleanup(self) -> Result<()> {
        self.runner.pool().close().await;
        self.database.cleanup().await
    }
}

/// One `chain_phase_state` row as JSON; absent columns insert as NULL.
#[derive(Clone, Debug)]
pub(crate) struct Row(Map<String, Value>);

impl Row {
    pub(crate) fn set(mut self, column: &str, value: Value) -> Self {
        self.0.insert(column.to_owned(), value);
        self
    }

    pub(crate) fn get(&self, column: &str) -> Value {
        self.0.get(column).cloned().unwrap_or(Value::Null)
    }

    fn phase_name(&self) -> Value {
        self.get("phase_name")
    }
}

pub(crate) fn marker(number: i64) -> BlockMarker {
    BlockMarker {
        number,
        hash: format!("h{number}"),
    }
}

/// A row `initialize_chain` inserts (`state.rs:101`): every column at its default.
pub(crate) fn idle(chain: &str, phase: PhaseName) -> Row {
    let mut columns = Map::new();
    columns.insert("chain_id".to_owned(), json!(chain));
    columns.insert("phase_name".to_owned(), json!(phase.as_str()));
    columns.insert("phase_status".to_owned(), json!("idle"));
    columns.insert("redo_in_progress".to_owned(), json!(false));
    columns.insert("redo_attempt_generation".to_owned(), json!(0));
    columns.insert("updated_at".to_owned(), json!(T0));
    Row(columns)
}

fn with_progress(row: Row, current: i64, target: i64) -> Row {
    row.set("current_block_number", json!(current))
        .set("current_block_hash", json!(format!("h{current}")))
        .set("target_block_number", json!(target))
        .set("target_block_hash", json!(format!("h{target}")))
}

/// A normal run under way: started, not finished, cursor at 60 of 100.
pub(crate) fn running(chain: &str, phase: PhaseName) -> Row {
    let row = idle(chain, phase)
        .set("phase_status", json!("running"))
        .set("started_at", json!(T0))
        .set(
            "input_content_hash",
            json!(crate::transitions::expected_content_hash(phase)),
        );
    with_progress(row, 60, 100)
}

pub(crate) fn paused(chain: &str, phase: PhaseName) -> Row {
    running(chain, phase).set("phase_status", json!("paused"))
}

/// A finished run at block 100, with the Ingest handoff and the Verify level a completion writes.
pub(crate) fn completed(chain: &str, phase: PhaseName) -> Row {
    let row = with_progress(running(chain, phase), 100, 100)
        .set("phase_status", json!("completed"))
        .set("finished_at", json!(T1));
    match phase {
        PhaseName::Ingest => row
            .set("live_handoff_block_number", json!(100))
            .set("live_handoff_block_hash", json!("h100")),
        PhaseName::Verify => row.set("verification_level", json!("quick_synced")),
        _ => row,
    }
}

pub(crate) fn failed(chain: &str, phase: PhaseName) -> Row {
    running(chain, phase)
        .set("phase_status", json!("failed"))
        .set("finished_at", json!(T1))
        .set("last_error", json!(EARLIER_FAILURE))
}

/// An Ingest or Verify completion a validation failure turned `failed`
/// (`completed_phase_recovery.rs:93`).
pub(crate) fn retained_completion(chain: &str, phase: PhaseName) -> Row {
    completed(chain, phase)
        .set("phase_status", json!("failed"))
        .set(
            "last_error",
            json!(format!(
                "{}test",
                crate::error::COMPLETED_VALIDATION_FAILURE_PREFIX
            )),
        )
}

/// `base` with a redo marker over `RANGE`, holding `base`'s lifecycle in the `redo_previous_*`
/// columns as a redo begin saves it (`redo_state.rs:211`).
pub(crate) fn redo(base: Row, mode: &str, last_error: Option<&str>) -> Row {
    let project = base.phase_name() == json!("project");
    let previous_status = base.get("phase_status");
    let previous_error = base.get("last_error");
    let previous_started = base.get("started_at");
    let previous_finished = base.get("finished_at");
    let row = base
        .set("phase_status", json!("running"))
        .set("redo_in_progress", json!(true))
        .set("redo_attempt_generation", json!(1))
        .set("redo_mode", json!(mode))
        .set("redo_previous_phase_status", previous_status)
        .set("redo_previous_last_error", previous_error)
        .set("redo_previous_started_at", previous_started)
        .set("redo_previous_finished_at", previous_finished)
        .set("redo_from_block_number", json!(RANGE.from))
        .set("redo_to_block_number", json!(RANGE.to))
        .set("last_error", json!(last_error))
        .set("started_at", json!(T2))
        .set("finished_at", Value::Null);
    if project {
        row.set("redo_requested_from_block_number", json!(RANGE.from))
            .set("redo_requested_to_block_number", json!(RANGE.to))
    } else {
        row
    }
}

pub(crate) fn operator_redo(chain: &str, phase: PhaseName) -> Row {
    redo(completed(chain, phase), "redo", None)
}

pub(crate) fn required_pending(chain: &str, phase: PhaseName) -> Row {
    redo(completed(chain, phase), "redo", Some(PENDING))
}

pub(crate) fn required_active(chain: &str, phase: PhaseName) -> Row {
    redo(completed(chain, phase), "redo", Some(ACTIVE))
}

/// Progress only the active redo has made: block 40 of 90.
pub(crate) fn with_redo_progress(row: Row) -> Row {
    row.set("redo_current_block_number", json!(40))
        .set("redo_current_block_hash", json!("h40"))
        .set("redo_target_block_number", json!(90))
        .set("redo_target_block_hash", json!("h90"))
}

pub(crate) async fn insert(pool: &PgPool, row: &Row) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO chain_phase_state
         SELECT * FROM jsonb_populate_record(NULL::chain_phase_state, $1)",
    )
    .bind(Value::Object(row.0.clone()))
    .execute(pool)
    .await
    .map(|_| ())
}

/// Inserts all five rows of `chain`: each phase from `rows`, or idle.
pub(crate) async fn seed(pool: &PgPool, chain: &str, rows: &[Row]) -> Result<()> {
    for phase in PhaseName::ALL {
        let row = rows
            .iter()
            .find(|row| row.phase_name() == json!(phase.as_str()))
            .cloned()
            .unwrap_or_else(|| idle(chain, phase));
        insert(pool, &row)
            .await
            .with_context(|| format!("failed to seed chain {chain} phase {phase}"))?;
    }
    Ok(())
}

pub(crate) async fn read(pool: &PgPool, chain: &str, phase: PhaseName) -> Result<Value> {
    sqlx::query_scalar::<_, Value>(
        "SELECT to_jsonb(p) FROM chain_phase_state p WHERE chain_id = $1 AND phase_name = $2",
    )
    .bind(chain)
    .bind(phase.as_str())
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to read chain {chain} phase {phase}"))
}

pub(crate) fn expect_state(row: &Value, expected: State) -> Result<()> {
    let found = state(row);
    ensure!(
        found == expected,
        "expected {expected:?}, found {found:?}: {row}"
    );
    Ok(())
}

/// Every column outside `written` and `updated_at` holds its value from `before`.
pub(crate) fn unchanged_except(before: &Value, after: &Value, written: &[&str]) -> Result<()> {
    let columns = before
        .as_object()
        .context("a phase row reads as a JSON object")?;
    for (column, value) in columns {
        if column == "updated_at" || written.contains(&column.as_str()) {
            continue;
        }
        ensure!(
            after[column.as_str()] == *value,
            "{column} moved from {value} to {} outside the statement's write set",
            after[column.as_str()]
        );
    }
    Ok(())
}

pub(crate) fn expect_unchanged(before: &Value, after: &Value) -> Result<()> {
    ensure!(before == after, "the row moved: {before} became {after}");
    Ok(())
}

/// The error of a call the test expects to refuse.
pub(crate) fn refusal<T>(result: RunnerResult<T>, what: &str) -> Result<RunnerError> {
    match result {
        Ok(_) => bail!("{what}: expected a refusal, the call succeeded"),
        Err(error) => Ok(error),
    }
}

/// The refusal text PostgreSQL gives a row a CHECK rejects, as it reaches a runner error.
pub(crate) fn expect_check_refusal(message: &str, constraint: &str) -> Result<()> {
    ensure!(
        message.contains(&format!("violates check constraint \"{constraint}\"")),
        "expected a refusal by {constraint}, got: {message}"
    );
    Ok(())
}

/// The write sets of `docs/phase-state.md` § Transitions, one per statement.
pub(crate) mod writes {
    pub(crate) const PROGRESS: &[&str] = &[
        "current_block_number",
        "current_block_hash",
        "target_block_number",
        "target_block_hash",
        "live_handoff_block_number",
        "live_handoff_block_hash",
        "verification_level",
    ];
    pub(crate) const REDO_PROGRESS: &[&str] = &[
        "redo_current_block_number",
        "redo_current_block_hash",
        "redo_target_block_number",
        "redo_target_block_hash",
        "redo_source_boundary_markers",
    ];
    pub(crate) const START: &[&str] = &[
        "phase_status",
        "verification_level",
        "current_block_number",
        "current_block_hash",
        "target_block_number",
        "target_block_hash",
        "input_content_hash",
        "live_handoff_block_number",
        "live_handoff_block_hash",
        "last_error",
        "started_at",
        "finished_at",
    ];
    pub(crate) const STATUS: &[&str] = &["phase_status"];
    pub(crate) const COMPLETE: &[&str] = &[
        "current_block_number",
        "current_block_hash",
        "target_block_number",
        "target_block_hash",
        "live_handoff_block_number",
        "live_handoff_block_hash",
        "verification_level",
        "phase_status",
        "settled_while_unconfigured",
        "finished_at",
    ];
    pub(crate) const FAIL: &[&str] = &["phase_status", "last_error", "finished_at"];
    pub(crate) const LIVE_MISMATCH: &[&str] =
        &["phase_status", "last_error", "started_at", "finished_at"];
    pub(crate) const SETTLE: &[&str] = &[
        "phase_status",
        "last_error",
        "settled_while_unconfigured",
        "live_handoff_block_number",
        "live_handoff_block_hash",
        "finished_at",
    ];
    pub(crate) const CLEAR_SETTLEMENT: &[&str] = &["settled_while_unconfigured"];
    pub(crate) const REVALIDATED: &[&str] = &[
        "phase_status",
        "last_error",
        "started_at",
        "finished_at",
        "settled_while_unconfigured",
    ];
    pub(crate) const RECOVERY_INGEST: &[&str] = &["last_error"];
    pub(crate) const RECOVERY: &[&str] = &["phase_status", "last_error", "finished_at"];
    pub(crate) const REDO_BEGIN: &[&str] = &[
        "phase_status",
        "redo_in_progress",
        "redo_attempt_generation",
        "redo_mode",
        "redo_previous_phase_status",
        "redo_previous_last_error",
        "redo_previous_started_at",
        "redo_previous_finished_at",
        "redo_from_block_number",
        "redo_to_block_number",
        "redo_requested_from_block_number",
        "redo_requested_to_block_number",
        "redo_current_block_number",
        "redo_current_block_hash",
        "redo_target_block_number",
        "redo_target_block_hash",
        "redo_source_boundary_markers",
        "redo_manifest_authority_fingerprint",
        "input_content_hash",
        "last_error",
        "started_at",
        "finished_at",
    ];
    /// Every column but `redo_attempt_generation`: the finish leaves only that one alone.
    pub(crate) const REDO_FINISH: &[&str] = &[
        "phase_status",
        "verification_level",
        "settled_while_unconfigured",
        "current_block_number",
        "current_block_hash",
        "target_block_number",
        "target_block_hash",
        "input_content_hash",
        "redo_in_progress",
        "redo_mode",
        "redo_previous_phase_status",
        "redo_previous_last_error",
        "redo_previous_started_at",
        "redo_previous_finished_at",
        "redo_from_block_number",
        "redo_to_block_number",
        "redo_requested_from_block_number",
        "redo_requested_to_block_number",
        "redo_current_block_number",
        "redo_current_block_hash",
        "redo_target_block_number",
        "redo_target_block_hash",
        "redo_source_boundary_markers",
        "redo_manifest_authority_fingerprint",
        "live_handoff_block_number",
        "live_handoff_block_hash",
        "last_error",
        "started_at",
        "finished_at",
    ];
    pub(crate) const SUPERSEDE: &[&str] = &[
        "phase_status",
        "last_error",
        "started_at",
        "finished_at",
        "redo_in_progress",
        "redo_mode",
        "redo_previous_phase_status",
        "redo_previous_last_error",
        "redo_previous_started_at",
        "redo_previous_finished_at",
        "redo_from_block_number",
        "redo_to_block_number",
        "redo_requested_from_block_number",
        "redo_requested_to_block_number",
        "redo_current_block_number",
        "redo_current_block_hash",
        "redo_target_block_number",
        "redo_target_block_hash",
        "redo_source_boundary_markers",
        "redo_manifest_authority_fingerprint",
    ];
    pub(crate) const WIDENED: &[&str] = &[
        "redo_current_block_number",
        "redo_current_block_hash",
        "redo_target_block_number",
        "redo_target_block_hash",
        "redo_source_boundary_markers",
        "redo_manifest_authority_fingerprint",
        "last_error",
    ];
    pub(crate) const CREATE_STAMP: &[&str] = &[
        "redo_attempt_generation",
        "phase_status",
        "redo_in_progress",
        "redo_mode",
        "redo_previous_phase_status",
        "redo_previous_last_error",
        "redo_previous_started_at",
        "redo_previous_finished_at",
        "redo_from_block_number",
        "redo_to_block_number",
        "redo_requested_from_block_number",
        "redo_requested_to_block_number",
        "redo_current_block_number",
        "redo_current_block_hash",
        "redo_target_block_number",
        "redo_target_block_hash",
        "redo_source_boundary_markers",
        "redo_manifest_authority_fingerprint",
        "last_error",
        "started_at",
        "finished_at",
    ];
    pub(crate) const EXTEND_STAMP: &[&str] = &[
        "redo_attempt_generation",
        "redo_requested_from_block_number",
        "redo_requested_to_block_number",
        "redo_from_block_number",
        "redo_to_block_number",
        "redo_current_block_number",
        "redo_current_block_hash",
        "redo_target_block_number",
        "redo_target_block_hash",
        "redo_source_boundary_markers",
        "redo_manifest_authority_fingerprint",
    ];
    pub(crate) const REDO_FAILURE: &[&str] = &[
        "last_error",
        "redo_current_block_number",
        "redo_current_block_hash",
        "redo_target_block_number",
        "redo_target_block_hash",
        "redo_source_boundary_markers",
    ];
    /// `required_ingest.rs:95`: `EXTEND_STAMP`'s set without the requested range, plus
    /// `last_error`.
    pub(crate) const INGEST_WIDEN: &[&str] = &[
        "redo_attempt_generation",
        "redo_from_block_number",
        "redo_to_block_number",
        "redo_current_block_number",
        "redo_current_block_hash",
        "redo_target_block_number",
        "redo_target_block_hash",
        "redo_source_boundary_markers",
        "redo_manifest_authority_fingerprint",
        "last_error",
    ];
    /// `required_ingest.rs:118`: `CREATE_STAMP`'s set without the requested range.
    pub(crate) const INGEST_CREATE: &[&str] = &[
        "redo_attempt_generation",
        "phase_status",
        "redo_in_progress",
        "redo_mode",
        "redo_previous_phase_status",
        "redo_previous_last_error",
        "redo_previous_started_at",
        "redo_previous_finished_at",
        "redo_from_block_number",
        "redo_to_block_number",
        "redo_current_block_number",
        "redo_current_block_hash",
        "redo_target_block_number",
        "redo_target_block_hash",
        "redo_source_boundary_markers",
        "redo_manifest_authority_fingerprint",
        "last_error",
        "started_at",
        "finished_at",
    ];
}
