//! Explicit same-node transport changes preserve the retained ingest extent.
use anyhow::{Context as _, Result, ensure};
use bigname_ingest::{
    LiveContinuation, Marker, SourceDescriptor, VerificationProvider, WatchFilter,
    admit_source_floor, enforce_source_floor, load_persisted_watch_filter, plan_live_continuation,
};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    config::{SeedBasis, SourceConfig, SourceRole, normalized_source_kind},
    database::RunnerDatabase,
    phase::PhaseName,
    state::PhaseStatus,
    transitions::{PhaseStateRow, lock_chain_phase_state, row_for},
};

/// The operator attests these endpoints expose the same node. This operation checks
/// retained canonical boundaries and the next block's watched logs; it does not grant
/// independent verification or replace the node's historical coverage.
pub async fn transition(
    database: &RunnerDatabase,
    old: &SourceConfig,
    new: &SourceConfig,
) -> Result<Value> {
    transition_with_readers(database, old, new, |source| {
        let kind = normalized_source_kind(&source.source_kind);
        Ok(VerificationProvider::new(
            &source.chain_id,
            &kind,
            source.endpoint(),
        )?)
    })
    .await
}

/// [`transition`] with the caller opening the reader for each descriptor. Tests use it to
/// stand an HTTP node double in for the direct database reader, which otherwise needs a real
/// Reth datadir; every check and the cursor update are the production ones.
#[doc(hidden)]
pub async fn transition_with_readers(
    database: &RunnerDatabase,
    old: &SourceConfig,
    new: &SourceConfig,
    open_reader: impl Fn(&SourceConfig) -> Result<VerificationProvider>,
) -> Result<Value> {
    validate_pair(old, new)?;
    let chain = &old.chain_id;
    let from_kind = normalized_source_kind(&old.source_kind);
    let to_kind = normalized_source_kind(&new.source_kind);
    let mut tx = database.pool().begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '2s'")
        .execute(&mut *tx)
        .await?;
    for phase in PhaseName::ALL {
        let locked: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, 0::bigint))",
        )
        .bind(crate::phase_lock::lock_name(chain, phase))
        .fetch_one(&mut *tx)
        .await?;
        ensure!(
            locked,
            "stop all phase writers before changing source transport"
        );
    }
    let rows: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(c) FROM ingest_cursors c WHERE chain_id = $1 FOR UPDATE",
    )
    .bind(chain)
    .fetch_all(&mut *tx)
    .await?;
    ensure!(
        rows.len() == 1,
        "transport change requires exactly one retained intake cursor"
    );
    let cursor = &rows[0];
    ensure!(
        cursor["source_key"].as_str() == Some(&old.source_key),
        "source key differs"
    );
    ensure!(
        cursor["source_kind"].as_str() == Some(&from_kind),
        "stored source kind differs"
    );
    ensure!(
        cursor["seed_basis"] == "ethereum_head",
        "stored seed basis differs"
    );
    ensure!(
        cursor["start_block_number"].as_i64() == Some(old.start_block_number),
        "stored start differs"
    );
    let phase: Value = sqlx::query_scalar(
        "SELECT to_jsonb(p) FROM chain_phase_state p WHERE chain_id = $1 AND phase_name = 'ingest' FOR UPDATE",
    )
    .bind(chain)
    .fetch_one(&mut *tx)
    .await?;
    let old_provider = open_reader(old)?;
    let new_provider = open_reader(new)?;
    let mut checked = Vec::new();
    for (row, number, hash) in [
        (
            cursor,
            "last_processed_block_number",
            "last_processed_block_hash",
        ),
        (&phase, "current_block_number", "current_block_hash"),
        (
            &phase,
            "redo_current_block_number",
            "redo_current_block_hash",
        ),
    ] {
        if let (Some(number), Some(hash)) = (row[number].as_i64(), row[hash].as_str()) {
            let left = old_provider
                .fetch(WatchFilter::default(), number, number)
                .await?;
            let right = new_provider
                .fetch(WatchFilter::default(), number, number)
                .await?;
            ensure!(
                left.end == right.end && right.end.hash == hash,
                "retained boundary differs at block {number}"
            );
            checked.push(json!({"block":number,"hash":hash}));
        }
    }
    ensure!(
        !checked.is_empty(),
        "transport change requires a retained boundary"
    );
    let resume = resume_point(
        &mut tx,
        database.pool(),
        chain,
        cursor,
        &phase,
        &new_provider,
    )
    .await?;
    let compared = resume.compared_block();
    let filter = load_persisted_watch_filter(database.pool(), chain, compared, compared).await?;
    let left = old_provider
        .fetch(filter.clone(), compared, compared)
        .await?;
    let right = new_provider.fetch(filter, compared, compared).await?;
    ensure!(
        left.end == right.end && left.logs == right.logs,
        "next block data differs"
    );
    if let ResumePoint::Live(continuation) = &resume
        && compared == continuation.ancestor.number
    {
        ensure!(
            right.end.hash == continuation.ancestor.hash,
            "published head differs at block {compared}"
        );
    }
    admit_retention_floor(chain, new, &to_kind, &new_provider, &phase, &resume).await?;
    let affected = sqlx::query(
        "UPDATE ingest_cursors SET source_kind = $3 WHERE chain_id = $1 AND source_key = $2 AND source_kind = $4",
    )
    .bind(chain).bind(&old.source_key).bind(&to_kind).bind(&from_kind)
    .execute(&mut *tx).await?.rows_affected();
    ensure!(affected == 1, "source cursor changed during transition");
    // Source changes cannot upgrade or clear any verification/redo state.
    tx.commit().await?;
    Ok(
        json!({"chain":chain,"source_key":old.source_key,"from_kind":from_kind,
        "to_kind":to_kind,"previous_cursor":cursor,"ingest_phase":phase,
        "checked_boundaries":checked,"next_block":resume.next_block(),
        "compared_block":compared,"compared_block_hash":right.end.hash,
        "compared_block_log_count":right.logs.len(),
        "live_continuation":resume.live_continuation_receipt(),"same_node_attested":true}),
    )
}

/// The work Ingest resumes with after the change, read from the persisted runner state the
/// way the runner reads it. Every check after the retained boundaries is judged on it.
enum ResumePoint {
    /// A redo in progress with blocks left to read reads this one next and is judged on what
    /// remains of its range.
    Redo(i64),
    /// Ingest plans another normal batch from the descriptor's declared start block; the
    /// cursor's next block is the one it fetches first.
    DeclaredStart(i64),
    /// Ingest completed and handed off, or holds a retained completion the runner revalidates
    /// without a historical rescan (`completed_phase_recovery`), so only live follow reads
    /// this node. The historical cursor stops moving at the handoff while live progress goes
    /// through `record_progress`, so the position is selected from the published chain head
    /// with the live engine's own common-ancestor rule instead.
    Live(LiveContinuation),
}

impl ResumePoint {
    /// The block Ingest or live follow reads next.
    fn next_block(&self) -> i64 {
        match self {
            Self::Redo(next) | Self::DeclaredStart(next) => *next,
            Self::Live(continuation) => continuation.next_block(),
        }
    }

    /// The block both interfaces are compared on: the next block, except when live follow
    /// has published the node's head and the next block does not exist yet. The published
    /// head is then the boundary live follow extends and the block whose retention matters,
    /// so it is compared instead; the floor is still applied to the block after it.
    fn compared_block(&self) -> i64 {
        match self {
            Self::Live(continuation) if !continuation.next_block_is_available() => {
                continuation.ancestor.number
            }
            _ => self.next_block(),
        }
    }

    fn live_continuation_receipt(&self) -> Value {
        let Self::Live(continuation) = self else {
            return Value::Null;
        };
        let marker = |marker: &Marker| json!({"number": marker.number, "hash": marker.hash});
        json!({
            "ancestor": marker(&continuation.ancestor),
            "node_head": marker(&continuation.node_head),
        })
    }
}

async fn resume_point(
    tx: &mut Transaction<'_, Postgres>,
    pool: &PgPool,
    chain: &str,
    cursor: &Value,
    phase: &Value,
    new_provider: &VerificationProvider,
) -> Result<ResumePoint> {
    let rows = lock_chain_phase_state(tx, chain).await?;
    let mut ingest = row_for(&rows, PhaseName::Ingest)?.clone();
    if ingest.redo_in_progress {
        let (Some(from), Some(to)) = (
            phase["redo_from_block_number"].as_i64(),
            phase["redo_to_block_number"].as_i64(),
        ) else {
            anyhow::bail!("ingest redo is in progress for chain {chain} without a redo range");
        };
        if let RedoPosition::Next(next) =
            redo_position(from, to, phase["redo_current_block_number"].as_i64())?
        {
            return Ok(ResumePoint::Redo(next));
        }
        // The redo read its last block and the process stopped before finish_redo cleared
        // the marker. Rerunning it reads nothing and clears the marker, restoring the
        // lifecycle the redo interrupted (redo_state::finish); the work that follows is the
        // work that restored state plans, so it is judged on that state. For a redo that
        // interrupted a running or paused Ingest, finish persists `failed` ("phase was
        // interrupted before redo; resume the normal phase") while this restored copy keeps
        // running or paused; both replan from the declared start.
        crate::redo_completion::restore_previous_lifecycle(&mut ingest)?;
    }
    if ingest_replans_from_declared_start(&ingest)? {
        return cursor["next_block_number"]
            .as_i64()
            .map(ResumePoint::DeclaredStart)
            .ok_or_else(|| anyhow::anyhow!("missing next ingest block"));
    }
    plan_live_continuation(pool, chain, new_provider)
        .await
        .map(ResumePoint::Live)
        .context("failed to select where live follow resumes on the proposed reader")
}

/// Where a persisted redo of `from..=to` stands.
#[derive(Debug)]
enum RedoPosition {
    /// Blocks remain; this one is read next.
    Next(i64),
    /// The position is the range's last block: nothing remains, only the marker.
    Exhausted,
}

/// Reads the redo position the way the engine admits it (`bigname_ingest` refuses a resume
/// marker outside the redo range before it plans anything) and tells an exhausted range
/// from one with blocks left.
fn redo_position(from: i64, to: i64, current: Option<i64>) -> Result<RedoPosition> {
    match current {
        None => Ok(RedoPosition::Next(from)),
        Some(current) if current == to => Ok(RedoPosition::Exhausted),
        Some(current) if current < from || current > to => anyhow::bail!(
            "ingest redo resume marker {current} is outside the redo range {from}..={to}"
        ),
        Some(current) => Ok(RedoPosition::Next(current + 1)),
    }
}

/// Applies, to the proposed descriptor, the source-floor admission that Ingest or live
/// follow applies when it resumes after the change, so a reader that resumed work would
/// refuse is refused here instead of after a committed switch.
///
/// The rule is the engine's own (`bigname_ingest::admit_source_floor`) and keeps its
/// distinction between the kinds of work that resume: a redo in progress is judged on what
/// remains of its range, Ingest that has not completed replans from the descriptor's
/// declared start block, and live follow is judged on the suffix from the block after its
/// common ancestor with the node. Only a direct reader reports a floor, so a change back
/// to the HTTP interface admits without one.
async fn admit_retention_floor(
    chain: &str,
    new: &SourceConfig,
    to_kind: &str,
    new_provider: &VerificationProvider,
    phase: &Value,
    resume: &ResumePoint,
) -> Result<()> {
    let Some(floor) = new_provider.earliest_available_block().await? else {
        return Ok(());
    };
    let descriptor = SourceDescriptor {
        key: new.source_key.clone(),
        kind: to_kind.to_owned(),
        start_block: new.start_block_number,
        endpoint: new.endpoint().to_owned(),
    };
    let admitted = match resume {
        ResumePoint::Redo(_) => {
            let (Some(from), Some(to)) = (
                phase["redo_from_block_number"].as_i64(),
                phase["redo_to_block_number"].as_i64(),
            ) else {
                anyhow::bail!("ingest redo is in progress for chain {chain} without a redo range");
            };
            let resumed = phase["redo_current_block_number"]
                .as_i64()
                .zip(phase["redo_current_block_hash"].as_str())
                .map(|(number, hash)| Marker {
                    number,
                    hash: hash.to_owned(),
                });
            admit_source_floor(&descriptor, Some((from, to)), resumed.as_ref(), floor)
        }
        ResumePoint::DeclaredStart(_) => admit_source_floor(&descriptor, None, None, floor),
        ResumePoint::Live(continuation) => {
            enforce_source_floor(&descriptor.key, continuation.next_block(), None, floor)
        }
    };
    admitted
        .context("the direct reader cannot serve the Ingest work that resumes after this change")
}

/// Whether Ingest plans another normal batch from its declared start when it resumes: the
/// same tests the runner applies before it starts Ingest. A failed phase that retains a
/// completed extent is revalidated, not rescanned, so it does not replan; every other
/// failed or unfinished phase does.
fn ingest_replans_from_declared_start(ingest: &PhaseStateRow) -> Result<bool> {
    if crate::completed_phase_recovery::locked_completion_recovery(ingest, PhaseName::Ingest) {
        return Ok(false);
    }
    Ok(ingest.status()? != PhaseStatus::Completed || ingest.ingest_completion_is_incomplete())
}

fn validate_pair(old: &SourceConfig, new: &SourceConfig) -> Result<()> {
    ensure!(
        old.chain_id == "ethereum-sepolia" && new.chain_id == old.chain_id,
        "transport change supports Sepolia only"
    );
    ensure!(
        old.source_key == new.source_key
            && old.seed_basis == new.seed_basis
            && old.seed_basis == SeedBasis::EthereumHead
            && old.start_block_number == new.start_block_number
            && old.start_block_number == 0
            && old.role == SourceRole::Intake
            && new.role == SourceRole::Intake,
        "transport change must preserve key, seed, start, and intake-only role"
    );
    let kinds = (
        normalized_source_kind(&old.source_kind),
        normalized_source_kind(&new.source_kind),
    );
    ensure!(
        matches!(
            (kinds.0.as_str(), kinds.1.as_str()),
            ("drpc", "reth_db") | ("reth_db", "drpc")
        ),
        "only same-node drpc/reth_db transport changes are supported"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redo_position_outside_the_range_is_refused() {
        assert!(matches!(
            redo_position(2, 5, None),
            Ok(RedoPosition::Next(2))
        ));
        assert!(matches!(
            redo_position(2, 5, Some(3)),
            Ok(RedoPosition::Next(4))
        ));
        assert!(matches!(
            redo_position(2, 5, Some(5)),
            Ok(RedoPosition::Exhausted)
        ));
        let error = redo_position(2, 5, Some(6)).unwrap_err().to_string();
        assert_eq!(
            error,
            "ingest redo resume marker 6 is outside the redo range 2..=5"
        );
        assert!(redo_position(2, 5, Some(1)).is_err());
    }
    fn source(kind: &str) -> SourceConfig {
        SourceConfig::new_with_role(
            "ethereum-sepolia",
            "node",
            kind,
            SeedBasis::EthereumHead,
            0,
            SourceRole::Intake,
            "/fixture",
        )
        .unwrap()
    }
    #[test]
    fn same_node_transport_preserves_identity_and_supports_rollback() {
        let old = source("drpc");
        let new = source("reth_db");
        assert!(validate_pair(&old, &new).is_ok());
        assert!(validate_pair(&new, &old).is_ok());
        assert!(validate_pair(&old, &old).is_err());
        let mut changed = new.clone();
        changed.source_key = "replacement".into();
        assert!(validate_pair(&old, &changed).is_err());
        let mut changed = new.clone();
        changed.start_block_number = 1;
        assert!(validate_pair(&old, &changed).is_err());
        let mut changed = new.clone();
        changed.role = SourceRole::Both;
        assert!(validate_pair(&old, &changed).is_err());
        let mut changed = new;
        changed.chain_id = "ethereum-mainnet".into();
        assert!(validate_pair(&old, &changed).is_err());
    }
}
