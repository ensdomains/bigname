//! The state a history cursor is bound to, read from one snapshot: each chain's Interpret and
//! Project redo counters and flags, whether an Interpret redo is active anywhere, and the lineage
//! row of the block the cursor is bound to on each chain.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::{PgConnection, PgPool};

use super::redo::interpret_redo_active;
use crate::lineage::{CanonicalityState, ChainLineageBlock, load_chain_lineage_block_internal};

/// One phase's redo counter and flag on one chain. The phase runner raises the counter when a
/// redo begins or widens and never lowers it, so an unchanged counter with the flag clear means
/// no redo of that phase ran in between.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhaseRedoState {
    pub generation: i64,
    pub in_progress: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChainRedoState {
    pub interpret: PhaseRedoState,
    pub project: PhaseRedoState,
}

/// A requested chain has no Interpret or no Project phase row, so its redo state is unknown.
#[derive(Debug)]
pub struct PhaseRedoStateMissing {
    pub chain_id: String,
}

impl std::fmt::Display for PhaseRedoStateMissing {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "chain {} has no Interpret or Project phase state",
            self.chain_id
        )
    }
}

impl std::error::Error for PhaseRedoStateMissing {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryBoundState {
    /// Exactly the requested chains.
    pub redo: BTreeMap<String, ChainRedoState>,
    /// Whether an Interpret redo is active on any chain, as the history fence has always
    /// checked (`ensure_interpret_not_redo`).
    pub interpret_redo_active: bool,
    bound_blocks: BTreeMap<String, Option<ChainLineageBlock>>,
}

impl HistoryBoundState {
    /// The lineage row of the bound block on `chain_id` when it is still readable: stored under
    /// the bound hash, at the bound height, and canonical, safe, or finalized.
    pub fn readable_bound_block(
        &self,
        chain_id: &str,
        block_number: i64,
    ) -> Option<&ChainLineageBlock> {
        self.bound_blocks
            .get(chain_id)?
            .as_ref()
            .filter(|block| block.block_number == block_number)
            .filter(|block| {
                matches!(
                    block.canonicality_state,
                    CanonicalityState::Canonical
                        | CanonicalityState::Safe
                        | CanonicalityState::Finalized
                )
            })
    }
}

/// Read, in one `REPEATABLE READ` snapshot, the redo state of every chain in `bound` and the
/// lineage row stored under each chain's bound block hash. `bound` maps chain to block hash.
pub async fn capture_history_bound_state(
    pool: &PgPool,
    bound: &BTreeMap<String, String>,
) -> Result<HistoryBoundState> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin history bound state transaction")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *transaction)
        .await
        .context("failed to configure history bound state transaction")?;
    let chain_ids = bound.keys().cloned().collect::<Vec<_>>();
    let redo = load_phase_redo_state(&mut transaction, &chain_ids).await?;
    let interpret_redo_active = interpret_redo_active(&mut transaction).await?;
    let mut bound_blocks = BTreeMap::new();
    for (chain_id, block_hash) in bound {
        let block =
            load_chain_lineage_block_internal(&mut *transaction, chain_id, block_hash).await?;
        bound_blocks.insert(chain_id.clone(), block);
    }
    transaction
        .commit()
        .await
        .context("failed to commit history bound state transaction")?;
    Ok(HistoryBoundState {
        redo,
        interpret_redo_active,
        bound_blocks,
    })
}

/// Both redo phases of every requested chain. A chain missing either row is an error, never a
/// shorter map that would compare equal to another shorter map.
async fn load_phase_redo_state(
    connection: &mut PgConnection,
    chain_ids: &[String],
) -> Result<BTreeMap<String, ChainRedoState>> {
    let rows: Vec<(String, String, i64, bool)> = sqlx::query_as(
        "SELECT chain_id, phase_name, redo_attempt_generation, redo_in_progress
         FROM bigname_phase.chain_phase_state
         WHERE phase_name IN ('interpret', 'project') AND chain_id = ANY($1)",
    )
    .bind(chain_ids)
    .fetch_all(connection)
    .await
    .context("failed to load Interpret and Project redo state")?;
    let mut phases = BTreeMap::new();
    for (chain_id, phase_name, generation, in_progress) in rows {
        phases.insert(
            (chain_id, phase_name),
            PhaseRedoState {
                generation,
                in_progress,
            },
        );
    }
    chain_ids
        .iter()
        .map(|chain_id| {
            let phase = |name: &str| phases.get(&(chain_id.clone(), name.to_owned())).copied();
            match (phase("interpret"), phase("project")) {
                (Some(interpret), Some(project)) => {
                    Ok((chain_id.clone(), ChainRedoState { interpret, project }))
                }
                _ => Err(PhaseRedoStateMissing {
                    chain_id: chain_id.clone(),
                }
                .into()),
            }
        })
        .collect()
}

#[cfg(test)]
#[path = "redo_phases_tests.rs"]
mod tests;
