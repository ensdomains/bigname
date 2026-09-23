//! History collections bound to a block (docs/api-v2-routes.md, "Shared Route Rules"). The first
//! page binds, for every chain in the request scope, the block the served publication stood at
//! and that chain's Interpret and Project redo counters and classification horizon, plus a digest
//! of the manifest revisions. Every page reads the history at or below that block, so new blocks
//! below the horizon never break the walk; a changed counter, an active redo, a changed manifest,
//! a bound block that is no longer readable, or a publication at the horizon expires it.

use std::collections::BTreeMap;

use bigname_storage::{HistoryBoundState, PhaseRedoStateMissing};

use crate::AppState;

use super::collection_snapshot::{changed_during_read, not_available, restart_required};
use super::cursor::{Binding, BindingPolicy, BoundChain, invalid_cursor_error};
use super::envelope::AsOf;
use super::support::{
    PublicNamespaceSet, derive_public_namespace_set, ensure_public_namespace,
    reload_collection_namespace_set,
};
use super::{CursorPayload, ErrorCode, Meta, V2Error, V2Result, api_error_to_v2};

pub(crate) struct HistoryCollection {
    namespaces: PublicNamespaceSet,
    namespace: Option<String>,
    manifests: String,
    /// Every chain of the request scope: its bound block and redo counters.
    bound: BTreeMap<String, BoundChain>,
    captured: HistoryBoundState,
    as_of: BTreeMap<String, AsOf>,
    continues_cursor: bool,
}

impl HistoryCollection {
    /// Admit a history request (docs/api-v2.md, "Cursors And Pagination"). `cursor` is the
    /// decoded request cursor, already checked against the route's sort and filters.
    pub(crate) async fn capture(
        state: &AppState,
        cursor: Option<&CursorPayload>,
        namespace: Option<&str>,
    ) -> V2Result<Self> {
        if let Some(namespace) = namespace {
            ensure_public_namespace(namespace).map_err(api_error_to_v2)?;
        }
        let binding = cursor.map(history_binding).transpose()?;
        let namespaces = derive_public_namespace_set(state)
            .await
            .map_err(api_error_to_v2)?
            .for_namespace(namespace);
        // A continuation compares its manifests before the availability exit: a namespace that
        // lost its manifests is unavailable for good, and retrying the cursor cannot recover.
        let manifests = namespaces.manifest_digest();
        if binding.is_some_and(|binding| binding.manifests != manifests) {
            return Err(restart_required());
        }
        let published = published_positions(&namespaces).ok_or_else(not_available)?;
        let bound_blocks = match binding {
            Some(binding) => {
                let chains = binding.chains.as_ref().ok_or_else(invalid_cursor_error)?;
                if !chains.keys().eq(published.keys()) {
                    return Err(restart_required());
                }
                chains
                    .iter()
                    .map(|(chain, bound)| (chain.clone(), bound.block_hash.clone()))
                    .collect()
            }
            None => published
                .iter()
                .map(|(chain, position)| (chain.clone(), position.block_hash.clone()))
                .collect(),
        };
        let captured = capture_bound_state(state, &bound_blocks).await?;
        let block_numbers = match binding {
            Some(binding) => binding
                .chains
                .iter()
                .flatten()
                .map(|(chain, bound)| (chain.clone(), bound.block_number))
                .collect(),
            None => published
                .iter()
                .map(|(chain, position)| (chain.clone(), position.block_number))
                .collect(),
        };
        let horizons = classification_horizons(state, &block_numbers).await?;
        let continues_cursor = binding.is_some();
        let bound = match binding {
            Some(binding) => binding.chains.clone().ok_or_else(invalid_cursor_error)?,
            None => published
                .iter()
                .map(|(chain, position)| {
                    let redo = captured.redo.get(chain).ok_or_else(not_available)?;
                    Ok((
                        chain.clone(),
                        BoundChain {
                            block_number: position.block_number,
                            block_hash: position.block_hash.clone(),
                            interpret_generation: redo.interpret.generation,
                            project_generation: redo.project.generation,
                            classification_horizon: horizons.get(chain).copied(),
                        },
                    ))
                })
                .collect::<V2Result<_>>()?,
        };
        let mut collection = Self {
            namespaces,
            namespace: namespace.map(str::to_owned),
            manifests,
            bound,
            captured,
            as_of: BTreeMap::new(),
            continues_cursor,
        };
        collection.admit(&published, &horizons)?;
        Ok(collection)
    }

    /// The captured state must still serve the bound: the counters the cursor carries, no redo
    /// in progress, a readable bound block, a publication at or above it, and the same
    /// classification horizon with the publication still below it.
    fn admit(
        &mut self,
        published: &BTreeMap<String, bigname_storage::ChainPosition>,
        horizons: &BTreeMap<String, i64>,
    ) -> V2Result<()> {
        if self.bound.iter().any(|(chain, bound)| {
            self.captured.redo.get(chain).is_none_or(|redo| {
                redo.interpret.generation != bound.interpret_generation
                    || redo.project.generation != bound.project_generation
            })
        }) {
            return Err(self.changed());
        }
        if self.captured.interpret_redo_active {
            return Err(super::history::history_redo_stale_error());
        }
        if self
            .captured
            .redo
            .values()
            .any(|redo| redo.project.in_progress)
        {
            return Err(project_redo_stale_error());
        }
        for (chain, bound) in &self.bound {
            let block = self
                .captured
                .readable_bound_block(chain, bound.block_number)
                .ok_or_else(|| self.changed())?;
            if published
                .get(chain)
                .is_none_or(|position| position.block_number < bound.block_number)
            {
                return Err(self.changed());
            }
            if bound.classification_horizon != horizons.get(chain).copied()
                || crossed_horizon(bound, published.get(chain), &self.captured, chain)
            {
                return Err(self.changed());
            }
            let numeric = super::slug_to_numeric(chain).ok_or_else(|| {
                V2Error::internal_error(format!("history bound uses unmapped chain_id {chain}"))
            })?;
            self.as_of.insert(
                numeric.to_string(),
                AsOf {
                    block_number: bound.block_number as u64,
                    block_hash: bound.block_hash.clone(),
                    timestamp: super::format_timestamp(block.block_timestamp),
                },
            );
        }
        Ok(())
    }

    /// The per-chain block every read of this request stops at.
    pub(crate) fn block_bounds(&self) -> BTreeMap<String, i64> {
        self.bound
            .iter()
            .map(|(chain, bound)| (chain.clone(), bound.block_number))
            .collect()
    }

    pub(crate) fn bind_cursor(&self, mut cursor: CursorPayload) -> CursorPayload {
        cursor.snapshot = None;
        cursor.evaluated_at = None;
        cursor.binding = Some(Binding {
            policy: BindingPolicy::HistoryBound,
            manifests: self.manifests.clone(),
            chains: Some(self.bound.clone()),
        });
        cursor
    }

    /// Recheck after the page, then disclose the bound as `meta.as_of`.
    pub(crate) async fn finish(&self, state: &AppState) -> V2Result<Meta> {
        #[cfg(test)]
        super::collection_snapshot::finish_test_hooks::run(&state.pool).await?;
        self.recheck(state).await?;
        Ok(Meta {
            as_of: Some(self.as_of.clone()),
            ..Meta::default()
        })
    }

    /// An exit after capture that would answer `400` or `404` answers `409 stale` instead when
    /// the recheck fails: the answer may have come from state the bound no longer describes.
    pub(crate) async fn fail(&self, state: &AppState, error: V2Error) -> V2Error {
        if !matches!(error.code(), ErrorCode::InvalidInput | ErrorCode::NotFound) {
            return error;
        }
        #[cfg(test)]
        if let Err(hook_error) =
            super::collection_snapshot::finish_test_hooks::run(&state.pool).await
        {
            return hook_error;
        }
        match self.recheck(state).await {
            Ok(()) => error,
            Err(stale) => stale,
        }
    }

    /// The check after the page, in fresh transactions: the page's repeatable-read snapshot
    /// cannot see a redo committed after it began. The redo state and bound blocks as captured,
    /// no redo in progress, the same manifests, and a publication still at or above the bound.
    /// A publication that advanced is fine: it did not change rows at or below the bound.
    async fn recheck(&self, state: &AppState) -> V2Result<()> {
        let bound_blocks = self
            .bound
            .iter()
            .map(|(chain, bound)| (chain.clone(), bound.block_hash.clone()))
            .collect();
        let current = capture_bound_state(state, &bound_blocks).await?;
        if current.redo != self.captured.redo
            || current.interpret_redo_active
            || current.redo.values().any(|redo| redo.project.in_progress)
            || self.bound.iter().any(|(chain, bound)| {
                current
                    .readable_bound_block(chain, bound.block_number)
                    .is_none()
            })
        {
            return Err(self.changed());
        }
        let namespaces =
            reload_collection_namespace_set(state, &self.namespaces, self.namespace.as_deref())
                .await
                .map_err(|error| {
                    if error.status == axum::http::StatusCode::CONFLICT {
                        self.changed()
                    } else {
                        api_error_to_v2(error)
                    }
                })?;
        if namespaces.manifest_digest() != self.manifests {
            return Err(self.changed());
        }
        let published = published_positions(&namespaces).ok_or_else(not_available)?;
        if !published.keys().eq(self.bound.keys())
            || self
                .bound
                .iter()
                .any(|(chain, bound)| published[chain].block_number < bound.block_number)
        {
            return Err(self.changed());
        }
        let horizons = classification_horizons(state, &self.block_bounds()).await?;
        if self.bound.iter().any(|(chain, bound)| {
            bound.classification_horizon != horizons.get(chain).copied()
                || crossed_horizon(bound, published.get(chain), &current, chain)
        }) {
            return Err(self.changed());
        }
        Ok(())
    }

    /// A first page is retried; a continuation's bound no longer holds, so it restarts.
    fn changed(&self) -> V2Error {
        if self.continues_cursor {
            restart_required()
        } else {
            changed_during_read()
        }
    }
}

/// The request cursor's history binding. A cursor carrying both the old publication token and
/// a binding is malformed (no server mints both); a binding of another family does not belong
/// to this route; a cursor without a binding was issued before this rule.
fn history_binding(cursor: &CursorPayload) -> V2Result<&Binding> {
    let Some(binding) = cursor.binding.as_ref() else {
        return Err(legacy_cursor());
    };
    if cursor.snapshot.is_some() || binding.policy != BindingPolicy::HistoryBound {
        return Err(invalid_cursor_error());
    }
    Ok(binding)
}

fn legacy_cursor() -> V2Error {
    V2Error::stale(
        "this cursor was issued before position-bound pagination; restart without a cursor",
    )
}

fn project_redo_stale_error() -> V2Error {
    V2Error::stale("history is temporarily unavailable while Project redo is in progress")
}

/// The served publication's position on every chain of the request scope, or `None` when some
/// scope has no servable publication. Two scopes on one chain bind the lower position.
fn published_positions(
    namespaces: &PublicNamespaceSet,
) -> Option<BTreeMap<String, bigname_storage::ChainPosition>> {
    if namespaces.is_empty() {
        return None;
    }
    let mut positions = BTreeMap::<String, bigname_storage::ChainPosition>::new();
    for scope in namespaces.request_scope() {
        for position in scope.selected()?.chain_positions.as_map().values() {
            let entry = positions
                .entry(position.chain_id.clone())
                .or_insert_with(|| position.clone());
            if position.block_number < entry.block_number {
                *entry = position.clone();
            }
        }
    }
    Some(positions)
}

/// Whether Project may already classify a resolver differently than it did at the bound, which
/// the bounded reads cannot see: the served publication or the chain's readable head reached the
/// bound's classification horizon. Project commits its projection swap before the phase runner
/// records the new position, and after a crash in that gap recovery labels the old position
/// completed again, so the served position can lag the classification the reads join
/// (bigname: `crates/project/src/engine.rs:44-63`, `apps/phase-runner/src/runner_batch.rs:160-163`,
/// `apps/phase-runner/src/runner_recovery.rs:96-135`). Project's normal target is the readable
/// head, so no swap reaches the horizon before the head does.
fn crossed_horizon(
    bound: &BoundChain,
    published: Option<&bigname_storage::ChainPosition>,
    state: &HistoryBoundState,
    chain: &str,
) -> bool {
    let reached = published
        .map(|position| position.block_number)
        .max(state.readable_heads.get(chain).copied());
    bound
        .classification_horizon
        .zip(reached)
        .is_some_and(|(horizon, reached)| reached >= horizon)
}

async fn classification_horizons(
    state: &AppState,
    block_bounds: &BTreeMap<String, i64>,
) -> V2Result<BTreeMap<String, i64>> {
    bigname_storage::load_classification_horizons(&state.pool, block_bounds)
        .await
        .map_err(|error| {
            tracing::error!(error = ?error, "failed to load classification horizons");
            V2Error::internal_error("failed to load history")
        })
}

async fn capture_bound_state(
    state: &AppState,
    bound_blocks: &BTreeMap<String, String>,
) -> V2Result<HistoryBoundState> {
    bigname_storage::capture_history_bound_state(&state.pool, bound_blocks)
        .await
        .map_err(|error| {
            if error.downcast_ref::<PhaseRedoStateMissing>().is_some() {
                not_available()
            } else {
                tracing::error!(error = ?error, "failed to capture history bound state");
                V2Error::internal_error("failed to load history")
            }
        })
}
