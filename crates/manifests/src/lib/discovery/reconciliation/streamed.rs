//! Streamed full-source discovery reconciliation for retained observation
//! sets which are too large to materialize in memory (#168).
//!
//! The in-memory `reconcile_discovery_observations` builds the observation
//! maps, the desired-spec set, and the full active-edge set as Rust
//! collections. This variant stages the same inputs into `ON COMMIT DROP`
//! temp tables, runs the identical fixed-point admission walk over
//! database pages, and computes the deactivate/insert diff as SQL set
//! differences using the exact spec equality `HashSet<ReconciledDiscovery
//! EdgeSpec>` uses today. Resident memory is bounded by one page, the
//! derived-contract closure, pending contract-instance seeds, and the
//! not-in-desired diff (expected near-empty for a verified full-closure
//! replay and guarded below).

#[path = "streamed/diff.rs"]
mod diff;
#[path = "streamed/staging.rs"]
mod staging;
#[path = "streamed/walk_pages.rs"]
mod walk_pages;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;

use anyhow::{Context, Result, bail};
use sqlx::PgPool;

use super::super::admission_epoch::{
    bump_discovery_admission_epochs, fence_discovery_admission_epoch_writes,
};
use super::super::loading::load_streamed_discovery_admission_state_with_excluded_source;
use super::super::types::{DiscoveryObservation, DiscoveryReconciliationSummary};
use super::bulk::{
    deactivate_reconciled_discovery_edge, insert_reconciled_discovery_edges,
    reconcile_historical_discovery_edges,
};
use super::cascade::cascade_deactivation_terminal_states;
use super::chronology::edge_starts_after_terminal;
use super::existing::{
    load_active_reconciled_discovery_edge_chains, load_active_reconciled_discovery_edge_count,
};
use super::full::protects_non_orphaned_newer_edge;
use super::support::{lock_discovery_reconciliation, observation_terminal_states};
use super::{compare_reconciled_discovery_edge_specs, safe_deactivation_terminal};
use crate::reconcile_active_contract_instance_addresses;

use self::diff::{
    collect_same_assignment_retained_edges, collect_streamed_historical_edges,
    count_streamed_deactivation_candidates, load_streamed_deactivation_candidates,
    load_streamed_insert_candidate_page, materialize_streamed_insert_candidates,
};
use self::staging::{
    count_temp_rows, create_streamed_reconcile_temp_tables, load_streamed_observations_for_keys,
    stage_streamed_observations,
};
use self::walk_pages::run_streamed_admission_walk;

/// Environment override for the streamed reconcile's deactivation guard.
/// Holds the maximum number of deactivation candidates permitted before the
/// reconcile aborts (replacing the default `max(10_000, 1%)` bound).
pub const DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV: &str =
    "BIGNAME_INDEXER_DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS";

const DEACTIVATION_GUARD_FLOOR: usize = 10_000;
const CANDIDATE_LOAD_CAP_FLOOR: usize = 100_000;
const DEFAULT_OBSERVATION_PAGE_LIMIT: i64 = 10_000;
const DEFAULT_MUTATION_BATCH_SIZE: usize = 1_000;

/// Ascending-key page source over a complete, latest-per-key retained
/// observation set for one discovery source.
///
/// Contract: pages are ordered by a stable, strictly ascending ordering key;
/// each call returns observations whose key sorts strictly after
/// `after_key`, at most `limit` of them (fewer is permitted); an empty page
/// ends the stream. Every observation must carry a unique
/// `provenance.observation_key` — the streamed reconcile rejects duplicates
/// because, unlike the in-memory reconcile's maps, staged rows cannot
/// preserve two observations for one key.
pub trait DiscoveryObservationPageSource {
    fn load_page(
        &self,
        after_key: Option<&str>,
        limit: i64,
    ) -> impl Future<Output = Result<Vec<(String, DiscoveryObservation)>>> + Send;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct StreamedDiscoveryReconciliationOptions {
    /// Replaces the default `max(10_000, 1% of active edges)` precise
    /// deactivation guard bound when set, and raises the coarse candidate
    /// load cap to at least the same value so an operator override stays
    /// effective end to end.
    pub(crate) max_deactivations_override: Option<usize>,
    /// Replaces the default `max(100_000, 10% of active edges)` coarse cap
    /// on how many deactivation candidates may be materialized in memory.
    /// Test hook; production overrides go through the env-driven precise
    /// bound, which raises this cap alongside it.
    pub(crate) coarse_deactivation_cap_override: Option<usize>,
    pub(crate) observation_page_limit: i64,
    pub(crate) mutation_batch_size: usize,
}

impl Default for StreamedDiscoveryReconciliationOptions {
    fn default() -> Self {
        Self {
            max_deactivations_override: None,
            coarse_deactivation_cap_override: None,
            observation_page_limit: DEFAULT_OBSERVATION_PAGE_LIMIT,
            mutation_batch_size: DEFAULT_MUTATION_BATCH_SIZE,
        }
    }
}

/// Reconcile a complete latest-per-key retained observation set for one
/// discovery source with memory bounded by pages instead of the observation
/// count, producing the same `discovery_edges` outcome as
/// `reconcile_discovery_observations` with default options would for the
/// identical observation set.
///
/// Differences from the in-memory function's contract:
/// - `admitted_edges` on the returned summary is always empty: the only
///   caller (the full-closure replay finalize) ignores it, and returning it
///   would reintroduce an observation-scale allocation.
///   `admitted_edge_count` is still exact.
/// - A deliberate two-level safety guard aborts before mutating: a coarse
///   candidate load cap of `max(100_000, 10%)` of the source's active edges
///   bounds the in-memory diff materialization, and the precise threshold
///   of `max(10_000, 1%)` applies to the post-chronology deactivation set
///   (override via `BIGNAME_INDEXER_DISCOVERY_FULL_RECONCILE_MAX_
///   DEACTIVATIONS`, which raises both): a full-closure finalize after a
///   verified rederive must be a near-no-op, and a mass deactivation
///   indicates spec drift.
pub async fn reconcile_discovery_observations_streamed(
    pool: &PgPool,
    discovery_source: &str,
    source: &impl DiscoveryObservationPageSource,
) -> Result<DiscoveryReconciliationSummary> {
    let options = StreamedDiscoveryReconciliationOptions {
        max_deactivations_override: max_deactivations_override_from_env()?,
        ..StreamedDiscoveryReconciliationOptions::default()
    };
    reconcile_discovery_observations_streamed_with_options(pool, discovery_source, source, options)
        .await
}

pub(crate) async fn reconcile_discovery_observations_streamed_with_options(
    pool: &PgPool,
    discovery_source: &str,
    source: &impl DiscoveryObservationPageSource,
    options: StreamedDiscoveryReconciliationOptions,
) -> Result<DiscoveryReconciliationSummary> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start streamed discovery-edge reconciliation transaction")?;
    lock_discovery_reconciliation(transaction.as_mut(), discovery_source).await?;
    create_streamed_reconcile_temp_tables(transaction.as_mut()).await?;

    let staged = stage_streamed_observations(transaction.as_mut(), source, &options).await?;

    let mut candidate_chains = staged.observation_chains.clone();
    candidate_chains.extend(
        load_active_reconciled_discovery_edge_chains(transaction.as_mut(), discovery_source)
            .await?,
    );
    fence_discovery_admission_epoch_writes(transaction.as_mut(), &candidate_chains).await?;

    let admission_state = load_streamed_discovery_admission_state_with_excluded_source(
        transaction.as_mut(),
        Some(discovery_source),
    )
    .await?;
    run_streamed_admission_walk(transaction.as_mut(), &admission_state, &options).await?;

    // Everything below diffs against the pre-mutation edge snapshot, exactly
    // like the in-memory reconcile computes its whole plan from one
    // `load_active_reconciled_discovery_edges` read before mutating.
    materialize_streamed_insert_candidates(transaction.as_mut(), discovery_source).await?;
    let insert_candidate_count =
        count_temp_rows(transaction.as_mut(), "reconcile_insert_candidates").await?;
    let deactivation_candidate_count =
        count_streamed_deactivation_candidates(transaction.as_mut(), discovery_source).await?;
    let active_edge_count_before =
        load_active_reconciled_discovery_edge_count(transaction.as_mut(), discovery_source).await?;
    let desired_edge_count =
        count_temp_rows(transaction.as_mut(), "reconcile_desired_edges").await?;
    tracing::info!(
        discovery_source,
        staged_observation_count = staged.staged_observation_count,
        desired_edge_count,
        active_edge_count = active_edge_count_before,
        deactivation_candidate_count,
        insert_candidate_count,
        "streamed discovery reconciliation diff computed"
    );
    // Coarse memory cap: candidates are materialized in memory below, so a
    // spec-drift flood must abort on the SQL count before anything is
    // loaded. Chronology-retained candidates are excluded from deactivation
    // only after loading, so the precise fail-closed threshold is applied
    // separately, right before mutating.
    let max_deactivation_candidates =
        options.coarse_deactivation_cap_override.unwrap_or_else(|| {
            default_max_deactivation_candidates(active_edge_count_before)
                .max(options.max_deactivations_override.unwrap_or(0))
        });
    if deactivation_candidate_count > max_deactivation_candidates {
        bail!(
            "streamed discovery reconciliation for {discovery_source} computed \
             {deactivation_candidate_count} deactivation candidates against \
             {active_edge_count_before} active edges, over the {max_deactivation_candidates} \
             candidate load cap; refusing to materialize the diff — this indicates spec drift, \
             raise {DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV} only after confirming the \
             diff is intended"
        );
    }

    let deactivation_candidates =
        load_streamed_deactivation_candidates(transaction.as_mut(), discovery_source).await?;
    let candidate_observations =
        load_streamed_observations_for_keys(transaction.as_mut(), &deactivation_candidates).await?;
    let direct_terminal_states_by_key = observation_terminal_states(&candidate_observations)?;
    let observations_by_key = candidate_observations
        .iter()
        .map(|observation| {
            Ok((
                super::super::provenance::observation_key(observation)?,
                observation,
            ))
        })
        .collect::<Result<HashMap<_, _>>>()?;

    let mut retained_newer_edge_ids = HashSet::<i64>::new();
    // Chronology rule 1: a non-orphaned edge which starts after its own
    // observation's terminal event stays current.
    for candidate in &deactivation_candidates {
        if candidate.active_from_block_is_orphaned {
            continue;
        }
        if let Some(terminal_state) =
            direct_terminal_states_by_key.get(&candidate.spec.observation_key)
            && edge_starts_after_terminal(candidate, terminal_state)
        {
            retained_newer_edge_ids.insert(candidate.discovery_edge_id);
        }
    }
    // Chronology rule 3: a desired assignment already materialized by an
    // earlier-starting epoch retains that one earliest epoch. Only desired
    // edges sharing an assignment identity with a deactivation candidate can
    // retain a candidate, so the min-epoch resolution stays diff-sized.
    collect_same_assignment_retained_edges(
        transaction.as_mut(),
        discovery_source,
        &deactivation_candidates,
        &mut retained_newer_edge_ids,
    )
    .await?;
    // Chronology rule 2: a desired edge with a newer non-orphaned successor
    // for the same assignment start is materialized as a closed historical
    // epoch and the successor is retained.
    let historical_edges = collect_streamed_historical_edges(
        transaction.as_mut(),
        discovery_source,
        &mut retained_newer_edge_ids,
    )
    .await?;
    let historical_row_ids = historical_edges
        .iter()
        .map(|(desired_row_id, _, _)| *desired_row_id)
        .collect::<HashSet<_>>();

    // The candidates are exactly the not-in-desired active edges, so the
    // cascade sees the same iteration the in-memory fixed point sees after
    // its desired-set filter.
    let empty_desired_set = HashSet::new();
    let deactivation_terminal_states_by_key = cascade_deactivation_terminal_states(
        &deactivation_candidates,
        &empty_desired_set,
        &observations_by_key,
        &direct_terminal_states_by_key,
    )?;

    // Precise fail-closed guard on the post-chronology deactivation set:
    // candidates the chronology rules retain are not deactivations and must
    // not trip it. A verified full-closure replay finalize is a near-no-op,
    // so a mass deactivation here indicates spec drift.
    let planned_deactivation_count = deactivation_candidates
        .iter()
        .filter(|candidate| !retained_newer_edge_ids.contains(&candidate.discovery_edge_id))
        .count();
    let max_deactivations = options
        .max_deactivations_override
        .unwrap_or_else(|| default_max_deactivations(active_edge_count_before));
    if planned_deactivation_count > max_deactivations {
        tracing::warn!(
            discovery_source,
            planned_deactivation_count,
            active_edge_count = active_edge_count_before,
            max_deactivations,
            "streamed discovery reconciliation aborting on the deactivation guard"
        );
        bail!(
            "streamed discovery reconciliation for {discovery_source} would deactivate \
             {planned_deactivation_count} of {active_edge_count_before} active edges, over the \
             {max_deactivations} guard; a verified full-closure replay must be a near-no-op, so \
             this indicates spec drift — override via {DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV} \
             only after confirming the diff is intended"
        );
    }
    if planned_deactivation_count > 0 && planned_deactivation_count * 2 >= max_deactivations {
        tracing::warn!(
            discovery_source,
            planned_deactivation_count,
            active_edge_count = active_edge_count_before,
            max_deactivations,
            "streamed discovery reconciliation deactivation diff is approaching the guard"
        );
    }

    let mut deactivated_edge_count = 0;
    let mut mutated_chains = BTreeSet::new();
    for candidate in &deactivation_candidates {
        if retained_newer_edge_ids.contains(&candidate.discovery_edge_id) {
            continue;
        }
        let terminal_state =
            deactivation_terminal_states_by_key.get(&candidate.spec.observation_key);
        if protects_non_orphaned_newer_edge(candidate, terminal_state, None) {
            continue;
        }
        let terminal_state = terminal_state
            .cloned()
            .map(|terminal_state| safe_deactivation_terminal(candidate, terminal_state));
        let deactivated = deactivate_reconciled_discovery_edge(
            transaction.as_mut(),
            candidate.discovery_edge_id,
            terminal_state.as_ref(),
        )
        .await?;
        if deactivated {
            mutated_chains.insert(candidate.spec.chain.clone());
            deactivated_edge_count += 1;
        }
    }

    let mut inserted_edge_count = 0;
    let mut after_row_id = 0i64;
    loop {
        let page = load_streamed_insert_candidate_page(
            transaction.as_mut(),
            after_row_id,
            i64::try_from(options.mutation_batch_size)
                .context("streamed reconcile mutation batch size overflowed i64")?,
        )
        .await?;
        let Some((last_row_id, _)) = page.last() else {
            break;
        };
        after_row_id = *last_row_id;
        let batch = page
            .iter()
            .filter(|(desired_row_id, _)| !historical_row_ids.contains(desired_row_id))
            .map(|(_, spec)| spec)
            .collect::<Vec<_>>();
        if batch.is_empty() {
            continue;
        }
        let edge_insert = insert_reconciled_discovery_edges(transaction.as_mut(), &batch).await?;
        inserted_edge_count += edge_insert.inserted_count + edge_insert.reactivated_count;
        mutated_chains.extend(batch.iter().map(|edge| edge.chain.clone()));
    }

    let mut historical_edges = historical_edges
        .into_iter()
        .map(|(_, spec, terminal_state)| (spec, terminal_state))
        .collect::<Vec<_>>();
    historical_edges
        .sort_by(|(left, _), (right, _)| compare_reconciled_discovery_edge_specs(left, right));
    let historical_edge_refs = historical_edges
        .iter()
        .map(|(spec, terminal_state)| (spec, terminal_state.clone()))
        .collect::<Vec<_>>();
    let historical_edge_reconciliation =
        reconcile_historical_discovery_edges(transaction.as_mut(), &historical_edge_refs).await?;
    inserted_edge_count += historical_edge_reconciliation.inserted_count;
    if historical_edge_reconciliation.inserted_count > 0
        || historical_edge_reconciliation.updated_count > 0
    {
        mutated_chains.extend(historical_edges.iter().map(|(edge, _)| edge.chain.clone()));
    }

    if inserted_edge_count > 0
        || historical_edge_reconciliation.updated_count > 0
        || deactivated_edge_count > 0
    {
        reconcile_active_contract_instance_addresses(transaction.as_mut()).await?;
    }
    let active_edge_count =
        load_active_reconciled_discovery_edge_count(transaction.as_mut(), discovery_source).await?;
    let admitted_edge_count = count_temp_rows(transaction.as_mut(), "reconcile_admitted_edges")
        .await
        .context("failed to count streamed admitted discovery edges")?;
    let admission_epoch_bump_count = mutated_chains.len();
    bump_discovery_admission_epochs(transaction.as_mut(), &mutated_chains).await?;

    transaction
        .commit()
        .await
        .context("failed to commit streamed discovery-edge reconciliation transaction")?;

    Ok(DiscoveryReconciliationSummary {
        active_edge_count,
        admitted_edge_count,
        inserted_edge_count,
        deactivated_edge_count,
        admission_epoch_bump_count,
        // Intentionally empty; see the function documentation.
        admitted_edges: Vec::new(),
    })
}

/// Default precise deactivation guard bound: a verified full-closure
/// finalize must be a near-no-op, so anything beyond `max(10_000, 1%)` of
/// the source's active edges aborts instead of silently rewriting the
/// source.
fn default_max_deactivations(active_edge_count: usize) -> usize {
    DEACTIVATION_GUARD_FLOOR.max(active_edge_count / 100)
}

/// Default coarse cap on how many deactivation candidates may be loaded
/// into memory for chronology and cascade resolution: `max(100_000, 10%)`
/// of the source's active edges bounds the only diff-sized allocation the
/// streamed reconcile makes.
fn default_max_deactivation_candidates(active_edge_count: usize) -> usize {
    CANDIDATE_LOAD_CAP_FLOOR.max(active_edge_count / 10)
}

fn max_deactivations_override_from_env() -> Result<Option<usize>> {
    match std::env::var(DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV) {
        Ok(value) => value
            .trim()
            .parse::<usize>()
            .map(Some)
            .with_context(|| format!(
                "failed to parse {DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV} as a deactivation count: {value:?}"
            )),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).context(format!(
            "failed to read {DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_deactivation_guard_uses_floor_and_percentage() {
        assert_eq!(default_max_deactivations(0), 10_000);
        assert_eq!(default_max_deactivations(500_000), 10_000);
        assert_eq!(default_max_deactivations(7_620_084), 76_200);
    }

    #[test]
    fn default_candidate_load_cap_uses_floor_and_percentage() {
        assert_eq!(default_max_deactivation_candidates(0), 100_000);
        assert_eq!(default_max_deactivation_candidates(500_000), 100_000);
        assert_eq!(default_max_deactivation_candidates(7_620_084), 762_008);
    }

    #[test]
    fn deactivation_guard_env_override_parses_or_rejects() {
        // One test drives every state of the process-global variable so
        // parallel test threads never race on it.
        assert_eq!(
            max_deactivations_override_from_env().expect("unset env must read as no override"),
            None
        );
        // SAFETY: this test is the only reader/writer of the variable, and
        // it exercises all transitions itself.
        unsafe { std::env::set_var(DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV, "123456") };
        assert_eq!(
            max_deactivations_override_from_env().expect("numeric override must parse"),
            Some(123_456)
        );
        unsafe {
            std::env::set_var(
                DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV,
                "not-a-count",
            )
        };
        assert!(max_deactivations_override_from_env().is_err());
        unsafe { std::env::remove_var(DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV) };
    }
}
