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
#[path = "streamed/guard.rs"]
mod guard;
#[path = "streamed/options.rs"]
mod options;
#[path = "streamed/progress.rs"]
mod progress;
#[path = "streamed/staging.rs"]
mod staging;
#[path = "streamed/walk_pages.rs"]
mod walk_pages;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;

use anyhow::{Context, Result, bail, ensure};
use sqlx::PgPool;

use super::super::admission_epoch::{
    bump_discovery_admission_epochs, fence_discovery_admission_epoch_writes,
};
use super::super::loading::load_streamed_discovery_admission_state_with_excluded_source;
use super::super::types::{DiscoveryObservation, DiscoveryReconciliationSummary};
use super::bulk::{
    deactivate_reconciled_discovery_edge, insert_reconciled_discovery_edges,
    reconcile_historical_discovery_edges_with_progress,
};
use super::cascade::cascade_deactivation_terminal_states;
use super::chronology::edge_starts_after_terminal;
use super::full::protects_non_orphaned_newer_edge;
use super::support::{lock_discovery_reconciliation, observation_terminal_states};
use super::{compare_reconciled_discovery_edge_specs, safe_deactivation_terminal};
use crate::{
    FullDiscoveryReconciliationOptions, ManifestRuntimeProgress,
    managed_edges::reconcile_active_contract_instance_addresses_with_mutations_and_progress,
};

use self::diff::{
    collect_same_assignment_retained_edges, collect_streamed_historical_edges,
    create_streamed_insert_candidate_table, finish_streamed_insert_candidate_table,
    load_streamed_deactivation_source_page, load_streamed_insert_candidate_page,
    materialize_streamed_insert_candidate_page,
};
use self::guard::{
    default_max_deactivation_candidates, default_max_deactivations,
    max_deactivations_override_from_env,
};
pub(crate) use self::options::StreamedDiscoveryReconciliationOptions;
use self::progress::{PageSourceManifestProgress, load_active_edge_summary_with_progress};
use self::staging::{
    create_streamed_reconcile_temp_tables, load_streamed_observations_for_keys,
    stage_streamed_observations,
};
use self::walk_pages::run_streamed_admission_walk;

/// Environment override for the streamed reconcile's deactivation guard.
/// Holds the maximum number of deactivation candidates permitted before the
/// reconcile aborts (replacing the default `max(10_000, 1%)` bound).
pub use guard::DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV;

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

    /// Report a completed bounded reconciliation unit. The default is a
    /// no-op; operational callers can use it to keep liveness tied to actual
    /// streamed progress without a detached timer.
    fn record_progress(&self) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
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
    source: &(impl DiscoveryObservationPageSource + Sync),
) -> Result<DiscoveryReconciliationSummary> {
    let options = StreamedDiscoveryReconciliationOptions {
        max_deactivations_override: max_deactivations_override_from_env()?,
        ..StreamedDiscoveryReconciliationOptions::default()
    };
    reconcile_discovery_observations_streamed_inner(
        pool,
        discovery_source,
        source,
        options,
        FullDiscoveryReconciliationOptions::default(),
    )
    .await
}

pub async fn reconcile_discovery_observations_streamed_with_full_options(
    pool: &PgPool,
    discovery_source: &str,
    source: &(impl DiscoveryObservationPageSource + Sync),
    full_options: FullDiscoveryReconciliationOptions<'_>,
) -> Result<DiscoveryReconciliationSummary> {
    let options = StreamedDiscoveryReconciliationOptions {
        max_deactivations_override: max_deactivations_override_from_env()?,
        ..StreamedDiscoveryReconciliationOptions::default()
    };
    reconcile_discovery_observations_streamed_inner(
        pool,
        discovery_source,
        source,
        options,
        full_options,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn reconcile_discovery_observations_streamed_with_options(
    pool: &PgPool,
    discovery_source: &str,
    source: &(impl DiscoveryObservationPageSource + Sync),
    options: StreamedDiscoveryReconciliationOptions,
) -> Result<DiscoveryReconciliationSummary> {
    reconcile_discovery_observations_streamed_inner(
        pool,
        discovery_source,
        source,
        options,
        FullDiscoveryReconciliationOptions::default(),
    )
    .await
}

async fn reconcile_discovery_observations_streamed_inner(
    pool: &PgPool,
    discovery_source: &str,
    source: &(impl DiscoveryObservationPageSource + Sync),
    options: StreamedDiscoveryReconciliationOptions,
    full_options: FullDiscoveryReconciliationOptions<'_>,
) -> Result<DiscoveryReconciliationSummary> {
    let through_block_number = full_options.through_block_number;
    let expected_admission_epoch = full_options
        .expected_admission_epoch
        .map(|expected| (expected.chain, expected.epoch));
    let mut transaction = pool
        .begin()
        .await
        .context("failed to start streamed discovery-edge reconciliation transaction")?;
    // One snapshot for the whole reconcile: the in-memory variant reads its
    // entire plan from single loads, and the streamed variant's per-page
    // known-address resolution and set-diff queries must see the same frozen
    // state rather than statement-level READ COMMITTED snapshots. Any
    // concurrent discovery mutation must bump the fenced admission-epoch
    // rows (#125), so an interleaving writer surfaces as a clean
    // serialization failure at the FOR UPDATE fence below instead of a
    // silently stale diff. Such a failure is fail-loud: the Err propagates
    // out of the replay run (the in-process loop repeats only on the
    // repeat_checkpoint_replay flag, never on errors) and the checkpoint
    // stays resumable, so the operator simply reruns the replay.
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(transaction.as_mut())
        .await
        .context("failed to pin the streamed reconcile transaction snapshot")?;
    lock_discovery_reconciliation(transaction.as_mut(), discovery_source).await?;
    create_streamed_reconcile_temp_tables(transaction.as_mut()).await?;

    let staged = stage_streamed_observations(transaction.as_mut(), source, &options).await?;

    let (active_edge_count_before, active_edge_chains) = load_active_edge_summary_with_progress(
        transaction.as_mut(),
        discovery_source,
        options.observation_page_limit,
        source,
    )
    .await?;
    let mut candidate_chains = staged.observation_chains.clone();
    candidate_chains.extend(active_edge_chains);
    if let Some((chain, _)) = expected_admission_epoch {
        candidate_chains.insert(chain.to_owned());
    }
    fence_discovery_admission_epoch_writes(transaction.as_mut(), &candidate_chains).await?;
    if let Some((chain, expected_epoch)) = expected_admission_epoch {
        ensure!(
            candidate_chains.iter().all(|candidate| candidate == chain),
            "discovery source {discovery_source} expected epoch fence for {chain} cannot reconcile observations from another chain"
        );
        let current_epoch = sqlx::query_scalar::<_, i64>(
            "SELECT epoch FROM discovery_admission_epochs WHERE chain_id = $1",
        )
        .bind(chain)
        .fetch_optional(transaction.as_mut())
        .await
        .with_context(|| {
            format!("failed to read discovery admission epoch for {chain} under the writer fence")
        })?
        .unwrap_or(0);
        ensure!(
            current_epoch == expected_epoch,
            "discovery admission epoch changed before full-source reconciliation for {chain}: expected {expected_epoch}, observed {current_epoch}"
        );
    }

    let admission_state = load_streamed_discovery_admission_state_with_excluded_source(
        transaction.as_mut(),
        Some(discovery_source),
        source,
    )
    .await?;
    source.record_progress().await?;
    let admitted_edge_count =
        run_streamed_admission_walk(transaction.as_mut(), &admission_state, &options, source)
            .await?;

    // Everything below diffs against the pre-mutation edge snapshot, exactly
    // like the in-memory reconcile computes its whole plan from one
    // `load_active_reconciled_discovery_edges` read before mutating.
    create_streamed_insert_candidate_table(transaction.as_mut()).await?;
    let diff_page_limit = i64::try_from(options.mutation_batch_size.max(1))
        .context("streamed reconcile mutation batch size overflowed i64")?;
    let mut insert_candidate_count = 0usize;
    let mut desired_edge_count = 0usize;
    let mut after_desired_row_id = 0i64;
    loop {
        let (last_row_id, source_rows, inserted_rows) = materialize_streamed_insert_candidate_page(
            transaction.as_mut(),
            discovery_source,
            after_desired_row_id,
            diff_page_limit,
        )
        .await?;
        let Some(last_row_id) = last_row_id else {
            break;
        };
        after_desired_row_id = last_row_id;
        desired_edge_count += source_rows;
        insert_candidate_count += inserted_rows;
        source.record_progress().await?;
    }
    finish_streamed_insert_candidate_table(transaction.as_mut()).await?;

    let max_deactivation_candidates =
        options.coarse_deactivation_cap_override.unwrap_or_else(|| {
            default_max_deactivation_candidates(active_edge_count_before)
                .max(options.max_deactivations_override.unwrap_or(0))
        });
    let mut deactivation_candidates = Vec::new();
    let mut after_edge_id = 0i64;
    #[cfg(test)]
    let mut deactivation_source_page_count = 0usize;
    loop {
        let page = load_streamed_deactivation_source_page(
            transaction.as_mut(),
            discovery_source,
            after_edge_id,
            diff_page_limit,
        )
        .await?;
        let Some(last_edge_id) = page.last_edge_id else {
            break;
        };
        after_edge_id = last_edge_id;
        let candidate_count_after_page = deactivation_candidates
            .len()
            .checked_add(page.candidates.len())
            .context("streamed deactivation candidate count overflowed usize")?;
        if candidate_count_after_page > max_deactivation_candidates {
            bail!(
                "streamed discovery reconciliation for {discovery_source} loaded at least \
                 {candidate_count_after_page} deactivation candidates against \
                 {active_edge_count_before} active edges, over the \
                 {max_deactivation_candidates} candidate load cap; refusing to materialize \
                 more of the diff — this indicates spec drift, raise \
                 {DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV} only after confirming the \
                 diff is intended"
            );
        }
        deactivation_candidates.extend(page.candidates);
        source.record_progress().await?;
        #[cfg(test)]
        {
            deactivation_source_page_count += 1;
            if options
                .fail_after_deactivation_source_pages
                .is_some_and(|limit| deactivation_source_page_count > limit)
            {
                bail!("injected deactivation source-page scan limit exceeded");
            }
        }
    }
    let deactivation_candidate_count = deactivation_candidates.len();
    tracing::info!(
        discovery_source,
        staged_observation_count = staged.staged_observation_count,
        desired_edge_count,
        active_edge_count = active_edge_count_before,
        deactivation_candidate_count,
        insert_candidate_count,
        "streamed discovery reconciliation diff computed"
    );
    let candidate_observations = load_streamed_observations_for_keys(
        transaction.as_mut(),
        &deactivation_candidates,
        options.mutation_batch_size,
        source,
    )
    .await?;
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
        options.mutation_batch_size,
        &mut retained_newer_edge_ids,
        source,
    )
    .await?;
    // Chronology rule 2: a desired edge with a newer non-orphaned successor
    // for the same assignment start is materialized as a closed historical
    // epoch and the successor is retained.
    let historical_edges = collect_streamed_historical_edges(
        transaction.as_mut(),
        discovery_source,
        i64::try_from(options.mutation_batch_size)
            .context("streamed reconcile mutation batch size overflowed i64")?,
        &mut retained_newer_edge_ids,
        source,
    )
    .await?;
    source.record_progress().await?;
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

    // Precise fail-closed guard on the exact planned deactivation set:
    // candidates the chronology rules retain or the cascade-terminal
    // protection skips (e.g. many descendants starting after an ancestor
    // tombstone's terminal event) are not deactivations and must not trip
    // it. The filter below evaluates the same skip predicates as the
    // mutation loop, so the count is exact. A verified full-closure replay
    // finalize is a near-no-op, so a mass deactivation here indicates spec
    // drift.
    let planned_deactivation_count = deactivation_candidates
        .iter()
        .filter(|candidate| {
            !retained_newer_edge_ids.contains(&candidate.discovery_edge_id)
                && !protects_non_orphaned_newer_edge(
                    candidate,
                    deactivation_terminal_states_by_key.get(&candidate.spec.observation_key),
                    through_block_number,
                )
        })
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
    for (candidate_index, candidate) in deactivation_candidates.iter().enumerate() {
        if candidate_index > 0 && candidate_index.is_multiple_of(options.mutation_batch_size.max(1))
        {
            source.record_progress().await?;
        }
        if retained_newer_edge_ids.contains(&candidate.discovery_edge_id) {
            continue;
        }
        let terminal_state =
            deactivation_terminal_states_by_key.get(&candidate.spec.observation_key);
        if protects_non_orphaned_newer_edge(candidate, terminal_state, through_block_number) {
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
    source.record_progress().await?;

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
            source.record_progress().await?;
            continue;
        }
        let edge_insert = insert_reconciled_discovery_edges(transaction.as_mut(), &batch).await?;
        inserted_edge_count += edge_insert.inserted_count + edge_insert.reactivated_count;
        mutated_chains.extend(batch.iter().map(|edge| edge.chain.clone()));
        source.record_progress().await?;
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
    let mut historical_progress = PageSourceManifestProgress::new(source);
    let historical_edge_reconciliation = reconcile_historical_discovery_edges_with_progress(
        transaction.as_mut(),
        &historical_edge_refs,
        pool,
        &mut historical_progress,
    )
    .await?;
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
        let mut callback = PageSourceManifestProgress::new(source);
        let mut progress: Option<&mut dyn ManifestRuntimeProgress> = Some(&mut callback);
        reconcile_active_contract_instance_addresses_with_mutations_and_progress(
            transaction.as_mut(),
            pool,
            &mut progress,
        )
        .await?;
    }
    let (active_edge_count, _) = load_active_edge_summary_with_progress(
        transaction.as_mut(),
        discovery_source,
        options.observation_page_limit,
        source,
    )
    .await?;
    let admission_epoch_bump_count = mutated_chains.len();
    bump_discovery_admission_epochs(transaction.as_mut(), &mutated_chains).await?;
    source.record_progress().await?;

    transaction
        .commit()
        .await
        .context("failed to commit streamed discovery-edge reconciliation transaction")?;
    source.record_progress().await?;

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
