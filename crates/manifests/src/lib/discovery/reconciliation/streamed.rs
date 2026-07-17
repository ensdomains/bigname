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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;

use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgConnection};

use super::super::admission::DiscoveryAdmissionState;
use super::super::admission_epoch::{
    bump_discovery_admission_epochs, fence_discovery_admission_epoch_writes,
};
use super::super::loading::{
    load_known_contract_instance_addresses,
    load_streamed_discovery_admission_state_with_excluded_source, scoped_address_key_vectors,
};
use super::super::provenance::is_zero_address;
use super::super::types::{
    AdmittedDiscoveryEdge, DiscoveryObservation, DiscoveryReconciliationSummary, EvmEventPosition,
    ExistingReconciledDiscoveryEdge, ObservationTerminalState, ReconciledDiscoveryEdgeSpec,
};
use super::bulk::{
    deactivate_reconciled_discovery_edge, insert_pending_contract_instance_seeds,
    insert_reconciled_discovery_edges, reconcile_historical_discovery_edges,
};
use super::cascade::cascade_deactivation_terminal_states;
use super::chronology::{
    assignment_starts_no_later, compare_edge_starts, edge_starts_after_spec,
    edge_starts_after_terminal,
};
use super::existing::{
    edge_from_row, load_active_reconciled_discovery_edge_chains,
    load_active_reconciled_discovery_edge_count,
};
use super::full::protects_non_orphaned_newer_edge;
use super::support::{lock_discovery_reconciliation, observation_terminal_states};
use super::walk::DiscoveryAdmissionWalk;
use super::{compare_reconciled_discovery_edge_specs, safe_deactivation_terminal};
use crate::{normalize_address, reconcile_active_contract_instance_addresses};

/// Environment override for the streamed reconcile's deactivation guard.
/// Holds the maximum number of deactivation candidates permitted before the
/// reconcile aborts (replacing the default `max(10_000, 1%)` bound).
pub const DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV: &str =
    "BIGNAME_INDEXER_DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS";

const DEACTIVATION_GUARD_FLOOR: usize = 10_000;
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
    /// Replaces the default `max(10_000, 1% of active edges)` deactivation
    /// guard bound when set.
    pub(crate) max_deactivations_override: Option<usize>,
    pub(crate) observation_page_limit: i64,
    pub(crate) mutation_batch_size: usize,
}

impl Default for StreamedDiscoveryReconciliationOptions {
    fn default() -> Self {
        Self {
            max_deactivations_override: None,
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
/// - A deliberate safety guard aborts before mutating when the computed
///   deactivation diff exceeds `max(10_000, 1%)` of the source's active
///   edges (override via `BIGNAME_INDEXER_DISCOVERY_FULL_RECONCILE_MAX_
///   DEACTIVATIONS`): a full-closure finalize after a verified rederive
///   must be a near-no-op, and a mass deactivation indicates spec drift.
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
    tracing::warn!(
        discovery_source,
        staged_observation_count = staged.staged_observation_count,
        desired_edge_count,
        active_edge_count = active_edge_count_before,
        deactivation_candidate_count,
        insert_candidate_count,
        "streamed discovery reconciliation diff computed"
    );
    let max_deactivations = options
        .max_deactivations_override
        .unwrap_or_else(|| default_max_deactivations(active_edge_count_before));
    if deactivation_candidate_count > max_deactivations {
        bail!(
            "streamed discovery reconciliation for {discovery_source} would deactivate \
             {deactivation_candidate_count} of {active_edge_count_before} active edges, over the \
             {max_deactivations} guard; a verified full-closure replay must be a near-no-op, so \
             this indicates spec drift — override via {DISCOVERY_FULL_RECONCILE_MAX_DEACTIVATIONS_ENV} \
             only after confirming the diff is intended"
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

/// Default deactivation guard bound: a verified full-closure finalize must
/// be a near-no-op, so anything beyond `max(10_000, 1%)` of the source's
/// active edges aborts instead of silently rewriting the source.
fn default_max_deactivations(active_edge_count: usize) -> usize {
    DEACTIVATION_GUARD_FLOOR.max(active_edge_count / 100)
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

async fn create_streamed_reconcile_temp_tables(executor: &mut PgConnection) -> Result<()> {
    // One row per staged observation key (the source is latest-per-key):
    // exactly the inputs the terminal states, the admission walk, and the
    // cascade need. `active_to_*` observation fields are not staged because
    // no full-reconciliation consumer reads them. Text keys use the "C"
    // collation so SQL ordering matches Rust byte order.
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.reconcile_observations (
            observation_key TEXT COLLATE "C" PRIMARY KEY,
            chain_id TEXT NOT NULL,
            from_address TEXT NOT NULL,
            normalized_from_address TEXT NOT NULL,
            to_address TEXT NOT NULL,
            edge_kind TEXT NOT NULL,
            discovery_source TEXT NOT NULL,
            active_from_block_number BIGINT,
            active_from_block_hash TEXT,
            provenance JSONB NOT NULL
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to create the streamed reconcile observation temp table")?;

    // Full `ReconciledDiscoveryEdgeSpec` rows. The unique constraint spans
    // the complete spec identity so `ON CONFLICT DO NOTHING` deduplicates
    // exactly like `HashSet<ReconciledDiscoveryEdgeSpec>` insertion
    // (provenance_json is compared as text, matching the spec's string
    // equality; the event-position columns are derived from it and stored
    // for SQL chronology comparisons).
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.reconcile_desired_edges (
            desired_row_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            observation_key TEXT COLLATE "C" NOT NULL,
            chain_id TEXT NOT NULL,
            edge_kind TEXT NOT NULL,
            from_contract_instance_id UUID NOT NULL,
            to_contract_instance_id UUID NOT NULL,
            discovery_source TEXT NOT NULL,
            source_manifest_id BIGINT NOT NULL,
            admission TEXT NOT NULL,
            active_from_block_number BIGINT,
            active_from_block_hash TEXT,
            active_from_transaction_index BIGINT,
            active_from_log_index BIGINT,
            provenance_json TEXT COLLATE "C" NOT NULL,
            UNIQUE NULLS NOT DISTINCT (
                observation_key,
                chain_id,
                edge_kind,
                from_contract_instance_id,
                to_contract_instance_id,
                discovery_source,
                source_manifest_id,
                admission,
                active_from_block_number,
                active_from_block_hash,
                active_from_transaction_index,
                active_from_log_index,
                provenance_json
            )
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to create the streamed reconcile desired-edge temp table")?;

    // Admitted-edge identities for the summary's exact admitted count
    // without holding the observation-scale admitted set in memory.
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.reconcile_admitted_edges (
            source_manifest_id BIGINT NOT NULL,
            chain_id TEXT NOT NULL,
            from_contract_instance_id UUID NOT NULL,
            to_contract_instance_id UUID NOT NULL,
            from_address TEXT NOT NULL,
            to_address TEXT NOT NULL,
            edge_kind TEXT NOT NULL,
            discovery_source TEXT NOT NULL,
            admission TEXT NOT NULL,
            from_role TEXT NOT NULL,
            UNIQUE (
                source_manifest_id,
                chain_id,
                from_contract_instance_id,
                to_contract_instance_id,
                from_address,
                to_address,
                edge_kind,
                discovery_source,
                admission,
                from_role
            )
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to create the streamed reconcile admitted-edge temp table")?;

    Ok(())
}

struct StagedStreamedObservations {
    staged_observation_count: usize,
    observation_chains: BTreeSet<String>,
}

async fn stage_streamed_observations(
    executor: &mut PgConnection,
    source: &impl DiscoveryObservationPageSource,
    options: &StreamedDiscoveryReconciliationOptions,
) -> Result<StagedStreamedObservations> {
    let mut staged_observation_count = 0usize;
    let mut observation_chains = BTreeSet::new();
    let mut after_key = None::<String>;
    loop {
        let page = source
            .load_page(after_key.as_deref(), options.observation_page_limit)
            .await?;
        let Some((last_key, _)) = page.last() else {
            break;
        };
        after_key = Some(last_key.clone());
        staged_observation_count += page.len();

        let mut rows = Vec::with_capacity(page.len());
        for (_, observation) in &page {
            observation_chains.insert(observation.chain.clone());
            rows.push((
                super::super::provenance::observation_key(observation)?,
                normalize_address(&observation.from_address),
                observation,
            ));
        }
        // Chunk below the bind-parameter protocol limit regardless of the
        // source's page size.
        for chunk in rows.chunks(options.mutation_batch_size.max(1)) {
            let mut builder = QueryBuilder::<Postgres>::new(
                r#"
                INSERT INTO pg_temp.reconcile_observations (
                    observation_key,
                    chain_id,
                    from_address,
                    normalized_from_address,
                    to_address,
                    edge_kind,
                    discovery_source,
                    active_from_block_number,
                    active_from_block_hash,
                    provenance
                )
                "#,
            );
            builder.push_values(
                chunk.iter(),
                |mut row, (observation_key, normalized_from_address, observation)| {
                    row.push_bind(observation_key)
                        .push_bind(&observation.chain)
                        .push_bind(&observation.from_address)
                        .push_bind(normalized_from_address)
                        .push_bind(&observation.to_address)
                        .push_bind(&observation.edge_kind)
                        .push_bind(&observation.discovery_source)
                        .push_bind(observation.active_from_block_number)
                        .push_bind(observation.active_from_block_hash.as_deref())
                        .push_bind(&observation.provenance);
                },
            );
            builder.build().execute(&mut *executor).await.context(
                "failed to stage streamed discovery observations (the page source must yield \
                 latest-per-key observations with unique observation keys)",
            )?;
        }
    }

    sqlx::query(
        r#"
        CREATE INDEX reconcile_observations_from_address_idx
        ON pg_temp.reconcile_observations (chain_id, normalized_from_address)
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to index the streamed reconcile observation temp table")?;

    Ok(StagedStreamedObservations {
        staged_observation_count,
        observation_chains,
    })
}

struct StreamedObservationRow {
    observation_key: String,
    normalized_from_address: String,
    observation: DiscoveryObservation,
}

fn streamed_observation_from_row(row: sqlx::postgres::PgRow) -> Result<StreamedObservationRow> {
    Ok(StreamedObservationRow {
        observation_key: row
            .try_get("observation_key")
            .context("failed to read staged observation_key")?,
        normalized_from_address: row
            .try_get("normalized_from_address")
            .context("failed to read staged normalized_from_address")?,
        observation: DiscoveryObservation {
            chain: row
                .try_get("chain_id")
                .context("failed to read staged observation chain_id")?,
            from_address: row
                .try_get("from_address")
                .context("failed to read staged observation from_address")?,
            to_address: row
                .try_get("to_address")
                .context("failed to read staged observation to_address")?,
            edge_kind: row
                .try_get("edge_kind")
                .context("failed to read staged observation edge_kind")?,
            discovery_source: row
                .try_get("discovery_source")
                .context("failed to read staged observation discovery_source")?,
            active_from_block_number: row
                .try_get("active_from_block_number")
                .context("failed to read staged observation active_from_block_number")?,
            active_from_block_hash: row
                .try_get("active_from_block_hash")
                .context("failed to read staged observation active_from_block_hash")?,
            // Not staged: no full-reconciliation consumer reads the
            // active_to window of an observation.
            active_to_block_number: None,
            active_to_block_hash: None,
            provenance: row
                .try_get("provenance")
                .context("failed to read staged observation provenance")?,
        },
    })
}

const STREAMED_OBSERVATION_COLUMNS: &str = r#"
    observation_key,
    chain_id,
    from_address,
    normalized_from_address,
    to_address,
    edge_kind,
    discovery_source,
    active_from_block_number,
    active_from_block_hash,
    provenance
"#;

const STREAMED_OBSERVATION_COLUMNS_QUALIFIED: &str = r#"
    obs.observation_key,
    obs.chain_id,
    obs.from_address,
    obs.normalized_from_address,
    obs.to_address,
    obs.edge_kind,
    obs.discovery_source,
    obs.active_from_block_number,
    obs.active_from_block_hash,
    obs.provenance
"#;

/// Fixed-point admission walk over the staged observations. Pass 1 pages the
/// complete staged set; passes >= 2 revisit only observations emitted from
/// addresses whose active-contract set grew (matching the in-memory walk's
/// requeue of a derived contract's address key). Memory stays bounded by the
/// derived-contract closure plus pending seeds for genuinely new addresses.
async fn run_streamed_admission_walk(
    executor: &mut PgConnection,
    admission_state: &DiscoveryAdmissionState,
    options: &StreamedDiscoveryReconciliationOptions,
) -> Result<()> {
    let mut walk = DiscoveryAdmissionWalk::new(admission_state);
    let mut desired_buffer = Vec::<ReconciledDiscoveryEdgeSpec>::new();
    let mut admitted_buffer = Vec::<AdmittedDiscoveryEdge>::new();
    let mut pending_derived_keys = BTreeSet::<(String, String)>::new();

    let mut after_key = None::<String>;
    loop {
        let rows = sqlx::query(&format!(
            r#"
            SELECT {STREAMED_OBSERVATION_COLUMNS}
            FROM pg_temp.reconcile_observations
            WHERE ($1::TEXT IS NULL OR observation_key > $1)
            ORDER BY observation_key
            LIMIT $2
            "#
        ))
        .bind(after_key.as_deref())
        .bind(options.observation_page_limit)
        .fetch_all(&mut *executor)
        .await
        .context("failed to page staged streamed discovery observations")?;
        if rows.is_empty() {
            break;
        }
        let rows = rows
            .into_iter()
            .map(streamed_observation_from_row)
            .collect::<Result<Vec<_>>>()?;
        after_key = rows.last().map(|row| row.observation_key.clone());
        admit_streamed_observation_page(
            &mut *executor,
            admission_state,
            &mut walk,
            &rows,
            &mut desired_buffer,
            &mut admitted_buffer,
            &mut pending_derived_keys,
            options,
        )
        .await?;
    }

    while !pending_derived_keys.is_empty() {
        let round_keys = std::mem::take(&mut pending_derived_keys);
        let (round_chains, round_addresses): (Vec<_>, Vec<_>) = round_keys.into_iter().unzip();
        let mut after_key = None::<String>;
        loop {
            let rows = sqlx::query(&format!(
                r#"
                SELECT {STREAMED_OBSERVATION_COLUMNS_QUALIFIED}
                FROM pg_temp.reconcile_observations obs
                JOIN UNNEST($1::TEXT[], $2::TEXT[]) AS derived(chain_id, address)
                  ON derived.chain_id = obs.chain_id
                 AND derived.address = obs.normalized_from_address
                WHERE ($3::TEXT IS NULL OR obs.observation_key > $3)
                ORDER BY obs.observation_key
                LIMIT $4
                "#
            ))
            .bind(&round_chains)
            .bind(&round_addresses)
            .bind(after_key.as_deref())
            .bind(options.observation_page_limit)
            .fetch_all(&mut *executor)
            .await
            .context("failed to page staged observations for derived discovery contracts")?;
            if rows.is_empty() {
                break;
            }
            let rows = rows
                .into_iter()
                .map(streamed_observation_from_row)
                .collect::<Result<Vec<_>>>()?;
            after_key = rows.last().map(|row| row.observation_key.clone());
            admit_streamed_observation_page(
                &mut *executor,
                admission_state,
                &mut walk,
                &rows,
                &mut desired_buffer,
                &mut admitted_buffer,
                &mut pending_derived_keys,
                options,
            )
            .await?;
        }
    }

    flush_desired_edge_buffer(&mut *executor, &mut desired_buffer).await?;
    flush_admitted_edge_buffer(&mut *executor, &mut admitted_buffer).await?;
    insert_pending_contract_instance_seeds(
        executor,
        &walk.into_sorted_pending_contract_instance_seeds(),
    )
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn admit_streamed_observation_page(
    executor: &mut PgConnection,
    admission_state: &DiscoveryAdmissionState,
    walk: &mut DiscoveryAdmissionWalk,
    rows: &[StreamedObservationRow],
    desired_buffer: &mut Vec<ReconciledDiscoveryEdgeSpec>,
    admitted_buffer: &mut Vec<AdmittedDiscoveryEdge>,
    pending_derived_keys: &mut BTreeSet<(String, String)>,
    options: &StreamedDiscoveryReconciliationOptions,
) -> Result<()> {
    // Resolve the page's target addresses through the same query and
    // first-row-wins fold the full known-address load uses, scoped to one
    // page instead of the whole `contract_instance_addresses` table.
    let (page_chains, page_addresses) = scoped_address_key_vectors(rows.iter().filter_map(|row| {
        let address = normalize_address(&row.observation.to_address);
        if is_zero_address(&address) {
            None
        } else {
            Some((row.observation.chain.clone(), address))
        }
    }));
    let known_contract_instances_by_address =
        load_known_contract_instance_addresses(&mut *executor, &page_chains, &page_addresses)
            .await?;

    for row in rows {
        if is_zero_address(&row.observation.to_address) {
            continue;
        }
        let contract_key = (
            row.observation.chain.clone(),
            row.normalized_from_address.clone(),
        );
        if !walk.has_contract_address(&contract_key) {
            continue;
        }
        for admitted in walk.admit_observation(
            admission_state,
            &known_contract_instances_by_address,
            &row.observation,
        )? {
            desired_buffer.push(admitted.desired_edge);
            admitted_buffer.push(admitted.admitted_edge);
            if let Some(derived_contract_key) = admitted.derived_contract_key {
                pending_derived_keys.insert(derived_contract_key);
            }
        }
        if desired_buffer.len() >= options.mutation_batch_size {
            flush_desired_edge_buffer(&mut *executor, desired_buffer).await?;
        }
        if admitted_buffer.len() >= options.mutation_batch_size {
            flush_admitted_edge_buffer(&mut *executor, admitted_buffer).await?;
        }
    }
    Ok(())
}

async fn flush_desired_edge_buffer(
    executor: &mut PgConnection,
    buffer: &mut Vec<ReconciledDiscoveryEdgeSpec>,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO pg_temp.reconcile_desired_edges (
            observation_key,
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            active_from_block_number,
            active_from_block_hash,
            active_from_transaction_index,
            active_from_log_index,
            provenance_json
        )
        "#,
    );
    builder.push_values(buffer.iter(), |mut row, edge| {
        row.push_bind(&edge.observation_key)
            .push_bind(&edge.chain)
            .push_bind(&edge.edge_kind)
            .push_bind(edge.from_contract_instance_id)
            .push_bind(edge.to_contract_instance_id)
            .push_bind(&edge.discovery_source)
            .push_bind(edge.source_manifest_id)
            .push_bind(&edge.admission)
            .push_bind(edge.active_from_block_number)
            .push_bind(edge.active_from_block_hash.as_deref())
            .push_bind(
                edge.active_from_event_position
                    .map(|position| position.transaction_index),
            )
            .push_bind(
                edge.active_from_event_position
                    .map(|position| position.log_index),
            )
            .push_bind(&edge.provenance_json);
    });
    builder.push(
        r#"
        ON CONFLICT (
            observation_key,
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            active_from_block_number,
            active_from_block_hash,
            active_from_transaction_index,
            active_from_log_index,
            provenance_json
        ) DO NOTHING
        "#,
    );
    builder
        .build()
        .execute(&mut *executor)
        .await
        .context("failed to stage streamed desired discovery edges")?;
    buffer.clear();
    Ok(())
}

async fn flush_admitted_edge_buffer(
    executor: &mut PgConnection,
    buffer: &mut Vec<AdmittedDiscoveryEdge>,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO pg_temp.reconcile_admitted_edges (
            source_manifest_id,
            chain_id,
            from_contract_instance_id,
            to_contract_instance_id,
            from_address,
            to_address,
            edge_kind,
            discovery_source,
            admission,
            from_role
        )
        "#,
    );
    builder.push_values(buffer.iter(), |mut row, edge| {
        row.push_bind(edge.source_manifest_id)
            .push_bind(&edge.chain)
            .push_bind(edge.from_contract_instance_id)
            .push_bind(
                edge.to_contract_instance_id
                    .expect("admitted discovery edges are resolved before buffering"),
            )
            .push_bind(&edge.from_address)
            .push_bind(&edge.to_address)
            .push_bind(&edge.edge_kind)
            .push_bind(&edge.discovery_source)
            .push_bind(&edge.admission)
            .push_bind(&edge.from_role);
    });
    builder.push(" ON CONFLICT DO NOTHING ");
    builder
        .build()
        .execute(&mut *executor)
        .await
        .context("failed to stage streamed admitted discovery edges")?;
    buffer.clear();
    Ok(())
}

/// Exact stored-spec equality between an active `discovery_edges` row (`de`,
/// with `cia` as its active to-address join) and a staged desired row
/// (`desired`). This mirrors `ReconciledDiscoveryEdgeSpec` equality against
/// the spec `load_active_reconciled_discovery_edges` reconstructs:
/// `source_manifest_id` NULL loads as -1, `observation_key` and the event
/// position come from provenance, and provenance compares as jsonb minus the
/// `active_to_*` position keys — the loader round-trips stored provenance
/// through jsonb, so jsonb equality is the loader's text equality.
const STREAMED_EXACT_SPEC_MATCH_SQL: &str = r#"
    desired.discovery_source = de.discovery_source
    AND desired.observation_key = de.provenance ->> 'observation_key'
    AND desired.chain_id = de.chain_id
    AND desired.edge_kind = de.edge_kind
    AND desired.from_contract_instance_id = de.from_contract_instance_id
    AND desired.to_contract_instance_id = de.to_contract_instance_id
    AND desired.source_manifest_id = COALESCE(de.source_manifest_id, -1)
    AND desired.admission = de.admission
    AND desired.active_from_block_number IS NOT DISTINCT FROM de.active_from_block_number
    AND desired.active_from_block_hash IS NOT DISTINCT FROM de.active_from_block_hash
    AND desired.provenance_json::JSONB = (
        de.provenance - 'active_to_transaction_index' - 'active_to_log_index'
    )
"#;

const STREAMED_ACTIVE_EDGE_FROM_SQL: &str = r#"
    FROM discovery_edges de
    JOIN contract_instance_addresses cia
      ON cia.contract_instance_id = de.to_contract_instance_id
     AND cia.deactivated_at IS NULL
    WHERE de.discovery_source = $1
      AND de.deactivated_at IS NULL
"#;

const STREAMED_EXISTING_EDGE_SELECT_SQL: &str = r#"
    SELECT
        de.discovery_edge_id,
        de.provenance ->> 'observation_key' AS observation_key,
        de.chain_id,
        de.edge_kind,
        de.from_contract_instance_id,
        de.to_contract_instance_id,
        de.discovery_source,
        de.source_manifest_id,
        de.admission,
        de.active_from_block_number,
        de.active_from_block_hash,
        de.provenance,
        cia.address AS to_address,
        EXISTS (
            SELECT 1
            FROM chain_lineage rb
            WHERE rb.chain_id = de.chain_id
              AND rb.block_hash = de.active_from_block_hash
              AND rb.canonicality_state = 'orphaned'::canonicality_state
        ) AS active_from_block_is_orphaned
"#;

const STREAMED_EDGE_IS_ORPHANED_SQL: &str = r#"
    EXISTS (
        SELECT 1
        FROM chain_lineage start_block
        WHERE start_block.chain_id = de.chain_id
          AND start_block.block_hash = de.active_from_block_hash
          AND start_block.canonicality_state = 'orphaned'::canonicality_state
    )
"#;

/// `assignment_starts_no_later(existing = de, desired)` in SQL: a missing
/// existing start is "no later"; an existing start needs a desired start to
/// compare; equal blocks fall back to the block-inclusive comparison unless
/// both sides carry a full event position.
const STREAMED_STARTS_NO_LATER_SQL: &str = r#"
    (
        de.active_from_block_number IS NULL
        OR (
            desired.active_from_block_number IS NOT NULL
            AND (
                de.active_from_block_number < desired.active_from_block_number
                OR (
                    de.active_from_block_number = desired.active_from_block_number
                    AND (
                        (de.provenance ->> 'transaction_index') IS NULL
                        OR (de.provenance ->> 'log_index') IS NULL
                        OR desired.active_from_transaction_index IS NULL
                        OR desired.active_from_log_index IS NULL
                        OR (
                            (de.provenance ->> 'transaction_index')::BIGINT,
                            (de.provenance ->> 'log_index')::BIGINT
                        ) <= (
                            desired.active_from_transaction_index,
                            desired.active_from_log_index
                        )
                    )
                )
            )
        )
    )
"#;

/// `starts_after(existing = de, desired)` in SQL: both block numbers must be
/// present; equal blocks only compare when both sides carry a full event
/// position.
const STREAMED_STARTS_AFTER_SQL: &str = r#"
    (
        de.active_from_block_number IS NOT NULL
        AND desired.active_from_block_number IS NOT NULL
        AND (
            de.active_from_block_number > desired.active_from_block_number
            OR (
                de.active_from_block_number = desired.active_from_block_number
                AND (de.provenance ->> 'transaction_index') IS NOT NULL
                AND (de.provenance ->> 'log_index') IS NOT NULL
                AND desired.active_from_transaction_index IS NOT NULL
                AND desired.active_from_log_index IS NOT NULL
                AND (
                    (de.provenance ->> 'transaction_index')::BIGINT,
                    (de.provenance ->> 'log_index')::BIGINT
                ) > (
                    desired.active_from_transaction_index,
                    desired.active_from_log_index
                )
            )
        )
    )
"#;

async fn count_streamed_deactivation_candidates(
    executor: &mut PgConnection,
    discovery_source: &str,
) -> Result<usize> {
    let count = sqlx::query_scalar::<_, i64>(&format!(
        r#"
        SELECT COUNT(*)::BIGINT
        {STREAMED_ACTIVE_EDGE_FROM_SQL}
          AND NOT EXISTS (
              SELECT 1
              FROM pg_temp.reconcile_desired_edges desired
              WHERE {STREAMED_EXACT_SPEC_MATCH_SQL}
          )
        "#
    ))
    .bind(discovery_source)
    .fetch_one(executor)
    .await
    .context("failed to count streamed discovery-edge deactivation candidates")?;
    usize::try_from(count).context("streamed deactivation candidate count overflowed usize")
}

async fn load_streamed_deactivation_candidates(
    executor: &mut PgConnection,
    discovery_source: &str,
) -> Result<Vec<ExistingReconciledDiscoveryEdge>> {
    let rows = sqlx::query(&format!(
        r#"
        {STREAMED_EXISTING_EDGE_SELECT_SQL}
        {STREAMED_ACTIVE_EDGE_FROM_SQL}
          AND NOT EXISTS (
              SELECT 1
              FROM pg_temp.reconcile_desired_edges desired
              WHERE {STREAMED_EXACT_SPEC_MATCH_SQL}
          )
        ORDER BY de.discovery_edge_id
        "#
    ))
    .bind(discovery_source)
    .fetch_all(executor)
    .await
    .context("failed to load streamed discovery-edge deactivation candidates")?;

    rows.into_iter().map(edge_from_row).collect()
}

async fn load_streamed_observations_for_keys(
    executor: &mut PgConnection,
    candidates: &[ExistingReconciledDiscoveryEdge],
) -> Result<Vec<DiscoveryObservation>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let observation_keys = candidates
        .iter()
        .map(|candidate| candidate.spec.observation_key.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let rows = sqlx::query(&format!(
        r#"
        SELECT {STREAMED_OBSERVATION_COLUMNS}
        FROM pg_temp.reconcile_observations
        WHERE observation_key = ANY($1::TEXT[])
        ORDER BY observation_key
        "#
    ))
    .bind(&observation_keys)
    .fetch_all(executor)
    .await
    .context("failed to load staged observations for deactivation candidates")?;

    rows.into_iter()
        .map(|row| Ok(streamed_observation_from_row(row)?.observation))
        .collect()
}

fn desired_edge_spec_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<(i64, ReconciledDiscoveryEdgeSpec)> {
    let desired_row_id = row
        .try_get("desired_row_id")
        .context("failed to read desired_row_id")?;
    let transaction_index: Option<i64> = row
        .try_get("active_from_transaction_index")
        .context("failed to read desired active_from_transaction_index")?;
    let log_index: Option<i64> = row
        .try_get("active_from_log_index")
        .context("failed to read desired active_from_log_index")?;
    let active_from_event_position = match (transaction_index, log_index) {
        (Some(transaction_index), Some(log_index)) => Some(EvmEventPosition {
            transaction_index,
            log_index,
        }),
        (None, None) => None,
        _ => bail!("staged desired discovery edge carries a partial event position"),
    };
    Ok((
        desired_row_id,
        ReconciledDiscoveryEdgeSpec {
            observation_key: row
                .try_get("observation_key")
                .context("failed to read desired observation_key")?,
            chain: row
                .try_get("chain_id")
                .context("failed to read desired chain_id")?,
            edge_kind: row
                .try_get("edge_kind")
                .context("failed to read desired edge_kind")?,
            from_contract_instance_id: row
                .try_get("from_contract_instance_id")
                .context("failed to read desired from_contract_instance_id")?,
            to_contract_instance_id: row
                .try_get("to_contract_instance_id")
                .context("failed to read desired to_contract_instance_id")?,
            discovery_source: row
                .try_get("discovery_source")
                .context("failed to read desired discovery_source")?,
            source_manifest_id: row
                .try_get("source_manifest_id")
                .context("failed to read desired source_manifest_id")?,
            admission: row
                .try_get("admission")
                .context("failed to read desired admission")?,
            active_from_block_number: row
                .try_get("active_from_block_number")
                .context("failed to read desired active_from_block_number")?,
            active_from_block_hash: row
                .try_get("active_from_block_hash")
                .context("failed to read desired active_from_block_hash")?,
            active_from_event_position,
            provenance_json: row
                .try_get("provenance_json")
                .context("failed to read desired provenance_json")?,
        },
    ))
}

const STREAMED_DESIRED_EDGE_COLUMNS: &str = r#"
    desired.desired_row_id,
    desired.observation_key,
    desired.chain_id,
    desired.edge_kind,
    desired.from_contract_instance_id,
    desired.to_contract_instance_id,
    desired.discovery_source,
    desired.source_manifest_id,
    desired.admission,
    desired.active_from_block_number,
    desired.active_from_block_hash,
    desired.active_from_transaction_index,
    desired.active_from_log_index,
    desired.provenance_json
"#;

/// Materialize insert candidates against the pre-mutation edge snapshot:
/// desired specs with no exact active match and no non-orphaned active edge
/// materializing the same assignment at a no-later start (`current_new_
/// edges` in the in-memory chronology, before its historical exclusion,
/// which the caller applies from `collect_streamed_historical_edges`).
async fn materialize_streamed_insert_candidates(
    executor: &mut PgConnection,
    discovery_source: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE pg_temp.reconcile_insert_candidates (
            desired_row_id BIGINT PRIMARY KEY
        ) ON COMMIT DROP
        "#,
    )
    .execute(&mut *executor)
    .await
    .context("failed to create the streamed reconcile insert-candidate temp table")?;

    sqlx::query(&format!(
        r#"
        INSERT INTO pg_temp.reconcile_insert_candidates (desired_row_id)
        SELECT desired.desired_row_id
        FROM pg_temp.reconcile_desired_edges desired
        WHERE NOT EXISTS (
            SELECT 1
            FROM discovery_edges de
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE de.discovery_source = $1
              AND de.deactivated_at IS NULL
              AND {STREAMED_EXACT_SPEC_MATCH_SQL}
        )
        AND NOT EXISTS (
            SELECT 1
            FROM discovery_edges de
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE de.discovery_source = $1
              AND de.deactivated_at IS NULL
              AND de.discovery_source = desired.discovery_source
              AND de.provenance ->> 'observation_key' = desired.observation_key
              AND de.chain_id = desired.chain_id
              AND de.edge_kind = desired.edge_kind
              AND de.from_contract_instance_id = desired.from_contract_instance_id
              AND de.to_contract_instance_id = desired.to_contract_instance_id
              AND COALESCE(de.source_manifest_id, -1) = desired.source_manifest_id
              AND de.admission = desired.admission
              AND NOT {STREAMED_EDGE_IS_ORPHANED_SQL}
              AND {STREAMED_STARTS_NO_LATER_SQL}
        )
        "#
    ))
    .bind(discovery_source)
    .execute(&mut *executor)
    .await
    .context("failed to materialize streamed discovery-edge insert candidates")?;

    Ok(())
}

async fn load_streamed_insert_candidate_page(
    executor: &mut PgConnection,
    after_row_id: i64,
    limit: i64,
) -> Result<Vec<(i64, ReconciledDiscoveryEdgeSpec)>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT {STREAMED_DESIRED_EDGE_COLUMNS}
        FROM pg_temp.reconcile_insert_candidates candidate
        JOIN pg_temp.reconcile_desired_edges desired
          ON desired.desired_row_id = candidate.desired_row_id
        WHERE candidate.desired_row_id > $1
        ORDER BY candidate.desired_row_id
        LIMIT $2
        "#
    ))
    .bind(after_row_id)
    .bind(limit)
    .fetch_all(executor)
    .await
    .context("failed to page streamed discovery-edge insert candidates")?;

    rows.iter().map(desired_edge_spec_from_row).collect()
}

/// Chronology rule 3 for the deactivation candidates: for every desired edge
/// sharing an assignment identity with a candidate at a no-later start,
/// resolve the earliest-starting active edge materializing that assignment
/// (over ALL active edges, not just candidates) and retain it.
async fn collect_same_assignment_retained_edges(
    executor: &mut PgConnection,
    discovery_source: &str,
    candidates: &[ExistingReconciledDiscoveryEdge],
    retained_newer_edge_ids: &mut HashSet<i64>,
) -> Result<()> {
    let mut matched_desired = HashSet::<ReconciledDiscoveryEdgeSpec>::new();
    for candidate in candidates {
        let rows = sqlx::query(&format!(
            r#"
            SELECT {STREAMED_DESIRED_EDGE_COLUMNS}
            FROM pg_temp.reconcile_desired_edges desired
            WHERE desired.observation_key = $1
              AND desired.chain_id = $2
              AND desired.edge_kind = $3
              AND desired.from_contract_instance_id = $4
              AND desired.to_contract_instance_id = $5
              AND desired.discovery_source = $6
              AND desired.source_manifest_id = $7
              AND desired.admission = $8
            "#
        ))
        .bind(&candidate.spec.observation_key)
        .bind(&candidate.spec.chain)
        .bind(&candidate.spec.edge_kind)
        .bind(candidate.spec.from_contract_instance_id)
        .bind(candidate.spec.to_contract_instance_id)
        .bind(&candidate.spec.discovery_source)
        .bind(candidate.spec.source_manifest_id)
        .bind(&candidate.spec.admission)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load same-assignment desired edges for a deactivation candidate")?;
        for row in &rows {
            let (_, desired) = desired_edge_spec_from_row(row)?;
            if !candidate.active_from_block_is_orphaned
                && assignment_starts_no_later(candidate, &desired)
            {
                matched_desired.insert(desired);
            }
        }
    }

    for desired in matched_desired {
        let rows = sqlx::query(&format!(
            r#"
            {STREAMED_EXISTING_EDGE_SELECT_SQL}
            {STREAMED_ACTIVE_EDGE_FROM_SQL}
              AND de.provenance ->> 'observation_key' = $2
              AND de.chain_id = $3
              AND de.edge_kind = $4
              AND de.from_contract_instance_id = $5
              AND de.to_contract_instance_id = $6
              AND COALESCE(de.source_manifest_id, -1) = $7
              AND de.admission = $8
            "#
        ))
        .bind(discovery_source)
        .bind(&desired.observation_key)
        .bind(&desired.chain)
        .bind(&desired.edge_kind)
        .bind(desired.from_contract_instance_id)
        .bind(desired.to_contract_instance_id)
        .bind(desired.source_manifest_id)
        .bind(&desired.admission)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load same-assignment active edges for a desired edge")?;
        let matching_edges = rows
            .into_iter()
            .map(edge_from_row)
            .collect::<Result<Vec<_>>>()?;
        if let Some(retained) = matching_edges
            .iter()
            .filter(|edge| {
                !edge.active_from_block_is_orphaned && assignment_starts_no_later(edge, &desired)
            })
            .min_by(compare_edge_starts)
        {
            retained_newer_edge_ids.insert(retained.discovery_edge_id);
        }
    }
    Ok(())
}

/// Chronology rule 2: desired edges with a non-orphaned active successor
/// (same observation key, chain, edge kind, and from-instance, starting
/// strictly after the desired start) become closed historical epochs with
/// the successor's start as their terminal; the successor is retained.
async fn collect_streamed_historical_edges(
    executor: &mut PgConnection,
    discovery_source: &str,
    retained_newer_edge_ids: &mut HashSet<i64>,
) -> Result<Vec<(i64, ReconciledDiscoveryEdgeSpec, ObservationTerminalState)>> {
    let rows = sqlx::query(&format!(
        r#"
        SELECT {STREAMED_DESIRED_EDGE_COLUMNS}
        FROM pg_temp.reconcile_desired_edges desired
        WHERE desired.active_from_block_number IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM discovery_edges de
              JOIN contract_instance_addresses cia
                ON cia.contract_instance_id = de.to_contract_instance_id
               AND cia.deactivated_at IS NULL
              WHERE de.discovery_source = $1
                AND de.deactivated_at IS NULL
                AND de.provenance ->> 'observation_key' = desired.observation_key
                AND de.chain_id = desired.chain_id
                AND de.edge_kind = desired.edge_kind
                AND de.from_contract_instance_id = desired.from_contract_instance_id
                AND NOT {STREAMED_EDGE_IS_ORPHANED_SQL}
                AND {STREAMED_STARTS_AFTER_SQL}
          )
        ORDER BY desired.desired_row_id
        "#
    ))
    .bind(discovery_source)
    .fetch_all(&mut *executor)
    .await
    .context("failed to load streamed historical desired discovery edges")?;
    let historical_desired = rows
        .iter()
        .map(desired_edge_spec_from_row)
        .collect::<Result<Vec<_>>>()?;

    let mut historical_edges = Vec::new();
    for (desired_row_id, desired) in historical_desired {
        let rows = sqlx::query(&format!(
            r#"
            {STREAMED_EXISTING_EDGE_SELECT_SQL}
            {STREAMED_ACTIVE_EDGE_FROM_SQL}
              AND de.provenance ->> 'observation_key' = $2
              AND de.chain_id = $3
              AND de.edge_kind = $4
              AND de.from_contract_instance_id = $5
            "#
        ))
        .bind(discovery_source)
        .bind(&desired.observation_key)
        .bind(&desired.chain)
        .bind(&desired.edge_kind)
        .bind(desired.from_contract_instance_id)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load successor candidates for a historical desired edge")?;
        let successor_candidates = rows
            .into_iter()
            .map(edge_from_row)
            .collect::<Result<Vec<_>>>()?;
        let Some(successor) = successor_candidates
            .iter()
            .filter(|edge| {
                !edge.active_from_block_is_orphaned && edge_starts_after_spec(edge, &desired)
            })
            .min_by(compare_edge_starts)
        else {
            continue;
        };
        retained_newer_edge_ids.insert(successor.discovery_edge_id);
        let terminal_state = ObservationTerminalState {
            chain: successor.spec.chain.clone(),
            block_number: successor.spec.active_from_block_number,
            block_hash: successor.spec.active_from_block_hash.clone(),
            event_position: successor.spec.active_from_event_position,
        };
        historical_edges.push((desired_row_id, desired, terminal_state));
    }
    Ok(historical_edges)
}

async fn count_temp_rows(executor: &mut PgConnection, table: &str) -> Result<usize> {
    let count =
        sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*)::BIGINT FROM pg_temp.{table}"))
            .fetch_one(executor)
            .await
            .with_context(|| format!("failed to count streamed reconcile rows in {table}"))?;
    usize::try_from(count).with_context(|| format!("streamed {table} count overflowed usize"))
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
