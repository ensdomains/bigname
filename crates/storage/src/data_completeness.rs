use anyhow::{Context, Result};

mod content;
mod manifest_targets;

use content::*;
pub use manifest_targets::load_active_manifest_deployment_profile;
use manifest_targets::*;

#[cfg(test)]
mod tests;

/// Per-chain intake frontiers.
///
/// `lineage_canonical_block_count` counts distinct non-orphaned block numbers, so a
/// contiguous lineage satisfies `count == head - floor + 1`.
///
/// `canonical_raw_log_head_block_number` is the head of the raw logs that normalized
/// replay is eligible to consume: it mirrors the replay bounds, which require both the
/// raw log and its lineage block to be canonical, safe, or finalized. The gate compares
/// replay progress against this head. `raw_log_head_block_number` is the non-orphaned
/// head including `observed` logs and is reported only, so a candidate whose newest logs
/// have not yet been promoted to canonical is not measured as lagging against them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainCompletenessRow {
    pub chain_id: String,
    pub canonical_block_number: Option<i64>,
    /// Whether the checkpoint's exact `(chain, block_hash, block_number)` resolves to a
    /// canonical/safe/finalized lineage row. An empty checkpoint has no anchor to validate.
    pub checkpoint_canonical_lineage_match: bool,
    pub lineage_head_block_number: Option<i64>,
    pub lineage_floor_block_number: Option<i64>,
    pub lineage_canonical_block_count: i64,
    /// Canonical/safe/finalized block heights carrying an additional non-orphaned hash. The
    /// distinct-block-number contiguity count cannot see these competing forks, so they are
    /// counted separately; a non-zero value is a canonicality violation, not a gap.
    pub duplicate_canonical_height_count: i64,
    /// Canonical/safe/finalized rows above the retained floor whose `parent_hash` does not
    /// resolve to the canonical/safe/finalized row at the preceding height.
    pub disconnected_canonical_parent_count: i64,
    pub canonical_raw_log_head_block_number: Option<i64>,
    pub raw_log_head_block_number: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayCursorRow {
    pub deployment_profile: String,
    pub chain_id: String,
    pub cursor_kind: String,
    /// Inclusive start of the cursor's admitted range. The post-replay backlog writer seeds
    /// this to the raw-fact replay target plus one so the two cursor ranges remain contiguous.
    pub range_start_block_number: i64,
    /// The completion authority. Replay is complete for a cursor's target when
    /// `next_block_number > target_block_number`; a reorg rewind lowers `next_block_number`
    /// but leaves `last_completed_block_number` at its high-water mark, so the gate reads the
    /// `next`/`target` pair and treats `last_completed_block_number` as reporting detail only.
    pub next_block_number: Option<i64>,
    pub target_block_number: Option<i64>,
    pub last_completed_block_number: Option<i64>,
    pub last_failure_reason: Option<String>,
}

/// A completed `current_projection_replay_status` marker for one `(replay_version, projection)`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionReplayMarker {
    pub replay_version: i32,
    pub projection: String,
    pub completed_normalized_target_block: Option<i64>,
}

/// Backfill lifecycle counts, scoped by deployment profile. Reported as an advisory rather
/// than gated: without coverage-fact reconciliation a `failed` range cannot be distinguished
/// from one superseded by a later successful retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackfillLifecycleRow {
    pub deployment_profile: String,
    pub failed_job_count: i64,
    pub failed_range_count: i64,
    pub incomplete_range_count: i64,
    pub expired_lease_range_count: i64,
}

/// A `(chain, namespace)` an active manifest version declares. These declarations ensure
/// manifest-only chains remain in the gate's active-chain set even if a partial restore lost
/// their materialized watch rows.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ManifestChainNamespace {
    pub chain: String,
    pub namespace: String,
}

/// A `(chain, source_family)` declared by an active manifest. Replay inspection uses this
/// external manifest authority to apply the same target-refresh policy as the writer.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ManifestChainSourceFamily {
    pub chain: String,
    pub source_family: String,
}

/// A root, contract, or proxy-implementation address declared by an active manifest payload.
/// This remains authoritative even when a partial restore has lost its materialized
/// `manifest_contract_instances` or `contract_instance_addresses` row.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ManifestDeclaredTarget {
    pub chain: String,
    pub source_family: String,
    pub address: String,
    pub active_from_block_number: Option<i64>,
}

/// An open discovery-edge endpoint whose contract instance has no matching open address row that
/// preserves the edge's admitted start. The edge remains authoritative even though the
/// materialized watch view cannot faithfully render it.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DiscoveryTargetMissingAddress {
    pub chain: String,
    pub source_family: String,
    pub contract_instance_id: uuid::Uuid,
}

/// An open resolver discovery edge whose registry source remains active while the resolver
/// target manifest required by the runtime watch view is absent. The edge remains admission
/// authority even though the materialized watch view can no longer assign its resolver family.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DiscoveryTargetMissingManifest {
    pub chain: String,
    pub source_family: String,
    pub contract_instance_id: uuid::Uuid,
}

/// One active manifest source that declares normalized adapter output, together with the
/// count of matching serving-canonical normalized events written under that exact manifest ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveManifestEventSource {
    pub manifest_id: i64,
    pub manifest_version: i64,
    pub chain: String,
    pub namespace: String,
    pub source_family: String,
    /// Distinct normalized event kinds declared by the active manifest ABI. Projection-content
    /// inspection maps these declarations to the current projection writers they can feed.
    pub normalized_event_kinds: Vec<String>,
    pub normalized_event_count: i64,
    /// Matching normalized events whose exact lineage anchor is absent or is no longer
    /// canonical/safe/finalized.
    pub normalized_events_missing_canonical_lineage_count: i64,
    /// Matching serving-canonical events sourced from a raw log whose exact canonical raw-log
    /// row, including its canonical lineage anchor, is absent.
    pub normalized_events_missing_canonical_raw_log_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionApplyCursorRow {
    pub cursor_name: String,
    pub last_change_id: i64,
}

/// A `(chain_id, lowercased address)` pair with at least one non-orphaned code observation
/// anchored to retained non-orphaned lineage.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObservedCodeAddress {
    pub chain_id: String,
    pub address: String,
    pub max_observed_block_number: i64,
}

/// Non-empty `name_current` count for one namespace. `name_current` carries no chain, so a
/// namespace is the finest dimension a name projection can be scoped to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameCurrentCount {
    pub namespace: String,
    pub count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataCompletenessRead {
    pub chains: Vec<ChainCompletenessRow>,
    /// Deployment profile inferred from the active manifest corpus using the same authority as
    /// replay admission (`mainnet` or `sepolia`). `None` is unresolved and fails closed.
    pub active_deployment_profile: Option<String>,
    pub replay_cursors: Vec<ReplayCursorRow>,
    pub projection_apply_cursors: Vec<ProjectionApplyCursorRow>,
    /// `MAX(change_id)` over `projection_normalized_event_changes`, loaded independently of
    /// the apply cursors so an absent cursor with a non-empty change log and a cursor ahead of
    /// retained history are both detectable. An empty log has an effective high-water mark of 0.
    pub max_projection_change_id: Option<i64>,
    /// Rows still queued in `projection_invalidations`. A successful apply deletes the row,
    /// so a fully applied projection queue is empty; a non-zero count is pending work.
    pub pending_projection_invalidation_count: i64,
    /// Rows in `projection_invalidation_dead_letters`: invalidations that exhausted their
    /// retries. A non-zero count is a terminal projection failure.
    pub projection_invalidation_dead_letter_count: i64,
    pub observed_code_addresses: Vec<ObservedCodeAddress>,
    /// Direct active-manifest payload targets, independently of the materialized watch view.
    pub manifest_declared_targets: Vec<ManifestDeclaredTarget>,
    /// Event-producing active manifest sources and exact-identity content counts.
    pub active_manifest_event_sources: Vec<ActiveManifestEventSource>,
    /// `name_current` counts grouped by namespace.
    pub name_current_counts: Vec<NameCurrentCount>,
    /// Non-orphaned `normalized_events` rows with a NULL `chain_id` — a data-integrity fault
    /// that would otherwise abort the per-chain read.
    pub normalized_events_null_chain_id_count: i64,
    /// Completed current-projection replay markers. The gate requires all current projections
    /// present at the worker's current replay version with target coverage matching the worker's
    /// bootstrap handoff.
    pub projection_replay_markers: Vec<ProjectionReplayMarker>,
    /// The target a projection bootstrap would request now: the greater of the normalized
    /// raw-fact replay target and the chain-checkpoint frontier.
    pub projection_replay_required_target_block: Option<i64>,
    /// Per-profile backfill lifecycle counts (advisory).
    pub backfill_lifecycle: Vec<BackfillLifecycleRow>,
    /// Deferred `normalized_events` projection indexes that currently exist. A fresh replay
    /// drops them and a later pass rebuilds them, so an absent index marks a mid-replay
    /// candidate.
    pub present_deferred_projection_indexes: Vec<String>,
    /// `(chain, namespace)` declared by active manifest versions — active-chain authority.
    pub manifest_chain_namespaces: Vec<ManifestChainNamespace>,
    /// `(chain, source_family)` declared by active manifests. This lets the gate interpret
    /// latched replay cursors using the writer's shared adapter replay policy.
    pub manifest_chain_source_families: Vec<ManifestChainSourceFamily>,
    /// Active manifest payload targets whose declaration/implementation instance or live
    /// address row does not match the payload's chain and address. A non-empty list is a
    /// watch-authority gap.
    pub manifest_declared_targets_missing_address: Vec<ManifestDeclaredTarget>,
    /// Materialized manifest proxy/implementation pairs that lack the active managed discovery
    /// edge consumed by the runtime watch view.
    pub manifest_proxy_implementations_missing_edge: Vec<ManifestDeclaredTarget>,
    /// Open discovery-edge endpoints with no range-preserving open address row on the edge's
    /// chain.
    pub discovery_targets_missing_address: Vec<DiscoveryTargetMissingAddress>,
    /// Open resolver edges whose active registry source has lost the active resolver manifest
    /// required by the runtime watch view.
    pub discovery_targets_missing_manifest: Vec<DiscoveryTargetMissingManifest>,
}

/// The deferred `normalized_events` projection indexes, owned by the replay drop/rebuild path
/// in `apps/indexer/src/main/normalized_replay_catchup/indexes.rs`.
pub const DEFERRED_NORMALIZED_EVENT_INDEXES: &[&str] = &[
    "normalized_events_namespace_idx",
    "normalized_events_kind_idx",
    "normalized_events_manifest_idx",
    "normalized_events_chain_position_idx",
    "normalized_events_name_projection_replay_idx",
    "normalized_events_resource_projection_replay_idx",
    "normalized_events_name_relevant_projection_idx",
    "normalized_events_record_inventory_resource_replay_idx",
];

pub async fn load_data_completeness(pool: &sqlx::PgPool) -> Result<DataCompletenessRead> {
    Ok(DataCompletenessRead {
        chains: load_chain_completeness(pool).await?,
        active_deployment_profile: load_active_manifest_deployment_profile(pool).await?,
        replay_cursors: load_replay_cursors(pool).await?,
        projection_apply_cursors: load_projection_apply_cursors(pool).await?,
        max_projection_change_id: load_max_projection_change_id(pool).await?,
        pending_projection_invalidation_count: count_table(pool, "projection_invalidations")
            .await?,
        projection_invalidation_dead_letter_count: count_table(
            pool,
            "projection_invalidation_dead_letters",
        )
        .await?,
        observed_code_addresses: load_observed_code_addresses(pool).await?,
        manifest_declared_targets: load_manifest_declared_targets(pool).await?,
        active_manifest_event_sources: load_active_manifest_event_sources(pool).await?,
        name_current_counts: load_name_current_counts(pool).await?,
        normalized_events_null_chain_id_count: load_normalized_events_null_chain_id_count(pool)
            .await?,
        projection_replay_markers: load_projection_replay_markers(pool).await?,
        projection_replay_required_target_block: load_projection_replay_required_target_block(pool)
            .await?,
        backfill_lifecycle: load_backfill_lifecycle(pool).await?,
        present_deferred_projection_indexes: load_present_deferred_projection_indexes(pool).await?,
        manifest_chain_namespaces: load_manifest_chain_namespaces(pool).await?,
        manifest_chain_source_families: load_manifest_chain_source_families(pool).await?,
        manifest_declared_targets_missing_address: load_manifest_declared_targets_missing_address(
            pool,
        )
        .await?,
        manifest_proxy_implementations_missing_edge:
            load_manifest_proxy_implementations_missing_edge(pool).await?,
        discovery_targets_missing_address: load_discovery_targets_missing_address(pool).await?,
        discovery_targets_missing_manifest: load_discovery_targets_missing_manifest(pool).await?,
    })
}

async fn load_chain_completeness(pool: &sqlx::PgPool) -> Result<Vec<ChainCompletenessRow>> {
    let rows = sqlx::query(
        r#"
        WITH known_chains AS (
            SELECT chain_id FROM chain_checkpoints
            UNION
            SELECT DISTINCT chain_id FROM chain_lineage
        ),
        canonical_lineage AS (
            SELECT chain_id, block_hash, parent_hash, block_number
            FROM chain_lineage
            WHERE canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
        ),
        lineage AS (
            SELECT
                chain_id,
                MAX(block_number) AS lineage_head_block_number,
                MIN(block_number) AS lineage_floor_block_number,
                COUNT(DISTINCT block_number) AS lineage_canonical_block_count
            FROM canonical_lineage
            GROUP BY chain_id
        ),
        canonical_raw_log_head AS (
            SELECT
                raw_logs.chain_id,
                MAX(raw_logs.block_number) AS canonical_raw_log_head_block_number
            FROM raw_logs
            JOIN chain_lineage
              ON chain_lineage.chain_id = raw_logs.chain_id
             AND chain_lineage.block_hash = raw_logs.block_hash
            WHERE raw_logs.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
              AND chain_lineage.canonicality_state IN (
                'canonical'::canonicality_state,
                'safe'::canonicality_state,
                'finalized'::canonicality_state
            )
            GROUP BY raw_logs.chain_id
        ),
        raw_log_head AS (
            SELECT chain_id, MAX(block_number) AS raw_log_head_block_number
            FROM raw_logs
            WHERE canonicality_state <> 'orphaned'::canonicality_state
            GROUP BY chain_id
        ),
        duplicate_canonical_height AS (
            SELECT chain_id, COUNT(*) AS duplicate_canonical_height_count
            FROM (
                SELECT canonical.chain_id, canonical.block_number
                FROM canonical_lineage canonical
                JOIN chain_lineage sibling
                  ON sibling.chain_id = canonical.chain_id
                 AND sibling.block_number = canonical.block_number
                 AND sibling.canonicality_state <> 'orphaned'::canonicality_state
                GROUP BY canonical.chain_id, canonical.block_number
                HAVING COUNT(DISTINCT sibling.block_hash) > 1
            ) duplicated
            GROUP BY chain_id
        ),
        disconnected_canonical_parent AS (
            SELECT child.chain_id, COUNT(*) AS disconnected_canonical_parent_count
            FROM canonical_lineage child
            JOIN lineage span ON span.chain_id = child.chain_id
            LEFT JOIN canonical_lineage parent
              ON parent.chain_id = child.chain_id
             AND parent.block_hash = child.parent_hash
             AND parent.block_number = child.block_number - 1
            WHERE child.block_number > span.lineage_floor_block_number
              AND parent.block_hash IS NULL
            GROUP BY child.chain_id
        )
        SELECT
            known_chains.chain_id,
            chain_checkpoints.canonical_block_number,
            CASE
                WHEN chain_checkpoints.canonical_block_number IS NULL THEN TRUE
                ELSE checkpoint_lineage.block_hash IS NOT NULL
            END AS checkpoint_canonical_lineage_match,
            lineage.lineage_head_block_number,
            lineage.lineage_floor_block_number,
            COALESCE(lineage.lineage_canonical_block_count, 0) AS lineage_canonical_block_count,
            COALESCE(duplicate_canonical_height.duplicate_canonical_height_count, 0)
                AS duplicate_canonical_height_count,
            COALESCE(disconnected_canonical_parent.disconnected_canonical_parent_count, 0)
                AS disconnected_canonical_parent_count,
            canonical_raw_log_head.canonical_raw_log_head_block_number,
            raw_log_head.raw_log_head_block_number
        FROM known_chains
        LEFT JOIN chain_checkpoints ON chain_checkpoints.chain_id = known_chains.chain_id
        LEFT JOIN canonical_lineage checkpoint_lineage
          ON checkpoint_lineage.chain_id = chain_checkpoints.chain_id
         AND checkpoint_lineage.block_hash = chain_checkpoints.canonical_block_hash
         AND checkpoint_lineage.block_number = chain_checkpoints.canonical_block_number
        LEFT JOIN lineage ON lineage.chain_id = known_chains.chain_id
        LEFT JOIN duplicate_canonical_height
          ON duplicate_canonical_height.chain_id = known_chains.chain_id
        LEFT JOIN disconnected_canonical_parent
          ON disconnected_canonical_parent.chain_id = known_chains.chain_id
        LEFT JOIN canonical_raw_log_head
          ON canonical_raw_log_head.chain_id = known_chains.chain_id
        LEFT JOIN raw_log_head ON raw_log_head.chain_id = known_chains.chain_id
        ORDER BY known_chains.chain_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load chain completeness frontiers")?;

    rows.into_iter()
        .map(|row| {
            Ok(ChainCompletenessRow {
                chain_id: crate::sql_row::get(&row, "chain_id")?,
                canonical_block_number: crate::sql_row::get(&row, "canonical_block_number")?,
                checkpoint_canonical_lineage_match: crate::sql_row::get(
                    &row,
                    "checkpoint_canonical_lineage_match",
                )?,
                lineage_head_block_number: crate::sql_row::get(&row, "lineage_head_block_number")?,
                lineage_floor_block_number: crate::sql_row::get(
                    &row,
                    "lineage_floor_block_number",
                )?,
                lineage_canonical_block_count: crate::sql_row::get(
                    &row,
                    "lineage_canonical_block_count",
                )?,
                duplicate_canonical_height_count: crate::sql_row::get(
                    &row,
                    "duplicate_canonical_height_count",
                )?,
                disconnected_canonical_parent_count: crate::sql_row::get(
                    &row,
                    "disconnected_canonical_parent_count",
                )?,
                canonical_raw_log_head_block_number: crate::sql_row::get(
                    &row,
                    "canonical_raw_log_head_block_number",
                )?,
                raw_log_head_block_number: crate::sql_row::get(&row, "raw_log_head_block_number")?,
            })
        })
        .collect()
}

async fn load_replay_cursors(pool: &sqlx::PgPool) -> Result<Vec<ReplayCursorRow>> {
    let rows = sqlx::query(
        r#"
        SELECT
            deployment_profile,
            chain_id,
            cursor_kind,
            range_start_block_number,
            next_block_number,
            target_block_number,
            last_completed_block_number,
            NULLIF(last_failure_reason, '') AS last_failure_reason
        FROM normalized_replay_cursors
        ORDER BY deployment_profile, chain_id, cursor_kind
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load normalized replay cursors")?;

    rows.into_iter()
        .map(|row| {
            Ok(ReplayCursorRow {
                deployment_profile: crate::sql_row::get(&row, "deployment_profile")?,
                chain_id: crate::sql_row::get(&row, "chain_id")?,
                cursor_kind: crate::sql_row::get(&row, "cursor_kind")?,
                range_start_block_number: crate::sql_row::get(&row, "range_start_block_number")?,
                next_block_number: crate::sql_row::get(&row, "next_block_number")?,
                target_block_number: crate::sql_row::get(&row, "target_block_number")?,
                last_completed_block_number: crate::sql_row::get(
                    &row,
                    "last_completed_block_number",
                )?,
                last_failure_reason: crate::sql_row::get(&row, "last_failure_reason")?,
            })
        })
        .collect()
}

async fn load_projection_apply_cursors(
    pool: &sqlx::PgPool,
) -> Result<Vec<ProjectionApplyCursorRow>> {
    let rows = sqlx::query(
        r#"
        SELECT cursor_name, last_change_id
        FROM projection_apply_cursors
        ORDER BY cursor_name
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load projection apply cursors")?;

    rows.into_iter()
        .map(|row| {
            Ok(ProjectionApplyCursorRow {
                cursor_name: crate::sql_row::get(&row, "cursor_name")?,
                last_change_id: crate::sql_row::get(&row, "last_change_id")?,
            })
        })
        .collect()
}

async fn load_max_projection_change_id(pool: &sqlx::PgPool) -> Result<Option<i64>> {
    sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(change_id) FROM projection_normalized_event_changes",
    )
    .fetch_one(pool)
    .await
    .context("failed to load max projection change id")
}
