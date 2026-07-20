//! Batched chronology rule-3 retention lookups for the streamed
//! full-source reconcile.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use sqlx::{Row, postgres::PgConnection};
use uuid::Uuid;

use super::super::super::super::types::{
    ExistingReconciledDiscoveryEdge, ReconciledDiscoveryEdgeSpec,
};
use super::super::super::chronology::{assignment_starts_no_later, compare_edge_starts};
use super::super::super::compare_reconciled_discovery_edge_specs;
use super::super::super::existing::edge_from_row;
use super::{
    STREAMED_DESIRED_EDGE_COLUMNS, STREAMED_EXISTING_EDGE_COLUMNS_QUALIFIED,
    desired_edge_spec_from_row,
};

/// Assignment-identity columns of one side of a rule-3 lookup, unnested with
/// ordinality so each returned row can be re-associated with the input it
/// matched.
const SAME_ASSIGNMENT_KEY_UNNEST_SQL: &str = r#"
    UNNEST(
        $1::TEXT[],
        $2::TEXT[],
        $3::TEXT[],
        $4::UUID[],
        $5::UUID[],
        $6::TEXT[],
        $7::BIGINT[],
        $8::TEXT[]
    ) WITH ORDINALITY AS assignment_key(
        observation_key,
        chain_id,
        edge_kind,
        from_contract_instance_id,
        to_contract_instance_id,
        discovery_source,
        source_manifest_id,
        admission,
        key_index
    )
"#;

struct SameAssignmentKeyArrays {
    observation_keys: Vec<String>,
    chains: Vec<String>,
    edge_kinds: Vec<String>,
    from_contract_instance_ids: Vec<Uuid>,
    to_contract_instance_ids: Vec<Uuid>,
    discovery_sources: Vec<String>,
    source_manifest_ids: Vec<i64>,
    admissions: Vec<String>,
}

impl SameAssignmentKeyArrays {
    fn from_specs<'a>(specs: impl Iterator<Item = &'a ReconciledDiscoveryEdgeSpec>) -> Self {
        let mut arrays = Self {
            observation_keys: Vec::new(),
            chains: Vec::new(),
            edge_kinds: Vec::new(),
            from_contract_instance_ids: Vec::new(),
            to_contract_instance_ids: Vec::new(),
            discovery_sources: Vec::new(),
            source_manifest_ids: Vec::new(),
            admissions: Vec::new(),
        };
        for spec in specs {
            arrays.observation_keys.push(spec.observation_key.clone());
            arrays.chains.push(spec.chain.clone());
            arrays.edge_kinds.push(spec.edge_kind.clone());
            arrays
                .from_contract_instance_ids
                .push(spec.from_contract_instance_id);
            arrays
                .to_contract_instance_ids
                .push(spec.to_contract_instance_id);
            arrays.discovery_sources.push(spec.discovery_source.clone());
            arrays.source_manifest_ids.push(spec.source_manifest_id);
            arrays.admissions.push(spec.admission.clone());
        }
        arrays
    }
}

/// Chronology rule 3 for the deactivation candidates: for every desired edge
/// sharing an assignment identity with a candidate at a no-later start,
/// resolve the earliest-starting active edge materializing that assignment
/// (over ALL active edges, not just candidates) and retain it.
///
/// Both lookup passes are batched with UNNEST joins per `batch_size` chunk
/// (one round trip per chunk instead of one per candidate/desired row); the
/// ordinality column re-associates each matched row with the candidate or
/// desired identity it matched, so the per-pair filters and the per-desired
/// min-epoch resolution are unchanged.
pub(in super::super) async fn collect_same_assignment_retained_edges(
    executor: &mut PgConnection,
    discovery_source: &str,
    candidates: &[ExistingReconciledDiscoveryEdge],
    batch_size: usize,
    retained_newer_edge_ids: &mut HashSet<i64>,
) -> Result<()> {
    // Orphaned candidates never satisfy the rule-3 filter, so they are
    // excluded from the batch up front (equivalent to the per-pair check).
    let live_candidates = candidates
        .iter()
        .filter(|candidate| !candidate.active_from_block_is_orphaned)
        .collect::<Vec<_>>();
    let mut matched_desired = HashSet::<ReconciledDiscoveryEdgeSpec>::new();
    for chunk in live_candidates.chunks(batch_size.max(1)) {
        let arrays =
            SameAssignmentKeyArrays::from_specs(chunk.iter().map(|candidate| &candidate.spec));
        let rows = sqlx::query(&format!(
            r#"
            SELECT assignment_key.key_index, {STREAMED_DESIRED_EDGE_COLUMNS}
            FROM {SAME_ASSIGNMENT_KEY_UNNEST_SQL}
            JOIN pg_temp.reconcile_desired_edges desired
              ON desired.observation_key = assignment_key.observation_key
             AND desired.chain_id = assignment_key.chain_id
             AND desired.edge_kind = assignment_key.edge_kind
             AND desired.from_contract_instance_id = assignment_key.from_contract_instance_id
             AND desired.to_contract_instance_id = assignment_key.to_contract_instance_id
             AND desired.discovery_source = assignment_key.discovery_source
             AND desired.source_manifest_id = assignment_key.source_manifest_id
             AND desired.admission = assignment_key.admission
            "#
        ))
        .bind(&arrays.observation_keys)
        .bind(&arrays.chains)
        .bind(&arrays.edge_kinds)
        .bind(&arrays.from_contract_instance_ids)
        .bind(&arrays.to_contract_instance_ids)
        .bind(&arrays.discovery_sources)
        .bind(&arrays.source_manifest_ids)
        .bind(&arrays.admissions)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load same-assignment desired edges for deactivation candidates")?;
        for row in &rows {
            let key_index: i64 = row
                .try_get("key_index")
                .context("failed to read same-assignment candidate ordinality")?;
            let candidate = chunk
                .get(usize::try_from(key_index - 1).context("candidate ordinality underflowed")?)
                .context("same-assignment candidate ordinality out of range")?;
            let (_, desired) = desired_edge_spec_from_row(row)?;
            if assignment_starts_no_later(candidate, &desired) {
                matched_desired.insert(desired);
            }
        }
    }

    // Deterministic batching; the retained-id union is order-independent.
    let mut matched_desired = matched_desired.into_iter().collect::<Vec<_>>();
    matched_desired.sort_by(compare_reconciled_discovery_edge_specs);
    for chunk in matched_desired.chunks(batch_size.max(1)) {
        let arrays = SameAssignmentKeyArrays::from_specs(chunk.iter());
        let rows = sqlx::query(&format!(
            r#"
            SELECT assignment_key.key_index, {STREAMED_EXISTING_EDGE_COLUMNS_QUALIFIED}
            FROM {SAME_ASSIGNMENT_KEY_UNNEST_SQL}
            JOIN discovery_edges de
              ON de.provenance ->> 'observation_key' = assignment_key.observation_key
             AND de.chain_id = assignment_key.chain_id
             AND de.edge_kind = assignment_key.edge_kind
             AND de.from_contract_instance_id = assignment_key.from_contract_instance_id
             AND de.to_contract_instance_id = assignment_key.to_contract_instance_id
             AND COALESCE(de.source_manifest_id, -1) = assignment_key.source_manifest_id
             AND de.admission = assignment_key.admission
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = de.to_contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE de.discovery_source = $9
              AND de.deactivated_at IS NULL
            "#
        ))
        .bind(&arrays.observation_keys)
        .bind(&arrays.chains)
        .bind(&arrays.edge_kinds)
        .bind(&arrays.from_contract_instance_ids)
        .bind(&arrays.to_contract_instance_ids)
        .bind(&arrays.discovery_sources)
        .bind(&arrays.source_manifest_ids)
        .bind(&arrays.admissions)
        .bind(discovery_source)
        .fetch_all(&mut *executor)
        .await
        .context("failed to load same-assignment active edges for desired edges")?;

        let mut matching_edges_by_desired = HashMap::<usize, Vec<_>>::new();
        for row in rows {
            let key_index: i64 = row
                .try_get("key_index")
                .context("failed to read same-assignment desired ordinality")?;
            let desired_index = usize::try_from(key_index - 1)
                .context("same-assignment desired ordinality underflowed")?;
            matching_edges_by_desired
                .entry(desired_index)
                .or_default()
                .push(edge_from_row(row)?);
        }
        for (desired_index, matching_edges) in matching_edges_by_desired {
            let desired = chunk
                .get(desired_index)
                .context("same-assignment desired ordinality out of range")?;
            if let Some(retained) = matching_edges
                .iter()
                .filter(|edge| {
                    !edge.active_from_block_is_orphaned && assignment_starts_no_later(edge, desired)
                })
                .min_by(compare_edge_starts)
            {
                retained_newer_edge_ids.insert(retained.discovery_edge_id);
            }
        }
    }
    Ok(())
}
