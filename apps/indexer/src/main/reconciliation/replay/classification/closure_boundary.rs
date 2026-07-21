use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use bigname_manifests::{
    ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES, UncoveredWatchedTuple,
    load_active_manifest_abi_events_by_chain_and_source_families, load_discovery_admission_epoch,
    load_required_watched_tuples,
};
use bigname_storage::load_raw_log_staging_input_version;
use sqlx::Row;

use crate::ens_v1_resolver::{
    GENERIC_SOURCE_SCOPE_ADDRESS, SOURCE_FAMILY_ENS_V1_RESOLVER_L1, generic_resolver_record_topic0s,
};
use crate::reconciliation::canonical::stored_lineage::find_uncovered_generation_bound_coverage_with_current_topics;

use super::{
    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY, SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_RESOLVER_L1, source_family_in,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetentionClosureAuthorityKind {
    EnsV2Proof,
    LegacyRegistryCoverage,
    Unsupported,
}

fn retention_closure_authority_kind(source_families: &[&str]) -> RetentionClosureAuthorityKind {
    if source_families
        .iter()
        .all(|family| ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES.contains(family))
    {
        RetentionClosureAuthorityKind::EnsV2Proof
    } else if source_families.iter().all(|family| {
        matches!(
            *family,
            SOURCE_FAMILY_ENS_V1_REGISTRY_L1 | SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
        )
    }) {
        RetentionClosureAuthorityKind::LegacyRegistryCoverage
    } else {
        RetentionClosureAuthorityKind::Unsupported
    }
}

const MAX_REPORTED_LEGACY_CLOSURE_COVERAGE_GAPS: i64 = 20;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LegacyRegistryNewlyRequiredCoverage {
    pub(crate) chain: String,
    pub(crate) retention_generation: i64,
    pub(crate) source_family: String,
    pub(crate) address: String,
    pub(crate) required_from_block: i64,
    pub(crate) required_to_block: i64,
}

impl std::fmt::Display for LegacyRegistryNewlyRequiredCoverage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "legacy registry closure on chain {} cannot use raw-log retention generation {}: current-generation backfill coverage is missing or stale for (source_family {}, address {}, blocks {}..={}); run generation-bound historical backfill/refetch and retry",
            self.chain,
            self.retention_generation,
            self.source_family,
            self.address,
            self.required_from_block,
            self.required_to_block,
        )
    }
}

impl std::error::Error for LegacyRegistryNewlyRequiredCoverage {}

struct GenerationBoundCoverageProof {
    input_version: bigname_storage::RawLogStagingInputVersion,
    admission_epoch: i64,
    requirement_count: usize,
    uncovered: Vec<UncoveredWatchedTuple>,
}

pub(super) async fn ensure_full_closure_retention_authority(
    pool: &sqlx::PgPool,
    chain: &str,
    closure_source_families: &[&str],
    through_block: i64,
) -> Result<()> {
    if closure_source_families.is_empty() {
        return Ok(());
    }

    let state = sqlx::query(
        r#"
        SELECT
            retained.retention_generation,
            retained.retained_history_complete,
            retained.proven_retention_generation,
            retained.proven_discovery_admission_epoch,
            retained.proven_through_block,
            admission.epoch AS current_discovery_admission_epoch
        FROM raw_log_staging_input_revisions retained
        LEFT JOIN discovery_admission_epochs admission
          ON admission.chain_id = retained.chain_id
        WHERE retained.chain_id = $1
        "#,
    )
    .bind(chain)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load raw-log retention authority for replay on chain {chain}")
    })?;
    let state = state.with_context(|| {
        format!("normalized-event replay on chain {chain} has no raw-log retention authority state")
    })?;

    let retention_generation = state
        .try_get::<i64, _>("retention_generation")
        .context("missing raw-log retention generation")?;
    // Generation zero is the never-destructively-rotated initial staging
    // corpus. Its earliest retained canonical fact remains an honest closure
    // boundary. Once retention rotates, absence is meaningful only under a
    // source-family-specific proof for the current generation.
    if retention_generation == 0 {
        return Ok(());
    }

    let ens_v2_proof_required = closure_source_families
        .iter()
        .any(|family| ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES.contains(family));
    if ens_v2_proof_required {
        let retained_history_complete = state
            .try_get::<bool, _>("retained_history_complete")
            .context("missing retained-history completeness state")?;
        let proven_retention_generation = state
            .try_get::<Option<i64>, _>("proven_retention_generation")
            .context("missing proven raw-log retention generation")?;
        let proven_discovery_admission_epoch = state
            .try_get::<Option<i64>, _>("proven_discovery_admission_epoch")
            .context("missing proven discovery-admission epoch")?;
        let proven_through_block = state
            .try_get::<Option<i64>, _>("proven_through_block")
            .context("missing retained-history proof boundary")?;
        let current_discovery_admission_epoch = state
            .try_get::<Option<i64>, _>("current_discovery_admission_epoch")
            .context("missing current discovery-admission epoch")?;
        if !retained_history_complete
            || proven_retention_generation != Some(retention_generation)
            || proven_discovery_admission_epoch != current_discovery_admission_epoch
            || current_discovery_admission_epoch.is_none()
            || proven_through_block.is_none_or(|proven| proven < through_block)
        {
            anyhow::bail!(
                "normalized-event replay cannot establish full closure from incomplete raw-log retention generation {retention_generation} on chain {chain}: the ENSv2 root/registry proof is absent, stale, or does not cover target block {through_block}; run generation-bound historical backfill/refetch"
            );
        }
    }

    let generation_covered_families = closure_source_families
        .iter()
        .copied()
        .filter(|family| !ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES.contains(family))
        .collect::<Vec<_>>();
    if !generation_covered_families.is_empty() {
        let proof = load_generation_bound_coverage_proof(
            pool,
            chain,
            &generation_covered_families,
            through_block,
        )
        .await?;
        ensure!(
            proof.requirement_count > 0,
            "normalized-event replay cannot establish full closure from raw-log retention generation {retention_generation} on chain {chain}: no historically authoritative watched tuple exists for source families {}; declare finite history or restore a versioned adapter snapshot",
            generation_covered_families.join(", ")
        );
        if !proof.uncovered.is_empty() {
            if let Some(uncovered) = proof
                .uncovered
                .iter()
                .find(|tuple| tuple.source_family == SOURCE_FAMILY_ENS_V2_RESOLVER_L1)
            {
                return Err(bigname_adapters::EnsV2MissingCoverage {
                    chain: chain.to_owned(),
                    retention_generation,
                    source_family: uncovered.source_family.clone(),
                    address: uncovered.address.clone(),
                    required_from_block: uncovered.required_from_block,
                    required_to_block: uncovered.required_to_block,
                }
                .into());
            }
            let listed = render_uncovered_tuples(&proof.uncovered);
            let suffix = elided_gap_suffix(&proof.uncovered);
            anyhow::bail!(
                "normalized-event replay cannot establish full closure from incomplete raw-log retention generation {retention_generation} on chain {chain}: current-generation backfill coverage is missing or stale for {listed}{suffix}; run generation-bound historical backfill/refetch"
            );
        }
    }
    Ok(())
}

/// Establish absence authority for the target-bounded ENSv1/Basenames
/// registry-discovery repair used before a reorg checkpoint advances.
///
/// Generation zero retains its original raw-log boundary. After a destructive
/// rotation, every historically authoritative registry emitter interval
/// through the target must have gap-free coverage facts produced by completed
/// jobs in the current generation. The returned admission epoch must be
/// carried into the absence-aware discovery writer, which rechecks it under
/// its writer fence before changing any edge.
pub(super) async fn ensure_legacy_registry_closure_retention_authority(
    pool: &sqlx::PgPool,
    chain: &str,
    closure_source_families: &[&str],
    through_block: i64,
) -> Result<i64> {
    ensure!(
        through_block >= 0,
        "legacy registry closure target block must not be negative"
    );
    ensure!(
        !closure_source_families.is_empty()
            && retention_closure_authority_kind(closure_source_families)
                == RetentionClosureAuthorityKind::LegacyRegistryCoverage,
        "legacy registry closure authority accepts only ENSv1/Basenames registry source families"
    );

    let proof =
        load_generation_bound_coverage_proof(pool, chain, closure_source_families, through_block)
            .await?;
    if proof.input_version.retention_generation == 0 {
        return Ok(proof.admission_epoch);
    }
    ensure!(
        proof.requirement_count > 0,
        "legacy registry closure on chain {chain} in raw-log retention generation {} has no historically authoritative watched tuple through block {through_block}",
        proof.input_version.retention_generation
    );
    if let Some(uncovered) = proof.uncovered.first() {
        return Err(anyhow::Error::new(LegacyRegistryNewlyRequiredCoverage {
            chain: chain.to_owned(),
            retention_generation: proof.input_version.retention_generation,
            source_family: uncovered.source_family.clone(),
            address: uncovered.address.clone(),
            required_from_block: uncovered.required_from_block,
            required_to_block: uncovered.required_to_block,
        }));
    }
    Ok(proof.admission_epoch)
}

async fn load_generation_bound_coverage_proof(
    pool: &sqlx::PgPool,
    chain: &str,
    source_families: &[&str],
    through_block: i64,
) -> Result<GenerationBoundCoverageProof> {
    let input_before = load_raw_log_staging_input_version(pool, chain).await?;
    let admission_epoch = load_discovery_admission_epoch(pool, chain).await?;
    if input_before.retention_generation == 0 {
        return Ok(GenerationBoundCoverageProof {
            input_version: input_before,
            admission_epoch,
            requirement_count: 0,
            uncovered: Vec::new(),
        });
    }

    let source_families = source_families
        .iter()
        .map(|family| (*family).to_owned())
        .collect::<Vec<_>>();
    let requirements =
        load_required_watched_tuples(pool, chain, 0, through_block, &source_families).await?;
    let events =
        load_active_manifest_abi_events_by_chain_and_source_families(pool, chain, &source_families)
            .await?;
    let mut current_topic0s_by_family = BTreeMap::<String, BTreeSet<String>>::new();
    for event in events {
        if let Some(topic0) = event.topic0 {
            current_topic0s_by_family
                .entry(event.source_family)
                .or_default()
                .insert(topic0.to_ascii_lowercase());
        }
    }
    let uncovered = find_uncovered_generation_bound_coverage_with_current_topics(
        pool,
        chain,
        &current_topic0s_by_family,
        &requirements,
        input_before.retention_generation,
        MAX_REPORTED_LEGACY_CLOSURE_COVERAGE_GAPS,
    )
    .await
    .map_err(anyhow::Error::msg)?;

    let input_after = load_raw_log_staging_input_version(pool, chain).await?;
    ensure!(
        input_after == input_before,
        "raw-log staging input changed while proving generation-bound closure for chain {chain}: expected generation {} revision {}, observed generation {} revision {}",
        input_before.retention_generation,
        input_before.revision,
        input_after.retention_generation,
        input_after.revision
    );
    let admission_epoch_after = load_discovery_admission_epoch(pool, chain).await?;
    ensure!(
        admission_epoch_after == admission_epoch,
        "discovery admission epoch changed while proving generation-bound closure for chain {chain}: expected {admission_epoch}, observed {admission_epoch_after}"
    );
    Ok(GenerationBoundCoverageProof {
        input_version: input_before,
        admission_epoch,
        requirement_count: requirements.len(),
        uncovered,
    })
}

fn render_uncovered_tuples(uncovered: &[UncoveredWatchedTuple]) -> String {
    uncovered
        .iter()
        .map(|tuple| {
            format!(
                "(source_family {}, address {}, blocks {}..={})",
                tuple.source_family,
                tuple.address,
                tuple.required_from_block,
                tuple.required_to_block
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn elided_gap_suffix(uncovered: &[UncoveredWatchedTuple]) -> &'static str {
    if uncovered.len() as i64 >= MAX_REPORTED_LEGACY_CLOSURE_COVERAGE_GAPS {
        " (further violations elided)"
    } else {
        ""
    }
}

pub(super) async fn earliest_required_raw_fact_block(
    pool: &sqlx::PgPool,
    chain: &str,
    source_scope: &[(String, String, i64, i64)],
    closure_source_families: &[&str],
) -> Result<Option<i64>> {
    let required_scope = source_scope
        .iter()
        .filter(|(source_family, _, _, _)| source_family_in(source_family, closure_source_families))
        .map(|(source_family, address, from_block, to_block)| {
            (
                source_family.clone(),
                address.to_ascii_lowercase(),
                *from_block,
                *to_block,
            )
        })
        .collect::<Vec<_>>();
    if required_scope.is_empty() {
        return Ok(None);
    }

    let mut source_families = Vec::with_capacity(required_scope.len());
    let mut addresses = Vec::with_capacity(required_scope.len());
    let mut from_blocks = Vec::with_capacity(required_scope.len());
    let mut to_blocks = Vec::with_capacity(required_scope.len());
    for (source_family, address, from_block, to_block) in required_scope {
        source_families.push(source_family);
        addresses.push(address);
        from_blocks.push(from_block);
        to_blocks.push(to_block);
    }
    let generic_resolver_topic0s = generic_resolver_record_topic0s()
        .into_iter()
        .map(|topic0| topic0.to_ascii_lowercase())
        .collect::<Vec<_>>();

    let row = sqlx::query(
        r#"
        WITH required_scope AS (
            SELECT DISTINCT source_family, address, from_block, to_block
            FROM unnest(
                $2::TEXT[],
                $3::TEXT[],
                $4::BIGINT[],
                $5::BIGINT[]
            ) AS scope(source_family, address, from_block, to_block)
        )
        SELECT MIN(logs.block_number) AS closure_start_block
        FROM raw_logs AS logs
        JOIN chain_lineage AS lineage
          ON lineage.chain_id = logs.chain_id
         AND lineage.block_hash = logs.block_hash
        WHERE logs.chain_id = $1
          AND lineage.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND logs.canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
          AND EXISTS (
              SELECT 1
              FROM required_scope
              WHERE logs.block_number >= required_scope.from_block
                AND logs.block_number <= required_scope.to_block
                AND (
                  (
                    required_scope.address <> $6
                    AND LOWER(logs.emitting_address) = required_scope.address
                  )
                  OR (
                    required_scope.source_family = $7
                    AND required_scope.address = $6
                    AND LOWER(logs.topics[1]) = ANY($8::TEXT[])
                  )
                )
          )
        "#,
    )
    .bind(chain)
    .bind(&source_families)
    .bind(&addresses)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .bind(GENERIC_SOURCE_SCOPE_ADDRESS)
    .bind(SOURCE_FAMILY_ENS_V1_RESOLVER_L1)
    .bind(&generic_resolver_topic0s)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to load normalized replay closure boundary for chain {chain}")
    })?;

    Ok(row.get::<Option<i64>, _>("closure_start_block"))
}

#[cfg(test)]
#[path = "closure_boundary/tests.rs"]
mod tests;
