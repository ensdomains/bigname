use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};
use bigname_manifests::{
    find_uncovered_required_watched_tuples_for_retention_generation,
    load_active_manifest_abi_events_by_chain_and_source_families, load_discovery_admission_epoch,
    load_required_watched_tuples,
};
use bigname_storage::load_raw_log_staging_input_version;
use sqlx::Row;

use crate::ens_v1_resolver::{
    GENERIC_SOURCE_SCOPE_ADDRESS, SOURCE_FAMILY_ENS_V1_RESOLVER_L1, generic_resolver_record_topic0s,
};
use crate::reconciliation::canonical::stored_lineage::ensure_required_topic_sets_undrifted_for_retention_generation;

use super::{
    SOURCE_FAMILY_BASENAMES_BASE_REGISTRY, SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
    SOURCE_FAMILY_ENS_V2_REGISTRY_L1, SOURCE_FAMILY_ENS_V2_ROOT_L1, source_family_in,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetentionClosureAuthorityKind {
    EnsV2Proof,
    LegacyRegistryCoverage,
    Unsupported,
}

fn retention_closure_authority_kind(source_families: &[&str]) -> RetentionClosureAuthorityKind {
    if source_families.iter().all(|family| {
        matches!(
            *family,
            SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1
        )
    }) {
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

    let unsupported_family = closure_source_families.iter().find(|family| {
        !matches!(
            **family,
            SOURCE_FAMILY_ENS_V2_ROOT_L1 | SOURCE_FAMILY_ENS_V2_REGISTRY_L1
        )
    });
    if let Some(unsupported_family) = unsupported_family {
        anyhow::bail!(
            "normalized-event replay cannot establish full closure from incomplete raw-log retention generation {retention_generation} on chain {chain}: source family {unsupported_family} has no generation-bound closure proof; run explicit historical backfill/refetch or restore a versioned adapter snapshot"
        );
    }

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

    let input_before = load_raw_log_staging_input_version(pool, chain).await?;
    let admission_epoch = load_discovery_admission_epoch(pool, chain).await?;
    if input_before.retention_generation == 0 {
        return Ok(admission_epoch);
    }

    let source_families = closure_source_families
        .iter()
        .map(|family| (*family).to_owned())
        .collect::<Vec<_>>();
    let requirements =
        load_required_watched_tuples(pool, chain, 0, through_block, &source_families).await?;
    ensure!(
        !requirements.is_empty(),
        "legacy registry closure on chain {chain} in raw-log retention generation {} has no historically authoritative watched tuple through block {through_block}",
        input_before.retention_generation
    );

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
    ensure_required_topic_sets_undrifted_for_retention_generation(
        pool,
        chain,
        &current_topic0s_by_family,
        &requirements,
        input_before.retention_generation,
    )
    .await
    .map_err(anyhow::Error::msg)?;

    let uncovered = find_uncovered_required_watched_tuples_for_retention_generation(
        pool,
        chain,
        &requirements,
        input_before.retention_generation,
        MAX_REPORTED_LEGACY_CLOSURE_COVERAGE_GAPS,
    )
    .await?;
    if !uncovered.is_empty() {
        let listed = uncovered
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
            .join(", ");
        let suffix = if uncovered.len() as i64 >= MAX_REPORTED_LEGACY_CLOSURE_COVERAGE_GAPS {
            " (further violations elided)"
        } else {
            ""
        };
        anyhow::bail!(
            "legacy registry closure on chain {chain} cannot use raw-log retention generation {} through block {through_block}: current-generation backfill coverage is missing or stale for {listed}{suffix}; run generation-bound historical backfill/refetch and retry",
            input_before.retention_generation
        );
    }

    let input_after = load_raw_log_staging_input_version(pool, chain).await?;
    ensure!(
        input_after == input_before,
        "raw-log staging input changed while proving legacy registry closure for chain {chain}: expected generation {} revision {}, observed generation {} revision {}",
        input_before.retention_generation,
        input_before.revision,
        input_after.retention_generation,
        input_after.revision
    );
    let admission_epoch_after = load_discovery_admission_epoch(pool, chain).await?;
    ensure!(
        admission_epoch_after == admission_epoch,
        "discovery admission epoch changed while proving legacy registry closure for chain {chain}: expected {admission_epoch}, observed {admission_epoch_after}"
    );
    Ok(admission_epoch)
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
mod tests {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    use super::*;

    #[test]
    fn legacy_registry_closure_has_generation_bound_coverage_strategy() {
        for source_families in [
            &[SOURCE_FAMILY_ENS_V1_REGISTRY_L1][..],
            &[SOURCE_FAMILY_BASENAMES_BASE_REGISTRY][..],
            &[
                SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
                SOURCE_FAMILY_BASENAMES_BASE_REGISTRY,
            ][..],
        ] {
            assert_eq!(
                retention_closure_authority_kind(source_families),
                RetentionClosureAuthorityKind::LegacyRegistryCoverage
            );
        }
        assert_eq!(
            retention_closure_authority_kind(&[
                SOURCE_FAMILY_ENS_V1_REGISTRY_L1,
                SOURCE_FAMILY_ENS_V1_RESOLVER_L1,
            ]),
            RetentionClosureAuthorityKind::Unsupported
        );
    }

    #[tokio::test]
    async fn full_closure_fails_closed_without_retention_authority_state() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("indexer_closure_boundary_missing_authority"),
            &bigname_storage::MIGRATOR,
            "failed to apply migrations for closure-boundary test",
        )
        .await?;

        let error = ensure_full_closure_retention_authority(
            database.pool(),
            "unconfigured-testnet",
            &[SOURCE_FAMILY_ENS_V2_REGISTRY_L1],
            1,
        )
        .await
        .expect_err("full closure without durable retention authority must fail closed");

        assert!(
            error
                .to_string()
                .contains("has no raw-log retention authority state"),
            "unexpected missing-authority error: {error:#}"
        );

        database.cleanup().await
    }
}
