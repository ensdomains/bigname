use std::collections::BTreeSet;

use anyhow::{Context, Result};
use bigname_manifests::{
    WATCHED_COVERAGE_VERIFICATION_CHUNK_BLOCKS, find_uncovered_watched_tuples,
    load_log_producing_source_families,
};
use bigname_storage::DataCompletenessRead;
use sqlx::PgPool;

const MAX_REPORTED_UNCOVERED_TUPLES: usize = 20;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BackfillCoverageGap {
    pub(super) chain: String,
    pub(super) source_family: String,
    pub(super) address: String,
    pub(super) required_from_block: i64,
    pub(super) required_to_block: i64,
}

/// Reconcile retained bounded-backfill spans against the same durable watched-tuple coverage
/// authority used by stored-lineage checkpoint promotion. Incomplete and failed job intervals
/// remain in scope because their retained lineage can be crash residue; facts from a later retry
/// can satisfy the interval. Evidence remains required after a checkpoint consumes the span:
/// checkpoint regression or database restore can make it promotion input again, and deleting the
/// evidence must not silently preserve a completeness pass.
pub(super) async fn load_backfill_coverage_gaps(
    pool: &PgPool,
    read: &DataCompletenessRead,
) -> Result<Vec<BackfillCoverageGap>> {
    let active_chains = read
        .manifest_chain_namespaces
        .iter()
        .map(|entry| entry.chain.as_str())
        .collect::<BTreeSet<_>>();
    let mut gaps = Vec::new();

    for chain in read
        .chains
        .iter()
        .filter(|row| active_chains.contains(row.chain_id.as_str()))
    {
        let (Some(lineage_floor), Some(lineage_head)) = (
            chain.lineage_floor_block_number,
            chain.lineage_head_block_number,
        ) else {
            continue;
        };
        let backfill_jobs = bigname_storage::load_backfill_jobs_intersecting_range(
            pool,
            &chain.chain_id,
            lineage_floor,
            lineage_head,
        )
        .await
        .with_context(|| {
            format!(
                "failed to load backfill evidence for completeness coverage on {}",
                chain.chain_id
            )
        })?;
        let retained_ranges = merged_retained_backfill_ranges(
            backfill_jobs
                .iter()
                .map(|job| (job.range_start_block_number, job.range_end_block_number)),
            lineage_floor,
            lineage_head,
        );
        if retained_ranges.is_empty() {
            // Ordinary provider-fetched live lineage does not use backfill coverage facts.
            // Persisted bounded jobs identify the retained spans that do.
            continue;
        }
        let source_families = load_log_producing_source_families(pool, &chain.chain_id)
            .await
            .with_context(|| {
                format!(
                    "failed to load log-producing source families for completeness coverage on {}",
                    chain.chain_id
                )
            })?;
        if source_families.is_empty() {
            continue;
        }

        for (from_block, through_block) in retained_ranges {
            let mut chunk_from = from_block;
            while chunk_from <= through_block && gaps.len() < MAX_REPORTED_UNCOVERED_TUPLES {
                let chunk_through = chunk_from
                    .saturating_add(WATCHED_COVERAGE_VERIFICATION_CHUNK_BLOCKS - 1)
                    .min(through_block);
                let remaining = MAX_REPORTED_UNCOVERED_TUPLES - gaps.len();
                let uncovered = find_uncovered_watched_tuples(
                    pool,
                    &chain.chain_id,
                    chunk_from,
                    chunk_through,
                    &source_families,
                    remaining as i64,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to reconcile completeness coverage for {} over {}..={}",
                        chain.chain_id, chunk_from, chunk_through
                    )
                })?;
                gaps.extend(uncovered.into_iter().map(|tuple| BackfillCoverageGap {
                    chain: chain.chain_id.clone(),
                    source_family: tuple.source_family,
                    address: tuple.address,
                    required_from_block: tuple.required_from_block,
                    required_to_block: tuple.required_to_block,
                }));

                let Some(next_chunk) = chunk_through.checked_add(1) else {
                    break;
                };
                chunk_from = next_chunk;
            }
            if gaps.len() >= MAX_REPORTED_UNCOVERED_TUPLES {
                break;
            }
        }
    }

    Ok(gaps)
}

fn merged_retained_backfill_ranges(
    ranges: impl IntoIterator<Item = (i64, i64)>,
    lineage_floor: i64,
    lineage_head: i64,
) -> Vec<(i64, i64)> {
    let mut ranges = ranges
        .into_iter()
        .filter_map(|(from_block, through_block)| {
            let from_block = from_block.max(lineage_floor);
            let through_block = through_block.min(lineage_head);
            (from_block <= through_block).then_some((from_block, through_block))
        })
        .collect::<Vec<_>>();
    ranges.sort_unstable();

    let mut merged = Vec::<(i64, i64)>::new();
    for (from_block, through_block) in ranges {
        if let Some((_, merged_through)) = merged.last_mut()
            && from_block <= merged_through.saturating_add(1)
        {
            *merged_through = (*merged_through).max(through_block);
        } else {
            merged.push((from_block, through_block));
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    use super::{load_backfill_coverage_gaps, merged_retained_backfill_ranges};

    #[test]
    fn backfill_ranges_are_clamped_and_merged_inside_retained_lineage() {
        assert_eq!(
            merged_retained_backfill_ranges([(90, 101), (102, 120), (130, 160)], 100, 150),
            vec![(100, 120), (130, 150)]
        );
    }

    #[tokio::test]
    async fn incomplete_backfill_residue_requires_durable_coverage_facts() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("worker_data_completeness_backfill_coverage")
                .admin_database("postgres")
                .pool_max_connections(5)
                .parse_context("failed to parse worker completeness test database URL")
                .admin_connect_context("failed to connect worker completeness admin pool")
                .pool_connect_context("failed to connect worker completeness test pool"),
            &bigname_storage::MIGRATOR,
            "failed to apply worker completeness test migrations",
        )
        .await?;
        let pool = database.pool();
        let manifest_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO manifest_versions
                (manifest_version, namespace, source_family, chain, deployment_epoch,
                 rollout_status, normalizer_version, file_path, manifest_payload)
            VALUES
                (1, 'ens', 'ens_v2_registry_l1', 'ethereum-sepolia',
                 'ens_v2_sepolia_dev', 'active', 'n', 'f',
                 '{"contracts":[{"role":"registry","address":"0xabc","start_block":1}],
                   "abi":{"events":[{"name":"ResolverChanged",
                   "fragment":"event ResolverChanged(bytes32 indexed node)"}]}}'::jsonb)
            RETURNING manifest_id
            "#,
        )
        .fetch_one(pool)
        .await?;
        sqlx::raw_sql(
            r#"
            INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
            VALUES ('11111111-1111-1111-1111-111111111111', 'ethereum-sepolia', 'contract')
            "#,
        )
        .execute(pool)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances
                (manifest_id, declaration_kind, declaration_name, contract_instance_id,
                 declared_address, role, proxy_kind)
            VALUES
                ($1, 'contract', 'registry', '11111111-1111-1111-1111-111111111111',
                 '0xabc', 'registry', 'none')
            "#,
        )
        .bind(manifest_id)
        .execute(pool)
        .await?;
        sqlx::raw_sql(
            r#"
            INSERT INTO contract_instance_addresses
                (contract_instance_id, chain_id, address, active_from_block_number)
            VALUES
                ('11111111-1111-1111-1111-111111111111', 'ethereum-sepolia', '0xabc', 1);

            INSERT INTO chain_lineage
                (chain_id, block_hash, parent_hash, block_number, block_timestamp,
                 canonicality_state)
            VALUES
                ('ethereum-sepolia', '0x100', '0x099', 100, now(), 'canonical'),
                ('ethereum-sepolia', '0x101', '0x100', 101, now(), 'canonical'),
                ('ethereum-sepolia', '0x102', '0x101', 102, now(), 'canonical');

            INSERT INTO chain_checkpoints
                (chain_id, canonical_block_hash, canonical_block_number)
            VALUES ('ethereum-sepolia', '0x100', 100)
            "#,
        )
        .execute(pool)
        .await?;

        let read = bigname_storage::load_data_completeness(pool).await?;
        assert!(
            load_backfill_coverage_gaps(pool, &read).await?.is_empty(),
            "provider-fetched live lineage without a bounded backfill must not require facts"
        );
        let backfill_job_id = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO backfill_jobs
                (deployment_profile, chain_id, source_identity, scan_mode,
                 range_start_block_number, range_end_block_number, idempotency_key,
                 status)
            VALUES
                ('sepolia', 'ethereum-sepolia', '{}'::jsonb, 'hash_pinned',
                 101, 102, 'missing-facts', 'running')
            RETURNING backfill_job_id
            "#,
        )
        .fetch_one(pool)
        .await?;

        sqlx::query(
            r#"
            UPDATE chain_checkpoints
            SET canonical_block_hash = '0x102', canonical_block_number = 102
            WHERE chain_id = 'ethereum-sepolia'
            "#,
        )
        .execute(pool)
        .await?;
        let read = bigname_storage::load_data_completeness(pool).await?;
        let gaps = load_backfill_coverage_gaps(pool, &read).await?;
        assert_eq!(gaps.len(), 1);
        assert_eq!(gaps[0].chain, "ethereum-sepolia");
        assert_eq!(gaps[0].source_family, "ens_v2_registry_l1");
        assert_eq!(gaps[0].address, "0xabc");
        assert_eq!(gaps[0].required_from_block, 101);
        assert_eq!(gaps[0].required_to_block, 102);

        sqlx::query(
            r#"
            UPDATE backfill_jobs
            SET status = 'completed', completed_at = now()
            WHERE backfill_job_id = $1
            "#,
        )
        .bind(backfill_job_id)
        .execute(pool)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO backfill_coverage_facts
                (backfill_job_id, chain_id, source_family, scope, address,
                 covered_from_block, covered_to_block, derivation)
            VALUES ($1, 'ethereum-sepolia', 'ens_v2_registry_l1', 'address', '0xabc',
                    101, 102, 'job_completion')
            "#,
        )
        .bind(backfill_job_id)
        .execute(pool)
        .await?;
        assert!(
            load_backfill_coverage_gaps(pool, &read).await?.is_empty(),
            "a successful retry's exact durable fact must satisfy the retained backfill interval"
        );

        database.cleanup().await
    }
}
