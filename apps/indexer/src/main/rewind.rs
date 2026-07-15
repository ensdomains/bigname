use anyhow::{Context, Result, bail};
use bigname_storage::{
    CheckpointBlockRef, IdentityOrphanCounts, RawFactOrphanCounts, chain_lineage_contains_ancestor,
    invalidate_execution_outcomes_for_orphaned_blocks, load_chain_checkpoint,
    load_chain_lineage_block, mark_block_derived_normalized_events_range_orphaned,
    mark_chain_lineage_range_orphaned, mark_identity_rows_range_orphaned,
    mark_raw_block_facts_range_orphaned, rewind_chain_checkpoints_to_ancestor,
};

use crate::{cli::RewindArgs, reconciliation::ensure_losing_branch_raw_blocks_exist};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RewindOutcome {
    pub(crate) deployment_profile: String,
    pub(crate) chain: String,
    pub(crate) from_block_hash: String,
    pub(crate) ancestor_block_hash: String,
    pub(crate) ancestor_block_number: i64,
    pub(crate) orphaned_lineage_count: usize,
    pub(crate) orphaned_raw_fact_counts: RawFactOrphanCounts,
    pub(crate) orphaned_normalized_event_count: u64,
    pub(crate) orphaned_identity_counts: IdentityOrphanCounts,
    pub(crate) invalidated_execution_outcome_count: u64,
}

pub(crate) async fn run_rewind(args: RewindArgs) -> Result<RewindOutcome> {
    let (pool, _rederive_guard) =
        bigname_storage::connect_with_base_normalized_rederive_writer_guard(
            &args.database,
            "bigname-indexer",
        )
        .await?;
    rewind_to_exact_ancestor(
        &pool,
        args.deployment_profile,
        args.chain,
        args.from_block_hash,
        CheckpointBlockRef {
            block_hash: args.ancestor_block_hash,
            block_number: args.ancestor_block_number,
        },
    )
    .await
}

async fn rewind_to_exact_ancestor(
    pool: &sqlx::PgPool,
    deployment_profile: String,
    chain: String,
    from_block_hash: Option<String>,
    ancestor: CheckpointBlockRef,
) -> Result<RewindOutcome> {
    if ancestor.block_number < 0 {
        bail!(
            "rewind ancestor for chain {chain} has negative block number {}",
            ancestor.block_number
        );
    }

    let ancestor_row = load_chain_lineage_block(pool, &chain, &ancestor.block_hash)
        .await?
        .with_context(|| {
            format!(
                "rewind ancestor block {} is not stored for chain {chain}",
                ancestor.block_hash
            )
        })?;
    if ancestor_row.block_number != ancestor.block_number {
        bail!(
            "rewind ancestor block {} for chain {chain} has stored block number {}, expected {}",
            ancestor.block_hash,
            ancestor_row.block_number,
            ancestor.block_number
        );
    }

    let from_block_hash = match from_block_hash {
        Some(from_block_hash) => from_block_hash,
        None => load_chain_checkpoint(pool, &chain)
            .await?
            .and_then(|checkpoint| checkpoint.canonical_block_hash)
            .with_context(|| {
                format!(
                    "rewind requires --from-block-hash or a stored canonical checkpoint for chain {chain}"
                )
            })?,
    };

    if !chain_lineage_contains_ancestor(pool, &chain, &from_block_hash, &ancestor.block_hash)
        .await?
    {
        bail!(
            "rewind ancestor {} is not on the stored lineage path from {} for chain {chain}",
            ancestor.block_hash,
            from_block_hash
        );
    }

    ensure_losing_branch_raw_blocks_exist(
        pool,
        &chain,
        &from_block_hash,
        Some(&ancestor.block_hash),
    )
    .await?;
    let orphaned_lineage = mark_chain_lineage_range_orphaned(
        pool,
        &chain,
        &from_block_hash,
        Some(&ancestor.block_hash),
    )
    .await?;
    let orphaned_raw_fact_counts = mark_raw_block_facts_range_orphaned(
        pool,
        &chain,
        &from_block_hash,
        Some(&ancestor.block_hash),
    )
    .await?;
    let orphaned_normalized_event_count = mark_block_derived_normalized_events_range_orphaned(
        pool,
        &chain,
        &from_block_hash,
        Some(&ancestor.block_hash),
    )
    .await?;
    let orphaned_identity_counts = mark_identity_rows_range_orphaned(
        pool,
        &chain,
        &from_block_hash,
        Some(&ancestor.block_hash),
    )
    .await?;
    let execution_summary = invalidate_execution_outcomes_for_orphaned_blocks(pool).await?;
    // Raw-code canonicality changes may enqueue resolver-profile repair. Do
    // not publish the rewound checkpoint until every observed generation has
    // completed its adapter and projection-invalidation handoff.
    crate::resolver_profile_convergence::drain_resolver_profile_input_changes(pool).await?;
    rewind_chain_checkpoints_to_ancestor(pool, &chain, &ancestor).await?;

    Ok(RewindOutcome {
        deployment_profile,
        chain,
        from_block_hash,
        ancestor_block_hash: ancestor.block_hash,
        ancestor_block_number: ancestor.block_number,
        orphaned_lineage_count: orphaned_lineage.len(),
        orphaned_raw_fact_counts,
        orphaned_normalized_event_count,
        orphaned_identity_counts,
        invalidated_execution_outcome_count: execution_summary.deleted_outcome_count,
    })
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bigname_storage::{
        ResolverProfileReconciliationTarget, enqueue_resolver_profile_reconciliations,
        load_chain_checkpoint,
    };
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    use super::*;

    #[tokio::test]
    async fn rewind_drains_resolver_profile_work_before_checkpoint_publication() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("indexer_rewind_resolver_profile_handoff"),
            &bigname_storage::MIGRATOR,
            "failed to apply migrations for rewind resolver-profile handoff test",
        )
        .await?;
        let chain = "ethereum-mainnet";
        let ancestor_hash = "0x1111111111111111111111111111111111111111111111111111111111111111";
        let losing_hash = "0x2222222222222222222222222222222222222222222222222222222222222222";
        sqlx::query(
            r#"
            INSERT INTO chain_lineage (
                chain_id, block_hash, parent_hash, block_number,
                block_timestamp, canonicality_state
            ) VALUES
                ($1, $2, NULL, 1, now(), 'canonical'),
                ($1, $3, $2, 2, now(), 'canonical')
            "#,
        )
        .bind(chain)
        .bind(ancestor_hash)
        .bind(losing_hash)
        .execute(database.pool())
        .await?;
        sqlx::query(
            r#"
            INSERT INTO chain_checkpoints (
                chain_id,
                canonical_block_hash, canonical_block_number,
                safe_block_hash, safe_block_number,
                finalized_block_hash, finalized_block_number
            ) VALUES ($1, $2, 2, $2, 2, $2, 2)
            "#,
        )
        .bind(chain)
        .bind(losing_hash)
        .execute(database.pool())
        .await?;
        enqueue_resolver_profile_reconciliations(
            database.pool(),
            &[ResolverProfileReconciliationTarget {
                chain_id: chain.to_owned(),
                contract_address: "0x00000000000000000000000000000000000000ff".to_owned(),
            }],
        )
        .await?;
        sqlx::query(
            r#"
            CREATE FUNCTION require_profile_queue_drained_before_rewind_checkpoint()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM resolver_profile_input_changes
                    WHERE processed_generation < generation
                ) THEN
                    RAISE EXCEPTION 'resolver-profile queue was pending at checkpoint rewind';
                END IF;
                RETURN NEW;
            END;
            $$;
            "#,
        )
        .execute(database.pool())
        .await?;
        sqlx::query(
            r#"
            CREATE TRIGGER require_profile_queue_drained_before_rewind_checkpoint
            BEFORE UPDATE ON chain_checkpoints
            FOR EACH ROW
            EXECUTE FUNCTION require_profile_queue_drained_before_rewind_checkpoint();
            "#,
        )
        .execute(database.pool())
        .await?;

        let outcome = rewind_to_exact_ancestor(
            database.pool(),
            "test".to_owned(),
            chain.to_owned(),
            Some(losing_hash.to_owned()),
            CheckpointBlockRef {
                block_hash: ancestor_hash.to_owned(),
                block_number: 1,
            },
        )
        .await?;
        assert_eq!(outcome.ancestor_block_hash, ancestor_hash);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM resolver_profile_input_changes WHERE processed_generation < generation"
            )
            .fetch_one(database.pool())
            .await?,
            0
        );
        let checkpoint = load_chain_checkpoint(database.pool(), chain)
            .await?
            .expect("rewind checkpoint must exist");
        assert_eq!(
            checkpoint.canonical_block_hash.as_deref(),
            Some(ancestor_hash)
        );

        database.cleanup().await
    }
}
