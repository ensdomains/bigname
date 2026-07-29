use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};

use super::*;

async fn insert_active_manifest(
    pool: &sqlx::PgPool,
    chain: &str,
    source_family: &str,
    event_name: &str,
) -> Result<()> {
    let payload = json!({
        "abi": {
            "events": [{
                "name": event_name,
                "fragment": format!("event {event_name}(bytes32 node)"),
            }],
        },
    });
    sqlx::query(
        r#"
        INSERT INTO manifest_versions (
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            rollout_status,
            normalizer_version,
            file_path,
            manifest_payload
        )
        VALUES (1, 'test', $1, $2, 'test', 'active', 'test', $3, $4)
        "#,
    )
    .bind(source_family)
    .bind(chain)
    .bind(format!("test/{chain}/{source_family}.toml"))
    .bind(payload)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_completed_coverage_fact(
    pool: &sqlx::PgPool,
    chain: &str,
    source_family: &str,
    address: &str,
) -> Result<()> {
    let job_id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO backfill_jobs (
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            status,
            completed_at
        )
        VALUES ('test', $1, '{}'::jsonb, 'test', 1, 10, $2, 'completed', now())
        RETURNING backfill_job_id
        "#,
    )
    .bind(chain)
    .bind(format!("chain-wide-cache-{source_family}"))
    .fetch_one(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO backfill_coverage_facts (
            backfill_job_id,
            chain_id,
            source_family,
            scope,
            address,
            covered_from_block,
            covered_to_block,
            derivation
        )
        VALUES ($1, $2, $3, 'address', $4, 1, 10, 'job_completion')
        "#,
    )
    .bind(job_id)
    .bind(chain)
    .bind(source_family)
    .bind(address)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn alternating_family_subsets_reuse_chain_wide_topic_authority() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("topic_evidence_chain_wide_cache"),
        &bigname_storage::MIGRATOR,
        "failed to migrate chain-wide coverage cache test",
    )
    .await?;
    let chain = "test-chain";
    let first_family = "first-family";
    let second_family = "second-family";
    let first_address = "0x0000000000000000000000000000000000000001";
    let second_address = "0x0000000000000000000000000000000000000002";
    insert_active_manifest(database.pool(), chain, first_family, "FirstChanged").await?;
    insert_active_manifest(database.pool(), chain, second_family, "SecondChanged").await?;
    insert_completed_coverage_fact(database.pool(), chain, first_family, first_address).await?;
    insert_completed_coverage_fact(database.pool(), chain, second_family, second_address).await?;

    let first_requirement = RequiredWatchedTuple {
        source_family: first_family.to_owned(),
        address: first_address.to_owned(),
        required_from_block: 1,
        required_to_block: 10,
    };
    let second_requirement = RequiredWatchedTuple {
        source_family: second_family.to_owned(),
        address: second_address.to_owned(),
        required_from_block: 1,
        required_to_block: 10,
    };
    let first_caller_subset = BTreeMap::from([(
        first_family.to_owned(),
        BTreeSet::from([format!("0x{:064x}", 1)]),
    )]);
    let second_caller_subset = BTreeMap::from([(
        second_family.to_owned(),
        BTreeSet::from([format!("0x{:064x}", 2)]),
    )]);

    let first_uncovered = find_uncovered_generation_bound_coverage_with_current_topics(
        database.pool(),
        chain,
        &first_caller_subset,
        &[first_requirement],
        0,
        20,
    )
    .await
    .map_err(anyhow::Error::msg)?;
    assert!(first_uncovered.is_empty());
    let rollup_ids_before = sqlx::query_scalar::<_, i64>(
        "SELECT full_closure_coverage_rollup_id FROM full_closure_coverage_rollups WHERE chain_id = $1 ORDER BY full_closure_coverage_rollup_id",
    )
    .bind(chain)
    .fetch_all(database.pool())
    .await?;

    let second_uncovered = find_uncovered_generation_bound_coverage_with_current_topics(
        database.pool(),
        chain,
        &second_caller_subset,
        &[second_requirement],
        0,
        20,
    )
    .await
    .map_err(anyhow::Error::msg)?;
    assert!(second_uncovered.is_empty());
    let rollup_ids_after = sqlx::query_scalar::<_, i64>(
        "SELECT full_closure_coverage_rollup_id FROM full_closure_coverage_rollups WHERE chain_id = $1 ORDER BY full_closure_coverage_rollup_id",
    )
    .bind(chain)
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        rollup_ids_after, rollup_ids_before,
        "changing only the caller's family subset must not cold-rebuild chain-wide rollups"
    );

    let saved_topics = sqlx::query_scalar::<_, Value>(
        "SELECT topic0s_by_family FROM full_closure_coverage_rollup_states WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(database.pool())
    .await?;
    assert!(saved_topics.get(first_family).is_some());
    assert!(saved_topics.get(second_family).is_some());
    database.cleanup().await
}
