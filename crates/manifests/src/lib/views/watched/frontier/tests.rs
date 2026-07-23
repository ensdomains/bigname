use std::collections::BTreeMap;

use anyhow::Result;
use bigname_storage::{
    StoredLineageCoverageFrontierPublication, StoredLineageCoveragePublicationGuard,
    begin_stored_lineage_coverage_frontier_publication,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::*;

const CHAIN: &str = "frontier-test-chain";
const FAMILY: &str = "frontier_test_family";
const ADDRESS_ONE: &str = "0x0000000000000000000000000000000000000001";
const ADDRESS_TWO: &str = "0x0000000000000000000000000000000000000002";

async fn database(name: &str) -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new(name),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for manifest frontier test",
    )
    .await
}

fn publication() -> StoredLineageCoverageFrontierPublication {
    StoredLineageCoverageFrontierPublication {
        discovery_admission_epoch: 0,
        verified_from_block: 10,
        verified_through_block: 40,
        topic0s_by_family: BTreeMap::from([(FAMILY.to_owned(), vec![format!("0x{:064x}", 1)])]),
    }
}

async fn stage(
    guard: &mut StoredLineageCoveragePublicationGuard,
    rows: &[(&str, i64, i64)],
) -> Result<()> {
    for (address, from, through) in rows {
        sqlx::query(&format!(
            r#"
                INSERT INTO pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE} (
                    source_family,
                    address,
                    required_intervals
                )
                VALUES ($1, $2, int8multirange(int8range($3, $4 + 1, '[)')))
                "#,
        ))
        .bind(FAMILY)
        .bind(address)
        .bind(from)
        .bind(through)
        .execute(guard.connection_mut())
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn server_delta_handles_addition_removal_readmission_and_topic_change() -> Result<()> {
    let database = database("manifest_frontier_delta").await?;
    let mut first =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), CHAIN, None, 0).await?;
    stage(&mut first, &[(ADDRESS_ONE, 10, 20)]).await?;
    first.publish(&publication()).await?;

    let mut replacement =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), CHAIN, Some(1), 0)
            .await?;
    stage(
        &mut replacement,
        &[(ADDRESS_ONE, 10, 15), (ADDRESS_TWO, 30, 40)],
    )
    .await?;
    let delta = load_stored_lineage_coverage_candidate_delta_page(
        replacement.connection_mut(),
        CHAIN,
        &[],
        false,
        None,
        32,
    )
    .await?;
    assert_eq!(
        delta.requirements,
        vec![RequiredWatchedTuple {
            source_family: FAMILY.to_owned(),
            address: ADDRESS_TWO.to_owned(),
            required_from_block: 30,
            required_to_block: 40,
        }],
        "a shortening/removal needs no fact read while an addition does"
    );
    replacement.publish(&publication()).await?;

    let mut readmission =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), CHAIN, Some(2), 0)
            .await?;
    stage(
        &mut readmission,
        &[(ADDRESS_ONE, 10, 20), (ADDRESS_TWO, 30, 40)],
    )
    .await?;
    let delta = load_stored_lineage_coverage_candidate_delta_page(
        readmission.connection_mut(),
        CHAIN,
        &[],
        false,
        None,
        32,
    )
    .await?;
    assert_eq!(
        delta.requirements,
        vec![RequiredWatchedTuple {
            source_family: FAMILY.to_owned(),
            address: ADDRESS_ONE.to_owned(),
            required_from_block: 16,
            required_to_block: 20,
        }],
        "readmission verifies only the interval absent from the saved replacement"
    );
    let topic_delta = load_stored_lineage_coverage_candidate_delta_page(
        readmission.connection_mut(),
        CHAIN,
        &[FAMILY.to_owned()],
        false,
        None,
        32,
    )
    .await?;
    assert_eq!(topic_delta.requirements.len(), 2);
    assert_eq!(topic_delta.requirements[0].required_from_block, 10);
    assert_eq!(topic_delta.requirements[0].required_to_block, 20);
    drop(readmission);

    database.cleanup().await
}

#[tokio::test]
async fn high_cardinality_candidate_returns_only_bounded_delta_pages() -> Result<()> {
    let database = database("manifest_frontier_bounded_delta").await?;
    let mut guard =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), CHAIN, None, 0).await?;
    sqlx::query(&format!(
        r#"
            INSERT INTO pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE} (
                source_family,
                address,
                required_intervals
            )
            SELECT
                $1,
                '0x' || lpad(to_hex(candidate), 40, '0'),
                int8multirange(int8range(10, 21, '[)'))
            FROM generate_series(1, 10000) candidate
            "#,
    ))
    .bind(FAMILY)
    .execute(guard.connection_mut())
    .await?;

    let first = load_stored_lineage_coverage_candidate_delta_page(
        guard.connection_mut(),
        CHAIN,
        &[],
        true,
        None,
        37,
    )
    .await?;
    assert_eq!(first.requirements.len(), 37);
    let second = load_stored_lineage_coverage_candidate_delta_page(
        guard.connection_mut(),
        CHAIN,
        &[],
        true,
        first.next_cursor.as_ref(),
        37,
    )
    .await?;
    assert_eq!(second.requirements.len(), 37);
    assert_ne!(first.requirements, second.requirements);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*)::BIGINT FROM pg_temp.{STORED_LINEAGE_COVERAGE_CANDIDATE_TABLE}"
        ))
        .fetch_one(guard.connection_mut())
        .await?,
        10_000,
        "the full candidate stays server-side while each returned delta is bounded"
    );
    drop(guard);

    database.cleanup().await
}
