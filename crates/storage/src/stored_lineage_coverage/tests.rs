use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::*;

async fn test_database(name: &str) -> Result<TestDatabase> {
    TestDatabase::create_migrated(
        TestDatabaseConfig::new(name),
        &crate::MIGRATOR,
        "failed to apply migrations for stored-lineage coverage test",
    )
    .await
}

fn publication(epoch: i64, from: i64, through: i64) -> StoredLineageCoverageFrontierPublication {
    StoredLineageCoverageFrontierPublication {
        discovery_admission_epoch: epoch,
        verified_from_block: from,
        verified_through_block: through,
        topic0s_by_family: BTreeMap::from([(
            "test_family".to_owned(),
            vec![format!("0x{:064x}", 1)],
        )]),
    }
}

async fn stage(
    guard: &mut StoredLineageCoveragePublicationGuard,
    address: &str,
    from: i64,
    through: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO pg_temp.stored_lineage_coverage_frontier_candidate_requirements (
            source_family,
            address,
            required_intervals
        )
        VALUES ('test_family', $1, int8multirange(int8range($2, $3 + 1, '[)')))
        "#,
    )
    .bind(address)
    .bind(from)
    .bind(through)
    .execute(guard.connection_mut())
    .await?;
    Ok(())
}

#[tokio::test]
async fn migration_is_cold_and_publication_replaces_requirements_atomically() -> Result<()> {
    let database = test_database("storage_stored_coverage_cold_replace").await?;
    let chain = "test-chain";
    assert_eq!(
        load_stored_lineage_coverage_frontier_header(database.pool(), chain).await?,
        None,
        "the migration must not seed a proof"
    );

    let mut first =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), chain, None, 0).await?;
    stage(
        &mut first,
        "0x0000000000000000000000000000000000000001",
        10,
        20,
    )
    .await?;
    assert_eq!(
        first.publish(&publication(0, 10, 20)).await?,
        StoredLineageCoveragePublicationOutcome::Published {
            snapshot_revision: 1
        }
    );

    let mut second =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), chain, Some(1), 0)
            .await?;
    stage(
        &mut second,
        "0x0000000000000000000000000000000000000002",
        15,
        18,
    )
    .await?;
    assert_eq!(
        second.publish(&publication(0, 10, 20)).await?,
        StoredLineageCoveragePublicationOutcome::Published {
            snapshot_revision: 2
        }
    );
    let requirements = sqlx::query_as::<_, (String, i64, i64)>(
        r#"
        SELECT address, lower(required_intervals), upper(required_intervals) - 1
        FROM stored_lineage_coverage_frontier_requirements
        WHERE chain_id = $1
        ORDER BY address
        "#,
    )
    .bind(chain)
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        requirements,
        vec![(
            "0x0000000000000000000000000000000000000002".to_owned(),
            15,
            18,
        )],
        "the second publication must atomically remove the old tuple and publish the shortened replacement"
    );

    database.cleanup().await
}

#[tokio::test]
async fn publication_rejects_stale_revision_and_epoch() -> Result<()> {
    let database = test_database("storage_stored_coverage_fences").await?;
    let chain = "test-chain";
    let mut first =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), chain, None, 0).await?;
    stage(
        &mut first,
        "0x0000000000000000000000000000000000000001",
        1,
        2,
    )
    .await?;
    first.publish(&publication(0, 1, 2)).await?;

    let mut stale =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), chain, None, 0).await?;
    stage(
        &mut stale,
        "0x0000000000000000000000000000000000000002",
        1,
        2,
    )
    .await?;
    assert_eq!(
        stale.publish(&publication(0, 1, 2)).await?,
        StoredLineageCoveragePublicationOutcome::Conflict
    );

    sqlx::query("UPDATE discovery_admission_epochs SET epoch = 1 WHERE chain_id = $1")
        .bind(chain)
        .execute(database.pool())
        .await?;
    let mut drifted =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), chain, Some(1), 0)
            .await?;
    stage(
        &mut drifted,
        "0x0000000000000000000000000000000000000001",
        1,
        2,
    )
    .await?;
    let error = drifted
        .publish(&publication(0, 1, 2))
        .await
        .expect_err("an epoch changed after optimistic proof must fail at publication");
    assert!(error.to_string().contains("changed from 0 to 1"));
    assert_eq!(
        load_stored_lineage_coverage_frontier_header(database.pool(), chain)
            .await?
            .expect("the first frontier remains published")
            .snapshot_revision,
        1,
        "epoch drift must roll back the replacement"
    );

    database.cleanup().await
}

#[tokio::test]
async fn repeatable_read_publication_maps_serialization_loss_to_cas_conflict() -> Result<()> {
    let database = test_database("storage_stored_coverage_serialization_conflict").await?;
    let chain = "test-chain";
    let mut first =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), chain, None, 0).await?;
    stage(
        &mut first,
        "0x0000000000000000000000000000000000000001",
        1,
        2,
    )
    .await?;
    first.publish(&publication(0, 1, 2)).await?;

    let mut loser =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), chain, Some(1), 0)
            .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SHOW transaction_isolation")
            .fetch_one(loser.connection_mut())
            .await?,
        "repeatable read"
    );
    let observed_revision = sqlx::query_scalar::<_, i64>(
        "SELECT snapshot_revision FROM stored_lineage_coverage_frontiers WHERE chain_id = $1",
    )
    .bind(chain)
    .fetch_one(loser.connection_mut())
    .await?;
    assert_eq!(observed_revision, 1);
    stage(
        &mut loser,
        "0x0000000000000000000000000000000000000001",
        1,
        3,
    )
    .await?;

    let mut winner =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), chain, Some(1), 0)
            .await?;
    stage(
        &mut winner,
        "0x0000000000000000000000000000000000000001",
        1,
        3,
    )
    .await?;
    assert_eq!(
        winner.publish(&publication(0, 1, 3)).await?,
        StoredLineageCoveragePublicationOutcome::Published {
            snapshot_revision: 2
        }
    );
    assert_eq!(
        loser.publish(&publication(0, 1, 3)).await?,
        StoredLineageCoveragePublicationOutcome::Conflict,
        "repeatable-read serialization loss must enter the bounded CAS retry path"
    );
    database.cleanup().await
}

#[tokio::test]
async fn malformed_current_format_metadata_is_loadable_but_ineligible() -> Result<()> {
    let database = test_database("storage_stored_coverage_malformed").await?;
    let chain = "test-chain";
    let mut guard =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), chain, None, 0).await?;
    stage(
        &mut guard,
        "0x0000000000000000000000000000000000000001",
        1,
        2,
    )
    .await?;
    guard.publish(&publication(0, 1, 2)).await?;
    sqlx::query(
        r#"
        UPDATE stored_lineage_coverage_frontiers
        SET topic0s_by_family = '{"test_family": []}'::JSONB
        WHERE chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;

    let header = load_stored_lineage_coverage_frontier_header(database.pool(), chain)
        .await?
        .expect("the malformed current-format header remains CAS-addressable");
    assert!(!header.is_well_formed);
    assert!(
        !stored_lineage_coverage_frontier_requirements_are_valid(database.pool(), &header).await?,
        "malformed metadata must force a cold candidate instead of saved interval subtraction"
    );

    database.cleanup().await
}

async fn publish_two_requirement_snapshot(database: &TestDatabase, chain: &str) -> Result<()> {
    let mut guard =
        begin_stored_lineage_coverage_frontier_publication(database.pool(), chain, None, 0).await?;
    for suffix in [1, 2] {
        stage(&mut guard, &format!("0x{suffix:040x}"), 10, 20).await?;
    }
    guard.publish(&publication(0, 10, 20)).await?;
    Ok(())
}

#[tokio::test]
async fn deleting_one_child_row_invalidates_saved_integrity() -> Result<()> {
    let database = test_database("storage_stored_coverage_deleted_child").await?;
    let chain = "test-chain";
    publish_two_requirement_snapshot(&database, chain).await?;
    sqlx::query(
        r#"
        DELETE FROM stored_lineage_coverage_frontier_requirements
        WHERE chain_id = $1
          AND address = '0x0000000000000000000000000000000000000002'
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    let header = load_stored_lineage_coverage_frontier_header(database.pool(), chain)
        .await?
        .expect("published header must remain");
    assert_eq!(header.requirement_row_count, 2);
    assert!(
        !stored_lineage_coverage_frontier_requirements_are_valid(database.pool(), &header).await?
    );
    database.cleanup().await
}

#[tokio::test]
async fn deleting_all_child_rows_invalidates_nonempty_saved_integrity() -> Result<()> {
    let database = test_database("storage_stored_coverage_deleted_all_children").await?;
    let chain = "test-chain";
    publish_two_requirement_snapshot(&database, chain).await?;
    sqlx::query("DELETE FROM stored_lineage_coverage_frontier_requirements WHERE chain_id = $1")
        .bind(chain)
        .execute(database.pool())
        .await?;
    let header = load_stored_lineage_coverage_frontier_header(database.pool(), chain)
        .await?
        .expect("published header must remain");
    assert!(
        !stored_lineage_coverage_frontier_requirements_are_valid(database.pool(), &header).await?
    );
    database.cleanup().await
}

#[tokio::test]
async fn bounded_same_count_child_tamper_invalidates_saved_fingerprint() -> Result<()> {
    let database = test_database("storage_stored_coverage_bounded_child_tamper").await?;
    let chain = "test-chain";
    publish_two_requirement_snapshot(&database, chain).await?;
    sqlx::query(
        r#"
        UPDATE stored_lineage_coverage_frontier_requirements
        SET required_intervals = int8multirange(int8range(11, 21, '[)'))
        WHERE chain_id = $1
          AND address = '0x0000000000000000000000000000000000000001'
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    let header = load_stored_lineage_coverage_frontier_header(database.pool(), chain)
        .await?
        .expect("published header must remain");
    assert_eq!(header.requirement_row_count, 2);
    assert!(
        !stored_lineage_coverage_frontier_requirements_are_valid(database.pool(), &header).await?,
        "bounded child tampering must be detected independently of row count"
    );
    database.cleanup().await
}

#[tokio::test]
async fn lower_infinite_child_is_rejected_even_with_matching_integrity_metadata() -> Result<()> {
    let database = test_database("storage_stored_coverage_lower_infinite_child").await?;
    let chain = "test-chain";
    publish_two_requirement_snapshot(&database, chain).await?;
    sqlx::query(
        "ALTER TABLE stored_lineage_coverage_frontier_requirements DROP CONSTRAINT stored_lineage_coverage_requirements_finite",
    )
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        UPDATE stored_lineage_coverage_frontier_requirements
        SET required_intervals = int8multirange(int8range(NULL, 21, '[)'))
        WHERE chain_id = $1
          AND address = '0x0000000000000000000000000000000000000001'
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    sqlx::query(
        r#"
        WITH row_hashes AS (
            SELECT md5(
                jsonb_build_array(source_family, address, required_intervals::TEXT)::TEXT
            ) AS row_hash
            FROM stored_lineage_coverage_frontier_requirements
            WHERE chain_id = $1
        ), integrity AS (
            SELECT
                COUNT(*)::BIGINT AS row_count,
                LPAD(to_hex(COALESCE(bit_xor(('x' || SUBSTRING(row_hash, 1, 16))::BIT(64)::BIGINT), 0)), 16, '0')
                || LPAD(to_hex(COALESCE(bit_xor(('x' || SUBSTRING(row_hash, 17, 16))::BIT(64)::BIGINT), 0)), 16, '0')
                    AS digest
            FROM row_hashes
        )
        UPDATE stored_lineage_coverage_frontiers header
        SET requirement_row_count = integrity.row_count,
            requirement_digest = integrity.digest
        FROM integrity
        WHERE header.chain_id = $1
        "#,
    )
    .bind(chain)
    .execute(database.pool())
    .await?;
    let header = load_stored_lineage_coverage_frontier_header(database.pool(), chain)
        .await?
        .expect("published header must remain");
    assert!(header.is_well_formed);
    assert!(
        !stored_lineage_coverage_frontier_requirements_are_valid(database.pool(), &header).await?,
        "matching count and fingerprint must not admit a lower-infinite child range"
    );

    let mut replacement = begin_stored_lineage_coverage_frontier_publication(
        database.pool(),
        chain,
        Some(header.snapshot_revision),
        0,
    )
    .await?;
    stage(
        &mut replacement,
        "0x0000000000000000000000000000000000000001",
        10,
        20,
    )
    .await?;
    assert_eq!(
        replacement.publish(&publication(0, 10, 20)).await?,
        StoredLineageCoveragePublicationOutcome::Published {
            snapshot_revision: 2
        }
    );
    let replaced = load_stored_lineage_coverage_frontier_header(database.pool(), chain)
        .await?
        .expect("replacement header must exist");
    assert!(
        stored_lineage_coverage_frontier_requirements_are_valid(database.pool(), &replaced).await?
    );
    database.cleanup().await
}
