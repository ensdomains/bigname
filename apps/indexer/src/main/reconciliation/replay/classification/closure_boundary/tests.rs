use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::*;
use crate::ens_v1_resolver::SOURCE_FAMILY_ENS_V1_RESOLVER_L1;
use crate::reconciliation::replay::classification::SOURCE_FAMILY_ENS_V2_REGISTRY_L1;

#[test]
fn legacy_registry_closure_has_generation_bound_coverage_strategy() {
    assert_eq!(
        retention_closure_authority_kind(ENS_V2_RETAINED_HISTORY_SOURCE_FAMILIES),
        RetentionClosureAuthorityKind::EnsV2Proof
    );
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

#[tokio::test]
async fn match_all_closure_boundary_includes_base_resolver_and_registry_created() -> Result<()> {
    let database = TestDatabase::create_migrated(
        TestDatabaseConfig::new("indexer_match_all_closure_boundary"),
        &bigname_storage::MIGRATOR,
        "failed to apply migrations for match-all closure-boundary test",
    )
    .await?;
    let cases = [
        (
            "base-mainnet",
            "basenames",
            "basenames_base_resolver",
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            "TextChanged(bytes32,string,string,string)",
            "RecordChanged",
            42,
        ),
        (
            "ethereum-sepolia",
            "ens",
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
            "RegistryCreated",
            "event RegistryCreated()",
            "RegistryCreated()",
            "RegistryCreated",
            43,
        ),
    ];
    for (
        chain,
        namespace,
        source_family,
        event_name,
        event_fragment,
        event_signature,
        normalized_event,
        block_number,
    ) in cases
    {
        insert_match_all_closure_case(
            database.pool(),
            chain,
            namespace,
            source_family,
            event_name,
            event_fragment,
            event_signature,
            normalized_event,
            block_number,
        )
        .await?;
        assert_eq!(
            earliest_required_raw_fact_block(
                database.pool(),
                chain,
                &[(
                    source_family.to_owned(),
                    GENERIC_SOURCE_SCOPE_ADDRESS.to_owned(),
                    block_number,
                    block_number,
                )],
                &[source_family],
            )
            .await?,
            Some(block_number),
            "{source_family} wildcard closure must include its unlisted emitter"
        );
    }

    database.cleanup().await
}

#[expect(clippy::too_many_arguments)]
async fn insert_match_all_closure_case(
    pool: &sqlx::PgPool,
    chain: &str,
    namespace: &str,
    source_family: &str,
    event_name: &str,
    event_fragment: &str,
    event_signature: &str,
    normalized_event: &str,
    block_number: i64,
) -> Result<()> {
    let payload = serde_json::json!({
        "abi": {
            "events": [{
                "name": event_name,
                "fragment": event_fragment,
                "normalized_events": [normalized_event],
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
        VALUES (1, $1, $2, $3, 'closure-test', 'active', 'test', $4, $5)
        "#,
    )
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .bind(format!("test/{source_family}/v1.toml"))
    .bind(payload)
    .execute(pool)
    .await?;
    let block_hash = format!("0x{block_number:064x}");
    sqlx::query(
        r#"
        INSERT INTO chain_lineage (
            chain_id,
            block_hash,
            block_number,
            block_timestamp,
            canonicality_state
        )
        VALUES ($1, $2, $3, now(), 'canonical')
        "#,
    )
    .bind(chain)
    .bind(&block_hash)
    .bind(block_number)
    .execute(pool)
    .await?;
    let topic0 = format!(
        "0x{}",
        alloy_primitives::hex::encode(alloy_primitives::keccak256(event_signature.as_bytes()))
    );
    sqlx::query(
        r#"
        INSERT INTO raw_logs (
            chain_id,
            block_hash,
            block_number,
            transaction_hash,
            transaction_index,
            log_index,
            emitting_address,
            topics,
            data,
            canonicality_state
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            0,
            0,
            '0x00000000000000000000000000000000000000ff',
            ARRAY[$5]::TEXT[],
            '\x'::BYTEA,
            'canonical'
        )
        "#,
    )
    .bind(chain)
    .bind(block_hash)
    .bind(block_number)
    .bind(format!("0x{:064x}", block_number + 1_000))
    .bind(topic0)
    .execute(pool)
    .await?;
    Ok(())
}
