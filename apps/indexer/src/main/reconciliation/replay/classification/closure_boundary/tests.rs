use bigname_test_support::{TestDatabase, TestDatabaseConfig};

use super::*;
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
