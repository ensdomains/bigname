//! Coverage is derived from retained authority inputs, never a stored serving summary.
#[path = "../../project/tests/families_support/mod.rs"]
mod families_support;

use anyhow::{Result, ensure};
use bigname_project::families::FamilyMode;
use bigname_storage::{
    PermissionCoverageExhaustiveness, PermissionCoverageStatus,
    PermissionCoverageUnsupportedReason, load_serving_permission_summaries,
};
use families_support::Fixture;
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
async fn permission_coverage_follows_retained_authority_in_single_and_batch_reads() -> Result<()> {
    let fixture = Fixture::new("permission_summaries", 2).await?;
    let registrar = Uuid::from_u128(1);
    let wrapper = Uuid::from_u128(2);
    let unknown = Uuid::from_u128(3);
    fixture.resource(&unknown.to_string()).await?;
    for (index, resource, kind, family) in [
        (0, registrar, "registrar", "ens_v1_registrar_l1"),
        (1, wrapper, "wrapper", "ens_v1_wrapper_l1"),
    ] {
        fixture
            .write(
                1,
                index,
                "AuthorityEpochChanged",
                family,
                None,
                Some(&resource.to_string()),
                json!({"authority_kind":kind}),
                "0x0000000000000000000000000000000000000a11",
            )
            .await?;
    }
    let outcome = fixture.apply(2, FamilyMode::Rebuild).await?;
    ensure!(outcome.marker.as_ref().map(|marker| marker.number) == Some(2));
    let batch =
        load_serving_permission_summaries(&fixture.pool, &[registrar, wrapper, unknown]).await?;
    for (resource, reason) in [
        (
            registrar,
            PermissionCoverageUnsupportedReason::OperatorApprovalSurfacesNotIngested,
        ),
        (
            wrapper,
            PermissionCoverageUnsupportedReason::WrapperParentAndResolverDelegationNotProjected,
        ),
        (
            unknown,
            PermissionCoverageUnsupportedReason::ResourcePermissionAuthorityNotProjected,
        ),
    ] {
        let single = load_serving_permission_summaries(&fixture.pool, &[resource]).await?;
        let coverage = &single[&resource].coverage;
        assert_eq!(coverage, &batch[&resource].coverage);
        assert_eq!(coverage.status(), PermissionCoverageStatus::Partial);
        assert_eq!(
            coverage.exhaustiveness(),
            PermissionCoverageExhaustiveness::BestEffort
        );
        assert_eq!(coverage.unsupported_reason(), Some(reason));
    }
    fixture.cleanup().await
}
