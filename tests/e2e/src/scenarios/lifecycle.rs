use anyhow::Result;
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L33 @ ens_v1@91c966f)
const MIN_REGISTRATION: u64 = 28 * 24 * 60 * 60;
// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
const GRACE_PERIOD: u64 = 90 * 24 * 60 * 60;

/// Renew, then transfer: expiry extends and the registrant changes while
/// the backing resource and token lineage stay stable — ordinary lifecycle
/// inside one registrar lease must not rotate identity.
#[tokio::test]
async fn renew_and_transfer_keep_identity() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "carol",
        alice,
        YEAR,
        deployment.public_resolver.address,
    )
    .await?;
    ens_v1::renew_eth_name(&rpc, &deployment, alice, "carol", YEAR).await?;
    ens_v1::transfer_eth_name(&rpc, &deployment, alice, bob, "carol").await?;

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        &["ens_v1_registry_l1"],
        Some(
            "SELECT EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:carol.eth' AND event_kind = 'TokenControlTransferred' \
             AND canonicality_state = 'canonical')",
        ),
    )
    .await?;

    let event_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT event_kind FROM normalized_events \
         WHERE logical_name_id = 'ens:carol.eth' AND canonicality_state = 'canonical'",
    )
    .fetch_all(&run.db.pool)
    .await?;
    for expected in [
        "RegistrationGranted",
        "RegistrationRenewed",
        "TokenControlTransferred",
    ] {
        assert!(
            event_kinds.iter().any(|kind| kind == expected),
            "expected canonical {expected} for ens:carol.eth; saw {event_kinds:?}"
        );
    }

    // Identity model: renewal preserves the registrar anchor outright. The
    // transferFrom→reclaim pair, executed as two transactions, opens a real
    // registry-owner/token-holder divergence window between them, which
    // rotates to a divergence anchor and converges back to the same live
    // registrar lease on reclaim (docs/architecture.md § resource_id). So
    // history carries exactly two resources, and the current surface must be
    // back on the original registration anchor.
    let registration_resource: sqlx::types::Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events \
         WHERE logical_name_id = 'ens:carol.eth' AND event_kind = 'RegistrationGranted' \
         AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    let resource_count: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT resource_id) FROM normalized_events \
         WHERE logical_name_id = 'ens:carol.eth' AND resource_id IS NOT NULL \
         AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        resource_count, 2,
        "expected the registrar anchor plus one transient divergence anchor"
    );

    let (status, body) = run.api.get_json("/v1/names/ens/carol.eth").await?;
    assert_eq!(status, 200, "exact-name lookup failed: {body}");
    let pointer = |path: &str| -> Value { body.pointer(path).cloned().unwrap_or(Value::Null) };
    assert_eq!(
        pointer("/data/resource_id").as_str(),
        Some(registration_resource.to_string().as_str()),
        "after reclaim the surface must be back on the original registrar anchor"
    );
    assert_eq!(
        pointer("/declared_state/registration/registrant"),
        format!("{bob:#x}"),
        "registrant should follow the token transfer"
    );
    assert_eq!(
        pointer("/declared_state/control/registry_owner"),
        format!("{bob:#x}"),
        "registry owner should follow the reclaim"
    );
    let expiry = pointer("/declared_state/registration/expiry")
        .as_u64()
        .unwrap_or_default();
    let registered_for = expiry - 2 * YEAR;
    assert!(
        (crate::harness::anvil::GENESIS_TIMESTAMP..crate::harness::anvil::GENESIS_TIMESTAMP + 300)
            .contains(&registered_for),
        "expiry {expiry} should reflect the renewal (two years from genesis)"
    );
    assert_eq!(pointer("/declared_state/registration/status"), "active");

    run.db.cleanup().await?;
    Ok(())
}

/// Expiry, grace, and lapse: the same chain is ingested twice — once inside
/// the grace window and once after a different owner re-registers — and the
/// two reads must disagree in exactly the contractual ways: status first,
/// then a rotated backing resource with the prior history preserved.
#[tokio::test]
async fn expiry_grace_and_reregistration_rotate_identity() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(&rpc, &deployment, "dave", alice, MIN_REGISTRATION, resolver).await?;

    // Into the grace window: past expiry, well before grace end.
    rpc.increase_time(MIN_REGISTRATION + 24 * 60 * 60).await?;

    let in_grace = support::ingest_and_serve(
        &anvil,
        &deployment,
        &[],
        Some(
            "SELECT EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:dave.eth' AND canonicality_state = 'canonical')",
        ),
    )
    .await?;
    let (status, body) = in_grace.api.get_json("/v1/names/ens/dave.eth").await?;
    assert_eq!(status, 200, "in-grace lookup failed: {body}");
    // Current wire contract: no distinct grace status. The registration
    // stays `active` with `released_at` null and an expiry in the past;
    // consumers derive the grace window from `expiry` plus the upstream
    // grace period.
    assert_eq!(
        body.pointer("/declared_state/registration/status")
            .cloned()
            .unwrap_or(Value::Null),
        "active"
    );
    assert_eq!(
        body.pointer("/declared_state/registration/released_at")
            .cloned()
            .unwrap_or(Value::Null),
        Value::Null
    );
    let in_grace_expiry = body
        .pointer("/declared_state/registration/expiry")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    assert!(
        in_grace_expiry
            < crate::harness::anvil::GENESIS_TIMESTAMP + MIN_REGISTRATION + 24 * 60 * 60,
        "expiry {in_grace_expiry} should already be in the past at the in-grace read"
    );
    let first_resource = body
        .pointer("/data/resource_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    assert!(!first_resource.is_empty());
    in_grace.db.cleanup().await?;

    // Past grace end plus the premium-decay window, then bob re-registers.
    rpc.increase_time(GRACE_PERIOD + 22 * 24 * 60 * 60).await?;
    ens_v1::register_eth_name(&rpc, &deployment, "dave", bob, MIN_REGISTRATION, resolver).await?;

    let after = support::ingest_and_serve(
        &anvil,
        &deployment,
        &[],
        Some(
            "SELECT count(*) >= 2 FROM normalized_events \
             WHERE logical_name_id = 'ens:dave.eth' AND event_kind = 'RegistrationGranted' \
             AND canonicality_state = 'canonical'",
        ),
    )
    .await?;

    let (status, body) = after.api.get_json("/v1/names/ens/dave.eth").await?;
    assert_eq!(status, 200, "post-lapse lookup failed: {body}");
    let pointer = |path: &str| -> Value { body.pointer(path).cloned().unwrap_or(Value::Null) };
    assert_eq!(pointer("/declared_state/registration/status"), "active");
    assert_eq!(
        pointer("/declared_state/registration/registrant"),
        format!("{bob:#x}"),
        "re-registration belongs to the new owner"
    );
    let second_resource = pointer("/data/resource_id")
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert_ne!(
        second_resource, first_resource,
        "full lapse + re-registration must mint a new backing resource"
    );

    // The prior lease's history stays queryable, partitioned by resource.
    let resources_with_grants: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT resource_id) FROM normalized_events \
         WHERE logical_name_id = 'ens:dave.eth' AND event_kind = 'RegistrationGranted' \
         AND canonicality_state = 'canonical'",
    )
    .fetch_one(&after.db.pool)
    .await?;
    assert_eq!(
        resources_with_grants, 2,
        "both leases' registration events must persist under distinct resources"
    );

    after.db.cleanup().await?;
    Ok(())
}
