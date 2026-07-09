use alloy_primitives::Address;
use anyhow::{Context, Result};
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L33 @ ens_v1@91c966f)
const MIN_REGISTRATION: u64 = 28 * 24 * 60 * 60;
// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
const GRACE_PERIOD: u64 = 90 * 24 * 60 * 60;

/// Zero-resolver registration follows the controller's direct registrar
/// branch: no registry resolver binding is written while the lease itself is
/// otherwise active
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L287 @ ens_v1@91c966f).
#[tokio::test]
async fn register_without_resolver_keeps_declared_resolver_empty() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let alice = rpc.accounts().await?[1];

    ens_v1::register_eth_name(&rpc, &deployment, "erin", alice, YEAR, Address::ZERO).await?;

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:erin.eth' AND event_kind = 'RegistrationGranted' \
             AND canonicality_state = 'canonical')",
        ),
    )
    .await?;

    let (status, body) = run.api.get_json("/v1/names/ens/erin.eth").await?;
    assert_eq!(status, 200, "exact-name lookup failed: {body}");
    let pointer = |path: &str| -> Value { body.pointer(path).cloned().unwrap_or(Value::Null) };
    assert_eq!(pointer("/data/normalized_name"), "erin.eth");
    assert_eq!(pointer("/coverage/status"), "full");
    assert_eq!(pointer("/coverage/exhaustiveness"), "authoritative");
    assert_eq!(pointer("/declared_state/registration/status"), "active");
    assert_eq!(
        pointer("/declared_state/registration/registrant"),
        format!("{alice:#x}"),
        "registrant should be the registering account"
    );
    assert_eq!(
        pointer("/declared_state/control/registry_owner"),
        format!("{alice:#x}"),
        "registry owner should still be populated from registrar registry update"
    );
    assert_eq!(
        pointer("/declared_state/resolver/address"),
        Value::Null,
        "zero-resolver registration should not declare a resolver; body: {body}"
    );
    assert_eq!(
        pointer("/declared_state/resolver/chain_id"),
        Value::Null,
        "zero-resolver registration should use the supported no-resolver shape"
    );

    run.db.cleanup().await?;
    Ok(())
}

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

/// Past expiry plus the upstream grace period, with no re-registration of
/// the name itself. Two contractual facts, pinned separately across two
/// ingests of the same chain:
///
/// 1. On a chain with no activity at all after the grace end, the release
///    never settles: release settlement runs at the full-closure boundary of
///    an authority sync round, and rounds are driven by log-bearing blocks —
///    empty blocks past grace end advance nothing. The registration stays
///    last-known (`active`, past expiry), same as the in-grace read.
/// 2. Any later admitted activity (here: an unrelated registration) gives
///    the next sync round a boundary past `expiry + GRACE_PERIOD`; the
///    release then materializes anchored to the first indexed block after
///    grace end, exact-name flips to `released`, and the name leaves the
///    current registrant collection while history is retained.
#[tokio::test]
async fn expire_without_reregistration_releases_and_unlists_registration() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);
    let resolver = deployment.public_resolver.address;

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "frank",
        alice,
        MIN_REGISTRATION,
        resolver,
    )
    .await?;
    rpc.increase_time(MIN_REGISTRATION + GRACE_PERIOD + 24 * 60 * 60)
        .await?;

    let quiet = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:frank.eth' AND event_kind = 'RegistrationGranted' \
             AND canonicality_state = 'canonical')",
        ),
    )
    .await?;
    let released_on_quiet_chain: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM normalized_events \
         WHERE logical_name_id = 'ens:frank.eth' AND event_kind = 'RegistrationReleased')",
    )
    .fetch_one(&quiet.db.pool)
    .await?;
    assert!(
        !released_on_quiet_chain,
        "release must not settle without a post-grace log-bearing boundary; \
         if this now settles, update this scenario and the ledger"
    );
    quiet.db.cleanup().await?;

    // Unrelated post-grace activity gives the next authority sync round a
    // boundary past the grace end.
    ens_v1::register_eth_name(&rpc, &deployment, "george", bob, MIN_REGISTRATION, resolver).await?;

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:frank.eth' AND event_kind = 'RegistrationReleased' \
             AND canonicality_state = 'canonical')",
        ),
    )
    .await?;

    let (status, body) = run.api.get_json("/v1/names/ens/frank.eth").await?;
    assert_eq!(status, 200, "exact-name lookup failed: {body}");
    let pointer = |path: &str| -> Value { body.pointer(path).cloned().unwrap_or(Value::Null) };
    assert_eq!(
        pointer("/declared_state/registration/status"),
        "released",
        "post-grace registration should be released; body: {body}"
    );
    assert_eq!(
        pointer("/declared_state/registration/latest_event_kind"),
        "RegistrationReleased"
    );
    assert_eq!(
        pointer("/declared_state/registration/registrant"),
        format!("{alice:#x}"),
        "released summary should retain the last registrant"
    );
    let expiry = pointer("/declared_state/registration/expiry")
        .as_i64()
        .context("released registration expiry missing")?;
    let released_at = pointer("/declared_state/registration/released_at")
        .as_i64()
        .context("released_at missing from released registration")?;
    let event_released_at: i64 = sqlx::query_scalar(
        "SELECT (after_state->>'released_at')::BIGINT FROM normalized_events \
         WHERE logical_name_id = 'ens:frank.eth' AND event_kind = 'RegistrationReleased' \
         AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        released_at, event_released_at,
        "exact-name released_at should come from the canonical release event"
    );
    assert!(
        released_at >= expiry + GRACE_PERIOD as i64,
        "released_at {released_at} should be at or after expiry {expiry} plus grace"
    );

    let address_path = format!("/v1/addresses/{alice:#x}/names?namespace=ens&relation=registrant");
    let (status, address_names) = run.api.get_json(&address_path).await?;
    assert_eq!(status, 200, "address names lookup failed: {address_names}");
    let address_entries = address_names
        .pointer("/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        address_entries
            .iter()
            .all(|entry| entry.get("normalized_name").and_then(Value::as_str) != Some("frank.eth")),
        "released name must not appear in the current registrant collection: {address_names}"
    );

    run.db.cleanup().await?;
    Ok(())
}
