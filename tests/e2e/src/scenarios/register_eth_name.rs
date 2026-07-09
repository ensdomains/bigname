use anyhow::{Context, Result};
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

/// Walking skeleton: deploy the pinned ENSv1 stack onto a local chain,
/// register alice.eth through the real registrar controller, ingest the
/// chain with the real indexer, replay projections with the real worker,
/// and assert at three layers — persisted raw logs, normalized events, and
/// public API output. Verified-resolution (execution-trace) coverage is
/// deliberately out of scope for this scenario: no execution RPC is
/// configured. The manifest profile mirrors the shipped mainnet rollout
/// exactly, so registry-driven facts (declared resolver binding, registry
/// owner) stay absent — see the registry_driven_reads scenario for the
/// activated variant.
#[tokio::test]
async fn register_eth_name_end_to_end() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    // --- on-chain scenario ---
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let user = rpc.accounts().await?[1];
    let registered = ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "alice",
        user,
        365 * 24 * 60 * 60,
        deployment.public_resolver.address,
    )
    .await?;

    // --- pipeline: live intake -> projections -> API ---
    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        &[],
        Some(
            "SELECT EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:alice.eth' AND canonicality_state = 'canonical')",
        ),
    )
    .await?;

    // --- layer 1: raw facts ---
    // The controller emits label-bearing NameRegistered at the register block
    // (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L116 @ ens_v1@91c966f).
    let controller_logs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM raw_logs WHERE emitting_address = $1 AND block_number = $2",
    )
    .bind(format!("{:#x}", deployment.controller.address))
    .bind(registered.register_block as i64)
    .fetch_one(&run.db.pool)
    .await?;
    assert!(
        controller_logs >= 1,
        "expected controller logs persisted at register block {}",
        registered.register_block
    );

    let alice_node = format!("{:#x}", ens_v1::namehash("alice.eth"));
    let registry_topic0s: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT topics[1] FROM raw_logs WHERE emitting_address = $1 AND topics[2] = $2",
    )
    .bind(format!("{:#x}", deployment.registry.address))
    .bind(&alice_node)
    .fetch_all(&run.db.pool)
    .await?;
    // NewResolver(bytes32,address) — the register call carried a resolver, so
    // the registry must have observed the binding on-chain
    // (upstream: .refs/ens_v1/contracts/registry/ENS.sol @ ens_v1@91c966f).
    let new_resolver_topic0 = format!(
        "{:#x}",
        alloy_primitives::keccak256("NewResolver(bytes32,address)")
    );
    assert!(
        registry_topic0s.contains(&new_resolver_topic0),
        "expected registry NewResolver raw log for alice.eth node; saw {registry_topic0s:?}"
    );

    // --- layer 2: normalized events ---
    let event_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT event_kind FROM normalized_events
         WHERE logical_name_id = 'ens:alice.eth' AND canonicality_state = 'canonical'",
    )
    .fetch_all(&run.db.pool)
    .await?;
    for expected in [
        "RegistrationGranted",
        "SurfaceBound",
        "ExpiryChanged",
        "AuthorityEpochChanged",
    ] {
        assert!(
            event_kinds.iter().any(|kind| kind == expected),
            "expected canonical {expected} normalized event for ens:alice.eth; saw {event_kinds:?}"
        );
    }

    // --- layer 4: public API output ---
    let (status, body) = run.api.get_json("/v1/names/ens/alice.eth").await?;
    assert_eq!(status, 200, "exact-name lookup failed: {body}");
    let pointer = |path: &str| -> Value { body.pointer(path).cloned().unwrap_or(Value::Null) };
    assert_eq!(
        pointer("/data/normalized_name"),
        "alice.eth",
        "body: {body}"
    );
    assert_eq!(pointer("/data/logical_name_id"), "ens:alice.eth");
    assert_eq!(pointer("/data/binding_kind"), "declared_registry_path");
    assert_eq!(pointer("/coverage/status"), "full");
    assert_eq!(pointer("/coverage/exhaustiveness"), "authoritative");
    assert_eq!(pointer("/declared_state/registration/status"), "active");
    assert_eq!(
        pointer("/declared_state/registration/registrant"),
        format!("{user:#x}"),
        "registrant should be the registering account"
    );
    // Shipped-profile pin: the mainnet profile leaves the registry family
    // as an inactive seed, so declared resolver state stays absent even
    // though the NewResolver raw log is persisted (asserted above). The
    // registry_driven_reads scenario covers the activated variant.
    assert_eq!(pointer("/declared_state/resolver/address"), Value::Null);
    assert_eq!(pointer("/declared_state/resolver/chain_id"), Value::Null);
    assert_eq!(
        pointer("/declared_state/control/registry_owner"),
        Value::Null
    );

    let expiry = pointer("/declared_state/registration/expiry")
        .as_u64()
        .context("registration expiry missing")?;
    let registered_for = expiry - 365 * 24 * 60 * 60;
    assert!(
        (crate::harness::anvil::GENESIS_TIMESTAMP..crate::harness::anvil::GENESIS_TIMESTAMP + 300)
            .contains(&registered_for),
        "expiry {expiry} should be ~duration past the warped genesis timestamp"
    );

    run.db.cleanup().await?;
    Ok(())
}
