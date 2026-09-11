use anyhow::{Context, Result};
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

/// Walking skeleton: deploy the pinned ENSv1 stack onto a local chain,
/// register alice.eth through the real registrar controller, execute the
/// phase-runner fixture spine, and assert persisted raw logs, normalized
/// events, and the schema-v2 current-name projection.
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

    // --- phase-runner fixture spine ---
    let ready_sql = support::canonical_event_ready_sql(
        "ens:0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec",
        "RegistrationGranted",
        None,
    );
    let run = support::ingest_and_serve(&anvil, &deployment, Some(&ready_sql)).await?;
    let logical_name_id = support::schema_v2_logical_name_id(
        "ens:0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec",
    );

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
    // (upstream: .refs/ens_v1/contracts/registry/ENS.sol:L12 @ ens_v1@91c966f).
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
         WHERE (logical_name_id = $1 OR namespace || ':' || lower(COALESCE(
             after_state->>'namehash', after_state->>'child_node', after_state->>'node')) = $1)
           AND canonicality_state = 'canonical'",
    )
    .bind(&logical_name_id)
    .fetch_all(&run.db.pool)
    .await?;
    for expected in [
        "RegistrationGranted",
        "PreimageObserved",
        "ExpiryChanged",
        "AuthorityEpochChanged",
    ] {
        assert!(
            event_kinds.iter().any(|kind| kind == expected),
            "expected canonical {expected} normalized event for ens:0x787192fc5378cc32aa956ddfdedbf26b24e8d78e40109add0eea2c1a012c3dec; saw {event_kinds:?}"
        );
    }

    // --- layer 3: schema-v2 projection ---
    let (projected_id, raw_name, binding_kind, declared_summary, support_status): (
        String,
        String,
        Option<String>,
        Value,
        String,
    ) = sqlx::query_as(
        "SELECT logical_name_id, raw_name, binding_kind, declared_summary, support_status
         FROM name_current WHERE namespace = 'ens' AND raw_name = 'alice.eth'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(projected_id, logical_name_id);
    assert_eq!(raw_name, "alice.eth");
    assert_eq!(binding_kind.as_deref(), Some("declared_registry_path"));
    assert_eq!(support_status, "supported");
    let pointer = |path: &str| crate::harness::responses::pointer(&declared_summary, path);
    assert_eq!(pointer("/coverage/status"), "projected");
    assert_eq!(pointer("/coverage/exhaustiveness"), "not_asserted");
    assert_eq!(pointer("/registration/status"), "active");
    assert_eq!(
        pointer("/registration/registrant"),
        format!("{user:#x}"),
        "registrant should be the registering account"
    );
    let expiry = pointer("/registration/expiry")
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
