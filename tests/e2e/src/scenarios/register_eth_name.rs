use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::support;
use crate::harness::{anvil::Anvil, db::HarnessDb, ens_v1, manifests, pipeline, repo_root};

/// Walking skeleton: deploy the pinned ENSv1 stack onto a local chain,
/// register alice.eth through the real registrar controller, ingest the
/// chain with the real indexer, replay projections with the real worker,
/// and assert at three layers — persisted raw logs, normalized events, and
/// public API output. Verified-resolution (execution-trace) coverage is
/// deliberately out of scope for this scenario: no execution RPC is
/// configured. Registry-driven declared state (resolver binding, registry
/// owner, children) is asserted in the registry_driven_reads scenario.
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

/// Production-loop smoke: keep `indexer run`, `worker run`, and the API live
/// while a registration and a later renewal land. This covers the automatic
/// projection bootstrap handoff and the continuous invalidation/apply path;
/// the broader matrix intentionally keeps using deterministic one-shot replay.
#[tokio::test]
async fn live_worker_applies_registration_and_renewal_while_api_serves() -> Result<()> {
    const LABEL: &str = "liveworker";
    const LOGICAL_NAME_ID: &str = "ens:liveworker.eth";

    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let root = repo_root();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &root).await?;
    rpc.mine(2).await?;

    let scratch = support::TempDir::create()?;
    let profile =
        manifests::generate_local_profile(scratch.path(), &root, &deployment.manifest_targets())?;
    let db = HarnessDb::create().await?;
    let mut indexer = pipeline::IndexerRunSession::start(
        &root,
        &db.url,
        &profile.root,
        &anvil.url,
        "live-worker",
    )
    .await?;
    indexer.wait_for_first_checkpoint(&db.pool).await?;

    let mut worker = pipeline::WorkerRunSession::start(&root, &db.url, "live-worker").await?;
    worker
        .wait_for_sql(
            &db.pool,
            "SELECT EXISTS (SELECT 1 FROM projection_apply_cursors \
             WHERE cursor_name = 'normalized_events_to_projection_invalidations')",
        )
        .await?;
    let api = pipeline::ApiServer::start(&root, &db.url).await?;

    let user = rpc.accounts().await?[1];
    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        LABEL,
        user,
        365 * 24 * 60 * 60,
        deployment.public_resolver.address,
    )
    .await?;
    rpc.mine(2).await?;
    let registration_head = rpc.block_number().await?;
    indexer
        .wait_for_checkpoint(
            &db.pool,
            registration_head,
            Some(
                "SELECT EXISTS (SELECT 1 FROM normalized_events \
                 WHERE logical_name_id = 'ens:liveworker.eth' \
                 AND event_kind = 'RegistrationGranted' \
                 AND canonicality_state = 'canonical')",
            ),
        )
        .await?;
    worker
        .wait_for_sql(
            &db.pool,
            "SELECT EXISTS (SELECT 1 FROM name_current \
             WHERE logical_name_id = 'ens:liveworker.eth' \
             AND coverage->>'status' = 'full')",
        )
        .await?;

    let (status, first_body) = api.get_json("/v1/names/ens/liveworker.eth").await?;
    assert_eq!(status, 200, "live registration did not serve: {first_body}");
    let first_expiry = first_body
        .pointer("/declared_state/registration/expiry")
        .and_then(Value::as_u64)
        .context("live registration expiry missing")?;

    ens_v1::renew_eth_name(&rpc, &deployment, user, LABEL, 24 * 60 * 60).await?;
    rpc.mine(2).await?;
    let renewal_head = rpc.block_number().await?;
    indexer
        .wait_for_checkpoint(
            &db.pool,
            renewal_head,
            Some(
                "SELECT EXISTS (SELECT 1 FROM normalized_events \
                 WHERE logical_name_id = 'ens:liveworker.eth' \
                 AND event_kind = 'RegistrationRenewed' \
                 AND canonicality_state = 'canonical')",
            ),
        )
        .await?;
    worker
        .wait_for_sql(
            &db.pool,
            &format!(
                "SELECT EXISTS (SELECT 1 FROM name_current \
                 WHERE logical_name_id = '{LOGICAL_NAME_ID}' \
                 AND (declared_summary #>> '{{registration,expiry}}')::BIGINT > {first_expiry})"
            ),
        )
        .await?;

    let (status, renewed_body) = api.get_json("/v1/names/ens/liveworker.eth").await?;
    assert_eq!(status, 200, "live renewal did not serve: {renewed_body}");
    let renewed_expiry = renewed_body
        .pointer("/declared_state/registration/expiry")
        .and_then(Value::as_u64)
        .context("live renewal expiry missing")?;
    assert!(
        renewed_expiry > first_expiry,
        "continuous worker apply did not advance expiry: {renewed_body}"
    );

    let (status, identity_body) = api
        .post_json(
            "/v1/identity:lookup",
            &json!({
                "profile": "feed",
                "namespace": "public",
                "inputs": [{
                    "id": "live-name",
                    "kind": "name",
                    "name": "LiveWorker.eth"
                }]
            }),
        )
        .await?;
    assert_eq!(
        status, 200,
        "native identity lookup failed: {identity_body}"
    );
    assert_eq!(
        identity_body.pointer("/results/0/status"),
        Some(&Value::String("success".to_owned())),
        "native identity lookup did not find the live projection: {identity_body}"
    );
    assert_eq!(
        identity_body.pointer("/results/0/record/name"),
        Some(&Value::String("liveworker.eth".to_owned())),
        "native identity lookup returned the wrong record: {identity_body}"
    );

    worker
        .wait_for_sql(
            &db.pool,
            "SELECT
                 COALESCE((
                     SELECT last_change_id
                     FROM projection_apply_cursors
                     WHERE cursor_name = 'normalized_events_to_projection_invalidations'
                 ), 0) >= COALESCE((
                     SELECT MAX(change_id)
                     FROM projection_normalized_event_changes
                 ), 0)
                 AND NOT EXISTS (SELECT 1 FROM projection_invalidations)",
        )
        .await?;
    let ready_timeout_secs = std::env::var("BIGNAME_E2E_READY_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(600);
    let ready_deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(ready_timeout_secs);
    let status_body = loop {
        let (status, body) = api.get_json("/v1/status").await?;
        if status == 200 && body.pointer("/data/status") == Some(&Value::String("ready".to_owned()))
        {
            break body;
        }
        if std::time::Instant::now() > ready_deadline {
            anyhow::bail!(
                "projection status did not become ready within {ready_timeout_secs}s: {body}"
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    };
    assert_eq!(
        status_body.pointer("/data/status"),
        Some(&Value::String("ready".to_owned())),
        "live worker should leave projection status ready: {status_body}"
    );

    worker.stop().await?;
    indexer.stop().await?;
    drop(api);
    db.cleanup().await?;
    drop(scratch);
    Ok(())
}
