use alloy_primitives::Address;
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::types::{Uuid, time::OffsetDateTime};

use super::support;
use crate::harness::{
    anvil::{self, Anvil},
    ens_v2, repo_root,
};

const YEAR: u64 = 365 * 24 * 60 * 60;

fn pointer(body: &Value, path: &str) -> Value {
    body.pointer(path).cloned().unwrap_or(Value::Null)
}

fn data_array(body: &Value) -> Vec<Value> {
    body.pointer("/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

async fn exact_name(run: &support::PipelineRun, name: &str) -> Result<Value> {
    let (status, body) = run.api.get_json(&format!("/v1/names/ens/{name}")).await?;
    assert_eq!(
        status, 200,
        "ENSv2 exact-name lookup for {name} failed: {body}"
    );
    Ok(body)
}

async fn children(run: &support::PipelineRun, name: &str) -> Result<Value> {
    let (status, body) = run
        .api
        .get_json(&format!(
            "/v1/names/ens/{name}/children?surface_classes=declared&include=counts&view=full"
        ))
        .await?;
    assert_eq!(
        status, 200,
        "ENSv2 declared children lookup for {name} failed: {body}"
    );
    Ok(body)
}

async fn permissions(run: &support::PipelineRun, resource_id: Uuid) -> Result<Vec<Value>> {
    let (status, body) = run
        .api
        .get_json(&format!("/v1/resources/{resource_id}/permissions"))
        .await?;
    assert_eq!(
        status, 200,
        "ENSv2 permissions lookup for {resource_id} failed: {body}"
    );
    Ok(data_array(&body))
}

fn resource_id_from_exact_name(body: &Value) -> Result<Uuid> {
    let value = pointer(body, "/data/resource_id");
    let raw = value
        .as_str()
        .context("exact-name body missing /data/resource_id")?;
    raw.parse()
        .with_context(|| format!("exact-name resource_id is not a UUID: {raw}; body: {body}"))
}

fn assert_child_absent(body: &Value, logical_name_id: &str) {
    let data = data_array(body);
    assert!(
        data.iter()
            .all(|row| pointer(row, "/logical_name_id") != logical_name_id),
        "did not expect child {logical_name_id} in children response after subregistry swap; body: {body}"
    );
}

/// ENSv2 post-audit Sepolia matrix over admitted families only:
/// - registration through ETHRegistrar with exact-name coverage,
/// - token regeneration from EAC role mutation while preserving resource identity,
/// - registry role grant and revoke vocabulary,
/// - subregistry attach and swap,
/// - unregister followed by re-register as a new resource lineage.
#[tokio::test]
async fn ens_v2_sepolia_post_audit_declared_matrix_end_to_end() -> Result<()> {
    let sepolia = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = sepolia.client();
    let repo_root = repo_root();

    let deployment = ens_v2::deploy_ens_v2(&rpc, &repo_root).await?;
    let accounts = rpc.accounts().await?;
    let alice = accounts[1];
    let bob = accounts[2];
    let carol = accounts[3];
    let alice_path = format!("{alice:#x}");
    let bob_path = format!("{bob:#x}");
    let carol_path = format!("{carol:#x}");

    let alice_registration = ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "alice",
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;

    let roles_registration = ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "roles",
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    let role_bitmap = ens_v2::role_bit(ens_v2::ROLE_SET_RESOLVER);
    let roles_label = ens_v2::label_id("roles");
    ens_v2::grant_roles(
        &rpc,
        deployment.eth_registry.address,
        alice,
        roles_label,
        role_bitmap,
        bob,
    )
    .await?;
    ens_v2::revoke_roles(
        &rpc,
        deployment.eth_registry.address,
        alice,
        roles_label,
        role_bitmap,
        bob,
    )
    .await?;
    ens_v2::grant_roles(
        &rpc,
        deployment.eth_registry.address,
        alice,
        roles_label,
        role_bitmap,
        carol,
    )
    .await?;
    let roles_resource_after_regen =
        ens_v2::resource_id(&rpc, deployment.eth_registry.address, roles_label).await?;
    assert_eq!(
        roles_resource_after_regen, roles_registration.resource_id,
        "TokenRegenerated should preserve the on-chain resource id for roles.eth"
    );

    let tree_registration = ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "tree",
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    let tree_label = ens_v2::label_id("tree");
    let child_a = ens_v2::deploy_child_registry(&rpc, &repo_root, &deployment).await?;
    ens_v2::attach_subregistry(
        &rpc,
        deployment.eth_registry.address,
        alice,
        tree_label,
        child_a.address,
    )
    .await?;
    ens_v2::set_parent(
        &rpc,
        child_a.address,
        deployment.deployer,
        deployment.eth_registry.address,
        "tree",
    )
    .await?;
    ens_v2::register_in_registry(
        &rpc,
        child_a.address,
        deployment.deployer,
        "leaf",
        alice,
        anvil::GENESIS_TIMESTAMP + 5 * YEAR,
    )
    .await?;

    // children_current is a worker projection and cannot gate intake
    // readiness; gate on the derivable registry events instead.
    let first_ready_sql = "SELECT EXISTS (
       SELECT 1 FROM normalized_events
       WHERE logical_name_id = 'ens:tree.eth'
         AND event_kind = 'SubregistryChanged'
         AND canonicality_state IN ('canonical', 'safe', 'finalized')
    )";
    let first_run =
        support::ingest_ens_v2_sepolia_and_serve(&sepolia, &deployment, Some(first_ready_sql))
            .await?;
    let child_a_logs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM raw_logs WHERE emitting_address = $1")
            .bind(format!("{:#x}", child_a.address))
            .fetch_one(&first_run.db.pool)
            .await?;
    let leaf_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events WHERE logical_name_id = 'ens:leaf.tree.eth'",
    )
    .fetch_one(&first_run.db.pool)
    .await?;
    let first_children = children(&first_run, "tree.eth").await?;
    // REVIEW POINT (pinned observed behavior): discovery admits the
    // subregistry edge, but the discovered child registry's own logs are
    // never fetched within the discovering session — registrations inside
    // discovered subregistries derive nothing live and need a later
    // backfill/ops-catchup. Recorded in the ledger.
    assert_eq!(
        child_a_logs, 0,
        "pinned: discovered child-registry logs are not scanned in-session"
    );
    assert_eq!(
        leaf_events, 0,
        "pinned: registrations inside discovered subregistries derive nothing live"
    );
    assert_child_absent(&first_children, "ens:leaf.tree.eth");
    first_run.db.cleanup().await?;

    let child_b = ens_v2::deploy_child_registry(&rpc, &repo_root, &deployment).await?;
    ens_v2::attach_subregistry(
        &rpc,
        deployment.eth_registry.address,
        alice,
        tree_label,
        child_b.address,
    )
    .await?;
    ens_v2::set_parent(
        &rpc,
        child_b.address,
        deployment.deployer,
        deployment.eth_registry.address,
        "tree",
    )
    .await?;
    ens_v2::register_in_registry(
        &rpc,
        child_b.address,
        deployment.deployer,
        "newleaf",
        alice,
        anvil::GENESIS_TIMESTAMP + 5 * YEAR,
    )
    .await?;

    let cycle_first = ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "cycle",
            owner: alice,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    ens_v2::unregister(
        &rpc,
        deployment.eth_registry.address,
        deployment.deployer,
        ens_v2::label_id("cycle"),
    )
    .await?;
    // The post-audit registrar keeps an unregistered label unavailable until
    // its grace period has elapsed.
    // (upstream: .refs/ens_v2/contracts/src/registrar/ETHRegistrar.sol:L259 @ ens_v2@48b3e2d).
    rpc.increase_time(ens_v2::GRACE_PERIOD + 1).await?;
    let cycle_second = ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: bob,
            label: "cycle",
            owner: bob,
            duration_secs: YEAR,
            subregistry: Address::ZERO,
            resolver: Address::ZERO,
        },
    )
    .await?;
    assert_ne!(
        cycle_first.resource_id, cycle_second.resource_id,
        "unregister followed by re-register should advance the resource id"
    );
    assert_ne!(
        cycle_first.token_id, cycle_second.token_id,
        "unregister followed by re-register should advance the token lineage id"
    );

    let final_ready_sql = format!(
        "SELECT
           EXISTS (
             SELECT 1 FROM raw_logs
             WHERE emitting_address = '{registrar:#x}'
               AND block_number = {alice_register_block}
           )
           AND EXISTS (
             SELECT 1 FROM normalized_events
             WHERE logical_name_id = 'ens:alice.eth'
               AND event_kind = 'RegistrationGranted'
               AND canonicality_state IN ('canonical', 'safe', 'finalized')
           )
           AND (
             SELECT count(*) >= 2 FROM normalized_events
             WHERE logical_name_id = 'ens:roles.eth'
               AND event_kind = 'TokenRegenerated'
               AND canonicality_state IN ('canonical', 'safe', 'finalized')
           )
           AND EXISTS (
             SELECT 1 FROM normalized_events
             WHERE event_kind = 'PermissionChanged'
               AND source_family = 'ens_v2_registry_l1'
               AND canonicality_state IN ('canonical', 'safe', 'finalized')
               AND after_state->>'subject' = '{carol}'
               AND after_state->'effective_powers' ? 'set_resolver'
           )
           AND EXISTS (
             SELECT 1 FROM normalized_events
             WHERE logical_name_id = 'ens:tree.eth'
               AND event_kind = 'SubregistryChanged'
               AND canonicality_state IN ('canonical', 'safe', 'finalized')
           )
           AND (
             SELECT count(DISTINCT resource_id) = 2 FROM normalized_events
             WHERE logical_name_id = 'ens:cycle.eth'
               AND event_kind = 'RegistrationGranted'
               AND canonicality_state IN ('canonical', 'safe', 'finalized')
           )
",
        registrar = deployment.eth_registrar.address,
        alice_register_block = alice_registration.register_block,
        carol = carol_path,
    );
    let run =
        support::ingest_ens_v2_sepolia_and_serve(&sepolia, &deployment, Some(&final_ready_sql))
            .await?;

    let alice_body = exact_name(&run, "alice.eth").await?;
    assert_eq!(
        pointer(&alice_body, "/data/logical_name_id"),
        "ens:alice.eth",
        "alice.eth logical name mismatch; body: {alice_body}"
    );
    assert_eq!(
        pointer(&alice_body, "/data/namespace"),
        "ens",
        "alice.eth namespace mismatch; body: {alice_body}"
    );
    assert_eq!(
        pointer(&alice_body, "/coverage/status"),
        "full",
        "freshly registered sepolia names should use the promoted exact-name profile; body: {alice_body}"
    );
    assert_eq!(
        pointer(&alice_body, "/coverage/exhaustiveness"),
        "authoritative",
        "promoted exact-name coverage should be authoritative; body: {alice_body}"
    );
    assert_eq!(
        pointer(&alice_body, "/coverage/enumeration_basis"),
        "exact_name_profile",
        "promoted exact-name coverage should identify its enumeration basis; body: {alice_body}"
    );
    assert!(
        alice_body
            .pointer("/coverage/unsupported_reason")
            .is_some_and(serde_json::Value::is_null),
        "promoted exact-name coverage should not carry an unsupported reason; body: {alice_body}"
    );
    assert_eq!(
        pointer(&alice_body, "/declared_state/registration/status"),
        "active",
        "alice.eth registration should be active; body: {alice_body}"
    );
    assert_eq!(
        pointer(&alice_body, "/declared_state/registration/registrant"),
        alice_path,
        "alice.eth registrant should match the registrar owner; body: {alice_body}"
    );
    assert_eq!(
        pointer(&alice_body, "/chain_positions/ethereum-sepolia/chain_id"),
        "ethereum-sepolia",
        "alice.eth chain position should be under ethereum-sepolia; body: {alice_body}"
    );

    let roles_body = exact_name(&run, "roles.eth").await?;
    let roles_resource_id = resource_id_from_exact_name(&roles_body)?;
    let distinct_role_resources: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT resource_id)
         FROM normalized_events
         WHERE logical_name_id = 'ens:roles.eth'
           AND event_kind = 'TokenRegenerated'
           AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    assert_eq!(
        distinct_role_resources, 1,
        "TokenRegenerated events for roles.eth should stay on one resource id"
    );
    let role_permission_rows = permissions(&run, roles_resource_id).await?;
    assert!(
        role_permission_rows.iter().any(|row| {
            pointer(row, "/subject") == carol_path
                && pointer(row, "/effective_powers")
                    .as_array()
                    .is_some_and(|powers| powers.iter().any(|power| power == "set_resolver"))
        }),
        "Carol should retain set_resolver on roles.eth; rows: {role_permission_rows:?}"
    );
    assert!(
        role_permission_rows
            .iter()
            .all(|row| pointer(row, "/subject") != bob_path),
        "Bob's revoked roles.eth permission should not remain current; rows: {role_permission_rows:?}"
    );

    let tree_body = exact_name(&run, "tree.eth").await?;
    assert_eq!(
        pointer(&tree_body, "/data/logical_name_id"),
        "ens:tree.eth",
        "tree.eth exact-name route should remain available after subregistry swap; body: {tree_body}"
    );
    let final_children = children(&run, "tree.eth").await?;
    eprintln!("PROBE final tree children: {final_children:?}");
    assert_child_absent(&final_children, "ens:leaf.tree.eth");

    let tree_resource_after_swap =
        ens_v2::resource_id(&rpc, deployment.eth_registry.address, tree_label).await?;
    assert_eq!(
        tree_resource_after_swap, tree_registration.resource_id,
        "setSubregistry swap should not change the parent resource id"
    );

    let cycle_body = exact_name(&run, "cycle.eth").await?;
    assert_eq!(
        pointer(&cycle_body, "/declared_state/registration/registrant"),
        bob_path,
        "re-registered cycle.eth should serve the successor owner; body: {cycle_body}"
    );
    let cycle_resource_id = resource_id_from_exact_name(&cycle_body)?;
    let cycle_bindings: Vec<(Uuid, OffsetDateTime, Option<OffsetDateTime>)> = sqlx::query_as(
        "SELECT resource_id, active_from, active_to FROM surface_bindings \
         WHERE logical_name_id = 'ens:cycle.eth' \
           AND canonicality_state IN ('canonical', 'safe', 'finalized') \
         ORDER BY active_from, surface_binding_id",
    )
    .fetch_all(&run.db.pool)
    .await?;
    assert_eq!(
        cycle_bindings.len(),
        2,
        "cycle.eth should retain both resource bindings"
    );
    assert!(
        cycle_bindings[0]
            .2
            .is_some_and(|closed_at| closed_at <= cycle_bindings[1].1)
    );
    assert_eq!(cycle_bindings[1].2, None);
    assert_eq!(cycle_resource_id, cycle_bindings[1].0);

    run.db.cleanup().await?;
    Ok(())
}
