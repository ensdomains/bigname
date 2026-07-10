use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::types::Uuid;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;

fn pointer(body: &Value, path: &str) -> Value {
    body.pointer(path).cloned().unwrap_or(Value::Null)
}

async fn address_names(
    run: &support::PipelineRun,
    address: &str,
    relation: &str,
) -> Result<Vec<Value>> {
    let (status, body) = run
        .api
        .get_json(&format!(
            "/v1/addresses/{address}/names?namespace=ens&relation={relation}"
        ))
        .await?;
    assert_eq!(
        status, 200,
        "address names lookup for {address} relation={relation} failed: {body}"
    );
    Ok(body
        .pointer("/data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default())
}

/// Transferring the registrar token without the separate reclaim call leaves
/// registry ownership behind
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172 @ ens_v1@91c966f).
#[tokio::test]
async fn transfer_without_reclaim_keeps_registry_owner_divergent() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();

    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "divergent",
        alice,
        YEAR,
        deployment.public_resolver.address,
    )
    .await?;
    ens_v1::transfer_eth_name_without_reclaim(&rpc, &deployment, alice, bob, "divergent").await?;

    let run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some(
            "SELECT EXISTS (SELECT 1 FROM normalized_events \
             WHERE logical_name_id = 'ens:divergent.eth' \
             AND event_kind = 'AuthorityEpochChanged' \
             AND after_state->>'authority_kind' = 'registry_only' \
             AND canonicality_state = 'canonical')",
        ),
    )
    .await?;

    let event_kinds: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT event_kind FROM normalized_events \
         WHERE logical_name_id = 'ens:divergent.eth' \
         AND canonicality_state = 'canonical'",
    )
    .fetch_all(&run.db.pool)
    .await?;
    for expected in [
        "RegistrationGranted",
        "TokenControlTransferred",
        "AuthorityEpochChanged",
    ] {
        assert!(
            event_kinds.iter().any(|kind| kind == expected),
            "expected {expected} for divergent.eth; saw {event_kinds:?}"
        );
    }

    let registrar_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events \
         WHERE logical_name_id = 'ens:divergent.eth' \
         AND event_kind = 'RegistrationGranted' \
         AND canonicality_state = 'canonical'",
    )
    .fetch_one(&run.db.pool)
    .await?;
    let (current_resource, current_lineage, authority_kind): (Uuid, Option<Uuid>, String) =
        sqlx::query_as(
            "SELECT binding.resource_id, resource.token_lineage_id, \
                    resource.provenance->>'authority_kind' \
             FROM surface_bindings binding \
             JOIN resources resource USING (resource_id) \
             WHERE binding.logical_name_id = 'ens:divergent.eth' \
             AND binding.active_to IS NULL \
             AND binding.canonicality_state = 'canonical' \
             AND resource.canonicality_state = 'canonical' \
             ORDER BY binding.active_from DESC LIMIT 1",
        )
        .fetch_one(&run.db.pool)
        .await?;
    assert_ne!(
        current_resource, registrar_resource,
        "the divergent state must bind to a distinct registry-only resource"
    );
    assert_eq!(current_lineage, None);
    assert_eq!(authority_kind, "registry_only");

    let (status, body) = run.api.get_json("/v1/names/ens/divergent.eth").await?;
    assert_eq!(status, 200, "exact-name lookup failed: {body}");
    assert_eq!(
        pointer(&body, "/data/resource_id"),
        current_resource.to_string()
    );
    assert_eq!(pointer(&body, "/data/token_lineage_id"), Value::Null);
    assert_eq!(
        pointer(&body, "/data/binding_kind"),
        "declared_registry_path"
    );
    assert_eq!(pointer(&body, "/coverage/status"), "full");
    assert_eq!(pointer(&body, "/coverage/exhaustiveness"), "authoritative");
    assert_eq!(
        pointer(&body, "/declared_state/registration/status"),
        "active"
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/authority_kind"),
        "registry_only"
    );
    assert_eq!(
        pointer(&body, "/declared_state/registration/registrant"),
        format!("{bob:#x}")
    );
    assert_eq!(
        pointer(&body, "/declared_state/control/registry_owner"),
        format!("{alice:#x}")
    );

    let alice_names = address_names(&run, &format!("{alice:#x}"), "effective_controller").await?;
    assert_eq!(alice_names.len(), 1, "old holder rows: {alice_names:?}");
    assert_eq!(
        alice_names[0].get("normalized_name"),
        Some(&json!("divergent.eth"))
    );
    assert_eq!(
        alice_names[0].get("relation_facets"),
        Some(&json!(["effective_controller"]))
    );

    for relation in ["registrant", "token_holder"] {
        let alice_names = address_names(&run, &format!("{alice:#x}"), relation).await?;
        assert!(
            alice_names.is_empty(),
            "registry-only binding must omit the old holder from relation={relation}: \
             {alice_names:?}"
        );
    }
    for relation in ["registrant", "token_holder"] {
        let bob_names = address_names(&run, &format!("{bob:#x}"), relation).await?;
        assert!(
            bob_names.is_empty(),
            "current registry-only binding omits the new token holder from relation={relation}: \
             {bob_names:?}"
        );
    }
    let bob_controller = address_names(&run, &format!("{bob:#x}"), "effective_controller").await?;
    assert!(
        bob_controller.is_empty(),
        "new token holder must not become the registry-only effective controller: \
         {bob_controller:?}"
    );

    run.db
        .cleanup()
        .await
        .context("clean up divergence database")?;
    Ok(())
}
