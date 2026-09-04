use alloy_primitives::{Address, B256};
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use uuid::Uuid;

use super::support;
use crate::harness::{anvil::Anvil, ens_v1, repo_root};

const NAME: &str = "operatorlife.eth";
const YEAR: u64 = 365 * 24 * 60 * 60;

fn operator_rows(body: &Value) -> Vec<&Value> {
    body["data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|row| row.get("grant_relation").and_then(Value::as_str) == Some("operator"))
        .collect()
}

async fn resource_id(run: &support::PipelineRun) -> Result<Uuid> {
    sqlx::query_scalar("SELECT resource_id FROM name_current WHERE raw_name=$1")
        .bind(NAME)
        .fetch_one(&run.db.pool)
        .await
        .context("current operatorlife resource")
}

async fn assert_operator(
    run: &support::PipelineRun,
    owner: Address,
    operator: Address,
    expected: bool,
) -> Result<()> {
    let operator_hex = format!("{operator:#x}");
    let resource = resource_id(run).await?;
    let storage = bigname_storage::load_effective_permissions_account_resource_page(
        &run.db.pool,
        Some(&operator_hex),
        Some(resource),
        None,
        10,
    )
    .await?;
    assert_eq!(
        storage
            .rows
            .iter()
            .any(|row| row.grant_relation
                == Some(bigname_storage::PermissionGrantRelation::Operator)),
        expected
    );

    for uri in [
        format!("/v2/permissions?address={operator_hex}"),
        format!("/v2/permissions?name={NAME}"),
        format!("/v2/permissions?registration_id={resource}"),
    ] {
        let (status, body) = run.api.get_json(&uri).await?;
        ensure!(status == 200, "{uri} failed: {body}");
        assert_eq!(!operator_rows(&body).is_empty(), expected, "{uri}: {body}");
        if expected {
            let row = operator_rows(&body)[0];
            assert_eq!(row["grant_scope"]["kind"], "account");
            assert_eq!(row["powers"], serde_json::json!(["registry_control"]));
        }
    }
    let (status, body) = run
        .api
        .get_json(&format!(
            "/v2/addresses/{owner:#x}/names?include=role_summary"
        ))
        .await?;
    ensure!(status == 200, "role summary failed: {body}");
    let found = body["data"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| row["role_summary"].as_array())
        .flatten()
        .filter_map(|role| role["grants"].as_array())
        .flatten()
        .any(|grant| grant.get("grant_relation") == Some(&serde_json::json!("operator")));
    assert_eq!(found, expected, "role summary: {body}");
    Ok(())
}

#[tokio::test]
async fn registry_operator_approval_serving_lifecycle() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let owner = accounts[1];
    let operator = accounts[2];

    ens_v1::create_legacy_subname(
        &rpc,
        &deployment,
        deployment.deployer,
        B256::ZERO,
        "eth",
        deployment.deployer,
    )
    .await?;
    ens_v1::create_legacy_subname(
        &rpc,
        &deployment,
        deployment.deployer,
        ens_v1::namehash("eth"),
        "operatorlife",
        owner,
    )
    .await?;
    ens_v1::set_legacy_registry_approval_for_all(&rpc, &deployment, owner, operator, true).await?;
    let grant_run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some("SELECT EXISTS (SELECT 1 FROM account_permission_state_current WHERE approved)"),
    )
    .await?;
    assert_operator(&grant_run, owner, operator, true).await?;
    grant_run.db.cleanup().await?;

    ens_v1::set_legacy_registry_approval_for_all(&rpc, &deployment, owner, operator, false).await?;
    let revoke_run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some("SELECT EXISTS (SELECT 1 FROM account_permission_state_current WHERE NOT approved)"),
    )
    .await?;
    assert_operator(&revoke_run, owner, operator, false).await?;
    revoke_run.db.cleanup().await?;

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "operatorlife",
        owner,
        YEAR,
        deployment.public_resolver.address,
    )
    .await?;
    let move_run = support::ingest_and_serve(&anvil, &deployment, None).await?;
    assert_operator(&move_run, owner, operator, false).await?;
    move_run.db.cleanup().await?;

    ens_v1::set_registry_approval_for_all(&rpc, &deployment, owner, operator, true).await?;
    let new_run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some("SELECT count(*) >= 2 FROM account_permission_state_current"),
    )
    .await?;
    assert_operator(&new_run, owner, operator, true).await?;
    new_run.db.cleanup().await
}
