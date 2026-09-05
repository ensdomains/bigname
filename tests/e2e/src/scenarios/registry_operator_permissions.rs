use alloy_primitives::Address;
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::types::Uuid;

use super::support;
use crate::harness::{anvil::Anvil, artifacts::Deployed, ens_v1, repo_root};

const NAME: &str = "operatorlife.eth";
const API: &str = "bigname-api";

struct RealApi {
    _child: tokio::process::Child,
    base: String,
}

impl RealApi {
    async fn start(run: &support::PipelineRun, anvil: &Anvil) -> Result<Self> {
        let root = repo_root();
        let output = std::process::Command::new("cargo")
            .current_dir(&root)
            .args(["build", "--locked", "--message-format=json", "-p", API])
            .output()?;
        ensure!(output.status.success(), "bigname-api build failed");
        let executable = serde_json::Deserializer::from_slice(&output.stdout)
            .into_iter::<Value>()
            .filter_map(Result::ok)
            .find(|message| message["target"]["name"] == API)
            .and_then(|message| message["executable"].as_str().map(std::path::PathBuf::from))
            .context("Cargo did not report the bigname-api executable")?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        drop(listener);
        let mut command = tokio::process::Command::new(executable);
        command
            .args([
                "serve",
                "--bind-addr",
                &address.to_string(),
                "--metrics-bind-addr",
                "127.0.0.1:0",
                "--database-url",
                &run.db.url,
                "--chain-rpc-url",
                &format!("ethereum-mainnet={}", anvil.url),
            ])
            .kill_on_drop(true);
        let child = command.spawn()?;
        let base = format!("http://{address}");
        for _ in 0..200 {
            if reqwest::get(format!("{base}/healthz")).await.is_ok() {
                return Ok(Self {
                    _child: child,
                    base,
                });
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        anyhow::bail!("production API did not bind at {address}")
    }

    async fn get_json(&self, path: &str) -> Result<(u16, Value)> {
        let response = reqwest::get(format!("{}{path}", self.base)).await?;
        Ok((response.status().as_u16(), response.json().await?))
    }
}

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
    anvil: &Anvil,
    owner: Address,
    operator: Address,
    expected: bool,
) -> Result<()> {
    let api = RealApi::start(run, anvil).await?;
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
    let found = storage
        .rows
        .iter()
        .any(|row| row.grant_relation == Some(bigname_storage::PermissionGrantRelation::Operator));
    assert_eq!(found, expected);

    for uri in [
        format!("/v2/permissions?address={operator_hex}"),
        format!("/v2/permissions?name={NAME}"),
        format!("/v2/permissions?registration_id={resource}"),
    ] {
        let (status, body) = api.get_json(&uri).await?;
        ensure!(status == 200, "{uri} failed: {body}");
        assert_eq!(!operator_rows(&body).is_empty(), expected, "{uri}: {body}");
        if expected {
            let row = operator_rows(&body)[0];
            assert_eq!(row["grant_scope"]["kind"], "account");
            assert_eq!(row["powers"], serde_json::json!(["registry_control"]));
        }
    }
    let (status, body) = api
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

    ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "operatorlife",
        owner,
        365 * 24 * 60 * 60,
        deployment.public_resolver.address,
    )
    .await?;
    ens_v1::set_registry_approval_for_all(&rpc, &deployment, owner, operator, true).await?;
    let grant_run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some("SELECT EXISTS (SELECT 1 FROM account_permission_state_current WHERE approved)"),
    )
    .await?;
    assert_operator(&grant_run, &anvil, owner, operator, true).await?;
    grant_run.db.cleanup().await?;

    ens_v1::set_registry_approval_for_all(&rpc, &deployment, owner, operator, false).await?;
    let revoke_run = support::ingest_and_serve(
        &anvil,
        &deployment,
        Some("SELECT EXISTS (SELECT 1 FROM account_permission_state_current WHERE NOT approved)"),
    )
    .await?;
    assert_operator(&revoke_run, &anvil, owner, operator, false).await?;
    revoke_run.db.cleanup().await?;

    ens_v1::set_registry_approval_for_all(&rpc, &deployment, owner, operator, true).await?;
    let mut next = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    next.legacy_registry = Deployed {
        address: deployment.registry.address,
        block_number: deployment.registry.block_number,
    };
    ens_v1::register_eth_name(
        &rpc,
        &next,
        "operatorlife",
        owner,
        365 * 24 * 60 * 60,
        next.public_resolver.address,
    )
    .await?;
    let move_run = support::ingest_and_serve(&anvil, &next, None).await?;
    // Manifest reconciliation retires account state for the demoted registry emitter.
    let old_registry_state: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM account_permission_state_current WHERE authority_contract = $1)",
    )
    .bind(format!("{:#x}", deployment.registry.address))
    .fetch_one(&move_run.db.pool)
    .await?;
    assert!(
        !old_registry_state,
        "old registry account state was retained"
    );
    assert_operator(&move_run, &anvil, owner, operator, false).await?;
    move_run.db.cleanup().await?;

    ens_v1::set_registry_approval_for_all(&rpc, &next, owner, operator, true).await?;
    let new_run = support::ingest_and_serve(
        &anvil,
        &next,
        Some("SELECT EXISTS (SELECT 1 FROM account_permission_state_current WHERE approved)"),
    )
    .await?;
    assert_operator(&new_run, &anvil, owner, operator, true).await?;
    new_run.db.cleanup().await
}
