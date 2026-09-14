use alloy_primitives::{Address, U256};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sqlx::types::Uuid;

use super::support;
use crate::harness::{anvil::Anvil, artifacts::Deployed, basenames, ens_v1, repo_root};

const NAME: &str = "operatorlife.eth";
const API: &str = "bigname-api";

struct RealApi {
    _child: tokio::process::Child,
    base: String,
}

impl RealApi {
    async fn start(run: &support::PipelineRun, anvil: &Anvil) -> Result<Self> {
        Self::start_for_chain(run, "ethereum-mainnet", anvil).await
    }

    async fn start_for_chain(
        run: &support::PipelineRun,
        chain: &str,
        anvil: &Anvil,
    ) -> Result<Self> {
        let root = repo_root();
        let output = std::process::Command::new("cargo")
            .current_dir(&root)
            .args(["build", "--locked", "--message-format=json", "-p", API])
            .output()?;
        ensure!(output.status.success(), "bigname-api build failed");
        let executable = serde_json::Deserializer::from_slice(&output.stdout)
            .into_iter::<Value>()
            .filter_map(Result::ok)
            .filter(|message| message["reason"] == "compiler-artifact")
            .find(|message| message["target"]["name"] == API)
            .and_then(|message| message["executable"].as_str().map(std::path::PathBuf::from))
            .context("Cargo did not report the bigname-api executable")?;
        let lock = crate::harness::lock_local_server_start().await;
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
                &format!("{chain}={}", anvil.url),
            ])
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let base = format!("http://{address}");
        for _ in 0..400 {
            if let Some(status) = child.try_wait()? {
                anyhow::bail!("API exited {status}");
            }
            if reqwest::get(format!("{base}/healthz")).await.is_ok() {
                drop(lock);
                return Ok(Self {
                    _child: child,
                    base,
                });
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        anyhow::bail!("production API did not bind at {address}")
    }

    async fn get(&self, path: &str) -> Result<Value> {
        let (status, body) = self.get_json(path).await?;
        ensure!(status == 200, "{path}: {status} {body}");
        Ok(body)
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
    new_run.db.cleanup().await?;
    drop(anvil);
    let ens = tokio::spawn(author_ens_real_producer_and_http()).await;
    let basenames = tokio::spawn(author_basenames_real_producer_and_http()).await;
    ensure!(
        matches!(&ens, Ok(Ok(()))) && matches!(&basenames, Ok(Ok(()))),
        "ENS: {ens:?}; Basenames: {basenames:?}"
    );
    Ok(())
}

// Registry calls use the pinned owner/operator authorization contract.
// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L20 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L112-L117 @ ens_v1@91c966f)
// (upstream: .refs/basenames/src/L2/Registry.sol:L49-L52 @ basenames@1809bbc)
// (upstream: .refs/basenames/src/L2/Registry.sol:L155-L157 @ basenames@1809bbc)
sol! {
    function setApprovalForAll(address operator, bool approved) external;
    function setOwner(bytes32 node, address owner) external;
    function owner(bytes32 node) external view returns (address);
    function isApprovedForAll(address owner, address operator) external view returns (bool);
}

fn has_operator(body: &Value) -> bool {
    body["data"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v["grant_relation"] == "operator")
}

async fn verify_snapshot(
    run: &support::PipelineRun,
    anvil: &Anvil,
    (chain, namespace, suffix, chain_number): (&str, &str, &str, u64),
    registry: Address,
    account: Address,
    operator: Address,
    changed_owner: Address,
) -> Result<()> {
    let rpc = anvil.client();
    let approval = rpc
        .eth_call(
            registry,
            &isApprovedForAllCall {
                owner: account,
                operator,
            }
            .abi_encode(),
        )
        .await?;
    ensure!(
        isApprovedForAllCall::abi_decode_returns(&approval)?,
        "on-chain approval control"
    );
    let projected_source: Value = sqlx::query_scalar("SELECT grant_source FROM account_permission_state_current WHERE subject=$1 AND owner=$2 AND approved")
        .bind(format!("{operator:#x}")).bind(format!("{account:#x}")).fetch_one(&run.db.pool).await?;
    assert_eq!(
        projected_source,
        json!({"kind":"raw_log","source_event":"ApprovalForAll"})
    );
    let api = RealApi::start_for_chain(run, chain, anvil).await?;
    let mut expected_resources = std::collections::BTreeSet::new();
    for (label, expected_owner, expected) in [
        ("authorcurrent", account, true),
        ("authortoken", account, true),
        ("authorchanged", changed_owner, false),
        ("authorzero", Address::ZERO, false),
    ] {
        let name = format!("{label}.{suffix}");
        let raw = rpc
            .eth_call(
                registry,
                &ownerCall {
                    node: ens_v1::namehash(&name),
                }
                .abi_encode(),
            )
            .await?;
        assert_eq!(
            ownerCall::abi_decode_returns(&raw)?,
            expected_owner,
            "getter {name}"
        );
        let body = api
            .get(&format!("/v2/permissions?name={name}&include=lineage"))
            .await?;
        let projected_names: Vec<Value> =
            sqlx::query_scalar("SELECT to_jsonb(n) FROM name_current n WHERE raw_name=$1")
                .bind(&name)
                .fetch_all(&run.db.pool)
                .await?;
        let projected_bindings: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(s) FROM permissions_current_resource_summary s WHERE resource_id IN (SELECT resource_id FROM name_current WHERE raw_name=$1)")
            .bind(&name).fetch_all(&run.db.pool).await?;
        let account_http = api
            .get(&format!(
                "/v2/permissions?address={operator:#x}&include=lineage"
            ))
            .await?;
        println!(
            "AUTHOR_UNTOUCHED_DIAGNOSTIC {}",
            json!({"name":name,"name_current":projected_names,"resource_summaries":projected_bindings,"account_http":account_http})
        );
        assert_eq!(has_operator(&body), expected, "name route {name}: {body}");
        if label == "authortoken" {
            let (selected, owner, token): (Uuid, Option<String>, Option<Uuid>) = sqlx::query_as(
                "SELECT resource_id, declared_summary #>> '{control,registry_owner}', token_lineage_id FROM name_current WHERE raw_name=$1",
            ).bind(&name).fetch_one(&run.db.pool).await?;
            let expected_resource: Uuid = if namespace == "ens" {
                "d9740532-4b9a-567d-8bb0-6d256816c926"
            } else {
                "a630d2a6-1b83-5afb-b66b-341698d44e2b"
            }
            .parse()?;
            assert_eq!(
                selected, expected_resource,
                "transfer keeps the original registry-only selection"
            );
            assert_eq!(owner.as_deref(), Some(format!("{account:#x}").as_str()));
            assert!(
                token.is_none(),
                "transfer must retain the registry-only resource"
            );
            let active: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM name_current n JOIN surface_bindings b ON b.surface_binding_id=n.surface_binding_id JOIN chain_lineage c ON c.chain_id=b.chain_id AND c.block_hash=b.block_hash AND c.block_number=b.block_number WHERE n.raw_name=$1 AND b.resource_id=$2 AND b.active_to IS NULL AND b.canonicality_state IN ('canonical','safe','finalized') AND c.canonicality_state IN ('canonical','safe','finalized'))",
            ).bind(&name).bind(selected).fetch_one(&run.db.pool).await?;
            assert!(
                active,
                "selected registry authority must have a canonical active binding"
            );
            let epoch: Value = sqlx::query_scalar(
                "SELECT after_state FROM normalized_events WHERE logical_name_id=$1 AND resource_id=$2 AND event_kind='AuthorityEpochChanged' AND after_state->>'source_event'='Transfer' ORDER BY block_number DESC, transaction_index DESC, log_index DESC, normalized_event_id DESC LIMIT 1",
            ).bind(format!("{namespace}:{:#x}", ens_v1::namehash(&name))).bind(selected).fetch_one(&run.db.pool).await?;
            assert_eq!(epoch["registry_owner"], format!("{account:#x}"));
            assert_eq!(epoch["authority_kind"], "registry_only");
            assert_eq!(projected_bindings.len(), 1);
            assert_eq!(
                projected_bindings[0]["registry_owner"],
                format!("{account:#x}")
            );
            assert_eq!(
                projected_bindings[0]["registry_contract"],
                format!("{registry:#x}")
            );
            assert!(
                body["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|row| row["registration_id"] == selected.to_string())
            );
            assert!(
                has_operator(&account_http),
                "address route retains the original operator"
            );
        }

        assert_eq!(body["meta"]["completeness"], "partial");
        if expected {
            let row = body["data"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["grant_relation"] == "operator")
                .unwrap();
            assert_eq!(
                row["lineage"],
                json!({"grant":{"kind":"event"}}),
                "producer-derived lineage {name}"
            );
            assert_eq!(
                row["grant_scope"],
                json!({"kind":"account","detail":{
                "chain_id":chain_number,"authority_kind":"registry","authority_contract":format!("{registry:#x}"),"owner":format!("{account:#x}")}})
            );
            assert_eq!(row["powers"], json!(["registry_control"]));
            assert_eq!(row["authority_context"], "current_for_name");
            let id = row["registration_id"].as_str().unwrap();
            expected_resources.insert(id.to_owned());
            let audit = api
                .get(&format!(
                    "/v2/permissions?registration_id={id}&address={operator:#x}&include=lineage"
                ))
                .await?;
            assert!(has_operator(&audit), "registration route {name}");
            assert_eq!(audit["data"][0]["authority_context"], "resource_audit");
            let other_namespace = if namespace == "ens" {
                "basenames"
            } else {
                "ens"
            };
            let empty = api.get(&format!("/v2/permissions?registration_id={id}&namespace={other_namespace}&address={operator:#x}")).await?;
            assert_eq!(empty["data"], json!([]));
        }
        println!(
            "AUTHOR_OPERATING_OBSERVATION {}",
            json!({"chain":chain,"name":name,"registry_owner":format!("{expected_owner:#x}"),"operator_expected":expected,"http":body})
        );
    }
    let mut actual_resources = std::collections::BTreeSet::new();
    let mut cursor = String::new();
    for _ in 0..10 {
        let body = api.get(&format!("/v2/permissions?address={operator:#x}&namespace={namespace}&include=lineage&page_size=1{cursor}")).await?;
        for row in body["data"].as_array().unwrap() {
            assert_eq!(row["grant_relation"], "operator");
            assert!(
                actual_resources.insert(row["registration_id"].as_str().unwrap().to_owned()),
                "duplicate page"
            );
        }
        if !body["page"]["has_more"].as_bool().unwrap() {
            break;
        }
        cursor = format!("&cursor={}", body["page"]["next_cursor"].as_str().unwrap());
    }
    assert_eq!(
        actual_resources, expected_resources,
        "account pagination agrees with current resources"
    );
    let names = api
        .get(&format!(
            "/v2/addresses/{account:#x}/names?namespace={namespace}&include=role_summary"
        ))
        .await?;
    for label in ["authorcurrent", "authortoken"] {
        let name = format!("{label}.{suffix}");
        let row = names["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["name"] == name)
            .context("owner/manager address membership")?;
        assert_ne!(
            row["registration_status"], "unregistered",
            "address name {name}: {row}"
        );
        assert!(
            row["role_summary"]
                .as_array()
                .unwrap()
                .iter()
                .any(|role| role["address"] == format!("{operator:#x}")
                    && role["grants"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|v| v["grant_relation"] == "operator")),
            "role summary {name}: {row}"
        );
    }
    let empty = api
        .get(&format!(
            "/v2/addresses/{operator:#x}/names?include=role_summary"
        ))
        .await?;
    assert_eq!(
        empty["data"],
        json!([]),
        "operator approval must not create address-name membership"
    );
    drop(api);
    Ok(())
}

async fn author_ens_real_producer_and_http() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let d = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (account, operator, next_owner) = (accounts[1], accounts[2], accounts[3]);
    ens_v1::set_registry_approval_for_all(&rpc, &d, account, operator, true).await?;
    for label in [
        "authorcurrent",
        "authortoken",
        "authorchanged",
        "authorzero",
    ] {
        ens_v1::register_eth_name(
            &rpc,
            &d,
            label,
            account,
            365 * 24 * 60 * 60,
            d.public_resolver.address,
        )
        .await?;
    }
    ens_v1::transfer_eth_name_without_reclaim(&rpc, &d, account, next_owner, "authortoken").await?;
    for (label, owner) in [("authorchanged", next_owner), ("authorzero", Address::ZERO)] {
        rpc.send_checked(
            account,
            d.registry.address,
            &setOwnerCall {
                node: ens_v1::namehash(&format!("{label}.eth")),
                owner,
            }
            .abi_encode(),
            U256::ZERO,
            "review registry owner change",
        )
        .await?;
    }
    let run = support::ingest_and_serve(
        &anvil,
        &d,
        Some("SELECT EXISTS (SELECT 1 FROM account_permission_state_current WHERE approved)"),
    )
    .await?;
    verify_snapshot(
        &run,
        &anvil,
        ("ethereum-mainnet", "ens", "eth", 1),
        d.registry.address,
        account,
        operator,
        next_owner,
    )
    .await?;
    run.db.cleanup().await
}

async fn author_basenames_real_producer_and_http() -> Result<()> {
    let anvil = Anvil::spawn_base_mainnet().await?;
    let rpc = anvil.client();
    let d = basenames::deploy_basenames(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (account, operator, next_owner) = (accounts[1], accounts[2], accounts[3]);
    rpc.send_checked(
        account,
        d.registry.address,
        &setApprovalForAllCall {
            operator,
            approved: true,
        }
        .abi_encode(),
        U256::ZERO,
        "review registry approval",
    )
    .await?;
    for label in [
        "authorcurrent",
        "authortoken",
        "authorchanged",
        "authorzero",
    ] {
        basenames::register_base_name(&rpc, &d, account, label, account, 365 * 24 * 60 * 60)
            .await?;
    }
    basenames::transfer_base_token(&rpc, &d, account, next_owner, "authortoken").await?;
    for (label, owner) in [("authorchanged", next_owner), ("authorzero", Address::ZERO)] {
        basenames::set_registry_owner(&rpc, &d, account, &format!("{label}.base.eth"), owner)
            .await?;
    }
    let run = support::ingest_basenames_and_serve(
        &anvil,
        &d,
        Some("SELECT EXISTS (SELECT 1 FROM account_permission_state_current WHERE approved)"),
    )
    .await?;
    verify_snapshot(
        &run,
        &anvil,
        ("base-mainnet", "basenames", "base.eth", 8453),
        d.registry.address,
        account,
        operator,
        next_owner,
    )
    .await?;
    run.db.cleanup().await
}
