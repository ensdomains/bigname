use std::path::Path;

use alloy_primitives::{Address, U256, keccak256};
use alloy_sol_types::{SolCall, sol};
use anyhow::{Context, Result};
use serde_json::Value;

use super::support;
use crate::harness::{
    anvil::Anvil, db::HarnessDb, ens_v1, manifests, pipeline, repo_root, rpc::RpcClient,
};

sol! {
    function ownerOf(uint256 tokenId) external view returns (address);
    function nameExpires(uint256 tokenId) external view returns (uint256);
    function owner(bytes32 node) external view returns (address);
    function resolver(bytes32 node) external view returns (address);
}

const YEAR: u64 = 365 * 24 * 60 * 60;
const GRACE: u64 = 90 * 24 * 60 * 60;

async fn export_fixture_corpus(
    rpc: &RpcClient,
    through_block: u64,
    path: &Path,
    observations: Value,
) -> Result<()> {
    let mut blocks = Vec::new();
    let mut receipts = Vec::new();
    for block_number in 0..=through_block {
        let block = rpc
            .call(
                "eth_getBlockByNumber",
                serde_json::json!([format!("{block_number:#x}"), true]),
            )
            .await?;
        for transaction in block["transactions"]
            .as_array()
            .context("block transactions")?
        {
            receipts.push(
                rpc.call(
                    "eth_getTransactionReceipt",
                    serde_json::json!([transaction["hash"]]),
                )
                .await?,
            );
        }
        blocks.push(block);
    }
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "through_block":through_block,
            "blocks":blocks,
            "receipts":receipts,
            "observations":observations,
        }))?,
    )?;
    Ok(())
}

async fn successor_resource(db: &HarnessDb, transaction_hash: &str) -> Result<String> {
    Ok(sqlx::query_scalar(
        "SELECT resource_id::text FROM normalized_events
         WHERE event_kind='RegistrationGranted' AND transaction_hash=$1
           AND source_family='ens_v1_registrar_l1'",
    )
    .bind(transaction_hash)
    .fetch_one(&db.pool)
    .await?)
}

async fn resolver_grants(
    db: &HarnessDb,
    transaction_hash: &str,
    subject: Address,
    resolver: Address,
) -> Result<(String, i64, i64)> {
    let resource_id = successor_resource(db, transaction_hash).await?;
    let current = sqlx::query_scalar(
        "SELECT count(*) FROM permissions_current
         WHERE resource_id::text=$1 AND lower(subject)=$2
           AND scope_kind='resolver' AND lower(scope_detail->>'resolver_address')=$3
           AND effective_powers ? 'resolver_control'",
    )
    .bind(&resource_id)
    .bind(format!("{subject:#x}"))
    .bind(format!("{resolver:#x}"))
    .fetch_one(&db.pool)
    .await?;
    let events = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE event_kind='PermissionChanged' AND transaction_hash=$1
           AND resource_id::text=$2 AND lower(after_state->>'subject')=$3
           AND after_state->'scope'->>'kind'='resolver'
           AND lower(after_state->'scope'->>'resolver_address')=$4
           AND after_state->'effective_powers' ? 'resolver_control'",
    )
    .bind(transaction_hash)
    .bind(&resource_id)
    .bind(format!("{subject:#x}"))
    .bind(format!("{resolver:#x}"))
    .fetch_one(&db.pool)
    .await?;
    Ok((resource_id, current, events))
}

async fn resolver_control_grants(db: &HarnessDb, resource_id: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE event_kind='PermissionChanged' AND resource_id::text=$1
           AND after_state->'effective_powers' ? 'resolver_control'",
    )
    .bind(resource_id)
    .fetch_one(&db.pool)
    .await?)
}

#[tokio::test]
async fn same_owner_reregistration_retains_resolver_permission() -> Result<()> {
    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let owner = rpc.accounts().await?[1];
    let resolver = deployment.public_resolver.address;
    let label = "retainedresolver";
    let name = format!("{label}.eth");
    let node = ens_v1::namehash(&name);
    let token_id = U256::from_be_bytes(ens_v1::labelhash(label).0);

    ens_v1::register_eth_name(&rpc, &deployment, label, owner, YEAR, resolver).await?;
    ens_v1::set_text_record_with_receipt(
        &rpc,
        resolver,
        owner,
        &name,
        "description",
        "before lapse",
    )
    .await?;
    let expiry = nameExpiresCall::abi_decode_returns(
        &rpc.eth_call(
            deployment.base_registrar.address,
            &nameExpiresCall { tokenId: token_id }.abi_encode(),
        )
        .await?,
    )?
    .to::<u64>();
    let now = u64::try_from(rpc.block_timestamp().await?)?;
    rpc.increase_time(expiry + GRACE + 3 * 24 * 60 * 60 + 1 - now)
        .await?;

    let second =
        ens_v1::register_eth_name(&rpc, &deployment, label, owner, YEAR, Address::ZERO).await?;
    let second_receipt = rpc
        .call(
            "eth_getTransactionReceipt",
            serde_json::json!([&second.register_tx_hash]),
        )
        .await?;
    let transfer_topic = format!("{:#x}", keccak256("Transfer(address,address,uint256)"));
    let logs = second_receipt["logs"]
        .as_array()
        .context("second receipt logs")?;
    let registrar_transfers = logs
        .iter()
        .filter(|log| {
            log["address"] == format!("{:#x}", deployment.base_registrar.address)
                && log["topics"][0] == transfer_topic
        })
        .count();
    anyhow::ensure!(
        registrar_transfers >= 2,
        "fresh registration omitted burn/mint logs"
    );
    anyhow::ensure!(
        logs.iter()
            .any(|log| log["address"] == format!("{:#x}", deployment.controller.address)),
        "fresh registration omitted controller log"
    );
    let retained = resolverCall::abi_decode_returns(
        &rpc.eth_call(
            deployment.registry.address,
            &resolverCall { node }.abi_encode(),
        )
        .await?,
    )?;
    let actual_owner = ownerOfCall::abi_decode_returns(
        &rpc.eth_call(
            deployment.base_registrar.address,
            &ownerOfCall { tokenId: token_id }.abi_encode(),
        )
        .await?,
    )?;
    let registry_owner = ownerCall::abi_decode_returns(
        &rpc.eth_call(
            deployment.registry.address,
            &ownerCall { node }.abi_encode(),
        )
        .await?,
    )?;
    let fresh_expiry = nameExpiresCall::abi_decode_returns(
        &rpc.eth_call(
            deployment.base_registrar.address,
            &nameExpiresCall { tokenId: token_id }.abi_encode(),
        )
        .await?,
    )?
    .to::<u64>();
    assert_eq!(retained, resolver);
    assert_eq!(actual_owner, owner);
    assert_eq!(registry_owner, owner);
    assert!(fresh_expiry > u64::try_from(rpc.block_timestamp().await?)?);
    let post_write = ens_v1::set_text_record_with_receipt(
        &rpc,
        retained,
        owner,
        &name,
        "description",
        "after reregistration",
    )
    .await?;
    let control = ens_v1::register_eth_name(
        &rpc,
        &deployment,
        "zeroresolvercontrol",
        owner,
        YEAR,
        Address::ZERO,
    )
    .await?;

    let target = rpc.block_number().await?;
    if let Some(path) = std::env::var_os("BIGNAME_816_FIXTURE_CORPUS") {
        export_fixture_corpus(
            &rpc,
            target,
            Path::new(&path),
            serde_json::json!({
                "registry_owner":format!("{registry_owner:#x}"),
                "registrar_owner":format!("{actual_owner:#x}"),
                "retained_resolver":format!("{retained:#x}"),
                "fresh_expiry":fresh_expiry,
                "successful_resolver_write":{
                    "transaction_hash":post_write.tx_hash,
                    "status":post_write.status_ok,
                },
            }),
        )
        .await?;
    }

    let scratch = support::TempDir::create()?;
    let profile = manifests::generate_local_profile(
        scratch.path(),
        &repo_root(),
        &deployment.manifest_targets(),
    )?;
    let db = HarnessDb::create().await?;
    pipeline::run_fixture_spine_through_block(
        &repo_root(),
        &db.url,
        &db.pool,
        &profile.root,
        &anvil.url,
        target,
        None,
    )
    .await?;
    let (successor, resolver_grant_count, resolver_grant_events) =
        resolver_grants(&db, &second.register_tx_hash, owner, resolver).await?;
    let selected: String =
        sqlx::query_scalar("SELECT resource_id::text FROM name_current WHERE logical_name_id=$1")
            .bind(format!("ens:{node:#x}"))
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(selected, successor);
    assert_eq!(
        resolver_grant_count, 1,
        "successor retained-resolver grant missing"
    );
    assert_eq!(
        resolver_grant_events, 1,
        "explicit successor resolver permission event missing"
    );
    let control_resource = successor_resource(&db, &control.register_tx_hash).await?;
    let control_grants = resolver_control_grants(&db, &control_resource).await?;
    assert_eq!(
        control_grants, 0,
        "zero-resolver control gained resolver power"
    );

    pipeline::phase_runner_replay_normalized_events(
        &repo_root(),
        &db.url,
        &profile.root,
        &anvil.url,
        target,
    )
    .await?;
    assert_eq!(
        resolver_grants(&db, &second.register_tx_hash, owner, resolver).await?,
        (successor.clone(), 1, 1),
        "full replay changed retained resolver permission",
    );
    pipeline::phase_runner_replay_current_projections(
        &repo_root(),
        &db.url,
        &profile.root,
        &anvil.url,
        target,
    )
    .await?;
    assert_eq!(
        resolver_grants(&db, &second.register_tx_hash, owner, resolver).await?,
        (successor, 1, 1),
        "projection replay changed retained resolver permission",
    );
    db.cleanup().await?;
    Ok(())
}
