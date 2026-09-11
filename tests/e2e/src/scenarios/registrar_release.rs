use anyhow::{Context, Result};
use serde_json::Value;

use super::support;
use crate::harness::{anvil::Anvil, db::HarnessDb, ens_v1, manifests, pipeline, repo_root};

async fn run_phases(anvil: &Anvil, db: &HarnessDb, manifests_root: &std::path::Path) -> Result<()> {
    let rpc = anvil.client();
    pipeline::run_fixture_spine_through_block(
        &repo_root(),
        &db.url,
        &db.pool,
        manifests_root,
        &anvil.url,
        rpc.block_number().await?,
        None,
    )
    .await
}

async fn capture(db: &HarnessDb, logical_name_id: &str, chain: Value) -> Result<Value> {
    let namehash = logical_name_id
        .strip_prefix("ens:")
        .context("ENS namehash")?;
    let registrar = chain["registrar_address"]
        .as_str()
        .context("registrar address")?;
    let registration_tx = chain["registration_transaction"]
        .as_str()
        .context("registration transaction")?;
    // Numeric registrar evidence can have no plaintext logical-name link. Associate it
    // through this actual registrar's receipt, exact namehash, namespace, and chain.
    let resource_ids = "SELECT resource_id FROM normalized_events WHERE chain_id='ethereum-mainnet' AND namespace='ens' AND resource_id IS NOT NULL AND
      (logical_name_id=$1 OR (source_family='ens_v1_registrar_l1' AND after_state->>'namehash'=$2
       AND raw_fact_ref->>'emitting_address'=$3 AND transaction_hash=$4))
      UNION SELECT resource_id FROM surface_bindings WHERE logical_name_id=$1 AND chain_id='ethereum-mainnet'";
    let events_sql = format!(
        "SELECT to_jsonb(e) FROM normalized_events e WHERE chain_id='ethereum-mainnet' AND namespace='ens' AND (logical_name_id=$1 OR resource_id IN ({resource_ids})) ORDER BY event_identity"
    );
    let events: Vec<Value> = sqlx::query_scalar(&events_sql)
        .bind(logical_name_id)
        .bind(namehash)
        .bind(registrar)
        .bind(registration_tx)
        .fetch_all(&db.pool)
        .await?;
    let name: Option<Value> =
        sqlx::query_scalar("SELECT to_jsonb(n) FROM name_current n WHERE logical_name_id=$1")
            .bind(logical_name_id)
            .fetch_optional(&db.pool)
            .await?;
    let permissions_sql = format!(
        "SELECT to_jsonb(p) FROM permissions_current p WHERE resource_id IN ({resource_ids}) ORDER BY resource_id,subject,scope"
    );
    let permissions: Vec<Value> = sqlx::query_scalar(&permissions_sql)
        .bind(logical_name_id)
        .bind(namehash)
        .bind(registrar)
        .bind(registration_tx)
        .fetch_all(&db.pool)
        .await?;
    Ok(serde_json::json!({"events":events,"name":name,"permissions":permissions}))
}

#[tokio::test]
async fn callback_registration_preserves_distinct_holder_and_revokes_it_after_lapse() -> Result<()>
{
    use crate::harness::artifacts::{Artifact, deploy};
    use alloy_primitives::{Address, B256, Bytes, U256};
    use alloy_sol_types::{SolCall, SolValue, sol};
    sol! {
        function ownerOf(uint256 tokenId) external view returns (address);
        function nameExpires(uint256 tokenId) external view returns (uint256);
        function available(uint256 tokenId) external view returns (bool);
    }

    let anvil = Anvil::spawn().await?;
    let rpc = anvil.client();
    let deployment = ens_v1::deploy_ens_v1(&rpc, &repo_root()).await?;
    let accounts = rpc.accounts().await?;
    let (payer, registry_owner) = (accounts[1], accounts[2]);
    let scratch = support::TempDir::create()?;
    let source = scratch.path().join("CallbackResolver.sol");
    std::fs::write(&source, include_str!("registrar_callback_resolver.sol"))?;
    let build = std::process::Command::new("forge")
        .args(["build", "--root"])
        .arg(scratch.path())
        .args(["--contracts", ".", "--use", "0.8.26", "--threads", "4"])
        .output()?;
    anyhow::ensure!(
        build.status.success(),
        "callback fixture build: {}{}",
        String::from_utf8_lossy(&build.stdout),
        String::from_utf8_lossy(&build.stderr)
    );
    let artifact: Value = serde_json::from_slice(&std::fs::read(
        scratch
            .path()
            .join("out/CallbackResolver.sol/CallbackResolver.json"),
    )?)?;
    let callback = deploy(
        &rpc,
        deployment.deployer,
        &Artifact {
            name: "CallbackResolver".into(),
            creation_code: alloy_primitives::hex::decode(
                artifact["bytecode"]["object"].as_str().unwrap(),
            )?,
        },
        &(deployment.registry.address, registry_owner).abi_encode_params(),
    )
    .await?;
    let profile = manifests::generate_local_profile(
        scratch.path(),
        &repo_root(),
        &deployment.manifest_targets(),
    )?;
    let db = HarnessDb::create().await?;
    let label = "registrarcallback";
    let node = ens_v1::namehash("registrarcallback.eth");
    let logical_name_id = format!("ens:{node:#x}");
    let registration = ens_v1::Registration {
        label: label.into(),
        owner: callback.address,
        duration: U256::from(365 * 24 * 60 * 60_u64),
        secret: B256::repeat_byte(0x45),
        resolver: callback.address,
        data: vec![Bytes::from_static(&[1])],
        reverseRecord: 0,
        referrer: B256::ZERO,
    };
    let commitment = ens_v1::makeCommitmentCall::abi_decode_returns(
        &rpc.eth_call(
            deployment.controller.address,
            &ens_v1::makeCommitmentCall {
                registration: registration.clone(),
            }
            .abi_encode(),
        )
        .await?,
    )?;
    rpc.send_checked(
        payer,
        deployment.controller.address,
        &ens_v1::commitCall { commitment }.abi_encode(),
        U256::ZERO,
        "callback commit",
    )
    .await?;
    rpc.increase_time(61).await?;
    let price = ens_v1::rentPriceCall::abi_decode_returns(
        &rpc.eth_call(
            deployment.controller.address,
            &ens_v1::rentPriceCall {
                label: label.into(),
                duration: registration.duration,
            }
            .abi_encode(),
        )
        .await?,
    )?;
    let registration_receipt = rpc
        .send_checked(
            payer,
            deployment.controller.address,
            &ens_v1::registerCall { registration }.abi_encode(),
            price.base + price.premium,
            "callback registration",
        )
        .await?;
    let actual_holder: Address = ownerOfCall::abi_decode_returns(
        &rpc.eth_call(
            deployment.base_registrar.address,
            &ownerOfCall {
                tokenId: U256::from_be_bytes(ens_v1::labelhash(label).0),
            }
            .abi_encode(),
        )
        .await?,
    )?;
    let actual_registry_owner = ens_v1::ownerCall::abi_decode_returns(
        &rpc.eth_call(
            deployment.registry.address,
            &ens_v1::ownerCall { node }.abi_encode(),
        )
        .await?,
    )?;
    assert_eq!(actual_holder, callback.address);
    assert_eq!(actual_registry_owner, registry_owner);
    let token_id = U256::from_be_bytes(ens_v1::labelhash(label).0);
    let expiry = nameExpiresCall::abi_decode_returns(
        &rpc.eth_call(
            deployment.base_registrar.address,
            &nameExpiresCall { tokenId: token_id }.abi_encode(),
        )
        .await?,
    )?;
    let receipt_json = rpc
        .call(
            "eth_getTransactionReceipt",
            serde_json::json!([registration_receipt.tx_hash]),
        )
        .await?;
    let before_block = rpc
        .call("eth_getBlockByNumber", serde_json::json!(["latest", false]))
        .await?;
    let holder = format!("{actual_holder:#x}");
    let registry_owner_text = format!("{actual_registry_owner:#x}");
    println!(
        "CALLBACK_PROOF onchain_registry_owner={registry_owner_text} onchain_holder={holder} registrar_expiry={expiry}"
    );
    run_phases(&anvil, &db, &profile.root).await?;
    let before = capture(&db, &logical_name_id, serde_json::json!({
        "registry_owner":registry_owner_text, "registrar_holder":holder,
        "registrar_address":format!("{:#x}",deployment.base_registrar.address), "registration_transaction":registration_receipt.tx_hash, "receipt":receipt_json,
        "expiry":expiry.to_string(), "block":before_block,
    })).await?;
    let granted = before["events"]
        .as_array()
        .context("events array")?
        .iter()
        .find(|e| e["event_kind"] == "RegistrationGranted")
        .context("production RegistrationGranted")?;
    let registrar_resource = granted["resource_id"]
        .as_str()
        .context("registration resource")?
        .to_owned();
    let registered_subject = granted["after_state"]["registrant"]
        .as_str()
        .context("event registrant")?
        .to_owned();
    let authority_owner = granted["after_state"]["authority_owner"]
        .as_str()
        .context("authority owner")?
        .to_owned();
    let selected_before = before["name"]["resource_id"]
        .as_str()
        .context("selected resource")?
        .to_owned();
    assert_eq!(granted["chain_id"], "ethereum-mainnet");
    assert_eq!(granted["namespace"], "ens");
    assert_eq!(
        granted["raw_fact_ref"]["emitting_address"],
        format!("{:#x}", deployment.base_registrar.address)
    );
    assert_eq!(granted["transaction_hash"], registration_receipt.tx_hash);
    let transferred = before["events"]
        .as_array()
        .context("events array")?
        .iter()
        .find(|e| {
            e["event_kind"] == "TokenControlTransferred"
                && e["resource_id"] == registrar_resource
                && e["after_state"]["to"] == holder
        })
        .context("same registrar resource transferred to actual holder")?;
    assert_eq!(
        transferred["raw_fact_ref"]["emitting_address"],
        format!("{:#x}", deployment.base_registrar.address)
    );
    assert_eq!(
        transferred["transaction_hash"],
        registration_receipt.tx_hash
    );
    assert!(transferred["log_index"].as_i64() > granted["log_index"].as_i64());
    assert_eq!(
        registered_subject,
        format!("{:#x}", deployment.controller.address)
    );
    assert_eq!(registered_subject, authority_owner);
    assert!(granted["logical_name_id"].is_null());
    assert_ne!(selected_before, registrar_resource);
    assert_eq!(
        before["name"]["declared_summary"]["control"]["registry_owner"],
        registry_owner_text
    );
    assert!(before["name"]["token_lineage_id"].is_null());
    assert_eq!(before["name"]["binding_kind"], "declared_registry_path");
    let registry_permissions = |snapshot: &Value| {
        snapshot["permissions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| p["resource_id"] == selected_before && p["subject"] == registry_owner_text)
            .map(|p| (p["scope"].clone(), p["effective_powers"].clone()))
            .collect::<Vec<_>>()
    };
    let retained = registry_permissions(&before);
    assert!(!retained.is_empty());
    assert!(
        before["permissions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["resource_id"] == registrar_resource
                && p["subject"] == holder
                && p["effective_powers"] == serde_json::json!(["resource_control"]))
    );
    db.cleanup().await?;
    // Release is derived at the first processed canonical block strictly after grace.
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L104 @ ens_v1@91c966f)
    let expiry = expiry.to::<u64>();
    for timestamp in [expiry, expiry + 90 * 86400, expiry + 90 * 86400 + 1] {
        rpc.call("evm_setNextBlockTimestamp", serde_json::json!([timestamp]))
            .await?;
        rpc.call("evm_mine", serde_json::json!([])).await?;
        assert_eq!(rpc.block_timestamp().await?, u128::from(timestamp));
        let released_now = timestamp > expiry + 90 * 86400;
        assert_eq!(
            availableCall::abi_decode_returns(
                &rpc.eth_call(
                    deployment.base_registrar.address,
                    &availableCall { tokenId: token_id }.abi_encode()
                )
                .await?
            )?,
            released_now
        );
        assert_eq!(
            ens_v1::ownerCall::abi_decode_returns(
                &rpc.eth_call(
                    deployment.registry.address,
                    &ens_v1::ownerCall { node }.abi_encode()
                )
                .await?
            )?,
            registry_owner
        );
        let db = HarnessDb::create().await?;
        run_phases(&anvil, &db, &profile.root).await?;
        let after = capture(
            &db,
            &logical_name_id,
            serde_json::json!({
                "registrar_address":format!("{:#x}",deployment.base_registrar.address),
                "registration_transaction":registration_receipt.tx_hash,
            }),
        )
        .await?;
        assert_eq!(after["name"]["resource_id"], selected_before);
        assert_eq!(registry_permissions(&after), retained);
        let releases = after["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| {
                e["event_kind"] == "RegistrationReleased" && e["resource_id"] == registrar_resource
            })
            .collect::<Vec<_>>();
        assert_eq!(releases.len(), usize::from(released_now));
        if released_now {
            assert_eq!(releases[0]["before_state"]["registrant"], holder);
        }
        let positive = after["permissions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| {
                p["resource_id"] == registrar_resource
                    && p["subject"] == holder
                    && p["scope"] == "resource"
                    && p["effective_powers"]
                        .as_array()
                        .is_some_and(|powers| !powers.is_empty())
            })
            .count();
        println!(
            "callback timestamp={timestamp} released={released_now} holder_positive_rows={positive}"
        );
        assert_eq!(
            positive,
            usize::from(!released_now),
            "expired registrar holder retains resource_control"
        );
        if released_now {
            assert!(
                !after["permissions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|p| p["resource_id"] == registrar_resource
                        && p["subject"] == holder
                        && p["effective_powers"]
                            .as_array()
                            .is_some_and(|powers| !powers.is_empty())),
                "integrated authority and release behavior must also remove stale resolver control"
            );
        }
        db.cleanup().await?;
    }
    Ok(())
}
