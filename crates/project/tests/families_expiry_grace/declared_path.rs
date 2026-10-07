//! The recognized root-to-ETHRegistry path for the synthetic missing-parent test. Its local
//! block coordinates start at zero; the API producer companion uses the real manifest starts.
use super::{CHAIN, Fixture, OWNER, hash, json, uuid};
use alloy_primitives::{U256, keccak256};
use anyhow::Result;

// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/RootRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
const ROOT: &str = "0xb458d6a3a77919449d03e7a6903c26827c1ec43f";
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/ETHRegistry.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
const ETH: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";

pub(super) async fn root_eth_entry(fixture: &Fixture) -> Result<()> {
    for (family, role, address) in [
        ("ens_v2_root_l1", "root_registry", ROOT),
        ("ens_v2_registry_l1", "registry", ETH),
    ] {
        let payload = json!({
            "manifest_version": 1, "namespace": "ens", "source_family": family,
            "chain": CHAIN, "deployment_epoch": "ens_v2_sepolia_20261001",
            "rollout_status": "active", "contracts": [{
                "role": role, "address": address, "proxy_kind": "none", "start_block": 0
            }]
        });
        let id: i64 = sqlx::query_scalar(
            "INSERT INTO manifest_versions (manifest_version, namespace, source_family,
                 chain_id, deployment_label, rollout_status, normalizer_version, file_path,
                 manifest_payload)
             VALUES (1,'ens',$1,$2,'fixture','active','fixture',$3,$4) RETURNING manifest_id",
        )
        .bind(family)
        .bind(CHAIN)
        .bind(format!("fixture/{family}.toml"))
        .bind(&payload)
        .fetch_one(&fixture.pool)
        .await?;
        sqlx::query(
            "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
                 manifest_version, source_manifest_id, chain_id, block_number, block_hash,
                 derivation_kind, canonicality_state, after_state)
             VALUES ($1,'ens','SourceManifestUpdated',$2,1,$3,$4,0,$5,
                 'manifest_sync','canonical',
                 jsonb_build_object('rollout_status','active','manifest_payload',$6::jsonb))",
        )
        .bind(format!("path-manifest:{id}"))
        .bind(family)
        .bind(id)
        .bind(CHAIN)
        .bind(hash(0))
        .bind(payload)
        .execute(&fixture.pool)
        .await?;
    }

    // Deployment registers eth with a zero resolver, ETHRegistry subregistry and max expiry.
    // No bob entry is created in ETHRegistry: that is the intended point of absence.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/deploy/01_ETHRegistry.ts:L39-L51 @ ens_v2_sepolia_20261001@07e55a05)
    let token = format!(
        "{:#066x}",
        U256::from_be_bytes(*keccak256("eth")) >> 32 << 32
    );
    let instance = uuid(0x4400).parse()?;
    let resource =
        bigname_storage::ens_v2_registry_resource_id(CHAIN, instance, &token).to_string();
    for (index, kind, resource_id, after) in [
        (
            0,
            "RegistrationGranted",
            None,
            json!({"source_event":"LabelRegistered", "token_id":token,
                "label":"eth", "label_hash":format!("{:#x}",keccak256("eth")),
                "registry_contract_instance_id":instance.to_string(),
                "resource_pending":true, "sender":OWNER, "registrant":OWNER,
                "authority_kind":"ens_v2_registry", "status":"registered", "expiry":u64::MAX}),
        ),
        (
            1,
            "TokenResourceLinked",
            Some(resource.as_str()),
            json!({"source_event":"TokenResource", "token_id":token,
                "upstream_resource":token, "resource_id":resource,
                "registry_contract_instance_id":instance.to_string()}),
        ),
        (
            2,
            "SubregistryChanged",
            Some(resource.as_str()),
            json!({"source_event":"SubregistryUpdated", "token_id":token,
                "subregistry":ETH, "sender":OWNER,
                "registry_contract_instance_id":instance.to_string()}),
        ),
    ] {
        fixture
            .write(
                0,
                index,
                kind,
                "ens_v2_root_l1",
                None,
                resource_id,
                after,
                ROOT,
            )
            .await?;
    }
    Ok(())
}

pub(super) async fn assert_deployed_path(fixture: &Fixture) -> Result<()> {
    let entries: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT entry.registry, entry.status,
             (SELECT event.after_state->>'subregistry' FROM normalized_events event
              WHERE event.resource_id=entry.resource_id AND event.event_kind='SubregistryChanged'
              ORDER BY event.block_number DESC, event.log_index DESC LIMIT 1)
         FROM project_ens_v2_entry_owner entry ORDER BY registry, entry_key",
    )
    .fetch_all(&fixture.pool)
    .await?;
    anyhow::ensure!(
        entries == vec![(ROOT.into(), "registered".into(), Some(ETH.into()))],
        "expected the root eth entry and ETHRegistry pointer, with no bob entry: {entries:?}"
    );
    Ok(())
}
