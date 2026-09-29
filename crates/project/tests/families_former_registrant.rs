//! The last holder of an ended ENSv2 registration (TYR-63). A released ENSv2 name serves
//! `lapsed_registration` with the registry token's holder when the registration ended, held
//! through the registry: `expired` for a registration past its expiry, which keeps that expiry
//! and its ENSv2 grace, and `unregistered` for an explicit unregister, which keeps neither. A
//! re-registration drops it.
#[path = "families_support/mod.rs"]
mod support;

use alloy_primitives::{B256, keccak256};
use anyhow::{Result, ensure};
use bigname_project::families::FamilyMode;
use bigname_storage::families::name::load_family_name;
use serde_json::{Value, json};
use support::{CHAIN, Event, Fixture, hash, uuid};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e6";
const ALICE: &str = "0x00000000000000000000000000000000000000aa";
const BOB: &str = "0x00000000000000000000000000000000000000bb";
const CAROL: &str = "0x00000000000000000000000000000000000000cc";
const EXPIRY: u64 = 1_800_000_060;
const DAY: u64 = 86_400;

fn namehash(name: &str) -> B256 {
    name.rsplit('.').fold(B256::ZERO, |parent, label| {
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(parent.as_slice());
        input[32..].copy_from_slice(keccak256(label.as_bytes()).as_slice());
        keccak256(input)
    })
}

async fn surface(fixture: &Fixture, name: &str) -> Result<String> {
    let node = format!("{:#x}", namehash(name));
    let logical_name_id = format!("ens:{node}");
    let normalized = bigname_domain::normalization::normalize_name(name)?;
    let labelhashes: Vec<String> = normalized
        .normalized_labels
        .iter()
        .map(|label| format!("{:#x}", keccak256(label.as_bytes())))
        .collect();
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'ens', $5, $6, $7, $2, $8, $9, 'active', $3, $4, 0, 'canonical')",
    )
    .bind(&logical_name_id)
    .bind(&node)
    .bind(CHAIN)
    .bind(hash(0))
    .bind(normalized.normalized_name)
    .bind(normalized.normalized_labels)
    .bind(normalized.dns_encoded_name)
    .bind(labelhashes)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .execute(&fixture.pool)
    .await?;
    Ok(logical_name_id)
}

/// `name` registered in the ENSv2 `eth` registry to ALICE at block 2 on resource `n`, then
/// transferred to BOB at block 3.
async fn registered(fixture: &Fixture, name: &str, n: u32) -> Result<(String, String)> {
    let logical_name_id = surface(fixture, name).await?;
    let resource = uuid(0x5000 + n);
    fixture
        .binding(
            &uuid(500 + n),
            &logical_name_id,
            &resource,
            "ens_v2",
            2,
            1,
            None,
        )
        .await?;
    let key = json!({"registry_contract_instance_id": "eth", "token_id": format!("{n}")});
    let with = |extra: Value| {
        let mut after = key.clone();
        after
            .as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        after
    };
    fixture
        .write(
            2,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            Some(&logical_name_id),
            Some(&resource),
            with(
                json!({"status": "registered", "registrant": ALICE, "owner": ALICE,
                        "expiry": EXPIRY}),
            ),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            3,
            1,
            "TokenControlTransferred",
            "ens_v2_registry_l1",
            Some(&logical_name_id),
            Some(&resource),
            with(json!({"from": ALICE, "to": BOB})),
            REGISTRY,
        )
        .await?;
    Ok((logical_name_id, resource))
}

async fn registration(fixture: &Fixture, logical_name_id: &str) -> Result<Value> {
    let row = load_family_name(&fixture.pool, logical_name_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("{logical_name_id} has no composed row"))?;
    Ok(row.declared_summary["registration"].clone())
}

#[tokio::test]
async fn an_expired_ens_v2_registration_names_its_last_holder_until_it_is_registered_again()
-> Result<()> {
    let fixture = Fixture::new("families_former_expired", 12).await?;
    let (name, resource) = registered(&fixture, "expired.eth", 1).await?;
    // The interpreter's path-expiry release: on the resource, without a name.
    let identity = "path-expired-6";
    fixture
        .event(
            Event::new(identity, 6, 1, "RegistrationReleased", "ens_v2_registry_l1")
                .resource(&resource)
                .after(json!({"source_event": "RegistryPathExpired",
                              "registry_contract_instance_id": "eth", "token_id": "1",
                              "expiry": EXPIRY, "released_at": EXPIRY}))
                .raw(json!({"emitting_address": REGISTRY}))
                .synthesised(),
        )
        .await?;
    fixture.apply(6, FamilyMode::Normal).await?;
    let released = registration(&fixture, &name).await?;
    ensure!(released["status"] == json!("released"), "{released}");
    ensure!(
        released["lapsed_registration"]["registrant"] == json!(BOB)
            && released["lapsed_registration"]["held_through"] == json!("registry")
            && released["lapsed_registration"]["release_kind"] == json!("expired"),
        "{released}"
    );
    ensure!(
        released["expiry"] == json!(EXPIRY.to_string())
            && released["grace_ends_at"] == json!((EXPIRY + 28 * DAY).to_string()),
        "{released}"
    );

    // CAROL registers it again: no former holder remains.
    fixture
        .write(
            8,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            Some(&name),
            Some(&uuid(0x5100)),
            json!({"registry_contract_instance_id": "eth", "token_id": "2",
                   "status": "registered", "registrant": CAROL, "owner": CAROL,
                   "expiry": EXPIRY + 400 * DAY}),
            REGISTRY,
        )
        .await?;
    fixture.apply(8, FamilyMode::Normal).await?;
    let again = registration(&fixture, &name).await?;
    ensure!(
        again.get("lapsed_registration").is_none_or(Value::is_null),
        "{again}"
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn an_unregistered_ens_v2_registration_names_its_last_holder_without_an_expiry() -> Result<()>
{
    let fixture = Fixture::new("families_former_unregistered", 12).await?;
    let (name, resource) = registered(&fixture, "gone.eth", 3).await?;
    fixture
        .write(
            4,
            1,
            "RegistrationReleased",
            "ens_v2_registry_l1",
            Some(&name),
            Some(&resource),
            json!({"source_event": "LabelUnregistered", "registry_contract_instance_id": "eth",
                   "token_id": "3", "released_at": 1_800_000_048}),
            REGISTRY,
        )
        .await?;
    fixture.apply(5, FamilyMode::Normal).await?;
    let released = registration(&fixture, &name).await?;
    ensure!(
        released["status"] == json!("released")
            && released["expiry"].is_null()
            && released["grace_ends_at"].is_null(),
        "{released}"
    );
    ensure!(
        released["lapsed_registration"]["registrant"] == json!(BOB)
            && released["lapsed_registration"]["release_kind"] == json!("unregistered"),
        "{released}"
    );
    fixture.cleanup().await
}
