//! A registration's `restrictions.locked_roles` counts the admin roles held on its registry's
//! root resource, with every row produced from real raw logs by Interpret and Project on the
//! checked-in Sepolia profile.
use super::v2_sepolia_redeploy::{
    CHAIN, NEW_REGISTRY, SENDER, checked_in_profile, complete_phases, get, interpret_and_project,
    seed_raw_facts, token,
};
use super::*;
use alloy_primitives::{U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_interpret::RunMode;

sol! {
    event RegistryCreated();
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event EACRolesChanged(uint256 indexed resource, address indexed account, uint256 oldRoleBitmap, uint256 newRoleBitmap);
}

const REGISTRY_OWNER: &str = "0x0000000000000000000000000000000000000044";
const TOKEN_RESOURCE: u64 = 7_001;
const ROLE_RENEW: usize = 16;
const ROLE_ADMIN_UNREGISTER: usize = 140;
const ROLE_ADMIN_SET_RESOLVER: usize = 152;
const ROLE_CAN_TRANSFER_ADMIN: usize = 156;

fn roles(resource: U256, account: &str, bits: &[usize]) -> Result<alloy_primitives::LogData> {
    Ok(EACRolesChanged {
        resource,
        account: account.parse()?,
        oldRoleBitmap: U256::ZERO,
        newRoleBitmap: bits
            .iter()
            .fold(U256::ZERO, |bitmap, bit| bitmap | (U256::from(1) << *bit)),
    }
    .encode_log_data())
}

#[tokio::test]
async fn registration_locked_roles_count_the_real_registry_root_admins() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let pool = &database.pool;
    bigname_manifests::sync_schema_v2_repository(
        pool,
        &bigname_manifests::load_repository(checked_in_profile())?,
    )
    .await?;
    let resource = U256::from(TOKEN_RESOURCE);
    seed_raw_facts(
        pool,
        [
            (
                11_820_399,
                NEW_REGISTRY,
                RegistryCreated {}.encode_log_data(),
            ),
            (
                11_821_600,
                NEW_REGISTRY,
                LabelRegistered {
                    tokenId: token("rooted"),
                    labelHash: keccak256(b"rooted"),
                    label: "rooted".to_owned(),
                    owner: SENDER.parse()?,
                    expiry: 1_900_000_000,
                    sender: SENDER.parse()?,
                }
                .encode_log_data(),
            ),
            (
                11_821_601,
                NEW_REGISTRY,
                TokenResource {
                    tokenId: token("rooted"),
                    resource,
                }
                .encode_log_data(),
            ),
            (
                11_821_602,
                NEW_REGISTRY,
                roles(resource, SENDER, &[ROLE_RENEW])?,
            ),
            (
                11_821_603,
                NEW_REGISTRY,
                roles(
                    U256::ZERO,
                    REGISTRY_OWNER,
                    &[
                        ROLE_ADMIN_UNREGISTER,
                        ROLE_ADMIN_SET_RESOLVER,
                        ROLE_CAN_TRANSFER_ADMIN,
                    ],
                )?,
            ),
        ],
    )
    .await?;
    complete_phases(pool).await?;
    interpret_and_project(pool, RunMode::Normal).await?;

    let (registration, instance): (Uuid, String) = sqlx::query_as(
        "SELECT resource_id, after_state ->> 'registry_contract_instance_id'
         FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RegistrationGranted' AND resource_id IS NOT NULL",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    let adapter_roots: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT resource_id FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RootPermissionChanged'",
    )
    .bind(CHAIN)
    .fetch_all(pool)
    .await?;
    // Drift pin: the serving read derives the root id it never stores; it must be the root
    // resource the adapter actually created.
    let root = bigname_storage::ens_v2_registry_root_resource_id(CHAIN, instance.parse()?);
    assert_eq!(adapter_roots, vec![root]);
    let root_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resources WHERE resource_id = $1 AND chain_id = $2",
    )
    .bind(root)
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    assert_eq!(root_rows, 1);

    // The root's admin_unregister and admin_set_resolver unlock their roles; its
    // can_transfer_admin does not, because a transfer checks only the token's own roles.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L536-L539 @ ens_v2_sepolia_20261001@07e55a05)
    let (status, permissions) = get(
        &database,
        &format!("/v1/permissions?registration_id={registration}"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{permissions:#}");
    assert_eq!(
        permissions["restrictions"],
        json!({
            "kind": "ens_v2_registry",
            "registration_id": registration.to_string(),
            "locked_roles": ["renew", "set_subregistry", "transfer"],
        }),
        "{permissions:#}"
    );

    let (status, names) = get(
        &database,
        &format!("/v1/addresses/{SENDER}/names?namespace=ens&include=role_summary"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{names:#}");
    let row = names["data"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|row| row["name"] == "rooted.eth")
        .unwrap_or_else(|| panic!("rooted.eth row: {names:#}"));
    assert_eq!(
        row["restrictions"]["locked_roles"],
        json!(["renew", "set_subregistry", "transfer"]),
        "{names:#}"
    );
    database.cleanup().await
}
