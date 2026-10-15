//! The Sepolia shape of a wrapped child unwrapped with no registrar lease, through Engine,
//! Project and the public routes: `vintagelabel.eth` is registered and wrapped, its holder
//! creates `unwrapped.vintagelabel.eth` wrapped for a second address through the NameWrapper,
//! and that address unwraps it to itself. The unwrap leaves the child with its registry owner.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L278 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L579-L581 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L414 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
use super::v2_continuing_lease::{ENS_REGISTRY, Logs, publish};
use super::v2_sepolia_redeploy::{NEW_REGISTRY, get, lookup};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};

const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const NAME_WRAPPER: &str = "0x0635513f179d50a207757e05759cbd106d7dfce8";
const PARENT_OWNER: &str = "0x0000000000000000000000000000000000000051";
const CHILD_OWNER: &str = "0x0000000000000000000000000000000000000053";
const BATCH_REGISTRAR: &str = "0x4a4c8b7cdab6b19dc2cdb417cdb53a2ccbaf5322";
const ENS_V1_RESOLVER: &str = "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0";
const LEASE: u64 = 2_000_000_000;
const GRACE_PERIOD: u64 = 90 * 86_400;
/// The default bonus `preMigration.ts` adds on top of the v1 expiry.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/script/preMigration.ts:L754 @ ens_v2_sepolia_20261001@07e55a05)
const BONUS_PERIOD: u64 = 62 * 86_400;
/// PARENT_CANNOT_CONTROL | IS_DOT_ETH, which `_wrapETH2LD` ORs into every `.eth` wrap.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1009-L1015 @ ens_v1@91c966f)
const DOT_ETH_FUSES: u32 = 0x30000;
const CHILD: &str = "unwrapped.vintagelabel.eth";

sol! {
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
    event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event NameUnwrapped(bytes32 indexed node, address owner);
}
mod ens_registry {
    alloy_sol_types::sol! { event Transfer(bytes32 indexed node, address owner); }
}

fn eth_node() -> B256 {
    keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat())
}

fn under(parent: B256, label: &str) -> B256 {
    keccak256([parent.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
}

fn token(node: B256) -> U256 {
    U256::from_be_bytes(node.0)
}

/// The ENSv1 registration of `vintagelabel.eth` to PARENT_OWNER, the ownerless reservation
/// `preMigration.ts` makes for it, then PARENT_OWNER's `wrapETH2LD`: the registrar token moves
/// to the NameWrapper, `reclaim` points the registry at it, and the wrapped token is minted.
/// `preMigration.ts` reserves through the BatchRegistrar with no subregistry and the ENSv1
/// resolver, so the reservation emits `LabelReserved` and `ResolverUpdated` with the BatchRegistrar as
/// sender, and no `SubregistryUpdated`.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/script/preMigration.ts:L1761-L1762 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/BatchRegistrar.sol:L61-L65 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L498-L513 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L264-L278 @ ens_v1@91c966f)
fn parent_registered_reserved_and_wrapped() -> Result<(Logs, Logs, Logs)> {
    let hash = keccak256(b"vintagelabel");
    let lease_token = U256::from_be_bytes(hash.0);
    let owner: Address = PARENT_OWNER.parse()?;
    let wrapper: Address = NAME_WRAPPER.parse()?;
    let parent = under(eth_node(), "vintagelabel");
    let reservation_token = lease_token >> 32 << 32;
    let batch_registrar: Address = BATCH_REGISTRAR.parse()?;
    let registered = vec![
        (
            BASE_REGISTRAR,
            Transfer {
                from: Address::ZERO,
                to: owner,
                tokenId: lease_token,
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            NewOwner {
                node: eth_node(),
                label: hash,
                owner,
            }
            .encode_log_data(),
        ),
        (
            BASE_REGISTRAR,
            NameRegistered {
                id: lease_token,
                owner,
                expires: U256::from(LEASE),
            }
            .encode_log_data(),
        ),
    ];
    let reserved = vec![
        (
            NEW_REGISTRY,
            LabelReserved {
                tokenId: reservation_token,
                labelHash: hash,
                label: "vintagelabel".into(),
                expiry: LEASE + BONUS_PERIOD,
                sender: batch_registrar,
            }
            .encode_log_data(),
        ),
        (
            NEW_REGISTRY,
            ResolverUpdated {
                tokenId: reservation_token,
                resolver: ENS_V1_RESOLVER.parse()?,
                sender: batch_registrar,
            }
            .encode_log_data(),
        ),
    ];
    let wrapped = vec![
        (
            BASE_REGISTRAR,
            Transfer {
                from: owner,
                to: wrapper,
                tokenId: lease_token,
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            NewOwner {
                node: eth_node(),
                label: hash,
                owner: wrapper,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: owner,
                from: Address::ZERO,
                to: owner,
                id: token(parent),
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameWrapped {
                node: parent,
                name: b"\x0cvintagelabel\x03eth\0".to_vec().into(),
                owner,
                fuses: DOT_ETH_FUSES,
                expiry: LEASE + GRACE_PERIOD,
            }
            .encode_log_data(),
        ),
    ];
    Ok((registered, reserved, wrapped))
}

/// PARENT_OWNER's NameWrapper `setSubnodeOwner` for CHILD_OWNER with fuses 0, then CHILD_OWNER's
/// `unwrap` to itself: the burn, the registry owner write, then `NameUnwrapped`.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L577-L581 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
fn child_created_then_unwrapped() -> Result<(Logs, Logs)> {
    let parent = under(eth_node(), "vintagelabel");
    let child = under(parent, "unwrapped");
    let parent_owner: Address = PARENT_OWNER.parse()?;
    let child_owner: Address = CHILD_OWNER.parse()?;
    let created = vec![
        (
            ENS_REGISTRY,
            NewOwner {
                node: parent,
                label: keccak256(b"unwrapped"),
                owner: NAME_WRAPPER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: parent_owner,
                from: Address::ZERO,
                to: child_owner,
                id: token(child),
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameWrapped {
                node: child,
                name: b"\x09unwrapped\x0cvintagelabel\x03eth\0".to_vec().into(),
                owner: child_owner,
                fuses: 0,
                expiry: LEASE + GRACE_PERIOD,
            }
            .encode_log_data(),
        ),
    ];
    let unwrapped = vec![
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: child_owner,
                from: child_owner,
                to: Address::ZERO,
                id: token(child),
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            ens_registry::Transfer {
                node: child,
                owner: child_owner,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameUnwrapped {
                node: child,
                owner: child_owner,
            }
            .encode_log_data(),
        ),
    ];
    Ok((created, unwrapped))
}

/// After the unwrap the child is served as a live name of its registry owner, on lookup and in
/// that owner's address names. It has no BaseRegistrar lease, so `ens_v1.expires_at`, the
/// lease's own expiry, is null.
/// (crates/storage/src/public_name_fields/ens_v1.rs:L11-L14 and L147-L150)
#[tokio::test]
async fn unwrapped_child_without_a_lease_serves_its_registry_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (registered, reserved, wrapped) = parent_registered_reserved_and_wrapped()?;
    let (created, unwrapped) = child_created_then_unwrapped()?;
    publish(
        &database,
        vec![
            (LEASE - 500, registered),
            (LEASE - 400, reserved),
            (LEASE - 300, wrapped),
            (LEASE - 200, created),
            (LEASE - 100, unwrapped),
        ],
    )
    .await?;

    let (status, answer) = lookup(
        &database,
        json!({"profile": "detail", "inputs": [{"name": CHILD}]}),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{answer:#}");
    let record = &answer["data"][0]["record"];
    assert_eq!(record["name"], CHILD, "{answer:#}");
    assert_eq!(record["status"], "active", "{answer:#}");
    assert_eq!(record["owner"], CHILD_OWNER, "{answer:#}");
    assert_eq!(record["manager"], CHILD_OWNER, "{answer:#}");
    assert_eq!(
        record["ens_v1"].get("expires_at"),
        Some(&Value::Null),
        "{answer:#}"
    );

    let (status, names) = get(
        &database,
        &format!("/v1/addresses/{CHILD_OWNER}/names?namespace=ens&relation=any"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{names:#}");
    assert!(
        names["data"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["name"] == CHILD)),
        "{names:#}"
    );
    database.cleanup().await
}
