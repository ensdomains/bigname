//! A registry-only child wrapped and unwrapped again, through Engine, Project and the public
//! permission routes. `grantparent.eth` is registered and left unwrapped. Its owner creates
//! `child.grantparent.eth` for a first account with a resolver, that account wraps it to
//! itself through `wrap`, then unwraps it to a second account. The NameWrapper is never the
//! registry owner after the unwrap, so it holds nothing on the child's registry-only resource.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L364-L374 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L403-L414 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
use super::v2_continuing_lease::{ENS_REGISTRY, Logs, publish};
use super::v2_sepolia_redeploy::{NEW_REGISTRY, get};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};

const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const NAME_WRAPPER: &str = "0x0635513f179d50a207757e05759cbd106d7dfce8";
const PARENT_OWNER: &str = "0x0000000000000000000000000000000000000061";
const FIRST_OWNER: &str = "0x0000000000000000000000000000000000000062";
const SECOND_OWNER: &str = "0x0000000000000000000000000000000000000063";
const RESOLVER: &str = "0x0000000000000000000000000000000000000064";
const BATCH_REGISTRAR: &str = "0x4a4c8b7cdab6b19dc2cdb417cdb53a2ccbaf5322";
const ENS_V1_RESOLVER: &str = "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0";
const LEASE: u64 = 2_000_000_000;
/// The default bonus `preMigration.ts` adds on top of the v1 expiry.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/script/preMigration.ts:L754 @ ens_v2_sepolia_20261001@07e55a05)
const BONUS_PERIOD: u64 = 62 * 86_400;

sol! {
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
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

fn under(parent: B256, label: &str) -> B256 {
    keccak256([parent.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
}

fn parent() -> B256 {
    under(under(B256::ZERO, "eth"), "grantparent")
}

fn child() -> B256 {
    under(parent(), "child")
}

/// The parent's ENSv1 registration, the reservation `preMigration.ts` makes for it, the
/// child's creation with a resolver, FIRST_OWNER's `wrap` and its `unwrap` to SECOND_OWNER, one
/// block each.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/BatchRegistrar.sol:L61-L65 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L498-L513 @ ens_v2_sepolia_20261001@07e55a05)
fn blocks() -> Result<Vec<Logs>> {
    let hash = keccak256(b"grantparent");
    let parent_owner: Address = PARENT_OWNER.parse()?;
    let first: Address = FIRST_OWNER.parse()?;
    let second: Address = SECOND_OWNER.parse()?;
    let child_token = U256::from_be_bytes(child().0);
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L152 @ ens_v1@91c966f)
    let registered = vec![
        (
            BASE_REGISTRAR,
            Transfer {
                from: Address::ZERO,
                to: parent_owner,
                tokenId: U256::from_be_bytes(hash.0),
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            NewOwner {
                node: under(B256::ZERO, "eth"),
                label: hash,
                owner: parent_owner,
            }
            .encode_log_data(),
        ),
        (
            BASE_REGISTRAR,
            NameRegistered {
                id: U256::from_be_bytes(hash.0),
                owner: parent_owner,
                expires: U256::from(LEASE),
            }
            .encode_log_data(),
        ),
    ];
    let reservation_token = U256::from_be_bytes(hash.0) >> 32 << 32;
    let reserved = vec![
        (
            NEW_REGISTRY,
            LabelReserved {
                tokenId: reservation_token,
                labelHash: hash,
                label: "grantparent".into(),
                expiry: LEASE + BONUS_PERIOD,
                sender: BATCH_REGISTRAR.parse()?,
            }
            .encode_log_data(),
        ),
        (
            NEW_REGISTRY,
            ResolverUpdated {
                tokenId: reservation_token,
                resolver: ENS_V1_RESOLVER.parse()?,
                sender: BATCH_REGISTRAR.parse()?,
            }
            .encode_log_data(),
        ),
    ];
    let created = vec![
        (
            ENS_REGISTRY,
            NewOwner {
                node: parent(),
                label: keccak256(b"child"),
                owner: first,
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            NewResolver {
                node: child(),
                resolver: RESOLVER.parse()?,
            }
            .encode_log_data(),
        ),
    ];
    let wrapped = vec![
        (
            ENS_REGISTRY,
            ens_registry::Transfer {
                node: child(),
                owner: NAME_WRAPPER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: first,
                from: Address::ZERO,
                to: first,
                id: child_token,
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameWrapped {
                node: child(),
                name: b"\x05child\x0bgrantparent\x03eth\0".to_vec().into(),
                owner: first,
                fuses: 0,
                expiry: 0,
            }
            .encode_log_data(),
        ),
    ];
    let unwrapped = vec![
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: first,
                from: first,
                to: Address::ZERO,
                id: child_token,
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            ens_registry::Transfer {
                node: child(),
                owner: second,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameUnwrapped {
                node: child(),
                owner: second,
            }
            .encode_log_data(),
        ),
    ];
    Ok(vec![registered, reserved, created, wrapped, unwrapped])
}

/// Publishes `blocks`, one batch each, and returns the child's registry-only resource: the one
/// its first registry write binds.
async fn publish_child(database: &TestDatabase, blocks: Vec<Logs>) -> Result<Uuid> {
    publish(
        database,
        blocks
            .into_iter()
            .enumerate()
            .map(|(index, logs)| (LEASE - 400 + 100 * index as u64, logs))
            .collect(),
    )
    .await?;
    Ok(sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events
         WHERE event_kind='SurfaceBound' AND logical_name_id=$1
           AND after_state->>'authority_kind'='registry_only'
         ORDER BY block_number, log_index LIMIT 1",
    )
    .bind(format!("ens:{:#x}", child()))
    .fetch_one(&database.pool)
    .await?)
}

/// Each row's (address, authority_context).
pub(super) fn served(body: &Value) -> std::collections::BTreeSet<(String, String)> {
    body["data"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    (
                        row["address"].as_str().unwrap_or_default().to_owned(),
                        row["authority_context"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The child's registry-only resource and its name serve only `owner`: as an audit of that
/// resource by registration, as the name's current authority by name, and no account in `gone`
/// keeps a row on it by address.
async fn serves_only(
    database: &TestDatabase,
    resource: Uuid,
    owner: &str,
    gone: &[&str],
) -> Result<Vec<String>> {
    serves_only_at(database, "child.grantparent.eth", resource, owner, gone).await
}

/// `serves_only` for the name `name` and its registry-only resource.
pub(super) async fn serves_only_at(
    database: &TestDatabase,
    name: &str,
    resource: Uuid,
    owner: &str,
    gone: &[&str],
) -> Result<Vec<String>> {
    let only = |context: &str| {
        [(owner.to_owned(), context.to_owned())]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
    };
    let mut failures = Vec::new();
    let (status, by_registration) = get(
        database,
        &format!("/v1/permissions?registration_id={resource}"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{by_registration:#}");
    let by_registration_served = served(&by_registration);
    if by_registration_served != only("resource_audit") {
        failures.push(format!(
            "registration {resource} serves {by_registration_served:?}: {by_registration:#}"
        ));
    }
    for subject in gone {
        let (status, by_address) =
            get(database, &format!("/v1/permissions?address={subject}")).await?;
        assert_eq!(status, StatusCode::OK, "{by_address:#}");
        let on_resource = by_address["data"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter(|row| row["registration_id"] == resource.to_string())
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !on_resource.is_empty() {
            failures.push(format!(
                "{subject} keeps rows on registration {resource}: {on_resource:#?}"
            ));
        }
    }
    let (status, by_name) = get(
        database,
        &format!("/v1/permissions?name={name}&namespace=ens"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{by_name:#}");
    let by_name_served = served(&by_name);
    if by_name_served != only("current_for_name") {
        failures.push(format!("the name serves {by_name_served:?}: {by_name:#}"));
    }
    Ok(failures)
}

/// The parent owner's registry `setSubnodeOwner` of the child to `owner`, as one block.
fn parent_write(owner: Address) -> Logs {
    vec![(
        ENS_REGISTRY,
        NewOwner {
            node: parent(),
            label: keccak256(b"child"),
            owner,
        }
        .encode_log_data(),
    )]
}

/// After the unwrap, the child's registry-only resource serves only SECOND_OWNER, by
/// registration, by name and by address. Neither the NameWrapper nor FIRST_OWNER keeps a row
/// on it.
#[tokio::test]
async fn registry_only_resource_reopened_by_an_unwrap_serves_only_its_new_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource = publish_child(&database, blocks()?).await?;
    let failures = serves_only(
        &database,
        resource,
        SECOND_OWNER,
        &[NAME_WRAPPER, FIRST_OWNER],
    )
    .await?;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    database.cleanup().await
}

/// While the child is wrapped, the NameWrapper is its ENSv1 registry owner, so its grants on
/// the registry-only resource match the chain. They are an audit of that resource, not the
/// name's current authority, which is the wrapper token's holder.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L16-L21 @ ens_v1@91c966f)
#[tokio::test]
async fn wrapped_child_serves_the_name_wrapper_only_as_an_audit_of_the_registry_only_resource()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let mut blocks = blocks()?;
    blocks.truncate(4);
    let resource = publish_child(&database, blocks).await?;
    let mut failures = Vec::new();
    let (status, by_registration) = get(
        &database,
        &format!("/v1/permissions?registration_id={resource}"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{by_registration:#}");
    let served = served(&by_registration);
    let expected = [(NAME_WRAPPER.to_owned(), "resource_audit".to_owned())]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    if served != expected {
        failures.push(format!(
            "registration {resource} serves {served:?}: {by_registration:#}"
        ));
    }
    let (status, by_name) = get(
        &database,
        "/v1/permissions?name=child.grantparent.eth&namespace=ens",
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{by_name:#}");
    let holders = by_name["data"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row["address"].as_str().map(str::to_owned))
                .collect::<std::collections::BTreeSet<_>>()
        })
        .unwrap_or_default();
    if holders != [FIRST_OWNER.to_owned()].into_iter().collect() {
        failures.push(format!(
            "the wrapped name serves {holders:?}, not only its holder: {by_name:#}"
        ));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    database.cleanup().await
}

/// The parent owner's registry `setSubnodeOwner` over the wrapped child hands the record to
/// SECOND_OWNER and ends the wrapper authority without an unwrap. The NameWrapper is no
/// longer the registry owner, so the child's registry-only resource serves only SECOND_OWNER,
/// by registration, by name and by address.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
#[tokio::test]
async fn registry_only_resource_reopened_by_a_parent_write_serves_only_its_new_owner() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let mut blocks = blocks()?;
    blocks.truncate(4);
    blocks.push(parent_write(SECOND_OWNER.parse()?));
    let resource = publish_child(&database, blocks).await?;
    let failures = serves_only(
        &database,
        resource,
        SECOND_OWNER,
        &[NAME_WRAPPER, FIRST_OWNER],
    )
    .await?;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    database.cleanup().await
}

/// The same handoff through the parent's zero-owner write, which ends the wrapper authority,
/// followed by its write naming SECOND_OWNER.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
#[tokio::test]
async fn registry_only_resource_reopened_after_a_parent_zero_write_serves_only_its_new_owner()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let mut blocks = blocks()?;
    blocks.truncate(4);
    blocks.push(parent_write(Address::ZERO));
    blocks.push(parent_write(SECOND_OWNER.parse()?));
    let resource = publish_child(&database, blocks).await?;
    let failures = serves_only(
        &database,
        resource,
        SECOND_OWNER,
        &[NAME_WRAPPER, FIRST_OWNER],
    )
    .await?;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    database.cleanup().await
}

/// The Sepolia Graveyard, which the ENSv1→ENSv2 migration manifest admits.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deployments/sepolia/Graveyard.json:L2 @ ens_v2_sepolia_20261001@07e55a05)
const GRAVEYARD: &str = "0xb58a90a39d13cce1d0e192b5da5c47640855b04d";

/// FIRST_OWNER's `unwrap` of the wrapped child into the admitted Graveyard, the ENSv1→ENSv2
/// migration's burn address for ENSv1 names. The locked receiver makes the same unwrap when it
/// cleans up an emancipated child in an ENSv1→ENSv2 migration. The unwrap ends the
/// NameWrapper's grants on the child's registry-only resource and grants the Graveyard nothing
/// there, so that resource serves nobody by registration, and neither the Graveyard nor the
/// NameWrapper keeps a row on it by address.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/Graveyard.sol:L20-L23 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/LockedWrapperReceiver.sol:L178-L180 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn an_unwrap_into_the_graveyard_leaves_the_registry_only_resource_serving_nobody()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let mut blocks = blocks()?;
    let graveyard: Address = GRAVEYARD.parse()?;
    let unwrapped = blocks.last_mut().expect("the unwrap block");
    unwrapped[1].1 = ens_registry::Transfer {
        node: child(),
        owner: graveyard,
    }
    .encode_log_data();
    unwrapped[2].1 = NameUnwrapped {
        node: child(),
        owner: graveyard,
    }
    .encode_log_data();
    let resource = publish_child(&database, blocks).await?;
    let mut failures = Vec::new();
    let (status, by_registration) = get(
        &database,
        &format!("/v1/permissions?registration_id={resource}"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{by_registration:#}");
    let by_registration_served = served(&by_registration);
    if !by_registration_served.is_empty() {
        failures.push(format!(
            "registration {resource} serves {by_registration_served:?}: {by_registration:#}"
        ));
    }
    for subject in [GRAVEYARD, NAME_WRAPPER] {
        let (status, by_address) =
            get(&database, &format!("/v1/permissions?address={subject}")).await?;
        assert_eq!(status, StatusCode::OK, "{by_address:#}");
        let on_resource = by_address["data"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter(|row| row["registration_id"] == resource.to_string())
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !on_resource.is_empty() {
            failures.push(format!(
                "{subject} keeps rows on registration {resource}: {on_resource:#?}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    database.cleanup().await
}
