//! `ens_v1.resolver`, the ENSv1 registry's resolver pointer for a name's node. The registry
//! getter returns the node's stored resolver
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137-L141 @ ens_v1@91c966f),
//! and `setResolver` emits `NewResolver` on every write
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L89-L95 @ ens_v1@91c966f). The
//! Sepolia tests run raw logs through Interpret and Project on the checked-in profile, which admits
//! the ENSv2 root registry, so the chain is cut over from its first block.
use super::v2_continuing_lease::{Logs, publish};
use super::v2_sepolia_redeploy::{NEW_REGISTRY, get, lookup};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_storage::families::name::seams;
use std::sync::{Arc, Mutex};

const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const OWNER: &str = "0x00000000000000000000000000000000000000e1";
/// Two admitted Sepolia PublicResolvers and an address no manifest admits.
const PUBLIC_RESOLVER: &str = "0x8fade66b79cc9f707ab26799354482eb93a5b7dd";
const OTHER_RESOLVER: &str = "0xe99638b40e4fff0129d56f03b55b6bbc4bbe49b5";
const CUSTOM_RESOLVER: &str = "0x00000000000000000000000000000000000c0ffe";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const SEPOLIA: u64 = 11_155_111;
const LEASE: u64 = 2_000_000_000;
const DAY: u64 = 86_400;

sol! {
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
}

fn node(label: &str) -> B256 {
    let eth = keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat());
    keccak256([eth.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
}

/// A BaseRegistrar registration of `label`.eth to `OWNER` until `LEASE`, and the ownerless
/// reservation premigration writes with it, 62 days past the lease
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L246-L268 @ ens_v2_sepolia_20261001@07e55a05).
fn registered(label: &str) -> Result<Logs> {
    registered_with(label, Vec::new())
}

/// `registered`, with `v1` (the ENSv1 resolver pointer) between the ENSv1 registration and the
/// reservation, as premigration's `register` orders them in one transaction
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L177-L178 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L217-L229 @ ens_v2_sepolia_20261001@07e55a05).
fn registered_with(label: &str, v1: Logs) -> Result<Logs> {
    let hash = keccak256(label.as_bytes());
    let eth = keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat());
    let owner: Address = OWNER.parse()?;
    let token = U256::from_be_bytes(hash.0);
    let mut logs = vec![
        (
            BASE_REGISTRAR,
            Transfer {
                from: Address::ZERO,
                to: owner,
                tokenId: token,
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            NewOwner {
                node: eth,
                label: hash,
                owner,
            }
            .encode_log_data(),
        ),
        (
            BASE_REGISTRAR,
            NameRegistered {
                id: token,
                owner,
                expires: U256::from(LEASE),
            }
            .encode_log_data(),
        ),
    ];
    logs.extend(v1);
    logs.push((
        NEW_REGISTRY,
        LabelReserved {
            tokenId: token >> 32 << 32,
            labelHash: hash,
            label: label.into(),
            expiry: LEASE + 62 * DAY,
            sender: owner,
        }
        .encode_log_data(),
    ));
    Ok(logs)
}

fn pointed(label: &str, resolver: &str) -> Result<(&'static str, alloy_primitives::LogData)> {
    Ok((
        ENS_REGISTRY,
        NewResolver {
            node: node(label),
            resolver: resolver.parse()?,
        }
        .encode_log_data(),
    ))
}

fn pointer(address: &str) -> Value {
    json!({"chain_id": SEPOLIA, "address": address})
}

/// The `ens_v1` object of `label`.eth on each route that lists the name: name detail, lookup
/// detail and feed, `OWNER`'s address names, the `/v1/names` expiry listing and search.
async fn ens_v1_by_route(database: &TestDatabase, label: &str) -> Result<Vec<(String, Value)>> {
    let name = format!("{label}.eth");
    let (status, detail) = get(database, &format!("/v1/names/{name}?namespace=ens")).await?;
    anyhow::ensure!(status == StatusCode::OK, "{detail}");
    let mut out = vec![("name detail".to_owned(), detail["data"]["ens_v1"].clone())];
    for profile in ["detail", "feed"] {
        let (status, body) = lookup(
            database,
            json!({"profile": profile, "inputs": [{"name": name}]}),
        )
        .await?;
        anyhow::ensure!(status == StatusCode::OK, "{body}");
        out.push((
            format!("lookup {profile}"),
            body["data"][0]["record"]["ens_v1"].clone(),
        ));
    }
    for (route, uri) in [
        (
            "address names",
            format!("/v1/addresses/{OWNER}/names?namespace=ens"),
        ),
        (
            "names listing",
            "/v1/names?namespace=ens&expires_after=0".to_owned(),
        ),
        ("search", format!("/v1/search?q={label}&namespace=ens")),
    ] {
        let (status, body) = get(database, &uri).await?;
        anyhow::ensure!(status == StatusCode::OK, "{route}: {body}");
        if let Some(row) = body["data"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|row| row["name"] == json!(name))
        {
            out.push((route.to_owned(), row["ens_v1"].clone()));
        }
    }
    Ok(out)
}

/// Every route that lists `label`.eth reports `expected` as `ens_v1.resolver`, present even
/// when null.
async fn assert_pointer(database: &TestDatabase, label: &str, expected: Value) -> Result<()> {
    for (route, ens_v1) in ens_v1_by_route(database, label).await? {
        assert_eq!(
            ens_v1.get("resolver"),
            Some(&expected),
            "{route}: {ens_v1:#}"
        );
    }
    Ok(())
}

async fn detail(database: &TestDatabase, label: &str) -> Result<Value> {
    let (status, body) = get(database, &format!("/v1/names/{label}.eth?namespace=ens")).await?;
    anyhow::ensure!(status == StatusCode::OK, "{body}");
    Ok(body["data"].clone())
}

/// Lease in its 90-day grace, reservation lapsed 62 days past the lease, chain cut over: the
/// name resolves to nothing, and `ens_v1.resolver` still reports the registry pointer on every
/// route, the stored lookup and search rows included.
#[tokio::test]
async fn v2_ens_v1_resolver_reports_the_pointer_beside_a_withheld_top_level_resolver() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (
                LEASE - 100,
                registered_with("lapsed", vec![pointed("lapsed", PUBLIC_RESOLVER)?])?,
            ),
            (LEASE + 70 * DAY, vec![]),
        ],
    )
    .await?;
    let data = detail(&database, "lapsed").await?;
    assert_eq!(data["authority"], json!("ens_v1"), "{data:#}");
    assert_eq!(
        data["unresolvable_reason"],
        json!("no_live_ens_v2_entry"),
        "{data:#}"
    );
    assert!(data.get("resolver").is_none(), "{data:#}");
    assert_eq!(
        data["ens_v1"]["expires_at"],
        json!(LEASE.to_string()),
        "{data:#}"
    );
    let routes = ens_v1_by_route(&database, "lapsed").await?;
    assert_eq!(routes.len(), 6, "{routes:#?}");
    for (route, ens_v1) in routes {
        assert_eq!(
            ens_v1["resolver"],
            pointer(PUBLIC_RESOLVER),
            "{route}: {ens_v1:#}"
        );
        assert_eq!(ens_v1, data["ens_v1"], "{route}");
    }
    database.cleanup().await
}

/// While ENSv1 resolves the name, the registry pointer is the resolver the name is served
/// through.
#[tokio::test]
async fn v2_ens_v1_resolver_equals_the_served_resolver_while_ens_v1_resolves_the_name() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let expected = json!({"chain_id": 1, "address": "0x0000000000000000000000000000000000000abc"});
    let detail = v2_name_record_payload_for_database(&database, "/v1/names/alice.eth").await?;
    assert_eq!(detail["data"]["resolver"], expected, "{detail:#}");
    assert_eq!(detail["data"]["ens_v1"]["resolver"], expected, "{detail:#}");
    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?;
    let record = &lookup["data"][0]["record"];
    assert_eq!(record["ens_v1"]["resolver"], expected, "{lookup:#}");
    database.cleanup().await
}

/// A lease past its 90-day grace is released, and nothing on chain clears the registry record's
/// resolver: `_register` writes only the owner
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L155 @ ens_v1@91c966f).
#[tokio::test]
async fn v2_ens_v1_resolver_a_released_lease_keeps_its_registry_pointer() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (
                LEASE - 100,
                registered_with("released", vec![pointed("released", PUBLIC_RESOLVER)?])?,
            ),
            (LEASE + 90 * DAY + 10, vec![]),
        ],
    )
    .await?;
    let data = detail(&database, "released").await?;
    assert_eq!(data["status"], json!("released"), "{data:#}");
    assert_eq!(data["authority"], json!("ens_v1"), "{data:#}");
    assert_pointer(&database, "released", pointer(PUBLIC_RESOLVER)).await?;
    database.cleanup().await
}

/// `setResolver(node, 0)` stores the zero address, which the getter returns: null.
#[tokio::test]
async fn v2_ens_v1_resolver_a_cleared_pointer_reads_null() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (
                LEASE - 100,
                registered_with("cleared", vec![pointed("cleared", PUBLIC_RESOLVER)?])?,
            ),
            (LEASE - 80, vec![pointed("cleared", ZERO)?]),
        ],
    )
    .await?;
    assert_pointer(&database, "cleared", Value::Null).await?;
    database.cleanup().await
}

/// A node whose pointer no event set reads null: the field says no pointer was observed.
#[tokio::test]
async fn v2_ens_v1_resolver_a_node_with_no_pointer_event_reads_null() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (LEASE - 100, registered("unpointed")?),
            (LEASE - 90, vec![]),
        ],
    )
    .await?;
    let data = detail(&database, "unpointed").await?;
    assert_eq!(data["authority"], json!("ens_v1"), "{data:#}");
    assert_pointer(&database, "unpointed", Value::Null).await?;
    database.cleanup().await
}

/// The registry stores any address as the resolver, so the field reports one no manifest
/// admits. It is a registry fact, not a claim that bigname reads records there.
#[tokio::test]
async fn v2_ens_v1_resolver_reports_a_custom_resolver_pointer() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![(
            LEASE - 100,
            registered_with("custom", vec![pointed("custom", CUSTOM_RESOLVER)?])?,
        )],
    )
    .await?;
    assert_pointer(&database, "custom", pointer(CUSTOM_RESOLVER)).await?;
    database.cleanup().await
}

/// Two `NewResolver` logs in one block: the later log is the stored resolver.
#[tokio::test]
async fn v2_ens_v1_resolver_the_later_pointer_of_a_block_wins() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (LEASE - 100, registered("twice")?),
            (
                LEASE - 90,
                vec![
                    pointed("twice", PUBLIC_RESOLVER)?,
                    pointed("twice", OTHER_RESOLVER)?,
                ],
            ),
        ],
    )
    .await?;
    assert_pointer(&database, "twice", pointer(OTHER_RESOLVER)).await?;
    database.cleanup().await
}

/// A pointer written in the block whose time releases the lease is reported on the released
/// name.
#[tokio::test]
async fn v2_ens_v1_resolver_a_pointer_set_in_the_release_block_is_reported() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (LEASE - 100, registered("lastblock")?),
            (
                LEASE + 90 * DAY + 10,
                vec![pointed("lastblock", CUSTOM_RESOLVER)?],
            ),
        ],
    )
    .await?;
    let data = detail(&database, "lastblock").await?;
    assert_eq!(data["status"], json!("released"), "{data:#}");
    assert_pointer(&database, "lastblock", pointer(CUSTOM_RESOLVER)).await?;
    database.cleanup().await
}

/// After the pointer moves, the field names the new resolver. The old one keeps the records it
/// holds for the node.
#[tokio::test]
async fn v2_ens_v1_resolver_reports_the_new_pointer_after_a_move() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (
                LEASE - 100,
                registered_with("moved", vec![pointed("moved", PUBLIC_RESOLVER)?])?,
            ),
            (LEASE - 80, vec![pointed("moved", OTHER_RESOLVER)?]),
        ],
    )
    .await?;
    assert_pointer(&database, "moved", pointer(OTHER_RESOLVER)).await?;
    database.cleanup().await
}

/// Wraps alice.eth through NameWrapper events that carry no registry write.
async fn wrap_alice(database: &TestDatabase) -> Result<()> {
    const HOLDER: &str = "0x00000000000000000000000000000000000000aa";
    append_alice_name_input(
        database,
        "AuthorityEpochChanged",
        "ens_v1_wrapper_l1",
        json!({"source_event":"NameWrapped", "authority_kind":"wrapper", "owner":HOLDER}),
    )
    .await?;
    append_alice_name_input(
        database,
        "TokenControlTransferred",
        "ens_v1_wrapper_l1",
        json!({"source_event":"NameWrapped", "owner":HOLDER}),
    )
    .await?;
    append_alice_name_input(
        database,
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"source_event":"NameWrapped", "wrapper_state":"emancipated", "fuses":196_608}),
    )
    .await?;
    append_alice_name_input(
        database,
        "ExpiryChanged",
        "ens_v1_wrapper_l1",
        json!({"source_event":"NameWrapped", "expiry":1_900_000_000u64}),
    )
    .await
}

async fn unwrap_alice(database: &TestDatabase) -> Result<()> {
    append_alice_name_input(
        database,
        "AuthorityEpochChanged",
        "ens_v1_wrapper_l1",
        json!({"source_event":"NameUnwrapped", "authority_kind":"wrapper"}),
    )
    .await
}

async fn alice_ens_v1_after_rebuild(database: &TestDatabase) -> Result<Value> {
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let detail = v2_name_record_payload_for_database(database, "/v1/names/alice.eth").await?;
    Ok(detail["data"]["ens_v1"].clone())
}

/// Wrapping sets the registry resolver only when the caller passes a non-zero one, and
/// unwrapping sets only the owner
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L368-L370 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1017-L1019 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f).
/// The field follows the registry's `NewResolver`, not the wrapping.
#[tokio::test]
async fn v2_ens_v1_resolver_wrapping_and_unwrapping_follow_the_registry_pointer() -> Result<()> {
    const SET: &str = "0x0000000000000000000000000000000000000abc";
    const WRAPPED_WITH: &str = "0x0000000000000000000000000000000000000def";
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let resolver = |address: &str| json!({"chain_id": 1, "address": address});

    // A wrap with a zero resolver argument writes no pointer, nor does the unwrap.
    wrap_alice(&database).await?;
    let ens_v1 = alice_ens_v1_after_rebuild(&database).await?;
    assert_eq!(ens_v1["wrapper_state"], json!("emancipated"), "{ens_v1:#}");
    assert_eq!(ens_v1["resolver"], resolver(SET), "{ens_v1:#}");
    unwrap_alice(&database).await?;
    let ens_v1 = alice_ens_v1_after_rebuild(&database).await?;
    assert_eq!(ens_v1["wrapper_state"], json!("unwrapped"), "{ens_v1:#}");
    assert_eq!(ens_v1["resolver"], resolver(SET), "{ens_v1:#}");
    // A wrap with a resolver argument calls the registry's `setResolver`. The unwrap after it
    // leaves the pointer where the wrap put it.
    wrap_alice(&database).await?;
    append_alice_name_input(
        &database,
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"source_event":"NewResolver",
               "node":bigname_lookup::ens_namehash_hex("alice.eth")?, "resolver":WRAPPED_WITH}),
    )
    .await?;
    let ens_v1 = alice_ens_v1_after_rebuild(&database).await?;
    assert_eq!(ens_v1["wrapper_state"], json!("emancipated"), "{ens_v1:#}");
    assert_eq!(ens_v1["resolver"], resolver(WRAPPED_WITH), "{ens_v1:#}");
    unwrap_alice(&database).await?;
    let ens_v1 = alice_ens_v1_after_rebuild(&database).await?;
    assert_eq!(ens_v1["wrapper_state"], json!("unwrapped"), "{ens_v1:#}");
    assert_eq!(ens_v1["resolver"], resolver(WRAPPED_WITH), "{ens_v1:#}");
    database.cleanup().await
}

/// A pointer block a reorg removes takes the field back to the previous pointer: a rebuild at
/// the earlier block serves it again.
#[tokio::test]
async fn v2_ens_v1_resolver_the_field_follows_the_publication_block() -> Result<()> {
    const LATER: &str = "0x0000000000000000000000000000000000000fed";
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    seed_schema_v2_lookup_head(
        &database.pool,
        "ethereum-mainnet",
        21_000_004,
        "0xlater",
        "2026-04-17T00:00:15Z",
    )
    .await?;
    let mut later = history_event(
        "alice-later-pointer",
        Some(&bigname_storage::logical_name_id_for_name(
            "ens",
            "alice.eth",
        )),
        Some(Uuid::from_u128(0x2200)),
        Some("ethereum-mainnet"),
        Some(21_000_004),
        Some("0xlater"),
        Some("0xalice-later"),
        Some(0),
        CanonicalityState::Canonical,
    );
    later.event_kind = "ResolverChanged".into();
    later.source_family = "ens_v1_registry_l1".into();
    later.before_state = json!({});
    later.after_state = json!({"source_event": "NewResolver",
        "node": bigname_lookup::ens_namehash_hex("alice.eth")?, "resolver": LATER});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[later]).await?;
    for (block, hash, expected) in [
        (21_000_004, "0xlater", LATER),
        (
            21_000_003,
            "0xbinding",
            "0x0000000000000000000000000000000000000abc",
        ),
    ] {
        rebuild_fixture_families(&database.pool, "ethereum-mainnet", block, hash).await?;
        let detail = v2_name_record_payload_for_database(&database, "/v1/names/alice.eth").await?;
        assert_eq!(
            detail["data"]["ens_v1"]["resolver"],
            json!({"chain_id": 1, "address": expected}),
            "{block}: {detail:#}"
        );
    }
    database.cleanup().await
}

/// A `NewResolver` the registry emitted for a registry child's node, which no name row composes.
fn child_pointer(identity: &str, node: &str, resolver: &str, block: i64) -> NormalizedEvent {
    family_event(
        identity,
        None,
        None,
        "ResolverChanged",
        "ens_v1_registry_l1",
        block,
        0,
        json!({"source_event": "NewResolver", "node": node, "resolver": resolver}),
    )
}

/// Registry children with no name row report their node's pointer on the address-names and
/// subnames routes, null when none was set.
#[tokio::test]
async fn v2_ens_v1_resolver_a_registry_child_without_a_name_row_reports_its_pointer() -> Result<()>
{
    const CHILD_RESOLVER: &str = "0x00000000000000000000000000000000000c41d0";
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (alpha, alpha_resource) =
        seed_family_name(&database, "alpha.eth", 0x7a5_0000, "ens_v1").await?;
    let pointed = insert_registry_child(
        &database,
        "alpha.eth",
        "pointed",
        RC_OWNER,
        202,
        Uuid::from_u128(0x7a5_0001),
    )
    .await?;
    let bare = insert_registry_child(
        &database,
        "alpha.eth",
        "bare",
        RC_OWNER,
        203,
        Uuid::from_u128(0x7a5_0002),
    )
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            family_event(
                "rp-alpha-grant",
                Some(&alpha),
                Some(alpha_resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                201,
                0,
                json!({"authority_kind": "registrar", "registrant": RC_OWNER,
                       "expiry": 1_900_000_000i64}),
            ),
            child_pointer("rp-pointed-resolver", &pointed, CHILD_RESOLVER, 204),
        ],
    )
    .await?;
    publish_test_families(&database, 240).await?;

    // Both children sit on one page, and the page reads their pointers in one statement.
    let reads = Arc::new(Mutex::new(Vec::new()));
    let rows = rows_of(
        &seams::with_registry_pointer_reads(
            reads.clone(),
            read_family_pages(
                &database,
                &format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=10"),
            ),
        )
        .await?,
    );
    assert_eq!(*reads.lock().unwrap(), [2]);
    reads.lock().unwrap().clear();
    let subnames = rows_of(
        &seams::with_registry_pointer_reads(
            reads.clone(),
            read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=10"),
        )
        .await?,
    );
    assert_eq!(*reads.lock().unwrap(), [2]);
    for (node, resolver) in [
        (&pointed, json!({"chain_id": 1, "address": CHILD_RESOLVER})),
        (&bare, Value::Null),
    ] {
        for served in [served_child(&rows, node), served_child(&subnames, node)] {
            assert_eq!(
                served["ens_v1"],
                json!({"expires_at": null, "resolver": resolver}),
                "{served:#}"
            );
        }
    }
    database.cleanup().await
}

/// A registry child whose only surface is a NameWrapper shadow omits the lifecycle fields but
/// reports its node's pointer, a registry fact about the node.
#[tokio::test]
async fn v2_ens_v1_resolver_a_lifecycle_shadow_child_omits_expires_at_but_reports_its_pointer()
-> Result<()> {
    const CHILD_RESOLVER: &str = "0x00000000000000000000000000000000000c41d1";
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (alpha, alpha_resource) =
        seed_family_name(&database, "alpha.eth", 0x7a6_0000, "ens_v1").await?;
    insert_family_label_preimage(&database.pool, b"Wrapped").await?;
    let wrapped = insert_registry_child(
        &database,
        "alpha.eth",
        "Wrapped",
        RC_WRAPPER,
        202,
        Uuid::from_u128(0x7a6_0001),
    )
    .await?;
    let wrapped_id = insert_shadow_child_surface(
        &database,
        &wrapped,
        "Wrapped",
        ("ens_v1_wrapper_l1", RC_WRAPPER),
        "NameWrapped",
        202,
    )
    .await?;
    let wrapper_resource = Uuid::from_u128(0x7a6_0021);
    upsert_test_resources(
        &database.pool,
        &[Resource {
            resource_id: wrapper_resource,
            token_lineage_id: None,
            chain_id: FAMILY_CHAIN.to_owned(),
            block_hash: "0xhistory202".to_owned(),
            block_number: 202,
            provenance: json!({"authority_kind": "wrapper"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            family_event(
                "rs-alpha-grant",
                Some(&alpha),
                Some(alpha_resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                201,
                0,
                json!({"authority_kind": "registrar", "registrant": RC_OWNER,
                       "expiry": 1_900_000_000i64}),
            ),
            family_event(
                "rs-wrapped-fuses",
                Some(&wrapped_id),
                Some(wrapper_resource),
                "PermissionScopeChanged",
                "ens_v1_wrapper_l1",
                202,
                1,
                json!({"source_event": "NameWrapped", "node": wrapped,
                       "wrapper_state": "emancipated", "fuses": 65_536,
                       "expiry": 1_900_000_000i64}),
            ),
            child_pointer("rs-wrapped-resolver", &wrapped, CHILD_RESOLVER, 203),
        ],
    )
    .await?;
    publish_test_families(&database, 240).await?;
    let subnames =
        rows_of(&read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=10").await?);
    let subname = served_child(&subnames, &wrapped);
    assert_eq!(subname["authority"], json!("ens_v1"), "{subname:#}");
    assert_eq!(
        subname["ens_v1"],
        json!({"resolver": {"chain_id": 1, "address": CHILD_RESOLVER}}),
        "{subname:#}"
    );
    database.cleanup().await
}
