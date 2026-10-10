//! `GET /v1/resolvers/{chain_id}/{address}/records?name=`: the record inventory a resolver holds
//! for a name's node, whatever the registry points at (docs/api-v1-routes.md). The Sepolia cases
//! run raw logs through Engine and Project with the checked-in profile; the Mainnet cases use the
//! family fixtures of the name records route, so both routes read one database.
use super::v2_continuing_lease::{
    ENS_REGISTRY, LEASE, Logs, NAME, OWNER, SECOND_OWNER, publish, registered, registered_with,
};
use super::v2_sepolia_redeploy::{NEW_REGISTRY, get};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use axum::http::{HeaderMap, StatusCode, header};

sol! {
    event NewResolver(bytes32 indexed node, address resolver);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
    event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
    event AddrChanged(bytes32 indexed node, address a);
    event ResolverUpdated(uint256 indexed tokenId, address indexed resolver, address indexed sender);
}

const SEPOLIA: u64 = 11_155_111;
/// The latest ENS Labs PublicResolver and an older generation, both in the checked-in Sepolia
/// profile.
const PUBLIC_RESOLVER: &str = "0xe99638b40e4fff0129d56f03b55b6bbc4bbe49b5";
const OTHER_RESOLVER: &str = "0x8948458626811dd0c23eb25cc74291247077cc51";
/// The ENSv1 mirror resolver the checked-in Sepolia profile declares.
const MIRROR_RESOLVER: &str = "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const DAY: u64 = 86_400;
const KEYS: &str = "keys=addr:60,text:avatar,text:description";
/// The ENSv1 resolver of `seed_alice_name_inputs`.
const ALICE_RESOLVER: &str = "0x0000000000000000000000000000000000000abc";

fn lease_node() -> B256 {
    let label = NAME.strip_suffix(".eth").expect("a .eth name");
    let eth = keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat());
    keccak256([eth.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
}

fn pointer(resolver: &str) -> Result<Logs> {
    Ok(vec![(
        ENS_REGISTRY,
        NewResolver {
            node: lease_node(),
            resolver: resolver.parse()?,
        }
        .encode_log_data(),
    )])
}

/// `text:avatar`, `text:description` and `addr:60` on `resolver`. `setAddr` for coin 60 emits
/// `AddressChanged` and then `AddrChanged`.
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L59-L62 @ ens_v1@91c966f)
fn records(resolver: &'static str, avatar: &str) -> Result<Logs> {
    let node = lease_node();
    let text = |key: &str, value: &str| {
        (
            resolver,
            TextChanged {
                node,
                indexedKey: keccak256(key.as_bytes()),
                key: key.into(),
                value: value.into(),
            }
            .encode_log_data(),
        )
    };
    let owner: Address = OWNER.parse()?;
    Ok(vec![
        text("avatar", avatar),
        text("description", "a lease profile"),
        (
            resolver,
            AddressChanged {
                node,
                coinType: U256::from(60),
                newAddress: owner.to_vec().into(),
            }
            .encode_log_data(),
        ),
        (resolver, AddrChanged { node, a: owner }.encode_log_data()),
    ])
}

/// The ENSv2 registry sets the reservation's resolver.
fn reservation_resolver(resolver: &str) -> Result<Logs> {
    let label = NAME.strip_suffix(".eth").expect("a .eth name");
    let token = U256::from_be_bytes(keccak256(label.as_bytes()).0) >> 32 << 32;
    Ok(vec![(
        NEW_REGISTRY,
        ResolverUpdated {
            tokenId: token,
            resolver: resolver.parse()?,
            sender: OWNER.parse()?,
        }
        .encode_log_data(),
    )])
}

/// Premigration's `register` with a resolver, in one transaction: `_registerV1` registers the
/// lease, sets the ENSv1 registry resolver and writes the records, then `_premigrate` reserves
/// the name with `ENSV1Resolver` as its ENSv2 resolver.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L177-L178 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L217-L229 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/testnet/TestnetV1PremigrationRegistrar.sol:L249-L266 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L498-L513 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deploy/testnet/01_TestnetV1PremigrationRegistrar.ts:L55 @ ens_v2_sepolia_20261001@07e55a05)
fn premigrated(avatar: &str) -> Result<Logs> {
    logs(vec![
        registered_with(
            None,
            OWNER,
            LEASE,
            logs(vec![
                pointer(PUBLIC_RESOLVER),
                records(PUBLIC_RESOLVER, avatar),
            ])?,
        ),
        reservation_resolver(MIRROR_RESOLVER),
    ])
}

fn logs(parts: Vec<Result<Logs>>) -> Result<Logs> {
    Ok(parts.into_iter().collect::<Result<Vec<_>>>()?.concat())
}

fn served(avatar: &str) -> Value {
    json!({
        "addr:60": {"status": "ok", "value": OWNER},
        "text:avatar": {"status": "ok", "value": avatar},
        "text:description": {"status": "ok", "value": "a lease profile"},
    })
}

fn route(chain: u64, resolver: &str, name: &str, query: &str) -> String {
    format!("/v1/resolvers/{chain}/{resolver}/records?name={name}&{query}")
}

async fn resolver_records(database: &TestDatabase, resolver: &str, query: &str) -> Result<Value> {
    let (status, body) = get(database, &route(SEPOLIA, resolver, NAME, query)).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(
        body["data"]["resolver"],
        json!({"chain_id": SEPOLIA, "address": resolver}),
        "{body:#}"
    );
    assert_eq!(body["meta"]["source"], json!("indexed"), "{body:#}");
    Ok(body)
}

async fn name_records(database: &TestDatabase) -> Result<Value> {
    let (status, body) = get(
        database,
        &format!("/v1/names/{NAME}/records?namespace=ens&{KEYS}"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    Ok(body)
}

fn assert_withheld(body: &Value) {
    assert!(body["data"]["resolver"].is_null(), "{body:#}");
    let records = body["data"]["records"].as_object().expect("records");
    assert_eq!(records.len(), 3, "{body:#}");
    assert!(
        records.values().all(|answer| answer["status"] != "ok"),
        "{body:#}"
    );
}

/// One fixture, published before and after the reservation lapses, with the name premigrated as
/// `premigrated` writes it. While the reservation is live, the name records route serves the
/// ENSv1 registry's resolver and its records. Once the reservation has expired and the lease is
/// in its 90-day grace, the name route withholds both. The resolver route serves the same records
/// both times.
#[tokio::test]
async fn serves_records_for_a_lapsed_reservation_the_name_route_withholds() -> Result<()> {
    for (head, live) in [(LEASE - 80, true), (LEASE + 62 * DAY + 2 + 3_600, false)] {
        let database = TestDatabase::new_migrated().await?;
        publish(
            &database,
            vec![(LEASE - 100, premigrated("lapsed.png")?), (head, vec![])],
        )
        .await?;
        let (status, detail) = get(&database, &format!("/v1/names/{NAME}?namespace=ens")).await?;
        assert_eq!(status, StatusCode::OK, "{detail:#}");
        let name = name_records(&database).await?;
        if live {
            assert!(
                detail["data"].get("unresolvable_reason").is_none(),
                "{detail:#}"
            );
            assert_eq!(
                detail["data"]["resolver"]["address"],
                json!(PUBLIC_RESOLVER),
                "{detail:#}"
            );
            assert_eq!(
                name["data"]["resolver"]["address"],
                json!(PUBLIC_RESOLVER),
                "{name:#}"
            );
            assert_eq!(name["data"]["records"], served("lapsed.png"), "{name:#}");
        } else {
            assert_eq!(
                detail["data"]["unresolvable_reason"],
                json!("no_live_ens_v2_entry"),
                "{detail:#}"
            );
            assert!(detail["data"].get("resolver").is_none(), "{detail:#}");
            assert_withheld(&name);
            for record in name["data"]["records"].as_object().unwrap().values() {
                assert_eq!(record["status"], json!("not_found"), "{name:#}");
            }
        }

        let body = resolver_records(
            &database,
            PUBLIC_RESOLVER,
            &format!("{KEYS}&include=inventory"),
        )
        .await?;
        assert_eq!(body["data"]["records"], served("lapsed.png"), "{body:#}");
        assert_eq!(body["data"]["namespace"], json!("ens"), "{body:#}");
        // The name's ENSv1 registry pointer names the resolver this route answers for, both
        // while the name route serves it and once it withholds it.
        assert_eq!(
            detail["data"]["ens_v1"]["resolver"]["address"], body["data"]["resolver"]["address"],
            "{detail:#} {body:#}"
        );
        assert_eq!(
            body["data"]["resolver"]["address"],
            json!(PUBLIC_RESOLVER),
            "{body:#}"
        );
        assert_eq!(
            body["data"]["inventory"]["known_keys"],
            json!(["addr:60", "text:avatar", "text:description"]),
            "{body:#}"
        );
        // Without keys the route answers every key the inventory holds.
        let unkeyed = resolver_records(&database, PUBLIC_RESOLVER, "").await?;
        assert_eq!(
            unkeyed["data"]["records"],
            served("lapsed.png"),
            "{unkeyed:#}"
        );
        database.cleanup().await?;
    }
    Ok(())
}

/// The lease ended 90 days after its expiry with nobody registering it again. Neither expiry nor
/// release writes the registry resolver, so the records stay where they were written.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L155 @ ens_v1@91c966f)
#[tokio::test]
async fn serves_records_for_a_released_lease() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (
                LEASE - 100,
                logs(vec![
                    registered(None, OWNER, LEASE),
                    pointer(PUBLIC_RESOLVER),
                    records(PUBLIC_RESOLVER, "released.png"),
                ])?,
            ),
            (LEASE + 90 * DAY + 10, vec![]),
        ],
    )
    .await?;
    let (status, detail) = get(&database, &format!("/v1/names/{NAME}?namespace=ens")).await?;
    assert_eq!(status, StatusCode::OK, "{detail:#}");
    assert_eq!(detail["data"]["status"], json!("released"), "{detail:#}");
    assert_withheld(&name_records(&database).await?);
    let body = resolver_records(&database, PUBLIC_RESOLVER, KEYS).await?;
    assert_eq!(body["data"]["records"], served("released.png"), "{body:#}");
    database.cleanup().await
}

/// A new owner registers the released name and points it at another resolver. Each resolver
/// answers its own inventory: the old one keeps the old owner's records for the node.
#[tokio::test]
async fn serves_each_resolver_its_own_inventory_after_a_re_registration() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let again = LEASE + 90 * DAY + 20;
    publish(
        &database,
        vec![
            (
                LEASE - 100,
                logs(vec![
                    registered(None, OWNER, LEASE),
                    pointer(PUBLIC_RESOLVER),
                    records(PUBLIC_RESOLVER, "first.png"),
                ])?,
            ),
            (LEASE + 90 * DAY + 10, vec![]),
            (
                again,
                logs(vec![
                    registered(Some(OWNER), SECOND_OWNER, again + 365 * DAY),
                    pointer(OTHER_RESOLVER),
                    records(OTHER_RESOLVER, "second.png"),
                ])?,
            ),
        ],
    )
    .await?;
    let name = name_records(&database).await?;
    assert_eq!(
        name["data"]["resolver"]["address"],
        json!(OTHER_RESOLVER),
        "{name:#}"
    );
    let old = resolver_records(&database, PUBLIC_RESOLVER, KEYS).await?;
    assert_eq!(old["data"]["records"], served("first.png"), "{old:#}");
    let new = resolver_records(&database, OTHER_RESOLVER, KEYS).await?;
    assert_eq!(new["data"]["records"], served("second.png"), "{new:#}");
    assert_eq!(new["data"]["records"], name["data"]["records"], "{name:#}");
    database.cleanup().await
}

#[tokio::test]
async fn serves_records_after_the_registry_pointer_was_cleared() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (
                LEASE - 100,
                logs(vec![
                    registered(None, OWNER, LEASE),
                    pointer(PUBLIC_RESOLVER),
                    records(PUBLIC_RESOLVER, "cleared.png"),
                ])?,
            ),
            (LEASE - 90, pointer(ZERO)?),
        ],
    )
    .await?;
    let name = name_records(&database).await?;
    assert!(name["data"]["resolver"].is_null(), "{name:#}");
    let body = resolver_records(&database, PUBLIC_RESOLVER, KEYS).await?;
    assert_eq!(body["data"]["records"], served("cleared.png"), "{body:#}");
    database.cleanup().await
}

/// The pointer moves to the public resolver and on to another one in one block. The route reads
/// the resolver it is asked about, not the latest pointer.
#[tokio::test]
async fn reads_the_resolver_not_the_latest_pointer_when_the_pointer_moved_twice_in_a_block()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (
                LEASE - 100,
                logs(vec![
                    registered(None, OWNER, LEASE),
                    records(PUBLIC_RESOLVER, "twice.png"),
                ])?,
            ),
            (
                LEASE - 90,
                logs(vec![pointer(PUBLIC_RESOLVER), pointer(OTHER_RESOLVER)])?,
            ),
        ],
    )
    .await?;
    let name = name_records(&database).await?;
    assert_eq!(
        name["data"]["resolver"]["address"],
        json!(OTHER_RESOLVER),
        "{name:#}"
    );
    let body = resolver_records(&database, PUBLIC_RESOLVER, KEYS).await?;
    assert_eq!(body["data"]["records"], served("twice.png"), "{body:#}");
    let other = resolver_records(&database, OTHER_RESOLVER, KEYS).await?;
    for record in other["data"]["records"].as_object().unwrap().values() {
        assert_eq!(record, &json!({"status": "not_found"}), "{other:#}");
    }
    let unkeyed = resolver_records(&database, OTHER_RESOLVER, "").await?;
    assert_eq!(unkeyed["data"]["records"], json!({}), "{unkeyed:#}");
    database.cleanup().await
}

/// The registry pointer is set in the block whose time ends the lease's grace.
#[tokio::test]
async fn serves_records_when_the_pointer_was_set_in_the_release_block() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (
                LEASE - 100,
                logs(vec![
                    registered(None, OWNER, LEASE),
                    records(PUBLIC_RESOLVER, "release-block.png"),
                ])?,
            ),
            (LEASE + 90 * DAY + 10, pointer(PUBLIC_RESOLVER)?),
        ],
    )
    .await?;
    let (status, detail) = get(&database, &format!("/v1/names/{NAME}?namespace=ens")).await?;
    assert_eq!(status, StatusCode::OK, "{detail:#}");
    assert_eq!(detail["data"]["status"], json!("released"), "{detail:#}");
    let body = resolver_records(&database, PUBLIC_RESOLVER, KEYS).await?;
    assert_eq!(
        body["data"]["records"],
        served("release-block.png"),
        "{body:#}"
    );
    database.cleanup().await
}

async fn read(database: &TestDatabase, uri: &str) -> Result<(StatusCode, HeaderMap, Value)> {
    read_with(database, Request::builder().uri(uri).body(Body::empty())?).await
}

async fn read_with(
    database: &TestDatabase,
    request: Request<Body>,
) -> Result<(StatusCode, HeaderMap, Value)> {
    let response = app_router(database.app_state()).oneshot(request).await?;
    let (status, headers) = (response.status(), response.headers().clone());
    let bytes = to_bytes(response.into_body(), usize::MAX).await?;
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)?
    };
    Ok((status, headers, body))
}

async fn ok(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, _, body) = read(database, uri).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    Ok(body)
}

const ALICE_KEYS: &str = "keys=addr:60,contenthash,text:avatar,text:description";

fn alice_route(name: &str, query: &str) -> String {
    route(1, ALICE_RESOLVER, name, query)
}

/// While the name records route serves the name's resolver, the resolver route at that resolver
/// gives the same answers, key for key, with and without `keys`.
#[tokio::test]
async fn agrees_with_the_name_route_while_the_name_route_serves() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    for query in [
        format!("{ALICE_KEYS}&include=inventory"),
        "include=inventory".to_owned(),
    ] {
        let name = ok(&database, &format!("/v1/names/alice.eth/records?{query}")).await?;
        assert_eq!(
            name["data"]["resolver"],
            json!({"chain_id": 1, "address": ALICE_RESOLVER}),
            "{name:#}"
        );
        let body = ok(&database, &alice_route("alice.eth", &query)).await?;
        assert_eq!(body["data"], name["data"], "{query}");
    }
    database.cleanup().await
}

/// Wrapping changes the name's owner and authority, not its resolver: the same resolver answers
/// the same inventory before and after.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L368-L370 @ ens_v1@91c966f)
#[tokio::test]
async fn answers_the_same_inventory_wrapped_and_unwrapped() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let unwrapped = ok(&database, &alice_route("alice.eth", ALICE_KEYS)).await?;
    assert_eq!(
        unwrapped["data"]["records"]["text:avatar"],
        json!({"status": "ok", "value": "https://example.test/avatar.png"}),
        "{unwrapped:#}"
    );
    // The NameWrapper's wrap of a `.eth` second-level name, as `v2_ens_v1_object.rs` seeds it.
    let lease_expiry: u64 = ok(&database, "/v1/names/alice.eth").await?["data"]["expires_at"]
        .as_str()
        .context("expires_at")?
        .parse()?;
    let holder = "0x00000000000000000000000000000000000000aa";
    append_alice_name_input(
        &database,
        "AuthorityEpochChanged",
        "ens_v1_wrapper_l1",
        json!({"source_event": "NameWrapped", "authority_kind": "wrapper", "owner": holder}),
    )
    .await?;
    append_alice_name_input(
        &database,
        "TokenControlTransferred",
        "ens_v1_wrapper_l1",
        json!({"source_event": "NameWrapped", "owner": holder}),
    )
    .await?;
    // PARENT_CANNOT_CONTROL | IS_DOT_ETH, which every wrapped `.eth` second-level name has: each
    // wrap path goes through `_wrapETH2LD`, which ORs both fuses in.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L272 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1015 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/INameWrapper.sol:L18-L19 @ ens_v1@91c966f)
    append_alice_name_input(
        &database,
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"source_event": "NameWrapped", "wrapper_state": "emancipated", "fuses": 196_608}),
    )
    .await?;
    append_alice_name_input(
        &database,
        "ExpiryChanged",
        "ens_v1_wrapper_l1",
        json!({"source_event": "NameWrapped", "expiry": lease_expiry + 90 * DAY}),
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let detail = ok(&database, "/v1/names/alice.eth").await?;
    assert_eq!(
        detail["data"]["ens_v1"]["wrapper_state"],
        json!("emancipated"),
        "{detail:#}"
    );
    let wrapped = ok(&database, &alice_route("alice.eth", ALICE_KEYS)).await?;
    assert_eq!(wrapped["data"], unwrapped["data"], "{wrapped:#}");
    database.cleanup().await
}

/// A resolver with an overview row whose implementation is not an admitted profile: every key
/// is unsupported with the row's reason, as on the name records route.
#[tokio::test]
async fn answers_unsupported_per_key_for_an_unsupported_classification() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_unknown_resolver_inputs(&database, &unknown_resolver_record_writes()).await?;
    let refused = json!({
        "status": "unsupported",
        "unsupported_reason": "resolver_implementation_unknown"
    });
    for query in [
        "keys=addr:60,text:description,text:url&include=inventory",
        "include=inventory",
    ] {
        let body = ok(&database, &alice_route("alice.eth", query)).await?;
        let name = ok(&database, &format!("/v1/names/alice.eth/records?{query}")).await?;
        assert_eq!(body["data"]["records"], name["data"]["records"], "{query}");
        assert_eq!(
            body["data"]["inventory"], name["data"]["inventory"],
            "{query}"
        );
        assert!(
            body["data"]["records"]
                .as_object()
                .unwrap()
                .values()
                .all(|answer| answer == &refused),
            "{query}: {body:#}"
        );
        assert_eq!(
            body["data"]["inventory"]["known_keys"],
            json!([]),
            "{query}: {body:#}"
        );
    }
    database.cleanup().await
}

/// A resolver no admitted source family ever named has no overview row, even when it emitted
/// record events, and answers 404 as the overview does.
#[tokio::test]
async fn answers_not_found_without_an_overview_row() -> Result<()> {
    const UNNAMED: &str = "0x00000000000000000000000000000000000dead1";
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let mut write = history_event(
        "unnamed-resolver-write",
        None,
        None,
        Some("ethereum-mainnet"),
        Some(21_000_003),
        Some("0xbinding"),
        Some("0xunnamed"),
        Some(950),
        CanonicalityState::Canonical,
    );
    write.event_kind = "RecordChanged".into();
    write.source_family = "ens_v1_resolver_l1".into();
    write.raw_fact_ref = json!({"kind": "raw_log", "emitting_address": UNNAMED,
        "transaction_index": 0});
    write.before_state = json!({});
    write.after_state = family_fixture_record_write("text:description", Some(json!("unnamed")));
    write.after_state["node"] = json!(bigname_lookup::ens_namehash_hex("alice.eth")?);
    write.after_state["resolver"] = json!(UNNAMED);
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[write]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let (status, _, body) = read(&database, &route(1, UNNAMED, "alice.eth", ALICE_KEYS)).await?;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body:#}");
    assert_eq!(
        body["error"],
        json!({"code": "not_found", "details": {},
               "message": format!("resolver {UNNAMED} was not found on chain 1")}),
        "{body:#}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn answers_empty_records_for_a_node_with_no_inventory() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let body = ok(&database, &alice_route("nobody.eth", "include=inventory")).await?;
    assert_eq!(
        body["data"],
        json!({
            "namespace": "ens",
            "resolver": {"chain_id": 1, "address": ALICE_RESOLVER},
            "records": {},
            "inventory": {"known_keys": [], "unset_keys": [], "unsupported_keys": [],
                          "abi_content_types": []},
        }),
        "{body:#}"
    );
    let keyed = ok(&database, &alice_route("nobody.eth", "keys=text:avatar")).await?;
    assert_eq!(
        keyed["data"]["records"],
        json!({"text:avatar": {"status": "not_found"}}),
        "{keyed:#}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn normalizes_and_accepts_bracketed_labelhash_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let alice = format!("[{}]", alloy_primitives::hex::encode(keccak256(b"alice")));
    let eth = format!("[{}]", alloy_primitives::hex::encode(keccak256(b"eth")));
    let expected = ok(&database, &alice_route("alice.eth", ALICE_KEYS)).await?;
    assert_eq!(
        expected["data"]["records"]["text:description"],
        json!({"status": "ok", "value": "Alice profile"}),
        "{expected:#}"
    );
    for name in [
        "Alice.ETH".to_owned(),
        format!("{alice}.eth"),
        format!("{alice}.{eth}"),
    ] {
        let body = ok(&database, &alice_route(&name, ALICE_KEYS)).await?;
        assert_eq!(body, expected, "{name}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn rejects_bad_input() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    let base = format!("/v1/resolvers/1/{ALICE_RESOLVER}/records");
    let upper = format!(
        "[{}]",
        alloy_primitives::hex::encode(keccak256(b"alice")).to_ascii_uppercase()
    );
    for (uri, message) in [
        (base.clone(), "name is required".to_owned()),
        (format!("{base}?name="), "name is required".to_owned()),
        (format!("{base}?name=%20"), "name is required".to_owned()),
        (
            format!("{base}?name={upper}.eth"),
            format!("bracketed labelhash {upper} must be lowercase hex"),
        ),
        (
            format!("{base}?name=alice.eth&page_size=1"),
            "unknown query parameter: page_size".to_owned(),
        ),
        (
            format!("{base}?name=alice.eth&source=verified"),
            "source must be indexed".to_owned(),
        ),
        (
            format!("{base}?name=alice.eth&source=auto&keys=text:avatar"),
            "source must be indexed".to_owned(),
        ),
        (
            format!("{base}?name=alice.eth&namespace=basenames"),
            "namespace must be ens for a resolver on chain 1".to_owned(),
        ),
        (
            format!("{base}?name=alice.base.eth"),
            "namespace must be ens for a resolver on chain 1".to_owned(),
        ),
        (
            format!("{base}?name=alice.eth&keys=text:avatar,text:avatar"),
            "keys must not contain duplicate record keys".to_owned(),
        ),
        (
            format!("{base}?name=alice.eth&include=counts"),
            "include must contain only inventory".to_owned(),
        ),
        (
            format!("/v1/resolvers/2/{ALICE_RESOLVER}/records?name=alice.eth"),
            "chain_id must be a supported numeric EVM chain id".to_owned(),
        ),
        (
            "/v1/resolvers/1/0xabc/records?name=alice.eth".to_owned(),
            "address must be a 0x-prefixed 20-byte hex string".to_owned(),
        ),
    ] {
        let (status, headers, body) = read(&database, &uri).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body:#}");
        assert_eq!(
            body["error"],
            json!({"code": "invalid_input", "details": {}, "message": message}),
            "{uri}"
        );
        assert!(headers.get(header::ETAG).is_none(), "{uri}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn answers_stale_off_the_current_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    // A time before the publication.
    let uri = alice_route("alice.eth", "at=2024-01-02T03:04:05Z");
    let (status, _, body) = read(&database, &uri).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{uri}: {body:#}");
    assert_eq!(body["error"]["code"], json!("stale"), "{uri}: {body:#}");
    // The publication itself is servable under an explicit `at`.
    let body = ok(&database, &alice_route("alice.eth", ALICE_KEYS)).await?;
    let token = body["meta"]["as_of_token"]
        .as_str()
        .expect("token")
        .to_owned();
    let pinned = ok(
        &database,
        &alice_route("alice.eth", &format!("{ALICE_KEYS}&at={token}")),
    )
    .await?;
    assert_eq!(pinned["data"], body["data"], "{pinned:#}");
    database.cleanup().await
}

/// An ENSv2-only name whose registry points at the manifest-declared PublicResolverV2: its
/// records are kept in the guarded partition, which the resolver read selects by the resolver's
/// own classification.
#[tokio::test]
async fn serves_guarded_ens_v2_records_by_resolver_and_node() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    seed_abi_public_resolver_v2_name(&database, "v2only.eth", &[]).await?;
    let query = "keys=text:url&include=inventory";
    let name = ok(&database, &format!("/v1/names/v2only.eth/records?{query}")).await?;
    let body = ok(
        &database,
        &route(1, PUBLIC_RESOLVER_V2, "v2only.eth", query),
    )
    .await?;
    assert_eq!(
        body["data"]["records"],
        json!({"text:url": {"status": "ok", "value": "https://example.test"}}),
        "{body:#}"
    );
    assert_eq!(body["data"]["records"], name["data"]["records"], "{name:#}");
    assert_eq!(
        body["data"]["inventory"], name["data"]["inventory"],
        "{name:#}"
    );
    database.cleanup().await
}

/// The path names the ENSv1 mirror resolver the Sepolia profile declares: the route answers what
/// the mirror's ENSv1 registry walk selects for the node, and keeps the mirror as `data.resolver`.
/// The family mirror fixture of the name records tests (`v2_mirror_records_database`) publishes no
/// collection snapshot, so resolver-scoped routes answer it `409 stale`. This case runs the
/// checked-in Sepolia profile instead.
#[tokio::test]
async fn follows_the_mirror_walk_for_a_mirror_resolver() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (LEASE - 100, premigrated("mirror.png")?),
            (LEASE - 80, vec![]),
        ],
    )
    .await?;
    // The mirror holds no records itself: the walk reads the ENSv1 registry's resolver for the
    // node and serves what that resolver holds. The reservation's ENSv2 resolver names the
    // mirror, which gives it an overview row.
    let body = resolver_records(&database, MIRROR_RESOLVER, KEYS).await?;
    assert_eq!(body["data"]["records"], served("mirror.png"), "{body:#}");
    // The live ENSv1 lease decides the name, so the name route names the ENSv1 registry's
    // resolver and serves the same records. `docs/api-v1-routes.md:1319-1322` keeps the mirror as
    // `data.resolver` only for a name its ENSv2 entry decides.
    let name = name_records(&database).await?;
    assert_eq!(
        name["data"]["resolver"]["address"],
        json!(PUBLIC_RESOLVER),
        "{name:#}"
    );
    assert_eq!(name["data"]["records"], served("mirror.png"), "{name:#}");
    let direct = resolver_records(&database, PUBLIC_RESOLVER, KEYS).await?;
    assert_eq!(
        body["data"]["records"], direct["data"]["records"],
        "{direct:#}"
    );
    database.cleanup().await
}

/// A name bigname has never seen, at the declared ENSv1 mirror resolver: no node the mirror's
/// walk consults has a projected ENSv1 resolver, so the route reports what it cannot see instead
/// of an empty inventory, as the name records route does for such a walk.
#[tokio::test]
async fn an_unseen_name_at_a_mirror_resolver_is_unsupported() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (LEASE - 100, premigrated("mirror.png")?),
            (LEASE - 80, vec![]),
        ],
    )
    .await?;
    let unseen = async |query: &str| {
        let (status, body) = get(
            &database,
            &route(SEPOLIA, MIRROR_RESOLVER, "nobody.eth", query),
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body:#}");
        anyhow::Ok(body)
    };
    let keyed = unseen(KEYS).await?;
    let refused = json!({
        "status": "unsupported",
        "unsupported_reason": "mirrored_resolver_not_projected"
    });
    assert_eq!(
        keyed["data"]["records"],
        json!({"addr:60": refused, "text:avatar": refused, "text:description": refused}),
        "{keyed:#}"
    );
    let unkeyed = unseen("").await?;
    assert_eq!(unkeyed["data"]["records"], json!({}), "{unkeyed:#}");
    database.cleanup().await
}

/// With `keys` and no `include=inventory`, the read loads only those keys and the sources of the
/// answers derived for them, and answers them as the full read does.
#[tokio::test]
async fn bounds_a_keyed_read_to_its_keys() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs_with_writes(
        &database,
        &[
            family_fixture_record_write(
                "addr:2147483648",
                Some(json!("0x0000000000000000000000000000000000000def")),
            ),
            family_fixture_record_write("avatar", Some(json!("https://example.test/avatar.png"))),
            family_fixture_record_write("contenthash", Some(json!("ipfs://alice"))),
            family_fixture_record_write("text:description", Some(json!("Alice profile"))),
            family_fixture_record_write("text:url", Some(json!("https://example.test"))),
            family_fixture_record_write("text:com.github", Some(json!("alice"))),
            family_fixture_record_write("text:com.twitter", Some(json!("alice"))),
        ],
    )
    .await?;
    enable_alice_ensip19_inputs(&database).await?;
    // `addr:2147483658` (OP Mainnet) is derived from the default address.
    let keys = "keys=addr:2147483658,avatar,text:description,text:missing";
    let work = |uri: String| {
        let database = &database;
        async move {
            let observations = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let body = bigname_storage::families::records::seams::with_lookup_work(
                observations.clone(),
                ok(database, &uri),
            )
            .await?;
            let loaded = |stage: &str| {
                observations
                    .lock()
                    .expect("observations")
                    .iter()
                    .filter(|observation| observation["stage"] == stage)
                    .map(|observation| {
                        (
                            observation["key_only"].as_bool().unwrap_or_default(),
                            observation["candidate_rows_loaded"]
                                .as_u64()
                                .unwrap_or_default(),
                        )
                    })
                    .collect::<Vec<_>>()
            };
            anyhow::Ok((body, loaded("partition_sources")))
        }
    };
    let (full, full_rows) = work(alice_route(
        "alice.eth",
        &format!("{keys}&include=inventory"),
    ))
    .await?;
    let (keyed, keyed_rows) = work(alice_route("alice.eth", keys)).await?;
    assert_eq!(
        keyed["data"]["records"]["addr:2147483658"]["meta"]["source_record_key"],
        json!("addr:2147483648"),
        "{keyed:#}"
    );
    assert_eq!(
        keyed["data"]["records"], full["data"]["records"],
        "{keyed:#}"
    );
    assert_eq!(full_rows, vec![(false, 7)], "{full:#}");
    // The default address, `avatar` and `text:description`.
    assert_eq!(keyed_rows, vec![(true, 3)], "{keyed:#}");
    database.cleanup().await
}

#[tokio::test]
async fn carries_indexed_cache_headers() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_name_inputs(&database).await?;
    for uri in [
        alice_route("alice.eth", ALICE_KEYS),
        alice_route("alice.eth", &format!("{ALICE_KEYS}&source=indexed")),
    ] {
        let (status, headers, _) = read(&database, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}");
        let etag = headers
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok())
            .with_context(|| format!("{uri}: ETag"))?
            .to_owned();
        assert_eq!(
            headers
                .get(header::CACHE_CONTROL)
                .map(|value| value.as_bytes()),
            Some("public, max-age=12, stale-while-revalidate=48".as_bytes()),
            "{uri}"
        );
        let (status, headers, body) = read_with(
            &database,
            Request::builder()
                .uri(&uri)
                .header(header::IF_NONE_MATCH, &etag)
                .body(Body::empty())?,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_MODIFIED, "{uri}");
        assert!(body.is_null(), "{uri}: {body:#}");
        assert_eq!(
            headers
                .get(header::ETAG)
                .and_then(|value| value.to_str().ok()),
            Some(etag.as_str()),
            "{uri}"
        );
    }
    let unknown = route(
        1,
        "0x00000000000000000000000000000000000dead2",
        "alice.eth",
        "",
    );
    for uri in [unknown, alice_route("alice.eth", "source=verified")] {
        let (status, headers, _) = read(&database, &uri).await?;
        assert!(status.is_client_error(), "{uri}");
        assert!(headers.get(header::ETAG).is_none(), "{uri}");
        assert!(headers.get(header::CACHE_CONTROL).is_none(), "{uri}");
    }
    database.cleanup().await
}
