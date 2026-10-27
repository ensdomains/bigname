//! Mainnet rows of the ENSv1 registry read (`v2::support::registry_read`): the checked-in mainnet
//! profile, cut over at its RootRegistry, with raw logs through Engine and Project, and the bounds
//! on the default records read's verified fill.
use super::v2_mainnet_admission::{
    DAY, MIRROR_RESOLVER, NAME, OWNER, PUBLIC_RESOLVER, REGISTERED, RESERVED, lease, publish,
    reservation, text,
};
use super::v2_sepolia_redeploy::{get, token};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use axum::http::StatusCode;

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
    event LabelRegistered(uint256 indexed tokenId, bytes32 indexed labelHash, string label, address owner, uint64 expiry, address indexed sender);
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event SubregistryUpdated(uint256 indexed tokenId, address indexed subregistry, address indexed sender);
    event ParentUpdated(address indexed parent, string label, address indexed sender);
}

const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
/// The admitted RootRegistry and the ETHRegistry of the checked-in mainnet profile.
const ROOT: &str = "0x2553c09be6447beffdb86fa622ad41fb8f77a91e";
const ETH_REGISTRY: &str = "0xf60aba43e1f7f3dab1186ff03b561401c9df85e2";
/// The holder of `eth` in the RootRegistry.
const ROOT_HOLDER: &str = "0x0000000000000000000000000000000000000053";
const MAINNET: u64 = 1;

fn namehash(name: &str) -> B256 {
    name.rsplit('.').fold(B256::ZERO, |node, label| {
        keccak256([node.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
    })
}

fn child_node() -> B256 {
    keccak256([namehash(NAME).as_slice(), keccak256(b"sub").as_slice()].concat())
}

/// The RootRegistry's `eth` entry with the ETHRegistry as its subregistry, so the ENSv2 path walk
/// of a name below `.eth` reaches the ETHRegistry. The mainnet deployment registers `eth` in the
/// RootRegistry with the ETHRegistry as its registry and then sets the ETHRegistry's parent to
/// the RootRegistry under `eth`. The logs follow that order: `register` emits `LabelRegistered`,
/// the mint's `TransferSingle`, `TokenResource` and `SubregistryUpdated`, and `setParent` then
/// emits `ParentUpdated`. The fixture leaves out the role grant's `EACRolesChanged`. The holder
/// here stands in for the deployer. With `ParentUpdated` before `SubregistryUpdated` the rows
/// here pass too, so no reader of that pair depends on its order.
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deploy/01_ETHRegistry.ts:L39-L53 @ ens_v2_mainnet_20261002@b1f97b91)
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/deploy/01_ETHRegistry.ts:L71-L81 @ ens_v2_mainnet_20261002@b1f97b91)
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/registry/PermissionedRegistry.sol:L501-L510 @ ens_v2_mainnet_20261002@b1f97b91)
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/erc1155/ERC1155Singleton.sol:L325-L331 @ ens_v2_mainnet_20261002@b1f97b91)
/// (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/registry/PermissionedRegistry.sol:L175-L182 @ ens_v2_mainnet_20261002@b1f97b91)
fn root_eth() -> Result<super::v2_mainnet_admission::Logs> {
    let holder: Address = ROOT_HOLDER.parse()?;
    Ok(vec![
        (
            ROOT,
            LabelRegistered {
                tokenId: token("eth"),
                labelHash: keccak256(b"eth"),
                label: "eth".into(),
                owner: holder,
                expiry: u64::MAX,
                sender: holder,
            }
            .encode_log_data(),
        ),
        (
            ROOT,
            TransferSingle {
                operator: holder,
                from: Address::ZERO,
                to: holder,
                id: token("eth"),
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            ROOT,
            TokenResource {
                tokenId: token("eth"),
                resource: token("eth"),
            }
            .encode_log_data(),
        ),
        (
            ROOT,
            SubregistryUpdated {
                tokenId: token("eth"),
                subregistry: ETH_REGISTRY.parse()?,
                sender: holder,
            }
            .encode_log_data(),
        ),
        (
            ETH_REGISTRY,
            ParentUpdated {
                parent: ROOT.parse()?,
                label: "eth".into(),
                sender: holder,
            }
            .encode_log_data(),
        ),
    ])
}

/// The lease owner's `setSubnodeOwner(node, keccak("sub"), owner)`, then its own registry
/// resolver, the PublicResolver, and an avatar there.
fn child() -> Result<super::v2_mainnet_admission::Logs> {
    Ok(vec![
        (
            ENS_REGISTRY,
            NewOwner {
                node: namehash(NAME),
                label: keccak256(b"sub"),
                owner: OWNER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            NewResolver {
                node: child_node(),
                resolver: PUBLIC_RESOLVER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            PUBLIC_RESOLVER,
            TextChanged {
                node: child_node(),
                indexedKey: keccak256(b"avatar"),
                key: "avatar".into(),
                value: "child.png".into(),
            }
            .encode_log_data(),
        ),
    ])
}

/// Name detail, the keyed records read and the default records read of `name`.
async fn served(database: &TestDatabase, name: &str) -> Result<Value> {
    let path = name.replace('[', "%5B").replace(']', "%5D");
    let mut view = serde_json::Map::new();
    for (field, uri) in [
        ("detail", format!("/v1/names/{path}?namespace=ens")),
        (
            "keyed",
            format!("/v1/names/{path}/records?namespace=ens&keys=text:avatar,addr:60"),
        ),
        ("default", format!("/v1/names/{path}/records?namespace=ens")),
    ] {
        let (status, body) = get(database, &uri).await?;
        anyhow::ensure!(status == StatusCode::OK, "{uri}: {body:#}");
        view.insert(field.into(), body["data"].clone());
    }
    let view = Value::Object(view);
    Ok(json!({
        "authority": view["detail"]["authority"],
        "resolver": view["detail"]["resolver"],
        "ens_v1_resolver": view["detail"]["ens_v1"]["resolver"],
        "unresolvable_reason": view["detail"]["unresolvable_reason"],
        "resolution_unsupported_reason": view["detail"]["resolution_unsupported_reason"],
        "records": view["detail"]["records"],
        "keyed_resolver": view["keyed"]["resolver"],
        "keyed": view["keyed"]["records"],
        "default_resolver": view["default"]["resolver"],
        "default": view["default"]["records"],
    }))
}

fn public_resolver() -> Value {
    json!({"chain_id": MAINNET, "address": PUBLIC_RESOLVER})
}

/// Pin: a reservation premigration made without an ENSv2 resolver, beside the live lease. A
/// `.eth` second-level name takes no ENSv2 path walk, so the composed row serves the lease's
/// registry resolver, and its registry pointer is that same resolver, so the registry read
/// leaves it alone.
#[tokio::test]
async fn pin_a_reservation_without_a_resolver_serves_the_lease_resolver() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let reserved_only: super::v2_mainnet_admission::Logs =
        reservation()?.into_iter().take(1).collect();
    publish(
        &database,
        vec![
            (REGISTERED, lease()?),
            (REGISTERED + 60, text()?),
            (RESERVED, reserved_only),
            (RESERVED + DAY, vec![]),
        ],
    )
    .await?;
    let view = served(&database, NAME).await?;
    database.cleanup().await?;
    for field in [
        "resolver",
        "ens_v1_resolver",
        "keyed_resolver",
        "default_resolver",
    ] {
        assert_eq!(view[field], public_resolver(), "{field}: {view:#}");
    }
    assert_eq!(
        view["keyed"]["text:avatar"],
        json!({"status": "ok", "value": "lease.png"}),
        "{view:#}"
    );
    Ok(())
}

/// Pin (sub.brantly.eth shape): a registry child of a reserved `.eth` name with its own registry
/// resolver and records. An unwrapped parent's child has no label-bearing event, so bigname knows
/// it by its label hash only. The ENSv2 path walk reaches the reservation's ENSv1 mirror at the
/// parent, the mirror's ENSv1 walk finds the child's own registry resolver, and the name serves
/// that resolver and its avatar on name detail and both records reads, as the mirror's
/// resolver-anchored read does. The registry read leaves it as composed.
#[tokio::test]
async fn a_child_of_a_reserved_mainnet_name_is_served() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
            (REGISTERED - 120, root_eth()?),
            (REGISTERED, lease()?),
            (REGISTERED + 60, text()?),
            (RESERVED, reservation()?),
            (RESERVED + 60, child()?),
            (RESERVED + DAY, vec![]),
        ],
    )
    .await?;
    let name = format!("[{:x}].{NAME}", keccak256(b"sub"));
    let view = served(&database, &name).await?;
    let (status, mirror) = get(
        &database,
        &format!(
            "/v1/resolvers/{MAINNET}/{MIRROR_RESOLVER}/records?name={}&keys=text:avatar",
            name.replace('[', "%5B").replace(']', "%5D")
        ),
    )
    .await?;
    database.cleanup().await?;
    for field in [
        "resolver",
        "ens_v1_resolver",
        "keyed_resolver",
        "default_resolver",
    ] {
        assert_eq!(view[field], public_resolver(), "{field}: {view:#}");
    }
    let avatar = json!({"status": "ok", "value": "child.png"});
    assert_eq!(view["keyed"]["text:avatar"], avatar, "{view:#}");
    assert_eq!(status, StatusCode::OK, "{mirror:#}");
    assert_eq!(
        mirror["data"]["records"]["text:avatar"], avatar,
        "{mirror:#}"
    );
    Ok(())
}

/// Row 7: the default read's fill takes the client's verified rate-limit token. With a limit of
/// one request per second, three default reads of a name with an unretained text from one client
/// at once all answer `200`, and one chain read fills one of them. The other two keep the indexed
/// `value_not_retained` answer.
#[tokio::test]
async fn r7_the_default_read_fill_is_rate_limited_per_client() -> Result<()> {
    let database = alice_with_an_unretained_text().await?;
    let (rpc_url, rpc_handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_text_response(
            "alice@example.test",
        )])
        .await?;
    let app = bounded_app(
        &database,
        &rpc_url,
        ApiBoundsConfig {
            verified_rate_limit_per_second: 1,
            verified_rate_limit_burst: 1,
            ..test_bounds()
        },
    )
    .await?;
    let answers = default_reads(&app, 3).await?;
    let email: Vec<&Value> = answers
        .iter()
        .map(|payload| &payload["data"]["records"]["text:email"])
        .collect();
    let filled = answers
        .iter()
        .filter(|payload| payload["meta"]["source"] == "verified")
        .count();
    assert_eq!(filled, 1, "{answers:#?}");
    assert_eq!(
        email
            .iter()
            .filter(|answer| ***answer == json!({"status": "ok", "value": "alice@example.test"}))
            .count(),
        1,
        "{answers:#?}"
    );
    assert_eq!(
        email
            .iter()
            .filter(|answer| ***answer
                == json!({"status": "unsupported", "unsupported_reason": "value_not_retained"}))
            .count(),
        2,
        "{answers:#?}"
    );
    let requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(requests.len(), 1, "{requests:#?}");
    database.cleanup().await
}

/// Row 7b: a default read with nothing to fill takes no verified token. With a limit of one
/// request per second, two default reads of a name with every value retained answer `200` from
/// the index with no chain read, and a `source=verified` read from the same client right after
/// them still passes the limiter, so neither default read took its token.
#[tokio::test]
async fn r7b_a_default_read_with_nothing_to_fill_takes_no_admission() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    let (rpc_url, rpc_handle) = spawn_primary_name_mock_rpc(Vec::new()).await?;
    let app = bounded_app(
        &database,
        &rpc_url,
        ApiBoundsConfig {
            verified_rate_limit_per_second: 1,
            verified_rate_limit_burst: 1,
            ..test_bounds()
        },
    )
    .await?;
    for payload in default_reads(&app, 2).await? {
        assert_eq!(payload["meta"]["source"], "indexed", "{payload:#}");
        let records = payload["data"]["records"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        assert!(
            records
                .values()
                .all(|answer| answer["unsupported_reason"] != "value_not_retained"),
            "{payload:#}"
        );
    }
    let verified = app
        .oneshot(request("/v1/names/Alice.eth/records?source=verified"))
        .await?;
    assert_ne!(verified.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_ne!(verified.status(), StatusCode::SERVICE_UNAVAILABLE);
    let requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert!(requests.is_empty(), "{requests:#?}");
    database.cleanup().await
}

/// The mainnet fixture name whose resolver event named `text:email` with no retained value, on a
/// chain whose lookup is admitted.
async fn alice_with_an_unretained_text() -> Result<TestDatabase> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_name_inputs(&database).await?;
    replace_alice_record_inputs(
        &database,
        &[family_fixture_record_write("text:email", None)],
    )
    .await?;
    enable_alice_ensip19_inputs(&database).await?;
    const HASH: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    seed_schema_v2_ens_lookup_head(&database.pool, 21_000_004, HASH, "2026-04-17T00:00:04Z")
        .await?;
    seed_schema_v2_ens_manifest(
        &database.pool,
        "ens_execution",
        "universal_resolver",
        "0xeeeeeeee14d718c2b47d9923deab1335e144eeee",
        Uuid::from_u128(0xc301),
        true,
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_004, HASH).await?;
    Ok(database)
}

fn test_bounds() -> ApiBoundsConfig {
    ApiBoundsConfig {
        request_timeout_ms: 600_000,
        ..ApiBoundsConfig::default()
    }
}

/// The production router with `bounds` and the mainnet lookup pointed at `rpc_url`.
async fn bounded_app(
    database: &TestDatabase,
    rpc_url: &str,
    bounds: ApiBoundsConfig,
) -> Result<axum::Router> {
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={rpc_url}"),
        ])?)
        .await?;
    Ok(crate::app_router_with_bounds(
        state.clone(),
        state.pool.clone(),
        &bounds,
    ))
}

fn request(uri: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .expect("request must build")
}

/// `count` default records reads of `Alice.eth` sent at once, each answering `200`.
async fn default_reads(app: &axum::Router, count: usize) -> Result<Vec<Value>> {
    let responses = futures_util::future::join_all(
        (0..count).map(|_| app.clone().oneshot(request("/v1/names/Alice.eth/records"))),
    )
    .await;
    let mut answers = Vec::new();
    for response in responses {
        let response = response?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        anyhow::ensure!(status == StatusCode::OK, "{status}: {payload:#}");
        answers.push(payload);
    }
    Ok(answers)
}
