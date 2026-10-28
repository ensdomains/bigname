//! Mainnet rows of the ENSv1 registry read (`v2::support::registry_read`): the checked-in mainnet
//! profile, cut over at its RootRegistry, with raw logs through Engine and Project, and the bounds
//! on the default records read's verified fill.
use super::v2_mainnet_admission::{
    DAY, MIRROR_RESOLVER, NAME, OWNER, PUBLIC_RESOLVER, REGISTERED, RESERVED, lease, publish,
    reservation, text,
};
use super::v2_sepolia_redeploy::get;
use super::*;
use alloy_primitives::{B256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use axum::http::StatusCode;

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
}

const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const MAINNET: u64 = 1;

fn namehash(name: &str) -> B256 {
    name.rsplit('.').fold(B256::ZERO, |node, label| {
        keccak256([node.as_slice(), keccak256(label.as_bytes()).as_slice()].concat())
    })
}

fn child_node() -> B256 {
    keccak256([namehash(NAME).as_slice(), keccak256(b"sub").as_slice()].concat())
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
    eprintln!("reservation without a resolver: {view:#}");
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

/// Row (sub.brantly.eth shape): a registry child of a reserved `.eth` name with its own registry
/// resolver and records. An unwrapped parent's child has no label-bearing event, so bigname knows
/// it by its label hash only. This asserts what bigname serves for it, which the registry read
/// leaves as composed: its stored resolver and its registry pointer are the same resolver.
#[tokio::test]
async fn a_child_of_a_reserved_mainnet_name_serves_as_composed() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    publish(
        &database,
        vec![
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
    let mirror = get(
        &database,
        &format!(
            "/v1/resolvers/{MAINNET}/{MIRROR_RESOLVER}/records?name={}&keys=text:avatar",
            name.replace('[', "%5B").replace(']', "%5D")
        ),
    )
    .await?;
    database.cleanup().await?;
    eprintln!("child of a reserved name: {view:#}\nmirror: {mirror:#?}");
    assert_eq!(view["ens_v1_resolver"], public_resolver(), "{view:#}");
    assert_eq!(view["resolver"], view["keyed_resolver"], "{view:#}");
    assert_eq!(view["resolver"], view["default_resolver"], "{view:#}");
    Ok(())
}

/// Row 7: the default records read's verified fill runs under the verified-execution admission.
/// With a client limit of one request per second, three default reads of a name with an
/// unretained text from one client at once make one chain read: one answers through it and two
/// answer `429 rate_limited`. An explicit `source=indexed` read from the same client is not
/// limited.
#[tokio::test]
async fn r7_the_default_read_fill_is_rate_limited_per_client() -> Result<()> {
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
    let (rpc_url, rpc_handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_text_response(
            "alice@example.test",
        )])
        .await?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("ethereum-mainnet={rpc_url}"),
        ])?)
        .await?;
    let bounds = ApiBoundsConfig {
        request_timeout_ms: 600_000,
        verified_rate_limit_per_second: 1,
        verified_rate_limit_burst: 1,
        ..ApiBoundsConfig::default()
    };
    let app = crate::app_router_with_bounds(state.clone(), state.pool.clone(), &bounds);
    let request = |uri: &str| {
        Request::builder()
            .uri(uri)
            .body(Body::empty())
            .expect("request must build")
    };
    let responses = futures_util::future::join_all(
        (0..3).map(|_| app.clone().oneshot(request("/v1/names/Alice.eth/records"))),
    )
    .await;
    let mut answers = Vec::new();
    for response in responses {
        let response = response?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        answers.push((status, payload));
    }
    let statuses: Vec<StatusCode> = answers.iter().map(|(status, _)| *status).collect();
    let limited = answers
        .iter()
        .filter(|(status, payload)| {
            *status == StatusCode::TOO_MANY_REQUESTS && payload["error"]["code"] == "rate_limited"
        })
        .count();
    assert_eq!(limited, 2, "{answers:#?}");
    let (_, filled) = answers
        .iter()
        .find(|(status, _)| *status == StatusCode::OK)
        .unwrap_or_else(|| panic!("one default read is admitted: {statuses:?}"));
    assert_eq!(
        filled["data"]["records"]["text:email"],
        json!({"status": "ok", "value": "alice@example.test"}),
        "{filled:#}"
    );
    assert_eq!(filled["meta"]["source"], "verified", "{filled:#}");
    let requests = join_primary_name_mock_rpc_requests(rpc_handle).await?;
    assert_eq!(requests.len(), 1, "{requests:#?}");

    let indexed = app
        .oneshot(request("/v1/names/Alice.eth/records?source=indexed"))
        .await?;
    assert_eq!(indexed.status(), StatusCode::OK);
    let payload: Value = read_json(indexed).await?;
    assert_eq!(
        payload["data"]["records"]["text:email"],
        json!({"status": "unsupported", "unsupported_reason": "value_not_retained"}),
        "{payload:#}"
    );
    database.cleanup().await
}
