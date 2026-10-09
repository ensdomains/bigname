use alloy_primitives::Address;
use anyhow::Result;
use serde_json::{Value, json};

use super::support;
use crate::harness::{anvil::Anvil, ens_v2, pipeline, repo_root};

const YEAR: u64 = 365 * 24 * 60 * 60;
const MOUNTED: &str = "child.management.eth";
const CLAIMED: &str = "child.unattached-claim.eth";
const ASIDE: &str = "child.aside.eth";

/// A registry's parent claim does not attach or detach it. `management.eth` points at a child
/// registry, so `child.management.eth` resolves whatever that registry claims. `setParent`
/// checks only the caller's role and stores the pair
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L175-L182 @ ens_v2_sepolia_20261001@07e55a05),
/// and a parent's subregistry pointer is read without it
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L280-L283 @ ens_v2_sepolia_20261001@07e55a05).
///
/// Three stages on one database, each resumed from the last:
///
/// - the claim points back at `management.eth`;
/// - the claim names `unattached-claim.eth`, a label the ETH registry holds no token for;
/// - the claim is cleared.
///
/// Every stage serves the child under `management.eth` on the direct read, the parent
/// listing, the owner's address names, search and lookup, and serves nothing under the
/// claimed path.
///
/// A last stage gives the registry a second mount, `aside.eth`, with a shorter term and a
/// claim that points back at it. The child is served under `aside.eth` until that token
/// expires, then under `management.eth` again. While `aside.eth` serves it,
/// `management.eth` still resolves it, as an alias path.
#[tokio::test]
async fn a_claim_that_points_nowhere_keeps_the_mount_path() -> Result<()> {
    let anvil = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = anvil.client();
    let root = repo_root();
    let deployment = ens_v2::deploy_ens_v2(&rpc, &root).await?;
    mount_eth_under_root(&rpc, &deployment).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);
    let eth_registry = deployment.eth_registry.address;

    let registry = ens_v2::deploy_child_registry(&rpc, &root, &deployment).await?;
    ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "management",
            owner: alice,
            duration_secs: YEAR,
            subregistry: registry.address,
            resolver: Address::ZERO,
        },
    )
    .await?;
    let expiry = u64::try_from(rpc.block_timestamp().await?)? + YEAR;
    let token = ens_v2::register_in_registry(
        &rpc,
        registry.address,
        deployment.deployer,
        "child",
        bob,
        expiry,
    )
    .await?;

    // The child gets a resolver, so each stage can show the name is served with it. The
    // harness deployment declares no resolver implementation, so the API serves the resolver
    // pointer and reports the record values as unsupported.
    let resolver = ens_v2::deploy_permissioned_resolver(&rpc, &root, &deployment, bob).await?;
    ens_v2::set_resolver_in_registry(&rpc, registry.address, bob, token, resolver.address).await?;
    let node = crate::harness::ens_v1::namehash(MOUNTED);

    let mut normal = support::IncrementalEnsV2Http::start(&deployment).await?;
    let mut registration = Value::Null;
    let claims = [
        (eth_registry, "management"),
        (eth_registry, "unattached-claim"),
        (Address::ZERO, ""),
    ];
    for (stage, (parent, label)) in claims.into_iter().enumerate() {
        ens_v2::set_parent(&rpc, registry.address, deployment.deployer, parent, label).await?;
        normal
            .prove_through_head(&anvil, &[("management.eth", format!("{alice:#x}"))])
            .await?;
        let api = pipeline::ProductionApi::start(&root, &mut normal.db, &anvil.url).await?;

        let (status, body) = api.get_indexed(&format!("/v1/names/{MOUNTED}")).await?;
        assert_eq!(status, 200, "stage {stage}: {body}");
        let data = &body["data"];
        assert_eq!(data["status"], "active", "stage {stage}: {body}");
        assert_eq!(data["owner"], format!("{bob:#x}"), "stage {stage}: {body}");
        assert_eq!(data["token_id"], token.to_string(), "stage {stage}: {body}");
        if stage == 0 {
            registration = data["registration_id"].clone();
        }
        assert_eq!(
            data["registration_id"], registration,
            "stage {stage}: {body}"
        );
        assert_eq!(
            data["resolver"]["address"],
            format!("{:#x}", resolver.address),
            "stage {stage}: {body}"
        );
        let (status, records) = api
            .get_indexed(&format!("/v1/names/{MOUNTED}/records"))
            .await?;
        assert_eq!(status, 200, "stage {stage}: {records}");
        assert_eq!(
            records["data"]["resolver"]["address"],
            format!("{:#x}", resolver.address),
            "stage {stage}: {records}"
        );
        // The record values are unsupported for this reason today. A harness change that
        // declares the implementation shows up here.
        let (status, keyed) = api
            .get_indexed(&format!("/v1/names/{MOUNTED}/records?keys=addr:60"))
            .await?;
        assert_eq!(status, 200, "stage {stage}: {keyed}");
        assert_eq!(
            keyed["data"]["records"]["addr:60"],
            json!({"status": "unsupported", "unsupported_reason": "resolver_implementation_unknown"}),
            "stage {stage}: {keyed}"
        );
        let (status, absent) = api.get_indexed(&format!("/v1/names/{CLAIMED}")).await?;
        assert_eq!(status, 404, "stage {stage}: {absent}");
        assert_eq!(
            absent["error"]["code"], "not_found",
            "stage {stage}: {absent}"
        );

        for (path, listed) in [
            (
                "/v1/names/management.eth/subnames?namespace=ens".to_owned(),
                true,
            ),
            (
                format!("/v1/addresses/{bob:#x}/names?namespace=ens&relation=owner"),
                true,
            ),
            (
                format!("/v1/addresses/{bob:#x}/names?namespace=ens&relation=former_owner"),
                false,
            ),
            ("/v1/search?q=child&namespace=ens".to_owned(), true),
        ] {
            let (status, body) = api.get(&path).await?;
            assert_eq!(status, 200, "stage {stage} {path}: {body}");
            // A list serves exactly the mounted name, with the same owner and status as the
            // direct read, or nothing.
            let rows = body["data"].as_array().cloned().unwrap_or_default();
            let served = rows
                .iter()
                .map(|row| {
                    (
                        &row["name"],
                        &row["status"],
                        &row["owner"],
                        &row["namehash"],
                    )
                })
                .collect::<Vec<_>>();
            let expected = [(
                &json!(MOUNTED),
                &json!("active"),
                &json!(format!("{bob:#x}")),
                &json!(format!("{node:#x}")),
            )];
            assert_eq!(
                served,
                if listed { &expected[..] } else { &[] },
                "stage {stage} {path}: {body}"
            );
        }
        let (status, children) = api
            .get("/v1/names/management.eth/subnames?namespace=ens")
            .await?;
        assert_eq!(status, 200, "stage {stage}: {children}");
        assert_eq!(
            children["data"].as_array().map(Vec::len),
            Some(1),
            "stage {stage}: {children}"
        );

        let (status, lookup) = api
            .post(
                "/v1/lookup",
                &json!({"namespace": "ens", "inputs": [
                    {"id": "mounted", "name": MOUNTED},
                    {"id": "claimed", "name": CLAIMED},
                ]}),
            )
            .await?;
        assert_eq!(status, 200, "stage {stage}: {lookup}");
        assert_eq!(lookup["data"][0]["status"], "ok", "stage {stage}: {lookup}");
        assert_eq!(
            lookup["data"][1]["status"], "not_found",
            "stage {stage}: {lookup}"
        );
        api.stop().await?;
    }

    // A second mount with a shorter term. The claim points back at it, so it chooses the
    // path. When that token expires the other mount still resolves, and the name moves to
    // it. The move is written at a block boundary, with no transaction or log position.
    // Anyone may register a second-level name, and the registry's deployer holds
    // `ROLE_SET_PARENT` on it.
    ens_v2::register_eth_name(
        &rpc,
        &deployment,
        ens_v2::RegisterEthName {
            from: alice,
            label: "aside",
            owner: alice,
            duration_secs: ens_v2::MIN_REGISTER_DURATION,
            subregistry: registry.address,
            resolver: Address::ZERO,
        },
    )
    .await?;
    ens_v2::set_parent(
        &rpc,
        registry.address,
        deployment.deployer,
        eth_registry,
        "aside",
    )
    .await?;
    for (served, ended, expired) in [(ASIDE, MOUNTED, false), (MOUNTED, ASIDE, true)] {
        if expired {
            rpc.increase_time(ens_v2::MIN_REGISTER_DURATION + 1).await?;
        }
        normal
            .prove_through_head(&anvil, &[("management.eth", format!("{alice:#x}"))])
            .await?;
        let api = pipeline::ProductionApi::start(&root, &mut normal.db, &anvil.url).await?;

        let (status, body) = api.get_indexed(&format!("/v1/names/{served}")).await?;
        assert_eq!(status, 200, "expired {expired}: {body}");
        let data = &body["data"];
        assert_eq!(data["status"], "active", "expired {expired}: {body}");
        assert_eq!(
            data["owner"],
            format!("{bob:#x}"),
            "expired {expired}: {body}"
        );
        assert_eq!(
            data["token_id"],
            token.to_string(),
            "expired {expired}: {body}"
        );
        assert_eq!(
            data["registration_id"], registration,
            "expired {expired}: {body}"
        );
        assert_eq!(
            data["resolver"]["address"],
            format!("{:#x}", resolver.address),
            "expired {expired}: {body}"
        );

        let (status, body) = api.get_indexed(&format!("/v1/names/{ended}")).await?;
        assert_eq!(status, 200, "expired {expired}: {body}");
        let data = &body["data"];
        if expired {
            // The expired path no longer resolves. Its row keeps the token's own term as its
            // status, with no owner or resolver.
            assert!(data.get("owner").is_none(), "{body}");
            assert!(data.get("resolver").is_none(), "{body}");
            assert!(data.get("canonical_name").is_none(), "{body}");
            assert_eq!(data["status"], "active", "{body}");
            assert_eq!(
                data["lapsed_registration"]["release_kind"], "expired",
                "{body}"
            );
        } else {
            // The claim move released the old path, but `management.eth` still points at
            // the registry. The path still resolves, so the direct read serves it as an alias
            // of the served path.
            assert_eq!(data["status"], "active", "{body}");
            assert_eq!(data["owner"], format!("{bob:#x}"), "{body}");
            assert_eq!(data["registration_id"], registration, "{body}");
            assert_eq!(data["canonical_name"], served, "{body}");
        }

        // The owner holds the served path. A claim move lists no former owner, because the
        // owner did not change. The expired path is listed for its former owner.
        let former = if expired { vec![json!(ended)] } else { vec![] };
        for (relation, expected) in [("owner", vec![json!(served)]), ("former_owner", former)] {
            let path = format!("/v1/addresses/{bob:#x}/names?namespace=ens&relation={relation}");
            let (status, body) = api.get(&path).await?;
            assert_eq!(status, 200, "expired {expired} {path}: {body}");
            let names = body["data"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|row| row["name"].clone())
                .collect::<Vec<_>>();
            assert_eq!(names, expected, "expired {expired} {path}: {body}");
        }
        api.stop().await?;
    }
    normal.cleanup().await
}

/// Every live mount path of a registry serves its token on the direct read, as the Universal
/// Resolver walks it
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/libraries/LibResolution.sol:L58-L85 @ ens_v2_sepolia_20261001@07e55a05).
/// Lists keep the one served path. Four stages on one database, each resumed from the last:
///
/// - `middle.eth` mounts the registry, which claims no parent, so `child.middle.eth` is
///   served, and `zulu.eth` mounts it too, so `child.zulu.eth` is an alias path;
/// - `alpha.eth`, a label with smaller bytes, moves the served path, and both other paths
///   are aliases;
/// - `zulu.eth` is pointed at an empty registry, so `child.zulu.eth` stops resolving, and the
///   registry claims `middle.eth`, which then serves the child again;
/// - `alpha.eth` expires, so `child.alpha.eth` serves its released row, and a nested registry
///   under the child registry serves `leaf.c.yankee.eth` through a new mount `yankee.eth`.
///
/// Anyone may register a second-level name with a subregistry, and the registry's deployer
/// holds `ROLE_SET_PARENT` on it.
#[tokio::test]
async fn every_live_mount_path_serves_the_token() -> Result<()> {
    let anvil = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = anvil.client();
    let root = repo_root();
    let deployment = ens_v2::deploy_ens_v2(&rpc, &root).await?;
    mount_eth_under_root(&rpc, &deployment).await?;
    let accounts = rpc.accounts().await?;
    let (alice, bob) = (accounts[1], accounts[2]);
    let eth_registry = deployment.eth_registry.address;
    let registry = ens_v2::deploy_child_registry(&rpc, &root, &deployment).await?;
    let mount = async |label: &str, duration_secs: u64| {
        ens_v2::register_eth_name(
            &rpc,
            &deployment,
            ens_v2::RegisterEthName {
                from: alice,
                label,
                owner: alice,
                duration_secs,
                subregistry: registry.address,
                resolver: Address::ZERO,
            },
        )
        .await
    };
    mount("middle", YEAR).await?;
    let expiry = u64::try_from(rpc.block_timestamp().await?)? + YEAR;
    let token = ens_v2::register_in_registry(
        &rpc,
        registry.address,
        deployment.deployer,
        "child",
        bob,
        expiry,
    )
    .await?;
    let resolver = ens_v2::deploy_permissioned_resolver(&rpc, &root, &deployment, bob).await?;
    ens_v2::set_resolver_in_registry(&rpc, registry.address, bob, token, resolver.address).await?;
    let mut normal = support::IncrementalEnsV2Http::start(&deployment).await?;
    let owner = format!("{bob:#x}");

    let prove = async |normal: &mut support::IncrementalEnsV2Http| {
        normal
            .prove_through_head(&anvil, &[("middle.eth", format!("{alice:#x}"))])
            .await
    };

    // A second mount with a larger label. The served path is unchanged.
    mount("zulu", YEAR).await?;
    prove(&mut normal).await?;
    let api = pipeline::ProductionApi::start(&root, &mut normal.db, &anvil.url).await?;
    let served = served_record(&api, "child.middle.eth", &owner).await?;
    alias(&api, "child.zulu.eth", "child.middle.eth", &served).await?;
    assert_lists(&api, &bob, "child.middle.eth", &["child.zulu.eth"]).await?;
    let (status, lookup) = api
        .post(
            "/v1/lookup",
            &json!({"namespace": "ens", "inputs": [{"name": "child.zulu.eth"}]}),
        )
        .await?;
    assert_eq!(status, 200, "{lookup}");
    assert_eq!(lookup["data"][0]["status"], "not_found", "{lookup}");
    api.stop().await?;

    // A label with smaller bytes moves the served path. The old path still resolves.
    mount("alpha", ens_v2::MIN_REGISTER_DURATION).await?;
    prove(&mut normal).await?;
    let api = pipeline::ProductionApi::start(&root, &mut normal.db, &anvil.url).await?;
    let moved = served_record(&api, "child.alpha.eth", &owner).await?;
    assert_eq!(moved["registration_id"], served["registration_id"]);
    alias(&api, "child.middle.eth", "child.alpha.eth", &moved).await?;
    alias(&api, "child.zulu.eth", "child.alpha.eth", &moved).await?;
    assert_lists(&api, &bob, "child.alpha.eth", &["child.zulu.eth"]).await?;
    api.stop().await?;

    // `zulu.eth` points at a registry with no `child`, and the claim points back at
    // `middle.eth`.
    let empty = ens_v2::deploy_child_registry(&rpc, &root, &deployment).await?;
    ens_v2::attach_subregistry(
        &rpc,
        eth_registry,
        alice,
        ens_v2::label_id("zulu"),
        empty.address,
    )
    .await?;
    ens_v2::set_parent(
        &rpc,
        registry.address,
        deployment.deployer,
        eth_registry,
        "middle",
    )
    .await?;
    prove(&mut normal).await?;
    let api = pipeline::ProductionApi::start(&root, &mut normal.db, &anvil.url).await?;
    let (status, body) = api.get_indexed("/v1/names/child.zulu.eth").await?;
    assert_eq!(status, 404, "{body}");
    let claimed = served_record(&api, "child.middle.eth", &owner).await?;
    alias(&api, "child.alpha.eth", "child.middle.eth", &claimed).await?;
    assert_lists(&api, &bob, "child.middle.eth", &["child.zulu.eth"]).await?;
    api.stop().await?;

    // `alpha.eth` expires, and a registry nested under `c` is reached through a new mount
    // `yankee.eth`.
    rpc.increase_time(ens_v2::MIN_REGISTER_DURATION + 1).await?;
    let nested = ens_v2::deploy_child_registry(&rpc, &root, &deployment).await?;
    let c = ens_v2::register_in_registry(
        &rpc,
        registry.address,
        deployment.deployer,
        "c",
        bob,
        expiry,
    )
    .await?;
    ens_v2::attach_subregistry(&rpc, registry.address, bob, c, nested.address).await?;
    ens_v2::set_parent(
        &rpc,
        nested.address,
        deployment.deployer,
        registry.address,
        "c",
    )
    .await?;
    ens_v2::register_in_registry(
        &rpc,
        nested.address,
        deployment.deployer,
        "leaf",
        bob,
        expiry,
    )
    .await?;
    mount("yankee", YEAR).await?;
    prove(&mut normal).await?;
    let api = pipeline::ProductionApi::start(&root, &mut normal.db, &anvil.url).await?;
    // The expired path no longer resolves and serves its released row.
    let (status, body) = api.get_indexed("/v1/names/child.alpha.eth").await?;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["status"], "released", "{body}");
    assert!(body["data"].get("canonical_name").is_none(), "{body}");
    assert!(body["data"].get("owner").is_none(), "{body}");
    served_record(&api, "child.middle.eth", &owner).await?;
    let leaf = served_record(&api, "leaf.c.middle.eth", &owner).await?;
    alias(&api, "leaf.c.yankee.eth", "leaf.c.middle.eth", &leaf).await?;
    api.stop().await?;
    normal.cleanup().await
}

/// Mounts the ETHRegistry at the root registry's `eth`, as the deployment does, so the
/// Universal Resolver's walk from the root reaches `.eth` names and their alias paths.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/deploy/01_ETHRegistry.ts:L39-L49 @ ens_v2_sepolia_20261001@07e55a05)
async fn mount_eth_under_root(
    rpc: &crate::harness::rpc::RpcClient,
    deployment: &ens_v2::EnsV2Deployment,
) -> Result<()> {
    let root = deployment.root_registry.address;
    ens_v2::register_in_registry(
        rpc,
        root,
        deployment.deployer,
        "eth",
        deployment.deployer,
        u64::MAX,
    )
    .await?;
    ens_v2::attach_subregistry(
        rpc,
        root,
        deployment.deployer,
        ens_v2::label_id("eth"),
        deployment.eth_registry.address,
    )
    .await
}

/// The direct read of a served path: active, owned, and with no `canonical_name`.
async fn served_record(api: &pipeline::ProductionApi, name: &str, owner: &str) -> Result<Value> {
    let (status, body) = api.get_indexed(&format!("/v1/names/{name}")).await?;
    assert_eq!(status, 200, "{name}: {body}");
    let data = body["data"].clone();
    assert_eq!(data["status"], "active", "{name}: {body}");
    assert_eq!(data["owner"], owner, "{name}: {body}");
    assert!(data.get("canonical_name").is_none(), "{name}: {body}");
    Ok(data)
}

/// `alias` serves `canonical`'s record `expected` under its own name, with `canonical_name`,
/// on the direct read and the records route.
async fn alias(
    api: &pipeline::ProductionApi,
    alias: &str,
    canonical: &str,
    expected: &Value,
) -> Result<()> {
    let (status, body) = api.get_indexed(&format!("/v1/names/{alias}")).await?;
    assert_eq!(status, 200, "{alias}: {body}");
    let mut want = expected.clone();
    want["name"] = json!(alias);
    want["display_name"] = json!(alias);
    want["namehash"] = json!(format!("{:#x}", crate::harness::ens_v1::namehash(alias)));
    want["canonical_name"] = json!(canonical);
    assert_eq!(body["data"], want, "{alias} under {canonical}");
    let (status, records) = api
        .get_indexed(&format!("/v1/names/{alias}/records"))
        .await?;
    assert_eq!(status, 200, "{alias}: {records}");
    assert_eq!(records["data"]["canonical_name"], canonical, "{records}");
    assert_eq!(
        records["data"]["resolver"], expected["resolver"],
        "{alias}: {records}"
    );
    Ok(())
}

/// The owner's names, search and the parent listing show `served` once. No list shows a path
/// that has no row of its own, and no listed row carries `canonical_name`.
async fn assert_lists(
    api: &pipeline::ProductionApi,
    owner: &Address,
    served: &str,
    never_stored: &[&str],
) -> Result<()> {
    let parent = |name: &str| name.split_once('.').map(|(_, parent)| parent.to_owned());
    let mut paths = vec![
        format!("/v1/addresses/{owner:#x}/names?namespace=ens&relation=owner"),
        "/v1/search?q=child&namespace=ens".to_owned(),
    ];
    for name in std::iter::once(served).chain(never_stored.iter().copied()) {
        let parent = parent(name).expect("a subname");
        paths.push(format!(
            "/v1/names/{parent}/subnames?namespace=ens&include_expired=true"
        ));
    }
    for path in paths {
        let (status, body) = api.get(&path).await?;
        assert_eq!(status, 200, "{path}: {body}");
        let rows = body["data"].as_array().cloned().unwrap_or_default();
        let count = |name: &str| rows.iter().filter(|row| row["name"] == name).count();
        if path.contains("relation=owner") {
            let names: Vec<&Value> = rows.iter().map(|row| &row["name"]).collect();
            assert_eq!(names, vec![&json!(served)], "{path}: {body}");
        }
        if !path.ends_with("/subnames?namespace=ens&include_expired=true")
            || parent(served).is_some_and(|parent| path.contains(&format!("/{parent}/")))
        {
            assert_eq!(count(served), 1, "{path}: {body}");
        }
        for name in never_stored {
            assert_eq!(count(name), 0, "{path}: {body}");
        }
        assert!(
            rows.iter().all(|row| row.get("canonical_name").is_none()),
            "{path}: {body}"
        );
    }
    Ok(())
}
