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
/// expires, then under `management.eth` again.
#[tokio::test]
async fn a_claim_that_points_nowhere_keeps_the_mount_path() -> Result<()> {
    let anvil = Anvil::spawn_ethereum_sepolia().await?;
    let rpc = anvil.client();
    let root = repo_root();
    let deployment = ens_v2::deploy_ens_v2(&rpc, &root).await?;
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

        // The path the name left is a released registration with no owner or resolver.
        let (status, body) = api.get_indexed(&format!("/v1/names/{ended}")).await?;
        assert_eq!(status, 200, "expired {expired}: {body}");
        let data = &body["data"];
        assert!(data.get("owner").is_none(), "expired {expired}: {body}");
        assert!(data.get("resolver").is_none(), "expired {expired}: {body}");
        if expired {
            // A path expiry leaves the token's own term as the status of the old path.
            assert_eq!(data["status"], "active", "{body}");
            assert_eq!(
                data["lapsed_registration"]["release_kind"], "expired",
                "{body}"
            );
        } else {
            assert_eq!(data["status"], "released", "{body}");
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
