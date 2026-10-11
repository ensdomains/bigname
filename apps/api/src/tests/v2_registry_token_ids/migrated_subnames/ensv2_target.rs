//! A retained ENSv1 child whose ENSv1 registry resolver is an admitted PermissionedResolver.
//! The ENSV1Resolver mirror forwards to whatever resolver the ENSv1 registry holds for the node,
//! with no family check, and the PermissionedResolver answers for the queried node: its linked
//! record, or its default record when the node has no link.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/AbstractMirrorResolver.sol:L67-L69 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/AbstractRecordResolver.sol:L110-L123 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/resolver/PermissionedResolver.sol:L380-L386 @ ens_v2_sepolia_20261001@07e55a05)
use super::permissioned::{FACTORY, LINKED, address, deploy_linked, link, text};
use super::*;
use alloy_primitives::B256;

const V1: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const PUBLIC_RESOLVER_V2: &str = "0xdc4a563d00f5c3012b699794eb9e13a561be386f";
const DEEP: &str = "deep.child.envoy1084.eth";

/// The child's owner points its ENSv1 registry entry at `resolver`.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L86-L95 @ ens_v1@91c966f)
fn point(resolver: &str) -> Result<Vec<(Address, alloy_primitives::LogData)>> {
    Ok(vec![(
        V1.parse()?,
        NewResolver {
            node: bigname_lookup::ens_namehash_hex(CHILD)?.parse()?,
            resolver: resolver.parse()?,
        }
        .encode_log_data(),
    )])
}

/// Runs block `BASE + block`, one transaction per entry of `transactions`, each sent by the
/// child's owner. A deployment's target is the factory.
async fn step(
    database: &TestDatabase,
    block: i64,
    transactions: Vec<Vec<(Address, alloy_primitives::LogData)>>,
    deployment: Option<i64>,
) -> Result<()> {
    let mut logs = Vec::new();
    let mut callers = Vec::new();
    for (tx, events) in transactions.into_iter().enumerate() {
        logs.extend(transaction(block, tx as i64, events));
        callers.push((block, tx as i64, GRANTEE));
    }
    for (index, log) in logs.iter_mut().enumerate() {
        log.log_index = index as i64;
    }
    let targets: Vec<_> = deployment
        .map(|tx| (block, tx, FACTORY))
        .into_iter()
        .collect();
    Box::pin(seed_and_run_with_targets(
        database, &logs, block, block, &callers, &targets, None,
    ))
    .await
}

/// `setup` through block 121, where the migrated parent's ENSv2 entry names the mirror.
async fn retained() -> Result<(TestDatabase, Address)> {
    let (database, mut logs, resolver) = Box::pin(setup()).await?;
    logs.retain(|log| log.block_number <= BASE + 121);
    Box::pin(seed_and_run(&database, &logs, 120, 121)).await?;
    Ok((database, resolver))
}

/// Row 1: at block 122 the proxy is deployed, the child's ENSv1 pointer names it, and record 1
/// is linked to the child.
async fn arrived() -> Result<(TestDatabase, Address)> {
    let (database, resolver) = retained().await?;
    step(
        &database,
        122,
        vec![
            deploy_linked(GRANTEE.parse()?)?,
            point(LINKED)?,
            vec![
                link(CHILD, 1)?,
                text(1, "description", "a")?,
                address(1, HOLDER)?,
            ],
        ],
        Some(0),
    )
    .await?;
    Ok((database, resolver))
}

async fn detail(database: &TestDatabase, name: &str) -> Result<Value> {
    path_get(database, &format!("/v1/names/{name}")).await
}

fn assert_served(detail: &Value, description: &str) {
    assert_eq!(detail["data"]["resolver"]["address"], LINKED, "{detail:#}");
    assert!(
        detail["data"]["resolution_unsupported_reason"].is_null(),
        "{detail:#}"
    );
    assert!(
        detail["data"]["unresolvable_reason"].is_null(),
        "{detail:#}"
    );
    assert_eq!(
        detail["data"]["records"]["texts"]["description"], description,
        "{detail:#}"
    );
}

fn assert_withheld(detail: &Value, reason: &str) {
    assert_eq!(
        detail["data"]["resolution_unsupported_reason"], reason,
        "{detail:#}"
    );
    assert!(
        detail["data"]["unresolvable_reason"].is_null(),
        "{detail:#}"
    );
    for field in ["resolver", "records", "primary_address"] {
        assert!(detail["data"][field].is_null(), "{field}: {detail:#}");
    }
}

async fn link_dependency(database: &TestDatabase, node: &str) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM project_lookup_dependency dependency
         JOIN project_lookup_name name ON name.chain_id = dependency.chain_id
          AND name.record_serving_resource_id = dependency.resource_id
         WHERE name.logical_name_id = $1 AND dependency.kind = 'link'
           AND dependency.key1 = $2 AND dependency.key2 = $3)",
    )
    .bind(format!("ens:{}", bigname_lookup::ens_namehash_hex(CHILD)?))
    .bind(LINKED)
    .bind(node)
    .fetch_one(&database.pool)
    .await?)
}

async fn get_status(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let response = app_router(state)
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

#[tokio::test]
async fn migrated_subname_ensv1_pointer_at_permissioned_resolver_is_retained() -> Result<()> {
    let (database, _) = arrived().await?;
    assert_served(&detail(&database, CHILD).await?, "a");
    assert_consumers(&database, true, LINKED.parse()?, None).await?;
    database.cleanup().await
}

#[tokio::test]
async fn migrated_subname_ensv1_pointer_at_permissioned_resolver_serves_default_record_at_node()
-> Result<()> {
    let (database, _) = retained().await?;
    step(
        &database,
        122,
        vec![
            deploy_linked(GRANTEE.parse()?)?,
            point(LINKED)?,
            vec![
                link("", 2)?,
                text(2, "description", "default")?,
                address(2, HOLDER)?,
            ],
        ],
        Some(0),
    )
    .await?;
    assert_served(&detail(&database, CHILD).await?, "default");
    assert_consumers(&database, true, LINKED.parse()?, None).await?;
    database.cleanup().await
}

/// The chain resolves `DEEP` through the child's PermissionedResolver as a wildcard, but `DEEP`
/// has no pointer of its own, so it is a different target, never a proven absence.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/libraries/LibResolution.sol:L39-L48 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn migrated_subname_ensv1_pointer_at_permissioned_resolver_ancestor_stays_unsupported()
-> Result<()> {
    let (database, _) = arrived().await?;
    step(
        &database,
        123,
        vec![vec![(
            V1.parse()?,
            NewOwner {
                node: bigname_lookup::ens_namehash_hex(CHILD)?.parse()?,
                label: keccak256("deep"),
                owner: GRANTEE.parse()?,
            }
            .encode_log_data(),
        )]],
        None,
    )
    .await?;
    assert_withheld(
        &detail(&database, DEEP).await?,
        "ens_v2_path_target_not_projected",
    );
    database.cleanup().await
}

/// A PublicResolverV2 under a retained ENSv1 pointer is not a PermissionedResolver, so the
/// child stays withheld. Reached through an ENSv1 pointer, its classification is an undeclared
/// ENSv1 resolver, so the target is unproved.
#[tokio::test]
async fn migrated_subname_ensv1_pointer_at_public_resolver_v2_stays_unsupported() -> Result<()> {
    let (database, resolver) = retained().await?;
    step(&database, 122, vec![point(PUBLIC_RESOLVER_V2)?], None).await?;
    assert_withheld(
        &detail(&database, CHILD).await?,
        "ens_v2_path_not_projected",
    );
    assert_consumers(
        &database,
        false,
        resolver,
        Some("ens_v2_path_not_projected"),
    )
    .await?;
    database.cleanup().await
}

#[tokio::test]
async fn migrated_subname_ensv1_pointer_at_permissioned_resolver_record_updates_after_arrival()
-> Result<()> {
    let (database, _) = arrived().await?;
    step(
        &database,
        123,
        vec![vec![text(1, "description", "b")?, address(1, GRANTEE)?]],
        None,
    )
    .await?;
    assert_served(&detail(&database, CHILD).await?, "b");
    assert_name_consumers(&database, CHILD, GRANTEE, true, LINKED.parse()?, None).await?;
    assert!(
        link_dependency(&database, &bigname_lookup::ens_namehash_hex(CHILD)?).await?,
        "the child's own link is tracked"
    );
    assert!(
        link_dependency(&database, &format!("{:#x}", B256::ZERO)).await?,
        "the default link is tracked"
    );
    database.cleanup().await
}

#[tokio::test]
async fn migrated_subname_ensv1_pointer_at_permissioned_resolver_pointer_away_and_back()
-> Result<()> {
    let (database, resolver) = arrived().await?;
    step(
        &database,
        123,
        vec![vec![text(1, "description", "b")?, address(1, GRANTEE)?]],
        None,
    )
    .await?;
    step(
        &database,
        124,
        vec![point(&format!("{resolver:#x}"))?],
        None,
    )
    .await?;
    let away = detail(&database, CHILD).await?;
    assert_eq!(
        away["data"]["resolver"]["address"],
        format!("{resolver:#x}"),
        "{away:#}"
    );
    assert_consumers(&database, true, resolver, None).await?;
    let before_back = replay::families(&database).await?;
    step(&database, 125, vec![point(LINKED)?], None).await?;
    assert_served(&detail(&database, CHILD).await?, "b");
    assert_name_consumers(&database, CHILD, GRANTEE, true, LINKED.parse()?, None).await?;
    assert_eq!(
        bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 124).await?,
        1
    );
    assert_eq!(
        replay::families(&database).await?,
        before_back,
        "undo restores exact family rows"
    );
    publish(&database, 125).await?;
    assert_served(&detail(&database, CHILD).await?, "b");
    Box::pin(replay::assert_rebuild(&database, 125)).await?;
    assert_served(&detail(&database, CHILD).await?, "b");
    assert_name_consumers(&database, CHILD, GRANTEE, true, LINKED.parse()?, None).await?;
    database.cleanup().await
}

#[tokio::test]
async fn migrated_subname_ensv1_pointer_at_permissioned_resolver_classification_arrives_after_pointer()
-> Result<()> {
    let (database, resolver) = retained().await?;
    step(&database, 122, vec![point(LINKED)?], None).await?;
    assert_withheld(
        &detail(&database, CHILD).await?,
        "ens_v2_path_not_projected",
    );
    assert_consumers(
        &database,
        false,
        resolver,
        Some("ens_v2_path_not_projected"),
    )
    .await?;
    step(
        &database,
        123,
        vec![
            deploy_linked(GRANTEE.parse()?)?,
            vec![
                link(CHILD, 1)?,
                text(1, "description", "a")?,
                address(1, HOLDER)?,
            ],
        ],
        Some(0),
    )
    .await?;
    assert_served(&detail(&database, CHILD).await?, "a");
    assert_consumers(&database, true, LINKED.parse()?, None).await?;
    database.cleanup().await
}

#[tokio::test]
async fn migrated_subname_ensv1_pointer_at_permissioned_resolver_lookup_matches_detail()
-> Result<()> {
    let (database, _) = arrived().await?;
    let detail = detail(&database, CHILD).await?;
    let lookup = path_lookup(
        &database,
        json!({"profile": "detail", "inputs": [{"name": CHILD}]}),
    )
    .await?;
    for field in [
        "resolver",
        "records",
        "primary_address",
        "unresolvable_reason",
        "resolution_unsupported_reason",
    ] {
        assert_eq!(
            lookup["data"][0]["record"][field], detail["data"][field],
            "{field}: {lookup:#}"
        );
    }
    assert_eq!(detail["data"]["resolver"]["address"], LINKED, "{detail:#}");
    let names = path_get(
        &database,
        &format!("/v1/addresses/{HOLDER}/names?namespace=ens&relation=resolves_to&coin_type=60"),
    )
    .await?;
    assert!(
        names["data"]
            .as_array()
            .context("resolves-to names")?
            .iter()
            .any(|record| record["name"] == CHILD),
        "{names:#}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn migrated_subname_ensv1_pointer_at_permissioned_resolver_at_publication_matches_current()
-> Result<()> {
    let (database, _) = arrived().await?;
    let current = detail(&database, CHILD).await?;
    let token = current["meta"]["as_of_token"]
        .as_str()
        .context("an as_of token")?
        .to_owned();
    let at = path_get(&database, &format!("/v1/names/{CHILD}?at={token}")).await?;
    assert_eq!(at["data"], current["data"]);
    assert_served(&at, "a");
    let older = 1_700_000_000 + BASE + 121;
    let (status, body) = get_status(&database, &format!("/v1/names/{CHILD}?at={older}")).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{body:#}");
    assert_eq!(body["error"]["code"], "stale", "{body:#}");
    database.cleanup().await
}
