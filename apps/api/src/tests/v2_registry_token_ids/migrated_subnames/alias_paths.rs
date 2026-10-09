//! ENSv2 alias paths on the direct read and the records route (docs/api-v1.md, "ENSv2 name
//! path"), from registry logs through Interpret and Project on the Sepolia manifests.
//!
//! The base state: the ETHRegistry `E` mounts user registry `R` at `m`, `R` holds `child` with
//! a resolver, and `R` has no parent claim, so `child.m.eth` is canonical. Every row id (T1...)
//! is a row of the ordering table in the TYR-280 scope.
use std::cell::Cell;

use super::nested::deploy;
use super::*;
use crate::v2::support::ALIAS_WALK_STATEMENTS;

sol! {
    event ParentUpdated(address indexed parent, string label, address indexed sender);
}

const E: &str = "0xd4ebcbbdf463c9c45784603db0ddd499bc44a8b4";
const R: &str = "0x0000000000000000000000000000000000002801";
const S: &str = "0x0000000000000000000000000000000000002802";
const EMPTY: &str = "0x0000000000000000000000000000000000002803";
const P: &str = "0x0000000000000000000000000000000000002804";

fn address(text: &str) -> Address {
    text.parse().expect("an address")
}

/// The timestamp of block `BASE + block`.
fn clock(block: i64) -> u64 {
    (1_700_000_000 + BASE + block) as u64
}

/// Runs block `BASE + block`, one transaction per entry of `transactions`.
async fn step(
    database: &TestDatabase,
    block: i64,
    transactions: Vec<Vec<(Address, alloy_primitives::LogData)>>,
) -> Result<()> {
    let logs: Vec<RawLogInput> = transactions
        .into_iter()
        .enumerate()
        .flat_map(|(tx, events)| transaction(block, tx as i64, events))
        .collect();
    seed_and_run(database, &logs, block, block).await
}

fn repoint(
    registry: &str,
    label: &str,
    subregistry: &str,
) -> Vec<(Address, alloy_primitives::LogData)> {
    vec![(
        address(registry),
        SubregistryUpdated {
            tokenId: label_token(label),
            subregistry: address(subregistry),
            sender: HOLDER.parse().expect("an address"),
        }
        .encode_log_data(),
    )]
}

fn claim(registry: &str, parent: &str, label: &str) -> Vec<(Address, alloy_primitives::LogData)> {
    vec![(
        address(registry),
        ParentUpdated {
            parent: address(parent),
            label: label.into(),
            sender: HOLDER.parse().expect("an address"),
        }
        .encode_log_data(),
    )]
}

fn unregister(registry: &str, label: &str) -> Result<Vec<(Address, alloy_primitives::LogData)>> {
    Ok(vec![(
        address(registry),
        LabelUnregistered {
            tokenId: label_token(label),
            sender: HOLDER.parse()?,
        }
        .encode_log_data(),
    )])
}

fn mount(
    label: &str,
    registry: &str,
    expiry: u64,
) -> Result<Vec<(Address, alloy_primitives::LogData)>> {
    register(
        address(E),
        label,
        Address::ZERO,
        address(registry),
        expiry,
        DEPLOYER.parse()?,
    )
}

/// The base state at block 123, with `m` and `child` registered until `m_expiry` and
/// `child_expiry`. Returns the database and the resolver `child` uses.
async fn base(m_expiry: u64, child_expiry: u64) -> Result<(TestDatabase, Address)> {
    let (database, logs, resolver) = setup().await?;
    seed_and_run(&database, &logs, 120, 122).await?;
    let owner = HOLDER.parse()?;
    step(
        &database,
        123,
        vec![
            deploy(address(R), 2801, owner)?,
            mount("m", R, m_expiry)?,
            register(
                address(R),
                "child",
                resolver,
                Address::ZERO,
                child_expiry,
                owner,
            )?,
        ],
    )
    .await?;
    Ok((database, resolver))
}

async fn get(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let (status, body, _) = get_counting(database, uri).await?;
    Ok((status, body))
}

/// The response and the statements the alias walk ran for it.
async fn get_counting(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value, usize)> {
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    ALIAS_WALK_STATEMENTS
        .scope(Cell::new(0), async {
            let response = app_router(state)
                .oneshot(Request::builder().uri(uri).body(Body::empty())?)
                .await?;
            let status = response.status();
            let body = read_json(response).await?;
            Ok((status, body, ALIAS_WALK_STATEMENTS.with(Cell::get)))
        })
        .await
}

async fn ok(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, body) = get(database, uri).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    Ok(body)
}

pub(super) async fn not_found(database: &TestDatabase, uri: &str) -> Result<()> {
    let (status, body) = get(database, uri).await?;
    assert_eq!(status, StatusCode::NOT_FOUND, "{uri}: {body:#}");
    assert_eq!(body["error"]["code"], "not_found", "{uri}: {body:#}");
    Ok(())
}

/// `alias` serves `canonical`'s record: only the name's identity differs, and `canonical_name`
/// names the canonical path.
pub(super) async fn assert_alias(
    database: &TestDatabase,
    alias: &str,
    canonical: &str,
) -> Result<Value> {
    let served = ok(database, &format!("/v1/names/{alias}")).await?;
    let stored = ok(database, &format!("/v1/names/{canonical}")).await?;
    assert!(stored["data"].get("canonical_name").is_none(), "{stored:#}");
    let mut expected = stored["data"].clone();
    expected["name"] = json!(alias);
    expected["display_name"] = json!(alias);
    expected["namehash"] = json!(bigname_lookup::ens_namehash_hex(alias)?);
    expected["canonical_name"] = json!(canonical);
    assert_eq!(served["data"], expected, "{alias} under {canonical}");
    let records = ok(
        database,
        &format!("/v1/names/{alias}/records?source=indexed"),
    )
    .await?;
    let canonical_records = ok(
        database,
        &format!("/v1/names/{canonical}/records?source=indexed"),
    )
    .await?;
    let mut expected = canonical_records["data"].clone();
    expected["canonical_name"] = json!(canonical);
    assert_eq!(
        records["data"], expected,
        "{alias} records under {canonical}"
    );
    Ok(served)
}

/// A read whose verified lookups go to the mock RPC at `url`.
async fn verified_get(database: &TestDatabase, url: &str, uri: &str) -> Result<Value> {
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("{PATH_CHAIN}={url}")])?,
    );
    let response = app_router(state)
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    let status = response.status();
    let body: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    Ok(body)
}

async fn names(database: &TestDatabase, uri: &str) -> Result<Vec<String>> {
    let body = ok(database, uri).await?;
    Ok(body["data"]
        .as_array()
        .context("a list")?
        .iter()
        .filter_map(|row| row["name"].as_str().map(str::to_owned))
        .collect())
}

async fn subnames(database: &TestDatabase, parent: &str) -> Result<Vec<String>> {
    names(
        database,
        &format!("/v1/names/{parent}/subnames?include_expired=true&page_size=200"),
    )
    .await
}

async fn owner_names(database: &TestDatabase) -> Result<Vec<String>> {
    names(
        database,
        &format!("/v1/addresses/{HOLDER}/names?relation=owner&page_size=200"),
    )
    .await
}

fn count(names: &[String], name: &str) -> usize {
    names.iter().filter(|listed| *listed == name).count()
}

/// T1. A second mount serves the token on the direct read and the records route, counts
/// included. Lists, address names, search, lookup, the resolver and registry overviews,
/// history and subnames never show the alias.
#[tokio::test]
async fn an_added_mount_serves_the_token_under_its_path() -> Result<()> {
    let (database, resolver) = base(u64::MAX, u64::MAX).await?;
    not_found(&database, "/v1/names/child.z.eth").await?;
    let history_before = get(&database, "/v1/names/child.z.eth/history").await?;
    let subnames_before = get(&database, "/v1/names/child.z.eth/subnames").await?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;

    let served = assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    assert_eq!(served["data"]["status"], "active", "{served:#}");
    assert!(served["data"]["registration_id"].is_string(), "{served:#}");
    assert_eq!(served["data"]["owner"], HOLDER, "{served:#}");

    assert_eq!(
        count(&subnames(&database, "m.eth").await?, "child.m.eth"),
        1
    );
    assert!(subnames(&database, "z.eth").await?.is_empty());
    let owners = owner_names(&database).await?;
    assert_eq!(count(&owners, "child.m.eth"), 1, "{owners:?}");
    assert_eq!(count(&owners, "child.z.eth"), 0, "{owners:?}");
    let search = names(&database, "/v1/search?q=child&namespace=ens&page_size=200").await?;
    assert_eq!(count(&search, "child.z.eth"), 0, "{search:?}");
    let listed = names(
        &database,
        "/v1/names?namespace=ens&expires_after=0&parent=m.eth&page_size=200",
    )
    .await?;
    assert_eq!(count(&listed, "child.m.eth"), 1, "{listed:?}");
    let listed = names(
        &database,
        "/v1/names?namespace=ens&expires_after=0&parent=z.eth&page_size=200",
    )
    .await?;
    assert!(listed.is_empty(), "{listed:?}");
    for overview in [
        format!("/v1/resolvers/11155111/{resolver:#x}"),
        format!("/v1/registries/11155111/{R}"),
    ] {
        let body = ok(&database, &overview).await?;
        assert!(
            !body.to_string().contains("child.z.eth"),
            "{overview}: {body:#}"
        );
    }
    let counted = ok(&database, "/v1/names/child.z.eth?include=counts").await?;
    let canonical = ok(&database, "/v1/names/child.m.eth?include=counts").await?;
    for count in ["subname_count", "record_count"] {
        assert_eq!(
            counted["data"][count], canonical["data"][count],
            "{counted:#}"
        );
    }
    let lookup = path_lookup(&database, json!({"inputs": [{"name": "child.z.eth"}]})).await?;
    assert_eq!(lookup["data"][0]["status"], "not_found", "{lookup:#}");
    // OQ-6: history and subnames under the alias answer what they answered before it resolved.
    let history_after = get(&database, "/v1/names/child.z.eth/history").await?;
    assert_eq!(history_after.0, history_before.0);
    assert_eq!(history_after.1["data"], history_before.1["data"]);
    assert_eq!(history_after.1["error"], history_before.1["error"]);
    let subnames_after = get(&database, "/v1/names/child.z.eth/subnames").await?;
    assert_eq!(subnames_after.0, subnames_before.0);
    assert_eq!(subnames_after.1["error"], subnames_before.1["error"]);
    database.cleanup().await
}

/// T2a, T2b. Repointing the mount ends the alias. A registry holding its own `child` serves
/// its own canonical row there.
#[tokio::test]
async fn a_repointed_mount_stops_serving_the_path() -> Result<()> {
    let (database, resolver) = base(u64::MAX, u64::MAX).await?;
    let owner = HOLDER.parse()?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    step(
        &database,
        125,
        vec![deploy(address(EMPTY), 2803, owner)?, repoint(E, "z", EMPTY)],
    )
    .await?;
    not_found(&database, "/v1/names/child.z.eth").await?;
    not_found(&database, "/v1/names/child.z.eth/records").await?;
    step(
        &database,
        126,
        vec![
            deploy(address(S), 2802, owner)?,
            register(
                address(S),
                "child",
                resolver,
                Address::ZERO,
                u64::MAX,
                owner,
            )?,
            repoint(E, "z", S),
        ],
    )
    .await?;
    let own = ok(&database, "/v1/names/child.z.eth").await?;
    assert!(own["data"].get("canonical_name").is_none(), "{own:#}");
    assert_eq!(own["data"]["status"], "active", "{own:#}");
    let canonical = ok(&database, "/v1/names/child.m.eth").await?;
    assert_ne!(
        own["data"]["registration_id"], canonical["data"]["registration_id"],
        "S's token, not R's"
    );
    database.cleanup().await
}

/// T3. A mount that expires stops serving the path. The canonical path is unchanged.
#[tokio::test]
async fn an_expired_mount_stops_serving_the_path() -> Result<()> {
    let (database, _) = base(u64::MAX, u64::MAX).await?;
    step(&database, 124, vec![mount("z", R, clock(126))?]).await?;
    step(&database, 125, vec![]).await?;
    assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    step(&database, 126, vec![]).await?;
    not_found(&database, "/v1/names/child.z.eth").await?;
    let canonical = ok(&database, "/v1/names/child.m.eth").await?;
    assert_eq!(canonical["data"]["status"], "active", "{canonical:#}");
    database.cleanup().await
}

/// T4, T5. A smaller label moves the canonical path, and the released old path still resolves,
/// so it serves as an alias. A parent claim then fixes the canonical path, and the other mounts
/// stay aliases while they resolve.
#[tokio::test]
async fn a_demoted_canonical_path_is_served_as_an_alias() -> Result<()> {
    let (database, _) = base(u64::MAX, u64::MAX).await?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    step(&database, 125, vec![mount("a", R, u64::MAX)?]).await?;
    let canonical = ok(&database, "/v1/names/child.a.eth").await?;
    assert_eq!(canonical["data"]["status"], "active", "{canonical:#}");
    assert!(canonical["data"].get("canonical_name").is_none());
    let demoted = assert_alias(&database, "child.m.eth", "child.a.eth").await?;
    assert_eq!(demoted["data"]["status"], "active", "{demoted:#}");
    assert_alias(&database, "child.z.eth", "child.a.eth").await?;
    assert_eq!(
        count(&subnames(&database, "a.eth").await?, "child.a.eth"),
        1
    );
    assert!(subnames(&database, "z.eth").await?.is_empty());
    let owners = owner_names(&database).await?;
    assert_eq!(count(&owners, "child.a.eth"), 1, "{owners:?}");
    for alias in ["child.m.eth", "child.z.eth"] {
        assert_eq!(count(&owners, alias), 0, "{owners:?}");
    }
    let former = names(
        &database,
        &format!("/v1/addresses/{HOLDER}/names?relation=former_owner&page_size=200"),
    )
    .await?;
    assert_eq!(count(&former, "child.z.eth"), 0, "{former:?}");

    step(&database, 126, vec![claim(R, E, "m")]).await?;
    let claimed = ok(&database, "/v1/names/child.m.eth").await?;
    assert_eq!(claimed["data"]["status"], "active", "{claimed:#}");
    assert!(
        claimed["data"].get("canonical_name").is_none(),
        "{claimed:#}"
    );
    assert_alias(&database, "child.a.eth", "child.m.eth").await?;
    assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    let owners = owner_names(&database).await?;
    assert_eq!(count(&owners, "child.m.eth"), 1, "{owners:?}");
    assert_eq!(count(&owners, "child.a.eth"), 0, "{owners:?}");
    database.cleanup().await
}

/// T6. The claimed mount expires at a block boundary. The canonical path moves to the next
/// mount, the expired path serves its lapsed row, and the other alias follows the move.
#[tokio::test]
async fn an_expired_claimed_mount_moves_the_aliases_with_the_canonical_path() -> Result<()> {
    let (database, _) = base(clock(130), u64::MAX).await?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    step(&database, 125, vec![mount("a", R, u64::MAX)?]).await?;
    step(&database, 126, vec![claim(R, E, "m")]).await?;
    assert_alias(&database, "child.a.eth", "child.m.eth").await?;
    for block in 127..=130 {
        step(&database, block, vec![]).await?;
    }
    let canonical = ok(&database, "/v1/names/child.a.eth").await?;
    assert_eq!(canonical["data"]["status"], "active", "{canonical:#}");
    assert!(canonical["data"].get("canonical_name").is_none());
    let lapsed = ok(&database, "/v1/names/child.m.eth").await?;
    assert!(lapsed["data"].get("canonical_name").is_none(), "{lapsed:#}");
    // The path's release keeps the token's own term as its status (TYR-277).
    assert_eq!(
        lapsed["data"]["lapsed_registration"]["release_kind"], "expired",
        "{lapsed:#}"
    );
    assert_alias(&database, "child.z.eth", "child.a.eth").await?;
    database.cleanup().await
}

/// T22. A parent claim that moves the registry under a parent with no name leaves the registry
/// without a canonical name. The old canonical path is released. An alias that still resolves
/// answers `404 not_found`, not the released row, since the token is live on chain.
#[tokio::test]
async fn an_alias_of_a_registry_without_a_name_is_not_found() -> Result<()> {
    let (database, _) = base(u64::MAX, u64::MAX).await?;
    let owner = HOLDER.parse()?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    step(
        &database,
        125,
        vec![
            deploy(address(S), 2802, owner)?,
            register(address(S), "x", Address::ZERO, address(R), u64::MAX, owner)?,
            claim(R, S, "x"),
        ],
    )
    .await?;
    let released = ok(&database, "/v1/names/child.m.eth").await?;
    assert_eq!(released["data"]["status"], "released", "{released:#}");
    assert!(
        released["data"].get("canonical_name").is_none(),
        "{released:#}"
    );
    not_found(&database, "/v1/names/child.z.eth").await?;
    not_found(&database, "/v1/names/child.z.eth/records").await?;
    database.cleanup().await
}

/// T22, the lapsed binding. `S` is mounted at `z` but has no name, since its parent claim
/// points back from `P.x` and `P` has none. `S.c` mounts R, so `child.c.z.eth` resolves. When
/// R's only named mount `m` expires, `child.m.eth` is released without a successor, and the
/// alias through `S` answers `404 not_found`, not the released row.
#[tokio::test]
async fn an_alias_of_a_registry_whose_name_lapsed_is_not_found() -> Result<()> {
    let (database, _) = base(clock(127), u64::MAX).await?;
    let owner = HOLDER.parse()?;
    step(
        &database,
        124,
        vec![
            deploy(address(S), 2802, owner)?,
            deploy(address(P), 2804, owner)?,
            register(address(P), "x", Address::ZERO, address(S), u64::MAX, owner)?,
            claim(S, P, "x"),
            mount("z", S, u64::MAX)?,
            register(address(S), "c", Address::ZERO, address(R), u64::MAX, owner)?,
        ],
    )
    .await?;
    step(&database, 125, vec![]).await?;
    not_found(&database, "/v1/names/c.z.eth").await?;
    assert_alias(&database, "child.c.z.eth", "child.m.eth").await?;
    step(&database, 126, vec![]).await?;
    step(&database, 127, vec![]).await?;
    let lapsed = ok(&database, "/v1/names/child.m.eth").await?;
    assert!(lapsed["data"].get("canonical_name").is_none(), "{lapsed:#}");
    // The path's release keeps the token's own term as its status (TYR-277).
    assert_eq!(
        lapsed["data"]["lapsed_registration"]["release_kind"], "expired",
        "{lapsed:#}"
    );
    not_found(&database, "/v1/names/child.c.z.eth").await?;
    not_found(&database, "/v1/names/child.c.z.eth/records").await?;
    database.cleanup().await
}

/// T23, the R19 shape. A manifest sync withdraws the ENSv2 root registry and ETHRegistry
/// manifests. Until the redo republishes, the walk starts at the root registry the publication
/// recorded, so the alias is served unchanged. After the redo the chain has no admitted root
/// registry, and the alias answers `404 not_found`.
#[tokio::test]
async fn a_manifest_sync_reaches_the_alias_walk_only_through_the_redo() -> Result<()> {
    let (database, _) = base(u64::MAX, u64::MAX).await?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    let before = assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    for family in ["ens_v2_root_l1", "ens_v2_registry_l1"] {
        shadow_by_manifest_sync(&database.pool, PATH_CHAIN, family, &[PATH_CHAIN]).await?;
    }
    let window = assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    assert_eq!(
        window["data"], before["data"],
        "between the manifest sync and the redo"
    );
    adopt_this_build(&database.pool, &[PATH_CHAIN]).await?;
    rebuild_fixture_families(
        &database.pool,
        PATH_CHAIN,
        BASE + 124,
        &format!("0xhistory{}", BASE + 124),
    )
    .await?;
    not_found(&database, "/v1/names/child.z.eth").await?;
    database.cleanup().await
}

/// T7, T8. A nested registry under an alias prefix, and a registry mounted under itself, are
/// served because the walk follows each pointer.
#[tokio::test]
async fn nested_and_cyclic_paths_are_served() -> Result<()> {
    let (database, resolver) = base(u64::MAX, u64::MAX).await?;
    let owner = HOLDER.parse()?;
    step(
        &database,
        124,
        vec![
            mount("z", R, u64::MAX)?,
            deploy(address(S), 2802, owner)?,
            register(address(R), "c", Address::ZERO, address(S), u64::MAX, owner)?,
            claim(S, R, "c"),
            register(address(S), "leaf", resolver, Address::ZERO, u64::MAX, owner)?,
            register(
                address(R),
                "self",
                Address::ZERO,
                address(R),
                u64::MAX,
                owner,
            )?,
        ],
    )
    .await?;
    assert_alias(&database, "leaf.c.z.eth", "leaf.c.m.eth").await?;
    assert_alias(&database, "child.self.m.eth", "child.m.eth").await?;
    assert_alias(&database, "child.self.self.z.eth", "child.m.eth").await?;
    let (_, _, statements) = get_counting(&database, "/v1/names/child.self.self.z.eth").await?;
    // The publication, four hops of two statements, the leaf and the association.
    assert_eq!(statements, 11);
    database.cleanup().await
}

/// T11, T12. The token's own expiry, then its unregistration: the alias serves whatever the
/// canonical row serves.
#[tokio::test]
async fn an_alias_serves_the_canonical_rows_status_after_the_token_lapses() -> Result<()> {
    let (database, _) = base(u64::MAX, clock(126)).await?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    step(&database, 125, vec![]).await?;
    step(&database, 126, vec![]).await?;
    let expired = assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    assert_ne!(expired["data"]["status"], "active", "{expired:#}");
    step(
        &database,
        127,
        vec![vec![(
            address(R),
            LabelUnregistered {
                tokenId: label_token("child"),
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
        )]],
    )
    .await?;
    let unregistered = assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    assert_ne!(unregistered["data"]["status"], "active", "{unregistered:#}");
    database.cleanup().await
}

/// T15. A reserved token reached through a second mount serves its reservation row.
#[tokio::test]
async fn an_alias_serves_a_reserved_token() -> Result<()> {
    let (database, _) = base(u64::MAX, u64::MAX).await?;
    step(
        &database,
        124,
        vec![
            mount("z", R, u64::MAX)?,
            vec![(
                address(R),
                LabelReserved {
                    tokenId: label_token("held"),
                    labelHash: keccak256("held"),
                    label: "held".into(),
                    expiry: u64::MAX,
                    sender: HOLDER.parse()?,
                }
                .encode_log_data(),
            )],
        ],
    )
    .await?;
    assert_alias(&database, "held.z.eth", "held.m.eth").await?;
    database.cleanup().await
}

/// T26. A reservation's row is bound to no token, so the alias matches it by expiry. R's
/// `held` reservation lapses, then `m` moves to `S`, which reserves its own `held` with another
/// expiry. `held.m.eth` serves S's reservation. The alias through `z` still reaches R's lapsed
/// reservation, whose last canonical name is `held.m.eth`, and answers `404 not_found`, not
/// S's reservation. T15 is the same reservation reached through both paths, which serves.
#[tokio::test]
async fn an_alias_of_a_reservation_whose_canonical_path_holds_another_is_not_found() -> Result<()> {
    let (database, _) = base(u64::MAX, u64::MAX).await?;
    let owner = HOLDER.parse()?;
    let reserve = |registry: &str, expiry: u64| -> Result<_> {
        Ok(vec![(
            address(registry),
            LabelReserved {
                tokenId: label_token("held"),
                labelHash: keccak256("held"),
                label: "held".into(),
                expiry,
                sender: HOLDER.parse()?,
            }
            .encode_log_data(),
        )])
    };
    step(
        &database,
        124,
        vec![mount("z", R, u64::MAX)?, reserve(R, clock(126))?],
    )
    .await?;
    assert_alias(&database, "held.z.eth", "held.m.eth").await?;
    step(&database, 125, vec![]).await?;
    step(&database, 126, vec![]).await?;
    step(
        &database,
        127,
        vec![
            deploy(address(S), 2802, owner)?,
            reserve(S, u64::MAX)?,
            repoint(E, "m", S),
        ],
    )
    .await?;
    let canonical = ok(&database, "/v1/names/held.m.eth").await?;
    assert_eq!(
        canonical["data"]["expires_at"], "18446744073709551615",
        "S's reservation: {canonical:#}"
    );
    not_found(&database, "/v1/names/held.z.eth").await?;
    not_found(&database, "/v1/names/held.z.eth/records").await?;
    database.cleanup().await
}

/// T27. A reservation beside a live ENSv1 registration. Before its ENSv1→ENSv2 migration,
/// `envoy1084.eth` keeps its ENSv1 registration and an ownerless reservation in the
/// ETHRegistry. `mirror` mounts the ETHRegistry again, so `envoy1084.mirror.eth` reaches the
/// reservation. Its row is decided by ENSv1 and bound to the ENSv1 registration, and the alias
/// serves it on the direct read and the records route.
#[tokio::test]
async fn an_alias_of_a_reservation_serves_its_ens_v1_row() -> Result<()> {
    let (database, logs, _) = setup().await?;
    let before: Vec<_> = logs
        .iter()
        .filter(|log| log.block_number <= BASE + 121)
        .cloned()
        .collect();
    seed_and_run(&database, &before, 120, 121).await?;
    step(&database, 122, vec![mount("mirror", E, u64::MAX)?]).await?;
    let alias = format!("{LABEL}.mirror.eth");
    let served = assert_alias(&database, &alias, NAME).await?;
    assert_eq!(served["data"]["authority"], "ens_v1", "{served:#}");
    database.cleanup().await
}

/// T13, T14, T20. A canonical read with a current registration never walks. A miss walks at
/// most its labels, and a canonical path never serves as its own alias.
#[tokio::test]
async fn the_walk_runs_only_without_a_current_registration() -> Result<()> {
    let (database, _) = base(u64::MAX, u64::MAX).await?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    let (status, _, statements) = get_counting(&database, "/v1/names/child.m.eth").await?;
    assert_eq!((status, statements), (StatusCode::OK, 0));
    // The publication, the root's `eth` entry and pointer, and the leaf's missing entry.
    let (status, _, statements) = get_counting(&database, "/v1/names/nothing.eth").await?;
    assert_eq!((status, statements), (StatusCode::NOT_FOUND, 4));
    // The same reads, ending at the `nothing` hop's missing entry.
    let (status, _, statements) = get_counting(&database, "/v1/names/a.b.c.nothing.eth").await?;
    assert_eq!((status, statements), (StatusCode::NOT_FOUND, 4));
    let (status, _, statements) = get_counting(&database, "/v1/names/nothing.m.eth").await?;
    assert_eq!((status, statements), (StatusCode::NOT_FOUND, 6));
    // An ENSv1 name whose ENSv2 path ends at its own token.
    let (status, body, _) = get_counting(&database, &format!("/v1/names/{CHILD}")).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert!(body["data"].get("canonical_name").is_none(), "{body:#}");
    database.cleanup().await
}

/// T17. Until the canonical row is composed, the alias answers what the canonical answers.
#[tokio::test]
async fn an_alias_answers_not_found_until_the_canonical_row_exists() -> Result<()> {
    let (database, _) = base(u64::MAX, u64::MAX).await?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    // An association naming a name with no composed row stands for one not composed yet.
    sqlx::query(
        "UPDATE project_lifecycle_association SET logical_name_id = $1
         WHERE logical_name_id = $2",
    )
    .bind(format!("ens:0x{}", "ab".repeat(32)))
    .bind(format!(
        "ens:{}",
        bigname_lookup::ens_namehash_hex("child.m.eth")?
    ))
    .execute(&database.pool)
    .await?;
    not_found(&database, "/v1/names/child.z.eth").await?;
    not_found(&database, "/v1/names/child.z.eth/records").await?;
    database.cleanup().await
}

/// T18, condition (b). `at` on an alias path reads the walk at the selected publication: the
/// response's own token reproduces it, and an older position answers `409 stale`.
#[tokio::test]
async fn an_alias_read_honours_at_like_the_canonical() -> Result<()> {
    let (database, _) = base(u64::MAX, u64::MAX).await?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    let served = assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    let token = served["meta"]["as_of_token"]
        .as_str()
        .context("an as_of token")?
        .to_owned();
    let again = ok(&database, &format!("/v1/names/child.z.eth?at={token}")).await?;
    assert_eq!(again["data"], served["data"]);
    let records = ok(
        &database,
        &format!("/v1/names/child.z.eth/records?at={token}"),
    )
    .await?;
    assert_eq!(records["data"]["canonical_name"], "child.m.eth");
    let older = clock(123);
    for route in ["child.z.eth", "child.m.eth"] {
        let (status, body) = get(&database, &format!("/v1/names/{route}?at={older}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{route}: {body:#}");
        assert_eq!(body["error"]["code"], "stale", "{route}: {body:#}");
    }
    database.cleanup().await
}

/// OQ-5. A verified read of an alias executes the requested path: the Universal Resolver call
/// carries the alias's DNS name and node, never the served path's.
#[tokio::test]
async fn a_verified_alias_read_resolves_the_requested_path() -> Result<()> {
    let (database, _) = base(u64::MAX, u64::MAX).await?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    let (url, handle) =
        spawn_primary_name_mock_rpc(vec![resolution_universal_resolver_addr60_response(GRANTEE)])
            .await?;
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::from_entries(&[format!("{PATH_CHAIN}={url}")])?,
    );
    let uri = "/v1/names/child.z.eth/records?source=verified&keys=addr:60";
    let response = app_router(state)
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    let status = response.status();
    let body: Value = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(body["data"]["canonical_name"], "child.m.eth", "{body:#}");
    assert_eq!(
        body["data"]["records"]["addr:60"]["value"], GRANTEE,
        "{body:#}"
    );
    let requests = join_primary_name_mock_rpc_requests(handle).await?;
    assert_eq!(requests.len(), 1, "{requests:#?}");
    let calldata = requests[0]["params"][0]["data"]
        .as_str()
        .context("eth_call data")?;
    let dns = bigname_domain::normalization::normalize_name("child.z.eth")?.dns_encoded_name;
    let node = |name: &str| -> Result<String> {
        Ok(bigname_lookup::ens_namehash_hex(name)?
            .trim_start_matches("0x")
            .to_owned())
    };
    assert!(
        calldata.contains(&alloy_primitives::hex::encode(dns)),
        "{calldata}"
    );
    assert!(calldata.contains(&node("child.z.eth")?), "{calldata}");
    assert!(!calldata.contains(&node("child.m.eth")?), "{calldata}");
    database.cleanup().await
}

/// T25. The served row must be the walked token's own. R's `child` is unregistered, then `m`
/// moves to `S`, where `child` is registered again and passed to GRANTEE. `child.m.eth` serves
/// S's token. The alias through `z` still reaches R's unregistered token, whose last canonical
/// name is `child.m.eth`, and answers `404 not_found`, not S's registration.
#[tokio::test]
async fn an_alias_whose_canonical_path_holds_another_token_is_not_found() -> Result<()> {
    let (database, resolver) = base(u64::MAX, u64::MAX).await?;
    let owner = HOLDER.parse()?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    step(&database, 125, vec![unregister(R, "child")?]).await?;
    let unregistered = assert_alias(&database, "child.z.eth", "child.m.eth").await?;
    step(
        &database,
        126,
        vec![
            deploy(address(S), 2802, owner)?,
            register(
                address(S),
                "child",
                resolver,
                Address::ZERO,
                u64::MAX,
                owner,
            )?,
            vec![(
                address(S),
                TransferSingle {
                    operator: owner,
                    from: owner,
                    to: GRANTEE.parse()?,
                    id: label_token("child"),
                    value: U256::from(1),
                }
                .encode_log_data(),
            )],
            repoint(E, "m", S),
        ],
    )
    .await?;
    let canonical = ok(&database, "/v1/names/child.m.eth").await?;
    assert_eq!(canonical["data"]["status"], "active", "{canonical:#}");
    assert_eq!(canonical["data"]["owner"], GRANTEE, "{canonical:#}");
    assert_ne!(
        canonical["data"]["registration_id"], unregistered["data"]["registration_id"],
        "S's token, not R's"
    );
    not_found(&database, "/v1/names/child.z.eth").await?;
    not_found(&database, "/v1/names/child.z.eth/records").await?;
    database.cleanup().await
}

/// A7, OQ-5. A verified alias read routes for the requested path, never by the canonical row's
/// projected resolver. `child.m.eth` projects `child`'s own resolver, so its own verified read
/// treats a `ResolverNotFound` as a failed call. The alias's read runs Universal Resolver
/// discovery for `child.z.eth`, so the chain's `ResolverNotFound` for that path is `not_found`.
#[tokio::test]
async fn a_verified_alias_read_without_a_resolver_is_not_found() -> Result<()> {
    let (database, _) = base(u64::MAX, u64::MAX).await?;
    step(&database, 124, vec![mount("z", R, u64::MAX)?]).await?;
    let records = "/records?source=verified&keys=addr:60";
    for (name, status) in [("child.m.eth", "failed"), ("child.z.eth", "not_found")] {
        let dns = bigname_domain::normalization::normalize_name(name)?.dns_encoded_name;
        let (url, handle) =
            spawn_primary_name_mock_rpc(vec![resolution_resolver_not_found_error(&dns)]).await?;
        let body = verified_get(&database, &url, &format!("/v1/names/{name}{records}")).await?;
        assert_eq!(
            body["data"]["records"]["addr:60"]["status"], status,
            "{name}: {body:#}"
        );
        assert_eq!(join_primary_name_mock_rpc_requests(handle).await?.len(), 1);
    }
    database.cleanup().await
}
