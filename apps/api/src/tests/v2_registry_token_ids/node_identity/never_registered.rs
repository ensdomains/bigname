//! A `.eth` second-level name only the 2017 registry recorded, from admitted logs through
//! Interpret, Project and the public routes. The current registry answers `owner(node)` from the
//! 2017 registry while it holds no record for the node, but that owner can write nothing, and
//! the BaseRegistrar never minted the token, so the name is free to register.
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L29-L35 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L17-L19 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L103 @ ens_v1@91c966f)
use super::*;

/// The block after `seed`'s logs, which touches none of the fixture's names.
const UNRELATED: i64 = 123;

/// Under `eth`, all in the 2017 registry at block 121: `plain` and `delta`, `pointed` with a
/// resolver and an address record, `parent` with a child that has its own record, and `zeroed`,
/// whose owner a later write clears. Published at block 122. Returns the public resolver.
async fn seed(database: &TestDatabase) -> Result<Address> {
    let (registry, resolvers) = admit_registry_and_resolvers(database).await?;
    let old = role_address(&registry, "registry_old");
    let resolver = role_address(&resolvers, "public_resolver");
    let holder = HOLDER.parse()?;
    let eth = node(&[b"eth"]);
    let pointed = node(&[b"pointed", b"eth"]);
    let logs = vec![
        owner(B256::ZERO, b"eth", holder, old, 120, 0),
        owner(eth, b"plain", holder, old, 121, 0),
        owner(eth, b"delta", holder, old, 121, 1),
        owner(eth, b"pointed", holder, old, 121, 2),
        owner(eth, b"parent", holder, old, 121, 3),
        owner(eth, b"zeroed", holder, old, 121, 4),
        owner(node(&[b"parent", b"eth"]), b"child", holder, old, 122, 0),
        log(
            events::NewResolver {
                node: pointed,
                resolver,
            }
            .encode_log_data(),
            old,
            122,
            1,
        ),
        log(
            events::AddressChanged {
                node: pointed,
                coinType: U256::from(60),
                newAddress: HOLDER.parse::<Address>()?.as_slice().to_vec().into(),
            }
            .encode_log_data(),
            resolver,
            122,
            2,
        ),
        log(
            events::Transfer {
                node: node(&[b"zeroed", b"eth"]),
                owner: Address::ZERO,
            }
            .encode_log_data(),
            old,
            122,
            3,
        ),
        owner(eth, b"unrelated", holder, old, UNRELATED, 0),
    ];
    intake(database, &logs, UNRELATED).await?;
    run(
        &bigname_interpret::Engine::new(database.pool.clone()),
        120,
        122,
        bigname_interpret::RunMode::Normal,
    )
    .await?;
    publish(database, 122).await?;
    Ok(resolver)
}

fn name_of(label: &[u8]) -> String {
    spelling(&[label, b"eth"])
}

/// A name below `eth` as the name, subnames and lookup routes serve it to a request that spells
/// `eth` bracketed, which proves that label (docs/api-v1.md, "Name inputs").
fn shown_of(labels: &[&[u8]]) -> String {
    format!("{}.eth", spelling(labels))
}

/// No registration, control or authority field, on any record that carries the name's status.
fn assert_no_registration(record: &Value) {
    assert_eq!(record["status"], "unregistered", "{record:#}");
    for field in [
        "owner",
        "manager",
        "authority",
        "ens_v1",
        "registered_at",
        "expires_at",
        "grace_ends_at",
        "registration_id",
        "token_id",
        "lapsed_registration",
    ] {
        assert!(record.get(field).is_none(), "{field}: {record:#}");
    }
}

/// An unknown label, on the name route, and left out of the expiry listing.
#[tokio::test]
async fn a_never_registered_name_serves_no_registration() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database).await?;
    assert_no_registration(&detail(&database, &[b"plain", b"eth"]).await?);
    let listing = get(
        &database,
        "/v1/names?namespace=ens&expires_before=2100-01-01T00:00:00Z&page_size=100",
    )
    .await?;
    assert!(
        !listing.to_string().contains(&name_of(b"plain")),
        "{listing:#}"
    );
    database.cleanup().await
}

/// Label bytes change the spelling and nothing else.
#[tokio::test]
async fn a_known_label_changes_only_the_spelling_of_a_never_registered_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database).await?;
    for label in [b"delta".as_slice(), b"eth"] {
        insert_family_label_preimage(&database.pool, label).await?;
    }
    let row = detail(&database, &[b"delta", b"eth"]).await?;
    assert_eq!(row["name"], "delta.eth", "{row:#}");
    assert_no_registration(&row);
    database.cleanup().await
}

/// Lookup serves the same answer on both profiles.
#[tokio::test]
async fn lookup_serves_a_never_registered_name_as_unregistered() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database).await?;
    for profile in ["detail", "feed"] {
        let response = v2_lookup_response_for_database_with_public_namespaces(
            &database,
            "/v1/lookup",
            json!({"profile": profile, "inputs": [{"name": name_of(b"plain")}]}),
            &["ens"],
        )
        .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let lookup: Value = read_json(response).await?;
        let record = &lookup["data"][0]["record"];
        assert_eq!(
            record["name"],
            shown_of(&[b"plain"]),
            "{profile}: {lookup:#}"
        );
        assert_no_registration(record);
    }
    database.cleanup().await
}

/// The record's owner is not listed, as an owner, a manager or a former owner.
#[tokio::test]
async fn a_never_registered_name_is_not_listed_under_its_record_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database).await?;
    let base = format!("/v1/addresses/{HOLDER}/names?namespace=ens&page_size=100");
    for relation in [
        "",
        "&relation=owner",
        "&relation=manager",
        "&relation=any",
        "&relation=former_owner",
    ] {
        let body = get(&database, &format!("{base}{relation}")).await?;
        assert!(
            !body.to_string().contains(&name_of(b"plain")),
            "{relation}: {body:#}"
        );
    }
    database.cleanup().await
}

/// The search row carries neither the owner nor the authority.
#[tokio::test]
async fn a_never_registered_search_row_names_no_owner_or_authority() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database).await?;
    let search = get(
        &database,
        &format!("/v1/search?q={:x}&match=contains", keccak256(b"plain")),
    )
    .await?;
    let rows = search["data"].as_array().expect("search data");
    assert!(!rows.is_empty(), "{search:#}");
    for row in rows {
        assert!(row.get("owner").is_none(), "{row:#}");
        assert!(row.get("authority").is_none(), "{row:#}");
    }
    database.cleanup().await
}

/// The parent's registry children stay listed with the authority of their own node.
#[tokio::test]
async fn the_children_of_a_never_registered_name_stay_listed() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database).await?;
    let child = shown_of(&[b"child", b"parent"]);
    let subnames = get(
        &database,
        &format!(
            "/v1/names/{}/subnames?page_size=10",
            tl_path(&name_of(b"parent"))
        ),
    )
    .await?;
    let row = subnames["data"]
        .as_array()
        .expect("subnames")
        .iter()
        .find(|row| row["name"] == child)
        .unwrap_or_else(|| panic!("{subnames:#}"));
    assert_eq!(row["authority"], "ens_v0", "{row:#}");
    database.cleanup().await
}

/// A name below a name only the 2017 registry recorded is a registry child, whose record's
/// owner owns it in the only registry that has it.
#[tokio::test]
async fn a_subname_of_a_never_registered_name_keeps_its_record_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database).await?;
    let row = detail(&database, &[b"child", b"parent", b"eth"]).await?;
    assert_eq!(row["status"], "active", "{row:#}");
    assert_eq!(row["authority"], "ens_v0", "{row:#}");
    assert_eq!(row["owner"], HOLDER, "{row:#}");
    assert_eq!(row["manager"], HOLDER, "{row:#}");
    let owned = get(
        &database,
        &format!("/v1/addresses/{HOLDER}/names?namespace=ens&relation=owner&page_size=100"),
    )
    .await?;
    assert!(
        owned
            .to_string()
            .contains(&spelling(&[b"child", b"parent", b"eth"])),
        "{owned:#}"
    );
    database.cleanup().await
}

/// An ownerless name without a lease is unregistered too.
#[tokio::test]
async fn an_unleased_ownerless_name_is_unregistered() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database).await?;
    assert_no_registration(&detail(&database, &[b"zeroed", b"eth"]).await?);
    database.cleanup().await
}

/// A name only the 2017 registry recorded and an ownerless name without a lease serialize
/// alike, but for what names them.
#[tokio::test]
async fn a_never_registered_name_serves_the_shape_of_an_unleased_ownerless_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database).await?;
    let shape = |mut row: Value| {
        let object = row.as_object_mut().expect("a name record");
        for field in ["name", "display_name", "namehash"] {
            object.remove(field);
        }
        row
    };
    assert_eq!(
        shape(detail(&database, &[b"plain", b"eth"]).await?),
        shape(detail(&database, &[b"zeroed", b"eth"]).await?),
    );
    database.cleanup().await
}

/// An `at=` read composes the same answer, before and after an unrelated block.
#[tokio::test]
async fn an_at_read_of_a_never_registered_name_is_unregistered() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database).await?;
    let uri = format!("/v1/names/{}", tl_path(&name_of(b"plain")));
    for block in [None, Some(UNRELATED)] {
        if let Some(block) = block {
            run(
                &bigname_interpret::Engine::new(database.pool.clone()),
                block,
                block,
                bigname_interpret::RunMode::Normal,
            )
            .await?;
            publish(&database, block).await?;
        }
        let current = get(&database, &uri).await?;
        let token = current["meta"]["as_of_token"]
            .as_str()
            .context("name read carries meta.as_of_token")?;
        let at = get(&database, &format!("{uri}?at={token}")).await?;
        assert_no_registration(&at["data"]);
        assert_eq!(at["data"], current["data"]);
    }
    database.cleanup().await
}

/// The `authority` filter's SQL agrees with the served value, on the owner relations and
/// on the resolution relation, which reads the resolver's records and not the name's owner.
#[tokio::test]
async fn the_authority_filter_leaves_out_a_never_registered_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed(&database).await?;
    for relation in ["any", "resolves_to"] {
        let base =
            format!("/v1/addresses/{HOLDER}/names?namespace=ens&relation={relation}&page_size=100");
        let unfiltered = get(&database, &base).await?;
        let filtered = get(&database, &format!("{base}&authority=ens_v0")).await?;
        let expected = unfiltered["data"]
            .as_array()
            .expect("address names")
            .iter()
            .filter(|row| row.get("authority") == Some(&json!("ens_v0")))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(filtered["data"], json!(expected), "{relation}");
        assert!(
            !filtered.to_string().contains(&name_of(b"pointed")),
            "{relation}: {filtered:#}"
        );
    }
    database.cleanup().await
}

/// The rows of a page's data that serve `namehash`.
fn named(body: &Value, namehash: &str) -> Vec<Value> {
    body["data"]
        .as_array()
        .expect("a page of rows")
        .iter()
        .filter(|row| row["namehash"] == namehash)
        .cloned()
        .collect()
}

/// The address names of `HOLDER` under `relation`.
async fn holder_names(database: &TestDatabase, relation: &str) -> Result<Value> {
    get(
        database,
        &format!("/v1/addresses/{HOLDER}/names?namespace=ens&relation={relation}&page_size=100"),
    )
    .await
}

/// The current registry answers `resolver(node)` from the 2017 registry while it holds no record
/// for the node, so a name only the 2017 registry recorded keeps the 2017 record's resolver and
/// resolves through it, as it did before it served no registration. It has no owner and no
/// binding, and the 2017 record's owner holds it under no ownership relation.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f)
#[tokio::test]
async fn a_never_registered_name_resolves_through_its_2017_resolver() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resolver = seed(&database).await?;
    let pointed = node(&[b"pointed", b"eth"]);
    let namehash = format!("{pointed:#x}");
    // What the name resolves to.
    let row = detail(&database, &[b"pointed", b"eth"]).await?;
    assert_eq!(row["resolver"]["address"], format!("{resolver:#x}"), "{row:#}");
    assert_eq!(row["records"]["addresses"]["60"], HOLDER, "{row:#}");
    let resolves = holder_names(&database, "resolves_to").await?;
    let listed = named(&resolves, &namehash);
    assert_eq!(listed.len(), 1, "{resolves:#}");
    let bound = get(&database, &format!("/v1/resolvers/1/{resolver:#x}?page_size=100")).await?;
    assert!(bound.to_string().contains(&name_of(b"pointed")), "{bound:#}");
    // What it serves as a name with no registration.
    assert_no_registration(&row);
    for field in ["owner", "manager", "authority"] {
        assert!(listed[0].get(field).is_none(), "{field}: {resolves:#}");
    }
    let stored = bigname_storage::load_name_current(&database.pool, &format!("ens:{namehash}"))
        .await?
        .context("the name has a composed row")?;
    assert!(
        stored.surface_binding_id.is_none() && stored.resource_id.is_none(),
        "{stored:#?}"
    );
    for relation in ["owner", "manager", "former_owner"] {
        let body = holder_names(&database, relation).await?;
        assert!(named(&body, &namehash).is_empty(), "{relation}: {body:#}");
    }
    let subnames = get(&database, "/v1/names/eth/subnames?page_size=100").await?;
    for child in named(&subnames, &namehash) {
        for field in ["owner", "manager"] {
            assert!(child.get(field).is_none(), "{field}: {subnames:#}");
        }
    }
    database.cleanup().await
}

/// A name only the 2017 registry recorded with no resolver record has no records and resolves
/// to no address.
#[tokio::test]
async fn a_never_registered_name_with_no_resolver_resolves_to_nothing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resolver = seed(&database).await?;
    let namehash = format!("{:#x}", node(&[b"plain", b"eth"]));
    let row = detail(&database, &[b"plain", b"eth"]).await?;
    assert_no_registration(&row);
    assert_eq!(row["resolver"], Value::Null, "{row:#}");
    assert_eq!(row["records"]["addresses"]["60"], Value::Null, "{row:#}");
    let stored = bigname_storage::load_name_current(&database.pool, &format!("ens:{namehash}"))
        .await?
        .context("the name has a composed row")?;
    assert!(stored.serving_resource_id.is_none(), "{stored:#?}");
    let resolves = holder_names(&database, "resolves_to").await?;
    assert!(named(&resolves, &namehash).is_empty(), "{resolves:#}");
    let bound = get(&database, &format!("/v1/resolvers/1/{resolver:#x}?page_size=100")).await?;
    assert!(!bound.to_string().contains(&name_of(b"plain")), "{bound:#}");
    database.cleanup().await
}

/// Once the BaseRegistrar registers the name, the registration writes the node's first
/// current-registry record, which holds no resolver, and the current registry stops reading the
/// 2017 record. The name serves the new registration and no resolver.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L24 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
#[tokio::test]
async fn a_registered_2017_record_serves_the_current_registry_pointer() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (registry, resolvers) = admit_registry_and_resolvers(&database).await?;
    let registrar_manifest =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registrar_l1", 2284).await?;
    let old = role_address(&registry, "registry_old");
    let current = role_address(&registry, "registry");
    let resolver = role_address(&resolvers, "public_resolver");
    let base_registrar = role_address(&registrar_manifest, "registrar");
    let holder: Address = HOLDER.parse()?;
    let pointed = node(&[b"pointed", b"eth"]);
    let mut logs = vec![
        owner(B256::ZERO, b"eth", holder, old, 120, 0),
        owner(node(&[b"eth"]), b"pointed", holder, old, 121, 0),
        log(
            events::NewResolver {
                node: pointed,
                resolver,
            }
            .encode_log_data(),
            old,
            121,
            1,
        ),
        log(
            events::AddressChanged {
                node: pointed,
                coinType: U256::from(60),
                newAddress: holder.as_slice().to_vec().into(),
            }
            .encode_log_data(),
            resolver,
            121,
            2,
        ),
    ];
    logs.extend(super::graveyard_held::registration(
        (base_registrar, current),
        b"pointed",
        (None, GRANTEE.parse()?),
        1_900_000_000,
        (122, 0),
    ));
    intake(&database, &logs, 122).await?;
    run(
        &bigname_interpret::Engine::new(database.pool.clone()),
        120,
        122,
        bigname_interpret::RunMode::Normal,
    )
    .await?;
    publish(&database, 122).await?;

    let row = detail(&database, &[b"pointed", b"eth"]).await?;
    assert_eq!(row["status"], "active", "{row:#}");
    assert_eq!(row["authority"], "ens_v1", "{row:#}");
    assert_eq!(row["owner"], GRANTEE, "{row:#}");
    assert!(row.get("registration_id").is_some(), "{row:#}");
    assert_eq!(row["resolver"], Value::Null, "{row:#}");
    assert_eq!(row["records"]["addresses"]["60"], Value::Null, "{row:#}");
    let resolves = holder_names(&database, "resolves_to").await?;
    assert!(named(&resolves, &format!("{pointed:#x}")).is_empty(), "{resolves:#}");
    let bound = get(&database, &format!("/v1/resolvers/1/{resolver:#x}?page_size=100")).await?;
    assert!(!bound.to_string().contains(&name_of(b"pointed")), "{bound:#}");
    database.cleanup().await
}
