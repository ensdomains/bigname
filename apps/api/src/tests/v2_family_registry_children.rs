// `GET /v1/addresses/{address}/names` lists an ENSv1 registry child that has no name surface (a
// node a registry NewOwner created and no label-bearing event named) for its current registry
// owner, as `manager`, in the form its parent's subnames route serves it.

const RC_OWNER: &str = "0x00000000000000000000000000000000000000d1";
const RC_BUYER: &str = "0x00000000000000000000000000000000000000d2";
/// The NameWrapper and a registrar controller, as the emitters of shadow observations.
const RC_WRAPPER: &str = "0x00000000000000000000000000000000000000d3";
const RC_CONTROLLER: &str = "0x00000000000000000000000000000000000000d4";

/// A registry NewOwner under `parent` for `label` to `owner` at `block`, carrying the child
/// node's registry-only resource as the adapter does. Returns the child node.
async fn insert_registry_child(
    database: &TestDatabase,
    parent: &str,
    label: &str,
    owner: &str,
    block: i64,
    resource: Uuid,
) -> Result<String> {
    insert_registry_child_from(database, parent, label, owner, block, resource, "registry").await
}

/// [`insert_registry_child`] from the registry `emitter_role` names: `registry`, the current
/// ENSv1 registry, or `registry_old`, the 2017 registry.
async fn insert_registry_child_from(
    database: &TestDatabase,
    parent: &str,
    label: &str,
    owner: &str,
    block: i64,
    resource: Uuid,
    emitter_role: &str,
) -> Result<String> {
    upsert_test_resources(
        &database.pool,
        &[Resource {
            resource_id: resource,
            token_lineage_id: None,
            chain_id: FAMILY_CHAIN.to_owned(),
            block_hash: format!("0xhistory{block}"),
            block_number: block,
            provenance: json!({"authority_kind": "registry_only"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    let labelhash = child_labelhash(label);
    let node = bigname_lookup::ens_namehash_hex(parent)?;
    let child = format!(
        "{:#x}",
        alloy_primitives::keccak256(
            [
                alloy_primitives::hex::decode(&node)?,
                alloy_primitives::hex::decode(&labelhash)?
            ]
            .concat()
        )
    );
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[family_event(
            &format!("registry-child-{label}"),
            None,
            Some(resource),
            "SubregistryChanged",
            "ens_v1_registry_l1",
            block,
            0,
            json!({"source_event": "NewOwner", "node": node, "child_node": child,
                   "labelhash": labelhash, "owner": owner, "owner_getter": owner,
                   "emitter_role": emitter_role}),
        )],
    )
    .await?;
    Ok(child)
}

/// alpha.eth and zeta.eth, registered to RC_OWNER at 201. Under alpha.eth, registry NewOwners to
/// RC_OWNER create `known` (label preimage observed), `unknown` (none), `moved` (transferred to
/// RC_BUYER at 207) and `gains`, which gains a surface, whose registry events name it from 206.
/// Published at 240. Returns the registry-only resources of known and unknown and the child
/// nodes of known, unknown and moved.
async fn seed_registry_children_fixture(
    database: &TestDatabase,
) -> Result<((Uuid, Uuid), (String, String, String))> {
    seed_bounded_membership_blocks(database, 240).await?;
    let (alpha, alpha_resource) =
        seed_family_name(database, "alpha.eth", 0x7a1_0000, "ens_v1").await?;
    let (zeta, zeta_resource) =
        seed_family_name(database, "zeta.eth", 0x7b1_0000, "ens_v1").await?;
    for label in ["known", "moved"] {
        insert_family_label_preimage(&database.pool, label.as_bytes()).await?;
    }
    let (known_resource, unknown_resource) =
        (Uuid::from_u128(0x7d1_0001), Uuid::from_u128(0x7d1_0002));
    let known = insert_registry_child(
        database,
        "alpha.eth",
        "known",
        RC_OWNER,
        202,
        known_resource,
    )
    .await?;
    let unknown = insert_registry_child(
        database,
        "alpha.eth",
        "unknown",
        RC_OWNER,
        203,
        unknown_resource,
    )
    .await?;
    let moved = insert_registry_child(
        database,
        "alpha.eth",
        "moved",
        RC_OWNER,
        204,
        Uuid::from_u128(0x7d1_0003),
    )
    .await?;
    let gains_node = insert_registry_child(
        database,
        "alpha.eth",
        "gains",
        RC_OWNER,
        205,
        Uuid::from_u128(0x7d1_0004),
    )
    .await?;
    let (gains, gains_resource) =
        seed_family_name(database, "gains.alpha.eth", 0x7c1_0000, "ens_v1").await?;
    let grant = |identity: &str, name: &str, resource: Uuid, block: i64| {
        family_event(
            identity,
            Some(name),
            Some(resource),
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            block,
            0,
            json!({"authority_kind": "registrar", "registrant": RC_OWNER,
                   "expiry": 1_900_000_000i64}),
        )
    };
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            grant("rc-alpha-grant", &alpha, alpha_resource, 201),
            grant("rc-zeta-grant", &zeta, zeta_resource, 201),
            // Once a surface names the child, its registry events carry the name.
            family_event(
                "rc-gains-named",
                Some(&gains),
                Some(gains_resource),
                "AuthorityTransferred",
                "ens_v1_registry_l1",
                206,
                0,
                json!({"source_event": "Transfer", "node": gains_node, "owner": RC_OWNER,
                       "owner_getter": RC_OWNER, "emitter_role": "registry",
                       "authority_kind": "registry_only"}),
            ),
            family_event(
                "rc-moved-transfer",
                None,
                Some(Uuid::from_u128(0x7d1_0003)),
                "AuthorityTransferred",
                "ens_v1_registry_l1",
                207,
                0,
                json!({"source_event": "Transfer", "node": moved, "owner": RC_BUYER,
                       "owner_getter": RC_BUYER, "emitter_role": "registry"}),
            ),
        ],
    )
    .await?;
    publish_test_families(database, 240).await?;
    Ok(((known_resource, unknown_resource), (known, unknown, moved)))
}

fn rows_of(pages: &[Value]) -> Vec<Value> {
    pages
        .iter()
        .flat_map(|page| page["data"].as_array().cloned().unwrap_or_default())
        .collect()
}

fn names_of(rows: &[Value]) -> Vec<String> {
    let mut names: Vec<String> = rows
        .iter()
        .map(|row| row["name"].as_str().expect("name").to_owned())
        .collect();
    names.sort();
    names
}

fn placeholder(label: &str, parent: &str) -> String {
    format!(
        "[{}].{parent}",
        child_labelhash(label).trim_start_matches("0x")
    )
}

#[tokio::test]
async fn v2_registry_children_are_listed_for_their_registry_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let ((known_resource, unknown_resource), (known, unknown, moved)) =
        seed_registry_children_fixture(&database).await?;
    let unknown_name = placeholder("unknown", "alpha.eth");

    // Named and surface-less rows page together through the route's own cursors.
    let pages = read_family_pages(
        &database,
        &format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=2"),
    )
    .await?;
    let rows = rows_of(&pages);
    let mut expected = vec![
        "alpha.eth".to_owned(),
        "gains.alpha.eth".to_owned(),
        "known.alpha.eth".to_owned(),
        "zeta.eth".to_owned(),
        unknown_name.clone(),
    ];
    expected.sort();
    assert_eq!(names_of(&rows), expected, "{pages:#?}");
    assert!(pages.len() >= 3, "{pages:#?}");
    assert_eq!(pages[0]["total_count"], json!(5), "{pages:#?}");

    // Each surface-less row is what the parent's subnames route serves for the child.
    let subnames =
        rows_of(&read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=10").await?);
    for (node, resource, name) in [
        (&known, known_resource, "known.alpha.eth".to_owned()),
        (&unknown, unknown_resource, unknown_name.clone()),
    ] {
        let row = rows
            .iter()
            .find(|row| row["namehash"] == json!(node))
            .unwrap_or_else(|| panic!("{name} is listed: {rows:#?}"));
        let subname = subnames
            .iter()
            .find(|row| row["namehash"] == json!(node))
            .unwrap_or_else(|| panic!("{name} is a subname: {subnames:#?}"));
        assert_eq!(row["name"], json!(name), "{row:#}");
        assert_eq!(row["display_name"], json!(name), "{row:#}");
        for field in [
            "name",
            "display_name",
            "owner",
            "manager",
            "registration_status",
            "authority",
        ] {
            assert_eq!(
                row[field], subname[field],
                "{field}: {row:#} vs {subname:#}"
            );
        }
        assert_eq!(row["owner"], json!(RC_OWNER), "{row:#}");
        assert_eq!(row["manager"], json!(RC_OWNER), "{row:#}");
        assert_eq!(
            row["permission_resource_id"],
            json!(resource.to_string()),
            "{row:#}"
        );
        assert_eq!(row["relations"], json!(["owner", "manager"]), "{row:#}");
        assert_eq!(row["is_primary"], json!(false), "{row:#}");
        assert_eq!(row["registration_status"], json!("unregistered"), "{row:#}");
        assert_eq!(row["authority"], json!("ens_v1"), "{row:#}");
        for absent in [
            "registrant",
            "registered_at",
            "created_at",
            "expires_at",
            "migrated_at",
        ] {
            assert!(row.get(absent).is_none(), "{absent}: {row:#}");
        }
    }
    // A child that gained a surface is its ordinary row only.
    let gains: Vec<&Value> = rows
        .iter()
        .filter(|row| row["name"] == json!("gains.alpha.eth"))
        .collect();
    assert_eq!(gains.len(), 1, "{rows:#?}");
    assert_eq!(
        gains[0]["permission_resource_id"],
        json!(Uuid::from_u128(0x7c1_0000).to_string()),
        "{:#}",
        gains[0]
    );

    // The transfer moved the child: the old owner no longer lists it, the new one does, and the
    // subnames route serves the new owner.
    assert!(
        !rows.iter().any(|row| row["namehash"] == json!(moved)),
        "{rows:#?}"
    );
    let bought = rows_of(
        &read_family_pages(
            &database,
            &format!("/v1/addresses/{RC_BUYER}/names?namespace=ens&page_size=5"),
        )
        .await?,
    );
    assert_eq!(names_of(&bought), ["moved.alpha.eth"], "{bought:#?}");
    assert_eq!(bought[0]["owner"], json!(RC_BUYER), "{bought:#?}");
    let moved_subname = subnames
        .iter()
        .find(|row| row["namehash"] == json!(moved))
        .expect("moved is a subname");
    assert_eq!(moved_subname["owner"], json!(RC_BUYER), "{moved_subname:#}");

    database.cleanup().await
}

#[tokio::test]
async fn v2_registry_children_follow_the_address_names_filters() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_children_fixture(&database).await?;
    let unknown_name = placeholder("unknown", "alpha.eth");
    let listed = |rows: &[Value], name: &str| rows.iter().any(|row| row["name"] == json!(name));
    let read = |query: String| {
        let database = &database;
        async move {
            anyhow::Ok(rows_of(
                &read_family_pages(
                    database,
                    &format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=2&{query}"),
                )
                .await?,
            ))
        }
    };

    for query in [
        "relation=owner",
        "relation=manager",
        "dedupe=registration",
        "is_migrated=false",
        "sort=expires_at",
        "sort=registered_at&order=desc",
        "sort=name&order=desc",
        "include=counts",
        "include=role_summary",
        "authority=ens_v1",
        "authority=ens_v0,ens_v1",
    ] {
        let rows = read(query.to_owned()).await?;
        assert!(listed(&rows, "known.alpha.eth"), "{query}: {rows:#?}");
        assert!(listed(&rows, &unknown_name), "{query}: {rows:#?}");
    }
    for query in [
        "relation=role_holder",
        "authority=ens_v0",
        "authority=ens_v2",
        "is_migrated=true",
    ] {
        let rows = read(query.to_owned()).await?;
        assert!(!listed(&rows, "known.alpha.eth"), "{query}: {rows:#?}");
        assert!(!listed(&rows, &unknown_name), "{query}: {rows:#?}");
    }
    // An unknown expiry sorts first ascending, as for any row without one.
    let by_expiry = read("sort=expires_at".to_owned()).await?;
    let first_dated = by_expiry
        .iter()
        .position(|row| row.get("expires_at").is_some())
        .expect("a row with an expiry");
    assert!(first_dated > 0, "{by_expiry:#?}");
    assert!(
        by_expiry[first_dated..]
            .iter()
            .all(|row| row.get("expires_at").is_some()),
        "{by_expiry:#?}"
    );
    // `q` matches the served text.
    let rows = read("q=kno".to_owned()).await?;
    assert_eq!(names_of(&rows), ["known.alpha.eth"], "{rows:#?}");

    database.cleanup().await
}

/// A registry child with no surface carries the authority of the registry that owns its node:
/// `ens_v1` for one the current ENSv1 registry recorded, `ens_v0` for one only the 2017 registry
/// did (docs/glossary.md#registry-generation): the current registry answers `owner(node)` and
/// `resolver(node)` from the 2017 registry while its own `recordExists(node)`, a nonzero stored
/// owner, is false
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L35 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L153-L157 @ ens_v1@91c966f).
/// Address names and subnames serve the same value, and the address-names `authority` filter
/// matches it.
#[tokio::test]
async fn v2_registry_children_serve_the_authority_of_their_registry() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (alpha, alpha_resource) =
        seed_family_name(&database, "alpha.eth", 0x7a1_0000, "ens_v1").await?;
    let current = insert_registry_child(
        &database,
        "alpha.eth",
        "current",
        RC_OWNER,
        202,
        Uuid::from_u128(0x7e1_0001),
    )
    .await?;
    let old = insert_registry_child_from(
        &database,
        "alpha.eth",
        "old",
        RC_OWNER,
        203,
        Uuid::from_u128(0x7e1_0002),
        "registry_old",
    )
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[family_event(
            "rc-authority-alpha-grant",
            Some(&alpha),
            Some(alpha_resource),
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            201,
            0,
            json!({"authority_kind": "registrar", "registrant": RC_OWNER,
                   "expiry": 1_900_000_000i64}),
        )],
    )
    .await?;
    publish_test_families(&database, 240).await?;

    let read = |query: &str| {
        let uri = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=10{query}");
        let database = &database;
        async move { anyhow::Ok(rows_of(&read_family_pages(database, &uri).await?)) }
    };
    let rows = read("").await?;
    let subnames =
        rows_of(&read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=10").await?);
    for (node, authority) in [(&current, "ens_v1"), (&old, "ens_v0")] {
        let row = rows
            .iter()
            .find(|row| row["namehash"] == json!(node))
            .unwrap_or_else(|| panic!("{node} is listed: {rows:#?}"));
        assert_eq!(row["authority"], json!(authority), "{row:#}");
        assert_eq!(row["registration_status"], json!("unregistered"), "{row:#}");
        assert_eq!(row["relations"], json!(["owner", "manager"]), "{row:#}");
        let subname = subnames
            .iter()
            .find(|row| row["namehash"] == json!(node))
            .unwrap_or_else(|| panic!("{node} is a subname: {subnames:#?}"));
        assert_eq!(subname["authority"], row["authority"], "{subname:#}");
        // No lease, so ENSv1's own object carries only a null expiry.
        assert_eq!(row["ens_v1"], json!({"expires_at": null}), "{row:#}");
        assert_eq!(subname["ens_v1"], row["ens_v1"], "{subname:#}");
    }
    let alpha_row = rows
        .iter()
        .find(|row| row["name"] == json!("alpha.eth"))
        .expect("alpha.eth is listed");
    assert_eq!(alpha_row["authority"], json!("ens_v1"), "{alpha_row:#}");

    let nodes = |rows: &[Value]| {
        let mut nodes: Vec<String> = rows
            .iter()
            .filter(|row| row["name"] != json!("alpha.eth"))
            .map(|row| row["namehash"].as_str().expect("namehash").to_owned())
            .collect();
        nodes.sort();
        nodes
    };
    let mut both = vec![current.clone(), old.clone()];
    both.sort();
    for (query, expected) in [
        ("&authority=ens_v1", vec![current.clone()]),
        ("&authority=ens_v0", vec![old.clone()]),
        ("&authority=ens_v0,ens_v1", both),
        ("&authority=ens_v2", Vec::new()),
    ] {
        let filtered = read(query).await?;
        assert_eq!(nodes(&filtered), expected, "{query}: {filtered:#?}");
    }

    database.cleanup().await
}

/// The shadow surface Interpret writes for `<label>.alpha.eth`, a name whose label fails
/// normalization (crates/adapters/src/schema_v2/identity.rs, `materialize`), with the
/// `PreimageObserved` event of the `family` observer at `emitter` that named it, at `block`.
/// Returns the child's name id.
async fn insert_shadow_child_surface(
    database: &TestDatabase,
    child: &str,
    label: &str,
    (family, emitter): (&str, &str),
    source_event: &str,
    block: i64,
) -> Result<String> {
    let id = format!("ens:{child}");
    let labels = [label, "alpha", "eth"];
    let mut dns = Vec::new();
    for label in labels {
        dns.push(u8::try_from(label.len())?);
        dns.extend_from_slice(label.as_bytes());
    }
    dns.push(0);
    sqlx::query(
        "INSERT INTO bigname_phase.name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, normalization_errors,
             deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
             provenance, canonicality_state)
         VALUES ($1, 'ens', $2, $3, $4, $5, $6, $7, 'shadow',
                 '[{\"error\": \"raw label is not byte-identical to its normalized form\"}]',
                 'normalization_gate', to_timestamp(1700000000 + $9), $8,
                 '0xhistory' || $9, $9, jsonb_build_object('source_event', $10::text),
                 'canonical')",
    )
    .bind(&id)
    .bind(labels.join("."))
    .bind(labels.to_vec())
    .bind(dns)
    .bind(child)
    .bind(
        labels
            .iter()
            .map(|label| child_labelhash(label))
            .collect::<Vec<_>>(),
    )
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(FAMILY_CHAIN)
    .bind(block)
    .bind(source_event)
    .execute(&database.pool)
    .await?;
    let mut observed = family_event(
        &format!("rc-shadow-preimage-{label}"),
        Some(&id),
        None,
        "PreimageObserved",
        family,
        block,
        2,
        json!({"source_event": source_event, "logical_name_id": id, "namehash": child,
               "visibility_state": "shadow", "deactivation_reason": "normalization_gate"}),
    );
    observed.raw_fact_ref = json!({"kind": "raw_log", "emitting_address": emitter});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[observed]).await?;
    Ok(id)
}

/// The served row of `node` among `rows`.
fn served_child<'a>(rows: &'a [Value], node: &str) -> &'a Value {
    rows.iter()
        .find(|row| row["namehash"] == json!(node))
        .unwrap_or_else(|| panic!("{node} is served: {rows:#?}"))
}

/// The address-names rows and `total_count` of `address` with no relation filter and under
/// `relation=any`, `relation=owner` and `relation=manager`, each with the `relations` a row
/// listed for both would match.
async fn address_rows_by_relation(
    database: &TestDatabase,
    address: &str,
) -> Result<Vec<(Value, Vec<Value>, Value)>> {
    let mut by_relation = Vec::new();
    for (relation, matched) in [
        ("", json!(["owner", "manager"])),
        ("&relation=any", json!(["owner", "manager"])),
        ("&relation=owner", json!(["owner"])),
        ("&relation=manager", json!(["manager"])),
    ] {
        let pages = read_family_pages(
            database,
            &format!("/v1/addresses/{address}/names?namespace=ens&page_size=10{relation}"),
        )
        .await?;
        by_relation.push((matched, rows_of(&pages), pages[0]["total_count"].clone()));
    }
    Ok(by_relation)
}

/// A child the NameWrapper created with `setSubnodeOwner` under a label that fails
/// normalization: the NameWrapper takes the node in the registry and mints the token to its
/// holder (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L579-L581 @ ens_v1@91c966f),
/// and the wrapper adapter writes only a shadow surface for it
/// (crates/adapters/src/schema_v2/protocol/v1/wrapper.rs, `name_wrapped`), so no name row
/// composes and both routes serve it from its registry. Its wrapper state and any lease are
/// projected without a composed name, so its `ens_v1` object claims no lifecycle: no `expires_at`
/// and no wrapper fields. Its registry owner is the NameWrapper contract, not its owner, so the
/// contract's address-names list it under no relation and its subname omits `owner` and
/// `manager`; its token holder is not listed either. A child whose only shadow a resolver
/// `NameChanged` wrote has no such state and, like a sibling no label-bearing event named, keeps
/// `expires_at: null` and serves its registry owner as owner and manager.
#[tokio::test]
async fn v2_shadowed_registry_child_serves_ens_v1_without_lifecycle() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (alpha, alpha_resource) =
        seed_family_name(&database, "alpha.eth", 0x7f1_0000, "ens_v1").await?;
    for label in [b"Wrapped".as_slice(), b"Named".as_slice()] {
        insert_family_label_preimage(&database.pool, label).await?;
    }
    let mut children = Vec::new();
    for (index, (label, owner)) in [
        ("Wrapped", RC_WRAPPER),
        ("Named", RC_OWNER),
        ("plain", RC_OWNER),
    ]
    .into_iter()
    .enumerate()
    {
        let offset = i64::try_from(index)?;
        children.push(
            insert_registry_child(
                &database,
                "alpha.eth",
                label,
                owner,
                202 + offset,
                Uuid::from_u128(0x7f1_0011 + u128::try_from(index)?),
            )
            .await?,
        );
    }
    let (wrapped, named, plain) = (&children[0], &children[1], &children[2]);
    let wrapped_id = insert_shadow_child_surface(
        &database,
        wrapped,
        "Wrapped",
        ("ens_v1_wrapper_l1", RC_WRAPPER),
        "NameWrapped",
        202,
    )
    .await?;
    // An admitted PublicResolver's `setName` takes any string for a node its caller controls
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L13-L19 @ ens_v1@91c966f),
    // so a reverse record can name the registry child under a label that fails normalization.
    // Its emitter is the child's registry owner: only a NameWrapper observation hides a child.
    insert_shadow_child_surface(
        &database,
        named,
        "Named",
        ("ens_v1_resolver_l1", RC_OWNER),
        "NameChanged",
        203,
    )
    .await?;
    // The NameWrapper resource the adapter writes beside the shadow surface.
    let wrapper_resource = Uuid::from_u128(0x7f1_0021);
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
                "rc-shadow-alpha-grant",
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
                "rc-shadow-wrapped-fuses",
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
        ],
    )
    .await?;
    publish_test_families(&database, 240).await?;

    for (relation, rows, total) in address_rows_by_relation(&database, RC_WRAPPER).await? {
        assert_eq!(rows, Vec::<Value>::new(), "{relation}");
        assert_eq!(total, json!(0), "{relation}");
    }
    let rows = rows_of(
        &read_family_pages(
            &database,
            &format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=10"),
        )
        .await?,
    );
    assert!(
        rows.iter().all(|row| row["namehash"] != json!(wrapped)),
        "{rows:#?}"
    );
    let subnames =
        rows_of(&read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=10").await?);
    let subname = served_child(&subnames, wrapped);
    assert_eq!(subname["authority"], json!("ens_v1"), "{subname:#}");
    assert_eq!(subname["ens_v1"], json!({}), "{subname:#}");
    assert_eq!(subname.get("owner"), None, "{subname:#}");
    assert_eq!(subname.get("manager"), None, "{subname:#}");
    for node in [named, plain] {
        let row = served_child(&rows, node);
        for served in [row, served_child(&subnames, node)] {
            assert_eq!(served["authority"], json!("ens_v1"), "{served:#}");
            assert_eq!(served["ens_v1"], json!({"expires_at": null}), "{served:#}");
            assert_eq!(served["owner"], json!(RC_OWNER), "{served:#}");
            assert_eq!(served["manager"], json!(RC_OWNER), "{served:#}");
        }
        assert_eq!(row["relations"], json!(["owner", "manager"]), "{row:#}");
    }

    database.cleanup().await
}

/// A child an ENSv1 registrar controller registered under a label that fails normalization: the
/// legacy controller accepts any label of three or more characters
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L191-L193 @ ens_v1@91c966f)
/// and the registration sets the registrant as registry owner
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L149 @ ens_v1@91c966f),
/// so, until a `reclaim` or registry transfer moves the record, the registrant stays listed as
/// owner and manager of the shadow child and its subname keeps `owner`. Only its `manager` field is withheld, as for every lifecycle shadow. The fixture puts
/// the child under alpha.eth: which observer emitted the shadow is all that decides the listing.
#[tokio::test]
async fn v2_registrar_shadow_child_stays_listed_for_its_registrant() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (alpha, alpha_resource) =
        seed_family_name(&database, "alpha.eth", 0x7f2_0000, "ens_v1").await?;
    insert_family_label_preimage(&database.pool, b"Leased").await?;
    let leased = insert_registry_child(
        &database,
        "alpha.eth",
        "Leased",
        RC_OWNER,
        202,
        Uuid::from_u128(0x7f2_0011),
    )
    .await?;
    insert_shadow_child_surface(
        &database,
        &leased,
        "Leased",
        ("ens_v1_registrar_l1", RC_CONTROLLER),
        "NameRegistered",
        202,
    )
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[family_event(
            "rc-leased-alpha-grant",
            Some(&alpha),
            Some(alpha_resource),
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            201,
            0,
            json!({"authority_kind": "registrar", "registrant": RC_OWNER,
                   "expiry": 1_900_000_000i64}),
        )],
    )
    .await?;
    publish_test_families(&database, 240).await?;

    for (relation, rows, _) in address_rows_by_relation(&database, RC_OWNER).await? {
        let row = served_child(&rows, &leased);
        assert_eq!(row["relations"], relation, "{row:#}");
        assert_eq!(row["owner"], json!(RC_OWNER), "{relation}: {row:#}");
        assert_eq!(row.get("manager"), None, "{relation}: {row:#}");
        assert_eq!(row["ens_v1"], json!({}), "{relation}: {row:#}");
    }
    let subnames =
        rows_of(&read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=10").await?);
    let subname = served_child(&subnames, &leased);
    assert_eq!(subname["owner"], json!(RC_OWNER), "{subname:#}");
    assert_eq!(subname.get("manager"), None, "{subname:#}");

    database.cleanup().await
}

/// A NameWrapper-held shadow child whose registry record later leaves the NameWrapper, as an
/// unwrap returns it to the controller the holder names
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f), is
/// listed for its new registry owner and served with that owner; the NameWrapper's earlier
/// observation does not keep it hidden.
#[tokio::test]
async fn v2_wrapper_shadow_child_follows_its_registry_owner_out_of_the_wrapper() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (alpha, alpha_resource) =
        seed_family_name(&database, "alpha.eth", 0x7f3_0000, "ens_v1").await?;
    insert_family_label_preimage(&database.pool, b"Wrapped").await?;
    let resource = Uuid::from_u128(0x7f3_0011);
    let wrapped =
        insert_registry_child(&database, "alpha.eth", "Wrapped", RC_WRAPPER, 202, resource)
            .await?;
    insert_shadow_child_surface(
        &database,
        &wrapped,
        "Wrapped",
        ("ens_v1_wrapper_l1", RC_WRAPPER),
        "NameWrapped",
        202,
    )
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            family_event(
                "rc-unwrapped-alpha-grant",
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
                "rc-unwrapped-transfer",
                None,
                Some(resource),
                "AuthorityTransferred",
                "ens_v1_registry_l1",
                207,
                0,
                json!({"source_event": "Transfer", "node": wrapped, "owner": RC_BUYER,
                       "owner_getter": RC_BUYER, "emitter_role": "registry"}),
            ),
        ],
    )
    .await?;
    publish_test_families(&database, 240).await?;

    for (relation, rows, total) in address_rows_by_relation(&database, RC_WRAPPER).await? {
        assert_eq!(rows, Vec::<Value>::new(), "{relation}");
        assert_eq!(total, json!(0), "{relation}");
    }
    for (relation, rows, _) in address_rows_by_relation(&database, RC_BUYER).await? {
        let row = served_child(&rows, &wrapped);
        assert_eq!(row["relations"], relation, "{row:#}");
        assert_eq!(row["owner"], json!(RC_BUYER), "{relation}: {row:#}");
        assert_eq!(row.get("manager"), None, "{relation}: {row:#}");
    }
    let subnames =
        rows_of(&read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=10").await?);
    let subname = served_child(&subnames, &wrapped);
    assert_eq!(subname["owner"], json!(RC_BUYER), "{subname:#}");
    assert_eq!(subname.get("manager"), None, "{subname:#}");

    database.cleanup().await
}

/// A name surface Interpret commits after the publication does not take the child out of the
/// published read: the child stays a registry-child row until Project publishes the block that
/// serves the name's ordinary row, which then replaces it.
#[tokio::test]
async fn v2_registry_child_stays_listed_until_its_new_surface_is_published() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (_, (known, _, _)) = seed_registry_children_fixture(&database).await?;
    let uri = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=2");
    let before = read_family_pages(&database, &uri).await?;

    // Interpret names the child at 241; the families are still published at 240.
    let (named, named_resource) = seed_family_name_at(
        &database,
        "known.alpha.eth",
        0x7e1_0000,
        "ens_v1",
        "ens",
        FAMILY_CHAIN,
        241,
    )
    .await?;
    let pages = read_family_pages(&database, &uri).await?;
    assert_eq!(names_of(&rows_of(&pages)), names_of(&rows_of(&before)), "{pages:#?}");
    assert_eq!(pages[0]["total_count"], json!(5), "{pages:#?}");
    let row = rows_of(&pages)
        .into_iter()
        .find(|row| row["namehash"] == json!(known))
        .expect("the child is still listed");
    assert_eq!(
        row["permission_resource_id"],
        json!(Uuid::from_u128(0x7d1_0001).to_string()),
        "{row:#}"
    );
    // The parent's subnames route still serves it the same way.
    let subnames =
        rows_of(&read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=10").await?);
    let subname = subnames
        .iter()
        .find(|subname| subname["namehash"] == json!(known))
        .expect("the child is still a subname");
    for field in ["name", "display_name", "owner", "registration_status"] {
        assert_eq!(row[field], subname[field], "{field}: {row:#} vs {subname:#}");
    }

    // Once 241 is published with the name's registry fact, the ordinary row alone serves it.
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[family_event(
            "rc-known-named",
            Some(&named),
            Some(named_resource),
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            241,
            0,
            json!({"source_event": "Transfer", "node": known, "owner": RC_OWNER,
                   "owner_getter": RC_OWNER, "emitter_role": "registry",
                   "authority_kind": "registry_only"}),
        )],
    )
    .await?;
    publish_test_families(&database, 241).await?;
    let rows = rows_of(&read_family_pages(&database, &uri).await?);
    let known_rows: Vec<&Value> = rows
        .iter()
        .filter(|row| row["name"] == json!("known.alpha.eth"))
        .collect();
    assert_eq!(known_rows.len(), 1, "{rows:#?}");
    assert_eq!(
        known_rows[0]["permission_resource_id"],
        json!(named_resource.to_string()),
        "{:#}",
        known_rows[0]
    );

    database.cleanup().await
}

/// A label preimage that arrives between two requests renames a registry child, which can move
/// it across a name-sorted cursor. The continuation retains its saved position; a fresh walk
/// serves the new name once.
#[tokio::test]
async fn v2_registry_child_rename_continues_a_name_sorted_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_children_fixture(&database).await?;
    let uri = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&sort=name&page_size=2");
    let (status, first) = read_family_response(&database, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{first:#}");
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("a second page")
        .to_owned();

    // Unchanged renderings: the continuation resumes.
    let (status, body) = read_family_response(&database, &format!("{uri}&cursor={cursor}")).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");

    insert_family_label_preimage(&database.pool, b"unknown").await?;
    let (status, error) =
        read_family_response(&database, &format!("{uri}&cursor={cursor}")).await?;
    assert_eq!(status, StatusCode::OK, "{error:#}");
    assert_eq!(error["data"][0]["name"], json!("gains.alpha.eth"));

    let rows = rows_of(&read_family_pages(&database, &uri).await?);
    let mut expected = vec![
        "alpha.eth",
        "gains.alpha.eth",
        "known.alpha.eth",
        "unknown.alpha.eth",
        "zeta.eth",
    ];
    expected.sort();
    assert_eq!(names_of(&rows), expected, "{rows:#?}");

    database.cleanup().await
}

/// When the renamed registry child is the continuation's anchor itself, the cursor's saved sort
/// value no longer matches the child. The read continues after the saved position; a malformed
/// cursor stays invalid.
#[tokio::test]
async fn v2_registry_child_rename_of_the_cursor_anchor_continues_from_its_position() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_children_fixture(&database).await?;
    let uri = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&sort=name&page_size=1");
    let (status, first) = read_family_response(&database, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{first:#}");
    // The placeholder sorts first, so it is the first page's only row and the cursor's anchor.
    assert_eq!(
        first["data"][0]["name"],
        json!(placeholder("unknown", "alpha.eth")),
        "{first:#}"
    );
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("a second page")
        .to_owned();

    insert_family_label_preimage(&database.pool, b"unknown").await?;
    let (status, error) =
        read_family_response(&database, &format!("{uri}&cursor={cursor}")).await?;
    assert_eq!(status, StatusCode::OK, "{error:#}");
    assert_eq!(error["data"][0]["name"], json!("alpha.eth"));

    let (status, error) =
        read_family_response(&database, &format!("{uri}&cursor=not-a-cursor")).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error:#}");

    database.cleanup().await
}

/// Re-encode an issued address-names cursor after `change`, as if a different publication had
/// issued it: its publication token no longer matches the served one.
fn stale_address_names_cursor(cursor: &str, change: impl FnOnce(&mut Value)) -> Result<String> {
    let mut payload: Value = serde_json::from_slice(&hex::decode(cursor)?)?;
    payload["snapshot"] = json!("a-superseded-publication");
    change(&mut payload);
    Ok(hex::encode(serde_json::to_vec(&payload)?))
}

/// Legacy publication and digest fields are ignored only after the query and position validate.
#[tokio::test]
async fn v2_legacy_ownership_cursor_continues_but_wrong_filter_and_bad_anchor_fail() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_children_fixture(&database).await?;
    let uri = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&sort=name&page_size=1");
    let (status, first) = read_family_response(&database, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{first:#}");
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("a second page")
        .to_owned();

    let stale = stale_address_names_cursor(&cursor, |_| {})?;
    let (status, error) = read_family_response(&database, &format!("{uri}&cursor={stale}")).await?;
    assert_eq!(status, StatusCode::OK, "{error:#}");

    let legacy = stale_address_names_cursor(&cursor, |payload| {
        payload["filters"]["registry_children"] = json!("old-rendering");
        payload["evaluated_at"] = json!("2020-01-01T00:00:00Z");
    })?;
    let (status, body) = read_family_response(&database, &format!("{uri}&cursor={legacy}")).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let wrong_filter = stale_address_names_cursor(&cursor, |payload| {
        payload["filters"]["address"] = json!("0x0000000000000000000000000000000000000001");
    })?;
    let bad_anchor = stale_address_names_cursor(&cursor, |payload| {
        payload["last_item"]["resource_id"] = json!("not-a-uuid");
    })?;
    for (case, cursor) in [("wrong address", wrong_filter), ("malformed anchor", bad_anchor)] {
        let (status, error) =
            read_family_response(&database, &format!("{uri}&cursor={cursor}")).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{case}: {error:#}");
        assert_eq!(error["error"]["code"], json!("invalid_input"), "{case}: {error:#}");
    }

    database.cleanup().await
}

/// A missing sort timestamp is the smallest value: undated rows come first ascending and last
/// descending, and a walk of two-row pages lists exactly the one-page answer.
async fn assert_undated_rows_are_smallest(database: &TestDatabase, base: &str, key: &str) -> Result<()> {
    for order in ["asc", "desc"] {
        let uri = format!("{base}&order={order}");
        let (status, body) = read_family_response(database, &format!("{uri}&page_size=50")).await?;
        anyhow::ensure!(status == StatusCode::OK, "{uri}: {body:#}");
        let whole = rows_of(&[body]);
        let walked = rows_of(&read_family_pages(database, &format!("{uri}&page_size=2")).await?);
        assert_eq!(walked, whole, "{uri}");

        let dated: Vec<bool> = whole
            .iter()
            .map(|row| row.get(key).is_some_and(|value| !value.is_null()))
            .collect();
        assert!(dated.contains(&true) && dated.contains(&false), "{uri}: {whole:#?}");
        // Ascending, the rows from the first dated one on are all dated; descending, the rows from
        // the first undated one on are all undated.
        let tail_dated = order == "asc";
        let boundary = dated.iter().position(|dated| *dated == tail_dated).expect("both kinds");
        assert!(
            dated[boundary..].iter().all(|dated| *dated == tail_dated),
            "{uri}: {whole:#?}"
        );
        let stamps: Vec<i64> = whole
            .iter()
            .filter_map(|row| row.get(key)?.as_str()?.parse().ok())
            .collect();
        let mut sorted = stamps.clone();
        sorted.sort();
        if order == "desc" {
            sorted.reverse();
        }
        assert_eq!(stamps, sorted, "{uri}");
    }
    Ok(())
}

#[tokio::test]
async fn v2_undated_address_names_sort_as_the_smallest_value_across_pages() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_children_fixture(&database).await?;
    // A surface-less registry child serves no timestamp, `created_at` included.
    for key in ["registered_at", "expires_at", "created_at"] {
        let base = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&sort={key}");
        assert_undated_rows_are_smallest(&database, &base, key).await?;
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_undated_subnames_sort_as_the_smallest_value_across_pages() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;
    for key in ["registered_at", "expires_at"] {
        let base = format!("/v1/names/alpha.eth/subnames?sort={key}");
        assert_undated_rows_are_smallest(&database, &base, key).await?;
    }
    database.cleanup().await
}

/// The expiry of the unnamed leases below, long past at every fixture block.
const LAPSED_EXPIRY: i64 = 1_600_000_000;

/// The events of one unnamed registrar lease on `node` under `family`, as the registrar adapter
/// emits them for a label bigname never observed: no name, the lease's resource, and the node
/// and labelhash in the after-state
/// (crates/adapters/src/schema_v2/protocol/v1/registrar/base.rs, `surface_known: false`).
fn unnamed_lease_events(
    identity: &str,
    kinds: &[&str],
    (family, namespace): (&str, &str),
    (node, labelhash): (&str, &str),
    (registrant, expiry): (&str, i64),
    block: i64,
    resource: Uuid,
) -> Vec<NormalizedEvent> {
    let source_event = if kinds.contains(&"RegistrationRenewed") {
        "NameRenewed"
    } else {
        "NameRegistered"
    };
    kinds
        .iter()
        .zip(0..)
        .map(|(kind, log)| {
            let mut event = family_event(
                &format!("{identity}-{kind}"),
                None,
                Some(resource),
                kind,
                family,
                block,
                log,
                json!({"source_event": source_event, "namehash": node, "labelhash": labelhash,
                       "token_id": labelhash, "registrant": registrant,
                       "authority_owner": registrant, "expiry": expiry, "surface_known": false,
                       "authority_kind": "registrar",
                       "authority_key": format!("registrar:{node}")}),
            );
            event.namespace = namespace.to_owned();
            event
        })
        .collect()
}

/// The `RegistrationReleased` Interpret synthesises at the first block past the lease's grace,
/// unnamed because no surface was materialized, before every transaction of its block
/// (crates/adapters/src/schema_v2.rs, `settle_block_boundary`).
fn synthesised_release(
    identity: &str,
    (family, namespace): (&str, &str),
    (node, labelhash): (&str, &str),
    block: i64,
    resource: Uuid,
) -> NormalizedEvent {
    let mut event = family_event(
        identity,
        None,
        Some(resource),
        "RegistrationReleased",
        family,
        block,
        0,
        json!({"source_event": "RegistrationReleased", "released_at": 1_700_000_000 + block,
               "labelhash": labelhash, "namehash": node, "expiry": LAPSED_EXPIRY}),
    );
    event.namespace = namespace.to_owned();
    event.log_index = None;
    event.transaction_hash = None;
    event
}

/// The registrar resource of [`seed_lapsed_registrar_child`]'s lease.
const LAPSED_LEASE: u128 = 0x7f3_0021;

/// `eth`, and under it `numeric`, a `.eth` name whose label bigname never observed: the
/// BaseRegistrar registers it to RC_OWNER at 202, writing the unnamed lease and, through
/// `setSubnodeOwner`, the registry NewOwner of its node
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L149 @ ens_v1@91c966f).
/// Published at 220. Returns the child node and its labelhash.
async fn seed_lapsed_registrar_child(database: &TestDatabase) -> Result<(String, String)> {
    seed_bounded_membership_blocks(database, 220).await?;
    seed_family_name(database, "eth", 0x7f3_0000, "ens_v1").await?;
    let node = insert_registry_child(
        database,
        "eth",
        "numeric",
        RC_OWNER,
        202,
        Uuid::from_u128(0x7f3_0011),
    )
    .await?;
    let labelhash = child_labelhash("numeric");
    upsert_test_resources(
        &database.pool,
        &[Resource {
            resource_id: Uuid::from_u128(LAPSED_LEASE),
            token_lineage_id: None,
            chain_id: FAMILY_CHAIN.to_owned(),
            block_hash: "0xhistory202".to_owned(),
            block_number: 202,
            provenance: json!({"authority_kind": "registrar"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &unnamed_lease_events(
            "rc-lapsed-grant",
            &["RegistrationGranted", "ExpiryChanged"],
            ("ens_v1_registrar_l1", "ens"),
            (&node, &labelhash),
            (RC_OWNER, LAPSED_EXPIRY),
            202,
            Uuid::from_u128(LAPSED_LEASE),
        ),
    )
    .await?;
    publish_test_families(database, 220).await?;
    Ok((node, labelhash))
}

/// The served subnames of `eth`, with their `total_count`, under `query`.
async fn eth_subnames(database: &TestDatabase, query: &str) -> Result<(Vec<Value>, Value)> {
    let pages =
        read_family_pages(database, &format!("/v1/names/eth/subnames?page_size=10{query}")).await?;
    Ok((rows_of(&pages), pages[0]["total_count"].clone()))
}

/// The child's registry owner and manager on both routes: listed for `address` under `owner`
/// and `manager`, and served as both on its parent's subnames row with `registration_status`.
async fn assert_child_served_to(
    database: &TestDatabase,
    node: &str,
    address: &str,
    registration_status: &str,
) -> Result<()> {
    for (matched, rows, _) in address_rows_by_relation(database, address).await? {
        let row = served_child(&rows, node);
        assert_eq!(row["relations"], matched, "{row:#}");
        assert_eq!(row["owner"], json!(address), "{row:#}");
        assert_eq!(row["manager"], json!(address), "{row:#}");
    }
    let (subnames, _) = eth_subnames(database, "").await?;
    let subname = served_child(&subnames, node);
    assert_eq!(subname["owner"], json!(address), "{subname:#}");
    assert_eq!(subname["manager"], json!(address), "{subname:#}");
    assert_eq!(
        subname["registration_status"],
        json!(registration_status),
        "{subname:#}"
    );
    Ok(())
}

/// A `.eth` name with no name surface keeps its registry record when its lease lapses: expiry
/// only makes the token unavailable
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f).
/// Once Interpret releases the lease, the registry owner is neither its owner nor its manager:
/// address names list it for no relation, and its subname is `released` with neither field and
/// no expiry fields, omitted under `include_expired=false`.
#[tokio::test]
async fn v2_released_surface_less_child_serves_no_owner_or_manager() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (node, labelhash) = seed_lapsed_registrar_child(&database).await?;
    assert_child_served_to(&database, &node, RC_OWNER, "unregistered").await?;
    let before = address_rows_by_relation(&database, RC_OWNER).await?;
    let (_, subnames_before) = eth_subnames(&database, "").await?;

    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[synthesised_release(
            "rc-lapsed-release",
            ("ens_v1_registrar_l1", "ens"),
            (&node, &labelhash),
            230,
            Uuid::from_u128(LAPSED_LEASE),
        )],
    )
    .await?;
    publish_test_families(&database, 231).await?;

    for ((relation, rows, total), (_, _, total_before)) in
        address_rows_by_relation(&database, RC_OWNER).await?.into_iter().zip(before)
    {
        assert!(
            rows.iter().all(|row| row["namehash"] != json!(node)),
            "{relation}: {rows:#?}"
        );
        assert_eq!(
            total.as_u64().map(|total| total + 1),
            total_before.as_u64(),
            "{relation}"
        );
    }
    let (subnames, total) = eth_subnames(&database, "").await?;
    assert_eq!(total, subnames_before, "{subnames:#?}");
    let subname = served_child(&subnames, &node);
    assert_eq!(subname["registration_status"], json!("released"), "{subname:#}");
    assert_eq!(subname["authority"], json!("ens_v1"), "{subname:#}");
    assert_eq!(subname["ens_v1"], json!({"expires_at": null}), "{subname:#}");
    for absent in [
        "owner",
        "manager",
        "expires_at",
        "grace_ends_at",
        "lapsed_registration",
    ] {
        assert_eq!(subname.get(absent), None, "{absent}: {subname:#}");
    }
    let (fenced, fenced_total) = eth_subnames(&database, "&include_expired=false").await?;
    assert!(
        fenced.iter().all(|row| row["namehash"] != json!(node)),
        "{fenced:#?}"
    );
    assert_eq!(
        fenced_total.as_u64().map(|total| total + 1),
        total.as_u64(),
        "{fenced:#?}"
    );

    database.cleanup().await
}

/// A registration in the block that released the lease is newer than the release, which
/// Interpret settles before every transaction of the block; `_register` sets the new registrant
/// as registry owner
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L147-L149 @ ens_v1@91c966f),
/// so the child is served to the new owner again and no longer to the old one.
#[tokio::test]
async fn v2_re_registered_surface_less_child_lists_for_its_new_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (node, labelhash) = seed_lapsed_registrar_child(&database).await?;
    let lease = Uuid::from_u128(LAPSED_LEASE);
    let mut events = vec![synthesised_release(
        "rc-rebought-release",
        ("ens_v1_registrar_l1", "ens"),
        (&node, &labelhash),
        230,
        lease,
    )];
    events.push(family_event(
        "rc-rebought-new-owner",
        None,
        Some(Uuid::from_u128(0x7f3_0011)),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        230,
        0,
        json!({"source_event": "NewOwner", "node": bigname_lookup::ens_namehash_hex("eth")?,
               "child_node": node, "labelhash": labelhash, "owner": RC_BUYER,
               "owner_getter": RC_BUYER, "emitter_role": "registry"}),
    ));
    events.extend(unnamed_lease_events(
        "rc-rebought-grant",
        &["RegistrationGranted", "ExpiryChanged"],
        ("ens_v1_registrar_l1", "ens"),
        (&node, &labelhash),
        (RC_BUYER, 1_900_000_000),
        230,
        lease,
    ));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_test_families(&database, 231).await?;

    assert_child_served_to(&database, &node, RC_BUYER, "unregistered").await?;
    for (relation, rows, _) in address_rows_by_relation(&database, RC_OWNER).await? {
        assert!(
            rows.iter().all(|row| row["namehash"] != json!(node)),
            "{relation}: {rows:#?}"
        );
    }

    database.cleanup().await
}

/// A renewal in grace keeps the lease, so the registry owner stays the child's owner and manager.
#[tokio::test]
async fn v2_renewed_surface_less_child_keeps_its_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (node, labelhash) = seed_lapsed_registrar_child(&database).await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &unnamed_lease_events(
            "rc-renewed",
            &["RegistrationRenewed", "ExpiryChanged"],
            ("ens_v1_registrar_l1", "ens"),
            (&node, &labelhash),
            (RC_OWNER, 1_900_000_000),
            230,
            Uuid::from_u128(LAPSED_LEASE),
        ),
    )
    .await?;
    publish_test_families(&database, 231).await?;

    assert_child_served_to(&database, &node, RC_OWNER, "unregistered").await?;

    database.cleanup().await
}

/// Publish the Base families at `target` over blocks 200..=241 of `base-mainnet`.
async fn publish_base_families(database: &TestDatabase, target: i64) -> Result<()> {
    const BASE: &str = "base-mainnet";
    let blocks = (200..=241)
        .map(|number| {
            raw_block(
                BASE,
                &format!("0xhistory{number}"),
                (number > 200)
                    .then(|| format!("0xhistory{}", number - 1))
                    .as_deref(),
                number,
                1_700_000_000 + number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    let hash = format!("0xhistory{target}");
    let timestamp = OffsetDateTime::from_unix_timestamp(1_700_000_000 + target)?;
    database
        .seed_snapshot_selector_chain_positions(&json!({"base": {
            "chain_id": BASE, "block_number": target, "block_hash": hash,
            "timestamp": timestamp.format(&time::format_description::well_known::Rfc3339)?
        }}))
        .await?;
    rebuild_fixture_families(&database.pool, BASE, target, &hash).await
}

/// The Basenames registrar runs the same lease and release as the ENSv1 BaseRegistrar
/// (crates/adapters/src/schema_v2/protocol/v1.rs routes `basenames_base_registrar` to the
/// registrar adapter), so a released surface-less child of a Basenames parent loses its owner
/// and manager on the subnames route too.
#[tokio::test]
async fn v2_released_surface_less_basenames_child_serves_no_owner_or_manager() -> Result<()> {
    const BASE: &str = "base-mainnet";
    let database = TestDatabase::new_migrated().await?;
    seed_family_name_on(&database, "base.eth", 0x7f4_0000, "basenames", "basenames", BASE)
        .await?;
    let parent = bigname_lookup::ens_namehash_hex("base.eth")?;
    let labelhash = child_labelhash("numeric");
    let node = format!(
        "{:#x}",
        alloy_primitives::keccak256(
            [
                alloy_primitives::hex::decode(&parent)?,
                alloy_primitives::hex::decode(&labelhash)?
            ]
            .concat()
        )
    );
    let lease = Uuid::from_u128(0x7f4_0021);
    upsert_test_resources(
        &database.pool,
        &[Resource {
            resource_id: lease,
            token_lineage_id: None,
            chain_id: BASE.to_owned(),
            block_hash: "0xhistory202".to_owned(),
            block_number: 202,
            provenance: json!({"authority_kind": "registrar"}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    let mut events = vec![family_event(
        "rc-base-new-owner",
        None,
        None,
        "SubregistryChanged",
        "basenames_base_registry",
        202,
        0,
        json!({"source_event": "NewOwner", "node": parent, "child_node": node,
               "labelhash": labelhash, "owner": RC_OWNER}),
    )];
    events.extend(unnamed_lease_events(
        "rc-base-grant",
        &["RegistrationGranted", "ExpiryChanged"],
        ("basenames_base_registrar", "basenames"),
        (&node, &labelhash),
        (RC_OWNER, LAPSED_EXPIRY),
        202,
        lease,
    ));
    for event in &mut events {
        event.namespace = "basenames".to_owned();
        event.chain_id = Some(BASE.to_owned());
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_base_families(&database, 220).await?;
    let uri = "/v1/names/base.eth/subnames?namespace=basenames&page_size=10";
    let subnames = rows_of(&read_family_pages(&database, uri).await?);
    let subname = served_child(&subnames, &node);
    assert_eq!(subname["owner"], json!(RC_OWNER), "{subname:#}");
    assert_eq!(subname["manager"], json!(RC_OWNER), "{subname:#}");

    let mut release = synthesised_release(
        "rc-base-release",
        ("basenames_base_registrar", "basenames"),
        (&node, &labelhash),
        230,
        lease,
    );
    release.chain_id = Some(BASE.to_owned());
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[release]).await?;
    publish_base_families(&database, 231).await?;
    let subnames = rows_of(&read_family_pages(&database, uri).await?);
    let subname = served_child(&subnames, &node);
    assert_eq!(subname["registration_status"], json!("released"), "{subname:#}");
    assert_eq!(subname.get("owner"), None, "{subname:#}");
    assert_eq!(subname.get("manager"), None, "{subname:#}");

    database.cleanup().await
}
