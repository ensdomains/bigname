// `GET /v1/addresses/{address}/names` lists an ENSv1 registry child that has no name surface (a
// node a registry NewOwner created and no label-bearing event named) for its current registry
// owner, as `manager`, in the form its parent's subnames route serves it.

const RC_OWNER: &str = "0x00000000000000000000000000000000000000d1";
const RC_BUYER: &str = "0x00000000000000000000000000000000000000d2";

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
                   "emitter_role": "registry"}),
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
        for field in ["name", "display_name", "owner", "registration_status"] {
            assert_eq!(
                row[field], subname[field],
                "{field}: {row:#} vs {subname:#}"
            );
        }
        assert_eq!(row["owner"], json!(RC_OWNER), "{row:#}");
        assert_eq!(
            row["permission_resource_id"],
            json!(resource.to_string()),
            "{row:#}"
        );
        assert_eq!(row["relations"], json!(["manager"]), "{row:#}");
        assert_eq!(row["is_primary"], json!(false), "{row:#}");
        for absent in [
            "registrant",
            "registered_at",
            "created_at",
            "expires_at",
            "authority",
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
        "relation=manager",
        "dedupe=registration",
        "is_migrated=false",
        "sort=expires_at",
        "sort=registered_at&order=desc",
        "sort=name&order=desc",
        "include=counts",
        "include=role_summary",
    ] {
        let rows = read(query.to_owned()).await?;
        assert!(listed(&rows, "known.alpha.eth"), "{query}: {rows:#?}");
        assert!(listed(&rows, &unknown_name), "{query}: {rows:#?}");
    }
    for query in ["relation=owner", "authority=ens_v1", "is_migrated=true"] {
        let rows = read(query.to_owned()).await?;
        assert!(!listed(&rows, "known.alpha.eth"), "{query}: {rows:#?}");
        assert!(!listed(&rows, &unknown_name), "{query}: {rows:#?}");
    }
    // An unknown expiry sorts last ascending, as for any row without one.
    let by_expiry = read("sort=expires_at".to_owned()).await?;
    let first_unknown = by_expiry
        .iter()
        .position(|row| row.get("expires_at").is_none())
        .expect("a row without an expiry");
    assert!(
        by_expiry[first_unknown..]
            .iter()
            .all(|row| row.get("expires_at").is_none()),
        "{by_expiry:#?}"
    );
    // `q` matches the served text.
    let rows = read("q=kno".to_owned()).await?;
    assert_eq!(names_of(&rows), ["known.alpha.eth"], "{rows:#?}");

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
/// it across a name-sorted cursor, so the continuation asks for a restart; a fresh read then
/// serves the new name once.
#[tokio::test]
async fn v2_registry_child_rename_restarts_a_name_sorted_read() -> Result<()> {
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
    assert_eq!(status, StatusCode::CONFLICT, "{error:#}");
    assert_eq!(error["error"]["code"], json!("stale"), "{error:#}");

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
/// value no longer matches the child. The read still answers the restart (409 stale) that a
/// rendering change calls for, not an invalid cursor; a cursor that is malformed stays invalid.
#[tokio::test]
async fn v2_registry_child_rename_of_the_cursor_anchor_restarts_the_read() -> Result<()> {
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
    assert_eq!(status, StatusCode::CONFLICT, "{error:#}");
    assert_eq!(error["error"]["code"], json!("stale"), "{error:#}");

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

/// The ownership cursor's shape and binding are checked before its publication: a legacy
/// eight-key cursor, or one whose anchor is malformed, is invalid input (400) even when the
/// publication it came from is gone, and only a well-formed stale cursor restarts (409).
#[tokio::test]
async fn v2_a_stale_malformed_ownership_cursor_is_invalid_before_it_is_stale() -> Result<()> {
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
    assert_eq!(status, StatusCode::CONFLICT, "{error:#}");
    assert_eq!(error["error"]["code"], json!("stale"), "{error:#}");

    let legacy = stale_address_names_cursor(&cursor, |payload| {
        let removed = payload["filters"]
            .as_object_mut()
            .and_then(|filters| filters.remove("registry_children"));
        assert!(removed.is_some(), "an issued cursor binds the registry children");
    })?;
    let bad_anchor = stale_address_names_cursor(&cursor, |payload| {
        payload["last_item"]["resource_id"] = json!("not-a-uuid");
    })?;
    for (case, cursor) in [("legacy eight-key", legacy), ("malformed anchor", bad_anchor)] {
        let (status, error) =
            read_family_response(&database, &format!("{uri}&cursor={cursor}")).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{case}: {error:#}");
        assert_eq!(error["error"]["code"], json!("invalid_input"), "{case}: {error:#}");
    }

    database.cleanup().await
}
