// A name surface that stores no raw bytes (only its node and label-hash path) is served through
// the ordinary name routes, named at read time: each label its verified preimage text, else
// `[<64 hex labelhash>]`.
//
// This reader fixture inserts the rows directly; v2_registry_token_ids/node_identity.rs
// separately exercises actual Interpret output: the surface, the
// node's registry-only resource, an open `declared_registry_path` binding, and the events a
// registry NewOwner of a node with a surface produces today (`SubregistryChanged`,
// `AuthorityTransferred` and the `SurfaceBound` of the registry-only binding, all naming the
// child), with `name_identity_observed` on the NewOwner.

const TL_ADDRESS: &str = "0x00000000000000000000000000000000000000e1";

/// The fixture's label hashes, and the names it serves while `alpha` and `eth` have preimages.
struct Textless {
    /// keccak256("first"): a test can add its preimage later.
    first: String,
    /// A labelhash with no preimage.
    nested: String,
    first_name: String,
    nested_name: String,
    known_name: String,
    /// The child whose preimage `Upper` is not normalized.
    unverified_name: String,
    /// The child whose preimage is not text.
    undecodable_name: String,
}

impl Textless {
    fn names(&self) -> [&String; 5] {
        [
            &self.first_name,
            &self.nested_name,
            &self.known_name,
            &self.unverified_name,
            &self.undecodable_name,
        ]
    }
}

fn tl_bracket(labelhash: &str) -> String {
    format!("[{}]", labelhash.trim_start_matches("0x"))
}

/// `name` with its brackets percent-encoded for a request path.
fn tl_path(name: &str) -> String {
    name.replace('[', "%5B").replace(']', "%5D")
}

async fn tl_get(database: &TestDatabase, uri: &str) -> Result<Value> {
    let (status, body) = read_family_response(database, uri).await?;
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {status} {body:#}");
    Ok(body)
}

/// The events that create a registry child with a surface at `block`: the registry NewOwner
/// under `parent_node`, and the binding of the child's surface to its registry record.
fn textless_child_events(
    id: &str,
    resource: Uuid,
    parent_node: &str,
    labelhash: &str,
    owner: &str,
    block: i64,
) -> Vec<NormalizedEvent> {
    let node = id.strip_prefix("ens:").expect("ens id");
    let new_owner = json!({"source_event": "NewOwner", "node": parent_node, "child_node": node,
        "labelhash": labelhash, "owner": owner, "owner_getter": owner,
        "emitter_role": "registry", "authority_kind": "registry_only",
        "name_identity_observed": true});
    let event = |kind: &str, after_state: Value| {
        family_event(
            &format!("tl-{kind}-{node}"),
            Some(id),
            Some(resource),
            kind,
            "ens_v1_registry_l1",
            block,
            0,
            after_state,
        )
    };
    vec![
        event("SubregistryChanged", new_owner.clone()),
        event("AuthorityTransferred", new_owner),
        event(
            "SurfaceBound",
            json!({"state_derived": true, "surface_materialization": true,
                   "source_event": "NewOwner", "node": node,
                   "authority_kind": "registry_only", "owner": owner, "owner_getter": owner,
                   "binding_kind": "declared_registry_path",
                   "active_from": 1_700_000_000 + block}),
        ),
    ]
}

/// alpha.eth with bytes, registered to RC_OWNER at 201. Below it `first` (202), and below
/// `first`: `nested` (203), `known` (204, a usable preimage), `unverified` (205, a preimage that
/// is not normalized) and `undecodable` (206, a preimage that is not text). None of the five
/// stores bytes; each is a registry child owned by RC_OWNER. `first` points at FAMILY_RESOLVER
/// from 207, where its `addr:60` is set at 208. Published at 240. `ancestors_known` adds the
/// `alpha` and `eth` preimages that the observation of alpha.eth's bytes brings.
async fn seed_textless_fixture(database: &TestDatabase, ancestors_known: bool) -> Result<Textless> {
    seed_textless_fixture_with(database, ancestors_known, false).await
}

/// [`seed_textless_fixture`], with the two names of [`decoded_textless_events`] when `decoded`.
async fn seed_textless_fixture_with(
    database: &TestDatabase,
    ancestors_known: bool,
    decoded: bool,
) -> Result<Textless> {
    seed_bounded_membership_blocks(database, 240).await?;
    let (alpha, alpha_resource) =
        seed_family_name(database, "alpha.eth", 0x8a1_0000, "ens_v1").await?;
    let alpha_node = alpha.strip_prefix("ens:").expect("ens id").to_owned();
    let mut preimages: Vec<&[u8]> = vec![b"known", b"Upper", &[0xff, 0xfe]];
    if ancestors_known {
        preimages.extend([b"alpha".as_slice(), b"eth"]);
    }
    for preimage in preimages {
        insert_family_label_preimage(&database.pool, preimage).await?;
    }
    let hash = |label: &[u8]| format!("{:#x}", alloy_primitives::keccak256(label));
    let (first, nested) = (hash(b"first"), format!("0x{}", "22".repeat(32)));
    let first_path = vec![first.clone(), hash(b"alpha"), hash(b"eth")];
    let (first_id, first_resource) =
        seed_textless_family_name(database, &first_path, 0x8b1_0000, "ens_v1", 202).await?;
    let first_node = first_id.strip_prefix("ens:").expect("ens id").to_owned();
    let mut events =
        textless_child_events(&first_id, first_resource, &alpha_node, &first, RC_OWNER, 202);
    let children = [
        nested.clone(),
        hash(b"known"),
        hash(b"Upper"),
        hash(&[0xff, 0xfe]),
    ];
    for (index, labelhash) in children.iter().enumerate() {
        let block = 203 + index as i64;
        let (id, resource) = seed_textless_family_name(
            database,
            &[vec![labelhash.clone()], first_path.clone()].concat(),
            0x8c1_0000 + 0x10 * index as u128,
            "ens_v1",
            block,
        )
        .await?;
        events.extend(textless_child_events(&id, resource, &first_node, labelhash, RC_OWNER, block));
    }
    let manifest_id = seed_family_resolver_declaration(database).await?;
    let mut record = family_event(
        "tl-first-addr",
        None,
        None,
        "RecordChanged",
        "ens_v1_resolver_l1",
        208,
        0,
        json!({"source_event": "AddressChanged", "node": first_node,
               "resolver": FAMILY_RESOLVER, "record_key": "addr:60", "record_family": "addr",
               "selector_key": "60", "value": TL_ADDRESS}),
    );
    record.raw_fact_ref["emitting_address"] = json!(FAMILY_RESOLVER);
    record.source_manifest_id = Some(manifest_id);
    record.manifest_version = 1;
    record.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    events.extend([
        family_event(
            "tl-alpha-grant",
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
            "tl-first-resolver",
            Some(&first_id),
            Some(first_resource),
            "ResolverChanged",
            "ens_v1_registry_l1",
            207,
            0,
            json!({"node": first_node, "resolver": FAMILY_RESOLVER}),
        ),
        record,
    ]);
    if decoded {
        events.extend(decoded_textless_events(database, &alpha_node, &first_node).await?);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_test_families(database, 240).await?;
    let first_name = format!("{}.alpha.eth", tl_bracket(&first));
    Ok(Textless {
        nested_name: format!("{}.{first_name}", tl_bracket(&nested)),
        known_name: format!("known.{first_name}"),
        unverified_name: format!("{}.{first_name}", tl_bracket(&hash(b"Upper"))),
        undecodable_name: format!("{}.{first_name}", tl_bracket(&hash(&[0xff, 0xfe]))),
        first_name,
        first,
        nested,
    })
}

/// The value at `field` of every row of `pages`, in served order.
fn tl_column(pages: &[Value], field: &str) -> Vec<String> {
    rows_of(pages)
        .iter()
        .map(|row| row[field].as_str().expect("a string field").to_owned())
        .collect()
}

#[tokio::test]
async fn v2_textless_name_detail_is_served_by_its_bracketed_route() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;

    let detail = tl_get(&database, &format!("/v1/names/{}", tl_path(&fixture.first_name))).await?;
    let data = &detail["data"];
    assert_eq!(data["name"], json!(fixture.first_name), "{detail:#}");
    assert_eq!(data["display_name"], json!(fixture.first_name), "{detail:#}");
    let first_node = label_path_node(&[
        fixture.first.clone(),
        child_labelhash("alpha"),
        child_labelhash("eth"),
    ])?;
    assert_eq!(data["namehash"], json!(first_node), "{detail:#}");
    assert_eq!(data["read_status"], json!("ok"), "{detail:#}");
    // The registry-only binding with an owner reads as it does for a name with bytes.
    assert_eq!(data["status"], json!("active"), "{detail:#}");
    assert_eq!(data["authority"], json!("ens_v1"), "{detail:#}");
    assert_eq!(
        data["ens_v1"],
        json!({
            "expires_at": null,
            "resolver": {"chain_id": 1, "address": "0x0000000000000000000000000000000000000abc"},
            "wrapper_state": "unwrapped"
        }),
        "{detail:#}"
    );
    assert_eq!(data["owner"], json!(RC_OWNER), "{detail:#}");
    assert_eq!(data["manager"], json!(RC_OWNER), "{detail:#}");
    assert_eq!(data["created_at"], json!("1700000202"), "{detail:#}");
    assert!(data.get("registered_at").is_none(), "{detail:#}");
    assert!(data.get("expires_at").is_none(), "{detail:#}");
    assert_eq!(
        data["registration_id"],
        json!(Uuid::from_u128(0x8b1_0000).to_string()),
        "{detail:#}"
    );
    assert_eq!(data["resolver"]["address"], json!(FAMILY_RESOLVER), "{detail:#}");
    assert_eq!(data["records"]["addresses"], json!({"60": TL_ADDRESS}), "{detail:#}");
    assert_eq!(data["primary_address"], json!(TL_ADDRESS), "{detail:#}");

    let records =
        tl_get(&database, &format!("/v1/names/{}/records", tl_path(&fixture.first_name))).await?;
    assert_eq!(records["data"]["resolver"]["address"], json!(FAMILY_RESOLVER), "{records:#}");
    assert_eq!(
        records["data"]["records"]["addr:60"],
        json!({"status": "ok", "value": TL_ADDRESS}),
        "{records:#}"
    );

    // A label with a usable preimage reads as text beside its parent's placeholder; a preimage
    // that is not normalized, or is not text, leaves the placeholder.
    for name in fixture.names() {
        let detail = tl_get(&database, &format!("/v1/names/{}", tl_path(name))).await?;
        assert_eq!(detail["data"]["name"], json!(name), "{detail:#}");
        assert_eq!(detail["data"]["display_name"], json!(name), "{detail:#}");
    }
    // The readable label is also addressable by its labelhash.
    let known = tl_get(&database, &format!("/v1/names/{}", tl_path(&fixture.known_name))).await?;
    let hashed = format!("{}.{}", tl_bracket(&child_labelhash("known")), fixture.first_name);
    let by_hash = tl_get(&database, &format!("/v1/names/{}", tl_path(&hashed))).await?;
    assert_eq!(by_hash["data"], known["data"]);

    database.cleanup().await
}

#[tokio::test]
async fn v2_textless_name_with_no_known_label_is_all_placeholders() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // No preimage for `alpha` or `eth`: the surface names nothing but its own label hashes.
    let fixture = seed_textless_fixture(&database, false).await?;
    let ancestors = format!(
        "{}.{}",
        tl_bracket(&child_labelhash("alpha")),
        tl_bracket(&child_labelhash("eth"))
    );
    let first = format!("{}.{ancestors}", tl_bracket(&fixture.first));

    // The route still reads `alpha.eth` as the labels it hashes to.
    for route in [&fixture.first_name, &first] {
        let detail = tl_get(&database, &format!("/v1/names/{}", tl_path(route))).await?;
        assert_eq!(detail["data"]["name"], json!(first), "{route}: {detail:#}");
        assert_eq!(detail["data"]["display_name"], json!(first), "{route}: {detail:#}");
    }
    let nested = tl_get(&database, &format!("/v1/names/{}", tl_path(&fixture.nested_name))).await?;
    assert_eq!(
        nested["data"]["name"],
        json!(format!("{}.{first}", tl_bracket(&fixture.nested))),
        "{nested:#}"
    );
    let known = tl_get(&database, &format!("/v1/names/{}", tl_path(&fixture.known_name))).await?;
    assert_eq!(known["data"]["name"], json!(format!("known.{first}")), "{known:#}");
    // alpha.eth stores its bytes and is served under them.
    let alpha = tl_get(&database, "/v1/names/alpha.eth").await?;
    assert_eq!(alpha["data"]["name"], json!("alpha.eth"), "{alpha:#}");

    database.cleanup().await
}

#[tokio::test]
async fn v2_textless_name_takes_label_text_from_a_preimage_at_read_time() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let bracketed = format!("/v1/names/{}", tl_path(&fixture.first_name));
    let before = tl_get(&database, &bracketed).await?;

    // The preimage arrives with no new publication.
    insert_family_label_preimage(&database.pool, b"first").await?;
    let after = tl_get(&database, &bracketed).await?;
    assert_eq!(after["data"]["name"], json!("first.alpha.eth"), "{after:#}");
    assert_eq!(after["data"]["display_name"], json!("first.alpha.eth"), "{after:#}");
    let mut renamed = before["data"].clone();
    renamed["name"] = json!("first.alpha.eth");
    renamed["display_name"] = json!("first.alpha.eth");
    assert_eq!(after["data"], renamed, "only the name changes");
    let readable = tl_get(&database, "/v1/names/first.alpha.eth").await?;
    assert_eq!(readable["data"], after["data"]);

    let nested = tl_get(&database, &format!("/v1/names/{}", tl_path(&fixture.nested_name))).await?;
    let nested_name = format!("{}.first.alpha.eth", tl_bracket(&fixture.nested));
    assert_eq!(nested["data"]["name"], json!(nested_name), "{nested:#}");
    let subnames = read_family_pages(&database, "/v1/names/first.alpha.eth/subnames?page_size=10")
        .await?;
    assert!(
        tl_column(&subnames, "name").contains(&"known.first.alpha.eth".to_owned()),
        "{subnames:#?}"
    );
    let history = tl_get(&database, "/v1/names/first.alpha.eth/history?page_size=50").await?;
    for row in history["data"].as_array().expect("rows") {
        assert_eq!(row["name"], json!("first.alpha.eth"), "{history:#}");
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_textless_name_history_reads_every_scope() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let base = format!("/v1/names/{}/history", tl_path(&fixture.first_name));

    let mut ids = BTreeMap::new();
    for scope in ["name", "registration", "both"] {
        let uri = format!("{base}?scope={scope}&page_size=50");
        let page = tl_get(&database, &uri).await?;
        let rows = page["data"].as_array().expect("rows");
        assert!(!rows.is_empty(), "{uri}: {page:#}");
        assert_eq!(page["page"]["total_count"], json!(rows.len()), "{uri}: {page:#}");
        for row in rows {
            assert_eq!(row["name"], json!(fixture.first_name), "{uri}: {row:#}");
        }
        // One row at a time, continued by cursor under the bracketed spelling.
        let paged = read_family_pages(&database, &format!("{base}?scope={scope}&page_size=1")).await?;
        let paged_ids = tl_column(&paged, "id");
        assert_eq!(
            paged_ids,
            rows.iter().map(|row| row["id"].as_str().expect("id").to_owned()).collect::<Vec<_>>(),
            "{uri}"
        );
        let types: BTreeSet<String> =
            rows.iter().map(|row| row["type"].as_str().expect("type").to_owned()).collect();
        ids.insert(scope, (paged_ids.into_iter().collect::<BTreeSet<_>>(), types));
    }
    let (name, registration, both) = (&ids["name"], &ids["registration"], &ids["both"]);
    // The NewOwner and the resolver change name the surface; the record write is the resource's.
    assert_eq!(
        name.1,
        BTreeSet::from(["authority".to_owned(), "resolver".to_owned(), "subregistry".to_owned()])
    );
    assert!(registration.1.contains("record"), "{registration:?}");
    assert_eq!(both.0, name.0.union(&registration.0).cloned().collect::<BTreeSet<_>>());

    database.cleanup().await
}

#[tokio::test]
async fn v2_textless_parent_lists_and_counts_its_subnames_once() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let children: BTreeSet<&String> = fixture.names().into_iter().skip(1).collect();
    let base = format!("/v1/names/{}/subnames", tl_path(&fixture.first_name));

    let whole = read_family_pages(&database, &format!("{base}?page_size=10&include=counts")).await?;
    let names = tl_column(&whole, "name");
    assert_eq!(names.len(), 4, "{whole:#?}");
    assert_eq!(names.iter().collect::<BTreeSet<_>>(), children, "{whole:#?}");
    assert_eq!(whole[0]["total_count"], json!(4), "{whole:#?}");
    for row in rows_of(&whole) {
        assert_eq!(row["display_name"], row["name"], "{row:#}");
        assert_eq!(row["owner"], json!(RC_OWNER), "{row:#}");
        assert_eq!(row["status"], json!("active"), "{row:#}");
        assert_eq!(row["authority"], json!("ens_v1"), "{row:#}");
        assert_eq!(row["subname_count"], json!(0), "{row:#}");
        // Each row is the name its own detail route serves.
        let detail =
            tl_get(&database, &format!("/v1/names/{}", tl_path(row["name"].as_str().unwrap())))
                .await?;
        assert_eq!(detail["data"]["namehash"], row["namehash"], "{row:#}");
    }
    // The same rows in the same order one at a time, and under the other sorts.
    let paged = read_family_pages(&database, &format!("{base}?page_size=1")).await?;
    assert_eq!(tl_column(&paged, "name"), names);
    // The timestamp sorts, on rows that have neither an expiry nor a registration time.
    for sort in ["expires_at", "registered_at"] {
        let sorted =
            read_family_pages(&database, &format!("{base}?page_size=3&sort={sort}")).await?;
        assert_eq!(
            tl_column(&sorted, "name").into_iter().collect::<BTreeSet<_>>(),
            names.iter().cloned().collect::<BTreeSet<_>>(),
            "sort={sort}"
        );
        assert_eq!(rows_of(&sorted).len(), 4, "sort={sort}");
    }
    let filtered = read_family_pages(&database, &format!("{base}?page_size=10&q=known")).await?;
    assert_eq!(tl_column(&filtered, "name"), [fixture.known_name.as_str()]);

    // The parent with bytes lists the child without bytes once, with its own children counted.
    let parent =
        read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=10&include=counts")
            .await?;
    let rows = rows_of(&parent);
    assert_eq!(rows.len(), 1, "{parent:#?}");
    assert_eq!(rows[0]["name"], json!(fixture.first_name), "{parent:#?}");
    assert_eq!(rows[0]["labelhash"], json!(fixture.first), "{parent:#?}");
    assert_eq!(rows[0]["subname_count"], json!(4), "{parent:#?}");

    database.cleanup().await
}

#[tokio::test]
async fn v2_textless_names_are_searched_by_their_readable_labels() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;

    let known = read_family_pages(&database, "/v1/search?q=known&page_size=10").await?;
    assert_eq!(tl_column(&known, "name"), [fixture.known_name.as_str()], "{known:#?}");

    // Every name of the fixture contains `alpha`: the one with bytes and the five without,
    // each once, in one order whatever the page size.
    let whole =
        read_family_pages(&database, "/v1/search?q=alpha&match=contains&page_size=10").await?;
    let names = tl_column(&whole, "name");
    let mut expected: Vec<String> = fixture.names().into_iter().cloned().collect();
    expected.push("alpha.eth".to_owned());
    assert_eq!(names.len(), expected.len(), "{whole:#?}");
    assert_eq!(
        names.iter().collect::<BTreeSet<_>>(),
        expected.iter().collect::<BTreeSet<_>>(),
        "{whole:#?}"
    );
    for page_size in [1, 2] {
        let paged = read_family_pages(
            &database,
            &format!("/v1/search?q=alpha&match=contains&page_size={page_size}"),
        )
        .await?;
        assert_eq!(tl_column(&paged, "name"), names, "page_size={page_size}");
    }
    // A label with no usable text is not found by text it does not serve.
    let (status, upper) = read_family_response(&database, "/v1/search?q=upper&page_size=10").await?;
    assert_eq!(status, StatusCode::OK, "{upper:#}");
    assert_eq!(upper["data"], json!([]), "{upper:#}");

    database.cleanup().await
}

#[tokio::test]
async fn v2_textless_names_are_listed_once_for_their_owner() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let mut expected: BTreeSet<String> = fixture.names().into_iter().cloned().collect();
    expected.insert("alpha.eth".to_owned());

    // The registry-child fallback lists a node with no name row; these have one, so each is
    // listed by the ordinary row alone.
    for query in ["", "&sort=name", "&sort=created_at", "&sort=expires_at", "&relation=manager"] {
        let uri = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=2{query}");
        let pages = read_family_pages(&database, &uri).await?;
        let names = tl_column(&pages, "name");
        assert_eq!(names.len(), expected.len(), "{uri}: {pages:#?}");
        assert_eq!(names.into_iter().collect::<BTreeSet<_>>(), expected, "{uri}");
        assert_eq!(pages[0]["total_count"], json!(expected.len()), "{uri}: {pages:#?}");
        for row in rows_of(&pages) {
            if row["name"] == json!("alpha.eth") {
                continue;
            }
            assert_eq!(row["display_name"], row["name"], "{uri}: {row:#}");
            let relations = if query == "&relation=manager" {
                json!(["manager"])
            } else {
                json!(["owner", "manager"])
            };
            assert_eq!(row["relations"], relations, "{uri}: {row:#}");
            assert_eq!(row["status"], json!("active"), "{uri}: {row:#}");
            assert!(row["created_at"].is_string(), "{uri}: {row:#}");
        }
    }

    database.cleanup().await
}
