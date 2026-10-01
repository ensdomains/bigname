
const CHILD_ROOT_REGISTRY: &str = "0x00000000000000000000000000000000000000b0";
const CHILD_ALPHA_REGISTRY: &str = "0x00000000000000000000000000000000000000b1";
const CHILD_OWNER: &str = "0x00000000000000000000000000000000000000c1";

fn child_labelhash(label: &str) -> String {
    format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()))
}

/// One ENSv2 registry event of the fixture: emitted by `emitter`, which the served registry
/// labels read as the registration's registry.
fn child_registry_event(
    identity: &str,
    logical_name_id: Option<&str>,
    kind: &str,
    block: i64,
    log: i64,
    emitter: &str,
    after_state: Value,
) -> NormalizedEvent {
    let mut event = family_event(
        identity,
        logical_name_id,
        None,
        kind,
        "ens_v2_registry_l1",
        block,
        log,
        after_state,
    );
    event.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
    event.raw_fact_ref = json!({
        "kind": "raw_log",
        "event_identity": identity,
        "emitting_address": emitter,
    });
    event
}

/// alpha.eth, bound at 200, holds two ENSv1 registry edges (carol, dave: no surface, labels by
/// preimage) and, through its ENSv2 subregistry `b1`, two registrations (one, two: surfaced).
/// dave's node is later transferred to the zero owner, which is its node's current registry owner,
/// so dave has no owner and is not listed. one.alpha.eth holds an ENSv1 edge of its own (erin),
/// so its subname count is one. Published at 240.
async fn seed_family_children_fixture(database: &TestDatabase) -> Result<()> {
    seed_family_children_fixture_expiring(database, 1_900_000_000, 1_900_000_000).await
}

/// The fixture with one.alpha.eth's and two.alpha.eth's registrations expiring at `one_expiry`
/// and `two_expiry` seconds.
async fn seed_family_children_fixture_expiring(
    database: &TestDatabase,
    one_expiry: i64,
    two_expiry: i64,
) -> Result<()> {
    seed_bounded_membership_blocks(database, 240).await?;
    let (alpha, alpha_resource) =
        seed_family_name(database, "alpha.eth", 0x6a1_0000, "ens_v1").await?;
    let (one, _) = seed_family_name(database, "one.alpha.eth", 0x6b1_0000, "ens_v2").await?;
    let (two, _) = seed_family_name(database, "two.alpha.eth", 0x6c1_0000, "ens_v2").await?;
    let alpha_node = alpha.strip_prefix("ens:").expect("ens id").to_owned();
    for label in ["carol", "dave", "erin", "one", "two"] {
        insert_family_label_preimage(&database.pool, label.as_bytes()).await?;
    }
    // The ENSv2 subregistry's contract instance, which the registrations name.
    let alpha_registry = Uuid::from_u128(0x6b0_0001);
    sqlx::query(
        "INSERT INTO bigname_phase.contract_instances (contract_instance_id, chain_id,
             contract_kind)
         VALUES ($1, $2, 'contract') ON CONFLICT DO NOTHING",
    )
    .bind(alpha_registry)
    .bind(FAMILY_CHAIN)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.contract_instance_addresses (contract_instance_id, chain_id,
             address, active_from_block_number)
         VALUES ($1, $2, $3, 200)",
    )
    .bind(alpha_registry)
    .bind(FAMILY_CHAIN)
    .bind(CHILD_ALPHA_REGISTRY)
    .execute(&database.pool)
    .await?;
    for (parent, label, block) in [("alpha.eth", "carol", 202), ("alpha.eth", "dave", 202), ("one.alpha.eth", "erin", 207)] {
        let labelhash = child_labelhash(label);
        insert_family_registry_child_edge(&database.pool, "ens", FAMILY_CHAIN, parent, &labelhash,
            CHILD_OWNER, block, &format!("0xhistory{block}")).await?;
    }
    let registration = |identity: &str, name: &str, block: i64, expiry: i64| {
        child_registry_event(
            identity,
            Some(name),
            "RegistrationGranted",
            block,
            0,
            CHILD_ALPHA_REGISTRY,
            json!({"registry_contract_instance_id": alpha_registry.to_string(),
                   "status": "registered", "registrant": CHILD_OWNER,
                   "expiry": expiry}),
        )
    };
    let dave_node = {
        let node = alloy_primitives::hex::decode(&alpha_node).expect("node hex");
        let label = alloy_primitives::hex::decode(child_labelhash("dave")).expect("label hex");
        format!("{:#x}", alloy_primitives::keccak256([node, label].concat()))
    };
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            family_event(
                "children-alpha-grant",
                Some(&alpha),
                Some(alpha_resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                201,
                0,
                json!({"authority_kind": "registrar", "registrant": CHILD_OWNER,
                       "expiry": 1_900_000_000i64}),
            ),
            child_registry_event(
                "children-root-created",
                None,
                "RegistryCreated",
                203,
                0,
                CHILD_ROOT_REGISTRY,
                json!({"source_event": "RegistryCreated", "registry": CHILD_ROOT_REGISTRY}),
            ),
            child_registry_event(
                "children-alpha-subregistry",
                Some(&alpha),
                "SubregistryChanged",
                204,
                0,
                CHILD_ROOT_REGISTRY,
                json!({"source_event": "SubregistryUpdated",
                       "subregistry": CHILD_ALPHA_REGISTRY}),
            ),
            child_registry_event(
                "children-alpha-created",
                None,
                "RegistryCreated",
                204,
                1,
                CHILD_ALPHA_REGISTRY,
                json!({"source_event": "RegistryCreated", "registry": CHILD_ALPHA_REGISTRY}),
            ),
            registration("children-one", &one, 205, one_expiry),
            registration("children-two", &two, 206, two_expiry),
            family_event(
                "children-dave-zeroed",
                None,
                None,
                "AuthorityTransferred",
                "ens_v1_registry_l1",
                208,
                0,
                json!({"source_event": "Transfer", "node": dave_node,
                       "owner": "0x0000000000000000000000000000000000000000",
                       "owner_getter": "0x0000000000000000000000000000000000000000",
                       "emitter_role": "registry"}),
            ),
        ],
    )
    .await?;
    publish_test_families(database, 240).await
}

#[tokio::test]
async fn v2_subnames_from_families() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;
    // A real unnamed registry event reaches the arm fallback: there is no surface for
    // carol, so publication cannot compose a name summary for it. Large registry parents
    // can have many such children; the page must not rescan them once per child.
    let carol = bigname_storage::logical_name_id_for_name("ens", "carol.alpha.eth");
    let has_summary: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM bigname_phase.project_name_summary
         WHERE logical_name_id = $1)",
    )
    .bind(&carol)
    .fetch_one(&database.pool)
    .await?;
    assert!(!has_summary, "the fixture must exercise the missing-summary arm fallback");
    let pages =
        read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=1")
            .await?;
    let names: Vec<&Value> = pages
        .iter()
        .flat_map(|page| page["data"].as_array().into_iter().flatten())
        .map(|row| &row["name"])
        .collect();
    assert_eq!(
        names,
        [
            &json!("carol.alpha.eth"),
            &json!("one.alpha.eth"),
            &json!("two.alpha.eth")
        ],
        "{pages:#?}"
    );
    assert_eq!(pages[0]["total_count"], json!(3), "{pages:#?}");
    let counted = read_family_pages(
        &database,
        "/v1/names/alpha.eth/subnames?include=counts&page_size=5",
    )
    .await?;
    let counts: Vec<(&Value, &Value)> = counted[0]["data"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| (&row["name"], &row["subname_count"]))
        .collect();
    assert_eq!(
        counts,
        [
            (&json!("carol.alpha.eth"), &json!(0)),
            (&json!("one.alpha.eth"), &json!(1)),
            (&json!("two.alpha.eth"), &json!(0))
        ],
        "{counted:#?}"
    );
    for uri in [
        "/v1/names/alpha.eth/subnames?sort=expires_at&order=desc&page_size=2",
        "/v1/names/alpha.eth/subnames?sort=registered_at&page_size=2",
        "/v1/names/alpha.eth/subnames?include_expired=false&page_size=2",
        "/v1/names/alpha.eth/subnames?q=o&page_size=2",
        "/v1/names/one.alpha.eth/subnames?include=counts&page_size=2",
    ] {
        read_family_pages(&database, uri).await?;
    }
    // The name record's subname count reads the same per-parent count.
    let (status, alpha) =
        read_family_response(&database, "/v1/names/alpha.eth?include=counts").await?;
    assert_eq!(status, StatusCode::OK, "{alpha:#}");
    let (status, _) =
        read_family_response(&database, "/v1/names/missing.eth/subnames?page_size=1")
            .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);

    database.cleanup().await
}

/// A 2017 registry `NewOwner` whose owner word was unmasked (#361) serves the word's low 20
/// bytes as the child's display owner, as the fallback registry's typed read returns it
/// (docs/architecture.md), though it names no control owner.
#[tokio::test]
async fn v2_subnames_serve_an_unmasked_new_owner_word_as_its_low_20_bytes() -> Result<()> {
    const RAW_WORD: &str = "0x6630353636393265383962390000000000000000000000000000000000000f11";
    const LOW_20: &str = "0x0000000000000000000000000000000000000f11";
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;
    insert_family_label_preimage(&database.pool, b"frank").await?;
    let node = bigname_lookup::ens_namehash_hex("alpha.eth")?;
    let labelhash = child_labelhash("frank");
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
    // The adapter emits both kinds from the one log with the same body.
    let body = json!({"source_event": "NewOwner", "node": node, "child_node": child,
                      "labelhash": labelhash, "owner": LOW_20,
                      "owner_word_unmasked": true, "owner_word_raw": RAW_WORD});
    let unmasked = |identity: &str, kind: &str| {
        family_event(identity, None, None, kind, "ens_v1_registry_l1", 241, 0, body.clone())
    };
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            unmasked("children-frank-edge", "SubregistryChanged"),
            unmasked("children-frank-authority", "AuthorityTransferred"),
        ],
    )
    .await?;
    publish_test_families(&database, 241).await?;

    let pages = read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=10").await?;
    let owners: Vec<(&Value, &Value)> = pages
        .iter()
        .flat_map(|page| page["data"].as_array().into_iter().flatten())
        .map(|row| (&row["name"], &row["owner"]))
        .collect();
    assert!(
        owners.contains(&(&json!("frank.alpha.eth"), &json!(LOW_20))),
        "{pages:#?}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_registry_labels_from_families() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;
    let pages = read_family_pages(
        &database,
        &format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}/labels?page_size=1"),
    )
    .await?;
    let names: Vec<&Value> = pages
        .iter()
        .flat_map(|page| page["data"].as_array().into_iter().flatten())
        .map(|row| &row["name"])
        .collect();
    assert_eq!(
        names,
        [&json!("one.alpha.eth"), &json!("two.alpha.eth")],
        "{pages:#?}"
    );
    assert_eq!(pages[0]["total_count"], json!(2), "{pages:#?}");
    read_family_pages(
        &database,
        &format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}/labels?include=counts&page_size=5"),
    )
    .await?;
    let (status, registry) = read_family_response(
        &database,
        &format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}?include=counts"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{registry:#}");
    assert_eq!(registry["data"]["counts"]["labels"], json!(2), "{registry:#}");
    let (status, registry) = read_family_response(&database, &format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}"))
        .await?;
    assert_eq!(status, StatusCode::OK, "{registry:#}");
    read_family_pages(
        &database,
        &format!("/v1/registries/1/{CHILD_ROOT_REGISTRY}/labels?page_size=5"),
    )
    .await?;

    database.cleanup().await
}
#[tokio::test]
async fn v2_child_reads_refuse_an_unservable_family_marker() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;
    let parent = bigname_storage::logical_name_id_for_name("ens", "alpha.eth");
    let registry = CHILD_ALPHA_REGISTRY.to_ascii_lowercase();
    for (label, update) in [
        (
            "a rebuild in flight",
            "UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'",
        ),
        (
            "another build's marker",
            "UPDATE bigname_phase.project_family_marker
             SET state = 'live', input_content_hash = 'another-build'",
        ),
    ] {
        sqlx::query(update).execute(&database.pool).await?;
        let pool = &database.pool;
        let parent = &parent;
        let registry = &registry;
        async move {
            let unavailable = |read: &str, result: Result<(), anyhow::Error>| {
                let error = result.expect_err(&format!("{label}: {read} must refuse"));
                assert!(
                    bigname_storage::families::name::is_publication_unavailable(&error),
                    "{label}: {read}: {error:#}"
                );
            };
            unavailable(
                "subnames page",
                bigname_storage::load_children_current_page_filtered(
                    pool,
                    parent,
                    &bigname_storage::ChildrenCurrentPageFilter::default(),
                    None,
                    5,
                )
                .await
                .map(drop),
            );
            unavailable(
                "registry labels page",
                bigname_storage::load_registry_children_current_page(
                    pool,
                    parent,
                    registry,
                    Some(bigname_storage::RegistryLabelOwnerFilter::ExcludeOwner(CHILD_OWNER)),
                    None,
                    5,
                )
                .await
                .map(drop),
            );
            unavailable(
                "registry label count",
                bigname_storage::count_registry_labels_current(pool, FAMILY_CHAIN, registry)
                    .await
                    .map(drop),
            );
            unavailable(
                "child counts",
                bigname_storage::load_children_current_summaries(pool, std::slice::from_ref(parent))
                    .await
                    .map(drop),
            );
            anyhow::Ok(())
        }
        .await?;
    }
    database.cleanup().await
}
async fn v2_get_with_marker_flip_at(
    database: &TestDatabase,
    uri: &str,
    flip: &str,
    flip_at: usize,
) -> Result<(StatusCode, Value, usize)> {
    let reached = std::sync::Arc::new(tokio::sync::Notify::new());
    let resume = std::sync::Arc::new(tokio::sync::Notify::new());
    let request = bigname_storage::families::name::seams::with_pause_before_snapshot(
        std::sync::Arc::clone(&reached),
        std::sync::Arc::clone(&resume),
        v2_get_response(database, uri),
    );
    tokio::pin!(request);
    let mut paused = 0;
    let response = loop {
        tokio::select! {
            response = &mut request => break response?,
            () = reached.notified() => {
                paused += 1;
                if paused == flip_at {
                    sqlx::query(flip).execute(&database.pool).await?;
                }
                resume.notify_one();
            }
        }
    };
    let status = response.status();
    let body: Value = read_json(response).await?;
    Ok((status, body, paused))
}

// Every route that reads the child families answers the stale 409, never a server error or an
// empty list, when the marker is not servable: before the request (a rebuild in flight, where the
// fence refuses first), and when it stops being servable after the route's fence, at each of the
// route's composed and child reads in turn, which reaches every child read's error mapping.
#[tokio::test]
async fn v2_child_reads_answer_409_when_the_marker_is_not_servable() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;
    let uris = [
        "/v1/names/alpha.eth/subnames".to_owned(),
        "/v1/names/alpha.eth/subnames?include=counts&include_expired=false".to_owned(),
        format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}/labels?include=counts"),
        format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}?include=counts"),
        "/v1/names/alpha.eth?include=counts".to_owned(),
        format!("/v1/addresses/{CHILD_OWNER}/names?namespace=ens&include=counts"),
    ];
    let flips = [
        "UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'",
        "UPDATE bigname_phase.project_family_marker SET input_content_hash = 'another-build'",
    ];
    let servable: (String, String) = sqlx::query_as(
        "SELECT state, input_content_hash FROM bigname_phase.project_family_marker",
    )
    .fetch_one(&database.pool)
    .await?;
    let restore = || async {
        sqlx::query(
            "UPDATE bigname_phase.project_family_marker SET state = $1, input_content_hash = $2",
        )
        .bind(&servable.0)
        .bind(&servable.1)
        .execute(&database.pool)
        .await
    };
    let mut answers = Vec::new();
    for uri in &uris {
        let (status, body, reads) = v2_get_with_marker_flip_at(&database, uri, "SELECT 1", 0).await?;
        assert_eq!(status, StatusCode::OK, "{uri} with a servable marker: {body:#}");
        assert!(reads > 0, "{uri}: no composed or child read");
        for flip in flips {
            // Before the request.
            sqlx::query(flip).execute(&database.pool).await?;
            let (status, body, _) =
                v2_get_with_marker_flip_at(&database, uri, "SELECT 1", 0).await?;
            restore().await?;
            answers.push((uri.clone(), flip, 0, status, body["error"]["code"].clone()));
            // After the fence, at each read.
            for at in 1..=reads {
                let (status, body, _) = v2_get_with_marker_flip_at(&database, uri, flip, at).await?;
                restore().await?;
                answers.push((uri.clone(), flip, at, status, body["error"]["code"].clone()));
            }
        }
    }
    let expected: Vec<_> = answers
        .iter()
        .map(|(uri, flip, at, ..)| (uri.clone(), *flip, *at, StatusCode::CONFLICT, json!("stale")))
        .collect();
    assert_eq!(answers, expected);
    database.cleanup().await
}
