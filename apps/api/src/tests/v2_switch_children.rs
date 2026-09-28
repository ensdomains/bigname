// The child lists under the publication switch (TYR-36 step 7b slice 2b): the subnames page, the
// child counts and the registry labels come from the family child relation and the name summary
// family (`project_name_summary`) with the switch on, and answer the same body as the served
// `children_current` reads with it off, `meta.as_of` excepted. Project and the owned key
// families both build the fixture from the same normalized events.

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
    let mut event = switch_event(
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
/// dave's node is later transferred to the zero owner; dave has no surface, and the override is
/// keyed by name, so dave stays listed. one.alpha.eth holds an ENSv1 edge of its own (erin), so
/// its subname count is one. Published at 240.
async fn seed_switch_children_fixture(database: &TestDatabase) -> Result<()> {
    seed_switch_children_fixture_expiring(database, 1_900_000_000, 1_900_000_000).await
}

/// The fixture with one.alpha.eth's and two.alpha.eth's registrations expiring at `one_expiry`
/// and `two_expiry` seconds.
async fn seed_switch_children_fixture_expiring(
    database: &TestDatabase,
    one_expiry: i64,
    two_expiry: i64,
) -> Result<()> {
    seed_bounded_membership_blocks(database, 240).await?;
    let (alpha, alpha_resource) =
        seed_switch_name(database, "alpha.eth", 0x6a1_0000, "ens_v1").await?;
    let (one, _) = seed_switch_name(database, "one.alpha.eth", 0x6b1_0000, "ens_v2").await?;
    let (two, _) = seed_switch_name(database, "two.alpha.eth", 0x6c1_0000, "ens_v2").await?;
    let alpha_node = alpha.strip_prefix("ens:").expect("ens id").to_owned();
    let one_node = one.strip_prefix("ens:").expect("ens id").to_owned();
    for label in ["carol", "dave", "erin", "one", "two"] {
        sqlx::query(
            "INSERT INTO bigname_phase.label_preimages (labelhash, raw_label, decoded_label,
                 normalizer_version, normalized_under_version, normalization_error,
                 source_kind, source_priority)
             VALUES ($1, convert_to($2, 'UTF8'), $2, 'ensip15', TRUE, NULL, 'fixture', 0)
             ON CONFLICT DO NOTHING",
        )
        .bind(child_labelhash(label))
        .bind(label)
        .execute(&database.pool)
        .await?;
    }
    // The ENSv2 subregistry's contract instance, which the registrations name.
    let alpha_registry = Uuid::from_u128(0x6b0_0001);
    sqlx::query(
        "INSERT INTO bigname_phase.contract_instances (contract_instance_id, chain_id,
             contract_kind)
         VALUES ($1, $2, 'contract') ON CONFLICT DO NOTHING",
    )
    .bind(alpha_registry)
    .bind(SWITCH_CHAIN)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.contract_instance_addresses (contract_instance_id, chain_id,
             address, active_from_block_number)
         VALUES ($1, $2, $3, 200)",
    )
    .bind(alpha_registry)
    .bind(SWITCH_CHAIN)
    .bind(CHILD_ALPHA_REGISTRY)
    .execute(&database.pool)
    .await?;
    let edge = |identity: &str, node: &str, label: &str, block: i64, log: i64| {
        let child = format!(
            "{:#x}",
            alloy_primitives::keccak256(
                [
                    alloy_primitives::hex::decode(node).expect("node hex"),
                    alloy_primitives::hex::decode(child_labelhash(label)).expect("labelhash hex"),
                ]
                .concat()
            )
        );
        switch_event(
            identity,
            None,
            None,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            block,
            log,
            json!({"source_event": "NewOwner", "node": node, "child_node": child,
                   "labelhash": child_labelhash(label), "owner": CHILD_OWNER}),
        )
    };
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
            switch_event(
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
            edge("children-carol", &alpha_node, "carol", 202, 0),
            edge("children-dave", &alpha_node, "dave", 202, 1),
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
            edge("children-erin", &one_node, "erin", 207, 0),
            switch_event(
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
    publish_project_and_families(database, 240).await
}

#[tokio::test]
async fn v2_subnames_are_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_children_fixture(&database).await?;
    let pages =
        assert_switch_differential_pages(&database, "/v1/names/alpha.eth/subnames?page_size=1")
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
            &json!("dave.alpha.eth"),
            &json!("one.alpha.eth"),
            &json!("two.alpha.eth")
        ],
        "{pages:#?}"
    );
    assert_eq!(pages[0]["total_count"], json!(4), "{pages:#?}");
    let counted = assert_switch_differential_pages(
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
            (&json!("dave.alpha.eth"), &json!(0)),
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
        assert_switch_differential_pages(&database, uri).await?;
    }
    // The name record's subname count reads the same per-parent count.
    let (status, alpha) =
        assert_switch_differential(&database, "/v1/names/alpha.eth?include=counts").await?;
    assert_eq!(status, StatusCode::OK, "{alpha:#}");
    let (status, _) =
        assert_switch_differential(&database, "/v1/names/missing.eth/subnames?page_size=1")
            .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_switch_on_ignores_served_tables(
        &database,
        "/v1/names/alpha.eth/subnames?include=counts&page_size=5",
        &["children_current", "name_current"],
    )
    .await?;
    database.cleanup().await
}

#[tokio::test]
async fn v2_registry_labels_are_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_children_fixture(&database).await?;
    let pages = assert_switch_differential_pages(
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
    assert_switch_differential_pages(
        &database,
        &format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}/labels?include=counts&page_size=5"),
    )
    .await?;
    let (status, registry) = assert_switch_differential(
        &database,
        &format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}?include=counts"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{registry:#}");
    assert_eq!(registry["data"]["counts"]["labels"], json!(2), "{registry:#}");
    assert_switch_differential(&database, &format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}"))
        .await?;
    assert_switch_differential_pages(
        &database,
        &format!("/v1/registries/1/{CHILD_ROOT_REGISTRY}/labels?page_size=5"),
    )
    .await?;
    assert_switch_on_ignores_served_tables(
        &database,
        &format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}/labels?page_size=5"),
        &["children_current"],
    )
    .await?;
    database.cleanup().await
}

// A family rebuild that begins after a route's fence passed, or a marker another build wrote,
// leaves no servable publication: the child reads under the switch fail with
// `FamilyPublicationUnavailable`, the error the API answers with the stale 409, never an empty
// list read against a half-built family.
#[tokio::test]
async fn v2_child_reads_refuse_an_unservable_family_marker() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_children_fixture(&database).await?;
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
        bigname_storage::publication_source::with_serve_from_families(true, async move {
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
                    pool, parent, registry, None, 5,
                )
                .await
                .map(drop),
            );
            unavailable(
                "registry label count",
                bigname_storage::count_registry_children_current(pool, parent, registry)
                    .await
                    .map(drop),
            );
            unavailable(
                "child counts",
                bigname_storage::load_children_current_summaries(pool, &[parent.clone()])
                    .await
                    .map(drop),
            );
            anyhow::Ok(())
        })
        .await?;
    }
    database.cleanup().await
}
