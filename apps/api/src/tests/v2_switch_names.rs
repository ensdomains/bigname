// The names group under the publication switch (TYR-36 step 7b slice 2): every route whose name
// rows now come from the composed name reader (`bigname_storage::families::name`) answers the
// same body with the switch off and on, `meta.as_of` excepted, over a fixture that Project and
// the owned key families both build from the same normalized events.

const SWITCH_ALICE: &str = "0x00000000000000000000000000000000000a11ce";
const SWITCH_RESOLVER: &str = "0x0000000000000000000000000000000000000abc";

/// alpha.eth granted at 201, pointed at a resolver at 202 and renewed at 203; beta.eth granted
/// at 204 and pointed at the same resolver at 205; both published at 240.
async fn seed_switch_names_fixture(database: &TestDatabase) -> Result<()> {
    seed_switch_names_events(database).await?;
    publish_project_and_families(database, 240).await
}

/// The events of `seed_switch_names_fixture`, unpublished. Returns alpha.eth's name id, node and
/// resource.
async fn seed_switch_names_events(database: &TestDatabase) -> Result<(String, String, Uuid)> {
    seed_bounded_membership_blocks(database, 240).await?;
    let (alpha, alpha_resource) = seed_switch_name(database, "alpha.eth", 0x5a1_0000, "ens_v1").await?;
    let (beta, beta_resource) = seed_switch_name(database, "beta.eth", 0x5b1_0000, "ens_v1").await?;
    let alpha_node = alpha.strip_prefix("ens:").expect("ens id").to_owned();
    let grant = |expiry: i64| {
        json!({"authority_kind": "registrar", "registrant": SWITCH_ALICE, "expiry": expiry})
    };
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            switch_event(
                "switch-alpha-grant",
                Some(&alpha),
                Some(alpha_resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                201,
                0,
                grant(1_900_000_000),
            ),
            switch_event(
                "switch-alpha-resolver",
                Some(&alpha),
                Some(alpha_resource),
                "ResolverChanged",
                "ens_v1_registry_l1",
                202,
                0,
                json!({"node": alpha_node, "resolver": SWITCH_RESOLVER}),
            ),
            switch_event(
                "switch-alpha-renewal",
                Some(&alpha),
                Some(alpha_resource),
                "RegistrationRenewed",
                "ens_v1_registrar_l1",
                203,
                0,
                json!({"expiry": 1_950_000_000i64}),
            ),
            switch_event(
                "switch-beta-resolver",
                Some(&beta),
                Some(beta_resource),
                "ResolverChanged",
                "ens_v1_registry_l1",
                205,
                0,
                json!({"node": beta.strip_prefix("ens:").expect("ens id"), "resolver": SWITCH_RESOLVER}),
            ),
            switch_event(
                "switch-beta-grant",
                Some(&beta),
                Some(beta_resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                204,
                0,
                grant(1_800_000_000),
            ),
        ],
    )
    .await?;
    Ok((alpha, alpha_node, alpha_resource))
}

#[tokio::test]
async fn v2_name_detail_is_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    let (status, alpha) = assert_switch_differential(&database, "/v1/names/alpha.eth").await?;
    assert_eq!(status, StatusCode::OK, "{alpha:#}");
    assert_eq!(alpha["data"]["registrant"], json!(SWITCH_ALICE), "{alpha:#}");
    assert_eq!(
        alpha["data"]["resolver"]["address"],
        json!(SWITCH_RESOLVER),
        "{alpha:#}"
    );
    for uri in [
        "/v1/names/beta.eth",
        "/v1/names/alpha.eth?include=counts",
        "/v1/names/missing.eth",
        // The diagnostics name and authority reads take the same row (ruling J11).
        "/v1/diagnostics/names/alpha.eth/coverage",
        "/v1/diagnostics/names/alpha.eth/binding",
        "/v1/diagnostics/names/alpha.eth/authority",
    ] {
        assert_switch_differential(&database, uri).await?;
    }
    for uri in [
        "/v1/names/alpha.eth",
        "/v1/diagnostics/names/alpha.eth/authority",
    ] {
        assert_switch_on_ignores_served_tables(&database, uri, &["name_current"]).await?;
    }
    database.cleanup().await
}

// Ruling J5: a composed row describes the family marker's publication only, so an `at` below
// it answers 409 with the switch on (storage's `family_name_for_snapshot`), with the wording a
// served row that cannot prove the position gets today. Project restamps every served row with
// the publication it writes, so the switch-off side refuses the same `at` for the same reason
// (a row newer than the selected position) and the bodies are equal.
#[tokio::test]
async fn v2_name_detail_refuses_an_at_below_the_publication_both_ways() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    for block in [230, 239] {
        let at = crate::v2::format_timestamp(OffsetDateTime::from_unix_timestamp(
            1_700_000_000 + block,
        )?);
        let (status, body) =
            assert_switch_differential(&database, &format!("/v1/names/alpha.eth?at={at}")).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{body:#}");
        assert_eq!(
            body["error"],
            json!({"code": "stale", "details": {},
                   "message": "requested snapshot is not available for name"}),
            "{body:#}"
        );
    }
    let at = crate::v2::format_timestamp(OffsetDateTime::from_unix_timestamp(1_700_000_240)?);
    let (status, _) =
        assert_switch_differential(&database, &format!("/v1/names/alpha.eth?at={at}")).await?;
    assert_eq!(status, StatusCode::OK);
    database.cleanup().await
}

fn switch_timestamp(seconds: i64) -> Result<String> {
    Ok(crate::v2::format_timestamp(
        OffsetDateTime::from_unix_timestamp(seconds)?,
    ))
}

#[tokio::test]
async fn v2_expiring_names_are_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    let after = switch_timestamp(1_700_000_000)?;
    let before = switch_timestamp(1_960_000_000)?;
    for order in ["asc", "desc"] {
        let pages = assert_switch_differential_pages(
            &database,
            &format!(
                "/v1/names?namespace=ens&expires_after={after}&expires_before={before}\
                 &order={order}&page_size=1"
            ),
        )
        .await?;
        let names: Vec<&Value> = pages
            .iter()
            .flat_map(|page| page["data"].as_array().into_iter().flatten())
            .map(|row| &row["name"])
            .collect();
        let expected = if order == "asc" {
            vec![json!("beta.eth"), json!("alpha.eth")]
        } else {
            vec![json!("alpha.eth"), json!("beta.eth")]
        };
        assert_eq!(names, expected.iter().collect::<Vec<_>>(), "{pages:#?}");
    }
    // A window that holds only the renewed expiry, and one that holds only the replaced one.
    for (after, before, count) in [
        (1_940_000_000, 1_960_000_000, 1),
        (1_890_000_000, 1_910_000_000, 0),
    ] {
        let pages = assert_switch_differential_pages(
            &database,
            &format!(
                "/v1/names?namespace=ens&expires_after={}&expires_before={}&page_size=5",
                switch_timestamp(after)?,
                switch_timestamp(before)?
            ),
        )
        .await?;
        assert_eq!(pages[0]["data"].as_array().map(Vec::len), Some(count));
    }
    assert_switch_on_ignores_served_tables(
        &database,
        &format!("/v1/names?namespace=ens&expires_after={after}&page_size=5"),
        &["name_current"],
    )
    .await?;
    database.cleanup().await
}

#[tokio::test]
async fn v2_search_is_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    for query in [
        "q=a&match=prefix&page_size=1",
        "q=eth&match=contains&page_size=1",
        "q=eth&match=contains&namespace=ens&page_size=5",
        "q=zzz&match=prefix&page_size=5",
    ] {
        assert_switch_differential_pages(&database, &format!("/v1/search?{query}")).await?;
    }
    let pages =
        assert_switch_differential_pages(&database, "/v1/search?q=eth&match=contains&page_size=1")
            .await?;
    assert_eq!(pages.len(), 2, "{pages:#?}");
    assert_switch_on_ignores_served_tables(
        &database,
        "/v1/search?q=eth&match=contains&page_size=5",
        &["name_current"],
    )
    .await?;
    database.cleanup().await
}


#[tokio::test]
async fn v2_resolver_bound_names_are_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // The declared resolver of the routes fixture, which Project and the families both describe.
    seed_switch_routes_fixture(&database).await?;
    let uri = format!("/v1/resolvers/1/{SWITCH_RESOLVER}?page_size=1");
    let pages = assert_switch_differential_pages_in(&database, &uri, "/data/bound_names").await?;
    let names: Vec<&Value> = pages
        .iter()
        .flat_map(|page| page["data"].as_array().into_iter().flatten())
        .map(|name| &name["name"])
        .collect();
    assert_eq!(names, [&json!("alpha.eth"), &json!("beta.eth")], "{pages:#?}");
    for uri in [
        format!("/v1/resolvers/1/{SWITCH_RESOLVER}"),
        format!("/v1/resolvers/1/{SWITCH_RESOLVER}?page_size=5"),
    ] {
        assert_switch_differential_pages_in(&database, &uri, "/data/bound_names").await?;
    }
    let (status, _) = assert_switch_differential(
        &database,
        "/v1/resolvers/1/0x0000000000000000000000000000000000000def",
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_switch_on_ignores_served_tables(
        &database,
        &format!("/v1/resolvers/1/{SWITCH_RESOLVER}"),
        &["name_current"],
    )
    .await?;
    database.cleanup().await
}

// The composed listings walk candidates in batches of at least 200, which a fixture of two names
// never fills; the test-only seam shrinks the batch so every page below straddles one (the
// storage-level comparison with cursors is apps/phase-runner/tests/families_shadow_name_batches.rs).
#[tokio::test]
async fn v2_name_listings_are_the_same_across_candidate_batches() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // The declared resolver of the routes fixture, which Project and the families both describe.
    seed_switch_routes_fixture(&database).await?;
    let after = switch_timestamp(1_700_000_000)?;
    let before = switch_timestamp(1_960_000_000)?;
    for batch in [1, 2] {
        for uri in [
            "/v1/search?q=eth&match=contains&page_size=1".to_owned(),
            "/v1/search?q=a&match=prefix&page_size=1".to_owned(),
            format!(
                "/v1/names?namespace=ens&expires_after={after}&expires_before={before}\
                 &order=asc&page_size=1"
            ),
            format!(
                "/v1/names?namespace=ens&expires_after={after}&expires_before={before}\
                 &order=desc&page_size=1"
            ),
        ] {
            let pages = bigname_storage::families::name::seams::with_batch_size(
                batch,
                assert_switch_differential_pages(&database, &uri),
            )
            .await?;
            assert!(
                pages.iter().any(|page| page["data"].as_array().is_some_and(|rows| !rows.is_empty())),
                "{uri}: {pages:#?}"
            );
        }
        let uri = format!("/v1/resolvers/1/{SWITCH_RESOLVER}?page_size=1");
        let pages = bigname_storage::families::name::seams::with_batch_size(
            batch,
            assert_switch_differential_pages_in(&database, &uri, "/data/bound_names"),
        )
        .await?;
        assert_eq!(pages.len(), 2, "{pages:#?}");
    }
    database.cleanup().await
}

const SWITCH_BOB: &str = "0x0000000000000000000000000000000000000b0b";
const SWITCH_REGISTRY: &str = "0x00000000000000000000000000000000000000f1";

/// `seed_switch_names_fixture` with the rows the routes that read composed name rows beside
/// their own served pages need: a child `sub.alpha.eth` under alpha.eth (an ENSv1 NewOwner edge
/// at 206), a resolver role granted to bob on alpha.eth's resource at 207, an address record of
/// alpha.eth resolving to alice at 208, and the resolver's declaring manifest, from which Project
/// and the families both describe the resolver.
async fn seed_switch_routes_fixture(database: &TestDatabase) -> Result<()> {
    seed_switch_routes_events(database).await?;
    publish_project_and_families(database, 240).await
}

/// The events of `seed_switch_routes_fixture`, unpublished.
async fn seed_switch_routes_events(database: &TestDatabase) -> Result<()> {
    let (_, alpha_node, alpha_resource) = seed_switch_names_events(database).await?;
    let (sub, _) = seed_switch_name(database, "sub.alpha.eth", 0x5c1_0000, "ens_v1").await?;
    let sub_node = sub.strip_prefix("ens:").expect("ens id").to_owned();
    let mut edge = switch_event(
        "switch-sub-edge",
        None,
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        206,
        0,
        json!({"source_event": "NewOwner", "node": alpha_node, "child_node": sub_node,
               "labelhash": labelhash_for_display_name("sub.alpha.eth"),
               "owner": SWITCH_ALICE}),
    );
    edge.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    let mut role = switch_event(
        "switch-alpha-role",
        None,
        Some(alpha_resource),
        "PermissionChanged",
        "ens_v2_resolver_l1",
        207,
        0,
        json!({
            "subject": SWITCH_BOB,
            "scope": {"kind": "resolver", "chain_id": SWITCH_CHAIN,
                      "resolver_address": SWITCH_RESOLVER},
            "effective_powers": ["set_text"],
            "grant_source": {"kind": "raw_log", "source_event": "EACRolesChanged",
                "upstream_resource": alpha_node, "root_resource": false,
                "changed_powers": ["set_text"]},
            "revocation_source": null,
            "inheritance_path": [], "transfer_behavior": {},
            "source_event": "EACRolesChanged", "upstream_resource": alpha_node,
            "resource": alpha_node, "root_resource": false,
            "storage_model": "resolver_record_id", "resolver": SWITCH_RESOLVER,
            "resolver_record_id": "0", "record_key": "permission",
        }),
    );
    role.derivation_kind = "ens_v2_permissions".to_owned();
    role.raw_fact_ref["emitting_address"] = json!(SWITCH_RESOLVER);
    // The resolver is declared, so Project indexes its address records.
    let payload = json!({"deployment_epoch": "fixture", "contracts": [{
        "role": "resolver", "address": SWITCH_RESOLVER, "proxy_kind": "none",
        "start_block": 0, "read_features": []
    }]});
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO bigname_phase.manifest_versions (manifest_version, namespace,
             source_family, chain_id, deployment_label, rollout_status, normalizer_version,
             file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v1_resolver_l1', $1, 'fixture', 'active', 'fixture',
                 'fixture/switch-resolver.toml', $2)
         RETURNING manifest_id",
    )
    .bind(SWITCH_CHAIN)
    .bind(&payload)
    .fetch_one(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.normalized_events (event_identity, namespace, event_kind,
             source_family, manifest_version, source_manifest_id, chain_id, derivation_kind,
             canonicality_state, after_state)
         VALUES ('switch-manifest', 'ens', 'SourceManifestUpdated', 'ens_v1_resolver_l1', 1,
                 $1, $2, 'manifest_sync', 'canonical', $3)",
    )
    .bind(manifest_id)
    .bind(SWITCH_CHAIN)
    .bind(json!({"rollout_status": "active", "normalizer_version": "fixture",
                 "manifest_payload": payload}))
    .execute(&database.pool)
    .await?;
    let mut record = switch_event(
        "switch-alpha-addr",
        None,
        None,
        "RecordChanged",
        "ens_v1_resolver_l1",
        208,
        0,
        json!({"source_event": "AddressChanged", "node": alpha_node,
               "resolver": SWITCH_RESOLVER, "record_key": "addr:60", "record_family": "addr",
               "selector_key": "60", "value": SWITCH_ALICE}),
    );
    record.raw_fact_ref["emitting_address"] = json!(SWITCH_RESOLVER);
    record.source_manifest_id = Some(manifest_id);
    record.manifest_version = 1;
    record.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    // An ENSv2 subregistry of alpha.eth at `SWITCH_REGISTRY` registering `two.alpha.eth` at
    // 209, so the registry's labels list a child.
    let registry_instance = Uuid::from_u128(0x5d1_0000);
    sqlx::query(
        "INSERT INTO bigname_phase.contract_instances (contract_instance_id, chain_id,
             contract_kind)
         VALUES ($1, $2, 'contract')",
    )
    .bind(registry_instance)
    .bind(SWITCH_CHAIN)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "INSERT INTO bigname_phase.contract_instance_addresses (contract_instance_id, chain_id,
             address, active_from_block_number)
         VALUES ($1, $2, $3, 200)",
    )
    .bind(registry_instance)
    .bind(SWITCH_CHAIN)
    .bind(SWITCH_REGISTRY)
    .execute(&database.pool)
    .await?;
    let (two, _) = seed_switch_name(database, "two.alpha.eth", 0x5e1_0000, "ens_v2").await?;
    let labels: Vec<String> = ["two", "alpha", "eth"]
        .iter()
        .map(|label| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes())))
        .collect();
    sqlx::query("UPDATE bigname_phase.name_surfaces SET labelhashes = $2 WHERE logical_name_id = $1")
        .bind(&two)
        .bind(&labels)
        .execute(&database.pool)
        .await?;
    let alpha_id = format!("ens:{alpha_node}");
    let mut subregistry = switch_event(
        "switch-alpha-subregistry",
        Some(&alpha_id),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        209,
        0,
        json!({"subregistry": SWITCH_REGISTRY}),
    );
    subregistry.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
    subregistry.raw_fact_ref["emitting_address"] = json!("0x00000000000000000000000000000000000000e3");
    let mut child = switch_event(
        "switch-two-granted",
        Some(&two),
        None,
        "RegistrationGranted",
        "ens_v2_registry_l1",
        209,
        1,
        json!({"registry_contract_instance_id": registry_instance.to_string(),
               "status": "registered", "registrant": SWITCH_BOB,
               "expiry": 1_990_000_000i64, "authority_kind": "registrar"}),
    );
    child.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
    child.raw_fact_ref["emitting_address"] = json!(SWITCH_REGISTRY);
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[edge, role, record, subregistry, child],
    )
    .await?;
    Ok(())
}

// The routes whose own pages stay on the served tables this slice but whose name rows come from
// the three switched loaders (`load_name_current`, `load_name_current_by_logical_name_ids`,
// `load_current_names_by_resource_ids`): each answers the same body with the switch off and on,
// `meta.as_of` excepted, and each lists at least one row, so the name rows are really read.
#[tokio::test]
async fn v2_routes_with_composed_name_rows_are_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_routes_fixture(&database).await?;
    for (uri, rows) in [
        // Parent row, then child rows (sub.alpha.eth by an ENSv1 edge, two.alpha.eth by the
        // ENSv2 subregistry).
        ("/v1/names/alpha.eth/subnames".to_owned(), "/data"),
        ("/v1/names/alpha.eth/history".to_owned(), "/data"),
        ("/v1/permissions?name=alpha.eth".to_owned(), "/data"),
        (format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens"), "/data"),
        (
            format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens&relation=resolves_to&coin_type=60"),
            "/data",
        ),
        (format!("/v1/registries/1/{SWITCH_REGISTRY}/labels"), "/data"),
        ("/v1/events?name=alpha.eth".to_owned(), "/data"),
        (format!("/v1/events?address={SWITCH_ALICE}"), "/data"),
        ("/v1/diagnostics/events?name=alpha.eth".to_owned(), "/data"),
        (format!("/v1/addresses/{SWITCH_ALICE}/history"), "/data"),
    ] {
        let (status, body) = assert_switch_differential(&database, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
        assert!(
            body.pointer(rows)
                .and_then(Value::as_array)
                .is_some_and(|rows| !rows.is_empty()),
            "{uri}: no rows in {body:#}"
        );
    }
    let (_, subnames) = assert_switch_differential(&database, "/v1/names/alpha.eth/subnames").await?;
    assert_eq!(
        subnames["data"].as_array().map(Vec::len),
        Some(2),
        "{subnames:#}"
    );
    for uri in [
        "/v1/names/alpha.eth/subnames".to_owned(),
        format!("/v1/registries/1/{SWITCH_REGISTRY}/labels"),
        format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens"),
    ] {
        assert_switch_on_ignores_served_tables(&database, &uri, &["name_current"]).await?;
    }
    database.cleanup().await
}

// A family rebuild in flight (the marker `bootstrap_pending`) leaves the families half built, so
// no composed row is servable: every route whose name rows are composed answers a 409 with the
// switch on, with its fence's wording when the fence refuses first (the collection routes say
// the collection publication is not available; search, with no namespace left to serve, answers
// a conflict) and with the name wording when the composed read refuses. An unknown name is stale
// too, not not found.
#[tokio::test]
async fn v2_composed_name_reads_answer_409_while_the_families_rebuild() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    seed_switch_resolver_current(&database).await?;
    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'")
        .execute(&database.pool)
        .await?;
    let mut messages = Vec::new();
    let bound_names = format!("/v1/resolvers/1/{SWITCH_RESOLVER}");
    for uri in [
        "/v1/names/alpha.eth",
        "/v1/names/alpha.eth/history",
        "/v1/names/alpha.eth/subnames",
        "/v1/permissions?name=alpha.eth",
        "/v1/names/nobody.eth",
        SWITCH_EXPIRING,
        SWITCH_SEARCH,
        bound_names.as_str(),
    ] {
        let response = bigname_storage::publication_source::with_serve_from_families(
            true,
            v2_get_response(&database, uri),
        )
        .await?;
        let status = response.status();
        let body: Value = read_json(response).await?;
        // Search's own fence answers a deployment with no servable namespace as a conflict.
        let code = if uri == SWITCH_SEARCH { "conflict" } else { "stale" };
        assert_eq!(
            (status, &body["error"]["code"]),
            (StatusCode::CONFLICT, &json!(code)),
            "{uri}: {body:#}"
        );
        messages.push((uri, body["error"]["message"].clone()));
    }
    assert_eq!(
        messages[0].1,
        json!("requested snapshot is not available for name"),
        "{messages:#?}"
    );
    database.cleanup().await
}

const SWITCH_EXPIRING: &str = "/v1/names?namespace=ens&expires_after=2020-01-01T00:00:00Z";
const SWITCH_SEARCH: &str = "/v1/search?q=eth&match=contains";

/// Runs `request` with the switch on, pausing each composed read before it opens its snapshot
/// (after the route's fence) to run `flip` on the family marker, then restores the marker: a
/// marker that stops being servable between a route's fence and its composed read.
async fn v2_get_with_marker_flip_after_fence(
    database: &TestDatabase,
    uri: &str,
    flip: &str,
) -> Result<(StatusCode, Value)> {
    let restore: (String, String) = sqlx::query_as(
        "SELECT state, input_content_hash FROM bigname_phase.project_family_marker LIMIT 1",
    )
    .fetch_one(&database.pool)
    .await?;
    let reached = std::sync::Arc::new(tokio::sync::Notify::new());
    let resume = std::sync::Arc::new(tokio::sync::Notify::new());
    let request = bigname_storage::families::name::seams::with_pause_before_snapshot(
        std::sync::Arc::clone(&reached),
        std::sync::Arc::clone(&resume),
        bigname_storage::publication_source::with_serve_from_families(
            true,
            v2_get_response(database, uri),
        ),
    );
    tokio::pin!(request);
    let mut paused = 0;
    let response = loop {
        tokio::select! {
            response = &mut request => break response?,
            () = reached.notified() => {
                paused += 1;
                sqlx::query(flip).execute(&database.pool).await?;
                resume.notify_one();
            }
        }
    };
    anyhow::ensure!(paused > 0, "{uri}: no composed read ran after the fence");
    let status = response.status();
    let body: Value = read_json(response).await?;
    sqlx::query(
        "UPDATE bigname_phase.project_family_marker SET state = $1, input_content_hash = $2",
    )
    .bind(&restore.0)
    .bind(&restore.1)
    .execute(&database.pool)
    .await?;
    Ok((status, body))
}

// The composed listings answer the stale 409 when the marker stops being servable after their
// fence passed and before their composed read (a window the fence cannot close): a rebuild
// starting, or a marker written by another interpreter build. They answer it as every other
// composed read does, not with a server error.
#[tokio::test]
async fn v2_composed_listings_answer_409_when_the_marker_changes_after_their_fence() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    seed_switch_resolver_current(&database).await?;
    let bound_names = format!("/v1/resolvers/1/{SWITCH_RESOLVER}");
    let mut answers = Vec::new();
    for uri in [SWITCH_EXPIRING, SWITCH_SEARCH, bound_names.as_str()] {
        let (status, body) = with_serve_on(&database, uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri} before any flip: {body:#}");
        for flip in [
            "UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'",
            "UPDATE bigname_phase.project_family_marker SET input_content_hash = 'another-build'",
        ] {
            let (status, body) = v2_get_with_marker_flip_after_fence(&database, uri, flip).await?;
            answers.push((uri.to_owned(), flip, status, body["error"]["code"].clone()));
        }
    }
    let expected: Vec<_> = answers
        .iter()
        .map(|(uri, flip, ..)| (uri.clone(), *flip, StatusCode::CONFLICT, json!("stale")))
        .collect();
    assert_eq!(answers, expected);
    database.cleanup().await
}

async fn with_serve_on(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let response = bigname_storage::publication_source::with_serve_from_families(
        true,
        v2_get_response(database, uri),
    )
    .await?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

// A NameWrapper expiry keeps the full u64 range (families/wrapper.rs), so the expiring walk
// meets expiries past the largest bigint; the listing still answers, the same with the switch
// off and on.
#[tokio::test]
async fn v2_expiring_names_walk_a_wrapper_expiry_past_bigint() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_events(&database).await?;
    let (gamma, gamma_resource) =
        seed_switch_name(&database, "gamma.eth", 0x5c1_0000, "ens_v1").await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[switch_event(
            "switch-gamma-wrapper-expiry",
            Some(&gamma),
            Some(gamma_resource),
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            206,
            0,
            json!({"expiry": u64::MAX}),
        )],
    )
    .await?;
    publish_project_and_families(&database, 240).await?;
    let stored: Option<String> = sqlx::query_scalar(
        "SELECT expiry_seconds::text FROM bigname_phase.project_wrapper_state
         WHERE resource_id = $1",
    )
    .bind(gamma_resource)
    .fetch_optional(&database.pool)
    .await?;
    assert_eq!(stored.as_deref(), Some("18446744073709551615"));
    for uri in [
        "/v1/names?namespace=ens&expires_after=2020-01-01T00:00:00Z&page_size=1".to_owned(),
        "/v1/names?namespace=ens&expires_after=2020-01-01T00:00:00Z&order=desc&page_size=1"
            .to_owned(),
    ] {
        assert_switch_differential_pages(&database, &uri).await?;
    }
    database.cleanup().await
}

// The primary-name claim gate reads the claimed name's row. alice claims alpha.eth, whose
// authority arm is ENSv1, and the execution manifest admits only ENSv2, so the verified answer is
// the gate's in-band refusal (no provider call): the same with the switch off and on, and the
// stale 409 when a rebuild starts after the route's fence or is already in flight.
#[tokio::test]
async fn v2_primary_name_gate_is_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    // The declared Universal Resolver at the publication, so the gate reads the admitted arms.
    let (hash, timestamp): (String, String) = sqlx::query_as(
        "SELECT block_hash,
                to_char(block_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
         FROM bigname_phase.chain_lineage WHERE chain_id = $1 AND block_number = 240",
    )
    .bind(SWITCH_CHAIN)
    .fetch_one(&database.pool)
    .await?;
    seed_schema_v2_ens_primary_name_authority(&database.pool, 240, &hash, &timestamp).await?;
    sqlx::query(
        "UPDATE bigname_phase.manifest_versions
         SET manifest_payload = manifest_payload
             || '{\"verified_authority_arms\": [\"ens_v2\"]}'::jsonb
         WHERE source_family = 'ens_execution'",
    )
    .execute(&database.pool)
    .await?;
    seed_phase_primary_name_snapshot(
        &database,
        SWITCH_ALICE,
        "ens",
        "60",
        bigname_storage::PrimaryNameClaimStatus::Success,
        Some("alpha.eth"),
        true,
    )
    .await?;
    let uri = format!("/v1/addresses/{SWITCH_ALICE}/primary-name?source=verified");
    let (status, body) = assert_switch_differential(&database, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    let verified = body["data"]["answers"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|answer| answer["source"] == json!("verified"))
        .cloned()
        .unwrap_or(Value::Null);
    assert_eq!(
        (&verified["status"], &verified["unsupported_reason"]),
        (&json!("unsupported"), &json!("exact_name_authority_not_verifiable")),
        "the gate refused in band: {body:#}"
    );
    // A rebuild starting after the route's fence reaches the gate's composed read (the pause
    // fires there), which answers the stale 409; the route words every stale answer for its
    // resource.
    let (status, body) = v2_get_with_marker_flip_after_fence(
        &database,
        &uri,
        "UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'",
    )
    .await?;
    assert_eq!(
        (status, &body["error"]["code"], &body["error"]["message"]),
        (
            StatusCode::CONFLICT,
            &json!("stale"),
            &json!("requested snapshot is not available for resource")
        ),
        "{body:#}"
    );
    // A rebuild already in flight is refused by the route's fence first.
    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'")
        .execute(&database.pool)
        .await?;
    let (status, body) = with_serve_on(&database, &uri).await?;
    assert_eq!(
        (status, &body["error"]["code"]),
        (StatusCode::CONFLICT, &json!("stale")),
        "{body:#}"
    );
    database.cleanup().await
}

// An exact name with no composed row may be one a rebuild has yet to reach: when a rebuild
// starts after the collection fence, subnames and the first history page of an unknown name
// answer the stale 409, not 404.
#[tokio::test]
async fn v2_unknown_parent_reads_answer_409_when_a_rebuild_starts_after_the_fence() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    let mut answers = Vec::new();
    for uri in ["/v1/names/nobody.eth/subnames", "/v1/names/nobody.eth/history"] {
        let (status, _) = with_serve_on(&database, uri).await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri} with a live marker");
        let (status, body) = v2_get_with_marker_flip_after_fence(
            &database,
            uri,
            "UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'",
        )
        .await?;
        answers.push((uri, status, body["error"]["code"].clone()));
    }
    assert_eq!(
        answers,
        [
            ("/v1/names/nobody.eth/subnames", StatusCode::CONFLICT, json!("stale")),
            ("/v1/names/nobody.eth/history", StatusCode::CONFLICT, json!("stale")),
        ]
    );
    database.cleanup().await
}
