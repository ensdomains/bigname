// The permissions and resolver collections group under the publication switch (TYR-36 step 7b
// slice 4): `/v1/permissions`, the resolver overview and its `/aliases`, `/links` and `/roles`
// answer the same body with the switch off and on, `meta.as_of` excepted, over a fixture that
// Project and the owned key families both build from the same normalized events; with the switch
// on they read no served permission or resolver row.

/// An ENSv2 permissioned resolver, a proxy upgraded to a declared implementation, so its role
/// holders, aliases and record links are all supported.
const SWITCH_V2_RESOLVER: &str = "0x0000000000000000000000000000000000000e2e";
const SWITCH_V2_IMPLEMENTATION: &str = "0x0000000000000000000000000000000000000e21";
const SWITCH_CAROL: &str = "0x00000000000000000000000000000000000ca201";
/// A granted resource no name is bound to.
const SWITCH_NAMELESS: u128 = 0x5f1_0000;

/// The tables the moved routes read with the switch off.
const SWITCH_SERVED_PERMISSION_TABLES: [&str; 4] = [
    "permissions_current",
    "permissions_current_resource_summary",
    "account_permission_state_current",
    "resolver_current",
];

/// One event the permissioned resolver emits, from its manifest.
fn switch_v2_resolver_event(
    identity: &str,
    resource_id: Option<Uuid>,
    kind: &str,
    (block, log): (i64, i64),
    derivation: &str,
    manifest_id: i64,
    after_state: Value,
) -> NormalizedEvent {
    let mut event = switch_event(
        identity,
        None,
        resource_id,
        kind,
        "ens_v2_resolver_l1",
        block,
        log,
        after_state,
    );
    event.derivation_kind = derivation.to_owned();
    event.raw_fact_ref["emitting_address"] = json!(SWITCH_V2_RESOLVER);
    event.source_manifest_id = Some(manifest_id);
    event.manifest_version = 1;
    event
}

/// A role change on the permissioned resolver: `subject` holds `powers` on `resource`'s node.
fn switch_v2_role(
    identity: &str,
    resource_id: Uuid,
    node: &str,
    subject: &str,
    (block, log): (i64, i64),
    powers: Value,
    manifest_id: i64,
) -> NormalizedEvent {
    switch_v2_resolver_event(
        identity,
        Some(resource_id),
        "PermissionChanged",
        (block, log),
        "ens_v2_permissions",
        manifest_id,
        json!({
            "subject": subject,
            "scope": {"kind": "resolver", "chain_id": SWITCH_CHAIN,
                      "resolver_address": SWITCH_V2_RESOLVER},
            "effective_powers": powers,
            "grant_source": {"kind": "raw_log", "source_event": "EACRolesChanged",
                "upstream_resource": node, "root_resource": false, "changed_powers": powers},
            "revocation_source": null, "inheritance_path": [], "transfer_behavior": {},
            "source_event": "EACRolesChanged", "upstream_resource": node, "resource": node,
            "root_resource": false, "storage_model": "resolver_record_id",
            "resolver": SWITCH_V2_RESOLVER, "resolver_record_id": "0",
            "record_key": "permission",
        }),
    )
}

/// `seed_switch_routes_fixture` (alpha.eth and beta.eth on an ENSv1 resolver, bob holding a
/// role on an ENSv1 resolver) plus the permissioned resolver: its manifest, its upgrade at 206,
/// role changes for bob on alpha.eth (210), alice on beta.eth (211, narrowed at 213) and carol on
/// a nameless resource (212), a record link for alpha.eth and a default link (214), and an alias
/// (215); all published at 240.
async fn seed_switch_permissions_fixture(database: &TestDatabase) -> Result<()> {
    seed_switch_routes_events(database).await?;
    let alpha = bigname_storage::logical_name_id_for_name("ens", "alpha.eth");
    let beta = bigname_storage::logical_name_id_for_name("ens", "beta.eth");
    let node = |id: &str| id.strip_prefix("ens:").expect("ens id").to_owned();
    let resource_of = |id: String| async move {
        sqlx::query_scalar::<_, Uuid>(
            "SELECT resource_id FROM bigname_phase.surface_bindings WHERE logical_name_id = $1",
        )
        .bind(id)
        .fetch_one(&database.pool)
        .await
    };
    let (alpha_resource, beta_resource) =
        (resource_of(alpha.clone()).await?, resource_of(beta.clone()).await?);
    let payload = json!({
        "deployment_epoch": "fixture",
        "resolver_implementations": [{"role": "permissioned_resolver",
                                      "address": SWITCH_V2_IMPLEMENTATION}],
        "contracts": [], "capability_flags": {},
        "abi": {"events": [{"name": "Linked",
            "fragment": "event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name)",
            "normalized_events": ["ResolverRecordLinked", "PreimageObserved"]}]},
    });
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO bigname_phase.manifest_versions (manifest_version, namespace,
             source_family, chain_id, deployment_label, rollout_status, normalizer_version,
             file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v2_resolver_l1', $1, 'fixture', 'active', 'fixture',
                 'fixture/switch-v2-resolver.toml', $2)
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
         VALUES ('switch-v2-manifest', 'ens', 'SourceManifestUpdated', 'ens_v2_resolver_l1', 1,
                 $1, $2, 'manifest_sync', 'canonical', $3)",
    )
    .bind(manifest_id)
    .bind(SWITCH_CHAIN)
    .bind(json!({"rollout_status": "active", "normalizer_version": "fixture",
                 "manifest_payload": payload}))
    .execute(&database.pool)
    .await?;
    let nameless = Uuid::from_u128(SWITCH_NAMELESS);
    sqlx::query(
        "INSERT INTO bigname_phase.resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1, $2, $3, 212, 'canonical')",
    )
    .bind(nameless)
    .bind(SWITCH_CHAIN)
    .bind("0xhistory212")
    .execute(&database.pool)
    .await?;
    let nameless_node = format!("0x{:064x}", 0x5f1u64);
    let link = |identity: &str, node: &str, record: &str, name: &str, log: i64| {
        switch_v2_resolver_event(
            identity,
            None,
            "ResolverRecordLinked",
            (214, log),
            "ens_v2_resolver",
            manifest_id,
            json!({"source_event": "Linked", "storage_model": "resolver_record_id",
                   "resolver": SWITCH_V2_RESOLVER, "node": node,
                   "resolver_record_id": record, "dns_encoded_name": name}),
        )
    };
    let events = vec![
        switch_v2_resolver_event(
            "switch-v2-upgrade",
            None,
            "Upgraded",
            (206, 1),
            "proxy_upgrade",
            manifest_id,
            json!({"source_event": "Upgraded", "proxy_address": SWITCH_V2_RESOLVER,
                   "implementation": SWITCH_V2_IMPLEMENTATION}),
        ),
        switch_v2_role(
            "switch-v2-bob",
            alpha_resource,
            &node(&alpha),
            SWITCH_BOB,
            (210, 0),
            json!(["set_text"]),
            manifest_id,
        ),
        switch_v2_role(
            "switch-v2-alice",
            beta_resource,
            &node(&beta),
            SWITCH_ALICE,
            (211, 0),
            json!(["set_addr", "set_text"]),
            manifest_id,
        ),
        switch_v2_role(
            "switch-v2-carol",
            nameless,
            &nameless_node,
            SWITCH_CAROL,
            (212, 0),
            json!(["set_text"]),
            manifest_id,
        ),
        switch_v2_role(
            "switch-v2-alice-narrowed",
            beta_resource,
            &node(&beta),
            SWITCH_ALICE,
            (213, 0),
            json!(["set_addr"]),
            manifest_id,
        ),
        link("switch-v2-link-alpha", &node(&alpha), "1", "0x05616c70686103657468", 0),
        link(
            "switch-v2-link-default",
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "2",
            "0x00",
            1,
        ),
        switch_v2_resolver_event(
            "switch-v2-alias",
            None,
            "AliasChanged",
            (215, 0),
            "ens_v2_resolver",
            manifest_id,
            json!({"source_event": "AliasChanged", "resolver": SWITCH_V2_RESOLVER,
                   "from_name": "old.eth", "to_name": "alpha.eth", "active": true}),
        ),
    ];
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_project_and_families(database, 240).await
}

/// Every moved route over the fixture, and whether its `data` must list rows.
fn switch_permission_uris() -> Vec<(String, bool)> {
    let resolver = format!("/v1/resolvers/1/{SWITCH_V2_RESOLVER}");
    vec![
        ("/v1/permissions?name=alpha.eth".to_owned(), true),
        ("/v1/permissions?name=beta.eth&include=lineage".to_owned(), true),
        (format!("/v1/permissions?address={SWITCH_BOB}&namespace=ens"), true),
        (format!("/v1/permissions?address={SWITCH_ALICE}&namespace=ens"), true),
        (
            format!("/v1/permissions?address={SWITCH_CAROL}&namespace=ens&include=lineage"),
            true,
        ),
        (format!("/v1/permissions?registration_id={}&namespace=ens",
                Uuid::from_u128(SWITCH_NAMELESS)), true),
        ("/v1/permissions?name=nobody.eth".to_owned(), false),
        (resolver.clone(), false),
        (format!("{resolver}/aliases"), true),
        (format!("{resolver}/links"), true),
        (format!("{resolver}/roles"), true),
        (format!("/v1/resolvers/1/{SWITCH_RESOLVER}/roles"), false),
        ("/v1/resolvers/1/0x0000000000000000000000000000000000000def".to_owned(), false),
    ]
}

#[tokio::test]
async fn v2_permissions_and_resolver_collections_are_the_same_with_the_switch_off_and_on(
) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_permissions_fixture(&database).await?;
    for (uri, rows) in switch_permission_uris() {
        let (status, body) = assert_switch_differential(&database, &uri).await?;
        if rows {
            assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
            assert!(
                body["data"].as_array().is_some_and(|rows| !rows.is_empty()),
                "{uri}: no rows in {body:#}"
            );
        }
    }
    // /roles: bob on alpha.eth, alice on beta.eth with the grant event of her first change,
    // carol on the nameless resource.
    let (_, roles) = assert_switch_differential(
        &database,
        &format!("/v1/resolvers/1/{SWITCH_V2_RESOLVER}/roles"),
    )
    .await?;
    let listed: Vec<(&Value, &Value, &Value)> = roles["data"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| (&row["address"], &row["powers"], &row["grant_event"]["block_number"]))
        .collect();
    assert_eq!(
        listed,
        [
            (&json!(SWITCH_BOB), &json!(["set_text"]), &json!(210)),
            (&json!(SWITCH_ALICE), &json!(["set_addr"]), &json!(211)),
            (&json!(SWITCH_CAROL), &json!(["set_text"]), &json!(212)),
        ],
        "{roles:#}"
    );
    for uri in [
        format!("/v1/resolvers/1/{SWITCH_V2_RESOLVER}/roles?page_size=1"),
        format!("/v1/resolvers/1/{SWITCH_V2_RESOLVER}/links?page_size=1"),
        format!("/v1/permissions?address={SWITCH_ALICE}&namespace=ens&page_size=1"),
        "/v1/permissions?name=beta.eth&page_size=1".to_owned(),
    ] {
        assert_switch_differential_pages(&database, &uri).await?;
    }
    database.cleanup().await
}

// With the served permission and resolver rows emptied, the switch-on answers do not move: the
// routes read the families.
#[tokio::test]
async fn v2_permissions_and_resolver_collections_read_no_served_row_with_the_switch_on(
) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_permissions_fixture(&database).await?;
    for (uri, _) in switch_permission_uris() {
        assert_switch_on_ignores_served_tables(&database, &uri, &SWITCH_SERVED_PERMISSION_TABLES)
            .await?;
    }
    database.cleanup().await
}

// J13: the families keep no lineage check at request time, so a role whose evidence event is
// orphaned after the publication is still listed, with the earliest readable event as its grant
// event; but a grant on a resource whose row is not readable is not served, on `/roles` or
// `/v1/permissions`, the same as the served route's readability join.
#[tokio::test]
async fn v2_resolver_roles_leave_out_an_unreadable_resource_with_the_switch_off_and_on(
) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_permissions_fixture(&database).await?;
    // Alice's first role change orphaned after the publication: her row is still listed (no
    // lineage check at request time), and its grant event is the earliest one still readable.
    sqlx::query(
        "UPDATE bigname_phase.normalized_events SET canonicality_state = 'orphaned'
         WHERE event_identity = 'switch-v2-alice'",
    )
    .execute(&database.pool)
    .await?;
    let (_, roles) = assert_switch_differential(
        &database,
        &format!("/v1/resolvers/1/{SWITCH_V2_RESOLVER}/roles"),
    )
    .await?;
    let alice = roles["data"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|row| row["address"] == json!(SWITCH_ALICE))
        .with_context(|| format!("no row for alice in {roles:#}"))?;
    assert_eq!(alice["grant_event"]["block_number"], json!(213), "{roles:#}");
    sqlx::query(
        "UPDATE bigname_phase.resources SET canonicality_state = 'orphaned'
         WHERE resource_id = $1",
    )
    .bind(Uuid::from_u128(SWITCH_NAMELESS))
    .execute(&database.pool)
    .await?;
    let (_, roles) = assert_switch_differential(
        &database,
        &format!("/v1/resolvers/1/{SWITCH_V2_RESOLVER}/roles"),
    )
    .await?;
    let holders: Vec<&Value> = roles["data"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| &row["address"])
        .collect();
    assert_eq!(holders, [&json!(SWITCH_BOB), &json!(SWITCH_ALICE)], "{roles:#}");
    assert_eq!(roles["page"]["total_count"], json!(2), "{roles:#}");
    let (_, carol) =
        assert_switch_differential(&database, &format!("/v1/permissions?address={SWITCH_CAROL}&namespace=ens"))
            .await?;
    assert_eq!(carol["data"], json!([]), "{carol:#}");
    database.cleanup().await
}

// A family rebuild in flight leaves nothing servable: each moved route answers a stale 409 with
// the switch on.
#[tokio::test]
async fn v2_permissions_and_resolver_collections_answer_409_while_the_families_rebuild(
) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_permissions_fixture(&database).await?;
    sqlx::query("UPDATE bigname_phase.project_family_marker SET state = 'bootstrap_pending'")
        .execute(&database.pool)
        .await?;
    for (uri, _) in switch_permission_uris() {
        let response = bigname_storage::publication_source::with_serve_from_families(
            true,
            v2_get_response(&database, &uri),
        )
        .await?;
        let status = response.status();
        let body: Value = read_json(response).await?;
        assert_eq!(
            (status, &body["error"]["code"]),
            (StatusCode::CONFLICT, &json!("stale")),
            "{uri}: {body:#}"
        );
    }
    database.cleanup().await
}
