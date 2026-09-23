#[tokio::test]
async fn pre_enrichment_registrar_resolver_keeps_the_registration_handle() -> Result<()> {
    const ADDRESS: &str = "0x0000000000000000000000000000000000007123";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = "ens:pre-enrichment-resolver.eth";
    let registrar_resource_id = Uuid::from_u128(0x7123);
    let surface_binding_id = Uuid::from_u128(0x9123);
    seed_identity_name(
        &database,
        logical_name_id,
        "pre-enrichment-resolver.eth",
        "pre-enrichment-resolver.eth",
        "node:pre-enrichment-resolver.eth",
        registrar_resource_id,
        Uuid::from_u128(0x8123),
        surface_binding_id,
        ADDRESS,
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    seed_v2_history_blocks(&database, 121..=122).await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000122)
         WHERE surface_binding_id = $1",
    )
    .bind(surface_binding_id)
    .execute(&database.pool)
    .await?;

    let mut resolver = v2_history_event(
        "registrar-resolver-before-enrichment",
        None,
        Some(registrar_resource_id),
        "ResolverChanged",
        121,
    );
    resolver.source_family = "ens_v1_registry_l1".to_owned();
    resolver.after_state = json!({
        "source_event": "NewResolver",
        "node": "node:pre-enrichment-resolver.eth",
        "authority_kind": "registrar",
        "resolver": "0x00000000000000000000000000000000000000aa"
    });
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[resolver]).await?;

    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={registrar_resource_id}&page_size=20"),
    )
    .await?;
    let rows = payload["data"].as_array().expect("registrar history rows");
    assert_eq!(rows.len(), 1, "pre-enrichment resolver was omitted: {rows:?}");
    assert_eq!(rows[0]["type"], json!("resolver"));
    assert_eq!(
        rows[0]["registration_id"],
        json!(registrar_resource_id.to_string())
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_registry_only_binding_does_not_admit_registration_history() -> Result<()> {
    assert_registry_binding_does_not_witness_registration(false).await
}

#[tokio::test]
async fn v2_retained_registry_after_release_does_not_admit_registration_history() -> Result<()> {
    assert_registry_binding_does_not_witness_registration(true).await
}

async fn assert_registry_binding_does_not_witness_registration(released: bool) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let name = "registry-witness.eth";
    let logical_name_id = "ens:registry-witness.eth";
    let registry = Uuid::from_u128(0x7132);
    let registrar = Uuid::from_u128(0x7133);
    seed_identity_name(
        &database,
        logical_name_id,
        name,
        name,
        "node:registry-witness.eth",
        registry,
        Uuid::from_u128(0x8132),
        Uuid::from_u128(0x9132),
        "0x0000000000000000000000000000000000007132",
        bigname_storage::AddressNameRelation::EffectiveController,
        80,
    )
    .await?;
    seed_v2_history_blocks(&database, 120..=125).await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp($2)
         WHERE resource_id = $1",
    )
    .bind(registry)
    .bind(if released {
        1_700_000_123_f64
    } else {
        1_700_000_120_f64
    })
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET serving_resource_id = $1, resource_id = NULL, token_lineage_id = NULL,
             surface_binding_id = NULL, binding_kind = NULL,
             declared_summary = jsonb_build_object(
                 'registration', jsonb_build_object('status', 'unregistered'),
                 'control', jsonb_build_object('status', $2::text)
             )
         WHERE resource_id = $1",
    )
    .bind(registry)
    .bind(if released { "unregistered" } else { "active" })
    .execute(&database.pool)
    .await?;
    for query in [
        "UPDATE bigname_phase.address_names_current SET token_lineage_id = NULL WHERE resource_id = $1",
        "UPDATE bigname_phase.resources SET token_lineage_id = NULL WHERE resource_id = $1",
    ] {
        sqlx::query(query)
            .bind(registry)
            .execute(&database.pool)
            .await?;
    }
    let mut binding = v2_history_event(
        "registry-witness-binding",
        Some(logical_name_id),
        Some(registry),
        "SurfaceBound",
        if released { 123 } else { 120 },
    );
    binding.source_family = "ens_v1_registry_l1".to_owned();
    let mut record = v2_history_event(
        "registry-witness-record",
        Some(logical_name_id),
        None,
        "RecordChanged",
        124,
    );
    record.source_family = "ens_v1_resolver_l1".to_owned();
    let mut events = vec![binding, record];
    for kind in ["AuthorityTransferred", "AuthorityEpochChanged"] {
        let mut authority = v2_history_event(
            &format!("registry-witness-{kind}"),
            Some(logical_name_id),
            Some(registry),
            kind,
            123,
        );
        authority.source_family = "ens_v1_registry_l1".to_owned();
        authority.after_state = json!({
            "authority_kind": "registry_only",
            "owner": "0x0000000000000000000000000000000000007132",
            "owner_getter": "0x0000000000000000000000000000000000007132",
        });
        events.push(authority);
    }
    if released {
        upsert_test_resources(
            &database.pool,
            &[address_name_resource(
                registrar,
                None,
                "0xregistry-witness-registrar",
                81,
            )],
        )
        .await?;
        let mut old_binding = address_name_surface_binding(
            Uuid::from_u128(0x9133),
            logical_name_id,
            registrar,
            "0xhistory120",
            120,
            1_700_000_120,
        );
        old_binding.active_to = Some(timestamp(1_700_000_123));
        upsert_test_surface_bindings(&database.pool, &[old_binding]).await?;
        for (kind, block, resource) in [
            ("RegistrationGranted", 120, Some(registrar)),
            ("RecordChanged", 121, None),
            ("RegistrationReleased", 122, Some(registrar)),
        ] {
            events.push(v2_history_event(
                &format!("registry-witness-{kind}"),
                Some(logical_name_id),
                resource,
                kind,
                block,
            ));
        }
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    let filtered = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={registry}&page_size=20"),
    )
    .await?;
    assert!(
        filtered["data"].as_array().is_some_and(Vec::is_empty),
        "registry resource admitted product registration history: {filtered:?}"
    );
    assert_eq!(filtered["page"]["has_more"], json!(false));
    for route in [
        format!("/v1/names/{name}/history?scope=both&page_size=20"),
        format!("/v1/events?name={name}&page_size=20"),
    ] {
        let payload = v2_history_payload_for_database(&database, &route).await?;
        let rows = payload["data"].as_array().expect("name history rows");
        assert_eq!(
            rows.len(),
            if released { 6 } else { 3 },
            "{route}: {rows:?}"
        );
        assert_eq!(rows[0]["type"], json!("record"));
        assert_eq!(rows[0]["block_number"], json!(124));
        assert_eq!(rows[0]["registration_id"], Value::Null);
        assert_eq!(
            rows.iter().filter(|row| row["type"] == "authority").count(),
            2
        );
    }
    let diagnostics = v2_history_payload_for_database(
        &database,
        &format!("/v1/diagnostics/events?name={name}&page_size=20"),
    )
    .await?;
    assert_eq!(
        diagnostics["data"].as_array().map(Vec::len),
        Some(if released { 7 } else { 4 })
    );
    for (resource_id, include_candidates, expected) in [
        (Some(registry), false, 0),
        (Some(registry), true, if released { 7 } else { 4 }),
        (None, false, if released { 6 } else { 3 }),
    ] {
        let page = bigname_storage::load_event_history_page(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                resource_id,
                logical_name_id: resource_id
                    .is_none()
                    .then(|| bigname_storage::logical_name_id_for_name("ens", name)),
                event_kinds: if include_candidates {
                    Vec::new()
                } else {
                    crate::v2::product_history_event_kinds()
                },
                ..bigname_storage::EventHistoryFilter::default()
            },
            true,
            None,
            1,
            bigname_storage::HistorySummaryMode::Full,
            include_candidates,
        )
        .await?;
        assert_eq!(page.rows.len(), usize::from(expected != 0));
        assert_eq!(page.next_cursor.is_some(), expected > 1);
        assert_eq!(page.summary.unwrap().total_count, expected);
    }
    if released {
        let previous = v2_history_payload_for_database(
            &database,
            &format!("/v1/events?registration_id={registrar}&page_size=20"),
        )
        .await?;
        let rows = previous["data"]
            .as_array()
            .expect("released registration history");
        assert_eq!(
            history_types(rows),
            vec!["release", "record", "registration"]
        );
        assert_eq!(rows[1]["block_number"], json!(121));
        assert_eq!(rows[1]["registration_id"], Value::Null);
    }
    database.cleanup().await
}

// A registry-only binding (a registrar token transferred without `reclaim`) can outlive several
// `registerOnly` re-registrations. Each successor lease has no binding of its own and no
// `NameWrapped` link, and only the latest one is the declared registration, so an earlier
// successor's grant, renewal, and release stay in registration-scoped name history through the
// name their grant carries.
#[tokio::test]
async fn registration_history_keeps_an_earlier_successor_lease_under_a_registry_only_binding()
-> Result<()> {
    const NAME: &str = "registry-successors.eth";
    const SEED_LOGICAL_NAME_ID: &str = "ens:registry-successors.eth";
    const HOLDER: &str = "0x0000000000000000000000000000000000007170";
    let database = TestDatabase::new_migrated().await?;
    let registry = Uuid::from_u128(0x7170);
    let earlier_lease = Uuid::from_u128(0x7171);
    let current_lease = Uuid::from_u128(0x7172);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);

    // The name is bound to the registry-only resource throughout.
    seed_identity_name(
        &database,
        SEED_LOGICAL_NAME_ID,
        NAME,
        NAME,
        "node:registry-successors.eth",
        registry,
        Uuid::from_u128(0x8170),
        Uuid::from_u128(0x9170),
        HOLDER,
        bigname_storage::AddressNameRelation::EffectiveController,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[
            address_name_resource(earlier_lease, None, "0xearlier-lease-resource", 78),
            address_name_resource(current_lease, None, "0xcurrent-lease-resource", 79),
        ],
    )
    .await?;
    let blocks = (120..=125)
        .map(|number| {
            let parent = (number > 120).then(|| format!("0xhistory{}", number - 1));
            raw_block(
                "ethereum-mainnet",
                &format!("0xhistory{number}"),
                parent.as_deref(),
                number,
                1_700_000_000 + number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    seed_schema_v2_ens_lookup_head(
        &database.pool,
        125,
        "0xhistory125",
        &crate::v2::format_timestamp(timestamp(1_700_000_125)),
    )
    .await?;
    // Project selected the latest successor lease as the name's registration.
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
             declared_summary, '{registration,resource_id}', to_jsonb($2::text)
         )
         WHERE resource_id = $1",
    )
    .bind(registry)
    .bind(current_lease.to_string())
    .execute(&database.pool)
    .await?;

    let lease_event = |identity: &str, resource: Uuid, kind: &str, block_number: i64| {
        let mut event = v2_history_event(
            identity,
            (kind != "RegistrationGranted").then_some(logical_name_id.as_str()),
            Some(resource),
            kind,
            block_number,
        );
        event.after_state["namehash"] = json!(&namehash);
        event
    };
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            lease_event(
                "successor-earlier-grant",
                earlier_lease,
                "RegistrationGranted",
                120,
            ),
            lease_event(
                "successor-earlier-renewal",
                earlier_lease,
                "RegistrationRenewed",
                121,
            ),
            lease_event(
                "successor-earlier-release",
                earlier_lease,
                "RegistrationReleased",
                122,
            ),
            lease_event(
                "successor-current-grant",
                current_lease,
                "RegistrationGranted",
                124,
            ),
        ],
    )
    .await?;

    let registration = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/{NAME}/history?scope=registration&page_size=20"),
    )
    .await?;
    assert_eq!(
        history_transaction_hashes(&registration),
        vec!["0xtx124", "0xtx122", "0xtx121", "0xtx120"],
        "registration-scoped name history lost the earlier successor lease: {registration}"
    );
    let rows = registration["data"].as_array().expect("registration rows");
    assert_eq!(rows[0]["registration_id"], json!(current_lease.to_string()));
    for row in &rows[1..] {
        assert_eq!(
            row["registration_id"],
            json!(earlier_lease.to_string()),
            "{row}"
        );
    }
    assert_eq!(registration["page"]["total_count"], json!(4));

    // The earlier lease is still a registration in its own right.
    let earlier = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={earlier_lease}&page_size=20"),
    )
    .await?;
    assert_eq!(
        history_transaction_hashes(&earlier),
        vec!["0xtx122", "0xtx121", "0xtx120"],
        "{earlier}"
    );

    // The grants are found through the node probe index, not by scanning normalized_events.
    let plan = bigname_storage::explain_registration_history_filter_for_test(
        &database.pool,
        earlier_lease,
        &logical_name_id,
        "ethereum-mainnet",
        "ens",
        &namehash,
    )
    .await?;
    let grant_plan = plan
        .split_once("registrar grants by name:\n")
        .and_then(|(_, rest)| rest.split_once("\n\n"))
        .map(|(grant_plan, _)| grant_plan)
        .expect("combined plan must contain the registrar grant section");
    assert!(
        grant_plan.contains("normalized_events_v1_direct_node_probe_idx"),
        "registrar grants by name must probe the node index:\n{grant_plan}"
    );

    // The retained registry authority serves successive leases without receiving a new
    // binding. Registrar grants above intentionally have no logical_name_id.
    sqlx::query("UPDATE bigname_phase.surface_bindings SET active_from = to_timestamp(1700000120), block_number = 120, block_hash = '0xhistory120' WHERE resource_id = $1")
        .bind(registry).execute(&database.pool).await?;
    let mut registry_epoch = v2_history_event(
        "successor-registry-epoch",
        Some(&logical_name_id),
        Some(registry),
        "SurfaceBound",
        120,
    );
    registry_epoch.log_index = Some(1);
    registry_epoch.after_state = json!({"authority_kind": "registry_only", "node": namehash});
    let mut registry_events = vec![registry_epoch];
    for (block, label) in [(121, "earlier"), (123, "gap"), (125, "current")] {
        let mut resolver = v2_history_event(
            &format!("successor-{label}-resolver"),
            Some(&logical_name_id),
            Some(registry),
            "ResolverChanged",
            block,
        );
        resolver.source_family = "ens_v1_registry_l1".to_owned();
        resolver.after_state["namehash"] = json!(namehash);
        resolver.log_index = Some(1);
        let mut record = v2_history_event(
            &format!("successor-{label}-record"),
            Some(&logical_name_id),
            None,
            "RecordChanged",
            block,
        );
        record.source_family = "ens_v1_resolver_l1".to_owned();
        record.log_index = Some(2);
        registry_events.extend([resolver, record]);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &registry_events).await?;
    let name_history = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/{NAME}/history?scope=both&page_size=20"),
    )
    .await?;
    for (block, expected) in [
        (121, json!(earlier_lease)),
        (123, Value::Null),
        (125, json!(current_lease)),
    ] {
        let resolver = name_history["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["type"] == "resolver" && row["block_number"] == block)
            .expect("registry resolver event");
        assert_eq!(resolver["registration_id"], expected, "{name_history}");
    }
    for (lease, block) in [(earlier_lease, 121), (current_lease, 125)] {
        let history = v2_history_payload_for_database(
            &database,
            &format!("/v1/events?registration_id={lease}&page_size=20"),
        )
        .await?;
        let rows = history["data"].as_array().unwrap();
        for kind in ["resolver", "record"] {
            let matches = rows
                .iter()
                .filter(|row| row["type"] == kind)
                .collect::<Vec<_>>();
            assert_eq!(
                matches.len(),
                1,
                "{lease} must keep exactly its own {kind}: {history}"
            );
            assert_eq!(matches[0]["block_number"], json!(block), "{history}");
        }
        let retained = bigname_storage::load_event_history(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                resource_id: Some(lease),
                ..Default::default()
            },
            false,
        )
        .await?;
        for kind in ["ResolverChanged", "RecordChanged"] {
            let matches = retained
                .iter()
                .filter(|row| row.event_kind == kind)
                .collect::<Vec<_>>();
            assert_eq!(matches.len(), 1, "{retained:?}");
            assert_eq!(matches[0].block_number, Some(block), "{retained:?}");
        }
    }
    let registry_history = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={registry}&page_size=20"),
    )
    .await?;
    assert!(
        registry_history["data"].as_array().unwrap().is_empty(),
        "{registry_history}"
    );

    // A candidate grant cannot make an otherwise unbound resource part of name history.
    let candidate_lease = Uuid::from_u128(0x7173);
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            candidate_lease,
            None,
            "0xcandidate-lease",
            79,
        )],
    )
    .await?;
    let mut candidate_grant = lease_event(
        "successor-candidate-grant",
        candidate_lease,
        "RegistrationGranted",
        123,
    );
    candidate_grant.log_index = Some(3);
    let mut candidate_renewal = lease_event(
        "successor-candidate-renewal",
        candidate_lease,
        "RegistrationRenewed",
        123,
    );
    candidate_renewal.logical_name_id = None;
    candidate_renewal.log_index = Some(4);
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[candidate_grant, candidate_renewal],
    )
    .await?;
    sqlx::query("UPDATE bigname_phase.normalized_events SET consumer_visibility = 'candidate', migration_correlation_ids = ARRAY['history-candidate-grant'] WHERE event_identity = 'successor-candidate-grant'")
        .execute(&database.pool).await?;
    let before_activation = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/{NAME}/history?scope=registration&page_size=20"),
    )
    .await?;
    assert!(
        before_activation["data"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| { row["registration_id"] != json!(candidate_lease) }),
        "candidate grant admitted activated lease history: {before_activation}"
    );
    sqlx::query("UPDATE bigname_phase.normalized_events SET consumer_visibility = 'activated' WHERE event_identity = 'successor-candidate-grant'")
        .execute(&database.pool).await?;
    let after_activation = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/{NAME}/history?scope=registration&page_size=20"),
    )
    .await?;
    assert_eq!(
        after_activation["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| { row["registration_id"] == json!(candidate_lease) })
            .count(),
        2,
        "the activated grant must not claim an earlier registry resolver in the same block: {after_activation}"
    );
    assert_eq!(
        after_activation["page"]["total_count"].as_u64(),
        before_activation["page"]["total_count"]
            .as_u64()
            .map(|count| count + 2)
    );

    database.cleanup().await
}

#[tokio::test]
async fn nameless_registry_events_keep_the_lease_before_surface_materialization() -> Result<()> {
    const NAME: &str = "nameless-registry-permissions.eth";
    const OWNER: &str = "0x0000000000000000000000000000000000007190";
    let database = TestDatabase::new_migrated().await?;
    let node = bigname_lookup::ens_namehash_hex(NAME)?;
    let registry = Uuid::from_u128(0x7190);
    let lease = Uuid::from_u128(0x7191);
    let successor = Uuid::from_u128(0x7192);
    seed_identity_name(
        &database,
        "ens:nameless-registry-permissions.eth",
        NAME,
        NAME,
        &node,
        registry,
        Uuid::from_u128(0x8190),
        Uuid::from_u128(0x9190),
        OWNER,
        bigname_storage::AddressNameRelation::EffectiveController,
        80,
    )
    .await?;
    let blocks = (120..=127)
        .map(|number| {
            let parent = (number > 120).then(|| format!("0xhistory{}", number - 1));
            raw_block(
                "ethereum-mainnet",
                &format!("0xhistory{number}"),
                parent.as_deref(),
                number,
                1_700_000_000 + number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    seed_schema_v2_ens_lookup_head(
        &database.pool,
        127,
        "0xhistory127",
        &crate::v2::format_timestamp(timestamp(1_700_000_127)),
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[
            address_name_resource(lease, None, "0xnameless-lease", 120),
            address_name_resource(successor, None, "0xnameless-successor", 125),
        ],
    )
    .await?;
    // Name materialization makes the registry resource reachable only after these rows
    // were emitted. It must not rewrite the rows' missing logical_name_id or node fields.
    sqlx::query("UPDATE bigname_phase.surface_bindings SET block_number = 126, block_hash = '0xhistory126', active_from = to_timestamp(1700000126) WHERE resource_id = $1")
        .bind(registry).execute(&database.pool).await?;
    sqlx::query("UPDATE bigname_phase.name_current SET declared_summary = jsonb_set(declared_summary, '{registration,resource_id}', to_jsonb($2::text)) WHERE resource_id = $1")
        .bind(registry).bind(successor.to_string()).execute(&database.pool).await?;
    // Registry control resources have no token lineage. None of the resolver rows below
    // has a binding at its event position; the later binding supplies only reachability.
    sqlx::query("UPDATE bigname_phase.address_names_current SET token_lineage_id = NULL WHERE resource_id = $1")
        .bind(registry).execute(&database.pool).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current SET token_lineage_id = NULL WHERE resource_id = $1",
    )
    .bind(registry)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.resources SET token_lineage_id = NULL WHERE resource_id = $1",
    )
    .bind(registry)
    .execute(&database.pool)
    .await?;
    let mut grant = v2_history_event(
        "nameless-registry-grant",
        None,
        Some(lease),
        "RegistrationGranted",
        120,
    );
    grant.after_state["namehash"] = json!(node);
    let mut authority = v2_history_event(
        "nameless-registry-authority",
        None,
        Some(registry),
        "AuthorityTransferred",
        121,
    );
    authority.source_family = "ens_v1_registry_l1".to_owned();
    authority.after_state = json!({"source_event": "Transfer", "node": node,
        "owner": OWNER, "owner_getter": OWNER, "authority_kind": "registry_only",
        "authority_key": format!("registry-only:ethereum-mainnet:{node}")});
    let mut release = v2_history_event(
        "nameless-registry-release",
        None,
        Some(lease),
        "RegistrationReleased",
        124,
    );
    release.after_state["namehash"] = json!(node);
    let mut next = v2_history_event(
        "nameless-registry-successor",
        None,
        Some(successor),
        "RegistrationGranted",
        125,
    );
    next.after_state["namehash"] = json!(node);
    let mut events = vec![grant, authority, release, next];
    // NewResolver retains the controlling registry resource but no logical name before
    // discovery, and its payload carries the node (registry::surface::link_resolver_event).
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L86-L94 @ ens_v1@91c966f)
    for block in [122, 124, 125] {
        let mut resolver = v2_history_event(
            &format!("nameless-resolver-{block}"),
            None,
            Some(registry),
            "ResolverChanged",
            block,
        );
        resolver.source_family = "ens_v1_registry_l1".into();
        resolver.log_index = Some(1);
        resolver.after_state = json!({"source_event": "NewResolver", "node": node,
            "resolver": "0x0000000000000000000000000000000000000abc"});
        events.push(resolver);
    }
    // These are the exact nameless payload shapes made by registry::push_permission_change
    // and protocol::permissions::{v1_grant_states,v1_revoke_states}: authority metadata
    // is nested under the permission source, and neither source contains the node.
    for (suffix, granted) in [("grant", true), ("revoke", false)] {
        let mut permission = v2_history_event(
            &format!("nameless-registry-permission-{suffix}"),
            None,
            Some(registry),
            "PermissionChanged",
            121,
        );
        permission.source_family = "ens_v1_registry_l1".to_owned();
        let source = json!({"kind": "ens_v1_authority", "authority_kind": "registry_only",
            "authority_key": format!("registry-only:ethereum-mainnet:{node}"),
            "source_event_kind": "AuthorityTransferred"});
        let state = |powers: Value, grant_source: Value, revocation_source: Value| {
            json!({
            "subject": if granted { OWNER } else { "0x0000000000000000000000000000000000007189" },
            "scope": {"kind": "resource"}, "effective_powers": powers,
            "grant_source": grant_source, "revocation_source": revocation_source,
            "inheritance_path": [], "transfer_behavior": "replace_on_authority_change"})
        };
        permission.before_state = if granted {
            state(json!([]), Value::Null, Value::Null)
        } else {
            state(json!(["resource_control"]), source.clone(), Value::Null)
        };
        permission.after_state = if granted {
            state(json!(["resource_control"]), source, Value::Null)
        } else {
            state(json!([]), Value::Null, source)
        };
        events.push(permission);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    for route in [
        format!("/v1/events?name={NAME}&page_size=20"),
        format!("/v1/events?registration_id={lease}&page_size=20"),
    ] {
        let history = v2_history_payload_for_database(&database, &route).await?;
        let permissions = history["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["type"] == "permission")
            .collect::<Vec<_>>();
        assert_eq!(permissions.len(), 2, "{route}: {history}");
        assert!(
            permissions
                .iter()
                .all(|row| row["registration_id"] == json!(lease)),
            "{route}: {history}"
        );
    }
    let all =
        v2_history_payload_for_database(&database, &format!("/v1/events?name={NAME}&page_size=20"))
            .await?;
    for (block, expected) in [
        (122, json!(lease)),
        (124, Value::Null),
        (125, json!(successor)),
    ] {
        let row = all["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["type"] == "resolver" && row["block_number"] == block)
            .unwrap();
        assert_eq!(row["registration_id"], expected, "{all}");
    }
    for (id, expected_block, count) in [(lease, 122, 6), (successor, 125, 2)] {
        let history = v2_history_payload_for_database(
            &database,
            &format!("/v1/events?registration_id={id}&page_size=20"),
        )
        .await?;
        let resolvers = history["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["type"] == "resolver")
            .collect::<Vec<_>>();
        assert_eq!(resolvers.len(), 1, "{history}");
        assert_eq!(
            resolvers[0]["block_number"],
            json!(expected_block),
            "{history}"
        );
        assert_eq!(history["page"]["total_count"], json!(count), "{history}");
    }
    let later = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={successor}&page_size=20"),
    )
    .await?;
    assert!(
        later["data"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["type"] != "permission"),
        "{later}"
    );
    let retained = bigname_storage::load_event_history(
        &database.pool,
        bigname_storage::EventHistoryFilter {
            resource_id: Some(lease),
            ..Default::default()
        },
        false,
    )
    .await?;
    let resolvers = retained
        .iter()
        .filter(|row| row.event_kind == "ResolverChanged")
        .collect::<Vec<_>>();
    assert_eq!(resolvers.len(), 1, "{retained:?}");
    assert_eq!(resolvers[0].block_number, Some(122), "{retained:?}");
    assert_eq!(resolvers[0].registration_id, Some(lease), "{retained:?}");
    let permissions = retained
        .iter()
        .filter(|row| row.event_kind == "PermissionChanged")
        .collect::<Vec<_>>();
    assert_eq!(permissions.len(), 2, "{retained:?}");
    assert!(
        permissions
            .iter()
            .all(|row| row.registration_id == Some(lease)),
        "{retained:?}"
    );
    // setRecord can clear an existing registry owner and then emit NewResolver in the
    // same call. The read resource survives, but its former control must not confer a lease.
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L29-L41 @ ens_v1@91c966f)
    let logical: String = sqlx::query_scalar(
        "SELECT logical_name_id FROM bigname_phase.surface_bindings WHERE resource_id = $1",
    )
    .bind(registry)
    .fetch_one(&database.pool)
    .await?;
    sqlx::query("UPDATE bigname_phase.surface_bindings SET active_to = to_timestamp(1700000127) WHERE resource_id = $1")
        .bind(registry).execute(&database.pool).await?;
    let mut ownerless = v2_history_event(
        "registry-control-cleared",
        Some(&logical),
        Some(registry),
        "AuthorityTransferred",
        127,
    );
    ownerless.source_family = "ens_v1_registry_l1".into();
    ownerless.after_state = json!({"source_event": "Transfer", "node": node,
        "owner": "0x0000000000000000000000000000000000000000",
        "owner_getter": "0x0000000000000000000000000000000000000000", "authority_kind": null});
    let mut read_resolver = v2_history_event(
        "registry-read-resolver",
        Some(&logical),
        Some(registry),
        "ResolverChanged",
        127,
    );
    read_resolver.source_family = "ens_v1_registry_l1".into();
    read_resolver.log_index = Some(1);
    read_resolver.after_state = json!({"source_event": "NewResolver", "node": node,
        "resolver": "0x0000000000000000000000000000000000000abd"});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[ownerless, read_resolver])
        .await?;
    let all =
        v2_history_payload_for_database(&database, &format!("/v1/events?name={NAME}&page_size=20"))
            .await?;
    let read_only = all["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["type"] == "resolver" && row["block_number"] == 127)
        .unwrap();
    assert_eq!(read_only["registration_id"], Value::Null, "{all}");
    let current = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={successor}&page_size=20"),
    )
    .await?;
    assert_eq!(current["page"]["total_count"], json!(2), "{current}");
    database.cleanup().await
}

#[tokio::test]
async fn noncanonical_history_of_a_name_wrapped_at_registration_keeps_the_registrar_lease_handle() -> Result<()> {
    const NAME: &str = "noncanonical-born-wrapped.eth";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let wrapper = Uuid::from_u128(0x7160);
    let registrar = Uuid::from_u128(0x7161);
    seed_identity_name(
        &database,
        "ens:noncanonical-born-wrapped.eth",
        NAME,
        NAME,
        &namehash,
        wrapper,
        Uuid::from_u128(0x8160),
        Uuid::from_u128(0x9160),
        "0x0000000000000000000000000000000000007160",
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            registrar,
            None,
            "0xnoncanonical-registrar",
            79,
        )],
    )
    .await?;
    let blocks = (130..=133)
        .map(|block_number| {
            let parent = (block_number > 130).then(|| format!("0xhistory{}", block_number - 1));
            raw_block(
                "ethereum-mainnet",
                &format!("0xhistory{block_number}"),
                parent.as_deref(),
                block_number,
                1_700_000_000 + block_number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000130), canonicality_state = 'orphaned',
             block_hash = '0xhistory130', block_number = 130
         WHERE resource_id = $1",
    )
    .bind(wrapper)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_surfaces
         SET canonicality_state = 'orphaned', block_hash = '0xhistory130', block_number = 130
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .execute(&database.pool)
    .await?;
    let mut grant = v2_history_event(
        "noncanonical-grant",
        None,
        Some(registrar),
        "RegistrationGranted",
        130,
    );
    grant.after_state["namehash"] = json!(namehash);
    let mut binding = v2_history_event(
        "noncanonical-binding",
        Some(&logical_name_id),
        Some(wrapper),
        "SurfaceBound",
        130,
    );
    binding.source_family = "ens_v1_wrapper_l1".to_owned();
    binding.after_state = json!({"wrapped_registrar_resource_id": registrar});
    let mut transfer = v2_history_event(
        "noncanonical-transfer",
        Some(&logical_name_id),
        Some(wrapper),
        "TokenControlTransferred",
        131,
    );
    transfer.source_family = "ens_v1_wrapper_l1".to_owned();
    let permission = v2_history_event(
        "noncanonical-permission",
        None,
        Some(registrar),
        "PermissionChanged",
        132,
    );
    let mut record = v2_history_event(
        "noncanonical-record",
        Some(&logical_name_id),
        None,
        "RecordChanged",
        133,
    );
    record.source_family = "ens_v1_resolver_l1".to_owned();
    let mut events = vec![grant, binding, transfer, permission, record];
    for event in &mut events {
        event.canonicality_state = CanonicalityState::Orphaned;
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    sqlx::query(
        "UPDATE bigname_phase.chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = 'ethereum-mainnet' AND block_number BETWEEN 130 AND 133",
    )
    .execute(&database.pool)
    .await?;
    for canonical_only in [true, false] {
        let name_rows = bigname_storage::load_name_history(
            &database.pool,
            &logical_name_id,
            &[wrapper, registrar],
            bigname_storage::HistoryScope::Both,
            canonical_only,
        )
        .await?;
        let registration_rows = bigname_storage::load_event_history(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                resource_id: Some(registrar),
                ..bigname_storage::EventHistoryFilter::default()
            },
            canonical_only,
        )
        .await?;
        for rows in [name_rows, registration_rows] {
            assert_eq!(rows.len(), if canonical_only { 0 } else { 5 }, "{rows:?}");
            if canonical_only {
                continue;
            }
            assert_eq!(
                rows.iter()
                    .map(|row| row.event_identity.as_str())
                    .collect::<Vec<_>>(),
                vec![
                    "noncanonical-record",
                    "noncanonical-permission",
                    "noncanonical-transfer",
                    "noncanonical-grant",
                    "noncanonical-binding"
                ]
            );
            assert!(
                rows.iter()
                    .filter(|row| row.resource_id.is_some())
                    .all(|row| row.registration_id == Some(registrar)),
                "{rows:?}"
            );
            assert_eq!(rows[0].resource_id, None);
            assert_eq!(rows[0].registration_id, None);
        }
    }
    // The NameWrapper resource is never a second handle for the same lease.
    let wrapper_rows = bigname_storage::load_event_history(
        &database.pool,
        bigname_storage::EventHistoryFilter {
            resource_id: Some(wrapper),
            ..bigname_storage::EventHistoryFilter::default()
        },
        false,
    )
    .await?;
    assert!(
        wrapper_rows.is_empty(),
        "second lifecycle handle: {wrapper_rows:?}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn name_wrapped_at_registration_uses_the_registrar_lease_handle() -> Result<()> {
    const NAME: &str = "born-wrapped-history.eth";
    const LOGICAL: &str = "ens:born-wrapped-history.eth";
    let database = TestDatabase::new_migrated().await?;
    let wrapper = Uuid::from_u128(0x7140);
    let registrar = Uuid::from_u128(0x7141);
    let rewrapper = Uuid::from_u128(0x7142);
    let orphan_wrapper = Uuid::from_u128(0x7143);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    seed_identity_name(
        &database,
        LOGICAL,
        NAME,
        NAME,
        &namehash,
        wrapper,
        Uuid::from_u128(0x8140),
        Uuid::from_u128(0x9140),
        "0x0000000000000000000000000000000000007140",
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[
            address_name_resource(registrar, None, "0xborn-wrap-registrar", 79),
            address_name_resource(rewrapper, None, "0xborn-wrap-rewrapper", 79),
            address_name_resource(orphan_wrapper, None, "0xborn-wrap-orphan", 79),
        ],
    )
    .await?;
    // Project serves the registrar lease as the registration resource of a wrapped name.
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
             declared_summary, '{registration,resource_id}', to_jsonb($2::text), true)
         WHERE raw_name = $1",
    )
    .bind(NAME)
    .bind(registrar)
    .execute(&database.pool)
    .await?;
    // Read the detail before the history blocks move the chain head past the seeded row.
    let detail =
        v2_history_payload_for_database(&database, &format!("/v1/names/{NAME}")).await?;
    assert_eq!(
        detail["data"]["registration_id"],
        json!(registrar.to_string()),
        "a name wrapped at registration must serve its registrar lease as registration_id"
    );
    // Block 137 keeps the chain head off block 136, which the last step orphans.
    seed_v2_history_blocks(&database, 130..=137).await?;
    // Bind the fixture at its normalized history time, rather than the seed's default date.
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000130) WHERE resource_id = $1",
    )
    .bind(wrapper)
    .execute(&database.pool)
    .await?;
    let wrapper_event = |identity: &str, resource: Uuid, kind: &str, block: i64| {
        let mut event = v2_history_event(identity, Some(LOGICAL), Some(resource), kind, block);
        event.source_family = "ens_v1_wrapper_l1".to_owned();
        if kind == "SurfaceBound" {
            event.after_state = json!({
                "source_event": "NameWrapped",
                "node": namehash,
                "wrapped_registrar_resource_id": registrar,
            });
        }
        event
    };
    // The BaseRegistrar grant and the NameWrapped binding share block 130: the name was
    // registered through the NameWrapper.
    let mut grant = v2_history_event(
        "born-wrap-grant",
        None,
        Some(registrar),
        "RegistrationGranted",
        130,
    );
    grant.after_state["namehash"] = json!(namehash);
    let mut setup = v2_history_event(
        "born-wrap-setup",
        None,
        Some(registrar),
        "AuthorityTransferred",
        130,
    );
    setup.source_family = "ens_v1_registry_l1".to_owned();
    setup.after_state = json!({
        "source_event": "NewOwner",
        "child_node": namehash,
        "owner": "0x0000000000000000000000000000000000007140",
    });
    let unwrapped_transfer = v2_history_event(
        "born-wrap-unwrapped-transfer",
        Some(LOGICAL),
        Some(registrar),
        "TokenControlTransferred",
        132,
    );
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            grant,
            setup,
            wrapper_event("born-wrap-binding", wrapper, "SurfaceBound", 130),
            wrapper_event("born-wrap-transfer", wrapper, "TokenControlTransferred", 131),
            wrapper_event("born-wrap-unbound", wrapper, "SurfaceUnbound", 132),
            unwrapped_transfer,
        ],
    )
    .await?;
    let lease_route = format!("/v1/events?registration_id={registrar}&page_size=20");
    let unwrapped = v2_history_payload_for_database(&database, &lease_route).await?;
    assert_eq!(
        history_types(unwrapped["data"].as_array().expect("unwrapped history data")),
        vec!["transfer", "transfer", "authority", "registration"]
    );

    let permission = v2_history_event(
        "born-wrap-registrar-permission",
        None,
        Some(registrar),
        "PermissionChanged",
        135,
    );
    let mut record = v2_history_event(
        "born-wrap-resource-less-record",
        Some(LOGICAL),
        None,
        "RecordChanged",
        135,
    );
    record.source_family = "ens_v1_resolver_l1".to_owned();
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            wrapper_event("born-wrap-rewrap-binding", rewrapper, "SurfaceBound", 133),
            wrapper_event("born-wrap-rewrap-transfer", rewrapper, "TokenControlTransferred", 134),
            permission,
            record,
        ],
    )
    .await?;
    let history = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/{NAME}/history?scope=registration&page_size=20"),
    )
    .await?;
    let rows = history["data"].as_array().expect("history data");
    let direct = v2_history_payload_for_database(&database, &lease_route).await?;
    let direct_rows = direct["data"].as_array().expect("direct history data");
    let expected_types = vec![
        "record",
        "permission",
        "transfer",
        "transfer",
        "transfer",
        "authority",
        "registration",
    ];
    assert_eq!(
        history_types(direct_rows),
        expected_types,
        "the registrar lease lost registrar, resource-less, or NameWrapper history: {direct_rows:?}"
    );
    let lifecycle_type = |row: &Value| {
        matches!(
            row["type"].as_str(),
            Some("registration" | "transfer" | "authority" | "permission")
        )
    };
    assert!(
        rows.iter()
            .filter(|row| lifecycle_type(row))
            .all(|row| row["registration_id"] == json!(registrar.to_string())),
        "name history split the lease across NameWrapper resources: {rows:?}"
    );
    assert!(
        direct_rows.iter().any(|row| {
            row["block_number"] == json!(134)
                && row["registration_id"] == json!(registrar.to_string())
        }),
        "the lease handle did not follow the re-wrap: {direct_rows:?}"
    );
    for (label, resource) in [("first wrapper", wrapper), ("re-wrapper", rewrapper)] {
        let payload = v2_history_payload_for_database(
            &database,
            &format!("/v1/events?registration_id={resource}&page_size=20"),
        )
        .await?;
        assert!(
            payload["data"].as_array().is_some_and(Vec::is_empty),
            "the {label} resource became a second handle for the lease: {payload:?}"
        );
    }
    let plan = bigname_storage::explain_registration_history_filter_for_test(
        &database.pool,
        registrar,
        LOGICAL,
        "ethereum-mainnet",
        "ens",
        &namehash,
    )
    .await?;
    assert_registration_history_plan_is_page_keyed(&plan);

    // Orphaned wrap evidence must not change what the canonical read serves.
    let orphan_grant = v2_history_event(
        "born-wrap-orphan-grant",
        None,
        Some(registrar),
        "RegistrationGranted",
        136,
    );
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            orphan_grant,
            wrapper_event("born-wrap-orphan-binding", orphan_wrapper, "SurfaceBound", 136),
        ],
    )
    .await?;
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = 'ethereum-mainnet' AND block_hash = '0xhistory136'",
    )
    .execute(&database.pool)
    .await?;
    let canonical = v2_history_payload_for_database(&database, &lease_route).await?;
    let canonical_rows = canonical["data"].as_array().expect("canonical history data");
    assert_eq!(
        history_types(canonical_rows),
        expected_types,
        "orphaned wrap evidence hid canonical registration history: {canonical_rows:?}"
    );
    assert!(
        canonical_rows
            .iter()
            .filter(|row| lifecycle_type(row))
            .all(|row| row["registration_id"] == json!(registrar.to_string())),
        "orphaned wrap evidence changed canonical registration identity: {canonical_rows:?}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn later_wrapped_name_keeps_one_followable_registrar_lifecycle_handle() -> Result<()> {
    const NAME: &str = "later-wrapped-history.eth";
    const SEED_LOGICAL_NAME_ID: &str = "ens:later-wrapped-history.eth";
    const HOLDER: &str = "0x0000000000000000000000000000000000007150";
    let database = TestDatabase::new_migrated().await?;
    let wrapper_resource_id = Uuid::from_u128(0x7150);
    let registrar_resource_id = Uuid::from_u128(0x7151);
    let older_registrar_resource_id = Uuid::from_u128(0x7152);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);

    seed_identity_name(
        &database,
        SEED_LOGICAL_NAME_ID,
        NAME,
        NAME,
        "node:later-wrapped-history.eth",
        wrapper_resource_id,
        Uuid::from_u128(0x8150),
        Uuid::from_u128(0x9150),
        HOLDER,
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[
            address_name_resource(
                registrar_resource_id,
                None,
                "0xregistrar-resource",
                79,
            ),
            address_name_resource(
                older_registrar_resource_id,
                None,
                "0xolder-registrar-resource",
                78,
            ),
        ],
    )
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
                 jsonb_set(
                     declared_summary,
                     '{registration,authority_kind}',
                     '\"wrapper\"'::jsonb,
                     true
                 ),
                 '{registration,resource_id}',
                 to_jsonb($2::text),
                 true
             )
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .bind(registrar_resource_id)
    .execute(&database.pool)
    .await?;
    let projected_registration_id: Option<String> = sqlx::query_scalar(
        "SELECT declared_summary #>> '{registration,resource_id}'
         FROM bigname_phase.name_current
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(projected_registration_id, Some(registrar_resource_id.to_string()));
    // Read the detail before the history blocks move the chain head past the seeded row.
    let exact_name = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/{NAME}"),
    )
    .await?;
    assert_eq!(
        exact_name["data"]["registration_id"],
        json!(registrar_resource_id.to_string()),
        "the exact-name response returned the wrapper resource instead of the registrar lifecycle handle"
    );

    seed_v2_history_blocks(&database, 120..=125).await?;

    // Bind the fixture at its normalized history time, rather than the seed's default date.
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000121) WHERE resource_id = $1",
    )
    .bind(wrapper_resource_id)
    .execute(&database.pool)
    .await?;
    let mut older_registration = v2_history_event(
        "later-wrap-older-unbound-registration",
        None,
        Some(older_registrar_resource_id),
        "RegistrationGranted",
        120,
    );
    older_registration.after_state["namehash"] = json!(&namehash);
    let mut registration = v2_history_event(
        "later-wrap-controller-free-registration",
        None,
        Some(registrar_resource_id),
        "RegistrationGranted",
        121,
    );
    registration.after_state["namehash"] = json!(&namehash);
    let mut wrapper_binding = v2_history_event(
        "later-wrap-binding",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "SurfaceBound",
        122,
    );
    wrapper_binding.source_family = "ens_v1_wrapper_l1".to_owned();
    wrapper_binding.after_state = json!({
        "source_event": "NameWrapped",
        "node": namehash,
        "wrapped_registrar_resource_id": registrar_resource_id,
    });
    let mut wrapper_transfer = v2_history_event(
        "later-wrap-holder-transfer",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "TokenControlTransferred",
        123,
    );
    wrapper_transfer.source_family = "ens_v1_wrapper_l1".to_owned();
    let mut wrapped_controller_renewal = v2_history_event(
        "later-wrap-wrapped-controller-renewal",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "ExpiryChanged",
        124,
    );
    wrapped_controller_renewal.source_family = "ens_v1_registrar_l1".to_owned();
    wrapped_controller_renewal.after_state["source_event"] = json!("NameRenewed");
    let mut wrapper_registry_resolver = v2_history_event(
        "later-wrap-registry-resolver",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "ResolverChanged",
        125,
    );
    wrapper_registry_resolver.source_family = "ens_v1_registry_l1".to_owned();
    wrapper_registry_resolver.after_state["source_event"] = json!("NewResolver");
    let surface_record = v2_history_event(
        "later-wrap-surface-record",
        Some(&logical_name_id),
        None,
        "RecordChanged",
        125,
    );
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            older_registration,
            registration,
            wrapper_binding,
            wrapper_transfer,
            wrapped_controller_renewal,
            wrapper_registry_resolver,
            surface_record,
        ],
    )
    .await?;

    let name_history_route =
        format!("/v1/names/{NAME}/history?scope=registration&page_size=20");
    {
        let payload = v2_history_payload_for_database(&database, &name_history_route).await?;
        let rows = payload["data"].as_array().expect("history data");
        assert!(
            rows.iter().any(|row| {
                row["type"] == json!("registration")
                    && row["registration_id"] == json!(registrar_resource_id.to_string())
            }),
            "{name_history_route} did not return the registrar registration: {rows:?}"
        );
        assert!(
            rows.iter().any(|row| {
                row["type"] == json!("transfer")
                    && row["registration_id"] == json!(registrar_resource_id.to_string())
            }),
            "{name_history_route} did not keep the wrapper holder change on the registrar lifecycle handle: {rows:?}"
        );
        assert!(
            rows.iter().any(|row| {
                row["type"] == json!("expiry")
                    && row["registration_id"] == json!(registrar_resource_id.to_string())
            }),
            "{name_history_route} did not keep the wrapped-controller renewal on the registrar lifecycle handle: {rows:?}"
        );
        assert!(
            rows.iter().any(|row| {
                row["type"] == json!("resolver")
                    && row["registration_id"] == json!(registrar_resource_id.to_string())
            }),
            "{name_history_route} did not keep the wrapper-active registry resolver on the registrar lifecycle handle: {rows:?}"
        );
    }

    let registration_route =
        format!("/v1/events?registration_id={registrar_resource_id}&page_size=20");
    let registration_payload =
        v2_history_payload_for_database(&database, &registration_route).await?;
    let registration_rows = registration_payload["data"]
        .as_array()
        .expect("history data");
    assert!(registration_rows.iter().any(|row| {
        row["type"] == json!("registration")
            && row["registration_id"] == json!(registrar_resource_id.to_string())
    }));
    assert!(registration_rows.iter().any(|row| {
        row["type"] == json!("transfer")
            && row["registration_id"] == json!(registrar_resource_id.to_string())
    }), "the registrar lifecycle filter omitted the later wrapper holder change: {registration_rows:?}");
    assert!(registration_rows.iter().any(|row| {
        row["type"] == json!("expiry")
            && row["registration_id"] == json!(registrar_resource_id.to_string())
    }), "the registrar lifecycle filter omitted the wrapped-controller renewal: {registration_rows:?}");
    assert!(registration_rows.iter().any(|row| {
        row["type"] == json!("resolver")
            && row["registration_id"] == json!(registrar_resource_id.to_string())
    }), "the registrar lifecycle filter omitted the wrapper-active registry resolver: {registration_rows:?}");
    assert!(registration_rows.iter().any(|row| {
        row["type"] == json!("record") && row["registration_id"] == Value::Null
    }), "the registrar lifecycle filter omitted resource-less exact-name history: {registration_rows:?}");
    let plan = bigname_storage::explain_registration_history_filter_for_test(
        &database.pool,
        registrar_resource_id,
        &logical_name_id,
        "ethereum-mainnet",
        "ens",
        &namehash,
    )
    .await?;
    assert!(
        plan.contains("normalized_events_resource_projection_replay_idx")
            || plan.contains("normalized_events_history_resource_lookup_idx")
            || plan.contains("normalized_events_resource_history_idx"),
        "registration history must retain an indexed registrar-resource anchor:\n{plan}"
    );
    assert!(
        plan.contains("normalized_events_name_projection_replay_idx")
            || plan.contains("normalized_events_name_history_idx"),
        "registration history must use the exact-name history index for associated rows:\n{plan}"
    );
    assert!(
        plan.contains("name_surfaces_exact_namehash_projection_idx")
            || plan.contains("name_surfaces_hash_idx")
            || plan.contains("name_surfaces_visibility_idx"),
        "wrapper association must use the exact-namehash surface index:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan on normalized_events"),
        "registration history and association discovery must not scan the complete normalized-event table:\n{plan}"
    );
    assert_registration_history_plan_is_page_keyed(&plan);
    let older_registration_payload = v2_history_payload_for_database(
        &database,
        &format!(
            "/v1/events?registration_id={older_registrar_resource_id}&page_size=20"
        ),
    )
    .await?;
    let older_registration_rows = older_registration_payload["data"]
        .as_array()
        .expect("older registration history");
    assert_eq!(history_types(older_registration_rows), vec!["registration"]);
    assert_eq!(
        older_registration_rows[0]["registration_id"],
        json!(older_registrar_resource_id.to_string())
    );

    let by_name = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?name={NAME}&page_size=20"),
    )
    .await?;
    let by_name_rows = by_name["data"].as_array().expect("history data");
    assert!(by_name_rows.iter().any(|row| {
        row["type"] == json!("registration")
            && row["registration_id"] == json!(registrar_resource_id.to_string())
    }));

    sqlx::query(
        "UPDATE bigname_phase.normalized_events
         SET canonicality_state = 'orphaned'
         WHERE event_identity = 'later-wrap-binding'",
    )
    .execute(&database.pool)
    .await?;
    let after_binding_retraction =
        v2_history_payload_for_database(&database, &registration_route).await?;
    assert_eq!(
        history_types(
            after_binding_retraction["data"]
                .as_array()
                .expect("history after wrapper-binding retraction")
        ),
        vec!["registration"]
    );

    database.cleanup().await
}

#[tokio::test]
async fn name_history_keeps_a_superseded_controller_free_wrapped_registration() -> Result<()> {
    const NAME: &str = "superseded-later-wrap.eth";
    const SEED_LOGICAL_NAME_ID: &str = "ens:superseded-later-wrap.eth";
    const HOLDER: &str = "0x0000000000000000000000000000000000007160";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let prior_wrapper_resource_id = Uuid::from_u128(0x7160);
    let prior_registrar_resource_id = Uuid::from_u128(0x7161);
    let current_registrar_resource_id = Uuid::from_u128(0x7162);

    seed_identity_name(
        &database,
        SEED_LOGICAL_NAME_ID,
        NAME,
        NAME,
        &namehash,
        current_registrar_resource_id,
        Uuid::from_u128(0x8162),
        Uuid::from_u128(0x9162),
        HOLDER,
        bigname_storage::AddressNameRelation::Registrant,
        84,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[
            address_name_resource(
                prior_wrapper_resource_id,
                None,
                "0xprior-wrapper-resource",
                79,
            ),
            address_name_resource(
                prior_registrar_resource_id,
                None,
                "0xprior-registrar-resource",
                78,
            ),
        ],
    )
    .await?;
    let mut prior_wrapper_binding = address_name_surface_binding(
        Uuid::from_u128(0x9160),
        SEED_LOGICAL_NAME_ID,
        prior_wrapper_resource_id,
        "0xprior-wrapper-binding",
        80,
        1_717_171_600,
    );
    prior_wrapper_binding.active_to = Some(timestamp(1_717_171_699));
    upsert_test_surface_bindings(&database.pool, &[prior_wrapper_binding]).await?;
    sqlx::query(
        "UPDATE bigname_phase.name_current
         SET declared_summary = jsonb_set(
             declared_summary,
             '{registration,resource_id}',
             to_jsonb($2::text),
             true
         )
         WHERE logical_name_id = $1",
    )
    .bind(&logical_name_id)
    .bind(current_registrar_resource_id)
    .execute(&database.pool)
    .await?;
    seed_v2_history_blocks(&database, 121..=124).await?;

    let prior_registration = v2_history_event(
        "superseded-later-wrap-registration",
        None,
        Some(prior_registrar_resource_id),
        "RegistrationGranted",
        121,
    );
    let mut prior_wrapper_binding_event = v2_history_event(
        "superseded-later-wrap-binding",
        Some(&logical_name_id),
        Some(prior_wrapper_resource_id),
        "SurfaceBound",
        122,
    );
    prior_wrapper_binding_event.source_family = "ens_v1_wrapper_l1".to_owned();
    prior_wrapper_binding_event.after_state = json!({
        "source_event": "NameWrapped",
        "node": namehash,
        "wrapped_registrar_resource_id": prior_registrar_resource_id,
    });
    let prior_release = v2_history_event(
        "superseded-later-wrap-release",
        None,
        Some(prior_registrar_resource_id),
        "RegistrationReleased",
        123,
    );
    let current_registration = v2_history_event(
        "superseded-current-registration",
        Some(&logical_name_id),
        Some(current_registrar_resource_id),
        "RegistrationGranted",
        124,
    );
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            prior_registration,
            prior_wrapper_binding_event,
            prior_release,
            current_registration,
        ],
    )
    .await?;

    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/{NAME}/history?scope=registration&page_size=20"),
    )
    .await?;
    let rows = payload["data"].as_array().expect("history data");
    assert!(
        rows.iter().any(|row| {
            row["type"] == json!("registration")
                && row["registration_id"] == json!(prior_registrar_resource_id.to_string())
        }),
        "name-scoped history forgot the superseded controller-free registrar lifecycle: {rows:?}"
    );
    assert!(rows.iter().any(|row| {
        row["type"] == json!("registration")
            && row["registration_id"] == json!(current_registrar_resource_id.to_string())
    }));

    let event_payload = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?name={NAME}&page_size=20"),
    )
    .await?;
    let event_rows = event_payload["data"].as_array().expect("event data");
    assert!(
        event_rows.iter().any(|row| {
            row["type"] == json!("registration")
                && row["registration_id"] == json!(prior_registrar_resource_id.to_string())
        }),
        "name-filtered events forgot the superseded controller-free registrar lifecycle: {event_rows:?}"
    );

    database.cleanup().await
}

#[tokio::test]
async fn wrapper_without_prior_registrar_keeps_name_history_readable() -> Result<()> {
    const NAME: &str = "wrapper-without-registrar.eth";
    const SEED_LOGICAL_NAME_ID: &str = "ens:wrapper-without-registrar.eth";
    const HOLDER: &str = "0x0000000000000000000000000000000000007170";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let wrapper_resource_id = Uuid::from_u128(0x7170);

    seed_identity_name(
        &database,
        SEED_LOGICAL_NAME_ID,
        NAME,
        NAME,
        &namehash,
        wrapper_resource_id,
        Uuid::from_u128(0x8170),
        Uuid::from_u128(0x9170),
        HOLDER,
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    seed_v2_history_blocks(&database, 121..=121).await?;
    let mut wrapper_binding_event = v2_history_event(
        "wrapper-without-prior-registrar-binding",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "SurfaceBound",
        121,
    );
    wrapper_binding_event.source_family = "ens_v1_wrapper_l1".to_owned();
    wrapper_binding_event.after_state = json!({
        "source_event": "NameWrapped",
        "node": namehash,
        "wrapped_registrar_resource_id": null,
    });
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[wrapper_binding_event],
    )
    .await?;

    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v1/names/{NAME}/history?scope=registration&page_size=20"),
    )
    .await?;
    assert_eq!(payload["data"], json!([]));

    database.cleanup().await
}

#[tokio::test]
async fn wrapped_subname_keeps_its_wrapper_registration_handle() -> Result<()> {
    const NAME: &str = "sub.wrapped-parent.eth";
    const SEED_LOGICAL_NAME_ID: &str = "ens:sub.wrapped-parent.eth";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let wrapper = Uuid::from_u128(0x7175);
    seed_identity_name(
        &database,
        SEED_LOGICAL_NAME_ID,
        NAME,
        NAME,
        &namehash,
        wrapper,
        Uuid::from_u128(0x8175),
        Uuid::from_u128(0x9175),
        "0x0000000000000000000000000000000000007175",
        bigname_storage::AddressNameRelation::TokenHolder,
        80,
    )
    .await?;
    let detail =
        v2_history_payload_for_database(&database, &format!("/v1/names/{NAME}")).await?;
    assert_eq!(detail["data"]["registration_id"], json!(wrapper.to_string()));
    seed_v2_history_blocks(&database, 121..=123).await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000121) WHERE resource_id = $1",
    )
    .bind(wrapper)
    .execute(&database.pool)
    .await?;
    // A wrapped subname has no BaseRegistrar lease, so NameWrapped records no link.
    let mut binding = v2_history_event(
        "wrapped-subname-binding",
        Some(&logical_name_id),
        Some(wrapper),
        "SurfaceBound",
        121,
    );
    binding.source_family = "ens_v1_wrapper_l1".to_owned();
    binding.after_state = json!({
        "source_event": "NameWrapped",
        "node": namehash,
        "wrapped_registrar_resource_id": null,
    });
    let mut transfer = v2_history_event(
        "wrapped-subname-transfer",
        Some(&logical_name_id),
        Some(wrapper),
        "TokenControlTransferred",
        122,
    );
    transfer.source_family = "ens_v1_wrapper_l1".to_owned();
    let mut record = v2_history_event(
        "wrapped-subname-record",
        Some(&logical_name_id),
        None,
        "RecordChanged",
        123,
    );
    record.source_family = "ens_v1_resolver_l1".to_owned();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[binding, transfer, record])
        .await?;

    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={wrapper}&page_size=20"),
    )
    .await?;
    let rows = payload["data"].as_array().expect("wrapped subname history");
    assert_eq!(history_types(rows), vec!["record", "transfer"], "{rows:?}");
    assert_eq!(rows[0]["registration_id"], Value::Null);
    assert_eq!(rows[1]["registration_id"], json!(wrapper.to_string()));

    database.cleanup().await
}

fn assert_registration_history_plan_is_page_keyed(plan: &str) {
    let history_plan = plan
        .split_once("history page:\n")
        .map(|(_, history_plan)| history_plan)
        .expect("combined plan must contain the history page section");
    let lines = history_plan.lines().collect::<Vec<_>>();
    let scans = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line.contains(" on normalized_events "))
        .filter_map(|(index, node)| {
            lines[index + 1..]
                .iter()
                .take_while(|line| !line.trim_start().starts_with("->"))
                .find(|line| line.trim_start().starts_with("Index Cond:"))
                .map(|condition| (*node, *condition))
        })
        .collect::<Vec<_>>();

    assert!(
        !scans.iter().any(|(_, condition)| {
            condition.contains("chain_id =") && !condition.contains(" AND ")
        }),
        "no normalized-events history scan may be keyed only by chain_id:\n{history_plan}"
    );
    assert!(
        !scans.iter().any(|(_, condition)| {
            condition.contains("event_kind =") && !condition.contains(" AND ")
        }),
        "no normalized-events history scan may be keyed only by event_kind:\n{history_plan}"
    );

    let wrapper_scans = scans
        .iter()
        .filter(|(node, _)| node.contains(" wrapper_binding"))
        .collect::<Vec<_>>();
    assert!(
        !wrapper_scans.is_empty()
            && wrapper_scans
                .iter()
                .all(|(_, condition)| condition.contains("resource_id =")),
        "every wrapper_binding scan must be keyed by resource_id:\n{history_plan}"
    );
}

#[tokio::test]
async fn noncanonical_registration_history_keeps_resource_less_events_on_their_fork() -> Result<()>
{
    const NAME: &str = "fork-registration-history.eth";
    const SEED: &str = "ens:fork-registration-history.eth";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let registration_a = Uuid::from_u128(0x7170);
    let registration_b = Uuid::from_u128(0x7171);
    seed_identity_name(
        &database,
        SEED,
        NAME,
        NAME,
        "node:fork-registration-history.eth",
        registration_a,
        Uuid::from_u128(0x8170),
        Uuid::from_u128(0x9170),
        "0x0000000000000000000000000000000000007170",
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            registration_b,
            None,
            "0xfork-resource-b",
            79,
        )],
    )
    .await?;
    let mut blocks = vec![raw_block(
        "ethereum-mainnet",
        "0xfork-common",
        None,
        129,
        1_700_000_129,
    )];
    for branch in ["a", "b"] {
        for number in 130..=131 {
            let parent = if number == 130 {
                "0xfork-common".to_owned()
            } else {
                format!("0xfork-{branch}-130")
            };
            let mut block = raw_block(
                "ethereum-mainnet",
                &format!("0xfork-{branch}-{number}"),
                Some(&parent),
                number,
                1_700_000_000 + number,
            );
            if branch == "b" {
                block.canonicality_state = CanonicalityState::Orphaned;
            }
            blocks.push(block);
        }
    }
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    // Both active intervals cover exactly the same wall-clock times. Fork hashes
    // are the only valid discriminator for the resource-less records below.
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
        SET block_hash = '0xfork-a-130', block_number = 130,
            active_from = to_timestamp(1700000130), active_to = NULL
        WHERE resource_id = $1",
    )
    .bind(registration_a)
    .execute(&database.pool)
    .await?;
    let mut binding_b = address_name_surface_binding(
        Uuid::from_u128(0x9171),
        SEED,
        registration_b,
        "0xfork-b-130",
        130,
        1_700_000_130,
    );
    binding_b.canonicality_state = CanonicalityState::Orphaned;
    upsert_test_surface_bindings(&database.pool, &[binding_b]).await?;
    let mut events = Vec::new();
    for (branch, resource, state) in [
        ("a", registration_a, CanonicalityState::Canonical),
        ("b", registration_b, CanonicalityState::Orphaned),
    ] {
        for (suffix, kind, number, resource_id) in [
            ("grant", "RegistrationGranted", 130, Some(resource)),
            ("record", "RecordChanged", 131, None),
        ] {
            let mut event = v2_history_event(
                &format!("fork-{branch}-{suffix}"),
                Some(&logical_name_id),
                resource_id,
                kind,
                number,
            );
            event.block_hash = Some(format!("0xfork-{branch}-{number}"));
            event.transaction_hash = Some(format!("0xfork-{branch}-tx-{number}"));
            event.canonicality_state = state;
            if resource_id.is_none() {
                event.source_family = "ens_v1_resolver_l1".to_owned();
            }
            events.push(event);
        }
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    // Repeat with both branches orphaned, so a canonicality-state equality
    // substitute cannot satisfy the regression.
    for both_orphaned in [false, true] {
        if both_orphaned {
            for table in ["normalized_events", "surface_bindings", "chain_lineage"] {
                sqlx::query(&format!("UPDATE bigname_phase.{table}
                    SET canonicality_state = 'orphaned'
                    WHERE chain_id = 'ethereum-mainnet' AND block_hash IN ('0xfork-a-130', '0xfork-a-131')"))
                    .execute(&database.pool).await?;
            }
        }
        let name_rows = bigname_storage::load_name_history(
            &database.pool,
            &logical_name_id,
            &[registration_a, registration_b],
            bigname_storage::HistoryScope::Both,
            false,
        )
        .await?;
        assert_eq!(name_rows.len(), 4, "{name_rows:?}");
        for (branch, resource, other_resource) in [
            ("a", registration_a, registration_b),
            ("b", registration_b, registration_a),
        ] {
            let filter = bigname_storage::EventHistoryFilter {
                resource_id: Some(resource),
                ..Default::default()
            };
            let rows =
                bigname_storage::load_event_history(&database.pool, filter.clone(), false).await?;
            assert_eq!(
                rows.iter()
                    .map(|row| row.event_identity.as_str())
                    .collect::<Vec<_>>(),
                vec![
                    format!("fork-{branch}-record"),
                    format!("fork-{branch}-grant")
                ]
            );
            assert_eq!(rows[0].registration_id, None);
            assert_eq!(rows[1].registration_id, Some(resource));
            let canonical =
                bigname_storage::load_event_history(&database.pool, filter.clone(), true).await?;
            assert_eq!(
                canonical.len(),
                if branch == "a" && !both_orphaned {
                    2
                } else {
                    0
                }
            );
            for summary_mode in [
                bigname_storage::HistorySummaryMode::Count,
                bigname_storage::HistorySummaryMode::Full,
            ] {
                let page = bigname_storage::load_event_history_page(
                    &database.pool,
                    filter.clone(),
                    false,
                    None,
                    1,
                    summary_mode,
                    false,
                )
                .await?;
                assert_eq!(page.rows.len(), 1);
                assert_eq!(page.rows[0].event_identity, format!("fork-{branch}-record"));
                assert_eq!(page.summary.as_ref().unwrap().total_count, 2);
                let cursor = page.next_cursor.as_ref().expect("same-fork grant remains");
                let next = bigname_storage::load_event_history_page(
                    &database.pool,
                    filter.clone(),
                    false,
                    Some(cursor),
                    1,
                    summary_mode,
                    false,
                )
                .await?;
                assert_eq!(next.rows.len(), 1);
                assert_eq!(next.rows[0].event_identity, format!("fork-{branch}-grant"));
                assert!(next.next_cursor.is_none());
                let error = bigname_storage::load_event_history_page(
                    &database.pool,
                    bigname_storage::EventHistoryFilter {
                        resource_id: Some(other_resource),
                        ..Default::default()
                    },
                    false,
                    Some(cursor),
                    1,
                    summary_mode,
                    false,
                )
                .await
                .expect_err("another fork's record must not anchor this registration");
                assert!(
                    error
                        .downcast_ref::<bigname_storage::InvalidHistoryCursor>()
                        .is_some()
                );
            }
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn pre_enrichment_registration_handle_requires_token_lineage() -> Result<()> {
    const NAME: &str = "pre-enrichment-token-history.eth";
    const SEED: &str = "ens:pre-enrichment-token-history.eth";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let registrar = Uuid::from_u128(0x7180);
    let registry = Uuid::from_u128(0x7181);
    seed_identity_name(
        &database,
        SEED,
        NAME,
        NAME,
        "node:pre-enrichment-token-history.eth",
        registrar,
        Uuid::from_u128(0x8180),
        Uuid::from_u128(0x9180),
        "0x0000000000000000000000000000000000007180",
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(registry, None, "0xresource", 99)],
    )
    .await?;
    seed_v2_history_blocks(&database, 120..=121).await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
        SET block_hash = '0xhistory120', block_number = 120,
            active_from = to_timestamp(1700000120), active_to = to_timestamp(1700000121)
        WHERE resource_id = $1",
    )
    .bind(registrar)
    .execute(&database.pool)
    .await?;
    upsert_test_surface_bindings(
        &database.pool,
        &[address_name_surface_binding(
            Uuid::from_u128(0x9181),
            SEED,
            registry,
            "0xhistory121",
            121,
            1_700_000_121,
        )],
    )
    .await?;
    let mut events = Vec::new();
    for (suffix, resource, block_number) in
        [("registrar", registrar, 120), ("registry", registry, 121)]
    {
        let mut event = v2_history_event(
            &format!("pre-enrichment-token-{suffix}"),
            None,
            Some(resource),
            "ResolverChanged",
            block_number,
        );
        event.source_family = "ens_v1_registry_l1".to_owned();
        event.after_state = json!({"source_event": "NewResolver",
            "node": "node:pre-enrichment-token-history.eth",
            "authority_kind": if resource == registrar { "registrar" } else { "registry_only" },
            "resolver": "0x00000000000000000000000000000000000000aa"});
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    // Each event lies inside its resource's non-overlapping canonical binding
    // epoch, so both rows retain identity. The public-handle check distinguishes token lineage.
    let name_rows = bigname_storage::load_name_history(
        &database.pool,
        &logical_name_id,
        &[registrar, registry],
        bigname_storage::HistoryScope::Both,
        true,
    )
    .await?;
    assert_eq!(name_rows.len(), 2, "{name_rows:?}");
    for resource in [registrar, registry] {
        assert!(
            name_rows
                .iter()
                .any(|row| row.registration_id == Some(resource)),
            "the identity mapper must not hide the handle eligibility regression: {name_rows:?}"
        );
    }
    // There is no grant, wrapper event or event-level name attribution for either
    // resource. Accepting arbitrary bound resources fails the negative assertion.
    for (resource, count) in [(registrar, 1), (registry, 0)] {
        let payload = v2_history_payload_for_database(
            &database,
            &format!("/v1/events?registration_id={resource}&page_size=20"),
        )
        .await?;
        let rows = payload["data"]
            .as_array()
            .expect("registration history rows");
        assert_eq!(rows.len(), count, "{resource}: {payload:?}");
        assert_eq!(payload["page"]["has_more"], json!(false));
        if resource == registrar {
            assert_eq!(rows[0]["type"], json!("resolver"));
            assert_eq!(rows[0]["registration_id"], json!(registrar.to_string()));
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn reserved_token_resource_is_not_a_public_registration_handle() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource = Uuid::from_u128(0x7190);
    let token = Uuid::from_u128(0x8190);
    seed_v2_history_blocks(&database, 120..=122).await?;
    // No name is seeded here, so publish a readable ENS position for the collection routes.
    database.seed_default_ens_snapshot_selector_position().await?;
    upsert_test_token_lineages(
        &database.pool,
        &[address_name_token_lineage(token, "0xhistory120", 120)],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            resource,
            Some(token),
            "0xhistory120",
            120,
        )],
    )
    .await?;
    // Match the reservation producer: token identity, no binding or grant, then
    // resource-bearing expiry and resolver updates while still reserved.
    let mut events = Vec::new();
    for (kind, source_event, number) in [
        ("RegistrationReserved", "LabelReserved", 120),
        ("ExpiryChanged", "ExpiryUpdated", 121),
        ("ResolverChanged", "ResolverUpdated", 122),
    ] {
        let mut event = v2_history_event(
            &format!("reserved-handle-{number}"),
            None,
            Some(resource),
            kind,
            number,
        );
        event.source_family = "ens_v2_registry_l1".to_owned();
        event.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
        event.after_state["source_event"] = json!(source_event);
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    for canonical_only in [true, false] {
        let rows = bigname_storage::load_event_history(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                resource_id: Some(resource),
                ..Default::default()
            },
            canonical_only,
        )
        .await?;
        assert!(rows.is_empty(), "reservation acquired a handle: {rows:?}");
        let payload = v2_history_payload_for_database(
            &database,
            &format!("/v1/events?registration_id={resource}&page_size=1"),
        )
        .await?;
        assert_eq!(payload["data"], json!([]), "{payload:?}");
        assert_eq!(payload["page"]["has_more"], json!(false));
    }
    let diagnostics = v2_history_payload_for_database(
        &database,
        &format!("/v1/diagnostics/events?registration_id={resource}&page_size=20"),
    )
    .await?;
    assert_eq!(
        diagnostics["data"]
            .as_array()
            .expect("diagnostic rows")
            .len(),
        3
    );
    database.cleanup().await
}

#[tokio::test]
async fn reservation_product_rows_omit_registration_identity() -> Result<()> {
    const NAME: &str = "reserved-product-history.eth";
    const SEED: &str = "ens:reserved-product-history.eth";
    let database = TestDatabase::new_migrated().await?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);
    let reservation = Uuid::from_u128(0x71a0);
    let registration = reservation;
    let blocks = (120..=125)
        .map(|number| {
            raw_block(
                "ethereum-mainnet",
                &format!("0xhistory{number}"),
                (number > 120)
                    .then(|| format!("0xhistory{}", number - 1))
                    .as_deref(),
                number,
                1_700_000_000 + number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    seed_identity_name(
        &database,
        SEED,
        NAME,
        NAME,
        "node:reserved-product-history.eth",
        registration,
        Uuid::from_u128(0x81a0),
        Uuid::from_u128(0x91a1),
        "0x00000000000000000000000000000000000071a1",
        bigname_storage::AddressNameRelation::EffectiveController,
        120,
    )
    .await?;
    // The seeded name published block 120; publish the rest of the range.
    seed_schema_v2_ens_lookup_head(&database.pool, 125, "0xhistory125", "2023-11-14T22:15:25Z")
        .await?;
    upsert_test_token_lineages(
        &database.pool,
        &[address_name_token_lineage(
            Uuid::from_u128(0x81a0),
            "0xhistory120",
            120,
        )],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            reservation,
            Some(Uuid::from_u128(0x81a0)),
            "0xhistory120",
            120,
        )],
    )
    .await?;
    let mut events = Vec::new();
    // A claim can retain the reservation resource; earlier rows must stay unregistered.
    for (resource, kind, source_event, number) in [
        (reservation, "RegistrationReserved", "LabelReserved", 120),
        (reservation, "ExpiryChanged", "ExpiryUpdated", 121),
        (reservation, "ResolverChanged", "ResolverUpdated", 122),
        (registration, "RegistrationGranted", "LabelRegistered", 123),
        (registration, "ExpiryChanged", "ExpiryUpdated", 124),
        (registration, "ResolverChanged", "ResolverUpdated", 125),
    ] {
        let mut event = v2_history_event(
            &format!("reservation-product-{number}"),
            Some(&logical_name_id),
            Some(resource),
            kind,
            number,
        );
        event.source_family = "ens_v2_registry_l1".to_owned();
        event.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
        event.after_state["source_event"] = json!(source_event);
        if kind == "RegistrationGranted" {
            event.after_state["authority_kind"] = json!("ens_v2_registry");
        }
        events.push(event);
    }
    for number in [122, 125] {
        let mut bridge = v2_history_event(
            &format!("reservation-product-bridge-{number}"),
            Some(&logical_name_id),
            Some(registration),
            "RegistrationRenewed",
            number,
        );
        bridge.source_family = "ens_v2_migration_l1".to_owned();
        bridge.derivation_kind = "ens_v2_migration".to_owned();
        bridge.log_index = Some(1);
        bridge.after_state = json!({"source_event":"NameRenewed","duration":100});
        events.push(bridge);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    sqlx::query("UPDATE bigname_phase.normalized_events SET derivation_kind = 'ens_v2_migration' WHERE event_identity LIKE 'reservation-product-bridge-%'")
        .execute(&database.pool).await?;
    for canonical_only in [true, false] {
        let rows = bigname_storage::load_event_history(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                logical_name_id: Some(logical_name_id.clone()),
                event_kinds: vec![
                    "RegistrationGranted".to_owned(),
                    "RegistrationRenewed".to_owned(),
                    "ExpiryChanged".to_owned(),
                    "ResolverChanged".to_owned(),
                ],
                ..Default::default()
            },
            canonical_only,
        )
        .await?;
        assert_eq!(rows.len(), 7, "canonical_only={canonical_only}: {rows:?}");
        for row in rows {
            let expected = (row.block_number.expect("block number") >= 123).then_some(registration);
            assert_eq!(row.registration_id, expected, "{row:?}");
        }
    }
    for route in [
        format!("/v1/names/{NAME}/history?scope=name&page_size=20"),
        format!("/v1/names/{NAME}/history?scope=both&page_size=20"),
        format!("/v1/events?name={NAME}&page_size=20"),
        "/v1/events?page_size=20".to_owned(),
    ] {
        let payload = v2_history_payload_for_database(&database, &route).await?;
        let rows = payload["data"].as_array().expect("product history rows");
        assert_eq!(rows.len(), 7, "{route}: {payload:?}");
        for row in rows {
            let number = row["block_number"].as_i64().expect("block number");
            let expected = if number < 123 {
                Value::Null
            } else {
                json!(registration)
            };
            assert_eq!(row["registration_id"], expected, "{route}: {row:?}");
        }
    }
    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={registration}&page_size=20"),
    )
    .await?;
    assert_eq!(
        payload["data"]
            .as_array()
            .expect("registration history")
            .len(),
        4
    );
    let diagnostics = v2_history_payload_for_database(
        &database,
        &format!("/v1/diagnostics/events?registration_id={reservation}&page_size=20"),
    )
    .await?;
    let rows = diagnostics["data"].as_array().expect("diagnostic rows");
    assert_eq!(rows.len(), 8);
    assert!(
        rows.iter()
            .all(|row| row["registration_id"] == json!(reservation))
    );
    database.cleanup().await
}

#[tokio::test]
async fn retained_registrar_resolver_survives_resource_reanchoring() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resource = Uuid::from_u128(0x7191);
    let token = Uuid::from_u128(0x8191);
    let mut blocks = vec![raw_block(
        "ethereum-mainnet",
        "0xreanchor-common",
        None,
        129,
        1_700_000_129,
    )];
    for branch in ["a", "b"] {
        for number in 130..=131 {
            let parent = if number == 130 {
                "0xreanchor-common".to_owned()
            } else {
                format!("0xreanchor-{branch}-130")
            };
            let mut block = raw_block(
                "ethereum-mainnet",
                &format!("0xreanchor-{branch}-{number}"),
                Some(&parent),
                number,
                1_700_000_000 + number,
            );
            if branch == "a" {
                block.canonicality_state = CanonicalityState::Orphaned;
            }
            blocks.push(block);
        }
    }
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    // No name is seeded here, so publish a readable ENS position for the collection routes.
    database.seed_default_ens_snapshot_selector_position().await?;
    upsert_test_token_lineages(
        &database.pool,
        &[address_name_token_lineage(token, "0xreanchor-a-130", 130)],
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[address_name_resource(
            resource,
            Some(token),
            "0xreanchor-a-130",
            130,
        )],
    )
    .await?;
    let mut events = Vec::new();
    for branch in ["a", "b"] {
        let mut event = v2_history_event(
            &format!("reanchor-{branch}"),
            None,
            Some(resource),
            "ResolverChanged",
            131,
        );
        event.source_family = "ens_v1_registry_l1".to_owned();
        event.block_hash = Some(format!("0xreanchor-{branch}-131"));
        event.transaction_hash = Some(format!("0xreanchor-{branch}-tx"));
        event.after_state["source_event"] = json!("NewResolver");
        event.after_state["authority_kind"] = json!("registrar");
        if branch == "a" {
            event.canonicality_state = CanonicalityState::Orphaned;
        }
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    // Reproduce the identity writer's post-reorg result. The normalized event
    // on A remains, while both singleton identity anchors now refer to B.
    sqlx::query(
        "UPDATE bigname_phase.resources SET block_hash = '0xreanchor-b-130',
         block_number = 130, canonicality_state = 'canonical' WHERE resource_id = $1",
    )
    .bind(resource)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.token_lineages SET block_hash = '0xreanchor-b-130',
         block_number = 130, canonicality_state = 'canonical' WHERE token_lineage_id = $1",
    )
    .bind(token)
    .execute(&database.pool)
    .await?;
    for (canonical_only, count) in [(true, 1), (false, 2)] {
        let rows = bigname_storage::load_event_history(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                resource_id: Some(resource),
                ..Default::default()
            },
            canonical_only,
        )
        .await?;
        assert_eq!(rows.len(), count, "retained branch history: {rows:?}");
        for summary_mode in [
            bigname_storage::HistorySummaryMode::Count,
            bigname_storage::HistorySummaryMode::Full,
        ] {
            let page = bigname_storage::load_event_history_page(
                &database.pool,
                bigname_storage::EventHistoryFilter {
                    resource_id: Some(resource),
                    ..Default::default()
                },
                canonical_only,
                None,
                1,
                summary_mode,
                false,
            )
            .await?;
            assert_eq!(
                page.summary.as_ref().expect("summary").total_count,
                count as u64
            );
            assert_eq!(page.rows.len(), 1);
            if canonical_only {
                assert_eq!(page.rows[0].event_identity, "reanchor-b");
                assert!(page.next_cursor.is_none());
            } else {
                let next = bigname_storage::load_event_history_page(
                    &database.pool,
                    bigname_storage::EventHistoryFilter {
                        resource_id: Some(resource),
                        ..Default::default()
                    },
                    false,
                    page.next_cursor.as_ref(),
                    1,
                    summary_mode,
                    false,
                )
                .await?;
                assert!(page.next_cursor.is_some());
                assert_eq!(next.rows.len(), 1);
                let mut identities = vec![
                    page.rows[0].event_identity.as_str(),
                    next.rows[0].event_identity.as_str(),
                ];
                identities.sort();
                assert_eq!(identities, vec!["reanchor-a", "reanchor-b"]);
                assert!(next.next_cursor.is_none());
            }
        }
        if canonical_only {
            let payload = v2_history_payload_for_database(
                &database,
                &format!("/v1/events?registration_id={resource}&page_size=1"),
            )
            .await?;
            assert_eq!(
                history_transaction_hashes(&payload),
                vec!["0xreanchor-b-tx"]
            );
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn block_only_reservation_release_omits_registration_identity() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let reserved = Uuid::from_u128(0x71b0);
    let registered = Uuid::from_u128(0x71b1);
    let current = raw_block(
        "ethereum-mainnet",
        "0xhistory120",
        Some("0xhistory119"),
        120,
        1_700_000_120,
    );
    upsert_phase_raw_blocks(
        &database.pool,
        &[
            raw_block("ethereum-mainnet", "0xhistory119", None, 119, 1_700_000_119),
            current,
        ],
    )
    .await?;
    // No name is seeded here, so publish a readable ENS position for the collection routes.
    database.seed_default_ens_snapshot_selector_position().await?;
    for (resource, token, number) in [
        (reserved, Uuid::from_u128(0x81b0), 120),
        (registered, Uuid::from_u128(0x81b1), 119),
    ] {
        upsert_test_token_lineages(
            &database.pool,
            &[address_name_token_lineage(
                token,
                &format!("0xhistory{number}"),
                number,
            )],
        )
        .await?;
        upsert_test_resources(
            &database.pool,
            &[address_name_resource(
                resource,
                Some(token),
                &format!("0xhistory{number}"),
                number,
            )],
        )
        .await?;
    }
    let mut events = Vec::new();
    for (resource, kind, source_event, number) in [
        (reserved, "RegistrationReserved", "LabelReserved", 120),
        (registered, "RegistrationGranted", "LabelRegistered", 119),
    ] {
        let mut event = v2_history_event(
            &format!("block-only-origin-{resource}"),
            None,
            Some(resource),
            kind,
            number,
        );
        event.source_family = "ens_v2_registry_l1".to_owned();
        event.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
        event.log_index = Some(7);
        event.after_state["source_event"] = json!(source_event);
        events.push(event);
    }
    for (resource, status) in [(reserved, "reserved"), (registered, "registered")] {
        let mut release = v2_history_event(
            &format!("block-only-release-{resource}"),
            None,
            Some(resource),
            "RegistrationReleased",
            120,
        );
        release.source_family = "ens_v2_registry_l1".to_owned();
        release.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
        release.log_index = None;
        release.transaction_hash = None;
        release.raw_fact_ref = json!({"kind":"raw_block","chain_id":"ethereum-mainnet","block_hash":"0xhistory120","block_number":120});
        release.before_state = json!({"status":status,"expiry":1_700_000_120});
        release.after_state = json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","status":"released","expiry":1_700_000_120});
        events.push(release);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    for canonical_only in [true, false] {
        let rows = bigname_storage::load_event_history(
            &database.pool,
            bigname_storage::EventHistoryFilter {
                event_kinds: vec!["RegistrationReleased".to_owned()],
                ..Default::default()
            },
            canonical_only,
        )
        .await?;
        assert_eq!(rows.len(), 2, "{rows:?}");
        for row in rows {
            let expected = row
                .event_identity
                .ends_with(&registered.to_string())
                .then_some(registered);
            assert_eq!(row.registration_id, expected, "{row:?}");
            assert!(row.log_index.is_none());
        }
    }
    let payload = v2_history_payload_for_database(&database, "/v1/events?page_size=20").await?;
    let rows = payload["data"].as_array().expect("product rows");
    let releases = rows
        .iter()
        .filter(|row| row["type"] == "release")
        .collect::<Vec<_>>();
    assert_eq!(releases.len(), 2, "{payload:?}");
    assert_eq!(
        releases
            .iter()
            .filter(|row| row["registration_id"].is_null())
            .count(),
        1,
        "{payload:?}"
    );
    assert_eq!(
        releases
            .iter()
            .filter(|row| row["registration_id"] == json!(registered))
            .count(),
        1,
        "{payload:?}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_history_ignores_an_unpublished_name_wrapped_link() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    let lease = Uuid::from_u128(0xf7200);
    let wrapper = Uuid::from_u128(0xf7201);
    upsert_test_resources(
        &database.pool,
        &[
            address_name_resource(lease, None, "0xhistory101", 101),
            address_name_resource(wrapper, None, "0xhistory101", 101),
        ],
    )
    .await?;
    // A published lease row that only a NameWrapped link can attach to the name.
    let grant = v2_history_event(
        "unpublished-link-lease-grant",
        None,
        Some(lease),
        "RegistrationGranted",
        106,
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    let routes = [
        "/v1/names/history.eth/history?page_size=1&include=total_count",
        "/v1/events?name=history.eth&page_size=1&include=total_count",
    ];
    let mut before = Vec::new();
    for route in routes {
        let page = v2_history_payload_for_database(&database, route).await?;
        before.push(page["page"]["total_count"].clone());
    }

    let wrapped = |identity: &str, block_number: i64, block_hash: &str| {
        let mut binding = v2_history_event(
            identity,
            Some("ens:history.eth"),
            Some(wrapper),
            "SurfaceBound",
            block_number,
        );
        binding.block_hash = Some(block_hash.to_owned());
        binding.source_family = "ens_v1_wrapper_l1".to_owned();
        binding.after_state = json!({
            "source_event": "NameWrapped",
            "wrapped_registrar_resource_id": lease,
        });
        binding
    };
    upsert_phase_raw_blocks(
        &database.pool,
        &[raw_block(
            "ethereum-mainnet",
            "0xfuture-wrap",
            None,
            21_000_004,
            1_776_384_004,
        )],
    )
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[wrapped("unpublished-name-wrapped", 21_000_004, "0xfuture-wrap")],
    )
    .await?;
    for (route, before) in routes.iter().zip(&before) {
        let page = v2_history_payload_for_database(&database, route).await?;
        assert_eq!(
            &page["page"]["total_count"], before,
            "{route}: an unpublished NameWrapped link attached lease history"
        );
    }

    // The same link at a published block attaches the lease's grant.
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[wrapped("published-name-wrapped", 107, "0xhistory107")],
    )
    .await?;
    for (route, before) in routes.iter().zip(&before) {
        let page = v2_history_payload_for_database(&database, route).await?;
        assert_eq!(
            page["page"]["total_count"].as_u64(),
            before.as_u64().map(|count| count + 1),
            "{route}: a published NameWrapped link must attach the lease's grant"
        );
    }
    database.cleanup().await
}

// A name's older and later BaseRegistrar leases are separate registrations. A node-keyed
// record write that only the later lease's resolver pointer attributes is not part of the older
// lease's history, its count, or its cursor anchors, while a write the pointer of the NameWrapper
// resource whose `NameWrapped` row recorded the older lease attributes is.
#[tokio::test]
async fn registration_history_excludes_record_writes_attributed_to_another_lease() -> Result<()> {
    const NAME: &str = "two-leases.eth";
    const SEED_LOGICAL_NAME_ID: &str = "ens:two-leases.eth";
    const HOLDER: &str = "0x0000000000000000000000000000000000007160";
    // The wrapped era selected one resolver and the later lease another.
    const WRAPPED_RESOLVER: &str = "0x00000000000000000000000000000000000000c3";
    const LATER_RESOLVER: &str = "0x00000000000000000000000000000000000000c4";
    let database = TestDatabase::new_migrated().await?;
    // The name is currently bound to its later, unwrapped lease.
    let later_lease_id = Uuid::from_u128(0x7160);
    // Its older lease was held through the NameWrapper, whose binding has ended.
    let older_lease_id = Uuid::from_u128(0x7161);
    let wrapper_resource_id = Uuid::from_u128(0x7162);
    let namehash = bigname_lookup::ens_namehash_hex(NAME)?;
    let logical_name_id = bigname_storage::logical_name_id_for_name("ens", NAME);

    seed_identity_name(
        &database,
        SEED_LOGICAL_NAME_ID,
        NAME,
        NAME,
        "node:two-leases.eth",
        later_lease_id,
        Uuid::from_u128(0x8160),
        Uuid::from_u128(0x9160),
        HOLDER,
        bigname_storage::AddressNameRelation::Registrant,
        80,
    )
    .await?;
    upsert_test_resources(
        &database.pool,
        &[
            address_name_resource(older_lease_id, None, "0xolder-lease-resource", 78),
            address_name_resource(wrapper_resource_id, None, "0xwrapper-resource", 79),
        ],
    )
    .await?;
    seed_v2_history_blocks(&database, 120..=125).await?;
    // The NameWrapper's ended binding makes it one of the name's resources.
    let mut ended_wrapper_binding = surface_binding(
        Uuid::from_u128(0x9161),
        SEED_LOGICAL_NAME_ID,
        wrapper_resource_id,
        timestamp(1_700_000_121),
    );
    ended_wrapper_binding.active_to = Some(timestamp(1_700_000_123));
    upsert_test_surface_bindings(&database.pool, &[ended_wrapper_binding]).await?;
    sqlx::query(
        "UPDATE bigname_phase.surface_bindings
         SET active_from = to_timestamp(1700000124) WHERE resource_id = $1",
    )
    .bind(later_lease_id)
    .execute(&database.pool)
    .await?;

    let node_write = |event_identity: &str, block_number: i64, resolver: &str| {
        let mut event = v2_history_event(event_identity, None, None, "RecordChanged", block_number);
        event.source_family = "ens_v1_resolver_l1".to_owned();
        event.raw_fact_ref["emitting_address"] = json!(resolver);
        event.after_state = json!({
            "source_event": "TextChanged",
            "resolver": resolver,
            "node": namehash,
            "record_key": "text:lease",
            "record_family": "text",
            "selector_key": "lease",
            "value_retained": true,
            "value": event_identity,
        });
        event
    };
    let mut older_grant = v2_history_event(
        "two-leases-older-grant",
        None,
        Some(older_lease_id),
        "RegistrationGranted",
        120,
    );
    older_grant.after_state["namehash"] = json!(&namehash);
    let mut wrapper_binding = v2_history_event(
        "two-leases-wrap",
        Some(&logical_name_id),
        Some(wrapper_resource_id),
        "SurfaceBound",
        121,
    );
    wrapper_binding.source_family = "ens_v1_wrapper_l1".to_owned();
    wrapper_binding.after_state = json!({
        "source_event": "NameWrapped",
        "node": namehash,
        "wrapped_registrar_resource_id": older_lease_id,
    });
    let older_write_identity = "two-leases-wrapped-era-write";
    let later_write_identity = "two-leases-later-write";
    // Each resource's registry pointer selects its era's resolver.
    let pointer = |event_identity: &str, resource: Uuid, resolver: &str, block_number: i64| {
        let mut event = v2_history_event(
            event_identity,
            Some(&logical_name_id),
            Some(resource),
            "ResolverChanged",
            block_number,
        );
        event.source_family = "ens_v1_registry_l1".to_owned();
        event.after_state = json!({"node": namehash, "resolver": resolver});
        event.log_index = Some(1);
        event
    };
    let mut later_grant = v2_history_event(
        "two-leases-later-grant",
        Some(&logical_name_id),
        Some(later_lease_id),
        "RegistrationGranted",
        124,
    );
    later_grant.after_state["namehash"] = json!(&namehash);
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            older_grant,
            wrapper_binding,
            pointer("two-leases-wrapped-pointer", wrapper_resource_id, WRAPPED_RESOLVER, 121),
            node_write(older_write_identity, 122, WRAPPED_RESOLVER),
            later_grant,
            pointer("two-leases-later-pointer", later_lease_id, LATER_RESOLVER, 124),
            node_write(later_write_identity, 125, LATER_RESOLVER),
        ],
    )
    .await?;

    let older_route = format!("/v1/events?registration_id={older_lease_id}&page_size=20");
    let older = v2_history_payload_for_database(&database, &older_route).await?;
    let older_hashes = history_transaction_hashes(&older);
    assert!(
        older_hashes.contains(&"0xtx122"),
        "the older lease lost the write attributed to the NameWrapper resource that wrapped it: {older_hashes:?}"
    );
    assert!(
        !older_hashes.contains(&"0xtx125"),
        "the older lease listed a write attributed only to the later lease: {older_hashes:?}"
    );
    assert_eq!(
        older["page"]["total_count"],
        json!(older_hashes.len()),
        "the older lease's count disagrees with its rows: {older}"
    );

    let later = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={later_lease_id}&page_size=20"),
    )
    .await?;
    let later_hashes = history_transaction_hashes(&later);
    assert!(later_hashes.contains(&"0xtx125"), "{later_hashes:?}");
    assert!(!later_hashes.contains(&"0xtx122"), "{later_hashes:?}");

    // The later lease's write cannot anchor a page of the older lease's history.
    let later_first = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?registration_id={later_lease_id}&page_size=1"),
    )
    .await?;
    assert_eq!(later_first["data"][0]["transaction_hash"], json!("0xtx125"));
    let mut foreign_anchor = crate::v2::decode(
        later_first["page"]["next_cursor"]
            .as_str()
            .expect("the later lease has more than one row"),
    )
    .expect("the later lease's cursor must decode");
    foreign_anchor.filters.insert(
        "registration_id".to_owned(),
        older_lease_id.to_string(),
    );
    let response = v2_history_response_for_database(
        &database,
        &format!(
            "/v1/events?registration_id={older_lease_id}&page_size=1&cursor={}",
            crate::v2::encode(&foreign_anchor)
        ),
    )
    .await?;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "a write of the later lease anchored a page of the older lease"
    );

    database.cleanup().await
}

mod registry_handoff_history {
    include!("v2_history_registry_handoff.rs");
}
