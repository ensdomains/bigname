const ROUND2_RESOLVER: &str = "0x0000000000000000000000000000000000000ab2";

async fn seed_switch_project_resolver(database: &TestDatabase) -> Result<()> {
    seed_switch_routes_fixture(database).await?;
    // This second resolver has a permission grant for alpha.eth, while alpha still points to
    // the first resolver. Its empty bound-name page and nonempty roles page are both real.
    let implementation = "0x0000000000000000000000000000000000000fed";
    let payload = json!({"deployment_epoch": "fixture", "contracts": [],
        "resolver_implementations": [{"role": "permissioned_resolver", "address": implementation}],
        "capability_flags": {}});
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
            deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v2_resolver_l1', $2, 'fixture', 'active', 'fixture',
            'fixture/round2-resolver.toml', $1) RETURNING manifest_id",
    )
    .bind(&payload)
    .bind(SWITCH_CHAIN)
    .fetch_one(&database.pool)
    .await?;
    sqlx::query("INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
        manifest_version, source_manifest_id, chain_id, derivation_kind, canonicality_state, after_state)
        VALUES ('round2-manifest', 'ens', 'SourceManifestUpdated', 'ens_v2_resolver_l1', 1, $2, $3,
        'manifest_sync', 'canonical', $1)")
        .bind(json!({"rollout_status": "active", "normalizer_version": "fixture", "manifest_payload": payload}))
        .bind(manifest_id).bind(SWITCH_CHAIN).execute(&database.pool).await?;
    let mut upgrade = switch_event(
        "round2-resolver-upgrade",
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        206,
        1,
        json!({"proxy_address": ROUND2_RESOLVER, "implementation": implementation}),
    );
    let (resource, mut grant): (Uuid, Value) = sqlx::query_as(
        "SELECT resource_id, after_state FROM normalized_events WHERE event_identity = 'switch-alpha-role'",
    ).fetch_one(&database.pool).await?;
    grant["scope"]["resolver_address"] = json!(ROUND2_RESOLVER);
    grant["resolver"] = json!(ROUND2_RESOLVER);
    let mut role = switch_event(
        "round2-alpha-role",
        None,
        Some(resource),
        "PermissionChanged",
        "ens_v2_resolver_l1",
        207,
        1,
        grant,
    );
    role.derivation_kind = "ens_v2_permissions".to_owned();
    for event in [&mut upgrade, &mut role] {
        event.source_manifest_id = Some(manifest_id);
        event.manifest_version = 1;
        event.raw_fact_ref["emitting_address"] = json!(ROUND2_RESOLVER);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[upgrade, role]).await?;
    reset_switch_families(database).await?;
    publish_project_and_families(database, 240).await?;
    let supported: String = sqlx::query_scalar(
        "SELECT declared_summary #>> '{role_holders,status}' FROM resolver_current
         WHERE resolver_address = $1",
    )
    .bind(ROUND2_RESOLVER)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(supported, "supported");
    Ok(())
}

async fn advance_switch_without_resolver_changes(database: &TestDatabase) -> Result<()> {
    bigname_project::Engine::new(database.pool.clone())
        .run_batch(bigname_project::BatchRequest {
            chain_id: SWITCH_CHAIN.to_owned(),
            target_block: 241,
            affected_from_block: 241,
            affected_to_block: 241,
            resume_current: Some(bigname_project::Marker {
                number: 240,
                hash: "0xhistory240".to_owned(),
            }),
            mode: bigname_project::RunMode::Normal,
        })
        .await?;
    publish_bounded_membership_at(database, 241).await?;
    sqlx::query(
        "UPDATE chain_phase_state SET current_block_number = 241, target_block_number = 241,
        current_block_hash = '0xhistory241', target_block_hash = '0xhistory241'
        WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(SWITCH_CHAIN)
    .execute(&database.pool)
    .await?;
    let token = bigname_project::families::input_token(&database.pool, SWITCH_CHAIN).await?;
    let outcome = bigname_project::families::apply(
        &database.pool,
        SWITCH_CHAIN,
        &bigname_project::Marker {
            number: 241,
            hash: "0xhistory241".to_owned(),
        },
        bigname_project::families::FamilyMode::Normal,
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ),
    )
    .await;
    anyhow::ensure!(
        outcome.skipped.is_none() && outcome.marker.as_ref().map(|m| m.number) == Some(241),
        "{outcome:?}"
    );
    let target: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint FROM resolver_current
         WHERE resolver_address = $1",
    )
    .bind(ROUND2_RESOLVER)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        target, 240,
        "unchanged resolver retains its previous publication target"
    );
    Ok(())
}

#[tokio::test]
async fn v2_resolver_roles_reject_historical_at_before_attaching_current_names() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_project_resolver(&database).await?;
    let uri = format!("/v1/resolvers/1/{ROUND2_RESOLVER}/roles");
    let (status, current) = with_serve_on(&database, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{current:#}");
    assert!(
        current["data"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["name"] == "alpha.eth")),
        "{current:#}"
    );
    advance_switch_without_resolver_changes(&database).await?;
    let at = switch_timestamp(1_700_000_240)?;
    let (status, historical) = with_serve_on(&database, &format!("{uri}?at={at}")).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{historical:#}");
    assert_eq!(historical["error"]["code"], "stale");
    database.cleanup().await
}

#[tokio::test]
async fn v2_empty_bound_names_refuse_a_historical_family_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_project_resolver(&database).await?;
    let uri = format!("/v1/resolvers/1/{ROUND2_RESOLVER}");
    let (status, before) = with_serve_on(&database, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{before:#}");
    assert_eq!(before["data"]["bound_names"]["data"], json!([]));
    advance_switch_without_resolver_changes(&database).await?;
    let (status, current) = with_serve_on(&database, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{current:#}");
    assert_eq!(current["data"]["bound_names"]["data"], json!([]));
    let at = switch_timestamp(1_700_000_240)?;
    let (status, historical) = with_serve_on(&database, &format!("{uri}?at={at}")).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{historical:#}");
    assert_eq!(historical["error"]["code"], "stale");
    database.cleanup().await
}
