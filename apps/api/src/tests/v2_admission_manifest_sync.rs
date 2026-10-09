// Manifest sync changes the ENSv2 root registry admission before the redo that adopts it
// (`docs/glossary.md` § Universal Resolver cutover).

const ALICE_RESOLVES_TO: &str = "/v1/addresses/0x0000000000000000000000000000000000000def/names?namespace=ens&relation=resolves_to&coin_type=60";

/// Seed alice.eth on a mainnet profile with a Universal Resolver entrypoint and an admitted
/// ENSv2 root registry, published by a rebuild. alice.eth has no ENSv2 entry.
async fn seed_alice_on_an_admitted_chain(database: &TestDatabase) -> Result<()> {
    seed_alice_name_inputs(database).await?;
    let payload = json!({"contracts": [{"role": "universal_resolver",
        "address": "0xeeeeeeee14d718c2b47d9923deab1335e144eeee", "start_block": 0}]});
    let execution = database
        .insert_manifest("ens", "ens_execution", "ethereum-mainnet", "admission-sync", 1, "active", "test")
        .await?;
    sqlx::query("UPDATE manifest_versions SET manifest_payload = $2 WHERE manifest_id = $1")
        .bind(execution)
        .bind(&payload)
        .execute(&database.pool)
        .await?;
    seed_fixture_manifest_update(&database.pool, execution, "ethereum-mainnet", "ens", "ens_execution", &payload)
        .await?;
    admit_ens_v2_root_registry(database, "ethereum-mainnet").await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await
}

/// What a client sees of alice.eth: name detail's resolver and unresolvable reason, the
/// namespace's mainnet `resolution`, the names that resolve to alice's address, and the batch
/// lookup status.
async fn alice_view(database: &TestDatabase) -> Result<Value> {
    let (status, detail) = read_family_response(database, "/v1/names/alice.eth").await?;
    anyhow::ensure!(status == StatusCode::OK, "name detail: {detail:#}");
    let (status, namespace) = read_family_response(database, "/v1/namespaces/ens").await?;
    anyhow::ensure!(status == StatusCode::OK, "namespace: {namespace:#}");
    let network = namespace["data"]["networks"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|network| network["network"] == json!("ethereum"))
        .cloned()
        .with_context(|| format!("no ethereum network: {namespace:#}"))?;
    let (status, resolves_to) = read_family_response(database, ALICE_RESOLVES_TO).await?;
    anyhow::ensure!(status == StatusCode::OK, "resolves_to: {resolves_to:#}");
    let lookup = v2_lookup_response_for_database(
        database,
        "/v1/lookup",
        json!({"profile": "detail", "inputs": [{"name": "alice.eth"}]}),
    )
    .await?
    .status();
    Ok(json!({
        "detail": {
            "resolver": detail["data"]["resolver"]["address"],
            "unresolvable_reason": detail["data"]["unresolvable_reason"],
        },
        "namespace": network["resolution"],
        "resolves_to": resolves_to["data"].as_array().into_iter().flatten()
            .map(|row| row["name"].clone()).collect::<Vec<_>>(),
        "lookup": lookup.as_u16(),
    }))
}

/// R19. A manifest sync turns the admitted root registry from `active` to `shadow`, writes its
/// SourceManifestUpdated event and, as `invalidate_changed_derived_epochs` does, moves the
/// Interpret and Project input hashes off this build's. Until the redo republishes, name detail, the namespace and the resolves_to
/// address read serve the previous publication's cut-over view unchanged, since they take the
/// admission the publication recorded. Stored lookup answers `409 stale`. After the redo all of
/// them serve alice.eth through ENSv1.
#[tokio::test]
async fn v2_a_root_registry_withdrawn_by_manifest_sync_serves_the_previous_publication_until_the_redo()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_alice_on_an_admitted_chain(&database).await?;
    let cut_over = json!({
        "detail": {"resolver": null, "unresolvable_reason": "no_live_ens_v2_entry"},
        "namespace": {"protocol": "ens_v2", "since_block": 0},
        "resolves_to": [],
        "lookup": 200,
    });
    assert_eq!(alice_view(&database).await?, cut_over, "before the manifest sync");

    shadow_by_manifest_sync(
        &database.pool,
        "ethereum-mainnet",
        "ens_v2_root_l1",
        &["ethereum-mainnet"],
    )
    .await?;
    let mut window = cut_over.clone();
    window["lookup"] = json!(409);
    assert_eq!(
        alice_view(&database).await?,
        window,
        "between the manifest sync and the redo"
    );

    // The redo adopts this build's hash, and its rebuild publishes the withdrawn admission.
    adopt_this_build(&database.pool, &["ethereum-mainnet"]).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    assert_eq!(
        alice_view(&database).await?,
        json!({
            "detail": {"resolver": "0x0000000000000000000000000000000000000abc",
                       "unresolvable_reason": null},
            "namespace": {"protocol": "ens_v1", "since_block": null},
            "resolves_to": ["alice.eth"],
            "lookup": 200,
        }),
        "after the redo"
    );
    database.cleanup().await
}

/// What manifest sync writes when it turns `chain`'s `family` manifest to `shadow`: the
/// manifest row, a blockless finalized SourceManifestUpdated event, and Interpret and Project
/// input hashes moved off this build's on every chain in `invalidated`.
async fn shadow_by_manifest_sync(
    pool: &PgPool,
    chain: &str,
    family: &str,
    invalidated: &[&str],
) -> Result<()> {
    sqlx::query(
        "UPDATE manifest_versions SET rollout_status = 'shadow'
         WHERE chain_id = $1 AND source_family = $2",
    )
    .bind(chain)
    .bind(family)
    .execute(pool)
    .await?;
    let written = sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, raw_fact_ref, derivation_kind, canonicality_state,
             before_state, after_state
         )
         SELECT 'manifest_sync:source_manifest_updated:shadow-' || $2, event.namespace,
                event.event_kind, event.source_family, event.manifest_version,
                event.source_manifest_id, event.chain_id,
                jsonb_build_object('manifest_id', manifest.manifest_id,
                    'namespace', manifest.namespace, 'source_family', manifest.source_family,
                    'chain', manifest.chain_id, 'deployment_epoch', manifest.deployment_label),
                event.derivation_kind, 'finalized', event.after_state,
                jsonb_set(event.after_state, '{rollout_status}', '\"shadow\"')
         FROM normalized_events event
         JOIN manifest_versions manifest ON manifest.manifest_id = event.source_manifest_id
         WHERE event.chain_id = $1 AND event.event_kind = 'SourceManifestUpdated'
           AND event.source_family = $2
         ORDER BY event.normalized_event_id DESC
         LIMIT 1",
    )
    .bind(chain)
    .bind(family)
    .execute(pool)
    .await?
    .rows_affected();
    anyhow::ensure!(written == 1, "no {family} manifest event on {chain}");
    sqlx::query(
        "UPDATE chain_phase_state SET input_content_hash = 'manifest-authority:shadow'
         WHERE chain_id = ANY($1) AND phase_name IN ('interpret', 'project')
           AND input_content_hash IS NOT NULL",
    )
    .bind(invalidated)
    .execute(pool)
    .await?;
    Ok(())
}

/// The redo's hash adoption on `chains`.
async fn adopt_this_build(pool: &PgPool, chains: &[&str]) -> Result<()> {
    sqlx::query(
        "UPDATE chain_phase_state SET input_content_hash = $2
         WHERE chain_id = ANY($1) AND phase_name IN ('interpret', 'project')",
    )
    .bind(chains)
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

/// The ids of the RecordChanged rows the mirror fixture's ENSv2 owner's history attributes.
async fn mirrored_record_ids(database: &TestDatabase) -> Result<Vec<String>> {
    let uri = format!(
        "/v1/addresses/{V2_ADDRESS}/history?relation=owner&scope=registration&kind=RecordChanged&page_size=200"
    );
    let state = AppState::new_with_rpc_urls(
        database.lookup_pool.clone(),
        bigname_lookup::ChainRpcUrls::default(),
    );
    let response = app_router(state)
        .oneshot(Request::builder().uri(&uri).body(Body::empty())?)
        .await?;
    let status = response.status();
    let body: Value = read_json(response).await?;
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {body:#}");
    Ok(hk_ids(&body))
}

/// R23. A manifest sync turns the Sepolia `ens_v2_resolver_l1` manifest, which declares the
/// ENSv1 mirror resolver, to `shadow`. The address history attributes the writes the mirror
/// follows by the declaration in the manifest set the publication recorded, so it keeps them
/// until the redo. The redo's publication no longer declares the mirror and attributes none.
#[tokio::test]
async fn v2_mirror_record_attribution_reads_the_published_manifest_set_until_the_redo()
-> Result<()> {
    const CHAIN: &str = "ethereum-sepolia";
    let database = v2_mirror_records_database(
        "alice.eth",
        "0x1010101010101010101010101010101010101010",
        MirrorFixtureSource::Exact,
        "resolver",
    )
    .await?;
    let before = mirrored_record_ids(&database).await.context("before")?;
    assert_eq!(before.len(), 3, "{before:?}");

    shadow_by_manifest_sync(&database.pool, CHAIN, "ens_v2_resolver_l1", &[CHAIN]).await?;
    assert_eq!(
        mirrored_record_ids(&database).await.context("window")?,
        before,
        "between the manifest sync and the redo"
    );

    adopt_this_build(&database.pool, &[CHAIN]).await?;
    rebuild_fixture_families(&database.pool, CHAIN, 21_000_003, "0xmirror").await?;
    assert_eq!(
        mirrored_record_ids(&database).await.context("after")?,
        Vec::<String>::new(),
        "after the redo"
    );
    database.cleanup().await
}
