// Review regressions for scoped empty reads and a family reset between address membership
// selection and the later record-count read.

async fn with_record_families(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let response = bigname_storage::publication_source::with_serve_from_families(
        true, v2_get_response(database, uri),
    ).await?;
    Ok((response.status(), read_json(response).await?))
}

#[tokio::test]
async fn v2_empty_ens_address_reads_ignore_an_unrelated_family_rebuild() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    sqlx::query(
        "INSERT INTO bigname_phase.project_family_marker (chain_id, state)
         VALUES ('base-mainnet', 'bootstrap_pending')",
    )
    .execute(&database.pool)
    .await?;
    let address = "0x0000000000000000000000000000000000000fff";
    for uri in [
        format!("/v1/addresses/{address}/names?namespace=ens"),
        format!("/v1/addresses/{address}/names?namespace=ens&relation=resolves_to&coin_type=60"),
        format!("/v1/addresses/{address}/names?namespace=ens&relation=resolves_to&coin_type=evm"),
        format!("/v1/addresses/{address}/primary-name?namespace=ens&source=indexed"),
    ] {
        let (status, body) = assert_switch_differential(&database, &uri).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {body:#}");
    }
    database.cleanup().await
}

async fn reset_records_families(database: &TestDatabase) -> Result<()> {
    let token = bigname_project::families::input_token(&database.pool, SWITCH_CHAIN).await?;
    let mut options = bigname_project::families::FamilyOptions::new(
        bigname_content_hash::INTERPRETER_CONTENT_HASH,
    );
    options.max_blocks_per_run = 0;
    let outcome = bigname_project::families::apply(
        &database.pool,
        SWITCH_CHAIN,
        &bigname_project::Marker { number: 240, hash: "0xhistory240".to_owned() },
        bigname_project::families::FamilyMode::Rebuild,
        &token,
        &options,
    ).await?;
    anyhow::ensure!(outcome.reset && outcome.marker.is_none(),
        "actual reset before replay: {outcome:?}");
    Ok(())
}

async fn assert_address_counts_reset_is_stale(relation: &str) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    let uri = format!("/v1/addresses/{SWITCH_ALICE}/names?namespace=ens&q=alpha\
                      &include=counts,role_summary{relation}");
    let (status, before) = with_record_families(&database, &uri).await?;
    assert_eq!(status, StatusCode::OK, "{before:#}");
    assert_eq!(before["data"].as_array().map(Vec::len), Some(1));
    let (_guard, control) =
        crate::v2::address_names_grant_read_test_hooks::install(&database.lookup_pool).await?;
    let (status, body) = {
        let request = with_record_families(&database, &uri);
        tokio::pin!(request);
        tokio::select! {
            response = &mut request => anyhow::bail!("request did not pause: {:?}", response?),
            () = control.wait_until_reached() => {}
        }
        reset_records_families(&database).await?;
        let ((), response) = tokio::join!(control.resume(), &mut request);
        response?
    };
    assert_eq!((status, &body["error"]["code"]),
        (StatusCode::CONFLICT, &json!("stale")), "{body:#}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_name_counts_after_family_reset_are_stale() -> Result<()> {
    assert_address_counts_reset_is_stale("").await
}

#[tokio::test]
async fn v2_resolves_to_counts_after_family_reset_are_stale() -> Result<()> {
    assert_address_counts_reset_is_stale("&relation=resolves_to&coin_type=60").await
}

#[tokio::test]
async fn v2_empty_address_collections_revalidate_the_requested_family_publication() -> Result<()> {
    for relation in ["", "&relation=resolves_to&coin_type=60", "&relation=resolves_to&coin_type=evm"] {
        let database = TestDatabase::new_migrated().await?;
        seed_switch_records_fixture(&database).await?;
        let uri = format!("/v1/addresses/0x0000000000000000000000000000000000000fff/names?namespace=ens{relation}");
        let (_guard, control) =
            crate::v2::collection_snapshot::finish_test_hooks::install(&database.lookup_pool).await?;
        let (status, body) = {
            let request = with_record_families(&database, &uri);
            tokio::pin!(request);
            tokio::select! {
                response = &mut request => anyhow::bail!("empty page did not reach its fence: {:?}", response?),
                () = control.wait_until_reached() => {}
            }
            reset_records_families(&database).await?;
            let ((), response) = tokio::join!(control.resume(), &mut request);
            response?
        };
        assert_eq!((status, &body["error"]["code"]),
            (StatusCode::CONFLICT, &json!("stale")), "{uri}: {body:#}");
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_missing_primary_claim_revalidates_the_requested_family_publication() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_records_fixture(&database).await?;
    let uri = "/v1/addresses/0x0000000000000000000000000000000000000fff/primary-name?namespace=ens&source=indexed";
    let (_guard, control) =
        crate::v2::support::indexed_read_test_hooks::install(&database.lookup_pool).await?;
    let (status, body) = {
        let request = with_record_families(&database, uri);
        tokio::pin!(request);
        tokio::select! {
            response = &mut request => anyhow::bail!("missing claim did not reach its fence: {:?}", response?),
            () = control.wait_until_reached() => {}
        }
        reset_records_families(&database).await?;
        let ((), response) = tokio::join!(control.resume(), &mut request);
        response?
    };
    assert_eq!((status, &body["error"]["code"]),
        (StatusCode::CONFLICT, &json!("stale")), "{body:#}");
    database.cleanup().await
}
