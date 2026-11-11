// TYR-454: the subnames page total and the registry overview's `counts.labels` count at most the
// child count cap and serve `null` above it, with the rows of the uncapped read; without
// `include=counts` the overview reads no label count at all.

/// `pages` with every page's total replaced by `null`.
fn without_totals(pages: &[Value]) -> Vec<Value> {
    pages
        .iter()
        .cloned()
        .map(|mut page| {
            page["total_count"] = Value::Null;
            page
        })
        .collect()
}

#[tokio::test]
async fn child_totals_are_null_above_the_cap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // alpha.eth has three subnames, two of them labels of its subregistry, which holds two
    // registrations under it.
    seed_family_children_fixture(&database).await?;
    let subnames = "/v1/names/alpha.eth/subnames?page_size=2";
    let narrowed = "/v1/names/alpha.eth/subnames?page_size=2&include_expired=false";
    let overview = format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}?include=counts");

    // Rows S1 and L8 below every cap.
    let uncapped = read_family_pages(&database, subnames).await?;
    assert_eq!(uncapped[0]["total_count"], json!(3), "{uncapped:#?}");
    let uncapped_narrowed = read_family_pages(&database, narrowed).await?;
    assert_eq!(uncapped_narrowed[0]["total_count"], json!(3), "{uncapped_narrowed:#?}");
    let (status, uncapped_overview) = read_family_response(&database, &overview).await?;
    assert_eq!(status, StatusCode::OK, "{uncapped_overview:#}");
    assert_eq!(uncapped_overview["data"]["counts"]["labels"], json!(2));

    // Row S3: a total equal to the cap is exact.
    crate::v2::with_child_count_cap(3, async {
        assert_eq!(read_family_pages(&database, subnames).await?, uncapped);
        assert_eq!(read_family_pages(&database, narrowed).await?, uncapped_narrowed);
        anyhow::Ok(())
    })
    .await?;

    // Rows S2 and S4: above the cap the total is null on every page, plain and narrowed, and the
    // rows and continuations are those of the uncapped read. Row L1 at the API: labels equal to
    // the cap count exactly.
    crate::v2::with_child_count_cap(2, async {
        assert_eq!(read_family_pages(&database, subnames).await?, without_totals(&uncapped));
        assert_eq!(
            read_family_pages(&database, narrowed).await?,
            without_totals(&uncapped_narrowed)
        );
        let (status, page) = read_family_response(&database, &overview).await?;
        assert_eq!(status, StatusCode::OK, "{page:#}");
        assert_eq!(page["data"]["counts"]["labels"], json!(2), "{page:#}");
        anyhow::Ok(())
    })
    .await?;

    // Row L6: more labels than the cap serve a present, null label count, and the other counts
    // are those of the uncapped read.
    crate::v2::with_child_count_cap(1, async {
        let (status, page) = read_family_response(&database, &overview).await?;
        assert_eq!(status, StatusCode::OK, "{page:#}");
        let counts = &page["data"]["counts"];
        assert!(counts.get("labels").is_some(), "{page:#}");
        assert_eq!(counts["labels"], Value::Null, "{page:#}");
        assert_eq!(counts["roles"], uncapped_overview["data"]["counts"]["roles"]);
        assert_eq!(counts["events"], uncapped_overview["data"]["counts"]["events"]);
        assert_eq!(
            page["data"]["referenced_by"],
            uncapped_overview["data"]["referenced_by"]
        );
        anyhow::Ok(())
    })
    .await?;
    database.cleanup().await
}

/// Row L5: without `include=counts` the overview serves no `counts.labels` and never reads the
/// stored label counts, so it answers while they are unreadable; with `include=counts` it reads
/// them.
#[tokio::test]
async fn registry_overview_reads_no_labels_without_counts() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;
    let plain = format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}");
    let (status, before) = read_family_response(&database, &plain).await?;
    assert_eq!(status, StatusCode::OK, "{before:#}");
    assert!(before["data"]["counts"].get("labels").is_none(), "{before:#}");

    sqlx::query(
        "ALTER TABLE bigname_phase.project_child_listing_count
         RENAME TO project_child_listing_count_hidden",
    )
    .execute(&database.pool)
    .await?;
    let (status, after) = read_family_response(&database, &plain).await?;
    assert_eq!(status, StatusCode::OK, "{after:#}");
    assert_eq!(after["data"], before["data"]);
    let (status, counted) =
        read_family_response(&database, &format!("{plain}?include=counts")).await?;
    assert_ne!(status, StatusCode::OK, "the counted overview must read the stored label counts: {counted:#}");
    database.cleanup().await
}

/// Row L-edge: a registry holding more registrations than the cap under its name, with at most
/// the cap in labels, counts its labels exactly. three.alpha.eth is registered by alpha.eth's
/// subregistry but serves its ENSv1 edge, so it is a subname and not a label: the subregistry
/// holds three registrations under alpha.eth and two labels. A count that read the
/// registrations stopped at a cap of two and served `null`.
#[tokio::test]
async fn the_label_count_is_exact_above_the_cap_in_registrations() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_family_children_fixture(&database).await?;
    insert_family_label_preimage(&database.pool, b"three").await?;
    let (three, _) = seed_family_name(&database, "three.alpha.eth", 0x6d1_0000, "ens_v1").await?;
    insert_family_registry_child_edge(
        &database.pool,
        "ens",
        FAMILY_CHAIN,
        "alpha.eth",
        &child_labelhash("three"),
        CHILD_OWNER,
        241,
        "0xhistory241",
    )
    .await?;
    let alpha_registry = Uuid::from_u128(0x6b0_0001);
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[child_registry_event(
            "children-three",
            Some(&three),
            "RegistrationGranted",
            241,
            1,
            CHILD_ALPHA_REGISTRY,
            json!({"registry_contract_instance_id": alpha_registry.to_string(),
                   "status": "registered", "registrant": CHILD_OWNER,
                   "expiry": 1_900_000_000i64}),
        )],
    )
    .await?;
    publish_test_families(&database, 241).await?;

    let registrations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM bigname_phase.project_child_registration_state
         WHERE registry_contract_instance_id = $1 AND event_kind <> 'RegistrationReleased'",
    )
    .bind(alpha_registry.to_string())
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(registrations, 3, "the subregistry holds three registrations");
    let names = |pages: &[Value]| -> Vec<Value> {
        pages
            .iter()
            .flat_map(|page| page["data"].as_array().into_iter().flatten())
            .map(|row| row["name"].clone())
            .collect()
    };
    let subnames = read_family_pages(&database, "/v1/names/alpha.eth/subnames?page_size=10").await?;
    assert!(names(&subnames).contains(&json!("three.alpha.eth")), "{subnames:#?}");
    let labels = read_family_pages(
        &database,
        &format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}/labels?page_size=10"),
    )
    .await?;
    assert_eq!(
        names(&labels),
        [json!("one.alpha.eth"), json!("two.alpha.eth")],
        "{labels:#?}"
    );
    let overview = format!("/v1/registries/1/{CHILD_ALPHA_REGISTRY}?include=counts");
    crate::v2::with_child_count_cap(2, async {
        let (status, page) = read_family_response(&database, &overview).await?;
        assert_eq!(status, StatusCode::OK, "{page:#}");
        assert_eq!(page["data"]["counts"]["labels"], json!(2), "{page:#}");
        anyhow::Ok(())
    })
    .await?;
    database.cleanup().await
}
