fn compact_search_dictionary(
    page: &bigname_storage::families::search_dictionary::SearchPage,
) -> Result<Value> {
    page.rows
        .iter()
        .map(|row| {
            let name = crate::v2::build_compact_search_name_for_test(row).map_err(|error| {
                anyhow::anyhow!("compact search serialization failed: {error:?}")
            })?;
            Ok(serde_json::to_value(name)?)
        })
        .collect::<Result<Vec<_>>>()
        .map(Value::Array)
}

fn search_dictionary(page: &bigname_storage::NameCurrentListPage) -> Result<Value> {
    page.rows
        .iter()
        .map(|row| {
            let name = crate::v2::build_search_name_for_test(row)
                .map_err(|error| anyhow::anyhow!("search serialization failed: {error:?}"))?;
            Ok(serde_json::to_value(name)?)
        })
        .collect::<Result<Vec<_>>>()
        .map(Value::Array)
}

async fn assert_search_lean_pages_match(
    database: &TestDatabase,
    filter: &bigname_storage::NameCurrentListFilter,
    page_size: u64,
) -> Result<Vec<Value>> {
    use bigname_storage::families::{name::load_family_search_page, search_dictionary::load_page};
    let mut cursor = None;
    let mut all = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..100 {
        let full =
            load_family_search_page(&database.pool, filter, cursor.as_ref(), page_size).await?;
        let lean = load_page(&database.pool, filter, cursor.as_ref(), page_size).await?;
        assert_eq!(
            compact_search_dictionary(&lean)?,
            search_dictionary(&full)?,
            "{filter:?}"
        );
        assert_eq!(lean.next_cursor, full.next_cursor);
        for row in &lean.rows {
            assert!(
                seen.insert(format!("{}:{}", row.namespace, row.namehash)),
                "duplicate name"
            );
        }
        let data = compact_search_dictionary(&lean)?;
        all.extend(data.as_array().unwrap().iter().cloned());
        let Some(next) = lean.next_cursor else {
            return Ok(all);
        };
        assert_eq!(lean.rows.len(), usize::try_from(page_size)?);
        assert_ne!(cursor.as_ref(), Some(&next));
        cursor = Some(next);
    }
    anyhow::bail!("bounded fixture failed to finish search pagination")
}

fn lean_filter(namespace: &str, contains: &str) -> bigname_storage::NameCurrentListFilter {
    bigname_storage::NameCurrentListFilter {
        namespace: Some(namespace.to_owned()),
        contains: Some(contains.to_owned()),
        supported_only: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn v2_search_durable_preserves_registration_wrapper_and_ownerless_dictionary() -> Result<()> {
    for state in [
        AliceInputState::Registry,
        AliceInputState::Wrapped,
        AliceInputState::Released,
        AliceInputState::Reserved,
        AliceInputState::Ownerless,
        AliceInputState::Unbound,
    ] {
        let database = TestDatabase::new_migrated().await?;
        seed_alice_state_inputs(&database, state).await?;
        let filter = lean_filter("ens", "alice");
        let rows = assert_search_lean_pages_match(&database, &filter, 1).await?;
        let (status, body) =
            read_family_response(&database, "/v1/search?q=alice&namespace=ens&page_size=1").await?;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["data"], json!(rows));
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn v2_search_durable_preserves_textless_preimages_and_continuations() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_textless_fixture(&database, true).await?;
    for size in [1, 2, 200] {
        for contains in ["eth", "missing-value", "a"] {
            assert_search_lean_pages_match(&database, &lean_filter("ens", contains), size).await?;
        }
    }
    // A verified preimage fixture changes the structural spelling at the next read.
    insert_family_label_preimage(&database.pool, b"first").await?;
    assert_search_lean_pages_match(&database, &lean_filter("ens", "eth"), 1).await?;
    database.cleanup().await
}

#[tokio::test]
async fn v2_search_durable_keeps_basenames_topology_timestamp() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_v2_basenames_auto_transition_fixture(&database, Uuid::from_u128(0x9260)).await?;
    let rows =
        assert_search_lean_pages_match(&database, &lean_filter("basenames", "alice"), 1).await?;
    assert_eq!(rows.len(), 1, "the Basenames fixture must be exercised");
    assert!(
        rows[0].get("created_at").is_some(),
        "timestamp must be compared"
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_search_durable_reads_finished_fields_without_request_composition() -> Result<()> {
    use bigname_storage::families::{name::seams, search_dictionary::load_page};
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    let database = TestDatabase::new_migrated().await?;
    seed_family_routes_fixture(&database).await?;
    let count = Arc::new(AtomicU64::new(0));
    let filter = lean_filter("ens", "eth");
    let page = seams::with_composed_names_counter(
        count.clone(),
        load_page(&database.pool, &filter, None, 1),
    )
    .await?;
    assert_eq!(page.rows.len(), 1);
    assert!(page.next_cursor.is_some());
    assert_eq!(count.load(Ordering::Relaxed), 0);
    assert_search_lean_pages_match(&database, &filter, 1).await?;
    database.cleanup().await
}

/// Read-only measurement against explicitly supplied retained fixture IDs. The harness derives
/// 201 supported IDs from baseline HTTP plus its continuation, and keeps the original 804 IDs.
#[tokio::test]
#[ignore = "requires explicit retained fixture input and output paths"]
async fn v2_search_lean_retained_component_measurement() -> Result<()> {
    use bigname_storage::families::name::seams::load_search_component;
    use std::{fs, path::Path, time::Instant};
    let input: Value =
        serde_json::from_slice(&fs::read(std::env::var("BIGNAME_SEARCH_COMPARE_INPUT")?)?)?;
    let output = std::env::var("BIGNAME_SEARCH_COMPARE_OUTPUT")?;
    let diagnostic = std::env::var_os("BIGNAME_SEARCH_COMPARE_DIAGNOSTIC").is_some();
    let options = PgConnectOptions::from_str(&std::env::var("BIGNAME_DATABASE_URL")?)?.options([
        ("search_path", "bigname_phase"),
        ("statement_timeout", "25000"),
    ]);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?;
    let publication_sql = "SELECT jsonb_build_object('head',current_block_number,'hash',current_block_hash,'input_hash',input_content_hash,'generation',sequence) FROM bigname_phase.project_family_marker WHERE chain_id='ethereum-sepolia' AND state='live'";
    let before: Value = sqlx::query_scalar(publication_sql).fetch_one(&pool).await?;
    assert_eq!(before, input["publication"]);
    assert_eq!(
        before["input_hash"],
        bigname_content_hash::INTERPRETER_CONTENT_HASH
    );
    let filter = lean_filter("ens", "a");
    let mut results = Vec::new();
    for cohort in input["sets"].as_array().context("component sets")? {
        let ids: Vec<String> = serde_json::from_value(cohort["ids"].clone())?;
        let mut full_reference = None;
        for iteration in 0..if diagnostic { 1 } else { 25 } {
            for lean in if iteration % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let mode = if lean { "lean" } else { "full" };
                let marker = format!("{}:{mode}:{iteration}", cohort["label"].as_str().unwrap());
                if diagnostic {
                    sqlx::query("SELECT $1::text /* search_component_start */")
                        .bind(&marker)
                        .execute(&pool)
                        .await?;
                }
                let started = Instant::now();
                let page = load_search_component(&pool, &filter, &ids, lean).await?;
                let data = search_dictionary(&page)?;
                let ms = started.elapsed().as_secs_f64() * 1000.0;
                if let Some(reference) = &full_reference {
                    assert_eq!(&data, reference, "{marker}");
                } else {
                    assert!(!lean);
                    full_reference = Some(data.clone());
                }
                if let Some(expected) = cohort.get("expected") {
                    assert_eq!(&data, expected, "{marker}");
                }
                results.push(
                    json!({"cohort":cohort["label"],"mode":mode,"iteration":iteration,
                    "warmup":iteration<5,"milliseconds":ms,"rows":page.rows.len(),"data":data}),
                );
                if diagnostic {
                    sqlx::query("SELECT $1::text /* search_component_end */")
                        .bind(&marker)
                        .execute(&pool)
                        .await?;
                }
            }
        }
    }
    let after: Value = sqlx::query_scalar(publication_sql).fetch_one(&pool).await?;
    assert_eq!(before, after);
    assert!(!Path::new(&output).exists());
    fs::write(
        output,
        serde_json::to_vec_pretty(&json!({
        "interpreter_content_hash":bigname_content_hash::INTERPRETER_CONTENT_HASH,
        "publication":before,"diagnostic":diagnostic,"results":results,
        "route_budget_acceptance":false}))?,
    )?;
    pool.close().await;
    Ok(())
}
