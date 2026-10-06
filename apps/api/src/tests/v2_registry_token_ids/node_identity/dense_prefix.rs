//! Dense-prefix query collection, using the parent's admitted raw-log producer fixture.
use super::*;
use bigname_storage::{NameCurrentListFilter, families::search_dictionary::load_page};
use sqlx::{Connection, PgConnection};

fn prefix_filter() -> NameCurrentListFilter {
    NameCurrentListFilter {
        namespace: Some("ens".into()),
        prefix: Some("a".into()),
        supported_only: true,
        ..Default::default()
    }
}

async fn import_labels(database: &TestDatabase, labels: &[&str]) -> Result<()> {
    for label in labels {
        sqlx::query("INSERT INTO ens_names(hash,name) VALUES ($1,$2) ON CONFLICT DO NOTHING")
            .bind(format!("{:#x}", keccak256(label.as_bytes())))
            .bind(label)
            .execute(&database.pool)
            .await?;
    }
    let imported =
        bigname_storage::import_label_preimages_from_ens_names_table(&database.pool, None, None)
            .await?;
    assert_eq!(imported.rejected_row_count, 0);
    Ok(())
}

async fn assert_prefix_postings(database: &TestDatabase, class: i16, count: i64) -> Result<()> {
    let actual: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM name_search_postings WHERE namespace='ens'
         AND spelling_class=$1 AND token_kind=2 AND token_length=1 AND token_bytes=$2",
    )
    .bind(class)
    .bind(b"a".as_slice())
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(actual, count, "actual prefix postings in class {class}");
    Ok(())
}

/// A new physical connection cannot inherit another request's prepared statements. Inspect
/// the actual reader's collection queries, using a bound pattern so inspection cannot match
/// itself. Result parity alone would miss the unlimited posting-ID round trip.
async fn assert_bounded_posting_collection(
    database: &TestDatabase,
    filter: &NameCurrentListFilter,
    class: i16,
) -> Result<()> {
    let mut connection =
        PgConnection::connect_with(database.pool.connect_options().as_ref()).await?;
    let mut snapshot = connection.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *snapshot)
        .await?;
    let page = load_page(&mut *snapshot, filter, None, 1).await?;
    assert_eq!(page.rows.len(), 1);
    assert!(page.next_cursor.is_some());
    let statements: Vec<(String, i32)> = sqlx::query_as(
        "SELECT statement, cardinality(parameter_types) FROM pg_prepared_statements
         WHERE statement LIKE $1 ORDER BY statement",
    )
    .bind("SELECT search_id FROM bigname_phase.name_search_postings%")
    .fetch_all(&mut *snapshot)
    .await?;
    assert!(
        !statements.is_empty(),
        "the actual posting probe must execute"
    );
    assert!(
        statements.iter().all(|(sql, parameters)| {
            *parameters == 6 && sql.trim_end().ends_with("ORDER BY search_id LIMIT $6;")
        }),
        "class {class} {filter:?}: every executed posting-ID collection must be bounded; \
         an unlimited ALL_TOKEN statement was executed: {statements:#?}"
    );
    let full =
        bigname_storage::families::name::load_family_search_page(&mut *snapshot, filter, None, 1)
            .await?;
    assert_eq!(compact_search_dictionary(&page)?, search_dictionary(&full)?);
    assert_eq!(page.next_cursor, full.next_cursor);
    snapshot.commit().await?;
    connection.close().await?;
    Ok(())
}

async fn assert_dense_http(database: &TestDatabase) -> Result<Vec<Value>> {
    let expected = assert_search_lean_pages_match(database, &prefix_filter(), 200).await?;
    assert_eq!(
        expected.len(),
        1025,
        "the walk must cross the probe threshold"
    );
    for page_size in [1, 200] {
        let uri = format!("/v1/search?q=a&namespace=ens&page_size={page_size}");
        let default = get(database, &uri).await?;
        let explicit = get(database, &format!("{uri}&match=prefix")).await?;
        assert_eq!(default, explicit, "default and explicit prefix response");
        assert_eq!(default["data"], json!(expected[..page_size]));
        assert_eq!(default["page"]["has_more"], true);
    }
    for mode in ["", "&match=prefix"] {
        let pages = read_family_pages(
            database,
            &format!("/v1/search?q=a&namespace=ens&page_size=200{mode}"),
        )
        .await?;
        assert_eq!(pages.len(), 6);
        let actual: Vec<_> = pages
            .iter()
            .flat_map(|page| page["data"].as_array().unwrap().iter().cloned())
            .collect();
        assert_eq!(actual, expected, "complete HTTP cursor walk {mode}");
    }
    let contains = lean_filter("ens", "a.");
    assert_bounded_posting_collection(database, &contains, -1).await?;
    assert_eq!(
        assert_search_lean_pages_match(database, &contains, 200).await?,
        expected,
        "dense contains keeps the same complete result"
    );
    Ok(expected)
}

async fn dense_prefix_fixture(raw_names: bool) -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let registry =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 2281).await?;
    let wrapper = admit_family_from(&database, "mainnet", CHAIN, "ens_v1_wrapper_l1", 2282).await?;
    let current = role_address(&registry, "registry");
    let wrapper = role_address(&wrapper, "name_wrapper");
    let holder = HOLDER.parse()?;
    // One verified shared leaf creates 1,025 distinct structural prefix matches without
    // manufacturing documents/postings or importing 1,025 independent spellings.
    import_labels(&database, &["a", "eth"]).await?;
    let mut logs = vec![owner(B256::ZERO, b"eth", holder, current, 120, 0)];
    for index in 0..1025 {
        let parent = format!("parent{index:04}");
        let block = if index < 1024 { 121 } else { 122 };
        let position = if index < 1024 { index * 3 } else { 0 };
        logs.push(owner(
            node(&[b"eth"]),
            parent.as_bytes(),
            holder,
            current,
            block,
            position,
        ));
        logs.push(owner(
            node(&[parent.as_bytes(), b"eth"]),
            b"a",
            if raw_names { wrapper } else { holder },
            current,
            block,
            position + 1,
        ));
        if raw_names {
            logs.push(wrapped(
                &[b"a", parent.as_bytes(), b"eth"],
                wrapper,
                block,
                position + 2,
            ));
        }
    }
    intake(&database, &logs, 123).await?;
    let engine = bigname_interpret::Engine::new(database.pool.clone());
    run(&engine, 120, 121, bigname_interpret::RunMode::Normal).await?;
    publish(&database, 121).await?;
    let class = if raw_names { 0 } else { 1 };
    assert_prefix_postings(&database, class, 1024).await?;
    assert_bounded_posting_collection(&database, &prefix_filter(), class).await?;
    let sparse = get(&database, "/v1/search?q=a&namespace=ens&page_size=200").await?;
    assert_eq!(sparse["data"].as_array().unwrap().len(), 200);
    assert_eq!(sparse["page"]["has_more"], true);

    run(&engine, 122, 122, bigname_interpret::RunMode::Normal).await?;
    publish(&database, 122).await?;
    assert_prefix_postings(&database, class, 1025).await?;
    // The old reader passes the 1,024 case and fails here by preparing ALL_TOKEN.
    assert_bounded_posting_collection(&database, &prefix_filter(), class).await?;
    let expected = assert_dense_http(&database).await?;
    if !raw_names {
        assert_structural_edges(&database, &engine, &registry, wrapper, &expected[0]).await?;
    }
    database.cleanup().await
}

async fn assert_structural_edges(
    database: &TestDatabase,
    engine: &bigname_interpret::Engine,
    registry: &Value,
    wrapper: Address,
    first: &Value,
) -> Result<()> {
    let current = role_address(registry, "registry");
    let holder = HOLDER.parse()?;
    // Reuse the parent's admitted old-registry unmasked-owner shape: its lexical identity
    // sorts first as "a", but real Project marks its missing authority unsupported.
    let mut unsupported = owner(
        B256::ZERO,
        b"a",
        holder,
        role_address(registry, "registry_old"),
        123,
        0,
    );
    unsupported.data = alloy_primitives::hex::decode(
        "0x6330363834636235336331363831343865616130313363333864316330663339",
    )?;
    let mut logs = vec![
        unsupported,
        wrapped(&[b"Alice", b"eth"], wrapper, 123, 1),
        owner(node(&[b"eth"]), b"Alice", holder, current, 123, 2),
    ];
    let mut parent = node(&[b"eth"]);
    for index in 0..31 {
        let label = format!("unknown{index}");
        logs.push(owner(
            parent,
            label.as_bytes(),
            holder,
            current,
            123,
            index + 3,
        ));
        parent = keccak256([parent.as_slice(), keccak256(label.as_bytes()).as_slice()].concat());
    }
    logs.push(owner(parent, b"longleaf", holder, current, 123, 34));
    let long_node = keccak256([parent.as_slice(), keccak256(b"longleaf").as_slice()].concat());
    store_logs(database, &logs).await?;
    run(engine, 123, 123, bigname_interpret::RunMode::Normal).await?;
    publish(database, 123).await?;
    import_labels(database, &["longleaf"]).await?;

    let supported: bool = sqlx::query_scalar(
        "SELECT search_supported FROM project_name_summary WHERE logical_name_id=$1",
    )
    .bind(format!("ens:{:#x}", node(&[b"a"])))
    .fetch_one(&database.pool)
    .await?;
    assert!(!supported);
    assert_bounded_posting_collection(database, &prefix_filter(), 1).await?;
    let page = get(database, "/v1/search?q=a&namespace=ens&page_size=1").await?;
    assert_eq!(&page["data"][0], first, "eligibility must precede LIMIT");
    let shadow: String =
        sqlx::query_scalar("SELECT visibility_state FROM name_surfaces WHERE logical_name_id=$1")
            .bind(format!("ens:{:#x}", node(&[b"Alice", b"eth"])))
            .fetch_one(&database.pool)
            .await?;
    assert_eq!(shadow, "shadow");
    assert_eq!(
        get(database, "/v1/search?q=alice&namespace=ens").await?["data"],
        json!([]),
        "mixed-case raw bytes must stay absent after registry reobservation"
    );
    let long: (i16, String) = sqlx::query_as(
        "SELECT spelling_class,name FROM name_search_documents WHERE logical_name_id=$1",
    )
    .bind(format!("ens:{long_node:#x}"))
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(long.0, 2);
    assert!(long.1.len() > 2000);
    let long_rows =
        assert_search_lean_pages_match(database, &lean_filter("ens", "longleaf"), 1).await?;
    assert_eq!(long_rows.len(), 1);
    let long_http = get(database, "/v1/search?q=longleaf&namespace=ens&page_size=1").await?;
    assert_eq!(long_http["data"], json!(long_rows));
    assert_eq!(long_http["data"][0]["name"], long.1);
    Ok(())
}

#[tokio::test]
async fn dense_raw_prefix_uses_bounded_posting_collection_and_complete_pages() -> Result<()> {
    dense_prefix_fixture(true).await
}

#[tokio::test]
async fn dense_structural_prefix_uses_bounded_posting_collection_and_complete_pages() -> Result<()>
{
    dense_prefix_fixture(false).await
}
