// `include=total_count` on the address-names ownership relations above the candidate cap: the
// count composes every candidate only while the address has at most the opt-in cap of them, and
// reports `null` above it without composing them. Both caps are lowered through the storage
// seams: 65 names, a candidate cap of none, and an opt-in cap of 65 or 64.

const OPT_IN_NAMES: usize = 65;

async fn with_opt_in_cap<F: std::future::Future>(cap: usize, future: F) -> F::Output {
    bigname_storage::families::records::seams::with_exact_total_opt_in_cap(cap, future).await
}

/// The payload of `uri` read with no candidate cap and an opt-in cap of `opt_in`, with the
/// paths it took and the size of every composition batch. The request is boxed, so the test's
/// own future stays small enough for the test thread's stack.
async fn opt_in_page(
    database: &TestDatabase,
    uri: &str,
    opt_in: usize,
) -> Result<(Value, Vec<&'static str>, Vec<usize>)> {
    let request = Box::pin(with_paths(bulk_payload(database, uri)));
    let ((payload, paths), batches) = Box::pin(with_batches(with_exact_total_cap(
        0,
        with_opt_in_cap(opt_in, request),
    )))
    .await;
    Ok((payload?, paths, batches))
}

#[tokio::test]
async fn address_names_opt_in_count_is_null_above_its_cap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, OPT_IN_NAMES).await?;
    let names = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=1");
    let counted = format!("{names}&include=total_count");
    let every = OPT_IN_NAMES;
    let over = OPT_IN_NAMES - 1;

    // The walk without the flag serves a null total and composes about a page.
    let (unasked, paths, batches) = opt_in_page(&database, &names, every).await?;
    assert_eq!(paths, ["walk"]);
    assert_eq!(unasked["page"]["total_count"], Value::Null, "{unasked}");
    assert!(batches.iter().sum::<usize>() <= 32, "{batches:?}");

    // At the opt-in cap the flag composes every candidate once for an exact count, then walks
    // about a page.
    let (exact, paths, batches) = opt_in_page(&database, &counted, every).await?;
    assert_eq!(paths, ["walk"]);
    assert_eq!(exact["page"]["total_count"], json!(OPT_IN_NAMES), "{exact}");
    let composed = batches.iter().sum::<usize>();
    assert!((OPT_IN_NAMES..=OPT_IN_NAMES + 32).contains(&composed), "{batches:?}");
    assert_eq!(page_body(&exact), page_body(&unasked));

    // One candidate over it the flag composes nothing for the count, and the page is the
    // one served without the flag.
    let (capped, paths, batches) = opt_in_page(&database, &counted, over).await?;
    assert_eq!(paths, ["walk"]);
    assert_eq!(capped["page"]["total_count"], Value::Null, "{capped}");
    assert!(batches.iter().sum::<usize>() <= 32, "{batches:?}");
    assert_eq!(page_body(&capped), page_body(&unasked));

    // Registration grouping, and the manager's count of `.eth` registrations, are
    // null over the cap and exact at it, where the exact value is the full read's.
    for query in [
        "dedupe=registration",
        "relation=owner&parent=eth&dedupe=registration",
    ] {
        let uri = format!("{counted}&{query}");
        let full = Box::pin(bulk_payload(&database, &uri)).await?;
        assert!(full["page"]["total_count"].is_u64(), "{query}: {full}");
        let (exact, _, _) = opt_in_page(&database, &uri, every).await?;
        assert_eq!(exact["page"]["total_count"], full["page"]["total_count"], "{query}");
        let (capped, paths, _) = opt_in_page(&database, &uri, over).await?;
        assert_eq!(paths, ["walk"], "{query}");
        assert_eq!(capped["page"]["total_count"], Value::Null, "{query}: {capped}");
        assert_eq!(page_body(&capped), page_body(&full), "{query}");
    }

    // `sort=created_at` still composes every candidate for its order over the cap; only
    // the count is dropped.
    let (created, _, batches) = opt_in_page(&database, &format!("{counted}&sort=created_at"), over).await?;
    assert_eq!(created["page"]["total_count"], Value::Null, "{created}");
    assert!(batches.iter().sum::<usize>() >= OPT_IN_NAMES, "{batches:?}");

    // A namespace holding none of the candidates is read in full with an exact zero.
    let (ens, _, _) = opt_in_page(&database, &format!("{counted}&namespace=ens"), over).await?;
    assert_eq!(ens["page"]["total_count"], Value::Null, "{ens}");
    let (none, paths, _) = opt_in_page(&database, &format!("{counted}&namespace=basenames"), over).await?;
    assert_eq!(paths, ["exact"]);
    assert_eq!(none["page"]["total_count"], json!(0), "{none}");

    // The flag stays refused on `relation=resolves_to`.
    let (status, refused) = read_family_response(
        &database,
        &format!("/v1/addresses/{BULK_ADDRESS}/names?relation=resolves_to&include=total_count"),
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");
    assert_eq!(refused["error"]["code"], json!("invalid_input"), "{refused}");

    // The candidates are gathered in the requested namespace, so the opt-in cap compares that
    // namespace's candidates. The address index also lists five names with no `ens` surface:
    // 70 candidates in all, 65 in `ens`, under an opt-in cap of 65.
    let others: Vec<String> = (0..5).map(|n| format!("basenames:0x{n:064x}")).collect();
    sqlx::query(
        "INSERT INTO bigname_phase.project_address_name_index
            (address, logical_name_id, relation, chain_id)
         SELECT $1, id, 'token_holder', 'ethereum-mainnet' FROM unnest($2::text[]) AS id",
    )
    .bind(BULK_ADDRESS)
    .bind(&others)
    .execute(&database.pool)
    .await?;
    let indexed: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT logical_name_id) FROM bigname_phase.project_address_name_index
         WHERE address = $1",
    )
    .bind(BULK_ADDRESS)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(indexed, i64::try_from(OPT_IN_NAMES + others.len())?);
    let (ens, paths, _) = opt_in_page(&database, &format!("{counted}&namespace=ens"), every).await?;
    assert_eq!(paths, ["walk"]);
    assert_eq!(ens["page"]["total_count"], json!(OPT_IN_NAMES), "{ens}");
    database.cleanup().await
}

/// A walk that gives way to the full read over a stale stored sort key serves the count
/// only at the opt-in cap, as the walk would have.
#[tokio::test]
async fn address_names_opt_in_count_after_a_walk_fallback_follows_its_cap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, OPT_IN_NAMES).await?;
    let logical = phase_logical_identity("ens", "bulk00050.eth")?.0;
    let updated = sqlx::query(
        "UPDATE bigname_phase.project_name_summary SET expires_at = 1 WHERE logical_name_id = $1",
    )
    .bind(&logical)
    .execute(&database.pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);
    let uri = format!(
        "/v1/addresses/{BULK_ADDRESS}/names?sort=expires_at&page_size=10&include=total_count"
    );
    for (opt_in, total) in [(OPT_IN_NAMES, json!(OPT_IN_NAMES)), (OPT_IN_NAMES - 1, Value::Null)] {
        let (payload, paths, _) = opt_in_page(&database, &uri, opt_in).await?;
        assert_eq!(paths, ["fallback"], "opt-in cap {opt_in}");
        assert_eq!(payload["page"]["total_count"], total, "opt-in cap {opt_in}: {payload}");
    }
    database.cleanup().await
}
