// `include=total_count` on the address-names ownership relations: the route counts the stored
// address listing's rows, so the total is exact however many candidate names the address has.
// The opt-in cap of the composed walk (`with_exact_total_opt_in_cap`) applies to that walk only,
// which stays as the oracle the listing is compared with. These rows lower both caps through the
// storage seams, 65 names under an opt-in cap of 64 and a candidate cap of none, and check that
// the served total is still the full read's.

const OPT_IN_NAMES: usize = 65;

async fn with_opt_in_cap<F: std::future::Future>(cap: usize, future: F) -> F::Output {
    bigname_storage::families::records::seams::with_exact_total_opt_in_cap(cap, future).await
}

/// The route's read of `uri` with no candidate cap and an opt-in cap one below the names.
async fn capped_listing_read(database: &TestDatabase, uri: &str) -> Result<ListingRead> {
    Box::pin(with_exact_total_cap(
        0,
        with_opt_in_cap(OPT_IN_NAMES - 1, listing_read(database, uri)),
    ))
    .await
}

#[tokio::test]
async fn address_names_count_is_exact_above_the_walks_opt_in_cap() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, OPT_IN_NAMES).await?;
    let names = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=1");
    let counted = format!("{names}&include=total_count");

    // Above the walk's opt-in cap the route reads the listing and counts every stored row.
    let unasked = capped_listing_read(&database, &names).await?;
    assert_listing_bound(&unasked, &names);
    let exact = capped_listing_read(&database, &counted).await?;
    assert_listing_bound(&exact, &counted);
    assert_eq!(
        exact.payload["page"]["total_count"],
        json!(OPT_IN_NAMES),
        "{}",
        exact.payload
    );
    assert_eq!(page_body(&exact.payload), page_body(&unasked.payload));

    // Registration grouping, the manager's `.eth` registrations and `sort=created_at` count
    // exactly too, and serve the full read's page.
    for query in [
        "dedupe=registration",
        "relation=owner&parent=eth&dedupe=registration",
        "sort=created_at",
    ] {
        let uri = format!("{counted}&{query}");
        let full = listing_oracle(bulk_payload(&database, &uri)).await?;
        assert!(full["page"]["total_count"].is_u64(), "{query}: {full}");
        let read = capped_listing_read(&database, &uri).await?;
        assert_listing_bound(&read, &uri);
        assert_eq!(
            read.payload["page"]["total_count"], full["page"]["total_count"],
            "{query}"
        );
        assert_eq!(page_body(&read.payload), page_body(&full), "{query}");
    }

    // A namespace holding none of the names counts an exact zero.
    let none = capped_listing_read(&database, &format!("{counted}&namespace=basenames")).await?;
    assert_eq!(
        none.payload["page"]["total_count"],
        json!(0),
        "{}",
        none.payload
    );

    // The flag stays refused on `relation=resolves_to`.
    let (status, refused) = read_family_response(
        &database,
        &format!("/v1/addresses/{BULK_ADDRESS}/names?relation=resolves_to&include=total_count"),
    )
    .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");
    assert_eq!(
        refused["error"]["code"],
        json!("invalid_input"),
        "{refused}"
    );

    // The count is of the requested namespace's rows: five more names in the address index
    // with no `ens` surface leave the `ens` total at 65.
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
    let ens = capped_listing_read(&database, &format!("{counted}&namespace=ens")).await?;
    assert_eq!(
        ens.payload["page"]["total_count"],
        json!(OPT_IN_NAMES),
        "{}",
        ens.payload
    );
    database.cleanup().await
}

/// A stale stored sort key no longer sends the read to a fallback: the listing serves the page
/// and the exact total.
#[tokio::test]
async fn address_names_count_over_a_stale_sort_key_is_exact() -> Result<()> {
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
    let read = capped_listing_read(&database, &uri).await?;
    assert_eq!(read.paths, ["listing"], "{uri}");
    assert_eq!(
        read.payload["page"]["total_count"],
        json!(OPT_IN_NAMES),
        "{}",
        read.payload
    );
    database.cleanup().await
}
