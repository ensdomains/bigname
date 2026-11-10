// The address-names route over the stored address listing: holders whose candidate names far
// outnumber the names they are served compose only their page, and every page and total equals
// the composed full read's. The fixtures and helpers are `v2_address_names_bounded.rs`'s.
//
// Each holder shape has three kinds of candidate: names the holder is served, names another
// address holds that the address index also lists for the holder, and index rows on names with
// no surface. The counts are shapes, not measurements.

const LISTING_CHAIN: &str = "ethereum-mainnet";
const LISTING_EMPTY_ADDRESS: &str = "0x00000000000000000000000000000000000e0e0e";

/// One request on the route: its payload, the name count of every composition it made and the
/// path each address-names read took.
struct ListingRead {
    payload: Value,
    batches: Vec<usize>,
    paths: Vec<&'static str>,
}

/// The route's answer read by the composed full read, which every listing page must equal.
async fn listing_oracle<F: std::future::Future>(future: F) -> F::Output {
    with_exact_total_cap(usize::MAX, future).await
}

async fn listing_read(database: &TestDatabase, uri: &str) -> Result<ListingRead> {
    let ((payload, batches), paths) = with_paths(with_batches(bulk_payload(database, uri))).await;
    Ok(ListingRead {
        payload: payload?,
        batches,
        paths,
    })
}

/// Every page of `uri`, each read on its own.
async fn listing_pages(database: &TestDatabase, uri: &str) -> Result<Vec<ListingRead>> {
    let separator = if uri.contains('?') { '&' } else { '?' };
    let mut pages = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page_uri = match &cursor {
            Some(cursor) => format!("{uri}{separator}cursor={cursor}"),
            None => uri.to_owned(),
        };
        let read = listing_read(database, &page_uri).await?;
        cursor = read.payload["page"]["next_cursor"]
            .as_str()
            .map(str::to_owned);
        pages.push(read);
        if cursor.is_none() || pages.len() == 1000 {
            return Ok(pages);
        }
    }
}

fn listing_page_size(uri: &str) -> usize {
    uri.split(['?', '&'])
        .find_map(|part| part.strip_prefix("page_size="))
        .map_or(50, |size| size.parse().expect("page_size"))
}

/// A request composed at most its page and the row after it, and read the listing.
fn assert_listing_bound(read: &ListingRead, uri: &str) {
    let composed: usize = read.batches.iter().sum();
    assert!(
        composed <= listing_page_size(uri) + 1,
        "{uri}: composed {composed} names: {:?}",
        read.batches
    );
    assert_eq!(read.paths, ["listing"], "{uri}");
}

fn listing_rows(pages: &[Value]) -> Vec<Value> {
    pages
        .iter()
        .flat_map(|page| page["data"].as_array().expect("rows").clone())
        .collect()
}

/// Every page of `uri` in ascending order is the full read's, each within the bound, and the
/// descending walk is its exact reverse. Returns the ascending pages.
async fn assert_listing_both_ways(database: &TestDatabase, uri: &str) -> Result<Vec<Value>> {
    let asc_uri = format!("{uri}&order=asc");
    let desc_uri = format!("{uri}&order=desc");
    let oracle = listing_oracle(walk_all_pages(database, &asc_uri)).await?;
    let asc_reads = listing_pages(database, &asc_uri).await?;
    let desc_reads = listing_pages(database, &desc_uri).await?;
    let asc: Vec<Value> = asc_reads.iter().map(|read| read.payload.clone()).collect();
    let desc: Vec<Value> = desc_reads.iter().map(|read| read.payload.clone()).collect();
    assert_eq!(
        oracle.iter().map(page_body).collect::<Vec<_>>(),
        asc.iter().map(page_body).collect::<Vec<_>>(),
        "{asc_uri}"
    );
    let mut reversed = listing_rows(&asc);
    reversed.reverse();
    assert_eq!(
        listing_rows(&desc),
        reversed,
        "{desc_uri}: descending is not the exact reverse of ascending"
    );
    for read in &asc_reads {
        assert_listing_bound(read, &asc_uri);
    }
    for read in &desc_reads {
        assert_listing_bound(read, &desc_uri);
    }
    Ok(asc)
}

/// Lists `count` ids with no name surface for `address` in the address index, numbered from
/// `first`. Today's reads compose each such id; the listing holds no row for any of them.
async fn list_listing_index_only(
    database: &TestDatabase,
    address: &str,
    first: usize,
    count: usize,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_address_name_index
            (address, logical_name_id, relation, chain_id)
         SELECT $1, 'ens:listing-index-only-' || n, 'token_holder', $2
         FROM generate_series($3::bigint, $4::bigint) AS n",
    )
    .bind(address)
    .bind(LISTING_CHAIN)
    .bind(first as i64)
    .bind((first + count) as i64 - 1)
    .execute(&database.pool)
    .await?;
    Ok(())
}

/// A holder shape: `served` bulk names held by [`BULK_ADDRESS`] (indexes below `served`, chosen by
/// `owner`), `unserved` more held by [`BULK_OTHER_ADDRESS`] and listed for the holder, and
/// `index_only` ids with no surface.
async fn seed_listing_holder(
    database: &TestDatabase,
    served: usize,
    unserved: usize,
    index_only: usize,
    owner: fn(usize) -> &'static str,
) -> Result<()> {
    seed_bulk_names(database, served + unserved, owner, &[]).await?;
    if unserved > 0 {
        list_bulk_names_without_a_relation(database, served..served + unserved).await?;
    }
    if index_only > 0 {
        list_listing_index_only(database, BULK_ADDRESS, 0, index_only).await?;
    }
    Ok(())
}

const EIGHTH_SERVED: usize = 92;
const THIRD_SERVED: usize = 420;
const NEARLY_ALL_SERVED: usize = 1_010;

fn eighth_owner(index: usize) -> &'static str {
    bulk_owner_below_count(index, EIGHTH_SERVED)
}

fn third_owner(index: usize) -> &'static str {
    bulk_owner_below_count(index, THIRD_SERVED)
}

fn nearly_all_owner(index: usize) -> &'static str {
    bulk_owner_below_count(index, NEARLY_ALL_SERVED)
}

fn none_served_owner(_index: usize) -> &'static str {
    BULK_OTHER_ADDRESS
}

fn bulk_owner_below_count(index: usize, served: usize) -> &'static str {
    if index < served {
        BULK_ADDRESS
    } else {
        BULK_OTHER_ADDRESS
    }
}

/// Pages and totals of `uri` with and without `include=total_count`, against the full read.
async fn assert_listing_pages_and_total(
    database: &TestDatabase,
    uri: &str,
    total_without_count: Option<u64>,
) -> Result<()> {
    for counted in [false, true] {
        let uri = if counted {
            format!("{uri}&include=total_count")
        } else {
            uri.to_owned()
        };
        let oracle = listing_oracle(walk_all_pages(database, &uri)).await?;
        let pages = listing_pages(database, &uri).await?;
        for read in &pages {
            assert_listing_bound(read, &uri);
        }
        let pages: Vec<Value> = pages.into_iter().map(|read| read.payload).collect();
        assert_eq!(
            oracle.iter().map(page_body).collect::<Vec<_>>(),
            pages.iter().map(page_body).collect::<Vec<_>>(),
            "{uri}"
        );
        let expected = if counted {
            oracle[0]["page"]["total_count"].clone()
        } else {
            json!(total_without_count)
        };
        assert_eq!(pages[0]["page"]["total_count"], expected, "{uri}");
    }
    Ok(())
}

/// An eighth of the candidates served, below the old candidate cap.
#[tokio::test]
async fn an_eighth_served_holder_pages_by_the_listing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_listing_holder(&database, EIGHTH_SERVED, 20, 620, eighth_owner).await?;
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?relation=any&sort=name&page_size=50");
    assert_listing_pages_and_total(&database, &uri, Some(EIGHTH_SERVED as u64)).await?;
    database.cleanup().await
}

#[tokio::test]
async fn an_eighth_served_holder_counts_from_the_listing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_listing_holder(&database, EIGHTH_SERVED, 20, 620, eighth_owner).await?;
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?relation=owner&page_size=50");
    let counted = format!("{uri}&include=total_count");
    let oracle = listing_oracle(bulk_payload(&database, &counted)).await?;
    for uri in [&counted, &uri] {
        let read = listing_read(&database, uri).await?;
        assert_listing_bound(&read, uri);
        assert_eq!(
            read.payload["page"]["total_count"], oracle["page"]["total_count"],
            "{uri}"
        );
    }
    assert_eq!(oracle["page"]["total_count"], json!(EIGHTH_SERVED));
    database.cleanup().await
}

/// A third served, above the old candidate cap.
#[tokio::test]
async fn a_third_served_holder_pages_and_counts_by_the_listing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_listing_holder(&database, THIRD_SERVED, 120, 717, third_owner).await?;
    let uri = format!(
        "/v1/addresses/{BULK_ADDRESS}/names?relation=owner&page_size=50&include=total_count"
    );
    let oracle = listing_oracle(walk_all_pages(&database, &uri)).await?;
    let pages = listing_pages(&database, &uri).await?;
    for read in &pages {
        assert_listing_bound(read, &uri);
    }
    let pages: Vec<Value> = pages.into_iter().map(|read| read.payload).collect();
    assert_eq!(
        oracle.iter().map(page_body).collect::<Vec<_>>(),
        pages.iter().map(page_body).collect::<Vec<_>>()
    );
    assert_eq!(pages[0]["page"]["total_count"], json!(THIRD_SERVED));
    database.cleanup().await
}

#[tokio::test]
async fn a_third_served_holder_walks_every_sort_both_ways() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_listing_holder(&database, THIRD_SERVED, 120, 717, third_owner).await?;
    for sort in ["name", "expires_at", "registered_at", "created_at"] {
        let uri =
            format!("/v1/addresses/{BULK_ADDRESS}/names?relation=any&sort={sort}&page_size=50");
        let asc = assert_listing_both_ways(&database, &uri).await?;
        assert_eq!(listing_rows(&asc).len(), THIRD_SERVED, "{uri}");
    }
    database.cleanup().await
}

/// Nearly every candidate served, above the cap: without the count the total is null.
#[tokio::test]
async fn a_nearly_all_served_holder_pages_with_and_without_the_count() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_listing_holder(&database, NEARLY_ALL_SERVED, 20, 15, nearly_all_owner).await?;
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?relation=any&sort=name&page_size=50");
    assert_listing_pages_and_total(&database, &uri, None).await?;
    database.cleanup().await
}

#[tokio::test]
async fn a_nearly_all_served_holder_counts_with_a_one_row_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_listing_holder(&database, NEARLY_ALL_SERVED, 20, 15, nearly_all_owner).await?;
    let uri = format!(
        "/v1/addresses/{BULK_ADDRESS}/names?relation=owner&page_size=1&include=total_count"
    );
    let oracle = listing_oracle(bulk_payload(&database, &uri)).await?;
    let read = listing_read(&database, &uri).await?;
    assert_listing_bound(&read, &uri);
    assert_eq!(page_body(&read.payload), page_body(&oracle));
    assert_eq!(
        read.payload["page"]["total_count"],
        json!(NEARLY_ALL_SERVED)
    );
    database.cleanup().await
}

/// An empty page with an exact zero total, no composition and no role read, with and without
/// the count, for each of `relations`.
async fn assert_listing_serves_nothing(
    database: &TestDatabase,
    address: &str,
    requests: &[&str],
) -> Result<()> {
    use bigname_storage::families::records::seams::with_role_read_counter;

    for request in requests {
        for count in ["", "&include=total_count"] {
            let uri = format!("/v1/addresses/{address}/names?{request}{count}");
            let reads = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let read = with_role_read_counter(reads.clone(), listing_read(database, &uri)).await?;
            assert_eq!(read.payload["data"], json!([]), "{uri}");
            assert_eq!(read.payload["page"]["total_count"], json!(0), "{uri}");
            assert_eq!(read.payload["page"]["has_more"], json!(false), "{uri}");
            assert_eq!(
                read.batches.iter().sum::<usize>(),
                0,
                "{uri}: {:?}",
                read.batches
            );
            assert!(
                reads.lock().unwrap().is_empty(),
                "{uri}: {:?}",
                reads.lock().unwrap()
            );
            assert_eq!(read.paths, ["listing"], "{uri}");
        }
    }
    Ok(())
}

/// A former holder of thousands of names it no longer holds, with no role candidates.
#[tokio::test]
async fn a_zero_served_holder_answers_an_empty_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_listing_holder(&database, 0, 1_200, 5_764, none_served_owner).await?;
    assert_listing_serves_nothing(
        &database,
        BULK_ADDRESS,
        &[
            "relation=any&sort=name&page_size=200",
            "sort=name&page_size=200",
            "relation=owner&page_size=1",
        ],
    )
    .await?;
    database.cleanup().await
}

/// A registry-owner-shaped holder: a candidate on every name, served none.
#[tokio::test]
async fn a_registry_owner_with_no_served_name_answers_in_a_bounded_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_listing_holder(&database, 0, 300, 60_000, none_served_owner).await?;
    assert_listing_serves_nothing(
        &database,
        BULK_ADDRESS,
        &["relation=owner&page_size=50", "relation=any&page_size=50"],
    )
    .await?;
    database.cleanup().await
}

/// The same holder at ten times the candidates, for a manual run.
#[tokio::test]
#[ignore = "seeds 600,000 index rows; run by hand"]
async fn a_registry_owner_with_no_served_name_answers_in_a_bounded_read_at_production_scale()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_listing_holder(&database, 0, 300, 600_000, none_served_owner).await?;
    assert_listing_serves_nothing(
        &database,
        BULK_ADDRESS,
        &["relation=owner&page_size=50", "relation=any&page_size=50"],
    )
    .await?;
    database.cleanup().await
}

#[tokio::test]
async fn an_address_with_no_names_answers_empty() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 5).await?;
    assert_listing_serves_nothing(
        &database,
        LISTING_EMPTY_ADDRESS,
        &[
            "page_size=10",
            "relation=owner",
            "relation=manager",
            "relation=role_holder",
            "relation=any&dedupe=registration&sort=created_at",
        ],
    )
    .await?;
    let former = bulk_payload(
        &database,
        &format!("/v1/addresses/{LISTING_EMPTY_ADDRESS}/names?relation=former_owner"),
    )
    .await?;
    assert_eq!(former["data"], json!([]), "{former}");
    assert_eq!(former["page"]["total_count"], Value::Null, "{former}");
    database.cleanup().await
}

/// Seven names whose creation and registration times repeat: a page boundary falls inside each
/// tie group, and descending order reverses the group as well.
#[tokio::test]
async fn a_tie_group_splits_across_pages_in_both_directions() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 7).await?;
    for sort in ["created_at", "registered_at", "expires_at", "name"] {
        for dedupe in ["name", "registration"] {
            let uri = format!(
                "/v1/addresses/{BULK_ADDRESS}/names?sort={sort}&dedupe={dedupe}&page_size=2"
            );
            let asc = assert_listing_both_ways(&database, &uri).await?;
            assert_eq!(listing_rows(&asc).len(), 7, "{uri}");
        }
    }
    database.cleanup().await
}

/// A name hidden by the read filter after its publication (its surface, its surface's block or
/// its resource no longer canonical) leaves the page, and the stored total still counts it.
#[tokio::test]
async fn a_hidden_name_leaves_the_page_and_stays_in_the_count() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // Five names, each created in its own block (`bulk_blocks`).
    seed_bulk_address_names(&database, 5).await?;
    let surface = bulk_logical_name_id(1)?;
    sqlx::query(
        "UPDATE bigname_phase.name_surfaces SET canonicality_state = 'orphaned'
         WHERE logical_name_id = $1",
    )
    .bind(&surface)
    .execute(&database.pool)
    .await?;
    let lineage = sqlx::query(
        "UPDATE bigname_phase.chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_hash = (
             SELECT block_hash FROM bigname_phase.name_surfaces WHERE logical_name_id = $2)",
    )
    .bind(LISTING_CHAIN)
    .bind(bulk_logical_name_id(2)?)
    .execute(&database.pool)
    .await?;
    assert_eq!(lineage.rows_affected(), 1);
    let resource = sqlx::query(
        "UPDATE bigname_phase.resources SET canonicality_state = 'orphaned'
         WHERE resource_id = (
             SELECT resource_id FROM bigname_phase.project_address_listing
             WHERE address = $1 AND logical_name_id = $2)",
    )
    .bind(BULK_ADDRESS)
    .bind(bulk_logical_name_id(3)?)
    .execute(&database.pool)
    .await?;
    assert_eq!(resource.rows_affected(), 1);
    for count in ["", "&include=total_count"] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=10{count}");
        let read = listing_read(&database, &uri).await?;
        let listed: Vec<&str> = read.payload["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["name"].as_str().unwrap())
            .collect();
        assert_eq!(listed.len(), 2, "{uri}: {listed:?}");
        assert!(
            listed
                .iter()
                .all(|name| name.starts_with(&bulk_label(0)) || name.starts_with(&bulk_label(4))),
            "{uri}: {listed:?}"
        );
        assert_eq!(read.payload["page"]["total_count"], json!(5), "{uri}");
        assert_eq!(read.paths, ["listing"], "{uri}");
    }
    database.cleanup().await
}

/// The cap counts listed entries, not candidates: a holder with the cap's number of names and
/// more candidates has an exact total without asking.
#[tokio::test]
async fn a_holder_at_the_cap_counts_without_asking() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_listing_holder(&database, 10, 20, 0, |index| {
        bulk_owner_below_count(index, 10)
    })
    .await?;
    for count in ["", "&include=total_count"] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=3{count}");
        let read = with_exact_total_cap(10, listing_read(&database, &uri)).await?;
        assert_eq!(read.payload["page"]["total_count"], json!(10), "{uri}");
        assert_listing_bound(&read, &uri);
    }
    database.cleanup().await
}

#[tokio::test]
async fn a_holder_over_the_cap_counts_only_when_asked() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_listing_holder(&database, 11, 20, 0, |index| {
        bulk_owner_below_count(index, 11)
    })
    .await?;
    for (count, total) in [("", Value::Null), ("&include=total_count", json!(11))] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=3{count}");
        let read = with_exact_total_cap(10, listing_read(&database, &uri)).await?;
        assert_eq!(read.payload["page"]["total_count"], total, "{uri}");
    }
    database.cleanup().await
}

/// A cursor the full read issued continues on the listing with the full read's next page.
#[tokio::test]
async fn cursors_issued_before_the_listing_continue_on_it() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 30).await?;
    for sort in ["name", "expires_at", "registered_at", "created_at"] {
        for dedupe in ["name", "registration"] {
            let uri = format!(
                "/v1/addresses/{BULK_ADDRESS}/names?sort={sort}&dedupe={dedupe}&page_size=7"
            );
            let first = listing_oracle(bulk_payload(&database, &uri)).await?;
            let cursor = first["page"]["next_cursor"]
                .as_str()
                .expect("a second page");
            let next = format!("{uri}&cursor={cursor}");
            let expected = listing_oracle(bulk_payload(&database, &next)).await?;
            let read = listing_read(&database, &next).await?;
            assert_eq!(page_body(&read.payload), page_body(&expected), "{next}");
            assert_listing_bound(&read, &next);
        }
    }
    database.cleanup().await
}

/// The read checks the publication of each chain the address has rows on, or of every chain
/// when it has none: a chain being rebuilt does not refuse an address it holds nothing on.
#[tokio::test]
async fn the_publication_check_reads_the_chains_the_address_has_rows_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 5).await?;
    let unservable = sqlx::query(
        "UPDATE bigname_phase.project_family_marker SET input_content_hash = 'another build'
         WHERE chain_id = 'base-mainnet'",
    )
    .execute(&database.pool)
    .await?;
    assert_eq!(unservable.rows_affected(), 1);
    let held = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=10");
    let read = listing_read(&database, &held).await?;
    assert_eq!(read.payload["data"].as_array().unwrap().len(), 5);
    assert_eq!(read.paths, ["listing"]);
    let (status, payload) = bulk_listing_response(
        &database,
        &format!("/v1/addresses/{LISTING_EMPTY_ADDRESS}/names"),
    )
    .await?;
    assert_eq!(status, StatusCode::CONFLICT, "{payload}");
    assert_eq!(payload["error"]["code"], json!("stale"), "{payload}");
    database.cleanup().await
}

/// A stored row on a chain no publication covers is neither listed nor counted.
#[tokio::test]
async fn a_row_on_a_chain_with_no_publication_is_not_served() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 5).await?;
    let copied = sqlx::query(
        "INSERT INTO bigname_phase.project_address_listing
         SELECT 'unmarked-chain', address, 'ens:listing-unmarked', namespace,
                'aaa-unmarked.eth', 'aaa-unmarked.eth', 'aaa-unmarked.eth', 'eth', namehash,
                resource_id, token_holder, effective_controller, role_holder, listed,
                former_owner, registry_child, registry_parent_logical_name_id,
                registration_lead, public_authority, migrated, expires_key, registered_key,
                created_key, former_expires_key, token_holder_block,
                effective_controller_block, role_holder_block, unhealed_labelhashes
         FROM bigname_phase.project_address_listing
         WHERE address = $1 AND logical_name_id = $2",
    )
    .bind(BULK_ADDRESS)
    .bind(bulk_logical_name_id(4)?)
    .execute(&database.pool)
    .await?;
    assert_eq!(copied.rows_affected(), 1);
    for query in [
        "page_size=10&include=total_count",
        "page_size=10",
        "page_size=2&sort=name",
    ] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?{query}");
        let oracle = listing_oracle(walk_all_pages(&database, &uri)).await?;
        let pages = listing_pages(&database, &uri).await?;
        let pages: Vec<Value> = pages.into_iter().map(|read| read.payload).collect();
        assert_eq!(
            oracle.iter().map(page_body).collect::<Vec<_>>(),
            pages.iter().map(page_body).collect::<Vec<_>>(),
            "{uri}"
        );
        assert_eq!(pages[0]["page"]["total_count"], json!(5), "{uri}");
    }
    database.cleanup().await
}

/// A former holder of a handful of names with many more candidates: the former-owner page reads
/// its own index and composes only the page.
#[tokio::test]
async fn a_former_holder_pages_by_the_former_index() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_former_cursor_names(&database, true).await?;
    list_listing_index_only(&database, FORMER_CURSOR_HOLDER, 0, 1_200).await?;
    let base = former_cursor_route();
    for query in [
        "order=asc&page_size=200",
        "order=desc&page_size=200",
        "order=asc&page_size=1",
        "order=desc&page_size=1",
        "order=asc&page_size=1&expires_after=2026-01-01T00:00:01Z",
        "order=desc&page_size=2&expires_before=2026-01-01T00:00:03Z",
        "order=asc&page_size=1&parent=eth",
    ] {
        let uri = format!("{base}&{query}");
        let oracle = listing_oracle(walk_all_pages(&database, &uri)).await?;
        let pages = listing_pages(&database, &uri).await?;
        for read in &pages {
            let composed: usize = read.batches.iter().sum();
            assert!(
                composed <= listing_page_size(&uri) + 1,
                "{uri}: composed {composed}: {:?}",
                read.batches
            );
        }
        let pages: Vec<Value> = pages.into_iter().map(|read| read.payload).collect();
        assert_eq!(
            oracle.iter().map(page_body).collect::<Vec<_>>(),
            pages.iter().map(page_body).collect::<Vec<_>>(),
            "{uri}"
        );
        assert!(!listing_rows(&pages).is_empty(), "{uri}");
    }
    database.cleanup().await
}

async fn bulk_listing_response(database: &TestDatabase, uri: &str) -> Result<(StatusCode, Value)> {
    let response = v2_address_names_response_for_database(database, uri).await?;
    let status = response.status();
    Ok((status, read_json::<Value>(response).await?))
}
