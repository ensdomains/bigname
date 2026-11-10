// The stored listing over the route's fixtures, against the composed full read, and the listing
// reader's own rules: keyset positions across writes, undated keys, a name on two chains, a name
// past the btree row limit, one snapshot for the page and its total, and the plans of its
// statements. The helpers are `v2_address_names_listing.rs`'s and `v2_address_names_bounded.rs`'s.

/// `uri` read on the listing equals the full read: ascending pages as the full read serves them
/// and its first page's total, descending rows as the exact reverse of ascending, every request
/// reading the listing.
async fn assert_listing_matches_the_full_read(database: &TestDatabase, uri: &str) -> Result<()> {
    let requests = address_name_test_requests(uri);
    let (asc_uri, desc_uri) = if uri.contains("order=desc") {
        (uri.replace("order=desc", "order=asc"), Some(uri.to_owned()))
    } else {
        (uri.to_owned(), None)
    };
    let oracle = listing_oracle(walk_all_pages(database, &asc_uri)).await?;
    let asc = listing_pages(database, &asc_uri).await?;
    for read in &asc {
        assert_eq!(read.paths, vec!["listing"; requests], "{asc_uri}");
    }
    let asc: Vec<Value> = asc.into_iter().map(|read| read.payload).collect();
    assert_eq!(
        oracle.iter().map(page_body).collect::<Vec<_>>(),
        asc.iter().map(page_body).collect::<Vec<_>>(),
        "{asc_uri}"
    );
    assert_eq!(
        asc[0]["page"]["total_count"], oracle[0]["page"]["total_count"],
        "{asc_uri}"
    );
    if let Some(desc_uri) = desc_uri {
        let desc = listing_pages(database, &desc_uri).await?;
        for read in &desc {
            assert_eq!(read.paths, vec!["listing"; requests], "{desc_uri}");
        }
        let desc: Vec<Value> = desc.into_iter().map(|read| read.payload).collect();
        let mut reversed = listing_rows(&asc);
        reversed.reverse();
        assert_eq!(listing_rows(&desc), reversed, "{desc_uri}");
    }
    Ok(())
}

/// The served names of `rows`, in order.
fn listing_names(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .map(|row| row["name"].as_str().expect("a name").to_owned())
        .collect()
}

/// The served names of every page of `uri` on the listing, in order.
async fn listing_walk_names(database: &TestDatabase, uri: &str) -> Result<Vec<String>> {
    let pages = listing_pages(database, uri).await?;
    let pages: Vec<Value> = pages.into_iter().map(|read| read.payload).collect();
    Ok(listing_names(&listing_rows(&pages)))
}

/// Every filter, grouping and sort of the rich fixture, at three page sizes.
#[tokio::test]
async fn the_rich_fixture_lists_as_the_full_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    for query in RICH_FIXTURE_QUERIES {
        for page_size in [1, 2, 50] {
            let uri = format!("/v1/addresses/{V2_ADDRESS}/names?page_size={page_size}&{query}");
            assert_listing_matches_the_full_read(&database, &uri).await?;
        }
    }
    let former = format!("/v1/addresses/{V2_ADDRESS}/names?page_size=1&relation=former_owner");
    assert_listing_matches_the_full_read(&database, &former).await?;
    database.cleanup().await
}

#[tokio::test]
async fn the_roles_fixture_lists_as_the_full_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_role_holder(&database, json!(["renew"])).await?;
    for address in [ROLE_HOLDER, V2_ADDRESS] {
        for query in ROLES_FIXTURE_QUERIES {
            let uri = format!("/v1/addresses/{address}/names?page_size=1&{query}");
            assert_listing_matches_the_full_read(&database, &uri).await?;
        }
    }
    database.cleanup().await
}

/// Surface-less registry children are listed with what their parent's subnames route serves,
/// past the read filter by their publication alone.
#[tokio::test]
async fn registry_children_list_as_the_full_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_children_fixture(&database).await?;
    let children: Vec<String> = sqlx::query_scalar(
        "SELECT display_name FROM bigname_phase.project_address_listing
         WHERE address = $1 AND listed AND registry_child",
    )
    .bind(RC_OWNER)
    .fetch_all(&database.pool)
    .await?;
    assert!(!children.is_empty());
    for query in REGISTRY_CHILDREN_QUERIES {
        let uri = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=2&{query}");
        assert_listing_matches_the_full_read(&database, &uri).await?;
    }
    let uri = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=50");
    let listed = listing_names(&listing_rows(&[listing_read(&database, &uri)
        .await?
        .payload]));
    assert!(
        children.iter().any(|child| listed.contains(child)),
        "{children:?} {listed:?}"
    );
    database.cleanup().await
}

/// Two names bound to one registration each, read by registration with and without filters.
#[tokio::test]
async fn registrations_list_as_the_full_read() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_names(&database, 12, |_| BULK_ADDRESS, &[(4, 5), (6, 9)]).await?;
    for query in [
        "dedupe=registration",
        "dedupe=registration&order=desc",
        "dedupe=registration&q=bulk0000",
        "dedupe=registration&relation=owner&sort=expires_at",
        "dedupe=registration&sort=created_at&order=desc",
        "dedupe=registration&sort=registered_at&include=total_count",
        "dedupe=name&q=bulk0000&include=total_count",
    ] {
        for page_size in [1, 3] {
            let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size={page_size}&{query}");
            assert_listing_matches_the_full_read(&database, &uri).await?;
        }
    }
    database.cleanup().await
}

/// The route reads the stored listing for every ownership relation, at any candidate count.
#[tokio::test]
async fn the_route_reads_the_stored_listing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 5).await?;
    for query in [
        "relation=owner",
        "relation=manager",
        "relation=role_holder",
        "relation=any",
        "relation=owner&dedupe=registration&sort=created_at",
        "relation=former_owner",
    ] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?{query}");
        let read = with_exact_total_cap(0, listing_read(&database, &uri)).await?;
        assert_eq!(read.paths, ["listing"], "{uri}");
    }
    database.cleanup().await
}

/// Copies `rows` of `address`'s listing (those `filter`, an SQL condition on `l`, keeps) into
/// `bigname_phase.listing_stash` and removes them from the listing.
async fn stash_listing_rows(database: &TestDatabase, filter: &str) -> Result<()> {
    sqlx::raw_sql(&format!(
        "CREATE TABLE IF NOT EXISTS bigname_phase.listing_stash
             (LIKE bigname_phase.project_address_listing);
         INSERT INTO bigname_phase.listing_stash
         SELECT * FROM bigname_phase.project_address_listing l WHERE {filter};
         DELETE FROM bigname_phase.project_address_listing l WHERE {filter}"
    ))
    .execute(&database.pool)
    .await?;
    Ok(())
}

async fn restore_listing_rows(database: &TestDatabase) -> Result<()> {
    sqlx::raw_sql(
        "INSERT INTO bigname_phase.project_address_listing
         SELECT * FROM bigname_phase.listing_stash;
         DELETE FROM bigname_phase.listing_stash",
    )
    .execute(&database.pool)
    .await?;
    Ok(())
}

fn bulk_name_filter(indexes: &[usize]) -> Result<String> {
    let ids = indexes
        .iter()
        .map(|index| Ok(format!("'{}'", bulk_logical_name_id(*index)?)))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!(
        "l.address = '{BULK_ADDRESS}' AND l.logical_name_id IN ({})",
        ids.join(", ")
    ))
}

/// A cursor names a position, not a row: rows written before and after it between two pages, and
/// the removal of its own row, leave the next page continuing from where the first ended.
#[tokio::test]
async fn a_cursor_survives_a_write_between_pages() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    // Names 4 to 7 spell `bulk0000N`, which sort before the four emoji names.
    seed_bulk_address_names(&database, 8).await?;
    stash_listing_rows(&database, &bulk_name_filter(&[4, 7])?).await?;
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?sort=name&page_size=2");
    let first = listing_read(&database, &uri).await?.payload;
    assert_eq!(
        listing_names(&listing_rows(std::slice::from_ref(&first))),
        [
            format!("{}.eth", bulk_label(5)),
            format!("{}.eth", bulk_label(6))
        ]
    );
    let cursor = first["page"]["next_cursor"].as_str().expect("a cursor");
    restore_listing_rows(&database).await?;
    stash_listing_rows(&database, &bulk_name_filter(&[6])?).await?;
    let mut names = Vec::new();
    let mut cursor = Some(cursor.to_owned());
    while let Some(after) = cursor {
        let page = listing_read(&database, &format!("{uri}&cursor={after}"))
            .await?
            .payload;
        names.extend(listing_names(&listing_rows(std::slice::from_ref(&page))));
        cursor = page["page"]["next_cursor"].as_str().map(str::to_owned);
    }
    assert_eq!(names.len(), 5, "{names:?}");
    assert_eq!(names[0], format!("{}.eth", bulk_label(7)), "{names:?}");
    for gone in [4, 6] {
        let gone = format!("{}.eth", bulk_label(gone));
        assert!(!names.contains(&gone), "{gone} in {names:?}");
    }
    database.cleanup().await
}

/// A name with no expiry is keyed `-1`: first ascending and last descending, and a cursor on
/// either side of the boundary continues across it.
#[tokio::test]
async fn undated_rows_sort_first_ascending_and_last_descending() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 8).await?;
    let undated = sqlx::query(&format!(
        "UPDATE bigname_phase.project_address_listing l SET expires_key = -1 WHERE {}",
        bulk_name_filter(&[4, 5])?
    ))
    .execute(&database.pool)
    .await?;
    assert_eq!(undated.rows_affected(), 2);
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?sort=expires_at&page_size=1");
    let asc = listing_walk_names(&database, &format!("{uri}&order=asc")).await?;
    let mut desc = listing_walk_names(&database, &format!("{uri}&order=desc")).await?;
    assert_eq!(asc.len(), 8, "{asc:?}");
    let mut first: Vec<String> = asc[..2].to_vec();
    first.sort();
    assert_eq!(
        first,
        [
            format!("{}.eth", bulk_label(4)),
            format!("{}.eth", bulk_label(5))
        ]
    );
    desc.reverse();
    assert_eq!(asc, desc);
    database.cleanup().await
}

/// A name with rows on two chains (synthetic: Project writes none, see the listing's module
/// docs) is one entry with both rows' relations, and the total counts it once, wherever a page
/// ends.
#[tokio::test]
async fn a_name_on_two_chains_is_one_entry() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 5).await?;
    let both = format!("/v1/addresses/{BULK_ADDRESS}/names?relation=owner,manager&page_size=10");
    let before = listing_read(&database, &both).await?.payload;
    let name = bulk_logical_name_id(2)?;
    let served = format!("{}.eth", bulk_label(2));
    let relations = before["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["name"] == json!(served))
        .expect("the name")["relations"]
        .clone();
    sqlx::query(
        "INSERT INTO bigname_phase.project_address_listing
            (chain_id, address, logical_name_id, namespace, display_key, display_name,
             normalized_name, parent_normalized_name, namehash, resource_id, token_holder,
             effective_controller, role_holder, listed, former_owner, registry_child,
             registry_parent_logical_name_id, registration_lead, public_authority, migrated,
             expires_key, registered_key, created_key, former_expires_key, token_holder_block,
             effective_controller_block, role_holder_block)
         SELECT 'base-mainnet', l.address, l.logical_name_id, l.namespace, l.display_key,
                l.display_name, l.normalized_name, l.parent_normalized_name, l.namehash,
                other.resource_id, FALSE, TRUE, FALSE, TRUE, FALSE, FALSE, NULL, FALSE,
                l.public_authority, l.migrated, l.expires_key, l.registered_key, l.created_key,
                l.former_expires_key, NULL, l.effective_controller_block, NULL
         FROM bigname_phase.project_address_listing l
         JOIN bigname_phase.project_address_listing other
           ON other.address = l.address AND other.logical_name_id = $3
         WHERE l.address = $1 AND l.logical_name_id = $2",
    )
    .bind(BULK_ADDRESS)
    .bind(&name)
    .bind(bulk_logical_name_id(3)?)
    .execute(&database.pool)
    .await?;
    let kept = sqlx::query(
        "UPDATE bigname_phase.project_address_listing SET effective_controller = FALSE
         WHERE chain_id = 'ethereum-mainnet' AND address = $1 AND logical_name_id = $2",
    )
    .bind(BULK_ADDRESS)
    .bind(&name)
    .execute(&database.pool)
    .await?;
    assert_eq!(kept.rows_affected(), 1);
    for page_size in 1..=5 {
        for order in ["asc", "desc"] {
            let uri = format!(
                "/v1/addresses/{BULK_ADDRESS}/names?relation=owner,manager&page_size={page_size}\
                 &order={order}&include=total_count"
            );
            let pages = listing_pages(&database, &uri).await?;
            let pages: Vec<Value> = pages.into_iter().map(|read| read.payload).collect();
            let rows = listing_rows(&pages);
            assert_eq!(rows.len(), 5, "{uri}");
            let entry: Vec<&Value> = rows
                .iter()
                .filter(|row| row["name"] == json!(served))
                .collect();
            assert_eq!(entry.len(), 1, "{uri}");
            assert_eq!(entry[0]["relations"], relations, "{uri}");
            assert_eq!(pages[0]["page"]["total_count"], json!(5), "{uri}");
        }
    }
    database.cleanup().await
}

/// A name past 2,000 bytes is keyed by its first 500 characters: its name cursor carries the cut
/// key, and `q` of its first 120 characters ranges on the first 100 and checks the rest.
#[tokio::test]
async fn a_name_past_the_btree_row_limit_pages_by_its_cut_key() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 6).await?;
    let long = format!("{}.eth", "a".repeat(2_100));
    let cut: String = long.chars().take(500).collect();
    let renamed = sqlx::query(&format!(
        "UPDATE bigname_phase.project_address_listing l
         SET normalized_name = $1, display_name = $1, display_key = $2 WHERE {}",
        bulk_name_filter(&[5])?
    ))
    .bind(&long)
    .bind(&cut)
    .execute(&database.pool)
    .await?;
    assert_eq!(renamed.rows_affected(), 1);
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?sort=name&page_size=1");
    let names = listing_walk_names(&database, &format!("{uri}&order=asc")).await?;
    assert_eq!(names.len(), 6, "{names:?}");
    assert_eq!(names[0], long);
    let mut reversed = names.clone();
    reversed.reverse();
    assert_eq!(
        listing_walk_names(&database, &format!("{uri}&order=desc")).await?,
        reversed
    );
    let q: String = long.chars().take(120).collect();
    let matched = listing_read(
        &database,
        &format!("/v1/addresses/{BULK_ADDRESS}/names?q={q}&include=total_count"),
    )
    .await?
    .payload;
    assert_eq!(
        listing_names(&listing_rows(std::slice::from_ref(&matched))),
        std::slice::from_ref(&long)
    );
    assert_eq!(matched["page"]["total_count"], json!(1));
    let past: String = format!("{}b", "a".repeat(119));
    let unmatched = listing_read(
        &database,
        &format!("/v1/addresses/{BULK_ADDRESS}/names?q={past}"),
    )
    .await?
    .payload;
    assert_eq!(unmatched["data"], json!([]), "{unmatched}");
    database.cleanup().await
}

/// A row written between the page and its total is in neither: both read one snapshot.
#[tokio::test]
async fn the_page_and_the_count_read_one_snapshot() -> Result<()> {
    use bigname_storage::families::records::seams::with_listing_page_hook;

    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 5).await?;
    let pool = database.pool.clone();
    let written = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook_written = written.clone();
    let hook: bigname_storage::families::records::seams::ListingPageHook =
        std::sync::Arc::new(move || {
            let pool = pool.clone();
            let written = hook_written.clone();
            let step: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                Box::pin(async move {
                    if written.swap(true, std::sync::atomic::Ordering::SeqCst) {
                        return;
                    }
                    sqlx::query(
                        "INSERT INTO bigname_phase.project_address_listing
                     SELECT chain_id, address, 'ens:listing-late', namespace, display_key,
                            display_name, normalized_name, parent_normalized_name, namehash,
                            resource_id, token_holder, effective_controller, role_holder, listed,
                            former_owner, registry_child, registry_parent_logical_name_id, FALSE,
                            public_authority, migrated, expires_key, registered_key, created_key,
                            former_expires_key, token_holder_block, effective_controller_block,
                            role_holder_block, unhealed_labelhashes
                     FROM bigname_phase.project_address_listing
                     WHERE address = $1 ORDER BY logical_name_id LIMIT 1",
                    )
                    .bind(BULK_ADDRESS)
                    .execute(&pool)
                    .await
                    .expect("the late row");
                });
            step
        });
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=10&include=total_count");
    let read = with_listing_page_hook(hook, listing_read(&database, &uri)).await?;
    assert!(written.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(read.payload["page"]["total_count"], json!(5));
    let after = listing_read(&database, &uri).await?;
    assert_eq!(after.payload["page"]["total_count"], json!(6));
    database.cleanup().await
}

/// Copies every listing row of `address` to `copies` other addresses, so the table holds the
/// address among many, then analyzes it.
async fn spread_listing_rows(database: &TestDatabase, address: &str, copies: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO bigname_phase.project_address_listing
         SELECT l.chain_id, '0x' || lpad(to_hex(1000000 + n), 40, '0'), l.logical_name_id,
                l.namespace, l.display_key, l.display_name, l.normalized_name,
                l.parent_normalized_name, l.namehash, l.resource_id, l.token_holder,
                l.effective_controller, l.role_holder, l.listed, l.former_owner,
                l.registry_child, l.registry_parent_logical_name_id, l.registration_lead,
                l.public_authority, l.migrated, l.expires_key, l.registered_key, l.created_key,
                l.former_expires_key, l.token_holder_block, l.effective_controller_block,
                l.role_holder_block, l.unhealed_labelhashes
         FROM bigname_phase.project_address_listing l, generate_series(1, $2::bigint) AS n
         WHERE l.address = $1",
    )
    .bind(address)
    .bind(copies)
    .execute(&database.pool)
    .await?;
    sqlx::raw_sql("ANALYZE bigname_phase.project_address_listing")
        .execute(&database.pool)
        .await?;
    Ok(())
}

/// Every node of a plan outside its subplans, which hold the read filter's probes.
fn listing_plan_nodes(plan: &Value) -> Vec<&Value> {
    fn walk<'a>(node: &'a Value, nodes: &mut Vec<&'a Value>) {
        nodes.push(node);
        for child in node["Plans"].as_array().into_iter().flatten() {
            if !matches!(
                child["Parent Relationship"].as_str(),
                Some("SubPlan" | "InitPlan")
            ) {
                walk(child, nodes);
            }
        }
    }
    let mut nodes = Vec::new();
    walk(&plan[0]["Plan"], &mut nodes);
    nodes
}

/// The plans of the listing statements `uri` ran, by statement.
async fn listing_plans(
    database: &TestDatabase,
    uri: &str,
) -> Result<(
    ListingRead,
    Vec<bigname_storage::families::records::seams::ListingPlan>,
)> {
    use bigname_storage::families::records::seams::with_listing_plans;

    let plans = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let read = with_listing_plans(plans.clone(), listing_read(database, uri)).await?;
    let plans = plans.lock().unwrap().clone();
    Ok((read, plans))
}

/// The scan of the listing a statement reads its rows from, aliased `l`, keyed by the address:
/// an index scan, or a bitmap scan for a total. No plan reads the listing by a sequential scan
/// or reads the address index.
fn listing_scan<'a>(plan: &'a Value, context: &str) -> &'a Value {
    let nodes = listing_plan_nodes(plan);
    for node in &nodes {
        assert_ne!(
            node["Relation Name"],
            json!("project_address_name_index"),
            "{context}: {plan}"
        );
        if node["Relation Name"] == json!("project_address_listing") {
            assert_ne!(node["Node Type"], json!("Seq Scan"), "{context}: {plan}");
        }
    }
    let scan = nodes
        .into_iter()
        .find(|node| {
            node["Relation Name"] == json!("project_address_listing") && node["Alias"] == json!("l")
        })
        .unwrap_or_else(|| panic!("{context}: no listing scan: {plan}"));
    let condition = if scan["Node Type"] == json!("Bitmap Heap Scan") {
        scan["Recheck Cond"].as_str()
    } else {
        scan["Index Cond"].as_str()
    };
    assert!(
        condition.is_some_and(|condition| condition.contains("address")),
        "{context}: {plan}"
    );
    scan
}

/// A page is a walk of its sort's index: a `Limit` over the index scan, forward ascending and
/// backward descending, the cursor's position in the index condition, and no sort, with the
/// values and generically. An address with no row reads a handful of buffers.
#[tokio::test]
async fn the_page_walks_the_sort_index() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 30).await?;
    spread_listing_rows(&database, BULK_ADDRESS, 70).await?;
    for (sort, index) in [
        ("name", "project_address_listing_name_idx"),
        ("expires_at", "project_address_listing_expires_idx"),
        ("registered_at", "project_address_listing_registered_idx"),
        ("created_at", "project_address_listing_created_idx"),
    ] {
        for (order, direction) in [("asc", "Forward"), ("desc", "Backward")] {
            let uri =
                format!("/v1/addresses/{BULK_ADDRESS}/names?sort={sort}&order={order}&page_size=5");
            let (first, plans) = listing_plans(&database, &uri).await?;
            let cursor = first.payload["page"]["next_cursor"]
                .as_str()
                .expect("a second page")
                .to_owned();
            let (_, next) = listing_plans(&database, &format!("{uri}&cursor={cursor}")).await?;
            for (plans, positioned) in [(plans, false), (next, true)] {
                let pages: Vec<_> = plans
                    .iter()
                    .filter(|plan| plan.statement == "page")
                    .collect();
                assert!(!pages.is_empty(), "{uri}");
                for plan in pages {
                    for (mode, plan) in [("custom", &plan.custom), ("generic", &plan.generic)] {
                        let context = format!("{uri} {mode} positioned {positioned}");
                        assert_eq!(plan[0]["Plan"]["Node Type"], json!("Limit"), "{context}");
                        assert!(
                            listing_plan_nodes(plan).iter().all(|node| {
                                !node["Node Type"]
                                    .as_str()
                                    .is_some_and(|kind| kind.ends_with("Sort"))
                            }),
                            "{context}: {plan}"
                        );
                        let scan = listing_scan(plan, &context);
                        assert_eq!(scan["Index Name"], json!(index), "{context}: {plan}");
                        assert_eq!(
                            scan["Scan Direction"],
                            json!(direction),
                            "{context}: {plan}"
                        );
                        if positioned {
                            assert!(
                                scan["Index Cond"]
                                    .as_str()
                                    .is_some_and(|condition| condition.contains("ROW(")),
                                "{context}: {plan}"
                            );
                        }
                    }
                }
            }
        }
    }
    let uri =
        format!("/v1/addresses/{LISTING_EMPTY_ADDRESS}/names?page_size=5&include=total_count");
    let (read, plans) = listing_plans(&database, &uri).await?;
    assert_eq!(read.payload["data"], json!([]));
    for plan in plans.iter().filter(|plan| plan.statement == "page") {
        let root = &plan.custom[0]["Plan"];
        let buffers = root["Shared Hit Blocks"].as_i64().unwrap_or_default()
            + root["Shared Read Blocks"].as_i64().unwrap_or_default();
        assert!(buffers <= 10, "{buffers} buffers: {}", plan.custom);
    }
    database.cleanup().await
}

/// The filters, both totals and the event anchors read the address's range of the listing by an
/// index, never by a sequential scan or the address index. The bounded total stops at the cap.
#[tokio::test]
async fn the_filters_totals_and_anchors_read_the_address_range() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 30).await?;
    spread_listing_rows(&database, BULK_ADDRESS, 70).await?;
    for query in [
        "q=bulk0001&include=total_count",
        "q=ulk&match=contains",
        "parent=eth&include=total_count",
        "dedupe=registration&include=total_count",
        "relation=owner&namespace=ens",
    ] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=5&{query}");
        let (_, plans) = listing_plans(&database, &uri).await?;
        assert!(plans.iter().any(|plan| plan.statement == "page"), "{uri}");
        for plan in &plans {
            for (mode, explained) in [("custom", &plan.custom), ("generic", &plan.generic)] {
                listing_scan(explained, &format!("{uri} {} {mode}", plan.statement));
            }
        }
    }
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=5");
    let (read, plans) = with_exact_total_cap(10, listing_plans(&database, &uri)).await?;
    assert_eq!(read.payload["page"]["total_count"], Value::Null);
    let bounded = plans
        .iter()
        .find(|plan| plan.statement == "bounded_total")
        .expect("the bounded total");
    let scan = listing_scan(&bounded.custom, "bounded total");
    assert!(
        scan["Actual Rows"]
            .as_f64()
            .is_some_and(|rows| rows <= 11.0),
        "{}",
        bounded.custom
    );
    assert!(
        listing_plan_nodes(&bounded.custom)
            .iter()
            .any(|node| node["Node Type"] == json!("Limit")),
        "{}",
        bounded.custom
    );
    let published: std::collections::BTreeMap<String, i64> = sqlx::query_as(
        "SELECT chain_id, current_block_number FROM bigname_phase.project_family_marker
         WHERE current_block_number IS NOT NULL",
    )
    .fetch_all(&database.pool)
    .await?
    .into_iter()
    .collect();
    let plans = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let anchors = bigname_storage::families::records::seams::with_listing_plans(
        plans.clone(),
        bigname_storage::load_stored_address_anchors(
            &database.pool,
            BULK_ADDRESS,
            None,
            None,
            &published,
        ),
    )
    .await?;
    assert_eq!(anchors.len(), 30);
    let plans = plans.lock().unwrap().clone();
    let anchor = plans
        .iter()
        .find(|plan| plan.statement == "anchors")
        .expect("the anchors statement");
    for (mode, explained) in [("custom", &anchor.custom), ("generic", &anchor.generic)] {
        listing_scan(explained, &format!("anchors {mode}"));
    }
    database.cleanup().await
}

/// The former-owner page is a `Limit` over the former-owner index, with no sort.
#[tokio::test]
async fn the_former_page_walks_the_former_index() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_former_cursor_names(&database, true).await?;
    spread_listing_rows(&database, FORMER_CURSOR_HOLDER, 300).await?;
    for order in ["asc", "desc"] {
        let uri = format!("{}&order={order}&page_size=1", former_cursor_route());
        let (first, plans) = listing_plans(&database, &uri).await?;
        let cursor = first.payload["page"]["next_cursor"]
            .as_str()
            .expect("a second page")
            .to_owned();
        let (_, next) = listing_plans(&database, &format!("{uri}&cursor={cursor}")).await?;
        for plan in plans.iter().chain(&next) {
            assert_eq!(plan.statement, "former_page");
            for (mode, explained) in [("custom", &plan.custom), ("generic", &plan.generic)] {
                let context = format!("{uri} {mode}");
                assert_eq!(
                    explained[0]["Plan"]["Node Type"],
                    json!("Limit"),
                    "{context}"
                );
                assert!(
                    listing_plan_nodes(explained).iter().all(|node| {
                        !node["Node Type"]
                            .as_str()
                            .is_some_and(|kind| kind.ends_with("Sort"))
                    }),
                    "{context}: {explained}"
                );
                let scan = listing_scan(explained, &context);
                assert_eq!(
                    scan["Index Name"],
                    json!("project_address_listing_former_owner_idx"),
                    "{context}: {explained}"
                );
            }
        }
    }
    database.cleanup().await
}

// Review findings on 24bc4ada3, red before their fix.

/// The filter key a descending ownership cursor binds its tie order under.
const LISTING_TIE_ORDER_KEY: &str = "tie_order";

/// The next cursor of `uri`'s first page, decoded.
async fn listing_next_cursor(
    database: &TestDatabase,
    uri: &str,
) -> Result<crate::v2::CursorPayload> {
    let page = v2_address_names_payload_for_database(database, uri).await?;
    let cursor = page["page"]["next_cursor"]
        .as_str()
        .context("a next cursor")?;
    crate::v2::decode(cursor).ok().context("the cursor decodes")
}

/// C-D1: a descending cursor in the shape the reader before this change issued (no tie-order
/// key) is refused, for every sort: it was issued under ascending ties.
#[tokio::test]
async fn a_descending_cursor_without_the_tie_order_is_refused() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 7).await?;
    for sort in ["name", "expires_at", "registered_at", "created_at"] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?sort={sort}&order=desc&page_size=2");
        let mut payload = listing_next_cursor(&database, &uri).await?;
        payload.filters.remove(LISTING_TIE_ORDER_KEY);
        let old = crate::v2::encode(&payload);
        let (status, body) =
            bulk_listing_response(&database, &format!("{uri}&cursor={old}")).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{sort}: {body}");
    }
    database.cleanup().await
}

/// C-D2: an ascending cursor keeps the shape it had before this change and continues.
#[tokio::test]
async fn an_ascending_cursor_keeps_its_shape_and_continues() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 7).await?;
    for sort in ["name", "expires_at", "registered_at", "created_at"] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?sort={sort}&page_size=2");
        let payload = listing_next_cursor(&database, &uri).await?;
        assert!(
            !payload.filters.contains_key(LISTING_TIE_ORDER_KEY),
            "{sort}: {:?}",
            payload.filters
        );
        assert_listing_matches_the_full_read(&database, &uri).await?;
    }
    database.cleanup().await
}

/// C-D3: a descending cursor the listing issues binds its tie order and walks the exact reverse
/// of ascending.
#[tokio::test]
async fn a_descending_cursor_binds_its_tie_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 7).await?;
    for sort in ["name", "expires_at", "registered_at", "created_at"] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?sort={sort}&order=desc&page_size=2");
        let payload = listing_next_cursor(&database, &uri).await?;
        assert!(
            payload.filters.contains_key(LISTING_TIE_ORDER_KEY),
            "{sort}: {:?}",
            payload.filters
        );
        assert_listing_matches_the_full_read(&database, &uri).await?;
    }
    database.cleanup().await
}

/// Gives the textless `first.alpha.eth` of the bound-paging fixture raw bytes witnessed at 241,
/// above the published 240, as Interpret does in place; `visible` false gives it bytes that
/// fail normalization, which shadow the surface.
async fn observe_first_raw_name(database: &TestDatabase, first: &str, visible: bool) -> Result<()> {
    let labels = if visible {
        ["first", "alpha", "eth"]
    } else {
        ["FIRST", "alpha", "eth"]
    };
    let mut dns = Vec::new();
    for label in labels {
        dns.push(u8::try_from(label.len())?);
        dns.extend(label.as_bytes());
    }
    dns.push(0);
    let witness = "listing-first-preimage";
    sqlx::query(
        "UPDATE name_surfaces SET raw_name = $2, raw_labels = $3, dns_encoded_name = $4,
             preimage_event_identity = $5
         WHERE logical_name_id = $1 AND raw_name IS NULL",
    )
    .bind(first)
    .bind(labels.join("."))
    .bind(labels.map(str::to_owned).to_vec())
    .bind(&dns)
    .bind(witness)
    .execute(&database.pool)
    .await?;
    if !visible {
        sqlx::query(
            "UPDATE name_surfaces SET visibility_state = 'shadow',
                 normalization_errors = '[{\"error\": \"label is not normalized\"}]',
                 deactivation_reason = 'normalization_gate', deactivated_at = to_timestamp(1700000241)
             WHERE logical_name_id = $1",
        )
        .bind(first)
        .execute(&database.pool)
        .await?;
    }
    let mut observed = family_event(
        witness,
        Some(first),
        None,
        "PreimageObserved",
        "ens_v1_registry_l1",
        241,
        0,
        json!({"raw_labels": labels}),
    );
    observed.before_state = json!({});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[observed]).await?;
    Ok(())
}

/// The owner's page lists the textless name under its placeholder.
async fn assert_first_is_listed_textless(database: &TestDatabase, uri: &str) -> Result<()> {
    let page = v2_address_names_payload_for_database(database, uri).await?;
    let names = listing_names(&listing_rows(std::slice::from_ref(&page)));
    assert!(
        names
            .iter()
            .any(|name| name.ends_with(".alpha.eth") && name.starts_with('[')),
        "precondition, the textless name is listed: {names:?}"
    );
    Ok(())
}

/// 4522787155: a raw name Interpret observes above the publication shows on the page at once, as
/// the full read shows it, until Project publishes the block.
#[tokio::test]
async fn a_raw_name_observed_above_the_publication_shows_on_the_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_bound_paging(&database).await?;
    let uri = format!("/v1/addresses/{RC_OWNER}/names?page_size=50");
    assert_first_is_listed_textless(&database, &uri).await?;
    observe_first_raw_name(&database, &paging.first, true).await?;
    let page = v2_address_names_payload_for_database(&database, &uri).await?;
    let full = listing_oracle(v2_address_names_payload_for_database(&database, &uri)).await?;
    assert_eq!(page_body(&page), page_body(&full), "{uri}");
    assert!(
        listing_names(&listing_rows(std::slice::from_ref(&page)))
            .contains(&"first.alpha.eth".to_owned()),
        "{page}"
    );
    database.cleanup().await
}

/// 4522787170: bytes observed above the publication that fail normalization shadow the surface.
/// The full read drops the name at once, and so does the page.
#[tokio::test]
async fn a_name_shadowed_above_the_publication_leaves_the_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_bound_paging(&database).await?;
    let uri = format!("/v1/addresses/{RC_OWNER}/names?page_size=50");
    assert_first_is_listed_textless(&database, &uri).await?;
    observe_first_raw_name(&database, &paging.first, false).await?;
    let page = v2_address_names_payload_for_database(&database, &uri).await?;
    let full = listing_oracle(v2_address_names_payload_for_database(&database, &uri)).await?;
    assert_eq!(page["data"], full["data"], "{uri}");
    database.cleanup().await
}

/// 4522787131: a label known only from `label_preimages` (an import, or a chain observation
/// Project has not published) shows in the served name. The full read filters and sorts by the
/// served name; the listing by its stored spelling.
#[tokio::test]
async fn a_label_known_only_from_its_preimage_is_filtered_by_the_served_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bound_paging(&database).await?;
    let base = format!("/v1/addresses/{RC_OWNER}/names?page_size=50");
    assert_first_is_listed_textless(&database, &base).await?;
    insert_family_label_preimage(&database.pool, b"first").await?;
    let mut red = Vec::new();
    for query in [
        "&q=first",
        "&q=irst&match=contains",
        "&parent=alpha.eth",
        "&sort=name",
    ] {
        let uri = format!("{base}{query}");
        let page = v2_address_names_payload_for_database(&database, &uri).await?;
        let full = listing_oracle(v2_address_names_payload_for_database(&database, &uri)).await?;
        if page["data"] != full["data"] {
            red.push(format!(
                "{query}: listing {:?} full {:?}",
                listing_names(&listing_rows(std::slice::from_ref(&page))),
                listing_names(&listing_rows(std::slice::from_ref(&full)))
            ));
        }
    }
    assert!(red.is_empty(), "{red:#?}");
    database.cleanup().await
}
