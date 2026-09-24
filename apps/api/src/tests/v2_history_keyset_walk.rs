// Whole-walk equality for history walks (docs/api-v2-routes.md, "Shared Route Rules"): on a
// fixed dataset, paging one row at a time returns exactly the unpaged ordered collection, in
// both directions, on every history route. Rows share a log position so only `event_identity`
// orders them, one row has no transaction hash or log index, and name history also merges
// direct child registrations. On a changing dataset the walk continues from the saved position:
// a deleted anchor is skipped, a moved anchor is met again where it now sorts, and a walk whose
// remaining rows are gone ends empty.

/// `history.eth` plus rows that stress the order: two rows at the exact log position of
/// `history-record`, one row without transaction hash or log index, and two direct child
/// registrations, one at the log position of `history-authority-epoch`.
async fn hkw_seed(database: &TestDatabase) -> Result<()> {
    seed_v2_history_fixture(database).await?;
    seed_child_surfaces(database, &["c1.history.eth", "c2.history.eth"]).await?;
    let mut loose = v2_history_event("hk-loose", Some("ens:history.eth"), None, "RecordChanged", 104);
    loose.transaction_hash = None;
    loose.log_index = None;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            v2_history_event("hk-tie-a", Some("ens:history.eth"), None, "RecordChanged", 106),
            v2_history_event("hk-tie-b", Some("ens:history.eth"), None, "ResolverChanged", 106),
            loose,
            v2_history_event(
                "hk-child-1",
                Some("ens:c1.history.eth"),
                None,
                "RegistrationGranted",
                105,
            ),
            v2_history_event(
                "hk-child-2",
                Some("ens:c2.history.eth"),
                None,
                "RegistrationGranted",
                108,
            ),
        ],
    )
    .await?;
    seed_child_registration_memberships(database, "history.eth", &["hk-child-1", "hk-child-2"])
        .await
}

/// The unpaged collection: every row id in order, and its `total_count`.
async fn hkw_baseline(database: &TestDatabase, base: &str) -> Result<(Vec<String>, Value)> {
    let page = hk_ok(database, &format!("{base}&page_size=200")).await?;
    anyhow::ensure!(page["page"]["has_more"] == json!(false), "{base}: baseline must fit");
    Ok((hk_ids(&page), page["page"]["total_count"].clone()))
}

/// The cursor after `pages` one-row pages from the start.
async fn hkw_cursor_after(database: &TestDatabase, base: &str, pages: usize) -> Result<String> {
    let mut cursor = hk_next_cursor(&hk_ok(database, &format!("{base}&page_size=1")).await?)?;
    for _ in 1..pages {
        cursor = hk_next_cursor(
            &hk_ok(database, &format!("{base}&page_size=1&cursor={cursor}")).await?,
        )?;
    }
    Ok(cursor)
}

/// Every row from `cursor` on, one row per page, checking each page's `total_count`.
async fn hkw_rest(
    database: &TestDatabase,
    base: &str,
    cursor: Option<String>,
    total_count: Option<&Value>,
) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    let mut cursor = cursor;
    loop {
        let uri = match cursor.as_ref() {
            Some(cursor) => format!("{base}&page_size=1&cursor={cursor}"),
            None => format!("{base}&page_size=1"),
        };
        let page = hk_ok(database, &uri).await?;
        if let Some(total_count) = total_count {
            assert_eq!(&page["page"]["total_count"], total_count, "{uri}");
        }
        ids.extend(hk_ids(&page));
        match page["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => return Ok(ids),
        }
    }
}

async fn hkw_move_event(
    database: &TestDatabase,
    event_identity: &str,
    block: i64,
    log_index: i64,
) -> Result<()> {
    let hash = format!("0xhistory{block}");
    let transaction = format!("0xtx{block}");
    let moved = sqlx::query(
        "UPDATE bigname_phase.normalized_events
         SET block_number = $2, block_hash = $3, transaction_hash = $4,
             transaction_index = 0, log_index = $5
         WHERE event_identity = $1",
    )
    .bind(event_identity)
    .bind(block)
    .bind(&hash)
    .bind(&transaction)
    .bind(log_index)
    .execute(&database.pool)
    .await?
    .rows_affected();
    anyhow::ensure!(moved == 1, "{event_identity} must exist");
    sqlx::query(
        "UPDATE bigname_phase.child_registration_events
         SET block_number = $2, block_hash = $3, transaction_order_key = $4, log_order_key = $5,
             target_block_number = $2, target_block_hash = $3
         WHERE event_identity = $1",
    )
    .bind(event_identity)
    .bind(block)
    .bind(&hash)
    .bind(&transaction)
    .bind(log_index)
    .execute(&database.pool)
    .await?;
    Ok(())
}

fn hkw_id(identity: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(identity.as_bytes()))
}

/// The stored event behind an API row id (`history_event_id`: SHA-256 of the identity).
async fn hkw_identity_for_id(database: &TestDatabase, id: &str) -> Result<String> {
    let identities: Vec<String> =
        sqlx::query_scalar("SELECT event_identity FROM bigname_phase.normalized_events")
            .fetch_all(&database.pool)
            .await?;
    identities
        .into_iter()
        .find(|identity| hkw_id(identity) == id)
        .context("row id must name a stored event")
}

#[tokio::test]
async fn v2_history_walks_equal_their_unpaged_collection() -> Result<()> {
    for route in hk_routes() {
        for order in ["desc", "asc"] {
            let database = TestDatabase::new_migrated().await?;
            hkw_seed(&database).await?;
            let base = format!("{route}&order={order}");

            // The unpaged order is itself checked where only `event_identity` separates rows:
            // the three rows at block 106's one log position sort by identity in the direction
            // asked for.
            if !route.starts_with("/v1/addresses/") {
                let (unpaged, _) = hkw_baseline(&database, &base).await?;
                let tied = unpaged
                    .iter()
                    .filter(|id| {
                        ["hk-tie-a", "hk-tie-b", "history-record"]
                            .iter()
                            .any(|identity| hkw_id(identity) == **id)
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let expected = if order == "desc" {
                    ["hk-tie-b", "hk-tie-a", "history-record"]
                } else {
                    ["history-record", "hk-tie-a", "hk-tie-b"]
                };
                assert_eq!(tied, expected.map(hkw_id).to_vec(), "{base}: identity tiebreaker");
            }

            // A fixed dataset: the walk is the collection, with the capped and the exact count.
            let exact = if base.contains("include=child_registrations") {
                base.replace("include=child_registrations", "include=child_registrations,total_count")
            } else {
                format!("{base}&include=total_count")
            };
            for counted in [base.clone(), exact] {
                let (baseline, total_count) = hkw_baseline(&database, &counted).await?;
                assert!(baseline.len() >= 4, "{counted}: {baseline:?}");
                assert_eq!(total_count, json!(baseline.len()), "{counted}");
                let walked = hkw_rest(&database, &counted, None, Some(&total_count)).await?;
                assert_eq!(walked, baseline, "{counted}");
            }
            let (baseline, _) = hkw_baseline(&database, &base).await?;
            // The rows that stress the order are part of the walk being checked.
            let mut stressed = Vec::new();
            if !route.starts_with("/v1/addresses/") {
                stressed.extend(["hk-tie-a", "hk-tie-b", "hk-loose", "history-record"]);
            }
            if route.contains("include=child_registrations") {
                stressed.extend(["hk-child-1", "hk-child-2", "history-authority-epoch"]);
            }
            for identity in stressed {
                assert!(baseline.contains(&hkw_id(identity)), "{base}: {identity}");
            }

            // A deleted anchor: the walk continues with the rows after it.
            let cursor = hkw_cursor_after(&database, &base, 2).await?;
            hk_delete_event(&database, &hk_anchor(&cursor)?).await?;
            let rest = hkw_rest(&database, &base, Some(cursor), None).await?;
            assert_eq!(rest, baseline[2..].to_vec(), "{base}: deleted anchor");
            let deleted = baseline[1].clone();
            let (baseline, _) = hkw_baseline(&database, &base).await?;
            assert!(!baseline.contains(&deleted), "{base}");

            // A moved anchor: the walk continues from the saved position and meets the row
            // again where it now sorts, at the far end of the walk.
            let cursor = hkw_cursor_after(&database, &base, 3).await?;
            let anchor = hk_anchor(&cursor)?;
            let anchor_id = baseline[2].clone();
            let (block, log_index) = if order == "desc" { (101, 7) } else { (111, 7) };
            hkw_move_event(&database, &anchor, block, log_index).await?;
            let (moved, _) = hkw_baseline(&database, &base).await?;
            let rest = hkw_rest(&database, &base, Some(cursor), None).await?;
            let expected = moved
                .iter()
                .filter(|id| baseline[3..].contains(id) || **id == anchor_id)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(rest, expected, "{base}: moved anchor");
            assert!(rest.contains(&anchor_id), "{base}: the moved anchor is met again");

            // An empty remainder: the last row is gone, so the walk ends on an empty page.
            let (baseline, _) = hkw_baseline(&database, &base).await?;
            let cursor = hkw_cursor_after(&database, &base, baseline.len() - 1).await?;
            let last = hkw_identity_for_id(&database, &baseline[baseline.len() - 1]).await?;
            hk_delete_event(&database, &last).await?;
            let page = hk_ok(&database, &format!("{base}&page_size=1&cursor={cursor}")).await?;
            assert_eq!(page["data"], json!([]), "{base}: empty remainder");
            assert_eq!(page["page"]["has_more"], json!(false), "{base}");
            assert_eq!(page["page"]["next_cursor"], Value::Null, "{base}");
            database.cleanup().await?;
        }
    }
    Ok(())
}
