// The history order within one block on every history route (docs/api-v1-routes.md, "Shared
// Route Rules"): transaction index, then log index, then `event_identity`, with a row that has
// no transaction position before every transaction of its block. The cursor carries the
// transaction index. A cursor issued while the order compared transaction hashes is read through
// its anchor row or another event of its transaction, and restarts once when neither exists.

const ORDER_NAME: &str = "ens:history.eth";
/// Block 122's transaction hashes: A's is the greater, B's the smaller, the reverse of their
/// index order.
const ORDER_TX_A: &str = "0xffa";
const ORDER_TX_B: &str = "0x00b";

/// Newest first: block 123, B, A's log 7, A's log 3, the row without a transaction, block 121.
const ORDER_NEWEST_FIRST: [&str; 6] = [
    "order-block-123",
    "order-b",
    "order-a-log-7",
    "order-a-log-3",
    "order-synthesised",
    "order-block-121",
];

/// `history.eth` plus blocks 121 to 123. Block 122 holds transaction A at index 1 with logs 3
/// and 7, transaction B at index 2, and a row with no transaction position. Every row is an
/// owner transfer to the address the address route reads.
async fn order_seed(database: &TestDatabase) -> Result<()> {
    seed_v2_history_fixture(database).await?;
    seed_v2_history_blocks(database, 121..=123).await?;
    let events = [
        ("order-block-121", 121),
        ("order-a-log-3", 122),
        ("order-a-log-7", 122),
        ("order-b", 122),
        ("order-synthesised", 122),
        ("order-block-123", 123),
    ]
    .map(|(identity, block)| {
        v2_history_event(identity, Some(ORDER_NAME), None, "AuthorityTransferred", block)
    });
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    sqlx::query(
        "UPDATE bigname_phase.normalized_events event
         SET transaction_hash = position.transaction_hash,
             transaction_index = position.transaction_index,
             log_index = position.log_index
         FROM (VALUES
             ('order-a-log-3', $1::text, 1::bigint, 3::bigint),
             ('order-a-log-7', $1::text, 1::bigint, 7::bigint),
             ('order-b', $2::text, 2::bigint, 9::bigint),
             ('order-synthesised', NULL::text, NULL::bigint, NULL::bigint)
         ) AS position(event_identity, transaction_hash, transaction_index, log_index)
         WHERE event.event_identity = position.event_identity",
    )
    .bind(ORDER_TX_A)
    .bind(ORDER_TX_B)
    .execute(&database.pool)
    .await?;
    Ok(())
}

fn order_expected(order: &str) -> Vec<String> {
    let mut expected = ORDER_NEWEST_FIRST.map(hkw_id).to_vec();
    if order == "asc" {
        expected.reverse();
    }
    expected
}

/// The fixture's rows among `ids`, in the order given.
fn order_rows(ids: &[String]) -> Vec<String> {
    let ours = ORDER_NEWEST_FIRST.map(hkw_id);
    ids.iter().filter(|id| ours.contains(id)).cloned().collect()
}

/// How a client holding a cursor from an earlier layout would send an issued cursor.
#[derive(Clone, Copy, Debug)]
enum OrderToken {
    /// As issued: the anchor's transaction index.
    Current,
    /// The transaction hash in place of the index, as issued before the order compared indexes.
    TransactionHash,
    /// Both keys, with a hash that names no transaction: read as current.
    Both,
    /// The anchor's numeric id and identity only.
    Legacy,
}

async fn order_token(database: &TestDatabase, cursor: &str, token: OrderToken) -> Result<String> {
    let mut value: Value = serde_json::from_slice(&hex::decode(cursor)?)?;
    let identity = value["last_item"]["event_identity"]
        .as_str()
        .context("cursor anchor")?
        .to_owned();
    let (transaction_hash, transaction_index): (Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT transaction_hash, transaction_index FROM bigname_phase.normalized_events
         WHERE event_identity = $1",
    )
    .bind(&identity)
    .fetch_one(&database.pool)
    .await?;
    let item = value["last_item"].as_object_mut().context("last_item")?;
    match token {
        OrderToken::Current => return Ok(cursor.to_owned()),
        OrderToken::Legacy => return hk_legacy_cursor(database, cursor).await,
        OrderToken::TransactionHash => {
            item.remove("transaction_index");
            if let Some(hash) = transaction_hash {
                item.insert("transaction_hash".to_owned(), json!(hash));
            }
        }
        OrderToken::Both => {
            if let Some(index) = transaction_index {
                item.insert("transaction_index".to_owned(), json!(index.to_string()));
                item.insert("transaction_hash".to_owned(), json!("0xnotatransaction"));
            }
        }
    }
    Ok(hex::encode(serde_json::to_vec(&value)?))
}

/// Every row from the start, one row per page, sending each issued cursor as `token`.
async fn order_walk(database: &TestDatabase, base: &str, token: OrderToken) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let uri = match cursor.as_ref() {
            Some(cursor) => format!("{base}&page_size=1&cursor={cursor}"),
            None => format!("{base}&page_size=1"),
        };
        let page = hk_ok(database, &uri).await?;
        ids.extend(hk_ids(&page));
        match page["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(order_token(database, next, token).await?),
            None => return Ok(ids),
        }
    }
}

/// The cursor a one-row page issues for the row `identity`.
async fn order_cursor_after(database: &TestDatabase, base: &str, identity: &str) -> Result<String> {
    let wanted = hkw_id(identity);
    let mut cursor: Option<String> = None;
    loop {
        let uri = match cursor.as_ref() {
            Some(cursor) => format!("{base}&page_size=1&cursor={cursor}"),
            None => format!("{base}&page_size=1"),
        };
        let page = hk_ok(database, &uri).await?;
        let next = hk_next_cursor(&page)?;
        if hk_ids(&page).contains(&wanted) {
            return Ok(next);
        }
        cursor = Some(next);
    }
}

/// Every row from `cursor` on, one row per page.
async fn order_rest(database: &TestDatabase, base: &str, cursor: String) -> Result<Vec<String>> {
    hkw_rest(database, base, Some(cursor), None).await
}

/// The unpaged order on each route and in each direction, and one-row walks that send every
/// continuation in each accepted token layout, all equal the block order.
#[tokio::test]
async fn v2_history_orders_one_block_by_transaction_index() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    order_seed(&database).await?;
    for route in hk_routes() {
        for order in ["desc", "asc"] {
            let base = format!("{route}&order={order}");
            let (unpaged, _) = hkw_baseline(&database, &base).await?;
            assert_eq!(order_rows(&unpaged), order_expected(order), "{base}: unpaged order");
            for token in [
                OrderToken::Current,
                OrderToken::TransactionHash,
                OrderToken::Both,
                OrderToken::Legacy,
            ] {
                let walked = order_walk(&database, &base, token).await?;
                assert_eq!(walked, unpaged, "{base}: walk with {token:?} cursors");
            }
        }
    }
    database.cleanup().await
}

/// Mid-block continuations. Newest first, after A's last row the row without a transaction
/// follows and never B; oldest first, after A's last row B follows.
#[tokio::test]
async fn v2_history_continues_inside_a_block_by_transaction_index() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    order_seed(&database).await?;
    for route in hk_routes() {
        let desc = format!("{route}&order=desc");
        let cursor = order_cursor_after(&database, &desc, "order-a-log-3").await?;
        let next = hk_ok(&database, &format!("{desc}&page_size=1&cursor={cursor}")).await?;
        assert_eq!(hk_ids(&next), vec![hkw_id("order-synthesised")], "{desc}");
        let asc = format!("{route}&order=asc");
        let cursor = order_cursor_after(&database, &asc, "order-a-log-7").await?;
        let next = hk_ok(&database, &format!("{asc}&page_size=1&cursor={cursor}")).await?;
        assert_eq!(hk_ids(&next), vec![hkw_id("order-b")], "{asc}");
    }
    database.cleanup().await
}

/// Each token layout when its anchor row is gone. A current cursor continues from its own
/// position. A cursor with a transaction hash finds its index through another event of the
/// transaction and continues the same way; when no event of the transaction is left it restarts
/// once. A cursor naming only its anchor restarts once.
#[tokio::test]
async fn v2_history_cursor_layouts_outlive_their_anchor_as_documented() -> Result<()> {
    for route in hk_routes() {
        for order in ["desc", "asc"] {
            let base = format!("{route}&order={order}");
            let database = TestDatabase::new_migrated().await?;
            order_seed(&database).await?;
            let (unpaged, _) = hkw_baseline(&database, &base).await?;
            let after = |identity: &str| {
                let position = unpaged
                    .iter()
                    .position(|id| *id == hkw_id(identity))
                    .expect("row in the walk");
                unpaged[position + 1..].to_vec()
            };

            // A's first row in walk order, anchored while A's other row stays.
            let first_a = if order == "desc" { "order-a-log-7" } else { "order-a-log-3" };
            let cursor = order_cursor_after(&database, &base, first_a).await?;
            let current = order_token(&database, &cursor, OrderToken::Current).await?;
            let hashed = order_token(&database, &cursor, OrderToken::TransactionHash).await?;
            let legacy = order_token(&database, &cursor, OrderToken::Legacy).await?;
            let expected = after(first_a);
            hk_delete_event(&database, first_a).await?;
            assert_eq!(order_rest(&database, &base, current).await?, expected, "{base}: current");
            assert_eq!(
                order_rest(&database, &base, hashed).await?,
                expected,
                "{base}: transaction hash, index from the transaction's other event"
            );
            let (status, payload) =
                hk_get(&database, &format!("{base}&page_size=1&cursor={legacy}")).await?;
            assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
            assert_eq!(payload["error"]["message"], json!(HK_RESTART), "{base}");

            // B is its transaction's only row: with it gone, a hash cursor has nothing to read
            // its index from and restarts, while a current cursor still continues.
            let cursor = order_cursor_after(&database, &base, "order-b").await?;
            let current = order_token(&database, &cursor, OrderToken::Current).await?;
            let hashed = order_token(&database, &cursor, OrderToken::TransactionHash).await?;
            let expected = after("order-b")
                .into_iter()
                .filter(|id| *id != hkw_id(first_a))
                .collect::<Vec<_>>();
            hk_delete_event(&database, "order-b").await?;
            let (status, payload) =
                hk_get(&database, &format!("{base}&page_size=1&cursor={hashed}")).await?;
            assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
            assert_eq!(payload["error"]["message"], json!(HK_RESTART), "{base}");
            assert_eq!(order_rest(&database, &base, current).await?, expected, "{base}: current");
            database.cleanup().await?;
        }
    }
    Ok(())
}
