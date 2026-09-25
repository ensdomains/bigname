// The history order within one block on every history route (docs/api-v1-routes.md, "Shared
// Route Rules"): transaction index, then log index, then `event_identity`, with a row that has
// no transaction position before every transaction of its block. The cursor carries the
// transaction index. A cursor issued while the order compared transaction hashes is read through
// its anchor row or another event of its transaction, and restarts once when neither exists.

const D12_NAME: &str = "ens:history.eth";
/// Block 122's transaction hashes: A's is the greater, B's the smaller, the reverse of their
/// index order.
const D12_TX_A: &str = "0xffa";
const D12_TX_B: &str = "0x00b";

/// Newest first: block 123, B, A's log 7, A's log 3, the row without a transaction, block 121.
const D12_NEWEST_FIRST: [&str; 6] = [
    "d12-block-123",
    "d12-b",
    "d12-a-log-7",
    "d12-a-log-3",
    "d12-synthesised",
    "d12-block-121",
];

/// `history.eth` plus blocks 121 to 123. Block 122 holds transaction A at index 1 with logs 3
/// and 7, transaction B at index 2, and a row with no transaction position. Every row is an
/// owner transfer to the address the address route reads.
async fn d12_seed(database: &TestDatabase) -> Result<()> {
    seed_v2_history_fixture(database).await?;
    seed_v2_history_blocks(database, 121..=123).await?;
    let events = [
        ("d12-block-121", 121),
        ("d12-a-log-3", 122),
        ("d12-a-log-7", 122),
        ("d12-b", 122),
        ("d12-synthesised", 122),
        ("d12-block-123", 123),
    ]
    .map(|(identity, block)| {
        v2_history_event(identity, Some(D12_NAME), None, "AuthorityTransferred", block)
    });
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    sqlx::query(
        "UPDATE bigname_phase.normalized_events event
         SET transaction_hash = position.transaction_hash,
             transaction_index = position.transaction_index,
             log_index = position.log_index
         FROM (VALUES
             ('d12-a-log-3', $1::text, 1::bigint, 3::bigint),
             ('d12-a-log-7', $1::text, 1::bigint, 7::bigint),
             ('d12-b', $2::text, 2::bigint, 9::bigint),
             ('d12-synthesised', NULL::text, NULL::bigint, NULL::bigint)
         ) AS position(event_identity, transaction_hash, transaction_index, log_index)
         WHERE event.event_identity = position.event_identity",
    )
    .bind(D12_TX_A)
    .bind(D12_TX_B)
    .execute(&database.pool)
    .await?;
    Ok(())
}

fn d12_expected(order: &str) -> Vec<String> {
    let mut expected = D12_NEWEST_FIRST.map(hkw_id).to_vec();
    if order == "asc" {
        expected.reverse();
    }
    expected
}

/// The fixture's rows among `ids`, in the order given.
fn d12_rows(ids: &[String]) -> Vec<String> {
    let ours = D12_NEWEST_FIRST.map(hkw_id);
    ids.iter().filter(|id| ours.contains(id)).cloned().collect()
}

/// How a client holding a cursor from an earlier layout would send an issued cursor.
#[derive(Clone, Copy, Debug)]
enum D12Token {
    /// As issued: the anchor's transaction index.
    Current,
    /// The transaction hash in place of the index, as issued before the order compared indexes.
    TransactionHash,
    /// Both keys, with a hash that names no transaction: read as current.
    Both,
    /// The anchor's numeric id and identity only.
    Legacy,
}

async fn d12_token(database: &TestDatabase, cursor: &str, token: D12Token) -> Result<String> {
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
        D12Token::Current => return Ok(cursor.to_owned()),
        D12Token::Legacy => return hk_legacy_cursor(database, cursor).await,
        D12Token::TransactionHash => {
            item.remove("transaction_index");
            if let Some(hash) = transaction_hash {
                item.insert("transaction_hash".to_owned(), json!(hash));
            }
        }
        D12Token::Both => {
            if let Some(index) = transaction_index {
                item.insert("transaction_index".to_owned(), json!(index.to_string()));
                item.insert("transaction_hash".to_owned(), json!("0xnotatransaction"));
            }
        }
    }
    Ok(hex::encode(serde_json::to_vec(&value)?))
}

/// Every row from the start, one row per page, sending each issued cursor as `token`.
async fn d12_walk(database: &TestDatabase, base: &str, token: D12Token) -> Result<Vec<String>> {
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
            Some(next) => cursor = Some(d12_token(database, next, token).await?),
            None => return Ok(ids),
        }
    }
}

/// The cursor a one-row page issues for the row `identity`.
async fn d12_cursor_after(database: &TestDatabase, base: &str, identity: &str) -> Result<String> {
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
async fn d12_rest(database: &TestDatabase, base: &str, cursor: String) -> Result<Vec<String>> {
    hkw_rest(database, base, Some(cursor), None).await
}

/// The unpaged order on each route and in each direction, and one-row walks that send every
/// continuation in each accepted token layout, all equal the D12 order.
#[tokio::test]
async fn v2_history_orders_one_block_by_transaction_index() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    d12_seed(&database).await?;
    for route in hk_routes() {
        for order in ["desc", "asc"] {
            let base = format!("{route}&order={order}");
            let (unpaged, _) = hkw_baseline(&database, &base).await?;
            assert_eq!(d12_rows(&unpaged), d12_expected(order), "{base}: unpaged order");
            for token in [
                D12Token::Current,
                D12Token::TransactionHash,
                D12Token::Both,
                D12Token::Legacy,
            ] {
                let walked = d12_walk(&database, &base, token).await?;
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
    d12_seed(&database).await?;
    for route in hk_routes() {
        let desc = format!("{route}&order=desc");
        let cursor = d12_cursor_after(&database, &desc, "d12-a-log-3").await?;
        let next = hk_ok(&database, &format!("{desc}&page_size=1&cursor={cursor}")).await?;
        assert_eq!(hk_ids(&next), vec![hkw_id("d12-synthesised")], "{desc}");
        let asc = format!("{route}&order=asc");
        let cursor = d12_cursor_after(&database, &asc, "d12-a-log-7").await?;
        let next = hk_ok(&database, &format!("{asc}&page_size=1&cursor={cursor}")).await?;
        assert_eq!(hk_ids(&next), vec![hkw_id("d12-b")], "{asc}");
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
            d12_seed(&database).await?;
            let (unpaged, _) = hkw_baseline(&database, &base).await?;
            let after = |identity: &str| {
                let position = unpaged
                    .iter()
                    .position(|id| *id == hkw_id(identity))
                    .expect("row in the walk");
                unpaged[position + 1..].to_vec()
            };

            // A's first row in walk order, anchored while A's other row stays.
            let first_a = if order == "desc" { "d12-a-log-7" } else { "d12-a-log-3" };
            let cursor = d12_cursor_after(&database, &base, first_a).await?;
            let current = d12_token(&database, &cursor, D12Token::Current).await?;
            let hashed = d12_token(&database, &cursor, D12Token::TransactionHash).await?;
            let legacy = d12_token(&database, &cursor, D12Token::Legacy).await?;
            let expected = after(first_a);
            hk_delete_event(&database, first_a).await?;
            assert_eq!(d12_rest(&database, &base, current).await?, expected, "{base}: current");
            assert_eq!(
                d12_rest(&database, &base, hashed).await?,
                expected,
                "{base}: transaction hash, index from the transaction's other event"
            );
            let (status, payload) =
                hk_get(&database, &format!("{base}&page_size=1&cursor={legacy}")).await?;
            assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
            assert_eq!(payload["error"]["message"], json!(HK_RESTART), "{base}");

            // B is its transaction's only row: with it gone, a hash cursor has nothing to read
            // its index from and restarts, while a current cursor still continues.
            let cursor = d12_cursor_after(&database, &base, "d12-b").await?;
            let current = d12_token(&database, &cursor, D12Token::Current).await?;
            let hashed = d12_token(&database, &cursor, D12Token::TransactionHash).await?;
            let expected = after("d12-b")
                .into_iter()
                .filter(|id| *id != hkw_id(first_a))
                .collect::<Vec<_>>();
            hk_delete_event(&database, "d12-b").await?;
            let (status, payload) =
                hk_get(&database, &format!("{base}&page_size=1&cursor={hashed}")).await?;
            assert_eq!(status, StatusCode::CONFLICT, "{base}: {payload}");
            assert_eq!(payload["error"]["message"], json!(HK_RESTART), "{base}");
            assert_eq!(d12_rest(&database, &base, current).await?, expected, "{base}: current");
            database.cleanup().await?;
        }
    }
    Ok(())
}
