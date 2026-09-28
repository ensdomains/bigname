// `include=child_registrations` under the history order within one block: child rows and the
// name's own rows follow transaction index, log index, then `event_identity`, and the child arm
// orders, limits and bounds its rows by the same key before the merge, so no child row a later
// page needs is dropped (docs/api-v1-routes.md, "Direct child registrations").

/// An event's transaction hash, transaction index and log index.
type ChildOrderPosition<'a> = (&'a str, i64, i64);

/// Sets each event's transaction hash, index and log index; `None` clears all three.
async fn child_order_position(
    database: &TestDatabase,
    positions: &[(&str, Option<ChildOrderPosition<'_>>)],
) -> Result<()> {
    for (identity, position) in positions {
        let updated = sqlx::query(
            "UPDATE bigname_phase.normalized_events
             SET transaction_hash = $2, transaction_index = $3, log_index = $4
             WHERE event_identity = $1",
        )
        .bind(identity)
        .bind(position.map(|(hash, _, _)| hash))
        .bind(position.map(|(_, index, _)| index))
        .bind(position.map(|(_, _, log)| log))
        .execute(&database.pool)
        .await?
        .rows_affected();
        anyhow::ensure!(updated == 1, "{identity} must exist");
    }
    Ok(())
}

async fn child_order_parent(database: &TestDatabase, parent: &str, seed: u128) -> Result<()> {
    seed_v2_history_blocks(database, 80..=80).await?;
    seed_family_identity_inputs(
        &database.pool,
        "ens",
        parent,
        "ethereum-mainnet",
        80,
        "0xhistory80",
        Uuid::from_u128(seed),
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
        "ens_v1",
    )
    .await?;
    Ok(())
}

/// `trio.eth` has three child registrations in block 132 and no rows of its own: A in
/// transaction 1 with the greatest hash, B in transaction 2, C in transaction 3 with the
/// smallest hash.
async fn child_order_seed_trio(database: &TestDatabase) -> Result<()> {
    child_order_parent(database, "trio.eth", 0xd100).await?;
    seed_child_surfaces(database, &["a.trio.eth", "b.trio.eth", "c.trio.eth"]).await?;
    seed_v2_history_blocks(database, 131..=133).await?;
    let events = ["a", "b", "c"].map(|label| {
        child_history_event(
            &format!("trio-{label}"),
            Some(&format!("{label}.trio.eth")),
            None,
            "RegistrationGranted",
            132,
        )
    });
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    child_order_position(
        database,
        &[
            ("trio-a", Some(("0xffa", 1, 1))),
            ("trio-b", Some(("0x88b", 2, 2))),
            ("trio-c", Some(("0x00c", 3, 3))),
        ],
    )
    .await?;
    publish_test_families(database, 133).await
}

/// `kids.eth` has its own rows in blocks 141 and 143, and block 142 holds five child
/// registrations and two of its own rows across five transactions whose hash order is the
/// reverse of their index order, plus a child registration with no transaction position.
async fn child_order_seed_kids(database: &TestDatabase) -> Result<()> {
    child_order_parent(database, "kids.eth", 0xd200).await?;
    seed_child_surfaces(
        database,
        &[
            "a.kids.eth",
            "b.kids.eth",
            "c.kids.eth",
            "d.kids.eth",
            "e.kids.eth",
            "s.kids.eth",
        ],
    )
    .await?;
    seed_v2_history_blocks(database, 141..=143).await?;
    let mut events = ["a", "b", "c", "d", "e", "s"]
        .map(|label| {
            child_history_event(
                &format!("kids-{label}"),
                Some(&format!("{label}.kids.eth")),
                None,
                "RegistrationGranted",
                142,
            )
        })
        .to_vec();
    for (identity, block) in [
        ("kids-n0", 141),
        ("kids-n1", 142),
        ("kids-n2", 142),
        ("kids-n3", 143),
    ] {
        events.push(child_history_event(
            identity,
            Some("kids.eth"),
            None,
            "RecordChanged",
            block,
        ));
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    child_order_position(
        database,
        &[
            ("kids-a", Some(("0xf1", 1, 1))),
            ("kids-n1", Some(("0xf1", 1, 2))),
            ("kids-b", Some(("0xe2", 2, 3))),
            ("kids-n2", Some(("0xd3", 3, 4))),
            ("kids-c", Some(("0xd3", 3, 5))),
            ("kids-d", Some(("0xc4", 4, 6))),
            ("kids-e", Some(("0xb5", 5, 7))),
            ("kids-s", None),
        ],
    )
    .await?;
    publish_test_families(database, 143).await
}

const KIDS_NEWEST_FIRST: [&str; 10] = [
    "kids-n3", "kids-e", "kids-d", "kids-c", "kids-n2", "kids-b", "kids-n1", "kids-a", "kids-s",
    "kids-n0",
];

fn child_order_expected(newest_first: &[&str], order: &str) -> Vec<String> {
    let mut expected = newest_first
        .iter()
        .map(|identity| hkw_id(identity))
        .collect::<Vec<_>>();
    if order == "asc" {
        expected.reverse();
    }
    expected
}

/// Every row from the start in pages of `page_size`.
async fn child_order_walk(database: &TestDatabase, base: &str, page_size: usize) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let uri = match cursor.as_ref() {
            Some(cursor) => format!("{base}&page_size={page_size}&cursor={cursor}"),
            None => format!("{base}&page_size={page_size}"),
        };
        let page = hk_ok(database, &uri).await?;
        let rows = hk_ids(&page);
        assert!(rows.len() <= page_size, "{uri}");
        ids.extend(rows);
        match page["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => return Ok(ids),
        }
    }
}

/// Three children in one block whose hash order is the reverse of their index order, one per
/// page: C, B, A newest first and A, B, C oldest first. An arm still keyed by hash under an
/// outer order by index returns B first in both directions.
#[tokio::test]
async fn v2_child_history_pages_one_block_by_transaction_index() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    child_order_seed_trio(&database).await?;
    let route = "/v1/names/trio.eth/history?include=child_registrations";
    for order in ["desc", "asc"] {
        let base = format!("{route}&order={order}");
        let expected = child_order_expected(&["trio-c", "trio-b", "trio-a"], order);
        let first = hk_ok(&database, &format!("{base}&page_size=1")).await?;
        assert_eq!(hk_ids(&first), expected[..1].to_vec(), "{base}: first page");
        assert_eq!(child_order_walk(&database, &base, 1).await?, expected, "{base}");
    }
    database.cleanup().await
}

/// Five children and the name's own rows in one block, more than one page's lookahead: every
/// page size walks the same block order through both arms in both directions, the count agrees,
/// and every accepted cursor layout continues from child and name rows alike.
#[tokio::test]
async fn v2_child_history_mixed_pages_follow_the_block_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    child_order_seed_kids(&database).await?;
    let route = "/v1/names/kids.eth/history?include=child_registrations";
    for order in ["desc", "asc"] {
        let base = format!("{route}&order={order}");
        let expected = child_order_expected(&KIDS_NEWEST_FIRST, order);
        let unpaged = hk_ok(&database, &format!("{base}&page_size=50")).await?;
        assert_eq!(hk_ids(&unpaged), expected, "{base}: unpaged");
        for page_size in [1, 2, 3] {
            assert_eq!(
                child_order_walk(&database, &base, page_size).await?,
                expected,
                "{base}: {page_size}"
            );
        }
        let counted = hk_ok(
            &database,
            &format!(
                "/v1/names/kids.eth/history?include=child_registrations,total_count&order={order}"
            ),
        )
        .await?;
        assert_eq!(
            counted["page"]["total_count"],
            json!(expected.len()),
            "{base}: count"
        );
        for token in [OrderToken::TransactionHash, OrderToken::Both, OrderToken::Legacy] {
            assert_eq!(
                order_walk(&database, &base, token).await?,
                expected,
                "{base}: {token:?}"
            );
        }
    }
    database.cleanup().await
}
