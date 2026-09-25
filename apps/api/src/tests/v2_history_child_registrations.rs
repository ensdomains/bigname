// `include=child_registrations` on `GET /v1/names/{name}/history`
// (docs/api-v1-routes.md, "Direct child registrations").

const CHILD_PARENT_RESOURCE: u128 = 0x7c00;

/// Membership rows as Project publishes them: one per (parent, event), at the event's position.
async fn seed_child_registration_memberships(
    database: &TestDatabase,
    parent_name: &str,
    event_identities: &[&str],
) -> Result<()> {
    let identities = event_identities
        .iter()
        .map(|identity| (*identity).to_owned())
        .collect::<Vec<_>>();
    let inserted = sqlx::query(
        "INSERT INTO bigname_phase.child_registration_events (
             parent_logical_name_id, event_identity, child_logical_name_id, namespace, chain_id,
             block_number, block_hash, transaction_order_key, log_order_key, event_kind,
             manifest_version, target_block_number, target_block_hash
         )
         SELECT $1, ne.event_identity, ne.logical_name_id, ne.namespace, ne.chain_id,
                ne.block_number, ne.block_hash, COALESCE(ne.transaction_index, -1),
                COALESCE(ne.log_index, -1), ne.event_kind, ne.manifest_version,
                ne.block_number, ne.block_hash
         FROM bigname_phase.normalized_events ne
         WHERE ne.event_identity = ANY($2::text[])",
    )
    .bind(bigname_storage::logical_name_id_for_name("ens", parent_name))
    .bind(&identities)
    .execute(&database.pool)
    .await?
    .rows_affected();
    assert_eq!(inserted, identities.len() as u64, "every membership cites a seeded event");
    Ok(())
}

async fn seed_child_surfaces(database: &TestDatabase, names: &[&str]) -> Result<()> {
    let surfaces = names
        .iter()
        .map(|name| collection_name_surface(&format!("ens:{name}"), name, &format!("node:{name}"), 90))
        .collect::<Vec<_>>();
    upsert_test_name_surfaces(&database.pool, &surfaces).await?;
    Ok(())
}

fn child_history_event(
    identity: &str,
    name: Option<&str>,
    resource_id: Option<Uuid>,
    kind: &str,
    block_number: i64,
) -> NormalizedEvent {
    let logical_name_id = name.map(|name| format!("ens:{name}"));
    v2_history_event(identity, logical_name_id.as_deref(), resource_id, kind, block_number)
}

/// `parent.eth` with its own rows, direct children `a`, `b`, `c`, and `d`, a grandchild, and a
/// second parent `q.eth` that holds many children after a registry moved to it.
async fn seed_child_registration_fixture(database: &TestDatabase) -> Result<()> {
    let parent_resource = Uuid::from_u128(CHILD_PARENT_RESOURCE);
    seed_v2_history_name(
        database,
        "ens:parent.eth",
        "parent.eth",
        "node:parent.eth",
        80,
        parent_resource,
        Uuid::from_u128(0x8c00),
        Uuid::from_u128(0x9c00),
    )
    .await?;
    seed_v2_history_name(
        database,
        "ens:q.eth",
        "q.eth",
        "node:q.eth",
        80,
        Uuid::from_u128(0x7c01),
        Uuid::from_u128(0x8c01),
        Uuid::from_u128(0x9c01),
    )
    .await?;
    // `a.parent.eth` is a current name, so its own history can be read.
    seed_v2_history_name(
        database,
        "ens:a.parent.eth",
        "a.parent.eth",
        "node:a.parent.eth",
        90,
        Uuid::from_u128(0x7c02),
        Uuid::from_u128(0x8c02),
        Uuid::from_u128(0x9c02),
    )
    .await?;
    seed_child_surfaces(
        database,
        &[
            "b.parent.eth",
            "c.parent.eth",
            "d.parent.eth",
            "x.a.parent.eth",
            "c.q.eth",
        ],
    )
    .await?;
    seed_v2_history_blocks(database, 101..=114).await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            child_history_event("p-grant", None, Some(parent_resource), "RegistrationGranted", 101),
            child_history_event("p-record", Some("parent.eth"), None, "RecordChanged", 102),
            child_history_event("a-grant", Some("a.parent.eth"), None, "RegistrationGranted", 103),
            child_history_event("x-grant", Some("x.a.parent.eth"), None, "RegistrationGranted", 104),
            child_history_event("b-grant", Some("b.parent.eth"), None, "RegistrationGranted", 105),
            child_history_event("a-release", Some("a.parent.eth"), None, "RegistrationReleased", 106),
            child_history_event("a-regrant", Some("a.parent.eth"), None, "RegistrationGranted", 107),
            child_history_event("p-resolver", Some("parent.eth"), None, "ResolverChanged", 108),
            // A registry granted `c` under `parent.eth`, then moved under `q.eth`.
            child_history_event("c-grant", Some("c.parent.eth"), None, "RegistrationGranted", 109),
            // A grant that is also a row of the parent's registration scope.
            child_history_event(
                "d-grant",
                Some("d.parent.eth"),
                Some(parent_resource),
                "RegistrationGranted",
                110,
            ),
            child_history_event("b-release", Some("b.parent.eth"), None, "RegistrationReleased", 111),
            child_history_event("qc-grant", Some("c.q.eth"), None, "RegistrationGranted", 112),
            child_history_event("p-renewal", None, Some(parent_resource), "RegistrationRenewed", 113),
        ],
    )
    .await?;
    seed_child_registration_memberships(
        database,
        "parent.eth",
        &["a-grant", "b-grant", "a-regrant", "c-grant", "d-grant"],
    )
    .await?;
    seed_child_registration_memberships(database, "a.parent.eth", &["x-grant"]).await?;
    seed_child_registration_memberships(database, "q.eth", &["qc-grant"]).await?;
    // The moved registry's later grants: many `q.eth` rows the `parent.eth` stream never reads.
    sqlx::raw_sql(
        "CREATE TEMP TABLE bulk_q_grants AS
             SELECT event.*, n AS bulk_n
             FROM bigname_phase.normalized_events event, generate_series(1, 5000) AS n
             WHERE event.event_identity = 'qc-grant';
         UPDATE bulk_q_grants SET event_identity = 'q-bulk-' || bulk_n, log_index = bulk_n,
             normalized_event_id = 1000000 + bulk_n;
         ALTER TABLE bulk_q_grants DROP COLUMN bulk_n;
         INSERT INTO bigname_phase.normalized_events OVERRIDING SYSTEM VALUE
             SELECT * FROM bulk_q_grants;
         DROP TABLE bulk_q_grants;",
    )
    .execute(&database.pool)
    .await?;
    let bulk = (1..=5000).map(|n| format!("q-bulk-{n}")).collect::<Vec<_>>();
    seed_child_registration_memberships(
        database,
        "q.eth",
        &bulk.iter().map(String::as_str).collect::<Vec<_>>(),
    )
    .await?;
    sqlx::raw_sql("ANALYZE bigname_phase.child_registration_events; ANALYZE bigname_phase.normalized_events")
        .execute(&database.pool)
        .await?;
    database.seed_default_ens_primary_name_fallback_context().await?;
    Ok(())
}

fn history_rows(payload: &Value) -> Vec<(String, String, String, String)> {
    payload["data"]
        .as_array()
        .expect("history data")
        .iter()
        .map(|row| {
            (
                row["transaction_hash"].as_str().expect("transaction_hash").to_owned(),
                row["type"].as_str().expect("type").to_owned(),
                row["name"].as_str().expect("name").to_owned(),
                row["subject"].as_str().unwrap_or("-").to_owned(),
            )
        })
        .collect()
}

fn row(block: i64, event_type: &str, name: &str, subject: &str) -> (String, String, String, String) {
    (format!("0xtx{block}"), event_type.to_owned(), name.to_owned(), subject.to_owned())
}

const CHILD_ROUTE: &str = "/v1/names/parent.eth/history?include=child_registrations";
const CHILD_COUNT_ROUTE: &str =
    "/v1/names/parent.eth/history?include=child_registrations,total_count";

#[tokio::test]
async fn v2_name_history_child_registrations_merge_into_the_stream() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_child_registration_fixture(&database).await?;

    let plain = v2_history_payload_for_database(&database, "/v1/names/parent.eth/history").await?;
    assert_eq!(
        history_rows(&plain),
        vec![
            row(113, "renewal", "parent.eth", "-"),
            row(110, "registration", "parent.eth", "-"),
            row(108, "resolver", "parent.eth", "-"),
            row(102, "record", "parent.eth", "-"),
            row(101, "registration", "parent.eth", "-"),
        ],
        "without the option the stream is unchanged and rows carry no subject"
    );

    let merged = v2_history_payload_for_database(
        &database,
        CHILD_COUNT_ROUTE,
    )
    .await?;
    assert_eq!(
        history_rows(&merged),
        vec![
            row(113, "renewal", "parent.eth", "name"),
            // In both arms: one row, as a child.
            row(110, "registration", "d.parent.eth", "child"),
            row(109, "registration", "c.parent.eth", "child"),
            row(108, "resolver", "parent.eth", "name"),
            // Registered again after a release; the release itself is the child's own row.
            row(107, "registration", "a.parent.eth", "child"),
            // Released since, still listed; the grandchild at 104 is not.
            row(105, "registration", "b.parent.eth", "child"),
            row(103, "registration", "a.parent.eth", "child"),
            row(102, "record", "parent.eth", "name"),
            row(101, "registration", "parent.eth", "name"),
        ]
    );
    assert_eq!(merged["page"]["total_count"], json!(9));

    // A child row is the same event the child's own history serves.
    let child_rows = merged["data"].as_array().context("data")?;
    let own = v2_history_payload_for_database(&database, "/v1/names/a.parent.eth/history").await?;
    let own_grant = own["data"]
        .as_array()
        .context("data")?
        .iter()
        .find(|row| row["transaction_hash"] == json!("0xtx107"))
        .context("the child's own history holds the grant")?;
    let merged_grant = child_rows
        .iter()
        .find(|row| row["transaction_hash"] == json!("0xtx107"))
        .context("merged grant")?;
    for field in ["id", "type", "name", "namespace", "registration_id", "block_number", "log_index"] {
        assert_eq!(merged_grant[field], own_grant[field], "field {field}");
    }

    // A sparse match: `a.parent.eth` has one direct child grant; its grandparent's rows and
    // its own grandchildren are not in its stream.
    let sparse = v2_history_payload_for_database(
        &database,
        "/v1/names/a.parent.eth/history?include=child_registrations",
    )
    .await?;
    let sparse_rows = history_rows(&sparse);
    assert_eq!(
        sparse_rows
            .iter()
            .filter(|(_, _, _, subject)| subject == "child")
            .collect::<Vec<_>>(),
        vec![&row(104, "registration", "x.a.parent.eth", "child")]
    );
    let own_rows = history_rows(&own);
    assert_eq!(
        sparse_rows
            .iter()
            .filter(|(_, _, _, subject)| subject == "name")
            .map(|(tx, event_type, name, _)| (tx.clone(), event_type.clone(), name.clone()))
            .collect::<Vec<_>>(),
        own_rows
            .iter()
            .map(|(tx, event_type, name, _)| (tx.clone(), event_type.clone(), name.clone()))
            .collect::<Vec<_>>()
    );

    // `q.eth` sees its own children, not `parent.eth`'s.
    let q = v2_history_payload_for_database(
        &database,
        "/v1/names/q.eth/history?include=child_registrations&page_size=3",
    )
    .await?;
    assert!(
        history_rows(&q)
            .iter()
            .all(|(_, _, name, _)| name != "c.parent.eth"),
        "{q}"
    );

    // With no child rows the option returns the plain stream, each row marked `name`.
    sqlx::query("DELETE FROM bigname_phase.child_registration_events WHERE parent_logical_name_id = $1")
        .bind(bigname_storage::logical_name_id_for_name("ens", "parent.eth"))
        .execute(&database.pool)
        .await?;
    let empty = v2_history_payload_for_database(&database, CHILD_COUNT_ROUTE).await?;
    assert_eq!(
        history_rows(&empty),
        history_rows(&plain)
            .into_iter()
            .map(|(tx, event_type, name, _)| (tx, event_type, name, "name".to_owned()))
            .collect::<Vec<_>>()
    );
    assert_eq!(empty["page"]["total_count"], json!(5));

    database.cleanup().await
}

#[tokio::test]
async fn v2_name_history_child_registrations_page_in_both_directions() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_child_registration_fixture(&database).await?;

    for order in ["desc", "asc"] {
        let full = v2_history_payload_for_database(
            &database,
            &format!("{CHILD_ROUTE}&order={order}&page_size=100"),
        )
        .await?;
        let mut paged = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let uri = match &cursor {
                Some(cursor) => format!("{CHILD_ROUTE}&order={order}&page_size=2&cursor={cursor}"),
                None => format!("{CHILD_ROUTE}&order={order}&page_size=2"),
            };
            let page = v2_history_payload_for_database(&database, &uri).await?;
            paged.extend(history_rows(&page));
            match page["page"]["next_cursor"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => break,
            }
        }
        assert_eq!(paged, history_rows(&full), "order={order}");
        assert_eq!(paged.len(), 9, "order={order}");
    }
    let desc = v2_history_payload_for_database(&database, CHILD_ROUTE).await?;
    let mut asc = history_rows(
        &v2_history_payload_for_database(&database, &format!("{CHILD_ROUTE}&order=asc")).await?,
    );
    asc.reverse();
    assert_eq!(asc, history_rows(&desc));

    database.cleanup().await
}

#[tokio::test]
async fn v2_name_history_child_registrations_follow_type_and_count_controls() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_child_registration_fixture(&database).await?;

    let records = v2_history_payload_for_database(
        &database,
        &format!("{CHILD_COUNT_ROUTE}&type=record,resolver"),
    )
    .await?;
    assert_eq!(
        history_rows(&records),
        vec![
            row(108, "resolver", "parent.eth", "name"),
            row(102, "record", "parent.eth", "name"),
        ]
    );
    assert_eq!(records["page"]["total_count"], json!(2));

    let registrations = v2_history_payload_for_database(
        &database,
        &format!("{CHILD_COUNT_ROUTE}&type=registration&page_size=2"),
    )
    .await?;
    assert_eq!(
        history_rows(&registrations),
        vec![
            row(110, "registration", "d.parent.eth", "child"),
            row(109, "registration", "c.parent.eth", "child"),
        ]
    );
    assert_eq!(registrations["page"]["total_count"], json!(6));
    let next = registrations["page"]["next_cursor"]
        .as_str()
        .context("more registrations")?;
    let rest = v2_history_payload_for_database(
        &database,
        &format!("{CHILD_ROUTE}&type=registration&page_size=10&cursor={next}"),
    )
    .await?;
    assert_eq!(
        history_rows(&rest),
        vec![
            row(107, "registration", "a.parent.eth", "child"),
            row(105, "registration", "b.parent.eth", "child"),
            row(103, "registration", "a.parent.eth", "child"),
            row(101, "registration", "parent.eth", "name"),
        ]
    );

    database.cleanup().await
}

#[tokio::test]
async fn v2_name_history_child_registrations_bind_the_cursor_and_refuse_registrar_roots()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_child_registration_fixture(&database).await?;

    let flagged = v2_history_payload_for_database(&database, &format!("{CHILD_ROUTE}&page_size=2"))
        .await?;
    let flagged_cursor = flagged["page"]["next_cursor"].as_str().context("cursor")?;
    let plain = v2_history_payload_for_database(
        &database,
        "/v1/names/parent.eth/history?page_size=2",
    )
    .await?;
    let plain_cursor = plain["page"]["next_cursor"].as_str().context("cursor")?;
    assert_ne!(flagged_cursor, plain_cursor);
    for uri in [
        format!("/v1/names/parent.eth/history?page_size=2&cursor={flagged_cursor}"),
        format!("{CHILD_ROUTE}&page_size=2&cursor={plain_cursor}"),
        "/v1/names/eth/history?include=child_registrations".to_owned(),
        "/v1/names/base.eth/history?include=child_registrations".to_owned(),
        "/v1/names/base.eth/history?namespace=basenames&include=child_registrations".to_owned(),
        "/v1/events?name=parent.eth&include=child_registrations".to_owned(),
        "/v1/addresses/0x00000000000000000000000000000000000000cc/history?include=child_registrations"
            .to_owned(),
    ] {
        let response = v2_history_response_for_database(&database, &uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "uri: {uri}");
        assert_eq!(
            read_json::<Value>(response).await?["error"]["code"],
            json!("invalid_input"),
            "uri: {uri}"
        );
    }

    database.cleanup().await
}

/// With another parent holding thousands of rows, a page of `parent.eth` reads its child arm
/// from the parent-leading index and touches at most `page_size + 1` membership rows.
#[tokio::test]
async fn v2_name_history_child_arm_reads_a_bounded_index_range() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_child_registration_fixture(&database).await?;
    let parent = bigname_storage::logical_name_id_for_name("ens", "parent.eth");
    let resources = [Uuid::from_u128(CHILD_PARENT_RESOURCE)];
    let options = bigname_storage::HistoryPageOptions::default();
    let first = bigname_storage::load_name_history_page_with_child_registrations(
        &database.pool,
        &parent,
        &resources,
        bigname_storage::HistoryScope::Both,
        None,
        2,
        bigname_storage::HistorySummaryMode::None,
        &options,
        None,
    )
    .await?;
    let deep = first.next_cursor.context("a second page")?;
    for cursor in [None, Some(&deep)] {
        let plan = bigname_storage::explain_name_history_page_with_child_registrations_for_test(
            &database.pool,
            &parent,
            &resources,
            bigname_storage::HistoryScope::Both,
            cursor,
            2,
            &options,
        )
        .await?;
        let arm_scans = plan_nodes(&plan);
        let arm = arm_scans
            .iter()
            .filter(|node| node["Index Name"] == json!("child_registration_events_parent_history_idx"))
            // The name arm's classification probe reads by the row it tests (`ne_…`); the arm's
            // own scan reads by parent and cursor only.
            .filter(|node| node["Index Cond"].as_str().is_some_and(|cond| !cond.contains("ne_")))
            .collect::<Vec<_>>();
        assert_eq!(arm.len(), 1, "the child arm reads the parent index once: {plan}");
        assert_eq!(arm[0]["Actual Loops"], json!(1), "{plan}");
        assert!(
            arm[0]["Actual Rows"].as_u64().context("actual rows")? <= 3,
            "at most page_size + 1 membership rows: {plan}"
        );
        if cursor.is_some() {
            assert!(
                arm[0]["Index Cond"].as_str().is_some_and(|cond| cond.contains("ROW(")),
                "a deep cursor is one row comparison on the index: {plan}"
            );
        }
        // Each membership row reads its one event by identity.
        let events = arm_scans
            .iter()
            .filter(|node| {
                node["Index Name"] == json!("normalized_events_event_identity_key")
                    && node["Index Cond"]
                        .as_str()
                        .is_some_and(|cond| cond.contains("event_identity = m"))
            })
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 1, "{plan}");
        assert!(events[0]["Actual Loops"].as_u64().context("loops")? <= 3, "{plan}");
        assert!(
            arm_scans.iter().all(|node| !(node["Node Type"] == json!("Seq Scan")
                && node["Relation Name"] == json!("child_registration_events"))),
            "{plan}"
        );
    }

    database.cleanup().await
}

#[tokio::test]
async fn v2_name_history_child_registrations_keep_the_scope_of_the_name_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_child_registration_fixture(&database).await?;

    let children = [
        row(110, "registration", "d.parent.eth", "child"),
        row(109, "registration", "c.parent.eth", "child"),
        row(107, "registration", "a.parent.eth", "child"),
        row(105, "registration", "b.parent.eth", "child"),
        row(103, "registration", "a.parent.eth", "child"),
    ];
    let name_scope = v2_history_payload_for_database(
        &database,
        &format!("{CHILD_COUNT_ROUTE}&scope=name"),
    )
    .await?;
    assert_eq!(
        history_rows(&name_scope),
        vec![
            children[0].clone(),
            children[1].clone(),
            row(108, "resolver", "parent.eth", "name"),
            children[2].clone(),
            children[3].clone(),
            children[4].clone(),
            row(102, "record", "parent.eth", "name"),
        ]
    );
    assert_eq!(name_scope["page"]["total_count"], json!(7));

    let registration_scope = v2_history_payload_for_database(
        &database,
        &format!("{CHILD_COUNT_ROUTE}&scope=registration"),
    )
    .await?;
    let mut expected = vec![row(113, "renewal", "parent.eth", "name")];
    expected.extend(children.iter().cloned());
    expected.push(row(101, "registration", "parent.eth", "name"));
    assert_eq!(history_rows(&registration_scope), expected);
    assert_eq!(registration_scope["page"]["total_count"], json!(7));

    // A name whose own scope holds no rows still serves its child rows.
    let quiet = v2_history_payload_for_database(
        &database,
        "/v1/names/a.parent.eth/history?include=child_registrations,total_count&scope=registration",
    )
    .await?;
    assert_eq!(
        history_rows(&quiet),
        vec![row(104, "registration", "x.a.parent.eth", "child")]
    );
    assert_eq!(quiet["page"]["total_count"], json!(1));

    // With no resources at all the name arm is the empty selector; the child arm still reads.
    let parent = bigname_storage::logical_name_id_for_name("ens", "parent.eth");
    let page = bigname_storage::load_name_history_page_with_child_registrations(
        &database.pool,
        &parent,
        &[],
        bigname_storage::HistoryScope::Resource,
        None,
        10,
        bigname_storage::HistorySummaryMode::Count,
        &bigname_storage::HistoryPageOptions::default(),
        None,
    )
    .await?;
    assert_eq!(
        page.rows
            .iter()
            .map(|row| (row.event.event_identity.as_str(), row.subject))
            .collect::<Vec<_>>(),
        vec![
            ("d-grant", bigname_storage::HistorySubject::Child),
            ("c-grant", bigname_storage::HistorySubject::Child),
            ("a-regrant", bigname_storage::HistorySubject::Child),
            ("b-grant", bigname_storage::HistorySubject::Child),
            ("a-grant", bigname_storage::HistorySubject::Child),
        ]
    );
    assert_eq!(page.summary.map(|summary| summary.total_count), Some(5));

    database.cleanup().await
}

/// The API bounds every name history read by a block window at the served publication. A child
/// grant above that window is not in the page or the count, exactly as the name's own rows above
/// it are not.
#[tokio::test]
async fn v2_name_history_child_registrations_stop_at_the_publication_bound() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_child_registration_fixture(&database).await?;
    let parent = bigname_storage::logical_name_id_for_name("ens", "parent.eth");
    let options = bigname_storage::HistoryPageOptions {
        block_window: Some(bigname_storage::HistoryBlockWindow {
            ranges: vec![bigname_storage::ChainBlockRange {
                chain_id: "ethereum-mainnet".to_owned(),
                from_block: None,
                to_block: Some(108),
            }],
        }),
        ..bigname_storage::HistoryPageOptions::default()
    };
    let page = bigname_storage::load_name_history_page_with_child_registrations(
        &database.pool,
        &parent,
        &[Uuid::from_u128(CHILD_PARENT_RESOURCE)],
        bigname_storage::HistoryScope::Both,
        None,
        10,
        bigname_storage::HistorySummaryMode::Count,
        &options,
        None,
    )
    .await?;
    assert_eq!(
        page.rows
            .iter()
            .map(|row| row.event.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["p-resolver", "a-regrant", "b-grant", "a-grant", "p-record", "p-grant"]
    );
    assert_eq!(page.summary.map(|summary| summary.total_count), Some(6));

    // A cursor anchored on a child grant above the window is not in the collection; one anchored
    // on a child grant inside it continues the page. The child arm reads its range from the
    // window alone, so the API must keep supplying a window capped at the publication.
    let cursor_for = |identity: &'static str| {
        let pool = database.pool.clone();
        async move {
            let normalized_event_id: i64 = sqlx::query_scalar(
                "SELECT normalized_event_id FROM bigname_phase.normalized_events
                 WHERE event_identity = $1",
            )
            .bind(identity)
            .fetch_one(&pool)
            .await?;
            anyhow::Ok(bigname_storage::HistoryCursor {
                normalized_event_id: Some(normalized_event_id),
                event_identity: identity.to_owned(),
                position: None,
            })
        }
    };
    let above = cursor_for("c-grant").await?;
    let refused = bigname_storage::load_name_history_page_with_child_registrations(
        &database.pool,
        &parent,
        &[Uuid::from_u128(CHILD_PARENT_RESOURCE)],
        bigname_storage::HistoryScope::Both,
        Some(&above),
        10,
        bigname_storage::HistorySummaryMode::None,
        &options,
        None,
    )
    .await
    .expect_err("a child grant above the window cannot anchor a cursor");
    assert!(
        refused
            .downcast_ref::<bigname_storage::InvalidHistoryCursor>()
            .is_some(),
        "{refused:?}"
    );
    let inside = cursor_for("b-grant").await?;
    let continued = bigname_storage::load_name_history_page_with_child_registrations(
        &database.pool,
        &parent,
        &[Uuid::from_u128(CHILD_PARENT_RESOURCE)],
        bigname_storage::HistoryScope::Both,
        Some(&inside),
        10,
        bigname_storage::HistorySummaryMode::None,
        &options,
        None,
    )
    .await?;
    assert_eq!(
        continued
            .rows
            .iter()
            .map(|row| row.event.event_identity.as_str())
            .collect::<Vec<_>>(),
        vec!["a-grant", "p-record", "p-grant"]
    );

    database.cleanup().await
}
