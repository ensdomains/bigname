// Repeated-window requests through the public router and family-published fixtures.
use bigname_storage::UnixSeconds as WindowSeconds;

fn names_windows_uri(windows: &[String], order: &str, page_size: u64, extra: &str) -> String {
    let mut query = form_urlencoded::Serializer::new(String::new());
    query
        .append_pair("namespace", "ens")
        .append_pair("sort", "expires_at")
        .append_pair("order", order)
        .append_pair("page_size", &page_size.to_string());
    for window in windows {
        query.append_pair("expires_window", window);
    }
    format!("/v1/names?{}{}", query.finish(), extra)
}

async fn names_windows_response(database: &TestDatabase, uri: &str) -> Result<Response> {
    crate::app_router(database.app_state())
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await
        .context("names windows request failed")
}

async fn names_windows_payload(database: &TestDatabase, uri: &str) -> Result<Value> {
    let response = names_windows_response(database, uri).await?;
    let status = response.status();
    let body: Value = read_json(response).await?;
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {status}: {body:#}");
    assert_eq!(body["page"]["total_count"], Value::Null);
    Ok(body)
}

async fn names_windows_walk(database: &TestDatabase, uri: &str) -> Result<Vec<Value>> {
    let mut next = None;
    let mut rows = Vec::new();
    for _ in 0..300 {
        let request = next
            .as_ref()
            .map_or_else(|| uri.to_owned(), |cursor| format!("{uri}&cursor={cursor}"));
        let body = names_windows_payload(database, &request).await?;
        let page = body["data"].as_array().context("window rows")?;
        assert!(page.len() as u64 <= body["page"]["page_size"].as_u64().unwrap());
        rows.extend(page.iter().cloned());
        next = body["page"]["next_cursor"].as_str().map(str::to_owned);
        assert_eq!(body["page"]["has_more"], json!(next.is_some()));
        if next.is_none() {
            return Ok(rows);
        }
    }
    anyhow::bail!("the union cursor did not terminate")
}

#[tokio::test]
async fn v2_names_windows_equal_the_globally_ordered_scalar_union() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_expiry_oracle_fixture(&database).await?;
    // Request identities deliberately differ from expiry order. Adjacent quarter-second
    // windows place boundary rows in exactly one window; the gap window has no matches.
    let windows = [
        "9223372036854775808..18446744073709551616",
        "1850000000.25..1850000000.75",
        "1599999999..1840000000.000000001",
        "1850000000..1850000000.25",
        "1850000000.75..1860000000",
        "1840000001..1849999999",
    ]
    .map(str::to_owned);
    for family in ["expires", "grace_ends"] {
        for order in ["asc", "desc"] {
            for extra in [
                "",
                "&parent=eth&authority=ens_v1,ens_v0",
                "&parent=late.eth&authority=ens_v2",
            ] {
                let mut groups = Vec::new();
                for (index, window) in windows.iter().enumerate() {
                    let (after, before) = window.split_once("..").unwrap();
                    let uri = format!(
                        "/v1/names?namespace=ens&{family}_after={after}&{family}_before={before}&order={order}&page_size=200{extra}"
                    );
                    let scalar = v2_names_payload(&database, &uri).await?;
                    assert!(
                        !scalar["page"]["has_more"].as_bool().unwrap(),
                        "fixture exceeds scalar page"
                    );
                    let mut group = Vec::new();
                    for row in scalar["data"].as_array().unwrap() {
                        assert!(row.get(format!("{family}_window_index")).is_none());
                        let mut row = row.clone();
                        row[format!("{family}_window_index")] = json!(index);
                        group.push(row);
                    }
                    groups.push((after.parse::<WindowSeconds>()?, group));
                }
                // The wire formatter can round fractional fixture expiries. Preserve each scalar
                // page's exact selector order and sort these disjoint groups by their exact bounds.
                groups.sort_by(|a, b| {
                    if order == "desc" {
                        b.0.cmp(&a.0)
                    } else {
                        a.0.cmp(&b.0)
                    }
                });
                let expected: Vec<Value> = groups.into_iter().flat_map(|(_, rows)| rows).collect();
                assert!(!expected.is_empty(), "{extra}");
                let unique: std::collections::BTreeSet<_> = expected
                    .iter()
                    .map(|row| row["name"].as_str().unwrap())
                    .collect();
                assert_eq!(
                    unique.len(),
                    expected.len(),
                    "a scalar result overlapped another window"
                );
                for page_size in [1, 200] {
                    let uri = names_windows_uri(&windows, order, page_size, extra)
                        .replace("expires", family);
                    let actual = names_windows_walk(&database, &uri).await?;
                    assert_eq!(actual.len(), expected.len(), "{uri}");
                    for (index, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
                        assert_eq!(actual, expected, "{uri}: row {index}");
                    }
                }
            }
        }
    }
    // Reuse the dependency's unbounded oracle on the real union filter too.
    let filter = bigname_storage::NameCurrentExpiringFilter {
        deadline: bigname_storage::NameCurrentDeadline::Expiry,
        namespace: "ens".to_owned(),
        windows: windows
            .iter()
            .map(|window| {
                let (after, before) = window.split_once("..").unwrap();
                Ok(bigname_storage::NameCurrentExpiryWindow {
                    expires_after: Some(after.parse()?),
                    expires_before: Some(before.parse()?),
                })
            })
            .collect::<Result<_>>()?,
        authorities: None,
        parent: None,
    };
    for order in oracle_orders() {
        oracle_walk(&database, &filter, order, 7).await?;
    }
    let search = v2_get_response(&database, "/v1/search?q=tie&namespace=ens").await?;
    assert_eq!(search.status(), StatusCode::OK);
    let search: Value = read_json(search).await?;
    let rows = search["data"].as_array().context("search rows")?;
    assert!(!rows.is_empty());
    assert!(
        rows.iter()
            .all(|row| row.get("expires_window_index").is_none())
    );
    database.cleanup().await
}

fn names_windows_with_empty_arms(count: usize, first: &str) -> Vec<String> {
    std::iter::once(first.to_owned())
        .chain((1..count).map(|index| {
            let after = 2_000_000_000 + index * 1000;
            format!("{after}..{}", after + 1)
        }))
        .collect()
}

#[tokio::test]
async fn v2_names_windows_compose_one_global_page_for_one_seven_and_thirty_two_windows()
-> Result<()> {
    use bigname_storage::families::name::seams;
    use std::sync::{Arc, atomic::AtomicU64};
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let mut fixture = OracleFixture::new();
    for index in 0..210 {
        fixture
            .name(
                &database,
                &format!("window-{index:03}.eth"),
                OracleShape::EnsV1,
                json!(ORACLE_TIE),
            )
            .await?;
    }
    for index in 0..40 {
        fixture
            .name(
                &database,
                &format!("gap-{index:03}.eth"),
                OracleShape::EnsV1,
                json!(1_870_000_000),
            )
            .await?;
    }
    fixture.insert(&database).await?;
    publish_test_families(&database, 240).await?;
    for grace in [false, true] {
        for count in [0, 1, 7, 32] {
            for (page_size, first, expected) in [
                (1, "1850000000..1850000001", 2),
                (200, "1850000000..1850000001", 201),
                (200, "1850000001..1850000002", 0),
            ] {
                let mut uri = names_windows_uri(
                    &names_windows_with_empty_arms(count, first),
                    "asc",
                    page_size,
                    "&parent=eth&authority=ens_v1",
                );
                if count == 0 {
                    let (after, before) = first.split_once("..").unwrap();
                    uri = format!(
                        "/v1/names?namespace=ens&expires_after={after}&expires_before={before}&page_size={page_size}&parent=eth&authority=ens_v1"
                    );
                }
                if grace {
                    uri = uri
                        .replace("expires_at", "grace_ends_at")
                        .replace("expires_window", "grace_ends_window")
                        .replace("expires_after", "grace_ends_after")
                        .replace("expires_before", "grace_ends_before")
                        .replace("1850000000", "1857776000")
                        .replace("1850000001", "1857776001")
                        .replace("1850000002", "1857776002");
                }
                let (composed, peak, submitted) = (
                    Arc::new(AtomicU64::new(0)),
                    Arc::new(AtomicU64::new(0)),
                    Arc::new(AtomicU64::new(0)),
                );
                let body = seams::with_composed_names_counter(
                    composed.clone(),
                    seams::with_peak_source_counter(
                        peak.clone(),
                        seams::with_submitted_rows_counter(
                            submitted.clone(),
                            names_windows_payload(&database, &uri),
                        ),
                    ),
                )
                .await?;
                assert_eq!(
                    (
                        composed.load(Ordering::Relaxed),
                        peak.load(Ordering::Relaxed),
                        submitted.load(Ordering::Relaxed)
                    ),
                    (expected, expected, expected),
                    "{count} windows page_size={page_size}"
                );
                assert_eq!(
                    body["data"].as_array().unwrap().len() as u64,
                    expected.min(page_size)
                );
                assert_eq!(body["page"]["has_more"], json!(expected > page_size));
            }
        }
    }
    database.cleanup().await
}

#[tokio::test]
async fn v2_names_windows_real_cursors_bind_thirty_two_ordered_windows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_expiry_oracle_fixture(&database).await?;
    let windows = names_windows_with_empty_arms(32, "1850000000..1850000001");
    let uri = names_windows_uri(&windows, "asc", 1, "");
    let body = names_windows_payload(&database, &uri).await?;
    let cursor = body["page"]["next_cursor"]
        .as_str()
        .context("first page cursor")?;
    let mut equivalent = windows.clone();
    equivalent[0] = "1850000000.000..1850000001.0".to_owned();
    let continued = names_windows_payload(
        &database,
        &format!(
            "{}&cursor={cursor}",
            names_windows_uri(&equivalent, "asc", 200, "")
        ),
    )
    .await?;
    assert!(!continued["data"].as_array().unwrap().is_empty());
    let mut reordered = windows.clone();
    reordered.swap(0, 1);
    let mut changed = windows.clone();
    changed[0] = "1850000000..1850000000.5".to_owned();
    for other in [
        names_windows_uri(&reordered, "asc", 1, ""),
        names_windows_uri(&changed, "asc", 1, ""),
        names_windows_uri(&windows[..31], "asc", 1, ""),
        names_windows_uri(&windows, "desc", 1, ""),
        names_windows_uri(&windows, "asc", 1, "&parent=eth"),
        names_windows_uri(&windows, "asc", 1, "&authority=ens_v1"),
    ] {
        let response =
            names_windows_response(&database, &format!("{other}&cursor={cursor}")).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{other}");
    }
    let mut too_many = windows.clone();
    too_many.push("3000000000..3000000001".to_owned());
    assert_eq!(
        names_windows_response(&database, &names_windows_uri(&too_many, "asc", 1, ""))
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    let scalar_uri =
        "/v1/names?namespace=ens&expires_after=1850000000&expires_before=1850000001&page_size=1";
    let scalar = v2_names_payload(&database, scalar_uri).await?;
    let scalar_cursor = scalar["page"]["next_cursor"].as_str().unwrap();
    let single = names_windows_uri(&windows[..1], "asc", 1, "");
    assert_eq!(
        names_windows_response(&database, &format!("{single}&cursor={scalar_cursor}"))
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    let single = names_windows_payload(&database, &single).await?;
    let window_cursor = single["page"]["next_cursor"].as_str().unwrap();
    assert_eq!(
        v2_names_response(&database, &format!("{scalar_uri}&cursor={window_cursor}"))
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_names_windows_share_one_snapshot_and_continue_after_publication() -> Result<()> {
    use crate::v2::collection_snapshot::finish_test_hooks::{Stage, install_at};
    for stage in [Stage::BeforeRead, Stage::Pinned] {
        let database = TestDatabase::new_migrated().await?;
        let fixture = seed_expiry_oracle_fixture(&database).await?;
        let windows = ["1849000000..1851000000", "1860000000..1860000001"].map(str::to_owned);
        let uri = names_windows_uri(&windows, "asc", 3, "");
        let before = names_windows_payload(&database, &uri).await?;
        let cursor = before["page"]["next_cursor"]
            .as_str()
            .context("before cursor")?
            .to_owned();
        let (_guard, control) = install_at(&database.pool, stage).await?;
        let app = crate::app_router(database.app_state());
        let request_uri = uri.clone();
        let request = tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .uri(request_uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            control.wait_until_reached(),
        )
        .await
        .context("union did not reach snapshot hook")?;
        let (logical, resource) = &fixture.names["early.eth"];
        bigname_storage::insert_normalized_event_fixtures(
            &database.pool,
            &[family_event(
                "windows-early-renewal",
                Some(logical),
                Some(*resource),
                "RegistrationRenewed",
                "ens_v1_registrar_l1",
                241,
                0,
                json!({"expiry": 1_849_500_000}),
            )],
        )
        .await?;
        publish_test_families(&database, 241).await?;
        control.resume().await;
        let response = request.await??;
        let status = response.status();
        let body: Value = read_json(response).await?;
        if stage == Stage::BeforeRead {
            assert_eq!(status, StatusCode::CONFLICT, "{body}");
            assert_eq!(body["error"]["code"], "stale");
        } else {
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["data"], before["data"]);
            assert_eq!(body["meta"]["as_of"], before["meta"]["as_of"]);
        }
        let fresh = names_windows_payload(&database, &uri).await?;
        assert_eq!(fresh["data"][0]["name"], "early.eth");
        let continued = names_windows_payload(&database, &format!("{uri}&cursor={cursor}")).await?;
        assert!(!continued["data"].as_array().unwrap().is_empty());
        assert!(
            continued["data"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["name"] != "early.eth"),
            "a renewal moved before the cursor"
        );
        database.cleanup().await?;
    }
    Ok(())
}
