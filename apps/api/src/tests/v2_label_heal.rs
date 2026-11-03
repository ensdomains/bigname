// A plain label of a name input proves its own text (docs/api-v1.md, "Name inputs"). The
// response shows it in place of the label's placeholder, and the label is staged in
// bigname_phase.ens_names for the preimage import. Mostly on the fixture of
// v2_textless_surface.rs, read through one `AppState` per test so its staging writer can be
// awaited and its outcomes counted.

use crate::label_staging::{LabelStagingConfig, Outcome};

async fn lh_get(state: &AppState, uri: &str) -> Result<(StatusCode, Value)> {
    let response = app_router(state.clone())
        .oneshot(Request::builder().uri(uri).body(Body::empty())?)
        .await?;
    Ok((response.status(), read_json(response).await?))
}

async fn lh_ok(state: &AppState, uri: &str) -> Result<Value> {
    let (status, body) = lh_get(state, uri).await?;
    anyhow::ensure!(status == StatusCode::OK, "{uri}: {status} {body:#}");
    Ok(body)
}

async fn lh_lookup(state: &AppState, body: Value) -> Result<Value> {
    let response = app_router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lookup")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body)?))?,
        )
        .await?;
    let status = response.status();
    let payload: Value = read_json(response).await?;
    anyhow::ensure!(status == StatusCode::OK, "lookup: {status} {payload:#}");
    Ok(payload)
}

/// Every page of `uri`, whole bodies, following the route's own cursors.
async fn lh_pages(state: &AppState, uri: &str) -> Result<Vec<Value>> {
    let mut pages = Vec::new();
    let mut next: Option<String> = None;
    loop {
        let page_uri = match &next {
            None => uri.to_owned(),
            Some(cursor) => format!("{uri}&cursor={cursor}"),
        };
        let body = lh_ok(state, &page_uri).await?;
        next = body["page"]["next_cursor"].as_str().map(str::to_owned);
        pages.push(body);
        if next.is_none() {
            return Ok(pages);
        }
        anyhow::ensure!(pages.len() < 100, "{uri}: too many pages");
    }
}

/// The rows of `ens_names` once every write `state` started has finished.
async fn lh_staged(database: &TestDatabase, state: &AppState) -> Result<Vec<(String, String)>> {
    state.label_staging.settled().await;
    Ok(sqlx::query_as("SELECT hash, name FROM bigname_phase.ens_names ORDER BY hash")
        .fetch_all(&database.pool)
        .await?)
}

/// The staged rows of `labels`, in the table's order.
fn lh_rows(labels: &[&str]) -> Vec<(String, String)> {
    let mut rows: Vec<_> = labels.iter().map(|label| (child_labelhash(label), (*label).to_owned())).collect();
    rows.sort();
    rows
}

/// The outcomes `state` counted, zeros left out.
fn lh_counts(state: &AppState) -> BTreeMap<&'static str, u64> {
    Outcome::ALL
        .into_iter()
        .filter_map(|outcome| {
            let count = state.label_staging.count_of(outcome);
            (count > 0).then_some((outcome.as_str(), count))
        })
        .collect()
}

/// `value` with `from` replaced by `to` in every `name` and `display_name` string.
fn lh_shown(value: &Value, from: &str, to: &str) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(|item| lh_shown(item, from, to)).collect()),
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, item)| {
                    let item = match (key.as_str(), item) {
                        ("name" | "display_name", Value::String(text)) => json!(text.replace(from, to)),
                        _ => lh_shown(item, from, to),
                    };
                    (key.clone(), item)
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

/// The documented `bigname_api` statement that starts with `grant` (deployment.md, "Surviving
/// services"), up to its grantee.
fn lh_documented_grant(grant: &str) -> Result<String> {
    let deployment = include_str!("../../../../docs/deployment.md")
        .split_once("## Surviving services")
        .context("deployment docs must contain the API service grants")?
        .1;
    Ok(deployment
        .split_once(grant)
        .with_context(|| format!("deployment docs must contain `{grant}`"))?
        .1
        .split_once("TO bigname_api;")
        .with_context(|| format!("deployment docs must terminate `{grant}`"))?
        .0
        .to_owned())
}

/// A pool whose sessions run the single statement `setup` first, on the test database.
async fn lh_pool(database: &TestDatabase, setup: String) -> Result<PgPool> {
    let config = database.database_config(2)?;
    let options = PgConnectOptions::from_str(config.database_url.as_deref().context("test URL")?)?
        .options([("search_path", "bigname_phase".to_owned())]);
    Ok(PgPoolOptions::new()
        .max_connections(2)
        .after_connect(move |connection, _metadata| {
            let setup = setup.clone();
            Box::pin(async move {
                sqlx::query(&setup).execute(&mut *connection).await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await?)
}

fn lh_state(pool: PgPool) -> AppState {
    AppState::new_with_rpc_urls(pool, bigname_lookup::ChainRpcUrls::default())
        .with_public_namespaces_for_test(["ens", "basenames"])
}

/// Rows 1 and 2: the plain spelling shows and stages the label it proves. The bracketed one
/// proves nothing. Only the name changes, `meta` included.
#[tokio::test]
async fn v2_label_heal_a_plain_spelling_shows_and_stages_the_label_it_proves() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let state = database.app_state();
    let bracketed_uri = format!("/v1/names/{}", tl_path(&fixture.first_name));

    let bracketed = lh_ok(&state, &bracketed_uri).await?;
    assert_eq!(bracketed["data"]["name"], json!(fixture.first_name), "{bracketed:#}");
    assert!(lh_staged(&database, &state).await?.is_empty());
    assert!(lh_counts(&state).is_empty(), "{:?}", lh_counts(&state));

    let plain = lh_ok(&state, "/v1/names/first.alpha.eth").await?;
    assert_eq!(plain["data"]["name"], json!("first.alpha.eth"), "{plain:#}");
    assert_eq!(plain["data"]["display_name"], json!("first.alpha.eth"), "{plain:#}");
    assert_eq!(plain, lh_shown(&bracketed, &fixture.first_name, "first.alpha.eth"));
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["first"]));
    assert_eq!(lh_counts(&state), BTreeMap::from([("staged", 1)]));

    // A staged candidate is not a preimage: the bracketed spelling still serves the placeholder.
    assert_eq!(lh_ok(&state, &bracketed_uri).await?, bracketed);

    database.cleanup().await
}

/// Row 3: with no known ancestor, the plain `alpha` and `eth` of the request heal and stage.
/// The bracketed `first` stays.
#[tokio::test]
async fn v2_label_heal_mixed_spelling_heals_only_its_plain_labels() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, false).await?;
    let state = database.app_state();

    let detail = lh_ok(&state, &format!("/v1/names/{}", tl_path(&fixture.first_name))).await?;
    assert_eq!(detail["data"]["name"], json!(fixture.first_name), "{detail:#}");
    assert_eq!(detail["data"]["display_name"], json!(fixture.first_name), "{detail:#}");
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["alpha", "eth"]));
    assert_eq!(lh_counts(&state), BTreeMap::from([("staged", 1)]));

    database.cleanup().await
}

/// Row 4: a label with a usable preimage serves its text as before, and nothing is staged.
#[tokio::test]
async fn v2_label_heal_a_known_label_stages_nothing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_textless_fixture(&database, true).await?;
    insert_family_label_preimage(&database.pool, b"first").await?;
    let state = database.app_state();

    let detail = lh_ok(&state, "/v1/names/first.alpha.eth").await?;
    assert_eq!(detail["data"]["name"], json!("first.alpha.eth"), "{detail:#}");
    let known = lh_ok(&state, "/v1/names/known.first.alpha.eth").await?;
    assert_eq!(known["data"]["name"], json!("known.first.alpha.eth"), "{known:#}");
    assert!(lh_staged(&database, &state).await?.is_empty());
    assert!(lh_counts(&state).is_empty(), "{:?}", lh_counts(&state));

    database.cleanup().await
}

/// Rows 5, 6 and 7: a label that fails ENSIP-15 is refused before any read and proves nothing.
/// A case-corrected spelling heals with the normalized text. A missing name serves nothing and
/// stages nothing, though its request proves an ancestor's label.
#[tokio::test]
async fn v2_label_heal_follows_the_normalized_request_and_the_served_row() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_textless_fixture(&database, true).await?;
    let state = database.app_state_with_public_namespaces(&["ens"]);

    let (status, invalid) = lh_get(&state, "/v1/names/fi%20rst.alpha.eth").await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid:#}");
    assert_eq!(invalid["error"]["code"], json!("invalid_input"), "{invalid:#}");
    let invalid = lh_lookup(&state, json!({"inputs": [{"name": "fi rst.alpha.eth"}]})).await?;
    assert_eq!(
        invalid["data"][0]["normalization"]["reason"],
        json!("invalid_normalized_name"),
        "{invalid:#}"
    );
    for uri in ["/v1/names/zzz.first.alpha.eth", "/v1/names/zzz.first.alpha.eth/subnames"] {
        let (status, missing) = lh_get(&state, uri).await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}: {missing:#}");
    }
    assert!(lh_staged(&database, &state).await?.is_empty());
    assert!(lh_counts(&state).is_empty(), "{:?}", lh_counts(&state));

    let corrected = lh_ok(&state, "/v1/names/First.alpha.eth").await?;
    assert_eq!(corrected["data"]["name"], json!("first.alpha.eth"), "{corrected:#}");
    assert_eq!(corrected["data"]["display_name"], json!("first.alpha.eth"), "{corrected:#}");
    let looked_up = lh_lookup(&state, json!({"inputs": [{"name": "First.alpha.eth"}]})).await?;
    let result = &looked_up["data"][0];
    assert_eq!(result["normalization"]["reason"], json!("case_normalized"), "{result:#}");
    assert_eq!(result["record"]["name"], json!("first.alpha.eth"), "{result:#}");
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["first"]));

    database.cleanup().await
}

/// Rows 8 and 9: the parent's proven label heals in every child's name. The child's own
/// unknown label stays bracketed. Cursors, totals, order and `meta` match the bracketed
/// spelling's, page by page and under every sort.
#[tokio::test]
async fn v2_label_heal_subnames_heal_the_parent_and_page_alike() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let state = database.app_state();
    let bracketed_parent = tl_path(&fixture.first_name);

    for query in [
        "page_size=1",
        "page_size=10&include=counts",
        "page_size=2&sort=expires_at",
        "page_size=2&sort=registered_at&order=desc",
        "page_size=10&include_expired=false",
    ] {
        let plain = lh_pages(&state, &format!("/v1/names/first.alpha.eth/subnames?{query}")).await?;
        let bracketed =
            lh_pages(&state, &format!("/v1/names/{bracketed_parent}/subnames?{query}")).await?;
        let expected: Vec<Value> = bracketed
            .iter()
            .map(|page| lh_shown(page, &fixture.first_name, "first.alpha.eth"))
            .collect();
        assert_eq!(plain, expected, "{query}");
        let names: Vec<&str> = plain
            .iter()
            .flat_map(|page| page["data"].as_array().expect("rows"))
            .map(|row| row["name"].as_str().expect("a name"))
            .collect();
        assert!(
            names.contains(&format!("{}.first.alpha.eth", tl_bracket(&fixture.nested)).as_str()),
            "{query}: {names:?}"
        );
        assert!(names.contains(&"known.first.alpha.eth"), "{query}: {names:?}");
    }
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["first"]));
    assert_eq!(lh_counts(&state)["staged"], 1);

    database.cleanup().await
}

/// Rows 10 and 11: a lookup batch heals every record it serves from its name inputs, an
/// address input's records included, and stages a label once however many inputs prove it. A
/// batch without the name input serves the placeholder. `meta` is unchanged.
#[tokio::test]
async fn v2_label_heal_a_lookup_batch_proves_labels_for_all_its_records() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let state = database.app_state_with_public_namespaces(&["ens"]);
    let address = json!({"address": RC_OWNER, "page_size": 50});

    for profile in ["feed", "detail"] {
        let alone = lh_lookup(&state, json!({"profile": profile, "inputs": [address]})).await?;
        let alone_names = tl_column(&[json!({"data": alone["data"][0]["records"]})], "name");
        assert!(alone_names.contains(&fixture.first_name), "{profile}: {alone_names:?}");

        let batch = lh_lookup(
            &state,
            json!({"profile": profile, "inputs": [
                {"name": "first.alpha.eth"},
                {"name": "known.first.alpha.eth"},
                {"name": "zzz.first.alpha.eth"},
                address,
            ]}),
        )
        .await?;
        let records = &batch["data"];
        assert_eq!(records[0]["record"]["name"], json!("first.alpha.eth"), "{batch:#}");
        assert_eq!(records[1]["record"]["name"], json!("known.first.alpha.eth"), "{batch:#}");
        assert_eq!(records[2]["status"], json!("not_found"), "{batch:#}");
        assert_eq!(
            records[3],
            lh_shown(&alone["data"][0], &fixture.first_name, "first.alpha.eth"),
            "{profile}: the address input's records heal, with their page unchanged"
        );
        assert_eq!(batch["meta"]["as_of"], alone["meta"]["as_of"], "{profile}");
    }
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["first"]));
    assert_eq!(lh_counts(&state), BTreeMap::from([("staged", 1), ("duplicate", 1)]));

    database.cleanup().await
}

/// Rows 12 and 13: a process writes a label once however often or concurrently it is
/// requested, every response healed. A second process conflicts on the key and adds no row.
#[tokio::test]
async fn v2_label_heal_stages_a_label_once_across_requests_and_processes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_textless_fixture(&database, true).await?;
    let state = database.app_state();

    let first = lh_ok(&state, "/v1/names/first.alpha.eth").await?;
    let again = lh_ok(&state, "/v1/names/first.alpha.eth").await?;
    assert_eq!(again, first);
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["first"]));
    assert_eq!(lh_counts(&state), BTreeMap::from([("staged", 1), ("duplicate", 1)]));

    let concurrent = database.app_state();
    let responses = futures_util::future::join_all(
        (0..8).map(|_| lh_ok(&concurrent, "/v1/names/first.alpha.eth")),
    )
    .await;
    for response in responses {
        assert_eq!(response?, first);
    }
    assert_eq!(lh_staged(&database, &concurrent).await?, lh_rows(&["first"]));
    // One write, which met the first process's row. Seven requests found the label reserved.
    assert_eq!(lh_counts(&concurrent), BTreeMap::from([("duplicate", 8)]));

    database.cleanup().await
}

/// Row 14: a role with only the documented reads serves healed answers and is counted
/// `denied`. Startup does not require the write. The documented INSERT grant is the whole of
/// what the write needs, and the running process writes as soon as it lands.
#[tokio::test]
async fn v2_label_heal_a_role_without_insert_serves_alike_and_the_documented_grant_writes()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_textless_fixture(&database, true).await?;
    let role = format!(
        "label_heal_{}_{}",
        std::process::id(),
        NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
    );
    for statement in [
        format!("CREATE ROLE {role} NOLOGIN"),
        format!("GRANT USAGE ON SCHEMA bigname_phase TO {role}"),
        format!(
            "GRANT SELECT ON TABLE {} TO {role}",
            lh_documented_grant("GRANT SELECT ON TABLE\n")?
        ),
        format!(
            "GRANT EXECUTE ON FUNCTION {} TO {role}",
            lh_documented_grant("GRANT EXECUTE ON FUNCTION ")?
        ),
    ] {
        sqlx::query(&statement).execute(&database.pool).await?;
    }
    let pool = lh_pool(&database, format!("SET ROLE {role}")).await?;
    crate::startup_preflight::ensure_verified_lookup_ddl_available(&pool).await?;

    let refused = lh_state(pool.clone());
    let detail = lh_ok(&refused, "/v1/names/first.alpha.eth").await?;
    assert_eq!(detail["data"]["name"], json!("first.alpha.eth"), "{detail:#}");
    assert!(lh_staged(&database, &refused).await?.is_empty());
    assert_eq!(lh_counts(&refused), BTreeMap::from([("denied", 1)]));

    let insert = lh_documented_grant("GRANT INSERT ON TABLE ")?;
    assert_eq!(insert.trim(), "bigname_phase.ens_names", "the API writes one table");
    sqlx::query(&format!("GRANT INSERT ON TABLE {insert} TO {role}"))
        .execute(&database.pool)
        .await?;
    // The same process writes once the grant lands: a refused label is not held as staged.
    assert_eq!(lh_ok(&refused, "/v1/names/first.alpha.eth").await?, detail);
    assert_eq!(lh_staged(&database, &refused).await?, lh_rows(&["first"]));
    assert_eq!(
        lh_counts(&refused),
        BTreeMap::from([("denied", 1), ("staged", 1)])
    );

    pool.close().await;
    sqlx::query(&format!("DROP OWNED BY {role}")).execute(&database.pool).await?;
    sqlx::query(&format!("DROP ROLE {role}")).execute(&database.pool).await?;
    database.cleanup().await
}

/// Rows 15 and 16: a read-only database, which a hot standby is, and staging configured off
/// both serve healed answers and write nothing.
#[tokio::test]
async fn v2_label_heal_a_read_only_database_or_staging_off_writes_nothing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_textless_fixture(&database, true).await?;

    let pool = lh_pool(&database, "SET default_transaction_read_only = on".to_owned()).await?;
    let read_only = lh_state(pool.clone());
    let detail = lh_ok(&read_only, "/v1/names/first.alpha.eth").await?;
    assert_eq!(detail["data"]["name"], json!("first.alpha.eth"), "{detail:#}");
    assert!(lh_staged(&database, &read_only).await?.is_empty());
    assert_eq!(lh_counts(&read_only), BTreeMap::from([("read_only", 1)]));
    pool.close().await;

    let off = database.app_state().with_label_staging(LabelStagingConfig {
        per_second: 0,
        ..LabelStagingConfig::default()
    });
    assert_eq!(lh_ok(&off, "/v1/names/first.alpha.eth").await?, detail);
    assert!(lh_staged(&database, &off).await?.is_empty());
    assert_eq!(lh_counts(&off), BTreeMap::from([("disabled", 1)]));

    database.cleanup().await
}

/// Row 17: writes beyond the budget are dropped, every response still healed, and the
/// refilled budget writes what was dropped.
#[tokio::test]
async fn v2_label_heal_throttles_writes_beyond_the_budget() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, false).await?;
    let state = database.app_state().with_label_staging(LabelStagingConfig {
        per_second: 1,
        burst: 1,
        ..LabelStagingConfig::default()
    });
    let alpha_eth = format!("/v1/names/{}", tl_path(&fixture.first_name));
    // `[eth]` is read as `eth` (docs/api-v1.md, "Name inputs"), so this spelling proves it too.
    let first = format!(
        "/v1/names/first.{}.{}",
        tl_path(&tl_bracket(&child_labelhash("alpha"))),
        tl_path(&tl_bracket(&child_labelhash("eth")))
    );

    let (one, two) = tokio::join!(lh_ok(&state, &alpha_eth), lh_ok(&state, &first));
    assert_eq!(one?["data"]["name"], json!(fixture.first_name));
    assert_eq!(
        two?["data"]["name"],
        json!(format!("first.{}.eth", tl_bracket(&child_labelhash("alpha"))))
    );
    let staged = lh_staged(&database, &state).await?;
    assert!(
        staged == lh_rows(&["alpha", "eth"]) || staged == lh_rows(&["first", "eth"]),
        "{staged:?}"
    );
    assert_eq!(lh_counts(&state), BTreeMap::from([("staged", 1), ("throttled", 1)]));

    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    lh_ok(&state, &alpha_eth).await?;
    lh_ok(&state, &first).await?;
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["alpha", "eth", "first"]));
    assert_eq!(
        lh_counts(&state),
        BTreeMap::from([("staged", 2), ("throttled", 1), ("duplicate", 1)])
    );

    database.cleanup().await
}

/// Row 18: a preimage stored as not normalized under an older normalizer is served bracketed,
/// so the request's proof heals it and stages it. The import keeps the stored row, which
/// `phase-runner redo --phase recompute-flags` repairs (docs/storage.md).
#[tokio::test]
async fn v2_label_heal_a_preimage_flagged_under_an_older_normalizer_heals_from_the_request()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let labelhash = insert_family_label_preimage(&database.pool, b"first").await?;
    sqlx::query(
        "UPDATE bigname_phase.label_preimages
         SET normalizer_version = 'older', normalized_under_version = false,
             normalization_error = 'not normalized under the older normalizer'
         WHERE labelhash = $1",
    )
    .bind(&labelhash)
    .execute(&database.pool)
    .await?;
    let state = database.app_state();
    let bracketed_uri = format!("/v1/names/{}", tl_path(&fixture.first_name));
    assert_eq!(
        lh_ok(&state, &bracketed_uri).await?["data"]["name"],
        json!(fixture.first_name)
    );

    let detail = lh_ok(&state, "/v1/names/first.alpha.eth").await?;
    assert_eq!(detail["data"]["name"], json!("first.alpha.eth"), "{detail:#}");
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["first"]));
    let imported =
        bigname_storage::import_label_preimages_from_ens_names_table(&database.pool, None, None)
            .await?;
    assert_eq!((imported.scanned_row_count, imported.retained_row_count), (1, 0));
    assert_eq!(
        lh_ok(&state, &bracketed_uri).await?["data"]["name"],
        json!(fixture.first_name)
    );

    database.cleanup().await
}

/// Row 21: the records route carries no name of the row, so its body is the bracketed
/// spelling's. The row's served name still stages the proven label.
#[tokio::test]
async fn v2_label_heal_the_records_route_stages_without_changing_its_body() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let state = database.app_state();

    for query in ["", "?source=indexed&keys=addr:60", "?include=inventory"] {
        let bracketed =
            lh_ok(&state, &format!("/v1/names/{}/records{query}", tl_path(&fixture.first_name)))
                .await?;
        let plain = lh_ok(&state, &format!("/v1/names/first.alpha.eth/records{query}")).await?;
        assert_eq!(plain, bracketed, "{query}");
        assert!(plain["data"].get("canonical_name").is_none(), "{plain:#}");
    }
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["first"]));
    assert_eq!(lh_counts(&state), BTreeMap::from([("staged", 1), ("duplicate", 2)]));

    database.cleanup().await
}

/// Row 22: routes whose requests are not names, and history, serve the placeholder as before
/// and stage nothing.
#[tokio::test]
async fn v2_label_heal_other_routes_are_unchanged_and_stage_nothing() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, true).await?;
    let state = database.app_state();
    let bracketed = tl_path(&fixture.first_name);

    for (plain, hashed) in [
        (
            "/v1/names/first.alpha.eth/history?page_size=50".to_owned(),
            format!("/v1/names/{bracketed}/history?page_size=50"),
        ),
        (
            "/v1/events?name=first.alpha.eth".to_owned(),
            format!("/v1/events?name={bracketed}"),
        ),
        (
            "/v1/permissions?name=first.alpha.eth".to_owned(),
            format!("/v1/permissions?name={bracketed}"),
        ),
    ] {
        let (status, body) = lh_get(&state, &plain).await?;
        let (hashed_status, hashed_body) = lh_get(&state, &hashed).await?;
        assert_eq!(status, hashed_status, "{plain}: {body:#}");
        assert_eq!(body["data"], hashed_body["data"], "{plain}");
    }
    let history = lh_ok(&state, "/v1/names/first.alpha.eth/history?page_size=50").await?;
    for row in history["data"].as_array().expect("rows") {
        assert_eq!(row["name"], json!(fixture.first_name), "{row:#}");
    }
    for uri in [
        format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=50"),
        "/v1/search?q=alpha&match=contains&page_size=50".to_owned(),
    ] {
        let listed = lh_ok(&state, &uri).await?;
        let names = tl_column(&[json!({"data": listed["data"]})], "name");
        assert!(names.contains(&fixture.first_name), "{uri}: {names:?}");
    }
    assert!(lh_staged(&database, &state).await?.is_empty());
    assert!(lh_counts(&state).is_empty(), "{:?}", lh_counts(&state));

    database.cleanup().await
}

/// Row 23: a label at two positions of one name heals at both and is one candidate.
#[tokio::test]
async fn v2_label_heal_a_label_at_two_positions_heals_both() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let x = child_labelhash("x");
    let (id, resource) = seed_textless_family_name(
        &database,
        &[x.clone(), x.clone(), child_labelhash("eth")],
        0x8f1_0000,
        "ens_v1",
        202,
    )
    .await?;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &textless_child_events(
            &id,
            resource,
            &bigname_lookup::ens_namehash_hex("x.eth")?,
            &x,
            RC_OWNER,
            202,
        ),
    )
    .await?;
    publish_test_families(&database, 240).await?;
    let state = database.app_state();

    let detail = lh_ok(&state, "/v1/names/x.x.eth").await?;
    assert_eq!(detail["data"]["name"], json!("x.x.eth"), "{detail:#}");
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["x", "eth"]));
    assert_eq!(lh_counts(&state), BTreeMap::from([("staged", 1)]));

    database.cleanup().await
}

/// Row 24: a bracketed `eth` is read as `eth`, so it proves itself. A name served under its
/// raw bytes has nothing to heal and stages nothing.
#[tokio::test]
async fn v2_label_heal_a_bracketed_eth_is_read_as_its_text() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let fixture = seed_textless_fixture(&database, false).await?;
    let state = database.app_state();
    let eth = tl_path(&tl_bracket(&child_labelhash("eth")));

    let alpha = lh_ok(&state, &format!("/v1/names/alpha.{eth}")).await?;
    assert_eq!(alpha["data"]["name"], json!("alpha.eth"), "{alpha:#}");
    assert!(lh_staged(&database, &state).await?.is_empty());

    let first = format!(
        "/v1/names/{}.{}.{eth}",
        tl_path(&tl_bracket(&fixture.first)),
        tl_path(&tl_bracket(&child_labelhash("alpha")))
    );
    let detail = lh_ok(&state, &first).await?;
    assert_eq!(
        detail["data"]["name"],
        json!(format!("{}.{}.eth", tl_bracket(&fixture.first), tl_bracket(&child_labelhash("alpha")))),
        "{detail:#}"
    );
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["eth"]));

    database.cleanup().await
}

/// A write that fails is counted, leaves nothing, and is tried again by a later request. A
/// write that cannot finish is cut off at one second, and the response never waited for it.
#[tokio::test]
async fn v2_label_heal_a_failed_or_slow_write_is_soft_and_retried() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_textless_fixture(&database, true).await?;
    let state = database.app_state();
    sqlx::raw_sql(
        "CREATE FUNCTION bigname_phase.label_heal_refuse() RETURNS trigger LANGUAGE plpgsql
         AS $$ BEGIN RAISE EXCEPTION 'refused by the test'; END $$;
         CREATE TRIGGER label_heal_refuse BEFORE INSERT ON bigname_phase.ens_names
         FOR EACH ROW EXECUTE FUNCTION bigname_phase.label_heal_refuse();",
    )
    .execute(&database.pool)
    .await?;
    let detail = lh_ok(&state, "/v1/names/first.alpha.eth").await?;
    assert_eq!(detail["data"]["name"], json!("first.alpha.eth"), "{detail:#}");
    assert!(lh_staged(&database, &state).await?.is_empty());
    assert_eq!(lh_counts(&state), BTreeMap::from([("failed", 1)]));

    sqlx::raw_sql(
        "CREATE OR REPLACE FUNCTION bigname_phase.label_heal_refuse() RETURNS trigger
         LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(5); RETURN NEW; END $$;",
    )
    .execute(&database.pool)
    .await?;
    let started = std::time::Instant::now();
    assert_eq!(lh_ok(&state, "/v1/names/first.alpha.eth").await?, detail);
    assert!(lh_staged(&database, &state).await?.is_empty());
    assert!(started.elapsed() < std::time::Duration::from_secs(4), "{:?}", started.elapsed());
    assert_eq!(lh_counts(&state), BTreeMap::from([("failed", 2)]));

    sqlx::raw_sql("DROP TRIGGER label_heal_refuse ON bigname_phase.ens_names")
        .execute(&database.pool)
        .await?;
    assert_eq!(lh_ok(&state, "/v1/names/first.alpha.eth").await?, detail);
    assert_eq!(lh_staged(&database, &state).await?, lh_rows(&["first"]));
    assert_eq!(lh_counts(&state), BTreeMap::from([("failed", 2), ("staged", 1)]));

    database.cleanup().await
}
