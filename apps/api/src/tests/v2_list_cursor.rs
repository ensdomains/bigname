// The continuation cursor of the current-state lists whose rows the composed name reader serves
// (TYR-36 step 7b slice 5, D10): `/v1/search`, `/v1/names` and a resolver's bound names. A
// cursor holds the list's sort, its filters and the position of the last row it returned, plus
// the `at` token when the request pinned `at`; no publication, generation or evaluation time. A
// continuation reads what is published when it runs (`v2::list_cursor`), so it is portable
// across publications and across the publication switch, and only a publication that lands
// during one request's own read refuses it.

const LIST_CURSOR_OTHER_RESOLVER: &str = "0x0000000000000000000000000000000000000def";
const LIST_CURSOR_INVALID: &str = "cursor must be a valid pagination cursor";
const LIST_CURSOR_RETRY: &str =
    "collection publication changed during the read; retry the request";

/// The adopted lists over the switch fixture: each walks in two pages of one row.
fn list_cursor_routes() -> Result<Vec<(String, &'static str)>> {
    let after = switch_timestamp(1_700_000_000)?;
    let before = switch_timestamp(1_960_000_000)?;
    Ok(vec![
        ("/v1/search?q=eth&match=contains&page_size=1".to_owned(), ""),
        ("/v1/search?q=eth&match=contains&namespace=ens&page_size=1".to_owned(), ""),
        (
            format!(
                "/v1/names?namespace=ens&expires_after={after}&expires_before={before}\
                 &order=asc&page_size=1"
            ),
            "",
        ),
        (
            format!(
                "/v1/names?namespace=ens&expires_after={after}&expires_before={before}\
                 &order=desc&page_size=1"
            ),
            "",
        ),
        (
            format!("/v1/resolvers/1/{SWITCH_RESOLVER}?page_size=1"),
            "/data/bound_names",
        ),
    ])
}

async fn list_cursor_get(database: &TestDatabase, on: bool, uri: &str) -> Result<(StatusCode, Value)> {
    let response = bigname_storage::publication_source::with_serve_from_families(
        on,
        v2_get_response(database, uri),
    )
    .await?;
    let status = response.status();
    Ok((status, read_json(response).await?))
}

/// The names of a page and its continuation, which must be a 200.
async fn list_cursor_page(
    database: &TestDatabase,
    on: bool,
    uri: &str,
    holder: &str,
) -> Result<(Vec<Value>, Option<String>)> {
    let (status, body) = list_cursor_get(database, on, uri).await?;
    anyhow::ensure!(status == StatusCode::OK, "{uri} (switch {on}): {body:#}");
    let held = body
        .pointer(holder)
        .with_context(|| format!("{uri}: no {holder} in {body:#}"))?;
    let names = held["data"]
        .as_array()
        .context("page data")?
        .iter()
        .map(|row| row["name"].clone())
        .collect();
    let next = held["page"]["next_cursor"].as_str().map(str::to_owned);
    anyhow::ensure!(
        held["page"]["has_more"] == json!(next.is_some()),
        "{uri}: has_more disagrees with next_cursor: {body:#}"
    );
    Ok((names, next))
}

fn list_cursor_continue(uri: &str, cursor: &str) -> String {
    format!("{uri}&cursor={cursor}")
}

/// An issued cursor moved to `position` (each key must already be a position key), in the
/// shape this contract writes: no publication token, generation or evaluation time.
fn list_cursor_at(cursor: &str, position: &[(&str, &str)]) -> String {
    let mut payload = crate::v2::decode(cursor).expect("issued cursor decodes");
    payload.last_item.remove("publication");
    payload.last_item.remove("resolver_generation");
    payload.snapshot = None;
    payload.evaluated_at = None;
    for (key, value) in position {
        assert!(payload.last_item.contains_key(*key), "{key} is a position key");
        payload.last_item.insert((*key).to_owned(), (*value).to_owned());
    }
    crate::v2::encode(&payload)
}

async fn assert_list_cursor_refused(
    database: &TestDatabase,
    on: bool,
    uri: &str,
    label: &str,
) -> Result<()> {
    let (status, body) = list_cursor_get(database, on, uri).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {uri}: {body:#}");
    assert_eq!(
        body["error"],
        json!({"code": "invalid_input", "details": {}, "message": LIST_CURSOR_INVALID}),
        "{label}: {uri}"
    );
    Ok(())
}

/// Every page of every adopted list, walked with the switch off and on: the same rows, and the
/// same cursor bytes, since a cursor no longer carries its side's generation. A cursor issued
/// with the switch off then continues with it on, and the other way, to the same set.
#[tokio::test]
async fn v2_list_cursors_are_the_same_with_the_switch_off_and_on() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    seed_switch_resolver_current(&database).await?;
    for (uri, holder) in list_cursor_routes()? {
        let mut walks = Vec::new();
        for on in [false, true] {
            let mut rows = Vec::new();
            let mut cursors = Vec::new();
            let mut page_uri = uri.clone();
            loop {
                let (names, next) = list_cursor_page(&database, on, &page_uri, holder).await?;
                rows.extend(names);
                let Some(next) = next else { break };
                page_uri = list_cursor_continue(&uri, &next);
                cursors.push(next);
                anyhow::ensure!(cursors.len() < 20, "{uri}: too many pages");
            }
            walks.push((rows, cursors));
        }
        assert_eq!(walks[0], walks[1], "{uri}: switch off (left) and on (right)");
        let (rows, cursors) = &walks[0];
        assert_eq!(rows.len(), 2, "{uri}: {rows:?}");
        assert_eq!(cursors.len(), 1, "{uri}");
        for (issued, continued) in [(false, true), (true, false)] {
            let (first, next) = list_cursor_page(&database, issued, &uri, holder).await?;
            let next = next.context("a continuation")?;
            let (rest, last) =
                list_cursor_page(&database, continued, &list_cursor_continue(&uri, &next), holder)
                    .await?;
            assert_eq!(last, None, "{uri}");
            let crossed = first.into_iter().chain(rest).collect::<Vec<_>>();
            assert_eq!(&crossed, rows, "{uri}: issued with the switch {issued}");
        }
        // The issued cursor holds the list's position and binding and nothing else.
        let payload = crate::v2::decode(&cursors[0]).expect("issued cursor decodes");
        assert_eq!(payload.snapshot, None, "{uri}");
        assert_eq!(payload.evaluated_at, None, "{uri}");
        assert_eq!(payload.last_item.len(), 4, "{uri}: {payload:?}");
    }
    database.cleanup().await
}

/// alpha.eth moves to another resolver and is renewed to a later expiry at block 241.
async fn advance_list_cursor_fixture(database: &TestDatabase) -> Result<()> {
    let (alpha, _) = phase_logical_identity("ens", "alpha.eth")?;
    let (alpha_resource,): (Uuid,) = sqlx::query_as(
        "SELECT resource_id FROM surface_bindings WHERE logical_name_id = $1",
    )
    .bind(&alpha)
    .fetch_one(&database.pool)
    .await?;
    let alpha_node = alpha.strip_prefix("ens:").expect("ens id").to_owned();
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            switch_event(
                "list-cursor-alpha-resolver",
                Some(&alpha),
                Some(alpha_resource),
                "ResolverChanged",
                "ens_v1_registry_l1",
                241,
                0,
                json!({"node": alpha_node, "resolver": LIST_CURSOR_OTHER_RESOLVER}),
            ),
            switch_event(
                "list-cursor-alpha-renewal",
                Some(&alpha),
                Some(alpha_resource),
                "RegistrationRenewed",
                "ens_v1_registrar_l1",
                241,
                1,
                json!({"expiry": 1_958_000_000i64}),
            ),
        ],
    )
    .await?;
    publish_project_and_families(database, 241).await?;
    // Project's batch replaces the served resolver rows, and writes one only for a declared
    // resolver (see `seed_switch_resolver_current`): seed it again at the new publication.
    sqlx::query(
        "INSERT INTO bigname_phase.resolver_current (chain_id, resolver_address,
             declared_summary, support_status, chain_positions, canonicality_summary,
             manifest_version)
         SELECT lineage.chain_id, $2, '{}'::jsonb, 'supported',
                jsonb_build_object('target_block_number', lineage.block_number,
                                   'target_block_hash', lineage.block_hash),
                jsonb_build_object('state', 'canonical_lineage'), 1
         FROM bigname_phase.chain_lineage lineage
         WHERE lineage.chain_id = $1 AND lineage.block_number = 241
         ON CONFLICT DO NOTHING",
    )
    .bind(SWITCH_CHAIN)
    .bind(SWITCH_RESOLVER)
    .execute(&database.pool)
    .await?;
    Ok(())
}

/// A cursor issued at block 240 continues after the publication moves to 241: the page reads
/// 241 and returns the rows after the cursor's position, whether or not the row the cursor came
/// from still sits there. alpha.eth leaves the resolver's bound names and moves ahead of the
/// descending expiry cursor that was issued on it; the continuation returns beta.eth, and the
/// ascending walk meets alpha.eth at its new expiry. With `at` pinned to 240 the same
/// continuation is refused as stale (ruling J5), and without it the pinned cursor is foreign.
#[tokio::test]
async fn v2_list_cursor_issued_before_a_publication_reads_what_is_there_now() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    seed_switch_resolver_current(&database).await?;
    let routes = list_cursor_routes()?;
    let at_240 = switch_timestamp(1_700_000_240)?;
    let pinned = format!("/v1/resolvers/1/{SWITCH_RESOLVER}?page_size=1&at={at_240}");
    let mut issued = Vec::new();
    for on in [false, true] {
        for (uri, holder) in &routes {
            let (first, next) = list_cursor_page(&database, on, uri, holder).await?;
            issued.push((on, uri.clone(), *holder, first, next.context("a continuation")?));
        }
        let (first, next) = list_cursor_page(&database, on, &pinned, "/data/bound_names").await?;
        assert_eq!(first, [json!("alpha.eth")]);
        issued.push((on, pinned.clone(), "/data/bound_names", first, next.context("pinned")?));
    }

    advance_list_cursor_fixture(&database).await?;

    for (on, uri, holder, first, cursor) in &issued {
        let continued = list_cursor_continue(uri, cursor);
        if uri == &pinned {
            let (status, body) = list_cursor_get(&database, *on, &continued).await?;
            assert_eq!(status, StatusCode::CONFLICT, "{continued}: {body:#}");
            assert_eq!(body["error"]["code"], json!("stale"), "{body:#}");
            let unpinned = continued.replace(&format!("&at={at_240}"), "");
            assert_list_cursor_refused(&database, *on, &unpinned, "at-pinned cursor without at")
                .await?;
            continue;
        }
        let (rest, last) = list_cursor_page(&database, *on, &continued, holder).await?;
        assert_eq!(last, None, "{continued} (switch {on})");
        let expected = if first == &[json!("alpha.eth")] {
            [json!("beta.eth")]
        } else {
            [json!("alpha.eth")]
        };
        assert_eq!(rest, expected, "{continued} (switch {on}), first page {first:?}");
        let (_, body) = list_cursor_get(&database, *on, &continued).await?;
        let as_of = &body["meta"]["as_of"];
        assert!(
            as_of
                .as_object()
                .is_some_and(|chains| chains.values().all(|at| at["block_number"] == json!(241))),
            "{continued}: the page reads 241: {as_of}"
        );
    }
    database.cleanup().await
}

/// A cursor that does not decode, belongs to another list, carries other filters, or still
/// holds the publication binding this contract dropped answers 400 `invalid_input`; nothing is
/// read. Restarting the list without the cursor is the remedy.
#[tokio::test]
async fn v2_list_cursor_refuses_malformed_foreign_and_publication_bound_cursors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    seed_switch_resolver_current(&database).await?;
    let routes = list_cursor_routes()?;
    for on in [false, true] {
        let mut cursors = Vec::new();
        for (uri, holder) in &routes {
            let (_, next) = list_cursor_page(&database, on, uri, holder).await?;
            cursors.push(next.context("a continuation")?);
        }
        for (index, (uri, _)) in routes.iter().enumerate() {
            for malformed in ["not-a-cursor", "7b7d", "00"] {
                assert_list_cursor_refused(
                    &database,
                    on,
                    &list_cursor_continue(uri, malformed),
                    "malformed",
                )
                .await?;
            }
            // Search with and without a namespace, and the two names orders, bind different
            // filters over the same position keys: each is foreign to the others.
            for (other, cursor) in cursors.iter().enumerate() {
                if other != index {
                    assert_list_cursor_refused(
                        &database,
                        on,
                        &list_cursor_continue(uri, cursor),
                        "another list's cursor",
                    )
                    .await?;
                }
            }
            let mut filtered = crate::v2::decode(&cursors[index]).expect("issued cursor decodes");
            filtered.filters.insert("namespace".to_owned(), "basenames".to_owned());
            let mut unknown = crate::v2::decode(&cursors[index]).expect("issued cursor decodes");
            unknown.last_item.insert("extra".to_owned(), "1".to_owned());
            let mut timed = crate::v2::decode(&cursors[index]).expect("issued cursor decodes");
            timed.evaluated_at = Some("2026-06-10T00:00:00Z".to_owned());
            let mut published = crate::v2::decode(&cursors[index]).expect("issued cursor decodes");
            published.snapshot = Some(format!("publication-0x{}", "ab".repeat(32)));
            let mut generation = crate::v2::decode(&cursors[index]).expect("issued cursor decodes");
            generation
                .last_item
                .insert("resolver_generation".to_owned(), "{}".to_owned());
            for (label, payload) in [
                ("other filters", filtered),
                ("an unknown position key", unknown),
                ("an evaluation time", timed),
                ("a publication token", published),
                ("a generation", generation),
            ] {
                assert_list_cursor_refused(
                    &database,
                    on,
                    &list_cursor_continue(uri, &crate::v2::encode(&payload)),
                    label,
                )
                .await?;
            }
        }
    }
    database.cleanup().await
}

/// A well-formed cursor whose position lies after the last row answers an empty last page, and
/// one whose position no row holds continues from where that row would sort.
#[tokio::test]
async fn v2_list_cursor_past_the_end_answers_an_empty_last_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    seed_switch_resolver_current(&database).await?;
    let namehash_ff = format!("0x{}", "ff".repeat(32));
    let end_expiry = switch_timestamp(1_959_999_999)?;
    let gap_expiry = switch_timestamp(1_850_000_000)?;
    let routes = list_cursor_routes()?;
    for on in [false, true] {
        for (uri, holder) in &routes {
            let (_, next) = list_cursor_page(&database, on, uri, holder).await?;
            let next = next.context("a continuation")?;
            let (past_end, gap, gap_expects): (Vec<(&str, &str)>, Vec<(&str, &str)>, &str) =
                if uri.starts_with("/v1/search") {
                    (
                        vec![("display_name", "zzzz.eth"), ("normalized_name", "zzzz.eth"),
                             ("namespace", "ens"), ("namehash", &namehash_ff)],
                        vec![("display_name", "alz.eth"), ("normalized_name", "alz.eth"),
                             ("namespace", "ens"), ("namehash", &namehash_ff)],
                        "beta.eth",
                    )
                } else if uri.starts_with("/v1/resolvers") {
                    (
                        vec![("sort_value", "zzzz.eth"), ("normalized_name", "zzzz.eth"),
                             ("namespace", "ens"), ("namehash", &namehash_ff)],
                        vec![("sort_value", "alz.eth"), ("normalized_name", "alz.eth"),
                             ("namespace", "ens"), ("namehash", &namehash_ff)],
                        "beta.eth",
                    )
                } else if uri.contains("order=asc") {
                    (
                        vec![("expires_at", &end_expiry), ("name", "zzzz.eth"),
                             ("namespace", "ens"), ("namehash", &namehash_ff)],
                        vec![("expires_at", &gap_expiry), ("name", "gap.eth"),
                             ("namespace", "ens"), ("namehash", &namehash_ff)],
                        "alpha.eth",
                    )
                } else {
                    continue;
                };
            let (rows, last) = list_cursor_page(
                &database,
                on,
                &list_cursor_continue(uri, &list_cursor_at(&next, &past_end)),
                holder,
            )
            .await?;
            assert!(rows.is_empty(), "{uri} past the end (switch {on}): {rows:?}");
            assert_eq!(last, None, "{uri} past the end");
            let (rows, last) = list_cursor_page(
                &database,
                on,
                &list_cursor_continue(uri, &list_cursor_at(&next, &gap)),
                holder,
            )
            .await?;
            assert_eq!(rows, [json!(gap_expects)], "{uri} from a gap (switch {on})");
            assert_eq!(last, None, "{uri} from a gap");
        }
    }
    database.cleanup().await
}

/// A publication that lands while one continuation reads still refuses that request, but the
/// cursor stays good: the 409 asks for a retry, and the same cursor then continues.
#[tokio::test]
async fn v2_list_cursor_continuation_retries_when_publication_changes_during_the_read()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_switch_names_fixture(&database).await?;
    seed_switch_resolver_current(&database).await?;
    for (uri, holder) in list_cursor_routes()? {
        if uri.starts_with("/v1/search") {
            // Search admits its namespaces with its own recheck and has no collection finish.
            continue;
        }
        let (_, next) = list_cursor_page(&database, false, &uri, holder).await?;
        let continued = list_cursor_continue(&uri, &next.context("a continuation")?);
        let message = resolver_publication_replaced_before_finish(&database, continued.clone())
            .await?;
        assert_eq!(message, LIST_CURSOR_RETRY, "{continued}");
        let (rows, last) = list_cursor_page(&database, false, &continued, holder).await?;
        assert_eq!(rows.len(), 1, "{continued}");
        assert_eq!(last, None, "{continued}");
    }
    database.cleanup().await
}
