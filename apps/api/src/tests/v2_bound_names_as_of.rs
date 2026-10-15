// The bound-name summary as the publication's block reads it
// (crates/storage/src/families/name/loaders.rs `surfaces_at`): Interpret fills a surface's raw
// bytes and its shadow state in place from a later preimage observation, and the summary writer
// counts them only from the block of that witness. Each refresh path that composes a name
// stores the same rows whether Interpret has observed the name's bytes past the block or not,
// and a witness repair never loses a name's summary. Every step checks the page and its count
// against the walk, and the stored summaries against a fresh composition of every surface.

/// The witness of `[first].alpha.eth` in these tests.
const AS_OF_WITNESS: &str = "as-of-first-preimage";

/// How Interpret observes the bytes of `[first].alpha.eth`: as the raw name `first.alpha.eth`,
/// or as bytes that are not text, which shadow the name.
#[derive(Clone, Copy)]
enum Observed {
    Raw,
    Shadow,
}

/// The fixture of `v2_bound_names_paging.rs`, with blocks through 244.
async fn seed_as_of(database: &TestDatabase) -> Result<Paging> {
    let paging = seed_bound_paging(database).await?;
    let blocks: Vec<RawBlock> = (242..=244)
        .map(|number| {
            raw_block(
                FAMILY_CHAIN,
                &format!("0xhistory{number}"),
                Some(&format!("0xhistory{}", number - 1)),
                number,
                1_700_000_000 + number,
            )
        })
        .collect();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    Ok(paging)
}

/// Interpret observes the bytes of `[first].alpha.eth` at `block` and writes them onto its
/// surface in place, which keeps its anchor at 202.
async fn observe_first(
    database: &TestDatabase,
    paging: &Paging,
    block: i64,
    observed: Observed,
) -> Result<()> {
    let labels = ["first", "alpha", "eth"];
    let mut dns = Vec::new();
    for label in labels {
        dns.push(u8::try_from(label.len())?);
        dns.extend(label.as_bytes());
    }
    dns.push(0);
    let (raw_name, raw_labels, visibility) = match observed {
        Observed::Raw => (
            "first.alpha.eth",
            labels.map(str::to_owned).to_vec(),
            "active",
        ),
        Observed::Shadow => ("", Vec::new(), "shadow"),
    };
    let shadowed = matches!(observed, Observed::Shadow);
    // As Interpret's identity write does, the surface and its search document change together.
    let mut transaction = database.pool.begin().await?;
    let path: Vec<String> = labels.iter().map(|label| child_labelhash(label)).collect();
    bigname_storage::identity_search::prepare(
        &mut transaction,
        bigname_storage::identity_search::LabelWrite::NONE,
        &[path],
        std::slice::from_ref(&paging.first),
    )
    .await?;
    sqlx::query(
        "UPDATE name_surfaces SET raw_name = $2, raw_labels = $3, dns_encoded_name = $4,
             preimage_event_identity = $5, visibility_state = $6,
             normalization_errors = CASE WHEN $7 THEN '[\"invalid_utf8\"]'::jsonb
                                         ELSE '[]'::jsonb END,
             deactivation_reason = CASE WHEN $7 THEN 'invalid_utf8' END,
             deactivated_at = CASE WHEN $7 THEN to_timestamp(1700000000 + $8) END
         WHERE logical_name_id = $1 AND raw_name IS NULL",
    )
    .bind(&paging.first)
    .bind(raw_name)
    .bind(&raw_labels)
    .bind(&dns)
    .bind(AS_OF_WITNESS)
    .bind(visibility)
    .bind(shadowed)
    .bind(block)
    .execute(&mut *transaction)
    .await?;
    bigname_storage::identity_search::refresh(
        &mut transaction,
        std::slice::from_ref(&paging.first),
        &[],
    )
    .await?;
    transaction.commit().await?;
    let mut event = family_event(
        AS_OF_WITNESS,
        Some(&paging.first),
        None,
        "PreimageObserved",
        "ens_v1_registry_l1",
        block,
        0,
        json!({"raw_labels": raw_labels, "visibility_state": visibility}),
    );
    event.before_state = json!({});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    Ok(())
}

/// A reorg removes the block of the witness: its observation is orphaned. With `block` the
/// block is orphaned too, which no later block of the fixture builds on.
async fn orphan_witness(database: &TestDatabase, block: Option<i64>) -> Result<()> {
    sqlx::query("UPDATE normalized_events SET canonicality_state = 'orphaned' WHERE event_identity = $1")
        .bind(AS_OF_WITNESS)
        .execute(&database.pool)
        .await?;
    if let Some(block) = block {
        sqlx::query(
            "UPDATE chain_lineage SET canonicality_state = 'orphaned'
             WHERE chain_id = $1 AND block_number = $2",
        )
        .bind(FAMILY_CHAIN)
        .bind(block)
        .execute(&database.pool)
        .await?;
    }
    Ok(())
}

/// A block that composes `[first].alpha.eth` again for a reason other than its bytes.
#[derive(Clone, Copy)]
enum Refresh {
    /// A registry transfer of the name to FAMILY_ALICE.
    Owner,
    /// The name pointed at FAMILY_RESOLVER again.
    Resolver,
    /// An observation of the name that gives its surface no bytes.
    Preimage,
}

async fn refresh_first(
    database: &TestDatabase,
    paging: &Paging,
    refresh: Refresh,
    block: i64,
) -> Result<()> {
    let node = paging.first.strip_prefix("ens:").expect("ens id");
    let (_, alpha_node) = phase_logical_identity("ens", "alpha.eth")?;
    let event = match refresh {
        Refresh::Owner => family_event(
            "as-of-first-owner",
            Some(&paging.first),
            Some(paging.first_resource),
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            block,
            0,
            json!({"source_event": "NewOwner", "node": alpha_node, "child_node": node,
                   "labelhash": paging.first_labelhash, "owner": FAMILY_ALICE,
                   "owner_getter": FAMILY_ALICE, "emitter_role": "registry",
                   "authority_kind": "registry_only", "name_identity_observed": true}),
        ),
        Refresh::Resolver => family_event(
            "as-of-first-resolver",
            Some(&paging.first),
            Some(paging.first_resource),
            "ResolverChanged",
            "ens_v1_registry_l1",
            block,
            0,
            json!({"node": node, "resolver": FAMILY_RESOLVER}),
        ),
        Refresh::Preimage => {
            let mut event = family_event(
                "as-of-first-empty-observation",
                Some(&paging.first),
                None,
                "PreimageObserved",
                "ens_v1_registry_l1",
                block,
                0,
                json!({"raw_labels": []}),
            );
            event.before_state = json!({});
            event
        }
    };
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
    Ok(())
}

/// A Project redo of `from..=to`, as an Interpret redo that reaches a published block stamps.
async fn project_redo(database: &TestDatabase, (from, to): (i64, i64)) -> Result<()> {
    let hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_number = $2
         AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(FAMILY_CHAIN)
    .bind(to)
    .fetch_one(&database.pool)
    .await?;
    let token = bigname_project::families::input_token(&database.pool, FAMILY_CHAIN).await?;
    let outcome = bigname_project::families::apply(
        &database.pool,
        FAMILY_CHAIN,
        &bigname_project::Marker { number: to, hash },
        bigname_project::families::FamilyMode::Redo { from, to },
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::PROJECTION_CONTENT_HASH,
        ),
    )
    .await?;
    anyhow::ensure!(
        outcome.marker.as_ref().map(|marker| marker.number) == Some(to) && !outcome.reset,
        "the Project redo of {from}..={to}: {outcome:?}"
    );
    Ok(())
}

/// Whether the stored summary of `[first].alpha.eth` composes the name: it has an order name.
/// A surface the block does not compose keeps a row with none.
async fn first_composed(database: &TestDatabase, paging: &Paging) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM project_name_summary
                        WHERE chain_id = $1 AND logical_name_id = $2
                          AND order_name IS NOT NULL)",
    )
    .bind(FAMILY_CHAIN)
    .bind(&paging.first)
    .fetch_one(&database.pool)
    .await?)
}

/// The listing checked against the walk, and whether `[first].alpha.eth` is in it, by either
/// name it can be shown under.
async fn first_listed(database: &TestDatabase) -> Result<bool> {
    let listed = paging_parity(database, None).await?;
    Ok(listed
        .iter()
        .any(|name| name == "first.alpha.eth" || name.ends_with("].alpha.eth")))
}

/// What a run leaves after a publication: every stored summary row, the page's names with their
/// order names, and the count. The page and the count are checked against the walk first.
async fn as_of_state(database: &TestDatabase) -> Result<Value> {
    paging_parity(database, None).await?;
    let summaries: Vec<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(summary) FROM project_name_summary summary WHERE chain_id = $1
         ORDER BY logical_name_id",
    )
    .bind(FAMILY_CHAIN)
    .fetch_all(&database.pool)
    .await?;
    let page = bigname_storage::families::name::load_family_bound_names(
        &database.pool,
        FAMILY_CHAIN,
        FAMILY_RESOLVER,
        None,
        None,
        1_000,
        true,
    )
    .await?;
    let keys: Vec<(String, String)> = page
        .rows
        .iter()
        .map(|name| (name.row.logical_name_id.clone(), name.order_name.clone()))
        .collect();
    Ok(json!({"summaries": summaries, "page": keys, "total_count": page.total_count}))
}

/// One run of a refresh at 241 with the bytes of `[first].alpha.eth` observed at 242: before
/// Project publishes 241 when `ahead`, else after. Returns the state after 241 and after 242.
async fn refresh_run(refresh: Refresh, ahead: bool) -> Result<[Value; 2]> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_as_of(&database).await?;
    if ahead {
        observe_first(&database, &paging, 242, Observed::Raw).await?;
    }
    refresh_first(&database, &paging, refresh, 241).await?;
    publish_test_families(&database, 241).await?;
    let published = as_of_state(&database).await?;
    if !ahead {
        observe_first(&database, &paging, 242, Observed::Raw).await?;
    }
    publish_test_families(&database, 242).await?;
    let observed = as_of_state(&database).await?;
    database.cleanup().await?;
    Ok([published, observed])
}

async fn assert_refresh_is_as_of(refresh: Refresh) -> Result<()> {
    let in_step = refresh_run(refresh, false).await?;
    let ahead = refresh_run(refresh, true).await?;
    for (block, (in_step, ahead)) in [241, 242].into_iter().zip(in_step.iter().zip(&ahead)) {
        assert_eq!(
            ahead, in_step,
            "at {block}, Interpret ahead (left) and in step (right)"
        );
    }
    Ok(())
}

#[tokio::test]
async fn an_owner_refresh_stores_the_same_summary_with_interpret_ahead_or_in_step() -> Result<()> {
    assert_refresh_is_as_of(Refresh::Owner).await
}

#[tokio::test]
async fn a_resolver_refresh_stores_the_same_summary_with_interpret_ahead_or_in_step() -> Result<()>
{
    assert_refresh_is_as_of(Refresh::Resolver).await
}

#[tokio::test]
async fn a_preimage_refresh_stores_the_same_summary_with_interpret_ahead_or_in_step() -> Result<()>
{
    assert_refresh_is_as_of(Refresh::Preimage).await
}

/// The sweep's block, the first after an Interpret redo that released `bound-05.eth`'s witness.
/// A regression guard, not red evidence: the sweep composes only surfaces without raw bytes,
/// which the publication's block and the live surface read alike.
async fn sweep_run(ahead: bool) -> Result<[Value; 2]> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_as_of(&database).await?;
    if ahead {
        observe_first(&database, &paging, 242, Observed::Raw).await?;
    }
    release_witness(&database, &paging.bound, 241).await?;
    publish_test_families(&database, 241).await?;
    assert_swept(&database, &paging.bound).await?;
    let published = as_of_state(&database).await?;
    if !ahead {
        observe_first(&database, &paging, 242, Observed::Raw).await?;
    }
    publish_test_families(&database, 242).await?;
    let observed = as_of_state(&database).await?;
    database.cleanup().await?;
    Ok([published, observed])
}

#[tokio::test]
async fn the_sweep_stores_the_same_summary_with_interpret_ahead_or_in_step() -> Result<()> {
    let in_step = sweep_run(false).await?;
    let ahead = sweep_run(true).await?;
    assert_eq!(ahead, in_step, "Interpret ahead (left) and in step (right)");
    Ok(())
}

/// A Project redo of 241, which composes `[first].alpha.eth` for its owner refresh, after
/// Interpret observed the name's bytes at 242 when `ahead`.
async fn project_redo_run(ahead: bool) -> Result<[Value; 2]> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_as_of(&database).await?;
    refresh_first(&database, &paging, Refresh::Owner, 241).await?;
    publish_test_families(&database, 241).await?;
    if ahead {
        observe_first(&database, &paging, 242, Observed::Raw).await?;
    }
    project_redo(&database, (241, 241)).await?;
    let redone = as_of_state(&database).await?;
    if !ahead {
        observe_first(&database, &paging, 242, Observed::Raw).await?;
    }
    publish_test_families(&database, 242).await?;
    let observed = as_of_state(&database).await?;
    database.cleanup().await?;
    Ok([redone, observed])
}

#[tokio::test]
async fn a_project_redo_stores_the_same_summary_with_interpret_ahead_or_in_step() -> Result<()> {
    let in_step = project_redo_run(false).await?;
    let ahead = project_redo_run(true).await?;
    assert_eq!(ahead, in_step, "Interpret ahead (left) and in step (right)");
    Ok(())
}

/// Interpret shadows `[first].alpha.eth` at 243, past the block Project publishes next, 241,
/// which composes the name for a resolver refresh. A reorg then removes 243, and the witness
/// repair of the Interpret redo above the marker leaves the surface active without bytes. The
/// summary composes the name throughout, so it is listed again once the shadow is gone, at 241 and
/// at 242.
#[tokio::test]
async fn a_shadow_witness_past_the_publication_that_a_reorg_removes_keeps_the_name_listed()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_as_of(&database).await?;
    observe_first(&database, &paging, 243, Observed::Shadow).await?;
    refresh_first(&database, &paging, Refresh::Resolver, 241).await?;
    publish_test_families(&database, 241).await?;
    // The live surface is a shadow: neither the walk nor the page lists the name.
    assert!(!first_listed(&database).await?);
    assert!(first_composed(&database, &paging).await?);
    orphan_witness(&database, Some(243)).await?;
    release_witness_in(&database, &paging.first, (243, 243)).await?;
    assert!(first_listed(&database).await?);
    publish_test_families(&database, 242).await?;
    assert!(first_listed(&database).await?);
    assert!(first_composed(&database, &paging).await?);
    database.cleanup().await
}

/// The same shadow at 243, past 241 and 242, which Project publishes. The reorg's Interpret
/// redo reaches 242, so it stamps a Project redo from 242, after the block that composed the
/// name with the shadow already written. The redo leaves the name listed.
#[tokio::test]
async fn a_project_redo_after_the_shadowed_composition_keeps_the_name_listed() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_as_of(&database).await?;
    observe_first(&database, &paging, 243, Observed::Shadow).await?;
    refresh_first(&database, &paging, Refresh::Resolver, 241).await?;
    publish_test_families(&database, 241).await?;
    publish_test_families(&database, 242).await?;
    assert!(!first_listed(&database).await?);
    orphan_witness(&database, Some(243)).await?;
    release_witness_in(&database, &paging.first, (242, 243)).await?;
    project_redo(&database, (242, 242)).await?;
    assert!(first_listed(&database).await?);
    assert!(first_composed(&database, &paging).await?);
    database.cleanup().await
}

/// A shadow witness at 241, which Project publishes: the summary stops composing the name there.
/// A regression guard, not red evidence: below the marker the stamped Project redo's undo and
/// replay restore the row on the previous build too. A reorg
/// then orphans the observation, and the Interpret redo of 241 reaches Project's marker, 242, so
/// a Project redo of 241 and 242 follows. The redo's undo and replay list the name again.
#[tokio::test]
async fn a_witness_repair_at_or_below_the_marker_lists_the_name_after_the_project_redo()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_as_of(&database).await?;
    observe_first(&database, &paging, 241, Observed::Shadow).await?;
    publish_test_families(&database, 241).await?;
    publish_test_families(&database, 242).await?;
    assert!(!first_listed(&database).await?);
    assert!(!first_composed(&database, &paging).await?);
    orphan_witness(&database, None).await?;
    release_witness_in(&database, &paging.first, (241, 241)).await?;
    project_redo(&database, (241, 242)).await?;
    assert!(first_listed(&database).await?);
    assert!(first_composed(&database, &paging).await?);
    database.cleanup().await
}

/// The stored order name of `[first].alpha.eth` and whether it is listed, after Project
/// publishes 241 with the name's bytes observed at `witness` and a resolver refresh at 241.
async fn witness_run(witness: i64, observed: Observed) -> Result<(Option<String>, bool)> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_as_of(&database).await?;
    observe_first(&database, &paging, witness, observed).await?;
    refresh_first(&database, &paging, Refresh::Resolver, 241).await?;
    publish_test_families(&database, 241).await?;
    let listed = first_listed(&database).await?;
    let stored: Option<Option<String>> = sqlx::query_scalar(
        "SELECT order_name FROM project_name_summary WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(FAMILY_CHAIN)
    .bind(&paging.first)
    .fetch_optional(&database.pool)
    .await?;
    database.cleanup().await?;
    Ok((stored.flatten(), listed))
}

/// Raw bytes witnessed at the publication's block order the name by them. One block above, the
/// name keeps the order of every label as its hash, though the page shows its raw name.
#[tokio::test]
async fn raw_bytes_witnessed_at_the_publication_block_count_and_one_block_above_do_not()
-> Result<()> {
    let placeholder = format!(
        "{}.{}.{}",
        tl_bracket(&child_labelhash("first")),
        tl_bracket(&child_labelhash("alpha")),
        tl_bracket(&child_labelhash("eth"))
    );
    assert_eq!(
        witness_run(241, Observed::Raw).await?,
        (Some("first.alpha.eth".to_owned()), true)
    );
    assert_eq!(
        witness_run(242, Observed::Raw).await?,
        (Some(placeholder), true)
    );
    Ok(())
}

/// A shadow witnessed at the publication's block drops the name's summary. One block above, the
/// summary keeps the name, which the page leaves out while its surface is a shadow.
#[tokio::test]
async fn a_shadow_witnessed_at_the_publication_block_counts_and_one_block_above_does_not()
-> Result<()> {
    assert_eq!(witness_run(241, Observed::Shadow).await?, (None, false));
    let (above, listed) = witness_run(242, Observed::Shadow).await?;
    assert!(above.is_some() && !listed, "{above:?} {listed}");
    Ok(())
}

/// `value` with the two names `[first].alpha.eth` is shown under, by its raw name or by its
/// label hash, replaced by one token. Its display follows the live surface, which the summary
/// does not decide. Cursors and snapshot tokens, which carry the shown name, are dropped.
fn without_first_display(value: &Value, bracket: &str) -> Value {
    match value {
        Value::String(text) => Value::String(
            text.replace("first.alpha.eth", "<first>.alpha.eth")
                .replace(bracket, "<first>"),
        ),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| without_first_display(item, bracket))
                .collect(),
        ),
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .filter(|(key, _)| !matches!(key.as_str(), "next_cursor" | "as_of_token" | "cursor"))
                .map(|(key, item)| (key.clone(), without_first_display(item, bracket)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// One run of the owner refresh at 241 with the bytes of `[first].alpha.eth` observed at 242,
/// before Project publishes 241 when `ahead`. Returns the response of `uri` after 241 and after
/// 242, without the name's live display.
async fn reader_run(
    uri: &str,
    ahead: bool,
    by_namehash: bool,
    observed: Observed,
) -> Result<[Value; 2]> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_as_of(&database).await?;
    let bracket = tl_bracket(&paging.first_labelhash);
    if ahead {
        observe_first(&database, &paging, 242, observed).await?;
    }
    refresh_first(&database, &paging, Refresh::Owner, 241).await?;
    publish_test_families(&database, 241).await?;
    let published = read_reader(&database, uri, &bracket, by_namehash).await?;
    if !ahead {
        observe_first(&database, &paging, 242, observed).await?;
    }
    publish_test_families(&database, 242).await?;
    let observed = read_reader(&database, uri, &bracket, by_namehash).await?;
    database.cleanup().await?;
    Ok([published, observed])
}

/// The response of `uri`, without the live display of `[first].alpha.eth`. With `by_namehash`
/// its names are put in namehash order, for a reader that orders by the shown name.
async fn read_reader(
    database: &TestDatabase,
    uri: &str,
    bracket: &str,
    by_namehash: bool,
) -> Result<Value> {
    let (status, body) = read_family_response(database, uri).await?;
    let mut body = without_first_display(&body, bracket);
    if by_namehash && let Some(Value::Array(names)) = body.get_mut("data") {
        names.sort_by_key(|name| name["namehash"].as_str().unwrap_or_default().to_owned());
    }
    Ok(json!({"status": status.as_u16(), "body": body}))
}

/// `uri` answers the same at 241 and at 242 with Interpret ahead or in step, apart from the
/// live display of `[first].alpha.eth` (and, with `by_namehash`, the order it gives).
async fn assert_reader_is_as_of(uri: &str, by_namehash: bool, observed: Observed) -> Result<()> {
    let in_step = reader_run(uri, false, by_namehash, observed).await?;
    let ahead = reader_run(uri, true, by_namehash, observed).await?;
    for (block, (in_step, ahead)) in [241, 242].into_iter().zip(in_step.iter().zip(&ahead)) {
        assert_eq!(
            ahead, in_step,
            "{uri} at {block}, Interpret ahead (left) and in step (right)"
        );
    }
    Ok(())
}

/// Search reads the name's stored search fields. It orders its results by the name it shows,
/// which follows the live surface, so the results are compared in namehash order. A pin, not
/// red evidence: the stored fields it reads do not depend on the text, so it is green on the
/// build before this change too.
#[tokio::test]
async fn search_answers_the_same_with_interpret_ahead_or_in_step() -> Result<()> {
    assert_reader_is_as_of(
        "/v1/search?q=alpha&match=contains&page_size=50",
        true,
        Observed::Raw,
    )
    .await
}

/// The children list of alpha.eth reads its children's summaries. A pin, not red evidence: the
/// stored fields it reads do not depend on the text, so it is green on the build before this
/// change too.
#[tokio::test]
async fn a_children_list_answers_the_same_with_interpret_ahead_or_in_step() -> Result<()> {
    assert_reader_is_as_of(
        "/v1/names/alpha.eth/subnames?page_size=10&include=counts",
        false,
        Observed::Raw,
    )
    .await
}

/// The names-by-expiry listing selects by the stored expiry fields. A pin, not red evidence: the
/// stored fields it reads do not depend on the text, so it is green on the build before this
/// change too.
#[tokio::test]
async fn an_expiry_listing_answers_the_same_with_interpret_ahead_or_in_step() -> Result<()> {
    assert_reader_is_as_of(
        "/v1/names?namespace=ens&expires_after=1800000000&expires_before=1950000000&page_size=100",
        false,
        Observed::Raw,
    )
    .await
}

/// A shadow witnessed at the publication's block keeps a summary row, as every surface does.
/// The row stores only the ownerless verdict, so no bound-name page or count reads it.
#[tokio::test]
async fn a_shadow_keeps_a_summary_row_that_no_bound_name_reader_lists() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_as_of(&database).await?;
    observe_first(&database, &paging, 241, Observed::Shadow).await?;
    publish_test_families(&database, 241).await?;
    let row: Value = sqlx::query_scalar(
        "SELECT to_jsonb(summary) FROM project_name_summary summary
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(FAMILY_CHAIN)
    .bind(&paging.first)
    .fetch_one(&database.pool)
    .await?;
    let set: Vec<&String> = row
        .as_object()
        .context("a summary row")?
        .iter()
        .filter(|(column, value)| {
            !matches!(
                column.as_str(),
                "chain_id" | "logical_name_id" | "namespace" | "namehash" | "zero_owner"
            ) && !matches!(value, Value::Null | Value::Bool(false))
        })
        .map(|(column, _)| column)
        .collect();
    assert!(set.is_empty(), "the shadow's row sets {set:?}: {row:#}");
    assert!(!first_listed(&database).await?);
    let page = bigname_storage::families::name::load_family_bound_names(
        &database.pool,
        FAMILY_CHAIN,
        FAMILY_RESOLVER,
        None,
        None,
        1_000,
        true,
    )
    .await?;
    assert!(page.rows.iter().all(|name| name.row.logical_name_id != paging.first));
    assert_eq!(page.total_count, Some(u64::try_from(page.rows.len())?));
    database.cleanup().await
}
