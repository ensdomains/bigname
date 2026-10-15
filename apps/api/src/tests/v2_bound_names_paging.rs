// The resolver overview's bound names, selected names first from the name summary
// (crates/storage/src/families/name/bound.rs) and checked against the walk they replaced
// (`bound_walk.rs`): pages across names with and without raw bytes, the count, the cost of a
// page among many names the rules do not list, the stored order name following a surface's raw
// name through an observed preimage and a released witness, and the capability gate following a
// classification change that touches no name.

const PAGING_LIVE: usize = 24;
const PAGING_RELEASED: usize = 40;

/// The paging fixture's names: the name without raw bytes, and a raw-name name the tests change.
struct Paging {
    first: String,
    first_resource: Uuid,
    first_labelhash: String,
    bound: String,
    bound_resource: Uuid,
}

/// Bound to FAMILY_RESOLVER: `0zero.eth` and alpha.eth (pointed at 209), `[first].alpha.eth`,
/// a registry child of alpha.eth whose surface stores no raw bytes (pointed at 207), and
/// `bound-NN.eth` (granted at 210, pointed at 211). `a-gone-NN.eth` were granted at 210,
/// pointed at 211 too and released at 212: the walk reaches them and the rules list none. The preimages of `alpha` and `eth` are imported, so the
/// textless name is shown as `[first].alpha.eth` and sorts as three bracketed labels. Published
/// at 240.
async fn seed_bound_paging(database: &TestDatabase) -> Result<Paging> {
    seed_bounded_membership_blocks(database, 240).await?;
    seed_family_resolver_declaration(database).await?;
    for preimage in [b"alpha".as_slice(), b"eth"] {
        insert_family_label_preimage(&database.pool, preimage).await?;
    }
    let hash = |label: &[u8]| format!("{:#x}", alloy_primitives::keccak256(label));
    let grant = |expiry: i64| json!({"authority_kind": "registrar", "registrant": FAMILY_ALICE, "expiry": expiry});
    let mut events = Vec::new();
    let pointed = |name: &str, resource: Uuid, block: i64, events: &mut Vec<NormalizedEvent>| {
        events.push(family_event(
            &format!("paging-{name}-resolver"),
            Some(name),
            Some(resource),
            "ResolverChanged",
            "ens_v1_registry_l1",
            block,
            0,
            json!({"node": name.strip_prefix("ens:").expect("ens id"), "resolver": FAMILY_RESOLVER}),
        ));
    };
    let (alpha, alpha_resource) =
        seed_family_name(database, "alpha.eth", 0x9a1_0000, "ens_v1").await?;
    let (zero, zero_resource) = seed_family_name(database, "0zero.eth", 0x9a2_0000, "ens_v1").await?;
    for (name, resource) in [(&alpha, alpha_resource), (&zero, zero_resource)] {
        events.push(family_event(
            &format!("paging-{name}-grant"),
            Some(name),
            Some(resource),
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            201,
            0,
            grant(1_900_000_000),
        ));
        pointed(name, resource, 209, &mut events);
    }
    let first_labelhash = hash(b"first");
    let (first, first_resource) = seed_textless_family_name(
        database,
        &[first_labelhash.clone(), hash(b"alpha"), hash(b"eth")],
        0x9b1_0000,
        "ens_v1",
        202,
    )
    .await?;
    let alpha_node = alpha.strip_prefix("ens:").expect("ens id");
    events.extend(textless_child_events(
        &first,
        first_resource,
        alpha_node,
        &first_labelhash,
        RC_OWNER,
        202,
    ));
    pointed(&first, first_resource, 207, &mut events);
    let (mut bound, mut bound_resource) = (String::new(), Uuid::nil());
    let mut released = Vec::new();
    for (prefix, count, expiry, seed) in [
        ("bound", PAGING_LIVE, 1_900_000_000, 0x9c0_0000u128),
        ("a-gone", PAGING_RELEASED, 1_900_000_000, 0x9d0_0000),
    ] {
        for n in 0..count {
            let (name, resource) = seed_family_name(
                database,
                &format!("{prefix}-{n:02}.eth"),
                seed + 0x100 * n as u128,
                "ens_v1",
            )
            .await?;
            events.push(family_event(
                &format!("paging-{name}-grant"),
                Some(&name),
                Some(resource),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                210,
                0,
                grant(expiry),
            ));
            pointed(&name, resource, 211, &mut events);
            if prefix == "a-gone" {
                events.push(family_event(
                    &format!("paging-{name}-release"),
                    Some(&name),
                    Some(resource),
                    "RegistrationReleased",
                    "ens_v1_registrar_l1",
                    212,
                    0,
                    json!({"authority_kind": "registrar", "released_at": 1_700_000_212,
                           "expiry": 1_900_000_000}),
                ));
                released.push(Uuid::from_u128(seed + 0x100 * n as u128 + 2));
            }
            if prefix == "bound" && n == 5 {
                (bound, bound_resource) = (name, resource);
            }
        }
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    // A released lease's binding ends at its release.
    sqlx::query(
        "UPDATE surface_bindings SET active_to = to_timestamp(1700000212)
         WHERE surface_binding_id = ANY($1)",
    )
    .bind(&released)
    .execute(&database.pool)
    .await?;
    publish_test_families(database, 240).await?;
    Ok(Paging {
        first,
        first_resource,
        first_labelhash,
        bound,
        bound_resource,
    })
}

async fn paging_parity(database: &TestDatabase, namespace: Option<&str>) -> Result<Vec<String>> {
    bigname_storage::families::name::assert_bound_names_match_the_walk(
        &database.pool,
        FAMILY_CHAIN,
        FAMILY_RESOLVER,
        namespace,
    )
    .await
}

/// The stored order name of `name`.
async fn order_name(database: &TestDatabase, name: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT order_name FROM project_name_summary WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(FAMILY_CHAIN)
    .bind(name)
    .fetch_one(&database.pool)
    .await?)
}

/// The listing pages through names with and without raw bytes at every page size, as the walk
/// does in the listing's order, under a namespace filter too. The route serves the exact count
/// only when asked, the same on every page, at the latest publication and pinned to it.
#[tokio::test]
async fn bound_names_match_the_walk_page_by_page_across_names_without_raw_bytes() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_bound_paging(&database).await?;
    let listed = paging_parity(&database, None).await?;
    let shown = format!("{}.alpha.eth", tl_bracket(&paging.first_labelhash));
    assert_eq!(listed.len(), PAGING_LIVE + 3, "{listed:?}");
    assert!(listed.contains(&shown), "{listed:?}");
    // The textless name sorts by its bracketed form, before every lower-case raw name.
    assert_eq!(&listed[..2], ["0zero.eth", shown.as_str()], "{listed:?}");
    assert_eq!(
        order_name(&database, &paging.first).await?,
        Some(format!(
            "{}.{}.{}",
            tl_bracket(&paging.first_labelhash),
            tl_bracket(&child_labelhash("alpha")),
            tl_bracket(&child_labelhash("eth"))
        ))
    );
    assert_eq!(paging_parity(&database, Some("ens")).await?, listed);
    assert!(paging_parity(&database, Some("basenames")).await?.is_empty());
    let (_, latest) = read_family_response(&database, &format!("/v1/resolvers/1/{FAMILY_RESOLVER}")).await?;
    let token = latest["meta"]["as_of_token"].as_str().context("a snapshot token")?;
    for page_size in [1, 7, 50] {
        for (include, total) in [("", Value::Null), ("&include=total_count", json!(listed.len()))] {
            for at in [String::new(), format!("&at={token}")] {
                let uri = format!(
                    "/v1/resolvers/1/{FAMILY_RESOLVER}?page_size={page_size}{include}{at}"
                );
                let pages = read_family_pages_in(&database, &uri, "/data/bound_names").await?;
                assert_eq!(tl_column(&pages, "name"), listed, "{uri}");
                for page in &pages {
                    assert_eq!(page["total_count"], total, "{uri}: {page:#}");
                }
            }
        }
    }
    database.cleanup().await
}

/// A block that points a listed name at another resolver is undone and run again: the undo
/// restores the stored values of the block before it, and the second run writes the first run's.
#[tokio::test]
async fn an_undone_block_restores_the_bound_names_and_its_rerun_repeats_them() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_bound_paging(&database).await?;
    let before = paging_parity(&database, None).await?;
    assert!(before.contains(&"bound-05.eth".to_owned()), "{before:?}");
    let elsewhere = "0x0000000000000000000000000000000000000def";
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[family_event(
            "paging-bound-05-elsewhere",
            Some(&paging.bound),
            Some(paging.bound_resource),
            "ResolverChanged",
            "ens_v1_registry_l1",
            241,
            0,
            json!({"node": paging.bound.strip_prefix("ens:").expect("ens id"), "resolver": elsewhere}),
        )],
    )
    .await?;
    publish_test_families(&database, 241).await?;
    let moved = paging_parity(&database, None).await?;
    let mut expected = before.clone();
    expected.retain(|name| name != "bound-05.eth");
    assert_eq!(moved, expected);
    let undone = bigname_project::families::undo_to(&database.pool, FAMILY_CHAIN, 240).await?;
    assert_eq!(undone, 1);
    assert_eq!(paging_parity(&database, None).await?, before);
    publish_test_families(&database, 241).await?;
    assert_eq!(paging_parity(&database, None).await?, moved);
    database.cleanup().await
}

/// Forty names the rules do not list sort before the listed raw names, and every page composes
/// only the names it can serve, where the walk composed every name a pointer reached.
#[tokio::test]
async fn a_bound_name_page_composes_only_its_names_among_many_unlisted_names() -> Result<()> {
    use bigname_storage::families::name::{load_family_bound_names, seams};
    let database = TestDatabase::new_migrated().await?;
    seed_bound_paging(&database).await?;
    let mut cursor: Option<bigname_storage::NameCurrentListCursor> = None;
    let mut pages = 0;
    loop {
        let composed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let page = seams::with_composed_names_counter(
            composed.clone(),
            load_family_bound_names(
                &database.pool,
                FAMILY_CHAIN,
                FAMILY_RESOLVER,
                None,
                cursor.as_ref(),
                2,
                false,
            ),
        )
        .await?;
        let composed = composed.load(std::sync::atomic::Ordering::Relaxed);
        assert!(composed <= 2, "page {pages} composed {composed} names");
        pages += 1;
        if page.rows.len() < 2 {
            break;
        }
        let last = &page.rows[0];
        cursor = Some(bigname_storage::NameCurrentListCursor {
            sort_value: bigname_storage::NameCurrentListCursorValue::Name(
                last.row.canonical_display_name.clone(),
            ),
            namespace: last.row.namespace.clone(),
            normalized_name: last.order_name.clone(),
            namehash: last.row.namehash.clone(),
        });
    }
    assert_eq!(pages, PAGING_LIVE + 3, "one page per listed name");
    let reached: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT logical_name_id) FROM project_named_resource_pointer
         WHERE chain_id = $1 AND resolver_address = $2",
    )
    .bind(FAMILY_CHAIN)
    .bind(FAMILY_RESOLVER)
    .fetch_one(&database.pool)
    .await?;
    assert!(
        reached >= (PAGING_LIVE + PAGING_RELEASED) as i64,
        "the walk's candidates: {reached}"
    );
    database.cleanup().await
}

/// An imported preimage changes how the textless name is shown, never where it sorts: the
/// order, the stored summary and a cursor taken before the import all stay.
#[tokio::test]
async fn an_imported_preimage_shows_a_bound_name_as_text_where_it_sorted() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_bound_paging(&database).await?;
    let before = paging_parity(&database, None).await?;
    let stored = order_name(&database, &paging.first).await?;
    let uri = format!("/v1/resolvers/1/{FAMILY_RESOLVER}?page_size=1");
    let (_, first_page) = read_family_response(&database, &uri).await?;
    let cursor = first_page["data"]["bound_names"]["page"]["next_cursor"]
        .as_str()
        .context("a next cursor")?
        .to_owned();
    insert_family_label_preimage(&database.pool, b"first").await?;
    let after = paging_parity(&database, None).await?;
    let position = before
        .iter()
        .position(|name| name.starts_with('['))
        .context("the textless name")?;
    assert_eq!(after[position], "first.alpha.eth", "{after:?}");
    let mut expected = before.clone();
    expected[position] = "first.alpha.eth".to_owned();
    assert_eq!(after, expected);
    assert_eq!(order_name(&database, &paging.first).await?, stored);
    // The cursor after `0zero.eth` continues at the name the import renamed.
    let (status, next) = read_family_response(&database, &format!("{uri}&cursor={cursor}")).await?;
    assert_eq!(status, StatusCode::OK, "{next:#}");
    assert_eq!(
        next["data"]["bound_names"]["data"][0]["name"],
        json!("first.alpha.eth"),
        "{next:#}"
    );
    database.cleanup().await
}

/// Interpret observes the textless name's raw bytes at a later block and gives its surface
/// them in place. Until Project publishes that block the name keeps its place, shown with its
/// raw name. The family step composes the name again in that block, and the name moves to the
/// place its raw name sorts at.
#[tokio::test]
async fn a_textless_bound_name_whose_raw_name_is_observed_later_moves_to_its_new_position()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_bound_paging(&database).await?;
    let before = paging_parity(&database, None).await?;
    let labels = ["first", "alpha", "eth"];
    let mut dns = Vec::new();
    for label in labels {
        dns.push(u8::try_from(label.len())?);
        dns.extend(label.as_bytes());
    }
    dns.push(0);
    let witness = "paging-first-preimage";
    sqlx::query(
        "UPDATE name_surfaces SET raw_name = $2, raw_labels = $3, dns_encoded_name = $4,
             preimage_event_identity = $5
         WHERE logical_name_id = $1 AND raw_name IS NULL",
    )
    .bind(&paging.first)
    .bind("first.alpha.eth")
    .bind(labels.map(str::to_owned).to_vec())
    .bind(&dns)
    .bind(witness)
    .execute(&database.pool)
    .await?;
    let mut observed = family_event(
        witness,
        Some(&paging.first),
        None,
        "PreimageObserved",
        "ens_v1_registry_l1",
        241,
        0,
        json!({"raw_labels": labels}),
    );
    observed.before_state = json!({});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[observed]).await?;
    // Before Project publishes 241 the name keeps its published place, shown with its raw name.
    let mut waiting = before.clone();
    waiting[1] = "first.alpha.eth".to_owned();
    assert_eq!(paging_parity(&database, None).await?, waiting);
    publish_test_families(&database, 241).await?;
    let after = paging_parity(&database, None).await?;
    assert!(!after.iter().any(|name| name.starts_with('[')), "{after:?}");
    let moved_from = before.iter().position(|name| name.starts_with('['));
    let moved_to = after.iter().position(|name| name == "first.alpha.eth");
    assert_eq!(moved_from, Some(1), "{before:?}");
    // `first.alpha.eth` sorts after every other listed raw name.
    assert_eq!(moved_to, Some(after.len() - 1), "{after:?}");
    assert_eq!(
        order_name(&database, &paging.first).await?.as_deref(),
        Some("first.alpha.eth")
    );
    database.cleanup().await
}

/// An Interpret redo whose range lies above Project's marker releases a raw-name name's
/// preimage witness and finds none again, so its surface loses its raw bytes in place, at a
/// block Project has already published and stamps no Project redo for. Until then the name
/// keeps its place. Project's next block composes the name again, and it sorts by its bracketed
/// form.
#[tokio::test]
async fn a_released_preimage_witness_is_swept_on_the_first_block_after_the_interpret_redo()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_bound_paging(&database).await?;
    let before_sweep = paging_parity(&database, None).await?;
    assert!(before_sweep.contains(&"bound-05.eth".to_owned()));
    release_witness(&database, &paging.bound, 241).await?;
    // Until the sweep the name keeps its published place, shown without raw bytes.
    let waiting = paging_parity(&database, None).await?;
    let mut expected = before_sweep.clone();
    let at = expected.iter().position(|name| name == "bound-05.eth").context("bound-05.eth")?;
    expected[at] = released_shown();
    assert_eq!(waiting, expected);
    publish_test_families(&database, 241).await?;
    assert_swept(&database, &paging.bound).await?;
    database.cleanup().await
}

/// The same release by an Interpret redo whose range reaches a block Project published, which
/// stamps a Project redo of that block. Undoing the block restores the input revision the block
/// before it recorded, so the replayed block sees a new revision and sweeps.
#[tokio::test]
async fn a_released_preimage_witness_is_swept_by_the_project_redo_the_interpret_redo_stamps()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let paging = seed_bound_paging(&database).await?;
    publish_test_families(&database, 241).await?;
    release_witness(&database, &paging.bound, 241).await?;
    let hash: String = sqlx::query_scalar(
        "SELECT block_hash FROM chain_lineage WHERE chain_id = $1 AND block_number = 241
         AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(FAMILY_CHAIN)
    .fetch_one(&database.pool)
    .await?;
    let token = bigname_project::families::input_token(&database.pool, FAMILY_CHAIN).await?;
    let outcome = bigname_project::families::apply(
        &database.pool,
        FAMILY_CHAIN,
        &bigname_project::Marker { number: 241, hash },
        bigname_project::families::FamilyMode::Redo { from: 241, to: 241 },
        &token,
        &bigname_project::families::FamilyOptions::new(
            bigname_content_hash::PROJECTION_CONTENT_HASH,
        ),
    )
    .await?;
    assert_eq!(outcome.marker.as_ref().map(|marker| marker.number), Some(241));
    assert!(
        !outcome.reset && outcome.undone_blocks == 1,
        "the redo undoes and replays block 241: {outcome:?}"
    );
    assert_swept(&database, &paging.bound).await?;
    assert_bound_fields_follow_the_publication(&database.pool, FAMILY_CHAIN).await?;
    database.cleanup().await
}

/// An Interpret redo of `block` that releases the preimage witness of `name` and finds none
/// again, as its witness repair writes it.
async fn release_witness(database: &TestDatabase, name: &str, block: i64) -> Result<()> {
    release_witness_in(database, name, (block, block)).await
}

/// [`release_witness`] by an Interpret redo of the blocks `from..=to`.
async fn release_witness_in(
    database: &TestDatabase,
    name: &str,
    (from, to): (i64, i64),
) -> Result<()> {
    database
        .simulate_interpret_redo_begin(FAMILY_CHAIN, "redo")
        .await?;
    sqlx::query(
        "UPDATE chain_phase_state SET redo_from_block_number = $2, redo_to_block_number = $3
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(FAMILY_CHAIN)
    .bind(from)
    .bind(to)
    .execute(&database.pool)
    .await?;
    sqlx::query(
        "UPDATE name_surfaces SET preimage_event_identity = NULL, raw_name = NULL,
             raw_labels = NULL, dns_encoded_name = NULL, visibility_state = 'active',
             normalization_errors = '[]'::jsonb, deactivation_reason = NULL,
             deactivated_at = NULL
         WHERE logical_name_id = $1",
    )
    .bind(name)
    .execute(&database.pool)
    .await?;
    database.simulate_interpret_redo_finish(FAMILY_CHAIN).await
}

/// `bound-05.eth` after its witness is released: shown with the imported `eth`.
fn released_shown() -> String {
    format!("{}.eth", tl_bracket(&child_labelhash("bound-05")))
}

/// The released `bound-05.eth` is listed at the place of every label as its hash.
async fn assert_swept(database: &TestDatabase, name: &str) -> Result<()> {
    let after = paging_parity(database, None).await?;
    assert!(after.contains(&released_shown()), "{after:?}");
    assert!(!after.contains(&"bound-05.eth".to_owned()), "{after:?}");
    assert_eq!(
        order_name(database, name).await?,
        Some(format!(
            "{}.{}",
            tl_bracket(&child_labelhash("bound-05")),
            tl_bracket(&child_labelhash("eth"))
        ))
    );
    Ok(())
}

/// The root-registry pointer of `eth` names a proxy whose implementation decides the
/// resolver's binding-enumeration support. Two later upgrades touch no name: the support flips
/// off and on, the stored bound-name values stay, and the listing follows the support.
#[tokio::test]
async fn the_capability_gate_follows_a_classification_change_that_touches_no_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let manifest = seed_root_registry_pointer_at_a_permissioned_resolver(&database).await?;
    let eth = bigname_storage::logical_name_id_for_name("ens", "eth");
    let stored = || async {
        sqlx::query_scalar::<_, Value>(
            "SELECT jsonb_build_array(resolver_chain_id, resolver_address,
                 resolver_listing_eligible, resolver_support_exempt, raw_name, order_name,
                 namehash)
             FROM project_name_summary WHERE chain_id = 'ethereum-mainnet'
               AND logical_name_id = $1",
        )
        .bind(&eth)
        .fetch_one(&database.pool)
        .await
    };
    let before = stored().await?;
    assert_eq!(
        bigname_storage::families::name::assert_bound_names_match_the_walk(
            &database.pool,
            "ethereum-mainnet",
            ROUND2_RESOLVER,
            None
        )
        .await?,
        ["eth"]
    );
    for (offset, implementation, supported) in [
        (1, "0x0000000000000000000000000000000000000fee", false),
        (2, "0x0000000000000000000000000000000000000fed", true),
    ] {
        let block = 21_000_003 + offset;
        let hash = format!("0xcapability-{block}");
        let time: String = sqlx::query_scalar(
            "SELECT to_char((block_timestamp + make_interval(secs => 12 * $2)) AT TIME ZONE 'UTC',
                            'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
             FROM chain_lineage WHERE chain_id = 'ethereum-mainnet' AND block_number = $1",
        )
        .bind(21_000_003i64)
        .bind(offset as f64)
        .fetch_one(&database.pool)
        .await?;
        seed_schema_v2_lookup_head(&database.pool, "ethereum-mainnet", block, &hash, &time)
            .await?;
        let mut upgrade = history_event(
            &format!("capability-upgrade-{block}"),
            None,
            None,
            Some("ethereum-mainnet"),
            Some(block),
            Some(&hash),
            Some("0xcapability"),
            Some(0),
            CanonicalityState::Canonical,
        );
        upgrade.event_kind = "Upgraded".into();
        upgrade.source_family = "ens_v2_resolver_l1".into();
        upgrade.source_manifest_id = Some(manifest);
        upgrade.manifest_version = 1;
        upgrade.before_state = json!({});
        upgrade.after_state =
            json!({"proxy_address": ROUND2_RESOLVER, "implementation": implementation});
        bigname_storage::insert_normalized_event_fixtures(&database.pool, &[upgrade]).await?;
        publish_test_families_on(&database.pool, "ethereum-mainnet", block).await?;
        let classification = bigname_storage::families::topology::load_family_resolver_current(
            &database.pool,
            "ethereum-mainnet",
            ROUND2_RESOLVER,
        )
        .await?
        .context("the proxy's classification")?;
        assert_eq!(
            classification.declared_summary["bindings"]["status"] == json!("supported"),
            supported,
            "{block}: {:#}",
            classification.declared_summary
        );
        assert_eq!(stored().await?, before, "{block}");
        let listed = bigname_storage::families::name::assert_bound_names_match_the_walk(
            &database.pool,
            "ethereum-mainnet",
            ROUND2_RESOLVER,
            None,
        )
        .await?;
        assert_eq!(listed.is_empty(), !supported, "{block}: {listed:?}");
    }
    database.cleanup().await
}

/// A cursor written before the listing continued by order names holds the shown name under
/// `normalized_name`. The route refuses it as malformed rather than continue from a name it no
/// longer orders by.
#[tokio::test]
async fn a_bound_names_cursor_from_before_the_order_names_is_refused() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bound_paging(&database).await?;
    let uri = format!("/v1/resolvers/1/{FAMILY_RESOLVER}?page_size=1");
    let (_, first) = read_family_response(&database, &uri).await?;
    let cursor = first["data"]["bound_names"]["page"]["next_cursor"]
        .as_str()
        .context("a next cursor")?;
    let (status, body) = read_family_response(&database, &format!("{uri}&cursor={cursor}")).await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    // The same position as the earlier build wrote it.
    let mut payload: Value = serde_json::from_slice(&hex::decode(cursor)?)?;
    let position = payload["last_item"].as_object_mut().context("a cursor position")?;
    if let Some(order) = position.remove("order_name") {
        position.insert("normalized_name".to_owned(), order);
    }
    let old = hex::encode(serde_json::to_vec(&payload)?);
    let (status, body) = read_family_response(&database, &format!("{uri}&cursor={old}")).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body:#}");
    assert_eq!(body["error"]["code"], json!("invalid_input"), "{body:#}");
    database.cleanup().await
}

/// The namehash of a label-hash path, leftmost label first.
fn labelhash_path_node(labelhashes: &[String]) -> Result<String> {
    let mut node = [0u8; 32];
    for labelhash in labelhashes.iter().rev() {
        let label = hex::decode(labelhash.trim_start_matches("0x"))?;
        node = alloy_primitives::keccak256([node.as_slice(), label.as_slice()].concat()).0;
    }
    Ok(format!("0x{}", hex::encode(node)))
}

/// Two names without raw bytes, 32 labels each, whose bracketed forms share their first 2000
/// bytes and differ only after them. Both are listed and counted, in namehash order after that
/// shared order name, and a page of one continues from the first to the second.
#[tokio::test]
async fn names_without_raw_bytes_past_the_length_bound_list_in_namehash_order() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bound_paging(&database).await?;
    let before = paging_parity(&database, None).await?;
    let hash = |label: &str| format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()));
    let shared: Vec<String> = (0..30).map(|n| hash(&format!("deep-{n}"))).collect();
    let mut events = Vec::new();
    let mut long = Vec::new();
    for (n, branch) in ["alpha", "beta"].into_iter().enumerate() {
        let mut labelhashes = shared.clone();
        labelhashes.extend([hash(branch), hash("eth")]);
        let (id, resource) = seed_textless_family_name(
            &database,
            &labelhashes,
            0x9e1_0000 + 0x100 * n as u128,
            "ens_v1",
            241,
        )
        .await?;
        let parent = labelhash_path_node(&labelhashes[1..])?;
        events.extend(textless_child_events(&id, resource, &parent, &labelhashes[0], RC_OWNER, 241));
        events.push(family_event(
            &format!("paging-{id}-resolver"),
            Some(&id),
            Some(resource),
            "ResolverChanged",
            "ens_v1_registry_l1",
            241,
            1,
            json!({"node": id.strip_prefix("ens:").expect("ens id"), "resolver": FAMILY_RESOLVER}),
        ));
        long.push(id);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    publish_test_families(&database, 241).await?;
    let listed = paging_parity(&database, None).await?;
    assert_eq!(listed.len(), before.len() + 2, "{listed:?}");
    let (first, second) = (
        order_name(&database, &long[0]).await?.context("an order name")?,
        order_name(&database, &long[1]).await?.context("an order name")?,
    );
    assert_eq!((first.len(), &first), (2000, &second));
    let positions: Vec<usize> = listed
        .iter()
        .enumerate()
        .filter(|(_, name)| name.starts_with(&tl_bracket(&shared[0])))
        .map(|(position, _)| position)
        .collect();
    assert_eq!(positions.len(), 2, "{listed:?}");
    assert_eq!(positions[1], positions[0] + 1, "{listed:?}");
    // The order name ties, so the smaller namehash comes first. `alpha` is imported, `beta` is not.
    let alpha_first = listed[positions[0]].ends_with(".alpha.eth");
    assert_eq!(alpha_first, long[0] < long[1], "{listed:?}");
    let uri = format!("/v1/resolvers/1/{FAMILY_RESOLVER}?page_size=1&include=total_count");
    let pages = read_family_pages_in(&database, &uri, "/data/bound_names").await?;
    assert_eq!(tl_column(&pages, "name"), listed);
    assert!(pages.iter().all(|page| page["total_count"] == json!(listed.len())));
    database.cleanup().await
}
