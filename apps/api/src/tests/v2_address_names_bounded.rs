// An address holding many names: every composition an address read makes stays within one
// chunk, the walk composes about a page, and every path serves the same pages.

const BULK_ADDRESS: &str = "0x00000000000000000000000000000000000b0b0b";
const BULK_REGISTRY: &str = "0x0000000000000000000000000000000000000b23";
const BULK_OTHER_ADDRESS: &str = "0x00000000000000000000000000000000000c0c0c";
// Names whose served spelling `ens_beautify` changes, so name order must follow the served name.
const BULK_EMOJI_LABELS: [&str; 4] = ["❤", "☺", "💩", "⌚"];

fn bulk_time(index: usize) -> (i64, String, String) {
    let at = format!("2023-{:02}-02T00:00:00Z", index + 1);
    (3000 + index as i64, format!("0xbulk-time-{index}"), at)
}

fn bulk_label(index: usize) -> String {
    if index < BULK_EMOJI_LABELS.len() {
        BULK_EMOJI_LABELS[index].to_owned()
    } else {
        format!("bulk{index:05}")
    }
}

/// `count` names, each registered to and owned by [`BULK_ADDRESS`], with repeating creation and
/// registration times and expiries so every sort has ties.
async fn seed_bulk_address_names(database: &TestDatabase, count: usize) -> Result<()> {
    seed_bulk_names(database, count, |_| BULK_ADDRESS, &[]).await
}

fn bulk_logical_name_id(index: usize) -> Result<String> {
    let name = format!("{}.eth", bulk_label(index));
    let normalized = bigname_domain::normalization::normalize_name(&name)?;
    Ok(phase_logical_identity("ens", &normalized.normalized_name)?.0)
}

/// Lists the names at `indexes` in the address index for [`BULK_ADDRESS`], as the index may
/// list a name the address has no relation to: such a name is a candidate that composes no row.
async fn list_bulk_names_without_a_relation(
    database: &TestDatabase,
    indexes: std::ops::Range<usize>,
) -> Result<()> {
    let ids = indexes
        .map(bulk_logical_name_id)
        .collect::<Result<Vec<_>>>()?;
    sqlx::query(
        "INSERT INTO bigname_phase.project_address_name_index
            (address, logical_name_id, relation, chain_id)
         SELECT $1, id, 'token_holder', 'ethereum-mainnet' FROM unnest($2::text[]) AS id",
    )
    .bind(BULK_ADDRESS)
    .bind(ids)
    .execute(&database.pool)
    .await?;
    Ok(())
}

/// [`seed_bulk_address_names`] with each name registered to and owned by `owner(index)`. For
/// each of `aliases` `(first, second)` the second name is bound to the first name's
/// registration, so `dedupe=registration` serves the two as one entry.
async fn seed_bulk_names(
    database: &TestDatabase,
    count: usize,
    owner: fn(usize) -> &'static str,
    aliases: &[(usize, usize)],
) -> Result<()> {
    seed_bulk_names_with_records(database, count, owner, aliases, |_| BulkRecords::default()).await
}

/// What a bulk registration's resolver holds: `addr` writes in order on
/// [`V2_RESOLVES_TO_RESOLVER`], which its registry pointer selects, and whether a later record
/// version clears them, which the inverse address index still lists.
#[derive(Default)]
struct BulkRecords {
    writes: Vec<(String, String)>,
    cleared: bool,
}

/// A resolver event of a bulk registration's node on [`V2_RESOLVES_TO_RESOLVER`].
fn bulk_resolver_event(
    manifest: i64,
    node: &str,
    block: i64,
    hash: &str,
    kind: &str,
    after: Value,
) -> NormalizedEvent {
    let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100;
    let mut event = history_event(
        &format!("bulk-resolver-{ordinal}"),
        None,
        None,
        Some("ethereum-mainnet"),
        Some(block),
        Some(hash),
        Some("0xrecords"),
        Some(ordinal),
        CanonicalityState::Canonical,
    );
    event.event_kind = kind.into();
    event.source_family = "ens_v1_resolver_l1".into();
    event.manifest_version = 1;
    event.source_manifest_id = Some(manifest);
    event.raw_fact_ref = json!({"kind":"raw_log", "emitting_address":V2_RESOLVES_TO_RESOLVER,
        "transaction_index":0});
    event.before_state = json!({});
    let mut after = after;
    after["node"] = json!(node);
    after["resolver"] = json!(V2_RESOLVES_TO_RESOLVER);
    event.after_state = after;
    event
}

/// The names per block `BIGNAME_BULK_BLOCK_NAMES` asks for in the manual measurement. Without it
/// every bulk name is created in one of five blocks and registered in one of four.
fn bulk_block_names() -> Option<usize> {
    std::env::var("BIGNAME_BULK_BLOCK_NAMES")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|names| *names > 0)
}

/// The blocks bulk name `index` is created and registered in. With a block size, names are
/// created in blocks 3000 on and registered in blocks 3500 on, one block per group of that
/// many names, at times that rise with the block.
fn bulk_blocks(index: usize) -> ((i64, String, String), (i64, String, String)) {
    let Some(names) = bulk_block_names() else {
        return (bulk_time(index % 5), bulk_time(5 + index % 4));
    };
    let group = index / names;
    let at = |month: u32| format!("2023-{month:02}-01T{:02}:{:02}:00Z", group / 60, group % 60);
    (
        (
            3000 + group as i64,
            format!("0xbulk-created-{group}"),
            at(1),
        ),
        (
            3500 + group as i64,
            format!("0xbulk-registered-{group}"),
            at(6),
        ),
    )
}

/// The Ethereum blocks the bulk names of [`bulk_blocks`] use, below the head at 4000.
fn bulk_lineage(count: usize) -> Result<Vec<(i64, String, String)>> {
    let Some(names) = bulk_block_names() else {
        return Ok((0..10).map(bulk_time).collect());
    };
    let groups = count.div_ceil(names);
    anyhow::ensure!(
        groups <= 500,
        "{count} names need more than 500 blocks of {names}"
    );
    Ok((0..groups)
        .flat_map(|group| {
            let (created, registered) = bulk_blocks(group * names);
            [created, registered]
        })
        .collect())
}

/// [`rebuild_fixture_families`] for the blocks of `BIGNAME_BULK_BLOCK_NAMES`, more than one run
/// applies: later runs advance from the marker the rebuild reached, as the Project phase does.
async fn rebuild_spread_bulk_families(pool: &PgPool) -> Result<()> {
    use bigname_project::families::{FamilyMode, FamilyOptions, apply, input_token};

    let target = bigname_project::Marker {
        number: 4000,
        hash: "0xbulk-head".to_owned(),
    };
    let options = FamilyOptions::new(bigname_content_hash::PROJECTION_CONTENT_HASH);
    for run in 0..16 {
        let mode = if run == 0 {
            FamilyMode::Rebuild
        } else {
            FamilyMode::Normal
        };
        let token = input_token(pool, "ethereum-mainnet").await?;
        let outcome = apply(pool, "ethereum-mainnet", &target, mode, &token, &options).await?;
        if outcome.marker.as_ref().map(|marker| marker.number) == Some(target.number) {
            return Ok(());
        }
        anyhow::ensure!(
            outcome.budget_exhausted,
            "the bulk rebuild stopped short of 4000: {outcome:?}"
        );
    }
    anyhow::bail!("the bulk rebuild did not reach 4000 in 16 runs")
}

/// Prints how long the bulk seed has run at `stage`, in the manual measurement only.
fn bulk_seed_stage(started: std::time::Instant, stage: &str) {
    if std::env::var("BIGNAME_BULK_NAMES").is_ok() {
        println!("seed stage {stage}: {:?}", started.elapsed());
    }
}

/// [`seed_bulk_names`] with each registration's resolver records `records(index)`, an alias
/// reading its registration's.
async fn seed_bulk_names_with_records(
    database: &TestDatabase,
    count: usize,
    owner: fn(usize) -> &'static str,
    aliases: &[(usize, usize)],
    records: fn(usize) -> BulkRecords,
) -> Result<()> {
    let started = std::time::Instant::now();
    database
        .seed_snapshot_selector_chain_positions(&json!({"base":{
            "chain_id":"base-mainnet", "block_number":1, "block_hash":"0xcount-base-empty",
            "timestamp":"2024-01-01T00:00:00Z"
        }}))
        .await?;
    rebuild_fixture_families(&database.pool, "base-mainnet", 1, "0xcount-base-empty").await?;
    bulk_seed_stage(started, "base chain rebuilt");
    for (block, hash, at) in bulk_lineage(count)? {
        sqlx::query(
            "INSERT INTO chain_lineage
                (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             VALUES ('ethereum-mainnet', $1, $2, $3::timestamptz, 'canonical')",
        )
        .bind(&hash)
        .bind(block)
        .bind(&at)
        .execute(&database.pool)
        .await?;
    }
    let (mut tokens, mut resources, mut surfaces, mut bindings, mut events) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut manifest = None;
    for index in 0..count {
        let name = format!("{}.eth", bulk_label(index));
        let normalized = bigname_domain::normalization::normalize_name(&name)?;
        let (logical, namehash) = phase_logical_identity("ens", &normalized.normalized_name)?;
        let ((created_block, created_hash, created_at), (registered_block, registered_hash, _)) =
            bulk_blocks(index);
        let registration = aliases
            .iter()
            .find(|(_, second)| *second == index)
            .map_or(index, |(first, _)| *first);
        let holder = owner(index);
        let resource = Uuid::from_u128(0x7000_0000 + registration as u128 * 4);
        let token = Uuid::from_u128(0x7000_0001 + registration as u128 * 4);
        let binding = Uuid::from_u128(0x7000_0002 + index as u128 * 4);
        let event_key = if registration == index {
            format!("bulk-{resource}")
        } else {
            format!("bulk-{resource}-{index}")
        };
        if registration == index {
            tokens.push(TokenLineage {
                token_lineage_id: token,
                chain_id: "ethereum-mainnet".into(),
                block_number: created_block,
                block_hash: created_hash.clone(),
                provenance: json!({}),
                canonicality_state: CanonicalityState::Canonical,
            });
            resources.push(Resource {
                resource_id: resource,
                token_lineage_id: Some(token),
                chain_id: "ethereum-mainnet".into(),
                block_number: created_block,
                block_hash: created_hash.clone(),
                provenance: json!({}),
                canonicality_state: CanonicalityState::Canonical,
            });
        }
        surfaces.push(NameSurface {
            logical_name_id: logical.clone(),
            namespace: "ens".into(),
            input_name: name.clone(),
            canonical_display_name: normalized.canonical_display_name,
            normalized_name: normalized.normalized_name.clone(),
            dns_encoded_name: Some(normalized.dns_encoded_name),
            namehash,
            labelhashes: vec![],
            normalizer_version: bigname_domain::normalization::ENS_NORMALIZER_VERSION.into(),
            normalization_warnings: json!([]),
            normalization_errors: json!([]),
            chain_id: "ethereum-mainnet".into(),
            block_number: created_block,
            block_hash: created_hash.clone(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        });
        bindings.push(SurfaceBinding {
            surface_binding_id: binding,
            logical_name_id: format!("ens:{name}"),
            resource_id: resource,
            binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
            authority_arm: "ens_v1".into(),
            active_from: parse_rfc3339_utc_timestamp(&created_at)
                .map_err(|error| anyhow::anyhow!("{error}"))?,
            active_to: None,
            chain_id: "ethereum-mainnet".into(),
            block_number: created_block,
            block_hash: created_hash.clone(),
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        });
        let logical_event = bigname_storage::logical_name_id_for_name("ens", &name);
        let node = bigname_lookup::ens_namehash_hex(&normalized.normalized_name)?;
        let expiry = 1_800_000_000_i64 + (index % 37) as i64 * 86_400;
        for (block, hash, kind, family, after) in [
            (
                created_block,
                &created_hash,
                "AuthorityTransferred",
                "ens_v1_registry_l1",
                json!({"source_event":"Transfer", "node":node, "owner":holder,
                    "owner_getter":holder, "registry_contract":BULK_REGISTRY,
                    "emitter_role":"registry"}),
            ),
            (
                registered_block,
                &registered_hash,
                "RegistrationGranted",
                "ens_v1_registrar_l1",
                json!({"authority_kind":"registrar", "registrant":holder,
                    "expiry":expiry}),
            ),
        ] {
            events.push(address_fixture_event(
                &format!("{event_key}-{kind}"),
                Some(&logical_event),
                Some(resource),
                kind,
                family,
                block,
                hash,
                NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100,
                after,
            ));
        }
        let held = records(index);
        if registration != index || held.writes.is_empty() {
            continue;
        }
        if manifest.is_none() {
            manifest = Some(
                declare_family_fixture_resolver(
                    &database.pool,
                    "ens",
                    "ethereum-mainnet",
                    "ens_v1_resolver_l1",
                    V2_RESOLVES_TO_RESOLVER,
                )
                .await?,
            );
        }
        let manifest = manifest.context("the bulk resolver is declared")?;
        events.push(address_fixture_event(
            &format!("{event_key}-ResolverChanged"),
            Some(&logical_event),
            Some(resource),
            "ResolverChanged",
            "ens_v1_registry_l1",
            registered_block,
            &registered_hash,
            NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 + 100,
            json!({"source_event":"NewResolver", "node":node, "resolver":V2_RESOLVES_TO_RESOLVER}),
        ));
        for (key, value) in &held.writes {
            events.push(bulk_resolver_event(
                manifest,
                &node,
                registered_block,
                &registered_hash,
                "RecordChanged",
                family_fixture_record_write(key, Some(json!(value))),
            ));
        }
        if held.cleared {
            events.push(bulk_resolver_event(
                manifest,
                &node,
                registered_block,
                &registered_hash,
                "RecordVersionChanged",
                json!({"source_event":"VersionChanged", "record_version":index + 1}),
            ));
        }
    }
    bulk_seed_stage(started, "rows built");
    for chunk in tokens.chunks(1000) {
        upsert_test_token_lineages(&database.pool, chunk).await?;
    }
    for chunk in resources.chunks(1000) {
        upsert_test_resources(&database.pool, chunk).await?;
    }
    for chunk in surfaces.chunks(1000) {
        upsert_test_name_surfaces(&database.pool, chunk).await?;
    }
    for chunk in bindings.chunks(1000) {
        upsert_test_surface_bindings(&database.pool, chunk).await?;
    }
    for chunk in events.chunks(1000) {
        bigname_storage::insert_normalized_event_fixtures(&database.pool, chunk).await?;
    }
    sqlx::query(
        "INSERT INTO chain_lineage
            (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ('ethereum-mainnet', '0xbulk-head', 4000, '2024-05-31T18:26:47Z', 'canonical')",
    )
    .execute(&database.pool)
    .await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
            "chain_id":"ethereum-mainnet", "block_number":4000, "block_hash":"0xbulk-head",
            "timestamp":"2024-05-31T18:26:47Z"
        }}))
        .await?;
    if manifest.is_some() {
        declare_resolves_to_default_address(database).await?;
    }
    bulk_seed_stage(started, "rows and events inserted");
    if bulk_block_names().is_some() {
        rebuild_spread_bulk_families(&database.pool).await?;
    } else {
        rebuild_fixture_families(&database.pool, "ethereum-mainnet", 4000, "0xbulk-head").await?;
    }
    bulk_seed_stage(started, "ethereum rebuilt");
    Ok(())
}

async fn bulk_payload(database: &TestDatabase, uri: &str) -> Result<Value> {
    v2_address_names_payload_for_database(database, uri).await
}

/// The response helper compares role summaries with a second complete per-resource request.
fn address_name_test_requests(uri: &str) -> usize {
    if uri.contains("role_summary") { 2 } else { 1 }
}

/// Every page of `uri` at `page_size`, following `next_cursor`.
async fn walk_all_pages(database: &TestDatabase, uri: &str) -> Result<Vec<Value>> {
    walk_pages(database, uri, 1000).await
}

/// The first `limit` pages of `uri`, following `next_cursor`.
async fn walk_pages(database: &TestDatabase, uri: &str, limit: usize) -> Result<Vec<Value>> {
    let separator = if uri.contains('?') { '&' } else { '?' };
    let mut pages = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page_uri = match &cursor {
            Some(cursor) => format!("{uri}{separator}cursor={cursor}"),
            None => uri.to_owned(),
        };
        let payload = bulk_payload(database, &page_uri).await?;
        cursor = payload["page"]["next_cursor"].as_str().map(str::to_owned);
        pages.push(payload);
        if cursor.is_none() || pages.len() == limit {
            return Ok(pages);
        }
    }
}

/// The parts of a page every path must serve alike: rows, cursor and whether more follow.
fn page_body(payload: &Value) -> Value {
    json!({
        "data": payload["data"],
        "next_cursor": payload["page"]["next_cursor"],
        "has_more": payload["page"]["has_more"],
    })
}

async fn with_paths<F: std::future::Future>(future: F) -> (F::Output, Vec<&'static str>) {
    use bigname_storage::families::records::seams::with_address_name_paths;
    let paths = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = with_address_name_paths(paths.clone(), future).await;
    let paths = paths.lock().unwrap().clone();
    (output, paths)
}

async fn with_exact_total_cap<F: std::future::Future>(cap: usize, future: F) -> F::Output {
    bigname_storage::families::records::seams::with_exact_total_cap(cap, future).await
}

async fn with_batches<F: std::future::Future>(future: F) -> (F::Output, Vec<usize>) {
    use bigname_storage::families::records::seams::with_composed_name_batches;
    let batches = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let output = with_composed_name_batches(batches.clone(), future).await;
    let batches = batches.lock().unwrap().clone();
    (output, batches)
}

#[tokio::test]
async fn v2_address_names_compose_a_large_address_in_bounded_chunks() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 600).await?;

    // The exact path, which serves 600 names below the cap, composes 64 names at a time.
    let names = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=1");
    let ((payload, paths), batches) = with_batches(with_compose_chunk(
        64,
        with_paths(bulk_payload(&database, &names)),
    ))
    .await;
    let payload = payload?;
    assert_eq!(paths, ["exact"]);
    assert_eq!(payload["page"]["total_count"], json!(600), "{payload}");
    assert_eq!(payload["page"]["has_more"], json!(true), "{payload}");
    assert!(batches.len() >= 10, "{batches:?}");
    assert!(batches.iter().all(|batch| *batch <= 64), "{batches:?}");
    assert_eq!(batches.iter().sum::<usize>(), 600, "{batches:?}");

    let history = format!("/v1/addresses/{BULK_ADDRESS}/history?page_size=1");
    let (payload, batches) =
        with_batches(with_compose_chunk(64, bulk_payload(&database, &history))).await;
    let payload = payload?;
    assert_eq!(payload["data"].as_array().unwrap().len(), 1, "{payload}");
    assert_eq!(payload["page"]["total_count"], Value::Null, "{payload}");
    assert_eq!(payload["page"]["has_more"], json!(true), "{payload}");
    assert!(payload["page"]["next_cursor"].is_string(), "{payload}");
    // History reads the published catalogue without composing the address's names.
    assert!(batches.is_empty(), "{batches:?}");

    // Former owners compose every indexed name in chunks, then the page's names in full.
    let former = format!("/v1/addresses/{BULK_ADDRESS}/names?relation=former_owner&page_size=1");
    let (payload, batches) =
        with_batches(with_compose_chunk(64, bulk_payload(&database, &former))).await;
    payload?;
    assert!(batches.len() >= 11, "{batches:?}");
    assert!(batches.iter().all(|batch| *batch <= 64), "{batches:?}");
    assert!(*batches.last().unwrap() <= 1, "{batches:?}");

    // Above the cap the first page composes one walk batch, not the address.
    let (payload, batches) = with_batches(with_exact_total_cap(
        100,
        with_paths(bulk_payload(&database, &names)),
    ))
    .await;
    let (payload, paths) = payload;
    let payload = payload?;
    assert_eq!(paths, ["walk"]);
    assert_eq!(payload["page"]["total_count"], Value::Null, "{payload}");
    assert_eq!(payload["page"]["has_more"], json!(true), "{payload}");
    assert!(batches.iter().sum::<usize>() <= 32, "{batches:?}");

    // include=total_count above the cap counts every candidate a chunk at a time, then walks.
    let exact = format!("{names}&include=total_count");
    let ((payload, paths), batches) = with_batches(with_exact_total_cap(
        100,
        with_paths(bulk_payload(&database, &exact)),
    ))
    .await;
    assert_eq!(paths, ["walk"]);
    assert_eq!(payload?["page"]["total_count"], json!(600));
    assert!(batches.iter().all(|batch| *batch <= 256), "{batches:?}");
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_walk_and_chunks_serve_identical_pages() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    // 65 names: three pages of 25, a partial last chunk of 2 at chunk size 7, and two 5-row
    // pages that each compose one 8-name walk step.
    seed_bulk_address_names(&database, 65).await?;
    for sort in ["name", "expires_at", "registered_at", "created_at"] {
        for order in ["asc", "desc"] {
            for dedupe in ["name", "registration"] {
                let uri = format!(
                    "/v1/addresses/{BULK_ADDRESS}/names?sort={sort}&order={order}&dedupe={dedupe}&page_size=25"
                );
                let exact = walk_all_pages(&database, &uri).await?;
                let chunked = with_compose_chunk(7, walk_all_pages(&database, &uri)).await?;
                assert_eq!(exact, chunked, "{uri}");
                // The walk composes in chunks of 7 too, so its batches span several chunks.
                let (walked, paths) = with_compose_chunk(
                    7,
                    with_paths(with_exact_total_cap(0, walk_all_pages(&database, &uri))),
                )
                .await;
                let walked = walked?;
                assert!(paths.iter().all(|path| *path == "walk"), "{uri}: {paths:?}");
                assert_eq!(
                    exact.iter().map(page_body).collect::<Vec<_>>(),
                    walked.iter().map(page_body).collect::<Vec<_>>(),
                    "{uri}"
                );
                assert_eq!(exact[0]["page"]["total_count"], json!(65), "{uri}");
                assert!(
                    walked
                        .iter()
                        .all(|page| page["page"]["total_count"].is_null()),
                    "{uri}"
                );
            }
        }
    }
    // Small pages stop the walk early: the first pages and their cursors match the full read,
    // and the first page composes a fraction of the address.
    for sort in ["name", "expires_at", "registered_at"] {
        for order in ["asc", "desc"] {
            for dedupe in ["name", "registration"] {
                let uri = format!(
                    "/v1/addresses/{BULK_ADDRESS}/names?sort={sort}&order={order}&dedupe={dedupe}&page_size=5"
                );
                let exact = walk_pages(&database, &uri, 2).await?;
                let (walked, batches) =
                    with_batches(with_exact_total_cap(0, walk_pages(&database, &uri, 2))).await;
                assert_eq!(
                    exact.iter().map(page_body).collect::<Vec<_>>(),
                    walked?.iter().map(page_body).collect::<Vec<_>>(),
                    "{uri}"
                );
                assert!(batches.iter().sum::<usize>() < 65, "{uri}: {batches:?}");
            }
        }
    }
    // Selective filters walk past most candidates without keeping their rows.
    for query in [
        "q=bulk0001",
        "q=zzz",
        "q=bulk00&match=contains&sort=created_at&dedupe=registration",
    ] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=3&{query}");
        let exact = walk_all_pages(&database, &uri).await?;
        let walked = with_exact_total_cap(0, walk_all_pages(&database, &uri)).await?;
        assert_eq!(
            exact.iter().map(page_body).collect::<Vec<_>>(),
            walked.iter().map(page_body).collect::<Vec<_>>(),
            "{uri}"
        );
    }
    // Served names, not stored spellings, decide name order: the emoji names sort as served.
    let first = bulk_payload(
        &database,
        &format!("/v1/addresses/{BULK_ADDRESS}/names?order=desc&page_size=4"),
    )
    .await?;
    let served: Vec<&str> = first["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["display_name"].as_str().unwrap())
        .collect();
    assert!(
        served.iter().any(|name| name.contains('\u{fe0f}')),
        "{served:?}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_walk_falls_back_when_a_stored_sort_key_disagrees() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 120).await?;
    let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?sort=expires_at&page_size=10");
    let exact = bulk_payload(&database, &uri).await?;
    // A stale stored expiry that walks the name first; composition still serves the real one.
    let logical = phase_logical_identity("ens", "bulk00050.eth")?.0;
    let updated = sqlx::query(
        "UPDATE bigname_phase.project_name_summary SET expires_at = 1 WHERE logical_name_id = $1",
    )
    .bind(&logical)
    .execute(&database.pool)
    .await?;
    assert_eq!(updated.rows_affected(), 1);
    let (walked, paths) = with_paths(with_exact_total_cap(0, bulk_payload(&database, &uri))).await;
    assert_eq!(paths, ["fallback"]);
    let walked = walked?;
    assert_eq!(page_body(&exact), page_body(&walked));
    assert!(walked["page"]["total_count"].is_null(), "{walked}");
    database.cleanup().await
}

const RICH_FIXTURE_QUERIES: [&str; 21] = [
    "",
    "relation=owner",
    "relation=manager",
    "relation=owner,manager",
    "relation=role_holder",
    "q=sha",
    "q=a&match=contains",
    "authority=ens_v1",
    "authority=ens_v0,ens_v2",
    "is_migrated=false",
    "parent=eth",
    "include=role_summary",
    "include=counts",
    "dedupe=registration",
    "dedupe=registration&order=desc",
    "sort=expires_at",
    "sort=expires_at&order=desc&dedupe=registration",
    "sort=registered_at",
    "sort=registered_at&order=desc",
    "sort=created_at&dedupe=registration",
    "sort=created_at&order=desc",
];

const ROLES_FIXTURE_QUERIES: [&str; 8] = [
    "",
    "relation=role_holder",
    "relation=owner",
    "dedupe=registration",
    "sort=expires_at&order=desc",
    "sort=registered_at&dedupe=registration",
    "sort=created_at&order=desc&dedupe=registration",
    "order=desc&relation=role_holder",
];

const REGISTRY_CHILDREN_QUERIES: [&str; 4] = [
    "",
    "order=desc&dedupe=registration",
    "sort=expires_at",
    "sort=registered_at&order=desc",
];

/// The rich fixture's query matrix: one chunk at a time and the walk serve the default pages.
#[tokio::test]
async fn v2_address_names_rich_fixture_is_identical_across_paths() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    for query in RICH_FIXTURE_QUERIES {
        for page_size in [1, 2, 50] {
            let uri = format!("/v1/addresses/{V2_ADDRESS}/names?page_size={page_size}&{query}");
            let exact = walk_all_pages(&database, &uri).await?;
            let chunked = with_compose_chunk(1, walk_all_pages(&database, &uri)).await?;
            assert_eq!(exact, chunked, "{uri}");
            let (walked, paths) =
                with_paths(with_exact_total_cap(0, walk_all_pages(&database, &uri))).await;
            let walked = walked?;
            assert!(paths.iter().all(|path| *path == "walk"), "{uri}: {paths:?}");
            assert_eq!(
                exact.iter().map(page_body).collect::<Vec<_>>(),
                walked.iter().map(page_body).collect::<Vec<_>>(),
                "{uri}"
            );
        }
    }
    for query in ["relation=resolves_to", "relation=former_owner"] {
        let uri = format!("/v1/addresses/{V2_ADDRESS}/names?page_size=1&{query}");
        let exact = walk_all_pages(&database, &uri).await?;
        let chunked = with_compose_chunk(1, walk_all_pages(&database, &uri)).await?;
        assert_eq!(exact, chunked, "{uri}");
    }
    let history = format!("/v1/addresses/{V2_ADDRESS}/history?page_size=2");
    let exact = walk_all_pages(&database, &history).await?;
    let chunked = with_compose_chunk(1, walk_all_pages(&database, &history)).await?;
    assert_eq!(exact, chunked);
    database.cleanup().await
}

#[tokio::test]
async fn v2_address_names_roles_fixture_is_identical_across_paths() -> Result<()> {
    use bigname_storage::families::records::seams::with_compose_chunk;

    let database = TestDatabase::new_migrated().await?;
    seed_role_holder(&database, json!(["renew"])).await?;
    for address in [ROLE_HOLDER, V2_ADDRESS] {
        for query in ROLES_FIXTURE_QUERIES {
            let uri = format!("/v1/addresses/{address}/names?page_size=1&{query}");
            let exact = walk_all_pages(&database, &uri).await?;
            let chunked = with_compose_chunk(1, walk_all_pages(&database, &uri)).await?;
            assert_eq!(exact, chunked, "{uri}");
            let walked = with_exact_total_cap(0, walk_all_pages(&database, &uri)).await?;
            assert_eq!(
                exact.iter().map(page_body).collect::<Vec<_>>(),
                walked.iter().map(page_body).collect::<Vec<_>>(),
                "{uri}"
            );
        }
    }
    database.cleanup().await
}

/// Surface-less registry children the index also lists do not push a capped address off the walk.
#[tokio::test]
async fn v2_address_names_walk_serves_registry_children() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_registry_children_fixture(&database).await?;
    for query in REGISTRY_CHILDREN_QUERIES {
        let uri = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=2&{query}");
        let exact = walk_all_pages(&database, &uri).await?;
        let (walked, paths) =
            with_paths(with_exact_total_cap(0, walk_all_pages(&database, &uri))).await;
        let walked = walked?;
        assert!(paths.iter().all(|path| *path == "walk"), "{uri}: {paths:?}");
        assert_eq!(
            exact.iter().map(page_body).collect::<Vec<_>>(),
            walked.iter().map(page_body).collect::<Vec<_>>(),
            "{uri}"
        );
    }
    database.cleanup().await
}

/// One logical page read above the cap, including both HTTP requests when the role-summary
/// response helper compares complete responses. Its batches and paths retain both observations.
struct CappedPage {
    payload: Value,
    batches: Vec<usize>,
    paths: Vec<&'static str>,
}

/// The first `limit` pages of `uri` for an address treated as above the cap when it has more
/// than `cap` candidate names (the route's own cap when `None`). `first_policy` sizes the walk's
/// steps as the walk first did: four pages of candidates, at least 32, then doubling.
async fn capped_pages(
    database: &TestDatabase,
    uri: &str,
    limit: usize,
    cap: Option<usize>,
    first_policy: bool,
) -> Result<Vec<CappedPage>> {
    use bigname_storage::families::records::seams::with_legacy_walk_batches;

    let separator = if uri.contains('?') { '&' } else { '?' };
    let mut pages = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page_uri = match &cursor {
            Some(cursor) => format!("{uri}{separator}cursor={cursor}"),
            None => uri.to_owned(),
        };
        let request = async {
            // Box the observed child to limit temporary stack use in debug polls.
            let request = Box::pin(with_paths(with_batches(bulk_payload(database, &page_uri))));
            match cap {
                Some(cap) => with_exact_total_cap(cap, request).await,
                None => request.await,
            }
        };
        let ((payload, batches), paths) = if first_policy {
            with_legacy_walk_batches(request).await
        } else {
            request.await
        };
        let payload = payload?;
        cursor = payload["page"]["next_cursor"].as_str().map(str::to_owned);
        pages.push(CappedPage {
            payload,
            batches,
            paths,
        });
        if cursor.is_none() || pages.len() == limit {
            return Ok(pages);
        }
    }
}

fn composed(page: &CappedPage) -> usize {
    assert_eq!(
        page.paths.len(),
        1,
        "composition work bounds require one HTTP request"
    );
    page.batches.iter().sum()
}

/// Every page of `uri` read above the cap with the walk's first step sizes and with its current
/// ones: the whole response bodies are equal, both reads take the same path, and the rows,
/// cursors and `has_more` are the exact read's. Returns the current pages.
async fn assert_step_sizes_change_no_page(
    database: &TestDatabase,
    uri: &str,
) -> Result<Vec<CappedPage>> {
    let exact = walk_all_pages(database, uri).await?;
    let first = capped_pages(database, uri, 1000, Some(0), true).await?;
    let current = capped_pages(database, uri, 1000, Some(0), false).await?;
    let payloads = |pages: &[CappedPage]| -> Vec<Value> {
        pages.iter().map(|page| page.payload.clone()).collect()
    };
    let paths = |pages: &[CappedPage]| -> Vec<Vec<&'static str>> {
        pages.iter().map(|page| page.paths.clone()).collect()
    };
    for page in first.iter().chain(&current) {
        assert_eq!(
            page.paths.len(),
            address_name_test_requests(uri),
            "{uri}: {:?}",
            page.paths
        );
    }
    assert_eq!(payloads(&first), payloads(&current), "{uri}");
    assert_eq!(paths(&first), paths(&current), "{uri}");
    if uri.contains("dedupe=registration") {
        assert_eq!(
            first.iter().map(|page| &page.batches).collect::<Vec<_>>(),
            current.iter().map(|page| &page.batches).collect::<Vec<_>>(),
            "{uri}: registration must preserve every old composition input partition"
        );
    }
    assert_eq!(
        exact.iter().map(page_body).collect::<Vec<_>>(),
        current
            .iter()
            .map(|page| page_body(&page.payload))
            .collect::<Vec<_>>(),
        "{uri}"
    );
    Ok(current)
}

/// The sorts the walk has a stored key for, in both orders and under both dedupes.
fn walkable_bulk_queries() -> Vec<String> {
    let mut queries = Vec::new();
    for sort in ["name", "expires_at", "registered_at"] {
        for order in ["asc", "desc"] {
            for dedupe in ["name", "registration"] {
                queries.push(format!("sort={sort}&order={order}&dedupe={dedupe}"));
            }
        }
    }
    queries
}

/// A first page composes the page and the one name after it that shows more follow, and at
/// least 8 names: every name of this address yields one row.
#[tokio::test]
async fn v2_address_names_walk_first_page_composes_one_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 420).await?;
    for query in walkable_bulk_queries() {
        for (page_size, names, registration_names) in [(5, 8, 32), (7, 8, 32), (200, 201, 420)] {
            let names = if query.contains("dedupe=registration") {
                registration_names
            } else {
                names
            };
            let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size={page_size}&{query}");
            let pages = capped_pages(&database, &uri, 1, Some(0), false).await?;
            assert_eq!(pages[0].paths, ["walk"], "{uri}");
            assert_eq!(
                pages[0].payload["data"].as_array().unwrap().len(),
                page_size,
                "{uri}"
            );
            assert_eq!(composed(&pages[0]), names, "{uri}: {:?}", pages[0].batches);
        }
    }
    database.cleanup().await
}

/// A resumed page also composes the cursor's own name, which the walk starts on and the page
/// leaves out.
#[tokio::test]
async fn v2_address_names_walk_resumed_page_composes_one_page() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 420).await?;
    for query in walkable_bulk_queries() {
        for (page_size, names, registration_names) in [(5, 8, 32), (7, 9, 32), (200, 202, 221)] {
            let names = if query.contains("dedupe=registration") {
                registration_names
            } else {
                names
            };
            let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size={page_size}&{query}");
            let pages = capped_pages(&database, &uri, 2, Some(0), false).await?;
            assert_eq!(pages[1].paths, ["walk"], "{uri}");
            assert_eq!(
                pages[1].payload["data"].as_array().unwrap().len(),
                page_size,
                "{uri}"
            );
            assert_eq!(composed(&pages[1]), names, "{uri}: {:?}", pages[1].batches);
        }
    }
    database.cleanup().await
}

/// The walk's step sizes change no page of a holder whose every name yields a row: each sort
/// once, both orders and dedupes among them, at a page size that leaves a short last page and
/// at a small one, and filters that drop most names or all of them.
#[tokio::test]
async fn v2_address_names_walk_batch_policy_serves_identical_pages() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bulk_address_names(&database, 65).await?;
    for query in [
        "sort=name&order=asc&dedupe=name",
        "sort=expires_at&order=desc&dedupe=registration",
        "sort=registered_at&order=asc&dedupe=registration",
        "sort=created_at&order=desc&dedupe=name",
    ] {
        for page_size in [25, 5] {
            let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size={page_size}&{query}");
            let pages = assert_step_sizes_change_no_page(&database, &uri).await?;
            assert_eq!(pages.len(), 65_usize.div_ceil(page_size), "{uri}");
            assert!(pages.iter().all(|page| page.paths == ["walk"]), "{uri}");
        }
    }
    for query in [
        "q=bulk0001",
        "q=zzz",
        "q=bulk00&match=contains&sort=created_at&dedupe=registration",
    ] {
        let uri = format!("/v1/addresses/{BULK_ADDRESS}/names?page_size=3&{query}");
        let pages = assert_step_sizes_change_no_page(&database, &uri).await?;
        assert!(pages.iter().all(|page| page.paths == ["walk"]), "{uri}");
    }
    database.cleanup().await
}

/// The walk's step sizes change no page of the fixtures with every relation, filter and include,
/// ENSv2 role holders, surface-less registry children and names on two chains. At page sizes 1
/// and 2 these walks take several steps.
#[tokio::test]
async fn v2_address_names_fixtures_identical_across_batch_policies() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    for query in RICH_FIXTURE_QUERIES {
        for page_size in [1, 2, 50] {
            let uri = format!("/v1/addresses/{V2_ADDRESS}/names?page_size={page_size}&{query}");
            let pages = assert_step_sizes_change_no_page(&database, &uri).await?;
            assert!(
                pages
                    .iter()
                    .all(|page| page.paths == vec!["walk"; address_name_test_requests(&uri)]),
                "{uri}"
            );
        }
    }
    // A Basenames name beside the ENS names: the walk composes each step's names per chain.
    seed_identity_name(
        &database,
        "basenames:alpha.eth",
        "alpha.eth",
        "alpha.eth",
        "node:basenames-alpha.eth",
        Uuid::from_u128(0xe100),
        Uuid::from_u128(0xe101),
        Uuid::from_u128(0xe102),
        V2_ADDRESS,
        bigname_storage::AddressNameRelation::TokenHolder,
        106,
    )
    .await?;
    for query in [
        "",
        "order=desc",
        "sort=expires_at",
        "dedupe=registration&sort=registered_at",
    ] {
        let uri = format!("/v1/addresses/{V2_ADDRESS}/names?page_size=1&{query}");
        let pages = assert_step_sizes_change_no_page(&database, &uri).await?;
        assert!(pages.iter().all(|page| page.paths == ["walk"]), "{uri}");
        let namespaces: std::collections::BTreeSet<&str> = pages
            .iter()
            .flat_map(|page| page.payload["data"].as_array().unwrap())
            .map(|row| row["namespace"].as_str().unwrap())
            .collect();
        assert_eq!(
            namespaces,
            std::collections::BTreeSet::from(["basenames", "ens"]),
            "{uri}"
        );
    }
    database.cleanup().await?;

    let database = TestDatabase::new_migrated().await?;
    seed_role_holder(&database, json!(["renew"])).await?;
    for address in [ROLE_HOLDER, V2_ADDRESS] {
        for query in ROLES_FIXTURE_QUERIES {
            let uri = format!("/v1/addresses/{address}/names?page_size=1&{query}");
            assert_step_sizes_change_no_page(&database, &uri).await?;
        }
    }
    database.cleanup().await?;

    let database = TestDatabase::new_migrated().await?;
    seed_registry_children_fixture(&database).await?;
    for query in REGISTRY_CHILDREN_QUERIES {
        let uri = format!("/v1/addresses/{RC_OWNER}/names?namespace=ens&page_size=2&{query}");
        let pages = assert_step_sizes_change_no_page(&database, &uri).await?;
        assert!(pages.iter().all(|page| page.paths == ["walk"]), "{uri}");
    }
    database.cleanup().await
}
