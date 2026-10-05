//! The real address route with small internal batches. Fixtures publish retained inputs through
//! Project; expected public identities are specified independently of the reader's candidates.

use super::*;
#[path = "v2_address_history_catalogue.rs"]
mod catalogue;
use std::sync::{Arc, Mutex};
use tracing::instrument::WithSubscriber;
use tracing_subscriber::prelude::*;

#[derive(Clone)]
struct StatementCounter(Arc<std::sync::atomic::AtomicUsize>);
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for StatementCounter {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() == "sqlx::query" {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

const ADDRESS: &str = "0x000000000000000000000000000000000000a235";
const OTHER: &str = "0x000000000000000000000000000000000000b235";
const RESOLVER: &str = "0x000000000000000000000000000000000000c235";
const BLOCK: i64 = 240;
const HASH: &str = "0xhistory240";

async fn measured(
    database: &TestDatabase,
    uri: &str,
    page_size: usize,
) -> Result<(Value, bigname_storage::AddressHistoryWorkingSet)> {
    let stats = Arc::new(Mutex::new(bigname_storage::AddressHistoryWorkingSet {
        batch_size: 7,
        ..Default::default()
    }));
    let statements = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let body = bigname_storage::with_address_history_working_set(
        stats.clone(),
        v2_history_payload_for_database(database, uri),
    )
    .with_subscriber(tracing_subscriber::registry().with(StatementCounter(statements.clone())))
    .await?;
    let mut stats = stats.lock().unwrap().clone();
    stats.counters.insert(
        "sql_statements",
        statements.load(std::sync::atomic::Ordering::Relaxed),
    );
    // SQL tracing is diagnostic; isolated deep/interleaving runs report statement work.
    // The live-allocation assertions below do not depend on tracing callbacks.
    assert!(
        stats.live.values().all(|live| *live == 0),
        "retained allocations after request: {stats:?}"
    );
    for key in [
        "witness_rows",
        "composed_inputs",
        "composed_results",
        "current_anchors",
        "attribution_inputs",
        "attribution_sql_rows",
        "attribution_results",
        "handoff_peers",
        "catalogue_proof_names",
    ] {
        assert!(
            stats.peak.get(key).copied().unwrap_or_default() <= 7,
            "{key}: {stats:?}"
        );
    }
    for key in ["cached_memberships", "cached_attribution"] {
        assert!(
            stats.peak.get(key).copied().unwrap_or_default() <= 1_024,
            "{key}: {stats:?}"
        );
    }
    assert!(
        stats
            .peak
            .get("retained_page_ids")
            .copied()
            .unwrap_or_default()
            <= page_size + 1,
        "{stats:?}"
    );
    assert!(
        stats.peak.get("page_payloads").copied().unwrap_or_default() <= page_size,
        "{stats:?}"
    );
    Ok((body, stats))
}

async fn seed_names(database: &TestDatabase, count: usize) -> Result<Vec<(i64, String)>> {
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum":{
            "chain_id":"ethereum-mainnet", "block_number":BLOCK, "block_hash":HASH,
            "timestamp":"2026-04-17T00:00:00Z"
        }}))
        .await?;
    let mut expected = Vec::new();
    for n in 0..count {
        let ids = 0x235000_u128 + n as u128 * 4;
        seed_relation_name_inputs_at(
            database,
            &format!("walk-{n:04}.eth"),
            ids,
            BLOCK,
            HASH,
            RelationNameAccounts {
                registrant: ADDRESS,
                controller: OTHER,
                resolver: RESOLVER,
            },
        )
        .await?;
        for (log, kind) in [
            "RegistrationGranted",
            "AuthorityTransferred",
            "ResolverChanged",
            "RecordChanged",
        ]
        .into_iter()
        .enumerate()
        {
            expected.push((log as i64, format!("relation-{ids:x}-{kind}")));
        }
    }
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", BLOCK, HASH).await?;
    expected.sort();
    Ok(expected)
}

#[tokio::test]
async fn address_history_walk_bounds_live_rows_and_preserves_complete_order() -> Result<()> {
    for count in [11, 259] {
        let database = TestDatabase::new_migrated().await?;
        let expected = seed_names(&database, count).await?;
        for order in ["asc", "desc"] {
            let mut ordered = expected.clone();
            if order == "desc" {
                ordered.reverse();
            }
            let expected_ids: Vec<_> = ordered
                .iter()
                .map(|(_, identity)| hkw_id(identity))
                .collect();
            let sizes: &[usize] = if count == 11 { &[1, 7, 200] } else { &[200] };
            for &size in sizes {
                let mut cursor = None;
                let mut actual = Vec::new();
                loop {
                    let mut uri = format!(
                        "/v1/addresses/{ADDRESS}/history?relation=owner&order={order}&page_size={size}&include=total_count"
                    );
                    if let Some(cursor) = &cursor {
                        uri.push_str(&format!("&cursor={cursor}"));
                    }
                    let (body, stats) = measured(&database, &uri, size).await?;
                    assert_eq!(
                        stats.counters.get("names_composed").copied().unwrap_or(0),
                        0,
                        "historical name/resource membership already proves these candidates: {stats:?}"
                    );
                    assert_eq!(
                        body["page"]["total_count"],
                        json!(expected_ids.len()),
                        "{body}"
                    );
                    assert!(
                        stats
                            .counters
                            .get("witness_batches")
                            .copied()
                            .unwrap_or_default()
                            > 0
                    );
                    let rows = body["data"].as_array().unwrap();
                    if body["page"]["has_more"] == true {
                        assert_eq!(rows.len(), size, "short nonterminal page: {body}");
                    }
                    actual.extend(hk_ids(&body));
                    cursor = body["page"]["next_cursor"].as_str().map(str::to_owned);
                    if cursor.is_none() {
                        break;
                    }
                }
                assert_eq!(actual, expected_ids, "{count}/{order}/{size}");
            }
        }
        for suffix in [
            "&record_key=missing",
            "&type=record&exclude_type=record",
            "&relation=role_holder",
        ] {
            let uri = format!("/v1/addresses/{ADDRESS}/history?page_size=7{suffix}");
            let (body, _) = measured(&database, &uri, 7).await?;
            assert_eq!(body["data"], json!([]), "{uri}: {body}");
            assert_eq!(body["page"]["total_count"], Value::Null);
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn address_history_walk_historical_name_keeps_a_different_current_resource() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let current = seed_role_holder(&database, json!(["set_resolver"])).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "beta.eth");
    let node = bigname_lookup::ens_namehash_hex("beta.eth")?;
    let (block, hash) = address_fixture_head(&database).await?;
    admit_fixture_resolver(
        &database.pool,
        "ens_v2_registry_l1",
        ROLE_REGISTRY,
        RESOLVER,
    )
    .await?;
    let (manifest, _) = declare_family_fixture_contract(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "ens_v2_resolver_l1",
        "public_resolver_v2",
        RESOLVER,
    )
    .await?;
    let pointer = address_fixture_event(
        "walk-current-resource-pointer",
        Some(&logical),
        Some(current),
        "ResolverChanged",
        "ens_v2_registry_l1",
        block,
        &hash,
        200_000,
        json!({"node":node,"resolver":RESOLVER}),
    );
    let renewal = address_fixture_event(
        "walk-current-resource-renewal",
        None,
        Some(current),
        "RegistrationRenewed",
        "ens_v2_registry_l1",
        block,
        &hash,
        200_001,
        json!({"expiry":1_900_000_000}),
    );
    let mut record = address_fixture_event(
        "walk-current-resource-record",
        None,
        None,
        "RecordChanged",
        "ens_v2_resolver_l1",
        block,
        &hash,
        200_002,
        json!({"node":node,"resolver":RESOLVER,"record_key":"text:description",
            "record_family":"text","selector_key":"description","value":"current"}),
    );
    record.source_manifest_id = Some(manifest);
    record.manifest_version = 1;
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[pointer, renewal, record])
        .await?;
    rebuild_address_fixture(&database).await?;
    let uri = format!(
        "/v1/addresses/{ROLE_HOLDER}/history?scope=registration&kind=RegistrationRenewed,RecordChanged&page_size=200&include=total_count"
    );
    let (before, stats) = measured(&database, &uri, 200).await?;
    let expected = vec![
        hkw_id("walk-current-resource-record"),
        hkw_id("walk-current-resource-renewal"),
    ];
    assert_eq!(hk_ids(&before), expected, "{before}");
    assert_eq!(before["page"]["total_count"], json!(2));
    // Explicit exact counts use the existing authoritative cursor. It composes this one
    // current-only role member, with the result cached for the rest of the same snapshot.
    assert_eq!(
        stats.counters.get("names_composed").copied().unwrap_or(0),
        1,
        "{stats:?}"
    );

    // The same name was held through another resource. Its logical-name proof cannot
    // suppress the current resource or the resolver records attributed to that resource.
    let historical = Uuid::from_u128(0xb235);
    sqlx::query("INSERT INTO resources(resource_id,chain_id,block_number,block_hash,canonicality_state) VALUES($1,'ethereum-mainnet',$2,$3,'canonical')")
        .bind(historical).bind(block).bind(&hash).execute(&database.pool).await?;
    let mut grant = address_fixture_event(
        "walk-prior-resource-grant",
        Some(&logical),
        Some(historical),
        "RegistrationGranted",
        "ens_v2_registry_l1",
        block,
        &hash,
        199_999,
        json!({"registrant":ROLE_HOLDER,"expiry":1_900_000_000}),
    );
    grant.derivation_kind = "ens_v2_registry_resource_surface".into();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    rebuild_address_fixture(&database).await?;
    let (after, stats) = measured(&database, &uri, 200).await?;
    assert_eq!(hk_ids(&after), expected, "{after}");
    assert_eq!(after["page"]["total_count"], before["page"]["total_count"]);
    assert_eq!(
        stats.counters.get("names_composed").copied().unwrap_or(0),
        1,
        "{stats:?}"
    );

    // Event filters must not narrow the historical membership evidence. The surface proof
    // applies even though this request excludes the grant that establishes it.
    let (surface, stats) = measured(
        &database,
        &format!(
            "/v1/addresses/{ROLE_HOLDER}/history?scope=name&kind=ResolverChanged&page_size=200"
        ),
        200,
    )
    .await?;
    assert!(
        hk_ids(&surface).contains(&hkw_id("walk-current-resource-pointer")),
        "{surface}"
    );
    assert_eq!(
        stats.counters.get("names_composed").copied().unwrap_or(0),
        0,
        "{stats:?}"
    );
    let (owner_only, _) = measured(&database, &format!("{uri}&relation=owner"), 200).await?;
    assert_eq!(
        owner_only["data"],
        json!([]),
        "the historical resource must not qualify the current one: {owner_only}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn address_history_walk_current_roles_cache_membership_and_bound_record_pairs() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let resource = seed_role_holder(&database, json!(["set_resolver"])).await?;
    // A name can retain many closed binding candidates. Keep them in the real Project
    // inputs: a bounded name batch is not a bound on one name's retained facts.
    sqlx::query("INSERT INTO surface_bindings(surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,active_to,chain_id,block_hash,block_number,canonicality_state) SELECT md5('walk-deep-'||n)::uuid,b.logical_name_id,b.resource_id,b.binding_kind,b.authority_arm,to_timestamp(n*2),to_timestamp(n*2+1),b.chain_id,b.block_hash,b.block_number,b.canonicality_state FROM (SELECT * FROM surface_bindings WHERE resource_id=$1 LIMIT 1) b CROSS JOIN generate_series(1,600) n")
        .bind(resource).execute(&database.pool).await?;
    let (block, hash) = address_fixture_head(&database).await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "beta.eth");
    let node = bigname_lookup::ens_namehash_hex("beta.eth")?;
    admit_fixture_resolver(
        &database.pool,
        "ens_v2_registry_l1",
        ROLE_REGISTRY,
        RESOLVER,
    )
    .await?;
    let (manifest, _) = declare_family_fixture_contract(
        &database.pool,
        "ens",
        "ethereum-mainnet",
        "ens_v2_resolver_l1",
        "public_resolver_v2",
        RESOLVER,
    )
    .await?;
    let pointer = address_fixture_event(
        "walk-role-pointer",
        Some(&logical),
        Some(resource),
        "ResolverChanged",
        "ens_v2_registry_l1",
        block,
        &hash,
        200_000,
        json!({"node":node,"resolver":RESOLVER}),
    );
    let mut events = vec![pointer];
    for n in 0..1_201 {
        let mut event = address_fixture_event(
            &format!("walk-role-record-{n:04}"),
            None,
            None,
            "RecordChanged",
            "ens_v2_resolver_l1",
            block,
            &hash,
            200_001 + n,
            json!({"node":node,"resolver":RESOLVER,"record_key":"text:description","record_family":"text","selector_key":"description","value":format!("value-{n}")}),
        );
        event.source_manifest_id = Some(manifest);
        event.manifest_version = 1;
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_address_fixture(&database).await?;
    let retained_candidates: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_binding_candidate WHERE logical_name_id=$1",
    )
    .bind(&logical)
    .fetch_one(&database.pool)
    .await?;
    assert!(
        retained_candidates >= 601,
        "Project must preserve the deep-name inputs"
    );
    let base = format!(
        "/v1/addresses/{ROLE_HOLDER}/history?relation=role_holder&scope=registration&kind=RecordChanged&page_size=200"
    );
    let (first, stats) = measured(&database, &base, 200).await?;
    assert_eq!(
        first["page"]["total_count"],
        Value::Null,
        "{first}; {stats:?}"
    );
    assert_eq!(
        stats.counters.get("names_composed").copied().unwrap_or(0),
        0,
        "published role membership must not be recomposed: {stats:?}"
    );
    eprintln!(
        "single-name stress: {retained_candidates} retained binding candidates, 1201 record events, no request composition; {stats:?}"
    );
    assert!(
        stats
            .counters
            .get("attribution_batches")
            .copied()
            .unwrap_or_default()
            > 1,
        "{stats:?}"
    );
    let expected: Vec<_> = (1_001..=1_200)
        .rev()
        .map(|n| hkw_id(&format!("walk-role-record-{n:04}")))
        .collect();
    assert_eq!(hk_ids(&first), expected);
    let cursor = first["page"]["next_cursor"].as_str().unwrap();
    let (next, stats) = measured(
        &database,
        &format!("{base}&cursor={cursor}&include=total_count"),
        200,
    )
    .await?;
    assert_eq!(next["page"]["total_count"], json!(1_201));
    assert_eq!(
        stats.counters.get("names_composed").copied().unwrap_or(0),
        1,
        "explicit exact count composes the current member once in the same snapshot: {stats:?}"
    );
    assert_eq!(
        hk_ids(&next),
        (801..=1_000)
            .rev()
            .map(|n| hkw_id(&format!("walk-role-record-{n:04}")))
            .collect::<Vec<_>>()
    );
    if let Ok(directory) = std::env::var("BIGNAME_HISTORY_KEEP_DEEP_DIR") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory)?;
        std::fs::write(directory.join("database-deep.txt"), &database.database_name)?;
        std::fs::write(
            directory.join("fixture-deep.json"),
            serde_json::to_vec_pretty(&json!({
                "database":database.database_name,"names":1,"retained_binding_candidates":retained_candidates,
                "record_events":1201,"route":base,"working_set":format!("{stats:?}")
            }))?,
        )?;
        database.pool.close().await;
        database.lookup_pool.close().await;
        return Ok(());
    }
    database.cleanup().await
}

#[tokio::test]
async fn address_history_walk_defaults_to_no_count_across_the_former_threshold() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_fixture(&database).await?;
    const HOLDER: &str = "0x00000000000000000000000000000000000000cc";
    let events = (0..9_998)
        .map(|n| {
            v2_history_event(
                &format!("walk-threshold-{n:05}"),
                Some("ens:history.eth"),
                None,
                "RegistrationRenewed",
                101,
            )
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    let base = format!("/v1/addresses/{HOLDER}/history?kind=RegistrationRenewed&page_size=1");
    for expected in [9_999, 10_000, 10_001] {
        if expected > 9_999 {
            let event = v2_history_event(
                &format!("walk-threshold-{expected}"),
                Some("ens:history.eth"),
                None,
                "RegistrationRenewed",
                101,
            );
            bigname_storage::insert_normalized_event_fixtures(&database.pool, &[event]).await?;
        }
        rebuild_address_fixture(&database).await?;
        let (body, stats) = measured(&database, &base, 1).await?;
        assert_eq!(body["page"]["total_count"], Value::Null);
        assert!(
            stats
                .peak
                .get("retained_page_ids")
                .copied()
                .unwrap_or_default()
                <= 2
        );
        let cursor = body["page"]["next_cursor"].as_str().unwrap();
        let (exact, _) = measured(
            &database,
            &format!("{base}&cursor={cursor}&include=total_count"),
            1,
        )
        .await?;
        assert_eq!(exact["page"]["total_count"], json!(expected));
    }
    database.cleanup().await
}

#[tokio::test]
async fn address_history_walk_handoff_winner_precedes_batches_and_public_cursors() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let originals = seed_names(&database, 19).await?;
    let mut events = Vec::new();
    let mut expected: Vec<_> = originals.into_iter().filter(|(log, _)| *log == 2).collect();
    for (origin, log) in [("walk-handoff-a", 100), ("walk-handoff-b", 101)] {
        // The smallest copy does not belong to this address. The minimum eligible copy is
        // followed by enough copies to cross both the main and peer FETCH boundaries.
        events.push(address_fixture_event(
            &format!("{origin}:ResolverChanged:registry-fallback-handoff:0000-outside"),
            None,
            None,
            "ResolverChanged",
            "ens_v1_registry_l1",
            BLOCK,
            HASH,
            log,
            json!({"node":"same-node","resolver":RESOLVER}),
        ));
        for n in 0..19 {
            let logical =
                bigname_storage::logical_name_id_for_name("ens", &format!("walk-{n:04}.eth"));
            let identity = format!(
                "{origin}:ResolverChanged:registry-fallback-handoff:{:04}",
                n + 1
            );
            events.push(address_fixture_event(
                &identity,
                Some(&logical),
                Some(Uuid::from_u128(0x235000 + n as u128 * 4)),
                "ResolverChanged",
                "ens_v1_registry_l1",
                BLOCK,
                HASH,
                log,
                json!({"node":"same-node","resolver":RESOLVER}),
            ));
            if n == 0 {
                expected.push((log, identity));
            }
        }
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", BLOCK, HASH).await?;
    expected.sort();
    for order in ["asc", "desc"] {
        let mut wanted = expected.clone();
        if order == "desc" {
            wanted.reverse();
        }
        let mut actual = Vec::new();
        let mut cursor = None;
        loop {
            let mut uri = format!(
                "/v1/addresses/{ADDRESS}/history?relation=owner&kind=ResolverChanged&page_size=1&order={order}"
            );
            if let Some(cursor) = &cursor {
                uri.push_str(&format!("&cursor={cursor}&include=total_count"));
            }
            let (body, stats) = measured(&database, &uri, 1).await?;
            assert_eq!(
                body["page"]["total_count"],
                if cursor.is_some() {
                    json!(21)
                } else {
                    Value::Null
                },
                "{body}"
            );
            if cursor.is_some() {
                // The explicit count enumerates the later duplicate groups; the default
                // first page can stop before reaching those groups.
                assert!(
                    stats
                        .counters
                        .get("handoff_batches")
                        .copied()
                        .unwrap_or_default()
                        > 1
                );
            }
            actual.extend(hk_ids(&body));
            cursor = body["page"]["next_cursor"].as_str().map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(
            actual,
            wanted.iter().map(|(_, id)| hkw_id(id)).collect::<Vec<_>>()
        );
    }
    database.cleanup().await
}

#[tokio::test]
#[ignore = "manual 1,027-name cache-interleaving and statement-count gate"]
async fn address_history_walk_interleaved_current_names_exceed_cache_capacity() -> Result<()> {
    const NAMES: usize = 1_027;
    let database = TestDatabase::new_migrated().await?;
    seed_names(&database, NAMES).await?;
    let mut events = Vec::new();
    for n in 0..NAMES {
        let logical = bigname_storage::logical_name_id_for_name("ens", &format!("walk-{n:04}.eth"));
        let resource = Uuid::from_u128(0x235000 + n as u128 * 4);
        events.push(address_role_event(
            Some(&logical),
            resource,
            ROLE_HOLDER,
            false,
            json!(["set_resolver"]),
            BLOCK,
            HASH,
        ));
        for wave in 0..3 {
            events.push(address_fixture_event(
                &format!("walk-interleaved-{wave}-{n:04}"),
                Some(&logical),
                Some(resource),
                "RegistrationRenewed",
                "ens_v1_registrar_l1",
                BLOCK,
                HASH,
                10_000 + wave,
                json!({"expiry":1_900_000_000_i64}),
            ));
        }
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", BLOCK, HASH).await?;
    let stats = Arc::new(Mutex::new(
        bigname_storage::AddressHistoryWorkingSet::default(),
    ));
    let started = std::time::Instant::now();
    let statements = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let body = bigname_storage::with_address_history_working_set(stats.clone(), v2_history_payload_for_database(&database,
        &format!("/v1/addresses/{ROLE_HOLDER}/history?relation=role_holder&kind=RegistrationRenewed&page_size=200&include=total_count")))
        .with_subscriber(tracing_subscriber::registry().with(StatementCounter(statements.clone()))).await?;
    let mut stats = stats.lock().unwrap().clone();
    stats.counters.insert(
        "sql_statements",
        statements.load(std::sync::atomic::Ordering::Relaxed),
    );
    assert_eq!(
        body["page"]["total_count"],
        json!(NAMES * 3),
        "{body}; {stats:?}"
    );
    assert!(stats.live.values().all(|n| *n == 0));
    assert_eq!(stats.peak.get("cached_memberships"), Some(&0));
    assert!(
        stats
            .peak
            .get("composed_inputs")
            .copied()
            .unwrap_or_default()
            <= 256
    );
    assert!(
        stats
            .counters
            .get("names_composed")
            .copied()
            .unwrap_or_default()
            <= NAMES * 3 + 256,
        "{stats:?}"
    );
    assert!(
        stats
            .counters
            .get("composition_batches")
            .copied()
            .unwrap_or_default()
            < NAMES / 4,
        "composition must be batched, not a query per event: {stats:?}"
    );
    eprintln!(
        "interleaved current names: elapsed={:?}; {stats:?}",
        started.elapsed()
    );
    database.cleanup().await
}

#[tokio::test]
async fn address_history_walk_applies_mirror_substitution_to_requested_pairs() -> Result<()> {
    for (source, expected) in [
        (MirrorFixtureSource::Exact, 3),
        (MirrorFixtureSource::Ancestor, 0),
        (MirrorFixtureSource::Absent, 0),
    ] {
        let database = v2_mirror_records_database(
            "alice.eth",
            "0x1010101010101010101010101010101010101010",
            source,
            "resolver",
        )
        .await?;
        let uri = format!(
            "/v1/addresses/{V2_ADDRESS}/history?relation=owner&scope=registration&kind=RecordChanged&page_size=200&include=total_count"
        );
        // The shared history fixture helper pins ENS to Mainnet. This fixture publishes
        // Sepolia, so use production manifest-derived collection admission.
        let stats = Arc::new(Mutex::new(bigname_storage::AddressHistoryWorkingSet {
            batch_size: 7,
            ..Default::default()
        }));
        let body = bigname_storage::with_address_history_working_set(stats.clone(), async {
            let state = AppState::new_with_rpc_urls(
                database.lookup_pool.clone(),
                bigname_lookup::ChainRpcUrls::default(),
            );
            let response = app_router(state)
                .oneshot(Request::builder().uri(&uri).body(Body::empty())?)
                .await?;
            let status = response.status();
            let body: Value = read_json(response).await?;
            assert_eq!(status, StatusCode::OK, "{body}");
            Ok::<_, anyhow::Error>(body)
        })
        .await?;
        let stats = stats.lock().unwrap().clone();
        assert!(stats.live.values().all(|n| *n == 0), "{stats:?}");
        assert!(stats.peak.get("witness_rows").copied().unwrap_or_default() <= 7);
        assert!(
            stats
                .peak
                .get("attribution_sql_rows")
                .copied()
                .unwrap_or_default()
                <= 7
        );
        assert_eq!(
            body["page"]["total_count"],
            json!(expected),
            "{body}; {stats:?}"
        );
        assert_eq!(body["data"].as_array().unwrap().len(), expected);
        if expected > 0 {
            assert_eq!(stats.peak.get("mirror_pointers"), Some(&1), "{stats:?}");
            assert!(
                stats
                    .peak
                    .get("mirror_sql_rows")
                    .copied()
                    .unwrap_or_default()
                    <= 8
            );
        }
        database.cleanup().await?;
    }
    Ok(())
}
