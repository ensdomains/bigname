// History membership judged at the published block. Every input that decides which events a
// history read admits must come from evidence at or below the block the read is bound to: a
// relation or resolver pointer recorded after that block must not pull older events into it.
// These tests bind storage reads to a block below the newest evidence, and bind the routes to a
// family publication below it.

const BOUNDED_CHAIN: &str = "ethereum-mainnet";
const BOUNDED_ADDRESS: &str = "0x00000000000000000000000000000000000b0a01";
const BOUNDED_RESOLVER: &str = "0x00000000000000000000000000000000000b0a0c";

/// Blocks 200..=241 with the lookup head and Project publication at `published`.
async fn seed_bounded_membership_blocks(database: &TestDatabase, published: i64) -> Result<()> {
    let blocks = (200..=241)
        .map(|number| {
            raw_block(
                BOUNDED_CHAIN,
                &format!("0xhistory{number}"),
                (number > 200)
                    .then(|| format!("0xhistory{}", number - 1))
                    .as_deref(),
                number,
                1_700_000_000 + number,
            )
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    publish_bounded_membership_at(database, published).await
}

async fn publish_bounded_membership_at(database: &TestDatabase, block: i64) -> Result<()> {
    let timestamp = sqlx::types::time::OffsetDateTime::from_unix_timestamp(1_700_000_000 + block)?;
    seed_schema_v2_ens_lookup_head(
        &database.pool,
        block,
        &format!("0xhistory{block}"),
        &crate::v2::format_timestamp(timestamp),
    )
    .await
}

fn bounded_at(block: i64) -> std::collections::BTreeMap<String, i64> {
    std::collections::BTreeMap::from([(BOUNDED_CHAIN.to_owned(), block)])
}

fn bounded_page_options(block: i64) -> bigname_storage::HistoryPageOptions {
    bigname_storage::HistoryPageOptions {
        publication_block_bounds: Some(bounded_at(block)),
        block_window: Some(bigname_storage::HistoryBlockWindow {
            ranges: vec![bigname_storage::ChainBlockRange {
                chain_id: BOUNDED_CHAIN.to_owned(),
                from_block: None,
                to_block: Some(block),
            }],
        }),
        ..bigname_storage::HistoryPageOptions::default()
    }
}

/// A name bound to its own resource and token lineage from block 200. The identity carries no
/// events: each test adds the events that create its relations and history.
async fn seed_bounded_name(database: &TestDatabase, name: &str, seed: u128) -> Result<(String, Uuid)> {
    let resource = Uuid::from_u128(seed);
    let logical_name_id = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        BOUNDED_CHAIN,
        200,
        "0xhistory200",
        resource,
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
        "ens_v1",
    )
    .await?;
    Ok((logical_name_id, resource))
}

async fn bounded_address_history_hashes(
    database: &TestDatabase,
    relation: Option<bigname_storage::AddressNameRelation>,
    block: i64,
) -> Result<Vec<String>> {
    let page = bigname_storage::load_address_history_page_for_relations(
        &database.pool,
        BOUNDED_ADDRESS,
        None,
        relation.as_ref().map(std::slice::from_ref),
        bigname_storage::HistoryScope::Both,
        true,
        None,
        50,
        bigname_storage::HistorySummaryMode::Count,
        &bounded_page_options(block),
        false,
    )
    .await?;
    Ok(page
        .rows
        .into_iter()
        .filter_map(|row| row.transaction_hash)
        .collect())
}

fn bounded_route_hashes(payload: &Value) -> Vec<String> {
    history_transaction_hashes(payload)
        .into_iter()
        .map(str::to_owned)
        .collect()
}

// Verdict counterexample 1: the address has history and no relation to a second name at the
// bound; the relation appears above it and the current row is published. A read bound below the
// relation must not admit that name's older events.
#[tokio::test]
async fn address_relation_cited_above_the_bound_admits_no_older_events() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (held, held_resource) = seed_bounded_name(&database, "held-before.eth", 0xb0a_1000)
    .await?;
    let (later, later_resource) = seed_bounded_name(&database, "held-later.eth", 0xb0a_2000)
    .await?;
    let mut grant = v2_history_event(
        "bounded-held-grant",
        Some(&held),
        Some(held_resource),
        "RegistrationGranted",
        210,
    );
    grant.after_state["registrant"] = json!(BOUNDED_ADDRESS);
    // The later name's token moves to the address at 241.
    let mut later_holder = v2_history_event(
        "bounded-later-holder",
        Some(&later),
        Some(later_resource),
        "TokenControlTransferred",
        241,
    );
    later_holder.after_state["to"] = json!(BOUNDED_ADDRESS);
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            grant,
            v2_history_event(
                "bounded-held-renew",
                Some(&held),
                Some(held_resource),
                "RegistrationRenewed",
                215,
            ),
            // The later name's own history at and below the bound: none of it names the address.
            v2_history_event(
                "bounded-later-grant",
                Some(&later),
                Some(later_resource),
                "RegistrationGranted",
                230,
            ),
            v2_history_event(
                "bounded-later-renew",
                Some(&later),
                Some(later_resource),
                "RegistrationRenewed",
                238,
            ),
            later_holder,
        ],
    )
    .await?;

    // Project has published 240: the relation cited at 241 is not part of that publication.
    publish_test_families(&database, 240).await?;
    let payload = v2_history_payload_for_database(
        &database,
        &format!("/v1/addresses/{BOUNDED_ADDRESS}/history?page_size=20&include=total_count"),
    )
    .await?;
    assert_eq!(
        bounded_route_hashes(&payload),
        vec!["0xtx215", "0xtx210"],
        "{payload}"
    );
    assert_eq!(payload["page"]["total_count"], json!(2), "{payload}");
    let events = v2_history_payload_for_database(
        &database,
        &format!("/v1/events?address={BOUNDED_ADDRESS}&page_size=20&include=total_count"),
    )
    .await?;
    assert_eq!(
        bounded_route_hashes(&events),
        vec!["0xtx215", "0xtx210"],
        "{events}"
    );

    // Once 241 is published, a storage read bound at 240 still excludes the relation cited
    // at 241, and one bound at 241 admits it.
    publish_test_families(&database, 241).await?;
    for relation in [
        None,
        Some(bigname_storage::AddressNameRelation::TokenHolder),
    ] {
        let hashes = bounded_address_history_hashes(&database, relation, 240).await?;
        assert!(
            !hashes
                .iter()
                .any(|hash| hash == "0xtx230" || hash == "0xtx238"),
            "relation={relation:?}: a relation cited at block 241 admitted older events into a \
             read bound at 240: {hashes:?}"
        );
        let hashes = bounded_address_history_hashes(&database, relation, 241).await?;
        assert!(
            hashes.iter().any(|hash| hash == "0xtx238")
                && hashes.iter().any(|hash| hash == "0xtx230"),
            "relation={relation:?}: a read bound at 241 must admit the relation: {hashes:?}"
        );
    }
    let before = bounded_address_history_hashes(&database, None, 240).await?;
    assert_eq!(before, vec!["0xtx215", "0xtx210"]);

    let published = v2_history_payload_for_database(
        &database,
        &format!("/v1/addresses/{BOUNDED_ADDRESS}/history?page_size=20"),
    )
    .await?;
    assert_eq!(
        bounded_route_hashes(&published),
        vec!["0xtx241", "0xtx238", "0xtx230", "0xtx215", "0xtx210"],
        "{published}"
    );
    database.cleanup().await
}

/// A node-keyed ENSv1 resolver write: no logical name or resource of its own.
fn bounded_node_write(
    identity: &str,
    name: &str,
    resolver: &str,
    block: i64,
) -> Result<NormalizedEvent> {
    let mut event = v2_history_event(identity, None, None, "RecordChanged", block);
    event.source_family = "ens_v1_resolver_l1".to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.after_state = json!({
        "source_event": "TextChanged",
        "node": bigname_lookup::ens_namehash_hex(name)?,
        "resolver": resolver,
        "record_key": "text:url",
        "record_family": "text",
        "selector_key": "url",
        "value_retained": true,
        "value": identity,
    });
    event.raw_fact_ref["emitting_address"] = json!(resolver);
    Ok(event)
}

/// An ENSv1 registry pointer from `logical_name_id`'s registration to `resolver`.
fn bounded_pointer(
    identity: &str,
    name: &str,
    logical_name_id: &str,
    resource: Uuid,
    resolver: &str,
    block: i64,
) -> Result<NormalizedEvent> {
    let mut event = v2_history_event(
        identity,
        Some(logical_name_id),
        Some(resource),
        "ResolverChanged",
        block,
    );
    event.source_family = "ens_v1_registry_l1".to_owned();
    event.after_state = json!({
        "node": bigname_lookup::ens_namehash_hex(name)?,
        "resolver": resolver,
    });
    event.log_index = Some(1);
    Ok(event)
}

async fn bounded_event_id(database: &TestDatabase, identity: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT normalized_event_id FROM bigname_phase.normalized_events WHERE event_identity = $1",
    )
    .bind(identity)
    .fetch_one(&database.pool)
    .await?)
}

async fn bounded_name_history_hashes(
    database: &TestDatabase,
    logical_name_id: &str,
    resources: &[Uuid],
    scope: bigname_storage::HistoryScope,
    block: i64,
) -> Result<Vec<String>> {
    let page = bigname_storage::load_name_history_page(
        &database.pool,
        logical_name_id,
        resources,
        scope,
        true,
        None,
        50,
        bigname_storage::HistorySummaryMode::Count,
        &bounded_page_options(block),
        None,
    )
    .await?;
    Ok(page
        .rows
        .into_iter()
        .filter_map(|row| row.transaction_hash)
        .collect())
}

// Verdict counterexample 2: a node-keyed write at block 220 on a resolver the registration first
// selects at block 241. A read bound at 240 must not list the write, although the record inventory
// Project publishes for 241 attributes it.
#[tokio::test]
async fn resolver_pointer_above_the_bound_attributes_no_older_write() -> Result<()> {
    const NAME: &str = "pointed-later.eth";
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (logical_name_id, resource) = seed_bounded_name(&database, NAME, 0xb0a_3000)
    .await?;
    // The resolver the registration points at from 241 is a declared ENSv1 resolver.
    let manifest = declare_family_fixture_resolver(
        &database.pool,
        "ens",
        BOUNDED_CHAIN,
        "ens_v1_resolver_l1",
        BOUNDED_RESOLVER,
    )
    .await?;
    let mut write = bounded_node_write("pointed-write-220", NAME, BOUNDED_RESOLVER, 220)?;
    write.source_manifest_id = Some(manifest);
    write.manifest_version = 1;
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            v2_history_event(
                "pointed-grant",
                Some(&logical_name_id),
                Some(resource),
                "RegistrationGranted",
                205,
            ),
            write,
            bounded_pointer(
                "pointed-pointer-241",
                NAME,
                &logical_name_id,
                resource,
                BOUNDED_RESOLVER,
                241,
            )?,
        ],
    )
    .await?;
    // Project publishes 240; the pointer at 241 is not part of that publication.
    publish_test_families(&database, 240).await?;
    for route in [
        format!("/v1/names/{NAME}/history?scope=registration&page_size=20&include=total_count"),
        format!("/v1/names/{NAME}/history?scope=both&page_size=20&include=total_count"),
        format!("/v1/events?registration_id={resource}&page_size=20&include=total_count"),
    ] {
        let payload = v2_history_payload_for_database(&database, &route).await?;
        assert_eq!(
            bounded_route_hashes(&payload),
            vec!["0xtx205"],
            "{route}: {payload}"
        );
        assert_eq!(
            payload["page"]["total_count"],
            json!(1),
            "{route}: {payload}"
        );
    }

    // Project's publication at 241 attributes the write through the new pointer. A storage read
    // bound at 240 still must not list it.
    publish_test_families(&database, 241).await?;
    for scope in [
        bigname_storage::HistoryScope::Resource,
        bigname_storage::HistoryScope::Both,
    ] {
        let hashes =
            bounded_name_history_hashes(&database, &logical_name_id, &[resource], scope, 240)
                .await?;
        assert!(
            !hashes.iter().any(|hash| hash == "0xtx220"),
            "scope={scope:?}: a pointer at 241 attributed a block-220 write to a read bound at 240: \
             {hashes:?}"
        );
        let hashes =
            bounded_name_history_hashes(&database, &logical_name_id, &[resource], scope, 241)
                .await?;
        assert!(
            hashes.iter().any(|hash| hash == "0xtx220"),
            "scope={scope:?}: a read bound at 241 must list the attributed write: {hashes:?}"
        );
    }

    for route in [
        format!("/v1/names/{NAME}/history?scope=registration&page_size=20"),
        format!("/v1/events?registration_id={resource}&page_size=20"),
    ] {
        let payload = v2_history_payload_for_database(&database, &route).await?;
        assert_eq!(
            bounded_route_hashes(&payload),
            vec!["0xtx241", "0xtx220", "0xtx205"],
            "{route}: {payload}"
        );
    }
    database.cleanup().await
}

// The ENSv2 arm of the bounded attribution: an ENSv2 registry pointer attributes the node-keyed
// writes of a resolver classified as a manifest-declared `public_resolver_v2`, when the declaring
// manifest is active in the pointer's namespace at the bound. The writes carry no logical name or
// resource, so only this arm lists them.
#[tokio::test]
async fn ens_v2_pointer_attributes_public_resolver_v2_writes() -> Result<()> {
    const NAME: &str = "v2-pointed.eth";
    const RESOLVER_V2: &str = "0x00000000000000000000000000000000000b0a2c";
    let database = TestDatabase::new_migrated().await?;
    seed_bounded_membership_blocks(&database, 240).await?;
    let (logical_name_id, resource) = seed_bounded_name(&database, NAME, 0xb0a_7000)
    .await?;
    // The ENSv2 registry admitted the resolver, and the resolver manifest declares it as a
    // public_resolver_v2.
    admit_fixture_resolver(
        &database.pool,
        "ens_v2_registry_l1",
        "0x00000000000000000000000000000000000b0a2e",
        RESOLVER_V2,
    )
    .await?;
    let declared = |role: &str| {
        json!({"contracts":[{"role":role, "address":RESOLVER_V2, "proxy_kind":"none",
            "start_block":0, "read_features":[]}]})
    };
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO bigname_phase.manifest_versions
            (manifest_version, namespace, source_family, chain_id, deployment_label,
             rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v2_resolver_l1', $1, 'bounded-v2', 'shadow', 'test',
                 'test/ens/bounded-v2-resolver.toml', $2)
         RETURNING manifest_id",
    )
    .bind(BOUNDED_CHAIN)
    .bind(declared("public_resolver_v2"))
    .fetch_one(&database.pool)
    .await?;

    let mut declaration =
        v2_history_event("v2-declaration", None, None, "SourceManifestUpdated", 201);
    declaration.source_family = "ens_v2_resolver_l1".to_owned();
    declaration.derivation_kind = "manifest_sync".to_owned();
    declaration.source_manifest_id = Some(manifest_id);
    declaration.manifest_version = 1;
    declaration.after_state =
        json!({"rollout_status": "active", "manifest_payload": declared("public_resolver_v2")});
    let mut pointer = v2_history_event(
        "v2-pointer-210",
        Some(&logical_name_id),
        Some(resource),
        "ResolverChanged",
        210,
    );
    pointer.source_family = "ens_v2_registry_l1".to_owned();
    pointer.derivation_kind = "ens_v2_registry_resource_surface".to_owned();
    pointer.after_state = json!({"resolver": RESOLVER_V2});
    pointer.log_index = Some(1);
    let node_write = |identity: &str, block: i64, name: &str| -> Result<NormalizedEvent> {
        let mut event = v2_history_event(identity, None, None, "RecordChanged", block);
        event.source_family = "ens_v2_resolver_l1".to_owned();
        event.derivation_kind = "ens_v2_resolver".to_owned();
        event.source_manifest_id = Some(manifest_id);
        event.manifest_version = 1;
        event.after_state = json!({
            "source_event": "TextChanged",
            "node": bigname_lookup::ens_namehash_hex(name)?,
            "resolver": RESOLVER_V2,
            "record_key": "text:url",
            "record_family": "text",
            "selector_key": "url",
            "value_retained": true,
            "value": identity,
        });
        Ok(event)
    };
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            declaration,
            v2_history_event(
                "v2-grant",
                Some(&logical_name_id),
                Some(resource),
                "RegistrationGranted",
                205,
            ),
            pointer,
            node_write("v2-write-220", 220, NAME)?,
            node_write("v2-write-other-225", 225, "other-v2.eth")?,
        ],
    )
    .await?;
    publish_test_families(&database, 240).await?;
    let write = bounded_event_id(&database, "v2-write-220").await?;

    let attributed = bigname_storage::load_bounded_record_attribution(
        &database.pool,
        &[resource],
        Some(&bounded_at(240)),
    )
    .await?;
    assert_eq!(
        attributed.get(&resource).cloned().unwrap_or_default(),
        std::collections::BTreeSet::from([write]),
        "the ENSv2 pointer must attribute exactly the write for its own node"
    );
    for (scope, listed) in [("registration", true), ("both", true), ("name", false)] {
        let route = format!("/v1/names/{NAME}/history?scope={scope}&page_size=20");
        let payload = v2_history_payload_for_database(&database, &route).await?;
        let hashes = bounded_route_hashes(&payload);
        assert_eq!(
            hashes.iter().any(|hash| hash == "0xtx220"),
            listed,
            "{route}: {payload}"
        );
        assert!(
            !hashes.iter().any(|hash| hash == "0xtx225"),
            "{route}: {payload}"
        );
    }
    // The same arm names the write in the events feed: it was made after the pointer.
    let other = bounded_event_id(&database, "v2-write-other-225").await?;
    assert_eq!(
        bigname_storage::load_positional_record_names(&database.pool, &[write, other]).await?,
        std::collections::BTreeMap::from([(
            write,
            std::collections::BTreeSet::from([logical_name_id.clone()])
        )])
    );

    // A resolver that is not a declared public_resolver_v2 attributes nothing through this arm:
    // the manifest later declares the address in another role.
    let mut redeclaration =
        v2_history_event("v2-redeclaration", None, None, "SourceManifestUpdated", 230);
    redeclaration.source_family = "ens_v2_resolver_l1".to_owned();
    redeclaration.derivation_kind = "manifest_sync".to_owned();
    redeclaration.source_manifest_id = Some(manifest_id);
    redeclaration.manifest_version = 1;
    redeclaration.after_state = json!({"rollout_status": "active",
        "manifest_payload": declared("permissioned_resolver")});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[redeclaration]).await?;
    rebuild_fixture_families(&database.pool, BOUNDED_CHAIN, 240, "0xhistory240").await?;
    let attributed = bigname_storage::load_bounded_record_attribution(
        &database.pool,
        &[resource],
        Some(&bounded_at(240)),
    )
    .await?;
    assert!(attributed.is_empty(), "{attributed:?}");
    assert!(
        bigname_storage::load_positional_record_names(&database.pool, &[write, other])
            .await?
            .is_empty()
    );
    database.cleanup().await
}
