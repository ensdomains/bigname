// History membership judged at the published block. Every input that decides which events a
// history read admits must come from evidence at or below the block the read is bound to: a
// relation or resolver pointer recorded after that block must not pull older events into it.
// These tests bind storage reads to a block below the newest evidence, and bind the routes to a
// publication below it, which is also the state a read sees between Project's projection swap and
// its recorded position.

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

/// Move Project's recorded position to `block`, leaving the chain head where it is.
async fn move_bounded_publication_to(database: &TestDatabase, block: i64) -> Result<()> {
    sqlx::query(
        "UPDATE chain_phase_state
         SET current_block_number = $1, current_block_hash = $2,
             target_block_number = $1, target_block_hash = $2
         WHERE chain_id = $3 AND phase_name = 'project'",
    )
    .bind(block)
    .bind(format!("0xhistory{block}"))
    .bind(BOUNDED_CHAIN)
    .execute(&database.pool)
    .await?;
    Ok(())
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

/// A name bound to its own resource and token lineage, with one current address relation whose
/// cited event lies at `relation_block`.
#[allow(clippy::too_many_arguments)]
async fn seed_bounded_name(
    database: &TestDatabase,
    name: &str,
    seed: u128,
    address: &str,
    relation: bigname_storage::AddressNameRelation,
    relation_block: i64,
) -> Result<(String, Uuid)> {
    let resource = Uuid::from_u128(seed);
    seed_identity_name(
        database,
        &format!("ens:{name}"),
        name,
        name,
        &format!("node:{name}"),
        resource,
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
        address,
        relation,
        relation_block,
    )
    .await?;
    Ok((
        bigname_storage::logical_name_id_for_name("ens", name),
        resource,
    ))
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
    let (held, held_resource) = seed_bounded_name(
        &database,
        "held-before.eth",
        0xb0a_1000,
        BOUNDED_ADDRESS,
        bigname_storage::AddressNameRelation::Registrant,
        210,
    )
    .await?;
    let (later, later_resource) = seed_bounded_name(
        &database,
        "held-later.eth",
        0xb0a_2000,
        BOUNDED_ADDRESS,
        bigname_storage::AddressNameRelation::TokenHolder,
        241,
    )
    .await?;
    let mut grant = v2_history_event(
        "bounded-held-grant",
        Some(&held),
        Some(held_resource),
        "RegistrationGranted",
        210,
    );
    grant.after_state["registrant"] = json!(BOUNDED_ADDRESS);
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
        ],
    )
    .await?;

    // Seeding a name moves the publication to its block; Project has published 240.
    move_bounded_publication_to(&database, 240).await?;

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

    // The route bound to the publication at 240 sees the same rows, although the relation row
    // for block 241 is already in place (Project swapped its projection before recording 241).
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

    publish_bounded_membership_at(&database, 241).await?;
    let published = v2_history_payload_for_database(
        &database,
        &format!("/v1/addresses/{BOUNDED_ADDRESS}/history?page_size=20"),
    )
    .await?;
    assert_eq!(
        bounded_route_hashes(&published),
        vec!["0xtx238", "0xtx230", "0xtx215", "0xtx210"],
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
    let (logical_name_id, resource) = seed_bounded_name(
        &database,
        NAME,
        0xb0a_3000,
        "0x00000000000000000000000000000000000b0a03",
        bigname_storage::AddressNameRelation::EffectiveController,
        205,
    )
    .await?;
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
            bounded_node_write("pointed-write-220", NAME, BOUNDED_RESOLVER, 220)?,
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
    // Seeding the name moves the head and publication to its block; publish 240 again.
    publish_bounded_membership_at(&database, 240).await?;
    let write = bounded_event_id(&database, "pointed-write-220").await?;
    // Project's row for 241 attributes the write through the new pointer.
    sqlx::query(
        "UPDATE bigname_phase.record_inventory_current
         SET provenance = provenance
             || jsonb_build_object('attributed_event_ids', jsonb_build_array($2::bigint))
         WHERE resource_id = $1",
    )
    .bind(resource)
    .bind(write)
    .execute(&database.pool)
    .await?;

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

    publish_bounded_membership_at(&database, 241).await?;
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
