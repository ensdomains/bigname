// History event data (TYR-77, TYR-78, TYR-79, TYR-80): confirmed ENSv1→ENSv2 migrations as their
// own rows, the name a primary-name event recorded, the registration action a grant row belongs
// to, and the name a resolver record write was made for at its position.

const EVENT_DATA_CHAIN: &str = "ethereum-mainnet";

async fn event_data_payload(database: &TestDatabase, uri: &str) -> Result<Value> {
    event_data_payload_in(database, &["ens"], uri).await
}

async fn event_data_payload_in(
    database: &TestDatabase,
    namespaces: &[&str],
    uri: &str,
) -> Result<Value> {
    let response = app_router(database.app_state_with_public_namespaces(namespaces))
        .oneshot(Request::builder().uri(uri).body(Body::empty()).expect("request must build"))
        .await
        .context("history event data request failed")?;
    let status = response.status();
    let payload = read_json(response).await?;
    assert_eq!(status, StatusCode::OK, "{uri}: unexpected response: {payload}");
    Ok(payload)
}

fn event_data_rows(payload: &Value) -> Vec<Value> {
    payload["data"].as_array().expect("data is an array").clone()
}

/// One normalized event at `(block, log)` in transaction `tx` on the mainnet fixture chain.
#[allow(clippy::too_many_arguments)]
fn event_data_event(
    identity: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    kind: &str,
    family: &str,
    block: i64,
    tx: &str,
    log: i64,
    emitter: &str,
    after: Value,
) -> NormalizedEvent {
    let mut event = history_event(
        identity,
        logical_name_id,
        resource_id,
        Some(EVENT_DATA_CHAIN),
        Some(block),
        Some(&format!("0xhistory{block}")),
        Some(tx),
        Some(log),
        CanonicalityState::Canonical,
    );
    event.event_kind = kind.to_owned();
    event.source_family = family.to_owned();
    event.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
    event.raw_fact_ref = json!({"kind": "raw_log", "emitting_address": emitter});
    event.before_state = json!({});
    event.after_state = after;
    event
}

async fn seed_event_data_name(
    database: &TestDatabase,
    name: &str,
    block: i64,
    resource: u128,
) -> Result<String> {
    seed_v2_history_blocks(database, block..=block).await?;
    let positions = align_phase_chain_positions(
        &database.pool,
        &json!({"ethereum": {"chain_id": EVENT_DATA_CHAIN, "block_number": block,
                             "block_hash": format!("0xhistory{block}"),
                             "timestamp": "2023-11-14T22:13:20Z"}}),
    )
    .await?;
    database.seed_snapshot_selector_chain_positions(&positions).await?;
    seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        EVENT_DATA_CHAIN,
        block,
        &format!("0xhistory{block}"),
        Uuid::from_u128(resource),
        Uuid::from_u128(resource + 0x1_0000),
        Uuid::from_u128(resource + 0x2_0000),
        "ens_v1",
    )
    .await?;
    Ok(bigname_storage::logical_name_id_for_name("ens", name))
}

async fn publish_event_data(database: &TestDatabase, events: &[NormalizedEvent], end: i64) -> Result<()> {
    bigname_storage::insert_normalized_event_fixtures(&database.pool, events).await?;
    let timestamp = sqlx::types::time::OffsetDateTime::from_unix_timestamp(1_700_000_000 + end)?;
    seed_schema_v2_ens_lookup_head(
        &database.pool,
        end,
        &format!("0xhistory{end}"),
        &crate::v2::format_timestamp(timestamp),
    )
    .await?;
    rebuild_fixture_families(&database.pool, EVENT_DATA_CHAIN, end, &format!("0xhistory{end}")).await
}

// TYR-77. A confirmed migration is its own `migration` row; a native ENSv2 registration and a
// migration whose correlation group never completed (a candidate) have none.
#[tokio::test]
async fn confirmed_migration_is_a_history_event() -> Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000077001";
    let database = TestDatabase::new_migrated().await?;
    let migrated = seed_event_data_name(&database, "migrated.eth", 300, 0x7701).await?;
    let native = seed_event_data_name(&database, "native.eth", 300, 0x7702).await?;
    let pending = seed_event_data_name(&database, "pending.eth", 300, 0x7703).await?;
    seed_v2_history_blocks(&database, 300..=304).await?;
    let grant = |identity: &str, logical: &str, resource: u128, block: i64| {
        event_data_event(
            identity,
            Some(logical),
            Some(Uuid::from_u128(resource)),
            "RegistrationGranted",
            "ens_v2_registry_l1",
            block,
            &format!("0xtx{block}"),
            5,
            REGISTRY,
            json!({"source_event": "LabelRegistered", "status": "registered",
                   "registrant": "0x00000000000000000000000000000000000000aa",
                   "expiry": 1_900_000_000_i64, "token_id": format!("0x{resource:064x}")}),
        )
    };
    // The migration producer's shape: the boundary sits on the pending `LabelRegistered` grant,
    // which a fresh registration has not given a resource yet, and names the successor binding's
    // resource in its payload. The registry's `TokenResource` copy two logs later carries it.
    let linked = |identity: &str, logical: &str, resource: u128, block: i64| {
        let mut event = grant(identity, logical, resource, block);
        event.event_identity =
            format!("{identity}:RegistrationGranted:linked:0x{resource:064x}:0");
        event.log_index = Some(7);
        event
    };
    let without_resource = |mut event: NormalizedEvent| {
        event.resource_id = None;
        event
    };
    let migration = |identity: &str, logical: &str, resource: u128, block: i64, path: &str| {
        let mut event = event_data_event(
            identity,
            Some(logical),
            None,
            "MigrationApplied",
            "ens_v2_migration_l1",
            block,
            &format!("0xtx{block}"),
            5,
            REGISTRY,
            json!({"source_event": "MigrationApplied", "migration_path": path,
                   "logical_name_id": logical, "stored_expiry": 1_900_000_000_i64,
                   "successor_binding": {"authority_epoch": "ens_v2",
                                         "resource_id": Uuid::from_u128(resource).to_string()}}),
        );
        event.derivation_kind = "ens_v2_migration".to_owned();
        event
    };
    publish_event_data(
        &database,
        &[
            without_resource(grant("migrated-grant", &migrated, 0x7701, 301)),
            linked("migrated-linked", &migrated, 0x7701, 301),
            migration(
                "ens_v2_migration:1:ethereum-mainnet:corr-migrated:MigrationApplied",
                &migrated,
                0x7701,
                301,
                "unwrapped",
            ),
            grant("native-grant", &native, 0x7702, 302),
            grant("pending-grant", &pending, 0x7703, 303),
            migration(
                "ens_v2_migration:1:ethereum-mainnet:corr-pending:MigrationApplied",
                &pending,
                0x7703,
                303,
                "locked_wrapped",
            ),
        ],
        304,
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events SET consumer_visibility = 'candidate',
                migration_correlation_ids = ARRAY['corr-pending']
         WHERE event_identity LIKE '%corr-pending%'",
    )
    .execute(&database.pool)
    .await?;

    let history =
        event_data_payload(&database, "/v1/names/migrated.eth/history?include=data").await?;
    let rows = event_data_rows(&history);
    let migrations = rows
        .iter()
        .filter(|row| row["type"] == json!("migration"))
        .collect::<Vec<_>>();
    assert_eq!(migrations.len(), 1, "one migration row: {rows:?}");
    assert_eq!(migrations[0]["name"], json!("migrated.eth"));
    assert_eq!(migrations[0]["block_number"], json!(301));
    assert_eq!(migrations[0]["transaction_hash"], json!("0xtx301"));
    assert_eq!(migrations[0]["log_index"], json!(5));
    assert_eq!(migrations[0]["data"], json!({"migration_path": "unwrapped"}));
    assert_eq!(history["page"]["total_count"], json!(rows.len()));
    // The migration row names the ENSv2 registration its successor binding holds, the one the
    // linked grant carries, although its own event has no resource.
    let registrations = rows
        .iter()
        .filter(|row| row["type"] == json!("registration"))
        .collect::<Vec<_>>();
    assert_eq!(registrations.len(), 2, "{registrations:?}");
    let registration_id = json!(Uuid::from_u128(0x7701).to_string());
    assert_eq!(migrations[0]["registration_id"], registration_id);
    assert!(
        registrations.iter().any(|row| row["registration_id"] == registration_id),
        "{registrations:?}"
    );
    let by_registration = event_data_rows(
        &event_data_payload(
            &database,
            &format!("/v1/events?registration_id={}&type=migration", Uuid::from_u128(0x7701)),
        )
        .await?,
    );
    assert_eq!(by_registration.len(), 1, "{by_registration:?}");

    for name in ["native.eth", "pending.eth"] {
        let rows = event_data_rows(
            &event_data_payload(&database, &format!("/v1/names/{name}/history")).await?,
        );
        assert!(
            rows.iter().all(|row| row["type"] != json!("migration")),
            "{name} has no confirmed migration: {rows:?}"
        );
    }

    let events = event_data_rows(&event_data_payload(&database, "/v1/events?type=migration").await?);
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["name"], json!("migrated.eth"));
    assert_eq!(events[0]["id"], migrations[0]["id"]);
    let raw = event_data_rows(
        &event_data_payload(&database, "/v1/events?type=migration&include=raw").await?,
    );
    assert_eq!(raw[0]["kind"], json!("MigrationApplied"));
    let registrations_only = event_data_rows(
        &event_data_payload(&database, "/v1/names/migrated.eth/history?type=registration").await?,
    );
    assert!(registrations_only.iter().all(|row| row["type"] == json!("registration")));

    // A retracted migration (its block left the canonical chain) is no longer served.
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned'
         WHERE event_identity LIKE '%corr-migrated%'",
    )
    .execute(&database.pool)
    .await?;
    let events = event_data_rows(&event_data_payload(&database, "/v1/events?type=migration").await?);
    assert!(events.is_empty(), "{events:?}");

    database.cleanup().await
}

// TYR-79. An ENSv2 registration stores a grant at the registry's `LabelRegistered` log and a
// linked copy at its `TokenResource` log in the same transaction; both carry one action id.
// A second registration in the same transaction has its own id, and a grant made when a label
// became reachable under a name is a separate action.
#[tokio::test]
async fn registration_rows_of_one_action_share_an_action_id() -> Result<()> {
    const REGISTRY: &str = "0x0000000000000000000000000000000000079001";
    const PARENT_REGISTRY: &str = "0x0000000000000000000000000000000000079002";
    let database = TestDatabase::new_migrated().await?;
    let alpha = seed_event_data_name(&database, "alpha-action.eth", 400, 0x7901).await?;
    let beta = seed_event_data_name(&database, "beta-action.eth", 400, 0x7902).await?;
    seed_v2_history_blocks(&database, 400..=403).await?;
    let token = |n: u128| format!("0x{n:064x}");
    let grant = |suffix: &str, logical: &str, resource: Option<u128>, block: i64, log: i64,
                 emitter: &str, token_id: &str, source_event: &str| {
        event_data_event(
            &format!("ens_v2_registry_resource_surface:1:ethereum-mainnet:0xhistory{block}:0xtx{block}:{log}:{suffix}:0"),
            Some(logical),
            resource.map(Uuid::from_u128),
            "RegistrationGranted",
            "ens_v2_registry_l1",
            block,
            &format!("0xtx{block}"),
            log,
            emitter,
            json!({"source_event": source_event, "status": "registered",
                   "registrant": "0x00000000000000000000000000000000000000aa",
                   "expiry": 1_900_000_000_i64, "token_id": token_id}),
        )
    };
    let (alpha_token, beta_token) = (token(0xa1), token(0xb2));
    publish_event_data(
        &database,
        &[
            grant("RegistrationGranted", &alpha, None, 401, 1, REGISTRY, &alpha_token, "LabelRegistered"),
            grant(&format!("RegistrationGranted:linked:{alpha_token}"), &alpha, Some(0x7901), 401, 3,
                  REGISTRY, &alpha_token, "LabelRegistered"),
            grant("RegistrationGranted", &beta, None, 401, 4, REGISTRY, &beta_token, "LabelRegistered"),
            grant(&format!("RegistrationGranted:linked:{beta_token}"), &beta, Some(0x7902), 401, 6,
                  REGISTRY, &beta_token, "LabelRegistered"),
            grant(&format!("RegistrationGranted:topology:{REGISTRY}:{alpha_token}"), &alpha,
                  Some(0x7901), 402, 2, PARENT_REGISTRY, &alpha_token, "SubregistryUpdated"),
        ],
        403,
    )
    .await?;

    let rows = event_data_rows(
        &event_data_payload(&database, "/v1/events?type=registration&include=data&order=asc").await?,
    );
    let action = |block: i64, log: i64| {
        let row = rows
            .iter()
            .find(|row| row["block_number"] == json!(block) && row["log_index"] == json!(log))
            .unwrap_or_else(|| panic!("row {block}/{log}: {rows:?}"));
        (row["data"]["action_id"].clone(), row["data"]["action_role"].clone())
    };
    let (alpha_grant, alpha_linked, beta_grant, beta_linked, alpha_reachable) =
        (action(401, 1), action(401, 3), action(401, 4), action(401, 6), action(402, 2));
    assert!(alpha_grant.0.as_str().is_some_and(|id| id.len() == 64), "{rows:?}");
    assert_eq!(alpha_grant.0, alpha_linked.0);
    assert_eq!(beta_grant.0, beta_linked.0);
    assert_ne!(alpha_grant.0, beta_grant.0, "two registrations in one transaction");
    assert_ne!(alpha_reachable.0, alpha_grant.0, "reachability is another action");
    assert_eq!(
        [alpha_grant.1, alpha_linked.1, beta_grant.1, beta_linked.1, alpha_reachable.1],
        [json!("registered"), json!("linked"), json!("registered"), json!("linked"), json!("reachable")]
    );

    // A page boundary between the rows of one action keeps the same id on both pages.
    let first = event_data_payload(
        &database,
        "/v1/events?type=registration&include=data&order=asc&page_size=1",
    )
    .await?;
    let cursor = first["page"]["next_cursor"].as_str().expect("second page").to_owned();
    let second = event_data_payload(
        &database,
        &format!("/v1/events?type=registration&include=data&order=asc&page_size=1&cursor={cursor}"),
    )
    .await?;
    assert_eq!(first["data"][0]["data"]["action_id"], second["data"][0]["data"]["action_id"]);
    assert_ne!(first["data"][0]["id"], second["data"][0]["id"]);

    // Name history and the events feed carry the same action id for the same row.
    let history = event_data_rows(
        &event_data_payload(
            &database,
            "/v1/names/alpha-action.eth/history?type=registration&include=data&order=asc",
        )
        .await?,
    );
    let ids = history
        .iter()
        .map(|row| row["data"]["action_id"].clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, [alpha_grant.0.clone(), alpha_grant.0.clone(), alpha_reachable.0]);

    database.cleanup().await
}

// TYR-78. A primary-name row returns the name its event recorded: the name the reverse registrar
// set on the reverse node in the same transaction, or the name a `NameForAddrChanged` carried.
#[tokio::test]
async fn primary_name_rows_return_the_recorded_name() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_address_names_fixture(&database).await?;
    publish_primary_claim(&database.pool, "ens", V2_ADDRESS, b"beta.eth").await?;
    publish_primary_claim(&database.pool, "ens", V2_ADDRESS, b"").await?;
    publish_primary_claim(&database.pool, "basenames", V2_ADDRESS, b"bob.base.eth").await?;
    publish_primary_claim(&database.pool, "basenames", V2_ADDRESS, b"").await?;
    // A claim with no name write in its transaction (`claim(owner)`) records no name.
    let (block, hash): (i64, String) = sqlx::query_as(
        "SELECT block_number, block_hash FROM bigname_phase.chain_lineage
         WHERE chain_id = 'ethereum-mainnet'
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY block_number DESC, block_hash LIMIT 1",
    )
    .fetch_one(&database.pool)
    .await?;
    let claims: Vec<Value> = sqlx::query_scalar(
        "SELECT after_state FROM normalized_events WHERE event_kind = 'ReverseChanged'
         AND namespace = 'ens' ORDER BY normalized_event_id LIMIT 1",
    )
    .fetch_all(&database.pool)
    .await?;
    let reverse_node = claims[0]["reverse_node"].as_str().expect("reverse node").to_owned();
    // A claim at a fresh log span of the block, and optionally a `NameChanged` on its reverse node
    // `gap` logs later from `resolver`.
    let claim_with_write = |label: &str, write: Option<(&str, i64)>| {
        let ordinal = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed) as i64 * 4 + 3;
        let tx = format!("0xclaim{label}");
        let mut claim = history_event(
            &format!("primary-claim-{label}"),
            None,
            None,
            Some(EVENT_DATA_CHAIN),
            Some(block),
            Some(&hash),
            Some(&tx),
            Some(ordinal),
            CanonicalityState::Canonical,
        );
        claim.event_kind = "ReverseChanged".to_owned();
        claim.source_family = "ens_v1_reverse_l1".to_owned();
        claim.derivation_kind = "ens_v1_reverse_claim".to_owned();
        claim.raw_fact_ref = json!({"kind": "raw_log", "emitting_address": ENS_REVERSE_REGISTRAR});
        claim.before_state = json!({});
        claim.after_state = claims[0].clone();
        let mut events = vec![claim];
        if let Some((resolver, gap)) = write {
            let mut name = history_event(
                &format!("primary-claim-{label}-write"),
                None,
                None,
                Some(EVENT_DATA_CHAIN),
                Some(block),
                Some(&hash),
                Some(&tx),
                Some(ordinal + gap),
                CanonicalityState::Canonical,
            );
            name.event_kind = "RecordChanged".to_owned();
            name.source_family = "ens_v1_resolver_l1".to_owned();
            name.derivation_kind = "ens_v1_unwrapped_authority".to_owned();
            name.raw_fact_ref = json!({"kind": "raw_log", "emitting_address": resolver});
            name.before_state = json!({});
            name.after_state = json!({"source_event": "NameChanged", "resolver": resolver,
                "node": reverse_node, "record_key": "name", "record_family": "name",
                "selector_key": null, "value_retained": false, "raw_name": "unrelated.eth"});
            events.push(name);
        }
        events
    };
    // `claim(owner)`: no name write in the transaction, so no name.
    let mut extra = claim_with_write("without-name", None);
    // The first name write after the claim is on a resolver the registry did not select for the
    // reverse node: a separate call, not the claim's.
    extra.extend(claim_with_write(
        "other-resolver",
        Some(("0x00000000000000000000000000000000000000b2", 1)),
    ));
    // A write on the claim's resolver beyond the registry's writes of the claim's own call.
    extra.extend(claim_with_write("later-call", Some((ENS_REVERSE_RESOLVER, 5))));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &extra).await?;

    let data = |payload: &Value| {
        event_data_rows(payload)
            .into_iter()
            .map(|row| row["data"].clone())
            .collect::<Vec<_>>()
    };
    let address = V2_ADDRESS.to_ascii_lowercase();
    let ens = data(
        &event_data_payload_in(
            &database,
            &["ens", "basenames"],
            "/v1/events?namespace=ens&type=primary_name&include=data&order=asc",
        )
        .await?,
    );
    assert_eq!(
        ens,
        [
            json!({"address": address, "coin_type": 60, "name": "alpha.eth", "name_status": "set"}),
            json!({"address": address, "coin_type": 60, "name": "beta.eth", "name_status": "set"}),
            json!({"address": address, "coin_type": 60, "name_status": "cleared"}),
            json!({"address": address, "coin_type": 60, "name_status": "unknown"}),
            json!({"address": address, "coin_type": 60, "name_status": "unknown"}),
            json!({"address": address, "coin_type": 60, "name_status": "unknown"}),
        ],
    );
    let basenames = data(
        &event_data_payload_in(
            &database,
            &["ens", "basenames"],
            "/v1/events?namespace=basenames&type=primary_name&include=data&order=asc",
        )
        .await?,
    );
    assert_eq!(
        basenames,
        [
            json!({"address": address, "coin_type": 2_147_492_101_u64, "name": "bob.base.eth",
                   "name_status": "set"}),
            json!({"address": address, "coin_type": 2_147_492_101_u64, "name_status": "cleared"}),
        ],
    );
    // The claim's companion record row names its reverse node, as the adapter stores it.
    let basenames_node: String = sqlx::query_scalar(
        "SELECT lower(after_state ->> 'reverse_node') FROM normalized_events
         WHERE namespace = 'basenames' AND event_kind = 'ReverseChanged' LIMIT 1",
    )
    .fetch_one(&database.pool)
    .await?;
    let records = data(
        &event_data_payload_in(
            &database,
            &["ens", "basenames"],
            "/v1/events?namespace=basenames&type=record&include=data&order=asc",
        )
        .await?,
    );
    assert_eq!(records.len(), 2, "{records:?}");
    for record in &records {
        assert_eq!(record["node"], json!(basenames_node), "{record}");
        assert!(record.get("resolver").is_some(), "{record}");
    }

    database.cleanup().await
}

// TYR-80. A record write names the name whose resolver pointer and, on a record-ID resolver,
// whose record link selected the written record at the write's position. A write nobody's links
// selected then, a record several names shared, the zero-node default record and an unknown node
// stay unnamed, and every write carries the resolver and its node or record ID.
#[tokio::test]
async fn record_events_name_the_name_linked_at_their_position() -> Result<()> {
    const RECORD_RESOLVER: &str = "0x0c47bc813361aeb3d0ad84f8f642bcce0e34b7f4";
    const NODE_RESOLVER: &str = "0x0000000000000000000000000000000000080a11";
    const DEFAULT_NODE: &str =
        "0x0000000000000000000000000000000000000000000000000000000000000000";
    let database = TestDatabase::new_migrated().await?;
    let mut names = std::collections::BTreeMap::new();
    for (index, name) in ["shaird.eth", "legal.eth", "agent-one.eth", "agent-two.eth", "mr-freshy.eth"]
        .into_iter()
        .enumerate()
    {
        let resource = 0x8001 + index as u128;
        let logical = seed_event_data_name(&database, name, 499, resource).await?;
        names.insert(name, (logical, Uuid::from_u128(resource)));
    }
    seed_v2_history_blocks(&database, 499..=513).await?;
    let node = |name: &str| bigname_lookup::ens_namehash_hex(name).expect("namehash");
    let mut events = Vec::new();
    let mut pointer = |name: &str, resolver: &str, block: i64, log: i64| {
        let (logical, resource) = &names[name];
        events.push(event_data_event(
            &format!("pointer:{name}:{block}:{log}"),
            Some(logical),
            Some(*resource),
            "ResolverChanged",
            "ens_v1_registry_l1",
            block,
            &format!("0xtx{block}"),
            log,
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e",
            json!({"source_event": "NewResolver", "node": node(name), "resolver": resolver}),
        ));
    };
    pointer("mr-freshy.eth", NODE_RESOLVER, 500, 0);
    pointer("legal.eth", RECORD_RESOLVER, 503, 1);
    pointer("shaird.eth", RECORD_RESOLVER, 505, 3);
    pointer("agent-one.eth", RECORD_RESOLVER, 506, 1);
    pointer("agent-two.eth", RECORD_RESOLVER, 506, 3);
    let link = |node: &str, record: u64, block: i64, log: i64| {
        event_data_event(
            &format!("link:{node}:{record}:{block}:{log}"),
            None,
            None,
            "ResolverRecordLinked",
            "ens_v2_resolver_l1",
            block,
            &format!("0xtx{block}"),
            log,
            RECORD_RESOLVER,
            json!({"source_event": "Linked", "storage_model": "resolver_record_id",
                   "resolver": RECORD_RESOLVER, "resolver_record_id": record.to_string(),
                   "node": node}),
        )
    };
    events.extend([
        link(&node("legal.eth"), 8, 503, 0),
        link(DEFAULT_NODE, 9, 503, 2),
        link(&node("shaird.eth"), 5, 505, 1),
        link(&node("agent-one.eth"), 8, 506, 0),
        link(&node("agent-two.eth"), 8, 506, 2),
        link(&node("shaird.eth"), 6, 508, 0),
    ]);
    let record_write = |record: u64, block: i64, log: i64| {
        event_data_event(
            &format!("record-write:{record}:{block}:{log}"),
            None,
            None,
            "RecordChanged",
            "ens_v2_resolver_l1",
            block,
            &format!("0xtx{block}"),
            log,
            RECORD_RESOLVER,
            json!({"source_event": "TextUpdated", "storage_model": "resolver_record_id",
                   "resolver": RECORD_RESOLVER, "resolver_record_id": record.to_string(),
                   "record_key": "text:description", "record_family": "text",
                   "selector_key": "description", "value_retained": true,
                   "value": format!("record {record} at {block}")}),
        )
    };
    let node_write = |node: &str, block: i64, log: i64| {
        event_data_event(
            &format!("node-write:{node}:{block}:{log}"),
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            block,
            &format!("0xtx{block}"),
            log,
            NODE_RESOLVER,
            json!({"source_event": "TextChanged", "resolver": NODE_RESOLVER, "node": node,
                   "record_key": "text:avatar", "record_family": "text",
                   "selector_key": "avatar", "value_retained": true, "value": "avatar"}),
        )
    };
    let unknown_node = "0x00000000000000000000000000000000000000000000000000000000000dead0";
    events.extend([
        node_write(&node("mr-freshy.eth"), 501, 0),
        node_write(unknown_node, 501, 1),
        record_write(8, 502, 0),
        record_write(8, 504, 0),
        record_write(9, 504, 1),
        record_write(5, 505, 4),
        record_write(8, 507, 0),
        record_write(5, 509, 0),
        record_write(6, 509, 1),
    ]);
    // mr-freshy.eth is wrapped at 510: the authority transition records the resolver it keeps on
    // the wrapper resource, and the owner then points the name at another resolver on that
    // resource. The registrar resource's pointer to the first resolver is never cleared, but the
    // registry no longer selects that resolver, so a later write to it is not mr-freshy.eth's.
    const OTHER_RESOLVER: &str = "0x0000000000000000000000000000000000080a12";
    let wrapper = Uuid::from_u128(0x8f01);
    sqlx::query(
        "INSERT INTO bigname_phase.resources (resource_id, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1, $2, '0xhistory510', 510, 'canonical')",
    )
    .bind(wrapper)
    .bind(EVENT_DATA_CHAIN)
    .execute(&database.pool)
    .await?;
    let freshy = names["mr-freshy.eth"].0.clone();
    let wrapped_pointer = |identity: &str, family: &str, resolver: &str, log: i64| {
        event_data_event(
            identity,
            Some(&freshy),
            Some(wrapper),
            "ResolverChanged",
            family,
            510,
            "0xtx510",
            log,
            "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e",
            json!({"source_event": "NewResolver", "node": node("mr-freshy.eth"),
                   "resolver": resolver}),
        )
    };
    events.extend([
        wrapped_pointer("wrap:mr-freshy.eth", "ens_v1_wrapper_l1", NODE_RESOLVER, 0),
        node_write(&node("mr-freshy.eth"), 510, 1),
        wrapped_pointer("repoint:mr-freshy.eth", "ens_v1_registry_l1", OTHER_RESOLVER, 2),
        node_write(&node("mr-freshy.eth"), 512, 0),
    ]);
    publish_event_data(&database, &events, 513).await?;

    let rows = event_data_rows(
        &event_data_payload(&database, "/v1/events?type=record&include=data&order=asc&page_size=50")
            .await?,
    );
    let at = |block: i64, log: i64| {
        rows.iter()
            .find(|row| row["block_number"] == json!(block) && row["log_index"] == json!(log))
            .unwrap_or_else(|| panic!("record row {block}/{log}: {rows:?}"))
            .clone()
    };
    let name_at = |block: i64, log: i64| at(block, log).get("name").cloned();
    assert_eq!(name_at(501, 0), Some(json!("mr-freshy.eth")), "pointer set earlier");
    assert_eq!(name_at(501, 1), None, "unknown node");
    assert_eq!(name_at(502, 0), None, "written before any name linked record 8");
    assert_eq!(name_at(504, 0), Some(json!("legal.eth")));
    assert_eq!(name_at(504, 1), None, "the zero-node default record");
    assert_eq!(name_at(505, 4), Some(json!("shaird.eth")), "link and pointer earlier in the transaction");
    assert_eq!(name_at(507, 0), None, "record 8 shared by three names");
    assert_eq!(name_at(509, 0), None, "record 5 after shaird.eth relinked away");
    assert_eq!(name_at(509, 1), Some(json!("shaird.eth")));
    assert_eq!(name_at(510, 1), Some(json!("mr-freshy.eth")), "wrapped, same resolver");
    assert_eq!(name_at(512, 0), None, "the registry selects another resolver since 510");

    let resolver = json!({"chain_id": 1, "address": RECORD_RESOLVER});
    assert_eq!(
        at(507, 0)["data"],
        json!({"key": "text:description", "value": "record 8 at 507", "resolver": resolver,
               "record_id": "8"})
    );
    assert_eq!(
        at(501, 1)["data"],
        json!({"key": "text:avatar", "value": "avatar",
               "resolver": {"chain_id": 1, "address": NODE_RESOLVER}, "node": unknown_node})
    );

    // A name-filtered read returns the name's inherited writes too; each row keeps the name it
    // was written for, so the pre-link and shared writes stay unnamed there as well.
    let legal = event_data_rows(
        &event_data_payload(&database, "/v1/events?name=legal.eth&type=record&order=asc").await?,
    );
    assert_eq!(
        legal
            .iter()
            .map(|row| (row["block_number"].clone(), row.get("name").cloned()))
            .collect::<Vec<_>>(),
        [
            (json!(502), None),
            (json!(504), Some(json!("legal.eth"))),
            (json!(507), None),
        ]
    );
    // Name history keeps the requested name on every row.
    let history = event_data_rows(
        &event_data_payload(&database, "/v1/names/legal.eth/history?type=record&order=asc").await?,
    );
    assert!(history.iter().all(|row| row["name"] == json!("legal.eth")), "{history:?}");
    assert_eq!(history.len(), 3);

    database.cleanup().await
}
