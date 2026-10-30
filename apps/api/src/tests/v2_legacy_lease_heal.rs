//! A `.eth` second-level ENSv1 name whose stored registration does not carry its BaseRegistrar
//! lease, read through name detail, the records route, lookup, address names and the name
//! listing (`v2::support::legacy_lease`). Each
//! case composes the registration from constructed retained inputs through Project's family
//! reducers, checks that composition, then reads the public routes at the publication.
use super::*;
use bigname_storage::{ChainPositions, NameCurrentRow, SelectedSnapshot, SnapshotConsistency};

const CHAIN: &str = "ethereum-mainnet";
const OWNER: &str = "0x00000000000000000000000000000000000000a5";
/// The block of the name's surface, its binding and its first current-registry record.
const HANDOFF: i64 = 120;
const GRANTED: i64 = 122;
const RENEWED: i64 = 124;
const RELEASED: i64 = 126;
/// The publication every request reads.
const HEAD: i64 = 130;
/// A block whose events were written after the publication the requests read.
const UNPUBLISHED: i64 = 135;
const FIRST_EXPIRY: i64 = 1_800_000_000;
const RENEWED_EXPIRY: i64 = 1_850_000_000;
const LATER_EXPIRY: i64 = 1_900_000_000;
/// A lease expiry between the grant's block and the publication's, so the lease is in grace there.
const LAPSED_EXPIRY: i64 = 1_700_000_125;
/// A NameWrapper expiry: the lease expiry plus the registrar grace.
const WRAPPED_EXPIRY: i64 = RENEWED_EXPIRY + GRACE_SECONDS;
const GRACE_SECONDS: i64 = 90 * 24 * 60 * 60;
const REGISTRAR: &str = "ens_v1_registrar_l1";
const REGISTRY: &str = "ens_v1_registry_l1";

fn hash(block: i64) -> String {
    format!("0xhistory{block}")
}

/// A block's time as served, from `seed_v2_history_blocks`.
fn seconds(block: i64) -> String {
    (1_700_000_000 + block).to_string()
}

fn event(
    logical: Option<&str>,
    resource: Uuid,
    kind: &str,
    family: &str,
    block: i64,
    log: i64,
    after: Value,
) -> NormalizedEvent {
    let mut event = history_event(
        &format!("legacy-lease-{resource}-{kind}-{block}-{log}"),
        logical,
        Some(resource),
        Some(CHAIN),
        Some(block),
        Some(hash(block).as_str()),
        Some(format!("0xtx{block}").as_str()),
        Some(log),
        CanonicalityState::Canonical,
    );
    event.event_kind = kind.into();
    event.source_family = family.into();
    event.before_state = json!({});
    event.after_state = after;
    event
}

/// A name bound to its node's registry record with no lease: the registry-only binding, its
/// registry-only epoch and the registry owner's write. With `handoff` that write is the current
/// registry's, the name's first current-registry record.
async fn seed_registry_only(
    database: &TestDatabase,
    name: &str,
    seed: u128,
    handoff: bool,
) -> Result<String> {
    let resource = Uuid::from_u128(seed);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        CHAIN,
        HANDOFF,
        &hash(HANDOFF),
        resource,
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
        "ens_v1",
    )
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = NULL WHERE resource_id = $1")
        .bind(resource)
        .execute(&database.pool)
        .await?;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let mut transfer = json!({"source_event": "Transfer", "node": node, "owner": OWNER,
        "owner_getter": OWNER, "registry_owner": OWNER});
    if handoff {
        transfer["emitter_role"] = json!("registry");
    }
    let epoch = json!({"source_event": "Transfer", "authority_kind": "registry_only",
        "authority_key": format!("registry:{CHAIN}:{node}"), "owner": OWNER});
    let logical_id = Some(logical.as_str());
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            event(
                logical_id,
                resource,
                "AuthorityTransferred",
                REGISTRY,
                HANDOFF,
                0,
                transfer,
            ),
            event(
                logical_id,
                resource,
                "AuthorityEpochChanged",
                REGISTRY,
                HANDOFF,
                1,
                epoch,
            ),
        ],
    )
    .await?;
    Ok(logical)
}

/// A name bound to its registrar lease whose grant the name's lifecycle does not hold: the
/// lease's renewal names the name, and its grant, at `grant_block` when given, names none.
async fn seed_undated_registrar(
    database: &TestDatabase,
    name: &str,
    seed: u128,
    grant_block: Option<i64>,
) -> Result<(String, Uuid)> {
    let lease = Uuid::from_u128(seed);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        CHAIN,
        HANDOFF,
        &hash(HANDOFF),
        lease,
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
        "ens_v1",
    )
    .await?;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let logical_id = Some(logical.as_str());
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            event(
                logical_id,
                lease,
                "AuthorityTransferred",
                REGISTRY,
                HANDOFF,
                0,
                json!({"source_event": "Transfer", "node": node, "owner": OWNER}),
            ),
            event(
                logical_id,
                lease,
                "AuthorityEpochChanged",
                REGISTRY,
                HANDOFF,
                1,
                json!({"authority_kind": "registrar",
                    "authority_key": format!("registrar:{CHAIN}:{lease}"),
                    "registry_owner": OWNER}),
            ),
            event(
                logical_id,
                lease,
                "RegistrationRenewed",
                REGISTRAR,
                RENEWED,
                0,
                json!({"authority_kind": "registrar", "expiry": RENEWED_EXPIRY}),
            ),
        ],
    )
    .await?;
    if let Some(block) = grant_block {
        insert_unnamed_grant(database, lease, block).await?;
    }
    Ok((logical, lease))
}

async fn insert_unnamed_grant(database: &TestDatabase, lease: Uuid, block: i64) -> Result<()> {
    let grant = event(
        None,
        lease,
        "RegistrationGranted",
        REGISTRAR,
        block,
        0,
        json!({"authority_kind": "registrar", "registrant": OWNER, "expiry": FIRST_EXPIRY}),
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;
    Ok(())
}

/// A lease's registrar events named for the name, `(kind, block, expiry)` in log order.
async fn seed_lease(
    database: &TestDatabase,
    logical: &str,
    lease: Uuid,
    events: &[(&str, i64, Option<i64>)],
) -> Result<()> {
    upsert_test_resources(
        &database.pool,
        &[Resource {
            resource_id: lease,
            token_lineage_id: None,
            chain_id: CHAIN.into(),
            block_hash: hash(HANDOFF),
            block_number: HANDOFF,
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    let events: Vec<NormalizedEvent> = events
        .iter()
        .enumerate()
        .map(|(log, (kind, block, expiry))| {
            let mut after = json!({"authority_kind": "registrar", "expiry": expiry});
            if *kind == "RegistrationGranted" {
                after["registrant"] = json!(OWNER);
            }
            event(
                Some(logical),
                lease,
                kind,
                REGISTRAR,
                *block,
                log as i64,
                after,
            )
        })
        .collect();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    Ok(())
}

/// A lease granted, then renewed 150 times with an `ExpiryChanged` beside each renewal: more
/// eligible events than the fill's events window holds.
fn long_lease() -> Vec<(&'static str, i64, Option<i64>)> {
    let mut events = vec![("RegistrationGranted", GRANTED, Some(FIRST_EXPIRY))];
    for renewal in 0..150 {
        let block = RENEWED + renewal / 50;
        let expiry = Some(FIRST_EXPIRY + 1 + renewal);
        events.push(("RegistrationRenewed", block, expiry));
        events.push(("ExpiryChanged", block, expiry));
    }
    events
}

/// A lease granted, then renewed with an `ExpiryChanged` beside the renewal.
fn renewed_lease() -> Vec<(&'static str, i64, Option<i64>)> {
    vec![
        ("RegistrationGranted", GRANTED, Some(FIRST_EXPIRY)),
        ("RegistrationRenewed", RENEWED, Some(RENEWED_EXPIRY)),
        ("ExpiryChanged", RENEWED, Some(RENEWED_EXPIRY)),
    ]
}

async fn publish(database: &TestDatabase) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum": {
            "chain_id": CHAIN,
            "block_number": HEAD,
            "block_hash": hash(HEAD),
            "timestamp": bigname_storage::UnixSeconds::from(timestamp(1_700_000_000 + HEAD))
                .internal_string(),
        }}))
        .await?;
    publish_test_families_on(&database.pool, CHAIN, HEAD).await
}

async fn new_database() -> Result<TestDatabase> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, HANDOFF..=HEAD).await?;
    Ok(database)
}

async fn composed(database: &TestDatabase, logical: &str) -> Result<NameCurrentRow> {
    bigname_storage::load_name_current(&database.pool, logical)
        .await?
        .context("the published name row")
}

/// The composition the heal fills: a registry-only registration with no lease resource and no
/// lease expiry, under the ENSv1 arm.
async fn assert_composed_unleased(database: &TestDatabase, logical: &str) -> Result<Value> {
    let row = composed(database, logical).await?;
    let registration = &row.declared_summary["registration"];
    assert_eq!(
        registration["authority_kind"], "registry_only",
        "{registration}"
    );
    assert_eq!(registration["resource_id"], Value::Null, "{registration}");
    assert_eq!(registration["ens_v1_expiry"], Value::Null, "{registration}");
    assert_eq!(
        row.provenance["authority_selection"]["authority_arm"], "ens_v1",
        "{}",
        row.provenance
    );
    Ok(row.provenance)
}

async fn detail(database: &TestDatabase, name: &str) -> Result<Value> {
    let mut payload = v2_get_json(database, &format!("/v1/names/{name}")).await?;
    Ok(payload["data"].take())
}

#[tokio::test]
async fn a_registry_only_lease_serves_its_identity_expiry_and_handoff_registration_time()
-> Result<()> {
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "migrated.eth", 0x7a10, true).await?;
    let lease = Uuid::from_u128(0x7a1f);
    seed_lease(&database, &logical, lease, &renewed_lease()).await?;
    publish(&database).await?;
    let provenance = assert_composed_unleased(&database, &logical).await?;
    assert_eq!(
        provenance["authority_selection"]["registry_handoff_block_number"],
        HANDOFF
    );

    let data = detail(&database, "migrated.eth").await?;
    assert_eq!(data["registration_id"], json!(lease.to_string()), "{data}");
    assert_eq!(data["registered_at"], json!(seconds(HANDOFF)), "{data}");
    assert_eq!(
        data["ens_v1"]["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{data}"
    );
    assert_eq!(
        data["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{data}"
    );
    assert_eq!(
        data["grace_ends_at"],
        json!((RENEWED_EXPIRY + GRACE_SECONDS).to_string()),
        "{data}"
    );
    assert_eq!(data["status"], "active", "{data}");
    assert_eq!(data["authority"], "ens_v1", "{data}");
    assert_eq!(data["owner"], OWNER, "{data}");

    // Lookup serves the same fill.
    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"id": "migrated", "name": "migrated.eth"}]}),
    )
    .await?;
    let record = &lookup["data"][0]["record"];
    assert_eq!(lookup["data"][0]["status"], "ok", "{lookup}");
    assert_eq!(
        record["registration_id"],
        json!(lease.to_string()),
        "{lookup}"
    );
    assert_eq!(record["registered_at"], json!(seconds(HANDOFF)), "{lookup}");
    assert_eq!(
        record["ens_v1"]["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{lookup}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn without_a_first_current_registry_record_the_lease_starts_at_its_first_event() -> Result<()>
{
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "migrated.eth", 0x7a20, false).await?;
    let lease = Uuid::from_u128(0x7a2f);
    seed_lease(&database, &logical, lease, &renewed_lease()).await?;
    publish(&database).await?;
    let provenance = assert_composed_unleased(&database, &logical).await?;
    assert!(
        provenance["authority_selection"]["registry_handoff_block_number"].is_null(),
        "{provenance}"
    );

    let data = detail(&database, "migrated.eth").await?;
    assert_eq!(data["registration_id"], json!(lease.to_string()), "{data}");
    assert_eq!(data["registered_at"], json!(seconds(GRANTED)), "{data}");
    assert_eq!(
        data["ens_v1"]["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{data}"
    );
    database.cleanup().await
}

#[tokio::test]
async fn an_undated_registrar_lease_is_dated_at_its_grant_block() -> Result<()> {
    let database = new_database().await?;
    let (logical, lease) =
        seed_undated_registrar(&database, "undated.eth", 0x7a30, Some(GRANTED)).await?;
    publish(&database).await?;
    let mut row = composed(&database, &logical).await?;
    let registration = row.declared_summary["registration"].take();
    assert_eq!(
        registration["authority_kind"], "registrar",
        "{registration}"
    );
    assert_eq!(
        registration["resource_id"],
        json!(lease.to_string()),
        "{registration}"
    );
    assert_eq!(registration["registered_at"], Value::Null, "{registration}");

    let data = detail(&database, "undated.eth").await?;
    assert_eq!(data["registered_at"], json!(seconds(GRANTED)), "{data}");
    assert_eq!(data["registration_id"], json!(lease.to_string()), "{data}");
    assert_eq!(data["status"], "active", "{data}");
    database.cleanup().await
}

/// A grant the snapshot does not reach, or one that is not activated, dates nothing. The dated
/// name beside them shows the fill runs on the same snapshot.
#[tokio::test]
async fn an_undated_registrar_lease_without_an_activated_grant_at_the_snapshot_stays_undated()
-> Result<()> {
    let database = new_database().await?;
    seed_undated_registrar(&database, "undated.eth", 0x7a40, Some(GRANTED)).await?;
    let (_, candidate) =
        seed_undated_registrar(&database, "candidate.eth", 0x7a50, Some(GRANTED)).await?;
    let (_, later) = seed_undated_registrar(&database, "later.eth", 0x7a60, None).await?;
    publish(&database).await?;
    sqlx::query(
        "UPDATE normalized_events SET consumer_visibility = 'candidate',
             migration_correlation_ids = ARRAY['legacy-lease-candidate']
         WHERE resource_id = $1 AND event_kind = 'RegistrationGranted'",
    )
    .bind(candidate)
    .execute(&database.pool)
    .await?;
    insert_unnamed_grant(&database, later, UNPUBLISHED).await?;

    let dated = detail(&database, "undated.eth").await?;
    assert_eq!(dated["registered_at"], json!(seconds(GRANTED)), "{dated}");
    for name in ["candidate.eth", "later.eth"] {
        let data = detail(&database, name).await?;
        assert!(data["registered_at"].is_null(), "{name}: {data}");
        assert_eq!(data["status"], "active", "{name}: {data}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn a_released_lease_serves_released_without_a_resolver_or_records() -> Result<()> {
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "lapsed.eth", 0x7a70, true).await?;
    let lease = Uuid::from_u128(0x7a7f);
    let mut events = renewed_lease();
    events.push(("RegistrationReleased", RELEASED, Some(RENEWED_EXPIRY)));
    seed_lease(&database, &logical, lease, &events).await?;
    publish(&database).await?;
    assert_composed_unleased(&database, &logical).await?;

    let data = detail(&database, "lapsed.eth").await?;
    assert_eq!(data["status"], "released", "{data}");
    assert_eq!(data["registration_id"], json!(lease.to_string()), "{data}");
    assert_eq!(data["registered_at"], json!(seconds(HANDOFF)), "{data}");
    assert_eq!(
        data["ens_v1"]["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{data}"
    );
    assert_eq!(
        data["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{data}"
    );
    assert!(data.get("lapsed_registration").is_none(), "{data}");
    assert!(data.get("resolver").is_none(), "{data}");
    assert!(data.get("records").is_none(), "{data}");
    // The fill leaves the composed control as it is.
    assert_eq!(data["owner"], OWNER, "{data}");

    let records = v2_get_json(
        &database,
        "/v1/names/lapsed.eth/records?source=indexed&keys=text:description&include=inventory",
    )
    .await?;
    assert_eq!(records["meta"]["source"], "indexed", "{records}");
    assert!(records["data"]["resolver"].is_null(), "{records}");
    assert!(records["data"].get("inventory").is_none(), "{records}");
    database.cleanup().await
}

/// Discovery is decided on the filled registration, so a released lease runs no verified read
/// and answers instead of failing the route check between discovery and the executed read.
#[tokio::test]
async fn a_released_lease_answers_a_verified_read_without_executing_it() -> Result<()> {
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "lapsed.eth", 0x7a80, true).await?;
    let mut events = renewed_lease();
    events.push(("RegistrationReleased", RELEASED, Some(RENEWED_EXPIRY)));
    seed_lease(&database, &logical, Uuid::from_u128(0x7a8f), &events).await?;
    publish(&database).await?;
    assert_composed_unleased(&database, &logical).await?;

    let records = v2_get_json(
        &database,
        "/v1/names/lapsed.eth/records?source=verified&keys=text:description",
    )
    .await?;
    assert_eq!(records["meta"]["source"], "verified", "{records}");
    assert!(records["data"]["resolver"].is_null(), "{records}");
    assert_eq!(
        records["data"]["records"]["text:description"]["status"], "unsupported",
        "{records}"
    );
    let detail = v2_get_json(&database, "/v1/names/lapsed.eth?source=verified").await?;
    assert_eq!(detail["meta"]["source"], "verified", "{detail}");
    assert_eq!(detail["data"]["status"], "released", "{detail}");
    database.cleanup().await
}

/// Registrar events above the publication the request reads are left out, as Interpret can
/// write them before Project publishes.
#[tokio::test]
async fn registrar_events_above_the_snapshot_are_left_out() -> Result<()> {
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "migrated.eth", 0x7a90, true).await?;
    let lease = Uuid::from_u128(0x7a9f);
    seed_lease(
        &database,
        &logical,
        lease,
        &[("RegistrationGranted", GRANTED, Some(FIRST_EXPIRY))],
    )
    .await?;
    publish(&database).await?;
    assert_composed_unleased(&database, &logical).await?;
    let renewal = [
        ("RegistrationRenewed", UNPUBLISHED, Some(LATER_EXPIRY)),
        ("ExpiryChanged", UNPUBLISHED, Some(LATER_EXPIRY)),
    ];
    let later: Vec<NormalizedEvent> = renewal
        .iter()
        .enumerate()
        .map(|(log, (kind, block, expiry))| {
            event(
                Some(logical.as_str()),
                lease,
                kind,
                REGISTRAR,
                *block,
                log as i64,
                json!({"authority_kind": "registrar", "expiry": expiry}),
            )
        })
        .collect();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &later).await?;

    let data = detail(&database, "migrated.eth").await?;
    assert_eq!(data["registration_id"], json!(lease.to_string()), "{data}");
    assert_eq!(
        data["ens_v1"]["expires_at"],
        json!(FIRST_EXPIRY.to_string()),
        "{data}"
    );
    assert_eq!(
        data["expires_at"],
        json!(FIRST_EXPIRY.to_string()),
        "{data}"
    );
    database.cleanup().await
}

/// A lease event Interpret wrote at candidate visibility is left out.
#[tokio::test]
async fn a_lease_event_at_candidate_visibility_is_left_out() -> Result<()> {
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "migrated.eth", 0x7ae0, true).await?;
    let lease = Uuid::from_u128(0x7aef);
    seed_lease(&database, &logical, lease, &renewed_lease()).await?;
    publish(&database).await?;
    assert_composed_unleased(&database, &logical).await?;
    sqlx::query(
        "UPDATE normalized_events SET consumer_visibility = 'candidate',
             migration_correlation_ids = ARRAY['legacy-lease-candidate']
         WHERE resource_id = $1 AND block_number = $2",
    )
    .bind(lease)
    .bind(RENEWED)
    .execute(&database.pool)
    .await?;

    let data = detail(&database, "migrated.eth").await?;
    assert_eq!(data["registration_id"], json!(lease.to_string()), "{data}");
    assert_eq!(
        data["ens_v1"]["expires_at"],
        json!(FIRST_EXPIRY.to_string()),
        "{data}"
    );
    database.cleanup().await
}

/// A registry-only name with no registrar events is served as composed. The filled name beside
/// it shows the fill runs on the same snapshot.
#[tokio::test]
async fn a_registry_only_name_without_registrar_events_is_served_as_composed() -> Result<()> {
    let database = new_database().await?;
    let filled = seed_registry_only(&database, "migrated.eth", 0x7aa0, true).await?;
    let lease = Uuid::from_u128(0x7aaf);
    seed_lease(&database, &filled, lease, &renewed_lease()).await?;
    let bare = seed_registry_only(&database, "unrenewed.eth", 0x7ab0, true).await?;
    publish(&database).await?;
    assert_composed_unleased(&database, &bare).await?;

    let data = detail(&database, "migrated.eth").await?;
    assert_eq!(data["registration_id"], json!(lease.to_string()), "{data}");
    let data = detail(&database, "unrenewed.eth").await?;
    assert!(data["registered_at"].is_null(), "{data}");
    assert!(data["ens_v1"]["expires_at"].is_null(), "{data}");
    assert_eq!(data["status"], "active", "{data}");
    database.cleanup().await
}

/// The routes always read a row whose publication and selected snapshot hold its chain, so the
/// fill's guard for a row without its chain is reached only by a direct call.
#[tokio::test]
async fn a_row_or_snapshot_without_the_rows_chain_is_left_as_composed() -> Result<()> {
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "migrated.eth", 0x7ac0, true).await?;
    seed_lease(
        &database,
        &logical,
        Uuid::from_u128(0x7acf),
        &renewed_lease(),
    )
    .await?;
    publish(&database).await?;
    let row = composed(&database, &logical).await?;
    let snapshot = SelectedSnapshot {
        chain_positions: ChainPositions::from_value(&row.chain_positions)?,
        consistency: SnapshotConsistency::Head,
    };
    let mut conn = database.pool.acquire().await?;

    let mut filled = row.clone();
    assert!(crate::v2::support::serve_legacy_lease(&mut conn, &mut filled, &snapshot).await?);
    assert_ne!(filled.declared_summary, row.declared_summary);

    let mut chainless = row.clone();
    chainless.chain_positions = json!({});
    assert!(!crate::v2::support::serve_legacy_lease(&mut conn, &mut chainless, &snapshot).await?);
    assert_eq!(chainless.declared_summary, row.declared_summary);

    let elsewhere = SelectedSnapshot {
        chain_positions: ChainPositions::new(BTreeMap::new()),
        consistency: SnapshotConsistency::Head,
    };
    let mut unread = row.clone();
    assert!(!crate::v2::support::serve_legacy_lease(&mut conn, &mut unread, &elsewhere).await?);
    assert_eq!(unread, row);
    drop(conn);
    database.cleanup().await
}

/// A failed fill read rolls back to its savepoint. The route serves the stored registration, and
/// the registry read after the fill runs on the same read transaction. Without the savepoint
/// the same failure leaves that transaction unusable.
#[tokio::test]
async fn a_failed_fill_read_serves_the_composed_registration_and_keeps_the_snapshot() -> Result<()>
{
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "faultyread.eth", 0x7ad0, true).await?;
    let lease = Uuid::from_u128(0x7adf);
    seed_lease(&database, &logical, lease, &renewed_lease()).await?;
    publish(&database).await?;
    let row = composed(&database, &logical).await?;
    let snapshot = SelectedSnapshot {
        chain_positions: ChainPositions::from_value(&row.chain_positions)?,
        consistency: SnapshotConsistency::Head,
    };
    let _fault = crate::v2::support::read_fault_test_hooks::install(&logical);

    let data = detail(&database, "faultyread.eth").await?;
    assert_eq!(
        data["registration_id"],
        json!(Uuid::from_u128(0x7ad0).to_string()),
        "{data}"
    );
    assert!(data["registered_at"].is_null(), "{data}");
    assert!(data["ens_v1"]["expires_at"].is_null(), "{data}");

    let mut reads = bigname_storage::begin_read_snapshot(&database.pool).await?;
    let served = crate::v2::support::load_served_name_for_selected_snapshot(
        &mut reads,
        "ens",
        "faultyread.eth",
        &snapshot,
    )
    .await
    .map_err(|error| anyhow::anyhow!("{}", error.message))?;
    assert_eq!(
        served.row.declared_summary["registration"],
        row.declared_summary["registration"]
    );
    bigname_storage::families::records::FamilyRecordInventory::load_ens_v1_registry_pointer(
        &mut reads,
        CHAIN,
        "ens",
        &row.namehash,
    )
    .await?;
    reads.commit().await?;

    let mut unprotected = bigname_storage::begin_read_snapshot(&database.pool).await?;
    let mut filled = row.clone();
    assert!(
        crate::v2::support::serve_legacy_lease(&mut unprotected, &mut filled, &snapshot)
            .await
            .is_err()
    );
    let aborted = sqlx::query("SELECT 1")
        .execute(&mut *unprotected)
        .await
        .expect_err("a failed statement aborts the read transaction");
    assert_eq!(
        aborted
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("25P02"),
        "{aborted}"
    );
    unprotected.rollback().await?;
    database.cleanup().await
}

/// The savepoint is taken only for a read. On a read transaction that a failed statement has
/// aborted, a row without its chain and a snapshot without the row's chain still return the
/// composed registration, so neither issued a statement. A shaped row's savepoint fails there,
/// and the fill returns that error. Its failure has ended the read transaction, as the plain
/// statement after it runs, so the route answers 500 rather than serving on a snapshot it no
/// longer holds.
#[tokio::test]
async fn a_savepoint_is_taken_only_for_a_read_and_a_failed_one_is_an_error() -> Result<()> {
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "migrated.eth", 0x7af0, true).await?;
    seed_lease(
        &database,
        &logical,
        Uuid::from_u128(0x7aff),
        &renewed_lease(),
    )
    .await?;
    publish(&database).await?;
    let row = composed(&database, &logical).await?;
    let snapshot = SelectedSnapshot {
        chain_positions: ChainPositions::from_value(&row.chain_positions)?,
        consistency: SnapshotConsistency::Head,
    };
    let elsewhere = SelectedSnapshot {
        chain_positions: ChainPositions::new(BTreeMap::new()),
        consistency: SnapshotConsistency::Head,
    };

    let mut reads = bigname_storage::begin_read_snapshot(&database.pool).await?;
    sqlx::query("SELECT 1 / 0")
        .execute(&mut *reads)
        .await
        .expect_err("a failed statement aborts the read transaction");

    let mut chainless = row.clone();
    chainless.chain_positions = json!({});
    assert!(
        !crate::v2::support::serve_legacy_lease_in_savepoint(&mut reads, &mut chainless, &snapshot)
            .await?
    );
    let mut unread = row.clone();
    assert!(
        !crate::v2::support::serve_legacy_lease_in_savepoint(&mut reads, &mut unread, &elsewhere)
            .await?
    );
    assert_eq!(unread, row);

    let mut shaped = row.clone();
    assert!(
        crate::v2::support::serve_legacy_lease_in_savepoint(&mut reads, &mut shaped, &snapshot)
            .await
            .is_err()
    );
    assert_eq!(shaped.declared_summary, row.declared_summary);
    sqlx::query("SELECT 1").execute(&mut *reads).await?;
    drop(reads);
    database.cleanup().await
}

/// A filled lease expiry that has passed at the snapshot, with its grace still running, serves
/// `expired` with the lease's expiry and grace end.
#[tokio::test]
async fn a_filled_lease_expiry_in_grace_at_the_snapshot_serves_expired() -> Result<()> {
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "migrated.eth", 0x7b10, true).await?;
    let lease = Uuid::from_u128(0x7b1f);
    seed_lease(
        &database,
        &logical,
        lease,
        &[("RegistrationGranted", GRANTED, Some(LAPSED_EXPIRY))],
    )
    .await?;
    publish(&database).await?;
    assert_composed_unleased(&database, &logical).await?;

    let data = detail(&database, "migrated.eth").await?;
    assert_eq!(data["registration_id"], json!(lease.to_string()), "{data}");
    assert_eq!(data["status"], json!("expired"), "{data}");
    assert_eq!(
        data["expires_at"],
        json!(LAPSED_EXPIRY.to_string()),
        "{data}"
    );
    assert_eq!(
        data["grace_ends_at"],
        json!((LAPSED_EXPIRY + GRACE_SECONDS).to_string()),
        "{data}"
    );
    database.cleanup().await
}

/// The registration time is the selected lease's first event, read on its own, however many
/// renewals follow it within the snapshot.
#[tokio::test]
async fn a_lease_with_more_events_than_the_history_window_starts_at_its_first_event() -> Result<()>
{
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "migrated.eth", 0x7b20, false).await?;
    let lease = Uuid::from_u128(0x7b2f);
    seed_lease(&database, &logical, lease, &long_lease()).await?;
    publish(&database).await?;
    assert_composed_unleased(&database, &logical).await?;

    let data = detail(&database, "migrated.eth").await?;
    assert_eq!(data["registration_id"], json!(lease.to_string()), "{data}");
    assert_eq!(data["registered_at"], json!(seconds(GRANTED)), "{data}");
    database.cleanup().await
}

/// The first-event statement on the long lease reads only the lease's own events. Beside the
/// name's other events, the table holds registrar events of many other leases, unnamed, below
/// and above the snapshot, so an index on the event kind, chain and block is unselective after
/// ANALYZE, as on a real chain. The row pins what bounds the read, not one plan: no sequential
/// scan, every index read keyed by the lease, and no more rows read than the lease's own events.
/// Whether the planner reads the lease's history index backward under the limit or reads the
/// lease's events and sorts them depends on its estimate of how many of them match, so the row
/// does not pin that choice. Both read at most the lease's events.
#[tokio::test]
async fn the_first_lease_event_reads_only_the_leases_own_events() -> Result<()> {
    let database = new_database().await?;
    let registry = Uuid::from_u128(0x7b30);
    let logical = seed_registry_only(&database, "migrated.eth", 0x7b30, false).await?;
    let lease = Uuid::from_u128(0x7b3f);
    let events = long_lease();
    seed_lease(&database, &logical, lease, &events).await?;
    publish(&database).await?;
    let name_noise: Vec<NormalizedEvent> = (0..4000)
        .map(|log| {
            event(
                Some(logical.as_str()),
                registry,
                "AuthorityTransferred",
                REGISTRY,
                HANDOFF + log % 10,
                1_000 + log,
                json!({"source_event": "Transfer", "owner": OWNER}),
            )
        })
        .collect();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &name_noise).await?;
    seed_v2_history_blocks(&database, HEAD + 1..=UNPUBLISHED).await?;
    sqlx::raw_sql(&format!(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
         SELECT md5('legacy-lease-plan-lease:' || n)::uuid, '{CHAIN}', '{block_hash}', {HEAD},
             'canonical'
         FROM generate_series(1, 2000) n;
         INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state)
         SELECT 'legacy-lease-plan-event:' || n || ':' || k, 'ens',
             md5('legacy-lease-plan-lease:' || n)::uuid,
             (ARRAY['RegistrationGranted', 'RegistrationRenewed', 'ExpiryChanged'])[k % 3 + 1],
             '{REGISTRAR}', 1, '{CHAIN}', {HANDOFF} + (n + k) % 16,
             '0xhistory' || ({HANDOFF} + (n + k) % 16), 'ens_v1_unwrapped_authority',
             'canonical'
         FROM generate_series(1, 2000) n, generate_series(0, 9) k;
         ANALYZE bigname_phase.normalized_events;",
        block_hash = hash(HEAD),
    ))
    .execute(&database.pool)
    .await?;

    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, FORMAT JSON) {}",
        crate::v2::support::FIRST_LEASE_EVENT_SQL
    ))
    .bind(lease)
    .bind(HEAD)
    .bind(&logical)
    .bind(CHAIN)
    .fetch_one(&database.pool)
    .await?;
    let nodes = plan_nodes(&plan);
    let mut rows_read = 0;
    for node in &nodes {
        let index = node["Index Name"].as_str().unwrap_or_default();
        let heap = node["Relation Name"] == json!("normalized_events");
        assert!(
            !(heap && node["Node Type"] == json!("Seq Scan")),
            "no sequential scan: {plan:#}"
        );
        if index.starts_with("normalized_events") {
            let condition = node["Index Cond"].as_str().unwrap_or_default();
            assert!(
                condition.contains("resource_id = ")
                    && (condition.contains(&lease.to_string()) || condition.contains("$1")),
                "{index} reads by the lease: {plan:#}"
            );
        }
        if heap {
            let count = |field: &str| node[field].as_u64().unwrap_or(0);
            rows_read += (count("Actual Rows")
                + count("Rows Removed by Filter")
                + count("Rows Removed by Index Recheck"))
                * count("Actual Loops").max(1);
        }
    }
    assert!(
        nodes
            .iter()
            .any(|node| node["Relation Name"] == json!("normalized_events")),
        "{plan:#}"
    );
    assert!(
        rows_read <= events.len() as u64,
        "{rows_read} rows read for {} lease events: {plan:#}",
        events.len()
    );
    assert_eq!(plan[0]["Plan"]["Actual Rows"], json!(1), "{plan:#}");
    database.cleanup().await
}

/// A wrapped name's renewal `ExpiryChanged`, as the registrar family carries it: on `resource`,
/// with the wrapper's authority kind and the NameWrapper expiry.
async fn insert_wrapper_renewal(
    database: &TestDatabase,
    logical: &str,
    resource: Uuid,
    block: i64,
) -> Result<()> {
    upsert_test_resources(
        &database.pool,
        &[Resource {
            resource_id: resource,
            token_lineage_id: None,
            chain_id: CHAIN.into(),
            block_hash: hash(HANDOFF),
            block_number: HANDOFF,
            provenance: json!({}),
            canonicality_state: CanonicalityState::Canonical,
        }],
    )
    .await?;
    let renewal = event(
        Some(logical),
        resource,
        "ExpiryChanged",
        REGISTRAR,
        block,
        900,
        json!({"source_event": "NameRenewed", "authority_kind": "wrapper",
            "expiry": WRAPPED_EXPIRY, "registrar_expiry": RENEWED_EXPIRY}),
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[renewal]).await?;
    Ok(())
}

/// A name once wrapped and now registry-only serves its BaseRegistrar lease, not the wrapper
/// whose renewal is the newest event the registrar family carries for it.
#[tokio::test]
async fn a_later_wrapper_renewal_does_not_become_the_lease() -> Result<()> {
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "migrated.eth", 0x7b50, false).await?;
    let lease = Uuid::from_u128(0x7b5f);
    seed_lease(&database, &logical, lease, &renewed_lease()).await?;
    insert_wrapper_renewal(&database, &logical, Uuid::from_u128(0x7b5e), RENEWED + 1).await?;
    publish(&database).await?;
    assert_composed_unleased(&database, &logical).await?;

    let data = detail(&database, "migrated.eth").await?;
    assert_eq!(data["registration_id"], json!(lease.to_string()), "{data}");
    assert_eq!(
        data["ens_v1"]["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{data}"
    );
    assert_eq!(
        data["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{data}"
    );
    assert_eq!(data["registered_at"], json!(seconds(GRANTED)), "{data}");
    database.cleanup().await
}

/// A wrapper renewal on the lease's own resource before its grant does not start the lease.
#[tokio::test]
async fn a_wrapper_renewal_on_the_lease_does_not_start_it() -> Result<()> {
    let database = new_database().await?;
    let logical = seed_registry_only(&database, "migrated.eth", 0x7b60, false).await?;
    let lease = Uuid::from_u128(0x7b6f);
    seed_lease(&database, &logical, lease, &renewed_lease()).await?;
    insert_wrapper_renewal(&database, &logical, lease, HANDOFF + 1).await?;
    publish(&database).await?;
    assert_composed_unleased(&database, &logical).await?;

    let data = detail(&database, "migrated.eth").await?;
    assert_eq!(data["registration_id"], json!(lease.to_string()), "{data}");
    assert_eq!(data["registered_at"], json!(seconds(GRANTED)), "{data}");
    database.cleanup().await
}

/// The ownerless fixture with a named BaseRegistrar release of its lease, which the stored
/// registration does not carry and the fill reads.
async fn released_ownerless() -> Result<TestDatabase> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_state_inputs(&database, AliceInputState::Ownerless).await?;
    append_alice_name_input(
        &database,
        "RegistrationReleased",
        REGISTRAR,
        json!({"authority_kind": "registrar", "expiry": 1_700_000_000}),
    )
    .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 21_000_003, "0xbinding").await?;
    Ok(database)
}

/// The ownerless fixture's name detail and indexed records read.
async fn ownerless_reads(database: &TestDatabase) -> Result<(Value, Value)> {
    let detail = v2_get_json(database, "/v1/names/alice.eth").await?;
    let records = v2_get_json(
        database,
        "/v1/names/alice.eth/records?source=indexed&keys=text:description",
    )
    .await?;
    Ok((detail["data"].clone(), records["data"].clone()))
}

/// An ownerless name served through its retained registry resolver pointer serves nothing once
/// the lease the fill reads is released.
#[tokio::test]
async fn an_ownerless_name_with_a_released_filled_lease_serves_nothing() -> Result<()> {
    let database = released_ownerless().await?;
    let logical = bigname_storage::logical_name_id_for_name("ens", "alice.eth");
    let stored = composed(&database, &logical).await?;
    let registration = &stored.declared_summary["registration"];
    assert!(registration["released_at"].is_null(), "{registration}");
    assert!(registration["authority_kind"].is_null(), "{registration}");

    let (detail, records) = ownerless_reads(&database).await?;
    assert_eq!(detail["status"], json!("released"), "{detail}");
    assert!(detail["resolver"].is_null(), "{detail}");
    assert!(detail["records"].is_null(), "{detail}");
    assert!(records["resolver"].is_null(), "{records}");
    assert_ne!(
        records["records"]["text:description"]["status"],
        json!("ok"),
        "{records}"
    );
    database.cleanup().await
}

/// The guard: an ownerless name with no release still serves through its retained pointer.
#[tokio::test]
async fn an_ownerless_name_without_a_release_serves_through_its_retained_pointer() -> Result<()> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_state_inputs(&database, AliceInputState::Ownerless).await?;

    let (detail, records) = ownerless_reads(&database).await?;
    let resolver = json!("0x0000000000000000000000000000000000000abc");
    assert_eq!(detail["resolver"]["address"], resolver, "{detail}");
    assert_eq!(records["resolver"]["address"], resolver, "{records}");
    assert_eq!(
        records["records"]["text:description"],
        json!({"status": "ok", "value": "Alice profile"}),
        "{records}"
    );
    database.cleanup().await
}

/// The alias walk is decided on the stored composed row, before the fill reads the release. That
/// row carries no release, so its retained registry pointer still counts as a current
/// registration, and the walk is not taken, with or without the gate. The name is served under
/// its own path, and the fill then reads its released lease.
#[tokio::test]
async fn a_released_ownerless_name_is_served_without_an_alias_walk() -> Result<()> {
    let database = released_ownerless().await?;
    let (status, body, walked) = crate::v2::support::ALIAS_WALK_STATEMENTS
        .scope(std::cell::Cell::new(0), async {
            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri("/v1/names/alice.eth")
                        .body(Body::empty())?,
                )
                .await?;
            let status = response.status();
            let body: Value = read_json(response).await?;
            Ok::<_, anyhow::Error>((
                status,
                body,
                crate::v2::support::ALIAS_WALK_STATEMENTS.with(std::cell::Cell::get),
            ))
        })
        .await?;
    assert_eq!(status, StatusCode::OK, "{body:#}");
    assert_eq!(walked, 0, "{body:#}");
    let data = &body["data"];
    assert_eq!(data["name"], json!("alice.eth"), "{data}");
    assert!(data.get("canonical_name").is_none(), "{data}");
    assert_eq!(
        data["registration_id"],
        json!(Uuid::from_u128(0x2200).to_string()),
        "{data}"
    );
    database.cleanup().await
}

/// One name of each input class, published together, with E `sub.migrated.eth`: a subname with
/// its own registrar events, of no shape.
struct InputClasses {
    /// A: a renewed lease the stored registry-only registration does not carry.
    migrated: (String, Uuid),
    /// B: that shape with a release as the lease's latest event.
    lapsed: (String, Uuid),
    /// C: a registrar lease with no registration time.
    undated: (String, Uuid),
    /// D: a dated registrar lease, of no shape.
    plain: (String, Uuid),
}

const MIGRATED_SEED: u128 = 0x7c10;

async fn seed_input_classes(database: &TestDatabase) -> Result<InputClasses> {
    let migrated = seed_registry_only(database, "migrated.eth", MIGRATED_SEED, true).await?;
    let migrated_lease = Uuid::from_u128(0x7c1f);
    seed_lease(database, &migrated, migrated_lease, &renewed_lease()).await?;

    let lapsed = seed_registry_only(database, "lapsed.eth", 0x7c20, true).await?;
    let lapsed_lease = Uuid::from_u128(0x7c2f);
    let mut released = renewed_lease();
    released.push(("RegistrationReleased", RELEASED, Some(RENEWED_EXPIRY)));
    seed_lease(database, &lapsed, lapsed_lease, &released).await?;

    let undated = seed_undated_registrar(database, "undated.eth", 0x7c30, Some(GRANTED)).await?;

    let plain = seed_undated_registrar(database, "plain.eth", 0x7c40, None).await?;
    let grant = event(
        Some(&plain.0),
        plain.1,
        "RegistrationGranted",
        REGISTRAR,
        GRANTED,
        0,
        json!({"authority_kind": "registrar", "registrant": OWNER, "expiry": FIRST_EXPIRY}),
    );
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[grant]).await?;

    let sub = seed_registry_only(database, "sub.migrated.eth", 0x7c50, true).await?;
    seed_lease(database, &sub, Uuid::from_u128(0x7c5f), &renewed_lease()).await?;
    publish(database).await?;

    assert_composed_unleased(database, &migrated).await?;
    assert_composed_unleased(database, &lapsed).await?;
    for (logical, shaped) in [(&undated.0, true), (&plain.0, false), (&sub, false)] {
        let row = composed(database, logical).await?;
        assert_eq!(
            crate::v2::support::lease_row_shape(&row).is_some(),
            shaped,
            "{logical}: {}",
            row.declared_summary
        );
    }
    Ok(InputClasses {
        migrated: (migrated, migrated_lease),
        lapsed: (lapsed, lapsed_lease),
        undated,
        plain,
    })
}

/// `read` with the lease savepoints it took. The fill issues every statement inside one, so no
/// savepoint means no statement.
async fn with_savepoints<T>(
    read: impl std::future::Future<Output = Result<T>>,
) -> Result<(T, usize)> {
    crate::v2::support::LEASE_SAVEPOINTS
        .scope(std::cell::Cell::new(0), async {
            let value = read.await?;
            let count = crate::v2::support::LEASE_SAVEPOINTS.with(std::cell::Cell::get);
            Ok((value, count))
        })
        .await
}

async fn lookup_records(database: &TestDatabase, profile: &str, names: &[&str]) -> Result<Value> {
    let inputs: Vec<Value> = names
        .iter()
        .map(|name| json!({"id": name, "name": name}))
        .collect();
    v2_lookup_json(database, json!({"profile": profile, "inputs": inputs})).await
}

/// The manager's address-names read.
fn owned_names_uri(page_size: u32, cursor: Option<&str>) -> String {
    let mut uri = format!(
        "/v1/addresses/{OWNER}/names?namespace=ens&relation=any&authority=ens_v1,ens_v0\
         &sort=name&order=asc&page_size={page_size}"
    );
    if let Some(cursor) = cursor {
        uri.push_str(&format!("&cursor={cursor}"));
    }
    uri
}

/// The same read on `relation=manager` alone.
fn managed_names_uri(page_size: u32, cursor: Option<&str>) -> String {
    let mut uri = format!(
        "/v1/addresses/{OWNER}/names?namespace=ens&relation=manager&authority=ens_v1,ens_v0\
         &sort=name&order=asc&page_size={page_size}"
    );
    if let Some(cursor) = cursor {
        uri.push_str(&format!("&cursor={cursor}"));
    }
    uri
}

fn listed_row<'a>(payload: &'a Value, name: &str) -> &'a Value {
    payload["data"]
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["name"] == name))
        .unwrap_or_else(|| panic!("{name} is listed: {payload:#}"))
}

fn listed_names(payload: &Value) -> Vec<String> {
    payload["data"]
        .as_array()
        .expect("data is an array")
        .iter()
        .map(|row| row["name"].as_str().expect("a row has a name").to_owned())
        .collect()
}

/// Rows A and B as every filling route serves them: the lease's expiry, its handoff
/// registration time and the status at the snapshot.
fn assert_filled_lease(row: &Value, released: bool) {
    assert_eq!(
        row["ens_v1"]["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{row}"
    );
    assert_eq!(
        row["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{row}"
    );
    assert_eq!(
        row["grace_ends_at"],
        json!((RENEWED_EXPIRY + GRACE_SECONDS).to_string()),
        "{row}"
    );
    let status = if released { "released" } else { "active" };
    assert_eq!(row["status"], status, "{row}");
}

/// Rows D and E as stored: the dated lease keeps its grant time, and the subname has no lease.
fn assert_stored(row: &Value, name: &str) {
    if name == "plain.eth" {
        assert_eq!(
            row["ens_v1"]["expires_at"],
            json!(RENEWED_EXPIRY.to_string()),
            "{row}"
        );
        assert_eq!(row["status"], "active", "{row}");
    } else {
        assert!(row["ens_v1"]["expires_at"].is_null(), "{row}");
        assert!(row.get("registered_at").is_none_or(Value::is_null), "{row}");
    }
}

/// Lookup `profile=detail` and `profile=feed` serve rows A to C filled and rows D and E as
/// stored, in input order, with one savepoint per filled row and none for the others.
#[tokio::test]
async fn lookup_fills_each_input_class_in_one_batch() -> Result<()> {
    let database = new_database().await?;
    let classes = seed_input_classes(&database).await?;
    let names = [
        "plain.eth",
        "migrated.eth",
        "sub.migrated.eth",
        "lapsed.eth",
        "undated.eth",
    ];

    let (detail, savepoints) = with_savepoints(lookup_records(&database, "detail", &names)).await?;
    assert_eq!(savepoints, 3, "{detail:#}");
    for (index, name) in names.iter().enumerate() {
        assert_eq!(detail["data"][index]["record"]["name"], *name, "{detail:#}");
        assert_eq!(detail["data"][index]["status"], "ok", "{detail:#}");
    }
    let record = |index: usize| &detail["data"][index]["record"];
    assert_eq!(
        record(1)["registration_id"],
        json!(classes.migrated.1.to_string()),
        "{detail:#}"
    );
    assert_eq!(
        record(1)["registered_at"],
        json!(seconds(HANDOFF)),
        "{detail:#}"
    );
    assert_filled_lease(record(1), false);
    assert_eq!(
        record(3)["registration_id"],
        json!(classes.lapsed.1.to_string()),
        "{detail:#}"
    );
    assert_eq!(
        record(3)["registered_at"],
        json!(seconds(HANDOFF)),
        "{detail:#}"
    );
    assert_filled_lease(record(3), true);
    assert_eq!(
        record(4)["registration_id"],
        json!(classes.undated.1.to_string()),
        "{detail:#}"
    );
    assert_eq!(
        record(4)["registered_at"],
        json!(seconds(GRANTED)),
        "{detail:#}"
    );
    assert_eq!(
        record(0)["registration_id"],
        json!(classes.plain.1.to_string()),
        "{detail:#}"
    );
    assert_eq!(
        record(0)["registered_at"],
        json!(seconds(GRANTED)),
        "{detail:#}"
    );
    assert_stored(record(0), "plain.eth");
    assert_stored(record(2), "sub.migrated.eth");

    let (feed, savepoints) = with_savepoints(lookup_records(&database, "feed", &names)).await?;
    assert_eq!(savepoints, 3, "{feed:#}");
    let record = |index: usize| &feed["data"][index]["record"];
    assert_filled_lease(record(1), false);
    assert_filled_lease(record(3), true);
    // The undated fill writes only `registered_at` and the registration identity, and feed
    // carries neither, so row C's feed record matches its detail record's feed fields. The
    // savepoint count shows the fill read it.
    let undated = record(4);
    assert!(
        undated.get("registered_at").is_none_or(Value::is_null),
        "{feed:#}"
    );
    assert!(
        undated.get("registration_id").is_none_or(Value::is_null),
        "{feed:#}"
    );
    for field in ["status", "expires_at", "grace_ends_at", "ens_v1"] {
        assert_eq!(
            undated[field], detail["data"][4]["record"][field],
            "{field}: {feed:#}"
        );
    }
    assert_stored(record(0), "plain.eth");
    assert_stored(record(2), "sub.migrated.eth");

    // A batch of only unshaped rows takes no savepoint.
    let unshaped = lookup_records(&database, "detail", &["plain.eth", "sub.migrated.eth"]);
    let (_, savepoints) = with_savepoints(unshaped).await?;
    assert_eq!(savepoints, 0);
    database.cleanup().await
}

/// A failed fill read serves that lookup row as composed and fills the others.
#[tokio::test]
async fn lookup_serves_a_failed_fill_read_as_composed_beside_a_filled_row() -> Result<()> {
    let database = new_database().await?;
    let classes = seed_input_classes(&database).await?;
    let _fault = crate::v2::support::read_fault_test_hooks::install(&classes.migrated.0);

    for profile in ["detail", "feed"] {
        let lookup = lookup_records(&database, profile, &["migrated.eth", "lapsed.eth"]).await?;
        let composed = &lookup["data"][0]["record"];
        assert_eq!(lookup["data"][0]["status"], "ok", "{lookup:#}");
        assert!(composed["ens_v1"]["expires_at"].is_null(), "{lookup:#}");
        assert!(composed["expires_at"].is_null(), "{lookup:#}");
        if profile == "detail" {
            assert_eq!(
                composed["registration_id"],
                json!(Uuid::from_u128(MIGRATED_SEED).to_string()),
                "{lookup:#}"
            );
        }
        assert_filled_lease(&lookup["data"][1]["record"], true);
    }
    database.cleanup().await
}

/// The manager's address-names read serves rows A to C filled and rows D and E as stored. Walked
/// one row a page, the page order is the full page's, and only a filled row takes a savepoint.
#[tokio::test]
async fn address_names_fill_each_input_class_on_the_managers_read() -> Result<()> {
    let database = new_database().await?;
    seed_input_classes(&database).await?;

    let (page, savepoints) =
        with_savepoints(v2_get_json(&database, &owned_names_uri(200, None))).await?;
    assert_eq!(savepoints, 3, "{page:#}");
    let migrated = listed_row(&page, "migrated.eth");
    assert_filled_lease(migrated, false);
    assert_eq!(
        migrated["registered_at"],
        json!(seconds(HANDOFF)),
        "{page:#}"
    );
    let lapsed = listed_row(&page, "lapsed.eth");
    assert_filled_lease(lapsed, true);
    assert_eq!(lapsed["registered_at"], json!(seconds(HANDOFF)), "{page:#}");
    let undated = listed_row(&page, "undated.eth");
    assert_eq!(
        undated["registered_at"],
        json!(seconds(GRANTED)),
        "{page:#}"
    );
    let plain = listed_row(&page, "plain.eth");
    assert_eq!(plain["registered_at"], json!(seconds(GRANTED)), "{page:#}");
    assert_stored(plain, "plain.eth");
    assert_stored(listed_row(&page, "sub.migrated.eth"), "sub.migrated.eth");

    let full = listed_names(&page);
    let mut walked = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let uri = owned_names_uri(1, cursor.as_deref());
        let (step, savepoints) = with_savepoints(v2_get_json(&database, &uri)).await?;
        let names = listed_names(&step);
        assert_eq!(names.len(), 1, "{step:#}");
        let shaped = matches!(
            names[0].as_str(),
            "migrated.eth" | "lapsed.eth" | "undated.eth"
        );
        assert_eq!(savepoints, usize::from(shaped), "{step:#}");
        assert_eq!(&step["data"][0], listed_row(&page, &names[0]), "{step:#}");
        walked.extend(names);
        match step["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }
    assert_eq!(walked, full);
    database.cleanup().await
}

/// A failed fill read serves that address-names row as composed and fills the others.
#[tokio::test]
async fn address_names_serve_a_failed_fill_read_as_composed_beside_a_filled_row() -> Result<()> {
    let database = new_database().await?;
    let classes = seed_input_classes(&database).await?;
    let _fault = crate::v2::support::read_fault_test_hooks::install(&classes.migrated.0);

    let page = v2_get_json(&database, &owned_names_uri(200, None)).await?;
    let composed = listed_row(&page, "migrated.eth");
    assert!(composed["ens_v1"]["expires_at"].is_null(), "{page:#}");
    assert!(composed["expires_at"].is_null(), "{page:#}");
    assert!(
        composed.get("registered_at").is_none_or(Value::is_null),
        "{page:#}"
    );
    assert_filled_lease(listed_row(&page, "lapsed.eth"), true);
    database.cleanup().await
}

/// `GET /v1/names` lists by stored deadlines, so a filled row is one whose stored registration
/// has an expiry: C, dated at its grant, beside D as stored. Walked one row a page, the order is
/// the full page's, and only the filled row takes a savepoint.
#[tokio::test]
async fn the_name_listing_dates_an_undated_lease_and_leaves_a_dated_one() -> Result<()> {
    let database = new_database().await?;
    seed_input_classes(&database).await?;
    let uri = |page_size: u32, cursor: Option<&str>| {
        let mut uri = format!(
            "/v1/names?namespace=ens&expires_after=2020-01-01T00:00:00Z&page_size={page_size}"
        );
        if let Some(cursor) = cursor {
            uri.push_str(&format!("&cursor={cursor}"));
        }
        uri
    };

    let (page, savepoints) = with_savepoints(v2_get_json(&database, &uri(200, None))).await?;
    let full = listed_names(&page);
    assert!(full.contains(&"undated.eth".to_owned()), "{page:#}");
    assert!(full.contains(&"plain.eth".to_owned()), "{page:#}");
    assert!(!full.contains(&"migrated.eth".to_owned()), "{page:#}");
    assert_eq!(savepoints, 1, "{page:#}");
    let undated = listed_row(&page, "undated.eth");
    assert_eq!(
        undated["registered_at"],
        json!(seconds(GRANTED)),
        "{page:#}"
    );
    assert_eq!(
        undated["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{page:#}"
    );
    let plain = listed_row(&page, "plain.eth");
    assert_eq!(plain["registered_at"], json!(seconds(GRANTED)), "{page:#}");
    assert_stored(plain, "plain.eth");

    let mut walked = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let step_uri = uri(1, cursor.as_deref());
        let (step, savepoints) = with_savepoints(v2_get_json(&database, &step_uri)).await?;
        let names = listed_names(&step);
        assert_eq!(names.len(), 1, "{step:#}");
        assert_eq!(
            savepoints,
            usize::from(names[0] == "undated.eth"),
            "{step:#}"
        );
        assert_eq!(&step["data"][0], listed_row(&page, &names[0]), "{step:#}");
        walked.extend(names);
        match step["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }
    assert_eq!(walked, full);
    database.cleanup().await
}

/// An ENSv2 reservation of the name expiring at `LATER_EXPIRY`, as `v2_ens_v1_object.rs` seeds
/// one on a chain whose root registry is admitted: a stored term the lease does not supply.
async fn seed_reservation(database: &TestDatabase, logical: &str, seed: u128) -> Result<()> {
    let mut reservation = history_event(
        &format!("legacy-lease-reservation-{seed:x}"),
        Some(logical),
        None,
        Some(CHAIN),
        Some(RENEWED),
        Some(hash(RENEWED).as_str()),
        Some(format!("0xtx{RENEWED}").as_str()),
        Some(5),
        CanonicalityState::Canonical,
    );
    reservation.event_kind = "RegistrationReserved".into();
    reservation.source_family = "ens_v2_registry_l1".into();
    reservation.before_state = json!({});
    reservation.after_state = json!({"source_event": "LabelReserved", "status": "reserved",
        "expiry": LATER_EXPIRY, "token_id": "4294967297", "current_token_id": "4294967297",
        "registry_contract_instance_id": Uuid::from_u128(seed + 0xa).to_string()});
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &[reservation]).await?;
    Ok(())
}

/// `GET /v1/names` lists a registry-only row only by a stored deadline, here an ENSv2
/// reservation's. The fill supplies the released lease's expiry and registration time, and the
/// live stored term keeps the stored expiry and status: the row serves them as the reserved row
/// with no registrar events beside it does, not released.
#[tokio::test]
async fn the_name_listing_keeps_a_live_stored_term_over_a_released_filled_lease() -> Result<()> {
    let database = new_database().await?;
    let reserved = seed_registry_only(&database, "reserved.eth", 0x7d10, true).await?;
    let mut released = renewed_lease();
    released.push(("RegistrationReleased", RELEASED, Some(RENEWED_EXPIRY)));
    seed_lease(&database, &reserved, Uuid::from_u128(0x7d1f), &released).await?;
    let unleased = seed_registry_only(&database, "unleased.eth", 0x7d20, true).await?;
    admit_ens_v2_root_registry(&database, CHAIN).await?;
    seed_reservation(&database, &reserved, 0x7d10).await?;
    seed_reservation(&database, &unleased, 0x7d20).await?;
    publish(&database).await?;
    for logical in [&reserved, &unleased] {
        assert_composed_unleased(&database, logical).await?;
        let row = composed(&database, logical).await?;
        assert!(
            !row.declared_summary["registration"]["expiry"].is_null(),
            "{logical}: {}",
            row.declared_summary
        );
    }

    let uri = "/v1/names?namespace=ens&expires_after=2020-01-01T00:00:00Z&page_size=200";
    let (page, savepoints) = with_savepoints(v2_get_json(&database, uri)).await?;
    assert_eq!(savepoints, 2, "{page:#}");
    let filled = listed_row(&page, "reserved.eth");
    let stored = listed_row(&page, "unleased.eth");
    assert_eq!(
        filled["ens_v1"]["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{page:#}"
    );
    assert_eq!(filled["registered_at"], json!(seconds(HANDOFF)), "{page:#}");
    assert!(stored["ens_v1"]["expires_at"].is_null(), "{page:#}");
    assert_eq!(
        filled["expires_at"],
        json!(LATER_EXPIRY.to_string()),
        "{page:#}"
    );
    assert_ne!(filled["status"], "released", "{page:#}");
    for field in ["status", "expires_at", "grace_ends_at"] {
        assert_eq!(filled[field], stored[field], "{field}: {page:#}");
    }
    database.cleanup().await
}

/// A failed savepoint fails a collection page. On a read snapshot that a failed statement has
/// aborted, a page with no shaped row still returns, as it issues no statement. A page with a
/// shaped row returns the internal error that address names and `GET /v1/names` answer with,
/// and its row stays as composed.
#[tokio::test]
async fn a_failed_savepoint_fails_a_collection_page() -> Result<()> {
    let database = new_database().await?;
    let classes = seed_input_classes(&database).await?;
    let state = database.app_state();
    let mut snapshot = crate::v2::collection_snapshot::CollectionSnapshot::capture_for_namespace(
        &state,
        None,
        Some("ens"),
    )
    .await
    .expect("the publication is served");
    sqlx::query("SELECT 1 / 0")
        .execute(
            snapshot
                .conn()
                .await
                .expect("the snapshot serves the capture"),
        )
        .await
        .expect_err("a failed statement aborts the read snapshot");

    let mut plain = composed(&database, &classes.plain.0).await?;
    let stored = plain.clone();
    snapshot
        .serve_legacy_leases(vec![&mut plain])
        .await
        .expect("a page of no shaped row issues no statement");
    assert_eq!(plain, stored);

    let mut migrated = composed(&database, &classes.migrated.0).await?;
    let unfilled = migrated.clone();
    let error = snapshot
        .serve_legacy_leases(vec![&mut migrated])
        .await
        .expect_err("the shaped row's savepoint fails on the aborted snapshot");
    assert_eq!(error.code(), crate::v2::ErrorCode::InternalError);
    assert_eq!(migrated, unfilled);
    drop(snapshot);
    database.cleanup().await
}

/// A `relation=manager` list over row B, a registry-only name whose lease's latest event is a
/// release. Membership, `relations`, `page.total_count` and the cursor come from the stored row
/// before the fill, so the row keeps its `manager` label and its count while it serves
/// `released` with no `manager`. Walked one row a page, every stored entry is listed once.
#[tokio::test]
async fn a_released_filled_name_keeps_its_stored_manager_listing() -> Result<()> {
    let database = new_database().await?;
    seed_input_classes(&database).await?;

    let uri = managed_names_uri(200, None);
    let page = v2_get_json(&database, &uri).await?;
    let row = listed_row(&page, "lapsed.eth");
    assert_filled_lease(row, true);
    assert_eq!(row["relations"], json!(["manager"]), "{page:#}");
    assert!(row.get("manager").is_none_or(Value::is_null), "{page:#}");
    let full = listed_names(&page);
    let mut unique = full.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), full.len(), "{page:#}");
    let total = &page["page"]["total_count"];
    assert_eq!(total, &json!(full.len()), "{page:#}");

    let mut walked = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let uri = managed_names_uri(1, cursor.as_deref());
        let step = v2_get_json(&database, &uri).await?;
        let names = listed_names(&step);
        assert_eq!(names.len(), 1, "{step:#}");
        assert_eq!(&step["data"][0], listed_row(&page, &names[0]), "{step:#}");
        assert_eq!(&step["page"]["total_count"], total, "{step:#}");
        walked.extend(names);
        match step["page"]["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_owned()),
            None => break,
        }
    }
    assert_eq!(walked, full);
    database.cleanup().await
}
