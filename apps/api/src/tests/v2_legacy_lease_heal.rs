//! A `.eth` second-level ENSv1 name whose stored registration does not carry its BaseRegistrar
//! lease, read through name detail and the records route (`v2::support::legacy_lease`). Each
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

    // Lookup reads the stored registration, whose identity is the bound registry record.
    let lookup = v2_lookup_json(
        &database,
        json!({"profile": "detail", "inputs": [{"id": "migrated", "name": "migrated.eth"}]}),
    )
    .await?;
    let record = &lookup["data"][0]["record"];
    assert_eq!(lookup["data"][0]["status"], "ok", "{lookup}");
    assert_eq!(
        record["registration_id"],
        json!(Uuid::from_u128(0x7a10).to_string()),
        "{lookup}"
    );
    assert!(record["registered_at"].is_null(), "{lookup}");
    assert_eq!(record["ens_v1"]["expires_at"], Value::Null, "{lookup}");
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
            let body = read_json(response).await?;
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
