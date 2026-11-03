//! A `.eth` second-level ENSv1 name whose selected authority records no lease, read through the
//! public routes after Project composes its implied lease (`docs/projections.md`, registry-only
//! bindings). Each case builds retained inputs, publishes the families, then reads the routes.
use super::*;

const CHAIN: &str = "ethereum-mainnet";
const OWNER: &str = "0x00000000000000000000000000000000000000a5";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
/// The block of the name's surface, its registry-only binding and its first current-registry
/// record.
const HANDOFF: i64 = 120;
/// The first controller renewal, which carries the grant the adapter derives beside it.
const RENEWED: i64 = 124;
/// The publication every request reads.
const HEAD: i64 = 130;
const RENEWED_EXPIRY: i64 = 1_850_000_000;
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
    logical: &str,
    resource: Uuid,
    kind: &str,
    family: &str,
    block: i64,
    log: i64,
    after: Value,
) -> NormalizedEvent {
    let mut event = history_event(
        &format!("implied-lease-{resource}-{kind}-{block}-{log}"),
        Some(logical),
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

/// A migrated name: its registry-only binding and epoch and the current registry's owner write
/// at `HANDOFF`, then a lease first seen at a controller renewal, whose grant the adapter derives
/// with the zero registrant.
async fn seed_migrated(database: &TestDatabase, name: &str, seed: u128) -> Result<String> {
    let registry = Uuid::from_u128(seed);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        CHAIN,
        HANDOFF,
        &hash(HANDOFF),
        registry,
        Uuid::from_u128(seed + 1),
        Uuid::from_u128(seed + 2),
        "ens_v1",
    )
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = NULL WHERE resource_id = $1")
        .bind(registry)
        .execute(&database.pool)
        .await?;
    let lease = Uuid::from_u128(seed + 0xf);
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
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let renewal = json!({"authority_kind": "registrar", "source_event": "NameRenewed",
        "registrant": ZERO, "expiry": RENEWED_EXPIRY, "namehash": node});
    bigname_storage::insert_normalized_event_fixtures(
        &database.pool,
        &[
            event(
                &logical,
                registry,
                "AuthorityTransferred",
                REGISTRY,
                HANDOFF,
                0,
                json!({"source_event": "Transfer", "node": node, "owner": OWNER,
                    "owner_getter": OWNER, "registry_owner": OWNER, "emitter_role": "registry"}),
            ),
            event(
                &logical,
                registry,
                "AuthorityEpochChanged",
                REGISTRY,
                HANDOFF,
                1,
                json!({"source_event": "Transfer", "authority_kind": "registry_only",
                    "authority_key": format!("registry:{CHAIN}:{node}"), "owner": OWNER}),
            ),
            event(
                &logical,
                lease,
                "RegistrationGranted",
                REGISTRAR,
                RENEWED,
                0,
                renewal.clone(),
            ),
            event(
                &logical,
                lease,
                "RegistrationRenewed",
                REGISTRAR,
                RENEWED,
                1,
                renewal.clone(),
            ),
            event(
                &logical,
                lease,
                "ExpiryChanged",
                REGISTRAR,
                RENEWED,
                2,
                renewal,
            ),
        ],
    )
    .await?;
    Ok(logical)
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

/// The ownerless fixture (`AliceInputState::Ownerless`) with its lease released: the name keeps
/// its retained registry resolver pointer and records, which a released name must not serve.
async fn released_ownerless() -> Result<TestDatabase> {
    let database = TestDatabase::new_with_schemas(false, true).await?;
    seed_alice_state_inputs(&database, AliceInputState::Ownerless).await?;
    append_alice_name_input(
        &database,
        "RegistrationReleased",
        REGISTRAR,
        json!({"authority_kind": "registrar", "expiry": 1_700_000_000,
            "released_at": 1_776_384_003}),
    )
    .await?;
    rebuild_fixture_families(&database.pool, CHAIN, 21_000_003, "0xbinding").await?;
    Ok(database)
}

/// The name listing reads the stored summary the composer writes, so a migrated name is listed
/// with its lease's expiry and dated at its first current-registry record, as name detail serves
/// it, not at the renewal that first shows the lease.
#[tokio::test]
async fn the_name_listing_dates_a_migrated_lease_at_its_first_registry_record() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, HANDOFF..=HEAD).await?;
    seed_migrated(&database, "migrated.eth", 0x7d10).await?;
    publish(&database).await?;

    let detail = v2_get_json(&database, "/v1/names/migrated.eth").await?;
    let data = &detail["data"];
    assert_eq!(data["status"], json!("active"), "{data}");
    assert_eq!(data["registered_at"], json!(seconds(HANDOFF)), "{data}");
    assert_eq!(
        data["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{data}"
    );

    let page = v2_get_json(
        &database,
        "/v1/names?namespace=ens&expires_after=2020-01-01T00:00:00Z&page_size=200",
    )
    .await?;
    let rows = page["data"].as_array().context("listing rows")?;
    let row = rows
        .iter()
        .find(|row| row["name"] == json!("migrated.eth"))
        .with_context(|| format!("migrated.eth is listed: {page:#}"))?;
    assert_eq!(row["registered_at"], json!(seconds(HANDOFF)), "{row}");
    assert_eq!(
        row["expires_at"],
        json!(RENEWED_EXPIRY.to_string()),
        "{row}"
    );
    assert_eq!(row["registered_at"], data["registered_at"], "{row}");
    database.cleanup().await
}

/// A released implied lease serves `released` with no resolver and no records on name detail
/// and on an indexed records read, although its retained registry pointer and records remain.
#[tokio::test]
async fn a_released_implied_lease_serves_no_resolver_or_records() -> Result<()> {
    let database = released_ownerless().await?;
    let detail = v2_get_json(&database, "/v1/names/alice.eth").await?;
    let data = &detail["data"];
    assert_eq!(data["status"], json!("released"), "{data}");
    assert!(data.get("resolver").is_none(), "{data}");
    assert!(data.get("records").is_none(), "{data}");
    assert!(data.get("owner").is_none(), "{data}");
    assert!(data.get("manager").is_none(), "{data}");

    let records = v2_get_json(
        &database,
        "/v1/names/alice.eth/records?source=indexed&keys=text:description",
    )
    .await?;
    assert_eq!(records["meta"]["source"], json!("indexed"), "{records}");
    assert!(records["data"]["resolver"].is_null(), "{records}");
    assert_ne!(
        records["data"]["records"]["text:description"]["status"],
        json!("ok"),
        "{records}"
    );
    database.cleanup().await
}

/// A verified records read of a released implied lease answers without dialling the provider:
/// the provider below is unreachable and would fail the request if it were called.
#[tokio::test]
async fn a_released_implied_lease_answers_a_verified_read_without_a_provider_call() -> Result<()> {
    let database = released_ownerless().await?;
    let unreachable = unreachable_rpc_url()?;
    let state = database
        .app_state_with_lookup_chain_rpc_urls(bigname_lookup::ChainRpcUrls::from_entries(&[
            format!("{CHAIN}={unreachable}"),
        ])?)
        .await?;
    for uri in [
        "/v1/names/alice.eth/records?source=verified&keys=text:description",
        "/v1/names/alice.eth?source=verified",
    ] {
        let response = app_router(state.clone())
            .oneshot(Request::builder().uri(uri).body(Body::empty())?)
            .await?;
        let status = response.status();
        let payload: Value = read_json(response).await?;
        assert_eq!(status, StatusCode::OK, "{uri}: {payload:#}");
        assert_eq!(
            payload["meta"]["source"],
            json!("verified"),
            "{uri}: {payload:#}"
        );
        assert!(payload["data"]["resolver"].is_null(), "{uri}: {payload:#}");
    }
    database.cleanup().await
}

/// A released ownerless name is served under its own path: the composed row carries the
/// release, and the read takes no alias discovery statement.
#[tokio::test]
async fn a_released_ownerless_name_takes_no_alias_walk() -> Result<()> {
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
    assert_eq!(data["status"], json!("released"), "{data}");
    database.cleanup().await
}
