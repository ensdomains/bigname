// Retained-input fixture helpers for the lookup, history and registry route tests. Every helper
// writes identity rows, manifests and normalized events, then runs the real family publisher.

/// The mainnet publication head the fixture last selected.
async fn mainnet_fixture_head(database: &TestDatabase) -> Result<(i64, String)> {
    Ok(sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash FROM chain_heads
         WHERE chain_id = 'ethereum-mainnet'",
    )
    .fetch_one(&database.pool)
    .await?)
}

/// Rebuild the mainnet families at the fixture's selected head after new inputs were added.
async fn republish_mainnet_fixture(database: &TestDatabase) -> Result<()> {
    let (block, hash) = mainnet_fixture_head(database).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", block, &hash).await
}

/// Select and publish a new mainnet head. The families follow their retained inputs to it, so
/// the publication a reader captured before this call is replaced.
async fn advance_mainnet_fixture_publication(
    database: &TestDatabase,
    block_number: i64,
    block_hash: &str,
) -> Result<()> {
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum": {
            "chain_id": "ethereum-mainnet", "block_number": block_number, "block_hash": block_hash,
            "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60)
        }}))
        .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", block_number, block_hash).await
}

/// The accounts behind one fixture name. The registrar grant names the registrant, which is
/// also the token holder of an unwrapped lease; the registry transfer names the controller.
#[derive(Clone, Copy)]
struct RelationNameAccounts<'a> {
    registrant: &'a str,
    controller: &'a str,
    resolver: &'a str,
}

/// An ENSv1 `.eth` name with a registrar grant, a registry transfer, a resolver pointer and a
/// resolver `addr:60` write, all at `block_number`, then a family rebuild at that block.
/// `ids` seeds the resource, token lineage and binding identities (`ids`, `ids + 1`, `ids + 2`).
async fn seed_relation_name(
    database: &TestDatabase,
    name: &str,
    ids: u128,
    block_number: i64,
    accounts: RelationNameAccounts<'_>,
) -> Result<()> {
    let hash = format!("0xname{block_number:02x}");
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum": {
            "chain_id": "ethereum-mainnet", "block_number": block_number, "block_hash": hash,
            "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60)
        }}))
        .await?;
    seed_relation_name_inputs_at(database, name, ids, block_number, &hash, accounts).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", block_number, &hash).await
}

/// The inputs of `seed_relation_name` on an existing block, without the rebuild, for fixtures
/// that add many names before one publication.
async fn seed_relation_name_inputs_at(
    database: &TestDatabase,
    name: &str,
    ids: u128,
    block_number: i64,
    hash: &str,
    accounts: RelationNameAccounts<'_>,
) -> Result<()> {
    let chain = "ethereum-mainnet";
    let resource = Uuid::from_u128(ids);
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        chain,
        block_number,
        hash,
        resource,
        Uuid::from_u128(ids + 1),
        Uuid::from_u128(ids + 2),
        "ens_v1",
    )
    .await?;
    let manifest = declare_family_fixture_resolver(
        &database.pool,
        "ens",
        chain,
        "ens_v1_resolver_l1",
        accounts.resolver,
    )
    .await?;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let facts = [
        (
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            json!({"authority_kind":"registrar", "registrant":accounts.registrant,
                "expiry":1_900_000_000_i64}),
        ),
        (
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            json!({"source_event":"Transfer", "node":node, "owner":accounts.controller}),
        ),
        (
            "ResolverChanged",
            "ens_v1_registry_l1",
            json!({"node":node, "resolver":accounts.resolver}),
        ),
        (
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({"source_event":"AddrChanged", "node":node, "resolver":accounts.resolver,
                "record_key":"addr:60", "record_family":"addr", "selector_key":"60",
                "value":"0x0000000000000000000000000000000000000abc"}),
        ),
    ];
    let events = facts
        .into_iter()
        .enumerate()
        .map(|(log, (kind, family, after))| {
            let named = kind != "RecordChanged";
            let mut event = history_event(
                &format!("relation-{name}-{kind}"),
                named.then_some(logical.as_str()),
                named.then_some(resource),
                Some(chain),
                Some(block_number),
                Some(hash),
                Some("0xrelation"),
                Some(log as i64),
                CanonicalityState::Canonical,
            );
            event.event_kind = kind.into();
            event.source_family = family.into();
            event.manifest_version = 1;
            event.source_manifest_id = (!named).then_some(manifest);
            event.raw_fact_ref = json!({"kind":"raw_log", "emitting_address":accounts.resolver,
                "transaction_index":0});
            event.before_state = json!({});
            event.after_state = after;
            event
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    Ok(())
}

/// A subname wrapped by the NameWrapper with no registrar lease, on the permissions fixture's
/// blocks: binding, authority epoch, holder and wrapper state, then a rebuild at block 130.
async fn seed_wrapped_subname_inputs(database: &TestDatabase, name: &str, wrapper: Uuid) -> Result<()> {
    let logical = seed_family_identity_inputs(
        &database.pool,
        "ens",
        name,
        "ethereum-mainnet",
        120,
        "0xperms120",
        wrapper,
        Uuid::from_u128(wrapper.as_u128() + 1),
        Uuid::from_u128(wrapper.as_u128() + 2),
        "ens_v1",
    )
    .await?;
    let node = bigname_lookup::ens_namehash_hex(name)?;
    let events = [
        permission_fixture_event(&format!("{name}-wrapper-binding"), Some(&logical), Some(wrapper),
            "SurfaceBound", "ens_v1_wrapper_l1", 120, 0,
            json!({"source_event":"NameWrapped", "node":node, "authority_kind":"wrapper",
                "wrapped_registrar_resource_id":null})),
        permission_fixture_event(&format!("{name}-wrapper-epoch"), Some(&logical), Some(wrapper),
            "AuthorityEpochChanged", "ens_v1_wrapper_l1", 120, 1,
            json!({"source_event":"NameWrapped", "node":node, "authority_kind":"wrapper",
                "owner":V2_PERMISSIONS_SUBJECT})),
        permission_fixture_event(&format!("{name}-wrapper-owner"), Some(&logical), Some(wrapper),
            "TokenControlTransferred", "ens_v1_wrapper_l1", 120, 2,
            json!({"source_event":"NameWrapped", "node":node, "owner":V2_PERMISSIONS_SUBJECT,
                "to":V2_PERMISSIONS_SUBJECT})),
    ];
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    insert_permission_wrapper_state(database, wrapper, "wrapped", 0, 1_800_000_000, 123).await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 130, "0xperms130").await
}

/// Admit `resolver` the way Interpret does for a registry pointer: an origin registry manifest
/// of `origin_family`, the resolver's contract address and an undated resolver discovery edge
/// from that registry. Returns the origin manifest id.
async fn admit_fixture_resolver(
    pool: &PgPool,
    origin_family: &str,
    origin_address: &str,
    resolver: &str,
) -> Result<i64> {
    let origin = Uuid::new_v4();
    seed_schema_v2_ens_manifest(pool, origin_family, "registry", origin_address, origin, false)
        .await?;
    let origin_manifest: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions WHERE source_family = $1
         ORDER BY manifest_id DESC LIMIT 1",
    )
    .bind(origin_family)
    .fetch_one(pool)
    .await?;
    let instance = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, 'ethereum-mainnet', 'contract')",
    )
    .bind(instance)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses
            (contract_instance_id, chain_id, address, source_manifest_id)
         VALUES ($1, 'ethereum-mainnet', $2, $3)",
    )
    .bind(instance)
    .bind(resolver)
    .bind(origin_manifest)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id,
            to_contract_instance_id, discovery_source, admission_basis, source_manifest_id,
            canonicality_state)
         VALUES ('ethereum-mainnet', 'resolver', $1, $2, 'ResolverChanged', 'registry_pointer',
            $3, 'canonical')",
    )
    .bind(origin)
    .bind(instance)
    .bind(origin_manifest)
    .execute(pool)
    .await?;
    Ok(origin_manifest)
}

/// Names with each authority shape the name routes present, all related to `holder`:
/// - `alpha.eth` is recorded only in the 2017 registry (`ens_v0`);
/// - `beta.eth` was recorded there first and then written by the current registry (`ens_v1`,
///   with a handoff);
/// - `gamma.eth` is recorded only in the current registry (`ens_v1`);
/// - `shared-one.eth` is an ownerless 2017-registry name: its binding ended and the registry
///   reports no owner for its node, so it serves no `authority`;
/// - `shared-two.eth` is an ENSv2 registration (`ens_v2`).
///
/// Each name is seeded at block 38; the shape events are at block 39, where the families are
/// published.
async fn seed_authority_shape_names(database: &TestDatabase, holder: &str) -> Result<()> {
    const OLD_REGISTRY: &str = "0x314159265dd8dbb310642f98f50c066173c1259b";
    const REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
    const ZERO: &str = "0x0000000000000000000000000000000000000000";
    for (index, name) in AUTHORITY_SHAPE_NAMES.into_iter().enumerate() {
        let index = index as u128;
        seed_identity_name(
            database,
            &format!("ens:{name}"),
            name,
            name,
            &format!("namehash:{name}"),
            Uuid::from_u128(0x7170_0100 + index),
            Uuid::from_u128(0x7170_0200 + index),
            Uuid::from_u128(0x7170_0300 + index),
            holder,
            bigname_storage::AddressNameRelation::TokenHolder,
            38,
        )
        .await?;
    }
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum": {
            "chain_id": "ethereum-mainnet", "block_number": 39, "block_hash": "0xname27",
            "timestamp": "2026-04-17T00:00:39Z"
        }}))
        .await?;
    let registry_write = |name: &str, role: &str, emitter: &str, owner: &str| -> Result<Value> {
        Ok(json!({"source_event":"Transfer", "node":bigname_lookup::ens_namehash_hex(name)?,
            "owner":owner, "owner_getter":owner, "emitter_role":role, "registry_contract":emitter}))
    };
    let mut facts = vec![
        ("alpha.eth", "AuthorityTransferred", "ens_v1_registry_l1",
            registry_write("alpha.eth", "registry_old", OLD_REGISTRY, holder)?),
        ("beta.eth", "AuthorityTransferred", "ens_v1_registry_l1",
            registry_write("beta.eth", "registry_old", OLD_REGISTRY, holder)?),
        ("beta.eth", "AuthorityTransferred", "ens_v1_registry_l1",
            registry_write("beta.eth", "registry", REGISTRY, holder)?),
        ("gamma.eth", "AuthorityTransferred", "ens_v1_registry_l1",
            registry_write("gamma.eth", "registry", REGISTRY, holder)?),
    ];
    let mut ownerless = registry_write("shared-one.eth", "registry_old", OLD_REGISTRY, ZERO)?;
    ownerless["owner_getter_reason"] = json!("literal_zero");
    facts.push(("shared-one.eth", "AuthorityTransferred", "ens_v1_registry_l1", ownerless));
    facts.extend([
        ("shared-two.eth", "RegistrationGranted", "ens_v2_registry_l1",
            json!({"source_event":"NameRegistered", "authority_kind":"ens_v2_registry",
                "owner":holder, "registrant":holder, "expiry":4_000_000_000_u64})),
        ("shared-two.eth", "TokenControlTransferred", "ens_v2_registry_l1",
            json!({"source_event":"Transfer", "from":ZERO, "to":holder})),
    ]);
    let mut events = Vec::new();
    for (log, (name, kind, family, after)) in facts.into_iter().enumerate() {
        let index = AUTHORITY_SHAPE_NAMES
            .iter()
            .position(|candidate| *candidate == name)
            .context("authority shape name")? as u128;
        let mut event = history_event(
            &format!("authority-shape-{name}-{log}"),
            Some(&bigname_storage::logical_name_id_for_name("ens", name)),
            Some(Uuid::from_u128(0x7170_0100 + index)),
            Some("ethereum-mainnet"),
            Some(39),
            Some("0xname27"),
            Some("0xauthority-shape"),
            Some(log as i64),
            CanonicalityState::Canonical,
        );
        event.event_kind = kind.into();
        event.source_family = family.into();
        event.before_state = json!({});
        event.after_state = after;
        events.push(event);
    }
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    // shared-one.eth: the binding ended and the registry control resource has no token.
    sqlx::query(
        "UPDATE surface_bindings SET active_to = '2026-04-17T00:00:39Z'
         WHERE surface_binding_id = $1",
    )
    .bind(Uuid::from_u128(0x7170_0303))
    .execute(&database.pool)
    .await?;
    sqlx::query("UPDATE resources SET token_lineage_id = NULL WHERE resource_id = $1")
        .bind(Uuid::from_u128(0x7170_0103))
        .execute(&database.pool)
        .await?;
    // shared-two.eth: the name is bound through the ENSv2 registry.
    sqlx::query("UPDATE surface_bindings SET authority_arm = 'ens_v2' WHERE surface_binding_id = $1")
        .bind(Uuid::from_u128(0x7170_0304))
        .execute(&database.pool)
        .await?;
    rebuild_fixture_families(&database.pool, "ethereum-mainnet", 39, "0xname27").await
}

const AUTHORITY_SHAPE_NAMES: [&str; 5] = [
    "alpha.eth",
    "beta.eth",
    "gamma.eth",
    "shared-one.eth",
    "shared-two.eth",
];
