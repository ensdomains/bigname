//! A `.eth` name registered by A, handed to B by a BaseRegistrar transfer without `reclaim`,
//! then migrated to ENSv2 by B through the unlocked migration controller.
//!
//! The transfer without `reclaim` leaves A as registry owner, so the name is bound to the
//! registry-only resource and A holds `resource_control` and `resolver_control` there. The
//! controller's reclaim at migration moves the registry record away from A and revokes those
//! grants on the registry-only resource. Permissions fold per resource, subject and scope taking
//! the latest surviving row, so the projection only clears A's control when Interpret keeps those
//! revocations: it does not infer a revocation from the binding's closure. The rows below carry
//! the fields the adapter emits for this scenario and that the projection reads (see the adapter
//! test `registry_only_handoff_cleanup_keeps_the_registrant_revocations_on_the_registry_resource`);
//! the authority keys are abbreviated, which the projection does not parse.
//!
//! In the second shape A sends the registry record to the Graveyard with `setOwner` after the
//! handoff, so the Graveyard holds both grants on the registry-only resource when B migrates. The
//! reclaim then revokes the Graveyard's grants, and the migration's own registry transfer to the
//! Graveyard grants the same subject and scopes again one log later before the cleanup revokes
//! them; reconciliation drops those transient rows and must still hand Project the reclaim's
//! revocations (see the adapter test
//! `registry_only_handoff_cleanup_keeps_the_reclaim_revocations_of_a_prior_graveyard_owner`).
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L68 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111-L119 @ ens_v2@a971bd64)

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-mainnet";
const NAMEHASH: &str = "0x2d2f286e69323d39eb4e0d31b25d3cd6e284808088409764548dd6d3e740d36d";
const LOGICAL: &str = "ens:0x2d2f286e69323d39eb4e0d31b25d3cd6e284808088409764548dd6d3e740d36d";
const LEASE_RESOURCE: &str = "ac858fa2-74ee-5cb8-8dc9-e9e1a992a8fd";
const LEASE_LINEAGE: &str = "ac858fa2-74ee-5cb8-8dc9-e9e1a992a8fe";
const LEASE_BINDING: &str = "ac858fa2-74ee-5cb8-8dc9-e9e1a992a8ff";
const REGISTRY_RESOURCE: &str = "fa4f08cc-e41e-58fd-bfc3-42a546c306d6";
const REGISTRY_BINDING: &str = "fa4f08cc-e41e-58fd-bfc3-42a546c306d7";
const V2_RESOURCE: &str = "6f859f98-8762-5467-86a5-591b802bb0a0";
const V2_LINEAGE: &str = "6f859f98-8762-5467-86a5-591b802bb0a1";
const V2_BINDING: &str = "6f859f98-8762-5467-86a5-591b802bb0a2";
const REGISTRANT: &str = "0x5151515151515151515151515151515151515151";
const HOLDER: &str = "0x5252525252525252525252525252525252525252";
const CONTROLLER: &str = "0xd8a5a9b31c3c0232e196d518e89fd8bf83acad43";
const GRAVEYARD: &str = "0x7969c5ed335650692bc04293b07f5bf2e7a673c0";
const RESOLVER: &str = "0x6161616161616161616161616161616161616161";
const V2_REGISTRY: &str = "0x36c02da8a0983159322a80ffe9f24b1acff8b570";
const V2_TOKEN: &str = "0x2d2f286e69323d39eb4e0d31b25d3cd6e284808088409764548dd6d3e740d36d";
const REGISTRATION_BLOCK: i64 = 234;
const HANDOFF_BLOCK: i64 = 235;
const MIGRATION_BLOCK: i64 = 236;
const EXPIRY: i64 = 1_900_000_000;
const REGISTRY_AUTHORITY_KEY: &str = "registry-only:ethereum-mainnet:0x2d2f286e69323d39eb4e0d31b25d3cd6e284808088409764548dd6d3e740d36d";
const LEASE_AUTHORITY_KEY: &str =
    "registrar:ethereum-mainnet:0x2d2f286e69323d39eb4e0d31b25d3cd6e284808088409764548dd6d3e740d36d";

fn hash(block: i64) -> String {
    format!("0x{block:064x}")
}

/// Block timestamps one second apart; Interpret positions a binding at its log's microsecond.
fn seconds(block: i64, log: i64) -> f64 {
    1_700_000_000.0 + block as f64 + log as f64 / 1_000_000.0
}

#[rustfmt::skip]
async fn test_database(prefix: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(TestDatabaseConfig::new(prefix)).await?;
    let pool = database.pool().clone(); let name: String = sqlx::query_scalar("SELECT current_database()").fetch_one(&pool).await?; let mut tx = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase").execute(&mut *tx).await?;
    raw_sql(&format!("ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public", name.replace('"', r#""""#))).execute(&mut *tx).await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public").execute(&mut *tx).await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    pool.set_connect_options(pool.connect_options().as_ref().clone().options([("search_path", "bigname_phase,public")]));
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        sqlx::query("SET search_path TO bigname_phase, public").execute(&mut **connection).await?;
    }
    Ok((database, pool))
}

/// One ENSv1 permission row as `push_permission_change` writes it: a grant carries the power
/// with its source, a revocation carries no power and names the source that closed it.
fn v1_permission(
    subject: &str,
    scope: Value,
    power: &str,
    grant: bool,
    authority_kind: &str,
    authority_key: &str,
    source_event_kind: &str,
) -> (Value, Value) {
    let source = json!({
        "kind": "ens_v1_authority",
        "authority_kind": authority_kind,
        "authority_key": authority_key,
        "source_event_kind": source_event_kind,
    });
    let row = |powers: Value, grant_source: Value, revocation_source: Value| {
        json!({
            "subject": subject,
            "scope": scope,
            "effective_powers": powers,
            "grant_source": grant_source,
            "revocation_source": revocation_source,
            "inheritance_path": [],
            "transfer_behavior": "replace_on_authority_change",
        })
    };
    if grant {
        (
            row(json!([]), Value::Null, Value::Null),
            row(json!([power]), source, Value::Null),
        )
    } else {
        (
            row(json!([power]), source.clone(), Value::Null),
            row(json!([]), Value::Null, source),
        )
    }
}

fn resource_scope() -> Value {
    json!({"kind": "resource"})
}

fn resolver_scope() -> Value {
    json!({"kind": "resolver", "chain_id": CHAIN, "resolver_address": RESOLVER})
}

struct Event<'a> {
    resource: Option<&'a str>,
    family: &'a str,
    block: i64,
    log: i64,
    kind: &'a str,
    suffix: &'a str,
    before: Value,
    after: Value,
}

async fn event(pool: &PgPool, event: Event<'_>) -> Result<()> {
    let derivation_kind = if event.kind == "MigrationApplied" {
        "ens_v2_migration"
    } else if event.family.starts_with("ens_v2") {
        "ens_v2_registry_resource_surface"
    } else {
        "ens_v1_unwrapped_authority"
    };
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, before_state, after_state, migration_correlation_ids
         ) VALUES ($1, 'ens', $2, $3::uuid, $4, $5, 1, $6, $7, $8, $9, 0, $10, $11,
                   'canonical', $12, $13,
                   CASE WHEN $4 = 'MigrationApplied' THEN ARRAY[$1] ELSE ARRAY[]::text[] END)",
    )
    .bind(format!(
        "{}:{}:{}:{}:{}",
        event.block,
        event.log,
        event.kind,
        event.suffix,
        event.resource.unwrap_or("-")
    ))
    .bind(LOGICAL)
    .bind(event.resource)
    .bind(event.kind)
    .bind(event.family)
    .bind(CHAIN)
    .bind(event.block)
    .bind(hash(event.block))
    .bind(format!("0x{:064x}", 7_000 + event.block))
    .bind(event.log)
    .bind(derivation_kind)
    .bind(event.before)
    .bind(event.after)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn permission(
    pool: &PgPool,
    resource: &str,
    family: &str,
    block: i64,
    log: i64,
    subject: &str,
    scope: Value,
    power: &str,
    grant: bool,
    source_event_kind: &str,
) -> Result<()> {
    let (authority_kind, authority_key) = if resource == REGISTRY_RESOURCE {
        ("registry_only", REGISTRY_AUTHORITY_KEY)
    } else {
        ("registrar", LEASE_AUTHORITY_KEY)
    };
    let (before, after) = v1_permission(
        subject,
        scope.clone(),
        power,
        grant,
        authority_kind,
        authority_key,
        source_event_kind,
    );
    let suffix = format!(
        "{}:{}:{}:{subject}",
        if grant { "grant" } else { "revoke" },
        power,
        scope["kind"].as_str().unwrap_or_default()
    );
    event(
        pool,
        Event {
            resource: Some(resource),
            family,
            block,
            log,
            kind: "PermissionChanged",
            suffix: &suffix,
            before,
            after,
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn binding(
    pool: &PgPool,
    id: &str,
    resource: &str,
    arm: &str,
    from_block: i64,
    from_log: i64,
    to: Option<(i64, i64)>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm,
             active_from, active_to, chain_id, block_hash, block_number, provenance,
             canonicality_state
         ) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', $4, to_timestamp($5),
                   to_timestamp($6), $7, $8, $9, $10, 'canonical')",
    )
    .bind(id)
    .bind(LOGICAL)
    .bind(resource)
    .bind(arm)
    .bind(seconds(from_block, from_log))
    .bind(to.map(|(block, log)| seconds(block, log)))
    .bind(CHAIN)
    .bind(hash(from_block))
    .bind(from_block)
    .bind(json!({
        "chain_id": CHAIN, "block_number": from_block, "block_hash": hash(from_block),
        "transaction_hash": format!("0x{:064x}", 7_000 + from_block),
        "transaction_index": 0, "log_index": from_log, "source": "raw_log",
    }))
    .execute(pool)
    .await?;
    Ok(())
}

/// Seeds the registration, the handoff and the migration transaction. `registry_owner` is the
/// registry owner when the migration starts: the registrant, or the Graveyard after the registrant
/// hands it the registry record in the block of the handoff. With `reclaim_revokes` false the
/// migration transaction carries no revocation of that owner's registry-only grants, which is
/// what the reconciliation consumer used to hand Project.
async fn seed(pool: &PgPool, reclaim_revokes: bool, registry_owner: &str) -> Result<()> {
    for block in [REGISTRATION_BLOCK, HANDOFF_BLOCK, MIGRATION_BLOCK] {
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state)
             VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(CHAIN).bind(hash(block)).bind(hash(block - 1)).bind(block).bind(seconds(block, 0))
        .execute(pool)
        .await?;
    }
    let (h234, h236) = (hash(REGISTRATION_BLOCK), hash(MIGRATION_BLOCK));
    raw_sql(&format!(
        "INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ('{LEASE_LINEAGE}', '{CHAIN}', '{h234}', {REGISTRATION_BLOCK}, 'canonical'),
                ('{V2_LINEAGE}', '{CHAIN}', '{h236}', {MIGRATION_BLOCK}, 'canonical');
         INSERT INTO resources (resource_id, token_lineage_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ('{LEASE_RESOURCE}', '{LEASE_LINEAGE}', '{CHAIN}', '{h234}', {REGISTRATION_BLOCK}, 'canonical'),
                ('{REGISTRY_RESOURCE}', NULL, '{CHAIN}', '{h234}', {REGISTRATION_BLOCK}, 'canonical'),
                ('{V2_RESOURCE}', '{V2_LINEAGE}', '{CHAIN}', '{h236}', {MIGRATION_BLOCK}, 'canonical');
         INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, chain_id, block_hash, block_number, canonicality_state)
         VALUES ('{LOGICAL}', 'ens', 'handoff.eth', ARRAY['handoff','eth'], '\\x0768616e646f66660365746800', '{NAMEHASH}',
                 ARRAY['0x{:064x}','0x{:064x}'], 'ensip15', 'active', '{CHAIN}', '{h234}', {REGISTRATION_BLOCK}, 'canonical')",
        1_u64, 2_u64
    ))
    .execute(pool)
    .await?;
    binding(
        pool,
        LEASE_BINDING,
        LEASE_RESOURCE,
        "ens_v1",
        REGISTRATION_BLOCK,
        0,
        Some((HANDOFF_BLOCK, 0)),
    )
    .await?;
    binding(
        pool,
        REGISTRY_BINDING,
        REGISTRY_RESOURCE,
        "ens_v1",
        HANDOFF_BLOCK,
        0,
        Some((MIGRATION_BLOCK, 5)),
    )
    .await?;
    binding(
        pool,
        V2_BINDING,
        V2_RESOURCE,
        "ens_v2",
        MIGRATION_BLOCK,
        8,
        None,
    )
    .await?;

    const REGISTRAR: &str = "ens_v1_registrar_l1";
    const REGISTRY: &str = "ens_v1_registry_l1";
    // Registration: the registrar names the registrant and the resolver is set.
    event(pool, Event {
        resource: Some(LEASE_RESOURCE), family: REGISTRAR, block: REGISTRATION_BLOCK, log: 1,
        kind: "RegistrationGranted", suffix: "registration", before: json!({}),
        after: json!({"source_event": "NameRegistered", "registrant": REGISTRANT, "expiry": EXPIRY, "status": "registered", "labelhash": format!("0x{:064x}", 1_u64)}),
    }).await?;
    permission(
        pool,
        LEASE_RESOURCE,
        REGISTRAR,
        REGISTRATION_BLOCK,
        1,
        REGISTRANT,
        resource_scope(),
        "resource_control",
        true,
        "RegistrationGranted",
    )
    .await?;
    event(
        pool,
        Event {
            resource: Some(LEASE_RESOURCE),
            family: REGISTRY,
            block: REGISTRATION_BLOCK,
            log: 2,
            kind: "ResolverChanged",
            suffix: "resolver",
            before: json!({}),
            after: json!({"source_event": "NewResolver", "resolver": RESOLVER}),
        },
    )
    .await?;
    permission(
        pool,
        LEASE_RESOURCE,
        REGISTRY,
        REGISTRATION_BLOCK,
        2,
        REGISTRANT,
        resolver_scope(),
        "resolver_control",
        true,
        "ResolverChanged",
    )
    .await?;

    // Handoff without reclaim: the token moves to the holder, the registry owner stays the
    // registrant, so the name binds to the registry-only resource and the registrant is granted
    // control there.
    event(pool, Event {
        resource: Some(LEASE_RESOURCE), family: REGISTRAR, block: HANDOFF_BLOCK, log: 0,
        kind: "TokenControlTransferred", suffix: "handoff", before: json!({"from": REGISTRANT}),
        after: json!({"source_event": "Transfer", "to": HOLDER, "token_lineage_id": LEASE_LINEAGE}),
    }).await?;
    permission(
        pool,
        LEASE_RESOURCE,
        REGISTRAR,
        HANDOFF_BLOCK,
        0,
        REGISTRANT,
        resource_scope(),
        "resource_control",
        false,
        "TokenControlTransferred",
    )
    .await?;
    permission(
        pool,
        LEASE_RESOURCE,
        REGISTRAR,
        HANDOFF_BLOCK,
        0,
        HOLDER,
        resource_scope(),
        "resource_control",
        true,
        "TokenControlTransferred",
    )
    .await?;
    permission(
        pool,
        REGISTRY_RESOURCE,
        REGISTRAR,
        HANDOFF_BLOCK,
        0,
        REGISTRANT,
        resource_scope(),
        "resource_control",
        true,
        "TokenControlTransferred",
    )
    .await?;
    permission(
        pool,
        LEASE_RESOURCE,
        REGISTRAR,
        HANDOFF_BLOCK,
        0,
        REGISTRANT,
        resolver_scope(),
        "resolver_control",
        false,
        "TokenControlTransferred",
    )
    .await?;
    permission(
        pool,
        REGISTRY_RESOURCE,
        REGISTRAR,
        HANDOFF_BLOCK,
        0,
        REGISTRANT,
        resolver_scope(),
        "resolver_control",
        true,
        "TokenControlTransferred",
    )
    .await?;
    event(pool, Event {
        resource: Some(REGISTRY_RESOURCE), family: REGISTRAR, block: HANDOFF_BLOCK, log: 0,
        kind: "AuthorityEpochChanged", suffix: "handoff", before: json!({}),
        after: json!({"source_event": "Transfer", "authority_kind": "registry_only", "registry_owner": REGISTRANT}),
    }).await?;
    if registry_owner == GRAVEYARD {
        // The registrant's `setOwner` moves the registry record to the Graveyard: the registrant's
        // registry-only grants close and the Graveyard is granted the same scopes there.
        event(
            pool,
            Event {
                resource: Some(REGISTRY_RESOURCE),
                family: REGISTRY,
                block: HANDOFF_BLOCK,
                log: 1,
                kind: "AuthorityTransferred",
                suffix: "graveyard-owner",
                before: json!({}),
                after: json!({"source_event": "Transfer", "node": NAMEHASH, "owner": GRAVEYARD}),
            },
        )
        .await?;
        for (subject, grant) in [(REGISTRANT, false), (GRAVEYARD, true)] {
            for (scope, power) in [
                (resource_scope(), "resource_control"),
                (resolver_scope(), "resolver_control"),
            ] {
                permission(
                    pool,
                    REGISTRY_RESOURCE,
                    REGISTRY,
                    HANDOFF_BLOCK,
                    1,
                    subject,
                    scope,
                    power,
                    grant,
                    "AuthorityTransferred",
                )
                .await?;
            }
        }
    }

    // Migration: the holder sends the token to the controller, the controller reclaims the
    // registry record, hands the record and the token to the Graveyard, and registers the name
    // in ENSv2 for the holder. Reconciliation keeps the registrar transfers and the revocations;
    // the transient controller and Graveyard authorities leave no grants.
    event(pool, Event {
        resource: Some(LEASE_RESOURCE), family: REGISTRAR, block: MIGRATION_BLOCK, log: 0,
        kind: "TokenControlTransferred", suffix: "incoming", before: json!({"from": HOLDER}),
        after: json!({"source_event": "Transfer", "to": CONTROLLER, "token_lineage_id": LEASE_LINEAGE}),
    }).await?;
    permission(
        pool,
        LEASE_RESOURCE,
        REGISTRAR,
        MIGRATION_BLOCK,
        0,
        HOLDER,
        resource_scope(),
        "resource_control",
        false,
        "TokenControlTransferred",
    )
    .await?;
    permission(
        pool,
        LEASE_RESOURCE,
        REGISTRAR,
        MIGRATION_BLOCK,
        0,
        CONTROLLER,
        resource_scope(),
        "resource_control",
        true,
        "TokenControlTransferred",
    )
    .await?;
    if reclaim_revokes {
        permission(
            pool,
            REGISTRY_RESOURCE,
            REGISTRY,
            MIGRATION_BLOCK,
            1,
            registry_owner,
            resource_scope(),
            "resource_control",
            false,
            "AuthorityTransferred",
        )
        .await?;
        permission(
            pool,
            REGISTRY_RESOURCE,
            REGISTRY,
            MIGRATION_BLOCK,
            1,
            registry_owner,
            resolver_scope(),
            "resolver_control",
            false,
            "AuthorityTransferred",
        )
        .await?;
    }
    event(
        pool,
        Event {
            resource: Some(LEASE_RESOURCE),
            family: REGISTRY,
            block: MIGRATION_BLOCK,
            log: 1,
            kind: "AuthorityTransferred",
            suffix: "reclaim",
            before: json!({}),
            after: json!({"source_event": "NewOwner", "node": NAMEHASH, "owner": CONTROLLER}),
        },
    )
    .await?;
    event(
        pool,
        Event {
            resource: Some(LEASE_RESOURCE),
            family: REGISTRY,
            block: MIGRATION_BLOCK,
            log: 2,
            kind: "AuthorityTransferred",
            suffix: "graveyard",
            before: json!({}),
            after: json!({"source_event": "Transfer", "node": NAMEHASH, "owner": GRAVEYARD}),
        },
    )
    .await?;
    permission(
        pool,
        LEASE_RESOURCE,
        REGISTRY,
        MIGRATION_BLOCK,
        2,
        CONTROLLER,
        resource_scope(),
        "resource_control",
        false,
        "AuthorityTransferred",
    )
    .await?;
    permission(
        pool,
        LEASE_RESOURCE,
        REGISTRY,
        MIGRATION_BLOCK,
        2,
        CONTROLLER,
        resolver_scope(),
        "resolver_control",
        false,
        "AuthorityTransferred",
    )
    .await?;
    event(pool, Event {
        resource: Some(LEASE_RESOURCE), family: REGISTRY, block: MIGRATION_BLOCK, log: 3,
        kind: "ResolverChanged", suffix: "clear", before: json!({}),
        after: json!({"source_event": "NewResolver", "resolver": "0x0000000000000000000000000000000000000000"}),
    }).await?;
    event(pool, Event {
        resource: Some(LEASE_RESOURCE), family: REGISTRAR, block: MIGRATION_BLOCK, log: 5,
        kind: "TokenControlTransferred", suffix: "cleanup", before: json!({"from": CONTROLLER}),
        after: json!({"source_event": "Transfer", "to": GRAVEYARD, "token_lineage_id": LEASE_LINEAGE}),
    }).await?;
    permission(
        pool,
        LEASE_RESOURCE,
        REGISTRAR,
        MIGRATION_BLOCK,
        5,
        CONTROLLER,
        resource_scope(),
        "resource_control",
        false,
        "TokenControlTransferred",
    )
    .await?;
    permission(
        pool,
        LEASE_RESOURCE,
        REGISTRAR,
        MIGRATION_BLOCK,
        5,
        GRAVEYARD,
        resource_scope(),
        "resource_control",
        true,
        "TokenControlTransferred",
    )
    .await?;
    let registered = json!({
        "source_event": "LabelRegistered", "status": "registered", "authority_kind": "ens_v2_registry",
        "registrant": HOLDER, "expiry": EXPIRY, "token_id": V2_TOKEN,
        "registry_contract_instance_id": "00000000-0000-0000-0000-000000000001",
    });
    event(
        pool,
        Event {
            resource: None,
            family: "ens_v2_registry_l1",
            block: MIGRATION_BLOCK,
            log: 6,
            kind: "RegistrationGranted",
            suffix: "label",
            before: json!({}),
            after: registered.clone(),
        },
    )
    .await?;
    let mut linked = registered;
    linked["source_event"] = json!("TokenResource");
    event(
        pool,
        Event {
            resource: Some(V2_RESOURCE),
            family: "ens_v2_registry_l1",
            block: MIGRATION_BLOCK,
            log: 8,
            kind: "RegistrationGranted",
            suffix: "resource",
            before: json!({}),
            after: linked,
        },
    )
    .await?;
    event(pool, Event {
        resource: Some(V2_RESOURCE), family: "ens_v2_registry_l1", block: MIGRATION_BLOCK, log: 9,
        kind: "PermissionChanged", suffix: "roles", before: json!({}),
        after: json!({
            "subject": HOLDER,
            "scope": {"kind": "registry", "chain_id": CHAIN, "registry_address": V2_REGISTRY},
            "effective_powers": ["resource_control"],
            "grant_source": {"kind": "raw_log", "source_event": "EACRolesChanged", "changed_powers": ["resource_control"], "registry_contract_instance_id": "00000000-0000-0000-0000-000000000001"},
            "revocation_source": null, "inheritance_path": [], "transfer_behavior": {},
            "source_event": "EACRolesChanged", "upstream_resource": V2_TOKEN, "resource": V2_TOKEN,
            "role_bitmap": format!("0x{:064x}", 1_u64), "old_role_bitmap": format!("0x{:064x}", 0_u64),
            "root_resource": false, "registry_contract_instance_id": "00000000-0000-0000-0000-000000000001",
        }),
    }).await?;
    event(
        pool,
        Event {
            resource: Some(V2_RESOURCE),
            family: "ens_v2_registry_l1",
            block: MIGRATION_BLOCK,
            log: 10,
            kind: "ResolverChanged",
            suffix: "resolver",
            before: json!({}),
            after: json!({"source_event": "ResolverUpdated", "resolver": RESOLVER}),
        },
    )
    .await?;
    // The activated migration boundary: the ENSv2 registration succeeds the lease at the cleanup.
    event(
        pool,
        Event {
            resource: None,
            family: "ens_v2_migration_l1",
            block: MIGRATION_BLOCK,
            log: 8,
            kind: "MigrationApplied",
            suffix: "boundary",
            before: json!({}),
            after: json!({
                "migration_path": "unwrapped",
                "successor_registry_contract_instance_id": "00000000-0000-0000-0000-000000000001",
                "predecessor_binding": {
                    "binding_id": REGISTRY_BINDING,
                    "resource_id": REGISTRY_RESOURCE,
                    "predecessor_cleanup": {"transaction_index": 0, "log_index": 5},
                },
                "successor_binding": {"binding_id": V2_BINDING, "resource_id": V2_RESOURCE},
                "evidence": [],
            }),
        },
    )
    .await?;
    Ok(())
}

async fn run(pool: &PgPool, target: i64, resume: Option<i64>) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: target,
            affected_from_block: resume.map_or(REGISTRATION_BLOCK, |number| number + 1),
            affected_to_block: target,
            resume_current: resume.map(|number| Marker {
                number,
                hash: hash(number),
            }),
            mode: RunMode::Normal,
        })
        .await?;
    Ok(())
}

/// `subject`'s rows on the registry-only resource as `(scope, effective_powers, revoked)`.
async fn registry_rows(pool: &PgPool, subject: &str) -> Result<Vec<(String, Value, bool)>> {
    Ok(sqlx::query_as(
        "SELECT scope, effective_powers, revocation_source IS NOT NULL
         FROM permissions_current
         WHERE resource_id = $1::uuid AND subject = $2
         ORDER BY scope",
    )
    .bind(REGISTRY_RESOURCE)
    .bind(subject)
    .fetch_all(pool)
    .await?)
}

fn powers(rows: &[(String, Value, bool)]) -> Vec<Value> {
    rows.iter().map(|(_, powers, _)| powers.clone()).collect()
}

async fn selected_name(pool: &PgPool) -> Result<(Option<String>, Option<String>, Option<String>)> {
    Ok(sqlx::query_as(
        "SELECT name.resource_id::text, name.surface_binding_id::text, binding.authority_arm
         FROM name_current name
         LEFT JOIN surface_bindings binding ON binding.surface_binding_id = name.surface_binding_id
         WHERE name.logical_name_id = $1",
    )
    .bind(LOGICAL)
    .fetch_one(pool)
    .await?)
}

/// `permissions_current` keeps only rows with effective powers, so a subject whose latest row on
/// the registry-only resource is a revocation has no row there at all.
fn assert_no_control(rows: &[(String, Value, bool)], shape: &str) {
    assert!(
        rows.is_empty(),
        "{shape}: the prior registry owner keeps effective powers on the registry-only resource: {rows:?}"
    );
}

#[tokio::test]
async fn reclaim_revocations_clear_the_registrant_on_the_registry_only_resource() -> Result<()> {
    let (database, pool) = test_database("migration_predecessor_permissions").await?;
    seed(&pool, true, REGISTRANT).await?;
    run(&pool, HANDOFF_BLOCK, None).await?;
    let handed_off = registry_rows(&pool, REGISTRANT).await?;
    assert_eq!(
        powers(&handed_off),
        [json!(["resolver_control"]), json!(["resource_control"])],
        "before the migration the registrant controls the registry-only resource: {handed_off:?}"
    );

    run(&pool, MIGRATION_BLOCK, Some(HANDOFF_BLOCK)).await?;
    let incremental = registry_rows(&pool, REGISTRANT).await?;
    assert_no_control(&incremental, "incremental");
    let (resource, binding, arm) = selected_name(&pool).await?;
    assert_eq!(
        (resource.as_deref(), binding.as_deref(), arm.as_deref()),
        (Some(V2_RESOURCE), Some(V2_BINDING), Some("ens_v2")),
        "the ENSv2 registration stays the selected name"
    );
    database.cleanup().await?;

    let (database, pool) = test_database("migration_predecessor_permissions_zero").await?;
    seed(&pool, true, REGISTRANT).await?;
    run(&pool, MIGRATION_BLOCK, None).await?;
    let from_zero = registry_rows(&pool, REGISTRANT).await?;
    assert_no_control(&from_zero, "from zero");
    assert_eq!(from_zero, incremental);
    database.cleanup().await?;
    Ok(())
}

/// The Graveyard became registry owner before the migration, so the reclaim's revocations name
/// the Graveyard. Reconciliation removes the migration's own grants to the Graveyard on the
/// registry-only resource and the revocations that close them, but the reclaim's revocations
/// close grants made before the transaction and reach Project; without them the Graveyard's
/// handoff-block grants would stay the latest rows on that resource.
#[tokio::test]
async fn reclaim_revocations_clear_a_prior_graveyard_owner_on_the_registry_only_resource()
-> Result<()> {
    let (database, pool) = test_database("migration_predecessor_permissions_graveyard").await?;
    seed(&pool, true, GRAVEYARD).await?;
    run(&pool, HANDOFF_BLOCK, None).await?;
    assert_no_control(&registry_rows(&pool, REGISTRANT).await?, "handoff");
    let handed_off = registry_rows(&pool, GRAVEYARD).await?;
    assert_eq!(
        powers(&handed_off),
        [json!(["resolver_control"]), json!(["resource_control"])],
        "before the migration the Graveyard controls the registry-only resource: {handed_off:?}"
    );

    run(&pool, MIGRATION_BLOCK, Some(HANDOFF_BLOCK)).await?;
    let incremental = registry_rows(&pool, GRAVEYARD).await?;
    assert_no_control(&incremental, "incremental");
    assert_no_control(&registry_rows(&pool, REGISTRANT).await?, "incremental");
    let (resource, binding, arm) = selected_name(&pool).await?;
    assert_eq!(
        (resource.as_deref(), binding.as_deref(), arm.as_deref()),
        (Some(V2_RESOURCE), Some(V2_BINDING), Some("ens_v2")),
        "the ENSv2 registration stays the selected name"
    );
    database.cleanup().await?;

    let (database, pool) =
        test_database("migration_predecessor_permissions_graveyard_zero").await?;
    seed(&pool, true, GRAVEYARD).await?;
    run(&pool, MIGRATION_BLOCK, None).await?;
    let from_zero = registry_rows(&pool, GRAVEYARD).await?;
    assert_no_control(&from_zero, "from zero");
    assert_no_control(&registry_rows(&pool, REGISTRANT).await?, "from zero");
    assert_eq!(from_zero, incremental);
    let (resource, _, arm) = selected_name(&pool).await?;
    assert_eq!(
        (resource.as_deref(), arm.as_deref()),
        (Some(V2_RESOURCE), Some("ens_v2"))
    );
    database.cleanup().await?;
    Ok(())
}

/// Project takes the latest surviving row per resource, subject and scope. It does not treat the
/// registry-only binding's closure as a revocation, so when Interpret drops the reclaim's
/// revocations the registrant's handoff grants remain the registry-only resource's latest rows.
/// This is the defect the adapter fix closes; Project's contract is unchanged.
#[tokio::test]
async fn without_the_reclaim_revocations_the_handoff_grants_stay_effective() -> Result<()> {
    let (database, pool) = test_database("migration_predecessor_permissions_dropped").await?;
    seed(&pool, false, REGISTRANT).await?;
    run(&pool, MIGRATION_BLOCK, None).await?;
    let rows = registry_rows(&pool, REGISTRANT).await?;
    assert_eq!(
        powers(&rows),
        [json!(["resolver_control"]), json!(["resource_control"])],
        "{rows:?}"
    );
    let (resource, _, arm) = selected_name(&pool).await?;
    assert_eq!(
        (resource.as_deref(), arm.as_deref()),
        (Some(V2_RESOURCE), Some("ens_v2"))
    );
    database.cleanup().await?;
    Ok(())
}
