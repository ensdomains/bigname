//! NameWrapper holder, operator, and delegate permission rows converge across full,
//! incremental, and redo builds, and the per-resource summary carries the wrapper
//! `resource_restrictions` block while the name is wrapped.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L238 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L421-L437 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L443-L470 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1058-L1068 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L105-L117 @ ens_v1@91c966f)

#[path = "support/bounded_registration.rs"]
mod bounded_registration;

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-mainnet";
const WRAPPER: &str = "0x00000000000000000000000000000000000026aa";
const WRAPPER_INSTANCE: &str = "00000000-0000-0000-0000-0000000026aa";
const NODE: &str = "0x3c98d1a1ba0bf053d695dc718d447ff5afcbfaacfc9b35d81f7491d1a140793c";
const NODE2: &str = "0x4b162e8ef5a976a025f29a8308523ae94e4f248a0db2d87addd10ce0ec703d84";
const RESOURCE: &str = "d8e91e79-2955-5d4a-880c-555a1a7de209";
const RESOURCE2: &str = "8c165352-a09a-58cb-8a53-cd6da17dda31";
const LINEAGE: &str = "d34c685f-2410-51ae-ac59-d7f9d9da824d";
const LINEAGE2: &str = "1c4c685f-2410-51ae-ac59-d7f9d9da824d";
const HOLDER: &str = "0x00000000000000000000000000000000000000a1";
const NEXT_HOLDER: &str = "0x00000000000000000000000000000000000000a2";
const OPERATOR: &str = "0x00000000000000000000000000000000000000e1";
const NEXT_OPERATOR: &str = "0x00000000000000000000000000000000000000e2";
const SECOND_OPERATOR: &str = "0x00000000000000000000000000000000000000e3";
const DELEGATE: &str = "0x00000000000000000000000000000000000000d1";
const EXPIRY: i64 = 1_900_000_000;
const PARENT_CANNOT_CONTROL: i64 = 1 << 16;
const IS_DOT_ETH: i64 = 1 << 17;
const CANNOT_UNWRAP: i64 = 1;
const CANNOT_SET_RESOLVER: i64 = 8;
const CAN_EXTEND_EXPIRY: i64 = 1 << 18;
const HOLDER_POWERS: &[&str] = &[
    "resource_control",
    "set_resolver",
    "set_ttl",
    "create_subnames",
    "transfer",
    "unwrap",
    "burn_fuses",
    "approve",
    "extend_subname_expiry",
    "extend_expiry",
];
const TABLES: &str =
    "permissions_current account_permission_state_current permissions_current_resource_summary";

fn hash(block: i64) -> String {
    format!("0x{block:064x}")
}

fn timestamp(block: i64) -> i64 {
    1_700_000_000 + block * 1_000
}

/// The second wrapped name expires between blocks 15 and 16.
const EXPIRY2: i64 = 1_700_015_500;

fn logical(node: &str) -> String {
    format!("ens:{node}")
}

#[rustfmt::skip]
async fn database(prefix: &str) -> Result<(TestDatabase, PgPool)> {
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

#[allow(clippy::too_many_arguments)]
async fn event(
    pool: &PgPool,
    node: Option<&str>,
    resource: Option<&str>,
    block: i64,
    log: i64,
    kind: &str,
    suffix: &str,
    before: Value,
    after: Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             transaction_hash, transaction_index, log_index, derivation_kind,
             canonicality_state, before_state, after_state
         ) VALUES ($1, 'ens', $2, $3::uuid, $4, 'ens_v1_wrapper_l1', 1, $5, $6, $7,
                   $8, 0, $9, $10, 'canonical', $11, $12)",
    )
    .bind(format!(
        "{block}:{log}:{kind}:{}:{suffix}",
        node.unwrap_or("account")
    ))
    .bind(node.map(logical))
    .bind(resource)
    .bind(kind)
    .bind(CHAIN)
    .bind(block)
    .bind(hash(block))
    .bind(format!("0x{:064x}", 7_000 + block))
    .bind(log)
    .bind(if kind == "AccountPermissionChanged" {
        "standard_approval"
    } else {
        "ens_v1_unwrapped_authority"
    })
    .bind(before)
    .bind(after)
    .execute(pool)
    .await?;
    Ok(())
}

fn authority_key(node: &str) -> String {
    format!("wrapper:{CHAIN}:1:{node}:{}:0", hash(10))
}

fn grant_source(node: &str, relation: &str, source_event: &str) -> Value {
    json!({
        "kind": "ens_v1_authority",
        "authority_kind": "wrapper",
        "authority_key": authority_key(node),
        "authority_contract": WRAPPER,
        "relation_kind": relation,
        "node": node,
        "source_event_kind": source_event,
    })
}

fn permission_state(
    subject: &str,
    powers: &[&str],
    node: &str,
    relation: &str,
    source_event: &str,
    grant: bool,
) -> Value {
    let source = grant_source(node, relation, source_event);
    let transfer_behavior = if relation == "token_approval" {
        "cleared_on_transfer_unless_cannot_approve"
    } else {
        "replace_on_authority_change"
    };
    json!({
        "subject": subject,
        "scope": {"kind": "resource"},
        "effective_powers": if grant { json!(powers) } else { json!([]) },
        "grant_source": if grant { source.clone() } else { Value::Null },
        "revocation_source": if grant { Value::Null } else { source },
        "inheritance_path": [],
        "transfer_behavior": transfer_behavior,
    })
}

#[allow(clippy::too_many_arguments)]
async fn permission(
    pool: &PgPool,
    node: &str,
    resource: &str,
    block: i64,
    subject: &str,
    powers: &[&str],
    relation: &str,
    source_event: &str,
    grant: bool,
) -> Result<()> {
    let before = permission_state(subject, powers, node, relation, source_event, !grant);
    let after = permission_state(subject, powers, node, relation, source_event, grant);
    let suffix = format!(
        "{relation}:{}:{subject}",
        if grant { "grant" } else { "revoke" }
    );
    event(
        pool,
        Some(node),
        Some(resource),
        block,
        0,
        "PermissionChanged",
        &suffix,
        before,
        after,
    )
    .await
}

async fn fuses(pool: &PgPool, node: &str, resource: &str, block: i64, fuses: i64) -> Result<()> {
    event(
        pool,
        Some(node),
        Some(resource),
        block,
        0,
        "PermissionScopeChanged",
        "fuses",
        json!({}),
        json!({
            "source_event": if block == 10 { "NameWrapped" } else { "FusesSet" },
            "node": node,
            "fuses": fuses,
            "wrapper_state": if fuses & CANNOT_UNWRAP != 0 { "locked" } else if fuses & PARENT_CANNOT_CONTROL == 0 { "wrapped" } else { "emancipated" },
            "expiry": if node == NODE { EXPIRY } else { EXPIRY2 },
        }),
    )
    .await
}

async fn approval(
    pool: &PgPool,
    block: i64,
    owner: &str,
    operator: &str,
    approved: bool,
) -> Result<()> {
    let source = json!({"kind": "raw_log", "source_event": "ApprovalForAll"});
    event(
        pool,
        None,
        None,
        block,
        0,
        "AccountPermissionChanged",
        &format!("{owner}:{operator}"),
        json!({}),
        json!({
            "subject": operator,
            "relation_kind": "operator",
            "approved": approved,
            "scope": {
                "kind": "account",
                "chain_id": CHAIN,
                "authority_kind": "wrapper",
                "authority_contract": WRAPPER,
                "authority_contract_instance_id": WRAPPER_INSTANCE,
                "owner": owner,
            },
            "effective_powers": if approved { json!(["wrapper_control"]) } else { json!([]) },
            "grant_source": if approved { source.clone() } else { json!({}) },
            "revocation_source": if approved { Value::Null } else { source },
            "inheritance_path": [],
            "transfer_behavior": {"mode": "owner_scoped", "on_holder_change": "ceases_to_apply"},
            "source_event": "ApprovalForAll",
        }),
    )
    .await
}

#[rustfmt::skip]
async fn seed_identity(pool: &PgPool) -> Result<()> {
    for block in 10..=16 {
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
             VALUES ($1, $2, $3, to_timestamp($4), 'canonical')",
        )
        .bind(CHAIN).bind(hash(block)).bind(block).bind(timestamp(block) as f64)
        .execute(pool)
        .await?;
    }
    let (h10, main, second) = (hash(10), logical(NODE), logical(NODE2));
    raw_sql(&format!(
        "INSERT INTO token_lineages (token_lineage_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ('{LINEAGE}', '{CHAIN}', '{h10}', 10, 'canonical'), ('{LINEAGE2}', '{CHAIN}', '{h10}', 10, 'canonical');
         INSERT INTO resources (resource_id, token_lineage_id, chain_id, block_hash, block_number, canonicality_state)
         VALUES ('{RESOURCE}', '{LINEAGE}', '{CHAIN}', '{h10}', 10, 'canonical'), ('{RESOURCE2}', '{LINEAGE2}', '{CHAIN}', '{h10}', 10, 'canonical');
         INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
             labelhashes, normalizer_version, visibility_state, chain_id, block_hash, block_number, canonicality_state)
         VALUES
           ('{main}', 'ens', 'wrapped.xyz', ARRAY['wrapped','xyz'], '\\x07777261707065640378797a00', '{NODE}', ARRAY['0x{:064x}','0x{:064x}'], 'ensip15', 'active', '{CHAIN}', '{h10}', 10, 'canonical'),
           ('{second}', 'ens', 'sub.wrapped.xyz', ARRAY['sub','wrapped','xyz'], '\\x0373756207777261707065640378797a00', '{NODE2}', ARRAY['0x{:064x}','0x{:064x}','0x{:064x}'], 'ensip15', 'active', '{CHAIN}', '{h10}', 10, 'canonical');
         INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm,
             active_from, chain_id, block_hash, block_number, canonicality_state)
         VALUES
           ('6347b94d-744e-5e3c-a8a9-38cefbcf0e25', '{main}', '{RESOURCE}', 'declared_registry_path', 'ens_v1', to_timestamp({}), '{CHAIN}', '{h10}', 10, 'canonical'),
           ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', '{second}', '{RESOURCE2}', 'declared_registry_path', 'ens_v1', to_timestamp({}), '{CHAIN}', '{h10}', 10, 'canonical')",
        1_u64, 2_u64, 3_u64, 1_u64, 2_u64, timestamp(10), timestamp(10),
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Block 10 `NameWrapped` rows for one name: expiry, token mint, fuse word, holder grant.
#[rustfmt::skip]
async fn wrap(pool: &PgPool, node: &str, resource: &str, expiry: i64, word: i64) -> Result<()> {
    event(pool, Some(node), Some(resource), 10, 0, "ExpiryChanged", "wrap", json!({}), json!({
        "source_event": "NameWrapped", "node": node, "expiry": expiry,
        "authority_kind": "wrapper", "authority_key": authority_key(node),
    })).await?;
    event(pool, Some(node), Some(resource), 10, 0, "TokenControlTransferred", "wrap", json!({"from": null}), json!({
        "source_event": "NameWrapped", "node": node, "owner": HOLDER, "to": HOLDER, "fuses": word,
        "authority_kind": "wrapper", "authority_key": authority_key(node),
    })).await?;
    fuses(pool, node, resource, 10, word).await?;
    permission(pool, node, resource, 10, HOLDER, HOLDER_POWERS, "holder", "NameWrapped", true).await
}

#[rustfmt::skip]
async fn seed(pool: &PgPool) -> Result<()> {
    seed_identity(pool).await?;
    // The second name starts `wrapped` (its parent still controls it) and is emancipated at 12.
    wrap(pool, NODE, RESOURCE, EXPIRY, PARENT_CANNOT_CONTROL | IS_DOT_ETH).await?;
    wrap(pool, NODE2, RESOURCE2, EXPIRY2, 0).await?;
    approval(pool, 11, HOLDER, OPERATOR, true).await?;
    approval(pool, 11, HOLDER, SECOND_OPERATOR, true).await?;
    permission(pool, NODE, RESOURCE, 12, DELEGATE, &["extend_subname_expiry"], "token_approval", "Approval", true).await?;
    approval(pool, 12, HOLDER, DELEGATE, true).await?;
    fuses(pool, NODE2, RESOURCE2, 12, PARENT_CANNOT_CONTROL).await?;
    fuses(pool, NODE, RESOURCE, 13, PARENT_CANNOT_CONTROL | IS_DOT_ETH | CANNOT_SET_RESOLVER | CAN_EXTEND_EXPIRY).await?;
    approval(pool, 13, HOLDER, SECOND_OPERATOR, false).await?;
    permission(pool, NODE, RESOURCE, 14, HOLDER, HOLDER_POWERS, "holder", "TransferSingle", false).await?;
    permission(pool, NODE, RESOURCE, 14, NEXT_HOLDER, HOLDER_POWERS, "holder", "TransferSingle", true).await?;
    permission(pool, NODE, RESOURCE, 14, DELEGATE, &["extend_subname_expiry"], "token_approval", "TransferSingle", false).await?;
    approval(pool, 15, NEXT_HOLDER, NEXT_OPERATOR, true).await?;
    permission(pool, NODE, RESOURCE, 16, NEXT_HOLDER, HOLDER_POWERS, "holder", "NameUnwrapped", false).await?;
    event(pool, Some(NODE), Some(RESOURCE), 16, 0, "AuthorityEpochChanged", "unwrap", json!({
        "authority_kind": "wrapper", "authority_key": authority_key(NODE),
    }), json!({
        "source_event": "NameUnwrapped", "node": NODE, "owner": NEXT_HOLDER,
        "unwrapped_at": timestamp(16), "authority_kind": null, "authority_key": null,
        "reactivated_resource_id": null, "reactivated_token_lineage_id": null,
    })).await?;
    Ok(())
}

async fn run(pool: &PgPool, target: i64, resume: Option<Marker>) -> Result<Marker> {
    let current = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: target,
            affected_from_block: resume.as_ref().map_or(10, |marker| marker.number + 1),
            affected_to_block: target,
            resume_current: resume,
            mode: RunMode::Normal,
        })
        .await?
        .current;
    bounded_registration::assert_selected_registrations_are_bounded(pool).await?;
    Ok(current)
}

async fn redo(pool: &PgPool, target: i64, block: i64) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: target,
            affected_from_block: block,
            affected_to_block: block,
            resume_current: None,
            mode: RunMode::Redo,
        })
        .await?;
    bounded_registration::assert_selected_registrations_are_bounded(pool).await?;
    Ok(())
}

async fn snapshot(pool: &PgPool) -> Result<Value> {
    let mut snapshot = serde_json::Map::new();
    for table in TABLES.split_whitespace() {
        let rows: Value = sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(value ORDER BY value::text), '[]'::jsonb)
             FROM (SELECT to_jsonb(row) - 'last_recomputed_at' - 'inserted_at'
                          - 'canonicality_summary' - 'chain_positions' AS value
                   FROM {table} row) canonical"
        ))
        .fetch_one(pool)
        .await?;
        snapshot.insert(table.to_owned(), rows);
    }
    Ok(Value::Object(snapshot))
}

/// `(subject, relation, powers)` per resource-scoped row of a registration.
async fn rows(pool: &PgPool, resource: &str) -> Result<Vec<(String, String, Value)>> {
    Ok(sqlx::query_as(
        "SELECT subject, grant_source ->> 'relation_kind', effective_powers
         FROM permissions_current
         WHERE resource_id = $1::uuid
         ORDER BY subject",
    )
    .bind(resource)
    .fetch_all(pool)
    .await?)
}

async fn restrictions(pool: &PgPool, resource: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT resource_restrictions FROM permissions_current_resource_summary
         WHERE resource_id = $1::uuid",
    )
    .bind(resource)
    .fetch_one(pool)
    .await?)
}

fn powers(without: &[&str]) -> Value {
    json!(
        HOLDER_POWERS
            .iter()
            .filter(|power| !without.contains(power))
            .collect::<Vec<_>>()
    )
}

fn holder(subject: &str, powers: Value) -> (String, String, Value) {
    (subject.to_owned(), "holder".to_owned(), powers)
}

fn operator(subject: &str, powers: Value) -> (String, String, Value) {
    (subject.to_owned(), "operator".to_owned(), powers)
}

/// `burn_fuses` needs `PARENT_CANNOT_CONTROL` and `extend_expiry` needs `CAN_EXTEND_EXPIRY`, so
/// an emancipated name without the latter fuse serves every holder power but `extend_expiry`.
fn emancipated() -> Value {
    powers(&["extend_expiry"])
}

/// A `wrapped` name cannot have its fuses burnt by its holder at all.
fn still_wrapped() -> Value {
    powers(&["burn_fuses", "extend_expiry"])
}

#[tokio::test]
async fn wrapper_holder_operator_and_delegate_rows_converge_across_build_modes() -> Result<()> {
    let (full_database, full) = database("wrapper_permissions_full").await?;
    seed(&full).await?;
    run(&full, 16, None).await?;
    let full_snapshot = snapshot(&full).await?;

    let (incremental_database, incremental) = database("wrapper_permissions_incremental").await?;
    seed(&incremental).await?;
    let mut marker = run(&incremental, 10, None).await?;
    assert_eq!(
        rows(&incremental, RESOURCE).await?,
        vec![holder(HOLDER, emancipated())]
    );
    assert_eq!(
        rows(&incremental, RESOURCE2).await?,
        vec![holder(HOLDER, still_wrapped())]
    );
    assert_eq!(
        restrictions(&incremental, RESOURCE2).await?,
        Some(json!({
            "kind": "ens_v1_wrapper",
            "wrapper_state": "wrapped",
            "fuses": 0,
            "expiry_seconds": EXPIRY2,
        }))
    );

    // ApprovalForAll fans the holder set out to each operator on every held registration.
    marker = run(&incremental, 11, Some(marker)).await?;
    assert_eq!(
        rows(&incremental, RESOURCE).await?,
        vec![
            holder(HOLDER, emancipated()),
            operator(OPERATOR, emancipated()),
            operator(SECOND_OPERATOR, emancipated()),
        ]
    );
    assert_eq!(
        rows(&incremental, RESOURCE2).await?,
        vec![
            holder(HOLDER, still_wrapped()),
            operator(OPERATOR, still_wrapped()),
            operator(SECOND_OPERATOR, still_wrapped()),
        ]
    );

    // The token approval carries only the `canExtendSubnames` branch, but a delegate who is also
    // an operator keeps the operator set; the parent emancipating the second name restores
    // `burn_fuses` there.
    marker = run(&incremental, 12, Some(marker)).await?;
    assert_eq!(
        rows(&incremental, RESOURCE).await?,
        vec![
            holder(HOLDER, emancipated()),
            operator(DELEGATE, emancipated()),
            operator(OPERATOR, emancipated()),
            operator(SECOND_OPERATOR, emancipated()),
        ]
    );
    assert_eq!(
        rows(&incremental, RESOURCE2).await?,
        vec![
            holder(HOLDER, emancipated()),
            operator(DELEGATE, emancipated()),
            operator(OPERATOR, emancipated()),
            operator(SECOND_OPERATOR, emancipated()),
        ]
    );

    // A burnt fuse masks holder and operator alike, `CAN_EXTEND_EXPIRY` unlocks `extend_expiry`,
    // the summary reports the word, and ApprovalForAll(false) removes an operator everywhere.
    marker = run(&incremental, 13, Some(marker)).await?;
    let masked = powers(&["set_resolver"]);
    assert_eq!(
        rows(&incremental, RESOURCE).await?,
        vec![
            holder(HOLDER, masked.clone()),
            operator(DELEGATE, masked.clone()),
            operator(OPERATOR, masked.clone()),
        ]
    );
    assert_eq!(
        rows(&incremental, RESOURCE2).await?,
        vec![
            holder(HOLDER, emancipated()),
            operator(DELEGATE, emancipated()),
            operator(OPERATOR, emancipated()),
        ]
    );
    assert_eq!(
        restrictions(&incremental, RESOURCE).await?,
        Some(json!({
            "kind": "ens_v1_wrapper",
            "wrapper_state": "emancipated",
            "fuses": PARENT_CANNOT_CONTROL | IS_DOT_ETH | CANNOT_SET_RESOLVER | CAN_EXTEND_EXPIRY,
            "expiry_seconds": EXPIRY,
        }))
    );

    // A transfer revokes the old holder, the delegate, and the old holder's operators.
    marker = run(&incremental, 14, Some(marker)).await?;
    assert_eq!(
        rows(&incremental, RESOURCE).await?,
        vec![holder(NEXT_HOLDER, masked.clone())]
    );
    assert_eq!(
        rows(&incremental, RESOURCE2).await?,
        vec![
            holder(HOLDER, emancipated()),
            operator(DELEGATE, emancipated()),
            operator(OPERATOR, emancipated()),
        ]
    );

    // The new holder's later approval reaches a registration whose own events lie outside the
    // window, through the operator-holder scope rule.
    marker = run(&incremental, 15, Some(marker)).await?;
    assert_eq!(
        rows(&incremental, RESOURCE).await?,
        vec![
            holder(NEXT_HOLDER, masked.clone()),
            operator(NEXT_OPERATOR, masked),
        ]
    );
    assert!(restrictions(&incremental, RESOURCE).await?.is_some());

    // Unwrapping clears the registration and its restrictions block; the second name expires by
    // clock alone and loses its holder, its operators, and its restrictions block.
    run(&incremental, 16, Some(marker)).await?;
    assert_eq!(rows(&incremental, RESOURCE).await?, Vec::new());
    assert_eq!(restrictions(&incremental, RESOURCE).await?, None);
    assert_eq!(rows(&incremental, RESOURCE2).await?, Vec::new());
    assert_eq!(restrictions(&incremental, RESOURCE2).await?, None);
    assert_eq!(snapshot(&incremental).await?, full_snapshot);

    for block in [11, 12, 13, 14, 15, 16] {
        redo(&full, 16, block).await?;
        assert_eq!(
            snapshot(&full).await?,
            full_snapshot,
            "redo of block {block}"
        );
    }

    full_database.cleanup().await?;
    incremental_database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn wrapper_operator_rows_carry_fanout_provenance_and_owner() -> Result<()> {
    let (database, pool) = database("wrapper_permissions_provenance").await?;
    seed(&pool).await?;
    run(&pool, 13, None).await?;

    let (grant_source, transfer_behavior, provenance): (Value, Value, Value) = sqlx::query_as(
        "SELECT grant_source, transfer_behavior, provenance FROM permissions_current
         WHERE resource_id = $1::uuid AND subject = $2",
    )
    .bind(RESOURCE)
    .bind(OPERATOR)
    .fetch_one(&pool)
    .await?;
    assert_eq!(grant_source["relation_kind"], json!("operator"));
    assert_eq!(grant_source["source_event_kind"], json!("ApprovalForAll"));
    assert_eq!(grant_source["owner"], json!(HOLDER));
    assert_eq!(grant_source["authority_kind"], json!("wrapper"));
    assert_eq!(
        transfer_behavior,
        json!({"mode": "owner_scoped", "on_holder_change": "ceases_to_apply"})
    );
    assert_eq!(
        provenance["derivation_kind"],
        json!("wrapper_operator_fanout")
    );
    assert_eq!(provenance["holder"], json!(HOLDER));
    assert!(provenance["operator_normalized_event_ids"].is_array());

    // The delegate who is also an operator carries the operator row and remembers the merge.
    let (grant_source, provenance): (Value, Value) = sqlx::query_as(
        "SELECT grant_source, provenance FROM permissions_current
         WHERE resource_id = $1::uuid AND subject = $2",
    )
    .bind(RESOURCE)
    .bind(DELEGATE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(grant_source["relation_kind"], json!("operator"));
    assert_eq!(
        provenance["superseded_relation_kind"],
        json!("token_approval")
    );

    let (authority_kind, powers): (String, Value) = sqlx::query_as(
        "SELECT authority_kind, effective_powers FROM account_permission_state_current
         WHERE owner = $1 AND subject = $2",
    )
    .bind(HOLDER)
    .bind(OPERATOR)
    .fetch_one(&pool)
    .await?;
    assert_eq!(authority_kind, "wrapper");
    assert_eq!(powers, json!(["wrapper_control"]));

    let (support_status, reason): (String, Option<String>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason FROM permissions_current_resource_summary
         WHERE resource_id = $1::uuid",
    )
    .bind(RESOURCE)
    .fetch_one(&pool)
    .await?;
    assert_eq!(support_status, "unsupported");
    assert_eq!(
        reason.as_deref(),
        Some("wrapper_parent_and_resolver_delegation_not_projected")
    );

    database.cleanup().await?;
    Ok(())
}

const REGISTRAR_RESOURCE: &str = "9a7c0c1e-5b2d-5a3e-8f10-00000000d0d0";

/// Full and incremental snapshots agree, and every redo of `blocks` reproduces the full snapshot.
async fn assert_converges(
    prefix: &str,
    seed_events: impl AsyncFn(&PgPool) -> Result<()>,
    target: i64,
    blocks: &[i64],
    check: impl AsyncFn(&PgPool, i64) -> Result<()>,
) -> Result<()> {
    let (full_database, full) = database(&format!("{prefix}_full")).await?;
    seed_identity(&full).await?;
    seed_events(&full).await?;
    run(&full, target, None).await?;
    let full_snapshot = snapshot(&full).await?;
    check(&full, target).await?;

    let (incremental_database, incremental) = database(&format!("{prefix}_incremental")).await?;
    seed_identity(&incremental).await?;
    seed_events(&incremental).await?;
    let mut marker = run(&incremental, 10, None).await?;
    check(&incremental, 10).await?;
    for block in 11..=target {
        marker = run(&incremental, block, Some(marker)).await?;
        check(&incremental, block).await?;
    }
    assert_eq!(snapshot(&incremental).await?, full_snapshot);
    for block in blocks {
        redo(&full, target, *block).await?;
        assert_eq!(
            snapshot(&full).await?,
            full_snapshot,
            "redo of block {block}"
        );
    }
    full_database.cleanup().await?;
    incremental_database.cleanup().await?;
    Ok(())
}

// A transfer to the approved delegate: the interpreter emits the token-approval revocation
// before the holder rows, so the fold keeps the recipient's holder grant, and the recipient's
// own operators fan out from it.
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L837-L840 @ ens_v1@91c966f)
#[tokio::test]
async fn a_delegate_who_receives_the_token_becomes_its_holder() -> Result<()> {
    #[rustfmt::skip]
    async fn events(pool: &PgPool) -> Result<()> {
        wrap(pool, NODE, RESOURCE, EXPIRY, PARENT_CANNOT_CONTROL | IS_DOT_ETH).await?;
        approval(pool, 11, HOLDER, OPERATOR, true).await?;
        approval(pool, 11, DELEGATE, NEXT_OPERATOR, true).await?;
        permission(pool, NODE, RESOURCE, 12, DELEGATE, &["extend_subname_expiry"], "token_approval", "Approval", true).await?;
        permission(pool, NODE, RESOURCE, 13, DELEGATE, &["extend_subname_expiry"], "token_approval", "TransferSingle", false).await?;
        permission(pool, NODE, RESOURCE, 13, HOLDER, HOLDER_POWERS, "holder", "TransferSingle", false).await?;
        permission(pool, NODE, RESOURCE, 13, DELEGATE, HOLDER_POWERS, "holder", "TransferSingle", true).await
    }
    async fn check(pool: &PgPool, block: i64) -> Result<()> {
        let expected = match block {
            10 => vec![holder(HOLDER, emancipated())],
            11 => vec![
                holder(HOLDER, emancipated()),
                operator(OPERATOR, emancipated()),
            ],
            12 => vec![
                holder(HOLDER, emancipated()),
                (
                    DELEGATE.to_owned(),
                    "token_approval".to_owned(),
                    json!(["extend_subname_expiry"]),
                ),
                operator(OPERATOR, emancipated()),
            ],
            _ => vec![
                holder(DELEGATE, emancipated()),
                operator(NEXT_OPERATOR, emancipated()),
            ],
        };
        assert_eq!(rows(pool, RESOURCE).await?, expected, "block {block}");
        assert!(
            restrictions(pool, RESOURCE).await?.is_some(),
            "block {block}"
        );
        Ok(())
    }
    assert_converges("wrapper_delegate_recipient", events, 13, &[12, 13], check).await
}

// The `.eth` 2LD unwrap puts `AuthorityEpochChanged` on the reactivated registrar resource and
// only `SurfaceUnbound` on the wrapper resource; the un-admitted `upgrade()` path burns the token
// without any `NameUnwrapped`. Both close the wrapper restrictions block.
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L483-L509 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
#[tokio::test]
async fn wrapper_restrictions_close_on_a_2ld_unwrap_and_on_an_upgrade_burn() -> Result<()> {
    #[rustfmt::skip]
    async fn events(pool: &PgPool) -> Result<()> {
        sqlx::query(
            "INSERT INTO resources (resource_id, token_lineage_id, chain_id, block_hash, block_number, canonicality_state)
             VALUES ($1::uuid, NULL, $2, $3, 10, 'canonical')",
        )
        .bind(REGISTRAR_RESOURCE).bind(CHAIN).bind(hash(10))
        .execute(pool)
        .await?;
        wrap(pool, NODE, RESOURCE, EXPIRY, PARENT_CANNOT_CONTROL | IS_DOT_ETH).await?;
        wrap(pool, NODE2, RESOURCE2, EXPIRY2, PARENT_CANNOT_CONTROL).await?;
        approval(pool, 11, HOLDER, OPERATOR, true).await?;
        // 2LD unwrap with registrar reactivation.
        event(pool, Some(NODE), Some(RESOURCE), 12, 0, "SurfaceUnbound", "unwrap", json!({
            "authority_kind": "wrapper", "authority_key": authority_key(NODE),
        }), json!({
            "source_event": "NameUnwrapped", "node": NODE, "owner": HOLDER, "unwrapped_at": timestamp(12),
            "authority_kind": "wrapper", "authority_key": authority_key(NODE), "active_to": timestamp(12),
            "reactivated_resource_id": REGISTRAR_RESOURCE, "reactivated_token_lineage_id": null,
        })).await?;
        event(pool, Some(NODE), Some(REGISTRAR_RESOURCE), 12, 0, "AuthorityEpochChanged", "unwrap", json!({
            "authority_kind": "wrapper", "authority_key": authority_key(NODE),
        }), json!({
            "source_event": "NameUnwrapped", "node": NODE, "owner": HOLDER, "unwrapped_at": timestamp(12),
            "authority_kind": "registrar", "authority_key": format!("registrar:{CHAIN}:1:{NODE}"),
            "reactivated_resource_id": REGISTRAR_RESOURCE, "reactivated_token_lineage_id": null,
        })).await?;
        permission(pool, NODE, RESOURCE, 12, HOLDER, HOLDER_POWERS, "holder", "NameUnwrapped", false).await?;
        // upgrade(): a bare burn, no NameUnwrapped.
        permission(pool, NODE2, RESOURCE2, 12, HOLDER, HOLDER_POWERS, "holder", "TransferSingle", false).await
    }
    async fn check(pool: &PgPool, block: i64) -> Result<()> {
        for resource in [RESOURCE, RESOURCE2] {
            if block < 12 {
                assert!(
                    restrictions(pool, resource).await?.is_some(),
                    "block {block}"
                );
                assert!(!rows(pool, resource).await?.is_empty(), "block {block}");
            } else {
                assert_eq!(restrictions(pool, resource).await?, None, "block {block}");
                assert_eq!(rows(pool, resource).await?, Vec::new(), "block {block}");
            }
        }
        Ok(())
    }
    assert_converges("wrapper_unwrap_shapes", events, 12, &[11, 12], check).await
}

// With CANNOT_APPROVE burnt (which `_canFusesBeBurned` allows only on a locked name) the
// delegate survives both transfers: it becomes the holder, then passes the token on and is
// served as the delegate again through the re-emitted grant.
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L108-L121 @ ens_v1@91c966f)
// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1058-L1068 @ ens_v1@91c966f)
#[tokio::test]
async fn a_retained_delegate_who_passes_the_token_on_is_served_as_delegate_again() -> Result<()> {
    const CANNOT_APPROVE: i64 = 64;
    #[rustfmt::skip]
    async fn events(pool: &PgPool) -> Result<()> {
        wrap(pool, NODE, RESOURCE, EXPIRY, PARENT_CANNOT_CONTROL | CANNOT_UNWRAP | IS_DOT_ETH | CANNOT_APPROVE).await?;
        permission(pool, NODE, RESOURCE, 11, DELEGATE, &["extend_subname_expiry"], "token_approval", "Approval", true).await?;
        permission(pool, NODE, RESOURCE, 12, HOLDER, HOLDER_POWERS, "holder", "TransferSingle", false).await?;
        permission(pool, NODE, RESOURCE, 12, DELEGATE, HOLDER_POWERS, "holder", "TransferSingle", true).await?;
        permission(pool, NODE, RESOURCE, 13, DELEGATE, HOLDER_POWERS, "holder", "TransferSingle", false).await?;
        permission(pool, NODE, RESOURCE, 13, NEXT_HOLDER, HOLDER_POWERS, "holder", "TransferSingle", true).await?;
        permission(pool, NODE, RESOURCE, 13, DELEGATE, &["extend_subname_expiry"], "token_approval", "TransferSingle", true).await
    }
    async fn check(pool: &PgPool, block: i64) -> Result<()> {
        // Locked: `resource_control` and `unwrap` clear; CANNOT_APPROVE removes `approve`.
        let masked = powers(&["resource_control", "unwrap", "approve", "extend_expiry"]);
        let delegate = (
            DELEGATE.to_owned(),
            "token_approval".to_owned(),
            json!(["extend_subname_expiry"]),
        );
        let expected = match block {
            10 => vec![holder(HOLDER, masked)],
            11 => vec![holder(HOLDER, masked), delegate],
            12 => vec![holder(DELEGATE, masked)],
            _ => vec![holder(NEXT_HOLDER, masked), delegate],
        };
        assert_eq!(rows(pool, RESOURCE).await?, expected, "block {block}");
        Ok(())
    }
    assert_converges("wrapper_retained_delegate", events, 13, &[12, 13], check).await
}
