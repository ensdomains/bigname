use anyhow::{Context, Result};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::raw_sql;

use super::stage::{AUTHORITY_EVENTS, SELECTED_BINDINGS};
use crate::{Marker, scope, stage};

const CHAIN: &str = "authority-events-plan";
const NAMES: i64 = 600;

const BASELINE: &[&str] = &[
    include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
    include_str!("../../../../../schema-v2/baseline/02_raw_facts.sql"),
    include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
    include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
    include_str!("../../../../../schema-v2/baseline/05_normalized_events.sql"),
    include_str!("../../../../../schema-v2/baseline/06_projections.sql"),
    include_str!("../../../../../schema-v2/baseline/07_labels.sql"),
    include_str!("../../../../../schema-v2/baseline/08_heartbeats.sql"),
    include_str!("../../../../../schema-v2/baseline/09_divergence.sql"),
    include_str!("../../../../../schema-v2/baseline/10_phase_state.sql"),
];

/// Every name is a `.eth` lease handed to a registry-only binding by a transfer without
/// `reclaim`, then renewed: a grant and a renewal that carry no name, the named transfer and
/// authority epoch, and as many registry rows that carry no name and belong to no staged name.
const SEED: &str = "
    INSERT INTO chain_lineage (
        chain_id, block_hash, block_number, block_timestamp, canonicality_state
    )
    SELECT 'authority-events-plan', '0x' || lpad(to_hex(block), 64, '0'), block,
           '2026-08-01T00:00:00Z'::timestamptz + make_interval(secs => block), 'canonical'
    FROM generate_series(8, 11) AS blocks(block);
    CREATE TEMP TABLE plan_names ON COMMIT DROP AS
    SELECT name,
           '0xa' || lpad(to_hex(name), 63, '0') AS namehash,
           'ens:0xa' || lpad(to_hex(name), 63, '0') AS logical_name_id,
           ('00000000-0000-0000-0001-' || lpad(to_hex(name), 12, '0'))::uuid AS registrar,
           ('00000000-0000-0000-0002-' || lpad(to_hex(name), 12, '0'))::uuid AS registry,
           ('00000000-0000-0000-0011-' || lpad(to_hex(name), 12, '0'))::uuid AS registrar_binding,
           ('00000000-0000-0000-0012-' || lpad(to_hex(name), 12, '0'))::uuid AS registry_binding
    FROM generate_series(1, 600) AS names(name);
    INSERT INTO name_surfaces (
        logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
        namehash, labelhashes, normalizer_version, visibility_state,
        chain_id, block_hash, block_number, canonicality_state
    )
    SELECT logical_name_id, 'ens', 'plan-' || name || '.eth',
           ARRAY['plan-' || name, 'eth'], decode('00', 'hex'), namehash,
           ARRAY['0x' || lpad(to_hex(name), 64, '0'), '0x' || lpad('2', 64, '0')],
           'test', 'active', 'authority-events-plan', '0x' || lpad('8', 64, '0'), 8,
           'canonical'
    FROM plan_names;
    INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state)
    SELECT resource, 'authority-events-plan', '0x' || lpad('8', 64, '0'), 8, 'canonical'
    FROM plan_names CROSS JOIN LATERAL (VALUES (registrar), (registry)) AS pair(resource);
    INSERT INTO surface_bindings (
        surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm,
        active_from, active_to, chain_id, block_hash, block_number, canonicality_state,
        provenance
    )
    SELECT binding.id, logical_name_id, binding.resource, 'declared_registry_path', 'ens_v1',
           binding.active_from, binding.active_to, 'authority-events-plan',
           '0x' || lpad(to_hex(binding.block), 64, '0'), binding.block, 'canonical',
           jsonb_build_object('transaction_index', 0, 'log_index', binding.log)
    FROM plan_names CROSS JOIN LATERAL (VALUES
        (registrar_binding, registrar, 8, 1, '2026-07-01T00:00:00Z'::timestamptz,
         '2026-08-01T00:00:10Z'::timestamptz),
        (registry_binding, registry, 10, 0, '2026-08-01T00:00:10Z'::timestamptz, NULL)
    ) AS binding(id, resource, block, log, active_from, active_to);
    INSERT INTO normalized_events (
        event_identity, namespace, logical_name_id, resource_id, event_kind,
        source_family, manifest_version, chain_id, block_number, block_hash,
        transaction_hash, transaction_index, log_index, derivation_kind,
        canonicality_state, after_state, raw_fact_ref
    )
    SELECT 'plan:' || name || ':' || fact.kind || ':' || fact.block,
           'ens', CASE WHEN fact.named THEN logical_name_id END,
           CASE WHEN fact.on_registry THEN registry ELSE registrar END, fact.kind,
           CASE WHEN fact.on_registry THEN 'ens_v1_registry_l1' ELSE 'ens_v1_registrar_l1' END,
           1, 'authority-events-plan', fact.block, '0x' || lpad(to_hex(fact.block), 64, '0'),
           '0x' || lpad(to_hex(name * 16 + fact.block), 64, '0'), 0, fact.log,
           'ens_v1_unwrapped_authority', 'canonical',
           CASE WHEN fact.on_registry
               THEN jsonb_build_object('authority_kind', 'registry_only')
               ELSE jsonb_build_object(
                   'authority_kind', 'registrar', 'namehash', namehash,
                   'registrant', '0x5555555555555555555555555555555555555555',
                   'to', '0x6666666666666666666666666666666666666666',
                   'expiry', 1700000000 + fact.block
               )
           END,
           '{}'::jsonb
    FROM plan_names CROSS JOIN LATERAL (VALUES
        ('RegistrationGranted', 8, 1, false, false),
        ('ExpiryChanged', 8, 2, false, false),
        ('TokenControlTransferred', 10, 0, true, false),
        ('AuthorityEpochChanged', 10, 7, true, true),
        ('RegistrationRenewed', 11, 1, false, false),
        ('ExpiryChanged', 11, 2, false, false)
    ) AS fact(kind, block, log, named, on_registry);
    INSERT INTO normalized_events (
        event_identity, namespace, event_kind, source_family, manifest_version, chain_id,
        block_number, block_hash, transaction_hash, transaction_index, log_index,
        derivation_kind, canonicality_state, after_state, raw_fact_ref
    )
    SELECT 'plan:nameless:' || value, 'ens', 'AuthorityTransferred', 'ens_v1_registry_l1', 1,
           'authority-events-plan', 9, '0x' || lpad('9', 64, '0'),
           '0x' || lpad(to_hex(100000 + value), 64, '0'), 1, value,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object(
               'node', '0xf' || lpad(to_hex(value), 63, '0'),
               'owner', '0x7777777777777777777777777777777777777777'
           ),
           '{}'::jsonb
    FROM generate_series(1, 3600) AS nameless(value);
    ANALYZE";

/// A database seeded with [`SEED`] plus `extra`, staged the way a full rebuild stages it, up to
/// the point where `build.sql` runs.
async fn staged(
    prefix: &str,
    extra: &str,
) -> Result<(
    TestDatabase,
    sqlx::Transaction<'static, sqlx::Postgres>,
    Marker,
)> {
    let database = TestDatabase::create(TestDatabaseConfig::new(prefix)).await?;
    let mut transaction = database.pool().begin().await?;
    raw_sql("CREATE SCHEMA bigname_phase; SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *transaction)
        .await?;
    for script in BASELINE {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    raw_sql(SEED).execute(&mut *transaction).await?;
    raw_sql(extra).execute(&mut *transaction).await?;

    let target = Marker {
        number: 11,
        hash: format!("0x{:064x}", 11),
    };
    stage::prepare(&mut transaction, CHAIN, &target).await?;
    scope::initialize(
        &mut transaction,
        CHAIN,
        &target,
        scope::Window {
            previous: None,
            from_block: 8,
            to_block: 11,
            full_rebuild: true,
            retain_retracted: false,
        },
    )
    .await?;
    stage::inputs(&mut transaction, CHAIN, &target, true).await?;
    super::stage::prepare(&mut transaction).await?;
    Ok((database, transaction, target))
}

/// The plan Postgres chooses for the real staged statement over a seeded database, staged the way
/// a full rebuild stages it. The events-to-names join must read each side once: a nested loop
/// there re-reads one relation per row of the other, which is what a rebuild over all of mainnet
/// cannot afford.
#[tokio::test]
async fn authority_events_joins_names_with_a_hash_or_merge_join() -> Result<()> {
    let (database, mut transaction, target) =
        staged("name_authority_events_plan", "SELECT 1").await?;
    sqlx::query(include_str!("build.sql"))
        .bind(CHAIN)
        .bind(target.number)
        .bind(&target.hash)
        .execute(&mut *transaction)
        .await?;
    for statement in SELECTED_BINDINGS {
        sqlx::query(statement).execute(&mut *transaction).await?;
    }

    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {AUTHORITY_EVENTS}"))
        .fetch_one(&mut *transaction)
        .await?;
    eprintln!("{}", outline(&plan[0]["Plan"], 0));
    let join = names_join(&plan[0]["Plan"]).context("no node joins events to names")?;
    let node_type = join["Node Type"].as_str().unwrap_or_default();
    assert!(
        matches!(node_type, "Hash Join" | "Merge Join"),
        "events are joined to names by a {node_type}:\n{}",
        outline(&plan[0]["Plan"], 0)
    );
    let condition = join["Hash Cond"]
        .as_str()
        .or_else(|| join["Merge Cond"].as_str())
        .unwrap_or_default();
    assert!(
        condition.contains("event.logical_name_id") && condition.contains("authority."),
        "the join condition is not the name equality: {condition}"
    );

    // The plan is of a statement that does its work: every name is selected through its
    // registry-only binding and keeps the late renewal of the lease it retained.
    sqlx::query(AUTHORITY_EVENTS)
        .execute(&mut *transaction)
        .await?;
    let (registry_only_names, late_renewals): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM project_name_authority authority
                 JOIN project_bindings binding USING (logical_name_id)
                 WHERE binding.resource_id::text LIKE '00000000-0000-0000-0002-%'),
                (SELECT count(*) FROM project_authority_events
                 WHERE event_kind = 'RegistrationRenewed' AND block_number = 11)",
    )
    .fetch_one(&mut *transaction)
    .await?;
    assert_eq!((registry_only_names, late_renewals), (NAMES, NAMES));

    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

/// Registry ownership rows for the seeded names: every name recorded by the 2017 registry, and
/// every even-numbered name recorded by the current registry too.
const REGISTRY_RECORDS: &str = "
    INSERT INTO normalized_events (
        event_identity, namespace, event_kind, source_family, manifest_version, chain_id,
        block_number, block_hash, transaction_hash, transaction_index, log_index,
        derivation_kind, canonicality_state, after_state, raw_fact_ref
    )
    SELECT 'plan:registry:' || name || ':' || record.role, 'ens', 'AuthorityTransferred',
           'ens_v1_registry_l1', 1, 'authority-events-plan', 9, '0x' || lpad('9', 64, '0'),
           '0x' || lpad(to_hex(200000 + name * 2 + record.log), 64, '0'), 2, record.log,
           'ens_v1_unwrapped_authority', 'canonical',
           jsonb_build_object(
               'source_event', 'NewOwner', 'node', '0x' || lpad('e', 64, '0'),
               'child_node', namehash, 'emitter_role', record.role,
               'owner', '0x7777777777777777777777777777777777777777'
           ),
           '{}'::jsonb
    FROM plan_names CROSS JOIN (VALUES ('registry_old', 0), ('registry', 1)) AS record(role, log)
    WHERE record.role = 'registry_old' OR name % 2 = 0;
    ANALYZE";

/// The registry records are joined to the staged names by namehash. That join must read each
/// side once, for the same reason as the events-to-names join.
#[tokio::test]
async fn registry_records_join_names_with_a_hash_or_merge_join() -> Result<()> {
    let (database, mut transaction, target) =
        staged("name_authority_registry_records_plan", REGISTRY_RECORDS).await?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (FORMAT JSON) {}",
        include_str!("build.sql")
    ))
    .bind(CHAIN)
    .bind(target.number)
    .bind(&target.hash)
    .fetch_one(&mut *transaction)
    .await?;
    let joins = namehash_joins(&plan[0]["Plan"]);
    assert!(
        !joins.is_empty(),
        "no join on the namehash:\n{}",
        outline(&plan[0]["Plan"], 0)
    );
    for join in joins {
        let node_type = join["Node Type"].as_str().unwrap_or_default();
        assert!(
            matches!(node_type, "Hash Join" | "Merge Join"),
            "registry records are joined to names by a {node_type}:\n{}",
            outline(&plan[0]["Plan"], 0)
        );
    }

    // The plan is of a statement that does its work.
    sqlx::query(include_str!("build.sql"))
        .bind(CHAIN)
        .bind(target.number)
        .bind(&target.hash)
        .execute(&mut *transaction)
        .await?;
    let generations: Vec<(Option<String>, Option<i64>, i64)> = sqlx::query_as(
        "SELECT registry_generation, registry_handoff_block_number, count(*)
         FROM project_name_authority GROUP BY 1, 2 ORDER BY 1, 2",
    )
    .fetch_all(&mut *transaction)
    .await?;
    assert_eq!(
        generations,
        [
            (Some("current".to_owned()), Some(9), NAMES / 2),
            (Some("old".to_owned()), None, NAMES / 2),
        ]
    );

    transaction.rollback().await?;
    database.cleanup().await?;
    Ok(())
}

/// Every join whose own condition compares a registry record's node to a surface's namehash.
fn namehash_joins(node: &Value) -> Vec<&Value> {
    let mut joins = node["Plans"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(namehash_joins)
        .collect::<Vec<_>>();
    let compares_namehash = ["Hash Cond", "Merge Cond", "Join Filter"]
        .iter()
        .any(|key| {
            node[*key].as_str().is_some_and(|condition| {
                condition.contains("namehash") && condition.contains("child_node")
            })
        });
    if compares_namehash {
        joins.push(node);
    }
    joins
}

/// The lowest join that has both the staged events and the selected authorities beneath it,
/// leaving out subplans: those belong to the row filter, not to the join.
fn names_join(node: &Value) -> Option<&Value> {
    let children = || {
        node["Plans"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|child| {
                !matches!(
                    child["Parent Relationship"].as_str(),
                    Some("SubPlan" | "InitPlan")
                )
            })
    };
    children().find_map(names_join).or_else(|| {
        (reads(node, "event") && reads(node, "authority") && children().count() == 2)
            .then_some(node)
    })
}

fn reads(node: &Value, alias: &str) -> bool {
    node["Alias"].as_str() == Some(alias)
        || node["Plans"].as_array().is_some_and(|children| {
            children.iter().any(|child| {
                !matches!(
                    child["Parent Relationship"].as_str(),
                    Some("SubPlan" | "InitPlan")
                ) && reads(child, alias)
            })
        })
}

fn outline(node: &Value, depth: usize) -> String {
    let mut line = format!(
        "{}{} {} {}\n",
        "  ".repeat(depth),
        node["Node Type"].as_str().unwrap_or_default(),
        node["Relation Name"].as_str().unwrap_or_default(),
        node["Alias"].as_str().unwrap_or_default(),
    );
    for child in node["Plans"].as_array().into_iter().flatten() {
        line.push_str(&outline(child, depth + 1));
    }
    line
}
