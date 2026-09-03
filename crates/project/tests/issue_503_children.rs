use anyhow::Result;
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const PARENT: &str = "ens:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHILD: &str = "ens:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const V1_RESOURCE: &str = "11111111-1111-1111-1111-111111111111";
const V1_WRAPPER_RESOURCE: &str = "77777777-7777-7777-7777-777777777777";
const V1_BINDING: &str = "22222222-2222-2222-2222-222222222222";
const V2_RESOURCE: &str = "33333333-3333-3333-3333-333333333333";
const V2_BINDING: &str = "44444444-4444-4444-4444-444444444444";
const REGISTRY: &str = "55555555-5555-5555-5555-555555555555";
const OTHER_REGISTRY: &str = "66666666-6666-6666-6666-666666666666";
const PARENT_REGISTRY: &str = "88888888-8888-8888-8888-888888888888";
const REGISTRY_ADDRESS: &str = "0x0000000000000000000000000000000000000503";
const OWNER: &str = "0x0000000000000000000000000000000000000001";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

fn hash(block: i64) -> String {
    format!("0x{block:064x}")
}

async fn database(prefix: &str) -> Result<(TestDatabase, PgPool)> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(prefix).pool_max_connections(1)).await?;
    let pool = database.pool().clone();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut tx = pool.begin().await?;
    sqlx::query("CREATE SCHEMA bigname_phase")
        .execute(&mut *tx)
        .await?;
    raw_sql(&format!(
        "ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public",
        name.replace('"', r#""""#)
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query("SET LOCAL search_path TO bigname_phase, public")
        .execute(&mut *tx)
        .await?;
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
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        sqlx::query("SET search_path TO bigname_phase, public")
            .execute(&mut **connection)
            .await?;
    }
    drop(connections);
    for block in [10, 11] {
        sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state) VALUES ($1, $2, $3, to_timestamp(1800000000 + $3), 'canonical')")
            .bind(CHAIN).bind(hash(block)).bind(block).execute(&pool).await?;
    }
    Ok((database, pool))
}

#[allow(clippy::too_many_arguments)]
async fn event(
    pool: &PgPool,
    identity: &str,
    logical: &str,
    resource: Option<&str>,
    family: &str,
    kind: &str,
    block: i64,
    log: i64,
    after: Value,
) -> Result<i64> {
    Ok(sqlx::query_scalar("INSERT INTO normalized_events (event_identity, namespace, logical_name_id, resource_id, event_kind, source_family, manifest_version, chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index, derivation_kind, canonicality_state, after_state, migration_correlation_ids) VALUES ($1, 'ens', $2, $3::uuid, $4, $5, 1, $6, $7, $8, $9, 0, $10, CASE WHEN $4 = 'MigrationApplied' THEN 'ens_v2_migration' ELSE 'ens_v2_registry_resource_surface' END, 'canonical', $11, CASE WHEN $4 = 'MigrationApplied' THEN ARRAY[$1] ELSE ARRAY[]::text[] END) RETURNING normalized_event_id")
        .bind(identity).bind(logical).bind(resource).bind(kind).bind(family).bind(CHAIN).bind(block).bind(hash(block)).bind(format!("0x{block:064x}")).bind(log).bind(after).fetch_one(pool).await?)
}

async fn seed_identity(pool: &PgPool, child_arms: &[&str]) -> Result<()> {
    sqlx::query("INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state, chain_id, block_hash, block_number, canonicality_state) VALUES ($1, 'ens', 'parent.eth', ARRAY['parent','eth'], '\\x00', $2, ARRAY['0x01','0x02'], 'ensip15', 'active', $3, $4, 10, 'canonical'), ($5, 'ens', 'child.parent.eth', ARRAY['child','parent','eth'], '\\x00', $6, ARRAY['0x03','0x01','0x02'], 'ensip15', 'active', $3, $4, 10, 'canonical')")
        .bind(PARENT).bind(PARENT.trim_start_matches("ens:")).bind(CHAIN).bind(hash(10)).bind(CHILD).bind(CHILD.trim_start_matches("ens:")).execute(pool).await?;
    for (resource, binding, arm) in [
        (V1_RESOURCE, V1_BINDING, "ens_v1"),
        (V2_RESOURCE, V2_BINDING, "ens_v2"),
    ] {
        sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
            .bind(resource).bind(CHAIN).bind(hash(10)).execute(pool).await?;
        if !child_arms.contains(&arm) {
            continue;
        }
        sqlx::query("INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm, active_from, chain_id, block_hash, block_number, provenance, canonicality_state) VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', $4, to_timestamp(1700000000), $5, $6, 10, '{\"transaction_index\":0,\"log_index\":0}', 'canonical')")
            .bind(binding).bind(CHILD).bind(resource).bind(arm).bind(CHAIN).bind(hash(10)).execute(pool).await?;
    }
    sqlx::query("INSERT INTO resources (resource_id, chain_id, block_hash, block_number, canonicality_state) VALUES ($1::uuid, $2, $3, 10, 'canonical')")
        .bind(V1_WRAPPER_RESOURCE).bind(CHAIN).bind(hash(10)).execute(pool).await?;
    Ok(())
}

async fn seed_v1_relation(pool: &PgPool, owner: &str, block: i64) -> Result<()> {
    event(pool, &format!("v1-child-{block}-{owner}"), CHILD, None, "ens_v1_registry_l1", "SubregistryChanged", block, 8, json!({"node":PARENT.trim_start_matches("ens:"),"child_node":CHILD.trim_start_matches("ens:"),"labelhash":"0x03","owner":owner})).await?;
    Ok(())
}

async fn seed_wrapper(pool: &PgPool, fuses: i64, expiry: i64) -> Result<()> {
    event(
        pool,
        "wrapper-fuses",
        CHILD,
        Some(V1_WRAPPER_RESOURCE),
        "ens_v1_wrapper_l1",
        "PermissionScopeChanged",
        10,
        4,
        json!({"fuses":fuses,"wrapper_state":"emancipated"}),
    )
    .await?;
    event(
        pool,
        "wrapper-expiry",
        CHILD,
        Some(V1_WRAPPER_RESOURCE),
        "ens_v1_wrapper_l1",
        "ExpiryChanged",
        10,
        5,
        json!({"expiry":expiry}),
    )
    .await?;
    Ok(())
}

async fn seed_migration(pool: &PgPool, path: &str, block: i64, identity: &str) -> Result<i64> {
    event(pool, identity, PARENT, None, "ens_v2_migration_l1", "MigrationApplied", block, 1, json!({"migration_path":path,"successor_registry_contract_instance_id":PARENT_REGISTRY,"successor_binding":{"binding_id":V2_BINDING,"resource_id":V2_RESOURCE}})).await
}

async fn seed_parent_migration_registry(pool: &PgPool, block: i64) -> Result<()> {
    for registry in [REGISTRY, OTHER_REGISTRY] {
        sqlx::query("INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES ($1::uuid, $2, 'contract') ON CONFLICT DO NOTHING").bind(registry).bind(CHAIN).execute(pool).await?;
    }
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number) VALUES ($1::uuid, $2, $3, 10) ON CONFLICT DO NOTHING")
        .bind(REGISTRY).bind(CHAIN).bind(REGISTRY_ADDRESS).execute(pool).await?;
    event(
        pool,
        "v2-parent-registry",
        PARENT,
        None,
        "ens_v2_registry_l1",
        "SubregistryChanged",
        block,
        2,
        json!({"subregistry":REGISTRY_ADDRESS}),
    )
    .await?;
    Ok(())
}

async fn seed_v2_relation(pool: &PgPool, block: i64) -> Result<()> {
    seed_parent_migration_registry(pool, block).await?;
    event(
        pool,
        "v2-child-registration",
        CHILD,
        Some(V2_RESOURCE),
        "ens_v2_registry_l1",
        "RegistrationGranted",
        block,
        3,
        json!({"registry_contract_instance_id":REGISTRY,"status":"registered","registrant":OWNER}),
    )
    .await?;
    Ok(())
}

async fn v2_history(pool: &PgPool, registry: &str, kind: &str, released: bool) -> Result<()> {
    event(
        pool,
        &format!("history-{registry}"),
        CHILD,
        None,
        "ens_v2_registry_l1",
        kind,
        10,
        6,
        json!({"registry_contract_instance_id":registry,"status":"registered","registrant":OWNER}),
    )
    .await?;
    if released {
        event(
            pool,
            &format!("release-{registry}"),
            CHILD,
            None,
            "ens_v2_registry_l1",
            "RegistrationReleased",
            10,
            7,
            json!({"registry_contract_instance_id":registry,"status":"released"}),
        )
        .await?;
    }
    Ok(())
}

async fn run(
    pool: &PgPool,
    target: i64,
    resume: Option<i64>,
    mode: RunMode,
) -> bigname_project::Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: target,
            affected_from_block: resume.map_or(10, |_| 11),
            affected_to_block: target,
            resume_current: resume.map(|number| Marker {
                number,
                hash: hash(number),
            }),
            mode,
        })
        .await
        .map(|_| ())
}

async fn visible(pool: &PgPool) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM children_current WHERE parent_logical_name_id = $1 AND child_logical_name_id = $2)").bind(PARENT).bind(CHILD).fetch_one(pool).await?)
}

struct Case<'a> {
    path: Option<&'a str>,
    fuses: i64,
    expiry: i64,
    owner: &'a str,
    history: Option<(&'a str, &'a str, bool)>,
    v2: bool,
    child_arms: &'a [&'a str],
}

async fn project_case(prefix: &str, case: Case<'_>) -> Result<bool> {
    let (_db, pool) = database(prefix).await?;
    seed_identity(&pool, case.child_arms).await?;
    seed_v1_relation(&pool, case.owner, 10).await?;
    seed_wrapper(&pool, case.fuses, case.expiry).await?;
    if case.v2 {
        seed_v2_relation(&pool, 10).await?;
        if case.child_arms == ["ens_v2"] {
            event(
                &pool,
                "child-migration",
                CHILD,
                None,
                "ens_v2_migration_l1",
                "MigrationApplied",
                10,
                9,
                json!({"migration_path":"locked_wrapped","successor_registry_contract_instance_id":REGISTRY,"successor_binding":{"binding_id":V2_BINDING,"resource_id":V2_RESOURCE}}),
            )
            .await?;
        }
    } else if matches!(case.path, Some("locked_wrapped" | "locked_child")) {
        seed_parent_migration_registry(&pool, 10).await?;
    }
    if let Some(path) = case.path {
        seed_migration(&pool, path, 10, "parent-migration").await?;
    }
    if let Some((registry, kind, released)) = case.history {
        v2_history(&pool, registry, kind, released).await?;
    }
    run(&pool, 10, None, RunMode::Normal).await?;
    visible(&pool).await
}

async fn same_position_wrapper_case(prefix: &str, reverse: bool) -> Result<bool> {
    let (_db, pool) = database(prefix).await?;
    seed_identity(&pool, &["ens_v1"]).await?;
    seed_v1_relation(&pool, OWNER, 10).await?;
    let mut evidence = vec![
        (
            "wrapper-fuses-a",
            "PermissionScopeChanged",
            json!({"fuses":0,"wrapper_state":"emancipated"}),
        ),
        (
            "wrapper-fuses-z",
            "PermissionScopeChanged",
            json!({"fuses":65_536,"wrapper_state":"emancipated"}),
        ),
        ("wrapper-expiry-a", "ExpiryChanged", json!({"expiry":1})),
        (
            "wrapper-expiry-z",
            "ExpiryChanged",
            json!({"expiry":2_000_000_000_i64}),
        ),
    ];
    if reverse {
        evidence.reverse();
    }
    for (identity, kind, after) in evidence {
        event(
            &pool,
            identity,
            CHILD,
            Some(V1_WRAPPER_RESOURCE),
            "ens_v1_wrapper_l1",
            kind,
            10,
            if kind == "PermissionScopeChanged" {
                4
            } else {
                5
            },
            after,
        )
        .await?;
    }
    seed_parent_migration_registry(&pool, 10).await?;
    seed_migration(&pool, "locked_wrapped", 10, "parent-migration").await?;
    run(&pool, 10, None, RunMode::Normal).await?;
    visible(&pool).await
}

macro_rules! visibility_test {
    ($name:ident, $prefix:literal, $case:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() -> Result<()> {
            assert_eq!(project_case($prefix, $case).await?, $expected);
            Ok(())
        }
    };
}

visibility_test!(
    unmigrated_parent_publishes_live_v1_child,
    "issue503_unmigrated",
    Case {
        path: None,
        fuses: 0,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: None,
        v2: false,
        child_arms: &["ens_v1"]
    },
    true
);
visibility_test!(
    unwrapped_parent_hides_v1_children,
    "issue503_unwrapped",
    Case {
        path: Some("unwrapped"),
        fuses: 65_536,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: None,
        v2: false,
        child_arms: &["ens_v1"]
    },
    false
);
visibility_test!(
    unlocked_wrapped_parent_hides_v1_children,
    "issue503_unlocked",
    Case {
        path: Some("unlocked_wrapped"),
        fuses: 65_536,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: None,
        v2: false,
        child_arms: &["ens_v1"]
    },
    false
);
#[tokio::test]
async fn locked_parent_publishes_migratable_v1_child() -> Result<()> {
    assert!(same_position_wrapper_case("issue503_wrapper_tie_forward", false).await?);
    assert!(same_position_wrapper_case("issue503_wrapper_tie_reverse", true).await?);

    let (_incremental_db, incremental) = database("issue503_wrapper_expiry_incremental").await?;
    seed_identity(&incremental, &["ens_v1"]).await?;
    seed_v1_relation(&incremental, OWNER, 10).await?;
    seed_wrapper(&incremental, 65_536, 1_800_000_010).await?;
    seed_parent_migration_registry(&incremental, 10).await?;
    seed_migration(&incremental, "locked_wrapped", 10, "parent-migration").await?;
    run(&incremental, 10, None, RunMode::Normal).await?;
    assert!(visible(&incremental).await?);
    run(&incremental, 11, Some(10), RunMode::Normal).await?;

    let (_fresh_db, fresh) = database("issue503_wrapper_expiry_fresh").await?;
    seed_identity(&fresh, &["ens_v1"]).await?;
    seed_v1_relation(&fresh, OWNER, 10).await?;
    seed_wrapper(&fresh, 65_536, 1_800_000_010).await?;
    seed_parent_migration_registry(&fresh, 10).await?;
    seed_migration(&fresh, "locked_wrapped", 10, "parent-migration").await?;
    run(&fresh, 11, None, RunMode::Normal).await?;

    assert_eq!(rows(&incremental).await?, rows(&fresh).await?);
    assert!(!visible(&incremental).await?);

    for bound in [false, true] {
        wrapper_retraction_converges(bound).await?;
    }
    Ok(())
}

async fn wrapper_retraction_converges(bound: bool) -> Result<()> {
    let suffix = if bound { "bound" } else { "rotated" };
    let (_redo_db, redo) = database(&format!("issue503_wrapper_retract_{suffix}_inc")).await?;
    seed_identity(&redo, &["ens_v1"]).await?;
    if bound {
        sqlx::query("UPDATE surface_bindings SET resource_id = $1::uuid WHERE surface_binding_id = $2::uuid")
            .bind(V1_WRAPPER_RESOURCE).bind(V1_BINDING).execute(&redo).await?;
    }
    seed_v1_relation(&redo, OWNER, 10).await?;
    seed_wrapper(&redo, 65_536, 2_000_000_000).await?;
    seed_parent_migration_registry(&redo, 10).await?;
    seed_migration(&redo, "locked_wrapped", 10, "parent-migration").await?;
    run(&redo, 10, None, RunMode::Normal).await?;
    assert!(visible(&redo).await?);
    sqlx::query(
        "DELETE FROM normalized_events WHERE event_identity IN ('wrapper-fuses', 'wrapper-expiry')",
    )
    .execute(&redo)
    .await?;
    run(&redo, 11, Some(10), RunMode::Redo).await?;

    let (_fresh_db, fresh) = database(&format!("issue503_wrapper_retract_{suffix}_fresh")).await?;
    seed_identity(&fresh, &["ens_v1"]).await?;
    if bound {
        sqlx::query("UPDATE surface_bindings SET resource_id = $1::uuid WHERE surface_binding_id = $2::uuid")
            .bind(V1_WRAPPER_RESOURCE).bind(V1_BINDING).execute(&fresh).await?;
    }
    seed_v1_relation(&fresh, OWNER, 10).await?;
    seed_parent_migration_registry(&fresh, 10).await?;
    seed_migration(&fresh, "locked_wrapped", 10, "parent-migration").await?;
    run(&fresh, 11, None, RunMode::Normal).await?;
    assert_eq!(rows(&redo).await?, rows(&fresh).await?);
    assert!(!visible(&redo).await?);
    Ok(())
}
visibility_test!(
    locked_parent_hides_child_without_parent_cannot_control,
    "issue503_no_pcc",
    Case {
        path: Some("locked_wrapped"),
        fuses: 0,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: None,
        v2: false,
        child_arms: &["ens_v1"]
    },
    false
);
visibility_test!(
    locked_parent_hides_dot_eth_child_even_with_parent_cannot_control,
    "issue503_dot_eth",
    Case {
        path: Some("locked_wrapped"),
        fuses: 196_608,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: None,
        v2: false,
        child_arms: &["ens_v1"]
    },
    false
);
visibility_test!(
    locked_parent_hides_ownerless_v1_child,
    "issue503_ownerless",
    Case {
        path: Some("locked_wrapped"),
        fuses: 65_536,
        expiry: 2_000_000_000,
        owner: ZERO,
        history: None,
        v2: false,
        child_arms: &["ens_v1"]
    },
    false
);
visibility_test!(
    locked_parent_hides_child_ever_registered_in_successor_v2_registry,
    "issue503_ever_v2",
    Case {
        path: Some("locked_wrapped"),
        fuses: 65_536,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: Some((REGISTRY, "RegistrationGranted", true)),
        v2: false,
        child_arms: &["ens_v1"]
    },
    false
);
visibility_test!(
    locked_parent_hides_child_with_lapsed_reservation,
    "issue503_reserved_v2",
    Case {
        path: Some("locked_wrapped"),
        fuses: 65_536,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: Some((REGISTRY, "RegistrationReserved", true)),
        v2: false,
        child_arms: &["ens_v1"]
    },
    false
);
visibility_test!(
    locked_parent_ignores_registration_in_unrelated_v2_registry,
    "issue503_other_v2",
    Case {
        path: Some("locked_wrapped"),
        fuses: 65_536,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: Some((OTHER_REGISTRY, "RegistrationRenewed", false)),
        v2: false,
        child_arms: &["ens_v1"]
    },
    true
);
visibility_test!(
    locked_child_parent_publishes_only_migratable_grandchild,
    "issue503_locked_child",
    Case {
        path: Some("locked_child"),
        fuses: 65_536,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: None,
        v2: false,
        child_arms: &["ens_v1"]
    },
    true
);
visibility_test!(
    locked_child_parent_hides_non_migratable_grandchild,
    "issue503_locked_child_no_pcc",
    Case {
        path: Some("locked_child"),
        fuses: 0,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: None,
        v2: false,
        child_arms: &["ens_v1"]
    },
    false
);
visibility_test!(
    emancipated_child_parent_hides_v1_descendants,
    "issue503_emancipated_child",
    Case {
        path: Some("emancipated_child"),
        fuses: 65_536,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: None,
        v2: false,
        child_arms: &["ens_v1"]
    },
    false
);

#[tokio::test]
async fn unknown_parent_migration_path_is_a_generation_failure() -> Result<()> {
    let (_db, pool) = database("issue503_unknown_path").await?;
    seed_identity(&pool, &["ens_v1"]).await?;
    seed_v1_relation(&pool, OWNER, 10).await?;
    seed_migration(&pool, "future_path", 10, "parent-migration").await?;
    let failure = run(&pool, 10, None, RunMode::Normal)
        .await
        .expect_err("unknown migration path must be visible");
    assert!(failure.to_string().contains("future_path"));
    Ok(())
}

#[tokio::test]
async fn child_authority_selects_arm_after_parent_reachability_filter() -> Result<()> {
    assert!(
        project_case(
            "issue503_child_authority",
            Case {
                path: Some("locked_wrapped"),
                fuses: 65_536,
                expiry: 2_000_000_000,
                owner: OWNER,
                history: None,
                v2: true,
                child_arms: &["ens_v2"]
            }
        )
        .await?
    );
    Ok(())
}

visibility_test!(
    unsupported_both_arm_child_is_omitted_after_reachability,
    "issue503_unsupported_pair",
    Case {
        path: None,
        fuses: 0,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: None,
        v2: true,
        child_arms: &["ens_v1", "ens_v2"]
    },
    false
);
visibility_test!(
    unreachable_v1_arm_does_not_suppress_reachable_v2_arm,
    "issue503_v2_survives",
    Case {
        path: Some("unlocked_wrapped"),
        fuses: 65_536,
        expiry: 2_000_000_000,
        owner: OWNER,
        history: None,
        v2: true,
        child_arms: &["ens_v2"]
    },
    true
);

async fn rows(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(row) - 'last_recomputed_at' - 'inserted_at' ORDER BY child_logical_name_id), '[]'::jsonb) FROM children_current row WHERE provenance ->> 'chain_id' = $1").bind(CHAIN).fetch_one(pool).await?)
}

async fn convergence(
    prefix: &str,
    initial_path: Option<&str>,
    replacement: Option<&str>,
    mode: RunMode,
) -> Result<(Value, Value, Value)> {
    let (_inc_db, incremental) = database(&format!("{prefix}_incremental")).await?;
    seed_identity(&incremental, &["ens_v1"]).await?;
    seed_v1_relation(&incremental, OWNER, 10).await?;
    seed_wrapper(&incremental, 65_536, 2_000_000_000).await?;
    for (path, block) in [(initial_path, 10), (replacement, 11)] {
        if matches!(path, Some("locked_wrapped" | "locked_child")) {
            seed_parent_migration_registry(&incremental, block).await?;
        }
    }
    if let Some(path) = initial_path {
        seed_migration(&incremental, path, 10, "old-migration").await?;
    }
    run(&incremental, 10, None, RunMode::Normal).await?;
    let initial = rows(&incremental).await?;
    if initial_path.is_some() {
        sqlx::query("DELETE FROM normalized_events WHERE event_identity = 'old-migration'")
            .execute(&incremental)
            .await?;
    }
    if let Some(path) = replacement {
        seed_migration(&incremental, path, 11, "new-migration").await?;
    }
    run(&incremental, 11, Some(10), mode).await?;

    let (_fresh_db, fresh) = database(&format!("{prefix}_fresh")).await?;
    seed_identity(&fresh, &["ens_v1"]).await?;
    seed_v1_relation(&fresh, OWNER, 10).await?;
    seed_wrapper(&fresh, 65_536, 2_000_000_000).await?;
    if matches!(replacement, Some("locked_wrapped" | "locked_child")) {
        seed_parent_migration_registry(&fresh, 11).await?;
    }
    if let Some(path) = replacement {
        seed_migration(&fresh, path, 11, "new-migration").await?;
    }
    run(&fresh, 11, None, RunMode::Normal).await?;
    Ok((initial, rows(&incremental).await?, rows(&fresh).await?))
}

#[tokio::test]
async fn parent_migration_flip_incremental_matches_fresh_rebuild() -> Result<()> {
    for path in ["unlocked_wrapped", "locked_wrapped"] {
        let (_, incremental, fresh) = convergence(
            &format!("issue503_flip_{path}"),
            None,
            Some(path),
            RunMode::Normal,
        )
        .await?;
        assert_eq!(incremental, fresh);
        assert_eq!(
            incremental.as_array().unwrap().is_empty(),
            path == "unlocked_wrapped"
        );
    }
    Ok(())
}

#[tokio::test]
async fn hidden_children_restore_when_parent_migration_is_retracted() -> Result<()> {
    let (initial, incremental, fresh) = convergence(
        "issue503_retract",
        Some("unlocked_wrapped"),
        None,
        RunMode::Redo,
    )
    .await?;
    assert!(initial.as_array().unwrap().is_empty());
    assert_eq!(incremental, fresh);
    assert!(!incremental.as_array().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn locked_to_unlocked_reclassification_matches_fresh_rebuild() -> Result<()> {
    let (initial, incremental, fresh) = convergence(
        "issue503_locked_unlocked",
        Some("locked_wrapped"),
        Some("unlocked_wrapped"),
        RunMode::Redo,
    )
    .await?;
    assert!(!initial.as_array().unwrap().is_empty());
    assert_eq!(incremental, fresh);
    assert!(incremental.as_array().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn unlocked_to_locked_reclassification_restages_previously_hidden_children() -> Result<()> {
    let (initial, incremental, fresh) = convergence(
        "issue503_unlocked_locked",
        Some("unlocked_wrapped"),
        Some("locked_wrapped"),
        RunMode::Redo,
    )
    .await?;
    assert!(initial.as_array().unwrap().is_empty());
    assert_eq!(incremental, fresh);
    assert!(!incremental.as_array().unwrap().is_empty());
    Ok(())
}
