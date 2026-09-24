//! Project's registry generation for ENSv1 names: `old` while the 2017 registry is the only
//! registry that recorded an owner for the node, `current` from the node's first
//! current-registry ownership record. The deployed registry answers from the 2017 registry until
//! it holds a record of its own, and only `setOwner` and `setSubnodeOwner` write that record
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L46 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L84 @ ens_v1@91c966f).
//! The events here are seeded by hand to pin the SQL; the producer path is exercised by the
//! phase-runner and API fixtures.

use anyhow::{Context, Result};
use bigname_project::{BatchRequest, Engine, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-mainnet";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const ROOT: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

fn block_hash(block: i64) -> String {
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
    raw_sql(&format!(
        "CREATE SCHEMA bigname_phase;
         ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public;
         SET LOCAL search_path TO bigname_phase, public",
        name.replace('"', r#""""#)
    ))
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
    let mut connection = pool.acquire().await?;
    sqlx::query("SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    drop(connection);
    for block in 1..=6 {
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
                                        canonicality_state)
             VALUES ($1, $2, $3, '2026-08-26T00:00:00Z'::timestamptz
                                 + make_interval(mins => $3::int), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(block))
        .bind(block)
        .execute(&pool)
        .await?;
    }
    Ok((database, pool))
}

fn namehash(name: &str) -> String {
    let labels: Vec<&[u8]> = if name.is_empty() {
        Vec::new()
    } else {
        name.split('.').map(str::as_bytes).collect()
    };
    format!("{:#x}", bigname_storage::ens_namehash_label_bytes(&labels))
}

fn labelhash(label: &str) -> String {
    format!("{:#x}", alloy_primitives::keccak256(label.as_bytes()))
}

/// A surface for `name`, bound on `arm` from block 1 (and closed at `closed_at`, if given).
async fn surface(
    pool: &PgPool,
    index: u16,
    namespace: &str,
    name: &str,
    arm: Option<&str>,
    closed_at: Option<i64>,
) -> Result<String> {
    let labels: Vec<&str> = if name.is_empty() {
        Vec::new()
    } else {
        name.split('.').collect()
    };
    let logical = format!("{namespace}:{}", namehash(name));
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, $2, $3, $4, '\\x00', $5, $6, 'ensip15', 'active', $7, $8, 1, 'canonical')",
    )
    .bind(&logical)
    .bind(namespace)
    .bind(name)
    .bind(&labels)
    .bind(namehash(name))
    .bind(
        labels
            .iter()
            .map(|label| labelhash(label))
            .collect::<Vec<_>>(),
    )
    .bind(CHAIN)
    .bind(block_hash(1))
    .execute(pool)
    .await?;
    if let Some(arm) = arm {
        let resource = format!("00000000-0000-0000-0001-{index:012x}");
        sqlx::query(
            "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
                                    canonicality_state)
             VALUES ($1::uuid, $2, $3, 1, 'canonical')",
        )
        .bind(&resource)
        .bind(CHAIN)
        .bind(block_hash(1))
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
                 binding_kind, authority_arm, active_from, active_to, chain_id, block_hash,
                 block_number, provenance, canonicality_state)
             SELECT $1::uuid, $2, $3::uuid, 'declared_registry_path', $4,
                    opened.block_timestamp, closed.block_timestamp, $5, opened.block_hash, 1,
                    '{\"transaction_index\":0,\"log_index\":0}', 'canonical'
             FROM chain_lineage opened
             LEFT JOIN chain_lineage closed
               ON closed.chain_id = opened.chain_id AND closed.block_number = $6
             WHERE opened.chain_id = $5 AND opened.block_number = 1",
        )
        .bind(format!("00000000-0000-0000-0002-{index:012x}"))
        .bind(&logical)
        .bind(&resource)
        .bind(arm)
        .bind(CHAIN)
        .bind(closed_at)
        .execute(pool)
        .await?;
    }
    Ok(logical)
}

struct Ownership<'a> {
    namespace: &'a str,
    node: String,
    child: Option<String>,
    emitter_role: &'a str,
    block: i64,
    owner: &'a str,
}

impl<'a> Ownership<'a> {
    /// `NewOwner(parent, label, owner)` from the registry with this emitter role.
    fn new_owner(parent: &str, label: &str, role: &'a str, block: i64) -> Self {
        let name = if parent.is_empty() {
            label.to_owned()
        } else {
            format!("{label}.{parent}")
        };
        Self {
            namespace: "ens",
            node: namehash(parent),
            child: Some(namehash(&name)),
            emitter_role: role,
            block,
            owner: OWNER,
        }
    }

    /// `Transfer(node, owner)` from the registry with this emitter role.
    fn transfer(name: &str, role: &'a str, block: i64) -> Self {
        Self {
            namespace: "ens",
            node: namehash(name),
            child: None,
            emitter_role: role,
            block,
            owner: OWNER,
        }
    }
}

/// Stores the rows Interpret derives from one registry ownership log. Pre-surface registry
/// observations carry no logical name, so neither do these.
async fn ownership(
    pool: &PgPool,
    identity: &str,
    event: Ownership<'_>,
    visibility: &str,
    canonicality: &str,
) -> Result<()> {
    let source_event = if event.child.is_some() {
        "NewOwner"
    } else {
        "Transfer"
    };
    let mut after = json!({
        "source_event": source_event,
        "node": event.node,
        "owner": event.owner,
        "owner_getter": event.owner,
        "emitter_role": event.emitter_role,
    });
    if let Some(child) = &event.child {
        after["child_node"] = json!(child);
    }
    let kinds: &[&str] = if event.child.is_some() {
        &["SubregistryChanged", "AuthorityTransferred"]
    } else {
        &["AuthorityTransferred"]
    };
    let block_hash = if canonicality == "orphaned" {
        let orphan = format!("0xorphan{}", event.block);
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
                                        canonicality_state)
             SELECT chain_id, $2, block_number, block_timestamp, 'orphaned'
             FROM chain_lineage WHERE chain_id = $1 AND block_number = $3
             ON CONFLICT DO NOTHING",
        )
        .bind(CHAIN)
        .bind(&orphan)
        .bind(event.block)
        .execute(pool)
        .await?;
        orphan
    } else {
        block_hash(event.block)
    };
    for (log, kind) in kinds.iter().enumerate() {
        sqlx::query(
            "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
                 manifest_version, chain_id, block_number, block_hash, transaction_hash,
                 transaction_index, log_index, derivation_kind, canonicality_state, after_state,
                 consumer_visibility, migration_correlation_ids)
             VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8, 0, $9, 'ens_v1_unwrapped_authority', $10::text::bigname_phase.canonicality_state, $11,
                     $12, CASE WHEN $12 = 'candidate' THEN ARRAY['tyr17-inactive']
                               ELSE ARRAY[]::text[] END)",
        )
        .bind(format!("{identity}:{kind}"))
        .bind(event.namespace)
        .bind(kind)
        .bind(if event.namespace == "ens" {
            "ens_v1_registry_l1"
        } else {
            "basenames_base_registry"
        })
        .bind(CHAIN)
        .bind(event.block)
        .bind(&block_hash)
        .bind(format!("{identity}-transaction"))
        .bind(log as i64)
        .bind(canonicality)
        .bind(&after)
        .bind(visibility)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn activated(pool: &PgPool, identity: &str, event: Ownership<'_>) -> Result<()> {
    ownership(pool, identity, event, "activated", "canonical").await
}

async fn project(pool: &PgPool, target: i64) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: target,
            affected_from_block: 1,
            affected_to_block: target,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    Ok(())
}

/// `(authority_arm, registry_generation, registry_handoff_block_number, ownerless_registry)`.
async fn selection(pool: &PgPool, logical: &str) -> Result<(Value, Value, Value, Value)> {
    let selection: Value = sqlx::query_scalar(
        "SELECT provenance -> 'authority_selection' FROM name_current WHERE logical_name_id = $1",
    )
    .bind(logical)
    .fetch_one(pool)
    .await
    .with_context(|| format!("no name_current row for {logical}"))?;
    Ok((
        selection["authority_arm"].clone(),
        selection["registry_generation"].clone(),
        selection["registry_handoff_block_number"].clone(),
        selection["ownerless_registry"].clone(),
    ))
}

#[tokio::test]
async fn registry_generation_follows_the_first_current_registry_record() -> Result<()> {
    let (database, pool) = database("tyr17_registry_generation").await?;
    let old = surface(&pool, 1, "ens", "old.eth", Some("ens_v1"), None).await?;
    let parent = surface(&pool, 2, "ens", "parent.eth", Some("ens_v1"), None).await?;
    let kid = surface(&pool, 3, "ens", "kid.parent.eth", Some("ens_v1"), None).await?;
    let sibling = surface(&pool, 4, "ens", "sib.parent.eth", Some("ens_v1"), None).await?;
    let moved = surface(&pool, 5, "ens", "moved.eth", Some("ens_v1"), None).await?;
    let fresh = surface(&pool, 6, "ens", "fresh.eth", Some("ens_v1"), None).await?;
    for (identity, event) in [
        (
            "old-1",
            Ownership::new_owner("eth", "old", "registry_old", 1),
        ),
        (
            "parent-1",
            Ownership::new_owner("eth", "parent", "registry_old", 1),
        ),
        (
            "kid-1",
            Ownership::new_owner("parent.eth", "kid", "registry_old", 1),
        ),
        (
            "sib-1",
            Ownership::new_owner("parent.eth", "sib", "registry_old", 2),
        ),
        (
            "moved-1",
            Ownership::new_owner("eth", "moved", "registry_old", 1),
        ),
        // The primary handoff: the parent writes the child's first current record with the
        // same owner the 2017 registry holds.
        ("old-4", Ownership::new_owner("eth", "old", "registry", 4)),
        // The child's handoff names the parent as `node`; it must not hand the parent over.
        (
            "kid-4",
            Ownership::new_owner("parent.eth", "kid", "registry", 4),
        ),
        // A Transfer names its own node.
        ("moved-4", Ownership::transfer("moved.eth", "registry", 4)),
        ("old-5", Ownership::transfer("old.eth", "registry", 5)),
        // Written only in the current registry: never read from the 2017 registry.
        (
            "fresh-2",
            Ownership::new_owner("eth", "fresh", "registry", 2),
        ),
    ] {
        activated(&pool, identity, event).await?;
    }

    project(&pool, 3).await?;
    for logical in [&old, &parent, &kid, &sibling, &moved] {
        assert_eq!(
            selection(&pool, logical).await?,
            (json!("ens_v1"), json!("old"), Value::Null, Value::Null),
            "{logical} before its handoff"
        );
    }
    assert_eq!(
        selection(&pool, &fresh).await?,
        (json!("ens_v1"), json!("current"), json!(2), Value::Null)
    );

    project(&pool, 5).await?;
    for (logical, expected) in [
        (&old, (json!("current"), json!(4))),
        (&parent, (json!("old"), Value::Null)),
        (&kid, (json!("current"), json!(4))),
        (&sibling, (json!("old"), Value::Null)),
        (&moved, (json!("current"), json!(4))),
        (&fresh, (json!("current"), json!(2))),
    ] {
        let (arm, generation, handoff, ownerless) = selection(&pool, logical).await?;
        assert_eq!(arm, json!("ens_v1"), "{logical}");
        assert_eq!(
            (generation, handoff),
            expected,
            "{logical} after the handoff"
        );
        assert_eq!(ownerless, Value::Null, "{logical}");
    }
    database.cleanup().await
}

#[tokio::test]
async fn registry_generation_ignores_inactive_orphaned_root_and_other_arms() -> Result<()> {
    let (database, pool) = database("tyr17_registry_generation_negative").await?;
    let candidate = surface(&pool, 1, "ens", "candidate.eth", Some("ens_v1"), None).await?;
    let orphaned = surface(&pool, 2, "ens", "orphaned.eth", Some("ens_v1"), None).await?;
    let root = surface(&pool, 3, "ens", "", Some("ens_v1"), None).await?;
    let v2 = surface(&pool, 4, "ens", "v2.eth", Some("ens_v2"), None).await?;
    let unresolved = surface(&pool, 5, "ens", "both.eth", Some("ens_v1"), None).await?;
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
                                canonicality_state)
         VALUES ('00000000-0000-0000-0009-000000000005', $1, $2, 1, 'canonical');
         ",
    )
    .bind(CHAIN)
    .bind(block_hash(1))
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
             binding_kind, authority_arm, active_from, chain_id, block_hash, block_number,
             provenance, canonicality_state)
         SELECT '00000000-0000-0000-0008-000000000005', $1,
                '00000000-0000-0000-0009-000000000005', 'declared_registry_path', 'ens_v2',
                block_timestamp, chain_id, block_hash, 1,
                '{\"transaction_index\":0,\"log_index\":1}', 'canonical'
         FROM chain_lineage WHERE chain_id = $2 AND block_number = 1",
    )
    .bind(&unresolved)
    .bind(CHAIN)
    .execute(&pool)
    .await?;
    let basenames = surface(
        &pool,
        6,
        "basenames",
        "old.base.eth",
        Some("basenames"),
        None,
    )
    .await?;
    for (identity, event) in [
        (
            "candidate-1",
            Ownership::new_owner("eth", "candidate", "registry_old", 1),
        ),
        (
            "orphaned-1",
            Ownership::new_owner("eth", "orphaned", "registry_old", 1),
        ),
        ("root-1", Ownership::transfer("", "registry_old", 1)),
        ("root-4", Ownership::transfer("", "registry", 4)),
        ("v2-1", Ownership::new_owner("eth", "v2", "registry_old", 1)),
        (
            "both-1",
            Ownership::new_owner("eth", "both", "registry_old", 1),
        ),
        (
            "basenames-1",
            Ownership {
                namespace: "basenames",
                ..Ownership::new_owner("base.eth", "old", "registry_old", 1)
            },
        ),
    ] {
        activated(&pool, identity, event).await?;
    }
    ownership(
        &pool,
        "candidate-4",
        Ownership::new_owner("eth", "candidate", "registry", 4),
        "candidate",
        "canonical",
    )
    .await?;
    ownership(
        &pool,
        "orphaned-4",
        Ownership::new_owner("eth", "orphaned", "registry", 4),
        "activated",
        "orphaned",
    )
    .await?;

    // The root has no name row; its staged selection is captured as Project publishes.
    raw_sql(
        "CREATE TABLE root_selection (arm text, generation text, handoff bigint);
         CREATE FUNCTION capture_root_selection() RETURNS trigger LANGUAGE plpgsql AS $capture$
         BEGIN
             INSERT INTO root_selection
             SELECT selected_authority_arm, registry_generation, registry_handoff_block_number
             FROM project_name_authority
             WHERE logical_name_id = 'ens:0x0000000000000000000000000000000000000000000000000000000000000000';
             RETURN NULL;
         END $capture$;
         CREATE TRIGGER capture_root_selection AFTER INSERT ON name_current
         FOR EACH STATEMENT EXECUTE FUNCTION capture_root_selection();",
    )
    .execute(&pool)
    .await?;

    project(&pool, 5).await?;
    for logical in [&candidate, &orphaned] {
        assert_eq!(
            selection(&pool, logical).await?,
            (json!("ens_v1"), json!("old"), Value::Null, Value::Null),
            "{logical}: inactive or orphaned current-registry evidence is not a handoff"
        );
    }
    let root_selection: Vec<(Option<String>, Option<String>, Option<i64>)> =
        sqlx::query_as("SELECT arm, generation, handoff FROM root_selection")
            .fetch_all(&pool)
            .await?;
    assert_eq!(
        root_selection,
        vec![(Some("ens_v1".to_owned()), Some("current".to_owned()), None)],
        "the constructor writes the root record without an event"
    );
    assert!(
        sqlx::query("SELECT 1 FROM name_current WHERE logical_name_id = $1")
            .bind(&root)
            .fetch_optional(&pool)
            .await?
            .is_none()
    );
    assert_eq!(
        selection(&pool, &v2).await?,
        (json!("ens_v2"), Value::Null, Value::Null, Value::Null)
    );
    assert_eq!(
        selection(&pool, &unresolved).await?,
        (Value::Null, Value::Null, Value::Null, Value::Null)
    );
    assert_eq!(
        selection(&pool, &basenames).await?,
        (json!("basenames"), Value::Null, Value::Null, Value::Null)
    );
    assert_eq!(namehash(""), ROOT);
    database.cleanup().await
}

/// A zero or self-address owner written in the current registry keeps the record, so the name
/// stays `current`; once its binding closes the row is the ownerless registry profile, which
/// keeps the ENSv1 arm its earlier named events supply and is marked so the API omits it.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L48-L55 @ ens_v1@91c966f)
#[tokio::test]
async fn ownerless_rows_keep_their_arm_and_are_marked() -> Result<()> {
    let (database, pool) = database("tyr17_registry_generation_ownerless").await?;
    let mut names = Vec::new();
    for (index, label, reason) in [(1, "zero", "zero_address"), (2, "self", "registry_self")] {
        let name = format!("{label}.eth");
        let logical = surface(&pool, index, "ens", &name, Some("ens_v1"), Some(4)).await?;
        activated(
            &pool,
            &format!("{label}-1"),
            Ownership::new_owner("eth", label, "registry_old", 1),
        )
        .await?;
        // The current-registry write of a zero-equivalent owner, named because the surface is
        // known, as Interpret stores it.
        sqlx::query(
            "INSERT INTO normalized_events (event_identity, namespace, logical_name_id,
                 resource_id, event_kind, source_family, manifest_version, chain_id,
                 block_number, block_hash, transaction_hash, transaction_index, log_index,
                 derivation_kind, canonicality_state, after_state)
             VALUES ($1, 'ens', $2, $3::uuid, 'AuthorityTransferred', 'ens_v1_registry_l1', 1,
                     $4, 4, $5, $1, 0, 0, 'ens_v1_unwrapped_authority', 'canonical', $6)",
        )
        .bind(format!("{label}-4"))
        .bind(&logical)
        .bind(format!("00000000-0000-0000-0001-{index:012x}"))
        .bind(CHAIN)
        .bind(block_hash(4))
        .bind(json!({
            "source_event": "NewOwner",
            "node": namehash("eth"),
            "child_node": namehash(&name),
            "owner": ZERO,
            "owner_getter": ZERO,
            "owner_getter_reason": reason,
            "emitter_role": "registry",
        }))
        .execute(&pool)
        .await?;
        names.push(logical);
    }

    project(&pool, 5).await?;
    for logical in &names {
        assert_eq!(
            selection(&pool, logical).await?,
            (json!("ens_v1"), json!("current"), json!(4), json!(true)),
            "{logical}"
        );
        let status: (String, Option<String>) = sqlx::query_as(
            "SELECT declared_summary #>> '{registration,status}', unsupported_reason
             FROM name_current WHERE logical_name_id = $1",
        )
        .bind(logical)
        .fetch_one(&pool)
        .await?;
        assert_eq!(status, ("unregistered".to_owned(), None), "{logical}");
    }
    database.cleanup().await
}
