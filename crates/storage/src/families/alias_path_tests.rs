//! The alias path walk over hand-written Project rows: ETHRegistry `E` mounts registry `R` at
//! `m`, `R` holds `child`, and the association names `child.m.eth` canonical.
use std::collections::BTreeMap;

use alloy_primitives::{hex, keccak256};
use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::types::time::OffsetDateTime;
use sqlx::{PgPool, raw_sql};

use super::*;
use crate::{
    logical_name_id_for_name,
    snapshot_selection::{ChainPosition, ChainPositions, SnapshotSelectionErrorKind},
};

const CHAIN: &str = "ethereum-sepolia";
const ROOT: &str = "0x00000000000000000000000000000000000000a0";
const E: &str = "0x00000000000000000000000000000000000000e0";
const R: &str = "0x00000000000000000000000000000000000000b0";
/// The publication block. Its timestamp is `CLOCK`.
const PUBLISHED: i64 = 3;
const CLOCK: i64 = 1_700_000_000 + PUBLISHED;
const LIVE: i64 = CLOCK + 1_000;

fn key(label: &str) -> String {
    let mut word = keccak256(label.as_bytes()).0;
    word[28..].fill(0);
    format!("{:#x}", B256::from(word))
}

fn resource(seed: u128) -> Uuid {
    Uuid::from_u128(seed)
}

async fn install(pool: &PgPool) -> Result<()> {
    for baseline in [
        include_str!("../../schema/baseline/01_chain.sql"),
        include_str!("../../schema/baseline/02_raw_facts.sql"),
        include_str!("../../schema/baseline/03_identity.sql"),
        include_str!("../../schema/baseline/04_manifests.sql"),
        include_str!("../../schema/baseline/05_normalized_events.sql"),
        include_str!("../../schema/baseline/06_projections.sql"),
        include_str!("../../schema/baseline/07_labels.sql"),
        include_str!("../../schema/baseline/08_heartbeats.sql"),
        include_str!("../../schema/baseline/09_divergence.sql"),
        include_str!("../../schema/baseline/10_phase_state.sql"),
    ] {
        raw_sql(baseline).execute(pool).await?;
    }
    let hash = bigname_content_hash::INTERPRETER_CONTENT_HASH;
    raw_sql(&format!(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
             canonicality_state)
         SELECT '{CHAIN}', 'b' || n, n, to_timestamp(1700000000 + n), 'finalized'
         FROM generate_series(1, 5) n;
         INSERT INTO project_family_marker (chain_id, current_block_number, current_block_hash,
             block_timestamp, input_content_hash, state)
         VALUES ('{CHAIN}', {PUBLISHED}, 'b{PUBLISHED}', to_timestamp({CLOCK}), '{hash}', 'live')"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// The publication records `ROOT` as its admitted root registry, and `ROOT.eth` points at `E`.
async fn anchors(pool: &PgPool) -> Result<()> {
    sqlx::query("UPDATE project_family_marker SET root_registry = $2 WHERE chain_id = $1")
        .bind(CHAIN)
        .bind(ROOT)
        .execute(pool)
        .await?;
    entry(pool, ROOT, "eth", "reserved", LIVE, resource(100)).await?;
    pointer(pool, resource(100), E, 1).await
}

async fn entry(
    pool: &PgPool,
    registry: &str,
    label: &str,
    status: &str,
    expiry: i64,
    resource_id: Uuid,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_ens_v2_entry_owner (chain_id, registry, entry_key, token_id,
             resource_id, status, expiry, block_number, event_identity)
         VALUES ($1, $2, $3, $3, $4, $5, $6::numeric, 1, $2 || $3)
         ON CONFLICT (chain_id, registry, entry_key) DO UPDATE SET status = EXCLUDED.status,
             expiry = EXCLUDED.expiry, resource_id = EXCLUDED.resource_id",
    )
    .bind(CHAIN)
    .bind(registry)
    .bind(key(label))
    .bind(resource_id)
    .bind(status)
    .bind(expiry.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

async fn pointer(pool: &PgPool, resource_id: Uuid, subregistry: &str, block: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO resources (resource_id, chain_id, block_hash, block_number)
         VALUES ($1, $2, 'b1', 1) ON CONFLICT DO NOTHING",
    )
    .bind(resource_id)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, after_state)
         VALUES ($1, 'ens', $2, 'SubregistryChanged', 'ens_v2_registry_l1', 2, $3, $4,
             'b' || $4, 'ens_v2_registry_resource_surface', 'finalized', $5)",
    )
    .bind(format!("pointer:{resource_id}:{block}"))
    .bind(resource_id)
    .bind(CHAIN)
    .bind(block)
    .bind(serde_json::json!({"subregistry": subregistry}))
    .execute(pool)
    .await?;
    Ok(())
}

async fn associate(pool: &PgPool, name: &str, resource_id: Uuid, block: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_lifecycle_association (chain_id, logical_name_id,
             registry_identifier, token_id, target_resource_id, event_kind, block_number,
             event_identity)
         VALUES ($1, $2, 'registry', 'token', $3, 'RegistrationGranted', $4, $2)",
    )
    .bind(CHAIN)
    .bind(logical_name_id_for_name("ens", name))
    .bind(resource_id)
    .bind(block)
    .execute(pool)
    .await?;
    Ok(())
}

/// `E.m` points at `R`, `R.child` is registered and canonical as `child.m.eth`.
async fn mounted(pool: &PgPool) -> Result<()> {
    anchors(pool).await?;
    entry(pool, E, "m", "registered", LIVE, resource(1)).await?;
    pointer(pool, resource(1), R, 1).await?;
    entry(pool, R, "child", "registered", LIVE, resource(9)).await?;
    associate(pool, "child.m.eth", resource(9), 1).await
}

fn at(block_number: i64) -> ChainPositions {
    ChainPositions::new(BTreeMap::from([(
        CHAIN.to_owned(),
        ChainPosition {
            slot: CHAIN.to_owned(),
            chain_id: CHAIN.to_owned(),
            block_number,
            block_hash: format!("b{block_number}"),
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000 + block_number)
                .expect("a timestamp"),
        },
    )]))
}

async fn walk(pool: &PgPool, name: &str) -> Result<AliasWalk> {
    Ok(resolve_alias_path(
        pool,
        "ens",
        name,
        &logical_name_id_for_name("ens", name),
        &at(PUBLISHED),
    )
    .await?)
}

async fn canonical(pool: &PgPool, name: &str) -> Result<Option<String>> {
    Ok(walk(pool, name)
        .await?
        .target
        .map(|target| target.canonical_logical_name_id))
}

async fn with_database(name: &str, check: impl AsyncFnOnce(&PgPool) -> Result<()>) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(2)).await?;
    let result = async {
        database.create_phase_schema().await?;
        install(database.pool()).await?;
        check(database.pool()).await
    }
    .await;
    database.cleanup().await?;
    result
}

fn id(name: &str) -> Option<String> {
    Some(logical_name_id_for_name("ens", name))
}

/// T9, T14. A publication with no admitted root walks nothing past the publication read. From
/// the root, a hop with no entry ends the walk at that entry read.
#[tokio::test]
async fn an_unanchored_label_set_answers_none() -> Result<()> {
    with_database("alias_path_unanchored", async |pool| {
        let none = walk(pool, "child.m.eth").await?;
        assert_eq!(
            none,
            AliasWalk {
                target: None,
                statements: 1
            }
        );
        mounted(pool).await?;
        let missing = walk(pool, "child.q.eth").await?;
        assert_eq!(
            missing,
            AliasWalk {
                target: None,
                statements: 4
            }
        );
        let other_tld = walk(pool, "child.m.box").await?;
        assert_eq!(
            other_tld,
            AliasWalk {
                target: None,
                statements: 2
            }
        );
        Ok(())
    })
    .await
}

/// T1 and T20. A second mount reaches the token, the canonical path answers none.
#[tokio::test]
async fn a_second_mount_reaches_the_canonical_token() -> Result<()> {
    with_database("alias_path_second_mount", async |pool| {
        mounted(pool).await?;
        assert_eq!(canonical(pool, "child.m.eth").await?, None);
        assert_eq!(canonical(pool, "child.z.eth").await?, None);
        entry(pool, E, "z", "registered", LIVE, resource(2)).await?;
        pointer(pool, resource(2), R, 2).await?;
        let alias = walk(pool, "child.z.eth").await?;
        assert_eq!(
            alias.target,
            Some(AliasTarget {
                canonical_logical_name_id: logical_name_id_for_name("ens", "child.m.eth"),
                resource_id: resource(9),
                reservation_expiry: None,
            })
        );
        // The publication, the `eth` and `z` hops' entries and pointers, the leaf, the
        // association.
        assert_eq!(alias.statements, 7);
        // A bracketed label stands for its labelhash.
        let bracketed = format!("[{}].z.eth", hex::encode(keccak256(b"child")));
        assert_eq!(canonical(pool, &bracketed).await?, id("child.m.eth"));
        Ok(())
    })
    .await
}

/// T8. A registry mounted under itself is walked once per requested label.
#[tokio::test]
async fn a_path_through_a_cycle_is_served_in_label_count_steps() -> Result<()> {
    with_database("alias_path_cycle", async |pool| {
        mounted(pool).await?;
        entry(pool, R, "self", "registered", LIVE, resource(3)).await?;
        pointer(pool, resource(3), R, 2).await?;
        let alias = walk(pool, "child.self.m.eth").await?;
        assert_eq!(
            alias.target.map(|target| target.canonical_logical_name_id),
            id("child.m.eth")
        );
        assert_eq!(alias.statements, 9);
        let deeper = walk(pool, "child.self.self.self.m.eth").await?;
        assert_eq!(deeper.statements, 13);
        Ok(())
    })
    .await
}

/// A walk reads at most `MAX_ALIAS_LABELS` labels. A cycle path of exactly that many labels is
/// served, and a name with one label more runs no statement.
#[tokio::test]
async fn a_name_over_the_label_cap_is_not_walked() -> Result<()> {
    with_database("alias_path_label_cap", async |pool| {
        mounted(pool).await?;
        entry(pool, R, "self", "registered", LIVE, resource(3)).await?;
        pointer(pool, resource(3), R, 2).await?;
        // `child`, the `self` hops, `m` and `eth`.
        let path = |labels: usize| {
            let hops = vec!["self"; labels - 3].join(".");
            format!("child.{hops}.m.eth")
        };
        let at_cap = walk(pool, &path(MAX_ALIAS_LABELS)).await?;
        assert_eq!(
            at_cap.target.map(|target| target.canonical_logical_name_id),
            id("child.m.eth")
        );
        assert_eq!(at_cap.statements, 2 * MAX_ALIAS_LABELS + 1);
        let over = walk(pool, &path(MAX_ALIAS_LABELS + 1)).await?;
        assert_eq!(over.target, None);
        assert_eq!(over.statements, 0);
        Ok(())
    })
    .await
}

/// T3. A hop whose expiry is the publication timestamp is expired, as `_isExpired` reads it.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L671-L673 @ ens_v2_sepolia_20261001@07e55a05)
#[tokio::test]
async fn a_hop_expiring_at_the_publication_is_dead() -> Result<()> {
    with_database("alias_path_expiry", async |pool| {
        mounted(pool).await?;
        entry(pool, E, "z", "registered", CLOCK + 1, resource(2)).await?;
        pointer(pool, resource(2), R, 2).await?;
        assert_eq!(canonical(pool, "child.z.eth").await?, id("child.m.eth"));
        entry(pool, E, "z", "registered", CLOCK, resource(2)).await?;
        assert_eq!(canonical(pool, "child.z.eth").await?, None);
        Ok(())
    })
    .await
}

/// T19's hop rules, as `facts::load_entry` decides them: a reserved hop is live, an
/// unregistered, unknown or pointerless hop ends the walk, a cleared pointer ends it.
#[tokio::test]
async fn hops_follow_the_composed_readers_entry_rules() -> Result<()> {
    with_database("alias_path_hops", async |pool| {
        mounted(pool).await?;
        entry(pool, E, "z", "reserved", LIVE, resource(2)).await?;
        assert_eq!(canonical(pool, "child.z.eth").await?, None, "pointerless");
        pointer(pool, resource(2), R, 2).await?;
        assert_eq!(canonical(pool, "child.z.eth").await?, id("child.m.eth"));
        for status in ["unregistered", "unknown"] {
            entry(pool, E, "z", status, LIVE, resource(2)).await?;
            assert_eq!(canonical(pool, "child.z.eth").await?, None, "{status}");
        }
        entry(pool, E, "z", "registered", LIVE, resource(2)).await?;
        pointer(
            pool,
            resource(2),
            "0x0000000000000000000000000000000000000000",
            3,
        )
        .await?;
        assert_eq!(canonical(pool, "child.z.eth").await?, None, "cleared");
        Ok(())
    })
    .await
}

/// T11, T12 and T15. The leaf is taken as it is: expired, unregistered or reserved, it still
/// names its canonical row, and a reservation also names its expiry. A leaf with no entry, or
/// an unknown one, names nothing.
#[tokio::test]
async fn the_leaf_is_taken_as_it_is() -> Result<()> {
    with_database("alias_path_leaf", async |pool| {
        mounted(pool).await?;
        entry(pool, E, "z", "registered", LIVE, resource(2)).await?;
        pointer(pool, resource(2), R, 2).await?;
        for status in ["reserved", "unregistered", "registered"] {
            entry(pool, R, "child", status, CLOCK - 10, resource(9)).await?;
            let target = walk(pool, "child.z.eth").await?.target;
            assert_eq!(
                target
                    .as_ref()
                    .map(|target| &target.canonical_logical_name_id),
                id("child.m.eth").as_ref(),
                "{status}"
            );
            // Only a reservation carries the expiry its row is matched by.
            assert_eq!(
                target.and_then(|target| target.reservation_expiry),
                (status == "reserved").then(|| (CLOCK - 10).to_string()),
                "{status}"
            );
        }
        entry(pool, R, "child", "unknown", LIVE, resource(9)).await?;
        assert_eq!(canonical(pool, "child.z.eth").await?, None);
        assert_eq!(canonical(pool, "other.z.eth").await?, None);
        Ok(())
    })
    .await
}

/// T4's canonical move: the latest association names the canonical path.
#[tokio::test]
async fn the_latest_association_names_the_canonical_path() -> Result<()> {
    with_database("alias_path_association", async |pool| {
        mounted(pool).await?;
        entry(pool, E, "a", "registered", LIVE, resource(4)).await?;
        pointer(pool, resource(4), R, 2).await?;
        associate(pool, "child.a.eth", resource(9), 2).await?;
        assert_eq!(canonical(pool, "child.m.eth").await?, id("child.a.eth"));
        assert_eq!(canonical(pool, "child.a.eth").await?, None);
        Ok(())
    })
    .await
}

/// T21. A token that never had a canonical name, such as one in a registry whose claim points
/// back at a parent with no name, is not served under an anchored mount either.
#[tokio::test]
async fn a_token_without_a_canonical_name_is_not_served() -> Result<()> {
    with_database("alias_path_unnamed", async |pool| {
        mounted(pool).await?;
        entry(pool, R, "orphan", "registered", LIVE, resource(7)).await?;
        let walk = walk(pool, "orphan.m.eth").await?;
        assert_eq!(
            walk,
            AliasWalk {
                target: None,
                statements: 7
            }
        );
        Ok(())
    })
    .await
}

/// T10. Undo removes the pointer event and the entry row, and the path stops resolving.
/// Replay restores them, and it resolves again.
#[tokio::test]
async fn an_undone_mount_stops_resolving_until_replayed() -> Result<()> {
    with_database("alias_path_reorg", async |pool| {
        mounted(pool).await?;
        entry(pool, E, "z", "registered", LIVE, resource(2)).await?;
        pointer(pool, resource(2), R, 2).await?;
        assert_eq!(canonical(pool, "child.z.eth").await?, id("child.m.eth"));
        sqlx::query("DELETE FROM normalized_events WHERE resource_id = $1")
            .bind(resource(2))
            .execute(pool)
            .await?;
        sqlx::query("DELETE FROM project_ens_v2_entry_owner WHERE resource_id = $1")
            .bind(resource(2))
            .execute(pool)
            .await?;
        assert_eq!(canonical(pool, "child.z.eth").await?, None);
        entry(pool, E, "z", "registered", LIVE, resource(2)).await?;
        pointer(pool, resource(2), R, 2).await?;
        assert_eq!(canonical(pool, "child.z.eth").await?, id("child.m.eth"));
        Ok(())
    })
    .await
}

/// Condition (b) and T18. The walk reads the publication. A selected position below it is
/// stale before any entry is read, as a composed row is.
#[tokio::test]
async fn a_walk_at_another_position_is_stale() -> Result<()> {
    with_database("alias_path_at", async |pool| {
        mounted(pool).await?;
        entry(pool, E, "z", "registered", LIVE, resource(2)).await?;
        pointer(pool, resource(2), R, 2).await?;
        let name = "child.z.eth";
        let id = logical_name_id_for_name("ens", name);
        for block in [PUBLISHED - 1, PUBLISHED + 1] {
            let error = resolve_alias_path(pool, "ens", name, &id, &at(block))
                .await
                .expect_err("a position other than the publication");
            assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale, "{block}");
        }
        sqlx::query("UPDATE project_family_marker SET state = 'bootstrap_pending'")
            .execute(pool)
            .await?;
        let error = resolve_alias_path(pool, "ens", name, &id, &at(PUBLISHED))
            .await
            .expect_err("no servable publication");
        assert_eq!(error.kind(), SnapshotSelectionErrorKind::Stale);
        Ok(())
    })
    .await
}

fn collapse(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn without_comment(statement: &str) -> &str {
    statement
        .split_once("*/")
        .map_or(statement, |(_, rest)| rest)
}

/// T19. The entry statement and the subregistry probe are the composed reader's own, so an
/// edit to either reader's statements fails here until the other follows.
#[test]
fn the_entry_and_pointer_statements_are_the_composed_readers_twins() {
    let facts = collapse(include_str!("name/resolution_path/facts.rs"));
    for statement in [without_comment(ENTRY_SQL), POINTER_TEMPLATE] {
        let statement = collapse(statement);
        assert!(
            facts.contains(&statement),
            "facts.rs no longer runs: {statement}"
        );
    }
}
