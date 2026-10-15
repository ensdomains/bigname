//! The explicit lookup publication APIs share the ordinary name and record selectors. These
//! inputs are admitted normalized events driven through Project; no lookup state is seeded.
#[path = "families_support/mod.rs"]
mod support;

use anyhow::{Result, ensure};
use bigname_project::families::{self, FamilyMode};
use bigname_storage::families::{
    lookup::{LookupInventoryDependency, compose_lookup_inventories_at, compose_lookup_names_at},
    name::{load_family_name, load_family_publication},
    records::{FamilyAttribution, load_family_record_inventory_detail},
};
use serde_json::json;
use support::{
    CHAIN, Event, Fixture,
    ens_v2_path::{self, Pointer},
    uuid,
};
use uuid::Uuid;

#[tokio::test]
async fn shared_lookup_publication_matches_name_selection_and_inventory() -> Result<()> {
    let fixture = Fixture::new("lookup_publication_parity", 20).await?;
    let name = format!("ens:0x{:064x}", 1);
    let resource = uuid(1);
    let resolver = "0x00000000000000000000000000000000000000a1";
    fixture
        .binding(&uuid(100), &name, &resource, "ens_v1", 9, 0, None)
        .await?;
    fixture.write(10, 1, "RegistrationGranted", "ens_v1_registrar_l1", Some(&name), Some(&resource),
        json!({"authority_kind":"registrar", "status":"registered", "registrant":"0x00000000000000000000000000000000000000aa", "expiry":2_000_000_000u64}),
        "0x00000000000000000000000000000000000000e3").await?;
    fixture
        .write(
            10,
            2,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&name),
            Some(&resource),
            json!({"node":format!("0x{:064x}",1), "resolver":resolver}),
            "0x00000000000000000000000000000000000000e1",
        )
        .await?;
    fixture.write(10, 3, "RecordChanged", "ens_v1_resolver_l1", Some(&name), Some(&resource),
        json!({"node":format!("0x{:064x}",1), "record_key":"text:arbitrary", "record_family":"text", "selector_key":"arbitrary", "value":"one", "source_event":"TextChanged"}), resolver).await?;
    fixture.write(10, 4, "RecordChanged", "ens_v1_resolver_l1", Some(&name), Some(&resource),
        json!({"node":format!("0x{:064x}",1), "record_key":"text:unchanged", "record_family":"text", "selector_key":"unchanged", "value":"kept", "source_event":"TextChanged"}), resolver).await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    let publication = load_family_publication(&fixture.pool, CHAIN)
        .await?
        .expect("published");
    let ordinary = load_family_name(&fixture.pool, &name)
        .await?
        .expect("served");
    let resource: Uuid = resource.parse()?;
    let inventory = load_family_record_inventory_detail(
        &fixture.pool,
        CHAIN,
        resource,
        FamilyAttribution::Omit,
    )
    .await?
    .expect("inventory");
    let mut conn = fixture.pool.acquire().await?;
    let names =
        compose_lookup_names_at(&mut conn, &publication, std::slice::from_ref(&name)).await?;
    let selected = names[&name].core.as_ref().expect("lookup name");
    assert_eq!(selected.resource_id, ordinary.resource_id);
    assert_eq!(
        selected.record_serving_resource_id,
        ordinary.record_serving_resource_id()
    );
    assert_eq!(
        selected.declared_summary["registration"],
        ordinary.declared_summary["registration"]
    );
    assert_eq!(
        selected.declared_summary["resolver"],
        ordinary.declared_summary["resolver"]
    );
    let composed = compose_lookup_inventories_at(&mut conn, &publication, &[resource]).await?;
    let actual = composed[&resource]
        .inventory
        .as_ref()
        .expect("lookup inventory");
    assert_eq!(actual.row, inventory.row);
    ensure!(composed[&resource].records.contains_key("text:arbitrary"));
    ensure!(
        composed[&resource]
            .dependencies
            .contains(&LookupInventoryDependency::Link {
                resolver_address: resolver.into(),
                node: bigname_storage::families::records::DEFAULT_RECORD_NODE.into(),
            }),
        "missing default links must remain dependencies"
    );
    let missing = Uuid::from_u128(999);
    let absent = compose_lookup_inventories_at(&mut conn, &publication, &[missing]).await?;
    assert!(absent[&missing].inventory.is_none());
    ensure!(
        absent[&missing]
            .dependencies
            .contains(&LookupInventoryDependency::ResourcePointer {
                resource_id: missing
            })
    );
    drop(conn);
    let before = fixture.rows("project_lookup_record").await?;
    ensure!(
        before.len() == 2,
        "Project publishes both selected keys: {before:?}"
    );
    let names_before = fixture.rows("project_lookup_name").await?;
    let dependencies_before = fixture.rows("project_lookup_dependency").await?;
    fixture.write(13, 1, "RecordChanged", "ens_v1_resolver_l1", Some(&name), Some(&resource.to_string()),
        json!({"node":format!("0x{:064x}",1), "record_key":"text:arbitrary", "record_family":"text", "selector_key":"arbitrary", "value":"two", "source_event":"TextChanged"}), resolver).await?;
    fixture.assert_undo_restores(13).await?;
    assert_eq!(
        fixture.rows("project_lookup_name").await?,
        names_before,
        "value-only writes must not rewrite name cores"
    );
    assert_eq!(
        fixture.rows("project_lookup_dependency").await?,
        dependencies_before,
        "value-only writes keep selection dependencies"
    );
    let changed_keys: Vec<String> = sqlx::query_scalar(
        "SELECT key::jsonb ->> 2 FROM project_family_undo
        WHERE chain_id=$1 AND block_number=13 AND family='project_lookup_record'",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        changed_keys,
        ["text:arbitrary"],
        "only the changed selected value is journalled"
    );
    let unchanged = |rows: Vec<serde_json::Value>| {
        rows.into_iter()
            .find(|row| row["record_key"] == "text:unchanged")
            .expect("unchanged key")
    };
    assert_eq!(
        unchanged(fixture.rows("project_lookup_record").await?),
        unchanged(before)
    );
    fixture.apply(14, FamilyMode::Normal).await?;
    let unrelated: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_family_undo
        WHERE chain_id=$1 AND block_number=14 AND family LIKE 'project_lookup_%'",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        unrelated, 0,
        "unrelated publication does not rewrite lookup components"
    );
    fixture.assert_rebuild_equal(14).await?;
    fixture.cleanup().await
}

// A resolver-pointer change on an ancestor resource can move other names'
// resolution paths: every name whose lookup depends on that pointer, the descendant
// included, is captured as lookup work
// and recomposed, not just the ancestor itself.
#[tokio::test]
async fn an_ancestor_resolver_pointer_change_recomposes_descendant_lookup_work() -> Result<()> {
    let fixture = Fixture::new("lookup_publication_descendant_pointer", 20).await?;
    let ancestor = format!("ens:0x{:064x}", 1);
    let descendant = format!("ens:0x{:064x}", 2);
    let resource = uuid(1);
    let first = "0x00000000000000000000000000000000000000a1";
    let second = "0x00000000000000000000000000000000000000a2";
    fixture
        .binding(&uuid(100), &ancestor, &resource, "ens_v1", 9, 0, None)
        .await?;
    fixture
        .binding(&uuid(101), &descendant, &resource, "ens_v1", 9, 0, None)
        .await?;
    fixture.write(10, 1, "RegistrationGranted", "ens_v1_registrar_l1", Some(&ancestor), Some(&resource),
        json!({"authority_kind":"registrar", "status":"registered", "registrant":"0x00000000000000000000000000000000000000aa", "expiry":2_000_000_000u64}),
        "0x00000000000000000000000000000000000000e3").await?;
    fixture
        .write(
            10,
            2,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&ancestor),
            Some(&resource),
            json!({"node":format!("0x{:064x}",1), "resolver":first}),
            "0x00000000000000000000000000000000000000e1",
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await?;
    fixture
        .write(
            13,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&ancestor),
            Some(&resource),
            json!({"node":format!("0x{:064x}",1), "resolver":second}),
            "0x00000000000000000000000000000000000000e1",
        )
        .await?;
    fixture.apply(13, FamilyMode::Normal).await?;
    // The ancestor itself is name work again, and the inventory every name bound to
    // its resource serves from — the descendant included — is recomposed against the
    // new resolver: its dependency now links the second resolver.
    let lookup_names: Vec<String> = sqlx::query_scalar(
        "SELECT key::jsonb ->> 1 FROM project_family_undo
        WHERE chain_id=$1 AND block_number=13 AND family='project_lookup_name'",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    ensure!(
        lookup_names.contains(&ancestor),
        "the ancestor is lookup work after its pointer change: {lookup_names:?}"
    );
    let inventories: Vec<String> = sqlx::query_scalar(
        "SELECT key::jsonb ->> 1 FROM project_family_undo
        WHERE chain_id=$1 AND block_number=13 AND family='project_lookup_inventory'",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    ensure!(
        inventories.contains(&resource),
        "the shared inventory is lookup work after the ancestor pointer change: {inventories:?}"
    );
    let publication = load_family_publication(&fixture.pool, CHAIN)
        .await?
        .expect("published");
    let resource_id: Uuid = resource.parse()?;
    let mut conn = fixture.pool.acquire().await?;
    let names =
        compose_lookup_names_at(&mut conn, &publication, std::slice::from_ref(&descendant)).await?;
    let selected = names[&descendant]
        .core
        .as_ref()
        .expect("descendant lookup name");
    assert_eq!(selected.resource_id, Some(resource_id));
    let composed = compose_lookup_inventories_at(&mut conn, &publication, &[resource_id]).await?;
    ensure!(
        composed[&resource_id]
            .dependencies
            .contains(&LookupInventoryDependency::Link {
                resolver_address: second.into(),
                node: bigname_storage::families::records::DEFAULT_RECORD_NODE.into(),
            }),
        "the descendant inventory follows the ancestor's new resolver"
    );
    drop(conn);
    fixture.cleanup().await
}

/// The ENSv2 mirror resolver `foo`'s entry points at, declared with the ENSv1 mirror role.
const MIRROR: &str = "0x00000000000000000000000000000000000000d7";
/// A resolver no manifest declares, so the mirror cannot read through it.
const UNDECLARED: &str = "0x00000000000000000000000000000000000000d8";
/// The ENSv1 registry address the node events carry and the mirror's manifest names.
const V1_REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const FIRST_OWNER: &str = "0x00000000000000000000000000000000000000b1";
const SECOND_OWNER: &str = "0x00000000000000000000000000000000000000b2";
const READ_AHEAD_PLAN_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];

/// What a composition of the descendant reads through the mirror: no ancestor resolver
/// (`Absent`), or the ancestor's undeclared resolver (`Unknown`).
#[derive(Clone, Copy, Debug)]
enum Through {
    Absent,
    Unknown,
}

/// The names of the read-ahead fixture: the parent `a.foo.eth`, which has no surface until a
/// test writes one, and the descendant `x.a.foo.eth`. They are on distinct resources.
struct ReadAhead {
    fixture: Fixture,
    parent: String,
    descendant: String,
}

/// Run every later statement of the fixture's one session, and of every new session, in
/// `mode`.
async fn plan_mode(fixture: &Fixture, mode: &str) -> Result<()> {
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&fixture.pool)
        .await?;
    sqlx::raw_sql(&format!(
        "ALTER DATABASE \"{}\" SET plan_cache_mode = {mode}; SET plan_cache_mode = {mode}",
        name.replace('"', r#""""#)
    ))
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// An ENSv2 resolver manifest at block 0 that declares `MIRROR` as an ENSv1 mirror resolver.
async fn mirror_manifest(fixture: &Fixture) -> Result<()> {
    let payload = json!({
        "manifest_version": 1, "namespace": "ens", "source_family": "ens_v2_resolver_l1",
        "chain": CHAIN, "rollout_status": "active",
        "correlation_addresses": {"ens_v1_registry": V1_REGISTRY},
        "contracts": [{"role": "ensv1_mirror_resolver", "address": MIRROR,
                       "proxy_kind": "none", "read_features": ["addr", "text"]}]
    });
    let manifest: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v2_resolver_l1', $1, 'fixture', 'active', 'fixture',
             'fixture/ens_v2_resolver_l1.toml', $2) RETURNING manifest_id",
    )
    .bind(CHAIN)
    .bind(&payload)
    .fetch_one(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, after_state)
         VALUES ($1, 'ens', 'SourceManifestUpdated', 'ens_v2_resolver_l1', 1, $2, $3, 0, $4,
             'manifest_sync', 'canonical',
             jsonb_build_object('rollout_status', 'active', 'manifest_payload', $5::jsonb))",
    )
    .bind(format!("mirror-manifest:{manifest}"))
    .bind(manifest)
    .bind(CHAIN)
    .bind(support::hash(0))
    .bind(&payload)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// Blocks 0 to 3 of the read-ahead history, published through block 3:
///
/// - 0: the root registry is admitted with its `eth` entry. An ENSv2 resolver manifest
///   declares `MIRROR` as an ENSv1 mirror resolver.
/// - 1: `foo` is registered in the `.eth` registry, and its entry's resolver is `MIRROR`.
/// - 2: the descendant has an ENSv1 registry owner and no resolver of its own. The parent's
///   node has an ENSv1 registry owner and the resolver `UNDECLARED`, from events that carry
///   no name.
/// - 3: a block that names nothing.
///
/// The descendant's composition walks the ENSv2 path to `MIRROR`, and the mirror walks the
/// ENSv1 registry up from the descendant. The parent's registry pointer is reachable only
/// through the parent's surface.
async fn read_ahead(prefix: &str, mode: &str) -> Result<ReadAhead> {
    let fixture = Fixture::new(prefix, 12).await?;
    plan_mode(&fixture, mode).await?;
    let eth = ens_v2_path::admit(&fixture).await?;
    mirror_manifest(&fixture).await?;
    ens_v2_path::register(&fixture, 1, 1, &eth, "foo", 1_900_000_000).await?;
    ens_v2_path::point(&fixture, 1, 3, &eth, "foo", Pointer::Resolver(MIRROR)).await?;

    let descendant = ens_v2_path::surface(&fixture, "x.a.foo.eth").await?;
    let resource = uuid(0x3001);
    fixture
        .binding(&uuid(301), &descendant, &resource, "ens_v1", 2, 1, None)
        .await?;
    fixture
        .write(
            2,
            1,
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            Some(&descendant),
            Some(&resource),
            json!({"authority_kind": "registry", "owner": ens_v2_path::OWNER,
                   "registry_owner": ens_v2_path::OWNER,
                   "node": descendant.trim_start_matches("ens:")}),
            V1_REGISTRY,
        )
        .await?;
    let parent = ens_v2_path::logical_name_id("a.foo.eth");
    let node = parent.trim_start_matches("ens:");
    fixture
        .write(
            2,
            2,
            "ResolverChanged",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"node": node, "resolver": UNDECLARED}),
            V1_REGISTRY,
        )
        .await?;
    owner(&fixture, 2, 3, node, FIRST_OWNER).await?;
    ens_v2_path::filler(&fixture, 3).await?;
    fixture.apply(3, FamilyMode::Normal).await?;
    let names = ReadAhead {
        fixture,
        parent,
        descendant,
    };
    names.assert_published(3, Through::Absent).await?;
    Ok(names)
}

/// A node-only ENSv1 registry owner change of `node` to `owner`: the event carries no name.
async fn owner(fixture: &Fixture, block: i64, log: i64, node: &str, owner: &str) -> Result<()> {
    fixture
        .write(
            block,
            log,
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"authority_kind": "registry", "owner": owner, "registry_owner": owner,
                   "node": node}),
            V1_REGISTRY,
        )
        .await?;
    Ok(())
}

impl ReadAhead {
    /// The publication is at `block`, the descendant's stored lookup core equals a fresh
    /// composition at that publication, and the composition reads `through`.
    async fn assert_published(&self, block: i64, through: Through) -> Result<()> {
        let publication = load_family_publication(&self.fixture.pool, CHAIN)
            .await?
            .expect("published");
        ensure!(
            publication.block_number == block,
            "published at {}, not {block}",
            publication.block_number
        );
        let stored: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT core FROM project_lookup_name WHERE chain_id = $1 AND logical_name_id = $2",
        )
        .bind(CHAIN)
        .bind(&self.descendant)
        .fetch_optional(&self.fixture.pool)
        .await?
        .flatten();
        let stored = stored.unwrap_or_default();
        let mut conn = self.fixture.pool.acquire().await?;
        let fresh = compose_lookup_names_at(
            &mut conn,
            &publication,
            std::slice::from_ref(&self.descendant),
        )
        .await?;
        drop(conn);
        let fresh = json!(fresh[&self.descendant].core);
        ensure!(
            stored == fresh,
            "at {block} the stored lookup core differs from a fresh composition\n\
             stored: {stored}\nfresh: {fresh}"
        );
        let summary = &stored["declared_summary"];
        let unresolvable = summary
            .get("unresolvable_reason")
            .filter(|reason| !reason.is_null());
        let unsupported = summary
            .get("resolution_unsupported_reason")
            .filter(|reason| !reason.is_null());
        match through {
            Through::Absent => ensure!(
                unresolvable.is_some() && unsupported.is_none(),
                "at {block} the descendant reads a resolver above the publication: {summary}"
            ),
            Through::Unknown => ensure!(
                unsupported == Some(&json!("ens_v2_path_not_projected")),
                "at {block} the descendant does not read the parent's resolver: {summary}"
            ),
        }
        Ok(())
    }

    /// The plan mode every session of the fixture still runs in.
    async fn assert_plan_mode(&self, mode: &str) -> Result<()> {
        let current: String = sqlx::query_scalar("SHOW plan_cache_mode")
            .fetch_one(&self.fixture.pool)
            .await?;
        ensure!(current == mode, "plan_cache_mode is {current}, not {mode}");
        Ok(())
    }
}

// Interpret runs ahead of Project: the parent's first surface, anchored at block 5, exists
// before Project publishes block 4. Block 4 changes the parent's owner from one address to
// another, which recomposes no name under it. A composition at block 4 reads no surface
// above block 4, so the descendant still finds no resolver through the mirror, as its stored
// core does. At block 5 the first surface recomposes the descendant, which now reads the
// parent's resolver. Undo, a lower-target rebuild and replay give the same rows.
#[tokio::test]
async fn a_surface_above_the_publication_is_not_read_through_the_mirror() -> Result<()> {
    for mode in READ_AHEAD_PLAN_MODES {
        let names = read_ahead(&format!("lookup_read_ahead_{mode}"), mode).await?;
        let fixture = &names.fixture;
        let node = names.parent.trim_start_matches("ens:").to_owned();
        owner(fixture, 4, 1, &node, SECOND_OWNER).await?;
        ens_v2_path::filler(fixture, 5).await?;
        ens_v2_path::surface_at(fixture, "a.foo.eth", 5).await?;

        fixture.apply(4, FamilyMode::Normal).await?;
        names.assert_published(4, Through::Absent).await?;
        fixture.apply(5, FamilyMode::Normal).await?;
        names.assert_published(5, Through::Unknown).await?;

        // Undo to block 4 with the surface still there, then rebuild to block 4.
        let undone = families::undo_to(&fixture.pool, CHAIN, 4).await?;
        ensure!(undone == 1, "undid {undone} blocks");
        names.assert_published(4, Through::Absent).await?;
        fixture.assert_rebuild_equal(4).await?;
        names.assert_published(4, Through::Absent).await?;
        fixture.apply(5, FamilyMode::Normal).await?;
        names.assert_published(5, Through::Unknown).await?;
        fixture.assert_undo_restores(5).await?;
        fixture.assert_rebuild_equal(5).await?;
        names.assert_published(5, Through::Unknown).await?;
        names.assert_plan_mode(mode).await?;
        names.fixture.cleanup().await?;
    }
    Ok(())
}

// The parent's surface is anchored at block 4 and Project publishes it there. An Interpret
// redo then re-anchors it at block 6, and Project replays blocks 4 and 5: neither reads the
// surface, and block 6 does. A reorg then orphans the surface, and the replay of block 6
// reads it no more.
#[tokio::test]
async fn a_re_anchored_or_orphaned_surface_is_read_from_its_block_only() -> Result<()> {
    for mode in READ_AHEAD_PLAN_MODES {
        let names = read_ahead(&format!("lookup_reanchor_{mode}"), mode).await?;
        let fixture = &names.fixture;
        let node = names.parent.trim_start_matches("ens:").to_owned();
        owner(fixture, 4, 1, &node, SECOND_OWNER).await?;
        for block in [5, 6] {
            ens_v2_path::filler(fixture, block).await?;
        }
        ens_v2_path::surface_at(fixture, "a.foo.eth", 4).await?;
        fixture.apply(4, FamilyMode::Normal).await?;
        names.assert_published(4, Through::Unknown).await?;

        sqlx::query(
            "UPDATE name_surfaces SET block_number = 6, block_hash = $2
             WHERE logical_name_id = $1",
        )
        .bind(&names.parent)
        .bind(support::hash(6))
        .execute(&fixture.pool)
        .await?;
        fixture
            .apply(5, FamilyMode::Redo { from: 4, to: 5 })
            .await?;
        names.assert_published(5, Through::Absent).await?;
        fixture.apply(6, FamilyMode::Normal).await?;
        names.assert_published(6, Through::Unknown).await?;

        sqlx::query(
            "UPDATE name_surfaces SET canonicality_state = 'orphaned'
             WHERE logical_name_id = $1",
        )
        .bind(&names.parent)
        .execute(&fixture.pool)
        .await?;
        fixture
            .apply(6, FamilyMode::Redo { from: 6, to: 6 })
            .await?;
        names.assert_published(6, Through::Absent).await?;
        fixture.assert_rebuild_equal(6).await?;
        names.assert_published(6, Through::Absent).await?;
        names.assert_plan_mode(mode).await?;
        names.fixture.cleanup().await?;
    }
    Ok(())
}

/// The stored lookup inventory of `resource` (its metadata and every stored record) and a fresh
/// composition of it at the current publication, in the same shape.
async fn stored_and_fresh_inventory(
    fixture: &Fixture,
    resource: Uuid,
) -> Result<(serde_json::Value, serde_json::Value)> {
    let publication = load_family_publication(&fixture.pool, CHAIN)
        .await?
        .expect("published");
    let metadata: Option<serde_json::Value> = sqlx::query_scalar(
        "SELECT metadata FROM project_lookup_inventory WHERE chain_id = $1 AND resource_id = $2",
    )
    .bind(CHAIN)
    .bind(resource)
    .fetch_optional(&fixture.pool)
    .await?
    .flatten();
    let records: Vec<(String, serde_json::Value)> = sqlx::query_as(
        "SELECT record_key, payload FROM project_lookup_record
         WHERE chain_id = $1 AND resource_id = $2 ORDER BY record_key",
    )
    .bind(CHAIN)
    .bind(resource)
    .fetch_all(&fixture.pool)
    .await?;
    let stored = json!({"metadata": metadata.unwrap_or_default(),
                        "records": records.into_iter().collect::<serde_json::Map<_, _>>()});
    let mut conn = fixture.pool.acquire().await?;
    let composed = compose_lookup_inventories_at(&mut conn, &publication, &[resource]).await?;
    drop(conn);
    let fresh = &composed[&resource];
    let fresh = json!({"metadata": fresh.metadata(), "records": fresh.records});
    Ok((stored, fresh))
}

/// The registry mounted under both `q.eth` and `a.eth`, and the one `q`'s entry moves to.
const SUB: &str = "0x00000000000000000000000000000000000000c7";
const OTHER: &str = "0x00000000000000000000000000000000000000c8";

// One registry is mounted at two parents: `q`'s and `a`'s entries in the `.eth` registry both
// name it as their subregistry, so `x.q.eth` and `x.a.eth` are bound to the one entry resource
// of its `x`. The mirror pointer of that entry names `x.q.eth`, and the mirror walk starts there
// and finds the resolver of `eth`. At block 4 `q`'s entry moves to another registry, which closes
// `x.q.eth`'s binding, so only `x.a.eth` reads the entry's records.
//
// An Interpret redo of block 1 then re-anchors `x.q.eth`'s surface at its surviving observation
// at block 6, and Project replays from block 1. Below block 6 the walk has no readable queried
// surface, so the inventory names no mirrored node. At block 6 the surface's identity dependency
// re-runs the inventory, which finds `eth` again. `x.q.eth` is no longer bound to the entry, so
// its new lookup row queues no reference work for it. Each block's stored inventory and lookup
// name cores equal a fresh composition.
#[tokio::test]
async fn a_mirror_pointer_name_above_the_publication_is_not_walked() -> Result<()> {
    for mode in READ_AHEAD_PLAN_MODES {
        let fixture = Fixture::new(&format!("lookup_queried_surface_{mode}"), 12).await?;
        plan_mode(&fixture, mode).await?;
        let eth = ens_v2_path::admit(&fixture).await?;
        mirror_manifest(&fixture).await?;
        let top = ens_v2_path::surface(&fixture, "eth").await?;
        let queried = ens_v2_path::surface_at(&fixture, "x.q.eth", 1).await?;
        let reader = ens_v2_path::surface(&fixture, "x.a.eth").await?;
        ens_v2_path::register(&fixture, 1, 1, &eth, "q", 1_900_000_000).await?;
        ens_v2_path::register(&fixture, 1, 3, &eth, "a", 1_900_000_000).await?;
        for (log, label) in [(5, "q"), (6, "a")] {
            ens_v2_path::point(&fixture, 1, log, &eth, label, Pointer::Subregistry(SUB)).await?;
        }
        ens_v2_path::created(&fixture, 1, 7, SUB).await?;
        ens_v2_path::parent(&fixture, 1, 8, SUB, &eth, "q").await?;
        ens_v2_path::register(&fixture, 1, 10, SUB, "x", 1_900_000_000).await?;
        let resource = ens_v2_path::entry_resource(SUB, "x");
        fixture
            .binding(&uuid(401), &queried, &resource, "ens_v2", 1, 0, Some(4))
            .await?;
        fixture
            .binding(&uuid(402), &reader, &resource, "ens_v2", 1, 0, None)
            .await?;
        fixture
            .write(
                1,
                12,
                "ResolverChanged",
                "ens_v1_registry_l1",
                None,
                None,
                json!({"node": top.trim_start_matches("ens:"), "resolver": UNDECLARED}),
                V1_REGISTRY,
            )
            .await?;
        ens_v2_path::point_named(
            &fixture,
            2,
            0,
            SUB,
            "x",
            Pointer::Resolver(MIRROR),
            &queried,
        )
        .await?;
        ens_v2_path::point(&fixture, 4, 0, &eth, "q", Pointer::Subregistry(OTHER)).await?;
        // The observation of `x.q.eth` the redo leaves outside its range.
        fixture
            .event(
                Event::new(
                    "observed-x.q.eth",
                    6,
                    1,
                    "PreimageObserved",
                    "ens_v1_registry_l1",
                )
                .name(&queried),
            )
            .await?;
        for block in [3, 5, 6] {
            ens_v2_path::filler(&fixture, block).await?;
        }
        let resource: Uuid = resource.parse()?;
        let top_node = top.trim_start_matches("ens:").to_owned();
        let check = async |block: i64, mirrored: Option<&str>| -> Result<()> {
            let publication = load_family_publication(&fixture.pool, CHAIN)
                .await?
                .expect("published");
            ensure!(
                publication.block_number == block,
                "published at {}",
                publication.block_number
            );
            let (stored, fresh) = stored_and_fresh_inventory(&fixture, resource).await?;
            ensure!(
                stored == fresh,
                "at {block} the stored inventory differs from a fresh composition\n\
                 stored: {stored}\nfresh: {fresh}"
            );
            for name in [&queried, &reader] {
                let stored: Option<serde_json::Value> = sqlx::query_scalar(
                    "SELECT core FROM project_lookup_name WHERE chain_id = $1 AND logical_name_id = $2",
                )
                .bind(CHAIN)
                .bind(name)
                .fetch_optional(&fixture.pool)
                .await?
                .flatten();
                let mut conn = fixture.pool.acquire().await?;
                let fresh =
                    compose_lookup_names_at(&mut conn, &publication, std::slice::from_ref(name))
                        .await?;
                drop(conn);
                let fresh = json!(fresh[name].core);
                let stored = stored.unwrap_or_default();
                ensure!(
                    stored == fresh,
                    "at {block} the stored lookup core of {name} differs from a fresh \
                     composition\nstored: {stored}\nfresh: {fresh}"
                );
            }
            let node = stored["metadata"]
                .pointer("/provenance/mirror/mirrored_node")
                .and_then(serde_json::Value::as_str);
            ensure!(
                node == mirrored,
                "at {block} the inventory's mirrored node is {node:?}, not {mirrored:?}: {stored}"
            );
            Ok(())
        };
        fixture.apply(3, FamilyMode::Normal).await?;
        check(3, Some(top_node.as_str())).await?;

        // Interpret's redo of block 1 re-observes nothing for `x.q.eth`, so the surface takes
        // the anchor of its earliest surviving observation outside the range, at block 6
        // (crates/interpret/src/write/reanchor.rs, `stable_identities`). The pointer event at
        // block 2 is outside the range and is not an observation that can anchor the surface.
        // The redo also orphans every binding that starts in its range
        // (crates/interpret/src/write.rs:345-367), so `x.q.eth`'s block-1 binding is orphaned.
        // The replay binds `x.a.eth` again, which the fixture's canonical binding stands for.
        sqlx::query(
            "UPDATE name_surfaces SET block_number = 6, block_hash = $2
             WHERE logical_name_id = $1",
        )
        .bind(&queried)
        .bind(support::hash(6))
        .execute(&fixture.pool)
        .await?;
        sqlx::query(
            "UPDATE surface_bindings SET canonicality_state = 'orphaned'
             WHERE surface_binding_id = $1::uuid",
        )
        .bind(uuid(401))
        .execute(&fixture.pool)
        .await?;
        fixture
            .apply(5, FamilyMode::Redo { from: 1, to: 5 })
            .await?;
        check(5, None).await?;
        fixture.apply(6, FamilyMode::Normal).await?;
        let serving: Option<String> = sqlx::query_scalar(
            "SELECT core ->> 'record_serving_resource_id' FROM project_lookup_name
             WHERE chain_id = $1 AND logical_name_id = $2",
        )
        .bind(CHAIN)
        .bind(&queried)
        .fetch_optional(&fixture.pool)
        .await?
        .flatten();
        ensure!(
            serving.as_deref() != Some(resource.to_string().as_str()),
            "x.q.eth's lookup row reads the entry's records, so it queues reference work"
        );
        check(6, Some(top_node.as_str())).await?;
        fixture.assert_undo_restores(6).await?;
        fixture.assert_rebuild_equal(6).await?;
        check(6, Some(top_node.as_str())).await?;
        let current: String = sqlx::query_scalar("SHOW plan_cache_mode")
            .fetch_one(&fixture.pool)
            .await?;
        ensure!(current == mode, "plan_cache_mode is {current}, not {mode}");
        fixture.cleanup().await?;
    }
    Ok(())
}
