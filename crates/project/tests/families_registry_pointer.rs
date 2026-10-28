//! The registry pointer (F4) and the record inventory a resolver holds for the pointer's node.
//! The inventory is kept by resolver and node, so moving the pointer away, and undoing the move,
//! leaves what the resolver holds for the node as it was.
mod families_support;

use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use bigname_project::families::{self, FamilyMode};
use bigname_storage::families::records::FamilyRecordInventory;
use families_support::{CHAIN, Fixture, ens_v2_path, hash, uuid};
use serde_json::{Value, json};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const R1: &str = "0x00000000000000000000000000000000000000a1";
const R2: &str = "0x00000000000000000000000000000000000000a2";

fn node() -> String {
    format!("0x{:064x}", 1)
}

fn name() -> String {
    format!("ens:{}", node())
}

/// An active ENSv1 resolver manifest from block 1 that declares R1 a public resolver, so R1 has
/// a supported classification.
async fn declare_r1(fixture: &Fixture) -> Result<()> {
    declare(fixture, R1).await
}

/// An active ENSv1 resolver manifest from block 1 that declares `resolver` a public resolver.
async fn declare(fixture: &Fixture, resolver: &str) -> Result<()> {
    let payload = json!({"contracts": [{"address": resolver, "role": "public_resolver"}]});
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v1_resolver_l1', $1, 'fixture', 'active', 'fixture',
                 'fixture/ens_v1_resolver_l1.yaml', $2)
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .bind(&payload)
    .fetch_one(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, block_number, block_hash,
             derivation_kind, canonicality_state, before_state, after_state, raw_fact_ref)
         VALUES ($1, 'ens', 'SourceManifestUpdated', 'ens_v1_resolver_l1', 1, $2, $3, 1, $4,
                 'ens_v2_registry_resource_surface', 'canonical', '{}'::jsonb,
                 jsonb_build_object('rollout_status', 'active', 'manifest_payload', $5::jsonb),
                 '{}'::jsonb)",
    )
    .bind(format!("manifest:{id}:1"))
    .bind(id)
    .bind(CHAIN)
    .bind(hash(1))
    .bind(&payload)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// What R1 holds for the node, as the resolver-anchored records route reads it.
async fn held_by_r1(fixture: &Fixture) -> Result<Value> {
    let mut conn = fixture.pool.acquire().await?;
    let inventory =
        FamilyRecordInventory::load_resolver_node_on(&mut conn, CHAIN, R1, "ens", &name(), &node())
            .await?
            .context("R1 has a classification")?;
    Ok(json!({
        "entries": inventory.row.entries,
        "selectors": inventory.row.selectors,
        "coverage": inventory.row.coverage,
    }))
}

async fn pointer(fixture: &Fixture) -> Result<Value> {
    let rows = fixture.rows("project_registry_pointer").await?;
    anyhow::ensure!(rows.len() == 1, "one pointer row: {rows:?}");
    Ok(rows[0]["resolver_address"].clone())
}

#[tokio::test]
async fn undo_restores_the_pointer_and_the_inventory_is_untouched() -> Result<()> {
    let fixture = Fixture::new("families_registry_pointer_undo", 20).await?;
    declare_r1(&fixture).await?;
    let resource = uuid(1);
    let set = |resolver: &str| json!({"node": node(), "resolver": resolver});
    fixture
        .write(
            10,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&name()),
            Some(&resource),
            set(R1),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            2,
            "RecordChanged",
            "ens_v1_resolver_l1",
            None,
            None,
            json!({"node": node(), "record_key": "text:url", "record_family": "text",
                   "selector_key": "url", "value": "https://r1.example",
                   "source_event": "TextChanged"}),
            R1,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&name()),
            Some(&resource),
            set(R2),
            REGISTRY,
        )
        .await?;

    fixture.apply(11, FamilyMode::Normal).await?;
    assert_eq!(pointer(&fixture).await?, json!(R1));
    let held = held_by_r1(&fixture).await?;
    assert!(
        held["entries"].to_string().contains("https://r1.example"),
        "{held:#}"
    );
    fixture.apply(12, FamilyMode::Normal).await?;
    assert_eq!(pointer(&fixture).await?, json!(R2));
    assert_eq!(held_by_r1(&fixture).await?, held, "after the pointer moved");
    let undone = families::undo_to(&fixture.pool, CHAIN, 11).await?;
    assert_eq!(undone, 1);
    assert_eq!(pointer(&fixture).await?, json!(R1));
    assert_eq!(
        held_by_r1(&fixture).await?,
        held,
        "after the move was undone"
    );

    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    assert_eq!(held_by_r1(&fixture).await?, held, "after a rebuild");
    fixture.cleanup().await
}

/// An ENSv1 registry `ResolverChanged` at `block` of `name`, on resource `n`, to `resolver`.
async fn point(
    fixture: &Fixture,
    block: i64,
    log: i64,
    name: &str,
    n: u32,
    resolver: &str,
) -> Result<i64> {
    let id = ens_v2_path::logical_name_id(name);
    let node = id.trim_start_matches("ens:").to_owned();
    fixture
        .write(
            block,
            log,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&id),
            Some(&uuid(n)),
            json!({"node": node, "resolver": resolver}),
            REGISTRY,
        )
        .await
}

/// The names the resolution path selection of the publication at `target` chose. The fixture's
/// pool holds one session, so a session table of the work table's name and shape, created
/// here, is the one the selection writes into (`CREATE TEMP TABLE IF NOT EXISTS`), and it
/// outlives the publication.
async fn selected(fixture: &Fixture, target: i64, names: &[&str]) -> Result<BTreeSet<String>> {
    sqlx::raw_sql(
        "CREATE TEMP TABLE bigname_resolution_path_work (logical_name_id text PRIMARY KEY)",
    )
    .execute(&fixture.pool)
    .await?;
    fixture.apply(target, FamilyMode::Normal).await?;
    let ids: Vec<String> =
        sqlx::query_scalar("SELECT logical_name_id FROM pg_temp.bigname_resolution_path_work")
            .fetch_all(&fixture.pool)
            .await?;
    sqlx::raw_sql("DROP TABLE pg_temp.bigname_resolution_path_work")
        .execute(&fixture.pool)
        .await?;
    Ok(ids
        .into_iter()
        .map(|id| {
            names
                .iter()
                .find(|name| ens_v2_path::logical_name_id(name) == id)
                .map_or(id, |name| (*name).to_owned())
        })
        .collect())
}

// One ENSv1 `ResolverChanged` for a second-level name with a resource writes both the node's
// registry pointer (F4) and the resource's pointer (F5). Under that name only the names of three
// or more labels with no resolver of their own read the change, so `b.foo.eth`, which has its
// own, is not selected. The new resolver is a declared public resolver that `bar.eth` points at
// from block 1, so its classification does not change, which would widen every name that
// points at it. `bar.eth` is not selected either.
#[tokio::test]
async fn an_ensv1_resolver_change_of_a_second_level_name_selects_only_its_readers() -> Result<()> {
    const NAMES: [&str; 6] = [
        "eth",
        "foo.eth",
        "a.foo.eth",
        "b.foo.eth",
        "x.a.foo.eth",
        "bar.eth",
    ];
    let fixture = Fixture::new("families_registry_pointer_readers", 6).await?;
    for name in NAMES {
        ens_v2_path::surface(&fixture, name).await?;
    }
    declare(&fixture, R2).await?;
    point(&fixture, 1, 1, "foo.eth", 1, R1).await?;
    point(&fixture, 1, 2, "b.foo.eth", 2, R1).await?;
    point(&fixture, 1, 3, "bar.eth", 3, R2).await?;
    point(&fixture, 5, 1, "foo.eth", 1, R2).await?;
    fixture.apply(4, FamilyMode::Normal).await?;

    let selected = selected(&fixture, 5, &NAMES).await?;
    let expected: BTreeSet<String> = ["foo.eth", "a.foo.eth", "x.a.foo.eth"]
        .map(str::to_owned)
        .into();
    let journalled: Vec<String> = sqlx::query_scalar(
        "SELECT family FROM project_family_undo WHERE chain_id = $1 AND block_number = 5
           AND family IN ('project_registry_pointer', 'project_resource_pointer')
         ORDER BY family",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    ensure!(
        journalled == ["project_registry_pointer", "project_resource_pointer"],
        "block 5 journalled {journalled:?}, not F4 and F5"
    );
    ensure!(
        selected == expected,
        "the resolver change of foo.eth selected {selected:?}, not {expected:?}"
    );
    fixture.assert_rebuild_equal(5).await?;
    fixture.cleanup().await
}
