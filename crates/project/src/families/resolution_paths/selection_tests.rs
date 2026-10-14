//! Row, visited-row and plan tests for the resolution path work selection
//! (`project:families.resolution_paths.prepare`).
//!
//! The statement is a function of the tables it reads and its five parameters, and it writes only
//! the work table. So a row test seeds those tables directly, runs `prepare` and compares the
//! names it selected with a literal set. Any seeded state is a valid input, including ones Interpret never
//! writes: a null or upper-case label hash, a surface first observed above the publication.
//!
//! Each test names the ordering or edge fact it holds in place. A publication is one
//! transaction: it journals the rows of its block, or of every block of its rebuild range under
//! the range's last block, writes the family tables, and only then selects. The selection reads
//! the journal at the publication's block, the family tables as just written, events up to the
//! block, and every name surface as a path member whatever its block, visibility or
//! canonicality. Only its last step keeps the readable surfaces at or below the block.
//!
//! `scaling_tests.rs` holds the tests on how many surfaces a selection reads.
mod scaling_tests;

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::keccak256;
use anyhow::{Context, Result, ensure};
use bigname_test_support::TestDatabase;
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, PgPool, raw_sql};

use super::{prepare, selection};
use crate::families::{
    derived::SUMMARY_WORK_LIST,
    guard_tests::{CHAIN, database, hash},
    input::BlockHeader,
};

/// The root registry the publication's manifests admit, bound as the statement's fifth
/// parameter.
const ROOT: &str = "0x00000000000000000000000000000000000000a0";
/// The registry the root's `eth` entry points at.
const ETH: &str = "0x00000000000000000000000000000000000000a1";
const PLAN_MODES: [&str; 2] = ["force_custom_plan", "force_generic_plan"];

/// A registry other than the root's and `eth`'s.
fn registry(n: u8) -> String {
    format!("0x{:040x}", 0xb0 + u32::from(n))
}

fn labelhash(label: &str) -> String {
    format!("{:#x}", keccak256(label.as_bytes()))
}

/// The label hashes of a dotted name, in label order.
fn path(name: &str) -> Vec<Option<String>> {
    name.split('.')
        .map(|label| Some(labelhash(label)))
        .collect()
}

/// A stand-in for the namehash: unique per spelling, which is all the selection needs.
fn node(name: &str) -> String {
    format!("{:#x}", keccak256(name.as_bytes()))
}

fn id(name: &str) -> String {
    format!("ens:{}", node(name))
}

/// The ENSv2 entry a label hash belongs to: the hash with its low 32 bits cleared.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/utils/LibLabel.sol:L7-L16 @ ens_v2_sepolia_20261001@07e55a05)
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L677-L693 @ ens_v2_sepolia_20261001@07e55a05)
fn entry_key(labelhash: &str) -> String {
    format!("{}00000000", &labelhash.to_lowercase()[..58])
}

/// One call of the statement: the publication's block, the block of the family marker it
/// follows (-1 with none) and the admitted root registry.
#[derive(Clone, Copy)]
struct Call<'a> {
    block: i64,
    after: i64,
    root: Option<&'a str>,
}

impl Call<'_> {
    fn time(&self) -> i64 {
        1_800_000_000 + self.block * 12
    }
}

/// A name surface row. `name` spells its identity and, unless `labels` replaces them, its
/// label hashes.
struct Surface<'a> {
    name: &'a str,
    namespace: &'a str,
    chain: &'a str,
    labels: Option<Vec<Option<String>>>,
    block: i64,
    canonicality: &'a str,
    shadow: bool,
}

impl<'a> Surface<'a> {
    fn of(name: &'a str) -> Self {
        Self {
            name,
            namespace: "ens",
            chain: CHAIN,
            labels: None,
            block: 0,
            canonicality: "canonical",
            shadow: false,
        }
    }

    fn labels(mut self, labels: Vec<Option<String>>) -> Self {
        self.labels = Some(labels);
        self
    }

    fn block(mut self, block: i64) -> Self {
        self.block = block;
        self
    }
}

/// A database with canonical blocks 0 to 12 of `CHAIN` and the names the test gave its
/// surfaces, to report a difference by name.
struct World {
    database: TestDatabase,
    pool: PgPool,
    names: BTreeMap<String, String>,
}

impl World {
    async fn new() -> Result<Self> {
        let (database, pool) = database().await?;
        Ok(Self {
            database,
            pool,
            names: BTreeMap::new(),
        })
    }

    async fn cleanup(self) -> Result<()> {
        self.database.cleanup().await
    }

    async fn surface(&mut self, surface: Surface<'_>) -> Result<()> {
        let namehash = node(surface.name);
        let logical_name_id = format!("{}:{namehash}", surface.namespace);
        let labels = surface.labels.unwrap_or_else(|| path(surface.name));
        // A row with no label bytes must be active, so a shadow row carries bytes.
        let raw: Option<Vec<String>> = surface
            .shadow
            .then(|| (0..labels.len()).map(|n| format!("l{n}")).collect());
        sqlx::query(
            "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
                 deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ($1, $2, array_to_string($3::text[], '.'), $3,
                     CASE WHEN $3::text[] IS NOT NULL THEN '\\x00'::bytea END, $4, $5, 'fixture',
                     CASE WHEN $3::text[] IS NULL THEN 'active' ELSE 'shadow' END,
                     CASE WHEN $3::text[] IS NOT NULL THEN 'fixture' END,
                     CASE WHEN $3::text[] IS NOT NULL THEN now() END, $6, $7, $8,
                     $9::canonicality_state)",
        )
        .bind(&logical_name_id)
        .bind(surface.namespace)
        .bind(raw)
        .bind(namehash)
        .bind(labels)
        .bind(surface.chain)
        .bind(hash(surface.block))
        .bind(surface.block)
        .bind(surface.canonicality)
        .execute(&self.pool)
        .await
        .with_context(|| format!("surface {}", surface.name))?;
        self.names.insert(logical_name_id, surface.name.to_owned());
        Ok(())
    }

    /// Active canonical `ens` surfaces at block 0, one per dotted name.
    async fn surfaces(&mut self, names: &[&str]) -> Result<()> {
        for name in names {
            self.surface(Surface::of(name)).await?;
        }
        Ok(())
    }

    /// A journal row of the publication at `block`.
    async fn journal(
        &self,
        block: i64,
        family: &str,
        key: Value,
        before: Option<Value>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO project_family_undo (chain_id, block_number, block_hash, family, key,
                 before_image)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(CHAIN)
        .bind(block)
        .bind(hash(block))
        .bind(family)
        .bind(key.to_string())
        .bind(before)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The publication at `block` wrote the name state row of `name`.
    async fn touch(&self, block: i64, name: &str) -> Result<()> {
        self.journal(
            block,
            "project_name_state",
            json!([CHAIN, "ens", id(name)]),
            None,
        )
        .await
    }

    /// The publication at `block` wrote the entry of `labelhash` in `registry`.
    async fn entry_changed(&self, block: i64, registry: &str, labelhash: &str) -> Result<()> {
        self.journal(
            block,
            "project_ens_v2_entry_owner",
            json!([CHAIN, registry, entry_key(labelhash)]),
            None,
        )
        .await
    }

    /// The publication at `block` changed the registry pointer of `name` from the resolver
    /// `before` to `after`: the current pointer row and its journal row.
    async fn registry_pointer_changed(
        &self,
        block: i64,
        name: &str,
        before: &str,
        after: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO project_registry_pointer (chain_id, namespace, node, block_number,
                 event_identity, resolver_address, source_family)
             VALUES ($1, 'ens', $2, $3, 'pointer', $4, 'ens_v1_registry_l1')
             ON CONFLICT (chain_id, namespace, node)
             DO UPDATE SET resolver_address = EXCLUDED.resolver_address",
        )
        .bind(CHAIN)
        .bind(node(name))
        .bind(block)
        .bind(after)
        .execute(&self.pool)
        .await?;
        self.journal(
            block,
            "project_registry_pointer",
            json!([CHAIN, "ens", node(name)]),
            Some(json!({"resolver_address": before})),
        )
        .await
    }

    /// The publication at `block` wrote the resource pointer of the resource behind
    /// `name`: the current pointer row and its journal row. The journal key is
    /// `[chain, resource]`, the way the statement reads it, and `before` is the
    /// before-image of the row the publication replaced.
    #[allow(clippy::too_many_arguments)]
    async fn resource_pointer_changed(
        &self,
        block: i64,
        name: &str,
        resource: &str,
        resolver: &str,
        nonzero_resolver: &str,
        boundary_kind: &str,
        pointer_position: Value,
        before: Value,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO project_resource_pointer (chain_id, resource_id, block_number,
                 event_identity, resolver_address, pointer_position, namespace,
                 source_family, namehash, nonzero_resolver_address, nonzero_position,
                 boundary_kind, boundary_position)
             VALUES ($1, $2::uuid, $3, 'pointer:foo', $4, $5, 'ens',
                     'ens_v2_registry_l1', $6, $7, $5, $8, $5)
             ON CONFLICT (chain_id, resource_id)
             DO UPDATE SET resolver_address = EXCLUDED.resolver_address,
                           pointer_position = EXCLUDED.pointer_position,
                           namespace = EXCLUDED.namespace,
                           namehash = EXCLUDED.namehash,
                           nonzero_resolver_address = EXCLUDED.nonzero_resolver_address,
                           boundary_kind = EXCLUDED.boundary_kind",
        )
        .bind(CHAIN)
        .bind(resource)
        .bind(block)
        .bind(resolver)
        .bind(pointer_position)
        .bind(node(name))
        .bind(nonzero_resolver)
        .bind(boundary_kind)
        .execute(&self.pool)
        .await?;
        self.journal(
            block,
            "project_resource_pointer",
            json!([CHAIN, resource]),
            Some(before),
        )
        .await
    }

    /// The publication at `block` changed the registry node state of `name`.
    /// Each side is `(owner, owner_getter, owner_getter_reason, owner_word_unmasked)`.
    /// A missing side means no row on that side.
    #[allow(clippy::type_complexity)]
    async fn registry_node_state_changed(
        &self,
        block: i64,
        name: &str,
        before: Option<(Option<&str>, Option<&str>, Option<&str>, bool)>,
        current: Option<(Option<&str>, Option<&str>, Option<&str>, bool)>,
    ) -> Result<()> {
        if let Some((owner, getter, reason, unmasked)) = current {
            sqlx::query(
                "INSERT INTO project_registry_node_state (chain_id, namespace, node,
                     block_number, event_identity, owner, owner_getter,
                     owner_getter_reason, owner_word_unmasked, has_old_record)
                 VALUES ($1, 'ens', $2, $3, 'node:foo', $4, $5, $6, $7, false)
                 ON CONFLICT (chain_id, namespace, node)
                 DO UPDATE SET owner = EXCLUDED.owner,
                               owner_getter = EXCLUDED.owner_getter,
                               owner_getter_reason = EXCLUDED.owner_getter_reason,
                               owner_word_unmasked = EXCLUDED.owner_word_unmasked",
            )
            .bind(CHAIN)
            .bind(node(name))
            .bind(block)
            .bind(owner)
            .bind(getter)
            .bind(reason)
            .bind(unmasked)
            .execute(&self.pool)
            .await?;
        }
        let before_image = before.map(|(owner, getter, reason, unmasked)| {
            json!({
                "namespace": "ens", "node": node(name), "owner": owner,
                "owner_getter": getter, "owner_getter_reason": reason,
                "owner_word_unmasked": unmasked
            })
        });
        self.journal(
            block,
            "project_registry_node_state",
            json!([CHAIN, "ens", node(name)]),
            before_image,
        )
        .await
    }

    /// The publication at `block` changed the wrapper state of `resource`.
    /// Each side names its wrapped name, fuses, whether it has a modifier,
    /// its stored expiry text, its wrapper state text and its unwrapped flag.
    /// A missing side means no row on that side.
    #[allow(clippy::type_complexity)]
    async fn wrapper_state_changed(
        &self,
        block: i64,
        resource: &str,
        before: Option<(
            Option<&str>,
            Option<i64>,
            bool,
            Option<&str>,
            Option<&str>,
            Option<bool>,
        )>,
        current: Option<(
            Option<&str>,
            Option<i64>,
            bool,
            Option<&str>,
            Option<&str>,
            Option<bool>,
        )>,
    ) -> Result<()> {
        let position = json!({"block_number": block, "event_identity": "wrapper:foo"});
        if let Some((name, fuses, has_modifier, expiry, state, unwrapped)) = current {
            sqlx::query(
                "INSERT INTO project_wrapper_state (chain_id, resource_id, logical_name_id,
                     block_number, event_identity, wrapper_state, fuses,
                     wrapper_state_position, expiry_seconds, expiry_position,
                     lifecycle_unwrapped)
                 VALUES ($1, $2::uuid, $3, $4, 'wrapper:foo', $5, $6, $7, $8::numeric, $9, $10)
                 ON CONFLICT (chain_id, resource_id)
                 DO UPDATE SET logical_name_id = EXCLUDED.logical_name_id,
                               wrapper_state = EXCLUDED.wrapper_state,
                               fuses = EXCLUDED.fuses,
                               wrapper_state_position = EXCLUDED.wrapper_state_position,
                               expiry_seconds = EXCLUDED.expiry_seconds,
                               expiry_position = EXCLUDED.expiry_position,
                               lifecycle_unwrapped = EXCLUDED.lifecycle_unwrapped",
            )
            .bind(CHAIN)
            .bind(resource)
            .bind(name.map(id))
            .bind(block)
            .bind(state)
            .bind(fuses)
            .bind(has_modifier.then(|| position.clone()))
            .bind(expiry)
            .bind(Some(position.clone()))
            .bind(unwrapped)
            .execute(&self.pool)
            .await?;
        }
        let before_image = before.map(|(name, fuses, has_modifier, expiry, state, unwrapped)| {
            json!({
                "logical_name_id": name.map(id), "fuses": fuses,
                "wrapper_state_position": has_modifier.then(|| position.clone()),
                "expiry_seconds": expiry.map(|text| {
                    serde_json::from_str::<Value>(text).unwrap_or(Value::String(text.to_owned()))
                }),
                "expiry_position": position.clone(),
                "wrapper_state": state, "lifecycle_unwrapped": unwrapped
            })
        });
        self.journal(
            block,
            "project_wrapper_state",
            json!([CHAIN, resource]),
            before_image,
        )
        .await
    }

    /// An activated canonical event at `block`, with no transaction position.
    #[allow(clippy::too_many_arguments)]
    async fn event(
        &self,
        identity: &str,
        kind: &str,
        family: &str,
        block: i64,
        name: Option<&str>,
        resource: Option<&str>,
        after: Value,
        emitter: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO normalized_events (event_identity, namespace, logical_name_id,
                 resource_id, event_kind, source_family, manifest_version, chain_id,
                 block_number, block_hash, derivation_kind, canonicality_state, after_state,
                 raw_fact_ref)
             VALUES ($1, 'ens', $2, $3::uuid, $4, $5, 1, $6, $7, $8,
                     'ens_v2_registry_resource_surface', 'canonical', $9,
                     jsonb_build_object('emitting_address', $10::text))",
        )
        .bind(identity)
        .bind(name)
        .bind(resource)
        .bind(kind)
        .bind(family)
        .bind(CHAIN)
        .bind(block)
        .bind(hash(block))
        .bind(after)
        .bind(emitter)
        .execute(&self.pool)
        .await
        .with_context(|| format!("event {identity}"))?;
        Ok(())
    }

    /// `registry`'s entry for `labelhash`, registered and pointing at `subregistry` since
    /// block 1: the entry row, its resource, and the `SubregistryChanged` event the walk reads
    /// the pointer from.
    async fn mount(&self, registry: &str, labelhash: &str, subregistry: &str) -> Result<()> {
        let resource: String = sqlx::query_scalar(
            "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES (md5($1 || $2)::uuid, $3, $4, 0, 'canonical')
             RETURNING resource_id::text",
        )
        .bind(registry)
        .bind(labelhash)
        .bind(CHAIN)
        .bind(hash(0))
        .fetch_one(&self.pool)
        .await?;
        let identity = format!("mount:{registry}:{labelhash}");
        sqlx::query(
            "INSERT INTO project_ens_v2_entry_owner (chain_id, registry, entry_key, token_id,
                 resource_id, status, block_number, event_identity)
             VALUES ($1, $2, $3, $4, $5::uuid, 'registered', 1, $6)",
        )
        .bind(CHAIN)
        .bind(registry)
        .bind(entry_key(labelhash))
        .bind(labelhash.to_lowercase())
        .bind(&resource)
        .bind(&identity)
        .execute(&self.pool)
        .await?;
        self.event(
            &identity,
            "SubregistryChanged",
            "ens_v2_registry_l1",
            1,
            None,
            Some(&resource),
            json!({"subregistry": subregistry, "token_id": labelhash.to_lowercase()}),
            registry,
        )
        .await
    }

    /// The root's `eth` entry, pointing at `ETH`.
    async fn eth(&self) -> Result<()> {
        self.mount(ROOT, &labelhash("eth"), ETH).await
    }

    /// An ENSv1 to ENSv2 migration of `name`, applied at `block`, whose evidence is the
    /// creation of `registry` at block 1.
    async fn migrated(&self, name: &str, registry: &str, block: i64) -> Result<()> {
        let created = format!("created:{registry}:{name}");
        self.event(
            &created,
            "RegistryCreated",
            "ens_v2_registry_l1",
            1,
            None,
            None,
            json!({"registry": registry}),
            ETH,
        )
        .await?;
        let logical_name_id = self
            .names
            .iter()
            .find(|(_, spelled)| spelled.as_str() == name)
            .map(|(id, _)| id.clone())
            .with_context(|| format!("{name} has no surface"))?;
        self.event(
            &format!("migration:{name}"),
            "MigrationApplied",
            "ens_v2_migration_l1",
            block,
            Some(&logical_name_id),
            None,
            json!({"evidence": [{"event_identity": created, "event_kind": "RegistryCreated"}]}),
            ETH,
        )
        .await
    }

    /// The names `prepare` selects for `call` under plan cache mode `mode`.
    async fn selected(&self, call: Call<'_>, mode: &str) -> Result<BTreeSet<String>> {
        let mut connection = self.pool.acquire().await?;
        raw_sql(&format!("SET plan_cache_mode = {mode}"))
            .execute(&mut *connection)
            .await?;
        let mut transaction = connection.begin().await?;
        let block = BlockHeader {
            number: call.block,
            hash: hash(call.block),
            predecessor_hash: None,
            timestamp_seconds: call.time(),
            timestamp: Value::Null,
        };
        prepare(&mut transaction, CHAIN, &block, call.after, call.root)
            .await
            .with_context(|| format!("prepare under {mode}"))?;
        let names = work(&mut transaction).await?;
        transaction.rollback().await?;
        raw_sql("SET plan_cache_mode = auto")
            .execute(&mut *connection)
            .await?;
        Ok(names)
    }

    /// `prepare` selects exactly `expected` for `call`.
    async fn assert_selects(&self, call: Call<'_>, expected: &[&str]) -> Result<()> {
        let expected: BTreeSet<String> = expected.iter().map(|name| (*name).to_owned()).collect();
        ensure!(
            expected.iter().all(|name| self
                .names
                .values()
                .filter(|spelled| *spelled == name)
                .count()
                == 1),
            "an expected name has no surface or two: {expected:?}"
        );
        let spelled = |names: BTreeSet<String>| -> BTreeSet<String> {
            names
                .into_iter()
                .map(|id| self.names.get(&id).cloned().unwrap_or(id))
                .collect()
        };
        for mode in PLAN_MODES {
            let selected = spelled(self.selected(call, mode).await?);
            ensure!(
                selected == expected,
                "{mode}: prepare selects {selected:?}, not {expected:?}"
            );
        }
        Ok(())
    }
}

async fn work(connection: &mut PgConnection) -> Result<BTreeSet<String>> {
    Ok(
        sqlx::query_scalar("SELECT logical_name_id FROM pg_temp.bigname_resolution_path_work")
            .fetch_all(connection)
            .await?
            .into_iter()
            .collect(),
    )
}

/// The statement text `prepare` sends.
fn current() -> String {
    selection::statement(SUMMARY_WORK_LIST)
}

// A rebuild range journals every block under its last block, before the selection runs. The
// entry of `foo` changed at block 10 of a range that publishes at block 12, and no pointer event
// of the range names it, so only the journal carries the change to the selection.
#[tokio::test]
async fn a_range_selects_an_entry_changed_in_its_first_block() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "sub.foo.eth", "bar.eth"])
        .await?;
    world.eth().await?;
    world
        .event(
            "transfer:foo",
            "TokenControlTransferred",
            "ens_v2_registry_l1",
            10,
            None,
            None,
            json!({"token_id": labelhash("foo")}),
            ETH,
        )
        .await?;
    world.entry_changed(12, ETH, &labelhash("foo")).await?;
    let range = Call {
        block: 12,
        after: 9,
        root: Some(ROOT),
    };
    world
        .assert_selects(range, &["foo.eth", "sub.foo.eth"])
        .await?;
    world.cleanup().await
}

// Entries and pointers written by earlier publications are current state, not work: the
// journal only seeds the walk. Root, `eth` and `foo` were mounted below the marker. The
// publication changes the entry of `a` in the registry `foo.eth` points at.
#[tokio::test]
async fn a_mount_from_an_earlier_block_carries_a_later_entry_change_to_its_descendants()
-> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "x.a.foo.eth", "b.foo.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world
        .entry_changed(5, &registry(1), &labelhash("a"))
        .await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(call, &["a.foo.eth", "x.a.foo.eth"])
        .await?;
    world.cleanup().await
}

// In a redo every surface of the chain already exists, including those first observed above
// the publication. They are path members, and only the last step of the statement drops them, so
// a redo writes what the live run wrote. The publication at block 5 touches three names.
// `a.foo.eth` has a descendant first observed at block 9. `p.eth`'s own surface is first
// observed at block 9, and its child's at block 0. `t.mig.eth`'s parent is first observed at
// block 9, and the parent's ENSv1→ENSv2 migration, at block 3, names the registry mounted at
// `m.eth`.
#[tokio::test]
async fn a_redo_reads_later_surfaces_as_path_members_and_never_selects_them() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "a.foo.eth",
            "q.p.eth",
            "t.mig.eth",
            "m.eth",
            "k.m.eth",
        ])
        .await?;
    for later in ["y.a.foo.eth", "p.eth", "mig.eth"] {
        world.surface(Surface::of(later).block(9)).await?;
    }
    world.eth().await?;
    world.mount(ETH, &labelhash("m"), &registry(1)).await?;
    world.migrated("mig.eth", &registry(1), 3).await?;
    for name in ["a.foo.eth", "p.eth", "t.mig.eth"] {
        world.touch(5, name).await?;
    }
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(call, &["a.foo.eth", "t.mig.eth"])
        .await?;
    world.cleanup().await
}

// A replay after a reorg publishes the rows the first run published. An orphaned surface is
// still a path member and never an output. `foo.eth` is orphaned and holds the changed entry's
// label beside its canonical child. `mig.eth` is orphaned, and it alone links the touched
// `t.mig.eth` to the ENSv1→ENSv2 migration that names the registry mounted at `m.eth`.
#[tokio::test]
async fn an_orphaned_surface_is_a_path_member_and_never_an_output() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "sub.foo.eth", "t.mig.eth", "m.eth", "k.m.eth"])
        .await?;
    for orphaned in ["foo.eth", "mig.eth"] {
        let mut surface = Surface::of(orphaned);
        surface.canonicality = "orphaned";
        world.surface(surface).await?;
    }
    world.eth().await?;
    world.mount(ETH, &labelhash("m"), &registry(1)).await?;
    world.migrated("mig.eth", &registry(1), 3).await?;
    world.entry_changed(5, ETH, &labelhash("foo")).await?;
    world.touch(5, "t.mig.eth").await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(call, &["sub.foo.eth", "t.mig.eth"])
        .await?;
    world.cleanup().await
}

// The first publication after a reset follows no marker, so every event and surface up to
// its block is in its window and every journalled before-image is null. It selects with no root
// admitted and with one, and from a database with no surface at all.
#[tokio::test]
async fn the_first_publication_selects_from_empty_family_state() -> Result<()> {
    let mut world = World::new().await?;
    world.surfaces(&["eth", "foo.eth", "a.foo.eth"]).await?;
    world.surface(Surface::of("later.eth").block(2)).await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world.entry_changed(1, ROOT, &labelhash("eth")).await?;
    world.entry_changed(1, ETH, &labelhash("foo")).await?;
    world.touch(1, "foo.eth").await?;
    for root in [None, Some(ROOT)] {
        let first = Call {
            block: 1,
            after: -1,
            root,
        };
        world
            .assert_selects(first, &["eth", "foo.eth", "a.foo.eth"])
            .await?;
    }
    sqlx::query("DELETE FROM name_surfaces")
        .execute(&world.pool)
        .await?;
    world.names.clear();
    for root in [None, Some(ROOT)] {
        let first = Call {
            block: 1,
            after: -1,
            root,
        };
        world.assert_selects(first, &[]).await?;
    }
    world.cleanup().await
}

// A name touched only in its own state selects itself alone. Its descendants, ancestors,
// siblings and names that hold its label on another path are not selected.
#[tokio::test]
async fn a_touched_name_selects_itself_only() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "a.foo.eth",
            "x.a.foo.eth",
            "b.foo.eth",
            "bar.eth",
            "a.bar.eth",
            "foo.a.eth",
        ])
        .await?;
    world.touch(5, "a.foo.eth").await?;
    let call = Call {
        block: 5,
        after: 4,
        root: None,
    };
    world.assert_selects(call, &["a.foo.eth"]).await?;
    world.cleanup().await
}

// An owner-only change of a parent entry recomposes the parent and no descendant: the
// entry's owner is not one of the fields a descendant reads through its ancestors. A
// resolver-pointer change of the same parent still recomposes its whole subtree.
#[tokio::test]
async fn an_owner_only_parent_change_selects_the_parent_only() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "x.a.foo.eth"])
        .await?;
    world.eth().await?;
    let token = labelhash("foo");
    sqlx::query(
        "INSERT INTO project_ens_v2_entry_owner (chain_id, registry, entry_key, token_id,
             status, block_number, event_identity)
         VALUES ($1, $2, $3, $4, 'registered', 1, 'entry:foo')",
    )
    .bind(CHAIN)
    .bind(ETH)
    .bind(entry_key(&token))
    .bind(&token)
    .execute(&world.pool)
    .await?;
    world
        .journal(
            5,
            "project_ens_v2_entry_owner",
            json!([CHAIN, ETH, entry_key(&token)]),
            Some(json!({
                "status": "registered", "token_id": token, "resource_id": null,
                "expiry": null, "registry_contract_instance_id": null,
                "owner": "0x00000000000000000000000000000000000000aa"
            })),
        )
        .await?;
    world.touch(5, "foo.eth").await?;
    let owner_only = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world.assert_selects(owner_only, &["foo.eth"]).await?;

    world
        .registry_pointer_changed(
            7,
            "foo.eth",
            "0x00000000000000000000000000000000000000c0",
            "0x00000000000000000000000000000000000000c1",
        )
        .await?;
    let pointer = Call {
        block: 7,
        after: 6,
        root: Some(ROOT),
    };
    world
        .assert_selects(pointer, &["foo.eth", "a.foo.eth", "x.a.foo.eth"])
        .await?;
    world.cleanup().await
}

// A resource pointer whose resolver changed recomposes the parent and its whole subtree:
// a descendant reads its resolver through its ancestors. The change here is from a cleared
// resolver, so the before-image's `resolver_address` is null and only a distinct-from
// comparison sees it. A later pointer row that only moves `pointer_position`, with
// `resolver_address`, `nonzero_resolver_address` and `boundary_kind` unchanged, recomposes
// the parent alone.
#[tokio::test]
async fn a_resource_pointer_resolver_change_selects_the_subtree_but_a_position_only_change_selects_the_parent_only()
-> Result<()> {
    const RESOURCE: &str = "00000000-0000-0000-0000-000000000002";
    const RESOLVER: &str = "0x00000000000000000000000000000000000000c1";
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "x.a.foo.eth"])
        .await?;
    let at_five = json!({"block_number": 5, "event_identity": "pointer:foo"});
    world
        .resource_pointer_changed(
            5,
            "foo.eth",
            RESOURCE,
            RESOLVER,
            RESOLVER,
            "ResolverChanged",
            at_five.clone(),
            json!({
                "namespace": "ens", "namehash": node("foo.eth"),
                "resolver_address": null,
                "nonzero_resolver_address": RESOLVER,
                "boundary_kind": "ResolverChanged",
                "pointer_position": {"block_number": 3, "event_identity": "pointer:foo"}
            }),
        )
        .await?;
    let resolver_changed = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(resolver_changed, &["foo.eth", "a.foo.eth", "x.a.foo.eth"])
        .await?;

    world
        .resource_pointer_changed(
            7,
            "foo.eth",
            RESOURCE,
            RESOLVER,
            RESOLVER,
            "ResolverChanged",
            json!({"block_number": 7, "event_identity": "pointer:foo"}),
            json!({
                "namespace": "ens", "namehash": node("foo.eth"),
                "resolver_address": RESOLVER,
                "nonzero_resolver_address": RESOLVER,
                "boundary_kind": "ResolverChanged",
                "pointer_position": at_five
            }),
        )
        .await?;
    let position_only = Call {
        block: 7,
        after: 6,
        root: Some(ROOT),
    };
    world.assert_selects(position_only, &["foo.eth"]).await?;
    world.cleanup().await
}

// A lifecycle event retained for an ancestor recomposes the ancestor and no descendant:
// no lifecycle kind writes a row naming another name (see the module docs), so the event
// is not path-bearing.
#[tokio::test]
async fn an_ancestor_lifecycle_insert_selects_the_ancestor_only() -> Result<()> {
    let mut world = World::new().await?;
    world.surfaces(&["eth", "foo.eth", "a.foo.eth"]).await?;
    let resource = "00000000-0000-0000-0000-000000000001";
    sqlx::query(
        "INSERT INTO project_lifecycle_event (chain_id, state_kind, state_key, block_number,
             event_identity, event_kind, original_logical_name_id, decoded_logical_name_id,
             resource_id, source_family)
         VALUES ($1, 'resource', $2, 5, 'lifecycle:foo', 'RegistrationGranted', $3, $3,
                 $2::uuid, 'ens_v1_registrar_l1')",
    )
    .bind(CHAIN)
    .bind(resource)
    .bind(id("foo.eth"))
    .execute(&world.pool)
    .await?;
    world
        .journal(
            5,
            "project_lifecycle_event",
            json!([CHAIN, "resource", resource, "lifecycle:foo"]),
            None,
        )
        .await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world.assert_selects(call, &["foo.eth"]).await?;
    world.cleanup().await
}

// A name with a path-bearing change reaches the ENSv1→ENSv2 migration evidence of each of
// its ancestors of two labels or more, itself included, and the ancestor is found by its
// whole ordered path. `a.foo.eth`'s registry pointer changed. Its own migration names the
// registry at `n.eth` and `foo.eth`'s the one at `m.eth`.
// `eth` has one label, and `eth.foo` holds `foo.eth`'s labels in another order: their
// migrations, of the registries at `o.eth` and `z.eth`, are not reached.
#[tokio::test]
async fn a_touched_subname_expands_its_untouched_ancestors_migration_registry() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "a.foo.eth",
            "eth.foo",
            "m.eth",
            "k.m.eth",
            "n.eth",
            "o.eth",
            "z.eth",
        ])
        .await?;
    world.eth().await?;
    for (n, (mounted, migrated)) in [
        ("m", "foo.eth"),
        ("n", "a.foo.eth"),
        ("o", "eth"),
        ("z", "eth.foo"),
    ]
    .into_iter()
    .enumerate()
    {
        let registry = registry(u8::try_from(n)?);
        world.mount(ETH, &labelhash(mounted), &registry).await?;
        world.migrated(migrated, &registry, 3).await?;
    }
    world
        .registry_pointer_changed(
            5,
            "a.foo.eth",
            "0x00000000000000000000000000000000000000c0",
            "0x00000000000000000000000000000000000000c1",
        )
        .await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(call, &["a.foo.eth", "m.eth", "k.m.eth", "n.eth"])
        .await?;
    world.cleanup().await
}

// An entry is keyed by the label hash with its low 32 bits cleared, so two label hashes that
// share their first 28 bytes share an entry.
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/utils/LibLabel.sol:L7-L16 @ ens_v2_sepolia_20261001@07e55a05)
// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L677-L693 @ ens_v2_sepolia_20261001@07e55a05)
// A change of that entry selects the names under both.
// A touched name selects only itself.
#[tokio::test]
async fn labels_sharing_an_entry_key_are_candidates_and_only_exact_suffixes_are_selected()
-> Result<()> {
    let mut world = World::new().await?;
    let shared = &labelhash("shared")[..58];
    let (first, second) = (format!("{shared}aaaaaaaa"), format!("{shared}bbbbbbbb"));
    let (x, eth) = (labelhash("x"), labelhash("eth"));
    world.surfaces(&["eth"]).await?;
    for (name, labels) in [
        ("first.eth", vec![&first, &eth]),
        ("second.eth", vec![&second, &eth]),
        ("x.first.eth", vec![&x, &first, &eth]),
        ("x.second.eth", vec![&x, &second, &eth]),
    ] {
        let labels = labels.into_iter().cloned().map(Some).collect();
        world.surface(Surface::of(name).labels(labels)).await?;
    }
    world.eth().await?;
    world.entry_changed(5, ETH, &first).await?;
    let entry = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(
            entry,
            &["first.eth", "second.eth", "x.first.eth", "x.second.eth"],
        )
        .await?;
    world.touch(7, "first.eth").await?;
    let touched = Call {
        block: 7,
        after: 6,
        root: Some(ROOT),
    };
    world.assert_selects(touched, &["first.eth"]).await?;
    world.cleanup().await
}

// Entry keys compare lower-cased label hashes. Paths compare the stored text. `FOO.eth` and
// `x.FOO.eth` hold `foo`'s label hash in upper case. A change of `foo`'s entry selects both
// spellings. A touched `x.FOO.eth` selects only itself.
#[tokio::test]
async fn a_mixed_case_label_hash_matches_by_key_and_not_by_suffix() -> Result<()> {
    let mut world = World::new().await?;
    let upper = format!("0x{}", labelhash("foo")[2..].to_uppercase());
    let eth = labelhash("eth");
    world
        .surfaces(&["eth", "foo.eth", "x.foo.eth", "m.eth", "n.eth"])
        .await?;
    world
        .surface(Surface::of("FOO.eth").labels(vec![Some(upper.clone()), Some(eth.clone())]))
        .await?;
    world
        .surface(Surface::of("x.FOO.eth").labels(vec![
            Some(labelhash("x")),
            Some(upper.clone()),
            Some(eth),
        ]))
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("m"), &registry(1)).await?;
    world.mount(ETH, &labelhash("n"), &registry(2)).await?;
    world.migrated("foo.eth", &registry(1), 3).await?;
    world.migrated("FOO.eth", &registry(2), 3).await?;
    world.entry_changed(5, ETH, &labelhash("foo")).await?;
    let entry = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(entry, &["foo.eth", "x.foo.eth", "FOO.eth", "x.FOO.eth"])
        .await?;
    world.touch(7, "x.FOO.eth").await?;
    let touched = Call {
        block: 7,
        after: 6,
        root: Some(ROOT),
    };
    world.assert_selects(touched, &["x.FOO.eth"]).await?;
    world.cleanup().await
}

// Nothing forbids a null element in `labelhashes`, and array equality treats two nulls as
// equal. `null.foo.eth` holds a null first label and `bar.null.eth` a null second label. Both
// are touched and select only themselves; the names below them are not selected.
#[tokio::test]
async fn a_null_label_hash_element_keeps_the_scan_result() -> Result<()> {
    let mut world = World::new().await?;
    let label = |text: &str| Some(labelhash(text));
    world.surfaces(&["eth", "foo.eth", "m.eth"]).await?;
    for (name, labels) in [
        ("null.foo.eth", vec![None, label("foo"), label("eth")]),
        (
            "x.null.foo.eth",
            vec![label("x"), None, label("foo"), label("eth")],
        ),
        ("bar.null.eth", vec![label("bar"), None, label("eth")]),
        (
            "y.bar.null.eth",
            vec![label("y"), label("bar"), None, label("eth")],
        ),
    ] {
        world.surface(Surface::of(name).labels(labels)).await?;
    }
    world.eth().await?;
    world.mount(ETH, &labelhash("m"), &registry(1)).await?;
    world.migrated("null.foo.eth", &registry(1), 3).await?;
    world.touch(5, "null.foo.eth").await?;
    world.touch(5, "bar.null.eth").await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(call, &["null.foo.eth", "bar.null.eth"])
        .await?;
    world.cleanup().await
}

// A registry created, upgraded or announced in the publication still expands through every
// observed name. The registry mounted at `foo.eth` is created at block 5. Its mount is an older
// entry that no changed entry names, so only the whole walk finds it. `bar`'s entry changes too.
#[tokio::test]
async fn a_changed_registry_keeps_the_whole_walk() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "a.foo.eth",
            "x.a.foo.eth",
            "bar.eth",
            "baz.eth",
        ])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world
        .event(
            "created",
            "RegistryCreated",
            "ens_v2_registry_l1",
            5,
            None,
            None,
            json!({"registry": registry(1)}),
            ETH,
        )
        .await?;
    world.entry_changed(5, ETH, &labelhash("bar")).await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(call, &["foo.eth", "a.foo.eth", "x.a.foo.eth", "bar.eth"])
        .await?;
    world.cleanup().await
}

// A change of the fallback resolver's classification, or of the root registry itself, selects
// every readable `.eth` name: not a name of another top-level label, a shadow surface, or a
// surface first observed above the publication.
#[tokio::test]
async fn a_global_change_selects_every_eth_name() -> Result<()> {
    const FALLBACK: &str = "0x322b7581ca210a69c6d0e0d7c88a7688d2789cb0";
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth", "foo.xyz", "xyz"])
        .await?;
    world.surface(Surface::of("later.eth").block(9)).await?;
    let mut shadow = Surface::of("shadow.eth");
    shadow.shadow = true;
    world.surface(shadow).await?;
    world.eth().await?;
    let every = ["eth", "foo.eth", "a.foo.eth", "bar.eth"];

    sqlx::query(
        "INSERT INTO project_resolver_classification (chain_id, resolver_address, block_number,
             event_identity, support_status)
         VALUES ($1, $2, 5, 'classified', 'supported')",
    )
    .bind(CHAIN)
    .bind(FALLBACK)
    .execute(&world.pool)
    .await?;
    world
        .journal(
            5,
            "project_resolver_classification",
            json!([CHAIN, FALLBACK]),
            Some(json!({"support_status": "unsupported"})),
        )
        .await?;
    let classified = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world.assert_selects(classified, &every).await?;

    world
        .event(
            "upgraded",
            "Upgraded",
            "ens_v2_registry_l1",
            7,
            None,
            None,
            json!({"proxy_address": ROOT}),
            ROOT,
        )
        .await?;
    let upgraded = Call {
        block: 7,
        after: 6,
        root: Some(ROOT),
    };
    world.assert_selects(upgraded, &every).await?;
    world.cleanup().await
}

// The indexes carry no chain, and the probes keep the chain and namespace tests. A surface
// of another namespace and one of another chain spell `x.a.foo.eth`'s path. Neither is selected
// with the touched `a.foo.eth`, which selects only itself. A surface of another namespace
// spells `foo.eth`'s path, and its ENSv1→ENSv2 migration, of the registry mounted at `z.eth`,
// is not reached.
#[tokio::test]
async fn a_surface_of_another_namespace_with_the_same_path_is_never_selected() -> Result<()> {
    const OTHER_CHAIN: &str = "base-sepolia";
    let mut world = World::new().await?;
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
             canonicality_state)
         VALUES ($1, $2, 0, to_timestamp(1800000000), 'canonical')",
    )
    .bind(OTHER_CHAIN)
    .bind(hash(0))
    .execute(&world.pool)
    .await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "x.a.foo.eth", "z.eth"])
        .await?;
    let mut other_namespace =
        Surface::of("other namespace x.a.foo.eth").labels(path("x.a.foo.eth"));
    other_namespace.namespace = "basenames";
    world.surface(other_namespace).await?;
    let mut other_chain = Surface::of("other chain x.a.foo.eth").labels(path("x.a.foo.eth"));
    other_chain.chain = OTHER_CHAIN;
    world.surface(other_chain).await?;
    let mut ancestor = Surface::of("other namespace foo.eth").labels(path("foo.eth"));
    ancestor.namespace = "basenames";
    world.surface(ancestor).await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("z"), &registry(1)).await?;
    world
        .migrated("other namespace foo.eth", &registry(1), 3)
        .await?;
    world.touch(5, "a.foo.eth").await?;
    // Only `a.foo.eth` itself is selected; the surfaces above spell its descendant's path
    // in another namespace and on another chain.
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world.assert_selects(call, &["a.foo.eth"]).await?;
    world.cleanup().await
}

const HOLDER_ADDR: &str = "0x00000000000000000000000000000000000000aa";
const OTHER_ADDR: &str = "0x00000000000000000000000000000000000000ab";
const ZERO_ADDR: &str = "0x0000000000000000000000000000000000000000";
const REGISTRY_ADDR: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";

/// A world where `child.foo.eth` is the original child, `foo.eth` holds its
/// migration to the registry mounted at `rebound.eth`, and
/// `child.rebound.eth` sits under that mount.
async fn rebound_world() -> Result<World> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "child.foo.eth",
            "rebound.eth",
            "child.rebound.eth",
        ])
        .await?;
    world.eth().await?;
    world
        .mount(ETH, &labelhash("rebound"), &registry(1))
        .await?;
    world.migrated("foo.eth", &registry(1), 3).await?;
    Ok(world)
}

// The original child's owner getter is withdrawn by setting its owner to the
// registry itself. The getter reads as zero. The rebound name reads the
// original node through wrapper eligibility, so it is recomposed with the
// names under its migration registry mount.
#[tokio::test]
async fn an_original_child_owner_getter_withdrawal_selects_the_rebound_name_in_its_migration_registry()
-> Result<()> {
    let world = rebound_world().await?;
    world
        .registry_node_state_changed(
            5,
            "child.foo.eth",
            Some((Some(HOLDER_ADDR), Some(HOLDER_ADDR), None, false)),
            Some((
                Some(REGISTRY_ADDR),
                Some(ZERO_ADDR),
                Some("registry_self"),
                false,
            )),
        )
        .await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(call, &["child.foo.eth", "rebound.eth", "child.rebound.eth"])
        .await?;
    world.cleanup().await
}

// The same withdrawal with a literal zero owner. The stored reason differs.
// The getter class is zero in both cases, so the rebound name is selected.
#[tokio::test]
async fn an_original_child_owner_getter_withdrawal_to_zero_selects_the_rebound_name_in_its_migration_registry()
-> Result<()> {
    let world = rebound_world().await?;
    world
        .registry_node_state_changed(
            5,
            "child.foo.eth",
            Some((Some(HOLDER_ADDR), Some(HOLDER_ADDR), None, false)),
            Some((Some(ZERO_ADDR), Some(ZERO_ADDR), Some("zero"), false)),
        )
        .await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(call, &["child.foo.eth", "rebound.eth", "child.rebound.eth"])
        .await?;
    world.cleanup().await
}

// A nonzero owner changes to another nonzero owner on a parent. The getter
// class does not change. The parent recomposes alone.
#[tokio::test]
async fn a_nonzero_to_nonzero_getter_change_on_a_parent_selects_that_parent_alone() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "x.a.foo.eth"])
        .await?;
    world.eth().await?;
    world
        .registry_node_state_changed(
            5,
            "foo.eth",
            Some((Some(HOLDER_ADDR), Some(HOLDER_ADDR), None, false)),
            Some((Some(OTHER_ADDR), Some(OTHER_ADDR), None, false)),
        )
        .await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world.assert_selects(call, &["foo.eth"]).await?;
    world.cleanup().await
}

// The parent-can-create fuse bit flips on the original child's wrapper row.
// Wrapper eligibility can move, so the rebound name is recomposed.
#[tokio::test]
async fn a_parent_can_create_fuse_flip_selects_the_rebound_name() -> Result<()> {
    const RESOURCE: &str = "00000000-0000-0000-0000-000000000011";
    let world = rebound_world().await?;
    world
        .wrapper_state_changed(
            5,
            RESOURCE,
            Some((
                Some("child.foo.eth"),
                Some(0),
                true,
                Some("1907776000"),
                Some("wrapped"),
                Some(false),
            )),
            Some((
                Some("child.foo.eth"),
                Some(65536),
                true,
                Some("1907776000"),
                Some("wrapped"),
                Some(false),
            )),
        )
        .await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(call, &["child.foo.eth", "rebound.eth", "child.rebound.eth"])
        .await?;
    world.cleanup().await
}

// The stored expiry changes on the original child's wrapper row. Wrapper
// eligibility can move, so the rebound name is recomposed.
#[tokio::test]
async fn a_stored_expiry_change_selects_the_rebound_name() -> Result<()> {
    const RESOURCE: &str = "00000000-0000-0000-0000-000000000012";
    let world = rebound_world().await?;
    world
        .wrapper_state_changed(
            5,
            RESOURCE,
            Some((
                Some("child.foo.eth"),
                Some(65536),
                true,
                Some("1907776000"),
                Some("wrapped"),
                Some(false),
            )),
            Some((
                Some("child.foo.eth"),
                Some(65536),
                true,
                Some("1907777000"),
                Some("wrapped"),
                Some(false),
            )),
        )
        .await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(call, &["child.foo.eth", "rebound.eth", "child.rebound.eth"])
        .await?;
    world.cleanup().await
}

// Another fuse bit changes alone. The parent-can-create bit, the stored
// expiry, the name and the modifier stay the same. The rebound name is not
// selected.
#[tokio::test]
async fn a_change_to_another_fuse_bit_alone_does_not_expand() -> Result<()> {
    const RESOURCE: &str = "00000000-0000-0000-0000-000000000013";
    let world = rebound_world().await?;
    world
        .wrapper_state_changed(
            5,
            RESOURCE,
            Some((
                Some("child.foo.eth"),
                Some(65536),
                true,
                Some("1907776000"),
                Some("wrapped"),
                Some(false),
            )),
            Some((
                Some("child.foo.eth"),
                Some(65538),
                true,
                Some("1907776000"),
                Some("wrapped"),
                Some(false),
            )),
        )
        .await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world.assert_selects(call, &["child.foo.eth"]).await?;
    world.cleanup().await
}

// A resource pointer moves from one name to another. The move is
// path-bearing for both names. Each name and the names under it are
// recomposed.
#[tokio::test]
async fn a_resource_pointer_move_selects_the_old_and_new_names_and_the_names_under_both()
-> Result<()> {
    const RESOURCE: &str = "00000000-0000-0000-0000-000000000014";
    const RESOLVER: &str = "0x00000000000000000000000000000000000000c1";
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth", "a.bar.eth"])
        .await?;
    world
        .resource_pointer_changed(
            5,
            "bar.eth",
            RESOURCE,
            RESOLVER,
            RESOLVER,
            "ResolverChanged",
            json!({"block_number": 5, "event_identity": "pointer:foo"}),
            json!({
                "namespace": "ens", "namehash": node("foo.eth"),
                "resolver_address": RESOLVER,
                "nonzero_resolver_address": RESOLVER,
                "boundary_kind": "ResolverChanged",
                "pointer_position": {"block_number": 3, "event_identity": "pointer:foo"}
            }),
        )
        .await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world
        .assert_selects(call, &["foo.eth", "a.foo.eth", "bar.eth", "a.bar.eth"])
        .await?;
    world.cleanup().await
}

// Only the boundary kind changes on a resource pointer. No resolver, no
// nonzero resolver and no name change. The name recomposes alone.
#[tokio::test]
async fn a_boundary_kind_only_change_selects_the_name_itself_alone() -> Result<()> {
    const RESOURCE: &str = "00000000-0000-0000-0000-000000000015";
    const RESOLVER: &str = "0x00000000000000000000000000000000000000c1";
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "x.a.foo.eth"])
        .await?;
    world
        .resource_pointer_changed(
            5,
            "foo.eth",
            RESOURCE,
            RESOLVER,
            RESOLVER,
            "SubregistryChanged",
            json!({"block_number": 5, "event_identity": "pointer:foo"}),
            json!({
                "namespace": "ens", "namehash": node("foo.eth"),
                "resolver_address": RESOLVER,
                "nonzero_resolver_address": RESOLVER,
                "boundary_kind": "ResolverChanged",
                "pointer_position": {"block_number": 5, "event_identity": "pointer:foo"}
            }),
        )
        .await?;
    let call = Call {
        block: 5,
        after: 4,
        root: Some(ROOT),
    };
    world.assert_selects(call, &["foo.eth"]).await?;
    world.cleanup().await
}
