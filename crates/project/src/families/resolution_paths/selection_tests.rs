//! Row, visited-row and plan tests for the resolution path work selection
//! (`project:families.resolution_paths.prepare`).
//!
//! The statement is a function of the tables it reads and its five parameters, and it writes only
//! the work table. So a row test seeds those tables directly, runs `prepare` and compares the
//! names it selected with a literal set and with `oracle::statement`, the selection as it stood
//! before it probed indexes. Any seeded state is a valid input, including ones Interpret never
//! writes: a null or upper-case label hash, a surface first observed above the publication.
//!
//! Each test names the ordering or edge fact it holds in place. A publication is one
//! transaction: it journals the rows of its block, or of every block of its rebuild range under
//! the range's last block, writes the family tables, and only then selects. The selection reads
//! the journal at the publication's block, the family tables as just written, events up to the
//! block, and every name surface as a path member whatever its block, visibility or
//! canonicality. Only its last step keeps the readable surfaces at or below the block.
mod oracle;

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::keccak256;
use anyhow::{Context, Result, ensure};
use bigname_test_support::TestDatabase;
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, PgPool, raw_sql};

use super::prepare;
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
const WORK_TABLE: &str = "CREATE TEMP TABLE bigname_resolution_path_work (
    logical_name_id text PRIMARY KEY) ON COMMIT DROP";

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

    /// The names the statement before the probes selects for `call`.
    async fn before(&self, call: Call<'_>) -> Result<BTreeSet<String>> {
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin().await?;
        raw_sql(WORK_TABLE).execute(&mut *transaction).await?;
        sqlx::query(&oracle::statement(SUMMARY_WORK_LIST))
            .bind(CHAIN)
            .bind(call.block)
            .bind(call.time())
            .bind(call.after)
            .bind(call.root)
            .execute(&mut *transaction)
            .await
            .context("the statement before the probes")?;
        let names = work(&mut transaction).await?;
        transaction.rollback().await?;
        Ok(names)
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

    /// `prepare` and the statement before the probes both select exactly `expected` for
    /// `call`.
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
        let before = spelled(self.before(call).await?);
        ensure!(
            before == expected,
            "the statement before the probes selects {before:?}, not {expected:?}"
        );
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
// block 9, and the parent's migration, at block 3, names the registry mounted at `m.eth`.
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
        .assert_selects(
            call,
            &["a.foo.eth", "q.p.eth", "t.mig.eth", "m.eth", "k.m.eth"],
        )
        .await?;
    world.cleanup().await
}

// A replay after a reorg publishes the rows the first run published. An orphaned surface is
// still a path member and never an output. `foo.eth` is orphaned and holds the changed entry's
// label beside its canonical child. `mig.eth` is orphaned, and it alone links the touched
// `t.mig.eth` to the migration that names the registry mounted at `m.eth`.
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
        .assert_selects(call, &["sub.foo.eth", "t.mig.eth", "m.eth", "k.m.eth"])
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

// A touched name selects itself and the names below it. Its ancestors, its siblings and
// names that hold its label on another path are not selected.
#[tokio::test]
async fn a_touched_name_selects_its_descendants_only() -> Result<()> {
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
    world
        .assert_selects(call, &["a.foo.eth", "x.a.foo.eth"])
        .await?;
    world.cleanup().await
}

// A touched name reaches the migration evidence of each of its ancestors of two labels or
// more, itself included, and the ancestor is found by its whole ordered path. `a.foo.eth` is
// touched. Its own migration names the registry at `n.eth` and `foo.eth`'s the one at `m.eth`.
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
    world.touch(5, "a.foo.eth").await?;
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
// A touched name still selects only the names that end in its exact label hashes.
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
    world
        .assert_selects(touched, &["first.eth", "x.first.eth"])
        .await?;
    world.cleanup().await
}

// Entry keys compare lower-cased label hashes. Paths compare the stored text. `FOO.eth` and
// `x.FOO.eth` hold `foo`'s label hash in upper case. A change of `foo`'s entry selects both
// spellings. A touched `x.FOO.eth` selects itself and reaches the migration of `FOO.eth`, whose
// registry is mounted at `n.eth`, and not that of `foo.eth`, mounted at `m.eth`.
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
    world
        .assert_selects(touched, &["x.FOO.eth", "n.eth"])
        .await?;
    world.cleanup().await
}

// Nothing forbids a null element in `labelhashes`, and array equality treats two nulls as
// equal. `null.foo.eth` holds a null first label and `bar.null.eth` a null second label. Both
// are touched and select the names below them. `null.foo.eth` is also its own ancestor, and its
// migration names the registry mounted at `m.eth`.
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
        .assert_selects(
            call,
            &[
                "null.foo.eth",
                "x.null.foo.eth",
                "bar.null.eth",
                "y.bar.null.eth",
                "m.eth",
            ],
        )
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
// with the touched `a.foo.eth`. A surface of another namespace spells `foo.eth`'s path, and its
// migration, of the registry mounted at `z.eth`, is not reached.
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
