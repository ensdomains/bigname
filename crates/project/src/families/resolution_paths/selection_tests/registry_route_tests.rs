//! Row tests for the selections that start from a registry rather than from a name or an entry.
//!
//! A registry is changed in a publication when an event of the publication's blocks creates,
//! upgrades or announces it, or when the retained evidence of an ENSv1 to ENSv2 migration of a
//! touched name, or of an ancestor of it, holds the registry's creation. The selection then takes
//! the names at and under every path at which the walk from the root registry reaches the changed
//! registry. The walk reaches a registry through an entry whose latest `SubregistryChanged` event
//! at or below the publication names it. These tests call such an entry a mount of the registry.
//!
//! Each test states the order of the mount, the change and the publication it holds in place.
use anyhow::Result;
use serde_json::json;

use super::{CHAIN, Call, ETH, ROOT, Surface, World, entry_key, hash, id, labelhash, registry};

/// A resolver a registry pointer of the mixed publication names.
const RESOLVER: &str = "0x00000000000000000000000000000000000000c1";

const REGISTRY_FAMILY: &str = "ens_v2_registry_l1";
const MIGRATION_FAMILY: &str = "ens_v2_migration_l1";
/// The subregistry value of an entry that points at no registry.
const NO_REGISTRY: &str = "0x0000000000000000000000000000000000000000";

fn at(block: i64) -> Call<'static> {
    Call {
        block,
        after: block - 1,
        root: Some(ROOT),
    }
}

impl World {
    async fn created(&self, registry: &str, block: i64) -> Result<String> {
        self.registry_changed("RegistryCreated", REGISTRY_FAMILY, registry, block)
            .await
    }

    /// `registry`'s entry for `labelhash`, pointing at no registry since block 1.
    async fn unmounted(&self, registry: &str, labelhash: &str) -> Result<()> {
        self.mount(registry, labelhash, NO_REGISTRY).await
    }

    async fn set_canonicality(&self, identity: &str, state: &str) -> Result<()> {
        let updated = sqlx::query(
            "UPDATE normalized_events SET canonicality_state = $2::canonicality_state
             WHERE event_identity = $1",
        )
        .bind(identity)
        .bind(state)
        .execute(&self.pool)
        .await?;
        anyhow::ensure!(updated.rows_affected() == 1, "no event {identity}");
        Ok(())
    }

    /// `registry`'s entry for `labelhash` as a row with no resource of its own, pointing at
    /// `subregistry` since block 1. The row cites the event that last wrote it, `kind`, which
    /// names the entry's token in `token_field` and carries the resource of the entry's
    /// `SubregistryChanged` event. The walk finds the pointer through that event.
    async fn mount_through_origin(
        &self,
        registry: &str,
        labelhash: &str,
        (kind, token_field): (&str, &str),
        subregistry: &str,
    ) -> Result<()> {
        let resource = self.resource(registry, labelhash).await?;
        sqlx::query(
            "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ($1::uuid, $2, $3, 0, 'canonical')",
        )
        .bind(&resource)
        .bind(CHAIN)
        .bind(hash(0))
        .execute(&self.pool)
        .await?;
        let origin = format!("origin:{registry}:{labelhash}");
        sqlx::query(
            "INSERT INTO project_ens_v2_entry_owner (chain_id, registry, entry_key, token_id,
                 resource_id, status, block_number, event_identity)
             VALUES ($1, $2, $3, $4, NULL, 'registered', 1, $5)",
        )
        .bind(CHAIN)
        .bind(registry)
        .bind(entry_key(labelhash))
        .bind(labelhash.to_lowercase())
        .bind(&origin)
        .execute(&self.pool)
        .await?;
        self.event(
            &origin,
            kind,
            REGISTRY_FAMILY,
            1,
            None,
            Some(&resource),
            json!({token_field: labelhash.to_lowercase()}),
            registry,
        )
        .await?;
        self.repoint(registry, labelhash, subregistry, 1).await?;
        Ok(())
    }

    /// A `MigrationApplied` event of `name` at block 3 with `after` as its state.
    async fn migration(&self, name: &str, after: serde_json::Value) -> Result<String> {
        let identity = format!("migration:{name}");
        self.event(
            &identity,
            "MigrationApplied",
            MIGRATION_FAMILY,
            3,
            Some(&id(name)),
            None,
            after,
            ETH,
        )
        .await?;
        Ok(identity)
    }
}

// The registry is created at block 5 and no entry points at it until block 7. The creation
// selects nothing, and is not carried to a later publication: block 7 selects the names under
// the entry because the entry's pointer changed there.
#[tokio::test]
async fn a_registry_created_before_its_mount_selects_at_the_mount() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth"])
        .await?;
    world.eth().await?;
    world.unmounted(ETH, &labelhash("foo")).await?;
    world.created(&registry(1), 5).await?;
    world
        .repoint(ETH, &labelhash("foo"), &registry(1), 7)
        .await?;
    world.assert_selects(at(5), &[]).await?;
    world
        .assert_selects(at(7), &["foo.eth", "a.foo.eth"])
        .await?;
    world.cleanup().await
}

// One publication holds the registry's creation and the pointer change that mounts it at
// `foo.eth`, as one transaction or one rebuild range does. `bar`'s entry changes in the same
// publication and names no registry. A changed registry does not hide a changed entry.
#[tokio::test]
async fn a_registry_created_and_mounted_in_one_publication_selects_once() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth", "baz.eth"])
        .await?;
    world.eth().await?;
    world.unmounted(ETH, &labelhash("foo")).await?;
    world.created(&registry(1), 5).await?;
    world
        .repoint(ETH, &labelhash("foo"), &registry(1), 5)
        .await?;
    world.entry_changed(5, ETH, &labelhash("bar")).await?;
    world
        .assert_selects(at(5), &["foo.eth", "a.foo.eth", "bar.eth"])
        .await?;
    world.cleanup().await
}

// The registry mounted at `foo.eth` is upgraded at block 5 and announced at block 7. Each
// publication selects the same names.
#[tokio::test]
async fn an_upgrade_then_an_announcement_select_the_same_names() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world
        .registry_changed("Upgraded", MIGRATION_FAMILY, &registry(1), 5)
        .await?;
    world
        .registry_changed("ContractDiscovered", REGISTRY_FAMILY, &registry(1), 7)
        .await?;
    for block in [5, 7] {
        world
            .assert_selects(at(block), &["foo.eth", "a.foo.eth"])
            .await?;
    }
    world.cleanup().await
}

// An entry that once pointed at the registry is not a mount of it. `foo`'s entry pointed at
// the registry at block 1 and at another registry from block 3. `bar`'s pointed at it at block 1
// and at no registry from block 3. The registry is created at block 5 and nothing is selected.
#[tokio::test]
async fn a_former_mount_of_a_changed_registry_selects_nothing() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth", "a.bar.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world
        .repoint(ETH, &labelhash("foo"), &registry(2), 3)
        .await?;
    world.mount(ETH, &labelhash("bar"), &registry(1)).await?;
    world
        .repoint(ETH, &labelhash("bar"), NO_REGISTRY, 3)
        .await?;
    world.created(&registry(1), 5).await?;
    world.assert_selects(at(5), &[]).await?;
    world.cleanup().await
}

// Two entries point at the registry: `foo` in the `.eth` registry, and `k` in the registry
// mounted at `m.eth`. Its creation selects the names under both paths, and not `m.eth`.
#[tokio::test]
async fn a_registry_mounted_twice_selects_under_both_paths() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "a.foo.eth",
            "m.eth",
            "k.m.eth",
            "x.k.m.eth",
            "j.m.eth",
            "baz.eth",
        ])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world.mount(ETH, &labelhash("m"), &registry(2)).await?;
    world
        .mount(&registry(2), &labelhash("k"), &registry(1))
        .await?;
    world.created(&registry(1), 5).await?;
    world
        .assert_selects(at(5), &["foo.eth", "a.foo.eth", "k.m.eth", "x.k.m.eth"])
        .await?;
    world.cleanup().await
}

// The walk stops at the first changed registry or changed entry of a path, and the names
// under it are selected from there. The registry at `a.foo.eth` and the one above it, at
// `foo.eth`, are both created at block 5. The registry at `b.bar.eth` is created at block 5 too,
// and `bar`'s entry above it changes.
#[tokio::test]
async fn a_changed_registry_under_a_changed_slot_stops_at_the_slot() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "a.foo.eth",
            "x.a.foo.eth",
            "bar.eth",
            "b.bar.eth",
            "x.b.bar.eth",
            "baz.eth",
        ])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world
        .mount(&registry(1), &labelhash("a"), &registry(2))
        .await?;
    world.mount(ETH, &labelhash("bar"), &registry(3)).await?;
    world
        .mount(&registry(3), &labelhash("b"), &registry(4))
        .await?;
    for changed in [1, 2, 4] {
        world.created(&registry(changed), 5).await?;
    }
    world.entry_changed(5, ETH, &labelhash("bar")).await?;
    world
        .assert_selects(
            at(5),
            &[
                "foo.eth",
                "a.foo.eth",
                "x.a.foo.eth",
                "bar.eth",
                "b.bar.eth",
                "x.b.bar.eth",
            ],
        )
        .await?;
    world.cleanup().await
}

// The root registry itself is upgraded. No entry points at the root and no entry changed, so
// the walk's first row alone carries the change: every readable `.eth` name is selected, and
// not a name of another top-level label, a shadow surface, or a surface first observed above
// the publication. With no root registry admitted the same event selects nothing.
#[tokio::test]
async fn a_changed_root_registry_selects_every_eth_name_with_no_changed_slot() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth", "foo.xyz", "xyz"])
        .await?;
    world.surface(Surface::of("later.eth").block(9)).await?;
    let mut shadow = Surface::of("shadow.eth");
    shadow.shadow = true;
    world.surface(shadow).await?;
    world.eth().await?;
    world
        .registry_changed("Upgraded", REGISTRY_FAMILY, ROOT, 7)
        .await?;
    world
        .assert_selects(at(7), &["eth", "foo.eth", "a.foo.eth", "bar.eth"])
        .await?;
    let unadmitted = Call {
        block: 7,
        after: 6,
        root: None,
    };
    world.assert_selects(unadmitted, &[]).await?;
    world.cleanup().await
}

// An entry row can hold no resource of its own. The walk then reads the entry's pointer
// through the resource of the event that last wrote the row. `foo`'s row was
// last written by an event that names its token as `token_id`, and `bar`'s by one that names it
// as `new_token_id`. Both point at the registry created at block 5.
#[tokio::test]
async fn a_mount_linked_through_its_origin_event_is_found() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "a.foo.eth",
            "bar.eth",
            "a.bar.eth",
            "baz.eth",
        ])
        .await?;
    world.eth().await?;
    for (label, origin) in [
        ("foo", ("ExpiryChanged", "token_id")),
        ("bar", ("TokenRegenerated", "new_token_id")),
    ] {
        world
            .mount_through_origin(ETH, &labelhash(label), origin, &registry(1))
            .await?;
    }
    world.created(&registry(1), 5).await?;
    world
        .assert_selects(at(5), &["foo.eth", "a.foo.eth", "bar.eth", "a.bar.eth"])
        .await?;
    world.cleanup().await
}

// A `SubregistryChanged` event carries a name only when its token has one. `foo`'s pointer
// event names `foo.eth` and `bar`'s names nothing. Both mount the registry created at block 5.
#[tokio::test]
async fn a_mount_whose_pointer_event_has_no_name_is_found() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth", "a.bar.eth"])
        .await?;
    world.eth().await?;
    world.unmounted(ETH, &labelhash("foo")).await?;
    let resource = world.resource(ETH, &labelhash("foo")).await?;
    world
        .event(
            "named pointer",
            "SubregistryChanged",
            REGISTRY_FAMILY,
            2,
            Some(&id("foo.eth")),
            Some(&resource),
            json!({"subregistry": registry(1), "token_id": labelhash("foo")}),
            ETH,
        )
        .await?;
    world.mount(ETH, &labelhash("bar"), &registry(1)).await?;
    world.created(&registry(1), 5).await?;
    world
        .assert_selects(at(5), &["foo.eth", "a.foo.eth", "bar.eth", "a.bar.eth"])
        .await?;
    world.cleanup().await
}

// In a redo every later event already exists. The publication at block 5 reads each entry's
// pointer as it stood at block 5. `foo`'s entry points at another registry until block 9 names
// the created one, so nothing is selected through it. `bar`'s points at the created registry
// until block 9 names another, so the names under it are selected.
#[tokio::test]
async fn a_redo_ignores_a_mount_made_after_the_publication() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth", "a.bar.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(2)).await?;
    world
        .repoint(ETH, &labelhash("foo"), &registry(1), 9)
        .await?;
    world.mount(ETH, &labelhash("bar"), &registry(1)).await?;
    world
        .repoint(ETH, &labelhash("bar"), &registry(2), 9)
        .await?;
    world.created(&registry(1), 5).await?;
    world
        .assert_selects(at(5), &["bar.eth", "a.bar.eth"])
        .await?;
    world.cleanup().await
}

// An orphaned event counts for nothing until a replay marks it canonical again. The pointer
// event that mounts the first registry at `foo.eth` is orphaned, and so is the creation of the
// second registry, mounted at `bar.eth`. Both registries are created at block 5. Nothing is
// selected. Each event then turns canonical in turn and its names are selected.
#[tokio::test]
async fn an_orphaned_mount_or_creation_selects_nothing_until_it_is_canonical() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth", "a.bar.eth"])
        .await?;
    world.eth().await?;
    world.unmounted(ETH, &labelhash("foo")).await?;
    let pointer = world
        .repoint(ETH, &labelhash("foo"), &registry(1), 3)
        .await?;
    world.created(&registry(1), 5).await?;
    world.mount(ETH, &labelhash("bar"), &registry(2)).await?;
    let creation = world.created(&registry(2), 5).await?;
    world.set_canonicality(&pointer, "orphaned").await?;
    world.set_canonicality(&creation, "orphaned").await?;
    world.assert_selects(at(5), &[]).await?;
    world.set_canonicality(&pointer, "canonical").await?;
    world
        .assert_selects(at(5), &["foo.eth", "a.foo.eth"])
        .await?;
    world.set_canonicality(&creation, "canonical").await?;
    world
        .assert_selects(at(5), &["foo.eth", "a.foo.eth", "bar.eth", "a.bar.eth"])
        .await?;
    world.cleanup().await
}

// A touched name of two labels counts as its own ancestor, and a name of one label has none.
// `mig.eth` is touched at block 5 and its ENSv1→ENSv2 migration holds the registry mounted at
// `m.eth`. `xyz` is touched at block 7 and its migration holds the registry mounted at `n.eth`.
#[tokio::test]
async fn a_touched_migrated_name_counts_as_its_own_ancestor() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "mig.eth",
            "s.mig.eth",
            "m.eth",
            "k.m.eth",
            "xyz",
            "n.eth",
        ])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("m"), &registry(1)).await?;
    world.migrated("mig.eth", &registry(1), 3).await?;
    world.mount(ETH, &labelhash("n"), &registry(2)).await?;
    world.migrated("xyz", &registry(2), 3).await?;
    world.touch(5, "mig.eth").await?;
    world.touch(7, "xyz").await?;
    world
        .assert_selects(at(5), &["mig.eth", "s.mig.eth", "m.eth", "k.m.eth"])
        .await?;
    world.assert_selects(at(7), &[]).await?;
    world.cleanup().await
}

// In a redo an ENSv1→ENSv2 migration applied above the publication does not exist yet.
// `mig.eth`'s migration is at block 9. A name under it is touched at block 5, where only the touched name is
// selected, and again at block 10, where the migration's registry is reached.
#[tokio::test]
async fn a_redo_ignores_a_migration_after_the_publication() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "mig.eth", "t.mig.eth", "m.eth", "k.m.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("m"), &registry(1)).await?;
    world.migrated("mig.eth", &registry(1), 9).await?;
    world.touch(5, "t.mig.eth").await?;
    world.touch(10, "t.mig.eth").await?;
    world.assert_selects(at(5), &["t.mig.eth"]).await?;
    world
        .assert_selects(at(10), &["t.mig.eth", "m.eth", "k.m.eth"])
        .await?;
    world.cleanup().await
}

// ENSv1→ENSv2 migration evidence counts only while the migration event and the creation it
// names are both canonical and the evidence names a creation. Seven names are migrated, each with a registry
// mounted at a name of its own, and a name under each is touched. The first migration is whole
// and its registry's names are selected. The others select nothing more: an orphaned migration
// event, an empty evidence list, no evidence field, evidence of an event that does not exist,
// evidence of an orphaned creation, and evidence that calls the creation another kind.
#[tokio::test]
async fn migration_evidence_that_is_orphaned_empty_or_dangling_selects_nothing() -> Result<()> {
    let cases = [
        "whole",
        "orphaned",
        "empty",
        "absent",
        "dangling",
        "uncreated",
        "miskind",
    ];
    let mut world = World::new().await?;
    world.surfaces(&["eth"]).await?;
    world.eth().await?;
    let mut touched = Vec::new();
    for (n, case) in cases.into_iter().enumerate() {
        let (migrated, child) = (format!("{case}.eth"), format!("t.{case}.eth"));
        let (mounted, below) = (format!("m{case}.eth"), format!("k.m{case}.eth"));
        world
            .surfaces(&[&migrated[..], &child[..], &mounted[..], &below[..]])
            .await?;
        let registry = registry(u8::try_from(n)?);
        world
            .mount(ETH, &labelhash(&format!("m{case}")), &registry)
            .await?;
        let creation = format!("creation:{case}");
        if case != "dangling" {
            world
                .event(
                    &creation,
                    "RegistryCreated",
                    REGISTRY_FAMILY,
                    1,
                    None,
                    None,
                    json!({"registry": registry}),
                    ETH,
                )
                .await?;
        }
        let kind = if case == "miskind" {
            "ContractDiscovered"
        } else {
            "RegistryCreated"
        };
        let after = match case {
            "empty" => json!({"evidence": []}),
            "absent" => json!({}),
            _ => json!({"evidence": [{"event_identity": creation, "event_kind": kind}]}),
        };
        let migration = world.migration(&migrated, after).await?;
        if case == "orphaned" {
            world.set_canonicality(&migration, "orphaned").await?;
        }
        if case == "uncreated" {
            world.set_canonicality(&creation, "orphaned").await?;
        }
        world.touch(5, &child).await?;
        touched.push(child);
    }
    let mut expected: Vec<&str> = touched.iter().map(String::as_str).collect();
    expected.extend(["mwhole.eth", "k.mwhole.eth"]);
    world.assert_selects(at(5), &expected).await?;
    world.cleanup().await
}

// The route reads no order between an ENSv1→ENSv2 migration and the mount of the registry it
// holds.
// `mig1.eth` is migrated at block 2 and its registry is mounted at `m1.eth` at block 4.
// `mig2.eth`'s registry is mounted at `m2.eth` at block 2 and the name is migrated at block 4. A
// name under each is touched at block 3, where neither registry is both evidenced and mounted,
// and at block 6, where both are.
#[tokio::test]
async fn a_migration_and_its_mount_in_either_order_select_the_same_names() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "mig1.eth",
            "t.mig1.eth",
            "m1.eth",
            "k.m1.eth",
            "mig2.eth",
            "t.mig2.eth",
            "m2.eth",
            "k.m2.eth",
        ])
        .await?;
    world.eth().await?;
    for (n, (migrated, mounted)) in [(2, 4), (4, 2)].into_iter().enumerate() {
        let n = u8::try_from(n + 1)?;
        let label = labelhash(&format!("m{n}"));
        world.unmounted(ETH, &label).await?;
        world.repoint(ETH, &label, &registry(n), mounted).await?;
        world
            .migrated(&format!("mig{n}.eth"), &registry(n), migrated)
            .await?;
    }
    for block in [3, 6] {
        world.touch(block, "t.mig1.eth").await?;
        world.touch(block, "t.mig2.eth").await?;
    }
    world
        .assert_selects(at(3), &["t.mig1.eth", "t.mig2.eth"])
        .await?;
    world
        .assert_selects(
            at(6),
            &[
                "t.mig1.eth",
                "m1.eth",
                "k.m1.eth",
                "t.mig2.eth",
                "m2.eth",
                "k.m2.eth",
            ],
        )
        .await?;
    world.cleanup().await
}

// The registry mounted at the root's `eth` entry is upgraded. Its one mount is that entry, so
// every name under `eth` is a candidate, and every readable `.eth` name is selected, `eth`
// included. A name of another top-level label is not.
#[tokio::test]
async fn a_changed_eth_registry_selects_every_name_under_eth() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth", "foo.xyz", "xyz"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world
        .registry_changed("Upgraded", REGISTRY_FAMILY, ETH, 5)
        .await?;
    world
        .assert_selects(at(5), &["eth", "foo.eth", "a.foo.eth", "bar.eth"])
        .await?;
    world.cleanup().await
}

// Anyone can point an entry at any registry, an ancestor of its own included. Each case points
// one entry back up the path and changes a registry on the cycle. The walk dedups by depth and
// stops at the first changed registry, so it selects what it reaches and terminates:
//
// - `loop.eth` points at the root, and the root is upgraded: every `.eth` name.
// - `self.foo.eth` points at its own registry, mounted at `foo.eth`, which is created: the
//   names under `foo.eth`.
// - `up.foo.eth` points at the `.eth` registry, which is upgraded: every `.eth` name.
// - `b.a.foo.eth` points back at the registry of `foo.eth`, and the registry at `a.foo.eth`,
//   reached only through `foo.eth`, is created: the names under `a.foo.eth`.
#[tokio::test]
async fn a_registry_mounted_in_a_cycle_selects_what_the_walk_reaches() -> Result<()> {
    let names = [
        "eth",
        "foo.eth",
        "loop.eth",
        "x.loop.eth",
        "self.foo.eth",
        "self.self.foo.eth",
        "up.foo.eth",
        "a.foo.eth",
        "b.a.foo.eth",
        "a.b.a.foo.eth",
        "bar.eth",
    ];
    let every: &[&str] = &names;
    let cases: [(&str, &str, &[&str]); 4] = [
        ("loop to the root", ROOT, every),
        (
            "loop to its own registry",
            "r1",
            &[
                "foo.eth",
                "self.foo.eth",
                "self.self.foo.eth",
                "up.foo.eth",
                "a.foo.eth",
                "b.a.foo.eth",
                "a.b.a.foo.eth",
            ],
        ),
        ("loop to the eth registry", ETH, every),
        (
            "loop through a deeper registry",
            "r2",
            &["a.foo.eth", "b.a.foo.eth", "a.b.a.foo.eth"],
        ),
    ];
    for (case, changed, expected) in cases {
        let mut world = World::new().await?;
        world.surfaces(&names).await?;
        world.eth().await?;
        world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
        world.mount(ETH, &labelhash("loop"), ROOT).await?;
        world
            .mount(&registry(1), &labelhash("self"), &registry(1))
            .await?;
        world.mount(&registry(1), &labelhash("up"), ETH).await?;
        world
            .mount(&registry(1), &labelhash("a"), &registry(2))
            .await?;
        world
            .mount(&registry(2), &labelhash("b"), &registry(1))
            .await?;
        match changed {
            "r1" => world.created(&registry(1), 5).await.map(drop)?,
            "r2" => world.created(&registry(2), 5).await.map(drop)?,
            address => world
                .registry_changed("Upgraded", REGISTRY_FAMILY, address, 5)
                .await
                .map(drop)?,
        }
        world
            .assert_selects(at(5), expected)
            .await
            .map_err(|error| error.context(case.to_owned()))?;
        world.cleanup().await?;
    }
    Ok(())
}

// One publication holds every arm at once: a created registry mounted at `foo.eth`, a changed
// entry `baz` elsewhere, an ENSv1 registry pointer change at `bar.eth`, which selects only the
// names that read it, and a reclassified resolver that `qux.eth` points at. The selection is
// the union of each arm's own. `c.bar.eth` has a resolver of its own and reads no other.
#[tokio::test]
async fn a_mixed_block_with_a_changed_registry_selects_the_union() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "a.foo.eth",
            "bar.eth",
            "b.bar.eth",
            "c.bar.eth",
            "x.c.bar.eth",
            "baz.eth",
            "k.baz.eth",
            "qux.eth",
            "y.qux.eth",
            "other.eth",
        ])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world.created(&registry(1), 5).await?;
    world.entry_changed(5, ETH, &labelhash("baz")).await?;
    world.registry_pointer_at("c.bar.eth", RESOLVER).await?;
    world
        .registry_pointer_changed(
            5,
            "bar.eth",
            RESOLVER,
            "0x00000000000000000000000000000000000000c2",
        )
        .await?;
    world
        .registry_pointer_at("qux.eth", "0x00000000000000000000000000000000000000c3")
        .await?;
    world
        .reclassify("0x00000000000000000000000000000000000000c3")
        .await?;
    world
        .assert_selects(
            at(5),
            &[
                "foo.eth",
                "a.foo.eth",
                "bar.eth",
                "b.bar.eth",
                "x.c.bar.eth",
                "baz.eth",
                "k.baz.eth",
                "qux.eth",
                "y.qux.eth",
            ],
        )
        .await?;
    world.cleanup().await
}

// A rebuild range journals every block under its last block. The range from block 10 to 12
// creates a registry and mounts it at `foo.eth` in its first block and changes the entry `a` in
// it in its last. It selects the names under `foo.eth`, as the two blocks do when each is a
// publication of its own.
#[tokio::test]
async fn a_range_that_creates_and_mounts_a_registry_selects_as_its_blocks_do() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "a.foo.eth",
            "x.a.foo.eth",
            "b.foo.eth",
            "bar.eth",
        ])
        .await?;
    world.eth().await?;
    world.unmounted(ETH, &labelhash("foo")).await?;
    world.created(&registry(1), 10).await?;
    world
        .repoint(ETH, &labelhash("foo"), &registry(1), 10)
        .await?;
    world
        .entry_changed(12, &registry(1), &labelhash("a"))
        .await?;
    let under_foo = ["foo.eth", "a.foo.eth", "x.a.foo.eth", "b.foo.eth"];
    let range = Call {
        block: 12,
        after: 9,
        root: Some(ROOT),
    };
    world.assert_selects(range, &under_foo).await?;
    world.assert_selects(at(10), &under_foo).await?;
    world
        .assert_selects(at(12), &["a.foo.eth", "x.a.foo.eth"])
        .await?;
    world.cleanup().await
}
