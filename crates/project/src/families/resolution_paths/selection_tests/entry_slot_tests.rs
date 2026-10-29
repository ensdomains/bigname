//! Row tests for the names a changed ENSv2 entry selects, by the registry that holds it.
//!
//! The walk from the root registry reads an entry of the root at a name's last label. It reads an
//! entry of any other registry at a label directly above an entry that points at the registry,
//! which these tests call a slot of the registry, as the statement does. A changed entry selects
//! the names under each path at which the walk reads it. A registry that no slot names is never
//! reached.
//!
//! Each test compares `prepare` with the frozen statement and a literal set. Where the frozen
//! statement keeps more candidates than the walk can use, the test also counts the surfaces the
//! `candidates` step keeps.
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use sqlx::{Connection, raw_sql};

use super::{
    CHAIN, Call, ETH, PLAN_MODES, ROOT, Surface, WORK_TABLE, World, current, id, labelhash,
    registry,
};

/// The subregistry value of an entry that points at no registry.
const NO_REGISTRY: &str = "0x0000000000000000000000000000000000000000";

fn at(block: i64) -> Call<'static> {
    Call {
        block,
        after: block - 1,
        root: Some(ROOT),
    }
}

/// The rows the plan node named `name` returned, over all its loops.
fn node_rows(node: &Value, name: &str) -> Option<i64> {
    if node["Subplan Name"] == name {
        let count = |field: &str| node[field].as_i64().unwrap_or(0);
        return Some(count("Actual Rows") * count("Actual Loops"));
    }
    node["Plans"]
        .as_array()
        .into_iter()
        .flatten()
        .find_map(|child| node_rows(child, name))
}

impl World {
    /// The surfaces the `candidates` step of `prepare`'s statement keeps for `call` under plan
    /// cache mode `mode`.
    async fn candidate_rows(&self, call: Call<'_>, mode: &str) -> Result<i64> {
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin().await?;
        raw_sql(&format!(
            "{WORK_TABLE}; SET LOCAL plan_cache_mode = {mode};
             PREPARE candidate_rows (text, bigint, bigint, bigint, text) AS {}",
            current()
        ))
        .execute(&mut *transaction)
        .await?;
        let root = call
            .root
            .map_or_else(|| "NULL".to_owned(), |root| format!("'{root}'"));
        let plan: Value = sqlx::query_scalar(&format!(
            "EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE candidate_rows ('{CHAIN}', {}, {}, {}, {root})",
            call.block,
            call.time(),
            call.after
        ))
        .fetch_one(&mut *transaction)
        .await?;
        raw_sql("DEALLOCATE candidate_rows")
            .execute(&mut *transaction)
            .await?;
        transaction.rollback().await?;
        Ok(node_rows(&plan[0]["Plan"], "CTE candidates").unwrap_or(0))
    }

    /// `assert_selects`, and the `candidates` step keeps `kept` surfaces under both plan modes.
    async fn assert_candidates(&self, call: Call<'_>, expected: &[&str], kept: i64) -> Result<()> {
        self.assert_selects(call, expected).await?;
        for mode in PLAN_MODES {
            let rows = self.candidate_rows(call, mode).await?;
            ensure!(
                rows == kept,
                "{mode}: the candidates step keeps {rows} surfaces, not {kept}"
            );
        }
        Ok(())
    }

    async fn set_event_state(&self, identity: &str, state: &str) -> Result<()> {
        let updated = sqlx::query(
            "UPDATE normalized_events SET canonicality_state = $2::canonicality_state
             WHERE event_identity = $1",
        )
        .bind(identity)
        .bind(state)
        .execute(&self.pool)
        .await?;
        ensure!(updated.rows_affected() == 1, "no event {identity}");
        Ok(())
    }
}

// The registry the root's `eth` entry points at changes its own `eth` entry, the reserved
// `eth.eth`. The entry's key and its slot's are both `eth`, and the root is directly above the
// slot. Only the names under `eth.eth` are selected, and only the two surfaces that hold `eth`
// directly above `eth` are candidates. `eth.foo.eth` holds `eth` twice, not adjacent.
#[tokio::test]
async fn an_eth_keyed_entry_of_the_eth_registry_selects_only_under_eth_eth() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "eth.eth", "a.eth.eth", "eth.foo.eth"])
        .await?;
    world.eth().await?;
    world.entry_changed(5, ETH, &labelhash("eth")).await?;
    world
        .assert_candidates(at(5), &["eth.eth", "a.eth.eth"], 2)
        .await?;
    world.cleanup().await
}

// A user registry is mounted at `x.eth` and changes its `eth` entry. Only the names under
// `eth.x.eth` are selected. `x.eth` holds both keys, but not `eth` directly above `x`, so it is
// no candidate. `eth.eth` is not under the mount.
#[tokio::test]
async fn an_eth_keyed_entry_of_a_mounted_user_registry_selects_only_under_its_mount() -> Result<()>
{
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "x.eth",
            "eth.x.eth",
            "a.eth.x.eth",
            "eth.eth",
            "foo.eth",
        ])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("x"), &registry(1)).await?;
    world
        .entry_changed(5, &registry(1), &labelhash("eth"))
        .await?;
    world
        .assert_candidates(at(5), &["eth.x.eth", "a.eth.x.eth"], 2)
        .await?;
    world.cleanup().await
}

// The root's `foo` entry is the top-level name `foo`. The walk reads it only at a name's last
// label, so `foo` and `a.foo` are the candidates and `foo.eth` is not. No `.eth` name is under
// `foo`, so nothing is selected.
#[tokio::test]
async fn a_root_entry_matches_only_the_last_label() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "foo", "a.foo"])
        .await?;
    world.eth().await?;
    world.entry_changed(5, ROOT, &labelhash("foo")).await?;
    world.assert_candidates(at(5), &[], 2).await?;
    world.cleanup().await
}

// Anyone can create a registry and change its entries, and no entry need point at it. The walk
// never reaches such a registry, so its entry selects nothing and keeps no candidate, though
// `foo.eth` and `a.foo.eth` hold the entry's label.
#[tokio::test]
async fn an_entry_of_a_registry_no_slot_names_selects_nothing() -> Result<()> {
    let mut world = World::new().await?;
    world.surfaces(&["eth", "foo.eth", "a.foo.eth"]).await?;
    world.eth().await?;
    world
        .entry_changed(5, &registry(9), &labelhash("foo"))
        .await?;
    world.assert_candidates(at(5), &[], 0).await?;
    world.cleanup().await
}

// The entry `a` of a registry changes at block 5, while `foo`'s entry points at no registry.
// From block 7 `foo`'s entry points at the registry. Block 5 selects nothing, and the change is
// not carried: block 7 selects the names under `foo.eth` because `foo`'s pointer changed there.
#[tokio::test]
async fn an_entry_changed_before_its_registry_is_mounted_selects_at_the_mount() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "x.a.foo.eth", "b.foo.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), NO_REGISTRY).await?;
    world
        .entry_changed(5, &registry(1), &labelhash("a"))
        .await?;
    world
        .repoint(ETH, &labelhash("foo"), &registry(1), 7)
        .await?;
    world.assert_selects(at(5), &[]).await?;
    world
        .assert_selects(at(7), &["foo.eth", "a.foo.eth", "x.a.foo.eth", "b.foo.eth"])
        .await?;
    world.cleanup().await
}

// One publication points `foo`'s entry at a registry and changes the registry's entry `a`. The
// slot itself changed, so the names under `foo.eth` are selected, each once.
#[tokio::test]
async fn a_mount_and_an_entry_change_in_one_block_select_once() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "x.a.foo.eth", "b.foo.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), NO_REGISTRY).await?;
    world
        .repoint(ETH, &labelhash("foo"), &registry(1), 5)
        .await?;
    world
        .entry_changed(5, &registry(1), &labelhash("a"))
        .await?;
    world
        .assert_selects(at(5), &["foo.eth", "a.foo.eth", "x.a.foo.eth", "b.foo.eth"])
        .await?;
    world.cleanup().await
}

// One publication points `foo`'s entry away from a registry and changes the registry's entry
// `a`. The lookup of the registry's slots has no block bound, and the walk reads `foo`'s pointer
// at the block. The slot changed, so the names under `foo.eth` are selected.
#[tokio::test]
async fn an_unmount_and_an_entry_change_in_one_block_select_under_the_slot() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "x.a.foo.eth", "b.foo.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world
        .repoint(ETH, &labelhash("foo"), &registry(2), 5)
        .await?;
    world
        .entry_changed(5, &registry(1), &labelhash("a"))
        .await?;
    world
        .assert_selects(at(5), &["foo.eth", "a.foo.eth", "x.a.foo.eth", "b.foo.eth"])
        .await?;
    world.cleanup().await
}

// A `SubregistryChanged` event carries a name only when its token has one. `foo`'s pointer event
// names `foo.eth` and `bar`'s names nothing. Both point at the registry whose entry `a` changes.
#[tokio::test]
async fn an_entry_of_a_registry_mounted_without_a_name_is_found() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "bar.eth", "a.bar.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), NO_REGISTRY).await?;
    let resource = world.resource(ETH, &labelhash("foo")).await?;
    world
        .event(
            "named pointer",
            "SubregistryChanged",
            "ens_v2_registry_l1",
            2,
            Some(&id("foo.eth")),
            Some(&resource),
            json!({"subregistry": registry(1), "token_id": labelhash("foo")}),
            ETH,
        )
        .await?;
    world.mount(ETH, &labelhash("bar"), &registry(1)).await?;
    world
        .entry_changed(5, &registry(1), &labelhash("a"))
        .await?;
    world
        .assert_selects(at(5), &["a.foo.eth", "a.bar.eth"])
        .await?;
    world.cleanup().await
}

// The registry is mounted at `foo.eth`, which has no surface. The names under the changed entry
// do, and the walk reaches them through their own suffixes.
#[tokio::test]
async fn an_entry_under_a_mount_with_no_surface_is_found() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "a.foo.eth", "x.a.foo.eth", "bar.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world
        .entry_changed(5, &registry(1), &labelhash("a"))
        .await?;
    world
        .assert_selects(at(5), &["a.foo.eth", "x.a.foo.eth"])
        .await?;
    world.cleanup().await
}

// Two entries point at the registry: `foo` in the `.eth` registry, and `k` in the registry
// mounted at `m.eth`. Its changed entry `a` selects the names under both paths. `a.m.eth` holds
// `a` above `m`, which is no slot of the registry.
#[tokio::test]
async fn an_entry_of_a_registry_mounted_twice_selects_under_both() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "a.foo.eth",
            "m.eth",
            "k.m.eth",
            "a.k.m.eth",
            "x.a.k.m.eth",
            "a.m.eth",
        ])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world.mount(ETH, &labelhash("m"), &registry(2)).await?;
    world
        .mount(&registry(2), &labelhash("k"), &registry(1))
        .await?;
    world
        .entry_changed(5, &registry(1), &labelhash("a"))
        .await?;
    world
        .assert_selects(at(5), &["a.foo.eth", "a.k.m.eth", "x.a.k.m.eth"])
        .await?;
    world.cleanup().await
}

// `foo`'s entry pointed at the registry at block 1 and at another from block 3. The slot is
// found, but the walk at block 5 follows `foo` to the other registry, which has no entry `a`.
#[tokio::test]
async fn an_entry_of_a_formerly_mounted_registry_selects_nothing() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "x.a.foo.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world
        .repoint(ETH, &labelhash("foo"), &registry(2), 3)
        .await?;
    world
        .entry_changed(5, &registry(1), &labelhash("a"))
        .await?;
    world.assert_selects(at(5), &[]).await?;
    world.cleanup().await
}

// A registry is mounted at `x.eth`, and a second at `y.x.eth`. The second registry's entry `z`
// changes. The names under `z.y.x.eth` are selected. `z.y.eth` holds `z` directly above `y`, so it
// is a candidate, but the walk never reaches `y.eth`.
#[tokio::test]
async fn an_entry_two_mounts_below_a_2ld_is_found() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "x.eth",
            "y.x.eth",
            "z.y.x.eth",
            "w.z.y.x.eth",
            "z.x.eth",
            "z.eth",
            "z.y.eth",
        ])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("x"), &registry(1)).await?;
    world
        .mount(&registry(1), &labelhash("y"), &registry(2))
        .await?;
    world
        .entry_changed(5, &registry(2), &labelhash("z"))
        .await?;
    world
        .assert_candidates(at(5), &["z.y.x.eth", "w.z.y.x.eth"], 3)
        .await?;
    world.cleanup().await
}

// Anyone can point an entry at any registry, the root and its own included. `loop.eth` points
// at the root, `self.foo.eth` at its own registry, `up.foo.eth` at the `.eth` registry, and
// `b.a.foo.eth` back at the registry of `foo.eth`. Each case changes one entry on the cycle:
//
// - The root's `eth` entry: every `.eth` name.
// - The entry `self` of the registry at `foo.eth`: the names under `self.foo.eth` and
//   `self.b.a.foo.eth`, two paths to the same entry.
// - The entry `foo` of the `.eth` registry: the names under `foo.eth`, and `foo.eth.loop.eth`,
//   which the walk reaches through the root a second time.
#[tokio::test]
async fn an_entry_in_a_mounted_cycle_equals_the_oracle() -> Result<()> {
    let names = [
        "eth",
        "foo.eth",
        "loop.eth",
        "x.loop.eth",
        "eth.loop.eth",
        "foo.eth.loop.eth",
        "self.foo.eth",
        "self.self.foo.eth",
        "up.foo.eth",
        "foo.up.foo.eth",
        "a.foo.eth",
        "b.a.foo.eth",
        "self.b.a.foo.eth",
        "bar.eth",
    ];
    let every: &[&str] = &names;
    let cases: [(&str, String, &str, &[&str]); 3] = [
        ("the root's eth entry", ROOT.to_owned(), "eth", every),
        (
            "an entry that points at its own registry",
            registry(1),
            "self",
            &["self.foo.eth", "self.self.foo.eth", "self.b.a.foo.eth"],
        ),
        (
            "an entry of the eth registry",
            ETH.to_owned(),
            "foo",
            &[
                "foo.eth",
                "self.foo.eth",
                "self.self.foo.eth",
                "up.foo.eth",
                "foo.up.foo.eth",
                "a.foo.eth",
                "b.a.foo.eth",
                "self.b.a.foo.eth",
                "foo.eth.loop.eth",
            ],
        ),
    ];
    for (case, holder, label, expected) in cases {
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
        world.entry_changed(5, &holder, &labelhash(label)).await?;
        world
            .assert_selects(at(5), expected)
            .await
            .map_err(|error| error.context(case.to_owned()))?;
        world.cleanup().await?;
    }
    Ok(())
}

// A registry is mounted at `x.eth`, its `eth` entry points at a second registry, and the second
// registry's `eth` entry changes. The entry's key and its slot's are both `eth`, so the probe
// also takes `x`, the first key above of another. Only the names under `eth.eth.x.eth` are
// selected, and only the two surfaces that hold `eth` directly above `eth` and also hold `x` are
// candidates.
#[tokio::test]
async fn an_entry_keyed_like_every_slot_up_to_a_2ld_selects_only_under_its_path() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "x.eth",
            "eth.x.eth",
            "eth.eth.x.eth",
            "a.eth.eth.x.eth",
            "eth.eth",
            "a.eth.eth",
            "foo.eth",
        ])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("x"), &registry(1)).await?;
    world
        .mount(&registry(1), &labelhash("eth"), &registry(2))
        .await?;
    world
        .entry_changed(5, &registry(2), &labelhash("eth"))
        .await?;
    world
        .assert_candidates(at(5), &["eth.eth.x.eth", "a.eth.eth.x.eth"], 2)
        .await?;
    world.cleanup().await
}

// A null label hash has no entry key, so the walk never passes it. The registry mounted at
// `foo.eth` changes its entry `a`. `null.a.foo.eth` holds a null first label and is selected
// under `a.foo.eth`. `y.a.null.foo.eth` holds `a` directly above a null label, and `a.null.eth`
// too, so neither is a candidate or selected.
#[tokio::test]
async fn a_null_label_hash_beside_a_changed_entry_label_selects_as_the_walk_does() -> Result<()> {
    let mut world = World::new().await?;
    let label = |text: &str| Some(labelhash(text));
    world.surfaces(&["eth", "foo.eth", "a.foo.eth"]).await?;
    for (name, labels) in [
        ("a.null.eth", vec![label("a"), None, label("eth")]),
        (
            "null.a.foo.eth",
            vec![None, label("a"), label("foo"), label("eth")],
        ),
        (
            "y.a.null.foo.eth",
            vec![label("y"), label("a"), None, label("foo"), label("eth")],
        ),
    ] {
        world.surface(Surface::of(name).labels(labels)).await?;
    }
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world
        .entry_changed(5, &registry(1), &labelhash("a"))
        .await?;
    world
        .assert_candidates(at(5), &["a.foo.eth", "null.a.foo.eth"], 2)
        .await?;
    world.cleanup().await
}

// A registry minting many entries in one transaction: 1,000 entries of the registry mounted at
// `m.eth` change in one block. The names under each are selected. `e0.eth` to `e4.eth` hold
// the same labels under the `.eth` registry and are no candidates.
#[tokio::test]
async fn a_thousand_entries_of_one_mounted_registry_select_the_names_under_each() -> Result<()> {
    let mut world = World::new().await?;
    world.surfaces(&["eth", "m.eth"]).await?;
    let labels: Vec<String> = (0..1000).map(|n| format!("e{n}")).collect();
    let mut expected: Vec<String> = labels
        .iter()
        .map(|label| format!("{label}.m.eth"))
        .collect();
    expected.push("x.e0.m.eth".to_owned());
    for name in &expected {
        world.surface(Surface::of(name)).await?;
    }
    for label in &labels[..5] {
        world.surface(Surface::of(&format!("{label}.eth"))).await?;
    }
    world.eth().await?;
    world.mount(ETH, &labelhash("m"), &registry(1)).await?;
    for label in &labels {
        world
            .entry_changed(5, &registry(1), &labelhash(label))
            .await?;
    }
    let expected: Vec<&str> = expected.iter().map(String::as_str).collect();
    world.assert_candidates(at(5), &expected, 1001).await?;
    world.cleanup().await
}

// One publication holds each arm: a created registry mounted at `foo.eth`, an entry of the `.eth`
// registry, an entry of the registry at `m.eth`, that registry's own `m` entry (its key and its
// slot's are the same), an entry of a registry nothing points at, an ENSv1 registry pointer
// change at `bar.eth`, and a reclassified resolver that `qux.eth` points at. The selection is
// the union of each arm's own.
#[tokio::test]
async fn a_mixed_block_of_entry_changes_selects_the_union() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&[
            "eth",
            "foo.eth",
            "a.foo.eth",
            "bar.eth",
            "b.bar.eth",
            "baz.eth",
            "k.baz.eth",
            "m.eth",
            "k.m.eth",
            "x.k.m.eth",
            "m.m.eth",
            "z.m.m.eth",
            "qux.eth",
            "y.qux.eth",
            "other.eth",
        ])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    world.mount(ETH, &labelhash("m"), &registry(2)).await?;
    world
        .registry_changed("RegistryCreated", "ens_v2_registry_l1", &registry(1), 5)
        .await?;
    world.entry_changed(5, ETH, &labelhash("baz")).await?;
    world
        .entry_changed(5, &registry(2), &labelhash("k"))
        .await?;
    world
        .entry_changed(5, &registry(2), &labelhash("m"))
        .await?;
    world
        .entry_changed(5, &registry(9), &labelhash("baz"))
        .await?;
    world
        .registry_pointer_changed(
            5,
            "bar.eth",
            "0x00000000000000000000000000000000000000c0",
            "0x00000000000000000000000000000000000000c1",
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
                "baz.eth",
                "k.baz.eth",
                "k.m.eth",
                "x.k.m.eth",
                "m.m.eth",
                "z.m.m.eth",
                "qux.eth",
                "y.qux.eth",
            ],
        )
        .await?;
    world.cleanup().await
}

// A rebuild range journals every block under its last block. The range from block 10 to 12
// points `foo`'s entry at a registry in its first block and changes the registry's entry `a` in
// its last. It selects the names under `foo.eth`, as the two blocks do when each is a publication
// of its own.
#[tokio::test]
async fn a_range_that_mounts_a_registry_and_changes_its_entry_selects_as_its_blocks_do()
-> Result<()> {
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
    world.mount(ETH, &labelhash("foo"), NO_REGISTRY).await?;
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

// An orphaned event counts for nothing until a replay marks it canonical again. The pointer event
// that mounts the registry at `foo.eth` is orphaned, and the registry's entry `a` changes. Nothing
// is selected. The event then turns canonical, and the names under `a.foo.eth` are.
#[tokio::test]
async fn an_orphaned_mount_reaches_no_entry_until_it_is_canonical() -> Result<()> {
    let mut world = World::new().await?;
    world
        .surfaces(&["eth", "foo.eth", "a.foo.eth", "x.a.foo.eth"])
        .await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
    let pointer = format!("mount:{ETH}:{}", labelhash("foo"));
    world
        .entry_changed(5, &registry(1), &labelhash("a"))
        .await?;
    world.set_event_state(&pointer, "orphaned").await?;
    world.assert_candidates(at(5), &[], 0).await?;
    world.set_event_state(&pointer, "canonical").await?;
    world
        .assert_selects(at(5), &["a.foo.eth", "x.a.foo.eth"])
        .await?;
    world.cleanup().await
}
