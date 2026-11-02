//! Which names a resolver's classification change expands through the names that point at it.
//!
//! A name whose registry pointer, or ENSv1 resource pointer, names the resolver reads it as its
//! own pointer: the ENSv1 mirror walk reads the nearest registry pointer that is not cleared.
//! So the change expands like a registry pointer change of that name: the name itself, and
//! under it the `.eth` names of three or more labels with no resolver of their own. A name
//! whose ENSv2 resource pointer names the resolver, or whose path another change of the
//! publication also names, keeps its whole subtree.
//!
//! A narrowing row checks the frozen statement too, through `World::assert_narrows`, and names
//! the names it selects that the current statement no longer does.
//!
//! The tree is the one of `registry_pointer_tests.rs`: `eth`, `foo.eth`, `a.foo.eth`,
//! `b.foo.eth`, `c.foo.eth`, `x.a.foo.eth`, `bar.eth` and `z.bar.eth`. Unless a row says
//! otherwise, `foo.eth` and `a.foo.eth` have a resolver, `c.foo.eth` has a pointer row with the
//! zero address, and the others have no pointer row.
use anyhow::{Context, Result, ensure};
use serde_json::json;

use super::registry_pointer_tests::{
    A_RESOLVER, AFTER, BEFORE, CALL, FOO_RESOLVER, POINTERS, SET, UNDER_ETH, pointer_changed, tree,
};
use super::{
    Call, ETH, HOLDER_ADDR, ROOT, Surface, World, ZERO_ADDR, labelhash, rebound_world, registry,
};

/// A resolver only the rows of a test point at.
const READ: &str = "0x00000000000000000000000000000000000000c3";
/// The resource behind `foo.eth` in the rows that give it a resource pointer.
const RESOURCE: &str = "00000000-0000-0000-0000-0000000000f6";
/// `foo.eth` and every name under it.
const FOO_SUBTREE: [&str; 5] = [
    "foo.eth",
    "a.foo.eth",
    "b.foo.eth",
    "c.foo.eth",
    "x.a.foo.eth",
];
/// The names under `foo.eth` that read its pointer, and `foo.eth`.
const FOO_READERS: [&str; 4] = ["foo.eth", "b.foo.eth", "c.foo.eth", "x.a.foo.eth"];

/// The publication at block 2 wrote `foo.eth`'s resource pointer at `FOO_RESOLVER`, of the
/// ENSv2 registry family.
async fn foo_resource_pointer(world: &World) -> Result<()> {
    world
        .resource_pointer_changed(
            2,
            "foo.eth",
            RESOURCE,
            FOO_RESOLVER,
            FOO_RESOLVER,
            "ResolverChanged",
            json!({"block_number": 2, "event_identity": "set:foo"}),
            json!({}),
        )
        .await
}

// Rows B1 and B2. A classification change of the resolver `eth` points at selects `eth` and the
// names of three or more labels without a resolver of their own, as a pointer change at `eth`
// does. The second-level names never read `eth`'s pointer, and `a.foo.eth` reads its own.
// `z.bar.eth` has no pointer at any node below `eth`, so its walk reaches `eth`'s. The root
// registry the publication admits plays no part.
#[tokio::test]
async fn a_resolver_change_at_eth_selects_the_deeper_names_without_a_resolver() -> Result<()> {
    let mut failures = Vec::new();
    for (case, root) in [("admitted root", Some(ROOT)), ("no admission", None)] {
        let world = tree(&POINTERS, None).await?;
        world.registry_pointer_at("eth", READ).await?;
        world.reclassify(READ).await?;
        if let Err(error) = world
            .assert_narrows(
                Call { root, ..CALL },
                &UNDER_ETH,
                &["foo.eth", "a.foo.eth", "bar.eth"],
            )
            .await
        {
            failures.push(format!("eth reader case {case}: {error:#}"));
        }
        world.cleanup().await?;
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// Row B3. A classification change of `foo.eth`'s resolver selects `foo.eth` and the names under
// it without a resolver of their own. `a.foo.eth` has one. `x.a.foo.eth` has none and is
// selected.
#[tokio::test]
async fn a_resolver_change_at_a_second_level_name_skips_a_child_with_its_own_resolver() -> Result<()>
{
    let world = tree(&POINTERS, None).await?;
    world.reclassify(FOO_RESOLVER).await?;
    world
        .assert_narrows(CALL, &FOO_READERS, &["a.foo.eth"])
        .await?;
    world.cleanup().await
}

// Row B5. A reader under which no name reads its pointer recomposes itself alone: `z.bar.eth`
// has a resolver of its own.
#[tokio::test]
async fn a_resolver_reader_with_no_reading_descendant_recomposes_itself() -> Result<()> {
    let mut pointers = POINTERS.to_vec();
    pointers.extend([("bar.eth", READ), ("z.bar.eth", SET)]);
    let world = tree(&pointers, None).await?;
    world.reclassify(READ).await?;
    world
        .assert_narrows(CALL, &["bar.eth"], &["z.bar.eth"])
        .await?;
    world.cleanup().await
}

// Row B6. A reader outside `.eth`, a top-level name or a name under one, selects no name, as
// before: neither itself nor a name under it.
#[tokio::test]
async fn a_resolver_reader_outside_eth_selects_no_name() -> Result<()> {
    let mut failures = Vec::new();
    for reader in ["xyz", "foo.xyz"] {
        let mut world = tree(&POINTERS, None).await?;
        world.surfaces(&["xyz", "foo.xyz", "a.foo.xyz"]).await?;
        world.registry_pointer_at(reader, READ).await?;
        world.reclassify(READ).await?;
        if let Err(error) = world.assert_selects(CALL, &[]).await {
            failures.push(format!("reader {reader}: {error:#}"));
        }
        world.cleanup().await?;
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// Row B7. One ENSv1 `ResolverChanged` of `foo.eth` writes its resource pointer, of an ENSv1
// family, beside its registry pointer. No name reads an ENSv1 resource pointer of an ancestor,
// so a reader through both stays narrow, as through its registry pointer alone.
#[tokio::test]
async fn an_ensv1_resource_pointer_reader_skips_a_child_with_its_own_resolver() -> Result<()> {
    let world = tree(&POINTERS, None).await?;
    foo_resource_pointer(&world).await?;
    world
        .set_by_ens_v1("set:foo", 2, "foo.eth", RESOURCE, FOO_RESOLVER)
        .await?;
    world.reclassify(FOO_RESOLVER).await?;
    world
        .assert_narrows(CALL, &FOO_READERS, &["a.foo.eth"])
        .await?;
    world.cleanup().await
}

// Row B8. An ENSv2 resource pointer at the resolver keeps the whole subtree, `a.foo.eth` with
// its own resolver included: the ENSv2 path walk reads the nearest entry resolver at any depth.
// A name whose registry pointer names the resolver too keeps it as well.
#[tokio::test]
async fn an_ensv2_resource_pointer_reader_keeps_the_whole_subtree() -> Result<()> {
    let mut failures = Vec::new();
    for (case, pointers) in [
        ("ENSv2 resource pointer alone", &POINTERS[1..]),
        ("with the registry pointer", &POINTERS[..]),
    ] {
        let world = tree(pointers, None).await?;
        foo_resource_pointer(&world).await?;
        world.reclassify(FOO_RESOLVER).await?;
        if let Err(error) = world.assert_selects(CALL, &FOO_SUBTREE).await {
            failures.push(format!("{case}: {error:#}"));
        }
        world.cleanup().await?;
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// Row B11. A reader whose path another change of the publication also names keeps its whole
// subtree: a wrapper row, an owner getter class or an ENSv2 entry of `foo.eth`.
#[tokio::test]
async fn a_resolver_reader_with_another_change_keeps_the_whole_subtree() -> Result<()> {
    const WRAPPED: &str = "00000000-0000-0000-0000-0000000000f7";
    let mut failures = Vec::new();
    for case in ["wrapper row", "owner getter class", "foo entry"] {
        let world = tree(&POINTERS, None).await?;
        world.eth().await?;
        world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
        world.reclassify(FOO_RESOLVER).await?;
        match case {
            "wrapper row" => {
                world
                    .wrapper_state_changed(
                        5,
                        WRAPPED,
                        None,
                        Some((
                            Some("foo.eth"),
                            Some(0),
                            true,
                            Some("1907776000"),
                            Some("wrapped"),
                            Some(false),
                        )),
                    )
                    .await?;
            }
            "owner getter class" => {
                world
                    .registry_node_state_changed(
                        5,
                        "foo.eth",
                        None,
                        Some((Some(HOLDER_ADDR), Some(HOLDER_ADDR), None, false)),
                    )
                    .await?;
            }
            _ => world.entry_changed(5, ETH, &labelhash("foo")).await?,
        }
        if let Err(error) = world.assert_selects(CALL, &FOO_SUBTREE).await {
            failures.push(format!("other change case {case}: {error:#}"));
        }
        world.cleanup().await?;
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// Row B12. One publication changes a resolver, a registry pointer, an entry and a registry, each
// of separate names, and each expands by its own rule. `foo.eth` reads the resolver and stays
// narrow, and so does `bar.eth`, whose pointer moves. The names under the changed entry of `qux`
// and under the changed registry mounted at `vex` are selected whatever their pointers.
#[tokio::test]
async fn in_a_mixed_batch_a_resolver_reader_expands_only_for_its_own_change() -> Result<()> {
    let mut world = tree(&POINTERS, None).await?;
    world
        .surfaces(&["qux.eth", "a.qux.eth", "vex.eth", "a.vex.eth"])
        .await?;
    world.registry_pointer_at("a.qux.eth", SET).await?;
    world.registry_pointer_at("a.vex.eth", SET).await?;
    world.eth().await?;
    world.mount(ETH, &labelhash("qux"), &registry(1)).await?;
    world.mount(ETH, &labelhash("vex"), &registry(2)).await?;
    world.reclassify(FOO_RESOLVER).await?;
    pointer_changed(&world, "bar.eth", None, SET).await?;
    world.entry_changed(5, ETH, &labelhash("qux")).await?;
    world
        .registry_changed("Upgraded", "ens_v2_registry_l1", &registry(2), 5)
        .await?;
    world
        .assert_narrows(
            CALL,
            &[
                "foo.eth",
                "b.foo.eth",
                "c.foo.eth",
                "x.a.foo.eth",
                "bar.eth",
                "z.bar.eth",
                "qux.eth",
                "a.qux.eth",
                "vex.eth",
                "a.vex.eth",
            ],
            &["a.foo.eth"],
        )
        .await?;
    world.cleanup().await
}

// Row B13. The selection reads every pointer as the publication left it. `b.foo.eth` sets a
// resolver of its own in the block that reclassifies `foo.eth`'s, so it no longer reads
// `foo.eth`'s pointer and is selected as a touched name. `a.foo.eth` clears its own in that
// block, so it reads `foo.eth`'s, and is touched too.
#[tokio::test]
async fn a_pointer_change_under_a_reader_in_the_same_batch_is_judged_after_the_batch() -> Result<()>
{
    let mut failures = Vec::new();
    for (case, (name, before, after), expected, dropped) in [
        (
            "b.foo.eth set",
            ("b.foo.eth", None, SET),
            &["foo.eth", "b.foo.eth", "c.foo.eth", "x.a.foo.eth"][..],
            &["a.foo.eth"][..],
        ),
        (
            "a.foo.eth cleared",
            ("a.foo.eth", Some(A_RESOLVER), ZERO_ADDR),
            &FOO_SUBTREE[..],
            &[][..],
        ),
    ] {
        let world = tree(&POINTERS, None).await?;
        world.reclassify(FOO_RESOLVER).await?;
        pointer_changed(&world, name, before, after).await?;
        let checked = if dropped.is_empty() {
            world.assert_selects(CALL, expected).await
        } else {
            world.assert_narrows(CALL, expected, dropped).await
        };
        if let Err(error) = checked {
            failures.push(format!("batch case {case}: {error:#}"));
        }
        world.cleanup().await?;
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// Row B14. `foo.eth`'s pointer leaves the resolver in the block that reclassifies it. The
// selection reads the pointer rows as the publication left them, not the window's events, so
// `foo.eth` reads the resolver no longer and expands for its pointer change alone.
#[tokio::test]
async fn a_pointer_that_leaves_a_reclassified_resolver_expands_as_a_pointer_change() -> Result<()> {
    let world = tree(&POINTERS, None).await?;
    pointer_changed(&world, "foo.eth", Some(FOO_RESOLVER), AFTER).await?;
    world.reclassify(FOO_RESOLVER).await?;
    world.assert_selects(CALL, &FOO_READERS).await?;
    world.cleanup().await
}

// Row B15. A reader node with no surface selects no name. The ENSv1 mirror walk reads an
// ancestor's pointer only through the ancestor's surface, so `k.ghost.eth` does not read the
// pointer of `ghost.eth`, which has none.
#[tokio::test]
async fn a_resolver_reader_with_no_surface_selects_no_name() -> Result<()> {
    let mut world = tree(&POINTERS, None).await?;
    world.surface(Surface::of("k.ghost.eth")).await?;
    world.registry_pointer_at("ghost.eth", READ).await?;
    world.reclassify(READ).await?;
    world.assert_selects(CALL, &[]).await?;
    world.cleanup().await
}

// Row B16. A reader whose surface has a null first label hash has no entry key to look its
// names up by, so it stays a full prefix and its whole subtree is selected: `x.null.foo.eth`
// has a resolver of its own and is selected.
#[tokio::test]
async fn a_resolver_reader_with_a_null_first_label_keeps_the_whole_subtree() -> Result<()> {
    let mut world = World::new().await?;
    let label = |text: &str| Some(labelhash(text));
    world.surfaces(&["eth", "foo.eth", "b.foo.eth"]).await?;
    for (name, labels) in [
        ("null.foo.eth", vec![None, label("foo"), label("eth")]),
        (
            "x.null.foo.eth",
            vec![label("x"), None, label("foo"), label("eth")],
        ),
    ] {
        world.surface(Surface::of(name).labels(labels)).await?;
    }
    world.registry_pointer_at("null.foo.eth", READ).await?;
    world.registry_pointer_at("x.null.foo.eth", SET).await?;
    world.reclassify(READ).await?;
    world
        .assert_selects(CALL, &["null.foo.eth", "x.null.foo.eth"])
        .await?;
    world.cleanup().await
}

// Row B17. A rebuild range journals every block under its last block. A range whose first block
// sets `eth`'s pointer to a resolver and whose last block reclassifies it selects what its two
// blocks select one by one.
#[tokio::test]
async fn a_range_selects_what_its_blocks_select_one_by_one() -> Result<()> {
    const DROPPED: [&str; 3] = ["foo.eth", "a.foo.eth", "bar.eth"];
    let at = |block, after| Call {
        block,
        after,
        root: Some(ROOT),
    };
    let world = tree(&POINTERS, None).await?;
    world
        .registry_pointer_changed(10, "eth", BEFORE, READ)
        .await?;
    world.reclassify_at(12, READ).await?;
    world
        .assert_selects(at(10, 9), &UNDER_ETH)
        .await
        .context("block 10")?;
    world
        .assert_narrows(at(12, 11), &UNDER_ETH, &DROPPED)
        .await
        .context("block 12")?;
    world.cleanup().await?;

    let world = tree(&POINTERS, None).await?;
    world
        .registry_pointer_changed(12, "eth", BEFORE, READ)
        .await?;
    world.reclassify_at(12, READ).await?;
    world
        .assert_narrows(at(12, 9), &UNDER_ETH, &DROPPED)
        .await
        .context("the range")?;
    world.cleanup().await
}

// Row B18. A reader whose surface is anchored in the publication's window is a full prefix, as
// any newly surfaced name is: `a.foo.eth`, with its own resolver, is selected.
#[tokio::test]
async fn a_resolver_reader_surfaced_in_the_window_keeps_the_whole_subtree() -> Result<()> {
    let world = tree(&POINTERS, Some("foo.eth")).await?;
    world.reclassify(FOO_RESOLVER).await?;
    world.assert_selects(CALL, &FOO_SUBTREE).await?;
    world.cleanup().await
}

// Row B19. A reader still has its ancestors looked up for ENSv1→ENSv2 migrations. The original
// child `child.foo.eth` reads a reclassified resolver, and `foo.eth` holds the migration to the
// registry mounted at `rebound.eth`, so the names under that mount are selected, as before.
#[tokio::test]
async fn a_resolver_reader_under_a_migrated_name_selects_the_rebound_names() -> Result<()> {
    let world = rebound_world().await?;
    world.registry_pointer_at("child.foo.eth", READ).await?;
    world.reclassify(READ).await?;
    world
        .assert_selects(CALL, &["child.foo.eth", "rebound.eth", "child.rebound.eth"])
        .await?;
    world.cleanup().await
}
