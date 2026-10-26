//! Which names an ENSv1 registry pointer change expands.
//!
//! Composition reads an ancestor's registry pointer only in the ENSv1 mirror walk, for a name
//! below a `.eth` second-level name, and the walk stops at the nearest pointer that is not
//! cleared. So a pointer change expands the names of three or more labels under the changed
//! node that have no resolver of their own. A name whose path any other change also names keeps
//! its whole subtree.
//!
//! The tree is `eth`, `foo.eth`, `a.foo.eth`, `b.foo.eth`, `c.foo.eth`, `x.a.foo.eth`, `bar.eth`
//! and `z.bar.eth`. Unless a row says otherwise, `foo.eth` and `a.foo.eth` have a resolver,
//! `c.foo.eth` has a pointer row with the zero address, and the others have no pointer row.
use anyhow::{Result, ensure};
use serde_json::json;

use super::{
    CHAIN, Call, ETH, HOLDER_ADDR, ROOT, Surface, World, ZERO_ADDR, labelhash, node, registry,
};

const NAMES: [&str; 8] = [
    "eth",
    "foo.eth",
    "a.foo.eth",
    "b.foo.eth",
    "c.foo.eth",
    "x.a.foo.eth",
    "bar.eth",
    "z.bar.eth",
];
const BEFORE: &str = "0x00000000000000000000000000000000000000c0";
const AFTER: &str = "0x00000000000000000000000000000000000000c1";
const SET: &str = "0x00000000000000000000000000000000000000c2";
const FOO_RESOLVER: &str = "0x00000000000000000000000000000000000000d1";
const A_RESOLVER: &str = "0x00000000000000000000000000000000000000d2";
const POINTERS: [(&str, &str); 3] = [
    ("foo.eth", FOO_RESOLVER),
    ("a.foo.eth", A_RESOLVER),
    ("c.foo.eth", ZERO_ADDR),
];
const CALL: Call<'static> = Call {
    block: 5,
    after: 4,
    root: Some(ROOT),
};
/// The names an `eth` pointer change selects in the default tree: `eth` itself, and the names
/// of three or more labels with no resolver of their own.
const UNDER_ETH: [&str; 5] = ["eth", "b.foo.eth", "c.foo.eth", "x.a.foo.eth", "z.bar.eth"];

/// The tree with `pointers` written by earlier publications. `window` names a surface anchored
/// at the publication's block instead of block 0.
async fn tree(pointers: &[(&str, &str)], window: Option<&str>) -> Result<World> {
    let mut world = World::new().await?;
    for name in NAMES {
        let block = if window == Some(name) { 5 } else { 0 };
        world.surface(Surface::of(name).block(block)).await?;
    }
    for (name, resolver) in pointers {
        world.registry_pointer_at(name, resolver).await?;
    }
    Ok(world)
}

/// The publication at block 5 set the registry pointer of `name` to `after`. `before` is the
/// resolver it replaced, or `None` for the node's first pointer row.
async fn pointer_changed(
    world: &World,
    name: &str,
    before: Option<&str>,
    after: &str,
) -> Result<()> {
    world
        .registry_pointer_changed(5, name, before.unwrap_or(ZERO_ADDR), after)
        .await?;
    if before.is_none() {
        sqlx::query(
            "UPDATE project_family_undo SET before_image = NULL
             WHERE chain_id = $1 AND block_number = 5 AND family = 'project_registry_pointer'
               AND key = $2",
        )
        .bind(CHAIN)
        .bind(json!([CHAIN, "ens", node(name)]).to_string())
        .execute(&world.pool)
        .await?;
    }
    Ok(())
}

// Row A, with row H. A pointer change at `eth` selects `eth` and the names of three or more
// labels without a resolver of their own. The second-level names never read `eth`'s pointer, and
// `a.foo.eth` reads its own. `b.foo.eth` and `x.a.foo.eth` are selected although `foo.eth` and
// `a.foo.eth` have a resolver: the check is on the name's own node. The root registry the
// publication admits plays no part, so a chain with no admission selects the same names.
#[tokio::test]
async fn an_eth_pointer_change_selects_the_deeper_names_without_a_resolver_of_their_own()
-> Result<()> {
    let mut failures = Vec::new();
    for (case, root) in [("admitted root", Some(ROOT)), ("no admission", None)] {
        let world = tree(&POINTERS, None).await?;
        pointer_changed(&world, "eth", Some(BEFORE), AFTER).await?;
        if let Err(error) = world
            .assert_selects(Call { root, ..CALL }, &UNDER_ETH)
            .await
        {
            failures.push(format!("eth pointer case {case}: {error:#}"));
        }
        world.cleanup().await?;
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// Row B. With no resolver below `eth`, every name of three or more labels is selected, and no
// second-level name is.
#[tokio::test]
async fn with_no_resolver_below_eth_no_second_level_name_is_selected() -> Result<()> {
    let world = tree(&[("c.foo.eth", ZERO_ADDR)], None).await?;
    pointer_changed(&world, "eth", Some(BEFORE), AFTER).await?;
    world
        .assert_selects(
            CALL,
            &[
                "eth",
                "a.foo.eth",
                "b.foo.eth",
                "c.foo.eth",
                "x.a.foo.eth",
                "z.bar.eth",
            ],
        )
        .await?;
    world.cleanup().await
}

// Rows D and J. A pointer change at `foo.eth`, to another resolver or as the node's first
// pointer row, selects `foo.eth` and the names under it without a resolver of their own.
// `a.foo.eth` has one. `x.a.foo.eth` has none and is selected.
#[tokio::test]
async fn a_second_level_pointer_change_skips_a_child_with_its_own_resolver() -> Result<()> {
    let mut failures = Vec::new();
    for (case, pointers, before) in [
        ("changed", &POINTERS[..], Some(BEFORE)),
        ("first row", &POINTERS[1..], None),
    ] {
        let world = tree(pointers, None).await?;
        pointer_changed(&world, "foo.eth", before, AFTER).await?;
        let expected = ["foo.eth", "b.foo.eth", "c.foo.eth", "x.a.foo.eth"];
        if let Err(error) = world.assert_selects(CALL, &expected).await {
            failures.push(format!("foo.eth pointer case {case}: {error:#}"));
        }
        world.cleanup().await?;
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// Rows F and F2, and the ordering rows of a batch. The selection reads every pointer as the
// publication left it. So a name below `eth` whose pointer the same publication changes is
// judged by its new pointer, and is selected anyway as a touched name. `x.a.foo.eth` comes in
// through `eth`, and through `a.foo.eth` when that change is pointer-only too.
#[tokio::test]
async fn a_pointer_change_below_eth_in_the_same_batch_is_judged_after_the_batch() -> Result<()> {
    let without_a = [("foo.eth", FOO_RESOLVER), ("c.foo.eth", ZERO_ADDR)];
    let without_foo = [("a.foo.eth", A_RESOLVER), ("c.foo.eth", ZERO_ADDR)];
    let a_changed: &[&str] = &[
        "eth",
        "a.foo.eth",
        "b.foo.eth",
        "c.foo.eth",
        "x.a.foo.eth",
        "z.bar.eth",
    ];
    let foo_changed: &[&str] = &[
        "eth",
        "foo.eth",
        "b.foo.eth",
        "c.foo.eth",
        "x.a.foo.eth",
        "z.bar.eth",
    ];
    let mut failures = Vec::new();
    for (case, pointers, changed, expected) in [
        (
            "a.foo.eth cleared",
            &POINTERS[..],
            ("a.foo.eth", Some(A_RESOLVER), ZERO_ADDR),
            a_changed,
        ),
        (
            "a.foo.eth set",
            &without_a[..],
            ("a.foo.eth", Some(ZERO_ADDR), SET),
            a_changed,
        ),
        (
            "a.foo.eth first row",
            &without_a[..],
            ("a.foo.eth", None, SET),
            a_changed,
        ),
        (
            "c.foo.eth first row, cleared",
            &[("foo.eth", FOO_RESOLVER), ("a.foo.eth", A_RESOLVER)][..],
            ("c.foo.eth", None, ZERO_ADDR),
            &UNDER_ETH[..],
        ),
        (
            "foo.eth set",
            &without_foo[..],
            ("foo.eth", Some(ZERO_ADDR), SET),
            foo_changed,
        ),
        (
            "foo.eth cleared",
            &POINTERS[..],
            ("foo.eth", Some(FOO_RESOLVER), ZERO_ADDR),
            foo_changed,
        ),
    ] {
        let world = tree(pointers, None).await?;
        pointer_changed(&world, "eth", Some(BEFORE), AFTER).await?;
        let (name, before, after) = changed;
        pointer_changed(&world, name, before, after).await?;
        if let Err(error) = world.assert_selects(CALL, expected).await {
            failures.push(format!("batch case {case}: {error:#}"));
        }
        world.cleanup().await?;
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// A name whose surface is anchored in the publication's window is a full prefix (see the module
// docs of the statement), so the names under it are selected whatever their pointers:
// `x.a.foo.eth` has a resolver and comes in through `a.foo.eth`. A name surfaced above the
// publication is never selected.
#[tokio::test]
async fn a_surface_in_the_window_and_one_above_the_block_are_read_as_surfaces() -> Result<()> {
    let mut pointers = POINTERS.to_vec();
    pointers.push(("x.a.foo.eth", SET));
    let mut world = tree(&pointers, Some("a.foo.eth")).await?;
    world.surface(Surface::of("y.foo.eth").block(9)).await?;
    pointer_changed(&world, "eth", Some(BEFORE), AFTER).await?;
    world
        .assert_selects(
            CALL,
            &[
                "eth",
                "a.foo.eth",
                "b.foo.eth",
                "c.foo.eth",
                "x.a.foo.eth",
                "z.bar.eth",
            ],
        )
        .await?;
    world.cleanup().await
}

// Row K. A pointer row whose resolver is empty is a clear, like the zero address.
#[tokio::test]
async fn an_empty_resolver_is_no_resolver_of_its_own() -> Result<()> {
    let world = tree(&[("b.foo.eth", ""), ("a.foo.eth", A_RESOLVER)], None).await?;
    pointer_changed(&world, "eth", Some(BEFORE), AFTER).await?;
    world.assert_selects(CALL, &UNDER_ETH).await?;
    world.cleanup().await
}

// A repeated pointer, as when the range set the resolver and set it back, is no change: `eth`
// recomposes alone.
#[tokio::test]
async fn a_pointer_set_back_to_its_resolver_selects_the_node_alone() -> Result<()> {
    let world = tree(&POINTERS, None).await?;
    pointer_changed(&world, "eth", Some(AFTER), AFTER).await?;
    world.assert_selects(CALL, &["eth"]).await?;
    world.cleanup().await
}

// Rows E and G. When `eth`'s pointer changes and another change in the publication names the
// same path, every `.eth` name is selected: a wrapper row, an owner getter class, a resource
// pointer, a reclassified resolver its pointer names, a surface in the window, or an ENSv2
// entry. An entry change of `foo` alone keeps the names under `foo.eth`, `a.foo.eth` included.
#[tokio::test]
async fn another_change_of_the_same_path_keeps_the_whole_subtree() -> Result<()> {
    const RESOURCE: &str = "00000000-0000-0000-0000-000000000003";
    let mut failures = Vec::new();
    for case in [
        "wrapper row",
        "owner getter class",
        "resource pointer",
        "reclassified resolver",
        "surface in the window",
        "eth entry",
        "foo entry alone",
    ] {
        let window = (case == "surface in the window").then_some("eth");
        let world = tree(&POINTERS, window).await?;
        world.eth().await?;
        world.mount(ETH, &labelhash("foo"), &registry(1)).await?;
        if case != "foo entry alone" {
            pointer_changed(&world, "eth", Some(BEFORE), AFTER).await?;
        }
        match case {
            "wrapper row" => {
                world
                    .wrapper_state_changed(
                        5,
                        RESOURCE,
                        None,
                        Some((
                            Some("eth"),
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
                        "eth",
                        None,
                        Some((Some(HOLDER_ADDR), Some(HOLDER_ADDR), None, false)),
                    )
                    .await?;
            }
            "resource pointer" => {
                world
                    .resource_pointer_changed(
                        5,
                        "eth",
                        RESOURCE,
                        SET,
                        SET,
                        "ResolverChanged",
                        json!({"block_number": 5, "event_identity": "set:eth"}),
                        json!({}),
                    )
                    .await?;
            }
            "reclassified resolver" => world.reclassify(AFTER).await?,
            "eth entry" => world.entry_changed(5, ROOT, &labelhash("eth")).await?,
            "foo entry alone" => world.entry_changed(5, ETH, &labelhash("foo")).await?,
            _ => {}
        }
        let expected: &[&str] = if case == "foo entry alone" {
            &[
                "foo.eth",
                "a.foo.eth",
                "b.foo.eth",
                "c.foo.eth",
                "x.a.foo.eth",
            ]
        } else {
            &NAMES
        };
        if let Err(error) = world.assert_selects(CALL, expected).await {
            failures.push(format!("other change case {case}: {error:#}"));
        }
        world.cleanup().await?;
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
