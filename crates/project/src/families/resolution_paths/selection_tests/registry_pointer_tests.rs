//! Which names an ENSv1 registry pointer change expands.
//!
//! The name summary and lookup name composition read an ancestor's registry pointer only in the
//! ENSv1 mirror walk, for a name below a `.eth` second-level name, and the walk stops at the
//! nearest pointer that is not cleared. So a pointer change expands the names of three or more labels under the changed
//! node that have no resolver of their own. A name whose path any other change also names keeps
//! its whole subtree.
//!
//! One ENSv1 `ResolverChanged` of a name with a resource writes the resource's pointer too.
//! That change counts as a registry pointer change while the row is an ENSv1 pointer before
//! and after it and names the same name. `an_ensv1_resource_pointer_change_counts_as_a_registry_pointer_change`
//! holds its input classes.
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

// Row D at three labels. A pointer-only change at `a.foo.eth`, with no change at `eth`, selects
// `a.foo.eth` and the names under it without a resolver of their own: `x.a.foo.eth`, and not
// `y.a.foo.eth`.
#[tokio::test]
async fn a_third_level_pointer_change_skips_a_child_with_its_own_resolver() -> Result<()> {
    let mut world = tree(&POINTERS, None).await?;
    world.surface(Surface::of("y.a.foo.eth")).await?;
    world.registry_pointer_at("y.a.foo.eth", SET).await?;
    pointer_changed(&world, "a.foo.eth", Some(A_RESOLVER), AFTER).await?;
    world
        .assert_selects(CALL, &["a.foo.eth", "x.a.foo.eth"])
        .await?;
    world.cleanup().await
}

// A narrow name whose surface has a null first label hash has no entry key to look its names
// up by, so it stays a full prefix and its whole subtree is selected: `x.null.foo.eth` has a
// resolver of its own and is selected.
#[tokio::test]
async fn a_pointer_change_on_a_null_first_label_keeps_the_whole_subtree() -> Result<()> {
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
    world.registry_pointer_at("x.null.foo.eth", SET).await?;
    pointer_changed(&world, "null.foo.eth", Some(BEFORE), AFTER).await?;
    world
        .assert_selects(CALL, &["null.foo.eth", "x.null.foo.eth"])
        .await?;
    world.cleanup().await
}

// A reclassified resolver makes the names whose pointers name it full prefixes, and only
// those. `a.foo.eth`'s resolver is reclassified in the block that changes `eth`'s pointer. The
// names under `a.foo.eth` are selected whatever their pointers, and `eth` stays narrow.
#[tokio::test]
async fn a_reclassified_resolver_widens_only_the_names_that_point_at_it() -> Result<()> {
    let mut world = tree(&POINTERS, None).await?;
    world.surface(Surface::of("y.a.foo.eth")).await?;
    world.registry_pointer_at("y.a.foo.eth", SET).await?;
    pointer_changed(&world, "eth", Some(BEFORE), AFTER).await?;
    world.reclassify(A_RESOLVER).await?;
    world
        .assert_selects(
            CALL,
            &[
                "eth",
                "a.foo.eth",
                "b.foo.eth",
                "c.foo.eth",
                "x.a.foo.eth",
                "y.a.foo.eth",
                "z.bar.eth",
            ],
        )
        .await?;
    world.cleanup().await
}

const V1: &str = "ens_v1_registry_l1";
const V2: &str = "ens_v2_registry_l1";
const RESOURCE: &str = "00000000-0000-0000-0000-0000000000f5";

/// The publication at block 5 wrote the resource pointer of `RESOURCE` as a `ResolverChanged`
/// of `family` naming `name`, at `resolver`. `before` is the row it replaced, or `None` for the
/// resource's first row.
async fn resource_pointer(
    world: &World,
    name: &str,
    family: &str,
    resolver: &str,
    before: Option<serde_json::Value>,
) -> Result<()> {
    let position = json!({"block_number": 5, "event_identity": format!("pointer:{name}")});
    sqlx::query(
        "INSERT INTO project_resource_pointer (chain_id, resource_id, block_number,
             event_identity, resolver_address, pointer_position, namespace, source_family,
             namehash, nonzero_resolver_address, nonzero_position, boundary_kind,
             boundary_position)
         VALUES ($1, $2::uuid, 5, 'pointer', $3, $4, 'ens', $5, $6, $3, $4, 'ResolverChanged', $4)",
    )
    .bind(CHAIN)
    .bind(RESOURCE)
    .bind(resolver)
    .bind(&position)
    .bind(family)
    .bind(node(name))
    .execute(&world.pool)
    .await?;
    world
        .journal(
            5,
            "project_resource_pointer",
            json!([CHAIN, RESOURCE]),
            before,
        )
        .await
}

/// The row a resource pointer change replaced.
#[derive(Clone, Copy)]
enum Before {
    /// No row: the resource's first.
    None,
    /// A version boundary alone, with no pointer.
    Boundary,
    /// A pointer of `family` naming `name`.
    Pointer(&'static str, &'static str),
}

impl Before {
    fn image(self) -> Option<serde_json::Value> {
        match self {
            Self::None => None,
            Self::Boundary => Some(json!({
                "resolver_address": null, "nonzero_resolver_address": null, "namespace": null,
                "namehash": null, "source_family": null, "boundary_kind": "RecordVersionChanged"
            })),
            Self::Pointer(family, name) => Some(json!({
                "resolver_address": FOO_RESOLVER, "nonzero_resolver_address": FOO_RESOLVER,
                "namespace": "ens", "namehash": node(name), "source_family": family,
                "boundary_kind": "ResolverChanged"
            })),
        }
    }
}

// The input classes of an ENSv1 resource pointer change at `foo.eth`. It selects what a
// registry pointer change there selects, `foo.eth` and the names under it without a resolver
// of their own, when the row was an ENSv1 pointer of `foo.eth` or no pointer at all. With an
// ENSv2 row on either side, a move from another name, or another change of the same name, the
// whole subtree is selected. A pointer change at `eth` in the same publication adds its own
// readers. Every case but the first changes `foo.eth`'s registry pointer too, as the one event
// does.
#[tokio::test]
async fn an_ensv1_resource_pointer_change_counts_as_a_registry_pointer_change() -> Result<()> {
    const READERS: &[&str] = &["foo.eth", "b.foo.eth", "c.foo.eth", "x.a.foo.eth"];
    const SUBTREE: &[&str] = &[
        "foo.eth",
        "a.foo.eth",
        "b.foo.eth",
        "c.foo.eth",
        "x.a.foo.eth",
    ];
    const MOVED: &[&str] = &[
        "foo.eth",
        "a.foo.eth",
        "b.foo.eth",
        "c.foo.eth",
        "x.a.foo.eth",
        "bar.eth",
        "z.bar.eth",
    ];
    const WITH_ETH: &[&str] = &[
        "eth",
        "foo.eth",
        "b.foo.eth",
        "c.foo.eth",
        "x.a.foo.eth",
        "z.bar.eth",
    ];
    let foo = |family| Before::Pointer(family, "foo.eth");
    let bar = Before::Pointer(V1, "bar.eth");
    let cases: [(&str, &str, Before, &[&str]); 10] = [
        ("ENSv1 row alone", V1, foo(V1), READERS),
        ("ENSv1 row", V1, foo(V1), READERS),
        ("ENSv1 first row", V1, Before::None, READERS),
        ("ENSv1 row over a boundary", V1, Before::Boundary, READERS),
        ("ENSv2 row", V2, foo(V2), SUBTREE),
        ("ENSv2 first row", V2, Before::None, SUBTREE),
        ("ENSv1 row over an ENSv2 row", V1, foo(V2), SUBTREE),
        ("ENSv1 row moved from bar.eth", V1, bar, MOVED),
        ("with an owner getter class change", V1, foo(V1), SUBTREE),
        ("with an eth pointer change", V1, foo(V1), WITH_ETH),
    ];
    let mut failures = Vec::new();
    for (case, family, before, expected) in cases {
        let world = tree(&POINTERS, None).await?;
        resource_pointer(&world, "foo.eth", family, AFTER, before.image()).await?;
        if case != "ENSv1 row alone" {
            pointer_changed(&world, "foo.eth", Some(FOO_RESOLVER), AFTER).await?;
        }
        match case {
            "with an owner getter class change" => {
                world
                    .registry_node_state_changed(
                        5,
                        "foo.eth",
                        None,
                        Some((Some(HOLDER_ADDR), Some(HOLDER_ADDR), None, false)),
                    )
                    .await?;
            }
            "with an eth pointer change" => {
                pointer_changed(&world, "eth", Some(BEFORE), AFTER).await?;
            }
            _ => {}
        }
        if let Err(error) = world.assert_selects(CALL, expected).await {
            failures.push(format!("resource pointer case {case}: {error:#}"));
        }
        world.cleanup().await?;
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
