//! The ENSv1 registry pointer the mirror walk reads is keyed by the name the event addresses, in
//! the adapters' shared order `child_node`, then `namehash`, then `node`
//! (`crates/adapters/src/schema_v2/seam.rs`, `V1_EVENT_NODE_FIELDS`). A state-derived
//! `ResolverChanged` for a newly linked child keeps the `NewOwner` observation, whose `node` is
//! the parent and whose `child_node` is the child
//! (`crates/adapters/src/schema_v2/protocol/v1/authority_transition.rs`), so reading `node` alone
//! would file the child's pointer, and its clear, under the parent.
use super::ancestor_gate::assert_unsupported_mirror_row;
use super::*;

const SIBLING: &str = "sibling.fixture";
const SIBLING_RESOURCE: &str = "69100000-0000-0000-0000-000000000004";
const SIBLING_BINDING: &str = "69100000-0000-0000-0000-000000000104";

/// Blocks, with `base = 10`:
/// - 10: the registry points `fixture` at `PARENT_RESOLVER` (a genuine `NewResolver`, keyed by
///   `node`); `mirror.fixture` and `sibling.fixture` point at the mirror in ENSv2.
/// - 11: a derived child pointer for `mirror.fixture` at `V1_RESOLVER`, carrying `node = fixture`
///   and `child_node = mirror.fixture`, and a `V1_RESOLVER` record for `mirror.fixture`.
/// - 12: the same derived shape clears `mirror.fixture`.
/// - 13: a wrapper pointer carrying only `namehash = mirror.fixture` points it at `V1_RESOLVER`.
async fn seed_identity(pool: &PgPool, fixture: &Fixture) -> Result<()> {
    seed(pool, fixture).await?;
    add_mirror_name(pool, fixture, SIBLING, SIBLING_RESOURCE, SIBLING_BINDING).await?;
    let node = bigname_lookup::ens_namehash_hex(NAME)?;
    let parent_node = bigname_lookup::ens_namehash_hex(PARENT_NAME)?;
    let logical_name_id = format!("ens:{node}");
    for (identity, block, resolver) in [
        ("derived-child-pointer", fixture.base + 1, V1_RESOLVER),
        ("derived-child-clear", fixture.base + 2, ZERO20),
    ] {
        insert_event(
            pool,
            fixture,
            Event {
                identity,
                logical_name_id: Some(logical_name_id.clone()),
                resource_id: Some(V1_RESOURCE),
                kind: "ResolverChanged",
                source_family: "ens_v1_registry_l1",
                manifest_id: None,
                block,
                log_index: 5,
                emitter: V1_REGISTRY,
                after_state: json!({
                    "source_event": "AuthorityEpochChanged",
                    "node": parent_node,
                    "child_node": node,
                    "label": "mirror",
                    "resolver": resolver,
                    "resolver_source_role": "registry"
                }),
            },
        )
        .await?;
    }
    insert_event(
        pool,
        fixture,
        Event {
            identity: "namehash-only-pointer",
            logical_name_id: None,
            resource_id: None,
            kind: "ResolverChanged",
            source_family: "ens_v1_wrapper_l1",
            manifest_id: None,
            block: fixture.base + 3,
            log_index: 5,
            emitter: V1_REGISTRY,
            after_state: json!({"namehash": node, "resolver": V1_RESOLVER}),
        },
    )
    .await?;
    insert_event(
        pool,
        fixture,
        record(
            "child-text-on-v1",
            fixture.base + 1,
            6,
            &node,
            V1_RESOLVER,
            manifest_id(pool, "ens_v1_resolver_l1").await?,
            json!({"record_key": "text:url", "record_family": "text", "selector_key": "url",
                   "source_event": "TextChanged", "value": "https://child.example"}),
        ),
    )
    .await?;
    Ok(())
}

async fn assert_state(pool: &PgPool, fixture: &Fixture, target: i64, label: &str) -> Result<()> {
    let node = bigname_lookup::ens_namehash_hex(NAME)?;
    let parent_node = bigname_lookup::ens_namehash_hex(PARENT_NAME)?;
    let parent_pointer = event_id(pool, fixture, "parent-pointer").await?;

    // The sibling's walk consults `sibling.fixture` and `fixture`. The child's pointer events never
    // move the parent's registry resolver, so the sibling always selects the genuine parent pointer.
    let sibling = inventory(pool, SIBLING_RESOURCE).await?;
    assert_unsupported_mirror_row(&sibling);
    let mirror = &sibling["provenance"]["mirror"];
    assert_eq!(mirror["mirrored_node"], parent_node, "{label}: {sibling}");
    assert_eq!(
        mirror["mirrored_resolver_address"], PARENT_RESOLVER,
        "{label}"
    );
    assert_eq!(
        mirror["mirrored_pointer_event_id"], parent_pointer,
        "{label}"
    );
    assert_eq!(
        mirror["mirrored_unsupported_reason"],
        "ancestor_resolver_not_extended"
    );

    let child = inventory(pool, V2_RESOURCE).await?;
    let history = history_attribution(pool, V2_RESOURCE, target).await?;
    if target == fixture.base + 2 {
        // The derived clear clears the child, which falls through to the parent's pointer.
        assert_unsupported_mirror_row(&child);
        let mirror = &child["provenance"]["mirror"];
        assert_eq!(mirror["mirrored_node"], parent_node, "{label}: {child}");
        assert_eq!(mirror["mirrored_pointer_event_id"], parent_pointer);
        assert!(history.is_empty(), "{label}: {history:?}");
        return Ok(());
    }
    let expected_pointer = if target == fixture.base + 1 {
        "derived-child-pointer"
    } else {
        "namehash-only-pointer"
    };
    assert_eq!(child["support_status"], "supported", "{label}: {child}");
    let mirror = &child["provenance"]["mirror"];
    assert_eq!(mirror["ancestor_depth"], 0, "{label}: {child}");
    assert_eq!(mirror["mirrored_node"], node);
    assert_eq!(mirror["mirrored_resolver_address"], V1_RESOLVER);
    assert_eq!(
        mirror["mirrored_pointer_event_id"],
        event_id(pool, fixture, expected_pointer).await?,
        "{label}"
    );
    assert_eq!(child["entries"][0]["value"], "https://child.example");
    // A valid derived child pointer stays followable for history.
    assert_eq!(
        history,
        std::collections::BTreeSet::from([event_id(pool, fixture, "child-text-on-v1").await?]),
        "{label}"
    );
    Ok(())
}

#[tokio::test]
async fn derived_child_pointer_is_keyed_to_the_child_node() -> Result<()> {
    let fixture = Fixture::declared("mirror_pointer_identity", V1Side::Absent).with_ancestor(
        Ancestor::Direct {
            pointer_block_offset: 0,
        },
    );
    let checkpoints = [fixture.base + 1, fixture.base + 2, fixture.target()];
    let mut full = Vec::new();
    for target in checkpoints {
        let (database, pool) = database(&format!("{}_full_{target}", fixture.id)).await?;
        seed_identity(&pool, &fixture).await?;
        run(&pool, target, 0, target, None, RunMode::Normal).await?;
        assert_state(&pool, &fixture, target, &format!("full {target}")).await?;
        full.push(content(inventory(&pool, V2_RESOURCE).await?));
        database.cleanup().await?;
    }

    let (database, pool) = database(&format!("{}_incremental", fixture.id)).await?;
    seed_identity(&pool, &fixture).await?;
    run(&pool, fixture.base, 0, fixture.base, None, RunMode::Normal).await?;
    let mut sibling_after_first = None;
    for (index, block) in checkpoints.into_iter().enumerate() {
        run(&pool, block, block, block, Some(block - 1), RunMode::Normal).await?;
        assert_state(&pool, &fixture, block, &format!("incremental {block}")).await?;
        assert_eq!(
            content(inventory(&pool, V2_RESOURCE).await?),
            full[index],
            "incremental {block} drifted from the full rebuild"
        );
        // Only the child's node changed after the first checkpoint, so the sibling, which consults
        // the parent but not the child, is never rebuilt: it keeps every field, including its
        // publication target.
        let sibling = inventory(&pool, SIBLING_RESOURCE).await?;
        match &sibling_after_first {
            None => sibling_after_first = Some(sibling),
            Some(previous) => assert_eq!(&sibling, previous, "sibling rebuilt at {block}"),
        }
    }
    run(
        &pool,
        fixture.target(),
        fixture.target(),
        fixture.target(),
        Some(fixture.target()),
        RunMode::Redo,
    )
    .await?;
    assert_state(&pool, &fixture, fixture.target(), "redo").await?;
    assert_eq!(
        content(inventory(&pool, V2_RESOURCE).await?),
        full[2],
        "redo"
    );
    database.cleanup().await?;
    Ok(())
}
