//! Selection versus eligibility on the mirror's registry walk. The walk selects the nearest
//! consulted ENSv1 node with a nonzero resolver; the mirror then keeps a resolver found above the
//! queried node only when it is an ENSIP-10 extended resolver, and answers with no resolver
//! otherwise. A rejected nearest ancestor does not continue the walk to a farther resolver.
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/resolver/ENSV1Resolver.sol:L40-L43 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/libraries/LibResolution.sol:L39-L48 @ ens_v2_sepolia_20260916@366de741)
use super::*;

const LEAF: &str = "leaf.mirror.fixture";
const LEAF_RESOURCE: &str = "69100000-0000-0000-0000-000000000005";
const LEAF_BINDING: &str = "69100000-0000-0000-0000-000000000105";

/// `leaf.mirror.fixture` points at the mirror. Its nearest ENSv1 resolver is `mirror.fixture`'s
/// declared, non-extended resolver one label up; `fixture`, two labels up, has a declared extended
/// resolver. The mirror rejects the nearer one and does not look further, so the row records the
/// nearer node with `ancestor_resolver_not_extended`, not the farther extended one.
#[tokio::test]
async fn rejected_nearest_ancestor_does_not_continue_the_walk() -> Result<()> {
    let fixture = Fixture::declared("mirror_gate_nearest", V1Side::Projected)
        .with_ancestor(Ancestor::Extended);
    let (database, pool) = database(fixture.id).await?;
    seed(&pool, &fixture).await?;
    add_mirror_name(&pool, &fixture, LEAF, LEAF_RESOURCE, LEAF_BINDING).await?;
    let leaf_node = bigname_lookup::ens_namehash_hex(LEAF)?;
    // The non-extended resolver stores a record for the leaf node, which a direct getter call
    // would read if the mirror kept that resolver.
    let leaf_write = insert_event(
        &pool,
        &fixture,
        record(
            "leaf-text-on-nearer-resolver",
            fixture.base + 1,
            9,
            &leaf_node,
            V1_RESOLVER,
            manifest_id(&pool, "ens_v1_resolver_l1").await?,
            json!({"record_key": "text:description", "record_family": "text",
                   "selector_key": "description", "source_event": "TextChanged",
                   "value": "leaf on the nearer resolver"}),
        ),
    )
    .await?;
    run(
        &pool,
        fixture.target(),
        0,
        fixture.target(),
        None,
        RunMode::Normal,
    )
    .await?;

    let leaf = inventory(&pool, LEAF_RESOURCE).await?;
    assert_unsupported_mirror_row(&leaf);
    let mirror = &leaf["provenance"]["mirror"];
    assert_eq!(mirror["queried_node"], leaf_node, "{leaf}");
    assert_eq!(
        mirror["mirrored_node"],
        bigname_lookup::ens_namehash_hex(NAME)?
    );
    assert_eq!(mirror["mirrored_name"], NAME);
    assert_eq!(mirror["ancestor_depth"], 1);
    assert_eq!(mirror["mirrored_resolver_address"], V1_RESOLVER);
    assert_eq!(mirror["mirrored_resource_id"], V1_RESOURCE);
    assert_eq!(
        mirror["mirrored_pointer_event_id"],
        event_id(&pool, &fixture, "v1-pointer").await?
    );
    assert_eq!(mirror["forwarding"], "direct_call");
    assert_eq!(
        mirror["mirrored_unsupported_reason"],
        "ancestor_resolver_not_extended"
    );
    assert!(
        history_attribution(&pool, LEAF_RESOURCE, fixture.target())
            .await?
            .is_empty(),
        "history must not follow the rejected ancestor to write {leaf_write}"
    );

    // The same non-extended resolver on the exact node is derived through.
    let exact = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(exact["support_status"], "supported", "{exact}");
    assert_eq!(exact["provenance"]["mirror"]["ancestor_depth"], 0);
    assert_eq!(
        exact["provenance"]["mirror"]["mirrored_resolver_address"],
        V1_RESOLVER
    );
    assert_eq!(exact["provenance"]["mirror"]["forwarding"], "direct_call");
    database.cleanup().await?;
    Ok(())
}

/// The exact node's resolver is derived through whatever its declared read features say; only an
/// ancestor selection depends on `ensip10_extended_resolver`.
#[tokio::test]
async fn exact_node_extended_resolver_is_still_derived() -> Result<()> {
    let fixture = Fixture::declared(
        "mirror_gate_exact_extended",
        V1Side::NodeOnly {
            resolver: PARENT_RESOLVER,
            pointer_block_offset: 0,
        },
    )
    .with_ancestor(Ancestor::Extended);
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v2["support_status"], "supported", "{v2}");
    let mirror = &v2["provenance"]["mirror"];
    assert_eq!(mirror["ancestor_depth"], 0, "{v2}");
    assert_eq!(mirror["mirrored_resolver_address"], PARENT_RESOLVER);
    assert_eq!(mirror["forwarding"], "extended_resolve");
    assert!(mirror.get("mirrored_unsupported_reason").is_none(), "{v2}");
    assert_eq!(v2["entries"][0]["record_key"], "text:description");
    database.cleanup().await?;
    Ok(())
}

/// A selected ancestor whose resolver fails an earlier classification check keeps that reason:
/// the two ancestor reasons only distinguish otherwise eligible ancestor selections.
#[tokio::test]
async fn classification_reasons_win_over_the_ancestor_reasons() -> Result<()> {
    for (id, resolver, expected) in [
        (
            "mirror_gate_undeclared_ancestor",
            UNDECLARED_RESOLVER,
            "resolver_not_declared",
        ),
        (
            "mirror_gate_mirror_ancestor",
            MIRROR,
            "mirrored_resolver_is_mirror",
        ),
    ] {
        let fixture = Fixture::declared(id, V1Side::Absent);
        let (database, pool) = database(fixture.id).await?;
        seed(&pool, &fixture).await?;
        insert_event(
            &pool,
            &fixture,
            Event {
                identity: "ancestor-node-pointer",
                logical_name_id: None,
                resource_id: None,
                kind: "ResolverChanged",
                source_family: "ens_v1_registry_l1",
                manifest_id: None,
                block: fixture.base,
                log_index: 2,
                emitter: V1_REGISTRY,
                after_state: json!({
                    "node": bigname_lookup::ens_namehash_hex(PARENT_NAME)?,
                    "resolver": resolver
                }),
            },
        )
        .await?;
        run(
            &pool,
            fixture.target(),
            0,
            fixture.target(),
            None,
            RunMode::Normal,
        )
        .await?;
        let v2 = inventory(&pool, V2_RESOURCE).await?;
        assert_unsupported_mirror_row(&v2);
        let mirror = &v2["provenance"]["mirror"];
        assert_eq!(mirror["ancestor_depth"], 1, "{id}: {v2}");
        assert_eq!(mirror["mirrored_resolver_address"], resolver, "{id}");
        assert_eq!(
            mirror["mirrored_unsupported_reason"], expected,
            "{id}: {v2}"
        );
        database.cleanup().await?;
    }
    Ok(())
}

/// The whole unsupported mirror row: no record values, no read rules and no attributed events,
/// while `provenance.resolver_address` stays the name's own mirror pointer.
pub(super) fn assert_unsupported_mirror_row(row: &Value) {
    assert_eq!(row["support_status"], "unsupported", "{row}");
    assert_eq!(row["unsupported_reason"], "mirrored_resolver_not_projected");
    assert_eq!(
        row["unsupported_families"],
        json!([{
            "record_family": "resolver_classification",
            "unsupported_reason": "mirrored_resolver_not_projected"
        }]),
        "{row}"
    );
    assert_eq!(row["entries"], json!([]), "{row}");
    assert_eq!(row["selectors"], json!([]), "{row}");
    let provenance = &row["provenance"];
    assert_eq!(provenance["resolver_address"], MIRROR, "{row}");
    for field in [
        "record_event_ids",
        "record_link_event_ids",
        "attributed_event_ids",
        "read_rules",
    ] {
        assert_eq!(provenance[field], json!([]), "{field}: {row}");
    }
    assert_eq!(
        row["record_version_boundary"]["normalized_event_id"],
        Value::Null
    );
    assert_eq!(row["last_change"]["event_kind"], "ResolverChanged");
}
