//! A name whose current ENSv2 resolver is a declared ENSv1 mirror resolver serves the records the
//! ENSv1 resolver the mirror finds for it stores for the queried node: the exact node's registry
//! resolver first, else the nearest ancestor's (`docs/projections.md` § Resolver and records,
//! ENSv1 mirror resolver).

use anyhow::{Context, Result};
use bigname_project::{BatchOutcome, BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const V1_REGISTRY: &str = "0x4444444444444444444444444444444444444401";
const V1_RESOLVER: &str = "0x1111111111111111111111111111111111111111";
const PARENT_RESOLVER: &str = "0x3333333333333333333333333333333333333333";
const UNDECLARED_RESOLVER: &str = "0x4848484848484848484848484848484848484848";
const MIRROR: &str = "0x1010101010101010101010101010101010101010";
const ZERO20: &str = "0x0000000000000000000000000000000000000000";
const ADDRESS: &str = "0x2222222222222222222222222222222222222222";
const V1_RESOURCE: &str = "69100000-0000-0000-0000-000000000001";
const V2_RESOURCE: &str = "69100000-0000-0000-0000-000000000002";
const V1_BINDING: &str = "69100000-0000-0000-0000-000000000101";
const V2_BINDING: &str = "69100000-0000-0000-0000-000000000102";
const PARENT_V1_RESOURCE: &str = "69100000-0000-0000-0000-000000000003";
const PARENT_V1_BINDING: &str = "69100000-0000-0000-0000-000000000103";
const ROOT_INSTANCE: &str = "69100000-0000-0000-0000-000000000201";
const MIRROR_INSTANCE: &str = "69100000-0000-0000-0000-000000000202";
const NAME: &str = "mirror.fixture";
const PARENT_NAME: &str = "fixture";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum V1Side {
    /// The ENSv1 registry points the same name at a declared ENSv1 resolver with records.
    Projected,
    /// The ENSv1 registry never selected a resolver for the name.
    Absent,
    /// The ENSv1 registry cleared its resolver at block `base + 2`.
    Cleared,
    /// The ENSv1 registry points the queried node at `resolver` from `base + pointer_block_offset`
    /// through a pointer event with no logical name and no resource (a pre-surface pointer for a
    /// node nobody owns in ENSv1); when `resolver` is the declared `V1_RESOLVER`, that resolver
    /// stores a `text:url` for the node from `base + 1`.
    NodeOnly {
        resolver: &'static str,
        pointer_block_offset: i64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ancestor {
    /// The parent name has no ENSv1 resolver either.
    None,
    /// The ENSv1 registry points the parent at a declared immediate resolver from this block; that
    /// resolver stores a `text:description` for the child node and a `text:url` for the parent
    /// node from block `base + 1`.
    Direct { pointer_block_offset: i64 },
    /// As `Direct` at `base`, but the parent's resolver is declared `ensip10_extended_resolver`.
    Extended,
}

#[derive(Clone, Debug)]
struct Fixture {
    id: &'static str,
    base: i64,
    mirror: &'static str,
    v2_payload: Option<Value>,
    v1_side: V1Side,
    ancestor: Ancestor,
    /// The name whose ENSv2 pointer targets the mirror: `NAME` or the single-label `PARENT_NAME`.
    queried: &'static str,
    /// Whether the queried name carries a surface binding of any arm. A root-registry TLD token
    /// whose registration was never observed has a resource and a pointer but no binding.
    queried_bound: bool,
    /// Root-registry lifecycle facts on the ENSv2 resource, in addition to its pointer.
    v2_lifecycle: V2Lifecycle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum V2Lifecycle {
    None,
    /// The label is reserved (owner zero) with the pointer supplied at reservation, `base`.
    Reserved,
    /// The token's path expired at `base + 2`: the interpreter releases the registration and
    /// clears the pointer through state-derived events that name the resource but no logical name.
    Expired,
}

impl Fixture {
    fn declared(id: &'static str, v1_side: V1Side) -> Self {
        Self {
            id,
            base: 10,
            mirror: MIRROR,
            v2_payload: None,
            v1_side,
            ancestor: Ancestor::None,
            queried: NAME,
            queried_bound: true,
            v2_lifecycle: V2Lifecycle::None,
        }
    }
    fn with_ancestor(mut self, ancestor: Ancestor) -> Self {
        self.ancestor = ancestor;
        self
    }
    fn single_label(mut self) -> Self {
        self.queried = PARENT_NAME;
        self
    }
    fn unbound(mut self) -> Self {
        self.queried_bound = false;
        self
    }
    fn with_v2_lifecycle(mut self, lifecycle: V2Lifecycle) -> Self {
        self.v2_lifecycle = lifecycle;
        self
    }
    fn parent_pointer_block(&self) -> Option<i64> {
        match self.ancestor {
            Ancestor::None => None,
            Ancestor::Direct {
                pointer_block_offset,
            } => Some(self.base + pointer_block_offset),
            Ancestor::Extended => Some(self.base),
        }
    }
    fn target(&self) -> i64 {
        self.base + 3
    }
    fn v2_payload(&self) -> Value {
        self.v2_payload.clone().unwrap_or_else(|| {
            json!({
                "deployment_epoch": "fixture",
                "correlation_addresses": {"ens_v1_registry": V1_REGISTRY},
                "contracts": [{
                    "role": "ensv1_mirror_resolver", "address": self.mirror,
                    "proxy_kind": "none", "start_block": 0
                }],
                "capability_flags": {}
            })
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum Execution {
    FromZero,
    PerBlock,
    TwoByTwo,
    Idempotent,
    RedoLastBlock,
}

#[tokio::test]
async fn mirrored_name_serves_the_ensv1_inventory_with_mirror_provenance() -> Result<()> {
    let fixture = Fixture::declared("mirror_projected", V1Side::Projected);
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let v1 = inventory(&pool, V1_RESOURCE).await?;
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v1["support_status"], "supported", "{v1}");
    assert_eq!(v2["support_status"], "supported", "{v2}");
    assert_eq!(v2["unsupported_reason"], Value::Null);
    assert_eq!(v2["entries"], v1["entries"], "{v2}");
    assert_eq!(v2["selectors"], v1["selectors"]);
    assert_eq!(v2["last_change"], v1["last_change"]);
    assert_eq!(v2["unsupported_families"], json!([]));
    let keys: Vec<_> = v2["entries"]
        .as_array()
        .context("entries")?
        .iter()
        .map(|entry| entry["record_key"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(keys, ["addr:60", "text:url"]);
    assert_eq!(v2["provenance"]["resolver_address"], MIRROR);
    assert_eq!(
        v2["provenance"]["read_rules"],
        json!([{"kind": "ensip19_default_address", "source_record_key": "addr:2147483648"}])
    );
    assert_eq!(
        v2["provenance"]["record_event_ids"],
        v1["provenance"]["record_event_ids"]
    );
    assert_eq!(
        v2["provenance"]["attributed_event_ids"],
        v1["provenance"]["attributed_event_ids"]
    );
    let v1_pointer = v1["provenance"]["resolver_pointer_event_id"].clone();
    let node = bigname_lookup::ens_namehash_hex(NAME)?;
    assert_eq!(
        v2["provenance"]["mirror"],
        json!({
            "resolver_address": MIRROR,
            "mirrored_source_family": "ens_v1_resolver_l1",
            "mirrored_registry_source_family": "ens_v1_registry_l1",
            "mirrored_registry_address": V1_REGISTRY,
            "queried_node": node,
            "mirrored_node": node,
            "mirrored_name": NAME,
            "ancestor_depth": 0,
            "forwarding": "direct_call",
            "mirrored_resolver_address": V1_RESOLVER,
            "mirrored_resource_id": V1_RESOURCE,
            "mirrored_pointer_event_id": v1_pointer,
            "mirrored_pointer_source_family": "ens_v1_registry_l1"
        }),
        "{v2}"
    );
    assert_ne!(
        v2["provenance"]["resolver_pointer_event_id"],
        v1["provenance"]["resolver_pointer_event_id"]
    );
    assert_eq!(v2["record_version_boundary"]["resource_id"], V2_RESOURCE);
    assert_eq!(
        v2["record_version_boundary"]["chain_position"],
        v1["record_version_boundary"]["chain_position"]
    );
    assert_eq!(
        v2["record_version_boundary_key"],
        boundary_key(&v2["record_version_boundary"], CHAIN)
    );
    assert_eq!(
        v2["chain_positions"]["block_number"],
        json!(fixture.base + 3)
    );

    let resolver = resolver_current(&pool, MIRROR).await?;
    assert_eq!(resolver["support_status"], "supported", "{resolver}");
    assert_eq!(
        resolver["declared_summary"]["classification"],
        json!({
            "source_family": "ens_v2_resolver_l1",
            "role": "ensv1_mirror_resolver",
            "basis": "manifest_declared_address",
            "read_features": [],
            "mirror": {
                "mirrored_source_family": "ens_v1_resolver_l1",
                "mirrored_registry_source_family": "ens_v1_registry_l1",
                "mirrored_registry_address": V1_REGISTRY
            }
        }),
        "{resolver}"
    );
    assert_eq!(
        resolver["declared_summary"]["bindings"]["status"],
        "supported"
    );
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn mirrored_inventory_converges_across_incremental_and_redo_execution() -> Result<()> {
    let fixture = Fixture::declared("mirror_replay", V1Side::Projected);
    let mut previous: Option<(Value, Value)> = None;
    for execution in [
        Execution::FromZero,
        Execution::PerBlock,
        Execution::TwoByTwo,
        Execution::Idempotent,
        Execution::RedoLastBlock,
    ] {
        let (database, pool) = project(&fixture, fixture.target(), execution).await?;
        let rows = (
            inventory(&pool, V2_RESOURCE).await?,
            resolver_current(&pool, MIRROR).await?,
        );
        assert_eq!(
            rows.0["support_status"], "supported",
            "{execution:?}: {}",
            rows.0
        );
        // The ENSv1 write at base + 3 must have rebuilt the mirrored row, not only the ENSv1 row.
        assert_eq!(
            rows.0["last_change"]["chain_position"]["block_number"],
            json!(fixture.base + 3),
            "{execution:?}: {}",
            rows.0
        );
        if let Some(previous) = &previous {
            assert_eq!(&rows, previous, "{execution:?} drifted");
        }
        previous = Some(rows);
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn mirror_without_a_projected_ensv1_resolver_is_explicitly_unsupported() -> Result<()> {
    for (id, v1_side, target_offset, expected_mirrored_resolver) in [
        ("mirror_absent", V1Side::Absent, 3, None),
        ("mirror_cleared", V1Side::Cleared, 3, None),
        ("mirror_before_clear", V1Side::Cleared, 1, Some(V1_RESOLVER)),
    ] {
        let fixture = Fixture::declared(id, v1_side);
        let node = bigname_lookup::ens_namehash_hex(NAME)?;
        let (database, pool) =
            project(&fixture, fixture.base + target_offset, Execution::FromZero).await?;
        let v2 = inventory(&pool, V2_RESOURCE).await?;
        if let Some(resolver) = expected_mirrored_resolver {
            assert_eq!(v2["support_status"], "supported", "{id}: {v2}");
            assert_eq!(
                v2["provenance"]["mirror"]["mirrored_resolver_address"],
                resolver
            );
            database.cleanup().await?;
            continue;
        }
        assert_eq!(v2["support_status"], "unsupported", "{id}: {v2}");
        assert_eq!(v2["unsupported_reason"], "mirrored_resolver_not_projected");
        assert_eq!(
            v2["unsupported_families"],
            json!([{
                "record_family": "resolver_classification",
                "unsupported_reason": "mirrored_resolver_not_projected"
            }])
        );
        assert_eq!(v2["entries"], json!([]));
        assert_eq!(v2["selectors"], json!([]));
        assert_eq!(v2["provenance"]["resolver_address"], MIRROR);
        assert_eq!(v2["provenance"]["read_rules"], json!([]));
        assert_eq!(
            v2["provenance"]["mirror"],
            json!({
                "resolver_address": MIRROR,
                "mirrored_source_family": "ens_v1_resolver_l1",
                "mirrored_registry_source_family": "ens_v1_registry_l1",
                "mirrored_registry_address": V1_REGISTRY,
                "queried_node": node
            }),
            "{id}: {v2}"
        );
        assert_eq!(
            v2["record_version_boundary"]["normalized_event_id"],
            Value::Null
        );
        assert_eq!(v2["record_version_boundary"]["event_kind"], Value::Null);
        assert_eq!(
            v2["record_version_boundary"]["chain_position"]["block_number"],
            json!(fixture.base)
        );
        assert_eq!(
            v2["record_version_boundary_key"],
            boundary_key(&v2["record_version_boundary"], CHAIN)
        );
        assert_eq!(v2["last_change"]["event_kind"], "ResolverChanged");
        let resolver = resolver_current(&pool, MIRROR).await?;
        assert_eq!(resolver["support_status"], "supported", "{id}: {resolver}");
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn mirror_walks_to_the_nearest_ancestor_resolver_and_reads_the_queried_node() -> Result<()> {
    let fixture =
        Fixture::declared("mirror_ancestor", V1Side::Absent).with_ancestor(Ancestor::Direct {
            pointer_block_offset: 0,
        });
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    let parent = inventory(&pool, PARENT_V1_RESOURCE).await?;
    assert_eq!(v2["support_status"], "supported", "{v2}");
    assert_eq!(v2["unsupported_reason"], Value::Null);
    // Only the parent's resolver storage for the queried (child) node is served: not the
    // parent's own records, and not the child's records on a resolver the walk did not select.
    let keys: Vec<_> = v2["entries"]
        .as_array()
        .context("entries")?
        .iter()
        .map(|entry| entry["record_key"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(keys, ["text:description"], "{v2}");
    assert_eq!(v2["entries"][0]["value"], "child on parent resolver");
    let parent_keys: Vec<_> = parent["entries"]
        .as_array()
        .context("parent entries")?
        .iter()
        .map(|entry| entry["record_key"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(parent_keys, ["text:url"], "{parent}");
    assert_eq!(v2["provenance"]["resolver_address"], MIRROR);
    assert_eq!(v2["provenance"]["read_rules"], json!([]));
    let node = bigname_lookup::ens_namehash_hex(NAME)?;
    let parent_node = bigname_lookup::ens_namehash_hex(PARENT_NAME)?;
    assert_eq!(
        v2["provenance"]["mirror"],
        json!({
            "resolver_address": MIRROR,
            "mirrored_source_family": "ens_v1_resolver_l1",
            "mirrored_registry_source_family": "ens_v1_registry_l1",
            "mirrored_registry_address": V1_REGISTRY,
            "queried_node": node,
            "mirrored_node": parent_node,
            "mirrored_name": PARENT_NAME,
            "ancestor_depth": 1,
            "forwarding": "direct_call",
            "mirrored_resolver_address": PARENT_RESOLVER,
            "mirrored_resource_id": PARENT_V1_RESOURCE,
            "mirrored_pointer_event_id": parent["provenance"]["resolver_pointer_event_id"],
            "mirrored_pointer_source_family": "ens_v1_registry_l1"
        }),
        "{v2}"
    );
    assert_eq!(v2["record_version_boundary"]["resource_id"], V2_RESOURCE);
    assert_eq!(
        v2["record_version_boundary_key"],
        boundary_key(&v2["record_version_boundary"], CHAIN)
    );
    database.cleanup().await?;

    // Before any write for the child node, the ancestor's resolver answers empty: supported, no
    // entries, exactly as the chain would answer the direct getter call.
    let (database, pool) = project(&fixture, fixture.base, Execution::FromZero).await?;
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v2["support_status"], "supported", "{v2}");
    assert_eq!(v2["entries"], json!([]));
    assert_eq!(v2["provenance"]["mirror"]["ancestor_depth"], 1);
    database.cleanup().await?;

    // The exact node's resolver wins over the ancestor's, and a cleared exact node falls through
    // to the ancestor.
    for (id, v1_side, expected_resolver, expected_depth) in [
        (
            "mirror_exact_over_ancestor",
            V1Side::Projected,
            V1_RESOLVER,
            0,
        ),
        (
            "mirror_cleared_to_ancestor",
            V1Side::Cleared,
            PARENT_RESOLVER,
            1,
        ),
    ] {
        let fixture = Fixture::declared(id, v1_side).with_ancestor(Ancestor::Direct {
            pointer_block_offset: 0,
        });
        let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
        let v2 = inventory(&pool, V2_RESOURCE).await?;
        assert_eq!(v2["support_status"], "supported", "{id}: {v2}");
        assert_eq!(
            v2["provenance"]["mirror"]["mirrored_resolver_address"], expected_resolver,
            "{id}: {v2}"
        );
        assert_eq!(v2["provenance"]["mirror"]["ancestor_depth"], expected_depth);
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn single_label_root_name_with_a_node_only_ensv1_pointer_records_the_walk() -> Result<()> {
    // The ENSv1 registry sets resolvers for nodes nobody owns there: the pointer event carries no
    // logical name and no resource. The walk must still see it (exact node, depth 0).
    let node = bigname_lookup::ens_namehash_hex(PARENT_NAME)?;
    let fixture = Fixture::declared(
        "mirror_tld_undeclared",
        V1Side::NodeOnly {
            resolver: UNDECLARED_RESOLVER,
            pointer_block_offset: 0,
        },
    )
    .single_label();
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v2["support_status"], "unsupported", "{v2}");
    assert_eq!(v2["unsupported_reason"], "mirrored_resolver_not_projected");
    let mirror = &v2["provenance"]["mirror"];
    assert_eq!(mirror["queried_node"], node, "{v2}");
    assert_eq!(mirror["mirrored_node"], node);
    assert_eq!(mirror["mirrored_name"], PARENT_NAME);
    assert_eq!(mirror["ancestor_depth"], 0);
    assert_eq!(mirror["forwarding"], "direct_call");
    assert_eq!(mirror["mirrored_resolver_address"], UNDECLARED_RESOLVER);
    // The pointer target is classified as an undeclared candidate; the walk records its reason.
    assert_eq!(
        mirror["mirrored_unsupported_reason"],
        "resolver_not_declared"
    );
    assert_eq!(
        mirror["mirrored_pointer_source_family"],
        "ens_v1_registry_l1"
    );
    assert!(mirror["mirrored_pointer_event_id"].is_number(), "{v2}");
    assert!(mirror.get("mirrored_resource_id").is_none(), "{v2}");
    database.cleanup().await?;

    // The same node-only pointer to a declared ENSv1 resolver derives the node's records, in
    // every execution shape, including when the pointer arrives after the record write.
    for (id, pointer_block_offset) in [("mirror_tld_declared", 0), ("mirror_tld_late", 2)] {
        let fixture = Fixture::declared(
            id,
            V1Side::NodeOnly {
                resolver: V1_RESOLVER,
                pointer_block_offset,
            },
        )
        .single_label();
        let mut previous: Option<Value> = None;
        for execution in [
            Execution::FromZero,
            Execution::PerBlock,
            Execution::TwoByTwo,
            Execution::Idempotent,
            Execution::RedoLastBlock,
        ] {
            let (database, pool) = project(&fixture, fixture.target(), execution).await?;
            let mut v2 = inventory(&pool, V2_RESOURCE).await?;
            for section in ["chain_positions", "canonicality_summary"] {
                let section = v2[section].as_object_mut().context("section")?;
                section.remove("target_block_number");
                section.remove("target_block_hash");
            }
            assert_eq!(
                v2["support_status"], "supported",
                "{id} {execution:?}: {v2}"
            );
            assert_eq!(
                v2["entries"][0]["record_key"], "text:url",
                "{id} {execution:?}: {v2}"
            );
            assert_eq!(v2["provenance"]["mirror"]["ancestor_depth"], 0);
            assert_eq!(v2["provenance"]["mirror"]["mirrored_node"], node);
            assert_eq!(
                v2["provenance"]["mirror"]["mirrored_resolver_address"],
                V1_RESOLVER
            );
            assert!(
                v2["provenance"]["mirror"]
                    .get("mirrored_resource_id")
                    .is_none()
            );
            if let Some(previous) = &previous {
                assert_eq!(&v2, previous, "{id} {execution:?} drifted");
            }
            previous = Some(v2);
            database.cleanup().await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn root_registry_tld_without_a_registration_serves_its_pointer() -> Result<()> {
    // A root-registry TLD token has a resource and a resolver pointer but no observed registration,
    // so no surface binding and no selected authority. The pointer still reaches `name_current`
    // as the TLD's serving resource; the inventory keeps classifying through the mirror walk.
    let node = bigname_lookup::ens_namehash_hex(PARENT_NAME)?;
    let logical_name_id = format!("ens:{node}");
    for (id, resolver, expected_support) in [
        ("tld_root_declared", V1_RESOLVER, "supported"),
        ("tld_root_undeclared", UNDECLARED_RESOLVER, "unsupported"),
    ] {
        let fixture = Fixture::declared(
            id,
            V1Side::NodeOnly {
                resolver,
                pointer_block_offset: 0,
            },
        )
        .single_label()
        .unbound();
        let mut previous: Option<Value> = None;
        for execution in [
            Execution::FromZero,
            Execution::PerBlock,
            Execution::RedoLastBlock,
        ] {
            let (database, pool) = project(&fixture, fixture.target(), execution).await?;
            let name = name_current(&pool, &logical_name_id)
                .await?
                .with_context(|| format!("{id} {execution:?}: TLD row"))?;
            assert_eq!(
                name["resource_id"],
                Value::Null,
                "{id} {execution:?}: {name}"
            );
            assert_eq!(name["surface_binding_id"], Value::Null);
            assert_eq!(name["binding_kind"], Value::Null);
            assert_eq!(name["serving_resource_id"], V2_RESOURCE, "{name}");
            assert_eq!(name["support_status"], "unsupported");
            assert_eq!(
                name["unsupported_reason"],
                "current_authority_not_projected"
            );
            let summary = &name["declared_summary"];
            assert_eq!(summary["resolver"]["address"], MIRROR, "{summary}");
            assert_eq!(summary["resolver"]["chain_id"], CHAIN);
            assert_eq!(summary["registration"]["status"], Value::Null);
            assert_eq!(summary["registration"]["authority_kind"], Value::Null);
            assert_eq!(
                summary["coverage"]["enumeration_basis"],
                "event_linked_registry_resolver"
            );
            assert_eq!(summary["topology"]["resolver_path"][0]["address"], MIRROR);
            assert_eq!(
                summary["topology"]["resolver_path"][0]["resource_id"],
                V2_RESOURCE
            );
            let provenance = &name["provenance"];
            assert_eq!(
                provenance["authority_selection"].get("authority_arm"),
                None,
                "{provenance}"
            );
            assert_eq!(
                provenance["read_reachability"]["basis"],
                "root_registry_resolver_pointer"
            );
            assert_eq!(
                provenance["read_reachability"]["serving_resource_id"],
                V2_RESOURCE
            );
            assert!(provenance["read_reachability"]["pointer_event_id"].is_number());
            assert_eq!(
                provenance["resolver_pointer_source_family"],
                "ens_v2_root_l1"
            );

            let v2 = inventory(&pool, V2_RESOURCE).await?;
            assert_eq!(v2["support_status"], expected_support, "{v2}");
            assert_eq!(v2["provenance"]["mirror"]["ancestor_depth"], 0);
            assert_eq!(
                v2["provenance"]["mirror"]["mirrored_resolver_address"],
                resolver
            );
            if expected_support == "supported" {
                assert_eq!(v2["entries"][0]["record_key"], "text:url");
            } else {
                assert_eq!(v2["unsupported_reason"], "mirrored_resolver_not_projected");
            }
            // Rows carry the Project target of the batch that last published them.
            let mut name = name;
            let row = name.as_object_mut().context("row")?;
            row.remove("chain_positions");
            row.remove("canonicality_summary");
            if let Some(previous) = &previous {
                assert_eq!(&name, previous, "{id} {execution:?} drifted");
            }
            previous = Some(name);
            database.cleanup().await?;
        }
    }

    // The rule is scoped to `current_authority_not_projected`: a TLD bound under both arms is a
    // mixed-history overlap and gets neither the pointer nor a serving resource.
    let fixture = Fixture::declared(
        "tld_root_bound",
        V1Side::NodeOnly {
            resolver: V1_RESOLVER,
            pointer_block_offset: 0,
        },
    )
    .single_label();
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let name = name_current(&pool, &logical_name_id)
        .await?
        .context("bound TLD row")?;
    assert_eq!(
        name["unsupported_reason"], "independent_ens_deployments_overlap",
        "{name}"
    );
    assert_eq!(name["serving_resource_id"], Value::Null);
    assert_eq!(name["declared_summary"]["resolver"]["address"], Value::Null);
    assert_eq!(name["provenance"]["read_reachability"], json!({}));
    database.cleanup().await?;

    // A reserved label and an expired token do not serve the pointer: the reservation is the
    // documented ENSv2 narrowing, and the expiry clears the pointer through a state-derived event
    // that carries the resource but no logical name.
    for (id, lifecycle) in [
        ("tld_root_reserved", V2Lifecycle::Reserved),
        ("tld_root_expired", V2Lifecycle::Expired),
    ] {
        let fixture = Fixture::declared(
            id,
            V1Side::NodeOnly {
                resolver: V1_RESOLVER,
                pointer_block_offset: 0,
            },
        )
        .single_label()
        .unbound()
        .with_v2_lifecycle(lifecycle);
        let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
        if let Some(name) = name_current(&pool, &logical_name_id).await? {
            assert_eq!(name["serving_resource_id"], Value::Null, "{id}: {name}");
            assert_eq!(
                name["declared_summary"]["resolver"]["address"],
                Value::Null,
                "{id}: {name}"
            );
            assert_eq!(name["provenance"]["read_reachability"], json!({}), "{id}");
        }
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn mirror_does_not_derive_through_an_extended_ancestor_resolver() -> Result<()> {
    let fixture = Fixture::declared("mirror_extended_ancestor", V1Side::Absent)
        .with_ancestor(Ancestor::Extended);
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v2["support_status"], "unsupported", "{v2}");
    assert_eq!(v2["unsupported_reason"], "mirrored_resolver_not_projected");
    assert_eq!(v2["entries"], json!([]));
    assert_eq!(v2["provenance"]["read_rules"], json!([]));
    let parent_node = bigname_lookup::ens_namehash_hex(PARENT_NAME)?;
    assert_eq!(v2["provenance"]["mirror"]["mirrored_node"], parent_node);
    assert_eq!(v2["provenance"]["mirror"]["ancestor_depth"], 1);
    assert_eq!(v2["provenance"]["mirror"]["forwarding"], "extended_resolve");
    assert_eq!(
        v2["provenance"]["mirror"]["mirrored_unsupported_reason"],
        "ensip10_extended_resolver"
    );
    assert_eq!(
        v2["provenance"]["mirror"]["mirrored_resolver_address"],
        PARENT_RESOLVER
    );
    let parent = inventory(&pool, PARENT_V1_RESOURCE).await?;
    assert_eq!(parent["support_status"], "supported", "{parent}");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ancestor_pointer_changes_rescope_the_mirrored_descendant() -> Result<()> {
    // The child's record on the parent's resolver is written at base + 1, before the ENSv1
    // registry points the parent at that resolver at base + 2. Every execution shape must publish
    // the same supported row once the pointer exists.
    let fixture =
        Fixture::declared("mirror_rescope", V1Side::Absent).with_ancestor(Ancestor::Direct {
            pointer_block_offset: 2,
        });
    let (database, pool) = project(&fixture, fixture.base + 1, Execution::FromZero).await?;
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v2["support_status"], "unsupported", "{v2}");
    assert_eq!(v2["unsupported_reason"], "mirrored_resolver_not_projected");
    assert!(v2["provenance"]["mirror"].get("mirrored_node").is_none());
    database.cleanup().await?;

    let mut previous: Option<Value> = None;
    for execution in [
        Execution::FromZero,
        Execution::PerBlock,
        Execution::TwoByTwo,
        Execution::Idempotent,
        Execution::RedoLastBlock,
    ] {
        let (database, pool) = project(&fixture, fixture.target(), execution).await?;
        let mut v2 = inventory(&pool, V2_RESOURCE).await?;
        // Nothing touches the name after base + 2, so an incremental run leaves the row published
        // at that block: compare content, not the publication's target marker.
        for section in ["chain_positions", "canonicality_summary"] {
            let section = v2[section].as_object_mut().context("section")?;
            section.remove("target_block_number");
            section.remove("target_block_hash");
        }
        assert_eq!(v2["support_status"], "supported", "{execution:?}: {v2}");
        assert_eq!(v2["provenance"]["mirror"]["ancestor_depth"], 1);
        assert_eq!(v2["entries"][0]["record_key"], "text:description");
        assert_eq!(
            v2["chain_positions"]["block_number"],
            json!(fixture.base + 2),
            "{execution:?}: {v2}"
        );
        if let Some(previous) = &previous {
            assert_eq!(&v2, previous, "{execution:?} drifted");
        }
        previous = Some(v2);
        database.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn undeclared_ensv2_resolver_without_upgrade_history_is_unchanged() -> Result<()> {
    let mut fixture = Fixture::declared("mirror_undeclared", V1Side::Projected);
    fixture.v2_payload = Some(json!({
        "deployment_epoch": "fixture", "contracts": [], "capability_flags": {}
    }));
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v2["support_status"], "unsupported", "{v2}");
    assert_eq!(v2["unsupported_reason"], "resolver_implementation_unknown");
    assert!(v2["provenance"].get("mirror").is_none(), "{v2}");
    let resolver = resolver_current(&pool, MIRROR).await?;
    assert_eq!(resolver["support_status"], "unsupported");
    assert_eq!(
        resolver["unsupported_reason"],
        "resolver_implementation_unknown"
    );
    assert_eq!(
        resolver["declared_summary"]["classification"]["basis"], "erc1967_upgraded_history",
        "{resolver}"
    );
    assert!(
        resolver["declared_summary"]["classification"]
            .get("mirror")
            .is_none()
    );
    let v1 = inventory(&pool, V1_RESOURCE).await?;
    assert_eq!(v1["support_status"], "supported", "{v1}");
    database.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn hackathon_manifest_declares_the_mirror_and_classifies_it() -> Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let repository = bigname_manifests::load_repository(root.join("manifests/sepolia-hackathon"))?;
    let manifest = &repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == "ens_v2_resolver_l1")
        .context("hackathon ens_v2_resolver_l1 manifest")?
        .manifest;
    let mirrors: Vec<_> = manifest
        .contracts
        .iter()
        .filter(|contract| contract.role == bigname_manifests::ENSV1_MIRROR_RESOLVER_ROLE)
        .collect();
    assert_eq!(
        mirrors
            .iter()
            .map(|contract| (contract.address.to_ascii_lowercase(), contract.start_block))
            .collect::<Vec<_>>(),
        [
            (
                "0x10107255fda20ab6c37a0efca1e9465f25066a00".to_owned(),
                Some(11_626_641)
            ),
            (
                "0x1f11e5b8bca2ccfe13bd8431853db159c4e9849c".to_owned(),
                Some(11_626_628)
            ),
        ]
    );
    let mirror = mirrors[0];
    assert_eq!(
        mirror.address.to_ascii_lowercase(),
        "0x10107255fda20ab6c37a0efca1e9465f25066a00"
    );
    assert_eq!(mirror.start_block, Some(11_626_641));
    assert_eq!(mirror.proxy_kind, "none");
    let registry = manifest.correlation_addresses
        [bigname_manifests::ENSV1_MIRROR_REGISTRY_CORRELATION_KEY]
        .to_ascii_lowercase();
    assert_eq!(registry, "0x82080cc8ca78597bde586a003d0a080c79a1814b");

    let address: &'static str = Box::leak(mirror.address.to_ascii_lowercase().into_boxed_str());
    let fixture = Fixture {
        id: "mirror_hackathon",
        base: i64::try_from(mirror.start_block.unwrap())? + 1,
        mirror: address,
        v2_payload: Some(serde_json::to_value(manifest)?),
        v1_side: V1Side::Projected,
        ancestor: Ancestor::None,
        queried: NAME,
        queried_bound: true,
        v2_lifecycle: V2Lifecycle::None,
    };
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let resolver = resolver_current(&pool, address).await?;
    assert_eq!(resolver["support_status"], "supported", "{resolver}");
    assert_eq!(
        resolver["declared_summary"]["classification"]["role"],
        "ensv1_mirror_resolver"
    );
    assert_eq!(
        resolver["declared_summary"]["classification"]["mirror"]["mirrored_registry_address"],
        registry
    );
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v2["support_status"], "supported", "{v2}");
    assert_eq!(
        v2["provenance"]["mirror"]["mirrored_registry_address"],
        registry
    );
    assert_eq!(
        v2["provenance"]["mirror"]["mirrored_resolver_address"],
        V1_RESOLVER
    );
    database.cleanup().await?;

    // The second declared instance classifies and serves the same way once a name points at it.
    let second: &'static str = Box::leak(mirrors[1].address.to_ascii_lowercase().into_boxed_str());
    let fixture = Fixture {
        id: "mirror_hackathon_second",
        base: i64::try_from(mirrors[1].start_block.unwrap())? + 1,
        mirror: second,
        v2_payload: Some(serde_json::to_value(manifest)?),
        v1_side: V1Side::Projected,
        ancestor: Ancestor::None,
        queried: NAME,
        queried_bound: true,
        v2_lifecycle: V2Lifecycle::None,
    };
    let (database, pool) = project(&fixture, fixture.target(), Execution::FromZero).await?;
    let resolver = resolver_current(&pool, second).await?;
    assert_eq!(resolver["support_status"], "supported", "{resolver}");
    assert_eq!(
        resolver["declared_summary"]["classification"]["role"],
        "ensv1_mirror_resolver"
    );
    let v2 = inventory(&pool, V2_RESOURCE).await?;
    assert_eq!(v2["support_status"], "supported", "{v2}");
    assert_eq!(v2["provenance"]["resolver_address"], second);
    database.cleanup().await?;
    Ok(())
}

fn boundary_key(boundary: &Value, chain_id: &str) -> String {
    let part = |value: &str| format!("{}:{value};", value.len());
    let text = |value: &Value| match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    [
        text(&boundary["logical_name_id"]),
        text(&boundary["resource_id"]),
        text(&boundary["normalized_event_id"]),
        text(&boundary["event_kind"]),
        chain_id.to_owned(),
        text(&boundary["chain_position"]["block_number"]),
        text(&boundary["chain_position"]["block_hash"]),
        text(&boundary["chain_position"]["timestamp"]),
    ]
    .iter()
    .map(|value| part(value))
    .collect()
}

async fn inventory(pool: &PgPool, resource: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(row) - 'last_recomputed_at' - 'inserted_at' \
         FROM record_inventory_current row WHERE resource_id = $1::uuid",
    )
    .bind(resource)
    .fetch_one(pool)
    .await?)
}

async fn name_current(pool: &PgPool, logical_name_id: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(row) - 'last_recomputed_at' - 'inserted_at' \
         FROM name_current row WHERE logical_name_id = $1",
    )
    .bind(logical_name_id)
    .fetch_optional(pool)
    .await?)
}

async fn resolver_current(pool: &PgPool, address: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT to_jsonb(row) - 'last_recomputed_at' - 'inserted_at' \
         FROM resolver_current row WHERE chain_id = $1 AND resolver_address = $2",
    )
    .bind(CHAIN)
    .bind(address)
    .fetch_one(pool)
    .await?)
}

async fn project(
    fixture: &Fixture,
    target: i64,
    execution: Execution,
) -> Result<(TestDatabase, PgPool)> {
    let (database, pool) = database(&format!("{}_{execution:?}", fixture.id)).await?;
    seed(&pool, fixture).await?;
    let base = fixture.base;
    match execution {
        Execution::FromZero => {
            run(&pool, target, 0, target, None, RunMode::Normal).await?;
        }
        Execution::PerBlock => {
            run(&pool, base, 0, base, None, RunMode::Normal).await?;
            for block in base + 1..=target {
                run(&pool, block, block, block, Some(block - 1), RunMode::Normal).await?;
            }
        }
        Execution::TwoByTwo => {
            run(&pool, base + 1, 0, base + 1, None, RunMode::Normal).await?;
            run(
                &pool,
                target,
                base + 2,
                target,
                Some(base + 1),
                RunMode::Normal,
            )
            .await?;
        }
        Execution::Idempotent => {
            run(&pool, target - 1, 0, target - 1, None, RunMode::Normal).await?;
            run(
                &pool,
                target,
                target,
                target,
                Some(target - 1),
                RunMode::Normal,
            )
            .await?;
            run(
                &pool,
                target,
                target,
                target,
                Some(target - 1),
                RunMode::Normal,
            )
            .await?;
        }
        Execution::RedoLastBlock => {
            run(&pool, target, 0, target, None, RunMode::Normal).await?;
            run(&pool, target, target, target, Some(target), RunMode::Redo).await?;
        }
    }
    let raw_count: i64 = sqlx::query_scalar("SELECT count(*) FROM raw_logs")
        .fetch_one(&pool)
        .await?;
    assert_eq!(raw_count, 0, "Project wrote raw facts");
    Ok((database, pool))
}

async fn run(
    pool: &PgPool,
    target_block: i64,
    affected_from_block: i64,
    affected_to_block: i64,
    resume_current: Option<i64>,
    mode: RunMode,
) -> Result<BatchOutcome> {
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block,
            affected_from_block,
            affected_to_block,
            resume_current: resume_current.map(|number| Marker {
                number,
                hash: block_hash(number),
            }),
            mode,
        })
        .await?;
    assert!(outcome.complete);
    assert_eq!(outcome.target.number, target_block);
    Ok(outcome)
}

async fn manifest(
    pool: &PgPool,
    fixture: &Fixture,
    source_family: &str,
    payload: &Value,
) -> Result<i64> {
    let manifest_id: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens',$1,$2,'fixture','active','fixture',$3,$4) RETURNING manifest_id")
        .bind(source_family).bind(CHAIN)
        .bind(format!("fixture/{}/{source_family}.toml", fixture.id)).bind(payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ($1,'ens','SourceManifestUpdated',$2,1,$3,$4,'manifest_sync','canonical',$5)")
        .bind(format!("manifest:{}:{source_family}", fixture.id)).bind(source_family)
        .bind(manifest_id).bind(CHAIN)
        .bind(json!({"rollout_status":"active","normalizer_version":"fixture","manifest_payload":payload}))
        .execute(pool).await?;
    Ok(manifest_id)
}

async fn seed(pool: &PgPool, fixture: &Fixture) -> Result<()> {
    let base = fixture.base;
    for number in base..=base + 3 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($4),'canonical')")
            .bind(CHAIN).bind(block_hash(number)).bind(number).bind(1_800_000_000 + number).execute(pool).await?;
    }
    let v2_manifest = manifest(pool, fixture, "ens_v2_resolver_l1", &fixture.v2_payload()).await?;
    let v1_manifest = manifest(
        pool,
        fixture,
        "ens_v1_resolver_l1",
        &json!({"deployment_epoch":"fixture","contracts":[{
            "role":"public_resolver","address":V1_RESOLVER,"proxy_kind":"none","start_block":0,
            "read_features":["ensip19_default_address"]
        }, {
            "role":"public_resolver_parent","address":PARENT_RESOLVER,"proxy_kind":"none",
            "start_block":0,
            "read_features": if fixture.ancestor == Ancestor::Extended {
                json!(["ensip10_extended_resolver"])
            } else {
                json!([])
            }
        }]}),
    )
    .await?;
    let root_manifest = manifest(pool, fixture, "ens_v2_root_l1", &json!({})).await?;
    let _ = v2_manifest;
    for instance in [ROOT_INSTANCE, MIRROR_INSTANCE] {
        sqlx::query("INSERT INTO contract_instances (contract_instance_id,chain_id,contract_kind) VALUES ($1::uuid,$2,'contract')")
            .bind(instance).bind(CHAIN).execute(pool).await?;
    }
    sqlx::query("INSERT INTO contract_instance_addresses (contract_instance_id,chain_id,address,active_from_block_number,active_from_block_hash,source_manifest_id) VALUES ($1::uuid,$2,$3,$4,$5,$6)")
        .bind(MIRROR_INSTANCE).bind(CHAIN).bind(fixture.mirror).bind(base).bind(block_hash(base))
        .bind(root_manifest).execute(pool).await?;
    sqlx::query("INSERT INTO discovery_edges (chain_id,edge_kind,from_contract_instance_id,to_contract_instance_id,discovery_source,admission_basis,source_manifest_id,active_from_block_number,active_from_block_hash,canonicality_state) VALUES ($1,'resolver',$2::uuid,$3::uuid,'fixture','fixture',$4,$5,$6,'canonical')")
        .bind(CHAIN).bind(ROOT_INSTANCE).bind(MIRROR_INSTANCE)
        .bind(root_manifest).bind(base).bind(block_hash(base)).execute(pool).await?;

    let node = bigname_lookup::ens_namehash_hex(NAME)?;
    let logical_name_id = format!("ens:{node}");
    let parent_node = bigname_lookup::ens_namehash_hex(PARENT_NAME)?;
    let parent_logical_name_id = format!("ens:{parent_node}");
    for (name, dns, node, logical) in [
        (
            NAME,
            b"\x06mirror\x07fixture\0".as_slice(),
            &node,
            &logical_name_id,
        ),
        (
            PARENT_NAME,
            b"\x07fixture\0".as_slice(),
            &parent_node,
            &parent_logical_name_id,
        ),
    ] {
        let labelhashes: Vec<_> = name
            .split('.')
            .map(|label| format!("{:#x}", alloy_primitives::keccak256(label)))
            .collect();
        sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens',$2,string_to_array($2,'.'),$3,$4,$5,'fixture','active',$6,$7,$8,'canonical')")
            .bind(logical).bind(name).bind(dns).bind(node)
            .bind(labelhashes).bind(CHAIN).bind(block_hash(base)).bind(base).execute(pool).await?;
    }
    let (queried_node, queried_logical_name_id) = if fixture.queried == NAME {
        (node.clone(), logical_name_id.clone())
    } else {
        (parent_node.clone(), parent_logical_name_id.clone())
    };
    for (resource, binding, arm, logical) in [
        (V1_RESOURCE, V1_BINDING, "ens_v1", &logical_name_id),
        (V2_RESOURCE, V2_BINDING, "ens_v2", &queried_logical_name_id),
        (
            PARENT_V1_RESOURCE,
            PARENT_V1_BINDING,
            "ens_v1",
            &parent_logical_name_id,
        ),
    ] {
        sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,$4,'canonical')")
            .bind(resource).bind(CHAIN).bind(block_hash(base)).bind(base).execute(pool).await?;
        if !fixture.queried_bound && logical == &queried_logical_name_id {
            continue;
        }
        sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path',$4,to_timestamp($5),$6,$7,$8,'canonical')")
            .bind(binding).bind(logical).bind(resource).bind(arm)
            .bind(1_800_000_000 + base).bind(CHAIN).bind(block_hash(base)).bind(base).execute(pool).await?;
    }

    let mut events = vec![Event {
        identity: "v2-pointer",
        logical_name_id: Some(queried_logical_name_id.clone()),
        resource_id: Some(V2_RESOURCE),
        kind: "ResolverChanged",
        source_family: "ens_v2_root_l1",
        manifest_id: Some(root_manifest),
        block: base,
        log_index: 0,
        emitter: V1_REGISTRY,
        after_state: json!({"node": queried_node, "resolver": fixture.mirror}),
    }];
    match fixture.v2_lifecycle {
        V2Lifecycle::None => {}
        V2Lifecycle::Reserved => events.push(Event {
            identity: "v2-reserved",
            logical_name_id: Some(queried_logical_name_id.clone()),
            resource_id: Some(V2_RESOURCE),
            kind: "RegistrationReserved",
            source_family: "ens_v2_root_l1",
            manifest_id: Some(root_manifest),
            block: base,
            log_index: 1,
            emitter: V1_REGISTRY,
            after_state: json!({"source_event": "LabelReserved", "status": "reserved",
                                "expiry": 1_900_000_000}),
        }),
        V2Lifecycle::Expired => {
            let expired = json!({
                "source_event": "RegistryPathExpired", "derived_from": "interpreter_state",
                "terminal_reason": "registry_name_binding_expired", "expiry": 1_800_000_011
            });
            for (identity, kind, log_index, field, value) in [
                (
                    "v2-expiry-release",
                    "RegistrationReleased",
                    0,
                    "status",
                    json!("released"),
                ),
                (
                    "v2-expiry-pointer",
                    "ResolverChanged",
                    1,
                    "resolver",
                    Value::Null,
                ),
            ] {
                let mut after_state = expired.clone();
                after_state[field] = value;
                events.push(Event {
                    identity,
                    logical_name_id: None,
                    resource_id: Some(V2_RESOURCE),
                    kind,
                    source_family: "ens_v2_root_l1",
                    manifest_id: Some(root_manifest),
                    block: base + 2,
                    log_index,
                    emitter: V1_REGISTRY,
                    after_state,
                });
            }
        }
    }
    if let V1Side::NodeOnly {
        resolver,
        pointer_block_offset,
    } = fixture.v1_side
    {
        events.push(Event {
            identity: "v1-node-pointer",
            logical_name_id: None,
            resource_id: None,
            kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            manifest_id: None,
            block: base + pointer_block_offset,
            log_index: 5,
            emitter: V1_REGISTRY,
            after_state: json!({"node": queried_node, "resolver": resolver}),
        });
        if resolver == V1_RESOLVER {
            events.push(record(
                "v1-node-text",
                base + 1,
                6,
                &queried_node,
                V1_RESOLVER,
                v1_manifest,
                json!({"record_key": "text:url", "record_family": "text", "selector_key": "url",
                       "source_event": "TextChanged", "value": "https://tld.example"}),
            ));
        }
    }
    if matches!(fixture.v1_side, V1Side::Projected | V1Side::Cleared) {
        events.push(Event {
            identity: "v1-pointer",
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(V1_RESOURCE),
            kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            manifest_id: None,
            block: base,
            log_index: 1,
            emitter: V1_REGISTRY,
            after_state: json!({"node": node, "resolver": V1_RESOLVER}),
        });
        events.push(record(
            "v1-text",
            base + 1,
            1,
            &node,
            V1_RESOLVER,
            v1_manifest,
            json!({"record_key": "text:url", "record_family": "text", "selector_key": "url",
                   "source_event": "TextChanged", "value": "https://one.example"}),
        ));
        events.push(record(
            "v1-addr",
            base + 3,
            1,
            &node,
            V1_RESOLVER,
            v1_manifest,
            json!({"record_key": "addr:60", "record_family": "addr", "selector_key": "60",
                   "source_event": "AddrChanged", "value": ADDRESS}),
        ));
    }
    if let Some(pointer_block) = fixture.parent_pointer_block() {
        events.push(Event {
            identity: "parent-pointer",
            logical_name_id: Some(parent_logical_name_id.clone()),
            resource_id: Some(PARENT_V1_RESOURCE),
            kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            manifest_id: None,
            block: pointer_block,
            log_index: 2,
            emitter: V1_REGISTRY,
            after_state: json!({"node": parent_node, "resolver": PARENT_RESOLVER}),
        });
        events.push(record(
            "parent-child-text",
            base + 1,
            3,
            &node,
            PARENT_RESOLVER,
            v1_manifest,
            json!({"record_key": "text:description", "record_family": "text",
                   "selector_key": "description", "source_event": "TextChanged",
                   "value": "child on parent resolver"}),
        ));
        events.push(record(
            "parent-own-text",
            base + 1,
            4,
            &parent_node,
            PARENT_RESOLVER,
            v1_manifest,
            json!({"record_key": "text:url", "record_family": "text", "selector_key": "url",
                   "source_event": "TextChanged", "value": "https://parent.example"}),
        ));
    }
    if fixture.v1_side == V1Side::Cleared {
        events.push(Event {
            identity: "v1-clear",
            logical_name_id: Some(logical_name_id.clone()),
            resource_id: Some(V1_RESOURCE),
            kind: "ResolverChanged",
            source_family: "ens_v1_registry_l1",
            manifest_id: None,
            block: base + 2,
            log_index: 0,
            emitter: V1_REGISTRY,
            after_state: json!({"node": node, "resolver": ZERO20}),
        });
    }
    for event in events {
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) VALUES ($1,'ens',$2,$3::uuid,$4,$5,1,$6,$7,$8,$9,$10,0,$11,'ens_v1_unwrapped_authority','canonical',$12,$13)")
            .bind(format!("{}:{}", fixture.id, event.identity)).bind(event.logical_name_id)
            .bind(event.resource_id).bind(event.kind).bind(event.source_family).bind(event.manifest_id)
            .bind(CHAIN).bind(event.block).bind(block_hash(event.block))
            .bind(format!("0x{:064x}", event.block * 10 + event.log_index)).bind(event.log_index)
            .bind(event.after_state).bind(json!({"emitting_address": event.emitter}))
            .execute(pool).await?;
    }
    Ok(())
}

struct Event {
    identity: &'static str,
    logical_name_id: Option<String>,
    resource_id: Option<&'static str>,
    kind: &'static str,
    source_family: &'static str,
    manifest_id: Option<i64>,
    block: i64,
    log_index: i64,
    emitter: &'static str,
    after_state: Value,
}

fn record(
    identity: &'static str,
    block: i64,
    log_index: i64,
    node: &str,
    resolver: &'static str,
    manifest_id: i64,
    mut after: Value,
) -> Event {
    after["node"] = json!(node);
    after["resolver"] = json!(resolver);
    Event {
        identity,
        logical_name_id: None,
        resource_id: None,
        kind: "RecordChanged",
        source_family: "ens_v1_resolver_l1",
        manifest_id: Some(manifest_id),
        block,
        log_index,
        emitter: resolver,
        after_state: after,
    }
}

fn block_hash(number: i64) -> String {
    format!("0x{number:064x}")
}

async fn database(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new(format!("mirror_{name}")).pool_max_connections(1),
    )
    .await?;
    let pool = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    raw_sql(&format!("CREATE SCHEMA bigname_phase; ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public; SET LOCAL search_path TO bigname_phase, public", database_name.replace('"', "\"\""))).execute(&mut *transaction).await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connection = pool.acquire().await?;
    sqlx::query("SET search_path TO bigname_phase, public")
        .execute(&mut *connection)
        .await?;
    drop(connection);
    Ok((database, pool))
}
