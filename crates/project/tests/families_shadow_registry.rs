//! Registry shadow reads (TYR-36 step 3, docs/glossary.md "Shadow read"): the F2c node rows give
//! an ENSv1 name's registry generation, handoff block and ownerless profile, and the registry
//! binding observations give each resource's served registry owner, contract and clear. Each
//! case publishes with the production batch, follows it with the families, and compares every
//! served name and resource with its family read; it also pins the served value it was written
//! for.
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use serde_json::{Value, json};
use shadow_support::publish_and_compare;
use support::{Fixture, uuid};

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const OTHER: &str = "0x00000000000000000000000000000000000000bb";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";
const V1_REGISTRY: &str = "ens_v1_registry_l1";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// Name 1 bound at block 9 to `lease` under arm ens_v1, the SurfaceBound naming its registry and
/// the registry owner the getter read, then granted at 10.
async fn bound(fixture: &Fixture, lease: &str) -> Result<()> {
    fixture
        .binding(&uuid(100), &name(1), lease, "ens_v1", 9, 0, None)
        .await?;
    fixture
        .write(
            9,
            0,
            "SurfaceBound",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": OWNER,
                   "expiry": 2_000_000_000u64}),
            REGISTRAR,
        )
        .await?;
    Ok(())
}

/// A registry AuthorityTransferred of name 1's node from the old or the current registry.
async fn transferred(fixture: &Fixture, block: i64, owner: &str, role: &str) -> Result<()> {
    fixture
        .write(
            block,
            1,
            "AuthorityTransferred",
            V1_REGISTRY,
            Some(&name(1)),
            None,
            json!({"node": node(1), "owner": owner, "owner_getter": owner, "emitter_role": role}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

async fn summary(fixture: &Fixture, resource: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object('registry_owner', registry_owner,
                    'registry_contract', registry_contract,
                    'clear_event_id', provenance -> 'registry_binding_clear_event_id')
         FROM permissions_current_resource_summary WHERE resource_id = $1::uuid",
    )
    .bind(resource)
    .fetch_optional(&fixture.pool)
    .await?)
}

async fn selection(fixture: &Fixture) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT provenance -> 'authority_selection' FROM name_current WHERE logical_name_id = $1",
    )
    .bind(name(1))
    .fetch_one(&fixture.pool)
    .await?)
}

#[tokio::test]
async fn a_surface_binding_serves_its_registry_owner() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_bound", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    assert_eq!(
        summary(&fixture, &lease).await?,
        Some(
            json!({"registry_owner": OWNER, "registry_contract": REGISTRY,
                    "clear_event_id": null})
        )
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn a_surface_unbinding_clears_the_registry_owner_with_its_event() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_unbound", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    fixture
        .write(
            14,
            1,
            "SurfaceUnbound",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            json!({"registry_contract": REGISTRY}),
            REGISTRAR,
        )
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let unbound: i64 = sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events WHERE event_identity = $1",
    )
    .bind("SurfaceUnbound:14:1")
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        summary(&fixture, &lease).await?,
        Some(json!({"registry_owner": null, "registry_contract": null,
                    "clear_event_id": unbound}))
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn the_old_registry_then_the_current_one_move_the_generation() -> Result<()> {
    for current in [false, true] {
        let fixture = Fixture::new(
            &format!("families_shadow_registry_generation_{current}"),
            20,
        )
        .await?;
        bound(&fixture, &uuid(1)).await?;
        transferred(&fixture, 11, OWNER, "registry_old").await?;
        if current {
            transferred(&fixture, 13, OWNER, "registry").await?;
        }
        let report = publish_and_compare(&fixture, 16).await?;
        shadow_support::assert_counts(&report, &[], &[]);
        let selection = selection(&fixture).await?;
        if current {
            assert_eq!(selection["registry_generation"], json!("current"));
            assert_eq!(selection["registry_handoff_block_number"], json!(13));
        } else {
            assert_eq!(selection["registry_generation"], json!("old"));
            assert_eq!(selection["registry_handoff_block_number"], Value::Null);
        }
        fixture.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_zero_registry_owner_keeps_its_getter_facts() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_zero", 20).await?;
    bound(&fixture, &uuid(1)).await?;
    transferred(&fixture, 11, ZERO, "registry").await?;
    let report = publish_and_compare(&fixture, 16).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let selection = selection(&fixture).await?;
    assert_eq!(selection["registry_generation"], json!("current"));
    assert_eq!(selection["registry_handoff_block_number"], json!(11));
    assert_eq!(
        selection["ownerless_registry"],
        Value::Null,
        "the lease still holds the name"
    );
    fixture.cleanup().await
}

/// Items 3 and 5 of the TYR-36 step 3 review (Q7, Q5), the control owner under the authority
/// admission. Name 1's node gets an AuthorityTransferred to OWNER at 11, which the admission
/// holds, then one to OTHER at 12 carrying another resource, which the admission leaves out
/// (authority_events.sql admits a resource-bearing event only on the selected resource), and the
/// lease is transferred at 13. The served control block reads the latest admitted events
/// (build.sql:649-694): owner OWNER, latest kind TokenControlTransferred. F2c keeps every
/// owner-setting event of the node (`project_registry_owner_event`), so the reader holds the
/// transfer at 11 under the admission, leaves the one at 12 out, and agrees on both fields.
#[tokio::test]
async fn an_excluded_later_transfer_is_not_the_control_owner() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_admitted_owner", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    transferred(&fixture, 11, OWNER, "registry").await?;
    fixture
        .write(
            12,
            1,
            "AuthorityTransferred",
            V1_REGISTRY,
            Some(&name(1)),
            Some(&uuid(2)),
            json!({"node": node(1), "owner": OTHER, "owner_getter": OTHER,
                   "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            13,
            1,
            "TokenControlTransferred",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            json!({"authority_kind": "registrar", "from": OWNER, "to": OTHER}),
            REGISTRAR,
        )
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    let (served, shadow) = shadow_support::name(&fixture, 16, &name(1)).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    assert_eq!(served.control("registry_owner"), json!(OWNER));
    assert_eq!(shadow.control["registry_owner"], json!(OWNER));
    assert_eq!(
        served.control("latest_event_kind"),
        json!("TokenControlTransferred")
    );
    fixture.cleanup().await
}

/// The shape the removed `registry_node_position_moved_by_later_write` cause covered, from the
/// fixture corpus: a registry-only name whose node gets an AuthorityTransferred, then an
/// AuthorityEpochChanged registry_only, then a SubregistryChanged of the parent that names the
/// node as its child, all in one block. The reader counts the name's admitted
/// AuthorityTransferred rows F2c keeps and never the SubregistryChanged, as the served lateral
/// does, and the served control block reads the admitted AuthorityEpochChanged, which is later
/// than the transfer, for both its owner and its latest kind, so the families agree.
#[tokio::test]
async fn a_later_subregistry_write_to_the_node_leaves_the_epoch_owner_equal() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_later_write", 20).await?;
    let node_resource = uuid(3);
    fixture
        .binding(&uuid(100), &name(1), &node_resource, "ens_v1", 10, 1, None)
        .await?;
    fixture
        .write(
            10,
            9,
            "AuthorityTransferred",
            V1_REGISTRY,
            Some(&name(1)),
            Some(&node_resource),
            json!({"source_event": "NewOwner", "node": node(1), "owner": OWNER,
                   "owner_getter": OWNER}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            11,
            "AuthorityEpochChanged",
            V1_REGISTRY,
            Some(&name(1)),
            Some(&node_resource),
            json!({"node": node(1), "authority_kind": "registry_only", "owner": OWNER}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            12,
            "SubregistryChanged",
            V1_REGISTRY,
            Some(&name(1)),
            Some(&node_resource),
            json!({"source_event": "NewOwner", "node": node(2), "child_node": node(1),
                   "owner": OWNER}),
            REGISTRY,
        )
        .await?;
    let report = publish_and_compare(&fixture, 12).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let (served, _) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(served.control("registry_owner"), json!(OWNER));
    assert_eq!(
        served.control("latest_event_kind"),
        json!("AuthorityEpochChanged")
    );
    fixture.cleanup().await
}

/// Codex thread PRRT_kwDOSJpxAs6l3jx7, the ENSv1 NewOwner shape: one registry log yields a
/// SubregistryChanged and then an AuthorityTransferred of the child node, at one block,
/// transaction and log. The registry-binding observation keeps one of them per name: the
/// families break the tie by event identity and take the SubregistryChanged, today's builder
/// breaks it by generated id and takes the AuthorityTransferred (permission_resources.rs:41-57),
/// so the served event id differs. It passes as a same-block delta only because the binding
/// read again in today's order gives exactly the served binding. The control block reads the
/// name's admitted AuthorityTransferred rows only, which F2c keeps apart from the
/// SubregistryChanged, so both sides agree there.
#[tokio::test]
async fn a_new_owner_subregistry_and_transfer_at_one_log_is_a_same_block_delta() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_new_owner", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    for kind in ["SubregistryChanged", "AuthorityTransferred"] {
        fixture
            .write(
                11,
                1,
                kind,
                V1_REGISTRY,
                Some(&name(1)),
                Some(&lease),
                json!({"source_event": "NewOwner", "node": node(2), "child_node": node(1),
                       "owner": OTHER, "owner_getter": OTHER, "emitter_role": "registry"}),
                REGISTRY,
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, 12).await?;
    shadow_support::assert_counts(
        &report,
        &[],
        &[("d12_same_block_order:registry_binding/event_ids", 1)],
    );
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(served.control("registry_owner"), json!(OTHER));
    assert_eq!(
        served.control("latest_event_kind"),
        json!("AuthorityTransferred")
    );
    assert_eq!(shadow.control["registry_owner"], json!(OTHER));
    fixture.cleanup().await
}
