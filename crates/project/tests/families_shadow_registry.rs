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
use support::{CHAIN, Event, Fixture, uuid};

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const OTHER: &str = "0x00000000000000000000000000000000000000bb";
const THIRD: &str = "0x00000000000000000000000000000000000000cc";
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

/// Name 1's node gets an admitted AuthorityTransferred at 11 carrying `extra` in its payload,
/// then one at 12 on another resource that the admission leaves out, so the node row's latest
/// owner-setting event is the excluded one. The served control block reads the transfer at 11
/// (build.sql:650-663): null under an unmasked owner word, else its registry_owner, else its
/// owner. Step 2 keeps both facts on every owner-event row, so the reader takes them from the
/// transfer at 11 itself and agrees.
async fn earlier_transfer_wins(label: &str, extra: Value) -> Result<(Value, Value)> {
    let fixture = Fixture::new(label, 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    let mut payload = json!({"node": node(1), "owner": OWNER, "owner_getter": OWNER,
                             "emitter_role": "registry"});
    for (key, value) in extra.as_object().into_iter().flatten() {
        payload[key] = value.clone();
    }
    fixture
        .write(
            11,
            1,
            "AuthorityTransferred",
            V1_REGISTRY,
            Some(&name(1)),
            None,
            payload,
            REGISTRY,
        )
        .await?;
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
    let report = publish_and_compare(&fixture, 16).await?;
    let (served, shadow) = shadow_support::name(&fixture, 16, &name(1)).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let owners = (
        served.control("registry_owner"),
        shadow.control["registry_owner"].clone(),
    );
    fixture.cleanup().await?;
    Ok(owners)
}

#[tokio::test]
async fn an_earlier_admitted_transfer_with_an_unmasked_word_reports_no_owner() -> Result<()> {
    let (served, shadow) = earlier_transfer_wins(
        "families_shadow_registry_earlier_unmasked",
        json!({"owner_word_unmasked": true}),
    )
    .await?;
    assert_eq!(served, Value::Null);
    assert_eq!(shadow, Value::Null);
    Ok(())
}

/// A synthetic shape: the interpreter never puts registry_owner on a registry
/// AuthorityTransferred (it sets it on the registrar epoch observation only,
/// crates/adapters/src/schema_v2/protocol/v1/registrar.rs). The fixture exercises the served
/// COALESCE(registry_owner, owner) rule of build.sql:660-663, not a chain shape.
#[tokio::test]
async fn an_earlier_admitted_transfer_reports_its_registry_owner() -> Result<()> {
    let (served, shadow) = earlier_transfer_wins(
        "families_shadow_registry_earlier_registry_owner",
        json!({"registry_owner": OTHER}),
    )
    .await?;
    assert_eq!(served, json!(OTHER));
    assert_eq!(shadow, json!(OTHER));
    Ok(())
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

    // Each mutation of the canonical SubregistryChanged observation, its identity and position
    // unchanged, must stay a mismatch: the families' value is checked against the observation
    // rebuilt from the event log, not only the today's-order rebuild against the served one.
    let original: Value = sqlx::query_scalar(
        "SELECT to_jsonb(observation) FROM bigname_phase.project_registry_binding_observation
         observation WHERE chain_id = $1 AND observation_identity = $2",
    )
    .bind(CHAIN)
    .bind(name(1))
    .fetch_one(&fixture.pool)
    .await?;
    for (case, update, fields) in [
        (
            "registry owner",
            "registry_owner = '0x00000000000000000000000000000000000000aa'",
            &[
                "registry_binding/event_ids",
                "registry_binding/registry_owner",
            ][..],
        ),
        (
            "registry contract",
            "registry_contract = '0x00000000000000000000000000000000000000bb'",
            &[
                "registry_binding/event_ids",
                "registry_binding/registry_contract",
            ][..],
        ),
        (
            "applicability and clear",
            "applicable = false, clear_event_identity = event_identity",
            &[
                "registry_binding/block_number",
                "registry_binding/clear_event_id",
                "registry_binding/event_ids",
                "registry_binding/log_index",
                "registry_binding/registry_contract",
                "registry_binding/registry_owner",
                "registry_binding/transaction_index",
            ][..],
        ),
        (
            "event attribution",
            "normalized_event_id = 999999",
            &["registry_binding/event_ids"][..],
        ),
        (
            "missing id",
            "normalized_event_id = NULL",
            &["registry_binding/event_ids"][..],
        ),
    ] {
        sqlx::query(&format!(
            "UPDATE bigname_phase.project_registry_binding_observation SET {update}
             WHERE chain_id = $1 AND observation_identity = $2"
        ))
        .bind(CHAIN)
        .bind(name(1))
        .execute(&fixture.pool)
        .await?;
        let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
        assert!(
            mutated.expected_delta_fields.is_empty() && mutated.known_discrepancy.is_empty(),
            "{case} must not pass as a same-block delta: {:#?}",
            mutated.lines
        );
        assert_eq!(
            failed_fields(&mutated),
            fields,
            "{case}: {:#?}",
            mutated.lines
        );
        sqlx::query(
            "UPDATE bigname_phase.project_registry_binding_observation observation
             SET registry_owner = saved.registry_owner,
                 registry_contract = saved.registry_contract, applicable = saved.applicable,
                 clear_event_identity = saved.clear_event_identity,
                 normalized_event_id = saved.normalized_event_id
             FROM jsonb_populate_record(
                 NULL::bigname_phase.project_registry_binding_observation, $1) saved
             WHERE observation.chain_id = saved.chain_id
               AND observation.observation_identity = saved.observation_identity",
        )
        .bind(&original)
        .execute(&fixture.pool)
        .await?;
        let restored = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
        shadow_support::assert_counts(
            &restored,
            &[],
            &[("d12_same_block_order:registry_binding/event_ids", 1)],
        );
    }
    fixture.cleanup().await
}

/// The fields a comparison fails on, sorted.
fn failed_fields(report: &shadow_support::compare::Report) -> Vec<String> {
    let mut fields: Vec<String> = report
        .lines
        .iter()
        .filter(|line| line.starts_with("SEPOLIA_END_TO_END_SHADOW_MISMATCH"))
        .filter_map(|line| Some(line.split(" field=").nth(1)?.split(' ').next()?.to_owned()))
        .collect();
    fields.sort();
    fields
}

/// Three producer events of name 1 at one block, transaction and log, written in each of the
/// six orders: a SubregistryChanged whose getter is THIRD and two AuthorityTransferred events
/// whose getters are OWNER and OTHER. The families keep the SubregistryChanged, the greatest
/// identity, in every order; today's builder keeps the last one written, the highest generated
/// id. When that is the SubregistryChanged nothing differs; otherwise the registry owner and
/// event ids of the binding are same-block deltas, and the served owner is the getter of the
/// last event written.
#[tokio::test]
async fn three_producer_events_at_one_log_in_every_order() -> Result<()> {
    let events = [
        ("SubregistryChanged:11:1", "SubregistryChanged", THIRD),
        ("AuthorityTransferred:11:1:a", "AuthorityTransferred", OWNER),
        ("AuthorityTransferred:11:1:b", "AuthorityTransferred", OTHER),
    ];
    let orders = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for (index, order) in orders.iter().enumerate() {
        let fixture = Fixture::new(&format!("families_shadow_registry_three_{index}"), 20).await?;
        let lease = uuid(1);
        bound(&fixture, &lease).await?;
        for &event in order {
            let (identity, kind, getter) = events[event];
            fixture
                .event(
                    Event::new(identity, 11, 1, kind, V1_REGISTRY)
                        .name(&name(1))
                        .resource(&lease)
                        .after(json!({"source_event": "NewOwner", "node": node(2),
                                      "child_node": node(1), "owner": OTHER,
                                      "owner_getter": getter, "emitter_role": "registry"}))
                        .raw(json!({"emitting_address": REGISTRY})),
                )
                .await?;
        }
        let report = publish_and_compare(&fixture, 12).await?;
        let last = events[order[2]];
        let served = summary(&fixture, &lease).await?.expect("summarised");
        assert_eq!(served["registry_owner"], json!(last.2), "order {order:?}");
        if last.1 == "SubregistryChanged" {
            shadow_support::assert_counts(&report, &[], &[]);
        } else {
            shadow_support::assert_counts(
                &report,
                &[],
                &[
                    ("d12_same_block_order:registry_binding/event_ids", 1),
                    ("d12_same_block_order:registry_binding/registry_owner", 1),
                ],
            );
        }
        fixture.cleanup().await?;
    }
    Ok(())
}

/// Scoped review of ea047c04, F1: a registry-only NewOwner yields a SubregistryChanged, an
/// AuthorityTransferred and, when the authority resource changes, an AuthorityEpochChanged
/// registry_only from one raw log, at one block, transaction and log, in that push order. The
/// canonical order breaks the tie by identity and takes the transfer last (`E` sorts before
/// `T`); today's lateral takes the epoch change, whose generated id is higher
/// (build.sql:689-693). Both report one owner, so only the latest kind differs, and it passes
/// as a same-block delta only because the families read again in today's order, the owner
/// event and the epoch start included, give exactly the served kind.
#[tokio::test]
async fn a_registry_only_new_owner_transfer_and_epoch_at_one_log_is_a_same_block_delta()
-> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_one_log_epoch", 20).await?;
    let node_resource = uuid(3);
    fixture
        .binding(&uuid(100), &name(1), &node_resource, "ens_v1", 10, 1, None)
        .await?;
    for (kind, after) in [
        (
            "SubregistryChanged",
            json!({"source_event": "NewOwner", "node": node(2), "child_node": node(1),
                   "owner": OWNER}),
        ),
        (
            "AuthorityTransferred",
            json!({"source_event": "NewOwner", "node": node(2), "child_node": node(1),
                   "owner": OWNER, "owner_getter": OWNER}),
        ),
        (
            "AuthorityEpochChanged",
            json!({"node": node(1), "authority_kind": "registry_only", "owner": OWNER}),
        ),
    ] {
        fixture
            .write(
                10,
                9,
                kind,
                V1_REGISTRY,
                Some(&name(1)),
                Some(&node_resource),
                after,
                REGISTRY,
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, 12).await?;
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(
        served.control("latest_event_kind"),
        json!("AuthorityEpochChanged")
    );
    assert_eq!(
        shadow.control["latest_event_kind"],
        json!("AuthorityTransferred")
    );
    assert_eq!(served.control("registry_owner"), json!(OWNER));
    // The registry binding of the node is the NewOwner shape of
    // `a_new_owner_subregistry_and_transfer_at_one_log_is_a_same_block_delta`: the families keep
    // the SubregistryChanged, which carries no getter and so clears the binding, and today's
    // builder keeps the transfer; each binding field passes only because the whole binding read
    // in today's order equals the served one.
    let binding = [
        "block_number",
        "clear_event_id",
        "event_ids",
        "log_index",
        "registry_contract",
        "registry_owner",
        "transaction_index",
    ]
    .map(|field| format!("d12_same_block_order:registry_binding/{field}"));
    let mut delta: Vec<(&str, usize)> = binding.iter().map(|field| (field.as_str(), 1)).collect();
    delta.push(("d12_same_block_order:control/latest_event_kind", 1));
    shadow_support::assert_counts(&report, &[], &delta);
    fixture.cleanup().await
}

/// Item 4 of the ea047c04..2533ef55 review: a state-derived registry-only SurfaceBound and a
/// registry AuthorityTransferred at one block, transaction and log, the SurfaceBound pushed
/// first. Both feed the control owner (build.sql:664-667). The canonical order puts the
/// SurfaceBound last (its identity sorts after the transfer's) and reports its bound owner;
/// today's order puts the transfer last (higher id) and reports its owner. The same-block read
/// orders the binding candidates' SurfaceBound positions by generated id too, so the owner is a
/// same-block delta.
#[tokio::test]
async fn a_registry_only_surface_bound_and_transfer_at_one_log_is_a_same_block_delta() -> Result<()>
{
    let fixture = Fixture::new("families_shadow_registry_one_log_bound", 20).await?;
    let node_resource = uuid(3);
    fixture
        .binding(&uuid(100), &name(1), &node_resource, "ens_v1", 10, 9, None)
        .await?;
    fixture
        .write(
            10,
            9,
            "SurfaceBound",
            "registry_only_binding",
            Some(&name(1)),
            Some(&node_resource),
            json!({"authority_kind": "registry_only", "state_derived": true,
                   "registry_contract": REGISTRY, "owner": OTHER}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            9,
            "AuthorityTransferred",
            V1_REGISTRY,
            Some(&name(1)),
            Some(&node_resource),
            json!({"node": node(1), "owner": OWNER, "owner_getter": OWNER,
                   "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    let report = publish_and_compare(&fixture, 12).await?;
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(served.control("registry_owner"), json!(OWNER));
    assert_eq!(shadow.control["registry_owner"], json!(OTHER));
    shadow_support::assert_counts(
        &report,
        &[],
        &[("d12_same_block_order:control/registry_owner", 1)],
    );
    fixture.cleanup().await
}
