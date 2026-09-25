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
const OPERATOR: &str = "0x00000000000000000000000000000000000000e1";
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
    // Pro Q2 on a5f61182: an epoch start must be an AuthorityEpochChanged of the name filed
    // under its family's arm. Filed under another arm, or pointed at the transfer of the same
    // log with the transfer's (absent) kind and key, the start fails the guard.
    let facts = name_facts(&fixture).await?;
    assert!(shadow_support::compare::control_facts_hold(&fixture.pool, CHAIN, 12, &facts).await?);
    let start = facts.authority_starts["ens_v1"].clone();
    let mut other_arm = facts.clone();
    other_arm.authority_starts = json!({"basenames": start.clone()});
    assert!(
        !shadow_support::compare::control_facts_hold(&fixture.pool, CHAIN, 12, &other_arm).await?,
        "a start filed under another arm"
    );
    let mut other_kind = facts.clone();
    let mut transfer = start.clone();
    transfer["event_identity"] = json!("AuthorityTransferred:10:9");
    transfer["authority_kind"] = Value::Null;
    transfer["authority_key"] = Value::Null;
    other_kind.authority_starts = json!({"ens_v1": transfer});
    assert!(
        !shadow_support::compare::control_facts_hold(&fixture.pool, CHAIN, 12, &other_kind).await?,
        "a start that is not an epoch change"
    );
    // A wrong owner on the canonically selected transfer must stay a mismatch, though today's
    // order selects the epoch, which carries the served owner.
    sqlx::query(
        "UPDATE bigname_phase.project_registry_owner_event SET owner = $1
         WHERE event_kind = 'AuthorityTransferred'",
    )
    .bind(THIRD)
    .execute(&fixture.pool)
    .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert!(
        mutated.known_discrepancy.is_empty(),
        "a wrong transfer owner must not pass: {:#?}",
        mutated.lines
    );
    assert!(
        !mutated
            .expected_delta_fields
            .keys()
            .any(|field| field.contains(":control/")),
        "no control field passes: {:#?}",
        mutated.lines
    );
    assert!(
        failed_fields(&mutated).contains(&"control/registry_owner".to_owned()),
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// A state-derived registry-only SurfaceBound of name 1 on `node_resource` (bound owner OTHER)
/// and a registry AuthorityTransferred (OWNER) at block 10, transaction 0, log 9, the
/// SurfaceBound written first.
async fn one_log_bound(fixture: &Fixture, node_resource: &str) -> Result<()> {
    fixture
        .binding(&uuid(100), &name(1), node_resource, "ens_v1", 10, 9, None)
        .await?;
    fixture
        .write(
            10,
            9,
            "SurfaceBound",
            "registry_only_binding",
            Some(&name(1)),
            Some(node_resource),
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
            Some(node_resource),
            json!({"node": node(1), "owner": OWNER, "owner_getter": OWNER,
                   "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    Ok(())
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
    one_log_bound(&fixture, &node_resource).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(served.control("registry_owner"), json!(OWNER));
    assert_eq!(shadow.control["registry_owner"], json!(OTHER));
    shadow_support::assert_counts(
        &report,
        &[],
        &[("d12_same_block_order:control/registry_owner", 1)],
    );
    // A wrong bound owner on the canonically selected SurfaceBound must stay a mismatch, though
    // today's order still selects the transfer, which carries the served owner.
    sqlx::query(
        "UPDATE bigname_phase.project_binding_candidate SET bound_owner = $1
         WHERE resource_id = $2::uuid",
    )
    .bind(THIRD)
    .bind(&node_resource)
    .execute(&fixture.pool)
    .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert!(
        mutated.expected_delta_fields.is_empty() && mutated.known_discrepancy.is_empty(),
        "a wrong bound owner must not pass: {:#?}",
        mutated.lines
    );
    assert_eq!(failed_fields(&mutated), ["control/registry_owner"]);
    fixture.cleanup().await
}

/// Pro Q2 on a5f61182: a binding candidate's SurfaceBound must be of the candidate's name and
/// resource with its authority kind, key and state-derived flag, since those decide whether it
/// enters the registry-only owner pool. In the one-log SurfaceBound fixture the guard holds;
/// the candidate read with another resource, authority kind or state-derived flag fails it.
/// End to end, a registrar SurfaceBound (not in the pool) and the transfer at one log serve the
/// transfer's owner on both sides; making the family candidate registry-only and state-derived
/// puts its bound owner last canonically, while today's order still gives the transfer's, and
/// the owner must stay a mismatch. Deleting the SurfaceBound's log row leaves the comparison
/// complete with the owner a mismatch.
#[tokio::test]
async fn a_binding_candidate_is_checked_against_its_surface_bound() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_candidate_facts", 20).await?;
    let node_resource = uuid(3);
    one_log_bound(&fixture, &node_resource).await?;
    publish_and_compare(&fixture, 12).await?;
    let facts = name_facts(&fixture).await?;
    assert!(shadow_support::compare::control_facts_hold(&fixture.pool, CHAIN, 12, &facts).await?);
    let index = facts
        .candidates
        .iter()
        .position(|candidate| candidate.surface_bound_position.is_some())
        .expect("a candidate with its SurfaceBound");
    for (case, mutate) in [
        (
            "resource",
            (|candidate: &mut bigname_storage::families::control::rows::BindingCandidate| {
                candidate.resource_id = uuid(9);
            }) as fn(&mut _),
        ),
        ("authority kind", |candidate| {
            candidate.authority_kind = Some("registrar".into());
        }),
        ("state derived", |candidate| {
            candidate.state_derived = Some(false);
        }),
    ] {
        let mut mutated = facts.clone();
        mutate(&mut mutated.candidates[index]);
        assert!(
            !shadow_support::compare::control_facts_hold(&fixture.pool, CHAIN, 12, &mutated)
                .await?,
            "{case}"
        );
    }
    sqlx::query("DELETE FROM normalized_events WHERE event_identity = 'SurfaceBound:10:9'")
        .execute(&fixture.pool)
        .await?;
    let missing = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert!(
        missing.expected_delta_fields.is_empty() && missing.known_discrepancy.is_empty(),
        "a missing log row must not pass: {:#?}",
        missing.lines
    );
    assert_eq!(failed_fields(&missing), ["control/registry_owner"]);
    fixture.cleanup().await?;

    let fixture = Fixture::new("families_shadow_registry_candidate_pool", 20).await?;
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
            json!({"authority_kind": "registrar", "state_derived": false,
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
    shadow_support::assert_counts(&report, &[], &[]);
    let (served, _) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(served.control("registry_owner"), json!(OWNER));
    sqlx::query(
        "UPDATE bigname_phase.project_binding_candidate
         SET authority_kind = 'registry_only', state_derived = true
         WHERE resource_id = $1::uuid",
    )
    .bind(&node_resource)
    .execute(&fixture.pool)
    .await?;
    let (_, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(
        shadow.control["registry_owner"],
        json!(OTHER),
        "the pool took it"
    );
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert!(
        mutated.expected_delta_fields.is_empty() && mutated.known_discrepancy.is_empty(),
        "a wrong candidate kind must not pass: {:#?}",
        mutated.lines
    );
    assert!(
        failed_fields(&mutated).contains(&"control/registry_owner".to_owned()),
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// Pro Q1 on ea047c04: F1 keeps one epoch start per arm, the latest, so an older admitted
/// AuthorityEpochChanged behind a newer one of the same arm that the admission leaves out is
/// not seen. Name 1's lease gets an epoch at 11 to THIRD; an epoch at 12 on another resource,
/// which the admission leaves out (authority_events.sql admits a resource-bearing event only on
/// the selected resource), replaces it in F1. The served control block reads the epoch at 11
/// (owner THIRD, latest kind AuthorityEpochChanged); the families have no admitted epoch and
/// serve null for both. This is a step 2 retention dependency, pinned as a mismatch with no
/// excuse: closing it needs F1 to keep every epoch of an arm, not only the latest.
#[tokio::test]
async fn an_older_admitted_epoch_behind_an_excluded_one_is_a_retention_gap() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_older_epoch", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    for (block, resource, owner) in [(11, uuid(1), THIRD), (12, uuid(2), OTHER)] {
        fixture
            .write(
                block,
                1,
                "AuthorityEpochChanged",
                V1_REGISTRAR,
                Some(&name(1)),
                Some(&resource),
                json!({"node": node(1), "authority_kind": "registrar", "owner": owner}),
                REGISTRAR,
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, 16).await?;
    let (served, shadow) = shadow_support::name(&fixture, 16, &name(1)).await?;
    assert_eq!(served.control("registry_owner"), json!(THIRD));
    assert_eq!(
        served.control("latest_event_kind"),
        json!("AuthorityEpochChanged")
    );
    assert_eq!(shadow.control["registry_owner"], Value::Null);
    assert_eq!(shadow.control["latest_event_kind"], Value::Null);
    assert!(
        report.expected_delta_fields.is_empty()
            && report.known_discrepancy.is_empty()
            && report.mismatched == 1,
        "{:#?}",
        report.lines
    );
    assert_eq!(
        failed_fields(&report),
        ["control/latest_event_kind", "control/registry_owner"]
    );
    fixture.cleanup().await
}

/// Pro Q3 on ea047c04, the positive ownerless profile: name 1 has no binding, and its node gets
/// an AuthorityTransferred naming it whose getter reads the zero address, then a
/// SubregistryChanged of the node. The latest transfer is still the zero-getter one, so both
/// sides serve the profile. Only the eligibility is compared: the served getter reason and
/// resource are not.
#[tokio::test]
async fn a_bindingless_zero_transfer_then_a_subregistry_write_is_ownerless() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_ownerless", 20).await?;
    transferred(&fixture, 11, ZERO, "registry").await?;
    fixture
        .write(
            12,
            1,
            "SubregistryChanged",
            V1_REGISTRY,
            Some(&name(1)),
            None,
            json!({"node": node(1), "owner": OTHER, "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    assert_eq!(
        selection(&fixture).await?["ownerless_registry"],
        json!(true)
    );
    fixture.cleanup().await
}

/// Codex thread PRRT_kwDOSJpxAs6l63Qy: a registry-binding same-block delta that changes the
/// binding's owner moves the registry-operator rows with it. A SubregistryChanged whose getter
/// is THIRD and an AuthorityTransferred whose getter is OTHER share one log, written in that
/// order; OTHER has approved OPERATOR on the registry. Today's binding is the transfer's
/// (OTHER), so the summary serves OPERATOR's row; the canonical binding is the
/// SubregistryChanged's (THIRD), so the families serve none. The operator difference passes
/// only because the rows computed from each rebuilt binding equal the served and shadow rows.
#[tokio::test]
async fn a_same_block_binding_delta_moves_the_registry_operator_rows() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_operator_delta", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    for (kind, getter) in [
        ("SubregistryChanged", THIRD),
        ("AuthorityTransferred", OTHER),
    ] {
        fixture
            .write(
                11,
                1,
                kind,
                V1_REGISTRY,
                Some(&name(1)),
                Some(&lease),
                json!({"source_event": "NewOwner", "node": node(2), "child_node": node(1),
                       "owner": OTHER, "owner_getter": getter, "emitter_role": "registry"}),
                REGISTRY,
            )
            .await?;
    }
    fixture
        .event(
            Event::new(
                "registry-approval",
                10,
                5,
                "AccountPermissionChanged",
                V1_REGISTRY,
            )
            .after(json!({
                "subject": OPERATOR, "relation_kind": "operator", "approved": true,
                "scope": {"kind": "account", "chain_id": CHAIN, "authority_kind": "registry",
                          "authority_contract": REGISTRY,
                          "authority_contract_instance_id": "00000000-0000-0000-0000-0000000000e5",
                          "owner": OTHER},
                "effective_powers": ["registry_control"],
                "grant_source": {"kind": "raw_log", "source_event": "ApprovalForAll"},
                "revocation_source": null, "inheritance_path": [],
                "transfer_behavior": {"mode": "owner_scoped", "on_holder_change": "ceases_to_apply"},
                "source_event": "ApprovalForAll",
            }))
            .raw(json!({"emitting_address": REGISTRY})),
        )
        .await?;
    let report = publish_and_compare(&fixture, 12).await?;
    shadow_support::assert_counts(
        &report,
        &[],
        &[
            ("d12_same_block_order:effective_operator_rows", 1),
            ("d12_same_block_order:registry_binding/event_ids", 1),
            ("d12_same_block_order:registry_binding/registry_owner", 1),
        ],
    );
    fixture.cleanup().await
}

/// Pro Q4 on a5f61182: the operator rows pass with the binding only as far as the approvals
/// the rows are computed from are right, and those come from the same family table, so an
/// approval only the canonical binding uses is not checked by that excuse. The operator-delta
/// fixture with THIRD, the canonical binding's owner, also approving FOURTH: the families
/// serve FOURTH's row and today's builder OPERATOR's, a same-block delta. Giving the family's
/// THIRD approval another subject moves the shadow rows and the rows computed from the
/// canonical rebuild together, so the operator field may still pass; the account comparison,
/// which compares every approval with its served row and excuses nothing, must fail the run.
#[tokio::test]
async fn a_wrong_approval_only_the_canonical_binding_uses_fails_the_account_rows() -> Result<()> {
    const FOURTH: &str = "0x00000000000000000000000000000000000000dd";
    let fixture = Fixture::new("families_shadow_registry_canonical_approval", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    for (kind, getter) in [
        ("SubregistryChanged", THIRD),
        ("AuthorityTransferred", OTHER),
    ] {
        fixture
            .write(
                11,
                1,
                kind,
                V1_REGISTRY,
                Some(&name(1)),
                Some(&lease),
                json!({"source_event": "NewOwner", "node": node(2), "child_node": node(1),
                       "owner": OTHER, "owner_getter": getter, "emitter_role": "registry"}),
                REGISTRY,
            )
            .await?;
    }
    for (identity, owner, subject) in [
        ("approval-other", OTHER, OPERATOR),
        ("approval-third", THIRD, FOURTH),
    ] {
        fixture
            .event(
                Event::new(identity, 10, 5, "AccountPermissionChanged", V1_REGISTRY)
                    .at(if owner == OTHER { 0 } else { 1 }, 5)
                    .after(json!({
                        "subject": subject, "relation_kind": "operator", "approved": true,
                        "scope": {"kind": "account", "chain_id": CHAIN,
                                  "authority_kind": "registry", "authority_contract": REGISTRY,
                                  "authority_contract_instance_id":
                                      "00000000-0000-0000-0000-0000000000e5",
                                  "owner": owner},
                        "effective_powers": ["registry_control"],
                        "grant_source": {"kind": "raw_log", "source_event": "ApprovalForAll"},
                        "revocation_source": null, "inheritance_path": [],
                        "transfer_behavior": {"mode": "owner_scoped",
                                              "on_holder_change": "ceases_to_apply"},
                        "source_event": "ApprovalForAll",
                    }))
                    .raw(json!({"emitting_address": REGISTRY})),
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, 12).await?;
    shadow_support::assert_counts(
        &report,
        &[],
        &[
            ("d12_same_block_order:effective_operator_rows", 1),
            ("d12_same_block_order:registry_binding/event_ids", 1),
            ("d12_same_block_order:registry_binding/registry_owner", 1),
        ],
    );
    sqlx::query(
        "UPDATE bigname_phase.project_account_approval SET subject = $1
         WHERE owner = $2 AND subject = $3",
    )
    .bind(OTHER)
    .bind(THIRD)
    .bind(FOURTH)
    .execute(&fixture.pool)
    .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert_eq!(
        failed_fields(&mutated),
        [
            "account_permission_state_current",
            "account_permission_state_current"
        ],
        "the served FOURTH approval and the family's wrong one: {:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// An unnamed registry event of the lease: identity key the lease resource, so it reaches the
/// lease as its own observation.
async fn unnamed_on(
    fixture: &Fixture,
    identity: &str,
    block: i64,
    resource: &str,
    getter: &str,
) -> Result<i64> {
    fixture
        .event(
            Event::new(identity, block, 1, "AuthorityTransferred", V1_REGISTRY)
                .resource(resource)
                .after(json!({"owner": getter, "owner_getter": getter,
                              "emitter_role": "registry"}))
                .raw(json!({"emitting_address": REGISTRY})),
        )
        .await
}

/// A named NewOwner-shaped registry event of name 1 on the lease.
async fn named_on(
    fixture: &Fixture,
    identity: &str,
    kind: &str,
    resource: &str,
    getter: &str,
) -> Result<i64> {
    fixture
        .event(
            Event::new(identity, 11, 1, kind, V1_REGISTRY)
                .name(&name(1))
                .resource(resource)
                .after(
                    json!({"source_event": "NewOwner", "node": node(2), "child_node": node(1),
                              "owner": OTHER, "owner_getter": getter,
                              "emitter_role": "registry"}),
                )
                .raw(json!({"emitting_address": REGISTRY})),
        )
        .await
}

/// Pro Q1 on 6cc8aa1e, the coherent stale row. Two observation identities reach the lease: the
/// lease itself (A, unnamed events) and name 1 (B). At block 10 A has z-10 (getter OWNER); at
/// block 11, one position, A has z-11 (THIRD) and B has m-11 (a SubregistryChanged, FOURTH) and
/// then a-11 (an AuthorityTransferred, OTHER), written in that order. Canonically A keeps z-11,
/// B keeps m-11 and the lease takes z-11, the greatest identity; today B keeps a-11 and the
/// lease takes it, the highest id. That is a legitimate same-block delta. Replacing A's family
/// row with its whole, self-consistent block-10 predecessor makes the families' binding m-11:
/// wrong, and it must stay a mismatch, because the rebuild takes A's latest event from the
/// event log rather than the position the family row names.
#[tokio::test]
async fn a_coherent_stale_observation_row_stays_a_mismatch() -> Result<()> {
    const FOURTH: &str = "0x00000000000000000000000000000000000000dd";
    let fixture = Fixture::new("families_shadow_registry_stale_row", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    let old_id = unnamed_on(&fixture, "z-10", 10, &lease, OWNER).await?;
    unnamed_on(&fixture, "z-11", 11, &lease, THIRD).await?;
    named_on(&fixture, "m-11", "SubregistryChanged", &lease, FOURTH).await?;
    named_on(&fixture, "a-11", "AuthorityTransferred", &lease, OTHER).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    shadow_support::assert_counts(
        &report,
        &[],
        &[
            ("d12_same_block_order:registry_binding/event_ids", 1),
            ("d12_same_block_order:registry_binding/registry_owner", 1),
        ],
    );
    assert_eq!(
        summary(&fixture, &lease).await?.expect("summarised")["registry_owner"],
        json!(OTHER)
    );
    sqlx::query(
        "UPDATE bigname_phase.project_registry_binding_observation
         SET block_number = 10, transaction_index = 0, log_index = 1, event_identity = 'z-10',
             normalized_event_id = $3, registry_owner = $4
         WHERE chain_id = $1 AND observation_identity = $2",
    )
    .bind(CHAIN)
    .bind(&lease)
    .bind(old_id)
    .bind(OWNER)
    .execute(&fixture.pool)
    .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert!(
        mutated.expected_delta_fields.is_empty() && mutated.known_discrepancy.is_empty(),
        "a stale family row must not pass: {:#?}",
        mutated.lines
    );
    assert_eq!(
        failed_fields(&mutated),
        [
            "registry_binding/event_ids",
            "registry_binding/registry_owner"
        ],
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// Pro Q1 on 6cc8aa1e: two AuthorityTransferred events of name 1 at one position with the same
/// payload and distinct identities, b written first. The families keep b (the greater
/// identity), today a (the higher id); the binding differs only in the event it names, an
/// event_ids-only same-block delta.
#[tokio::test]
async fn two_same_payload_events_with_distinct_identities_differ_in_event_ids_only() -> Result<()> {
    let fixture = Fixture::new("families_shadow_registry_same_payload", 20).await?;
    let lease = uuid(1);
    bound(&fixture, &lease).await?;
    named_on(
        &fixture,
        "AuthorityTransferred:11:1:b",
        "AuthorityTransferred",
        &lease,
        OTHER,
    )
    .await?;
    named_on(
        &fixture,
        "AuthorityTransferred:11:1:a",
        "AuthorityTransferred",
        &lease,
        OTHER,
    )
    .await?;
    let report = publish_and_compare(&fixture, 12).await?;
    shadow_support::assert_counts(
        &report,
        &[],
        &[("d12_same_block_order:registry_binding/event_ids", 1)],
    );
    fixture.cleanup().await
}

/// Pro Q1 on 6cc8aa1e: each condition of the registry-binding delta rejects on its own, the
/// others holding. The event log's identities are unique (normalized_events.event_identity),
/// so two deliveries of one identity cannot differ by id there; the same-identity case is
/// the selected-identity condition below.
#[test]
fn each_condition_of_the_binding_delta_rejects_on_its_own() {
    use bigname_storage::families::control::{position::Position, registry::RegistryBinding};
    use shadow_support::compare::{BindingOrders, binding_json};
    use std::collections::{BTreeMap, BTreeSet};

    let binding = |identity: &str, owner: &str, id: i64| RegistryBinding {
        registry_owner: Some(owner.to_owned()),
        registry_contract: Some(REGISTRY.to_owned()),
        position: Some(Position {
            block_number: 11,
            transaction_index: Some(0),
            log_index: Some(1),
            event_identity: identity.to_owned(),
        }),
        normalized_event_id: Some(id),
        ..RegistryBinding::default()
    };
    let orders =
        |canonical: RegistryBinding, legacy: RegistryBinding, unverified: bool| BindingOrders {
            canonical: BTreeMap::from([("r".to_owned(), canonical)]),
            legacy: BTreeMap::from([("r".to_owned(), legacy)]),
            unverified: if unverified {
                BTreeSet::from(["r".to_owned()])
            } else {
                BTreeSet::new()
            },
        };
    let (m, a) = (binding("m", THIRD, 3), binding("a", OTHER, 4));
    let (shadow, served) = (binding_json(&m), binding_json(&a));
    assert!(orders(m.clone(), a.clone(), false).same_block_delta("r", &served, &shadow));
    // A canonical rebuild that is not the shadow value.
    assert!(
        !orders(binding("m", OWNER, 3), a.clone(), false).same_block_delta("r", &served, &shadow)
    );
    // A today's rebuild that is not the served value.
    assert!(
        !orders(m.clone(), binding("a", OWNER, 4), false).same_block_delta("r", &served, &shadow)
    );
    // Both orders select one event identity, however their ids differ.
    let same_identity = binding("m", OTHER, 4);
    assert!(
        !orders(m.clone(), same_identity.clone(), false).same_block_delta(
            "r",
            &binding_json(&same_identity),
            &shadow
        )
    );
    // A resource an unverified family observation reaches.
    assert!(!orders(m, a, true).same_block_delta("r", &served, &shadow));
}

/// Pro Q1 on a5f61182: the registry rebuild reads only the publication-visible event log, the
/// set family intake reads (crates/project/src/families/input.rs:175-195, :309-319): activated,
/// canonical, at the canonical lineage's hash for its height, at or below the target. Name 1's
/// visible AuthorityTransferred a-visible (getter OTHER) is what both sides serve; each case
/// then points the families at a wrong selection, which must stay a mismatch rather than pass
/// as a same-block delta:
/// - a rival z-rival at a-visible's position, written first (lower id), that is a candidate,
///   or canonical-looking but at an orphaned hash of block 11, or at block 13 past the target;
///   the family observation of name 1 is repointed at it with its getter THIRD. Read without
///   the predicate, the canonical rebuild would take z-rival (its identity sorts last) and
///   today's a-visible (higher id), and the difference would pass.
/// - an extra family observation of the lease, which the event log does not hold.
/// - a missing one: an unnamed z-lease (THIRD) on the lease, then m-name (a SubregistryChanged
///   of name 1, FOURTH) and a-visible at one position. Canonically the lease takes z-lease,
///   today a-visible, a legitimate delta; dropping the lease's family row makes the families
///   serve m-name.
#[tokio::test]
async fn a_family_selection_outside_the_published_log_stays_a_mismatch() -> Result<()> {
    const FOURTH: &str = "0x00000000000000000000000000000000000000dd";
    const ORPHAN: &str = "0x00000000000000000000000000000000000000000000000000000000000dead0";
    for case in [
        "unactivated",
        "wrong lineage",
        "past the target",
        "extra",
        "missing",
    ] {
        let fixture = Fixture::new("families_shadow_registry_unpublished", 20).await?;
        let lease = uuid(1);
        bound(&fixture, &lease).await?;
        let rival_block = if case == "past the target" { 13 } else { 11 };
        let name_1 = name(1);
        let rival_id = if matches!(case, "extra" | "missing") {
            None
        } else {
            let event = Event::new(
                "z-rival",
                rival_block,
                1,
                "AuthorityTransferred",
                V1_REGISTRY,
            )
            .name(&name_1)
            .resource(&lease)
            .after(
                json!({"source_event": "NewOwner", "node": node(2), "child_node": node(1),
                           "owner": OTHER, "owner_getter": THIRD, "emitter_role": "registry"}),
            )
            .raw(json!({"emitting_address": REGISTRY}));
            Some(fixture.event(event).await?)
        };
        if case == "missing" {
            unnamed_on(&fixture, "z-lease", 11, &lease, THIRD).await?;
            named_on(&fixture, "m-name", "SubregistryChanged", &lease, FOURTH).await?;
        }
        named_on(&fixture, "a-visible", "AuthorityTransferred", &lease, OTHER).await?;
        match case {
            "unactivated" => {
                sqlx::query(
                    "UPDATE normalized_events SET consumer_visibility = 'candidate',
                         migration_correlation_ids = ARRAY['fixture']
                     WHERE event_identity = 'z-rival'",
                )
                .execute(&fixture.pool)
                .await?;
            }
            "wrong lineage" => {
                sqlx::query(
                    "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                         block_timestamp, canonicality_state)
                     VALUES ($1, $2, $3, 11, to_timestamp(1800000132), 'orphaned')",
                )
                .bind(CHAIN)
                .bind(ORPHAN)
                .bind(support::hash(10))
                .execute(&fixture.pool)
                .await?;
                sqlx::query(
                    "UPDATE normalized_events SET block_hash = $1 WHERE event_identity = 'z-rival'",
                )
                .bind(ORPHAN)
                .execute(&fixture.pool)
                .await?;
            }
            _ => {}
        }
        let report = publish_and_compare(&fixture, 12).await?;
        let baseline: &[(&str, usize)] = if case == "missing" {
            &[
                ("d12_same_block_order:registry_binding/event_ids", 1),
                ("d12_same_block_order:registry_binding/registry_owner", 1),
            ]
        } else {
            &[]
        };
        shadow_support::assert_counts(&report, &[], baseline);
        assert_eq!(
            summary(&fixture, &lease).await?.expect("summarised")["registry_owner"],
            json!(OTHER),
            "{case}"
        );
        let (mutation, fields): (String, &[&str]) = match case {
            "extra" => (
                format!(
                    "INSERT INTO bigname_phase.project_registry_binding_observation
                     SELECT (jsonb_populate_record(
                         NULL::bigname_phase.project_registry_binding_observation,
                         to_jsonb(observation) || jsonb_build_object(
                             'observation_identity', '{lease}', 'logical_name_id', NULL,
                             'attributed_via', 'own', 'event_identity', 'z-extra',
                             'registry_owner', '{THIRD}', 'normalized_event_id', 999999))).*
                     FROM bigname_phase.project_registry_binding_observation observation
                     WHERE observation.observation_identity = $1"
                ),
                &[
                    "registry_binding/event_ids",
                    "registry_binding/registry_owner",
                ],
            ),
            "missing" => (
                "DELETE FROM bigname_phase.project_registry_binding_observation
                 WHERE observation_identity <> $1"
                    .to_owned(),
                &[
                    "registry_binding/event_ids",
                    "registry_binding/registry_owner",
                ],
            ),
            _ => (
                format!(
                    "UPDATE bigname_phase.project_registry_binding_observation
                     SET event_identity = 'z-rival', block_number = {rival_block},
                         normalized_event_id = {}, registry_owner = '{THIRD}'
                     WHERE observation_identity = $1",
                    rival_id.expect("a rival")
                ),
                if case == "past the target" {
                    &[
                        "registry_binding/block_number",
                        "registry_binding/event_ids",
                        "registry_binding/registry_owner",
                    ]
                } else {
                    &[
                        "registry_binding/event_ids",
                        "registry_binding/registry_owner",
                    ]
                },
            ),
        };
        sqlx::query(&mutation)
            .bind(name(1))
            .execute(&fixture.pool)
            .await?;
        let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
        assert!(
            mutated.expected_delta_fields.is_empty() && mutated.known_discrepancy.is_empty(),
            "{case}: a wrong family selection must not pass: {:#?}",
            mutated.lines
        );
        assert_eq!(
            failed_fields(&mutated),
            fields,
            "{case}: {:#?}",
            mutated.lines
        );
        fixture.cleanup().await?;
    }
    Ok(())
}

/// Name 1's facts as the harness loads them.
async fn name_facts(
    fixture: &Fixture,
) -> Result<bigname_storage::families::control::lifecycle::NameFacts> {
    use bigname_storage::families::control::lifecycle::{
        AuthoritySelection, NameInput, load_name_facts,
    };
    let rows =
        bigname_storage::load_name_current_by_logical_name_ids(&fixture.pool, &[name(1)]).await?;
    let row = &rows[&name(1)];
    let input = NameInput {
        logical_name_id: row.logical_name_id.clone(),
        namehash: row.namehash.to_ascii_lowercase(),
        selection: AuthoritySelection::from_provenance(&row.provenance),
    };
    Ok(load_name_facts(&fixture.pool, CHAIN, &[input])
        .await?
        .pop()
        .expect("name 1 has facts"))
}

/// Pro Q1 on a5f61182: the control-fact guard and the generated ids read only the
/// publication-visible event log. In the one-log SurfaceBound fixture the owner difference is a
/// same-block delta; the transfer's log row is then made a candidate, or moved to an orphaned
/// hash of its block, after publication. Each read on its own then no longer finds it, and the
/// whole comparison leaves the owner a mismatch. Read at a target before block 10, neither
/// finds the block's events.
#[tokio::test]
async fn the_control_guard_and_generated_ids_read_the_published_log_only() -> Result<()> {
    use shadow_support::compare::{control_facts_hold, control_positions, generated_ids};
    const ORPHAN: &str = "0x00000000000000000000000000000000000000000000000000000000000dead0";
    const TRANSFER: &str = "AuthorityTransferred:10:9";
    for case in ["unactivated", "wrong lineage"] {
        let fixture = Fixture::new("families_shadow_registry_published_control", 20).await?;
        let node_resource = uuid(3);
        one_log_bound(&fixture, &node_resource).await?;
        let report = publish_and_compare(&fixture, 12).await?;
        shadow_support::assert_counts(
            &report,
            &[],
            &[("d12_same_block_order:control/registry_owner", 1)],
        );
        let facts = name_facts(&fixture).await?;
        let identities: Vec<String> = control_positions(&facts)
            .into_iter()
            .map(|position| position.event_identity)
            .collect();
        assert!(identities.iter().any(|identity| identity == TRANSFER));
        assert!(control_facts_hold(&fixture.pool, CHAIN, 12, &facts).await?);
        assert!(!control_facts_hold(&fixture.pool, CHAIN, 9, &facts).await?);
        assert!(
            generated_ids(&fixture.pool, CHAIN, 9, &identities)
                .await?
                .is_empty()
        );
        if case == "unactivated" {
            sqlx::query(
                "UPDATE normalized_events SET consumer_visibility = 'candidate',
                     migration_correlation_ids = ARRAY['fixture']
                 WHERE event_identity = $1",
            )
            .bind(TRANSFER)
            .execute(&fixture.pool)
            .await?;
        } else {
            sqlx::query(
                "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                     block_timestamp, canonicality_state)
                 VALUES ($1, $2, $3, 10, to_timestamp(1800000120), 'orphaned')",
            )
            .bind(CHAIN)
            .bind(ORPHAN)
            .bind(support::hash(9))
            .execute(&fixture.pool)
            .await?;
            sqlx::query("UPDATE normalized_events SET block_hash = $1 WHERE event_identity = $2")
                .bind(ORPHAN)
                .bind(TRANSFER)
                .execute(&fixture.pool)
                .await?;
        }
        assert!(
            !control_facts_hold(&fixture.pool, CHAIN, 12, &facts).await?,
            "{case}: the guard must not find the transfer"
        );
        assert!(
            !generated_ids(&fixture.pool, CHAIN, 12, &identities)
                .await?
                .contains_key(TRANSFER),
            "{case}: no generated id for the transfer"
        );
        let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
        assert!(
            mutated.expected_delta_fields.is_empty() && mutated.known_discrepancy.is_empty(),
            "{case}: {:#?}",
            mutated.lines
        );
        assert_eq!(
            failed_fields(&mutated),
            ["control/registry_owner"],
            "{case}: {:#?}",
            mutated.lines
        );
        fixture.cleanup().await?;
    }
    Ok(())
}
