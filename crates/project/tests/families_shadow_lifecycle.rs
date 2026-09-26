//! Lifecycle shadow reads (TYR-36 step 3, docs/glossary.md "Shadow read"): the F2a key state,
//! triple summaries, association and retained events, with the F1 identity facts, give each
//! name's registration and control blocks. Each case publishes with the production batch, follows
//! it with the families, and compares the family read with what name_current serves from today's
//! tables at the same publication; the case also pins the served values it was written for, so a
//! comparator that silently agrees on an empty read fails.
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use bigname_storage::families::control::lifecycle::ShadowName;
use serde_json::{Value, json};
use shadow_support::{Served, assert_counts, publish, publish_and_compare};
use support::{CHAIN, Event, Fixture, uuid};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const ALICE: &str = "0x00000000000000000000000000000000000000aa";
const BOB: &str = "0x00000000000000000000000000000000000000bb";
const V2_REGISTRY: &str = "ens_v2_registry_l1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// An ENSv2 registry lifecycle event of triple (name 1, registry R, token 7).
async fn v2(
    fixture: &Fixture,
    block: i64,
    kind: &str,
    resource: Option<&str>,
    after: Value,
) -> Result<i64> {
    let mut after = after;
    after["registry_contract_instance_id"] = json!("R");
    after["token_id"] = json!("7");
    after["authority_kind"] = json!("registrar");
    fixture
        .write(
            block,
            1,
            kind,
            V2_REGISTRY,
            Some(&name(1)),
            resource,
            after,
            REGISTRY,
        )
        .await
}

fn path_expiry(expiry: i64) -> Value {
    json!({"source_event": "RegistryPathExpired", "derived_from": "interpreter_state",
           "terminal_reason": "registry_name_binding_expired", "expiry": expiry})
}

/// The interpreter's path-expiry release of `resource` in the shape the adapter emits it: the
/// resource and no name, with no transaction or log (a block-boundary event), and the path-expiry
/// facts (crates/adapters/src/schema_v2/protocol/v2_registry/expiry.rs:57-64).
async fn unnamed_path_expiry(
    fixture: &Fixture,
    identity: &str,
    block: i64,
    resource: &str,
    expiry: i64,
) -> Result<()> {
    let mut after = path_expiry(expiry);
    after["registry_contract_instance_id"] = json!("R");
    after["token_id"] = json!("7");
    fixture
        .event(
            Event::new(identity, block, 0, "RegistrationReleased", V2_REGISTRY)
                .resource(resource)
                .after(after)
                .raw(json!({"emitting_address": REGISTRY}))
                .synthesised(),
        )
        .await?;
    Ok(())
}

/// The name's selected event, status and expiry as the shadow reads them.
async fn shadow_reads(fixture: &Fixture, target: i64) -> Result<(Served, ShadowName)> {
    shadow_support::name(fixture, target, &name(1)).await
}

/// The registration and control fields of name 1, served and shadow, asserted equal.
async fn assert_name_equal(fixture: &Fixture, target: i64) -> Result<(Served, Value)> {
    let (served, shadow) = shadow_support::name(fixture, target, &name(1)).await?;
    for field in [
        "status",
        "expiry",
        "registrant",
        "registered_at",
        "released_at",
        "latest_event_kind",
    ] {
        assert_eq!(
            shadow
                .registration
                .get(field)
                .cloned()
                .unwrap_or(Value::Null),
            served.registration(field),
            "registration/{field} at {target}, trace {:?}",
            shadow.trace
        );
    }
    for field in ["status", "expiry", "registrant", "latest_event_kind"] {
        assert_eq!(
            shadow.control.get(field).cloned().unwrap_or(Value::Null),
            served.control(field),
            "control/{field} at {target}, trace {:?}",
            shadow.trace
        );
    }
    Ok((served, Value::Object(shadow.trace)))
}

/// Name 1's ENSv2 binding to `resource` at block 9 with the SurfaceBound that opened it (the
/// families read a binding in the block of its event, as Interpret writes them).
async fn v2_binding(fixture: &Fixture, resource: &str) -> Result<()> {
    fixture
        .binding(&uuid(100), &name(1), resource, "ens_v2", 9, 0, None)
        .await?;
    fixture
        .write(
            9,
            0,
            "SurfaceBound",
            V2_REGISTRY,
            Some(&name(1)),
            Some(resource),
            json!({"authority_kind": "registrar", "state_derived": false}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn a_grant_serves_its_registrant_and_expiry() -> Result<()> {
    let fixture = Fixture::new("families_shadow_grant", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 12).await?;
    assert_eq!(report.names, 1);
    shadow_support::assert_counts(&report, &[], &[]);
    assert_eq!(report.equal, report.names + report.resources);
    let (served, _) = assert_name_equal(&fixture, 12).await?;
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("RegistrationGranted")
    );
    assert_eq!(served.registration("registrant"), json!(ALICE));
    fixture.cleanup().await
}

#[tokio::test]
async fn grant_then_expiry_change_serves_the_expiry_change() -> Result<()> {
    let fixture = Fixture::new("families_shadow_grant_expiry", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    v2(
        &fixture,
        12,
        "ExpiryChanged",
        Some(&k1),
        json!({"expiry": 2_100_000_000u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 14).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let (served, _) = assert_name_equal(&fixture, 14).await?;
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("ExpiryChanged")
    );
    assert_eq!(served.registration("expiry"), json!(2_100_000_000u64));
    assert_eq!(served.registration("status"), json!("active"));
    fixture.cleanup().await
}

/// G, P, X: a grant, the interpreter's unnamed path-expiry release on the same resource, then an
/// expiry change. The key's candidate is the path release, so the families serve the name
/// released with the lapsed expiry the later change set; today's name-scoped membership never
/// sees the release and serves the grant as active. Every differing field is the disclosed
/// served-side difference, counted exactly.
#[tokio::test]
async fn grant_path_expiry_then_expiry_change_serves_the_path_release() -> Result<()> {
    let fixture = Fixture::new("families_shadow_path_expiry", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 1_800_000_100u64}),
    )
    .await?;
    unnamed_path_expiry(&fixture, "path-expiry", 12, &k1, 1_800_000_100).await?;
    v2(
        &fixture,
        14,
        "ExpiryChanged",
        Some(&k1),
        json!({"expiry": 1_800_000_200u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    let cause = "served_membership_skips_unnamed_path_expiry";
    assert_counts(
        &report,
        &[
            (&format!("{cause}:registration/status"), 1),
            (&format!("{cause}:registration/authority_kind"), 1),
            (&format!("{cause}:registration/registrant"), 1),
            (&format!("{cause}:control/status"), 1),
            (&format!("{cause}:control/expiry"), 1),
            (&format!("{cause}:control/registrant"), 1),
        ],
        &[],
    );
    assert_eq!(report.served_side_bug_names, vec![name(1)]);
    let (served, shadow) = shadow_reads(&fixture, 16).await?;
    assert_eq!(
        shadow.trace["key_candidates"][0]["kind"],
        json!("PathExpiry")
    );
    assert_eq!(shadow.trace["selected_event"], json!("path-expiry"));
    assert_eq!(shadow.registration["status"], json!("released"));
    assert_eq!(shadow.registration["expiry"], json!(1_800_000_200u64));
    assert_eq!(
        shadow.registration["latest_event_kind"],
        json!("ExpiryChanged")
    );
    assert_eq!(shadow.control["status"], json!("unregistered"));
    assert_eq!(served.registration("status"), json!("active"));
    fixture.cleanup().await
}

#[tokio::test]
async fn grant_then_reservation_serves_reserved_with_the_grant_time() -> Result<()> {
    let fixture = Fixture::new("families_shadow_reserved", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    v2(
        &fixture,
        12,
        "RegistrationReserved",
        Some(&k1),
        json!({"status": "reserved", "expiry": 2_000_000_000u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 14).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let (served, _) = assert_name_equal(&fixture, 14).await?;
    assert_eq!(served.registration("status"), json!("reserved"));
    assert_eq!(
        served.registration("registered_at"),
        json!("2027-01-15T08:02:00+00:00")
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn reservation_then_expiry_change_keeps_the_reservation_kind() -> Result<()> {
    let fixture = Fixture::new("families_shadow_reservation_expiry", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationReserved",
        Some(&k1),
        json!({"status": "reserved", "expiry": 2_000_000_000u64}),
    )
    .await?;
    v2(
        &fixture,
        12,
        "ExpiryChanged",
        Some(&k1),
        json!({"expiry": 2_100_000_000u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 14).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let (served, _) = assert_name_equal(&fixture, 14).await?;
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("RegistrationReserved")
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn a_null_resource_release_joins_the_associated_key_and_the_other_grant_wins() -> Result<()> {
    let fixture = Fixture::new("families_shadow_association", 20).await?;
    let (k1, k2) = (uuid(1), uuid(2));
    v2_binding(&fixture, &k2).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    v2(
        &fixture,
        12,
        "RegistrationGranted",
        Some(&k2),
        json!({"status": "registered", "registrant": BOB, "expiry": 2_000_000_000u64}),
    )
    .await?;
    v2(
        &fixture,
        14,
        "RegistrationReleased",
        None,
        json!({"status": "released"}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let (served, trace) = assert_name_equal(&fixture, 16).await?;
    // The release follows the association to K2 and makes K2's candidate explicit; K1's active
    // grant wins the cross-key preference, served with the identity mismatch: no registrant or
    // registration time, the grant's own expiry.
    assert_eq!(
        trace["key_candidates"][1],
        json!({"event": "RegistrationReleased:14:1", "key": k2, "kind": "Explicit"})
    );
    assert_eq!(trace["identity_mismatch"], json!(true));
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("RegistrationGranted")
    );
    assert_eq!(served.registration("registrant"), Value::Null);
    assert_eq!(served.registration("registered_at"), Value::Null);
    assert_eq!(served.registration("expiry"), json!(2_000_000_000u64));
    fixture.cleanup().await
}

/// Two grants of one triple on two resources in one transaction, inserted so the generated ids
/// run against the log order. With no null-resource event to follow the association, each key
/// keeps its own active candidate and the binding's key is preferred in either order, so the
/// families and today's read agree and nothing is excused.
#[tokio::test]
async fn two_grants_on_two_keys_in_one_transaction_serve_the_binding_key() -> Result<()> {
    let fixture = Fixture::new("families_shadow_d12", 20).await?;
    let (k1, k2) = (uuid(1), uuid(2));
    v2_binding(&fixture, &k2).await?;
    for (resource, log, identity, registrant) in
        [(&k2, 5, "grant-b", BOB), (&k1, 1, "grant-a", ALICE)]
    {
        fixture.resource(resource).await?;
        fixture
            .event(
                Event::new(identity, 10, log, "RegistrationGranted", V2_REGISTRY)
                    .name(&name(1))
                    .resource(resource)
                    .after(
                        json!({"registry_contract_instance_id": "R", "token_id": "7",
                                  "authority_kind": "registrar", "status": "registered",
                                  "registrant": registrant, "expiry": 2_000_000_000u64}),
                    )
                    .raw(json!({"emitting_address": REGISTRY})),
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, 12).await?;
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(
        shadow.registration["registrant"],
        json!(BOB),
        "the D12 answer"
    );
    // The grants sit on different keys, so each key keeps its own candidate and the binding's
    // key wins in both orders: no difference, and none may be excused.
    assert_eq!(served.registration("registrant"), json!(BOB));
    assert_counts(&report, &[], &[]);
    fixture.cleanup().await
}

/// A synthesised grant and a synthesised expiry change of one key in one block, with no
/// transaction or log: D12 orders them by identity bytes, so the grant `b-grant` is the later
/// one, while today's order takes the higher generated id, the expiry change `a-expiry`. The
/// families serve the D12 answer, the production answer differs, and the difference is counted as
/// the disclosed same-block delta (brief section 4.3).
/// Step 2's amended D12 (39990c38) in the shadow reader: a release and then a grant written
/// from one log, identities ending with their emission ordinals 0 and 1 as the adapter writes
/// them (adapters schema_v2/normalized.rs:118-131). The family keeps each kind's maximum in its
/// own column, and the reader compares them across kinds. Compared as text, the grant
/// (`RegistrationGranted`) sorts before the release (`RegistrationReleased`) and the reader would
/// take the release as latest and serve the name released, which the harness would disclose as a
/// same-block ordering delta; in emission order the grant is latest, as today's builder reads it
/// (the grant is written second and has the higher generated id), and the name reads equal.
#[tokio::test]
async fn a_release_then_a_grant_from_one_log_take_the_emission_order() -> Result<()> {
    let fixture = Fixture::new("families_shadow_d12_emission", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    for (identity, kind, after) in [
        (
            "0xtx14:1:RegistrationReleased:0",
            "RegistrationReleased",
            json!({"status": "released"}),
        ),
        (
            "0xtx14:1:RegistrationGranted:1",
            "RegistrationGranted",
            json!({"status": "registered", "registrant": BOB, "expiry": 2_100_000_000u64}),
        ),
    ] {
        let mut after = after;
        after["registry_contract_instance_id"] = json!("R");
        after["token_id"] = json!("7");
        after["authority_kind"] = json!("registrar");
        fixture
            .event(
                Event::new(identity, 14, 1, kind, V2_REGISTRY)
                    .name(&name(1))
                    .resource(&k1)
                    .after(after)
                    .raw(json!({"emitting_address": REGISTRY})),
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, 16).await?;
    assert_counts(&report, &[], &[]);
    assert_eq!(
        (report.equal, report.mismatched),
        (report.names + report.resources, 0),
        "{:#?}",
        report.lines
    );
    let (served, trace) = assert_name_equal(&fixture, 16).await?;
    assert_eq!(trace["key_candidates"][0]["kind"], json!("Active"));
    assert_eq!(served.registration("status"), json!("active"));
    assert_eq!(served.registration("registrant"), json!(BOB));
    assert_eq!(served.control("status"), json!("registered"));
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("RegistrationGranted")
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn synthesised_events_in_one_block_take_the_identity_order() -> Result<()> {
    let fixture = Fixture::new("families_shadow_d12_synthesised", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    for (identity, kind, after) in [
        (
            "b-grant",
            "RegistrationGranted",
            json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
        ),
        (
            "a-expiry",
            "ExpiryChanged",
            json!({"expiry": 2_100_000_000u64}),
        ),
    ] {
        let mut after = after;
        after["registry_contract_instance_id"] = json!("R");
        after["token_id"] = json!("7");
        after["authority_kind"] = json!("registrar");
        fixture
            .event(
                Event::new(identity, 10, 0, kind, V2_REGISTRY)
                    .name(&name(1))
                    .resource(&k1)
                    .after(after)
                    .raw(json!({"emitting_address": REGISTRY}))
                    .synthesised(),
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, 12).await?;
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(
        shadow.registration["latest_event_kind"],
        json!("RegistrationGranted"),
        "the D12 answer"
    );
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("ExpiryChanged"),
        "the production answer differs"
    );
    assert_eq!(report.expected_delta, 1, "and the difference is disclosed");
    assert_counts(
        &report,
        &[],
        &[
            ("d12_same_block_order:registration/latest_event_kind", 1),
            ("d12_same_block_order:registration/expiry", 1),
            ("d12_same_block_order:control/expiry", 1),
        ],
    );
    // Pro Q3 on a5f61182: a wrong expiry on the canonically selected grant, the log and the
    // served rows unchanged, makes the families' expiry wrong while today's order still selects
    // a-expiry, which carries the served one. The retained grant no longer equals its log
    // rebuild, so the name gets no excuse at all: both expiry fields and the latest kind fail.
    sqlx::query(
        "UPDATE bigname_phase.project_lifecycle_event
         SET expiry = '2200000000'::jsonb, expiry_seconds = 2200000000
         WHERE event_identity = 'b-grant'",
    )
    .execute(&fixture.pool)
    .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert_eq!(
        failed_fields(&mutated),
        [
            "control/expiry",
            "registration/expiry",
            "registration/latest_event_kind"
        ],
        "a wrong canonical expiry must not pass: {:#?}",
        mutated.lines
    );
    assert!(
        mutated.expected_delta_fields.is_empty(),
        "{:#?}",
        mutated.lines
    );
    sqlx::query(
        "UPDATE bigname_phase.project_lifecycle_event
         SET expiry = '2000000000'::jsonb, expiry_seconds = 2000000000
         WHERE event_identity = 'b-grant'",
    )
    .execute(&fixture.pool)
    .await?;
    // Codex thread PRRT_kwDOSJpxAs6l7vYS: an event the family still holds whose log row is no
    // longer canonical has no readable generated id, so the block's today's order is unknown
    // and the differences stay mismatches rather than same-block deltas.
    sqlx::query(
        "UPDATE normalized_events SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND event_identity = 'a-expiry'",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert!(
        mutated.expected_delta_fields.is_empty() && mutated.known_discrepancy.is_empty(),
        "an orphaned event must not order the block: {:#?}",
        mutated.lines
    );
    assert_eq!(
        failed_fields(&mutated),
        [
            "control/expiry",
            "registration/expiry",
            "registration/latest_event_kind"
        ],
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// An ENSv1 registrar event of name 1 on `lease`.
async fn v1(fixture: &Fixture, block: i64, kind: &str, lease: &str, after: Value) -> Result<i64> {
    let mut after = after;
    after["authority_kind"] = json!("registrar");
    fixture
        .write(
            block,
            1,
            kind,
            V1_REGISTRAR,
            Some(&name(1)),
            Some(lease),
            after,
            REGISTRAR,
        )
        .await
}

async fn v1_binding(fixture: &Fixture, lease: &str) -> Result<()> {
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
            json!({"authority_kind": "registrar", "state_derived": false}),
            REGISTRAR,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn an_ensv1_lease_serves_its_transfer_and_renewal() -> Result<()> {
    let fixture = Fixture::new("families_shadow_v1_lease", 20).await?;
    let lease = uuid(1);
    v1_binding(&fixture, &lease).await?;
    v1(
        &fixture,
        10,
        "RegistrationGranted",
        &lease,
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    v1(
        &fixture,
        12,
        "TokenControlTransferred",
        &lease,
        json!({"from": ALICE, "to": BOB}),
    )
    .await?;
    v1(
        &fixture,
        14,
        "RegistrationRenewed",
        &lease,
        json!({"expiry": 2_100_000_000u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let (served, _) = assert_name_equal(&fixture, 16).await?;
    assert_eq!(served.registration("registrant"), json!(BOB));
    assert_eq!(served.registration("expiry"), json!(2_100_000_000u64));
    assert_eq!(
        served.registration("registered_at"),
        json!("2027-01-15T08:02:00+00:00")
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn an_ensv1_release_names_its_before_state_registrant() -> Result<()> {
    let fixture = Fixture::new("families_shadow_v1_release", 20).await?;
    let lease = uuid(1);
    v1_binding(&fixture, &lease).await?;
    v1(
        &fixture,
        10,
        "RegistrationGranted",
        &lease,
        json!({"status": "registered", "registrant": ALICE, "expiry": 1_800_000_150u64}),
    )
    .await?;
    fixture
        .event(
            Event::new("release", 14, 1, "RegistrationReleased", V1_REGISTRAR)
                .name(&name(1))
                .resource(&lease)
                .before(json!({"registrant": ALICE}))
                .after(json!({"status": "released", "authority_kind": "registrar"}))
                .raw(json!({"emitting_address": REGISTRAR})),
        )
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let (served, _) = assert_name_equal(&fixture, 16).await?;
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("RegistrationReleased")
    );
    fixture.cleanup().await
}

/// A grant whose after-state has no `authority_kind`: step 2 retains the kind raw, null when
/// absent (migration 20260926100800), and the served block reads the raw after-state and serves
/// null (build.sql:30, :394). The families agree; the old `authority_kind_defaulted_to_registrar`
/// cause is gone, so a difference here fails.
#[tokio::test]
async fn a_grant_without_authority_kind_is_served_null_by_both() -> Result<()> {
    let fixture = Fixture::new("families_shadow_authority_kind", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            V2_REGISTRY,
            Some(&name(1)),
            Some(&k1),
            json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64,
                   "registry_contract_instance_id": "R", "token_id": "7"}),
            REGISTRY,
        )
        .await?;
    let report = publish_and_compare(&fixture, 12).await?;
    assert_counts(&report, &[], &[]);
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(served.registration("authority_kind"), Value::Null);
    assert_eq!(shadow.registration["authority_kind"], Value::Null);
    fixture.cleanup().await
}

/// History A: grant, the interpreter's unnamed path-expiry release, explicit release, all on one
/// resource. The path release lies between the grant and the explicit release, so the explicit
/// release has no witness and the path release is the key's candidate: the families serve P
/// with the lapsed expiry kept. Today's name-scoped membership sees G and E only, so E is
/// witnessed and served, and an explicit ENSv2 release clears the expiry. The expiry is the one
/// field that differs, the disclosed served-side difference.
#[tokio::test]
async fn history_a_grant_path_expiry_explicit_release_serves_the_path_release() -> Result<()> {
    let fixture = Fixture::new("families_shadow_history_a", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 1_800_000_100u64}),
    )
    .await?;
    unnamed_path_expiry(&fixture, "path-expiry", 12, &k1, 1_800_000_100).await?;
    v2(
        &fixture,
        14,
        "RegistrationReleased",
        Some(&k1),
        json!({"status": "released"}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    assert_counts(
        &report,
        &[(
            "served_membership_skips_unnamed_path_expiry:registration/expiry",
            1,
        )],
        &[],
    );
    assert_eq!(report.served_side_bug_names, vec![name(1)]);
    let (served, shadow) = shadow_reads(&fixture, 16).await?;
    assert_eq!(
        shadow.trace["key_candidates"][0]["kind"],
        json!("PathExpiry")
    );
    assert_eq!(shadow.trace["selected_event"], json!("path-expiry"));
    assert_eq!(shadow.registration["status"], json!("released"));
    assert_eq!(shadow.registration["expiry"], json!(1_800_000_100u64));
    assert_eq!(served.registration("status"), json!("released"));
    assert_eq!(served.registration("expiry"), Value::Null);
    fixture.cleanup().await
}

/// History B: grant then explicit release. The grant witnesses the release, which is the
/// candidate; an explicit ENSv2 release clears the expiry.
#[tokio::test]
async fn history_b_grant_explicit_release_serves_the_release_without_expiry() -> Result<()> {
    let fixture = Fixture::new("families_shadow_history_b", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    v2(
        &fixture,
        14,
        "RegistrationReleased",
        Some(&k1),
        json!({"status": "released"}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let (served, trace) = assert_name_equal(&fixture, 16).await?;
    assert_eq!(trace["key_candidates"][0]["kind"], json!("Explicit"));
    assert_eq!(served.registration("status"), json!("released"));
    assert_eq!(served.registration("expiry"), Value::Null);
    assert_eq!(served.control("status"), json!("unregistered"));
    fixture.cleanup().await
}

/// P, V, W: the interpreter's unnamed path expiry, a renewal that revives from it, then an
/// ordinary renewal. Renewals are not registration candidates, so the path release stays the
/// key's candidate and the latest of the five kinds is the renewal; the revival keeps the
/// resource from retirement (design:63). Today's membership never sees the release and serves
/// the grant, the disclosed served-side difference.
#[tokio::test]
async fn a_revival_then_an_ordinary_renewal_keep_the_path_release_as_candidate() -> Result<()> {
    let fixture = Fixture::new("families_shadow_revival", 22).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 1_800_000_100u64}),
    )
    .await?;
    unnamed_path_expiry(&fixture, "path-expiry", 12, &k1, 1_800_000_100).await?;
    v2(
        &fixture,
        14,
        "RegistrationRenewed",
        Some(&k1),
        json!({"expiry": 1_900_000_000u64, "revived_from_expiry": true}),
    )
    .await?;
    v2(
        &fixture,
        16,
        "RegistrationRenewed",
        Some(&k1),
        json!({"expiry": 2_000_000_000u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 18).await?;
    let cause = "served_membership_skips_unnamed_path_expiry";
    assert_counts(
        &report,
        &[
            (&format!("{cause}:registration/status"), 1),
            (&format!("{cause}:registration/authority_kind"), 1),
            (&format!("{cause}:registration/registrant"), 1),
            (&format!("{cause}:control/status"), 1),
            (&format!("{cause}:control/expiry"), 1),
            (&format!("{cause}:control/registrant"), 1),
        ],
        &[],
    );
    assert_eq!(report.served_side_bug_names, vec![name(1)]);
    let (served, shadow) = shadow_reads(&fixture, 18).await?;
    assert_eq!(
        shadow.trace["key_candidates"][0]["kind"],
        json!("PathExpiry")
    );
    assert_eq!(
        shadow.registration["latest_event_kind"],
        json!("RegistrationRenewed")
    );
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("RegistrationRenewed")
    );
    fixture.cleanup().await
}

/// Grant then an admitted renewal: a JSON-number renewal expiry is the served expiry, a
/// non-numeric one leaves the grant's in place.
#[tokio::test]
async fn a_renewal_serves_its_expiry_only_when_it_is_a_number() -> Result<()> {
    for numeric in [true, false] {
        let fixture =
            Fixture::new(&format!("families_shadow_renewal_numeric_{numeric}"), 20).await?;
        let k1 = uuid(1);
        v2_binding(&fixture, &k1).await?;
        v2(
            &fixture,
            10,
            "RegistrationGranted",
            Some(&k1),
            json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
        )
        .await?;
        let renewed = if numeric {
            json!(2_100_000_000u64)
        } else {
            json!("2100000000")
        };
        v2(
            &fixture,
            12,
            "RegistrationRenewed",
            Some(&k1),
            json!({ "expiry": renewed }),
        )
        .await?;
        let report = publish_and_compare(&fixture, 14).await?;
        shadow_support::assert_counts(&report, &[], &[]);
        let (served, _) = assert_name_equal(&fixture, 14).await?;
        let expected = if numeric {
            2_100_000_000u64
        } else {
            2_000_000_000u64
        };
        assert_eq!(served.registration("expiry"), json!(expected), "{numeric}");
        fixture.cleanup().await?;
    }
    Ok(())
}

/// The served registration names the winning grant's `authority_key` (build.sql:31,
/// :393-420); step 2 retains it on the lifecycle row (migration 20260926100800), so the families
/// serve the same key and a difference fails.
#[tokio::test]
async fn a_grant_authority_key_is_read_from_the_retained_row() -> Result<()> {
    let fixture = Fixture::new("families_shadow_authority_key", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64,
               "authority_key": "registrar:k1"}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 12).await?;
    assert_counts(&report, &[], &[]);
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(served.registration("authority_key"), json!("registrar:k1"));
    assert_eq!(shadow.registration["authority_key"], json!("registrar:k1"));
    fixture.cleanup().await
}

/// D12 association (brief 4.3 item 3): G_a on K1 at transaction 2 log 5 carries the higher
/// generated id, G_b on K2 at transaction 3 log 1 the lower one, then a release with no
/// resource. The families associate the triple with the later position, K2, so the release
/// makes K2's candidate explicit and the bound K1 keeps its live grant: Alice is served. Today's
/// decoder associates by generated id (v2_lifecycle_events.sql:19), so the release lands on K1,
/// K2's live grant wins the preference with the identity mismatch, and no registrant is served.
/// The difference is counted as the disclosed same-block delta, not as a mismatch.
#[tokio::test]
async fn the_association_follows_the_later_position_not_the_generated_id() -> Result<()> {
    let fixture = Fixture::new("families_shadow_d12_association", 20).await?;
    let (k1, k2) = (uuid(1), uuid(2));
    v2_binding(&fixture, &k1).await?;
    fixture.resource(&k2).await?;
    for (resource, transaction, log, identity, registrant) in
        [(&k2, 3, 1, "grant-b", BOB), (&k1, 2, 5, "grant-a", ALICE)]
    {
        fixture
            .event(
                Event::new(identity, 10, log, "RegistrationGranted", V2_REGISTRY)
                    .at(transaction, log)
                    .name(&name(1))
                    .resource(resource)
                    .after(
                        json!({"registry_contract_instance_id": "R", "token_id": "7",
                               "authority_kind": "registrar", "status": "registered",
                               "registrant": registrant, "expiry": 2_000_000_000u64}),
                    )
                    .raw(json!({"emitting_address": REGISTRY})),
            )
            .await?;
    }
    v2(
        &fixture,
        14,
        "RegistrationReleased",
        None,
        json!({"status": "released"}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    let (served, shadow) = shadow_support::name(&fixture, 16, &name(1)).await?;
    assert_eq!(
        shadow.registration["registrant"],
        json!(ALICE),
        "the D12 answer"
    );
    assert_eq!(
        served.registration("registrant"),
        Value::Null,
        "the production answer differs"
    );
    assert_eq!(report.expected_delta, 1, "and the difference is disclosed");
    assert_counts(
        &report,
        &[],
        &[
            ("d12_same_block_order:registration/registrant", 1),
            ("d12_same_block_order:registration/registered_at", 1),
            ("d12_same_block_order:registration/authority_kind", 1),
            ("d12_same_block_order:control/registrant", 1),
            ("d12_same_block_order:control/expiry", 1),
        ],
    );
    fixture.cleanup().await
}

/// Ruling R1: the interpreter's path-expiry release names its resource and no name
/// (crates/adapters/src/schema_v2/protocol/v2_registry/expiry.rs:58-59). The F2a key state of
/// the resource counts it (design:40, decoder rule 1), and the chain has expired the
/// registration (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L36 @
/// ens_v2@a971bd64), so the families serve the release: released, latest kind
/// RegistrationReleased, control unregistered. Today's name-scoped membership (build.sql:322,
/// :366-367) serves the renewal as active, the served-side difference disclosed for cutover.
#[tokio::test]
async fn an_unnamed_path_expiry_on_the_resource_serves_the_release() -> Result<()> {
    let fixture = Fixture::new("families_shadow_unnamed_path_expiry", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 1_800_000_100u64}),
    )
    .await?;
    v2(
        &fixture,
        12,
        "RegistrationRenewed",
        Some(&k1),
        json!({"expiry": 1_800_000_150u64}),
    )
    .await?;
    unnamed_path_expiry(&fixture, "path-expiry", 14, &k1, 1_800_000_150).await?;
    let report = publish_and_compare(&fixture, 16).await?;
    let cause = "served_membership_skips_unnamed_path_expiry";
    assert_counts(
        &report,
        &[
            (&format!("{cause}:registration/status"), 1),
            (&format!("{cause}:registration/latest_event_kind"), 1),
            (&format!("{cause}:registration/authority_kind"), 1),
            (&format!("{cause}:registration/registrant"), 1),
            (&format!("{cause}:control/status"), 1),
            (&format!("{cause}:control/expiry"), 1),
            (&format!("{cause}:control/registrant"), 1),
        ],
        &[],
    );
    assert_eq!(report.served_side_bug_names, vec![name(1)]);
    let (served, shadow) = shadow_reads(&fixture, 16).await?;
    assert_eq!(shadow.registration["status"], json!("released"));
    assert_eq!(
        shadow.registration["latest_event_kind"],
        json!("RegistrationReleased")
    );
    assert_eq!(shadow.control["status"], json!("unregistered"));
    assert_eq!(served.registration("status"), json!("active"));
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("RegistrationRenewed")
    );

    // Codex threads PRRT_kwDOSJpxAs6l43wy and PRRT_kwDOSJpxAs6l4hOz: the named cause passes a
    // field only when today's name-scoped membership, the families read without the unnamed
    // release, gives the served value. A served status the membership does not give, and a
    // wrong expiry in the families that the shadow presents, each stay a mismatch.
    let snapshot: Value =
        sqlx::query_scalar("SELECT declared_summary FROM name_current WHERE logical_name_id = $1")
            .bind(name(1))
            .fetch_one(&fixture.pool)
            .await?;
    sqlx::query(
        "UPDATE name_current
         SET declared_summary = jsonb_set(declared_summary, '{registration,status}', '\"reserved\"')
         WHERE logical_name_id = $1",
    )
    .bind(name(1))
    .execute(&fixture.pool)
    .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
    assert_eq!(
        failed_fields(&mutated),
        ["registration/status"],
        "a served status the membership does not give: {:#?}",
        mutated.lines
    );
    sqlx::query("UPDATE name_current SET declared_summary = $2 WHERE logical_name_id = $1")
        .bind(name(1))
        .bind(&snapshot)
        .execute(&fixture.pool)
        .await?;
    sqlx::query(
        "UPDATE bigname_phase.project_lifecycle_event
         SET expiry = '1800000999'::jsonb, expiry_seconds = 1800000999
         WHERE event_kind = 'RegistrationRenewed'",
    )
    .execute(&fixture.pool)
    .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
    assert!(
        failed_fields(&mutated).contains(&"registration/expiry".to_owned()),
        "a wrong family expiry: {:#?}",
        mutated.lines
    );
    assert!(
        !mutated
            .known_discrepancy
            .keys()
            .any(|field| field.ends_with(":registration/expiry")),
        "{:#?}",
        mutated.lines
    );
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

/// A subregistry rebind moves resource K1 from name 1 to name 2 in one raw log: the adapter
/// emits a named RegistrationReleased for the previous name and a named RegistrationGranted for
/// the current one on the same resource (crates/adapters/src/schema_v2/protocol/v2_registry/
/// topology.rs:65-84, :363-370), and their identities sort the grant first. The key state of K1
/// then holds both names' events. A name's membership of its key is its own events and the
/// unnamed ones, never another name's: name 2 stays active on its grant, name 1 is released, and
/// the other name's events on the key are traced.
#[tokio::test]
async fn a_topology_rebind_keeps_each_name_to_its_own_events_on_the_shared_key() -> Result<()> {
    let fixture = Fixture::new("families_shadow_topology_rebind", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    fixture
        .binding(&uuid(101), &name(2), &k1, "ens_v2", 12, 0, None)
        .await?;
    let registry = json!({"registry_contract_instance_id": "R", "token_id": "7",
                          "authority_kind": "registrar"});
    for (identity, kind, logical_name, mut after) in [
        (
            "rebind:RegistrationReleased:topology:R:7",
            "RegistrationReleased",
            name(1),
            json!({"source_event": "SubregistryUpdated",
                   "terminal_reason": "registry_name_binding_changed", "status": "released",
                   "released_at": 1_800_000_144u64}),
        ),
        (
            "rebind:RegistrationGranted:topology:R:7",
            "RegistrationGranted",
            name(2),
            json!({"source_event": "SubregistryUpdated", "status": "registered",
                   "registrant": BOB, "expiry": 2_000_000_000u64}),
        ),
    ] {
        for (field, value) in registry.as_object().expect("an object") {
            after[field] = value.clone();
        }
        fixture
            .event(
                Event::new(identity, 12, 1, kind, V2_REGISTRY)
                    .name(&logical_name)
                    .resource(&k1)
                    .after(after)
                    .raw(json!({"emitting_address": REGISTRY})),
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, 14).await?;
    assert_counts(&report, &[], &[]);
    let (served, shadow) = shadow_support::name(&fixture, 14, &name(2)).await?;
    assert_eq!(served.registration("status"), json!("active"));
    assert_eq!(shadow.registration["status"], json!("active"));
    assert_eq!(shadow.registration["registrant"], json!(BOB));
    assert_eq!(
        shadow.trace["foreign_named_members"],
        json!([
            "RegistrationGranted:10:1",
            "rebind:RegistrationReleased:topology:R:7"
        ])
    );
    let (served, shadow) = shadow_support::name(&fixture, 14, &name(1)).await?;
    assert_eq!(served.registration("status"), json!("released"));
    assert_eq!(shadow.registration["status"], json!("released"));
    fixture.cleanup().await
}

/// The real path-expiry shape for a name the interpreter knows: at the expiry block the adapter
/// closes the name's ENSv2 binding and emits a named SurfaceUnbound and a named path-expiry
/// release on the token resource (crates/adapters/src/schema_v2/protocol/v2_registry/
/// topology.rs:251-296). Here the name also has an open ENSv1 lease from before its ENSv2
/// registration. The served name authority then selects arm ens_v1, because nothing is open on
/// ENSv2 and an ENSv1 binding is (name_authority/build.sql:611-618), so today's row serves the
/// live ENSv1 lease. Under Tate's ruling an expired ENSv2 registration stays ENSv2 and is served
/// unregistered, so that is a second served-side bug. Step 3 cannot see it: the shadow takes the
/// authority selection from the served row as input (selection is step 6's work), follows it to
/// the ENSv1 lease and agrees with the served row. This test pins the served arm so the bug stays
/// visible until step 6 changes the selection.
#[tokio::test]
async fn a_real_path_expiry_with_an_ensv1_lease_is_served_from_the_lease() -> Result<()> {
    let fixture = Fixture::new("families_shadow_real_path_expiry", 20).await?;
    let (lease, k1) = (uuid(2), uuid(1));
    fixture
        .binding(&uuid(102), &name(1), &lease, "ens_v1", 5, 0, None)
        .await?;
    fixture
        .write(
            5,
            0,
            "SurfaceBound",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false}),
            REGISTRAR,
        )
        .await?;
    v1(
        &fixture,
        6,
        "RegistrationGranted",
        &lease,
        json!({"status": "registered", "registrant": BOB, "expiry": 2_000_000_000u64}),
    )
    .await?;
    fixture
        .binding(&uuid(100), &name(1), &k1, "ens_v2", 9, 0, Some(14))
        .await?;
    fixture
        .write(
            9,
            0,
            "SurfaceBound",
            V2_REGISTRY,
            Some(&name(1)),
            Some(&k1),
            json!({"authority_kind": "registrar", "state_derived": false}),
            REGISTRY,
        )
        .await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 1_800_000_150u64}),
    )
    .await?;
    let mut unbound = path_expiry(1_800_000_150);
    unbound["registry_contract_instance_id"] = json!("R");
    unbound["token_id"] = json!("7");
    unbound["topology_rebind"] = json!(true);
    let mut released = path_expiry(1_800_000_150);
    released["registry_contract_instance_id"] = json!("R");
    released["token_id"] = json!("7");
    released["status"] = json!("released");
    released["released_at"] = json!(1_800_000_168u64);
    for (identity, kind, after) in [
        ("x:SurfaceUnbound:expiry:R:7", "SurfaceUnbound", unbound),
        (
            "x:RegistrationReleased:expiry:R:7",
            "RegistrationReleased",
            released,
        ),
    ] {
        fixture
            .event(
                Event::new(identity, 14, 0, kind, V2_REGISTRY)
                    .name(&name(1))
                    .resource(&k1)
                    .before(json!({"status": "registered", "registrant": ALICE}))
                    .after(after)
                    .raw(json!({"emitting_address": REGISTRY}))
                    .synthesised(),
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, 16).await?;
    let (served, shadow) = shadow_support::name(&fixture, 16, &name(1)).await?;
    // Pinned today: the served row routes the expired ENSv2 name back to the ENSv1 lease, and
    // the shadow, taking that selection as input, agrees. Nothing is counted, so the harness
    // cannot see this flavour of the bug.
    assert_eq!(
        served.provenance["authority_selection"]["authority_arm"],
        json!("ens_v1")
    );
    assert_eq!(served.registration("status"), json!("active"));
    assert_eq!(served.registration("registrant"), json!(BOB));
    assert_eq!(served.registration("resource_id"), json!(lease));
    assert_eq!(shadow.registration["status"], json!("active"));
    assert_eq!(shadow.registration["registrant"], json!(BOB));
    assert!(report.served_side_bug_names.is_empty());
    assert_counts(&report, &[], &[]);
    fixture.cleanup().await
}

/// Item 2 of the TYR-36 step 3 review (Q2): a name with ENSv2 lifecycle events and a Basenames
/// event, and no open binding, has two event arms and no ENSv1 history, so no authority arm is
/// selected (name_authority/build.sql:599-620). The selection reads a missing arm as ENSv2
/// (`COALESCE(selected_authority_arm, 'ens_v2') = 'ens_v2'`, build.sql:347), so its explicit
/// release is the registration; the presentation reads the same resolved arm, so the release is
/// served whole: released, no registrant, authority or expiry, control unregistered and nothing
/// else (ruling R1: a released ENSv2 registration stays ENSv2 and is unregistered).
#[tokio::test]
async fn a_release_with_no_selected_arm_is_presented_as_an_ensv2_release() -> Result<()> {
    let fixture = Fixture::new("families_shadow_null_arm_release", 20).await?;
    let k1 = uuid(1);
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    fixture
        .event(
            Event::new("release-14", 14, 1, "RegistrationReleased", V2_REGISTRY)
                .name(&name(1))
                .resource(&k1)
                .before(json!({"registrant": ALICE}))
                .after(
                    json!({"registry_contract_instance_id": "R", "token_id": "7",
                              "authority_kind": "registrar", "status": "released",
                              "expiry": 2_000_000_000u64,
                              "released_at": "2027-01-15T08:02:48+00:00"}),
                )
                .raw(json!({"emitting_address": REGISTRY})),
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "AuthorityTransferred",
            "basenames_base_registry",
            Some(&name(1)),
            None,
            json!({"node": node(1), "owner": BOB}),
            REGISTRY,
        )
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    let (served, shadow) = shadow_reads(&fixture, 16).await?;
    assert_eq!(
        served.provenance["authority_selection"]["authority_arm"],
        Value::Null,
        "no arm is selected"
    );
    assert_eq!(
        Value::Object(shadow.registration.clone()),
        json!({
            "status": "released", "authority_kind": null, "authority_key": null,
            "resource_id": null, "registrant": null, "expiry": null, "registered_at": null,
            "released_at": "2027-01-15T08:02:48+00:00",
            "latest_event_kind": "RegistrationReleased",
        }),
        "trace {:?}",
        shadow.trace
    );
    assert_eq!(
        Value::Object(shadow.control.clone()),
        json!({"status": "unregistered"})
    );
    // Today's presentation compares the raw arm with 'ens_v2', so it serves the same release
    // with its expiry and the control block's live status: the two differences are reported
    // under their own cause, and nothing else differs.
    assert_eq!(served.registration("status"), json!("released"));
    assert_eq!(served.registration("expiry"), json!(2_000_000_000u64));
    assert_eq!(served.control("status"), json!("released"));
    let cause = "served_release_presentation_reads_the_raw_arm";
    assert_counts(
        &report,
        &[
            (&format!("{cause}:registration/expiry"), 1),
            (&format!("{cause}:control/status"), 1),
        ],
        &[],
    );
    fixture.cleanup().await
}

/// Pro Q2 on a5f61182: two synthesised ENSv2 TokenControlTransferred events of name 1 in one
/// block, b-transfer (to BOB) written first and a-transfer (to CAROL) second. The canonical
/// order takes b-transfer (its identity sorts last) and today's order a-transfer (higher id),
/// so the control owner is a legitimate same-block delta. Flipping only the canonically
/// selected family row's unmasked-word flag, or its recipient, makes the families' owner wrong
/// while today's order still selects a-transfer; the owner must then stay a mismatch, because
/// the canonical read of the lifecycle events rebuilt from the log gives BOB.
#[tokio::test]
async fn a_wrong_fact_on_the_canonically_selected_transfer_stays_a_mismatch() -> Result<()> {
    const CAROL: &str = "0x00000000000000000000000000000000000000cc";
    let fixture = Fixture::new("families_shadow_canonical_transfer", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    for (identity, to) in [("b-transfer", BOB), ("a-transfer", CAROL)] {
        fixture
            .event(
                Event::new(identity, 11, 0, "TokenControlTransferred", V2_REGISTRY)
                    .name(&name(1))
                    .resource(&k1)
                    .after(
                        json!({"registry_contract_instance_id": "R", "token_id": "7",
                                  "authority_kind": "registrar", "from": ALICE, "to": to}),
                    )
                    .raw(json!({"emitting_address": REGISTRY}))
                    .synthesised(),
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, 12).await?;
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(served.control("registry_owner"), json!(CAROL));
    assert_eq!(shadow.control["registry_owner"], json!(BOB));
    // The recipient is also the registrant both blocks report.
    assert_counts(
        &report,
        &[],
        &[
            ("d12_same_block_order:control/registrant", 1),
            ("d12_same_block_order:control/registry_owner", 1),
            ("d12_same_block_order:registration/registrant", 1),
        ],
    );
    let delta = report.expected_delta_fields.clone();
    for (case, update) in [
        ("unmasked word", "owner_word_unmasked = true"),
        (
            "recipient",
            "to_address = '0x00000000000000000000000000000000000000dd'",
        ),
    ] {
        sqlx::query(&format!(
            "UPDATE bigname_phase.project_lifecycle_event SET {update}
             WHERE event_identity = 'b-transfer'"
        ))
        .execute(&fixture.pool)
        .await?;
        let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
        assert!(
            !mutated
                .expected_delta_fields
                .keys()
                .any(|field| field.contains(":control/registry_owner")),
            "{case}: a wrong canonical owner must not pass: {:#?}",
            mutated.lines
        );
        assert!(
            failed_fields(&mutated).contains(&"control/registry_owner".to_owned()),
            "{case}: {:#?}",
            mutated.lines
        );
        sqlx::query(
            "UPDATE bigname_phase.project_lifecycle_event
             SET owner_word_unmasked = NULL, to_address = $1
             WHERE event_identity = 'b-transfer'",
        )
        .bind(BOB)
        .execute(&fixture.pool)
        .await?;
        let restored = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
        assert_eq!(restored.expected_delta_fields, delta, "{case}");
        assert_eq!(restored.mismatched, 0, "{case}");
    }
    fixture.cleanup().await
}

/// Pro Q3 on a5f61182: a grant with no numeric expiry, then the interpreter's unnamed
/// path-expiry release of its resource. With no admitted numeric expiry on the key the reader
/// serves the release's own expiry (served.rs:173-183, :244-268), and today's name-scoped
/// membership serves the grant with none: a field of the named cause. A wrong expiry on the
/// family's release row moves both the shadow value and the reader's trace, and removing the
/// release for the served-side read removes it too, so the cause must check the release against
/// the log: the canonical read of the events rebuilt from it gives 1,800,000,150, and the field
/// stays a mismatch.
#[tokio::test]
async fn an_unnamed_release_expiry_is_checked_against_the_log() -> Result<()> {
    let fixture = Fixture::new("families_shadow_unnamed_release_expiry", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE}),
    )
    .await?;
    unnamed_path_expiry(&fixture, "path-expiry", 14, &k1, 1_800_000_150).await?;
    let report = publish_and_compare(&fixture, 16).await?;
    let (served, shadow) = shadow_reads(&fixture, 16).await?;
    assert_eq!(served.registration("expiry"), Value::Null);
    assert_eq!(shadow.registration["expiry"], json!(1_800_000_150u64));
    let cause = "served_membership_skips_unnamed_path_expiry:registration/expiry";
    let known: Vec<String> = [
        "control/registrant",
        "control/status",
        "registration/authority_kind",
        "registration/expiry",
        "registration/latest_event_kind",
        "registration/registrant",
        "registration/status",
    ]
    .iter()
    .map(|field| format!("served_membership_skips_unnamed_path_expiry:{field}"))
    .collect();
    let known: Vec<(&str, usize)> = known.iter().map(|field| (field.as_str(), 1)).collect();
    assert_counts(&report, &known, &[]);
    sqlx::query(
        "UPDATE bigname_phase.project_lifecycle_event
         SET expiry = '1800000999'::jsonb, expiry_seconds = 1800000999
         WHERE event_identity = 'path-expiry'",
    )
    .execute(&fixture.pool)
    .await?;
    let (_, shadow) = shadow_reads(&fixture, 16).await?;
    assert_eq!(shadow.registration["expiry"], json!(1_800_000_999u64));
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
    assert!(
        !mutated.known_discrepancy.contains_key(cause),
        "a wrong release expiry must not pass: {:#?}",
        mutated.lines
    );
    assert!(
        failed_fields(&mutated).contains(&"registration/expiry".to_owned()),
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// Codex thread PRRT_kwDOSJpxAs6l8A2X: the corpus expectation reads the same publication-visible
/// events as the comparison. An unnamed path-expiry release counts its name on eight fields; the
/// release made a candidate, or moved to an orphaned hash of its block, counts nothing, and a
/// later grant that is a candidate does not stop it counting. Pro Q9 on 6c8bdf8b: it also counts
/// exactly the names the harness compares, so a name_current row the serving reader excludes
/// counts nothing.
#[tokio::test]
async fn the_corpus_expectation_reads_the_published_log_only() -> Result<()> {
    use shadow_support::compare::corpus_expectation;
    const ORPHAN: &str = "0x00000000000000000000000000000000000000000000000000000000000dead0";
    for case in [
        "visible",
        "unactivated",
        "wrong lineage",
        "candidate later grant",
        "stale name",
    ] {
        let fixture = Fixture::new("families_shadow_corpus_expectation", 20).await?;
        let k1 = uuid(1);
        v2_binding(&fixture, &k1).await?;
        v2(
            &fixture,
            10,
            "RegistrationGranted",
            Some(&k1),
            json!({"status": "registered", "registrant": ALICE, "expiry": 1_800_000_100u64}),
        )
        .await?;
        unnamed_path_expiry(&fixture, "path-expiry", 14, &k1, 1_800_000_150).await?;
        shadow_support::publish(&fixture, 16).await?;
        match case {
            "unactivated" => {
                sqlx::query(
                    "UPDATE normalized_events SET consumer_visibility = 'candidate',
                         migration_correlation_ids = ARRAY['fixture']
                     WHERE event_identity = 'path-expiry'",
                )
                .execute(&fixture.pool)
                .await?;
            }
            "wrong lineage" => {
                sqlx::query(
                    "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                         block_timestamp, canonicality_state)
                     VALUES ($1, $2, $3, 14, to_timestamp(1800000168), 'orphaned')",
                )
                .bind(CHAIN)
                .bind(ORPHAN)
                .bind(support::hash(13))
                .execute(&fixture.pool)
                .await?;
                sqlx::query(
                    "UPDATE normalized_events SET block_hash = $1
                     WHERE event_identity = 'path-expiry'",
                )
                .bind(ORPHAN)
                .execute(&fixture.pool)
                .await?;
            }
            "candidate later grant" => {
                let id = v2(
                    &fixture,
                    15,
                    "RegistrationGranted",
                    Some(&k1),
                    json!({"status": "registered", "registrant": ALICE}),
                )
                .await?;
                sqlx::query(
                    "UPDATE normalized_events SET consumer_visibility = 'candidate',
                         migration_correlation_ids = ARRAY['fixture']
                     WHERE normalized_event_id = $1",
                )
                .bind(id)
                .execute(&fixture.pool)
                .await?;
            }
            "stale name" => {
                sqlx::query(
                    "UPDATE name_current SET canonicality_summary =
                         canonicality_summary || '{\"state\": \"orphaned\"}'::jsonb",
                )
                .execute(&fixture.pool)
                .await?;
            }
            _ => {}
        }
        let expected = corpus_expectation(&fixture.pool, CHAIN, 16).await?;
        let counted = !matches!(case, "unactivated" | "wrong lineage" | "stale name");
        assert_eq!(
            expected.len(),
            if counted { 8 } else { 0 },
            "{case}: {expected:?}"
        );
        assert!(expected.values().all(|count| *count == 1), "{case}");
        fixture.cleanup().await?;
    }
    Ok(())
}

/// Codex thread PRRT_kwDOSJpxAs6l8Nw6: a name_current row the serving reader excludes
/// (name_current.rs `DEFAULT_NAME_CURRENT_READ_FILTER`), here one whose canonicality summary is
/// off the canonical lineage, is not served, so the comparison does not read it. The names
/// already came through the serving loader, so this pins that rather than fixing it.
#[tokio::test]
async fn a_name_the_serving_reader_excludes_is_not_compared() -> Result<()> {
    let fixture = Fixture::new("families_shadow_served_name_filter", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 12).await?;
    assert_counts(&report, &[], &[]);
    assert_eq!(report.names, 1);
    sqlx::query(
        "UPDATE name_current
         SET canonicality_summary = canonicality_summary || '{\"state\": \"orphaned\"}'::jsonb",
    )
    .execute(&fixture.pool)
    .await?;
    let filtered = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert_counts(&filtered, &[], &[]);
    assert_eq!(filtered.names, 0, "{:#?}", filtered.lines);
    fixture.cleanup().await
}

/// Adversarial review of 6c8bdf8b, note 1: the named causes need the name's identity facts to
/// match the log too, as the same-block case does. Under an unnamed path-expiry release, an
/// epoch start the families hold that the log does not give (here one pointing at the grant,
/// which is no AuthorityEpochChanged) leaves the released name's fields mismatches rather than
/// the named cause. The start carries no resource, so the reader skips it and the shadow read
/// is unchanged; only the check fails.
#[tokio::test]
async fn a_wrong_epoch_start_under_an_unnamed_release_is_not_the_named_cause() -> Result<()> {
    let fixture = Fixture::new("families_shadow_unnamed_release_start", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 1_800_000_100u64}),
    )
    .await?;
    unnamed_path_expiry(&fixture, "path-expiry", 14, &k1, 1_800_000_150).await?;
    let report = publish_and_compare(&fixture, 16).await?;
    let cause = "served_membership_skips_unnamed_path_expiry";
    let known: Vec<String> = [
        "control/expiry",
        "control/registrant",
        "control/status",
        "registration/authority_kind",
        "registration/latest_event_kind",
        "registration/registrant",
        "registration/status",
    ]
    .iter()
    .map(|field| format!("{cause}:{field}"))
    .collect();
    let known: Vec<(&str, usize)> = known.iter().map(|field| (field.as_str(), 1)).collect();
    assert_counts(&report, &known, &[]);
    let (_, before) = shadow_reads(&fixture, 16).await?;
    sqlx::query(
        "INSERT INTO bigname_phase.project_name_state (namespace, logical_name_id, chain_id,
             block_number, transaction_index, log_index, event_identity,
             authority_start_positions)
         VALUES ('ens', $1, $2, 10, 0, 1, 'RegistrationGranted:10:1',
                 jsonb_build_object('ens_v2', jsonb_build_object(
                     'block_number', 10, 'transaction_index', 0, 'log_index', 1,
                     'event_identity', 'RegistrationGranted:10:1',
                     'authority_kind', 'registrar', 'authority_key', NULL, 'owner', NULL,
                     'resource_id', NULL)))",
    )
    .bind(name(1))
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    let (_, after) = shadow_reads(&fixture, 16).await?;
    assert_eq!(after, before, "the reader skips the start");
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
    assert!(
        mutated.known_discrepancy.is_empty() && mutated.expected_delta_fields.is_empty(),
        "a wrong epoch start must not pass as the named cause: {:#?}",
        mutated.lines
    );
    assert_eq!(failed_fields(&mutated).len(), 7, "{:#?}", mutated.lines);
    fixture.cleanup().await
}

/// Three admitted transfers of name 1 at one block, transaction and log, identities a, b and c
/// to Alice, Bob and Carol, written c first so their generated ids run a > b > c. The canonical
/// order takes c (the identity sorts last) and today's order takes a (the highest id), so the
/// recipient is a same-block delta.
async fn three_transfers_at_one_log(fixture: &Fixture, resource: &str) -> Result<()> {
    const CAROL: &str = "0x00000000000000000000000000000000000000cc";
    const GRANTEE: &str = "0x00000000000000000000000000000000000000dd";
    v2_binding(fixture, resource).await?;
    v2(
        fixture,
        10,
        "RegistrationGranted",
        Some(resource),
        json!({"status": "registered", "registrant": GRANTEE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    for (identity, to) in [("c", CAROL), ("b", BOB), ("a", ALICE)] {
        fixture
            .event(
                Event::new(identity, 11, 0, "TokenControlTransferred", V2_REGISTRY)
                    .name(&name(1))
                    .resource(resource)
                    .after(
                        json!({"registry_contract_instance_id": "R", "token_id": "7",
                                  "authority_kind": "registrar", "from": GRANTEE, "to": to}),
                    )
                    .raw(json!({"emitting_address": REGISTRY}))
                    .synthesised(),
            )
            .await?;
    }
    Ok(())
}

/// Pro Q3 on 6c8bdf8b: with the retained row of the canonically selected transfer c deleted,
/// the shadow and a canonical read of the remaining events both give Bob, and today's order over
/// them still gives the served Alice. The log holds c under the name's resource, so the families
/// lack a retained event the reducer keeps: no field of the name may pass.
#[tokio::test]
async fn a_missing_decisive_transfer_row_stays_a_mismatch() -> Result<()> {
    let fixture = Fixture::new("families_shadow_missing_transfer", 20).await?;
    let k1 = uuid(1);
    three_transfers_at_one_log(&fixture, &k1).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(served.control("registry_owner"), json!(ALICE));
    assert_eq!(
        shadow.control["registry_owner"],
        json!("0x00000000000000000000000000000000000000cc")
    );
    let delta = [
        ("d12_same_block_order:control/registrant", 1),
        ("d12_same_block_order:control/registry_owner", 1),
        ("d12_same_block_order:registration/registrant", 1),
    ];
    assert_counts(&report, &[], &delta);
    sqlx::query("DELETE FROM bigname_phase.project_lifecycle_event WHERE event_identity = 'c'")
        .execute(&fixture.pool)
        .await?;
    let (_, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(shadow.control["registry_owner"], json!(BOB));
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert!(
        mutated.known_discrepancy.is_empty() && mutated.expected_delta_fields.is_empty(),
        "a missing decisive transfer must not pass: {:#?}",
        mutated.lines
    );
    assert_eq!(
        failed_fields(&mutated),
        [
            "control/registrant",
            "control/registry_owner",
            "registration/registrant"
        ],
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// Pro Q3 on 6c8bdf8b: the same shape with the name's binding candidate deleted. The shadow
/// still reads the selected resource, but the families no longer hold the candidate the
/// surface binding gives, so the same-block deltas must not pass.
#[tokio::test]
async fn a_missing_binding_candidate_stays_a_mismatch() -> Result<()> {
    let fixture = Fixture::new("families_shadow_missing_candidate", 20).await?;
    let k1 = uuid(1);
    three_transfers_at_one_log(&fixture, &k1).await?;
    publish_and_compare(&fixture, 12).await?;
    sqlx::query("DELETE FROM bigname_phase.project_binding_candidate WHERE logical_name_id = $1")
        .bind(name(1))
        .execute(&fixture.pool)
        .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert!(
        mutated.known_discrepancy.is_empty() && mutated.expected_delta_fields.is_empty(),
        "a missing candidate must not pass: {:#?}",
        mutated.lines
    );
    assert!(
        failed_fields(&mutated).contains(&"control/registry_owner".to_owned()),
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// Pro Q3 on 6c8bdf8b: the three-transfer shape with the name's ENSv2 epoch start at block 9
/// deleted from the families. The log holds the AuthorityEpochChanged whose latest per arm the
/// families keep, so the families lack a fact both reads use and the same-block deltas must not
/// pass, though the start decides none of the differing fields.
#[tokio::test]
async fn a_missing_epoch_start_stays_a_mismatch() -> Result<()> {
    let fixture = Fixture::new("families_shadow_missing_start", 20).await?;
    let k1 = uuid(1);
    three_transfers_at_one_log(&fixture, &k1).await?;
    fixture
        .write(
            9,
            1,
            "AuthorityEpochChanged",
            V2_REGISTRY,
            Some(&name(1)),
            Some(&k1),
            json!({"authority_kind": "registrar", "owner": ALICE}),
            REGISTRY,
        )
        .await?;
    let report = publish_and_compare(&fixture, 12).await?;
    let delta = [
        ("d12_same_block_order:control/registrant", 1),
        ("d12_same_block_order:control/registry_owner", 1),
        ("d12_same_block_order:registration/registrant", 1),
    ];
    assert_counts(&report, &[], &delta);
    sqlx::query(
        "UPDATE bigname_phase.project_name_state SET authority_start_positions = '{}'
         WHERE logical_name_id = $1",
    )
    .bind(name(1))
    .execute(&fixture.pool)
    .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert!(
        mutated.known_discrepancy.is_empty() && mutated.expected_delta_fields.is_empty(),
        "a missing epoch start must not pass: {:#?}",
        mutated.lines
    );
    assert!(
        failed_fields(&mutated).contains(&"control/registry_owner".to_owned()),
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// Pro Q4 on 6c8bdf8b: the family keeps each retained event under the key step 2 derives from
/// its log row, and the reader partitions events by that key. Refiling the canonically selected
/// transfer c under a fresh resource S drops it from the name's facts: the shadow and a
/// canonical read of the rest give Bob while today's order still gives the served Alice.
/// Refiling it under the name's own triple keeps it loaded, and the reader still gives Carol,
/// but the placement is not what step 2 derives. Neither may pass.
#[tokio::test]
async fn a_transfer_refiled_under_another_key_stays_a_mismatch() -> Result<()> {
    let fixture = Fixture::new("families_shadow_refiled_transfer", 20).await?;
    let k1 = uuid(1);
    three_transfers_at_one_log(&fixture, &k1).await?;
    publish_and_compare(&fixture, 12).await?;
    let triple = json!([name(1), "R", "7"]).to_string();
    for (case, kind, key) in [
        ("a fresh resource", "resource", uuid(9)),
        ("the name's triple", "triple", triple),
    ] {
        sqlx::query(
            "UPDATE bigname_phase.project_lifecycle_event SET state_kind = $1, state_key = $2
             WHERE event_identity = 'c'",
        )
        .bind(kind)
        .bind(&key)
        .execute(&fixture.pool)
        .await?;
        let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
        assert!(
            mutated.known_discrepancy.is_empty() && mutated.expected_delta_fields.is_empty(),
            "{case}: a refiled transfer must not pass: {:#?}",
            mutated.lines
        );
        // Either way the three same-block deltas of the baseline are refused, and nothing
        // else differs.
        assert_eq!(
            failed_fields(&mutated),
            [
                "control/registrant",
                "control/registry_owner",
                "registration/registrant"
            ],
            "{case}: {:#?}",
            mutated.lines
        );
    }
    fixture.cleanup().await
}

/// Pro Q4 on 6c8bdf8b: the decoded name of a retained row is loaded but not read by this
/// comparison (admission recomputes an unnamed event's name from its resource, namehash and
/// binding candidates), so corrupting it changes neither the shadow nor the report. This pins
/// that the comparison does not detect decoded-name corruption.
#[tokio::test]
async fn a_wrong_decoded_name_changes_nothing_the_comparison_reads() -> Result<()> {
    let fixture = Fixture::new("families_shadow_decoded_name", 20).await?;
    let k1 = uuid(1);
    three_transfers_at_one_log(&fixture, &k1).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    let (_, before) = shadow_support::name(&fixture, 12, &name(1)).await?;
    sqlx::query(
        "UPDATE bigname_phase.project_lifecycle_event SET decoded_logical_name_id = $1
         WHERE chain_id = $2",
    )
    .bind(name(2))
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    let (_, after) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(after, before);
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert_eq!(mutated.lines, report.lines);
    assert_eq!(mutated.expected_delta_fields, report.expected_delta_fields);
    assert_eq!(mutated.mismatched, report.mismatched);
    fixture.cleanup().await
}

/// Name `n` bound under arm ens_v1 to `lease` at block 9 (its SurfaceBound at log `n`), then two
/// registrar snapshot grants of it at block 10, transaction 0, log `n`: `b{n}` written first
/// with registration time `b_time` and `a{n}` second with `a_time`, so a has the higher
/// generated id. The canonical order takes b (its identity sorts last) and today's order takes
/// a, so the registration time is a same-block delta.
async fn snapshot_grants(
    fixture: &Fixture,
    n: u64,
    lease: &str,
    a_time: i64,
    b_time: i64,
) -> Result<()> {
    let logical = name(n);
    fixture
        .binding(
            &uuid(100 + n as u32),
            &logical,
            lease,
            "ens_v1",
            9,
            n as i64,
            None,
        )
        .await?;
    let bound = format!("bound{n}");
    fixture
        .event(
            Event::new(&bound, 9, n as i64, "SurfaceBound", V1_REGISTRAR)
                .name(&logical)
                .resource(lease)
                .after(json!({"authority_kind": "registrar", "state_derived": false}))
                .raw(json!({"emitting_address": REGISTRAR})),
        )
        .await?;
    for (identity, time) in [(format!("b{n}"), b_time), (format!("a{n}"), a_time)] {
        fixture
            .event(
                Event::new(&identity, 10, n as i64, "RegistrationGranted", V1_REGISTRAR)
                    .name(&logical)
                    .resource(lease)
                    .after(
                        json!({"authority_kind": "registrar", "status": "registered",
                                  "registrant": ALICE, "expiry": 2_000_000_000u64,
                                  "state_derived": true, "surface_materialization": true,
                                  "registrar_surface_snapshot": true,
                                  "original_registered_at": time}),
                    )
                    .raw(json!({"emitting_address": REGISTRAR})),
            )
            .await?;
    }
    Ok(())
}

/// The `registration/registered_at` lines of `name` in a report.
fn registered_at_lines(report: &shadow_support::compare::Report, name: &str) -> Vec<String> {
    report
        .lines
        .iter()
        .filter(|line| {
            line.contains(&format!("key={name} "))
                && line.contains("field=registration/registered_at ")
        })
        .cloned()
        .collect()
}

/// Pro Q7 on 6c8bdf8b: the snapshot registration time renders through a seconds-to-timestamp
/// map the loader builds from the family rows of the whole chunk. Names 1 and 2 each hold the
/// two-grant shape. With name 1's family time for b cleared, its shadow gives null; a canonical
/// rebuild restores b's logged time V but looked V up in that map, which held V only when name 2
/// shared the chunk. So name 1 passed as a same-block delta in a chunk of its own and failed
/// beside name 2. The conversions now come from the rebuilt events themselves: name 1's
/// registration time is a mismatch in chunks of one and of five hundred, and every result is
/// the same in both.
#[tokio::test]
async fn a_snapshot_time_is_rebuilt_from_the_log_in_any_chunk() -> Result<()> {
    const U: i64 = 1_700_000_000;
    const V: i64 = 1_700_000_500;
    let fixture = Fixture::new("families_shadow_snapshot_time", 20).await?;
    snapshot_grants(&fixture, 1, &uuid(1), U, V).await?;
    snapshot_grants(&fixture, 2, &uuid(2), U, V).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    assert_counts(
        &report,
        &[],
        &[("d12_same_block_order:registration/registered_at", 2)],
    );
    sqlx::query(
        "UPDATE bigname_phase.project_lifecycle_event SET original_registered_at = NULL
         WHERE event_identity = 'b1'",
    )
    .execute(&fixture.pool)
    .await?;
    let (_, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(shadow.registration["registered_at"], Value::Null);
    let apart = shadow_support::compare::compare_in_chunks(&fixture.pool, CHAIN, 12, 1).await?;
    assert!(
        registered_at_lines(&apart, &name(1))
            .iter()
            .all(|line| line.starts_with("SEPOLIA_END_TO_END_SHADOW_MISMATCH")),
        "alone: a cleared snapshot time must not pass: {:#?}",
        apart.lines
    );
    let together =
        shadow_support::compare::compare_in_chunks(&fixture.pool, CHAIN, 12, 500).await?;
    assert_eq!(together, apart, "the chunk decides a result");
    assert_eq!(
        together.expected_delta_fields,
        [(
            "d12_same_block_order:registration/registered_at".to_owned(),
            1
        )]
        .into(),
        "{:#?}",
        together.lines
    );
    assert_eq!(together.mismatched, 1, "{:#?}", together.lines);
    fixture.cleanup().await
}

/// Name `n` bound under arm ens_v2 to `resource` at block 9 (log `n`), granted at block 10 and
/// then given three transfers at block 11, all at log `n`: `c{n}`, `b{n}` and `a{n}` to Carol,
/// Bob and Alice in that write order, so the canonical order takes c and today's takes a.
async fn transfers_of(fixture: &Fixture, n: u64, resource: &str) -> Result<()> {
    const CAROL: &str = "0x00000000000000000000000000000000000000cc";
    let logical = name(n);
    let log = n as i64;
    fixture
        .binding(
            &uuid(100 + n as u32),
            &logical,
            resource,
            "ens_v2",
            9,
            log,
            None,
        )
        .await?;
    let v2_after = |after: Value| {
        let mut after = after;
        after["registry_contract_instance_id"] = json!("R");
        after["token_id"] = json!(n.to_string());
        after["authority_kind"] = json!("registrar");
        after
    };
    let (bound, grant) = (format!("bound{n}"), format!("grant{n}"));
    fixture
        .event(
            Event::new(&bound, 9, log, "SurfaceBound", V2_REGISTRY)
                .name(&logical)
                .resource(resource)
                .after(json!({"authority_kind": "registrar", "state_derived": false}))
                .raw(json!({"emitting_address": REGISTRY})),
        )
        .await?;
    fixture
        .event(
            Event::new(&grant, 10, log, "RegistrationGranted", V2_REGISTRY)
                .name(&logical)
                .resource(resource)
                .after(v2_after(
                    json!({"status": "registered", "registrant": ALICE,
                                       "expiry": 2_000_000_000u64}),
                ))
                .raw(json!({"emitting_address": REGISTRY})),
        )
        .await?;
    for (identity, to) in [
        (format!("c{n}"), CAROL),
        (format!("b{n}"), BOB),
        (format!("a{n}"), ALICE),
    ] {
        fixture
            .event(
                Event::new(&identity, 11, log, "TokenControlTransferred", V2_REGISTRY)
                    .name(&logical)
                    .resource(resource)
                    .after(v2_after(json!({"from": ALICE, "to": to})))
                    .raw(json!({"emitting_address": REGISTRY})),
            )
            .await?;
    }
    Ok(())
}

/// Pro Q7 on 6c8bdf8b: the excuse pass loads its inputs once per chunk, so every name's result
/// must not depend on which names share its chunk. Names 1 and 2 share one resource, names 3
/// and 4 are ENSv1 children of one parent node, and name 5's family rows are deleted after the
/// publication, so it differs with no facts; the whole report is the same in chunks of one, two
/// and five hundred.
#[tokio::test]
async fn every_name_gets_the_same_result_in_any_chunk() -> Result<()> {
    let fixture = Fixture::new("families_shadow_chunk_equivalence", 20).await?;
    let (shared, own) = (uuid(1), uuid(5));
    transfers_of(&fixture, 1, &shared).await?;
    transfers_of(&fixture, 2, &shared).await?;
    for n in [3u64, 4] {
        let lease = uuid(n as u32);
        fixture
            .binding(
                &uuid(100 + n as u32),
                &name(n),
                &lease,
                "ens_v1",
                9,
                n as i64,
                None,
            )
            .await?;
        let bound = format!("bound{n}");
        fixture
            .event(
                Event::new(&bound, 9, n as i64, "SurfaceBound", V1_REGISTRAR)
                    .name(&name(n))
                    .resource(&lease)
                    .after(json!({"authority_kind": "registrar", "state_derived": false}))
                    .raw(json!({"emitting_address": REGISTRAR})),
            )
            .await?;
        let owner = format!("owner{n}");
        fixture
            .event(
                Event::new(
                    &owner,
                    10,
                    n as i64,
                    "SubregistryChanged",
                    "ens_v1_registry_l1",
                )
                .name(&name(n))
                .after(json!({"source_event": "NewOwner", "node": node(50),
                                  "child_node": node(n), "owner": ALICE,
                                  "emitter_role": "registry"}))
                .raw(json!({"emitting_address": REGISTRY})),
            )
            .await?;
        let grant = format!("grant{n}");
        fixture
            .event(
                Event::new(
                    &grant,
                    10,
                    10 + n as i64,
                    "RegistrationGranted",
                    V1_REGISTRAR,
                )
                .name(&name(n))
                .resource(&lease)
                .after(
                    json!({"authority_kind": "registrar", "status": "registered",
                                  "registrant": ALICE, "expiry": 2_000_000_000u64}),
                )
                .raw(json!({"emitting_address": REGISTRAR})),
            )
            .await?;
    }
    transfers_of(&fixture, 5, &own).await?;
    publish(&fixture, 12).await?;
    for table in [
        "project_lifecycle_event",
        "project_binding_candidate",
        "project_lifecycle_key_state",
        "project_lifecycle_triple_summary",
        "project_lifecycle_association",
    ] {
        let column = if table == "project_lifecycle_event" {
            "original_logical_name_id"
        } else {
            "logical_name_id"
        };
        sqlx::query(&format!(
            "DELETE FROM bigname_phase.{table} WHERE {column} = $1"
        ))
        .bind(name(5))
        .execute(&fixture.pool)
        .await?;
    }
    let reports = [
        shadow_support::compare::compare_in_chunks(&fixture.pool, CHAIN, 12, 1).await?,
        shadow_support::compare::compare_in_chunks(&fixture.pool, CHAIN, 12, 2).await?,
        shadow_support::compare::compare_in_chunks(&fixture.pool, CHAIN, 12, 500).await?,
    ];
    assert_eq!(reports[0].names, 5);
    assert!(
        reports[0].expected_delta > 0 && reports[0].mismatched > 0,
        "{:#?}",
        reports[0].lines
    );
    assert_eq!(reports[0], reports[1]);
    assert_eq!(reports[0], reports[2]);
    fixture.cleanup().await
}

/// Pro Q9 on 6c8bdf8b: the corpus expectation is read by the comparison itself, at the
/// publication its report is for, rather than once after every target has run. Replacing the
/// release's block with another at the same height afterwards removes the release from the
/// log, so a later read counts nothing, but the report keeps what its own publication gave.
#[tokio::test]
async fn the_corpus_expectation_is_read_at_the_report_publication() -> Result<()> {
    use shadow_support::compare::{Options, Publication, compare_with, corpus_expectation};
    const REPLACEMENT: &str = "0x00000000000000000000000000000000000000000000000000000000000bee14";
    let fixture = Fixture::new("families_shadow_corpus_snapshot", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 1_800_000_100u64}),
    )
    .await?;
    unnamed_path_expiry(&fixture, "path-expiry", 14, &k1, 1_800_000_150).await?;
    shadow_support::publish(&fixture, 16).await?;
    let options = Options {
        corpus: true,
        ..Options::default()
    };
    let publication = Publication::readable(&fixture.pool, CHAIN, 16).await?;
    let report = compare_with(&fixture.pool, CHAIN, &publication, options).await?;
    let recorded = report.corpus_expected.expect("read with the report");
    assert_eq!(recorded.len(), 8, "{recorded:?}");
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 14",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state)
         VALUES ($1, $2, $3, 14, to_timestamp(1800000168), 'canonical')",
    )
    .bind(CHAIN)
    .bind(REPLACEMENT)
    .bind(support::hash(13))
    .execute(&fixture.pool)
    .await?;
    assert!(
        corpus_expectation(&fixture.pool, CHAIN, 16)
            .await?
            .is_empty()
    );
    fixture.cleanup().await
}

/// Pro Q1 on 6c8bdf8b: the comparison is for one publication, a height and its hash, and it
/// reads the served tables, the families and the log separately. Once block 12 is replaced by
/// 12' at the same height, the comparison refuses to run until the families and the served
/// publication both stand on 12', and then compares.
#[tokio::test]
async fn a_same_height_replacement_is_compared_only_once_both_sides_follow_it() -> Result<()> {
    const REPLACEMENT: &str = "0x00000000000000000000000000000000000000000000000000000000000bee12";
    let fixture = Fixture::new("families_shadow_replaced_target", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 12).await?;
    assert_counts(&report, &[], &[]);
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number = 12",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state)
         VALUES ($1, $2, $3, 12, to_timestamp(1800000144), 'canonical')",
    )
    .bind(CHAIN)
    .bind(REPLACEMENT)
    .bind(support::hash(11))
    .execute(&fixture.pool)
    .await?;
    let refused = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await;
    assert!(refused.is_err(), "neither side follows 12': {refused:?}");
    let token = bigname_project::families::input_token(&fixture.pool, CHAIN).await?;
    let outcome = bigname_project::families::apply(
        &fixture.pool,
        CHAIN,
        &bigname_project::Marker {
            number: 12,
            hash: REPLACEMENT.to_owned(),
        },
        bigname_project::families::FamilyMode::Normal,
        &token,
        &bigname_project::families::FamilyOptions::new(support::CONTENT_HASH),
    )
    .await;
    assert_eq!(outcome.skipped, None);
    let refused = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await;
    assert!(
        refused.is_err(),
        "the served side does not follow 12': {refused:?}"
    );
    shadow_support::publish_served(&fixture, 12).await?;
    let report = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert_counts(&report, &[], &[]);
    assert_eq!(report.names, 1);
    fixture.cleanup().await
}

/// Codex thread PRRT_kwDOSJpxAs6mBYSt: the lifecycle loader reads F1's epoch starts for the
/// requested chain only, like every other lifecycle read. Name 1 has no epoch on this chain;
/// another chain's name-state row for the same logical name carries an ENSv2 epoch start on the
/// name's resource, after its grant and owned by Bob. Read for this chain, the name has no
/// epoch start, and its registration and control values and the comparison stay the baseline.
#[tokio::test]
async fn another_chains_epoch_start_is_not_read() -> Result<()> {
    let fixture = Fixture::new("families_shadow_other_chain_start", 20).await?;
    let k1 = uuid(1);
    v2_binding(&fixture, &k1).await?;
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    assert_counts(&report, &[], &[]);
    let (_, before) = shadow_reads(&fixture, 16).await?;
    sqlx::query(
        "INSERT INTO bigname_phase.project_name_state (namespace, logical_name_id, chain_id,
             block_number, transaction_index, log_index, event_identity,
             authority_start_positions)
         VALUES ('ens', $1, 'other-chain', 11, 0, 1, 'AuthorityEpochChanged:11:1',
                 jsonb_build_object('ens_v2', jsonb_build_object(
                     'block_number', 11, 'transaction_index', 0, 'log_index', 1,
                     'event_identity', 'AuthorityEpochChanged:11:1',
                     'authority_kind', 'registrar', 'authority_key', NULL, 'owner', $2::text,
                     'resource_id', $3::text)))",
    )
    .bind(name(1))
    .bind(BOB)
    .bind(&k1)
    .execute(&fixture.pool)
    .await?;
    let rows =
        bigname_storage::load_name_current_by_logical_name_ids(&fixture.pool, &[name(1)]).await?;
    let row = &rows[&name(1)];
    let input = bigname_storage::families::control::lifecycle::NameInput {
        logical_name_id: row.logical_name_id.clone(),
        namehash: row.namehash.to_ascii_lowercase(),
        selection:
            bigname_storage::families::control::lifecycle::AuthoritySelection::from_provenance(
                &row.provenance,
            ),
    };
    let facts = bigname_storage::families::control::lifecycle::load_name_facts(
        &fixture.pool,
        CHAIN,
        std::slice::from_ref(&input),
    )
    .await?;
    assert_eq!(
        facts[0].authority_starts,
        Value::Null,
        "another chain's epoch start is not this chain's"
    );
    let (_, after) = shadow_reads(&fixture, 16).await?;
    assert_eq!(after, before);
    let other = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
    assert_counts(&other, &[], &[]);
    assert_eq!(
        (other.names, other.equal),
        (report.names, report.equal),
        "{:#?}",
        other.lines
    );
    fixture.cleanup().await
}

/// Pro r6 Q6 on ba2ffbd5, after step 2 keyed `project_name_state` by chain (28aa091b): two
/// chains publish the same logical name, each with its own ENSv1 epoch start, and each keeps its
/// own row; the lifecycle loader reads each chain's start for that chain. A later epoch on one
/// chain moves that chain's row and start only. Today a logical name's surface belongs to one
/// chain (normalized_events references name_surfaces by chain), so, as step 2's own chain-key
/// test does, the fixture drops that reference to let a second chain carry the name.
#[tokio::test]
async fn two_chains_keep_their_own_epoch_start_for_one_name() -> Result<()> {
    const OTHER: &str = "other-chain";
    const V1_REGISTRY: &str = "ens_v1_registry_l1";
    let fixture = Fixture::new("families_shadow_two_chain_starts", 20).await?;
    fixture.lineage(OTHER, 20).await?;
    sqlx::query(
        "ALTER TABLE normalized_events
             DROP CONSTRAINT normalized_events_chain_id_logical_name_id_fkey",
    )
    .execute(&fixture.pool)
    .await?;
    let named = name(1);
    let epoch = |identity: &'static str, block: i64, chain: &'static str, family: &'static str| {
        let kind = if family == V1_REGISTRAR {
            "registrar"
        } else {
            "registry_only"
        };
        Event::new(identity, block, 1, "AuthorityEpochChanged", family)
            .on(chain)
            .name(&named)
            .after(json!({"authority_kind": kind}))
            .raw(json!({"emitting_address": REGISTRAR}))
    };
    fixture
        .event(epoch("this:epoch:10", 10, CHAIN, V1_REGISTRAR))
        .await?;
    fixture
        .event(epoch("other:epoch:11", 11, OTHER, V1_REGISTRY))
        .await?;
    let apply = |chain: &'static str, target: i64| {
        let fixture = &fixture;
        async move {
            let outcome = fixture.apply_on(chain, target).await;
            assert_eq!(outcome.skipped, None, "{chain} at {target}");
        }
    };
    apply(CHAIN, 12).await;
    apply(OTHER, 12).await;
    // Each chain's row, and the start the loader reads for that chain.
    let starts = || async {
        let rows: Vec<(String, Value)> = sqlx::query_as(
            "SELECT chain_id, authority_start_positions -> 'ens_v1'
             FROM bigname_phase.project_name_state WHERE logical_name_id = $1 ORDER BY 1",
        )
        .bind(&named)
        .fetch_all(&fixture.pool)
        .await?;
        let input = bigname_storage::families::control::lifecycle::NameInput {
            logical_name_id: named.clone(),
            namehash: node(1),
            selection: Default::default(),
        };
        let mut out = Vec::new();
        for (chain, stored) in rows {
            let facts = bigname_storage::families::control::lifecycle::load_name_facts(
                &fixture.pool,
                &chain,
                std::slice::from_ref(&input),
            )
            .await?;
            let read = &facts[0].authority_starts["ens_v1"];
            assert_eq!(read, &stored, "the loader reads {chain}'s own start");
            out.push(
                json!({"chain": chain, "block_number": stored["block_number"],
                            "authority_kind": stored["authority_kind"]}),
            );
        }
        Ok::<_, anyhow::Error>(out)
    };
    let (this, other) = (
        json!({"chain": CHAIN, "block_number": 10, "authority_kind": "registrar"}),
        json!({"chain": OTHER, "block_number": 11, "authority_kind": "registry_only"}),
    );
    let sorted = |mut rows: Vec<Value>| {
        rows.sort_by_key(|row| row["chain"].as_str().map(str::to_owned));
        rows
    };
    assert_eq!(starts().await?, sorted(vec![this.clone(), other]));
    fixture
        .event(epoch("other:epoch:13", 13, OTHER, V1_REGISTRY))
        .await?;
    apply(OTHER, 14).await;
    let moved = json!({"chain": OTHER, "block_number": 13, "authority_kind": "registry_only"});
    assert_eq!(starts().await?, sorted(vec![this, moved]));
    fixture.cleanup().await
}
