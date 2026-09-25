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
use serde_json::{Value, json};
use shadow_support::{Served, publish_and_compare};
use support::{Event, Fixture, uuid};

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
    assert_eq!((report.names, report.mismatched), (1, 0));
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
    assert_eq!(report.mismatched, 0);
    let (served, _) = assert_name_equal(&fixture, 14).await?;
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("ExpiryChanged")
    );
    assert_eq!(served.registration("expiry"), json!(2_100_000_000u64));
    assert_eq!(served.registration("status"), json!("active"));
    fixture.cleanup().await
}

#[tokio::test]
async fn grant_path_expiry_then_expiry_change_keeps_the_path_release() -> Result<()> {
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
    v2(
        &fixture,
        12,
        "RegistrationReleased",
        None,
        path_expiry(1_800_000_100),
    )
    .await?;
    v2(
        &fixture,
        14,
        "ExpiryChanged",
        Some(&k1),
        json!({"expiry": 1_800_000_200u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    assert_eq!(report.mismatched, 0);
    let (served, trace) = assert_name_equal(&fixture, 16).await?;
    // The path release stays the key's candidate; the later expiry change is the latest kind
    // and gives the expiry, and nothing retires the name.
    assert_eq!(trace["key_candidates"][0]["kind"], json!("PathExpiry"));
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("ExpiryChanged")
    );
    assert_eq!(served.registration("expiry"), json!(1_800_000_200u64));
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
    assert_eq!(report.mismatched, 0);
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
    assert_eq!(report.mismatched, 0);
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
    assert_eq!(report.mismatched, 0);
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

/// Two grants of one triple in one transaction, inserted so the generated ids run against the
/// log order: the families take the later log (D12), and a production difference, if any, is
/// counted as the disclosed same-block delta rather than a mismatch.
#[tokio::test]
async fn two_grants_in_one_transaction_take_the_later_log() -> Result<()> {
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
    assert_eq!(report.mismatched, 0);
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(
        shadow.registration["registrant"],
        json!(BOB),
        "the D12 answer"
    );
    if served.registration("registrant") != json!(BOB) {
        assert_eq!(
            report.expected_delta, 1,
            "the production difference is disclosed"
        );
    }
    fixture.cleanup().await
}

/// A synthesised grant and a synthesised expiry change of one key in one block, with no
/// transaction or log: D12 orders them by identity bytes, so the grant `b-grant` is the later
/// one, while today's order takes the higher generated id, the expiry change `a-expiry`. The
/// families serve the D12 answer, the production answer differs, and the difference is counted as
/// the disclosed same-block delta (brief section 4.3).
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
    assert_eq!(report.mismatched, 0);
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
    assert_eq!(report.mismatched, 0);
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
    assert_eq!(report.mismatched, 0);
    let (served, _) = assert_name_equal(&fixture, 16).await?;
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("RegistrationReleased")
    );
    fixture.cleanup().await
}

/// Step 2 finding, kept visible: a grant whose after-state has no `authority_kind` is retained
/// as `registrar` (crates/project/src/families/lifecycle.rs:317-322) while the served block reads
/// the raw after-state and serves null (build.sql:30, :394). The comparison counts it under its
/// name rather than as a mismatch.
#[tokio::test]
async fn a_grant_without_authority_kind_is_the_known_default_discrepancy() -> Result<()> {
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
    assert_eq!(report.mismatched, 0);
    assert_eq!(
        report
            .known_discrepancy
            .get("authority_kind_defaulted_to_registrar"),
        Some(&1)
    );
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(served.registration("authority_kind"), Value::Null);
    assert_eq!(shadow.registration["authority_kind"], json!("registrar"));
    fixture.cleanup().await
}

/// History A: grant, path expiry, explicit release. The path release lies between the grant and
/// the explicit release, so the explicit release has no witness and the path release is the
/// key's candidate. The interpreter's path release carries no resource, so the released-for-
/// another-resource exclusion (build.sql:339-343) drops it against the bound resource and the
/// name serves the binding as active, in both reads.
#[tokio::test]
async fn history_a_grant_path_expiry_explicit_release_picks_the_path_release() -> Result<()> {
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
    v2(
        &fixture,
        12,
        "RegistrationReleased",
        None,
        path_expiry(1_800_000_100),
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
    assert_eq!(report.mismatched, 0, "{:?}", report.lines);
    let (served, trace) = assert_name_equal(&fixture, 16).await?;
    assert_eq!(trace["key_candidates"][0]["kind"], json!("PathExpiry"));
    assert_eq!(trace["selected_event"], Value::Null);
    assert_eq!(served.registration("status"), json!("active"));
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
    assert_eq!(report.mismatched, 0, "{:?}", report.lines);
    let (served, trace) = assert_name_equal(&fixture, 16).await?;
    assert_eq!(trace["key_candidates"][0]["kind"], json!("Explicit"));
    assert_eq!(served.registration("status"), json!("released"));
    assert_eq!(served.registration("expiry"), Value::Null);
    assert_eq!(served.control("status"), json!("unregistered"));
    fixture.cleanup().await
}

/// P, V, W: a path expiry, a renewal that revives from it, then an ordinary renewal. Renewals
/// are not registration candidates, so the path release stays the candidate, and the latest of
/// the five kinds is the renewal.
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
    v2(
        &fixture,
        12,
        "RegistrationReleased",
        None,
        path_expiry(1_800_000_100),
    )
    .await?;
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
    assert_eq!(report.mismatched, 0, "{:?}", report.lines);
    let (served, trace) = assert_name_equal(&fixture, 18).await?;
    assert_eq!(trace["key_candidates"][0]["kind"], json!("PathExpiry"));
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
        assert_eq!(report.mismatched, 0, "{:?}", report.lines);
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

/// Step 2 finding, kept visible: the served registration names the winning grant's
/// `authority_key` (build.sql:31, :393-420), and step 2 retains that key nowhere a reader can
/// see it (not on the retained row, the key state's `last_grant`, or F1's start positions). The
/// comparison counts it under its name; it closes once step 2 stores the key, because the
/// reader already reads each of those places.
#[tokio::test]
async fn a_grant_authority_key_is_the_known_unretained_key() -> Result<()> {
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
    assert_eq!(report.mismatched, 0, "{:?}", report.lines);
    let (served, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(served.registration("authority_key"), json!("registrar:k1"));
    if shadow.registration["authority_key"].is_null() {
        assert_eq!(
            report.known_discrepancy.get("authority_key_not_retained"),
            Some(&1)
        );
    } else {
        assert_eq!(shadow.registration["authority_key"], json!("registrar:k1"));
    }
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
    assert_eq!(report.mismatched, 0, "{:?}", report.lines);
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
    fixture.cleanup().await
}

/// Finding, kept visible: the interpreter's path-expiry release names its resource and no name
/// (crates/adapters/src/schema_v2/protocol/v2_registry/expiry.rs:58-59). The F2a key state of the
/// resource counts it (crates/project/src/families/lifecycle.rs:71-76), today's name-scoped
/// membership does not (build.sql:322, :366-367), so the five-kind latest differs. The comparison
/// counts it under its name, not as a mismatch, until the design rules on it.
#[tokio::test]
async fn an_unnamed_path_expiry_on_the_resource_is_the_known_membership_finding() -> Result<()> {
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
    let mut after = path_expiry(1_800_000_150);
    after["registry_contract_instance_id"] = json!("R");
    after["token_id"] = json!("7");
    fixture
        .event(
            Event::new("path-expiry", 14, 0, "RegistrationReleased", V2_REGISTRY)
                .resource(&k1)
                .after(after)
                .raw(json!({"emitting_address": REGISTRY}))
                .synthesised(),
        )
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    assert_eq!(report.mismatched, 0, "{:?}", report.lines);
    let (served, shadow) = shadow_support::name(&fixture, 16, &name(1)).await?;
    assert_eq!(
        served.registration("latest_event_kind"),
        json!("RegistrationRenewed")
    );
    assert_eq!(
        shadow.registration["latest_event_kind"],
        json!("RegistrationReleased")
    );
    assert_eq!(
        report
            .known_discrepancy
            .get("unnamed_resource_event_in_key_state"),
        Some(&1)
    );
    fixture.cleanup().await
}
