//! Staging and admission shadow reads (TYR-36 step 3, docs/glossary.md "Shadow read"): an ENSv1
//! registrar row the adapter emits without a name is named by the two staging passes of
//! name_authority/stage.rs:149-198, first through a binding of its lease whose surface
//! namehash is the row's, then, only when no name matches that way, through a NameWrapper
//! candidate that recorded the lease at the row's node. The families decide both passes once
//! over the candidates of every name (the step 2 decoder's attachment, decode.rs:110-139) and
//! trace which pass named a row for a name. Each case publishes with the production batch,
//! follows it with the families and compares every served name and resource with its family
//! read.
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use serde_json::{Value, json};
use shadow_support::{assert_counts, publish_and_compare};
use support::{Event, Fixture, uuid};

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const WRAPPER: &str = "0x00000000000000000000000000000000000000e4";
const ALICE: &str = "0x00000000000000000000000000000000000000aa";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";
const V1_WRAPPER: &str = "ens_v1_wrapper_l1";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// An unnamed registrar row of `lease` at node 1, as the registrar adapter emits it before
/// staging names it.
async fn unnamed(
    fixture: &Fixture,
    identity: &str,
    block: i64,
    kind: &str,
    lease: &str,
    after: Value,
) -> Result<()> {
    let mut after = after;
    after["namehash"] = json!(node(1));
    after["authority_kind"] = json!("registrar");
    fixture
        .event(
            Event::new(identity, block, 1, kind, V1_REGISTRAR)
                .resource(lease)
                .after(after)
                .raw(json!({"emitting_address": REGISTRAR})),
        )
        .await?;
    Ok(())
}

/// A NameWrapper SurfaceBound of `name` on `wrapper` that recorded `lease` at node 1.
fn wrapper_bound<'a>(identity: &'a str, name: &'a str, wrapper: &'a str, lease: &str) -> Event<'a> {
    Event::new(identity, 9, 2, "SurfaceBound", V1_WRAPPER)
        .name(name)
        .resource(wrapper)
        .after(json!({"authority_kind": "wrapper", "node": node(1),
                      "wrapped_registrar_resource_id": lease, "state_derived": false}))
        .raw(json!({"emitting_address": WRAPPER}))
}

async fn trace(fixture: &Fixture, target: i64, logical_name_id: &str) -> Result<Value> {
    let (_, shadow) = shadow_support::name(fixture, target, logical_name_id).await?;
    Ok(Value::Object(shadow.trace))
}

/// Item 8 of the TYR-36 step 3 review (Q3), pass one before pass two across names. Name 1 is
/// bound to lease L; name 2's NameWrapper candidate recorded L at name 1's node. The unnamed
/// renewal of L matches name 1 in pass one, so pass two never runs for it and name 2 does not
/// take it, although its wrapper candidate would match on its own.
#[tokio::test]
async fn a_direct_binding_of_one_name_wins_over_another_names_wrapper() -> Result<()> {
    let fixture = Fixture::new("families_shadow_admission_precedence", 20).await?;
    let (lease, wrapper) = (uuid(1), uuid(2));
    fixture
        .binding(&uuid(100), &name(1), &lease, "ens_v1", 9, 0, None)
        .await?;
    fixture
        .write(
            9,
            0,
            "SurfaceBound",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false}),
            REGISTRAR,
        )
        .await?;
    fixture
        .binding(&uuid(101), &name(2), &wrapper, "ens_v1", 9, 2, None)
        .await?;
    let second = name(2);
    fixture
        .event(wrapper_bound("wrap-2", &second, &wrapper, &lease))
        .await?;
    unnamed(
        &fixture,
        "grant-10",
        10,
        "RegistrationGranted",
        &lease,
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    unnamed(
        &fixture,
        "renewal-12",
        12,
        "RegistrationRenewed",
        &lease,
        json!({"expiry": 2_100_000_000u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    assert_counts(&report, &[], &[]);
    let (served, _) = shadow_support::name(&fixture, 16, &name(1)).await?;
    assert_eq!(served.registration("expiry"), json!(2_100_000_000u64));
    assert_eq!(
        trace(&fixture, 16, &name(1)).await?["staged"],
        json!({"grant-10": "Direct", "renewal-12": "Direct"})
    );
    assert_eq!(trace(&fixture, 16, &name(2)).await?["staged"], Value::Null);
    fixture.cleanup().await
}

/// Item 8 of the TYR-36 step 3 review (Q3), a NameWrapper candidate with no transaction or
/// emitter. Step 2 records the wrapped lease and the node only for a NameWrapper SurfaceBound,
/// so they mark it as one; pass two names the unnamed grant and renewal of the lease for the
/// wrapped name, and the wrapped-lease admission holds them, as today's stage does from the
/// SurfaceBound's family.
#[tokio::test]
async fn a_wrapper_candidate_without_transaction_or_emitter_still_names_its_lease() -> Result<()> {
    let fixture = Fixture::new("families_shadow_admission_wrapper_metadata", 20).await?;
    let (lease, wrapper) = (uuid(1), uuid(2));
    fixture
        .binding(&uuid(100), &name(1), &wrapper, "ens_v1", 9, 2, None)
        .await?;
    let first = name(1);
    fixture
        .event(
            wrapper_bound("wrap-1", &first, &wrapper, &lease)
                .raw(json!({}))
                .synthesised(),
        )
        .await?;
    fixture
        .event(
            Event::new("scope-10", 10, 3, "PermissionScopeChanged", V1_WRAPPER)
                .name(&first)
                .resource(&wrapper)
                .after(
                    json!({"source_event": "NameWrapped", "node": node(1), "fuses": 0,
                              "wrapper_state": "wrapped", "expiry": 2_200_000_000u64}),
                )
                .raw(json!({"emitting_address": WRAPPER})),
        )
        .await?;
    fixture.resource(&lease).await?;
    unnamed(
        &fixture,
        "grant-8",
        8,
        "RegistrationGranted",
        &lease,
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    unnamed(
        &fixture,
        "renewal-12",
        12,
        "RegistrationRenewed",
        &lease,
        json!({"expiry": 2_100_000_000u64}),
    )
    .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    assert_counts(&report, &[], &[]);
    let (served, _) = shadow_support::name(&fixture, 16, &name(1)).await?;
    assert_eq!(served.registration("expiry"), json!(2_100_000_000u64));
    let trace = trace(&fixture, 16, &name(1)).await?;
    assert_eq!(
        trace["staged"],
        json!({"grant-8": "Wrapper", "renewal-12": "Wrapper"})
    );
    fixture.cleanup().await
}

/// Item 8 of the TYR-36 step 3 review (Q3), several names matching one row. Two names with one
/// namehash in different namespaces are both bound to lease L, so pass one matches the unnamed
/// grant for both. The families name it for exactly one, the least name, as the step 2
/// decoder does. Today's stage names it in an UPDATE that takes one unspecified match
/// (stage.rs:149-158), so the served side of this shape is not asserted: only that the shadow
/// gives the row to one name.
#[tokio::test]
async fn a_row_several_names_match_is_named_for_the_least_name_only() -> Result<()> {
    let fixture = Fixture::new("families_shadow_admission_multi_name", 20).await?;
    let lease = uuid(1);
    let other = format!("basenames:{}", node(1));
    // A logical name is its namespace and namehash (name_surfaces_logical_identity_check), so
    // two names share a namehash only across namespaces.
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'basenames', $1, ARRAY[$1], '\\x00', $2, ARRAY[$2], 'ensip15', 'active',
                 $3, $4, 0, 'canonical')",
    )
    .bind(&other)
    .bind(node(1))
    .bind(support::CHAIN)
    .bind(support::hash(0))
    .execute(&fixture.pool)
    .await?;
    fixture
        .binding(&uuid(100), &name(1), &lease, "ens_v1", 9, 0, None)
        .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
             binding_kind, authority_arm, active_from, active_to, chain_id, block_hash,
             block_number, provenance, canonicality_state)
         VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v1',
                 to_timestamp(1800000000 + 9 * 12), NULL, $4, $5, 9,
                 jsonb_build_object('transaction_index', 0, 'log_index', 1), 'canonical')",
    )
    .bind(uuid(101))
    .bind(&other)
    .bind(&lease)
    .bind(support::CHAIN)
    .bind(support::hash(9))
    .execute(&fixture.pool)
    .await?;
    for (logical, log) in [(name(1), 0), (other.clone(), 1)] {
        fixture
            .event(
                Event::new(
                    &format!("bound-{log}"),
                    9,
                    log,
                    "SurfaceBound",
                    V1_REGISTRAR,
                )
                .name(&logical)
                .resource(&lease)
                .after(json!({"authority_kind": "registrar", "state_derived": false}))
                .raw(json!({"emitting_address": REGISTRAR})),
            )
            .await?;
    }
    unnamed(
        &fixture,
        "grant-10",
        10,
        "RegistrationGranted",
        &lease,
        json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
    )
    .await?;
    publish_and_compare(&fixture, 16).await?;
    assert_eq!(
        trace(&fixture, 16, &other).await?["staged"],
        json!({"grant-10": "Direct"})
    );
    assert_eq!(trace(&fixture, 16, &name(1)).await?["staged"], Value::Null);
    fixture.cleanup().await
}
