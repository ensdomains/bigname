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
use shadow_support::{assert_counts, publish, publish_and_compare};
use support::{CHAIN, Event, Fixture, uuid};

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
    // The block synthesised both the binding and its SurfaceBound: neither carries a transaction
    // or log index, which is how step 2 pairs them (identity.rs opening_event).
    sqlx::query(
        "UPDATE surface_bindings SET provenance = provenance - 'transaction_index' - 'log_index'
         WHERE surface_binding_id = $1::uuid",
    )
    .bind(uuid(100))
    .execute(&fixture.pool)
    .await?;
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
/// grant for both. The step 2 decoder names a row through one name only and names nothing when
/// several match, and the families read it the same way: neither name stages the grant. Today's
/// stage names it in an UPDATE that takes one unspecified match (stage.rs:149-158), so the
/// served side of this shape is not asserted.
#[tokio::test]
async fn a_row_several_names_match_is_named_for_none_of_them() -> Result<()> {
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
    // Shadow-only: today's stage names such a row for one unspecified match, so the served
    // side is not compared here.
    publish(&fixture, 16).await?;
    assert_eq!(trace(&fixture, 16, &other).await?["staged"], Value::Null);
    assert_eq!(trace(&fixture, 16, &name(1)).await?["staged"], Value::Null);
    fixture.cleanup().await
}

const BOB: &str = "0x00000000000000000000000000000000000000bb";
const CAROL: &str = "0x00000000000000000000000000000000000000cc";
const HOLDER: &str = "0x00000000000000000000000000000000000000dd";

/// The complete comparison's mismatched fields, sorted.
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

/// Pro r5 Q1 on c23e3e5b: name 1 is bound directly to lease L (a candidate whose surface
/// namehash is node 1) and granted at block 10. At block 11 three registrar transfers share one
/// block, transaction and log: `c`, unnamed on L at node 1, to Carol, then `b` and `a`, named
/// for name 1, to Bob and Alice, written in that order so their generated ids run a > b > c.
/// Pass one names `c` for name 1 through the candidate's namehash, so the canonical order takes
/// `c` (Carol) and today's order `a` (Alice): both registrants are same-block deltas. Name 2 is a
/// bystander placed by `second`.
async fn staged_transfer_race(fixture: &Fixture) -> Result<(String, String)> {
    staged_race(fixture, Second::Apart).await
}

/// Where the race puts name 2.
enum Second {
    /// Bound directly to its own lease M (`uuid(3)`), which no staging pass of L reads.
    Apart,
    /// Bound directly to lease L too, at its own node, so pass one of L reads its candidate
    /// and passes it over.
    Beside,
    /// Bound to NameWrapper resource W (`uuid(2)`), whose SurfaceBound recorded L at node 1
    /// (the shape of `a_direct_binding_of_one_name_wins_over_another_names_wrapper`), so pass
    /// two of L would match it; pass one matches name 1 first.
    Wrapped,
}

/// The race with name 2 placed by `second`. Returns L and name 2's resource.
async fn staged_race(fixture: &Fixture, second: Second) -> Result<(String, String)> {
    let lease = uuid(1);
    let placed = match second {
        Second::Apart => Some(uuid(3)),
        Second::Beside => Some(lease.clone()),
        Second::Wrapped => None,
    };
    let mut bound = vec![(uuid(100), name(1), lease.clone(), 0)];
    bound.extend(
        placed
            .clone()
            .map(|resource| (uuid(101), name(2), resource, 3)),
    );
    for (binding, logical, resource, log) in &bound {
        fixture
            .binding(binding, logical, resource, "ens_v1", 9, *log, None)
            .await?;
        fixture
            .write(
                9,
                *log,
                "SurfaceBound",
                V1_REGISTRAR,
                Some(logical),
                Some(resource),
                json!({"authority_kind": "registrar", "state_derived": false}),
                REGISTRAR,
            )
            .await?;
    }
    let second_resource = match placed {
        Some(resource) => resource,
        None => {
            let wrapper = uuid(2);
            fixture
                .binding(&uuid(101), &name(2), &wrapper, "ens_v1", 9, 2, None)
                .await?;
            let second = name(2);
            fixture
                .event(wrapper_bound("wrap-2", &second, &wrapper, &lease))
                .await?;
            wrapper
        }
    };
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            json!({"authority_kind": "registrar", "status": "registered", "registrant": HOLDER,
                   "expiry": 2_000_000_000u64}),
            REGISTRAR,
        )
        .await?;
    let first = name(1);
    for (identity, to, named) in [("c", CAROL, false), ("b", BOB, true), ("a", ALICE, true)] {
        let mut after = json!({"authority_kind": "registrar", "from": HOLDER, "to": to});
        let mut event = Event::new(identity, 11, 1, "TokenControlTransferred", V1_REGISTRAR)
            .resource(&lease)
            .raw(json!({"emitting_address": REGISTRAR}));
        if named {
            event = event.name(&first);
        } else {
            after["namehash"] = json!(node(1));
        }
        fixture.event(event.after(after)).await?;
    }
    Ok((lease, second_resource))
}

const RACE_DELTA: [(&str, usize); 2] = [
    ("d12_same_block_order:control/registrant", 1),
    ("d12_same_block_order:registration/registrant", 1),
];

/// Pro r5 Q1 on c23e3e5b: only the candidate's surface namehash is set to null after
/// publication. The families no longer name `c` for name 1, while `a` and `b` stay named, so the
/// shadow and the canonical read of the log-rebuilt events both give Bob and today's order still
/// gives the served Alice. Step 2 writes the namehash from the name (identity.rs:398-402), so the
/// candidate is not what the log gives and both registrants stay mismatches.
#[tokio::test]
async fn a_wrong_candidate_surface_namehash_stays_a_mismatch() -> Result<()> {
    let fixture = Fixture::new("families_shadow_admission_candidate_namehash", 20).await?;
    staged_transfer_race(&fixture).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    assert_counts(&report, &[], &RACE_DELTA);
    let updated = sqlx::query(
        "UPDATE bigname_phase.project_binding_candidate SET surface_namehash = NULL
         WHERE surface_binding_id = $1::uuid",
    )
    .bind(uuid(100))
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    assert_eq!(updated, 1);
    let (_, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(shadow.control["registrant"], json!(BOB));
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert!(
        mutated.known_discrepancy.is_empty() && mutated.expected_delta_fields.is_empty(),
        "{:#?}",
        mutated.lines
    );
    assert_eq!(
        failed_fields(&mutated),
        ["control/registrant", "registration/registrant"],
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// Pro r5 Q3 on c23e3e5b, another name's staging candidate: the staging passes choose among the
/// candidates of every name on the unnamed row's lease (admission.rs `attachment`). A family
/// candidate of name 2 on lease L at node 1, which no binding of the log gives, makes pass one
/// ambiguous, so `c` is named for no one: the shadow and the canonical read of the rebuilt events
/// give Bob, today's order Alice. Name 2's own values do not change. The staging candidates of
/// L are not what the log gives, so both registrants of name 1 stay mismatches.
#[tokio::test]
async fn a_wrong_candidate_of_another_name_on_the_lease_stays_a_mismatch() -> Result<()> {
    let fixture = Fixture::new("families_shadow_admission_foreign_candidate", 20).await?;
    let (lease, _) = staged_transfer_race(&fixture).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    assert_counts(&report, &[], &RACE_DELTA);
    let inserted = sqlx::query(
        "INSERT INTO bigname_phase.project_binding_candidate
         SELECT (jsonb_populate_record(NULL::bigname_phase.project_binding_candidate,
                    to_jsonb(candidate) || jsonb_build_object(
                        'surface_binding_id', $2::text, 'resource_id', $3::text,
                        'surface_namehash', $4::text))).*
         FROM bigname_phase.project_binding_candidate candidate
         WHERE candidate.surface_binding_id = $1::uuid",
    )
    .bind(uuid(101))
    .bind(uuid(102))
    .bind(&lease)
    .bind(node(1))
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    assert_eq!(inserted, 1);
    let (_, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(shadow.control["registrant"], json!(BOB));
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?;
    assert!(
        mutated.known_discrepancy.is_empty() && mutated.expected_delta_fields.is_empty(),
        "{:#?}",
        mutated.lines
    );
    assert_eq!(
        failed_fields(&mutated),
        ["control/registrant", "registration/registrant"],
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// The adversarial pass on ba2ffbd5, the load path for another name on the lease that is not
/// in the chunk (retention.rs `RetentionLog::load`, the names on an unnamed registrar event's
/// lease): name 2 is bound directly to L at its own node and reads equal, so only name 1 has
/// facts, in one chunk and in chunks of one. Its candidate is loaded from the log, and the
/// race's same-block deltas stay excused. Then only name 2's candidate loses its surface
/// namehash. Pass one of L still names `c` for name 1 alone and name 2 still reads equal, so the
/// outcome is unchanged, but the candidates staging read are not what the log gives and both
/// registrants of name 1 are mismatches.
#[tokio::test]
async fn another_names_binding_on_the_lease_outside_the_chunk_is_checked() -> Result<()> {
    let fixture = Fixture::new("families_shadow_admission_foreign_binding", 20).await?;
    staged_race(&fixture, Second::Beside).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    assert_counts(&report, &[], &RACE_DELTA);
    assert_eq!(
        (report.names, report.expected_delta, report.mismatched),
        (2, 1, 0),
        "{:#?}",
        report.lines
    );
    let chunked = shadow_support::compare::compare_in_chunks(&fixture.pool, CHAIN, 12, 1).await?;
    assert_counts(&chunked, &[], &RACE_DELTA);
    let updated = sqlx::query(
        "UPDATE bigname_phase.project_binding_candidate SET surface_namehash = NULL
         WHERE surface_binding_id = $1::uuid",
    )
    .bind(uuid(101))
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    assert_eq!(updated, 1);
    let (_, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(
        shadow.control["registrant"],
        json!(CAROL),
        "pass one still names `c`"
    );
    for mutated in [
        shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?,
        shadow_support::compare::compare_in_chunks(&fixture.pool, CHAIN, 12, 1).await?,
    ] {
        assert!(
            mutated.known_discrepancy.is_empty() && mutated.expected_delta_fields.is_empty(),
            "{:#?}",
            mutated.lines
        );
        assert_eq!(mutated.mismatched, 1, "{:#?}", mutated.lines);
        assert_eq!(
            failed_fields(&mutated),
            ["control/registrant", "registration/registrant"],
            "{:#?}",
            mutated.lines
        );
    }
    fixture.cleanup().await
}

/// The adversarial pass on ba2ffbd5, another name's wrapper-recorded candidate: name 2 is bound
/// to NameWrapper resource W, whose SurfaceBound recorded L at node 1, so pass two of L would
/// match it across names; pass one names `c` for name 1 first. Name 2 reads equal and has no
/// facts, so its candidate is loaded through its SurfaceBound and the race's deltas stay
/// excused, in one chunk and in chunks of one. Then only name 2's wrapper candidate loses its
/// node. Pass one still wins and name 2 still reads equal, but the candidates staging read are
/// not what the log gives and both registrants of name 1 are mismatches.
#[tokio::test]
async fn another_names_wrapper_candidate_on_the_lease_goes_through_the_staging_check() -> Result<()>
{
    let fixture = Fixture::new("families_shadow_admission_foreign_wrapper", 20).await?;
    staged_race(&fixture, Second::Wrapped).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    assert_counts(&report, &[], &RACE_DELTA);
    assert_eq!(
        (report.names, report.expected_delta, report.mismatched),
        (2, 1, 0),
        "{:#?}",
        report.lines
    );
    let chunked = shadow_support::compare::compare_in_chunks(&fixture.pool, CHAIN, 12, 1).await?;
    assert_counts(&chunked, &[], &RACE_DELTA);
    let updated = sqlx::query(
        "UPDATE bigname_phase.project_binding_candidate SET node = NULL
         WHERE surface_binding_id = $1::uuid AND node IS NOT NULL",
    )
    .bind(uuid(101))
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    assert_eq!(updated, 1);
    let (_, shadow) = shadow_support::name(&fixture, 12, &name(1)).await?;
    assert_eq!(
        shadow.control["registrant"],
        json!(CAROL),
        "pass one still names `c`"
    );
    for mutated in [
        shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?,
        shadow_support::compare::compare_in_chunks(&fixture.pool, CHAIN, 12, 1).await?,
    ] {
        assert!(
            mutated.known_discrepancy.is_empty() && mutated.expected_delta_fields.is_empty(),
            "{:#?}",
            mutated.lines
        );
        assert_eq!(mutated.mismatched, 1, "{:#?}", mutated.lines);
        assert_eq!(
            failed_fields(&mutated),
            ["control/registrant", "registration/registrant"],
            "{:#?}",
            mutated.lines
        );
    }
    fixture.cleanup().await
}

/// Pro r5 Q3 on c23e3e5b, the wrapper rows: name 1 is bound to NameWrapper resource W, whose
/// SurfaceBound recorded lease L at node 1, and W carries a NameWrapped modifier. L's unnamed
/// grant at 8 and two synthesised unnamed renewals at 12, `b-renew` (2,200,000,000) written
/// before `a-renew` (2,100,000,000), are named for name 1 by pass two and admitted as the
/// wrapped lease's events, which needs the wrapper row's modifier (admission.rs `wrapped_lease`).
/// The canonical order takes `b-renew`, today's `a-renew`: a same-block delta on both expiries.
/// The harness does not rebuild the wrapper rows from the log, so a name that reads one gets no
/// excuse: the delta is refused, and a wrong modifier (the row's state position cleared) is too.
#[tokio::test]
async fn a_name_that_reads_a_wrapper_row_gets_no_excuse() -> Result<()> {
    let fixture = Fixture::new("families_shadow_admission_wrapper_modifier", 20).await?;
    let (lease, wrapper) = (uuid(1), uuid(2));
    fixture
        .binding(&uuid(100), &name(1), &wrapper, "ens_v1", 9, 2, None)
        .await?;
    let first = name(1);
    fixture
        .event(wrapper_bound("wrap-1", &first, &wrapper, &lease))
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
    for (identity, expiry) in [("b-renew", 2_200_000_000u64), ("a-renew", 2_100_000_000u64)] {
        fixture
            .event(
                Event::new(identity, 12, 0, "RegistrationRenewed", V1_REGISTRAR)
                    .resource(&lease)
                    .after(json!({"authority_kind": "registrar", "namehash": node(1),
                                  "expiry": expiry}))
                    .raw(json!({"emitting_address": REGISTRAR}))
                    .synthesised(),
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, 16).await?;
    let refused = ["control/expiry", "registration/expiry"];
    assert!(
        report.known_discrepancy.is_empty() && report.expected_delta_fields.is_empty(),
        "{:#?}",
        report.lines
    );
    assert_eq!(failed_fields(&report), refused, "{:#?}", report.lines);
    let updated = sqlx::query(
        "UPDATE bigname_phase.project_wrapper_state SET wrapper_state_position = NULL
         WHERE resource_id = $1::uuid",
    )
    .bind(&wrapper)
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    assert_eq!(updated, 1);
    // Without the modifier the lease's events are not admitted for name 1: the shadow holds no
    // registration, and every field it moves is a mismatch.
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
    assert!(
        mutated.known_discrepancy.is_empty() && mutated.expected_delta_fields.is_empty(),
        "{:#?}",
        mutated.lines
    );
    assert_eq!(
        failed_fields(&mutated),
        [
            "control/expiry",
            "control/registrant",
            "control/status",
            "registration/authority_kind",
            "registration/expiry",
            "registration/latest_event_kind",
            "registration/registered_at",
            "registration/registrant",
            "registration/resource_id"
        ],
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// The wrapped three-transfer race (Pro r6 Q3 on ba2ffbd5). Name 1 was bound to lease L at 8
/// (closed at 9) and is bound to NameWrapper resource W at 9, whose SurfaceBound recorded L at
/// node 1; W carries a NameWrapped PermissionScopeChanged modifier at 10 and a named grant at
/// 10. At block 11 three transfers share one block, transaction and log: `c`, unnamed on L at
/// node 1, to Carol, then `b` and `a`, named on W, to Bob and Alice, written in that order so
/// their generated ids run a > b > c. With the modifier, L is the selected binding's
/// predecessor lease and `c` is admitted (admission.rs `wrapped_lease`): the canonical order
/// takes `c` (Carol), today's `a` (Alice). Without it only `b` and `a` are admitted and the
/// canonical order takes `b` (Bob).
async fn wrapped_transfer_race(fixture: &Fixture) -> Result<(String, String)> {
    let (lease, wrapper) = (uuid(1), uuid(2));
    let first = name(1);
    fixture
        .binding(&uuid(99), &first, &lease, "ens_v1", 8, 0, Some(9))
        .await?;
    fixture
        .write(
            8,
            0,
            "SurfaceBound",
            V1_REGISTRAR,
            Some(&first),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false}),
            REGISTRAR,
        )
        .await?;
    fixture
        .binding(&uuid(100), &first, &wrapper, "ens_v1", 9, 2, None)
        .await?;
    fixture
        .event(wrapper_bound("wrap-1", &first, &wrapper, &lease))
        .await?;
    fixture
        .event(
            Event::new("scope-10", 10, 3, "PermissionScopeChanged", V1_WRAPPER)
                .name(&first)
                .resource(&wrapper)
                .after(
                    json!({"source_event": "NameWrapped", "node": node(1), "fuses": 0,
                              "wrapper_state": "wrapped"}),
                )
                .raw(json!({"emitting_address": WRAPPER})),
        )
        .await?;
    fixture.resource(&lease).await?;
    fixture
        .event(
            Event::new("grant-10", 10, 4, "RegistrationGranted", V1_WRAPPER)
                .name(&first)
                .resource(&wrapper)
                .after(json!({"authority_kind": "wrapper", "status": "registered",
                              "registrant": HOLDER, "expiry": 2_000_000_000u64}))
                .raw(json!({"emitting_address": WRAPPER})),
        )
        .await?;
    for (identity, to, named) in [("c", CAROL, false), ("b", BOB, true), ("a", ALICE, true)] {
        let event = if named {
            Event::new(identity, 11, 1, "TokenControlTransferred", V1_WRAPPER)
                .name(&first)
                .resource(&wrapper)
                .after(json!({"authority_kind": "wrapper", "from": HOLDER, "to": to}))
                .raw(json!({"emitting_address": WRAPPER}))
        } else {
            Event::new(identity, 11, 1, "TokenControlTransferred", V1_REGISTRAR)
                .resource(&lease)
                .after(
                    json!({"authority_kind": "registrar", "from": HOLDER, "to": to,
                              "namehash": node(1)}),
                )
                .raw(json!({"emitting_address": REGISTRAR}))
        };
        fixture.event(event).await?;
    }
    Ok((lease, wrapper))
}

/// Name 1's shadow registrant at block 12.
async fn registrant(fixture: &Fixture) -> Result<Value> {
    let (_, shadow) = shadow_support::name(fixture, 12, &name(1)).await?;
    Ok(shadow.registration["registrant"].clone())
}

/// Pro r6 Q3 on ba2ffbd5, a missing or rekeyed wrapper row. The name reads W's wrapper row, so
/// its registrant difference (Carol against Alice) gets no excuse. Deleting that row, or moving
/// it to another resource, leaves the shadow read and both excuse reads without the modifier:
/// the shadow and the canonical read give Bob and today's order Alice, which passed as a
/// same-block delta while the refusal read only the rows present. The log still gives W's
/// modifier, so the name is refused either way and the registrant stays a mismatch; the row
/// restored, the baseline is back. Since TYR-36 step 6 (de24ff32) serves a wrapper grant's
/// control owner, the refused registrant shows in the control block too; before it both sides
/// served that control as unsupported and only `registration/registrant` differed.
#[tokio::test]
async fn a_missing_or_rekeyed_wrapper_row_gets_no_excuse() -> Result<()> {
    let fixture = Fixture::new("families_shadow_admission_wrapper_absent", 20).await?;
    let (_, wrapper) = wrapped_transfer_race(&fixture).await?;
    let elsewhere = uuid(9);
    fixture.resource(&elsewhere).await?;
    let refused = |report: &shadow_support::compare::Report| {
        assert!(
            report.known_discrepancy.is_empty() && report.expected_delta_fields.is_empty(),
            "{:#?}",
            report.lines
        );
        assert_eq!(
            (report.mismatched, failed_fields(report)),
            (
                1,
                vec![
                    "control/registrant".to_owned(),
                    "registration/registrant".to_owned(),
                ]
            ),
            "{:#?}",
            report.lines
        );
    };
    let baseline = publish_and_compare(&fixture, 12).await?;
    refused(&baseline);
    assert_eq!(registrant(&fixture).await?, json!(CAROL));
    let saved: Value = sqlx::query_scalar(
        "SELECT to_jsonb(wrapper) FROM bigname_phase.project_wrapper_state wrapper
         WHERE resource_id = $1::uuid",
    )
    .bind(&wrapper)
    .fetch_one(&fixture.pool)
    .await?;
    for (case, mutation) in [
        (
            "deleted",
            "DELETE FROM bigname_phase.project_wrapper_state WHERE resource_id = $1::uuid",
        ),
        (
            "rekeyed",
            "UPDATE bigname_phase.project_wrapper_state SET resource_id = $2::uuid
             WHERE resource_id = $1::uuid",
        ),
    ] {
        let changed = sqlx::query(mutation)
            .bind(&wrapper)
            .bind(&elsewhere)
            .execute(&fixture.pool)
            .await?
            .rows_affected();
        assert_eq!(changed, 1, "{case}");
        assert_eq!(registrant(&fixture).await?, json!(BOB), "{case}");
        refused(&shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?);
        let removed = sqlx::query(
            "DELETE FROM bigname_phase.project_wrapper_state
             WHERE resource_id IN ($1::uuid, $2::uuid)",
        )
        .bind(&wrapper)
        .bind(&elsewhere)
        .execute(&fixture.pool)
        .await?
        .rows_affected();
        assert_eq!(removed, u64::from(case == "rekeyed"), "{case}");
        let restored = sqlx::query(
            "INSERT INTO bigname_phase.project_wrapper_state
             SELECT * FROM jsonb_populate_record(NULL::bigname_phase.project_wrapper_state, $1)",
        )
        .bind(&saved)
        .execute(&fixture.pool)
        .await?
        .rows_affected();
        assert_eq!(restored, 1, "{case}");
        assert_eq!(registrant(&fixture).await?, json!(CAROL), "{case}");
        refused(&shadow_support::compare::compare(&fixture.pool, CHAIN, 12).await?);
    }
    fixture.cleanup().await
}

/// Codex thread PRRT_kwDOSJpxAs6mMMhv, the wrapped registrar lease in one block. Name 1 is
/// bound to NameWrapper resource W twice in block 9: at log 2 (closed at log 3) by a
/// SurfaceBound that recorded lease L1, and at log 3 by one that recorded L2. The log 3 event
/// is written first, so today's builder, which takes the latest SurfaceBound by block and
/// generated id (build.sql:348-358), serves L1 as the registration's resource. The canonical
/// order takes the SurfaceBound at log 3, L2, and the families read in today's order give L1
/// again, so the difference is a same-block delta. W carries no wrapper modifier, so the name
/// is not refused.
#[tokio::test]
async fn a_wrapped_lease_chosen_in_one_block_takes_the_event_order() -> Result<()> {
    let fixture = Fixture::new("families_shadow_admission_wrapped_lease_order", 20).await?;
    let (first_lease, second_lease, wrapper) = (uuid(1), uuid(3), uuid(2));
    let first = name(1);
    fixture.resource(&first_lease).await?;
    fixture.resource(&second_lease).await?;
    fixture.surface(&first, &node(1)).await?;
    fixture.resource(&wrapper).await?;
    // In-block bindings take log-microsecond times: the log 2 binding closes as the log 3 one
    // opens.
    for (binding, log, from, to) in [(uuid(100), 2, 2, Some(3)), (uuid(101), 3, 3, None::<i64>)] {
        sqlx::query(
            "INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
                 binding_kind, authority_arm, active_from, active_to, chain_id, block_hash,
                 block_number, provenance, canonicality_state)
             VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v1',
                     to_timestamp(1800000000 + 9 * 12) + $4 * interval '1 microsecond',
                     to_timestamp(1800000000 + 9 * 12) + $5 * interval '1 microsecond',
                     $6, $7, 9, jsonb_build_object('transaction_index', 0, 'log_index', $8),
                     'canonical')",
        )
        .bind(&binding)
        .bind(&first)
        .bind(&wrapper)
        .bind(from)
        .bind(to)
        .bind(CHAIN)
        .bind(support::hash(9))
        .bind(log)
        .execute(&fixture.pool)
        .await?;
    }
    for (identity, log, lease, binding) in [
        ("wrap-b", 3, &second_lease, uuid(101)),
        ("wrap-a", 2, &first_lease, uuid(100)),
    ] {
        fixture
            .event(
                Event::new(identity, 9, log, "SurfaceBound", V1_WRAPPER)
                    .name(&first)
                    .resource(&wrapper)
                    .after(json!({"authority_kind": "wrapper", "node": node(1),
                                  "wrapped_registrar_resource_id": lease,
                                  "surface_binding_id": binding, "state_derived": false}))
                    .raw(json!({"emitting_address": WRAPPER})),
            )
            .await?;
    }
    fixture
        .event(
            Event::new("grant-10", 10, 4, "RegistrationGranted", V1_WRAPPER)
                .name(&first)
                .resource(&wrapper)
                .after(json!({"authority_kind": "wrapper", "status": "registered",
                              "registrant": HOLDER, "expiry": 2_000_000_000u64}))
                .raw(json!({"emitting_address": WRAPPER})),
        )
        .await?;
    let report = publish_and_compare(&fixture, 12).await?;
    let (served, shadow) = shadow_support::name(&fixture, 12, &first).await?;
    assert_eq!(served.registration("resource_id"), json!(first_lease));
    assert_eq!(shadow.registration["resource_id"], json!(second_lease));
    assert_counts(
        &report,
        &[],
        &[("d12_same_block_order:registration/resource_id", 1)],
    );
    fixture.cleanup().await
}
