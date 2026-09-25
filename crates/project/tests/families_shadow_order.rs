//! Same-block order shadow reads (TYR-36 step 3, brief section 4.3, docs/glossary.md "Shadow
//! read"): the families order events in one block by the canonical order (D12) while today's
//! builders break the tie by the generated normalized event id. A difference passes as a
//! same-block delta only when reading the same families again in today's order, at the selectors
//! today's builders use it, gives exactly the served value and the canonical read gives exactly
//! the shadow value. Each case pins the served and shadow values it was written for, and the
//! mutation cases show that a wrong value under the same field label fails.
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use bigname_storage::{
    families::control::{
        compare::Difference,
        lifecycle::{AuthoritySelection, Clock, NameFacts, NameInput, evaluate, load_name_facts},
        permissions::ResourceInput,
    },
    load_name_current_by_logical_name_ids,
};
use serde_json::{Value, json};
use shadow_support::{
    assert_counts,
    compare::{Excuse, association_keys, generated_ids, legacy_facts, resource_excuses},
    publish_and_compare,
    wrapper::timestamp,
};
use support::{CHAIN, Event, Fixture, uuid};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const ALICE: &str = "0x00000000000000000000000000000000000000aa";
const BOB: &str = "0x00000000000000000000000000000000000000bb";
const V2_REGISTRY: &str = "ens_v2_registry_l1";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// Name 1's ENSv2 binding to `resource` at block 9 with the SurfaceBound that opened it.
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

/// An ENSv2 registry grant of triple (`name`, registry R, token 7) on `resource`.
fn grant<'a>(
    identity: &'a str,
    block: i64,
    name: &'a str,
    resource: &'a str,
    registrant: &str,
) -> Event<'a> {
    Event::new(identity, block, 1, "RegistrationGranted", V2_REGISTRY)
        .name(name)
        .resource(resource)
        .after(
            json!({"registry_contract_instance_id": "R", "token_id": "7",
                      "authority_kind": "registrar", "status": "registered",
                      "registrant": registrant, "expiry": 2_000_000_000u64}),
        )
        .raw(json!({"emitting_address": REGISTRY}))
}

/// The interpreter's path-expiry release of `resource`: the resource and no name, no
/// transaction or log (crates/adapters/src/schema_v2/protocol/v2_registry/expiry.rs:57-64).
fn unnamed_path_expiry<'a>(identity: &'a str, block: i64, resource: &'a str) -> Event<'a> {
    Event::new(identity, block, 0, "RegistrationReleased", V2_REGISTRY)
        .resource(resource)
        .after(json!({"source_event": "RegistryPathExpired",
                      "derived_from": "interpreter_state",
                      "terminal_reason": "registry_name_binding_expired",
                      "expiry": 1_800_000_100u64,
                      "registry_contract_instance_id": "R", "token_id": "7"}))
        .raw(json!({"emitting_address": REGISTRY}))
        .synthesised()
}

/// A registry-scoped ENSv2 permission grant to `subject` on `resource`.
fn registry_grant<'a>(
    identity: &'a str,
    block: i64,
    resource: &'a str,
    subject: &str,
) -> Event<'a> {
    Event::new(identity, block, 1, "PermissionChanged", V2_REGISTRY)
        .resource(resource)
        .after(json!({
            "subject": subject,
            "scope": {"kind": "registry", "chain_id": CHAIN, "registry_address": REGISTRY},
            "effective_powers": ["set_resolver", "set_subregistry"],
            "grant_source": {"kind": "raw_log", "source_event": "EACRolesChanged"},
            "revocation_source": null, "inheritance_path": [], "transfer_behavior": {},
            "source_event": "EACRolesChanged",
        }))
        .raw(json!({"emitting_address": REGISTRY}))
}

/// The served summary input the harness reads for `resource`.
async fn resource_input(fixture: &Fixture, resource: &str) -> Result<ResourceInput> {
    let (authority_kind, root_resource_id): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT authority_kind, root_resource_id::text FROM permissions_current_resource_summary
         WHERE resource_id = $1::uuid",
    )
    .bind(resource)
    .fetch_optional(&fixture.pool)
    .await?
    .unwrap_or_default();
    Ok(ResourceInput {
        resource_id: resource.to_owned(),
        authority_kind,
        root_resource_id,
    })
}

async fn served_rows(fixture: &Fixture, resource: &str) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object('subject', subject, 'effective_powers', effective_powers)
         FROM permissions_current WHERE resource_id = $1::uuid ORDER BY subject, scope",
    )
    .bind(resource)
    .fetch_all(&fixture.pool)
    .await?)
}

/// The one differing field of `diffs` with the excuse the harness gives it.
async fn excuse(
    fixture: &Fixture,
    target: i64,
    input: &ResourceInput,
    field: &str,
    served: Value,
    shadow: Value,
) -> Result<Excuse> {
    let clock = Clock {
        block_number: target,
        timestamp_seconds: timestamp(target),
    };
    let diffs = [Difference {
        field: field.to_owned(),
        served,
        shadow,
    }];
    Ok(resource_excuses(&fixture.pool, CHAIN, &clock, input, &diffs).await?[0])
}

/// The registry-scoped row BOB holds on `resource` as the summary serves it.
fn bob_row(resource: &str) -> Value {
    json!({
        "resource_id": resource, "subject": BOB, "scope": "registry",
        "scope_kind": "registry",
        "scope_detail": {"kind": "registry", "chain_id": CHAIN, "registry_address": REGISTRY},
        "effective_powers": ["set_resolver", "set_subregistry"],
        "grant_source": {"kind": "raw_log", "source_event": "EACRolesChanged"},
        "revocation_source": null, "inheritance_path": [], "transfer_behavior": {},
    })
}

/// Items 1 and F1 of the TYR-36 step 3 reviews (Q7): a grant and the interpreter's unnamed
/// path-expiry release of one resource in one block, the grant written first. D12 puts the
/// release, which has no transaction or log, before the grant, so the registration is live and
/// the shadow serves the permission row; today's (block, generated id) order puts the release
/// last, so the drop rule (permissions.rs:111-133, :391-398) serves nothing. The families read
/// in today's order are empty too, so matching them would only show that the served value is
/// empty: the harness leaves this direction a mismatch, whatever the shadow row holds.
#[tokio::test]
async fn a_same_block_path_expiry_today_serves_as_empty_is_a_mismatch() -> Result<()> {
    let fixture = Fixture::new("families_shadow_order_permissions", 20).await?;
    let (k1, n1) = (uuid(1), name(1));
    v2_binding(&fixture, &k1).await?;
    fixture
        .event(grant("grant-10", 10, &n1, &k1, ALICE))
        .await?;
    fixture
        .event(registry_grant("permission-11", 11, &k1, BOB))
        .await?;
    fixture
        .event(grant("grant-14", 14, &n1, &k1, ALICE))
        .await?;
    fixture
        .event(unnamed_path_expiry("path-expiry-14", 14, &k1))
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    assert!(report.known_discrepancy.is_empty(), "{:#?}", report.lines);
    assert!(
        report.expected_delta_fields.is_empty(),
        "{:#?}",
        report.lines
    );
    assert_eq!(report.mismatched, 1, "{:#?}", report.lines);
    assert_eq!(served_rows(&fixture, &k1).await?, Vec::<Value>::new());
    let input = resource_input(&fixture, &k1).await?;
    assert_eq!(
        excuse(
            &fixture,
            16,
            &input,
            "permissions_current",
            json!([]),
            json!([bob_row(&k1)])
        )
        .await?,
        Excuse::None,
    );
    fixture.cleanup().await
}

/// The name side of the passing fixture: the families select the unnamed path-expiry release,
/// which today's name-scoped membership never sees (the served-side bug the harness names).
const RELEASED_NAME: [(&str, usize); 7] = [
    (
        "served_membership_skips_unnamed_path_expiry:control/expiry",
        1,
    ),
    (
        "served_membership_skips_unnamed_path_expiry:control/registrant",
        1,
    ),
    (
        "served_membership_skips_unnamed_path_expiry:control/status",
        1,
    ),
    (
        "served_membership_skips_unnamed_path_expiry:registration/authority_kind",
        1,
    ),
    (
        "served_membership_skips_unnamed_path_expiry:registration/latest_event_kind",
        1,
    ),
    (
        "served_membership_skips_unnamed_path_expiry:registration/registrant",
        1,
    ),
    (
        "served_membership_skips_unnamed_path_expiry:registration/status",
        1,
    ),
];
const DELTA: [(&str, usize); 1] = [("d12_same_block_order:permissions_current", 1)];

/// Item F1 of the TYR-36 step 3 review (Q7), the direction that passes. The unnamed path-expiry
/// release of the resource is written first at transaction 0 log 2 and a grant second at log 1
/// of the same block. D12 puts the grant first and the release last, so the registration lapses
/// and the shadow serves no row; today's order puts the grant last, so the registration is live
/// and the summary serves BOB's row. The difference passes as a same-block delta because the
/// families read again in today's order give exactly that row. Each mutation writes a wrong
/// value into the families themselves and runs the whole comparison again: the read in today's
/// order then differs from the served row and the field fails.
#[tokio::test]
async fn a_same_block_release_after_a_grant_passes_only_from_the_families_read() -> Result<()> {
    let fixture = Fixture::new("families_shadow_order_permissions_live", 20).await?;
    let (k1, n1) = (uuid(1), name(1));
    v2_binding(&fixture, &k1).await?;
    fixture
        .event(grant("grant-10", 10, &n1, &k1, ALICE))
        .await?;
    fixture
        .event(registry_grant("permission-11", 11, &k1, BOB))
        .await?;
    fixture
        .event(unnamed_path_expiry("path-expiry-14", 14, &k1).at(0, 2))
        .await?;
    fixture
        .event(grant("grant-14", 14, &n1, &k1, ALICE))
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    assert_counts(&report, &RELEASED_NAME, &DELTA);
    let served = served_rows(&fixture, &k1).await?;
    assert_eq!(
        served,
        vec![json!({"subject": BOB, "effective_powers": ["set_resolver", "set_subregistry"]})]
    );

    for (case, update) in [
        (
            "wrong subject",
            "UPDATE bigname_phase.project_grant SET subject = $2
             WHERE resource_id = $1::uuid AND subject = $3",
        ),
        (
            "wrong powers",
            "UPDATE bigname_phase.project_grant SET effective_powers = '[\"set_resolver\"]'
             WHERE resource_id = $1::uuid AND subject = $3 AND $2 <> ''",
        ),
    ] {
        sqlx::query(update)
            .bind(&k1)
            .bind(ALICE)
            .bind(BOB)
            .execute(&fixture.pool)
            .await?;
        let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
        assert!(
            mutated.expected_delta_fields.is_empty() && mutated.mismatched == 1,
            "{case} must fail: {:#?}",
            mutated.lines
        );
        assert_eq!(
            mutated.known_discrepancy,
            RELEASED_NAME
                .iter()
                .map(|(field, count)| ((*field).to_owned(), *count))
                .collect(),
            "{case}: the released name's named causes stand whole"
        );
        let failed: Vec<&str> = mutated
            .lines
            .iter()
            .filter(|line| line.starts_with("SEPOLIA_END_TO_END_SHADOW_MISMATCH"))
            .filter_map(|line| line.split(" field=").nth(1)?.split(' ').next())
            .collect();
        assert_eq!(
            failed,
            vec!["permissions_current"],
            "{case}: {:#?}",
            mutated.lines
        );
        // Put the families back for the next mutation.
        sqlx::query(
            "UPDATE bigname_phase.project_grant
             SET subject = $2, effective_powers = '[\"set_resolver\", \"set_subregistry\"]'
             WHERE resource_id = $1::uuid AND subject IN ($2, $3) AND scope = 'registry'",
        )
        .bind(&k1)
        .bind(BOB)
        .bind(ALICE)
        .execute(&fixture.pool)
        .await?;
        let restored = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
        assert_counts(&restored, &RELEASED_NAME, &DELTA);
    }

    let input = resource_input(&fixture, &k1).await?;
    let restriction = json!({"kind": "ens_v2_registry", "locked_roles": ["unregister"]});
    assert_eq!(
        excuse(
            &fixture,
            16,
            &input,
            "resource_restrictions",
            restriction,
            Value::Null
        )
        .await?,
        Excuse::None,
        "a restriction the families do not give must fail"
    );
    fixture.cleanup().await
}

/// Codex thread PRRT_kwDOSJpxAs6l4hOv: the passing direction of the same-block release with
/// BOB's registry grant carrying an admin power. Today's order keeps the registration live and
/// serves the admin power from BOB's row (resource_summary.rs:272-297); the canonical order
/// lapses it and the families serve no admin powers (permissions/mod.rs, `admins`). The admin
/// difference has the same cause as the permission rows and passes the same way: the families
/// read again in today's order give exactly the served admin powers.
#[tokio::test]
async fn a_same_block_release_moves_the_admin_powers_with_the_rows() -> Result<()> {
    let fixture = Fixture::new("families_shadow_order_permissions_admin", 20).await?;
    let (k1, n1) = (uuid(1), name(1));
    v2_binding(&fixture, &k1).await?;
    fixture
        .event(grant("grant-10", 10, &n1, &k1, ALICE))
        .await?;
    fixture
        .event(
            Event::new("permission-11", 11, 1, "PermissionChanged", V2_REGISTRY)
                .resource(&k1)
                .after(json!({
                    "subject": BOB,
                    "scope": {"kind": "registry", "chain_id": CHAIN, "registry_address": REGISTRY},
                    "effective_powers": ["admin_set_resolver", "set_resolver"],
                    "grant_source": {"kind": "raw_log", "source_event": "EACRolesChanged"},
                    "revocation_source": null, "inheritance_path": [], "transfer_behavior": {},
                    "source_event": "EACRolesChanged",
                }))
                .raw(json!({"emitting_address": REGISTRY})),
        )
        .await?;
    fixture
        .event(unnamed_path_expiry("path-expiry-14", 14, &k1).at(0, 2))
        .await?;
    fixture
        .event(grant("grant-14", 14, &n1, &k1, ALICE))
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    let served: Vec<String> = sqlx::query_scalar(
        r"SELECT DISTINCT power.value FROM permissions_current served
          CROSS JOIN LATERAL jsonb_array_elements_text(served.effective_powers) power
          WHERE served.resource_id = $1::uuid AND power.value LIKE 'admin\_%'",
    )
    .bind(&k1)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(served, vec!["admin_set_resolver".to_owned()]);
    assert_counts(
        &report,
        &RELEASED_NAME,
        &[
            ("d12_same_block_order:admin_powers", 1),
            ("d12_same_block_order:permissions_current", 1),
        ],
    );
    fixture.cleanup().await
}

/// Name 1's facts as the harness loads them, with the counterfactual that reads them in today's
/// order.
async fn facts_and_legacy(fixture: &Fixture) -> Result<(NameFacts, NameFacts)> {
    let rows = load_name_current_by_logical_name_ids(&fixture.pool, &[name(1)]).await?;
    let row = &rows[&name(1)];
    let input = NameInput {
        logical_name_id: row.logical_name_id.clone(),
        namehash: row.namehash.to_ascii_lowercase(),
        selection: AuthoritySelection::from_provenance(&row.provenance),
    };
    let facts = load_name_facts(&fixture.pool, CHAIN, &[input])
        .await?
        .pop()
        .expect("name 1 has facts");
    let identities: Vec<String> = facts
        .events
        .iter()
        .map(|event| event.position.event_identity.clone())
        .collect();
    let ids = generated_ids(&fixture.pool, CHAIN, &identities).await?;
    let keys = association_keys(&fixture.pool, CHAIN, &identities).await?;
    let legacy =
        legacy_facts(&facts, &ids, &keys).expect("a block reads differently in today's order");
    Ok((facts, legacy))
}

fn admitted(facts: &NameFacts, target: i64) -> Value {
    let clock = Clock {
        block_number: target,
        timestamp_seconds: timestamp(target),
    };
    evaluate(facts, &clock).trace["admitted"].clone()
}

/// An ENSv2 event of name 1 on `resource` at `(transaction 0, log)`.
fn v2_event<'a>(
    identity: &'a str,
    block: i64,
    log: i64,
    kind: &'a str,
    name: &'a str,
    resource: Option<&'a str>,
    after: Value,
) -> Event<'a> {
    let mut after = after;
    after["registry_contract_instance_id"] = after
        .get("registry_contract_instance_id")
        .cloned()
        .unwrap_or(json!("R"));
    after["token_id"] = after.get("token_id").cloned().unwrap_or(json!("7"));
    after["authority_kind"] = json!("registrar");
    let mut event = Event::new(identity, block, log, kind, V2_REGISTRY)
        .name(name)
        .after(after)
        .raw(json!({"emitting_address": REGISTRY}));
    if let Some(resource) = resource {
        event = event.resource(resource);
    }
    event
}

/// Item 4 of the TYR-36 step 3 review (Q7): the same-block counterfactual reads the name's
/// facts in today's order at the selectors that use it and keeps every position, so the
/// authority admission, whose epoch bound compares three-part positions (authority_events.sql
/// :262-311), admits exactly what it admits in the canonical read. Name 1 has a migration proof
/// at block 12, log 5, and in that block an ExpiryChanged at log 7, written first, and a renewal
/// at log 3: the generated ids and the canonical order disagree, and the ExpiryChanged is
/// admitted after the epoch start in both reads. Rewriting positions would have moved it before
/// the bound and changed what the laterals read, an admission change no same-block excuse may
/// rest on.
#[tokio::test]
async fn the_order_counterfactual_keeps_the_admission_of_a_reordered_block() -> Result<()> {
    let fixture = Fixture::new("families_shadow_order_admission", 20).await?;
    let (k1, n1) = (uuid(1), name(1));
    v2_binding(&fixture, &k1).await?;
    fixture
        .event(grant("grant-10", 10, &n1, &k1, ALICE))
        .await?;
    fixture
        .event(
            Event::new(
                "migration-12",
                12,
                5,
                "MigrationApplied",
                "ens_v2_migration_l1",
            )
            .name(&n1)
            .after(json!({"migration_path": "unlocked_wrapped",
                              "successor_binding": {"binding_id": uuid(100), "resource_id": k1}})),
        )
        .await?;
    fixture
        .event(v2_event(
            "expiry-12",
            12,
            7,
            "ExpiryChanged",
            &n1,
            Some(&k1),
            json!({"expiry": 2_200_000_000u64}),
        ))
        .await?;
    fixture
        .event(v2_event(
            "renewal-12",
            12,
            3,
            "RegistrationRenewed",
            &n1,
            Some(&k1),
            json!({"expiry": 2_100_000_000u64}),
        ))
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    assert_counts(&report, &[], &[]);
    let (facts, legacy) = facts_and_legacy(&fixture).await?;
    assert_eq!(
        facts.input.selection.epoch_start,
        Some((12, 0, 5)),
        "the proof opens the epoch mid-block"
    );
    assert_eq!(admitted(&facts, 16), json!(["expiry-12"]));
    assert_eq!(admitted(&legacy, 16), admitted(&facts, 16));

    // Pro Q7b: the reordered block makes the name's same-block read run, but it computes only
    // the registration and control blocks. A wrong non-null handoff block in the families, the
    // served one null, must stay a mismatch rather than match an uncomputed null.
    sqlx::query(
        "INSERT INTO bigname_phase.project_registry_node_state (chain_id, namespace, node,
             block_number, event_identity, first_current_record_block)
         VALUES ($1, 'ens', $2, 5, 'node-5', 5)",
    )
    .bind(CHAIN)
    .bind(node(1))
    .execute(&fixture.pool)
    .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
    assert!(
        mutated.expected_delta_fields.is_empty()
            && mutated.known_discrepancy.is_empty()
            && mutated.mismatched == 1,
        "a wrong handoff block must fail: {:#?}",
        mutated.lines
    );
    assert!(
        mutated.lines.iter().any(|line| {
            line.starts_with("SEPOLIA_END_TO_END_SHADOW_MISMATCH")
                && line.contains("field=authority_selection/registry_handoff_block_number")
        }),
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// Item 4 of the TYR-36 step 3 review (Q7): today's association takes the latest linked grant
/// of the same name, registry and token (v2_lifecycle_events.sql:10-23), so the counterfactual
/// may move a triple's association only to a grant of that complete triple. Name 1 has a grant
/// of triple (R, 7) on K1 at log 5 and, in the same block, a grant of the unrelated triple
/// (R2, 9) on K2 at log 1 written after it, then a null-resource ExpiryChanged of (R, 7). The
/// block's two orders disagree, but the unrelated grant is no rival: the triple stays on K1.
#[tokio::test]
async fn an_unrelated_triple_in_the_block_is_not_an_association_rival() -> Result<()> {
    let fixture = Fixture::new("families_shadow_order_unrelated_triple", 20).await?;
    let (k1, k2, n1) = (uuid(1), uuid(2), name(1));
    v2_binding(&fixture, &k1).await?;
    fixture.resource(&k2).await?;
    fixture
        .event(v2_event(
            "grant-r7",
            12,
            5,
            "RegistrationGranted",
            &n1,
            Some(&k1),
            json!({"status": "registered", "registrant": ALICE, "expiry": 2_000_000_000u64}),
        ))
        .await?;
    fixture
        .event(v2_event(
            "grant-r2-9",
            12,
            1,
            "RegistrationGranted",
            &n1,
            Some(&k2),
            json!({"status": "registered", "registrant": BOB, "expiry": 2_000_000_000u64,
                   "registry_contract_instance_id": "R2", "token_id": "9"}),
        ))
        .await?;
    fixture
        .event(v2_event(
            "expiry-r7",
            14,
            1,
            "ExpiryChanged",
            &n1,
            None,
            json!({"expiry": 2_100_000_000u64}),
        ))
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    assert_counts(&report, &[], &[]);
    let (facts, legacy) = facts_and_legacy(&fixture).await?;
    let target = |facts: &NameFacts| {
        facts
            .triples
            .iter()
            .find(|triple| triple.key[1] == "R" && triple.key[2] == "7")
            .map(|triple| {
                (
                    triple.target.clone(),
                    triple
                        .target_position
                        .as_ref()
                        .map(|position| position.event_identity.clone()),
                )
            })
    };
    let expected = Some((Some(k1.clone()), Some("grant-r7".to_owned())));
    assert_eq!(target(&facts), expected);
    assert_eq!(target(&legacy), expected, "the unrelated grant is no rival");
    fixture.cleanup().await
}
