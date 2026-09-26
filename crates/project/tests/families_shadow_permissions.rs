//! Raw permission shadow reads (TYR-36 step 3, docs/glossary.md "Shadow read"): the F8 grant
//! rows and admin aggregates, masked by the F2b wrapper row and the F2a key state at the
//! publication clock, give each resource's served permission rows, admin powers and locked roles;
//! the F9 approvals give every account approval and, crossed with the registry binding, the
//! registry-operator rows the effective-permission reader adds. Each case publishes with the
//! production batch, follows it with the families, compares every served name, resource and
//! account with its family read, and pins the served rows it was written for.
#[path = "families_shadow_support/mod.rs"]
mod shadow_support;
#[path = "families_support/mod.rs"]
mod support;

use anyhow::Result;
use bigname_storage::{
    families::control::{
        lifecycle::{AuthoritySelection, Clock, NameInput, load_shadow_names},
        permissions::{ResourceInput, load_shadow_approvals, load_shadow_permissions},
        registry::load_observations,
    },
    load_name_current_by_logical_name_ids,
};
use serde_json::{Value, json};
use shadow_support::{
    publish, publish_and_compare,
    wrapper::{
        CANNOT_UNWRAP, DELEGATE, GRACE_PERIOD, HOLDER, HOLDER_POWERS, IS_DOT_ETH, OPERATOR,
        PARENT_CANNOT_CONTROL, V1_WRAPPER, WRAPPER, approval, name, node, permission,
        permission_changed, timestamp, wrapped, wrapper_event,
    },
};
use support::{CHAIN, Event, Fixture, uuid};
use uuid::Uuid;

const NEXT_HOLDER: &str = "0x00000000000000000000000000000000000000a2";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const REGISTRY_INSTANCE: &str = "00000000-0000-0000-0000-0000000000e5";
const OWNER: &str = "0x00000000000000000000000000000000000000aa";
const TARGET: i64 = 16;

/// An emancipated name without CAN_EXTEND_EXPIRY: every holder power but `extend_expiry`.
fn emancipated() -> Value {
    json!(
        HOLDER_POWERS
            .iter()
            .filter(|power| **power != "extend_expiry")
            .collect::<Vec<_>>()
    )
}

fn row(subject: &str, relation: &str) -> (String, String, Value) {
    (subject.to_owned(), relation.to_owned(), emancipated())
}

/// `(subject, relation, powers)` per served resource-scoped row of `resource`.
async fn rows(fixture: &Fixture, resource: &str) -> Result<Vec<(String, String, Value)>> {
    Ok(sqlx::query_as(
        "SELECT subject, grant_source ->> 'relation_kind', effective_powers
         FROM permissions_current WHERE resource_id = $1::uuid ORDER BY subject, 2",
    )
    .bind(resource)
    .fetch_all(&fixture.pool)
    .await?)
}

#[tokio::test]
async fn a_delegate_who_is_also_an_operator_keeps_the_operator_set() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_delegate", 20).await?;
    let resource = wrapped(
        &fixture,
        PARENT_CANNOT_CONTROL,
        timestamp(TARGET) + 1_000_000,
    )
    .await?;
    permission_changed(
        &fixture,
        12,
        1,
        &resource,
        DELEGATE,
        "token_approval",
        &["extend_subname_expiry"],
        "Approval",
        true,
    )
    .await?;
    approval(&fixture, 12, 2, HOLDER, DELEGATE, true).await?;
    let report = publish_and_compare(&fixture, TARGET).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    assert_eq!(report.accounts, 2);
    // The delegate's token approval and the holder's approval of it collide on one subject: the
    // operator set is served and the narrower delegate row is not.
    assert_eq!(
        rows(&fixture, &resource).await?,
        vec![
            row(HOLDER, "holder"),
            row(DELEGATE, "operator"),
            row(OPERATOR, "operator")
        ]
    );
    // The collision row whole: the operator's powers, source and transfer behaviour, and the
    // delegate row's other columns, revocation source included.
    let (source, revocation, transfer): (Value, Option<Value>, Value) = sqlx::query_as(
        "SELECT grant_source, revocation_source, transfer_behavior FROM permissions_current
         WHERE resource_id = $1::uuid AND subject = $2",
    )
    .bind(&resource)
    .bind(DELEGATE)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(source["relation_kind"], json!("operator"));
    assert_eq!(source["source_event_kind"], json!("ApprovalForAll"));
    assert_eq!(source["owner"], json!(HOLDER));
    assert_eq!(source["authority_contract"], json!(WRAPPER));
    assert_eq!(revocation, None);
    assert_eq!(
        transfer,
        json!({"mode": "owner_scoped", "on_holder_change": "ceases_to_apply"})
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn a_holder_transfer_moves_the_operator_rows_to_the_new_owner() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_transfer", 20).await?;
    let resource = wrapped(
        &fixture,
        PARENT_CANNOT_CONTROL,
        timestamp(TARGET) + 1_000_000,
    )
    .await?;
    approval(&fixture, 11, 1, NEXT_HOLDER, DELEGATE, true).await?;
    permission_changed(
        &fixture,
        13,
        1,
        &resource,
        HOLDER,
        "holder",
        HOLDER_POWERS,
        "TransferSingle",
        false,
    )
    .await?;
    permission_changed(
        &fixture,
        13,
        2,
        &resource,
        NEXT_HOLDER,
        "holder",
        HOLDER_POWERS,
        "TransferSingle",
        true,
    )
    .await?;
    let report = publish_and_compare(&fixture, TARGET).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    // The old holder's operator falls away with the token and the new holder's arrives.
    assert_eq!(
        rows(&fixture, &resource).await?,
        vec![row(NEXT_HOLDER, "holder"), row(DELEGATE, "operator")]
    );
    fixture.cleanup().await
}

#[tokio::test]
async fn an_expired_emancipated_wrapper_drops_its_holder_row() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_expired", 20).await?;
    let resource = wrapped(&fixture, PARENT_CANNOT_CONTROL, timestamp(TARGET) - 1).await?;
    let report = publish_and_compare(&fixture, TARGET).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    assert_eq!(rows(&fixture, &resource).await?, vec![]);
    fixture.cleanup().await
}

/// A registry ApprovalForAll by the registry owner of a lease: the effective-permission reader
/// adds a registry-operator row for the lease, and the families cross the F9 approval with the
/// F2c registry binding to the same row.
#[tokio::test]
async fn a_registry_operator_reaches_the_lease_its_owner_holds() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_registry_operator", 20).await?;
    let lease = uuid(1);
    fixture
        .binding(&uuid(100), &name(1), &lease, "ens_v1", 9, 0, None)
        .await?;
    fixture
        .write(
            9,
            0,
            "SurfaceBound",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false,
                   "registry_contract": REGISTRY, "owner_getter": OWNER}),
            REGISTRAR,
        )
        .await?;
    let source = json!({"kind": "raw_log", "source_event": "ApprovalForAll"});
    fixture
        .event(
            support::Event::new(
                "registry-approval",
                11,
                0,
                "AccountPermissionChanged",
                "ens_v1_registry_l1",
            )
            .after(json!({
                "subject": OPERATOR, "relation_kind": "operator", "approved": true,
                "scope": {"kind": "account", "chain_id": support::CHAIN,
                          "authority_kind": "registry", "authority_contract": REGISTRY,
                          "authority_contract_instance_id": REGISTRY_INSTANCE,
                          "owner": OWNER},
                "effective_powers": ["registry_control"], "grant_source": source,
                "revocation_source": null, "inheritance_path": [],
                "transfer_behavior": {"mode": "owner_scoped",
                                      "on_holder_change": "ceases_to_apply"},
                "source_event": "ApprovalForAll",
            }))
            .raw(json!({"emitting_address": REGISTRY})),
        )
        .await?;
    let report = publish_and_compare(&fixture, TARGET).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    assert_eq!(report.accounts, 1);
    let operators: Vec<String> = bigname_storage::load_effective_permissions_by_resource_ids(
        &fixture.pool,
        &[Uuid::parse_str(&lease)?],
        None,
    )
    .await?
    .into_iter()
    .filter(|row| {
        matches!(
            row.scope,
            bigname_storage::EffectivePermissionScope::Account { .. }
        )
    })
    .map(|row| row.subject)
    .collect();
    assert_eq!(operators, vec![OPERATOR.to_owned()]);
    fixture.cleanup().await
}

/// The family loaders read family tables and identity rows only (design:597, brief tripwire 7).
/// `project_events` is a temporary table that exists only inside the Project transaction, so no
/// loader can reach it; `normalized_events` is hidden under another name while every loader runs,
/// so a loader that read it would fail here.
#[tokio::test]
async fn the_family_loaders_read_no_event_log() -> Result<()> {
    let fixture = Fixture::new("families_shadow_loaders_no_event_log", 20).await?;
    let resource = wrapped(
        &fixture,
        PARENT_CANNOT_CONTROL,
        timestamp(TARGET) + 1_000_000,
    )
    .await?;
    publish(&fixture, TARGET).await?;
    let rows = load_name_current_by_logical_name_ids(&fixture.pool, &[name(1)]).await?;
    let row = &rows[&name(1)];
    let input = NameInput {
        logical_name_id: row.logical_name_id.clone(),
        namehash: row.namehash.to_ascii_lowercase(),
        selection: AuthoritySelection::from_provenance(&row.provenance),
    };
    let clock = Clock {
        block_number: TARGET,
        timestamp_seconds: timestamp(TARGET),
    };
    let resources = [ResourceInput {
        resource_id: resource.clone(),
        authority_kind: Some("wrapper".into()),
        root_resource_id: None,
    }];
    sqlx::query("ALTER TABLE normalized_events RENAME TO normalized_events_hidden")
        .execute(&fixture.pool)
        .await?;
    let names = load_shadow_names(&fixture.pool, CHAIN, &clock, &[input]).await;
    let permissions = load_shadow_permissions(&fixture.pool, CHAIN, &clock, &resources).await;
    let observations = load_observations(&fixture.pool, CHAIN).await;
    let approvals = load_shadow_approvals(&fixture.pool, CHAIN).await;
    sqlx::query("ALTER TABLE normalized_events_hidden RENAME TO normalized_events")
        .execute(&fixture.pool)
        .await?;
    assert!(names?.contains_key(&name(1)));
    assert!(!permissions?[&resource].grants.is_empty());
    observations?;
    assert_eq!(approvals?.len(), 1);
    fixture.cleanup().await
}

const CANNOT_TRANSFER: i64 = 4;
const CANNOT_SET_RESOLVER: i64 = 8;
const CANNOT_APPROVE: i64 = 64;

/// `(subject, relation, powers)` per served resource-scoped row with `powers`.
fn with(subject: &str, relation: &str, powers: &[&str]) -> (String, String, Value) {
    (subject.to_owned(), relation.to_owned(), json!(powers))
}

/// Item 6 of the TYR-36 step 3 review (Q6), combined masks. A locked name with
/// CANNOT_SET_RESOLVER and CANNOT_TRANSFER loses `resource_control` to the lock, `unwrap`,
/// `set_resolver` and `transfer` to their fuses and `extend_expiry` to the missing
/// CAN_EXTEND_EXPIRY, and its operator gets exactly the masked holder powers
/// (permissions.rs:348-377, wrapper_operators.rs:21-137). In the `.eth` grace period only
/// approval survives, and CANNOT_APPROVE takes that too: no holder row and no operator row.
#[tokio::test]
async fn combined_fuses_grace_and_the_operator_fan_out_mask_together() -> Result<()> {
    let masked = [
        "set_ttl",
        "create_subnames",
        "burn_fuses",
        "approve",
        "extend_subname_expiry",
    ];
    for (case, fuses, offset, expected) in [
        (
            "locked",
            PARENT_CANNOT_CONTROL | CANNOT_UNWRAP | CANNOT_SET_RESOLVER | CANNOT_TRANSFER,
            1_000_000,
            vec![
                with(HOLDER, "holder", &masked),
                with(OPERATOR, "operator", &masked),
            ],
        ),
        (
            "grace",
            PARENT_CANNOT_CONTROL | IS_DOT_ETH | CANNOT_APPROVE,
            GRACE_PERIOD - 1_000,
            vec![],
        ),
    ] {
        let fixture = Fixture::new(&format!("families_shadow_permissions_mask_{case}"), 20).await?;
        let resource = wrapped(&fixture, fuses, timestamp(TARGET) + offset).await?;
        let report = publish_and_compare(&fixture, TARGET).await?;
        shadow_support::assert_counts(&report, &[], &[]);
        assert_eq!(rows(&fixture, &resource).await?, expected, "{case}");
        fixture.cleanup().await?;
    }
    Ok(())
}

/// Item 6 of the TYR-36 step 3 review (Q6): the wrapper restriction block is served while the
/// wrapper's newest mint, holder grant, holder revocation or NameUnwrapped is a mint or a
/// holder grant (resource_summary.rs:164-196). Step 2 keeps that lifecycle on the wrapper row,
/// NameUnwrapped included (`project_wrapper_state.lifecycle_unwrapped`), so an unwrap that
/// revokes no holder grant is seen: neither side serves a restriction block.
#[tokio::test]
async fn an_unwrap_that_revokes_no_holder_grant_clears_the_restrictions() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_unwrap", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL;
    let expiry = timestamp(TARGET) + 1_000_000;
    let resource = wrapped(&fixture, fuses, expiry).await?;
    wrapper_event(
        &fixture,
        12,
        1,
        "AuthorityEpochChanged",
        &resource,
        json!({}),
        json!({"source_event": "NameUnwrapped", "node": node(1), "authority_kind": "wrapper"}),
    )
    .await?;
    let report = publish_and_compare(&fixture, TARGET).await?;
    let served: Option<Value> = sqlx::query_scalar(
        "SELECT resource_restrictions FROM permissions_current_resource_summary
         WHERE resource_id = $1::uuid",
    )
    .bind(&resource)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(served, None, "today's reader sees the unwrap");
    let shadow = load_shadow_permissions(
        &fixture.pool,
        CHAIN,
        &Clock {
            block_number: TARGET,
            timestamp_seconds: timestamp(TARGET),
        },
        &[ResourceInput {
            resource_id: resource.clone(),
            authority_kind: Some("wrapper".into()),
            root_resource_id: None,
        }],
    )
    .await?;
    assert_eq!(
        shadow[&resource].restrictions, None,
        "the families see it too"
    );
    shadow_support::assert_counts(&report, &[], &[]);
    fixture.cleanup().await
}

/// Scoped review of ea047c04, Q5: the wrapper is unwrapped at 12 with no holder revocation, then
/// wrapped again on the same resource at 13 (a NameWrapped mint). The newest lifecycle event is
/// the mint, so both today's reader (resource_summary.rs:172-197) and the families'
/// `lifecycle_unwrapped` serve the restriction block again.
#[tokio::test]
async fn an_unwrap_then_a_rewrap_brings_the_restrictions_back() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_rewrap", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL;
    let expiry = timestamp(TARGET) + 1_000_000;
    let resource = wrapped(&fixture, fuses, expiry).await?;
    wrapper_event(
        &fixture,
        12,
        1,
        "AuthorityEpochChanged",
        &resource,
        json!({}),
        json!({"source_event": "NameUnwrapped", "node": node(1), "authority_kind": "wrapper"}),
    )
    .await?;
    wrapper_event(
        &fixture,
        13,
        1,
        "TokenControlTransferred",
        &resource,
        json!({"from": null}),
        json!({"source_event": "NameWrapped", "node": node(1), "owner": HOLDER, "to": HOLDER,
               "fuses": fuses, "authority_kind": "wrapper"}),
    )
    .await?;
    let report = publish_and_compare(&fixture, TARGET).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    let served: Option<Value> = sqlx::query_scalar(
        "SELECT resource_restrictions FROM permissions_current_resource_summary
         WHERE resource_id = $1::uuid",
    )
    .bind(&resource)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        served,
        Some(
            json!({"kind": "ens_v1_wrapper", "wrapper_state": "emancipated",
                    "fuses": fuses, "expiry_seconds": expiry})
        ),
        "the rewrap serves the block again"
    );
    fixture.cleanup().await
}

const V2_INSTANCE: &str = "00000000-0000-0000-0000-000000000021";
const V2_REGISTRY: &str = "0x00000000000000000000000000000000000021aa";
const ZERO_WORD: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const CHILD_WORD: &str = "0x0000000000000000000000000000000000000000000000000000000000001389";
const ROOT_ADMIN: &str = "0x0000000000000000000000000000000000002022";

/// One ENSv2 role change (EACRolesChanged) on the registration or, with `root`, on the registry
/// root, in the shape of crates/project/tests/v2_locked_roles.rs.
async fn role(
    fixture: &Fixture,
    root: bool,
    block: i64,
    resource: &str,
    subject: &str,
    powers: &[&str],
) -> Result<()> {
    let upstream = if root { ZERO_WORD } else { CHILD_WORD };
    let source = json!({"kind": "raw_log", "source_event": "EACRolesChanged",
                        "upstream_resource": upstream, "root_resource": root,
                        "changed_powers": powers, "registry_contract_instance_id": V2_INSTANCE});
    let identity = format!("role:{block}:{subject}");
    fixture
        .event(
            support::Event::new(
                &identity,
                block,
                if root { 2 } else { 1 },
                if root {
                    "RootPermissionChanged"
                } else {
                    "PermissionChanged"
                },
                "ens_v2_registry_l1",
            )
            .resource(resource)
            .after(json!({
                "subject": subject,
                "scope": {"kind": if root { "registry_root" } else { "registry" },
                          "chain_id": CHAIN, "registry_address": V2_REGISTRY},
                "effective_powers": powers, "grant_source": source, "revocation_source": null,
                "inheritance_path": if root {
                    json!([{"kind": "registry_root_fallback", "chain_id": CHAIN,
                            "registry_address": V2_REGISTRY, "upstream_resource": upstream}])
                } else {
                    json!([])
                },
                "transfer_behavior": {}, "source_event": "EACRolesChanged",
                "upstream_resource": upstream, "resource": upstream, "root_resource": root,
                "registry_contract_instance_id": V2_INSTANCE,
            }))
            .raw(json!({"emitting_address": V2_REGISTRY})),
        )
        .await?;
    Ok(())
}

/// `(subject, powers)` per served row of `resource`.
async fn served_rows(fixture: &Fixture, resource: &str) -> Result<Vec<(String, Value)>> {
    Ok(sqlx::query_as(
        "SELECT subject, effective_powers FROM permissions_current
         WHERE resource_id = $1::uuid ORDER BY subject, scope",
    )
    .bind(resource)
    .fetch_all(&fixture.pool)
    .await?)
}

async fn locked_roles(fixture: &Fixture, resource: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT resource_restrictions FROM permissions_current_resource_summary
         WHERE resource_id = $1::uuid",
    )
    .bind(resource)
    .fetch_optional(&fixture.pool)
    .await?
    .flatten())
}

/// Item 6 of the TYR-36 step 3 review (Q6), admin powers under expiry. An ENSv2 registration
/// whose holder has no admin role and whose registry root has an `admin_renew` admin: before
/// expiry the registration serves its holder row and every role but `renew` is locked; once
/// the interpreter's path expiry lapses it, the registration serves no rows, no admin powers
/// and no restriction block, while the root keeps serving its admin (resource_summary.rs
/// :272-325, permissions.rs:391-398). The harness compares each resource's admin powers
/// directly.
#[tokio::test]
async fn a_lapsed_registration_drops_its_rows_and_the_root_keeps_its_admin() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_root_admin", 20).await?;
    let (child, root) = (uuid(1), uuid(50));
    for (resource, upstream) in [(&child, CHILD_WORD), (&root, ZERO_WORD)] {
        fixture.resource(resource).await?;
        sqlx::query("UPDATE resources SET provenance = $2 WHERE resource_id = $1::uuid")
            .bind(resource)
            .bind(json!({"adapter": "ens_v2_permissions", "chain_id": CHAIN,
                         "source_family": "ens_v2_registry_l1", "registry_address": V2_REGISTRY,
                         "registry_contract_instance_id": V2_INSTANCE,
                         "upstream_resource": upstream}))
            .execute(&fixture.pool)
            .await?;
    }
    fixture
        .binding(&uuid(100), &name(1), &child, "ens_v2", 9, 0, None)
        .await?;
    let v2_after = |kind: &str| {
        json!({"authority_kind": "ens_v2_registry", "registry_contract_instance_id": V2_INSTANCE,
               "token_id": "5001", "status": kind, "registrant": HOLDER,
               "expiry": 1_800_000_100u64, "state_derived": false})
    };
    fixture
        .write(
            9,
            1,
            "SurfaceBound",
            "ens_v2_registry_l1",
            Some(&name(1)),
            Some(&child),
            json!({"authority_kind": "ens_v2_registry", "state_derived": false}),
            V2_REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            Some(&name(1)),
            Some(&child),
            v2_after("registered"),
            V2_REGISTRY,
        )
        .await?;
    role(
        &fixture,
        false,
        10,
        &child,
        HOLDER,
        &["unregister", "set_resolver"],
    )
    .await?;
    role(&fixture, true, 11, &root, ROOT_ADMIN, &["admin_renew"]).await?;
    let report = publish_and_compare(&fixture, 12).await?;
    shadow_support::assert_counts(&report, &[], &[]);
    assert_eq!(
        locked_roles(&fixture, &child).await?,
        Some(json!({"kind": "ens_v2_registry",
                    "locked_roles": ["unregister", "set_subregistry", "set_resolver", "transfer"]}))
    );
    assert_eq!(
        served_rows(&fixture, &child).await?,
        vec![(HOLDER.to_owned(), json!(["unregister", "set_resolver"]))]
    );

    fixture
        .event(
            support::Event::new(
                "path-expiry",
                14,
                0,
                "RegistrationReleased",
                "ens_v2_registry_l1",
            )
            .resource(&child)
            .after(json!({"source_event": "RegistryPathExpired",
                              "derived_from": "interpreter_state",
                              "terminal_reason": "registry_name_binding_expired",
                              "expiry": 1_800_000_100u64,
                              "registry_contract_instance_id": V2_INSTANCE,
                              "token_id": "5001"}))
            .raw(json!({"emitting_address": V2_REGISTRY}))
            .synthesised(),
        )
        .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    let admins: Vec<(String, Vec<String>)> = sqlx::query_as(
        r"SELECT served.resource_id::text, array_agg(DISTINCT power.value ORDER BY power.value)
         FROM permissions_current served
         CROSS JOIN LATERAL jsonb_array_elements_text(served.effective_powers) power
         WHERE served.scope_kind IN ('registry', 'root')
           AND (power.value LIKE 'admin\_%' OR power.value = 'can_transfer_admin')
         GROUP BY 1",
    )
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(admins, vec![(root.clone(), vec!["admin_renew".to_owned()])]);
    assert_eq!(
        served_rows(&fixture, &child).await?,
        vec![],
        "the lapsed registration serves no rows"
    );
    assert_eq!(
        served_rows(&fixture, &root).await?,
        vec![(ROOT_ADMIN.to_owned(), json!(["admin_renew"]))]
    );
    assert_eq!(locked_roles(&fixture, &child).await?, None);
    // The name itself is the disclosed path-expiry shape; every resource field is equal.
    let cause = "served_membership_skips_unnamed_path_expiry";
    let known: Vec<String> = [
        "registration/status",
        "registration/authority_kind",
        "registration/registrant",
        "registration/latest_event_kind",
        "control/status",
        "control/expiry",
        "control/registrant",
    ]
    .iter()
    .map(|field| format!("{cause}:{field}"))
    .collect();
    let known: Vec<(&str, usize)> = known.iter().map(|field| (field.as_str(), 1)).collect();
    shadow_support::assert_counts(&report, &known, &[]);
    fixture.cleanup().await
}

/// Pro Q5 on ea047c04, conflicting lifecycle events at one position. A NameWrapper
/// TransferSingle emits the old holder's revoke and then the new holder's grant from one log
/// (adapters schema_v2/protocol/v1/wrapper/transfer.rs:158-159), their identities ending with
/// the facts' emission ordinals in the adapter's order (adapters schema_v2/normalized.rs:118-131):
/// the log's TokenControlTransferred is fact 0, so here `holder:0:revoke:<old>:1` and
/// `holder:0:grant:<new>:2`. Today's summary ranks wrapper lifecycle events by
/// position and then generated id (resource_summary.rs:172-196), so the grant, pushed second, is
/// the latest and the restriction block stays. Under step 2's amended D12 (39990c38) the family
/// folds facts of one log by that emission ordinal, so it takes the grant too and serves the same
/// block. Until 39990c38 the revoke's identity sorted after the grant's and this was pinned as a
/// mismatch on `resource_restrictions`; it is now pinned equal.
#[tokio::test]
async fn a_holder_transfer_from_one_log_keeps_the_restrictions_in_emission_order() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_one_log_transfer", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL;
    let expiry = timestamp(TARGET) + 1_000_000;
    let resource = wrapped(&fixture, fuses, expiry).await?;
    for (ordinal, (subject, action, grant)) in
        [(HOLDER, "revoke", false), (NEXT_HOLDER, "grant", true)]
            .into_iter()
            .enumerate()
    {
        let identity = format!(
            "0xtx13:1:PermissionChanged:holder:0:{action}:{subject}:{}",
            ordinal + 1
        );
        fixture
            .event(
                Event::new(&identity, 13, 1, "PermissionChanged", V1_WRAPPER)
                    .name(&name(1))
                    .resource(&resource)
                    .before(permission(
                        subject,
                        "holder",
                        HOLDER_POWERS,
                        "TransferSingle",
                        !grant,
                    ))
                    .after(permission(
                        subject,
                        "holder",
                        HOLDER_POWERS,
                        "TransferSingle",
                        grant,
                    ))
                    .raw(json!({"emitting_address": WRAPPER})),
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, TARGET).await?;
    let served: Option<Value> = sqlx::query_scalar(
        "SELECT resource_restrictions FROM permissions_current_resource_summary
         WHERE resource_id = $1::uuid",
    )
    .bind(&resource)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        served,
        Some(
            json!({"kind": "ens_v1_wrapper", "wrapper_state": "emancipated", "fuses": fuses,
                    "expiry_seconds": expiry})
        ),
        "today's summary keeps the block through the transfer"
    );
    shadow_support::assert_counts(&report, &[], &[]);
    assert_eq!(
        (report.equal, report.mismatched),
        (3, 0),
        "{:#?}",
        report.lines
    );
    fixture.cleanup().await
}

/// Pro Q5 on ea047c04: an unwrap and a wrapper expiry update in one block, in both log orders,
/// read exactly at the new expiry and one block after it. The unwrap gate and the expiry mask
/// are separate: whichever log comes first, the name is unwrapped, so neither side serves a
/// restriction block, and every name and resource equals its family read at both blocks.
#[tokio::test]
async fn an_unwrap_and_an_expiry_update_in_one_block_in_both_orders() -> Result<()> {
    for (index, unwrap_first) in [true, false].into_iter().enumerate() {
        let fixture = Fixture::new(
            &format!("families_shadow_permissions_unwrap_expiry_{index}"),
            20,
        )
        .await?;
        let fuses = PARENT_CANNOT_CONTROL;
        let resource = wrapped(&fixture, fuses, timestamp(TARGET) + 1_000_000).await?;
        let (unwrap_log, expiry_log) = if unwrap_first { (1, 2) } else { (2, 1) };
        let mut events = vec![
            (
                unwrap_log,
                "AuthorityEpochChanged",
                json!({"source_event": "NameUnwrapped", "node": node(1),
                       "authority_kind": "wrapper"}),
            ),
            (
                expiry_log,
                "ExpiryChanged",
                json!({"source_event": "ExpiryExtended", "node": node(1),
                       "expiry": timestamp(14), "authority_kind": "wrapper"}),
            ),
        ];
        events.sort_by_key(|(log, _, _)| *log);
        for (log, kind, after) in events {
            wrapper_event(&fixture, 12, log, kind, &resource, json!({}), after).await?;
        }
        for target in [14, 15] {
            let report = publish_and_compare(&fixture, target).await?;
            shadow_support::assert_counts(&report, &[], &[]);
            let served: Option<Value> = sqlx::query_scalar(
                "SELECT resource_restrictions FROM permissions_current_resource_summary
                 WHERE resource_id = $1::uuid",
            )
            .bind(&resource)
            .fetch_one(&fixture.pool)
            .await?;
            assert_eq!(served, None, "unwrap first {unwrap_first}, target {target}");
        }
        fixture.cleanup().await?;
    }
    Ok(())
}

/// The second one-log transfer shape (Codex thread PRRT_kwDOSJpxAs6l4hOw): the recipient of a
/// wrapped transfer is the token's approved delegate. `_beforeTransfer` clears the approval, so
/// the adapter emits the delegate's token-approval revoke before the holder rows from the same
/// log, and relies on the recipient's holder grant being the later row
/// (adapters schema_v2/protocol/v1/wrapper/transfer.rs:141-146). Both rows fold to one family
/// grant key, the resource, subject and resource scope. Today's builder keeps the grant, the
/// higher generated id, and serves the recipient's holder row. Under step 2's amended D12
/// (39990c38) the family folds the log's facts by their emission ordinals in the adapter's order
/// (after the TokenControlTransferred at 0: 1 for the delegate's revoke, 3 for the grant), keeps the grant and serves the same rows and restriction block. Until
/// 39990c38 the revoke's identity (`token_approval`) sorted after the grant's (`holder`) and this
/// was pinned as a mismatch on `permissions_current` and `resource_restrictions`; it is now
/// pinned equal.
#[tokio::test]
async fn a_transfer_to_the_delegate_from_one_log_keeps_the_recipients_holder_row() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_one_log_delegate", 20).await?;
    let fuses = PARENT_CANNOT_CONTROL;
    let resource = wrapped(&fixture, fuses, timestamp(TARGET) + 1_000_000).await?;
    permission_changed(
        &fixture,
        12,
        1,
        &resource,
        NEXT_HOLDER,
        "token_approval",
        &["extend_subname_expiry"],
        "Approval",
        true,
    )
    .await?;
    for (ordinal, (relation, powers, action, subject, grant)) in [
        (
            "token_approval",
            &["extend_subname_expiry"][..],
            "revoke",
            NEXT_HOLDER,
            false,
        ),
        ("holder", HOLDER_POWERS, "revoke", HOLDER, false),
        ("holder", HOLDER_POWERS, "grant", NEXT_HOLDER, true),
    ]
    .into_iter()
    .enumerate()
    {
        let identity = format!(
            "0xtx13:1:PermissionChanged:{relation}:0:{action}:{subject}:{}",
            ordinal + 1
        );
        fixture
            .event(
                Event::new(&identity, 13, 1, "PermissionChanged", V1_WRAPPER)
                    .name(&name(1))
                    .resource(&resource)
                    .before(permission(
                        subject,
                        relation,
                        powers,
                        "TransferSingle",
                        !grant,
                    ))
                    .after(permission(
                        subject,
                        relation,
                        powers,
                        "TransferSingle",
                        grant,
                    ))
                    .raw(json!({"emitting_address": WRAPPER})),
            )
            .await?;
    }
    let report = publish_and_compare(&fixture, TARGET).await?;
    let served = rows(&fixture, &resource).await?;
    assert_eq!(
        served,
        [(
            NEXT_HOLDER.to_owned(),
            "holder".to_owned(),
            // Every holder power but `extend_expiry`, which the served row derives away.
            json!(
                HOLDER_POWERS
                    .iter()
                    .filter(|power| **power != "extend_expiry")
                    .collect::<Vec<_>>()
            )
        )],
        "today's builder serves the recipient's holder row only"
    );
    shadow_support::assert_counts(&report, &[], &[]);
    assert_eq!(
        (report.equal, report.mismatched),
        (3, 0),
        "{:#?}",
        report.lines
    );
    fixture.cleanup().await
}

/// An ENSv2 registration's after-state for token `token`.
fn v2_grant_after(token: &str) -> Value {
    json!({"authority_kind": "ens_v2_registry", "registry_contract_instance_id": V2_INSTANCE,
           "token_id": token, "status": "registered", "registrant": HOLDER,
           "expiry": 2_000_000_000u64, "state_derived": false})
}

/// Child registration `uuid(1)` of registry root `uuid(50)`, both resources of the ENSv2
/// registry: the child bound to name 1 at 9, granted at 10 and its holder given `child_powers`
/// on it. The root has no event.
async fn child_of_root(fixture: &Fixture, child_powers: &[&str]) -> Result<(String, String)> {
    let (child, root) = (uuid(1), uuid(50));
    for (resource, upstream) in [(&child, CHILD_WORD), (&root, ZERO_WORD)] {
        fixture.resource(resource).await?;
        sqlx::query("UPDATE resources SET provenance = $2 WHERE resource_id = $1::uuid")
            .bind(resource)
            .bind(json!({"adapter": "ens_v2_permissions", "chain_id": CHAIN,
                         "source_family": "ens_v2_registry_l1", "registry_address": V2_REGISTRY,
                         "registry_contract_instance_id": V2_INSTANCE,
                         "upstream_resource": upstream}))
            .execute(&fixture.pool)
            .await?;
    }
    fixture
        .binding(&uuid(100), &name(1), &child, "ens_v2", 9, 0, None)
        .await?;
    fixture
        .write(
            9,
            1,
            "SurfaceBound",
            "ens_v2_registry_l1",
            Some(&name(1)),
            Some(&child),
            json!({"authority_kind": "ens_v2_registry", "state_derived": false}),
            V2_REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            Some(&name(1)),
            Some(&child),
            v2_grant_after("5001"),
            V2_REGISTRY,
        )
        .await?;
    role(fixture, false, 10, &child, HOLDER, child_powers).await?;
    Ok((child, root))
}

/// The root-reversal shape: child registration `uuid(1)` bound to name 1 and granted at 10, its
/// holder given `child_powers` on it; registry root `uuid(50)` granted at 10 with `admin_renew`
/// at 11, and at block 14 the root's path-expiry release (log 2) written before a new grant of it
/// (log 1). The canonical order takes the release last and lapses the root; today's order takes
/// the grant (higher id) and keeps it live.
async fn root_reversal(fixture: &Fixture, child_powers: &[&str]) -> Result<(String, String)> {
    let (child, root) = child_of_root(fixture, child_powers).await?;
    let root_grant = |identity: &'static str, block: i64| {
        support::Event::new(
            identity,
            block,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
        )
        .resource(&root)
        .after(v2_grant_after("9001"))
        .raw(json!({"emitting_address": V2_REGISTRY}))
    };
    fixture.event(root_grant("root-grant-10", 10)).await?;
    role(fixture, true, 11, &root, ROOT_ADMIN, &["admin_renew"]).await?;
    fixture
        .event(
            support::Event::new(
                "root-path-expiry",
                14,
                2,
                "RegistrationReleased",
                "ens_v2_registry_l1",
            )
            .resource(&root)
            .after(json!({"source_event": "RegistryPathExpired",
                              "derived_from": "interpreter_state",
                              "terminal_reason": "registry_name_binding_expired",
                              "expiry": 1_800_000_100u64,
                              "registry_contract_instance_id": V2_INSTANCE,
                              "token_id": "9001"}))
            .raw(json!({"emitting_address": V2_REGISTRY})),
        )
        .await?;
    fixture.event(root_grant("root-grant-14", 14)).await?;
    Ok((child, root))
}

/// The resource's `project_resource_admin_aggregate.admin_powers` object, keyed by holder.
async fn admin_aggregate(fixture: &Fixture, resource: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT admin_powers FROM bigname_phase.project_resource_admin_aggregate
         WHERE resource_id = $1::uuid",
    )
    .bind(resource)
    .fetch_one(&fixture.pool)
    .await?)
}

/// Replace the resource's admin aggregate object; exactly one row must change.
async fn set_admin_aggregate(fixture: &Fixture, resource: &str, powers: &Value) -> Result<()> {
    let updated = sqlx::query(
        "UPDATE bigname_phase.project_resource_admin_aggregate SET admin_powers = $2
         WHERE resource_id = $1::uuid",
    )
    .bind(resource)
    .bind(powers)
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    assert_eq!(updated, 1, "one admin aggregate row of {resource}");
    Ok(())
}

/// Every holder of the aggregate given exactly `powers`.
fn every_holder(aggregate: &Value, powers: &[&str]) -> Value {
    Value::Object(
        aggregate
            .as_object()
            .expect("the reducer keeps an object keyed by holder")
            .keys()
            .map(|holder| (holder.clone(), json!(powers)))
            .collect(),
    )
}

/// Codex thread PRRT_kwDOSJpxAs6l9h_u: a live ENSv2 registration's locked roles read its
/// registry root's admin powers (resource_summary.rs:272-325), which the root's own path-expiry
/// drop decides. The root holds a grant at block 10 and its `admin_renew` admin at 11; at block
/// 14 the interpreter's path-expiry release of the root (log 2) is written before a new grant
/// of it (log 1). The canonical order takes the release last and lapses the root, so the
/// families lock every role of the child; today's order takes the grant (higher id), keeps the
/// root live and serves `renew` unlocked. The child's restriction block differs only through
/// its root, and passes as a same-block delta only when the whole block read in today's order
/// equals the served one and the canonical read the shadow one. The child's holder also holds
/// `admin_set_subregistry`, so both resources have an admin aggregate. A wrong admin role in the
/// families, on the root or on the child, written into that holder-keyed object and restored
/// before the next case, leaves the child's block a mismatch and fails the report.
#[tokio::test]
async fn a_root_reversal_moves_the_child_restrictions_as_a_same_block_delta() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_root_reversal", 20).await?;
    let (child, root) = root_reversal(
        &fixture,
        &["unregister", "set_resolver", "admin_set_subregistry"],
    )
    .await?;
    let report = publish_and_compare(&fixture, 16).await?;
    assert_eq!(
        locked_roles(&fixture, &child).await?,
        Some(json!({"kind": "ens_v2_registry",
                    "locked_roles": ["unregister", "set_resolver", "transfer"]})),
        "today's order keeps the root live and serves renew unlocked"
    );
    // The root's own rows, admin powers and restriction block are the resource's own lapse
    // delta; the child's restriction block is the second restriction delta.
    shadow_support::assert_counts(
        &report,
        &[],
        &[
            ("d12_same_block_order:admin_powers", 1),
            ("d12_same_block_order:permissions_current", 1),
            ("d12_same_block_order:resource_restrictions", 2),
        ],
    );
    let baseline = (
        report.expected_delta_fields.clone(),
        report.mismatched,
        report.equal,
    );
    for (case, resource, fields) in [
        (
            "root admin",
            &root,
            vec![
                (root.as_str(), "admin_powers"),
                (root.as_str(), "resource_restrictions"),
                (child.as_str(), "resource_restrictions"),
            ],
        ),
        (
            "child admin",
            &child,
            vec![
                (child.as_str(), "admin_powers"),
                (child.as_str(), "resource_restrictions"),
            ],
        ),
    ] {
        let original = admin_aggregate(&fixture, resource).await?;
        set_admin_aggregate(
            &fixture,
            resource,
            &every_holder(&original, &["admin_set_resolver"]),
        )
        .await?;
        let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
        let mut failed: Vec<(String, String)> = mutated
            .lines
            .iter()
            .filter(|line| line.starts_with("SEPOLIA_END_TO_END_SHADOW_MISMATCH"))
            .filter_map(|line| {
                let key = line.split(" key=").nth(1)?.split(' ').next()?;
                let field = line.split(" field=").nth(1)?.split(' ').next()?;
                Some((key.to_owned(), field.to_owned()))
            })
            .collect();
        failed.sort();
        let mut expected: Vec<(String, String)> = fields
            .iter()
            .map(|(key, field)| ((*key).to_owned(), (*field).to_owned()))
            .collect();
        expected.sort();
        assert_eq!(failed, expected, "{case}: {:#?}", mutated.lines);
        set_admin_aggregate(&fixture, resource, &original).await?;
        let restored = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
        assert_eq!(
            (
                restored.expected_delta_fields,
                restored.mismatched,
                restored.equal
            ),
            baseline,
            "{case}: the baseline is back before the next case"
        );
    }
    fixture.cleanup().await
}

/// Pro r5 Q6 on c23e3e5b, overlapping admin powers: the child's holder and the root's admin
/// both hold `admin_renew`, so `renew` is unlocked on the child in both orders and nothing
/// differs. With `admin_renew` removed only from the child's aggregate (its row deleted, as the
/// reducer leaves an emptied aggregate), today's read of the
/// child's block still unlocks `renew` through the live root and equals the served block, while
/// the canonical read, the root lapsed, locks it and equals the corrupt shadow: the restriction
/// block alone could pass as the root-reversal delta. The child's own admin powers still differ
/// and no excuse covers them, so the report fails through `admin_powers`.
#[tokio::test]
async fn a_child_admin_power_the_root_also_holds_still_fails_the_report() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_root_overlap", 20).await?;
    let (child, _) =
        root_reversal(&fixture, &["unregister", "set_resolver", "admin_renew"]).await?;
    let report = publish_and_compare(&fixture, 16).await?;
    shadow_support::assert_counts(
        &report,
        &[],
        &[
            ("d12_same_block_order:admin_powers", 1),
            ("d12_same_block_order:permissions_current", 1),
            ("d12_same_block_order:resource_restrictions", 1),
        ],
    );
    // The child's holder is the aggregate's one holder and `admin_renew` its one admin power.
    // Dropping that power empties the object, and the reducer deletes an empty aggregate row
    // (crates/project/src/families/permissions.rs:386-396), so the row gone is what a reducer
    // that lost the power would leave.
    let holders = admin_aggregate(&fixture, &child).await?;
    assert_eq!(
        holders
            .as_object()
            .map(|holders| holders.values().collect::<Vec<_>>()),
        Some(vec![&json!(["admin_renew"])]),
        "{holders}"
    );
    let deleted = sqlx::query(
        "DELETE FROM bigname_phase.project_resource_admin_aggregate WHERE resource_id = $1::uuid",
    )
    .bind(&child)
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    assert_eq!(deleted, 1);
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
    // The child's restriction block passes as the root-reversal delta, as the rule allows; the
    // child's admin powers are the one mismatch, and they fail the run.
    let failed: Vec<&str> = mutated
        .lines
        .iter()
        .filter(|line| line.starts_with("SEPOLIA_END_TO_END_SHADOW_MISMATCH"))
        .filter(|line| line.contains(&format!("key={child} ")))
        .filter_map(|line| line.split(" field=").nth(1)?.split(' ').next())
        .collect();
    assert_eq!(failed, ["admin_powers"], "{:#?}", mutated.lines);
    assert_eq!(mutated.mismatched, 1, "{:#?}", mutated.lines);
    assert_eq!(
        mutated.expected_delta_fields,
        [
            ("d12_same_block_order:admin_powers".to_owned(), 1),
            ("d12_same_block_order:permissions_current".to_owned(), 1),
            ("d12_same_block_order:resource_restrictions".to_owned(), 2),
        ]
        .into(),
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// Scoped pass on 4f480c6c, note 2: the one-direction permission excuse reads the root's admin
/// powers in today's order too, so it also needs the root's retained events to match the log.
/// The child is granted at 10 and, at block 14, released by an unnamed path expiry (log 2)
/// written before a new grant (log 1): today's order keeps it live, the canonical order lapses
/// it, and its permission rows pass as a same-block delta. With the root's grant row dropped
/// from the families, the root's retained events no longer match the log, and the child's
/// permission-row and restriction excuses are refused (the admin-power one would be too; the
/// child has no admin difference here): the refusal is in the safe direction.
#[tokio::test]
async fn a_root_retention_gap_refuses_the_child_permission_excuse() -> Result<()> {
    let fixture = Fixture::new("families_shadow_permissions_root_gap", 20).await?;
    let (child, root, n1) = (uuid(1), uuid(50), name(1));
    for (resource, upstream) in [(&child, CHILD_WORD), (&root, ZERO_WORD)] {
        fixture.resource(resource).await?;
        sqlx::query("UPDATE resources SET provenance = $2 WHERE resource_id = $1::uuid")
            .bind(resource)
            .bind(json!({"adapter": "ens_v2_permissions", "chain_id": CHAIN,
                         "source_family": "ens_v2_registry_l1", "registry_address": V2_REGISTRY,
                         "registry_contract_instance_id": V2_INSTANCE,
                         "upstream_resource": upstream}))
            .execute(&fixture.pool)
            .await?;
    }
    fixture
        .binding(&uuid(100), &name(1), &child, "ens_v2", 9, 0, None)
        .await?;
    fixture
        .write(
            9,
            1,
            "SurfaceBound",
            "ens_v2_registry_l1",
            Some(&name(1)),
            Some(&child),
            json!({"authority_kind": "ens_v2_registry", "state_derived": false}),
            V2_REGISTRY,
        )
        .await?;
    let grant_after = |token: &str| {
        json!({"authority_kind": "ens_v2_registry", "registry_contract_instance_id": V2_INSTANCE,
               "token_id": token, "status": "registered", "registrant": HOLDER,
               "expiry": 2_000_000_000u64, "state_derived": false})
    };
    let child_grant = |identity: &'static str, block: i64| {
        support::Event::new(
            identity,
            block,
            1,
            "RegistrationGranted",
            "ens_v2_registry_l1",
        )
        .name(&n1)
        .resource(&child)
        .after(grant_after("5001"))
        .raw(json!({"emitting_address": V2_REGISTRY}))
    };
    fixture.event(child_grant("child-grant-10", 10)).await?;
    role(
        &fixture,
        false,
        10,
        &child,
        HOLDER,
        &["unregister", "set_resolver"],
    )
    .await?;
    fixture
        .event(
            support::Event::new(
                "root-grant-10",
                10,
                2,
                "RegistrationGranted",
                "ens_v2_registry_l1",
            )
            .resource(&root)
            .after(v2_grant_after("9001"))
            .raw(json!({"emitting_address": V2_REGISTRY})),
        )
        .await?;
    role(&fixture, true, 11, &root, ROOT_ADMIN, &["admin_renew"]).await?;
    fixture
        .event(
            support::Event::new(
                "child-path-expiry",
                14,
                2,
                "RegistrationReleased",
                "ens_v2_registry_l1",
            )
            .resource(&child)
            .after(json!({"source_event": "RegistryPathExpired",
                              "derived_from": "interpreter_state",
                              "terminal_reason": "registry_name_binding_expired",
                              "expiry": 1_800_000_100u64,
                              "registry_contract_instance_id": V2_INSTANCE,
                              "token_id": "5001"}))
            .raw(json!({"emitting_address": V2_REGISTRY})),
        )
        .await?;
    fixture.event(child_grant("child-grant-14", 14)).await?;
    let child_rows = |report: &shadow_support::compare::Report, kind: &str| {
        report.lines.iter().any(|line| {
            line.starts_with(kind)
                && line.contains(&format!("key={child} "))
                && line.contains("field=permissions_current")
        })
    };
    let report = publish_and_compare(&fixture, 16).await?;
    // Name 1 reads the unnamed release as served-side bug 1 on seven fields.
    let known: Vec<(String, usize)> = [
        "control/expiry",
        "control/registrant",
        "control/status",
        "registration/authority_kind",
        "registration/latest_event_kind",
        "registration/registrant",
        "registration/status",
    ]
    .iter()
    .map(|field| {
        (
            format!("served_membership_skips_unnamed_path_expiry:{field}"),
            1,
        )
    })
    .collect();
    let known_pairs: Vec<(&str, usize)> = known
        .iter()
        .map(|(field, count)| (field.as_str(), *count))
        .collect();
    shadow_support::assert_counts(
        &report,
        &known_pairs,
        &[
            ("d12_same_block_order:permissions_current", 1),
            ("d12_same_block_order:resource_restrictions", 1),
        ],
    );
    sqlx::query(
        "DELETE FROM bigname_phase.project_lifecycle_event WHERE event_identity = 'root-grant-10'",
    )
    .execute(&fixture.pool)
    .await?;
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, 16).await?;
    assert!(
        child_rows(&mutated, "SEPOLIA_END_TO_END_SHADOW_MISMATCH"),
        "a root retention gap must leave the child's permission rows a mismatch: {:#?}",
        mutated.lines
    );
    // Both differing fields of the child are refused; they are the permission-row and
    // restriction excuses `resource_excuses` gives.
    assert!(
        mutated.expected_delta_fields.is_empty(),
        "{:#?}",
        mutated.lines
    );
    assert_eq!(
        mutated.known_discrepancy,
        known.into_iter().collect(),
        "{:#?}",
        mutated.lines
    );
    let mut failed: Vec<&str> = mutated
        .lines
        .iter()
        .filter(|line| line.starts_with("SEPOLIA_END_TO_END_SHADOW_MISMATCH"))
        .filter_map(|line| line.split(" field=").nth(1)?.split(' ').next())
        .collect();
    failed.sort();
    assert_eq!(
        failed,
        ["permissions_current", "resource_restrictions"],
        "{:#?}",
        mutated.lines
    );
    fixture.cleanup().await
}

/// Pro r6 Q4 and Q5 on ba2ffbd5, a root the comparison reaches only through a child. The child
/// names registry root `uuid(50)` in its served summary, and the family reader loads the root
/// for the child's restriction block whatever the served tables hold. The comparison set was
/// built from this chain's served summaries and permission rows only, so a root with neither
/// (here its summary row is deleted; the root has no permission row) was not compared, and
/// another chain's direct row on it, which the effective-permission reader serves by resource
/// id, went unchecked. The set is now closed over the summaries' roots: the root is compared
/// and equals, a family grant copied onto it is exactly a `permissions_current` mismatch of the
/// root, and the foreign row is an `other_chain_rows` mismatch with no excuse.
#[tokio::test]
async fn a_root_reached_only_through_a_child_is_audited() -> Result<()> {
    const OTHER: &str = "other-chain";
    let fixture = Fixture::new("families_shadow_permissions_dependency_root", 20).await?;
    let (child, root) = child_of_root(&fixture, &["unregister", "set_resolver"]).await?;
    let baseline = publish_and_compare(&fixture, TARGET).await?;
    shadow_support::assert_counts(&baseline, &[], &[]);
    let deleted = sqlx::query(
        "DELETE FROM permissions_current_resource_summary WHERE resource_id = $1::uuid",
    )
    .bind(&root)
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    assert_eq!(deleted, 1);
    let (named_root, root_rows): (Option<String>, i64) = sqlx::query_as(
        "SELECT (SELECT root_resource_id::text FROM permissions_current_resource_summary
                 WHERE resource_id = $1::uuid),
                (SELECT count(*) FROM permissions_current WHERE resource_id = $2::uuid)",
    )
    .bind(&child)
    .bind(&root)
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        (named_root.as_deref(), root_rows),
        (Some(root.as_str()), 0),
        "the child names the root, which serves nothing here"
    );
    let closed = shadow_support::compare::compare(&fixture.pool, CHAIN, TARGET).await?;
    shadow_support::assert_counts(&closed, &[], &[]);
    assert_eq!(
        (closed.resources, closed.equal, closed.mismatched),
        (baseline.resources, baseline.equal, 0),
        "{:#?}",
        closed.lines
    );
    // The root is audited on its own, not only reached: a family grant on it that the served
    // tables lack (the child's holder grant copied onto the root) is its own mismatch.
    let copied = sqlx::query(
        "INSERT INTO bigname_phase.project_grant
         SELECT (jsonb_populate_record(NULL::bigname_phase.project_grant,
                    to_jsonb(grant_row) || jsonb_build_object('resource_id', $1::text))).*
         FROM bigname_phase.project_grant grant_row
         WHERE grant_row.resource_id = $2::uuid AND grant_row.subject = $3",
    )
    .bind(&root)
    .bind(&child)
    .bind(HOLDER)
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    assert_eq!(copied, 1);
    let mutated = shadow_support::compare::compare(&fixture.pool, CHAIN, TARGET).await?;
    assert!(
        mutated.known_discrepancy.is_empty() && mutated.expected_delta_fields.is_empty(),
        "{:#?}",
        mutated.lines
    );
    let failed: Vec<&str> = mutated
        .lines
        .iter()
        .filter(|line| line.starts_with("SEPOLIA_END_TO_END_SHADOW_MISMATCH"))
        .filter_map(|line| {
            line.contains(&format!("key={root} "))
                .then(|| line.split(" field=").nth(1)?.split(' ').next())
                .flatten()
        })
        .collect();
    assert_eq!(failed, ["permissions_current"], "{:#?}", mutated.lines);
    assert_eq!(mutated.mismatched, 1, "{:#?}", mutated.lines);
    let removed =
        sqlx::query("DELETE FROM bigname_phase.project_grant WHERE resource_id = $1::uuid")
            .bind(&root)
            .execute(&fixture.pool)
            .await?
            .rows_affected();
    assert_eq!(removed, 1);
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state)
         SELECT $2, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
         FROM chain_lineage WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .bind(OTHER)
    .execute(&fixture.pool)
    .await?;
    let inserted = sqlx::query(
        "INSERT INTO permissions_current
         SELECT (jsonb_populate_record(NULL::permissions_current,
                    to_jsonb(row) || jsonb_build_object('resource_id', $1::text,
                        'provenance', row.provenance || jsonb_build_object('chain_id', $2::text)))).*
         FROM permissions_current row WHERE row.resource_id = $3::uuid AND row.subject = $4",
    )
    .bind(&root)
    .bind(OTHER)
    .bind(&child)
    .bind(HOLDER)
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    assert_eq!(inserted, 1);
    let effective = bigname_storage::load_effective_permissions_by_resource_ids(
        &fixture.pool,
        &[root.parse()?],
        None,
    )
    .await?;
    assert!(
        effective
            .iter()
            .any(|row| row.subject == HOLDER && row.provenance["chain_id"] == json!(OTHER)),
        "the API serves the other chain's row on the root"
    );
    let report = shadow_support::compare::compare(&fixture.pool, CHAIN, TARGET).await?;
    assert!(
        report.known_discrepancy.is_empty() && report.expected_delta_fields.is_empty(),
        "{:#?}",
        report.lines
    );
    let failed: Vec<&str> = report
        .lines
        .iter()
        .filter(|line| line.starts_with("SEPOLIA_END_TO_END_SHADOW_MISMATCH"))
        .filter_map(|line| {
            line.contains(&format!("key={root} "))
                .then(|| line.split(" field=").nth(1)?.split(' ').next())
                .flatten()
        })
        .collect();
    assert_eq!(failed, ["other_chain_rows"], "{:#?}", report.lines);
    assert_eq!(
        (report.resources, report.mismatched),
        (baseline.resources, 1),
        "{:#?}",
        report.lines
    );
    fixture.cleanup().await
}
