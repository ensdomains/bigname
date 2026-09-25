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
        PARENT_CANNOT_CONTROL, WRAPPER, approval, name, node, permission_changed, timestamp,
        wrapped, wrapper_event,
    },
};
use support::{CHAIN, Fixture, uuid};
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
