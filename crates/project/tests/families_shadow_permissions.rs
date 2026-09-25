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
        DELEGATE, HOLDER, HOLDER_POWERS, OPERATOR, PARENT_CANNOT_CONTROL, approval, name,
        permission_changed, timestamp, wrapped,
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
