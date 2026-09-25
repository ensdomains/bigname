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
use bigname_storage::families::control::{
    compare::Difference, lifecycle::Clock, permissions::ResourceInput,
};
use serde_json::{Value, json};
use shadow_support::{
    assert_counts,
    compare::{Excuse, resource_excuses},
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

/// Item 1 of the TYR-36 step 3 review (Q7): a grant and the interpreter's unnamed path-expiry
/// release of one resource in one block, the grant written first. D12 puts the release, which
/// has no transaction or log, before the grant, so the registration is live and the resource
/// serves its permission row; today's (block, generated id) order puts the release last, so the
/// drop rule (permissions.rs:111-133, :391-398) serves nothing. The difference passes as a
/// same-block delta only because the whole permission read in today's order is the served empty
/// set and the whole canonical read is the shadow row; any other shadow or served value fails.
#[tokio::test]
async fn a_same_block_path_expiry_before_a_grant_passes_only_with_both_values() -> Result<()> {
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
    assert_counts(
        &report,
        &[],
        &[("d12_same_block_order:permissions_current", 1)],
    );
    assert_eq!(served_rows(&fixture, &k1).await?, Vec::<Value>::new());

    let input = resource_input(&fixture, &k1).await?;
    let row = json!({
        "resource_id": k1, "subject": BOB, "scope": "registry",
        "scope_kind": "registry",
        "scope_detail": {"kind": "registry", "chain_id": CHAIN, "registry_address": REGISTRY},
        "effective_powers": ["set_resolver", "set_subregistry"],
        "grant_source": {"kind": "raw_log", "source_event": "EACRolesChanged"},
        "revocation_source": null, "inheritance_path": [], "transfer_behavior": {},
    });
    let passes = |served: Value, shadow: Value| {
        let (fixture, input) = (&fixture, &input);
        async move { excuse(fixture, 16, input, "permissions_current", served, shadow).await }
    };
    assert_eq!(
        passes(json!([]), json!([row.clone()])).await?,
        Excuse::SameBlockOrder,
        "the served empty set and the canonical row"
    );
    let mut wrong_powers = row.clone();
    wrong_powers["effective_powers"] = json!(["set_resolver"]);
    let mut wrong_subject = row.clone();
    wrong_subject["subject"] = json!(ALICE);
    let mut collision = row.clone();
    collision["grant_source"] = json!({"kind": "raw_log", "relation_kind": "operator"});
    collision["transfer_behavior"] =
        json!({"mode": "owner_scoped", "on_holder_change": "ceases_to_apply"});
    for (case, served, shadow) in [
        ("wrong powers", json!([]), json!([wrong_powers])),
        ("wrong subject", json!([]), json!([wrong_subject])),
        (
            "collision output",
            json!([]),
            json!([row.clone(), collision]),
        ),
        (
            "served not empty",
            json!([row.clone()]),
            json!([row.clone()]),
        ),
        ("empty shadow", json!([]), json!([])),
    ] {
        assert_eq!(
            passes(served, shadow).await?,
            Excuse::None,
            "{case} must fail"
        );
    }
    let restriction = json!({"kind": "ens_v2_registry", "locked_roles": ["unregister"]});
    assert_eq!(
        excuse(
            &fixture,
            16,
            &input,
            "resource_restrictions",
            Value::Null,
            restriction
        )
        .await?,
        Excuse::None,
        "a wrong restriction must fail"
    );
    fixture.cleanup().await
}
