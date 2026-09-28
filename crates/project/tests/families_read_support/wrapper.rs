//! A wrapped ENSv1 name on the families_support fixture, in the shapes the ENSv1 wrapper adapter
//! emits (crates/project/tests/wrapper_permissions.rs carries the same rows for the production
//! builders alone).
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L238 @ ens_v1@91c966f)
use anyhow::Result;
use serde_json::{Value, json};

use crate::support::{CHAIN, Event, Fixture, hash, uuid};

pub const WRAPPER: &str = "0x00000000000000000000000000000000000000e4";
pub const WRAPPER_INSTANCE: &str = "00000000-0000-0000-0000-0000000000e4";
pub const HOLDER: &str = "0x00000000000000000000000000000000000000a1";
pub const OPERATOR: &str = "0x00000000000000000000000000000000000000e1";
pub const DELEGATE: &str = "0x00000000000000000000000000000000000000d1";
pub const V1_WRAPPER: &str = "ens_v1_wrapper_l1";
pub const PARENT_CANNOT_CONTROL: i64 = 1 << 16;
pub const IS_DOT_ETH: i64 = 1 << 17;
pub const CANNOT_UNWRAP: i64 = 1;
pub const GRACE_PERIOD: i64 = 7_776_000;
pub const HOLDER_POWERS: &[&str] = &[
    "resource_control",
    "set_resolver",
    "set_ttl",
    "create_subnames",
    "transfer",
    "unwrap",
    "burn_fuses",
    "approve",
    "extend_subname_expiry",
    "extend_expiry",
];

pub fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

pub fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// The families_support block clock.
pub fn timestamp(block: i64) -> i64 {
    1_800_000_000 + block * 12
}

fn authority_key() -> String {
    format!("wrapper:{CHAIN}:1:{}:{}:0", node(1), hash(10))
}

fn wrapper_state(fuses: i64) -> &'static str {
    if fuses & CANNOT_UNWRAP != 0 {
        "locked"
    } else if fuses & PARENT_CANNOT_CONTROL == 0 {
        "wrapped"
    } else {
        "emancipated"
    }
}

/// A NameWrapper event of name 1 on `resource`.
#[allow(clippy::too_many_arguments)]
pub async fn wrapper_event(
    fixture: &Fixture,
    block: i64,
    log: i64,
    kind: &str,
    resource: &str,
    before: Value,
    after: Value,
) -> Result<()> {
    let identity = format!("{kind}:{block}:{log}");
    fixture
        .event(
            Event::new(&identity, block, log, kind, V1_WRAPPER)
                .name(&name(1))
                .resource(resource)
                .before(before)
                .after(after)
                .raw(json!({"emitting_address": WRAPPER})),
        )
        .await?;
    Ok(())
}

/// A resource-scoped wrapper permission state: granted, or revoked with its source.
pub fn permission(
    subject: &str,
    relation: &str,
    powers: &[&str],
    source_event: &str,
    grant: bool,
) -> Value {
    let source = json!({
        "kind": "ens_v1_authority", "authority_kind": "wrapper", "authority_key": authority_key(),
        "authority_contract": WRAPPER, "relation_kind": relation, "node": node(1),
        "source_event_kind": source_event,
    });
    let transfer_behavior = if relation == "token_approval" {
        "cleared_on_transfer_unless_cannot_approve"
    } else {
        "replace_on_authority_change"
    };
    json!({
        "subject": subject,
        "scope": {"kind": "resource"},
        "effective_powers": if grant { json!(powers) } else { json!([]) },
        "grant_source": if grant { source.clone() } else { Value::Null },
        "revocation_source": if grant { Value::Null } else { source },
        "inheritance_path": [],
        "transfer_behavior": transfer_behavior,
    })
}

/// A PermissionChanged granting (or revoking) `powers` to `subject` on `resource`.
#[allow(clippy::too_many_arguments)]
pub async fn permission_changed(
    fixture: &Fixture,
    block: i64,
    log: i64,
    resource: &str,
    subject: &str,
    relation: &str,
    powers: &[&str],
    source_event: &str,
    grant: bool,
) -> Result<()> {
    wrapper_event(
        fixture,
        block,
        log,
        "PermissionChanged",
        resource,
        permission(subject, relation, powers, source_event, !grant),
        permission(subject, relation, powers, source_event, grant),
    )
    .await
}

/// A NameWrapper ApprovalForAll from `owner` to `operator`.
pub async fn approval(
    fixture: &Fixture,
    block: i64,
    log: i64,
    owner: &str,
    operator: &str,
    approved: bool,
) -> Result<()> {
    let source = json!({"kind": "raw_log", "source_event": "ApprovalForAll"});
    let identity = format!("approval:{block}:{log}:{owner}:{operator}");
    fixture
        .event(
            Event::new(
                &identity,
                block,
                log,
                "AccountPermissionChanged",
                V1_WRAPPER,
            )
            .after(json!({
                "subject": operator, "relation_kind": "operator", "approved": approved,
                "scope": {"kind": "account", "chain_id": CHAIN, "authority_kind": "wrapper",
                          "authority_contract": WRAPPER,
                          "authority_contract_instance_id": WRAPPER_INSTANCE,
                          "owner": owner},
                "effective_powers": if approved { json!(["wrapper_control"]) } else { json!([]) },
                "grant_source": if approved { source.clone() } else { json!({}) },
                "revocation_source": if approved { Value::Null } else { source },
                "inheritance_path": [],
                "transfer_behavior": {"mode": "owner_scoped",
                                      "on_holder_change": "ceases_to_apply"},
                "source_event": "ApprovalForAll",
            }))
            .raw(json!({"emitting_address": WRAPPER})),
        )
        .await?;
    Ok(())
}

/// Name 1 bound at 9 and wrapped at block 10 on resource 1 with `fuses` and `expiry`, held by
/// HOLDER, with OPERATOR approved for HOLDER's tokens at 11. Returns the resource.
pub async fn wrapped(fixture: &Fixture, fuses: i64, expiry: i64) -> Result<String> {
    let resource = uuid(1);
    fixture
        .binding(&uuid(100), &name(1), &resource, "ens_v1", 9, 0, None)
        .await?;
    wrapper_event(
        fixture,
        9,
        0,
        "SurfaceBound",
        &resource,
        json!({}),
        json!({"authority_kind": "wrapper", "node": node(1), "state_derived": false}),
    )
    .await?;
    let authority = |mut after: Value| {
        after["authority_kind"] = json!("wrapper");
        after["authority_key"] = json!(authority_key());
        after
    };
    wrapper_event(
        fixture,
        10,
        1,
        "ExpiryChanged",
        &resource,
        json!({}),
        authority(json!({"source_event": "NameWrapped", "node": node(1), "expiry": expiry})),
    )
    .await?;
    wrapper_event(
        fixture,
        10,
        2,
        "TokenControlTransferred",
        &resource,
        json!({"from": null}),
        authority(
            json!({"source_event": "NameWrapped", "node": node(1), "owner": HOLDER,
                         "to": HOLDER, "fuses": fuses}),
        ),
    )
    .await?;
    wrapper_event(
        fixture,
        10,
        3,
        "PermissionScopeChanged",
        &resource,
        json!({}),
        json!({"source_event": "NameWrapped", "node": node(1), "fuses": fuses,
               "wrapper_state": wrapper_state(fuses), "expiry": expiry}),
    )
    .await?;
    permission_changed(
        fixture,
        10,
        4,
        &resource,
        HOLDER,
        "holder",
        HOLDER_POWERS,
        "NameWrapped",
        true,
    )
    .await?;
    approval(fixture, 11, 0, HOLDER, OPERATOR, true).await?;
    Ok(resource)
}
