//! NameWrapper holder and delegate permission rows: the ERC-1155 holder (and, through Project,
//! every owner-wide operator) carries the `canModifyName` powers; the per-token approved address
//! carries only the `canExtendSubnames` branch.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L238 @ ens_v1@91c966f)

use serde_json::json;

use crate::schema_v2::protocol::permissions::{V1WrapperGrant, v1_wrapper_states};
use crate::schema_v2::protocol::{EventDraft, Interpreted};
use crate::schema_v2::state::V1NameState;

/// Powers of the ERC-1155 holder and of every owner-wide operator: `canModifyName` and the
/// ERC-1155-fuse approve and transfer checks accept both identically, both pass
/// `canExtendSubnames`, and `extendExpiry` lets the name's own controller extend its expiry once
/// `CAN_EXTEND_EXPIRY` is burnt. Project masks `burn_fuses` and `extend_expiry` on the
/// expiry-effective fuse word; the interpreter emits the unmasked set.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L238 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L443-L470 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L37-L47 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L137-L150 @ ens_v1@91c966f)
const WRAPPER_HOLDER_POWERS: &[&str] = &[
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
/// The per-token approved address only passes the `getApproved` branch of `canExtendSubnames`.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L228-L238 @ ens_v1@91c966f)
const WRAPPER_DELEGATE_POWERS: &[&str] = &["extend_subname_expiry"];
const RESOLVER_CONTROL_POWERS: &[&str] = &["resolver_control"];

pub(in crate::schema_v2) struct WrapperPermissionContext<'a> {
    pub name: &'a V1NameState,
    pub resolver: Option<String>,
    pub chain_id: &'a str,
    pub wrapper: String,
    pub source_event_kind: &'a str,
    pub identity_suffix: &'a str,
}

pub(super) fn append_holder_permissions(
    output: &mut Interpreted,
    context: &WrapperPermissionContext<'_>,
    subject: &str,
    grant: bool,
) {
    let mut scopes = vec![(json!({"kind":"resource"}), WRAPPER_HOLDER_POWERS)];
    if let Some(resolver) = &context.resolver {
        scopes.push((
            json!({"kind":"resolver","chain_id":context.chain_id,"resolver_address":resolver}),
            RESOLVER_CONTROL_POWERS,
        ));
    }
    push_wrapper_permissions(output, context, subject, grant, "holder", scopes);
}

pub(in crate::schema_v2) fn append_delegate_permission(
    output: &mut Interpreted,
    context: &WrapperPermissionContext<'_>,
    subject: &str,
    grant: bool,
) {
    let scopes = vec![(json!({"kind":"resource"}), WRAPPER_DELEGATE_POWERS)];
    push_wrapper_permissions(output, context, subject, grant, "token_approval", scopes);
}

fn push_wrapper_permissions(
    output: &mut Interpreted,
    context: &WrapperPermissionContext<'_>,
    subject: &str,
    grant: bool,
    relation_kind: &str,
    scopes: Vec<(serde_json::Value, &[&str])>,
) {
    let Some(authority_key) = context.name.authority_key.as_deref() else {
        return;
    };
    let node = context
        .name
        .logical_name_id
        .rsplit(':')
        .next()
        .unwrap_or_default()
        .to_owned();
    let action = if grant { "grant" } else { "revoke" };
    // Token-approval rows share one retained-state key per name and subject whether they come
    // from an `Approval` log or from the event-less clear on transfer or burn, so a restore that
    // keeps only the newest row per key sees the clear that followed a grant. Holder rows keep
    // the default per-subject key.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L837-L840 @ ens_v1@91c966f)
    let state_scope = if relation_kind == "token_approval" {
        format!("{}:{node}:-:{subject}:token_approval", context.wrapper)
    } else {
        String::new()
    };
    for (index, (scope, powers)) in scopes.into_iter().enumerate() {
        let (before, after) = v1_wrapper_states(
            grant,
            V1WrapperGrant {
                subject,
                scope,
                powers,
                node: &node,
                authority_key,
                authority_contract: &context.wrapper,
                relation_kind,
                source_event_kind: context.source_event_kind,
            },
        );
        output.events.push(EventDraft {
            event_kind: "PermissionChanged".to_owned(),
            logical_name_id: Some(context.name.logical_name_id.clone()),
            resource_id: Some(context.name.resource_id),
            identity_suffix: format!(
                "PermissionChanged:{}:{relation_kind}:{index}:{action}:{subject}",
                context.identity_suffix
            ),
            explicit_before: Some(before),
            after_state: after,
            state_scope: state_scope.clone(),
        });
    }
}
