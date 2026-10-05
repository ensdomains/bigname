use super::authority_kind;
use crate::schema_v2::{
    model::RawLogInput,
    protocol::{
        EventDraft, Interpreted,
        permissions::{v1_grant_states, v1_revoke_states},
    },
    state::V1NameState,
};
use serde_json::{Value, json};

#[allow(clippy::too_many_arguments)]
pub(in crate::schema_v2::protocol::v1) fn push_permission_change(
    output: &mut Interpreted,
    authority: &V1NameState,
    subject: &str,
    scope: Value,
    power: &str,
    grant: bool,
    source_event_kind: &str,
    suffix: &str,
) {
    let Some(authority_key) = authority.authority_key.as_deref() else {
        return;
    };
    let (before, after) = if grant {
        v1_grant_states(
            subject,
            scope,
            power,
            authority_kind(authority),
            authority_key,
            source_event_kind,
        )
    } else {
        v1_revoke_states(
            subject,
            scope,
            power,
            authority_kind(authority),
            authority_key,
            source_event_kind,
        )
    };
    output.events.push(EventDraft {
        event_kind: "PermissionChanged".to_owned(),
        logical_name_id: authority
            .surface_known
            .then(|| authority.logical_name_id.clone()),
        resource_id: Some(authority.resource_id),
        identity_suffix: format!("PermissionChanged:{suffix}:{subject}"),
        explicit_before: Some(before),
        after_state: after,
        state_scope: String::new(),
    });
}

pub(super) fn append_authority_permissions(
    output: &mut Interpreted,
    previous: Option<&V1NameState>,
    current: Option<&V1NameState>,
    resolver: Option<String>,
    raw: &RawLogInput,
) {
    if let Some(previous) = previous
        && let Some(subject) = previous.owner.as_deref()
    {
        push_permission_change(
            output,
            previous,
            subject,
            json!({"kind":"resource"}),
            "resource_control",
            false,
            "AuthorityTransferred",
            "resource-revoke",
        );
        if let Some(resolver) = resolver.as_deref() {
            push_permission_change(
                output,
                previous,
                subject,
                json!({"kind":"resolver","chain_id":raw.chain_id,"resolver_address":resolver}),
                "resolver_control",
                false,
                "AuthorityTransferred",
                "resolver-revoke",
            );
        }
    }
    if let Some(current) = current
        && let Some(subject) = current.owner.as_deref()
    {
        push_permission_change(
            output,
            current,
            subject,
            json!({"kind":"resource"}),
            "resource_control",
            true,
            "AuthorityTransferred",
            "resource-grant",
        );
        if let Some(resolver) = resolver {
            push_permission_change(
                output,
                current,
                subject,
                json!({"kind":"resolver","chain_id":raw.chain_id,"resolver_address":resolver}),
                "resolver_control",
                true,
                "AuthorityTransferred",
                "resolver-grant",
            );
        }
    }
}
