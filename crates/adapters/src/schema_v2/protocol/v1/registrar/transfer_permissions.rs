use super::*;

pub(super) fn append_transfer_permissions(
    output: &mut Interpreted,
    from: &str,
    after: &V1NameState,
    previous_authority: Option<&V1NameState>,
    current_authority: Option<&V1NameState>,
    resolver: Option<String>,
    chain_id: &str,
) {
    let Some(to) = after.owner.as_deref() else {
        return;
    };
    if from.eq_ignore_ascii_case(to) {
        return;
    }
    for (grant, subject, action) in [(false, from, "revoke"), (true, to, "grant")] {
        push_transfer_permission(
            output,
            after,
            subject,
            json!({"kind":"resource"}),
            grant,
            action,
        );
    }
    let Some(resolver) = resolver else { return };
    let scope = json!({"kind":"resolver","chain_id":chain_id,"resolver_address":resolver});
    match (previous_authority, current_authority) {
        (Some(previous), Some(current)) if previous.resource_id != current.resource_id => {
            for (authority, grant, action) in
                [(previous, false, "revoke"), (current, true, "grant")]
            {
                if let Some(subject) = authority.owner.as_deref() {
                    push_transfer_permission(
                        output,
                        authority,
                        subject,
                        scope.clone(),
                        grant,
                        action,
                    );
                }
            }
        }
        (Some(previous), Some(current))
            if previous.resource_id == after.resource_id
                && current.resource_id == after.resource_id =>
        {
            for (grant, subject, action) in [(false, from, "revoke"), (true, to, "grant")] {
                push_transfer_permission(output, after, subject, scope.clone(), grant, action);
            }
        }
        _ => {}
    }
}

fn push_transfer_permission(
    output: &mut Interpreted,
    authority: &V1NameState,
    subject: &str,
    scope: Value,
    grant: bool,
    action: &str,
) {
    let Some(authority_key) = authority.authority_key.as_deref() else {
        return;
    };
    let power = if scope["kind"] == "resource" {
        "resource_control"
    } else {
        "resolver_control"
    };
    let (before_state, after_state) = if grant {
        v1_grant_states(
            subject,
            scope,
            power,
            authority_kind(authority),
            authority_key,
            "TokenControlTransferred",
        )
    } else {
        v1_revoke_states(
            subject,
            scope,
            power,
            authority_kind(authority),
            authority_key,
            "TokenControlTransferred",
        )
    };
    output.events.push(EventDraft {
        event_kind: "PermissionChanged".to_owned(),
        logical_name_id: Some(authority.logical_name_id.clone()),
        resource_id: Some(authority.resource_id),
        identity_suffix: format!("PermissionChanged:transfer:{power}:{action}:{subject}"),
        explicit_before: Some(before_state),
        after_state,
        state_scope: String::new(),
    });
}
