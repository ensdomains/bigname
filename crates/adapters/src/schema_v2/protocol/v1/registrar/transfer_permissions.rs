use super::*;

pub(super) fn append_transfer_permissions(
    output: &mut Interpreted,
    from: &str,
    after: &V1NameState,
    resolver: Option<String>,
    chain_id: &str,
) {
    let (Some(to), Some(authority_key)) = (after.owner.as_deref(), after.authority_key.as_deref())
    else {
        return;
    };
    if from.eq_ignore_ascii_case(to) {
        return;
    }
    let mut scopes = vec![(json!({"kind":"resource"}), "resource_control")];
    if let Some(resolver) = resolver {
        scopes.push((
            json!({"kind":"resolver","chain_id":chain_id,"resolver_address":resolver}),
            "resolver_control",
        ));
    }
    for (index, (scope, power)) in scopes.into_iter().enumerate() {
        for (grant, subject, action) in [(false, from, "revoke"), (true, to, "grant")] {
            let (before_state, after_state) = if grant {
                v1_grant_states(
                    subject,
                    scope.clone(),
                    power,
                    "registrar",
                    authority_key,
                    "TokenControlTransferred",
                )
            } else {
                v1_revoke_states(
                    subject,
                    scope.clone(),
                    power,
                    "registrar",
                    authority_key,
                    "TokenControlTransferred",
                )
            };
            output.events.push(EventDraft {
                event_kind: "PermissionChanged".to_owned(),
                logical_name_id: Some(after.logical_name_id.clone()),
                resource_id: Some(after.resource_id),
                identity_suffix: format!("PermissionChanged:transfer:{index}:{action}:{subject}"),
                explicit_before: Some(before_state),
                after_state,
                state_scope: String::new(),
            });
        }
    }
}
