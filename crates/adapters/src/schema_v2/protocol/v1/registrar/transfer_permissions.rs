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
        push_permission_change(
            output,
            after,
            subject,
            json!({"kind":"resource"}),
            "resource_control",
            grant,
            "TokenControlTransferred",
            &format!("transfer-resource-{action}"),
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
                    push_permission_change(
                        output,
                        authority,
                        subject,
                        scope.clone(),
                        "resolver_control",
                        grant,
                        "TokenControlTransferred",
                        &format!("transfer-authority-{action}"),
                    );
                }
            }
        }
        (Some(previous), Some(current))
            if previous.resource_id == after.resource_id
                && current.resource_id == after.resource_id =>
        {
            for (grant, subject, action) in [(false, from, "revoke"), (true, to, "grant")] {
                push_permission_change(
                    output,
                    after,
                    subject,
                    scope.clone(),
                    "resolver_control",
                    grant,
                    "TokenControlTransferred",
                    &format!("transfer-resolver-{action}"),
                );
            }
        }
        _ => {}
    }
}
