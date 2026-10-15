use super::super::registry::registry_owner_grants::{self, Rows};
use super::*;

/// The permission rows of a registrar token transfer. When the transfer changes the selected
/// authority between the node's registry-only resource and the lease, the resource's holder
/// changes with it: the lease holds the grants while it is selected, and the registry owner holds
/// them again when the registry-only resource is.
pub(super) fn append_transfer_permissions(
    output: &mut Interpreted,
    state: &mut State,
    node: (&str, &str),
    raw: &RawLogInput,
    from: &str,
    after: &V1NameState,
    (previous_authority, current_authority): (Option<&V1NameState>, Option<&V1NameState>),
) {
    let Some(to) = after.owner.as_deref() else {
        return;
    };
    let resolver = state.v1_resolver(node.0, node.1);
    let chain_id = raw.chain_id.as_str();
    let changed = previous_authority.map(|authority| authority.resource_id)
        != current_authority.map(|authority| authority.resource_id);
    if from.eq_ignore_ascii_case(to) && !changed {
        return;
    }
    let registry_only = registry_owner_grants::resource_id(chain_id, node.1);
    let is_registry_only = |authority: &&V1NameState| authority.resource_id == registry_only;
    let previous_registry_only = previous_authority.filter(is_registry_only);
    let current_registry_only = current_authority.filter(is_registry_only);
    let handoff = (changed
        && (previous_registry_only.is_some() || current_registry_only.is_some()))
    .then(|| {
        let target = current_registry_only
            .and_then(|authority| registry_owner_grants::selected_holder(state, node, authority))
            .map(str::to_owned);
        registry_owner_grants::hand_off(
            state,
            registry_only,
            target.as_deref(),
            resolver.as_deref(),
            None,
        )
    });
    let rows = Rows {
        emitter: &raw.emitting_address,
        chain_id,
        source_event_kind: "TokenControlTransferred",
        resource: [
            "transfer-authority-resource-revoke",
            "transfer-authority-resource-grant",
        ],
        resolver: ["transfer-authority-revoke", "transfer-authority-grant"],
        boundary: false,
    };
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
    if changed {
        for (authority, grant, action) in [
            (previous_authority, false, "revoke"),
            (current_authority, true, "grant"),
        ] {
            let Some(authority) =
                authority.filter(|authority| authority.resource_id != after.resource_id)
            else {
                continue;
            };
            if authority.resource_id == registry_only {
                if let Some(handoff) = handoff.as_ref() {
                    if grant {
                        handoff.push_grant_resource(&mut output.events, authority, rows);
                    } else {
                        handoff.push_revoke_resource(&mut output.events, authority, rows);
                    }
                }
            } else if let Some(subject) = authority.owner.as_deref() {
                push_permission_change(
                    output,
                    authority,
                    subject,
                    json!({"kind":"resource"}),
                    "resource_control",
                    grant,
                    "TokenControlTransferred",
                    &format!("transfer-authority-resource-{action}"),
                );
            }
        }
    }
    // The registry-only side's `resolver_control` follows its holder, whose resolver is the
    // node's resolver when the handoff set it.
    let resolver_change = |output: &mut Interpreted, authority: &V1NameState, grant: bool| {
        if authority.resource_id == registry_only {
            if let Some(handoff) = handoff.as_ref() {
                if grant {
                    handoff.push_grant_resolver(&mut output.events, authority, rows);
                } else {
                    handoff.push_revoke_resolver(&mut output.events, authority, rows);
                }
            }
            return;
        }
        let (Some(subject), Some(resolver)) = (authority.owner.as_deref(), resolver.as_deref())
        else {
            return;
        };
        push_permission_change(
            output,
            authority,
            subject,
            json!({"kind":"resolver","chain_id":chain_id,"resolver_address":resolver}),
            "resolver_control",
            grant,
            "TokenControlTransferred",
            if grant {
                "transfer-authority-grant"
            } else {
                "transfer-authority-revoke"
            },
        );
    };
    match (previous_authority, current_authority) {
        (Some(previous), Some(current)) if changed => {
            resolver_change(output, previous, false);
            resolver_change(output, current, true);
        }
        (Some(previous), Some(current))
            if previous.resource_id == after.resource_id
                && current.resource_id == after.resource_id =>
        {
            let Some(resolver) = resolver.as_deref() else {
                return;
            };
            for (grant, subject, action) in [(false, from, "revoke"), (true, to, "grant")] {
                push_permission_change(
                    output,
                    after,
                    subject,
                    json!({"kind":"resolver","chain_id":chain_id,"resolver_address":resolver}),
                    "resolver_control",
                    grant,
                    "TokenControlTransferred",
                    &format!("transfer-resolver-{action}"),
                );
            }
        }
        (None, Some(current)) => resolver_change(output, current, true),
        // A registry-only resource that stops being selected still ends its holder's
        // `resolver_control`.
        (Some(previous), None) if previous.resource_id == registry_only => {
            resolver_change(output, previous, false);
        }
        _ => {}
    }
}
