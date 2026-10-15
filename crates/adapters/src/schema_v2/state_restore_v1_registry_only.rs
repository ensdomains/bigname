use crate::schema_v2::{model::PriorEventInput, state::State};

/// Records a permission row on a registry-only resource. The restore sets each resource's holder
/// from the newest row of every subject and scope once it has read them all
/// (`finish_v1_registry_only_restore`).
pub(super) fn restore(state: &mut State, event: &PriorEventInput) {
    if event.event_kind != "PermissionChanged" {
        return;
    }
    let Some(resource_id) = event.resource_id else {
        return;
    };
    let after = &event.after_state;
    let granted = after["effective_powers"]
        .as_array()
        .is_some_and(|powers| !powers.is_empty());
    let source = &after[if granted {
        "grant_source"
    } else {
        "revocation_source"
    }];
    if source["kind"] != "ens_v1_authority" || source["authority_kind"] != "registry_only" {
        return;
    }
    let Some(subject) = after["subject"].as_str() else {
        return;
    };
    let resolver = match after["scope"]["kind"].as_str() {
        Some("resource") => None,
        Some("resolver") => match after["scope"]["resolver_address"].as_str() {
            Some(resolver) => Some(resolver),
            None => return,
        },
        _ => return,
    };
    state.restore_v1_registry_only_row(resource_id, subject, resolver, granted);
}
