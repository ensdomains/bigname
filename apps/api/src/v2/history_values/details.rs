//! Caller and original permission words retained by the event's own log.
use bigname_storage::HistoryEvent;
use serde_json::{Map, Value};

pub(super) fn append(data: &mut Map<String, Value>, row: &HistoryEvent) {
    let after = &row.after_state;
    if matches!(
        row.source_family.as_str(),
        "ens_v2_registry_l1" | "ens_v2_root_l1"
    ) && matches!(
        after["source_event"].as_str(),
        Some("LabelRegistered" | "ResolverUpdated" | "SubregistryUpdated")
    ) && let Some(sender) = super::address(after.get("sender"))
    {
        data.insert("sender".into(), Value::String(sender));
    }
    if after["source_event"] == "EACRolesChanged" {
        super::amount(data, "old_role_bitmap", after.get("old_role_bitmap"));
        super::amount(data, "new_role_bitmap", after.get("role_bitmap"));
    }
}
