use serde_json::json;

use super::RegistryObservationContext;
use crate::ens_v2_registry::{
    constants::EVENT_KIND_TOKEN_CONTROL_TRANSFERRED, names::state_for_token_mut,
    normalized::normalized_event, types::ObservationRef,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_token_control_transferred(
    token_id: String,
    operator: String,
    from: String,
    to: String,
    amount: String,
    source_event: &'static str,
    transfer_index: Option<usize>,
    reference: ObservationRef,
    context: &mut RegistryObservationContext<'_>,
) {
    let linked_state = state_for_token_mut(
        context.states_by_registry_token,
        context.token_aliases,
        &reference.emitting_address,
        &token_id,
    )
    .and_then(|state| {
        state
            .resource
            .clone()
            .map(|resource| (state.name.logical_name_id.clone(), resource))
    });
    let identity_suffix = match transfer_index {
        Some(index) => format!("token-control-transferred:{token_id}:batch:{index}"),
        None => format!("token-control-transferred:{token_id}:single"),
    };
    let (logical_name_id, resource_id, upstream_resource, token_lineage_id, pending) =
        match linked_state {
            Some((logical_name_id, resource)) => (
                Some(logical_name_id),
                Some(resource.resource_id),
                Some(resource.upstream_resource),
                Some(resource.token_lineage_id.to_string()),
                false,
            ),
            None => (None, None, None, None, true),
        };
    let mut event = normalized_event(
        &reference,
        logical_name_id,
        resource_id,
        EVENT_KIND_TOKEN_CONTROL_TRANSFERRED,
        json!({"from": from}),
        json!({
            "source_event": source_event,
            "token_id": token_id,
            "to": to,
            "operator": operator,
            "amount": amount,
            "transfer_index": transfer_index,
            "upstream_resource": upstream_resource,
            "token_lineage_id": token_lineage_id,
            "registry_contract_instance_id": reference.emitting_contract_instance_id.to_string(),
            "registry_hydration_pending": pending,
        }),
        identity_suffix,
    );
    if !pending {
        event
            .after_state
            .as_object_mut()
            .expect("normalized transfer after_state is an object")
            .remove("registry_hydration_pending");
    }
    context.graph_events.push(event);
}
