use serde_json::{Value, json};

use super::merge;
use crate::schema_v2::{
    protocol::{EventDraft, Interpreted},
    state::{V2NameTransition, V2TokenState},
};

pub(super) fn resource_revival(
    before: &V2TokenState,
    after: &V2TokenState,
    transitions: &[V2NameTransition],
    token_id: &str,
    revived: bool,
    event_state: Value,
) -> Option<EventDraft> {
    (after.resource_id.is_some()
        && after.registration.is_none()
        && before.resource_expiry_retirement_emitted
        && revived
        && transitions
            .iter()
            .all(|transition| transition.token_id != token_id))
    .then(|| EventDraft {
        event_kind: "RegistrationRenewed".to_owned(),
        logical_name_id: None,
        resource_id: after.resource_id,
        identity_suffix: format!("RegistrationRenewed:detached:{token_id}"),
        explicit_before: Some(json!({"expiry":before.expiry})),
        after_state: merge(
            event_state,
            json!({"status":"reserved","reservation_resource":true}),
        ),
        state_scope: String::new(),
    })
}

pub(super) fn append_resource_expiration(
    output: &mut Interpreted,
    transition: &V2NameTransition,
    released_at: i64,
) -> anyhow::Result<()> {
    let expiry = transition
        .expiry
        .ok_or_else(|| anyhow::anyhow!("ENSv2 resource expiry has no retained expiry"))?;
    let registry = transition.registry.to_ascii_lowercase();
    let registrant = transition
        .registration
        .as_ref()
        .and_then(|registration| {
            registration
                .get("registrant")
                .or_else(|| registration.get("owner"))
        })
        .cloned()
        .unwrap_or(Value::Null);
    output.events.push(EventDraft {
        event_kind: "RegistrationReleased".to_owned(), logical_name_id: None,
        resource_id: transition.resource_id,
        identity_suffix: format!("RegistrationReleased:expiry:{registry}:{}", transition.token_id),
        explicit_before: Some(json!({"status":if transition.registration.is_some() {"registered"} else {"reserved"},"expiry":expiry,"registrant":registrant})),
        after_state: json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","registry":registry,"token_id":transition.token_id,"registry_contract_instance_id":transition.registry_contract_instance_id.map(|id| id.to_string()),"expiry":expiry,"status":"released","released_at":released_at}),
        state_scope: transition_scope(transition),
    });
    append_expired_pointers(output, transition, expiry);
    Ok(())
}

fn append_expired_pointers(output: &mut Interpreted, transition: &V2NameTransition, expiry: u64) {
    let registry = transition.registry.to_ascii_lowercase();
    let instance = transition
        .registry_contract_instance_id
        .map(|id| id.to_string());
    for (event_kind, field, prior) in [
        ("ResolverChanged", "resolver", transition.resolver.as_ref()),
        (
            "SubregistryChanged",
            "subregistry",
            transition.subregistry.as_ref(),
        ),
    ] {
        let Some(prior) = prior else { continue };
        output.events.push(EventDraft {
            event_kind: event_kind.to_owned(), logical_name_id: None,
            resource_id: transition.resource_id,
            identity_suffix: format!("{event_kind}:expiry:{registry}:{}", transition.token_id),
            explicit_before: Some(json!({(field):prior})),
            after_state: json!({"source_event":"RegistryPathExpired","derived_from":"interpreter_state","terminal_reason":"registry_name_binding_expired","registry":registry,"token_id":transition.token_id,"registry_contract_instance_id":instance,"expiry":expiry,(field):Value::Null}),
            state_scope: transition_scope(transition),
        });
    }
}

fn transition_scope(transition: &V2NameTransition) -> String {
    format!(
        "{}:-:{}:-:RegistryPathExpired",
        transition.registry.to_ascii_lowercase(),
        transition.token_id
    )
}

/// An `ExpiryUpdated` for an entry with no token state. `renew` lets a root renewer revive an
/// entry `unregister` left without a token, and names the entry's current token id. No name or
/// resource is bound, so the log is kept as the registry's own expiry fact after `output` and
/// moves no name state.
/// (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L227-L258 @ ens_v2_sepolia_20261001@07e55a05)
pub(super) fn tokenless(
    selected: &crate::schema_v2::catalog::Selected,
    mut output: super::Interpreted,
    event: &super::ExpiryUpdated,
) -> anyhow::Result<super::Interpreted> {
    super::ensure_declared(selected, &["ExpiryChanged"])?;
    output.append(&mut super::single_event(
        "ExpiryChanged",
        None,
        None,
        serde_json::json!({
            "source_event": "ExpiryUpdated",
            "token_id": crate::evm_abi::u256_word_hex(event.tokenId),
            "expiry": event.newExpiry,
            "sender": crate::evm_abi::address_hex(event.sender),
            "token_state_absent": true,
            "registry_contract_instance_id": selected.contract_instance_id.to_string(),
        }),
    ));
    Ok(output)
}
