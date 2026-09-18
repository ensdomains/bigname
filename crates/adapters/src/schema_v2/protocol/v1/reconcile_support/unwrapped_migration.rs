use std::collections::BTreeSet;

use super::super::refresh_interpreter_state_key;
use crate::schema_v2::{
    migration::UnwrappedReconciliation,
    model::{BatchOutput, NormalizedEvent},
};

/// The subject and scope a permission row is about.
fn permission_key(event: &NormalizedEvent) -> (String, String) {
    (
        event.after_state["subject"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase(),
        event.after_state["scope"].to_string(),
    )
}

pub(in crate::schema_v2::protocol) fn reconcile(
    output: &mut BatchOutput,
    proofs: &[UnwrappedReconciliation],
) {
    for proof in proofs {
        // Grants on the registry-only resource inside the proven transaction come from the
        // temporary authorities the controller's reclaim and cleanup close one log later; they are
        // removed below, and so are the revocations that close them.
        let transient_registry_grants = output
            .normalized_events
            .iter()
            .filter(|event| {
                proof.contains(event)
                    && event.event_kind == "PermissionChanged"
                    && event.resource_id == Some(proof.registry_resource_id)
                    && !event.after_state["grant_source"].is_null()
            })
            .map(permission_key)
            .collect::<BTreeSet<_>>();
        output.normalized_events.retain_mut(|event| {
            if !proof.contains(event) {
                return true;
            }
            if matches!(
                event.event_kind.as_str(),
                "SurfaceBound" | "SurfaceUnbound" | "AuthorityEpochChanged"
            ) || (event.event_kind == "ResolverChanged"
                && (event.after_state["source_event"] == "AuthorityEpochChanged"
                    || event.after_state["surface_materialization"] == true))
            {
                return false;
            }
            if event.event_kind == "PermissionChanged" {
                // Keep actual registrar-token transfers and predecessor revocations for audit.
                // Grants derived from a temporary registry authority are not durable permissions.
                // Registrar-token transfers are kept on the lease only: a transfer-sourced grant on
                // the registry-only resource inside this transaction is the temporary authority the
                // controller's reclaim closes one log later.
                let registrar_transfer = event.resource_id == Some(proof.resource_id)
                    && event.source_family == "ens_v1_registrar_l1"
                    && event.after_state["scope"]["kind"] == "resource"
                    && ["grant_source", "revocation_source"].iter().any(|field| {
                        event.after_state[field]["source_event_kind"] == "TokenControlTransferred"
                    });
                // A revocation stays on the resource whose grant it closes. After a registrar
                // transfer without `reclaim` the name is bound to the registry-only resource and
                // the registry owner holds resource and resolver control there from before this
                // transaction; the controller's reclaim revokes those grants on that resource, so
                // the revocations are durable permission history rather than transient authority.
                // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
                // (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L111 @ ens_v2@a971bd64)
                let revoked = event
                    .after_state
                    .get("revocation_source")
                    .is_some_and(|value| !value.is_null());
                let predecessor_revocation = revoked
                    && (event.resource_id == Some(proof.resource_id)
                        || (event.resource_id == Some(proof.registry_resource_id)
                            && !transient_registry_grants.contains(&permission_key(event))));
                return registrar_transfer || predecessor_revocation;
            }
            if event.source_family == "ens_v1_registry_l1" {
                event.logical_name_id = Some(proof.logical_name_id.clone());
                event.resource_id = Some(proof.resource_id);
                if let Some(state) = event.after_state.as_object_mut() {
                    state.remove("authority_kind");
                    state.remove("authority_key");
                }
                refresh_interpreter_state_key(event);
            }
            true
        });
        // Suppress every intervening V1 opening, including the registrar reopening at cleanup.
        // The unchanged writer closes the original registrar interval at its strict cleanup time.
        output.surface_bindings.retain(|binding| {
            !(binding.chain_id == proof.chain_id
                && binding.block_hash == proof.block_hash
                && binding.logical_name_id == proof.logical_name_id
                && binding.authority_arm == "ens_v1"
                && (binding.resource_id == proof.resource_id
                    || binding.resource_id == proof.registry_resource_id)
                && binding.provenance["transaction_index"].as_i64()
                    == Some(proof.transaction_index)
                && binding.provenance["log_index"]
                    .as_i64()
                    .is_some_and(|log| proof.first_log <= log && log <= proof.cleanup_log))
        });
        output.binding_closures.retain(|closure| {
            !(closure.chain_id == proof.chain_id
                && closure.block_number == proof.block_number
                && closure.logical_name_id == proof.logical_name_id
                && closure.authority_arm == "ens_v1"
                && closure.transaction_index == proof.transaction_index
                && proof.first_log <= closure.log_index
                && closure.log_index <= proof.cleanup_log)
        });
    }
}
