use bigname_adapters::schema_v2::{BatchOutput, MigrationAuthorityTransition, seam};
use sqlx::{Postgres, Transaction, types::Uuid};

use crate::{InterpretError, Result};

pub(super) fn validate_boundaries(output: &BatchOutput) -> Result<()> {
    for transition in &output.migration_authority_transitions {
        let matching = output
            .normalized_events
            .iter()
            .filter(|event| {
                event.event_identity == transition.boundary_event_identity
                    && event.event_kind == seam::MIGRATION_APPLIED_EVENT_KIND
                    && event.consumer_visibility == "activated"
                    && event.migration_correlation_ids
                        == [transition.migration_correlation_id.clone()]
                    && event.logical_name_id.as_deref() == Some(transition.logical_name_id.as_str())
                    && event.chain_id == transition.chain_id
                    && event.block_number == Some(transition.block_number)
                    && event.transaction_index == Some(transition.transaction_index)
                    && event.log_index == Some(transition.log_index)
                    && matches!(
                        event.canonicality_state.as_str(),
                        "canonical" | "safe" | "finalized"
                    )
                    && event.after_state["predecessor_binding"] == transition.predecessor_selector
                    && event.after_state["successor_binding"]["binding_id"]
                        == transition.successor_surface_binding_id.to_string()
                    && event.after_state["successor_binding"]["resource_id"]
                        == transition.successor_resource_id.to_string()
                    && event.after_state["successor_binding"]["authority_epoch"]
                        == transition.successor_arm
            })
            .count();
        if matching != 1 {
            return Err(InterpretError::data_integrity(format!(
                "migration authority transition {} has {matching} exact activated MigrationApplied boundaries; expected one",
                transition.boundary_event_identity
            )));
        }
    }
    Ok(())
}

pub(super) async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    transitions: &[MigrationAuthorityTransition],
) -> Result<()> {
    for transition in transitions {
        let selector = validate(transition)?;
        let boundary_time: Option<time::OffsetDateTime> = sqlx::query_scalar(&format!(
            "SELECT lineage.block_timestamp + $8 * interval '1 microsecond'
             FROM surface_bindings binding
             JOIN chain_lineage lineage
               ON lineage.chain_id = binding.chain_id
              AND lineage.block_hash = binding.block_hash
              AND lineage.block_number = binding.block_number
             WHERE binding.surface_binding_id = $1
               AND binding.logical_name_id = $2
               AND binding.resource_id = $3
               AND binding.authority_arm = $4
               AND binding.chain_id = $5
               AND binding.block_number = $6
               AND COALESCE((binding.provenance ->> '{}')::bigint, -1) = $7
               AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
             FOR UPDATE OF binding",
            seam::TRANSACTION_INDEX_KEY,
        ))
        .bind(transition.successor_surface_binding_id)
        .bind(&transition.logical_name_id)
        .bind(transition.successor_resource_id)
        .bind(&transition.successor_arm)
        .bind(&transition.chain_id)
        .bind(transition.block_number)
        .bind(transition.transaction_index)
        .bind(transition.log_index)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to lock migration successor binding", error)
        })?;
        let boundary_time = boundary_time.ok_or_else(|| {
            InterpretError::data_integrity(format!(
                "activated migration boundary {} has no exact ENSv2 successor binding",
                transition.boundary_event_identity
            ))
        })?;

        let predecessors: Vec<Uuid> = sqlx::query_scalar(&format!(
            "SELECT surface_binding_id
             FROM surface_bindings
             WHERE chain_id = $1
               AND logical_name_id = $2
               AND authority_arm = $3
               AND canonicality_state IN ('canonical', 'safe', 'finalized')
               AND (
                   block_number < $4
                   OR (
                       block_number = $4
                       AND (
                           COALESCE((provenance ->> '{}')::bigint, -1),
                           COALESCE((provenance ->> '{}')::bigint, -1)
                       ) < ($5, $6)
                   )
               )
               AND active_from < $7
               AND (active_to IS NULL OR active_to >= $7)
               AND EXISTS (
                   SELECT 1
                   FROM normalized_events evidence
                   WHERE evidence.chain_id = surface_bindings.chain_id
                     AND evidence.resource_id = surface_bindings.resource_id
                     AND evidence.canonicality_state IN ('canonical', 'safe', 'finalized')
                     AND (
                         (
                             $8 = 'registrar_backed_registration'
                             AND evidence.after_state ->> 'token_id' = $9
                             AND EXISTS (
                                 SELECT 1
                                 FROM contract_instance_addresses address
                                 WHERE address.chain_id = evidence.chain_id
                                   AND address.contract_instance_id = $10
                                   AND lower(address.address) = lower(
                                       evidence.raw_fact_ref ->> 'emitting_address'
                                   )
                                   AND (
                                       address.active_from_block_number IS NULL
                                       OR address.active_from_block_number <= evidence.block_number
                                   )
                                   AND (
                                       address.active_to_block_number IS NULL
                                       OR address.active_to_block_number >= evidence.block_number
                                   )
                             )
                         )
                         OR (
                             $8 = 'wrapper_backed_control'
                             AND evidence.after_state ->> 'authority_kind' = 'wrapper'
                             AND evidence.after_state ->> 'node' = $9
                             AND lower(evidence.raw_fact_ref ->> 'emitting_address') = lower($11)
                         )
                     )
               )
             ORDER BY surface_binding_id
             FOR UPDATE",
            seam::TRANSACTION_INDEX_KEY,
            seam::LOG_INDEX_KEY,
        ))
        .bind(&transition.chain_id)
        .bind(&transition.logical_name_id)
        .bind(&transition.expected_predecessor_arm)
        .bind(transition.block_number)
        .bind(transition.transaction_index)
        .bind(transition.log_index)
        .bind(boundary_time)
        .bind(&selector.anchor_kind)
        .bind(&selector.identity)
        .bind(selector.contract_instance_id)
        .bind(&selector.contract_address)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to lock migration predecessor binding", error)
        })?;
        if predecessors.len() != 1 {
            return Err(InterpretError::data_integrity(format!(
                "activated .eth 2LD migration boundary {} has {} active ENSv1 predecessors matching its resource selector; expected exactly one",
                transition.boundary_event_identity,
                predecessors.len()
            )));
        }
        sqlx::query(
            "UPDATE surface_bindings
             SET active_to = $2, observed_at = now()
             WHERE surface_binding_id = $1",
        )
        .bind(predecessors[0])
        .bind(boundary_time)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to apply migration authority transition", error)
        })?;
    }
    Ok(())
}

struct PredecessorSelector {
    anchor_kind: String,
    identity: String,
    contract_instance_id: Option<Uuid>,
    contract_address: Option<String>,
}

fn validate(transition: &MigrationAuthorityTransition) -> Result<PredecessorSelector> {
    let selection = transition
        .predecessor_selector
        .get("selection")
        .and_then(serde_json::Value::as_str);
    let selector_name = transition
        .predecessor_selector
        .get("logical_name_id")
        .and_then(serde_json::Value::as_str);
    let selector_arm = transition
        .predecessor_selector
        .get("authority_epoch")
        .and_then(serde_json::Value::as_str);
    let resource = transition.predecessor_selector.get("resource");
    let resource_selection = resource
        .and_then(|value| value.get("selection"))
        .and_then(serde_json::Value::as_str);
    let anchor_kind = resource
        .and_then(|value| value.get("anchor_kind"))
        .and_then(serde_json::Value::as_str);
    if transition.expected_predecessor_arm != "ens_v1"
        || transition.successor_arm != "ens_v2"
        || selector_arm != Some(transition.expected_predecessor_arm.as_str())
        || selection != Some("active_immediately_before_boundary")
        || selector_name != Some(transition.logical_name_id.as_str())
        || transition.block_number < 0
        || transition.transaction_index < 0
        || transition.log_index < 0
    {
        return Err(InterpretError::data_integrity(format!(
            "activated migration boundary {} has an invalid authority selector or position",
            transition.boundary_event_identity
        )));
    }
    match anchor_kind {
        Some("registrar_backed_registration") => {
            let token_id = resource
                .and_then(|value| value.get("token_id"))
                .and_then(serde_json::Value::as_str);
            let labelhash = resource
                .and_then(|value| value.get("labelhash"))
                .and_then(serde_json::Value::as_str);
            let contract_instance_id = resource
                .and_then(|value| value.get("contract_instance_id"))
                .and_then(serde_json::Value::as_str)
                .and_then(|value| value.parse().ok());
            if resource_selection != Some("current_registrar_resource_immediately_before_boundary")
                || token_id.is_none()
                || token_id != labelhash
                || contract_instance_id.is_none()
            {
                return Err(invalid_selector(transition));
            }
            Ok(PredecessorSelector {
                anchor_kind: "registrar_backed_registration".to_owned(),
                identity: token_id.unwrap().to_owned(),
                contract_instance_id,
                contract_address: None,
            })
        }
        Some("wrapper_backed_control") => {
            let namehash = resource
                .and_then(|value| value.get("namehash"))
                .and_then(serde_json::Value::as_str);
            let wrapper_token_id = resource
                .and_then(|value| value.get("wrapper_token_id"))
                .and_then(serde_json::Value::as_str);
            let contract_address = resource
                .and_then(|value| value.get("contract_address"))
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty());
            if resource_selection != Some("current_wrapper_resource_immediately_before_boundary")
                || namehash.is_none()
                || namehash != wrapper_token_id
                || contract_address.is_none()
            {
                return Err(invalid_selector(transition));
            }
            Ok(PredecessorSelector {
                anchor_kind: "wrapper_backed_control".to_owned(),
                identity: namehash.unwrap().to_owned(),
                contract_instance_id: None,
                contract_address: contract_address.map(str::to_owned),
            })
        }
        _ => Err(invalid_selector(transition)),
    }
}

fn invalid_selector(transition: &MigrationAuthorityTransition) -> InterpretError {
    InterpretError::data_integrity(format!(
        "activated migration boundary {} has an invalid predecessor resource selector",
        transition.boundary_event_identity
    ))
}
