use bigname_adapters::schema_v2::seam::REDO_RESOLVER_EVIDENCE_SELECT_SQL;
use sqlx::{Postgres, Transaction};

use crate::Result;

pub(super) async fn capture_project_evidence(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    let statement = format!(
        r#"INSERT INTO project_redo_resolver_evidence (
            chain_id, event_identity, block_number, event_kind,
            source_family, resource_id,
            before_resolver_address, after_resolver_address
        )
        {REDO_RESOLVER_EVIDENCE_SELECT_SQL}
        ON CONFLICT (chain_id, event_identity) DO NOTHING"#,
    );
    sqlx::query(&statement)
        .bind(chain_id)
        .bind(from_block)
        .bind(to_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            crate::InterpretError::database(
                "failed to stage resolver evidence for Project redo",
                error,
            )
        })?;
    capture_expiry_roots(transaction, chain_id, from_block, to_block).await?;
    capture_child_registration_history(transaction, chain_id, from_block, to_block).await
}

async fn capture_expiry_roots(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    // The first copy is the pre-delete fact; a retry must not replace it with changed evidence.
    sqlx::query(
        r#"
        INSERT INTO project_redo_expiry_roots (
            chain_id, event_identity, block_number, logical_name_id, resource_id
        )
        SELECT event.chain_id, event.event_identity,
               event.block_number, event.logical_name_id, event.resource_id
        FROM normalized_events event
        WHERE event.chain_id = $1
          AND event.block_number BETWEEN $2 AND $3
          AND (event.logical_name_id IS NOT NULL OR event.resource_id IS NOT NULL)
          AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
          AND event.event_kind = 'RegistrationReleased'
          AND event.after_state ->> 'source_event' = 'RegistryPathExpired'
          AND event.after_state ->> 'derived_from' = 'interpreter_state'
          AND event.after_state ->> 'terminal_reason' =
              'registry_name_binding_expired'
        ON CONFLICT (chain_id, event_identity) DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        crate::InterpretError::database(
            "failed to preserve path-expiry scope for Project redo",
            error,
        )
    })?;
    Ok(())
}

async fn capture_child_registration_history(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    // Preserve the first pre-delete copy so a retry cannot narrow the original redo suffix.
    sqlx::query(
        r#"
        INSERT INTO project_redo_child_registration_history (chain_id, event_identity,
            block_number, event_kind, logical_name_id, registry_contract_instance_id)
        SELECT DISTINCT event.chain_id, event.event_identity, event.block_number,
               event.event_kind, event.logical_name_id,
               association.registry_contract_instance_id
        FROM normalized_events event
        JOIN migration_discovery_associations association
          ON association.chain_id = event.chain_id
         AND association.registry_contract_instance_id::text = event.after_state ->> 'registry_contract_instance_id'
         AND association.correlation_kind = 'migration_registry_creation'
        WHERE event.chain_id = $1 AND event.block_number BETWEEN $2 AND $3
          AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
          AND event.event_kind IN ('RegistrationReserved', 'RegistrationGranted', 'RegistrationRenewed')
          AND event.logical_name_id IS NOT NULL
        ON CONFLICT (chain_id, event_identity) DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        crate::InterpretError::database(
            "failed to preserve child registration history for Project redo",
            error,
        )
    })?;
    Ok(())
}
