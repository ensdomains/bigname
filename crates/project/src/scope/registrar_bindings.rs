use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

// Staging names a `.eth` BaseRegistrar lifecycle row that carries no name through any binding of
// its resource to a name with the same namehash, closed bindings included
// (`builders::name_authority::stage::bind_resource_events`). A rebuild stages every row, so an
// incremental batch has to bring the same rows and names into scope. The general binding closure
// follows only bindings that are open at the target block, and the latest binding of each arm.

const UNNAMED_LEASE_ROW: &str = "registrar.logical_name_id IS NULL
               AND registrar.source_family = 'ens_v1_registrar_l1'
               AND registrar.event_kind IN (
                   'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased',
                   'ExpiryChanged', 'TokenControlTransferred'
               )
               AND registrar.chain_id = $1
               AND registrar.block_number <= $2
               AND registrar.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND lower(registrar.after_state ->> 'namehash') = lower(surface.namehash)";

const CANONICAL_BINDING: &str = "binding.chain_id = $1
           AND binding.block_number <= $2
           AND binding.authority_arm = 'ens_v1'
           AND binding.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')";

/// A scoped name brings in every resource it was ever bound to that holds lease rows without a
/// name, so the batch stages the rows a rebuild would name.
pub(super) async fn include_unnamed_lease_resources_for_scoped_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    let statement = format!(
        "/* project:scope.registrar_bindings */ INSERT INTO project_scope_resources
         SELECT DISTINCT binding.resource_id
         FROM project_scope_names scope
         JOIN surface_bindings binding USING (logical_name_id)
         JOIN name_surfaces surface USING (logical_name_id)
         JOIN chain_lineage lineage
           ON lineage.chain_id = binding.chain_id
          AND lineage.block_hash = binding.block_hash
          AND lineage.block_number = binding.block_number
         WHERE {CANONICAL_BINDING}
           AND EXISTS (
               SELECT 1 FROM normalized_events registrar
               WHERE registrar.resource_id = binding.resource_id
                 AND {UNNAMED_LEASE_ROW}
           )
         ON CONFLICT DO NOTHING"
    );
    sqlx::query(
        &super::frontier::query(transaction, "unnamed_leases_from_names", &statement).await?,
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to scope registrar resources that hold lease rows without a name",
            error,
        )
    })?;
    Ok(())
}

/// A scoped resource that holds lease rows without a name brings in every name it was ever bound
/// to with the same namehash, so the batch rebuilds the names a rebuild would give those rows to.
pub(super) async fn include_names_for_scoped_unnamed_lease_rows(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // Closed bindings remain candidates. The history match is existential: repeated lifecycle
    // events cannot add another name, and must not multiply the binding/lineage work.
    let statement = format!(
        include_str!("names_from_unnamed_leases.sql"),
        CANONICAL_BINDING = CANONICAL_BINDING,
        UNNAMED_LEASE_ROW = UNNAMED_LEASE_ROW,
    );
    let statement =
        super::frontier::query(transaction, "names_from_unnamed_leases", &statement).await?;
    #[cfg(test)]
    if crate::profile::execute(
        transaction,
        chain_id,
        target_block,
        &statement,
        crate::profile::Stage::UnnamedLeaseNames,
    )
    .await?
    {
        return Ok(());
    }
    sqlx::query(&statement)
        .bind(chain_id)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database(
                "failed to scope names bound to registrar resources with lease rows without a name",
                error,
            )
        })?;
    Ok(())
}
