use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn include_names_for_scoped_registrars(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    let statement = super::frontier::query(
        transaction,
        "wrapper_names_from_registrars",
        include_str!("wrapper_names_from_registrars.sql"),
    )
    .await?;
    #[cfg(test)]
    if crate::profile::execute(
        transaction,
        chain_id,
        target_block,
        &statement,
        crate::profile::Stage::WrapperNames,
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
                "failed to scope later-wrapped names from registrar resources",
                error,
            )
        })?;
    Ok(())
}

pub(super) async fn include_registrars_for_scoped_wrappers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        &super::frontier::query(
            transaction,
            "registrars_from_wrappers",
            REGISTRARS_FOR_SCOPED_WRAPPERS,
        )
        .await?,
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to scope wrapped registrar resources from wrapper bindings",
            error,
        )
    })?;
    Ok(())
}

// The lease is looked up through the `resources` primary key: the recorded text is cast to a
// uuid, not every resource id to text.
const REGISTRARS_FOR_SCOPED_WRAPPERS: &str = "WITH scoped_wrappers AS (
             SELECT wrapper.normalized_event_id
             FROM project_scope_resources scope
             JOIN LATERAL (
                 SELECT normalized_event_id FROM normalized_events
                 WHERE resource_id = scope.resource_id AND chain_id = $1 AND block_number <= $2
                   AND source_family = 'ens_v1_wrapper_l1' AND event_kind = 'SurfaceBound'
                   AND consumer_visibility = 'activated'
                   AND canonicality_state IN ('canonical', 'safe', 'finalized') OFFSET 0
             ) wrapper ON TRUE
             UNION
             SELECT wrapper.normalized_event_id
             FROM project_scope_names scope
             JOIN LATERAL (
                 SELECT normalized_event_id FROM normalized_events
                 WHERE logical_name_id = scope.logical_name_id AND chain_id = $1 AND block_number <= $2
                   AND source_family = 'ens_v1_wrapper_l1' AND event_kind = 'SurfaceBound'
                   AND consumer_visibility = 'activated'
                   AND canonicality_state IN ('canonical', 'safe', 'finalized') OFFSET 0
             ) wrapper ON TRUE
         )
         INSERT INTO project_scope_resources
         SELECT DISTINCT registrar.resource_id
         FROM scoped_wrappers scope
         JOIN normalized_events wrapper USING (normalized_event_id)
         JOIN chain_lineage wrapper_lineage
           ON wrapper_lineage.chain_id = wrapper.chain_id
          AND wrapper_lineage.block_hash = wrapper.block_hash
          AND wrapper_lineage.block_number = wrapper.block_number
         JOIN resources registrar
           ON registrar.chain_id = wrapper.chain_id
          AND registrar.resource_id =
              (wrapper.after_state ->> 'wrapped_registrar_resource_id')::uuid
         WHERE wrapper.chain_id = $1
           AND wrapper.block_number <= $2
           AND wrapper.source_family = 'ens_v1_wrapper_l1'
           AND wrapper.event_kind = 'SurfaceBound'
           AND wrapper.consumer_visibility = 'activated'
           AND wrapper.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND wrapper_lineage.canonicality_state IN (
               'canonical', 'safe', 'finalized'
           )
         ON CONFLICT DO NOTHING";

#[cfg(test)]
mod tests {
    #[test]
    fn wrapped_registrar_lookup_uses_the_resources_primary_key() {
        let query = super::REGISTRARS_FOR_SCOPED_WRAPPERS;
        assert!(
            !query.contains("registrar.resource_id::text"),
            "casting resources.resource_id to text defeats its primary key"
        );
        assert!(query.contains(
            "registrar.resource_id =\n              \
             (wrapper.after_state ->> 'wrapped_registrar_resource_id')::uuid"
        ));
    }
}
