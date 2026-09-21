use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

/// Node-keyed record writes (every ENSv1 resolver write and every PublicResolverV2 write) carry no
/// logical name or resource of their own, so only a resolver pointer can attribute them to a
/// registration. Attribute each one through the pointer that selected its resolver: a pointer
/// covers the writes on its resolver at chain positions before the pointer that superseded it, and
/// the latest pointer is open-ended. Resolver storage is persistent and ENSv1 reads it at read
/// time, so a write made before the pointer moved onto that resolver still served the name while
/// the pointer stood — hence the window is bounded only on the newer side.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L137 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/TextResolver.sol:L28 @ ens_v1@91c966f)
///
/// The latest non-zero pointer's window is unbounded, so this reproduces the attribution the value
/// selection already makes through `project_record_pointers` and adds every superseded pointer's.
pub(super) async fn build(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    // This statement and the inventory build after it join the pointer stages and the staged
    // resolver rows to every node-keyed record event. Temporary tables are never analyzed
    // automatically; with the default guess of a few rows the planner compares every record
    // event with every pointer.
    for table in [
        "project_record_pointer_history",
        "project_record_pointers",
        "project_stage_resolver_current",
    ] {
        sqlx::query(&format!("ANALYZE {table}"))
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to analyze pointer stages", error))?;
    }
    sqlx::query(ATTRIBUTE_RECORD_HISTORY)
        .bind(chain_id)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database(
                "failed to attribute historical pointer record writes",
                error,
            )
        })?;
    sqlx::query("CREATE INDEX ON project_record_history_attribution (resource_id)")
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to index historical pointer attribution", error)
        })?;
    Ok(())
}

pub(in crate::builders) const ATTRIBUTE_RECORD_HISTORY: &str = r#"
        CREATE TEMP TABLE project_record_history_attribution ON COMMIT DROP AS
        SELECT pointer.resource_id, event.normalized_event_id
        FROM project_record_pointer_history pointer
        JOIN project_events event
          ON event.chain_id = $1
         AND event.logical_name_id IS NULL
         AND lower(event.after_state ->> 'node') = pointer.namehash
         AND lower(COALESCE(
                NULLIF(event.after_state ->> 'resolver', ''),
                NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
             )) = pointer.resolver_address
        WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
          AND (
              pointer.next_event_id IS NULL
              OR ROW(
                  COALESCE(event.block_number, -1),
                  COALESCE(event.transaction_index, -1),
                  COALESCE(event.log_index, -1),
                  event.normalized_event_id
              ) < ROW(
                  pointer.next_block_number,
                  pointer.next_transaction_index,
                  pointer.next_log_index,
                  pointer.next_event_id
              )
          )
          AND (
              (
                  event.source_family = 'ens_v1_resolver_l1'
                  AND pointer.pointer_source_family IN (
                      'ens_v1_registry_l1',
                      'ens_v1_registrar_l1',
                      'ens_v1_wrapper_l1'
                  )
              )
              OR (
                  event.source_family = 'basenames_base_resolver'
                  AND pointer.pointer_source_family = 'basenames_base_registry'
              )
          )
        UNION
        -- The guarded ENSv2-origin exception uses the exact declaration already selected by
        -- resolver classification and applies only to pointers in that declaration's namespace.
        SELECT pointer.resource_id, event.normalized_event_id
        FROM project_record_pointer_history pointer
        JOIN project_stage_resolver_current resolver
          ON resolver.chain_id = $1
         AND resolver.resolver_address = pointer.resolver_address
         AND resolver.support_status = 'supported'
         AND (resolver.declared_summary #>> '{classification,source_family}' =
             'ens_v1_resolver_l1'
          OR (resolver.declared_summary #>> '{classification,source_family}' =
                  'ens_v2_resolver_l1'
              AND resolver.declared_summary #>> '{classification,role}' =
                  'public_resolver_v2'))
         AND resolver.declared_summary #>> '{classification,basis}' =
             'manifest_declared_address'
        JOIN project_manifests declaration_manifest
          ON declaration_manifest.manifest_id =
             (resolver.provenance ->> 'manifest_id')::bigint
         AND declaration_manifest.namespace = pointer.pointer_namespace
        JOIN project_events event
          ON event.chain_id = $1
         AND event.logical_name_id IS NULL
         AND event.source_family =
             resolver.declared_summary #>> '{classification,source_family}'
         AND (event.source_family <> 'ens_v2_resolver_l1'
              OR (event.namespace = pointer.pointer_namespace
                  AND event.source_manifest_id = declaration_manifest.manifest_id))
         AND lower(event.after_state ->> 'node') = pointer.namehash
         AND lower(COALESCE(
                NULLIF(event.after_state ->> 'resolver', ''),
                NULLIF(event.raw_fact_ref ->> 'emitting_address', '')
             )) = pointer.resolver_address
        WHERE event.event_kind IN ('RecordChanged', 'RecordVersionChanged')
          AND pointer.pointer_source_family IN (
              'ens_v2_registry_l1', 'ens_v2_root_l1'
          )
          AND (
              pointer.next_event_id IS NULL
              OR ROW(
                  COALESCE(event.block_number, -1),
                  COALESCE(event.transaction_index, -1),
                  COALESCE(event.log_index, -1),
                  event.normalized_event_id
              ) < ROW(
                  pointer.next_block_number,
                  pointer.next_transaction_index,
                  pointer.next_log_index,
                  pointer.next_event_id
              )
          )
        UNION
        SELECT resource_id, normalized_event_id
        FROM project_linked_record_history_attribution
        "#;
