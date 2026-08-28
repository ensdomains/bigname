use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn close(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // Frontier probes return few rows through endpoint indexes, but PostgreSQL can price the
    // combined branches above jit_above_cost on a large history. Compiling every iteration then
    // costs more than the indexed work, so keep JIT off only while topology scope is closed.
    let previous_jit = sqlx::query_scalar::<_, String>("SHOW jit")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to read topology JIT", error))?;
    sqlx::query("SET LOCAL jit = off")
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to disable topology JIT", error))?;
    create_frontier_tables(transaction).await?;
    include_changed_current_edges(transaction, chain_id).await?;
    sqlx::query(
        "INSERT INTO project_scope_topology_pending
         SELECT logical_name_id FROM project_scope_names
         UNION
         SELECT logical_name_id FROM project_scope_children
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to seed topology frontier", error))?;

    loop {
        sqlx::query("TRUNCATE project_scope_topology_current, project_scope_topology_candidates")
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to reset topology frontier", error))?;
        let moved = sqlx::query(
            "WITH moved AS (
                 DELETE FROM project_scope_topology_pending
                 RETURNING logical_name_id
             )
             INSERT INTO project_scope_topology_current (logical_name_id)
             SELECT logical_name_id FROM moved",
        )
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to advance topology frontier", error))?
        .rows_affected();
        if moved == 0 {
            break;
        }
        sqlx::query(
            "INSERT INTO project_scope_topology_seen
             SELECT logical_name_id FROM project_scope_topology_current
             ON CONFLICT DO NOTHING",
        )
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to mark topology frontier", error))?;

        include_current_edges(transaction, chain_id).await?;
        include_v1_event_edges(transaction, chain_id, target_block).await?;
        include_v2_event_edges(transaction, chain_id, target_block).await?;

        sqlx::query(
            "INSERT INTO project_scope_children
             SELECT logical_name_id FROM project_scope_topology_candidates
             ON CONFLICT DO NOTHING",
        )
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to retain topology candidates", error))?;
        sqlx::query(
            "INSERT INTO project_scope_topology_pending
             SELECT candidate.logical_name_id
             FROM project_scope_topology_candidates candidate
             LEFT JOIN project_scope_topology_seen seen USING (logical_name_id)
             WHERE seen.logical_name_id IS NULL
             ON CONFLICT DO NOTHING",
        )
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to queue topology candidates", error))?;
    }
    restore_jit(transaction, &previous_jit).await?;
    Ok(())
}

async fn restore_jit(
    transaction: &mut Transaction<'_, Postgres>,
    previous_jit: &str,
) -> Result<()> {
    sqlx::query("SELECT set_config('jit', $1, true)")
        .bind(previous_jit)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to restore transaction JIT", error))?;
    Ok(())
}

async fn create_frontier_tables(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    for statement in [
        "CREATE TEMP TABLE project_scope_topology_pending (logical_name_id text PRIMARY KEY) ON COMMIT DROP",
        "CREATE TEMP TABLE project_scope_topology_current (logical_name_id text PRIMARY KEY) ON COMMIT DROP",
        "CREATE TEMP TABLE project_scope_topology_seen (logical_name_id text PRIMARY KEY) ON COMMIT DROP",
        "CREATE TEMP TABLE project_scope_topology_candidates (logical_name_id text PRIMARY KEY) ON COMMIT DROP",
    ] {
        sqlx::query(statement)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to create topology frontier", error))?;
    }
    Ok(())
}

async fn include_changed_current_edges(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_children
         SELECT child.parent_logical_name_id
         FROM children_current child
         JOIN project_changed_events changed
          ON changed.namespace = child.namespace
          AND lower(changed.after_state ->> 'labelhash') = lower(child.labelhash)
          AND child.parent_logical_name_id =
              changed.namespace || ':' || lower(changed.after_state ->> 'node')
         WHERE child.provenance ->> 'chain_id' = $1
         UNION
         SELECT child.child_logical_name_id
         FROM children_current child
         JOIN project_changed_events changed
          ON changed.namespace = child.namespace
          AND lower(changed.after_state ->> 'labelhash') = lower(child.labelhash)
          AND child.parent_logical_name_id =
              changed.namespace || ':' || lower(changed.after_state ->> 'node')
         WHERE child.provenance ->> 'chain_id' = $1
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to seed changed current topology", error))?;
    Ok(())
}

async fn include_current_edges(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_scope_topology_candidates
         SELECT child.parent_logical_name_id
         FROM project_scope_topology_current scope
         JOIN children_current child
           ON child.parent_logical_name_id = scope.logical_name_id
         WHERE child.provenance ->> 'chain_id' = $1
         UNION
         SELECT child.child_logical_name_id
         FROM project_scope_topology_current scope
         JOIN children_current child
           ON child.parent_logical_name_id = scope.logical_name_id
         WHERE child.provenance ->> 'chain_id' = $1
         UNION
         SELECT child.parent_logical_name_id
         FROM project_scope_topology_current scope
         JOIN children_current child
           ON child.namespace = split_part(scope.logical_name_id, ':', 1)
          AND child.namehash = substring(
              scope.logical_name_id FROM position(':' IN scope.logical_name_id) + 1
          )
         WHERE child.provenance ->> 'chain_id' = $1
         UNION
         SELECT child.child_logical_name_id
         FROM project_scope_topology_current scope
         JOIN children_current child
           ON child.namespace = split_part(scope.logical_name_id, ':', 1)
          AND child.namehash = substring(
              scope.logical_name_id FROM position(':' IN scope.logical_name_id) + 1
          )
         WHERE child.provenance ->> 'chain_id' = $1
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to expand current topology frontier", error))?;
    Ok(())
}

pub(crate) const V1_AFTER_NODE_SQL: &str = r#"
WITH edges AS (
    SELECT event.namespace || ':' || lower(event.after_state ->> 'node') AS parent_id,
           event.namespace || ':' || lower(event.after_state ->> 'child_node') AS child_id
    FROM project_scope_topology_current scope
    CROSS JOIN LATERAL (
        SELECT namespace, chain_id, block_number, block_hash, after_state
        FROM normalized_events
-- issue-435-after-node-predicate-begin
WHERE event_kind = 'SubregistryChanged'
  AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND after_state ->> 'node' IS NOT NULL
  AND btrim(after_state ->> 'node') <> ''
  AND after_state ->> 'child_node' IS NOT NULL
  AND btrim(after_state ->> 'child_node') <> ''
-- issue-435-after-node-predicate-end
          AND chain_id = $1
          AND block_number <= $2
          AND namespace || ':' || lower(after_state ->> 'node') =
              scope.logical_name_id
        OFFSET 0
    ) event
    JOIN chain_lineage lineage
      ON lineage.chain_id = event.chain_id
     AND lineage.block_number = event.block_number
     AND lineage.block_hash = event.block_hash
    WHERE lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
)
INSERT INTO project_scope_topology_candidates
SELECT parent_id FROM edges UNION SELECT child_id FROM edges
ON CONFLICT DO NOTHING
"#;

pub(crate) const V1_AFTER_CHILD_SQL: &str = r#"
WITH edges AS (
    SELECT event.namespace || ':' || lower(event.after_state ->> 'node') AS parent_id,
           event.namespace || ':' || lower(event.after_state ->> 'child_node') AS child_id
    FROM project_scope_topology_current scope
    CROSS JOIN LATERAL (
        SELECT namespace, chain_id, block_number, block_hash, after_state
        FROM normalized_events
-- issue-435-after-child-predicate-begin
WHERE event_kind = 'SubregistryChanged'
  AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND after_state ->> 'node' IS NOT NULL
  AND btrim(after_state ->> 'node') <> ''
  AND after_state ->> 'child_node' IS NOT NULL
  AND btrim(after_state ->> 'child_node') <> ''
-- issue-435-after-child-predicate-end
          AND chain_id = $1
          AND block_number <= $2
          AND namespace || ':' || lower(after_state ->> 'child_node') =
              scope.logical_name_id
        OFFSET 0
    ) event
    JOIN chain_lineage lineage
      ON lineage.chain_id = event.chain_id
     AND lineage.block_number = event.block_number
     AND lineage.block_hash = event.block_hash
    WHERE lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
)
INSERT INTO project_scope_topology_candidates
SELECT parent_id FROM edges UNION SELECT child_id FROM edges
ON CONFLICT DO NOTHING
"#;

pub(crate) const V1_BEFORE_NODE_SQL: &str = r#"
WITH edges AS (
    SELECT event.namespace || ':' || lower(event.before_state ->> 'node') AS parent_id,
           event.namespace || ':' || lower(event.before_state ->> 'child_node') AS child_id
    FROM project_scope_topology_current scope
    CROSS JOIN LATERAL (
        SELECT namespace, chain_id, block_number, block_hash, before_state
        FROM normalized_events
-- issue-435-before-node-predicate-begin
WHERE event_kind = 'SubregistryChanged'
  AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND before_state ->> 'node' IS NOT NULL
  AND btrim(before_state ->> 'node') <> ''
  AND before_state ->> 'child_node' IS NOT NULL
  AND btrim(before_state ->> 'child_node') <> ''
-- issue-435-before-node-predicate-end
          AND chain_id = $1
          AND block_number <= $2
          AND namespace || ':' || lower(before_state ->> 'node') =
              scope.logical_name_id
        OFFSET 0
    ) event
    JOIN chain_lineage lineage
      ON lineage.chain_id = event.chain_id
     AND lineage.block_number = event.block_number
     AND lineage.block_hash = event.block_hash
    WHERE lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
)
INSERT INTO project_scope_topology_candidates
SELECT parent_id FROM edges UNION SELECT child_id FROM edges
ON CONFLICT DO NOTHING
"#;

pub(crate) const V1_BEFORE_CHILD_SQL: &str = r#"
WITH edges AS (
    SELECT event.namespace || ':' || lower(event.before_state ->> 'node') AS parent_id,
           event.namespace || ':' || lower(event.before_state ->> 'child_node') AS child_id
    FROM project_scope_topology_current scope
    CROSS JOIN LATERAL (
        SELECT namespace, chain_id, block_number, block_hash, before_state
        FROM normalized_events
-- issue-435-before-child-predicate-begin
WHERE event_kind = 'SubregistryChanged'
  AND source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
  AND consumer_visibility = 'activated'
  AND canonicality_state IN ('canonical', 'safe', 'finalized')
  AND before_state ->> 'node' IS NOT NULL
  AND btrim(before_state ->> 'node') <> ''
  AND before_state ->> 'child_node' IS NOT NULL
  AND btrim(before_state ->> 'child_node') <> ''
-- issue-435-before-child-predicate-end
          AND chain_id = $1
          AND block_number <= $2
          AND namespace || ':' || lower(before_state ->> 'child_node') =
              scope.logical_name_id
        OFFSET 0
    ) event
    JOIN chain_lineage lineage
      ON lineage.chain_id = event.chain_id
     AND lineage.block_number = event.block_number
     AND lineage.block_hash = event.block_hash
    WHERE lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
)
INSERT INTO project_scope_topology_candidates
SELECT parent_id FROM edges UNION SELECT child_id FROM edges
ON CONFLICT DO NOTHING
"#;

pub(crate) const V1_EVENT_EDGE_SQLS: [&str; 4] = [
    V1_AFTER_NODE_SQL,
    V1_AFTER_CHILD_SQL,
    V1_BEFORE_NODE_SQL,
    V1_BEFORE_CHILD_SQL,
];

async fn include_v1_event_edges(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    for query in V1_EVENT_EDGE_SQLS {
        sqlx::query(query)
            .bind(chain_id)
            .bind(target_block)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to expand v1 topology frontier", error)
            })?;
    }
    Ok(())
}

async fn include_v2_event_edges(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        "WITH edges AS (
             SELECT DISTINCT topology.logical_name_id AS parent_id,
                    registration.logical_name_id AS child_id
             FROM project_scope_topology_current scope
             CROSS JOIN LATERAL (SELECT * FROM normalized_events topology
               WHERE topology.logical_name_id = scope.logical_name_id
              AND topology.chain_id = $1
              AND topology.block_number <= $2
              AND topology.event_kind = 'SubregistryChanged'
              AND topology.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
              AND topology.consumer_visibility = 'activated'
              AND topology.canonicality_state IN ('canonical', 'safe', 'finalized') OFFSET 0) topology
             JOIN chain_lineage topology_lineage
               ON topology_lineage.chain_id = topology.chain_id
              AND topology_lineage.block_number = topology.block_number
              AND topology_lineage.block_hash = topology.block_hash
              AND topology_lineage.canonicality_state IN (
                  'canonical', 'safe', 'finalized'
              )
             CROSS JOIN LATERAL (
                 VALUES (topology.after_state ->> 'subregistry'),
                        (topology.before_state ->> 'subregistry')
             ) pointer(address)
             CROSS JOIN LATERAL (SELECT * FROM contract_instance_addresses address
               WHERE address.chain_id = topology.chain_id
              AND lower(address.address) = lower(pointer.address)
              AND (address.active_from_block_number IS NULL
                   OR address.active_from_block_number <= $2)
              AND (address.active_to_block_number IS NULL
                   OR address.active_to_block_number > $2)
              AND address.deactivated_at IS NULL OFFSET 0) address
             CROSS JOIN LATERAL (SELECT * FROM normalized_events registration
               WHERE registration.chain_id = $1
              AND registration.after_state ->> 'registry_contract_instance_id' =
                  address.contract_instance_id::text
              AND registration.block_number <= $2
              AND registration.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased'
              )
              AND registration.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
              AND registration.consumer_visibility = 'activated'
              AND registration.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND registration.logical_name_id IS NOT NULL OFFSET 0) registration
             JOIN chain_lineage registration_lineage
               ON registration_lineage.chain_id = registration.chain_id
              AND registration_lineage.block_number = registration.block_number
              AND registration_lineage.block_hash = registration.block_hash
              AND registration_lineage.canonicality_state IN (
                  'canonical', 'safe', 'finalized'
              )
             WHERE pointer.address IS NOT NULL AND btrim(pointer.address) <> ''
             UNION ALL
             SELECT DISTINCT topology.logical_name_id,
                    registration.logical_name_id
             FROM project_scope_topology_current scope
             CROSS JOIN LATERAL (
                 SELECT *
                 FROM (
                     SELECT * FROM normalized_events
                     WHERE logical_name_id = scope.logical_name_id
                       AND chain_id = $1
                       AND block_number <= $2
                       AND canonicality_state IN ('canonical', 'safe', 'finalized')
                     OFFSET 0
                 ) registration_frontier
                 WHERE registration_frontier.event_kind IN (
                  'RegistrationGranted', 'RegistrationRenewed', 'RegistrationReleased'
                 )
                   AND registration_frontier.source_family IN (
                       'ens_v2_root_l1', 'ens_v2_registry_l1'
                   )
                   AND registration_frontier.consumer_visibility = 'activated'
                   AND registration_frontier.after_state ->>
                       'registry_contract_instance_id' IS NOT NULL
             ) registration
             JOIN chain_lineage registration_lineage
               ON registration_lineage.chain_id = registration.chain_id
              AND registration_lineage.block_number = registration.block_number
              AND registration_lineage.block_hash = registration.block_hash
              AND registration_lineage.canonicality_state IN (
                  'canonical', 'safe', 'finalized'
              )
             CROSS JOIN LATERAL (
                 SELECT * FROM (
                     SELECT * FROM contract_instance_addresses
                     WHERE contract_instance_id = CASE WHEN pg_input_is_valid(registration.after_state ->> 'registry_contract_instance_id', 'uuid') THEN (registration.after_state ->> 'registry_contract_instance_id')::uuid END
                       AND deactivated_at IS NULL
                     OFFSET 0
                 ) address_frontier
                 WHERE address_frontier.chain_id = registration.chain_id
                   AND (address_frontier.active_from_block_number IS NULL
                        OR address_frontier.active_from_block_number <= $2)
                   AND (address_frontier.active_to_block_number IS NULL
                        OR address_frontier.active_to_block_number > $2)
             ) address
             CROSS JOIN LATERAL (
                 SELECT logical_name_id, chain_id, block_number, block_hash
                 FROM normalized_events
                 WHERE chain_id = $1
                   AND block_number <= $2
                   AND event_kind = 'SubregistryChanged'
                   AND source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                   AND consumer_visibility = 'activated'
                   AND canonicality_state IN ('canonical', 'safe', 'finalized')
                   AND logical_name_id IS NOT NULL
                   AND ARRAY[
                           lower(after_state ->> 'subregistry'),
                           lower(before_state ->> 'subregistry')
                       ] @> ARRAY[lower(address.address)]
                 OFFSET 0
             ) topology
             JOIN chain_lineage topology_lineage
               ON topology_lineage.chain_id = topology.chain_id
              AND topology_lineage.block_number = topology.block_number
              AND topology_lineage.block_hash = topology.block_hash
              AND topology_lineage.canonicality_state IN (
                  'canonical', 'safe', 'finalized'
              )
         )
         INSERT INTO project_scope_topology_candidates
         SELECT parent_id FROM edges
         UNION
         SELECT child_id FROM edges
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to expand v2 topology frontier", error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{V1_EVENT_EDGE_SQLS, restore_jit};

    #[sqlx::test]
    #[rustfmt::skip]
    async fn jit_restoration_preserves_session_and_local_values(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let mut transaction = pool.begin().await?; sqlx::query("SET SESSION jit = off").execute(&mut *transaction).await?;
        for expected in ["off", "on"] {
            if expected == "on" { sqlx::query("SET LOCAL jit = on").execute(&mut *transaction).await?; } let previous = sqlx::query_scalar::<_, String>("SHOW jit").fetch_one(&mut *transaction).await?; sqlx::query("SET LOCAL jit = off").execute(&mut *transaction).await?; restore_jit(&mut transaction, &previous).await?; assert_eq!(sqlx::query_scalar::<_, String>("SHOW jit").fetch_one(&mut *transaction).await?, expected);
        }
        Ok(())
    }

    const AFTER_NODE_MIGRATION: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/20260827130000_normalized_events_v1_after_node_scope_idx.sql"
    ));
    const AFTER_CHILD_MIGRATION: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/20260827130100_normalized_events_v1_after_child_scope_idx.sql"
    ));
    const BEFORE_NODE_MIGRATION: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/20260827130200_normalized_events_v1_before_node_scope_idx.sql"
    ));
    const BEFORE_CHILD_MIGRATION: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../migrations/20260827130300_normalized_events_v1_before_child_scope_idx.sql"
    ));

    fn predicate<'a>(source: &'a str, label: &str) -> &'a str {
        let start_marker = format!("-- issue-435-{label}-predicate-begin\n");
        let end_marker = format!("-- issue-435-{label}-predicate-end");
        let start = source
            .find(&start_marker)
            .unwrap_or_else(|| panic!("missing start marker for {label}"))
            + start_marker.len();
        let end = source[start..]
            .find(&end_marker)
            .unwrap_or_else(|| panic!("missing end marker for {label}"))
            + start;
        &source[start..end]
    }

    #[test]
    fn v1_scope_index_predicates_exactly_match_project_probes() {
        for (index, (label, migration)) in [
            ("after-node", AFTER_NODE_MIGRATION),
            ("after-child", AFTER_CHILD_MIGRATION),
            ("before-node", BEFORE_NODE_MIGRATION),
            ("before-child", BEFORE_CHILD_MIGRATION),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                predicate(&migration.replace("\r\n", "\n"), label),
                predicate(&V1_EVENT_EDGE_SQLS[index].replace("\r\n", "\n"), label),
                "{label} partial-index predicate drifted from the Project probe"
            );
        }
    }
}
