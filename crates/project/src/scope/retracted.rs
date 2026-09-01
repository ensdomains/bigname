use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result, scope::Window};

/// Retain keys whose cited events Interpret deleted during redo so Project can retract losing-fork output.
pub(super) async fn seed(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    window: &Window<'_>,
    target_block: i64,
) -> Result<()> {
    seed_names(
        transaction,
        chain_id,
        window.from_block,
        window.to_block,
        target_block,
    )
    .await?;
    seed_children(transaction, chain_id).await?;
    seed_resources(transaction, chain_id).await?;
    seed_resolvers(transaction, chain_id, window.from_block, window.to_block).await?;
    seed_primary(transaction, chain_id).await?;
    Ok(())
}

async fn seed_names(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
    target_block: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH citations AS (
            SELECT row.logical_name_id, citation.event_id
            FROM name_current row
            CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(
                row.provenance -> 'selected_event_ids', '[]'::jsonb
            )) citation(event_id)
            WHERE row.provenance ->> 'chain_id' = $1
            UNION ALL
            SELECT row.logical_name_id, row.provenance ->> 'normalized_event_id'
            FROM address_names_current row
            WHERE row.provenance ->> 'chain_id' = $1
        )
        INSERT INTO project_scope_names
        SELECT DISTINCT citation.logical_name_id
        FROM citations citation
        WHERE citation.event_id IS NOT NULL
          AND citation.event_id NOT IN ('', 'null')
          AND NOT EXISTS (
              SELECT 1 FROM normalized_events event
              LEFT JOIN chain_lineage lineage
                ON lineage.chain_id = event.chain_id
               AND lineage.block_hash = event.block_hash
               AND lineage.block_number = event.block_number
              WHERE event.normalized_event_id = citation.event_id::bigint
                AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                AND (
                    (event.block_number IS NULL AND event.block_hash IS NULL)
                    OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                )
          )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to retain retracted name scope", error))?;
    sqlx::query(
        r#"
        INSERT INTO project_scope_names
        SELECT DISTINCT event.logical_name_id
        FROM normalized_events event
        JOIN chain_lineage lineage ON (lineage.chain_id, lineage.block_hash, lineage.block_number) = (event.chain_id, event.block_hash, event.block_number)
        WHERE event.chain_id = $1 AND event.block_number BETWEEN $2 AND $3
          AND event.logical_name_id IS NOT NULL
          AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
          AND event.event_kind = 'RegistrationReleased' AND event.after_state ->> 'source_event' = 'RegistryPathExpired'
          AND event.after_state ->> 'derived_from' = 'interpreter_state' AND event.after_state ->> 'terminal_reason' = 'registry_name_binding_expired'
          AND (event.canonicality_state = 'orphaned' OR lineage.canonicality_state = 'orphaned')
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to retain orphaned expiry name scope", error)
    })?;
    sqlx::query(
        r#"
        WITH affected_times AS (
            SELECT COALESCE((
                       SELECT extract(epoch FROM prior.block_timestamp)
                       FROM chain_lineage prior
                       WHERE prior.chain_id = $1
                         AND prior.block_number < $2
                         AND prior.canonicality_state IN ('canonical', 'safe', 'finalized')
                       ORDER BY prior.block_number DESC
                       LIMIT 1
                   ), -1::numeric) AS prior_seconds,
                   max(extract(epoch FROM affected.block_timestamp)) AS target_seconds
            FROM chain_lineage affected
            WHERE affected.chain_id = $1
              AND affected.block_number BETWEEN $2 AND $3
              AND affected.canonicality_state IN (
                  'canonical', 'safe', 'finalized', 'orphaned'
              )
        ), expiry_candidate_events AS MATERIALIZED (
            SELECT event.logical_name_id,
                   COALESCE(event.resource_id::text, (
                       SELECT linked.resource_id::text
                       FROM normalized_events linked
                       JOIN chain_lineage linked_lineage ON (linked_lineage.chain_id, linked_lineage.block_hash, linked_lineage.block_number) = (linked.chain_id, linked.block_hash, linked.block_number)
                       WHERE (linked.chain_id, linked.logical_name_id) = (event.chain_id, event.logical_name_id)
                         AND linked.block_number <= $4 AND linked.resource_id IS NOT NULL
                         AND linked.event_kind IN ('RegistrationGranted', 'RegistrationReserved')
                         AND linked.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
                         AND linked.canonicality_state IN ('canonical', 'safe', 'finalized')
                         AND linked_lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                         AND COALESCE(linked.after_state ->> 'registry_contract_instance_id', linked.raw_fact_ref ->> 'emitting_address', linked.after_state ->> 'registry') = COALESCE(event.after_state ->> 'registry_contract_instance_id', event.raw_fact_ref ->> 'emitting_address', event.after_state ->> 'registry')
                         AND linked.after_state ->> 'token_id' = event.after_state ->> 'token_id'
                       ORDER BY linked.block_number DESC NULLS LAST, linked.normalized_event_id DESC LIMIT 1
                   ), NULLIF(CONCAT(
                       COALESCE(event.after_state ->> 'registry_contract_instance_id',
                                event.raw_fact_ref ->> 'emitting_address',
                                event.after_state ->> 'registry'),
                       ':', event.after_state ->> 'token_id'), ':')) AS lifecycle_key
            FROM normalized_events event
            JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_hash = event.block_hash
             AND lineage.block_number = event.block_number
            CROSS JOIN affected_times affected
            WHERE event.chain_id = $1
              AND event.block_number <= $4
              AND event.logical_name_id IS NOT NULL
              AND event.source_family IN ('ens_v2_root_l1', 'ens_v2_registry_l1')
              AND event.event_kind IN (
                  'RegistrationGranted', 'RegistrationReserved',
                  'RegistrationRenewed', 'RegistrationReleased', 'ExpiryChanged'
              )
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND jsonb_typeof(event.after_state -> 'expiry') = 'number'
              AND (event.after_state ->> 'expiry')::numeric > affected.prior_seconds
              AND (event.after_state ->> 'expiry')::numeric <= affected.target_seconds
        ), candidate_lifecycles AS MATERIALIZED (
            SELECT DISTINCT event.logical_name_id, event.lifecycle_key
            FROM expiry_candidate_events event
            WHERE event.lifecycle_key IS NOT NULL
        ), registration_events AS (
            SELECT event.*, candidate.lifecycle_key
            FROM candidate_lifecycles candidate
            JOIN LATERAL (
                SELECT history.*
                FROM normalized_events history
                JOIN chain_lineage lineage
                  ON lineage.chain_id = history.chain_id
                 AND lineage.block_hash = history.block_hash
                 AND lineage.block_number = history.block_number
                WHERE history.chain_id = $1
                  AND history.block_number <= $4
                  AND history.logical_name_id = candidate.logical_name_id
                  AND history.source_family IN (
                      'ens_v2_root_l1', 'ens_v2_registry_l1'
                  )
                  AND history.event_kind IN (
                      'RegistrationGranted', 'RegistrationReserved',
                      'RegistrationRenewed', 'RegistrationReleased', 'ExpiryChanged'
                  )
                  AND history.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND COALESCE(history.resource_id::text, NULLIF(CONCAT(
                      COALESCE(history.after_state ->> 'registry_contract_instance_id',
                               history.raw_fact_ref ->> 'emitting_address',
                               history.after_state ->> 'registry'),
                      ':', history.after_state ->> 'token_id'), ':')) =
                      candidate.lifecycle_key
            ) event ON TRUE
        ), lifecycle_heads AS (
            SELECT DISTINCT ON (event.logical_name_id, event.lifecycle_key)
                   event.logical_name_id, event.lifecycle_key, event.event_kind,
                   event.after_state
            FROM registration_events event
            WHERE event.lifecycle_key IS NOT NULL
              AND event.after_state ->> 'status' IN ('registered', 'reserved', 'released')
            ORDER BY event.logical_name_id, event.lifecycle_key, event.block_number DESC,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ), expiry_heads AS (
            SELECT DISTINCT ON (event.logical_name_id, event.lifecycle_key)
                   event.logical_name_id, event.lifecycle_key,
                   (event.after_state ->> 'expiry')::numeric AS expiry
            FROM registration_events event
            WHERE event.lifecycle_key IS NOT NULL
              AND jsonb_typeof(event.after_state -> 'expiry') = 'number'
            ORDER BY event.logical_name_id, event.lifecycle_key, event.block_number DESC,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        )
        INSERT INTO project_scope_expiry_names
        SELECT DISTINCT lifecycle.logical_name_id
        FROM lifecycle_heads lifecycle
        JOIN expiry_heads expiry USING (logical_name_id, lifecycle_key)
        CROSS JOIN affected_times affected
        WHERE lifecycle.after_state ->> 'status' IN ('registered', 'reserved')
          AND ((expiry.expiry > affected.prior_seconds
                AND expiry.expiry <= affected.target_seconds)
               OR EXISTS (SELECT 1 FROM registration_events changed
                           WHERE changed.logical_name_id = lifecycle.logical_name_id
                             AND changed.lifecycle_key = lifecycle.lifecycle_key
                             AND changed.block_number BETWEEN $2 AND $3))
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .bind(target_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to retain expiry name scope", error))?;
    sqlx::query(
        "INSERT INTO project_scope_names
         SELECT logical_name_id FROM project_scope_expiry_names
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to install expiry name scope", error))?;
    Ok(())
}

async fn seed_children(transaction: &mut Transaction<'_, Postgres>, chain_id: &str) -> Result<()> {
    sqlx::query(
        r#"
        WITH citations AS (
            SELECT row.parent_logical_name_id, row.child_logical_name_id,
                   citation.event_id
            FROM children_current row
            CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(
                row.provenance -> 'normalized_event_ids', '[]'::jsonb
            )) citation(event_id)
            WHERE row.provenance ->> 'chain_id' = $1
        ), retracted AS (
            SELECT citation.parent_logical_name_id, citation.child_logical_name_id
            FROM citations citation
            WHERE citation.event_id IS NOT NULL
              AND citation.event_id NOT IN ('', 'null')
              AND NOT EXISTS (
                  SELECT 1 FROM normalized_events event
                  LEFT JOIN chain_lineage lineage
                    ON lineage.chain_id = event.chain_id
                   AND lineage.block_hash = event.block_hash
                   AND lineage.block_number = event.block_number
                  WHERE event.normalized_event_id = citation.event_id::bigint
                    AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                    AND (
                        (event.block_number IS NULL AND event.block_hash IS NULL)
                        OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                    )
              )
        )
        INSERT INTO project_scope_children
        SELECT DISTINCT logical_name_id
        FROM retracted
        CROSS JOIN LATERAL (
            VALUES (parent_logical_name_id), (child_logical_name_id)
        ) candidate(logical_name_id)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to retain retracted child scope", error))?;
    Ok(())
}

async fn seed_resources(transaction: &mut Transaction<'_, Postgres>, chain_id: &str) -> Result<()> {
    sqlx::query(
        r#"
        WITH citations AS (
            SELECT row.resource_id, citation.event_id, false AS force_scope
            FROM permissions_current row
            CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(
                row.provenance -> 'normalized_event_ids', '[]'::jsonb
            )) citation(event_id)
            WHERE row.provenance ->> 'chain_id' = $1
            UNION ALL
            SELECT row.resource_id, citation.event_id, false
            FROM record_inventory_current row
            CROSS JOIN LATERAL jsonb_array_elements_text(
                COALESCE(row.provenance -> 'record_event_ids', '[]'::jsonb)
                || jsonb_build_array(
                    row.provenance -> 'resolver_pointer_event_id',
                    row.record_version_boundary -> 'normalized_event_id',
                    row.last_change -> 'normalized_event_id'
                )
            ) citation(event_id)
            WHERE row.provenance ->> 'chain_id' = $1
            UNION ALL
            SELECT row.resource_id, row.provenance ->> 'normalized_event_id', false
            FROM address_names_current row
            WHERE row.provenance ->> 'chain_id' = $1 AND row.resource_id IS NOT NULL
            UNION ALL
            SELECT row.resource_id, citation.event_id, false
            FROM permissions_current_resource_summary row
            CROSS JOIN LATERAL (VALUES
                (row.provenance ->> 'authority_event_id'),
                (row.provenance -> 'wrapper_expiry_boundary' ->> 'fuses_event_id'),
                (row.provenance -> 'wrapper_expiry_boundary' ->> 'expiry_event_id'),
                (row.provenance ->> 'expiry_retirement_event_id')
            ) citation(event_id)
            WHERE row.provenance ->> 'chain_id' = $1
        )
        INSERT INTO project_scope_resources
        SELECT DISTINCT citation.resource_id
        FROM citations citation
        WHERE citation.force_scope OR (
            citation.event_id IS NOT NULL
            AND citation.event_id NOT IN ('', 'null')
            AND NOT EXISTS (
                SELECT 1 FROM normalized_events event
                LEFT JOIN chain_lineage lineage
                  ON lineage.chain_id = event.chain_id
                 AND lineage.block_hash = event.block_hash
                 AND lineage.block_number = event.block_number
                WHERE event.normalized_event_id = citation.event_id::bigint
                  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND (
                      (event.block_number IS NULL AND event.block_hash IS NULL)
                      OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                  )
            )
        )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to retain retracted resource scope", error))?;
    Ok(())
}

async fn seed_resolvers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        WITH citations AS (
            SELECT row.resolver_address, citation.event_id
            FROM resolver_current row
            CROSS JOIN LATERAL (
                VALUES (row.provenance ->> 'manifest_event_id'),
                       (row.provenance ->> 'upgrade_event_id')
            ) citation(event_id)
            WHERE row.chain_id = $1
        )
        INSERT INTO project_scope_resolvers
        SELECT DISTINCT lower(citation.resolver_address)
        FROM citations citation
        WHERE citation.event_id IS NOT NULL
          AND citation.event_id NOT IN ('', 'null')
          AND NOT EXISTS (
              SELECT 1 FROM normalized_events event
              LEFT JOIN chain_lineage lineage
                ON lineage.chain_id = event.chain_id
               AND lineage.block_hash = event.block_hash
               AND lineage.block_number = event.block_number
              WHERE event.normalized_event_id = citation.event_id::bigint
                AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                AND (
                    (event.block_number IS NULL AND event.block_hash IS NULL)
                    OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                )
          )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to retain retracted resolver scope", error))?;

    sqlx::query(
        r#"
        WITH old_candidates AS (
            SELECT evidence.event_identity,
                   lower(candidate.resolver_address) AS resolver_address,
                   CASE
                       WHEN evidence.source_family LIKE 'ens_v2_%'
                           THEN 'ens_v2_resolver_l1'
                       WHEN evidence.source_family LIKE 'basenames_%'
                           THEN 'basenames_base_resolver'
                       ELSE 'ens_v1_resolver_l1'
                   END AS source_family,
                   evidence.event_kind
            FROM project_redo_resolver_evidence evidence
            CROSS JOIN LATERAL (VALUES
                (evidence.before_resolver_address),
                (evidence.after_resolver_address)
            ) candidate(resolver_address)
            WHERE evidence.chain_id = $1
              AND evidence.block_number BETWEEN $2 AND $3
              AND candidate.resolver_address IS NOT NULL
        ), retracted AS (
            SELECT old.resolver_address, old.source_family, old.event_kind
            FROM old_candidates old
            WHERE NOT EXISTS (
                SELECT 1
                FROM normalized_events event
                LEFT JOIN chain_lineage lineage
                  ON lineage.chain_id = event.chain_id
                 AND lineage.block_hash = event.block_hash
                 AND lineage.block_number = event.block_number
                CROSS JOIN LATERAL (VALUES
                    (CASE WHEN event.event_kind = 'ResolverChanged'
                          THEN event.before_state ->> 'resolver' END),
                    (CASE WHEN event.event_kind = 'ResolverChanged'
                          THEN event.after_state ->> 'resolver' END),
                    (CASE WHEN event.event_kind = 'AliasChanged'
                          THEN COALESCE(
                              event.before_state ->> 'resolver',
                              event.raw_fact_ref ->> 'emitting_address'
                          ) END),
                    (CASE WHEN event.event_kind = 'AliasChanged'
                          THEN COALESCE(
                              event.after_state ->> 'resolver',
                              event.raw_fact_ref ->> 'emitting_address'
                          ) END),
                    (CASE WHEN event.event_kind = 'PermissionChanged'
                               AND event.before_state #>> '{scope,kind}' = 'resolver'
                          THEN event.before_state #>> '{scope,resolver_address}' END),
                    (CASE WHEN event.event_kind = 'PermissionChanged'
                               AND event.after_state #>> '{scope,kind}' = 'resolver'
                          THEN event.after_state #>> '{scope,resolver_address}' END)
                ) current(resolver_address)
                WHERE event.chain_id = $1
                  AND event.event_identity = old.event_identity
                  AND event.consumer_visibility = 'activated'
                  AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                  AND lower(current.resolver_address) = old.resolver_address
                  AND CASE
                          WHEN event.source_family LIKE 'ens_v2_%'
                              THEN 'ens_v2_resolver_l1'
                          WHEN event.source_family LIKE 'basenames_%'
                              THEN 'basenames_base_resolver'
                          ELSE 'ens_v1_resolver_l1'
                      END = old.source_family
            )
        )
        INSERT INTO project_scope_retracted_resolver_evidence
        SELECT DISTINCT resolver_address, source_family, event_kind FROM retracted
        WHERE resolver_address <> '0x0000000000000000000000000000000000000000'
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to scope resolver references retracted by redo",
            error,
        )
    })?;
    sqlx::query(
        "INSERT INTO project_scope_resolvers
         SELECT DISTINCT resolver_address
         FROM project_scope_retracted_resolver_evidence
         ON CONFLICT DO NOTHING",
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to rebuild resolvers with retracted evidence", error)
    })?;
    sqlx::query(
        r#"
        INSERT INTO project_scope_resources
        SELECT DISTINCT evidence.resource_id
        FROM project_redo_resolver_evidence evidence
        CROSS JOIN LATERAL (VALUES
            (evidence.before_resolver_address),
            (evidence.after_resolver_address)
        ) candidate(resolver_address)
        JOIN project_scope_retracted_resolver_evidence retracted
          ON retracted.resolver_address = lower(candidate.resolver_address)
         AND retracted.event_kind = evidence.event_kind
         AND retracted.source_family = CASE
                 WHEN evidence.source_family LIKE 'ens_v2_%'
                     THEN 'ens_v2_resolver_l1'
                 WHEN evidence.source_family LIKE 'basenames_%'
                     THEN 'basenames_base_resolver'
                 ELSE 'ens_v1_resolver_l1'
             END
        WHERE evidence.chain_id = $1
          AND evidence.block_number BETWEEN $2 AND $3
          AND evidence.event_kind = 'PermissionChanged'
          AND evidence.resource_id IS NOT NULL
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to rebuild resources with retracted permissions",
            error,
        )
    })?;
    Ok(())
}

pub(super) async fn consume(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM project_redo_resolver_evidence
         WHERE chain_id = $1 AND block_number BETWEEN $2 AND $3",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database(
            "failed to consume resolver evidence during Project publication",
            error,
        )
    })?;
    Ok(())
}

async fn seed_primary(transaction: &mut Transaction<'_, Postgres>, chain_id: &str) -> Result<()> {
    sqlx::query(
        r#"
        WITH citations AS (
            SELECT row.address, row.coin_type, row.namespace, citation.event_id
            FROM primary_names_current row
            CROSS JOIN LATERAL (
                VALUES (row.claim_provenance ->> 'reverse_event_id'),
                       (row.claim_provenance ->> 'claim_event_id'),
                       (row.claim_provenance ->> 'resolver_event_id')
            ) citation(event_id)
            WHERE row.claim_provenance ->> 'chain_id' = $1
        )
        INSERT INTO project_scope_primary (address, coin_type, namespace)
        SELECT DISTINCT citation.address, citation.coin_type, citation.namespace
        FROM citations citation
        WHERE citation.event_id IS NOT NULL
          AND citation.event_id NOT IN ('', 'null')
          AND NOT EXISTS (
              SELECT 1 FROM normalized_events event
              LEFT JOIN chain_lineage lineage
                ON lineage.chain_id = event.chain_id
               AND lineage.block_hash = event.block_hash
               AND lineage.block_number = event.block_number
              WHERE event.normalized_event_id = citation.event_id::bigint
                AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
                AND (
                    (event.block_number IS NULL AND event.block_hash IS NULL)
                    OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                )
          )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(chain_id)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        ProjectError::database("failed to retain retracted primary-name scope", error)
    })?;
    Ok(())
}
