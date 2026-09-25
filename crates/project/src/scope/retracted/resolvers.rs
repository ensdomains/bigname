use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(super) async fn seed_resolvers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(
        r#"/* project:scope.retracted.resolvers.seed_resolvers.insert_scope_resolvers_from_resolver_current */
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
        r#"/* project:scope.retracted.resolvers.seed_resolvers.insert_scope_retracted_resolver_evidence */
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
        "/* project:scope.retracted.resolvers.seed_resolvers.insert_scope_resolvers_from_scope_retracted_resolver_evidence */ INSERT INTO project_scope_resolvers
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
        r#"/* project:scope.retracted.resolvers.seed_resolvers.insert_scope_resources */
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

/// A record-ID resolver's link section cites no event redo can check -- a node with
/// no current name or resource consumer leaves nothing else to scope the resolver --
/// so compare the section's digest with the canonical link set at the target and
/// rebuild every resolver whose set changed.
pub(super) async fn seed_relinked_resolvers(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<()> {
    // One pass over the chain's link events, grouped by resolver, then compared
    // with every row: the cost is the link history once, not once per resolver.
    // The emitter predicate is the indexed one (normalized_events_emitter_history_idx);
    // a record-ID resolver emits its own Linked logs.
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L363-L367 @ ens_v2@a971bd64)
    let statement = format!(
        r#"/* project:scope.retracted.resolvers.seed_relinked_resolvers */
        WITH latest AS (
            SELECT DISTINCT ON (lower(event.raw_fact_ref ->> 'emitting_address'),
                                lower(event.after_state ->> 'node'))
                   lower(event.raw_fact_ref ->> 'emitting_address') AS resolver_address,
                   lower(event.after_state ->> 'node') AS node,
                   event.after_state ->> 'resolver_record_id' AS record_id,
                   event.event_identity
            FROM normalized_events event
            JOIN chain_lineage lineage
              ON lineage.chain_id = event.chain_id
             AND lineage.block_hash = event.block_hash
             AND lineage.block_number = event.block_number
            WHERE event.chain_id = $1
              AND event.event_kind = 'ResolverRecordLinked'
              AND event.after_state ->> 'storage_model' = 'resolver_record_id'
              AND event.raw_fact_ref ->> 'emitting_address' IS NOT NULL
              AND lower(event.after_state ->> 'resolver') =
                  lower(event.raw_fact_ref ->> 'emitting_address')
              AND event.consumer_visibility = 'activated'
              AND event.block_number <= $2
              AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
              AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
            ORDER BY lower(event.raw_fact_ref ->> 'emitting_address'),
                     lower(event.after_state ->> 'node'),
                     event.block_number DESC NULLS LAST,
                     event.transaction_index DESC NULLS LAST,
                     event.log_index DESC NULLS LAST,
                     event.normalized_event_id DESC
        ),
        digests AS (
            SELECT resolver_address, {digest} AS digest
            FROM latest
            WHERE record_id <> '0'
            GROUP BY resolver_address
        )
        INSERT INTO project_scope_resolvers
        SELECT lower(row.resolver_address)
        FROM resolver_current row
        LEFT JOIN digests ON digests.resolver_address = lower(row.resolver_address)
        WHERE row.chain_id = $1
          AND row.declared_summary #>> '{{links,status}}' = 'supported'
          AND row.declared_summary #>> '{{links,digest}}' IS DISTINCT FROM
              COALESCE(digests.digest, md5(''))
        ON CONFLICT DO NOTHING
        "#,
        digest = crate::builders::LINK_DIGEST_SQL
    );
    sqlx::query(&statement)
        .bind(chain_id)
        .bind(target_block)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to retain relinked resolver scope", error)
        })?;
    Ok(())
}
