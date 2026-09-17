use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

/// The empty-name node, `namehash("")`: a record linked there is the resolver's
/// default record, answering any node with no link of its own.
/// (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L380-L386 @ ens_v2@a971bd64)
pub(crate) const DEFAULT_RECORD_NODE: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

/// Stage the latest `Linked` observation per resolver node, then summarize the
/// nodes bound to a non-zero record per resolver. `project_resolver_links` is
/// also what record selection reads, so the two never disagree on which link
/// is current. Record ID `0` is the unlinked state.
/// (upstream: .refs/ens_v2/contracts/src/resolver/interfaces/IRecordResolver.sol:L32-L38 @ ens_v2@a971bd64)
pub(super) async fn stage(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    sample_limit: i32,
) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TEMP TABLE project_resolver_links ON COMMIT DROP AS
        SELECT DISTINCT ON (chain_id, lower(after_state ->> 'resolver'),
                            lower(after_state ->> 'node')) event.*
        FROM project_events event
        WHERE event_kind = 'ResolverRecordLinked'
          AND after_state ->> 'storage_model' = 'resolver_record_id'
        ORDER BY chain_id, lower(after_state ->> 'resolver'),
                 lower(after_state ->> 'node'), block_number DESC NULLS LAST,
                 transaction_index DESC NULLS LAST, log_index DESC NULLS LAST,
                 normalized_event_id DESC
        "#,
    )
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to select resolver links", error))?;
    sqlx::query(
        r#"
        CREATE TEMP TABLE project_resolver_link_summary ON COMMIT DROP AS
        WITH active_links AS (
            SELECT lower(link.after_state ->> 'resolver') AS resolver_address,
                   lower(link.after_state ->> 'node') AS node,
                   link.after_state ->> 'resolver_record_id' AS record_id,
                   link.normalized_event_id,
                   link.block_number,
                   link.block_hash,
                   link.transaction_hash,
                   link.log_index,
                   link.chain_id
            FROM project_resolver_links link
            WHERE link.chain_id = $1
              AND link.after_state ->> 'resolver_record_id' <> '0'
        ),
        named_nodes AS (
            SELECT DISTINCT ON (lower(surface.namehash))
                   lower(surface.namehash) AS node,
                   surface.logical_name_id,
                   surface.raw_name,
                   surface.namespace
            FROM project_surfaces surface
            ORDER BY lower(surface.namehash), surface.namespace, surface.logical_name_id
        ),
        items AS (
            SELECT link.resolver_address,
                   link.record_id,
                   link.node,
                   jsonb_strip_nulls(jsonb_build_object(
                       'record_id', link.record_id,
                       'namehash', link.node,
                       'default', link.node = $3,
                       'logical_name_id', named.logical_name_id,
                       'name', named.raw_name,
                       'namespace', named.namespace,
                       'normalized_event_id', link.normalized_event_id,
                       'chain_position', jsonb_strip_nulls(jsonb_build_object(
                           'chain_id', link.chain_id,
                           'block_number', link.block_number,
                           'block_hash', link.block_hash,
                           'transaction_hash', link.transaction_hash,
                           'log_index', link.log_index,
                           'timestamp', to_char(lineage.block_timestamp AT TIME ZONE 'UTC',
                                                'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                       ))
                   )) AS item,
                   row_number() OVER (
                       PARTITION BY link.resolver_address
                       ORDER BY lpad(link.record_id, 78, '0'), link.node
                   ) AS sample_rank
            FROM active_links link
            LEFT JOIN named_nodes named USING (node)
            LEFT JOIN chain_lineage lineage
              ON lineage.chain_id = link.chain_id
             AND lineage.block_number = link.block_number
             AND lineage.block_hash = link.block_hash
        )
        SELECT resolver_address,
               count(*)::integer AS link_count,
               count(DISTINCT record_id)::integer AS record_count,
               COALESCE(jsonb_agg(item ORDER BY lpad(record_id, 78, '0'), node)
                   FILTER (WHERE sample_rank <= $2), '[]'::jsonb) AS items
        FROM items
        GROUP BY resolver_address
        "#,
    )
    .bind(chain_id)
    .bind(sample_limit)
    .bind(DEFAULT_RECORD_NODE)
    .execute(&mut **transaction)
    .await
    .map_err(|error| ProjectError::database("failed to group resolver links", error))?;
    Ok(())
}
