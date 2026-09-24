//! Read dependencies for mirrors, kept separate from publication scope.
use sqlx::{Postgres, Transaction};

use crate::{ProjectError, Result};

pub(crate) async fn create(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
    for sql in [
        "CREATE TEMP TABLE IF NOT EXISTS project_mirror_evidence_names(logical_name_id text PRIMARY KEY) ON COMMIT DROP",
        "CREATE TEMP TABLE IF NOT EXISTS project_mirror_evidence_nodes(namespace text, namehash text, PRIMARY KEY(namespace,namehash)) ON COMMIT DROP",
        "CREATE TEMP TABLE IF NOT EXISTS project_mirror_evidence_events(normalized_event_id bigint PRIMARY KEY) ON COMMIT DROP",
        "CREATE TEMP TABLE IF NOT EXISTS project_mirror_evidence_resolvers(resolver_address text PRIMARY KEY) ON COMMIT DROP",
    ] {
        sqlx::query(sql)
            .execute(&mut **tx)
            .await
            .map_err(|e| ProjectError::database("failed to create mirror input dependencies", e))?;
    }
    Ok(())
}

/// Every eligible historical ENSv1 registry-side pointer, clears included, for the consulted
/// nodes. The node is the name the event addresses, read in the adapters' shared order
/// (`V1_EVENT_NODE_FIELDS` in `crates/adapters/src/schema_v2/seam.rs`), which
/// `normalized_events_project_v1_pointer_addressed_node_idx` keys.
pub(crate) const EVIDENCE_EVENTS_SQL: &str = "INSERT INTO project_mirror_evidence_events
         SELECT event.normalized_event_id
         FROM project_mirror_evidence_nodes node
         JOIN normalized_events event
           ON event.namespace=node.namespace
          AND lower(COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node'))=node.namehash
         JOIN chain_lineage lineage
           ON lineage.chain_id=event.chain_id AND lineage.block_number=event.block_number
          AND lineage.block_hash=event.block_hash
         WHERE event.chain_id=$1 AND event.block_number <= $2
           AND event.event_kind='ResolverChanged'
           AND COALESCE(event.after_state ->> 'child_node', event.after_state ->> 'namehash', event.after_state ->> 'node') IS NOT NULL
           AND event.source_family IN ('ens_v1_registry_l1','ens_v1_registrar_l1','ens_v1_wrapper_l1')
           AND event.consumer_visibility='activated'
           AND event.canonicality_state IN ('canonical','safe','finalized')
           AND lineage.canonicality_state IN ('canonical','safe','finalized')
         ON CONFLICT DO NOTHING";

pub(crate) async fn resolve_inputs(
    tx: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    target_block: i64,
) -> Result<bool> {
    sqlx::query("ANALYZE project_mirror_evidence_nodes")
        .execute(&mut **tx)
        .await
        .map_err(|e| ProjectError::database("failed to analyze mirror input nodes", e))?;
    // Preserve all historical pointers, including clears and unlinked node pointers.
    // The shared event stage applies its serving eligibility filter again.
    for sql in [
        EVIDENCE_EVENTS_SQL,
        "INSERT INTO project_mirror_evidence_resolvers
         SELECT DISTINCT lower(candidate.address)
         FROM project_mirror_evidence_events evidence
         JOIN normalized_events event USING(normalized_event_id)
         CROSS JOIN LATERAL (VALUES (event.after_state ->> 'resolver'),
                                   (event.before_state ->> 'resolver')) candidate(address)
         WHERE candidate.address IS NOT NULL AND btrim(candidate.address) <> ''
           AND lower(candidate.address) <> '0x0000000000000000000000000000000000000000'
           AND $1::text IS NOT NULL AND $2::bigint IS NOT NULL
         ON CONFLICT DO NOTHING",
    ] {
        sqlx::query(sql)
            .bind(chain_id)
            .bind(target_block)
            .execute(&mut **tx)
            .await
            .map_err(|e| {
                ProjectError::database("failed to resolve mirror input dependencies", e)
            })?;
    }
    // These keys must bypass passthrough and invalidate direct as well as mirrored users.
    let inserted = sqlx::query(
        "WITH invalid AS MATERIALIZED (
         SELECT evidence.resolver_address FROM project_mirror_evidence_resolvers evidence
         WHERE NOT EXISTS (
             SELECT 1 FROM resolver_current current
             JOIN chain_lineage lineage
               ON lineage.chain_id=current.chain_id
              AND lineage.block_number=(current.chain_positions ->> 'target_block_number')::bigint
              AND lineage.block_hash=current.chain_positions ->> 'target_block_hash'
             WHERE current.chain_id=$1
               AND lower(current.resolver_address)=evidence.resolver_address
               AND lineage.block_number <= $2
               AND lineage.canonicality_state IN ('canonical','safe','finalized')
         )), added AS (
             INSERT INTO project_scope_resolvers SELECT resolver_address FROM invalid
             ON CONFLICT DO NOTHING
         )
         INSERT INTO project_scope_resolver_dependents SELECT resolver_address FROM invalid
         ON CONFLICT DO NOTHING",
    )
    .bind(chain_id)
    .bind(target_block)
    .execute(&mut **tx)
    .await
    .map_err(|e| ProjectError::database("failed to require fresh mirror classification", e))?
    .rows_affected();
    Ok(inserted > 0)
}

pub(crate) async fn invalidate_resolver_dependents(
    tx: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    #[cfg(test)]
    if crate::reference::enabled(tx).await? {
        return Ok(());
    }
    sqlx::query(
        "WITH affected AS MATERIALIZED (
             SELECT inventory.resource_id, inventory.provenance ->> 'logical_name_id' AS logical_name_id
             FROM project_scope_resolver_dependents changed
             JOIN record_inventory_current inventory
               ON lower(inventory.provenance #>> '{mirror,mirrored_resolver_address}')=
                  lower(changed.resolver_address)
             WHERE inventory.provenance ->> 'chain_id'=$1
         ), names AS (
             INSERT INTO project_scope_names
             SELECT logical_name_id FROM affected WHERE logical_name_id IS NOT NULL
             ON CONFLICT DO NOTHING
         )
         INSERT INTO project_scope_resources SELECT resource_id FROM affected
         ON CONFLICT DO NOTHING",
    ).bind(chain_id).execute(&mut **tx).await.map_err(|e| {
        ProjectError::database("failed to invalidate changed mirror classifications", e)
    })?;
    Ok(())
}

pub(crate) async fn classifications(
    tx: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<()> {
    // Only these documented classification fields may cross from an unaffected row.
    // Retained target metadata and record values are not inputs to a rebuilt mirror.
    sqlx::query(
        "CREATE TEMP TABLE project_mirror_resolver_classifications ON COMMIT DROP AS
         SELECT chain_id, resolver_address,
                jsonb_build_object('classification', declared_summary -> 'classification') AS declared_summary,
                support_status, unsupported_reason, manifest_version,
                jsonb_build_object('manifest_id', provenance -> 'manifest_id') AS provenance
         FROM project_stage_resolver_current
         UNION ALL
         SELECT current.chain_id, current.resolver_address,
                jsonb_build_object('classification', current.declared_summary -> 'classification'),
                current.support_status, current.unsupported_reason, current.manifest_version,
                jsonb_build_object('manifest_id', current.provenance -> 'manifest_id')
         FROM resolver_current current
         JOIN project_mirror_evidence_resolvers evidence
           ON evidence.resolver_address=lower(current.resolver_address)
         WHERE current.chain_id=$1
           AND NOT EXISTS (SELECT 1 FROM project_scope_resolvers scope
                           WHERE lower(scope.resolver_address)=evidence.resolver_address)
           AND NOT EXISTS (SELECT 1 FROM project_stage_resolver_current staged
                           WHERE staged.chain_id=current.chain_id
                             AND lower(staged.resolver_address)=evidence.resolver_address)",
    ).bind(chain_id).execute(&mut **tx).await.map_err(|e| {
        ProjectError::database("failed to stage mirror classification inputs", e)
    })?;
    Ok(())
}

/// Keep the deployed reference query source literal. Candidate inventory reads its
/// documented classification inputs; historical direct-pointer attribution keeps
/// its inner stage join (the mirror walk also replaces its inner classification join).
pub(crate) async fn classification_sql<'a>(
    _tx: &mut Transaction<'_, Postgres>,
    statement: &'a str,
    include_inner_joins: bool,
) -> Result<std::borrow::Cow<'a, str>> {
    #[cfg(test)]
    if crate::reference::enabled(_tx).await? {
        return Ok(std::borrow::Cow::Borrowed(statement));
    }
    let join = if include_inner_joins {
        "JOIN"
    } else {
        "LEFT JOIN"
    };
    Ok(std::borrow::Cow::Owned(statement.replace(
        &format!("{join} project_stage_resolver_current resolver"),
        &format!("{join} project_mirror_resolver_classifications resolver"),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    #[tokio::test]
    async fn reference_classification_query_stays_literal_and_candidate_uses_input_rows()
    -> anyhow::Result<()> {
        let database =
            TestDatabase::create(TestDatabaseConfig::new("mirror_classification_source")).await?;
        let mut tx = database.pool().begin().await?;
        sqlx::raw_sql("CREATE TEMP TABLE project_stage_resolver_current(resolver_address text,support_status text);
            CREATE TEMP TABLE project_mirror_resolver_classifications(LIKE project_stage_resolver_current);
            INSERT INTO project_stage_resolver_current VALUES('0x1','published-stage');
            INSERT INTO project_mirror_resolver_classifications VALUES('0x1','classification-input')")
            .execute(&mut *tx).await?;
        let original = "SELECT resolver.support_status FROM (VALUES ('0x1')) pointer(address) LEFT JOIN project_stage_resolver_current resolver ON resolver.resolver_address=pointer.address";
        let candidate = classification_sql(&mut tx, original, false).await?;
        let value: String = sqlx::query_scalar(&candidate).fetch_one(&mut *tx).await?;
        assert_eq!(value, "classification-input");
        let inner = original.replace("LEFT JOIN", "JOIN");
        assert_eq!(classification_sql(&mut tx, &inner, false).await?, inner);
        assert!(
            classification_sql(&mut tx, &inner, true)
                .await?
                .contains("project_mirror_resolver_classifications")
        );
        sqlx::query("SET LOCAL bigname.benchmark_reference='on'")
            .execute(&mut *tx)
            .await?;
        for source in [original, inner.as_str()] {
            let reference = classification_sql(&mut tx, source, true).await?;
            assert_eq!(reference, source);
            let value: String = sqlx::query_scalar(&reference).fetch_one(&mut *tx).await?;
            assert_eq!(value, "published-stage");
        }
        tx.rollback().await?;
        database.cleanup().await?;
        Ok(())
    }
}
