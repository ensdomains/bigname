//! Independent, deliberately expensive directional mirror oracle. Never builds outputs.
use super::Scopes;
use crate::{BatchRequest, Marker, ProjectError, RunMode};
use anyhow::{Result, ensure};
use sqlx::{Acquire, Postgres, Transaction};

pub(crate) async fn enabled(tx: &mut Transaction<'_, Postgres>) -> crate::Result<bool> {
    let audit: bool = sqlx::query_scalar(
        "SELECT COALESCE(current_setting('bigname.contract_audit',true),'')='on'",
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| ProjectError::database("read contract audit mode", e))?;
    if audit && !crate::reference::enabled(tx).await? {
        return Err(ProjectError::transient(
            "contract audit requires literal reference mode",
        ));
    }
    Ok(audit)
}

pub(crate) async fn mirror_stage(
    tx: &mut Transaction<'_, Postgres>,
    chain: &str,
    target: i64,
) -> crate::Result<()> {
    // Literal deployed full graph, independently constructed without candidate frontiers.
    for sql in [include_str!("../testdata/sql/scope/mirror_reference/mirror.sql"), include_str!("../testdata/sql/scope/mirror_reference/mirror_bulk.sql"),
        "CREATE TEMP TABLE project_contract_audit_input_names(logical_name_id text PRIMARY KEY) ON COMMIT DROP;
         CREATE TEMP TABLE project_contract_audit_input_resources(resource_id uuid PRIMARY KEY) ON COMMIT DROP;
         CREATE TEMP TABLE project_contract_audit_changed_nodes ON COMMIT DROP AS SELECT * FROM project_mirror_changed_nodes"] {
        crate::reference::execute(tx,chain,target,None,sql).await?;
    }
    Ok(())
}

pub(crate) async fn mirror_include(tx: &mut Transaction<'_, Postgres>) -> crate::Result<()> {
    crate::reference::execute(tx,"",0,None,
        "INSERT INTO project_scope_resources
         SELECT DISTINCT pair.mirror_resource_id FROM project_mirror_pairs pair
         WHERE pair.changed
            OR EXISTS (SELECT 1 FROM project_scope_names scope WHERE scope.logical_name_id=pair.consulted_logical_name_id)
            OR EXISTS (SELECT 1 FROM project_scope_resources scope WHERE scope.resource_id=pair.consulted_resource_id)
         ON CONFLICT DO NOTHING;
         INSERT INTO project_contract_audit_input_names
         SELECT DISTINCT pair.consulted_logical_name_id FROM project_mirror_pairs pair
         JOIN project_scope_resources scope ON scope.resource_id=pair.mirror_resource_id ON CONFLICT DO NOTHING;
         INSERT INTO project_contract_audit_input_resources
         SELECT DISTINCT pair.consulted_resource_id FROM project_mirror_pairs pair
         JOIN project_scope_resources scope ON scope.resource_id=pair.mirror_resource_id
         WHERE pair.consulted_resource_id IS NOT NULL ON CONFLICT DO NOTHING").await
}

pub(crate) async fn mirror_finish(tx: &mut Transaction<'_, Postgres>) -> crate::Result<()> {
    crate::reference::execute(
        tx,
        "",
        0,
        None,
        "ALTER TABLE project_mirror_pairs RENAME TO project_contract_audit_pairs",
    )
    .await
}

pub(in crate::engine) async fn capture(
    tx: &mut Transaction<'_, Postgres>,
    request: &BatchRequest,
    target: &Marker,
) -> Result<Scopes> {
    ensure!(
        request.chain_id == "ethereum-sepolia"
            && request.mode == RunMode::Normal
            && request.resume_current.is_some(),
        "contract audit supports only normal incremental Sepolia"
    );
    // SQLx nested transactions restore GUCs, retracted-pointer consumption and temp tables
    // on success, failure and cancellation. Only the immutable scope leaves this branch.
    let mut branch = tx.begin().await?;
    sqlx::query("SET LOCAL bigname.benchmark_reference='on'")
        .execute(&mut *branch)
        .await?;
    sqlx::query("SET LOCAL bigname.contract_audit='on'")
        .execute(&mut *branch)
        .await?;
    crate::stage::prepare(&mut branch, &request.chain_id, target).await?;
    crate::scope::initialize(
        &mut branch,
        &request.chain_id,
        target,
        crate::scope::Window {
            previous: request.resume_current.as_ref(),
            from_block: request.affected_from_block,
            to_block: request.affected_to_block,
            full_rebuild: false,
            retain_retracted: false,
        },
    )
    .await?;
    reject_unsupported(&mut branch, request, target).await?;
    let scope = Scopes::capture(&mut branch).await?;
    branch.rollback().await?;
    ensure!(
        !enabled(tx).await?,
        "contract audit mode leaked across savepoint"
    );
    Ok(scope)
}

pub(super) async fn reject_unsupported(
    tx: &mut Transaction<'_, Postgres>,
    request: &BatchRequest,
    target: &Marker,
) -> Result<()> {
    // Newly discovered classification-only dependencies are not in the old expected
    // semantics. Reject them explicitly rather than letting an incomplete old audit
    // authorize retention. This can be widened only with a separate content oracle.
    let unsupported: bool = sqlx::query_scalar(
        "SELECT EXISTS (
          SELECT 1 FROM project_scope_resolver_dependents changed
          JOIN record_inventory_current inventory
            ON lower(inventory.provenance #>> '{mirror,mirrored_resolver_address}')=lower(changed.resolver_address)
          WHERE inventory.provenance ->> 'chain_id'='ethereum-sepolia'
            AND NOT EXISTS (SELECT 1 FROM project_scope_resources required WHERE required.resource_id=inventory.resource_id))")
        .fetch_one(&mut **tx).await?;
    ensure!(
        !unsupported,
        "contract audit does not yet support newly required classification-only mirror dependencies"
    );
    let invalid_input: bool = sqlx::query_scalar(
        "WITH nodes AS MATERIALIZED (
          SELECT DISTINCT surface.namespace,lower(surface.namehash) namehash
          FROM project_contract_audit_input_names consulted
          JOIN name_surfaces surface USING(logical_name_id)
          JOIN chain_lineage lineage ON lineage.chain_id=surface.chain_id AND lineage.block_number=surface.block_number AND lineage.block_hash=surface.block_hash
          WHERE surface.chain_id=$1 AND surface.block_number <= $2
            AND surface.canonicality_state IN ('canonical','safe','finalized') AND lineage.canonicality_state IN ('canonical','safe','finalized')
        ), addresses AS (
          SELECT DISTINCT lower(address) address FROM nodes
          JOIN normalized_events event ON event.namespace=nodes.namespace AND lower(event.after_state ->> 'node')=nodes.namehash
          JOIN chain_lineage lineage ON lineage.chain_id=event.chain_id AND lineage.block_number=event.block_number AND lineage.block_hash=event.block_hash
          CROSS JOIN LATERAL (VALUES(event.before_state ->> 'resolver'),(event.after_state ->> 'resolver')) pointer(address)
          WHERE event.chain_id=$1 AND event.block_number <= $2 AND event.event_kind='ResolverChanged'
            AND event.source_family IN ('ens_v1_registry_l1','ens_v1_registrar_l1','ens_v1_wrapper_l1')
            AND event.consumer_visibility='activated' AND event.canonicality_state IN ('canonical','safe','finalized') AND lineage.canonicality_state IN ('canonical','safe','finalized')
            AND address IS NOT NULL AND btrim(address)<>'' AND lower(address)<>'0x0000000000000000000000000000000000000000'
        ) SELECT EXISTS(SELECT 1 FROM addresses
          WHERE NOT EXISTS (SELECT 1 FROM resolver_current retained
              JOIN chain_lineage lineage ON lineage.chain_id=retained.chain_id AND lineage.block_number=(retained.chain_positions ->> 'target_block_number')::bigint AND lineage.block_hash=retained.chain_positions ->> 'target_block_hash'
              WHERE retained.chain_id=$1 AND lower(retained.resolver_address)=address AND lineage.block_number <= $3
                AND lineage.canonicality_state IN ('canonical','safe','finalized')))")
        .bind(&request.chain_id).bind(target.number).bind(request.resume_current.as_ref().unwrap().number)
        .fetch_one(&mut **tx).await?;
    ensure!(
        !invalid_input,
        "contract audit does not yet support missing/stale mirror input classifications"
    );
    Ok(())
}
