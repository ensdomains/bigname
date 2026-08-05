use std::collections::BTreeMap;

use bigname_storage::{
    SelectedSnapshot, SnapshotConsistency, SnapshotSelectionError, SnapshotSelectionErrorKind,
    SnapshotSelectionScope, SnapshotSelectorInput, resolve_exact_name_snapshot_selection,
    snapshot_chain_has_head,
};
use sqlx::PgPool;

use crate::v2::{
    Meta, SnapshotReadResource, V2Error, V2Result, sanitized_snapshot_internal_error, snapshot_meta,
};

pub(super) struct ServedHead {
    selected: SelectedSnapshot,
    project_generations: BTreeMap<String, String>,
}

impl ServedHead {
    pub(super) fn selected(&self) -> &SelectedSnapshot {
        &self.selected
    }

    pub(super) fn meta(&self) -> V2Result<Meta> {
        snapshot_meta(&self.selected)
    }
}

pub(crate) async fn load_served_head(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
) -> V2Result<Option<ServedHead>> {
    let input = SnapshotSelectorInput::new(None, None, SnapshotConsistency::Head)
        .map_err(|_| V2Error::internal_error("failed to build lookup served head selector"))?;
    let selected = match resolve_exact_name_snapshot_selection(pool, scope, &input).await {
        Ok(selected) => selected,
        Err(error) if served_head_scope_conflict(&error) => {
            if served_head_absent_for_single_scope(pool, scope, &error).await? {
                return Ok(None);
            }
            return Err(V2Error::conflict(
                "served head is unavailable for snapshot scope",
            ));
        }
        Err(error) if error.kind() == SnapshotSelectionErrorKind::Stale => {
            return Err(V2Error::stale(
                "served data is not available at the current phase head",
            ));
        }
        Err(error) if error.kind() == SnapshotSelectionErrorKind::InvalidInput => {
            return Err(V2Error::internal_error(
                "failed to build lookup served head selector",
            ));
        }
        Err(error) => {
            return Err(sanitized_snapshot_internal_error(
                &error,
                SnapshotReadResource::Resource,
            ));
        }
    };
    let project_generations = load_project_generations(pool, &selected).await?;
    Ok(Some(ServedHead {
        selected,
        project_generations,
    }))
}

pub(super) async fn revalidate_served_head(pool: &PgPool, served: &ServedHead) -> V2Result<()> {
    let current = load_project_generations(pool, &served.selected).await?;
    if current != served.project_generations {
        return Err(V2Error::stale(
            "served data changed while the lookup was being read",
        ));
    }
    Ok(())
}

async fn load_project_generations(
    pool: &PgPool,
    selected: &SelectedSnapshot,
) -> V2Result<BTreeMap<String, String>> {
    let mut generations = BTreeMap::new();
    for position in selected.chain_positions.as_map().values() {
        let generation = sqlx::query_scalar::<_, String>(
            r#"
            SELECT xmin::TEXT
            FROM chain_phase_state
            WHERE chain_id = $1
              AND phase_name = 'project'
              AND phase_status = 'completed'
              AND current_block_number = $2
              AND current_block_hash = $3
              AND input_content_hash = $4
            "#,
        )
        .bind(&position.chain_id)
        .bind(position.block_number)
        .bind(&position.block_hash)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .fetch_optional(pool)
        .await
        .map_err(|_| V2Error::internal_error("failed to validate lookup project publication"))?
        .ok_or_else(|| V2Error::stale("served data is not available at the selected phase head"))?;
        generations.insert(position.chain_id.clone(), generation);
    }
    Ok(generations)
}

async fn served_head_absent_for_single_scope(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
    error: &SnapshotSelectionError,
) -> V2Result<bool> {
    if scope.required_positions().len() != 1 || error.kind() != SnapshotSelectionErrorKind::Conflict
    {
        return Ok(false);
    }
    let chain_id = &scope.required_positions()[0].chain_id;
    snapshot_chain_has_head(pool, chain_id)
        .await
        .map(|has_head| !has_head)
        .map_err(|error| sanitized_snapshot_internal_error(&error, SnapshotReadResource::Resource))
}

fn served_head_scope_conflict(error: &SnapshotSelectionError) -> bool {
    error.kind() == SnapshotSelectionErrorKind::Conflict
        || error.message().contains("mismatched hash and number")
}
