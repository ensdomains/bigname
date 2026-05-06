use super::*;

pub(super) async fn orphan_stale_overlapping_surface_bindings(
    pool: &PgPool,
    incoming: &[SurfaceBinding],
    existing: &mut Vec<SurfaceBinding>,
) -> Result<usize> {
    let candidates = stale_overlapping_surface_binding_candidates(incoming, existing);
    if candidates.is_empty() {
        return Ok(0);
    }

    let mut surface_binding_ids = Vec::with_capacity(candidates.len());
    let mut resource_ids = Vec::with_capacity(candidates.len());
    for (surface_binding_id, resource_id) in candidates {
        surface_binding_ids.push(surface_binding_id);
        resource_ids.push(resource_id);
    }

    let rows = sqlx::query_scalar::<_, Uuid>(
        r#"
        WITH candidate(surface_binding_id, resource_id) AS (
            SELECT *
            FROM unnest($1::UUID[], $2::UUID[])
        ),
        event_backed AS (
            SELECT DISTINCT normalized_events.resource_id
            FROM normalized_events
            JOIN candidate
              ON candidate.resource_id = normalized_events.resource_id
            WHERE normalized_events.canonicality_state IN ('canonical', 'safe', 'finalized')
        )
        UPDATE surface_bindings
        SET
            canonicality_state = 'orphaned'::canonicality_state,
            observed_at = now()
        FROM candidate
        WHERE surface_bindings.surface_binding_id = candidate.surface_binding_id
          AND surface_bindings.canonicality_state IN ('canonical', 'safe', 'finalized')
          AND NOT EXISTS (
              SELECT 1
              FROM event_backed
              WHERE event_backed.resource_id = candidate.resource_id
          )
        RETURNING surface_bindings.surface_binding_id
        "#,
    )
    .bind(&surface_binding_ids)
    .bind(&resource_ids)
    .fetch_all(pool)
    .await
    .context(
        "failed to orphan stale overlapping surface bindings before restricted authority replay",
    )?;

    if rows.is_empty() {
        return Ok(0);
    }

    let orphaned_ids = rows.into_iter().collect::<HashSet<_>>();
    existing.retain(|binding| !orphaned_ids.contains(&binding.surface_binding_id));

    tracing::warn!(
        adapter = DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY,
        orphaned_surface_binding_count = orphaned_ids.len(),
        "orphaned stale overlapping surface bindings before restricted authority replay"
    );

    Ok(orphaned_ids.len())
}

pub(super) fn stale_overlapping_surface_binding_candidates(
    incoming: &[SurfaceBinding],
    existing: &[SurfaceBinding],
) -> BTreeMap<Uuid, Uuid> {
    let mut incoming_by_name = BTreeMap::<&str, Vec<&SurfaceBinding>>::new();
    for binding in incoming
        .iter()
        .filter(|binding| surface_binding_exclusion_applies(binding.canonicality_state))
    {
        incoming_by_name
            .entry(binding.logical_name_id.as_str())
            .or_default()
            .push(binding);
    }

    let mut candidates = BTreeMap::new();
    for existing in existing.iter().filter(|binding| {
        binding.provenance.get("adapter").and_then(Value::as_str)
            == Some(DERIVATION_KIND_ENS_V1_UNWRAPPED_AUTHORITY)
            && surface_binding_exclusion_applies(binding.canonicality_state)
    }) {
        let Some(incoming_for_name) = incoming_by_name.get(existing.logical_name_id.as_str())
        else {
            continue;
        };
        if incoming_for_name.iter().any(|incoming| {
            incoming.surface_binding_id != existing.surface_binding_id
                && surface_binding_ranges_overlap(existing, incoming)
        }) {
            candidates.insert(existing.surface_binding_id, existing.resource_id);
        }
    }

    candidates
}

fn surface_binding_ranges_overlap(left: &SurfaceBinding, right: &SurfaceBinding) -> bool {
    right
        .active_to
        .is_none_or(|right_active_to| left.active_from < right_active_to)
        && left
            .active_to
            .is_none_or(|left_active_to| right.active_from < left_active_to)
}
