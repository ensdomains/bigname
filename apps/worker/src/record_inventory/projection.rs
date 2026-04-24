use anyhow::{Context, Result};
use bigname_storage::{
    RecordInventoryCurrentRow, clear_record_inventory_current, upsert_record_inventory_current_rows,
};
use serde_json::{Value, json};
use sqlx::{PgPool, types::time::OffsetDateTime};
use uuid::Uuid;

use super::{
    chain_position::{
        build_chain_positions, build_record_version_boundary, collect_chain_position_events,
        load_basenames_transport_chain_positions,
    },
    constants::*,
    json::{
        build_canonicality_summary, build_coverage, build_entries, build_explicit_gaps,
        build_last_change, build_provenance, build_selectors, build_unsupported_families,
        gap_value, resolver_family_pending_value,
    },
    loading::{load_relevant_events, load_target_resource_ids},
    profile::{ResolverProfileGate, resolver_local_source_family},
    types::{RecordInventoryCurrentRebuildSummary, RelevantEvent},
};

pub(super) async fn rebuild_record_inventory_current(
    pool: &PgPool,
    resource_id: Option<&str>,
) -> Result<RecordInventoryCurrentRebuildSummary> {
    match resource_id {
        Some(resource_id) => rebuild_one_resource(pool, resource_id).await,
        None => rebuild_all_resources(pool).await,
    }
}

async fn rebuild_all_resources(pool: &PgPool) -> Result<RecordInventoryCurrentRebuildSummary> {
    let profile_gate = ResolverProfileGate::load(pool).await?;
    let resource_ids = load_target_resource_ids(pool).await?;

    let mut rows = Vec::with_capacity(resource_ids.len());
    for resource_id in &resource_ids {
        if let Some(row) = build_row(pool, &profile_gate, *resource_id).await? {
            rows.push(row);
        }
    }

    let upserted_row_count = upsert_record_inventory_current_rows(pool, &rows)
        .await?
        .len();
    let deleted_row_count = delete_stale_record_inventory_current_rows(pool, &rows).await?;
    Ok(RecordInventoryCurrentRebuildSummary {
        requested_resource_count: resource_ids.len(),
        upserted_row_count,
        deleted_row_count,
    })
}

async fn rebuild_one_resource(
    pool: &PgPool,
    resource_id: &str,
) -> Result<RecordInventoryCurrentRebuildSummary> {
    let profile_gate = ResolverProfileGate::load(pool).await?;
    let resource_id = Uuid::parse_str(resource_id)
        .with_context(|| format!("resource_id must be a UUID: {resource_id}"))?;
    let Some(row) = build_row(pool, &profile_gate, resource_id).await? else {
        let deleted_row_count =
            delete_record_inventory_rows_for_resource(pool, resource_id).await?;
        return Ok(RecordInventoryCurrentRebuildSummary {
            requested_resource_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };

    let upserted_row_count = upsert_record_inventory_current_rows(pool, std::slice::from_ref(&row))
        .await?
        .len();
    let deleted_row_count =
        delete_stale_record_inventory_current_rows_for_resource(pool, resource_id, &row).await?;
    Ok(RecordInventoryCurrentRebuildSummary {
        requested_resource_count: 1,
        upserted_row_count,
        deleted_row_count,
    })
}

async fn delete_record_inventory_rows_for_resource(
    pool: &PgPool,
    resource_id: Uuid,
) -> Result<u64> {
    sqlx::query(
        r#"
        DELETE FROM record_inventory_current
        WHERE resource_id = $1
        "#,
    )
    .bind(resource_id)
    .execute(pool)
    .await
    .with_context(|| {
        format!("failed to delete record_inventory_current rows for resource_id {resource_id}")
    })
    .map(|result| result.rows_affected())
}

async fn delete_stale_record_inventory_current_rows(
    pool: &PgPool,
    rows: &[RecordInventoryCurrentRow],
) -> Result<u64> {
    if rows.is_empty() {
        return clear_record_inventory_current(pool).await;
    }

    let resource_ids = rows.iter().map(|row| row.resource_id).collect::<Vec<_>>();
    let record_version_boundaries = rows
        .iter()
        .map(|row| {
            serde_json::to_string(&row.record_version_boundary)
                .context("failed to serialize record_inventory_current boundary for cleanup")
        })
        .collect::<Result<Vec<_>>>()?;

    sqlx::query(
        r#"
        DELETE FROM record_inventory_current current
        WHERE NOT EXISTS (
            SELECT 1
            FROM UNNEST($1::UUID[], $2::TEXT[]) AS replacement(
                resource_id,
                record_version_boundary
            )
            WHERE replacement.resource_id = current.resource_id
              AND replacement.record_version_boundary::JSONB = current.record_version_boundary
        )
        "#,
    )
    .bind(&resource_ids)
    .bind(&record_version_boundaries)
    .execute(pool)
    .await
    .context("failed to delete stale record_inventory_current rows after rebuild")
    .map(|result| result.rows_affected())
}

async fn delete_stale_record_inventory_current_rows_for_resource(
    pool: &PgPool,
    resource_id: Uuid,
    row: &RecordInventoryCurrentRow,
) -> Result<u64> {
    let record_version_boundary = serde_json::to_string(&row.record_version_boundary)
        .context("failed to serialize record_inventory_current boundary for cleanup")?;

    sqlx::query(
        r#"
        DELETE FROM record_inventory_current current
        WHERE current.resource_id = $1
          AND current.record_version_boundary <> $2::JSONB
        "#,
    )
    .bind(resource_id)
    .bind(record_version_boundary)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to delete stale record_inventory_current rows for resource_id {resource_id}"
        )
    })
    .map(|result| result.rows_affected())
}

async fn build_row(
    pool: &PgPool,
    profile_gate: &ResolverProfileGate,
    resource_id: Uuid,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let events = load_relevant_events(pool, resource_id).await?;
    if events.is_empty() {
        return Ok(None);
    }

    let latest_resolver_event = events
        .iter()
        .rev()
        .find(|event| event.event_kind == EVENT_KIND_RESOLVER_CHANGED);
    if let Some(resolver_event) = latest_resolver_event
        && profile_gate
            .current_record_status(resolver_event)
            .is_some_and(|status| status != RESOLVER_PROFILE_STATUS_SUPPORTED)
    {
        return build_pending_profile_row(pool, resource_id, resolver_event).await;
    }

    let boundary_index = events.iter().rposition(|event| {
        event.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED
            || event.event_kind == EVENT_KIND_RESOLVER_CHANGED
    });
    let scoped_events = &events[boundary_index.unwrap_or(0)..];
    let boundary_anchor = match boundary_index {
        Some(index) => events
            .get(index)
            .context("record_inventory_current rebuild boundary index out of range")?,
        None => events
            .last()
            .context("record_inventory_current rebuild requires at least one event")?,
    };
    let has_record_version_boundary_pointer =
        boundary_anchor.event_kind == EVENT_KIND_RECORD_VERSION_CHANGED;
    let record_version_boundary =
        build_record_version_boundary(boundary_anchor, has_record_version_boundary_pointer)?;
    let record_change_events = scoped_events
        .iter()
        .filter(|event| {
            event.event_kind == EVENT_KIND_RECORD_CHANGED && profile_gate.allows_event(event)
        })
        .collect::<Vec<_>>();
    let provenance_events = scoped_events
        .iter()
        .filter(|event| {
            event.event_kind == EVENT_KIND_RESOLVER_CHANGED
                || resolver_local_source_family(&event.source_family).is_none()
                || profile_gate.allows_event(event)
        })
        .cloned()
        .collect::<Vec<_>>();

    let selectors = build_selectors(&record_change_events)?;
    let explicit_gaps = build_explicit_gaps(&selectors);
    let unsupported_families = build_unsupported_families(&record_change_events)?;
    let entries = build_entries(&selectors);
    let last_change = provenance_events
        .last()
        .map(build_last_change)
        .transpose()?;
    let chain_position_events = collect_chain_position_events(boundary_anchor, &provenance_events);
    let supplemental_chain_positions =
        load_basenames_transport_chain_positions(pool, &chain_position_events).await?;

    Ok(Some(RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary,
        enumeration_basis: json!({
            "observed_selectors": true,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
        selectors: Value::Array(
            selectors
                .into_values()
                .map(|selector| {
                    json!({
                        "record_key": selector.record_key,
                        "record_family": selector.record_family,
                        "selector_key": selector.selector_key,
                        "cacheable": true,
                    })
                })
                .collect(),
        ),
        explicit_gaps: Value::Array(explicit_gaps),
        unsupported_families: Value::Array(unsupported_families),
        last_change,
        entries: Value::Array(entries),
        provenance: build_provenance(&provenance_events)?,
        coverage: build_coverage(&provenance_events),
        chain_positions: build_chain_positions(
            &chain_position_events,
            supplemental_chain_positions,
        ),
        canonicality_summary: build_canonicality_summary(&provenance_events),
        manifest_version: provenance_events
            .iter()
            .map(|event| event.manifest_version)
            .max()
            .unwrap_or(1),
        last_recomputed_at: provenance_events
            .iter()
            .filter_map(|event| event.block_timestamp)
            .max()
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
    }))
}

async fn build_pending_profile_row(
    pool: &PgPool,
    resource_id: Uuid,
    resolver_event: &RelevantEvent,
) -> Result<Option<RecordInventoryCurrentRow>> {
    let supplemental_chain_positions =
        load_basenames_transport_chain_positions(pool, std::slice::from_ref(resolver_event))
            .await?;

    Ok(Some(RecordInventoryCurrentRow {
        resource_id,
        record_version_boundary: build_record_version_boundary(resolver_event, false)?,
        enumeration_basis: json!({
            "observed_selectors": false,
            "capability_declared_families": true,
            "globally_enumerable": false,
        }),
        selectors: Value::Array(vec![]),
        explicit_gaps: Value::Array(vec![gap_value(
            UNSUPPORTED_CONTENTHASH_RECORD_KEY,
            UNSUPPORTED_CONTENTHASH_RECORD_FAMILY,
            None,
        )]),
        unsupported_families: Value::Array(vec![
            resolver_family_pending_value(SUPPORTED_ADDR_RECORD_FAMILY),
            resolver_family_pending_value(SUPPORTED_TEXT_RECORD_FAMILY),
        ]),
        last_change: Some(build_last_change(resolver_event)?),
        entries: Value::Array(vec![]),
        provenance: build_provenance(std::slice::from_ref(resolver_event))?,
        coverage: json!({
            "status": "partial",
            "exhaustiveness": "best_effort",
            "source_classes_considered": [resolver_event.source_family],
            "unsupported_reason": RESOLVER_FAMILY_PENDING_REASON,
            "enumeration_basis": RECORD_INVENTORY_ENUMERATION_BASIS,
        }),
        chain_positions: build_chain_positions(
            std::slice::from_ref(resolver_event),
            supplemental_chain_positions,
        ),
        canonicality_summary: build_canonicality_summary(std::slice::from_ref(resolver_event)),
        manifest_version: resolver_event.manifest_version,
        last_recomputed_at: resolver_event
            .block_timestamp
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
    }))
}
