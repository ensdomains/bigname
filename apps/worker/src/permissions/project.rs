use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_storage::PermissionsCurrentRow;
use serde_json::{Value, json};
use sqlx::{PgPool, types::time::OffsetDateTime};
use uuid::Uuid;

use super::canonicality::{build_canonicality_summary, build_chain_positions};
use super::json::{
    build_coverage, build_provenance, json_object_or_default, json_optional_object,
    json_string_array, json_text, parse_scope,
};
use super::load::load_permission_events;
use super::types::{PermissionKey, RelevantEvent};

pub(super) async fn build_rows(
    pool: &PgPool,
    resource_ids: &[Uuid],
) -> Result<Vec<PermissionsCurrentRow>> {
    let mut rows = Vec::new();

    for resource_id in resource_ids {
        let events = load_permission_events(pool, *resource_id).await?;
        rows.extend(project_rows(*resource_id, &events)?);
    }

    Ok(rows)
}

fn project_rows(resource_id: Uuid, events: &[RelevantEvent]) -> Result<Vec<PermissionsCurrentRow>> {
    let mut latest_by_key = BTreeMap::<PermissionKey, usize>::new();
    let mut history_by_key = BTreeMap::<PermissionKey, Vec<&RelevantEvent>>::new();

    for (index, event) in events.iter().enumerate() {
        let subject = json_text(&event.after_state, &["subject"])?;
        let scope = parse_scope(&event.after_state)?;
        let key = PermissionKey {
            subject,
            scope: scope.storage_key(),
        };
        latest_by_key.insert(key.clone(), index);
        history_by_key.entry(key).or_default().push(event);
    }

    let mut rows = Vec::new();
    for (key, latest_index) in latest_by_key {
        let latest = &events[latest_index];
        let effective_powers = json_string_array(&latest.after_state, &["effective_powers"])?;
        if effective_powers.is_empty() {
            continue;
        }

        let history = history_by_key
            .get(&key)
            .context("missing permissions_current history for projected key")?;
        let scope = parse_scope(&latest.after_state)?;

        rows.push(PermissionsCurrentRow {
            resource_id,
            subject: key.subject,
            scope,
            effective_powers: Value::Array(
                effective_powers
                    .into_iter()
                    .map(Value::String)
                    .collect::<Vec<_>>(),
            ),
            grant_source: json_object_or_default(&latest.after_state, "grant_source"),
            revocation_source: json_optional_object(&latest.after_state, "revocation_source"),
            inheritance_path: latest
                .after_state
                .get("inheritance_path")
                .cloned()
                .unwrap_or_else(|| json!([])),
            transfer_behavior: json_object_or_default(&latest.after_state, "transfer_behavior"),
            provenance: build_provenance(history)?,
            coverage: build_coverage(history),
            chain_positions: build_chain_positions(history),
            canonicality_summary: build_canonicality_summary(history),
            manifest_version: history
                .iter()
                .map(|event| event.manifest_version)
                .max()
                .unwrap_or(1),
            last_recomputed_at: history
                .iter()
                .filter_map(|event| event.block_timestamp)
                .max()
                .unwrap_or(OffsetDateTime::UNIX_EPOCH),
        });
    }

    Ok(rows)
}
