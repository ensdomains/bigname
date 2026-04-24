use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use bigname_storage::{PermissionsCurrentRow, ResolverCurrentRow, SurfaceBindingKind};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::types::time::OffsetDateTime;

use super::{
    EVENT_KIND_ALIAS_CHANGED, EVENT_KIND_PERMISSION_CHANGED, EVENT_KIND_RESOLVER_CHANGED,
    RESOLVER_CURRENT_DERIVATION_KIND, RESOLVER_CURRENT_ENUMERATION_BASIS,
    RESOLVER_FAMILY_PENDING_REASON, RESOLVER_PROFILE_STATUS_SUPPORTED,
    profile::ResolverProfileGate,
    state_helpers::{build_canonicality_summary, build_chain_positions},
    target_loading::{
        AliasSeed, CurrentBindingSeed, ResolverTarget, load_alias_events, load_current_bindings,
        load_resolver_permissions,
    },
};

pub(super) async fn build_resolver_current_row(
    pool: &PgPool,
    profile_gate: &ResolverProfileGate,
    target: &ResolverTarget,
) -> Result<Option<ResolverCurrentRow>> {
    let bindings = load_current_bindings(pool, target).await?;
    let aliases = load_alias_events(pool, target).await?;
    let permissions = load_resolver_permissions(pool, target).await?;
    if bindings.is_empty() && aliases.is_empty() && permissions.is_empty() {
        return Ok(None);
    }

    let provenance = build_provenance(&bindings, &aliases, &permissions)?;
    let chain_positions = build_chain_positions(&bindings, &aliases, &permissions);
    let canonicality_summary = build_canonicality_summary(&bindings, &aliases, &permissions)?;
    let manifest_version = manifest_version(&bindings, &aliases, &permissions);
    let last_recomputed_at = last_recomputed_at(&bindings, &aliases, &permissions);
    let target_status = profile_gate.target_status_for_bindings(target, &bindings);
    let (declared_summary, coverage) = if target_status != RESOLVER_PROFILE_STATUS_SUPPORTED {
        (
            build_unsupported_declared_summary(RESOLVER_FAMILY_PENDING_REASON),
            build_unsupported_coverage(&bindings, &aliases, &permissions),
        )
    } else {
        (
            build_declared_summary(&bindings, &aliases, &permissions),
            build_coverage(&bindings, &aliases, &permissions),
        )
    };

    Ok(Some(ResolverCurrentRow {
        chain_id: target.chain_id.clone(),
        resolver_address: target.resolver_address.clone(),
        declared_summary,
        provenance,
        coverage,
        chain_positions,
        canonicality_summary,
        manifest_version,
        last_recomputed_at,
    }))
}

fn manifest_version(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> i64 {
    bindings
        .iter()
        .map(|binding| binding.manifest_version)
        .chain(aliases.iter().map(|alias| alias.manifest_version))
        .chain(
            permissions
                .iter()
                .map(|permission| permission.manifest_version),
        )
        .max()
        .unwrap_or(1)
}

fn last_recomputed_at(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> OffsetDateTime {
    bindings
        .iter()
        .filter_map(|binding| binding.block_timestamp)
        .chain(aliases.iter().filter_map(|alias| alias.block_timestamp))
        .chain(
            permissions
                .iter()
                .map(|permission| permission.last_recomputed_at),
        )
        .max()
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

fn build_declared_summary(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    json!({
        "bindings": build_binding_summary(bindings.iter()),
        "aliases": build_alias_summary(bindings, aliases),
        "permissions": {
            "status": "supported",
            "count": permissions.len(),
            "items": permissions
                .iter()
                .map(|permission| {
                    json!({
                        "resource_id": permission.resource_id,
                        "subject": permission.subject,
                        "effective_powers": permission.effective_powers,
                        "grant_source": permission.grant_source,
                        "revocation_source": permission.revocation_source,
                    })
                })
                .collect::<Vec<_>>(),
        },
        "role_holders": build_role_holders_summary(permissions),
        "event_summary": build_event_summary(bindings, aliases, permissions),
    })
}

fn build_binding_summary<'a>(bindings: impl Iterator<Item = &'a CurrentBindingSeed>) -> Value {
    let items = bindings.map(build_binding_item).collect::<Vec<_>>();
    json!({
        "status": "supported",
        "count": items.len(),
        "items": items,
    })
}

fn build_binding_item(binding: &CurrentBindingSeed) -> Value {
    json!({
        "logical_name_id": binding.logical_name_id,
        "canonical_display_name": binding.canonical_display_name,
        "normalized_name": binding.normalized_name,
        "namehash": binding.namehash,
        "resource_id": binding.resource_id,
        "surface_binding_id": binding.surface_binding_id,
        "binding_kind": binding.binding_kind.as_str(),
    })
}

fn build_alias_summary(bindings: &[CurrentBindingSeed], aliases: &[AliasSeed]) -> Value {
    let mut items = bindings
        .iter()
        .filter(|binding| binding.binding_kind == SurfaceBindingKind::ResolverAliasPath)
        .map(build_binding_item)
        .collect::<Vec<_>>();
    items.extend(aliases.iter().map(build_alias_item));
    items.sort_by(|left, right| {
        left.get("logical_name_id")
            .and_then(Value::as_str)
            .cmp(&right.get("logical_name_id").and_then(Value::as_str))
            .then(
                left.get("from_dns_encoded_name")
                    .and_then(Value::as_str)
                    .cmp(&right.get("from_dns_encoded_name").and_then(Value::as_str)),
            )
    });
    json!({
        "status": "supported",
        "count": items.len(),
        "items": items,
    })
}

fn build_alias_item(alias: &AliasSeed) -> Value {
    json!({
        "logical_name_id": alias.logical_name_id,
        "resource_id": alias.resource_id,
        "binding_kind": "resolver_alias_path",
        "alias_state": alias.after_state.get("alias_state").cloned().unwrap_or_else(|| json!("active")),
        "active": alias.after_state.get("active").cloned().unwrap_or(Value::Bool(true)),
        "chain_id": alias.chain_id,
        "resolver_address": alias.resolver_address,
        "from_dns_encoded_name": alias.after_state.get("from_dns_encoded_name").cloned().unwrap_or(Value::Null),
        "to_dns_encoded_name": alias.after_state.get("to_dns_encoded_name").cloned().unwrap_or(Value::Null),
        "from_name": alias.after_state.get("from_name").cloned().unwrap_or(Value::Null),
        "to_name": alias.after_state.get("to_name").cloned().unwrap_or(Value::Null),
        "to_logical_name_id": alias.after_state.get("to_logical_name_id").cloned().unwrap_or(Value::Null),
        "to_resource_id": alias.after_state.get("to_resource_id").cloned().unwrap_or(Value::Null),
        "latest_event_kind": EVENT_KIND_ALIAS_CHANGED,
    })
}

fn build_role_holders_summary(permissions: &[PermissionsCurrentRow]) -> Value {
    let mut holders = BTreeMap::<String, (BTreeSet<String>, BTreeSet<String>)>::new();

    for permission in permissions {
        let entry = holders
            .entry(permission.subject.clone())
            .or_insert_with(|| (BTreeSet::new(), BTreeSet::new()));
        entry.0.insert(permission.resource_id.to_string());
        for power in json_string_array(&permission.effective_powers) {
            entry.1.insert(power);
        }
    }

    json!({
        "status": "supported",
        "count": holders.len(),
        "items": holders
            .into_iter()
            .map(|(subject, (resource_ids, powers))| {
                json!({
                    "subject": subject,
                    "resource_count": resource_ids.len(),
                    "permission_row_count": resource_ids.len(),
                    "effective_powers": powers.into_iter().collect::<Vec<_>>(),
                    "resource_ids": resource_ids.into_iter().collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn build_event_summary(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    let resolver_changed_count = bindings.len();
    let alias_changed_count = aliases.len();
    let permission_changed_count = permissions
        .iter()
        .map(|permission| {
            permission
                .provenance
                .get("normalized_event_ids")
                .and_then(Value::as_array)
                .map(|ids| ids.len())
                .unwrap_or(0)
        })
        .sum::<usize>();
    let total_count = resolver_changed_count + alias_changed_count + permission_changed_count;
    let mut by_kind = serde_json::Map::new();
    if alias_changed_count > 0 {
        by_kind.insert(
            EVENT_KIND_ALIAS_CHANGED.to_owned(),
            Value::Number(alias_changed_count.into()),
        );
    }
    if permission_changed_count > 0 {
        by_kind.insert(
            EVENT_KIND_PERMISSION_CHANGED.to_owned(),
            Value::Number(permission_changed_count.into()),
        );
    }
    if resolver_changed_count > 0 {
        by_kind.insert(
            EVENT_KIND_RESOLVER_CHANGED.to_owned(),
            Value::Number(resolver_changed_count.into()),
        );
    }

    json!({
        "status": "supported",
        "count": total_count,
        "by_kind": by_kind,
    })
}

fn build_unsupported_declared_summary(unsupported_reason: &str) -> Value {
    json!({
        "bindings": unsupported_summary(unsupported_reason),
        "aliases": unsupported_summary(unsupported_reason),
        "permissions": unsupported_summary(unsupported_reason),
        "role_holders": unsupported_summary(unsupported_reason),
        "event_summary": unsupported_summary(unsupported_reason),
    })
}

fn unsupported_summary(unsupported_reason: &str) -> Value {
    json!({
        "status": "unsupported",
        "unsupported_reason": unsupported_reason,
    })
}

fn build_provenance(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Result<Value> {
    let normalized_event_ids = bindings
        .iter()
        .map(|binding| Value::Number(binding.normalized_event_id.into()))
        .chain(
            aliases
                .iter()
                .map(|alias| Value::Number(alias.normalized_event_id.into())),
        )
        .chain(permissions.iter().flat_map(|permission| {
            extract_json_array(&permission.provenance, "normalized_event_ids")
        }))
        .collect::<Vec<_>>();
    let raw_fact_refs = bindings
        .iter()
        .map(|binding| binding.raw_fact_ref.clone())
        .chain(aliases.iter().map(|alias| alias.raw_fact_ref.clone()))
        .chain(
            permissions
                .iter()
                .flat_map(|permission| extract_json_array(&permission.provenance, "raw_fact_refs")),
        )
        .collect::<Vec<_>>();
    let manifest_versions =
        bindings
            .iter()
            .map(|binding| {
                json!({
                    "source_manifest_id": binding.source_manifest_id,
                    "source_family": binding.source_family,
                    "manifest_version": binding.manifest_version,
                })
            })
            .chain(aliases.iter().map(|alias| {
                json!({
                    "source_manifest_id": alias.source_manifest_id,
                    "source_family": alias.source_family,
                    "manifest_version": alias.manifest_version,
                })
            }))
            .chain(permissions.iter().flat_map(|permission| {
                extract_json_array(&permission.provenance, "manifest_versions")
            }))
            .collect::<Vec<_>>();

    Ok(json!({
        "normalized_event_ids": dedupe_json_values(normalized_event_ids)?,
        "raw_fact_refs": dedupe_json_values(raw_fact_refs)?,
        "manifest_versions": dedupe_json_values(manifest_versions)?,
        "execution_trace_id": Value::Null,
        "derivation_kind": RESOLVER_CURRENT_DERIVATION_KIND,
    }))
}

fn build_coverage(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    let mut source_classes = bindings
        .iter()
        .map(|binding| binding.source_family.clone())
        .collect::<BTreeSet<_>>();

    source_classes.extend(aliases.iter().map(|alias| alias.source_family.clone()));

    for permission in permissions {
        for value in extract_json_string_array(&permission.coverage, "source_classes_considered") {
            source_classes.insert(value);
        }
    }

    json!({
        "status": "full",
        "exhaustiveness": "authoritative",
        "source_classes_considered": source_classes.into_iter().collect::<Vec<_>>(),
        "unsupported_reason": Value::Null,
        "enumeration_basis": RESOLVER_CURRENT_ENUMERATION_BASIS,
    })
}

fn build_unsupported_coverage(
    bindings: &[CurrentBindingSeed],
    aliases: &[AliasSeed],
    permissions: &[PermissionsCurrentRow],
) -> Value {
    let mut coverage = build_coverage(bindings, aliases, permissions);
    coverage["status"] = json!("partial");
    coverage["exhaustiveness"] = json!("best_effort");
    coverage["unsupported_reason"] = json!(RESOLVER_FAMILY_PENDING_REASON);
    coverage
}

fn extract_json_array(value: &Value, field: &str) -> Vec<Value> {
    value
        .get(field)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn extract_json_string_array(value: &Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn json_string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn dedupe_json_values(values: Vec<Value>) -> Result<Vec<Value>> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();

    for value in values {
        let key = serde_json::to_string(&value).context("failed to serialize JSON value")?;
        if seen.insert(key) {
            deduped.push(value);
        }
    }

    Ok(deduped)
}
