use std::collections::HashMap;

use anyhow::{Context, Result};

use crate::{
    SourceManifest,
    support::{DeclarationKey, PersistedManifestEntry},
};

pub(crate) async fn replace_manifest_children(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
    manifest: &SourceManifest,
    existing_entries: &HashMap<DeclarationKey, PersistedManifestEntry>,
    planned_entries: &[PersistedManifestEntry],
) -> Result<bool> {
    let planned_entries_by_key = planned_entries
        .iter()
        .cloned()
        .map(|entry| (entry.key.clone(), entry))
        .collect::<HashMap<_, _>>();
    let existing_capabilities = sqlx::query_as::<_, (String, String, Option<String>)>(
        r#"
        SELECT capability_name, status::TEXT, notes
        FROM manifest_capability_flags
        WHERE manifest_id = $1
        ORDER BY capability_name, status::TEXT, notes
        "#,
    )
    .bind(manifest_id)
    .fetch_all(&mut *executor)
    .await
    .with_context(|| format!("failed to load capability flags for manifest_id {manifest_id}"))?;
    let mut desired_capabilities = manifest
        .capability_flags
        .iter()
        .map(|(name, flag)| {
            (
                name.clone(),
                flag.status.as_db_value().to_owned(),
                flag.notes.clone(),
            )
        })
        .collect::<Vec<_>>();
    desired_capabilities.sort();
    let existing_discovery_rules = sqlx::query_as::<_, (String, String, String)>(
        r#"
        SELECT edge_kind, from_role, admission
        FROM manifest_discovery_rules
        WHERE manifest_id = $1
        ORDER BY edge_kind, from_role, admission
        "#,
    )
    .bind(manifest_id)
    .fetch_all(&mut *executor)
    .await
    .with_context(|| format!("failed to load discovery rules for manifest_id {manifest_id}"))?;
    let mut desired_discovery_rules = manifest
        .discovery_rules
        .iter()
        .map(|rule| {
            (
                rule.edge_kind.clone(),
                rule.from_role.clone(),
                rule.admission.clone(),
            )
        })
        .collect::<Vec<_>>();
    desired_discovery_rules.sort();

    let children_changed = existing_entries != &planned_entries_by_key
        || existing_capabilities != desired_capabilities
        || existing_discovery_rules != desired_discovery_rules;
    if !children_changed {
        return Ok(false);
    }

    sqlx::query("DELETE FROM manifest_contract_instances WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!("failed to clear manifest_contract_instances for manifest_id {manifest_id}")
        })?;
    sqlx::query("DELETE FROM manifest_capability_flags WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!("failed to clear manifest_capability_flags for manifest_id {manifest_id}")
        })?;
    sqlx::query("DELETE FROM manifest_discovery_rules WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!("failed to clear manifest_discovery_rules for manifest_id {manifest_id}")
        })?;

    for entry in planned_entries {
        sqlx::query(
            r#"
            INSERT INTO manifest_contract_instances (
                manifest_id,
                declaration_kind,
                declaration_name,
                contract_instance_id,
                declared_address,
                code_hash,
                abi_ref,
                role,
                proxy_kind,
                implementation_contract_instance_id,
                declared_implementation_address
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(manifest_id)
        .bind(&entry.key.declaration_kind)
        .bind(&entry.key.declaration_name)
        .bind(entry.contract_instance_id)
        .bind(&entry.declared_address)
        .bind(entry.code_hash.as_deref())
        .bind(entry.abi_ref.as_deref())
        .bind(entry.role.as_deref())
        .bind(entry.proxy_kind.as_deref())
        .bind(entry.implementation_contract_instance_id)
        .bind(entry.declared_implementation_address.as_deref())
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert manifest entry {} {} for manifest_id {manifest_id}",
                entry.key.declaration_kind, entry.key.declaration_name
            )
        })?;
    }

    for (capability_name, capability_flag) in &manifest.capability_flags {
        sqlx::query(
            r#"
            INSERT INTO manifest_capability_flags (
                manifest_id,
                capability_name,
                status,
                notes
            )
            VALUES ($1, $2, $3::capability_support_status, $4)
            "#,
        )
        .bind(manifest_id)
        .bind(capability_name)
        .bind(capability_flag.status.as_db_value())
        .bind(capability_flag.notes.as_deref())
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert capability {} for manifest_id {manifest_id}",
                capability_name
            )
        })?;
    }

    for discovery_rule in &manifest.discovery_rules {
        sqlx::query(
            r#"
            INSERT INTO manifest_discovery_rules (
                manifest_id,
                edge_kind,
                from_role,
                admission
            )
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(manifest_id)
        .bind(&discovery_rule.edge_kind)
        .bind(&discovery_rule.from_role)
        .bind(&discovery_rule.admission)
        .execute(&mut *executor)
        .await
        .with_context(|| {
            format!(
                "failed to insert discovery rule {} for manifest_id {manifest_id}",
                discovery_rule.edge_kind
            )
        })?;
    }

    Ok(true)
}
