use std::collections::HashMap;

use anyhow::{Context, Result};
use sqlx::Row;

use crate::{
    LoadedManifest,
    support::{DeclarationKey, ManifestStorageKey, PersistedManifestEntry},
};

pub(super) struct ExistingManifestVersion {
    pub(super) manifest_id: i64,
    pub(super) storage_key: ManifestStorageKey,
}

pub(super) async fn load_existing_manifest_versions(
    executor: &mut sqlx::postgres::PgConnection,
) -> Result<Vec<ExistingManifestVersion>> {
    let rows = sqlx::query(
        r#"
        SELECT manifest_id, namespace, source_family, chain, deployment_epoch, manifest_version
        FROM manifest_versions
        "#,
    )
    .fetch_all(executor)
    .await
    .context("failed to load existing manifest versions")?;

    rows.into_iter()
        .map(|row| {
            let manifest_version = row
                .try_get::<i64, _>("manifest_version")
                .context("failed to read existing manifest_version")?;
            Ok(ExistingManifestVersion {
                manifest_id: row
                    .try_get("manifest_id")
                    .context("failed to read existing manifest_id")?,
                storage_key: ManifestStorageKey {
                    namespace: row
                        .try_get("namespace")
                        .context("failed to read existing namespace")?,
                    source_family: row
                        .try_get("source_family")
                        .context("failed to read existing source_family")?,
                    chain: row
                        .try_get("chain")
                        .context("failed to read existing chain")?,
                    deployment_epoch: row
                        .try_get("deployment_epoch")
                        .context("failed to read existing deployment_epoch")?,
                    manifest_version,
                },
            })
        })
        .collect()
}

pub(super) async fn delete_stale_manifest_version(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
) -> Result<()> {
    sqlx::query("DELETE FROM manifest_versions WHERE manifest_id = $1")
        .bind(manifest_id)
        .execute(executor)
        .await
        .with_context(|| format!("failed to delete stale manifest_id {manifest_id}"))?;

    Ok(())
}

pub(super) async fn upsert_manifest_version(
    executor: &mut sqlx::postgres::PgConnection,
    loaded_manifest: &LoadedManifest,
) -> Result<i64> {
    let manifest_payload = serde_json::to_string(&loaded_manifest.manifest)
        .context("failed to serialize manifest payload")?;
    let manifest_key = ManifestStorageKey::from_loaded_manifest(loaded_manifest)?;

    let row = sqlx::query(
        r#"
        INSERT INTO manifest_versions (
            manifest_version,
            namespace,
            source_family,
            chain,
            deployment_epoch,
            rollout_status,
            normalizer_version,
            file_path,
            manifest_payload
        )
        VALUES ($1, $2, $3, $4, $5, $6::manifest_rollout_status, $7, $8, $9::jsonb)
        ON CONFLICT (namespace, source_family, chain, deployment_epoch, manifest_version)
        DO UPDATE SET
            rollout_status = EXCLUDED.rollout_status,
            normalizer_version = EXCLUDED.normalizer_version,
            file_path = EXCLUDED.file_path,
            manifest_payload = EXCLUDED.manifest_payload,
            loaded_at = now()
        RETURNING manifest_id
        "#,
    )
    .bind(manifest_key.manifest_version)
    .bind(&manifest_key.namespace)
    .bind(&manifest_key.source_family)
    .bind(&manifest_key.chain)
    .bind(&manifest_key.deployment_epoch)
    .bind(loaded_manifest.manifest.rollout_status.as_db_value())
    .bind(&loaded_manifest.manifest.normalizer_version)
    .bind(loaded_manifest.relative_path.to_string_lossy().into_owned())
    .bind(manifest_payload)
    .fetch_one(executor)
    .await
    .with_context(|| {
        format!(
            "failed to upsert manifest version from {}",
            loaded_manifest.path.display()
        )
    })?;

    row.try_get("manifest_id")
        .context("failed to read manifest_id from manifest upsert")
}

pub(super) async fn load_existing_manifest_entries(
    executor: &mut sqlx::postgres::PgConnection,
    manifest_id: i64,
) -> Result<HashMap<DeclarationKey, PersistedManifestEntry>> {
    let rows = sqlx::query(
        r#"
        SELECT
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
        FROM manifest_contract_instances
        WHERE manifest_id = $1
        "#,
    )
    .bind(manifest_id)
    .fetch_all(executor)
    .await
    .with_context(|| {
        format!("failed to load existing manifest children for manifest_id {manifest_id}")
    })?;

    rows.into_iter()
        .map(|row| {
            let declaration_kind = row
                .try_get::<String, _>("declaration_kind")
                .context("failed to read declaration_kind")?;
            let declaration_name = row
                .try_get::<String, _>("declaration_name")
                .context("failed to read declaration_name")?;
            let entry = PersistedManifestEntry {
                key: DeclarationKey {
                    declaration_kind: declaration_kind.clone(),
                    declaration_name: declaration_name.clone(),
                },
                contract_instance_id: row
                    .try_get("contract_instance_id")
                    .context("failed to read contract_instance_id")?,
                declared_address: row
                    .try_get("declared_address")
                    .context("failed to read declared_address")?,
                code_hash: row
                    .try_get("code_hash")
                    .context("failed to read code_hash")?,
                abi_ref: row.try_get("abi_ref").context("failed to read abi_ref")?,
                role: row.try_get("role").context("failed to read role")?,
                proxy_kind: row
                    .try_get("proxy_kind")
                    .context("failed to read proxy_kind")?,
                implementation_contract_instance_id: row
                    .try_get("implementation_contract_instance_id")
                    .context("failed to read implementation_contract_instance_id")?,
                declared_implementation_address: row
                    .try_get("declared_implementation_address")
                    .context("failed to read declared_implementation_address")?,
            };
            Ok((entry.key.clone(), entry))
        })
        .collect()
}
