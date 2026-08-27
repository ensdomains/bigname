use std::collections::HashMap;

use alloy_primitives::keccak256;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sqlx::{Postgres, Row, Transaction};

use crate::LoadedManifest;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct ManifestKey {
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) chain_id: String,
    pub(super) deployment_label: String,
    pub(super) manifest_version: i64,
}

#[derive(Clone, Debug)]
pub(super) struct StoredManifestState {
    pub(super) manifest_id: i64,
    pub(super) key: ManifestKey,
    pub(super) rollout_status: String,
    normalizer_version: String,
    manifest_payload: Value,
    latest_event_state: Option<Value>,
}

impl StoredManifestState {
    pub(super) fn event_state(&self) -> Value {
        json!({
            "manifest_version": self.key.manifest_version,
            "normalizer_version": self.normalizer_version,
            "rollout_status": self.rollout_status,
            "manifest_payload": self.manifest_payload,
        })
    }

    pub(super) fn authority_matches(&self, other: &Self) -> bool {
        self.rollout_status == other.rollout_status
            && self.normalizer_version == other.normalizer_version
            && self.manifest_payload == other.manifest_payload
    }

    pub(super) fn history_matches(&self) -> bool {
        self.latest_event_state
            .as_ref()
            .is_some_and(|latest| latest == &self.event_state())
    }

    pub(super) fn latest_event_state_or_empty(&self) -> Value {
        self.latest_event_state.clone().unwrap_or_else(|| json!({}))
    }
}

pub(super) async fn load_manifest_states(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<HashMap<ManifestKey, StoredManifestState>> {
    let rows = sqlx::query(
        "SELECT manifest.manifest_id, manifest.namespace, manifest.source_family,
                manifest.chain_id, manifest.deployment_label, manifest.manifest_version,
                manifest.rollout_status, manifest.normalizer_version, manifest.manifest_payload,
                latest.after_state AS latest_event_state
         FROM manifest_versions manifest
         LEFT JOIN LATERAL (
             SELECT event.after_state
             FROM normalized_events event
             WHERE event.source_manifest_id = manifest.manifest_id
               AND event.event_kind = 'SourceManifestUpdated'
             ORDER BY event.normalized_event_id DESC
             LIMIT 1
         ) latest ON true",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load existing schema-v2 manifest states")?;
    rows.into_iter()
        .map(|row| {
            let key = ManifestKey {
                namespace: row.try_get("namespace")?,
                source_family: row.try_get("source_family")?,
                chain_id: row.try_get("chain_id")?,
                deployment_label: row.try_get("deployment_label")?,
                manifest_version: row.try_get("manifest_version")?,
            };
            let state = StoredManifestState {
                manifest_id: row.try_get("manifest_id")?,
                key: key.clone(),
                rollout_status: row.try_get("rollout_status")?,
                normalizer_version: row.try_get("normalizer_version")?,
                manifest_payload: row.try_get("manifest_payload")?,
                latest_event_state: row.try_get("latest_event_state")?,
            };
            Ok((key, state))
        })
        .collect::<std::result::Result<_, sqlx::Error>>()
        .context("failed to decode existing schema-v2 manifest states")
}

pub(super) fn manifest_state(
    manifest_id: i64,
    loaded: &LoadedManifest,
) -> Result<StoredManifestState> {
    let manifest = &loaded.manifest;
    let manifest_version = i64::try_from(manifest.manifest_version).with_context(|| {
        format!(
            "manifest version {} in {} exceeds BIGINT",
            manifest.manifest_version,
            loaded.path.display()
        )
    })?;
    Ok(StoredManifestState {
        manifest_id,
        key: ManifestKey {
            namespace: manifest.namespace.clone(),
            source_family: manifest.source_family.clone(),
            chain_id: manifest.chain.clone(),
            deployment_label: manifest.deployment_epoch.clone(),
            manifest_version,
        },
        rollout_status: manifest.rollout_status.as_db_value().to_owned(),
        normalizer_version: manifest.normalizer_version.clone(),
        manifest_payload: super::watch::manifest_payload(manifest)
            .with_context(|| format!("failed to compile {}", loaded.path.display()))?,
        latest_event_state: None,
    })
}

pub(super) async fn write_manifest_event(
    transaction: &mut Transaction<'_, Postgres>,
    before_state: Value,
    after: &StoredManifestState,
) -> Result<()> {
    let applied_change_count: i64 = sqlx::query_scalar(
        "UPDATE manifest_versions
         SET applied_change_count = applied_change_count + 1
         WHERE manifest_id = $1
         RETURNING applied_change_count",
    )
    .bind(after.manifest_id)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to advance applied-change count for schema-v2 manifest {}",
            after.manifest_id
        )
    })?;
    let after_state = after.event_state();
    let raw_fact_ref = json!({
        "manifest_id": after.manifest_id,
        "namespace": after.key.namespace,
        "source_family": after.key.source_family,
        "chain": after.key.chain_id,
        "deployment_epoch": after.key.deployment_label,
        "applied_change_count": applied_change_count,
    });
    let identity_material = json!({
        "manifest_id": after.manifest_id,
        "applied_change_count": applied_change_count,
        "before_state": &before_state,
        "after_state": &after_state,
    });
    let identity_bytes = serde_json::to_vec(&identity_material)
        .context("failed to serialize SourceManifestUpdated identity")?;
    let event_identity = format!(
        "manifest_sync:source_manifest_updated:{:#x}",
        keccak256(identity_bytes)
    );
    let inserted = sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, raw_fact_ref, derivation_kind,
             canonicality_state, before_state, after_state
         )
         VALUES (
             $1, $2, 'SourceManifestUpdated', $3, $4, $5, $6, $7,
             'manifest_sync', 'finalized', $8, $9
         )
         ON CONFLICT (event_identity) DO NOTHING",
    )
    .bind(&event_identity)
    .bind(&after.key.namespace)
    .bind(&after.key.source_family)
    .bind(after.key.manifest_version)
    .bind(after.manifest_id)
    .bind(&after.key.chain_id)
    .bind(&raw_fact_ref)
    .bind(&before_state)
    .bind(&after_state)
    .execute(&mut **transaction)
    .await
    .with_context(|| {
        format!(
            "failed to write SourceManifestUpdated for schema-v2 manifest {}",
            after.manifest_id
        )
    })?
    .rows_affected();
    if inserted == 0 {
        let compatible: Option<bool> = sqlx::query_scalar(
            "SELECT namespace = $2
                AND event_kind = 'SourceManifestUpdated'
                AND source_family = $3
                AND manifest_version = $4
                AND source_manifest_id = $5
                AND chain_id = $6
                AND raw_fact_ref = $7
                AND derivation_kind = 'manifest_sync'
                AND canonicality_state = 'finalized'
                AND before_state = $8
                AND after_state = $9
             FROM normalized_events
             WHERE event_identity = $1",
        )
        .bind(&event_identity)
        .bind(&after.key.namespace)
        .bind(&after.key.source_family)
        .bind(after.key.manifest_version)
        .bind(after.manifest_id)
        .bind(&after.key.chain_id)
        .bind(&raw_fact_ref)
        .bind(&before_state)
        .bind(&after_state)
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to verify idempotent SourceManifestUpdated event")?;
        if compatible != Some(true) {
            bail!("SourceManifestUpdated event identity collision for {event_identity}");
        }
    }
    Ok(())
}
