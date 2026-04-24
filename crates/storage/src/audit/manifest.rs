use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::CanonicalityState;

use super::{
    decode::decode_manifest_drift_alert_observation,
    manifest_json::serialize_json_object,
    manifest_live::{
        load_live_code_hash_drift_candidates, load_live_proxy_implementation_candidates,
    },
    manifest_validation::{
        ensure_existing_manifest_alert_matches_request,
        manifest_alert_observation_create_from_rendered,
        validate_manifest_drift_alert_observation_create,
    },
    types::{
        ManifestDriftAlertInspection, ManifestDriftAlertKind, ManifestDriftAlertObservation,
        ManifestDriftAlertObservationCreate, OBSERVATION_KIND_MANIFEST_DRIFT,
        OBSERVATION_KIND_PROXY_IMPLEMENTATION_DRIFT,
    },
};

impl ManifestDriftAlertInspection {
    pub fn total_alert_count(&self) -> usize {
        self.code_hash_drift_alerts.len() + self.proxy_implementation_alerts.len()
    }

    /// Return the actionable alert total from live manifest-drift audit JSON.
    pub fn audit_total_alert_count(audit: &Value) -> Result<u64> {
        audit
            .get("counts")
            .and_then(|counts| counts.get("total"))
            .and_then(Value::as_u64)
            .context("manifest drift audit JSON is missing counts.total")
    }

    /// Compute live manifest-drift and proxy-implementation audit output from
    /// existing persisted state. This is intentionally operational JSON and
    /// performs no alert persistence or manifest/discovery mutation.
    pub async fn compute_live_manifest_drift_audit(pool: &PgPool) -> Result<Value> {
        let code_hash_alerts = load_live_code_hash_drift_candidates(pool).await?;
        let proxy_alerts = load_live_proxy_implementation_candidates(pool).await?;

        Ok(json!({
            "command": "manifest-drift audit",
            "read_only": true,
            "persistence": {
                "writes_normalized_events": false,
                "writes_alert_table": false,
                "mutates_manifest_truth": false,
                "mutates_discovery_edges": false,
                "mutates_watch_plan": false,
            },
            "counts": {
                "manifest_code_hash_drift": code_hash_alerts.len(),
                "manifest_proxy_implementation": proxy_alerts.len(),
                "total": code_hash_alerts.len() + proxy_alerts.len(),
            },
            "manifest_code_hash_drift_alerts": code_hash_alerts,
            "proxy_implementation_alerts": proxy_alerts,
        }))
    }

    /// Persist one rendered worker alert observation into the worker-owned
    /// alert table. This compatibility API keeps callers on the exported
    /// observation shape while avoiding adapter-owned normalized-event writes.
    pub async fn persist_manifest_drift_alert_observation(
        pool: &PgPool,
        observation: &ManifestDriftAlertObservation,
    ) -> Result<ManifestDriftAlertObservation> {
        let create = manifest_alert_observation_create_from_rendered(observation)?;
        upsert_manifest_drift_alert_observation(pool, &create).await
    }
}

pub async fn list_manifest_drift_alert_observations(
    pool: &PgPool,
) -> Result<ManifestDriftAlertInspection> {
    let rows = sqlx::query(
        r#"
        SELECT
            manifest_alert_observation_id,
            observation_identity,
            observation_kind,
            lifecycle_status,
            namespace,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            contract_instance_id,
            proxy_contract_instance_id,
            expected_implementation_contract_instance_id,
            observed_implementation_contract_instance_id,
            discovery_edge_id,
            expected_code_hash,
            observed_code_hash,
            observed_code_byte_length,
            observed_block_number,
            observed_block_hash,
            observed_canonicality_state::TEXT AS observed_canonicality_state,
            raw_fact_ref,
            expected_material,
            observed_material,
            watch_plan_metadata,
            alert_metadata,
            remediation_status,
            remediation_metadata,
            first_observed_at,
            last_observed_at,
            remediated_at
        FROM manifest_alert_observations
        WHERE observation_kind IN ($1, $2)
        ORDER BY
            observation_kind,
            chain_id,
            source_family,
            manifest_version,
            observation_identity
        "#,
    )
    .bind(OBSERVATION_KIND_MANIFEST_DRIFT)
    .bind(OBSERVATION_KIND_PROXY_IMPLEMENTATION_DRIFT)
    .fetch_all(pool)
    .await
    .context("failed to list stored manifest drift alert observations")?;

    let mut inspection = ManifestDriftAlertInspection::default();
    for row in rows {
        let observation = decode_manifest_drift_alert_observation(row)?;
        match observation.alert_kind {
            ManifestDriftAlertKind::CodeHashDrift => {
                inspection.code_hash_drift_alerts.push(observation);
            }
            ManifestDriftAlertKind::ProxyImplementation => {
                inspection.proxy_implementation_alerts.push(observation);
            }
        }
    }

    Ok(inspection)
}

/// Persist one worker-owned manifest drift/proxy alert observation
/// idempotently. This writes only the `manifest_alert_observations` family.
pub async fn upsert_manifest_drift_alert_observation(
    pool: &PgPool,
    observation: &ManifestDriftAlertObservationCreate,
) -> Result<ManifestDriftAlertObservation> {
    validate_manifest_drift_alert_observation_create(observation)?;

    let raw_fact_ref = serialize_json_object(
        "manifest drift alert raw_fact_ref",
        &observation.raw_fact_ref,
    )?;
    let expected_material = serialize_json_object(
        "manifest drift alert expected_material",
        &observation.expected_material,
    )?;
    let observed_material = serialize_json_object(
        "manifest drift alert observed_material",
        &observation.observed_material,
    )?;
    let watch_plan_metadata = serialize_json_object(
        "manifest drift alert watch_plan_metadata",
        &observation.watch_plan_metadata,
    )?;
    let alert_metadata = serialize_json_object(
        "manifest drift alert alert_metadata",
        &observation.alert_metadata,
    )?;
    let remediation_metadata = observation
        .remediation_metadata
        .as_ref()
        .map(|metadata| {
            serialize_json_object("manifest drift alert remediation_metadata", metadata)
        })
        .transpose()?;

    let inserted = sqlx::query(
        r#"
        INSERT INTO manifest_alert_observations (
            observation_identity,
            observation_kind,
            lifecycle_status,
            namespace,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            contract_instance_id,
            proxy_contract_instance_id,
            expected_implementation_contract_instance_id,
            observed_implementation_contract_instance_id,
            discovery_edge_id,
            expected_code_hash,
            observed_code_hash,
            observed_code_byte_length,
            observed_block_number,
            observed_block_hash,
            observed_canonicality_state,
            raw_fact_ref,
            expected_material,
            observed_material,
            watch_plan_metadata,
            alert_metadata,
            remediation_status,
            remediation_metadata,
            first_observed_at,
            last_observed_at,
            remediated_at
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14,
            $15,
            $16,
            $17,
            $18,
            $19::canonicality_state,
            $20::jsonb,
            $21::jsonb,
            $22::jsonb,
            $23::jsonb,
            $24::jsonb,
            $25,
            $26::jsonb,
            $27,
            $28,
            $29
        )
        ON CONFLICT (observation_identity) DO NOTHING
        RETURNING
            manifest_alert_observation_id,
            observation_identity,
            observation_kind,
            lifecycle_status,
            namespace,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            contract_instance_id,
            proxy_contract_instance_id,
            expected_implementation_contract_instance_id,
            observed_implementation_contract_instance_id,
            discovery_edge_id,
            expected_code_hash,
            observed_code_hash,
            observed_code_byte_length,
            observed_block_number,
            observed_block_hash,
            observed_canonicality_state::TEXT AS observed_canonicality_state,
            raw_fact_ref,
            expected_material,
            observed_material,
            watch_plan_metadata,
            alert_metadata,
            remediation_status,
            remediation_metadata,
            first_observed_at,
            last_observed_at,
            remediated_at
        "#,
    )
    .bind(&observation.observation_identity)
    .bind(observation.alert_kind.observation_kind())
    .bind(observation.lifecycle_status.as_str())
    .bind(&observation.namespace)
    .bind(&observation.source_family)
    .bind(observation.manifest_version)
    .bind(observation.source_manifest_id)
    .bind(&observation.chain_id)
    .bind(observation.contract_instance_id)
    .bind(observation.proxy_contract_instance_id)
    .bind(observation.expected_implementation_contract_instance_id)
    .bind(observation.observed_implementation_contract_instance_id)
    .bind(observation.discovery_edge_id)
    .bind(&observation.expected_code_hash)
    .bind(&observation.observed_code_hash)
    .bind(observation.observed_code_byte_length)
    .bind(observation.observed_block_number)
    .bind(&observation.observed_block_hash)
    .bind(
        observation
            .observed_canonicality_state
            .map(CanonicalityState::as_str),
    )
    .bind(raw_fact_ref)
    .bind(expected_material)
    .bind(observed_material)
    .bind(watch_plan_metadata)
    .bind(alert_metadata)
    .bind(&observation.remediation_status)
    .bind(remediation_metadata)
    .bind(observation.first_observed_at)
    .bind(observation.last_observed_at)
    .bind(observation.remediated_at)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!(
            "failed to insert manifest drift alert observation {}",
            observation.observation_identity
        )
    })?;

    let stored = match inserted {
        Some(row) => decode_manifest_drift_alert_observation(row)?,
        None => load_manifest_drift_alert_observation_by_identity(
            pool,
            &observation.observation_identity,
        )
        .await?
        .with_context(|| {
            format!(
                "manifest drift alert observation {} conflicted but no row was found",
                observation.observation_identity
            )
        })?,
    };
    ensure_existing_manifest_alert_matches_request(&stored, observation)?;
    Ok(stored)
}

async fn load_manifest_drift_alert_observation_by_identity(
    pool: &PgPool,
    observation_identity: &str,
) -> Result<Option<ManifestDriftAlertObservation>> {
    let row = sqlx::query(
        r#"
        SELECT
            manifest_alert_observation_id,
            observation_identity,
            observation_kind,
            lifecycle_status,
            namespace,
            source_family,
            manifest_version,
            source_manifest_id,
            chain_id,
            contract_instance_id,
            proxy_contract_instance_id,
            expected_implementation_contract_instance_id,
            observed_implementation_contract_instance_id,
            discovery_edge_id,
            expected_code_hash,
            observed_code_hash,
            observed_code_byte_length,
            observed_block_number,
            observed_block_hash,
            observed_canonicality_state::TEXT AS observed_canonicality_state,
            raw_fact_ref,
            expected_material,
            observed_material,
            watch_plan_metadata,
            alert_metadata,
            remediation_status,
            remediation_metadata,
            first_observed_at,
            last_observed_at,
            remediated_at
        FROM manifest_alert_observations
        WHERE observation_identity = $1
        "#,
    )
    .bind(observation_identity)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to load manifest drift alert observation {observation_identity}")
    })?;

    row.map(decode_manifest_drift_alert_observation).transpose()
}
