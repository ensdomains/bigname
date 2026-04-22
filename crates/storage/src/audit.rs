use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Row, types::time::OffsetDateTime};
use uuid::Uuid;

use crate::{
    CanonicalityState, ChainLineageBlock, RawPayloadCacheMetadata,
    list_raw_payload_cache_metadata_by_block_hash, load_chain_lineage_block,
};

const EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT: &str = "ManifestCodeHashDriftAlert";
const EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT: &str = "ManifestProxyImplementationAlert";
const MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND: &str = "proxy_implementation";
const MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE: &str = "manifest_declared_proxy";
const OBSERVATION_KIND_MANIFEST_DRIFT: &str = "manifest_drift";
const OBSERVATION_KIND_PROXY_IMPLEMENTATION_DRIFT: &str = "proxy_implementation_drift";

/// Audit-facing canonicality status for one requested block identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalityInspectionStatus {
    Missing,
    Observed,
    Canonical,
    Safe,
    Finalized,
    Orphaned,
}

impl From<CanonicalityState> for CanonicalityInspectionStatus {
    fn from(value: CanonicalityState) -> Self {
        match value {
            CanonicalityState::Observed => Self::Observed,
            CanonicalityState::Canonical => Self::Canonical,
            CanonicalityState::Safe => Self::Safe,
            CanonicalityState::Finalized => Self::Finalized,
            CanonicalityState::Orphaned => Self::Orphaned,
        }
    }
}

/// Block-scoped raw fact counts by storage family.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RawFactAuditCounts {
    pub raw_block_count: u64,
    pub raw_code_hash_count: u64,
    pub raw_transaction_count: u64,
    pub raw_receipt_count: u64,
    pub raw_log_count: u64,
    pub raw_call_snapshot_count: u64,
}

impl RawFactAuditCounts {
    pub const fn total(&self) -> u64 {
        self.raw_block_count
            + self.raw_code_hash_count
            + self.raw_transaction_count
            + self.raw_receipt_count
            + self.raw_log_count
            + self.raw_call_snapshot_count
    }
}

/// Read-only audit summary for retained payload-cache metadata on one block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawPayloadCacheAuditMetadata {
    pub payload_kind: String,
    pub digest_algorithm: Option<String>,
    pub retained_digest: Option<String>,
    pub block_number: Option<i64>,
    pub payload_size_bytes: i64,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub cache_metadata: Value,
    pub canonicality_state: CanonicalityState,
    pub first_observed_at: OffsetDateTime,
    pub last_observed_at: OffsetDateTime,
}

/// Read-only canonicality and fact-count inspection for one block hash.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalityInspection {
    pub chain_id: String,
    pub block_hash: String,
    pub status: CanonicalityInspectionStatus,
    pub lineage_state: Option<CanonicalityState>,
    pub parent_hash: Option<String>,
    pub block_number: Option<i64>,
    pub raw_fact_counts: RawFactAuditCounts,
    pub normalized_event_count: u64,
}

/// Stored lineage row for bounded read-only range inspection.
pub type StoredLineageRangeBlock = ChainLineageBlock;

/// Read-only stored manifest drift/proxy alert inspection.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ManifestDriftAlertInspection {
    pub code_hash_drift_alerts: Vec<ManifestDriftAlertObservation>,
    pub proxy_implementation_alerts: Vec<ManifestDriftAlertObservation>,
}

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

/// Alert family represented by a stored manifest alert observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestDriftAlertKind {
    CodeHashDrift,
    ProxyImplementation,
}

impl ManifestDriftAlertKind {
    pub const fn observation_kind(self) -> &'static str {
        match self {
            Self::CodeHashDrift => OBSERVATION_KIND_MANIFEST_DRIFT,
            Self::ProxyImplementation => OBSERVATION_KIND_PROXY_IMPLEMENTATION_DRIFT,
        }
    }

    pub const fn event_kind(self) -> &'static str {
        match self {
            Self::CodeHashDrift => EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT,
            Self::ProxyImplementation => EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT,
        }
    }

    pub const fn alert_type(self) -> &'static str {
        match self {
            Self::CodeHashDrift => "manifest_code_hash_drift",
            Self::ProxyImplementation => "manifest_proxy_implementation_edge",
        }
    }

    fn parse_observation_kind(observation_kind: &str) -> Result<Self> {
        match observation_kind {
            OBSERVATION_KIND_MANIFEST_DRIFT => Ok(Self::CodeHashDrift),
            OBSERVATION_KIND_PROXY_IMPLEMENTATION_DRIFT => Ok(Self::ProxyImplementation),
            _ => bail!("unsupported manifest drift observation kind {observation_kind}"),
        }
    }
}

/// Persisted lifecycle state for a worker-owned manifest alert observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestDriftAlertLifecycleStatus {
    Active,
    Acknowledged,
    Remediated,
    Dismissed,
}

impl ManifestDriftAlertLifecycleStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Acknowledged => "acknowledged",
            Self::Remediated => "remediated",
            Self::Dismissed => "dismissed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "acknowledged" => Ok(Self::Acknowledged),
            "remediated" => Ok(Self::Remediated),
            "dismissed" => Ok(Self::Dismissed),
            _ => bail!("unsupported manifest drift alert lifecycle status {value}"),
        }
    }
}

/// Immutable creation contract for a worker-owned manifest drift/proxy alert
/// observation. Reusing the same `observation_identity` is idempotent only when
/// all persisted alert material matches the existing row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestDriftAlertObservationCreate {
    pub observation_identity: String,
    pub alert_kind: ManifestDriftAlertKind,
    pub lifecycle_status: ManifestDriftAlertLifecycleStatus,
    pub namespace: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: String,
    pub contract_instance_id: Uuid,
    pub proxy_contract_instance_id: Option<Uuid>,
    pub expected_implementation_contract_instance_id: Option<Uuid>,
    pub observed_implementation_contract_instance_id: Option<Uuid>,
    pub discovery_edge_id: Option<i64>,
    pub expected_code_hash: Option<String>,
    pub observed_code_hash: Option<String>,
    pub observed_code_byte_length: Option<i64>,
    pub observed_block_number: Option<i64>,
    pub observed_block_hash: Option<String>,
    pub observed_canonicality_state: Option<CanonicalityState>,
    pub raw_fact_ref: Value,
    pub expected_material: Value,
    pub observed_material: Value,
    pub watch_plan_metadata: Value,
    pub alert_metadata: Value,
    pub remediation_status: Option<String>,
    pub remediation_metadata: Option<Value>,
    pub first_observed_at: OffsetDateTime,
    pub last_observed_at: OffsetDateTime,
    pub remediated_at: Option<OffsetDateTime>,
}

/// One stored manifest drift/proxy alert observation. The `normalized_event_id`
/// field is the alert observation row id kept under its historic API name so
/// existing worker inspection rendering remains source-compatible.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestDriftAlertObservation {
    pub normalized_event_id: i64,
    pub event_identity: String,
    pub alert_kind: ManifestDriftAlertKind,
    pub namespace: String,
    pub source_family: String,
    pub manifest_version: i64,
    pub source_manifest_id: Option<i64>,
    pub chain_id: Option<String>,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub raw_fact_ref: Value,
    pub canonicality_state: CanonicalityState,
    pub alert_state: Value,
    pub observed_at: OffsetDateTime,
}

/// Inspect one block by hash-first identity without mutating storage.
pub async fn inspect_block_canonicality(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<CanonicalityInspection> {
    validate_block_identity(chain_id, block_hash)?;

    let lineage = load_chain_lineage_block(pool, chain_id, block_hash).await?;
    let raw_fact_counts = load_raw_fact_counts(pool, chain_id, block_hash).await?;
    let normalized_event_count = load_normalized_event_count(pool, chain_id, block_hash).await?;

    Ok(build_inspection(
        chain_id,
        block_hash,
        lineage,
        raw_fact_counts,
        normalized_event_count,
    ))
}

/// List retained payload-cache metadata for audit tooling without dereferencing
/// object-backed cache or re-fetching provider bytes.
pub async fn list_raw_payload_cache_audit_metadata(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<Vec<RawPayloadCacheAuditMetadata>> {
    validate_block_identity(chain_id, block_hash)?;

    let rows = list_raw_payload_cache_metadata_by_block_hash(pool, chain_id, block_hash).await?;
    Ok(rows
        .into_iter()
        .map(raw_payload_cache_audit_metadata)
        .collect())
}

/// Inspect every stored lineage block in a bounded block-number range. Missing
/// heights cannot be inferred without a requested block hash, so this returns
/// only stored lineage identities in range order.
pub async fn inspect_canonicality_range(
    pool: &PgPool,
    chain_id: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<Vec<CanonicalityInspection>> {
    validate_range(chain_id, range_start_block_number, range_end_block_number)?;

    let rows = sqlx::query(
        r#"
        SELECT block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number >= $2
          AND block_number <= $3
        ORDER BY block_number, block_hash
        "#,
    )
    .bind(chain_id)
    .bind(range_start_block_number)
    .bind(range_end_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load lineage block hashes for chain {chain_id} range {range_start_block_number}..={range_end_block_number}"
        )
    })?;

    let mut inspections = Vec::with_capacity(rows.len());
    for row in rows {
        let block_hash = row
            .try_get::<String, _>("block_hash")
            .context("missing block_hash from canonicality range row")?;
        inspections.push(inspect_block_canonicality(pool, chain_id, &block_hash).await?);
    }

    Ok(inspections)
}

/// List only stored lineage rows in a bounded block-number range. The helper
/// does not infer missing heights, gaps, range completeness, or span-wide
/// canonicality.
pub async fn list_stored_lineage_range(
    pool: &PgPool,
    chain_id: &str,
    range_start_block_number: i64,
    range_end_block_number: i64,
) -> Result<Vec<StoredLineageRangeBlock>> {
    validate_range(chain_id, range_start_block_number, range_end_block_number)?;

    let rows = sqlx::query(
        r#"
        SELECT
            chain_id,
            block_hash,
            parent_hash,
            block_number,
            block_timestamp,
            logs_bloom,
            transactions_root,
            receipts_root,
            state_root,
            canonicality_state::TEXT AS canonicality_state
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number >= $2
          AND block_number <= $3
        ORDER BY block_number, block_hash
        "#,
    )
    .bind(chain_id)
    .bind(range_start_block_number)
    .bind(range_end_block_number)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to list stored lineage rows for chain {chain_id} range {range_start_block_number}..={range_end_block_number}"
        )
    })?;

    rows.into_iter().map(decode_stored_lineage_block).collect()
}

/// List stored manifest drift and proxy implementation alert observations.
/// The helper reads the worker-owned manifest-alert table; it does not
/// compare chain state, create alerts, update alert lifecycle, or mutate
/// manifest/discovery state.
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

async fn load_live_code_hash_drift_candidates(pool: &PgPool) -> Result<Vec<Value>> {
    let rows = sqlx::query(
        r#"
        WITH active_targets AS (
            SELECT
                mv.manifest_id,
                mv.manifest_version,
                mv.namespace,
                mv.source_family,
                mv.chain,
                mv.deployment_epoch,
                mci.declaration_kind,
                mci.declaration_name,
                mci.contract_instance_id,
                lower(mci.declared_address) AS declared_address,
                mci.code_hash AS expected_code_hash,
                CASE
                    WHEN mci.declaration_kind = 'root' THEN 'manifest_root'
                    ELSE 'manifest_contract'
                END::TEXT AS watched_source,
                cia.active_from_block_number,
                cia.active_to_block_number
            FROM manifest_versions mv
            JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
            JOIN contract_instance_addresses cia
              ON cia.contract_instance_id = mci.contract_instance_id
             AND cia.deactivated_at IS NULL
            WHERE mv.rollout_status = 'active'
              AND mci.code_hash IS NOT NULL
        ),
        latest_code AS (
            SELECT DISTINCT ON (
                active_targets.chain,
                active_targets.contract_instance_id,
                active_targets.declared_address
            )
                active_targets.*,
                raw_code_hashes.raw_code_hash_id,
                raw_code_hashes.block_hash AS observed_block_hash,
                raw_code_hashes.block_number AS observed_block_number,
                raw_code_hashes.code_hash AS observed_code_hash,
                raw_code_hashes.code_byte_length AS observed_code_byte_length,
                raw_code_hashes.canonicality_state::TEXT AS observed_canonicality_state,
                raw_code_hashes.observed_at AS raw_observed_at
            FROM active_targets
            JOIN raw_code_hashes
              ON raw_code_hashes.chain_id = active_targets.chain
             AND lower(raw_code_hashes.contract_address) = active_targets.declared_address
            WHERE raw_code_hashes.canonicality_state <> 'orphaned'
            ORDER BY
                active_targets.chain,
                active_targets.contract_instance_id,
                active_targets.declared_address,
                raw_code_hashes.block_number DESC,
                CASE raw_code_hashes.canonicality_state
                    WHEN 'finalized' THEN 4
                    WHEN 'safe' THEN 3
                    WHEN 'canonical' THEN 2
                    WHEN 'observed' THEN 1
                    ELSE 0
                END DESC,
                raw_code_hashes.raw_code_hash_id DESC
        )
        SELECT *
        FROM latest_code
        WHERE observed_code_hash <> expected_code_hash
        ORDER BY namespace, source_family, chain, declaration_kind, declaration_name, declared_address
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to compute live manifest code-hash drift audit candidates")?;

    rows.into_iter()
        .map(render_live_code_hash_drift_candidate)
        .collect()
}

async fn load_live_proxy_implementation_candidates(pool: &PgPool) -> Result<Vec<Value>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id,
            mv.manifest_version,
            mv.namespace,
            mv.source_family,
            mv.chain,
            mci.declaration_name,
            mci.role,
            mci.proxy_kind,
            mci.contract_instance_id AS proxy_contract_instance_id,
            lower(mci.declared_address) AS proxy_address,
            mci.implementation_contract_instance_id AS expected_implementation_contract_instance_id,
            lower(mci.declared_implementation_address) AS expected_implementation_address,
            de.discovery_edge_id,
            de.to_contract_instance_id AS observed_implementation_contract_instance_id,
            lower(implementation_address.address) AS observed_implementation_address,
            de.admission,
            de.active_from_block_number,
            de.active_to_block_number,
            de.provenance
        FROM manifest_versions mv
        JOIN manifest_contract_instances mci ON mci.manifest_id = mv.manifest_id
        LEFT JOIN discovery_edges de
          ON de.source_manifest_id = mv.manifest_id
         AND de.from_contract_instance_id = mci.contract_instance_id
         AND de.edge_kind = $1
         AND de.discovery_source = $2
         AND de.deactivated_at IS NULL
        LEFT JOIN contract_instance_addresses implementation_address
          ON implementation_address.contract_instance_id = de.to_contract_instance_id
         AND implementation_address.deactivated_at IS NULL
        WHERE mv.rollout_status = 'active'
          AND mci.declaration_kind = 'contract'
          AND mci.proxy_kind IS NOT NULL
          AND mci.proxy_kind <> 'none'
          AND mci.implementation_contract_instance_id IS NOT NULL
          AND (
              de.discovery_edge_id IS NULL
              OR de.to_contract_instance_id <> mci.implementation_contract_instance_id
          )
        ORDER BY mv.namespace, mv.source_family, mv.chain, mci.declaration_name, mci.declared_address
        "#,
    )
    .bind(MANIFEST_PROXY_IMPLEMENTATION_EDGE_KIND)
    .bind(MANIFEST_PROXY_IMPLEMENTATION_DISCOVERY_SOURCE)
    .fetch_all(pool)
    .await
    .context("failed to compute live manifest proxy implementation audit candidates")?;

    rows.into_iter()
        .map(render_live_proxy_implementation_candidate)
        .collect()
}

fn build_inspection(
    chain_id: &str,
    block_hash: &str,
    lineage: Option<ChainLineageBlock>,
    raw_fact_counts: RawFactAuditCounts,
    normalized_event_count: u64,
) -> CanonicalityInspection {
    let status = lineage
        .as_ref()
        .map(|block| CanonicalityInspectionStatus::from(block.canonicality_state))
        .unwrap_or(CanonicalityInspectionStatus::Missing);
    let lineage_state = lineage.as_ref().map(|block| block.canonicality_state);
    let parent_hash = lineage.as_ref().and_then(|block| block.parent_hash.clone());
    let block_number = lineage.as_ref().map(|block| block.block_number);

    CanonicalityInspection {
        chain_id: chain_id.to_owned(),
        block_hash: block_hash.to_owned(),
        status,
        lineage_state,
        parent_hash,
        block_number,
        raw_fact_counts,
        normalized_event_count,
    }
}

fn raw_payload_cache_audit_metadata(row: RawPayloadCacheMetadata) -> RawPayloadCacheAuditMetadata {
    RawPayloadCacheAuditMetadata {
        payload_kind: row.payload_kind,
        digest_algorithm: row.digest_algorithm,
        retained_digest: row.retained_digest,
        block_number: row.block_number,
        payload_size_bytes: row.payload_size_bytes,
        content_type: row.content_type,
        content_encoding: row.content_encoding,
        cache_metadata: row.cache_metadata,
        canonicality_state: row.canonicality_state,
        first_observed_at: row.first_observed_at,
        last_observed_at: row.last_observed_at,
    }
}

async fn load_raw_fact_counts(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<RawFactAuditCounts> {
    let row = sqlx::query(
        r#"
        SELECT
          (SELECT COUNT(*)::BIGINT FROM raw_blocks WHERE chain_id = $1 AND block_hash = $2) AS raw_block_count,
          (SELECT COUNT(*)::BIGINT FROM raw_code_hashes WHERE chain_id = $1 AND block_hash = $2) AS raw_code_hash_count,
          (SELECT COUNT(*)::BIGINT FROM raw_transactions WHERE chain_id = $1 AND block_hash = $2) AS raw_transaction_count,
          (SELECT COUNT(*)::BIGINT FROM raw_receipts WHERE chain_id = $1 AND block_hash = $2) AS raw_receipt_count,
          (SELECT COUNT(*)::BIGINT FROM raw_logs WHERE chain_id = $1 AND block_hash = $2) AS raw_log_count,
          (SELECT COUNT(*)::BIGINT FROM raw_call_snapshots WHERE chain_id = $1 AND block_hash = $2) AS raw_call_snapshot_count
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_one(pool)
    .await
    .with_context(|| format!("failed to load raw fact audit counts for chain {chain_id} block {block_hash}"))?;

    Ok(RawFactAuditCounts {
        raw_block_count: decode_count(&row, "raw_block_count")?,
        raw_code_hash_count: decode_count(&row, "raw_code_hash_count")?,
        raw_transaction_count: decode_count(&row, "raw_transaction_count")?,
        raw_receipt_count: decode_count(&row, "raw_receipt_count")?,
        raw_log_count: decode_count(&row, "raw_log_count")?,
        raw_call_snapshot_count: decode_count(&row, "raw_call_snapshot_count")?,
    })
}

async fn load_normalized_event_count(
    pool: &PgPool,
    chain_id: &str,
    block_hash: &str,
) -> Result<u64> {
    let row = sqlx::query(
        r#"
        SELECT COUNT(*)::BIGINT AS normalized_event_count
        FROM normalized_events
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(chain_id)
    .bind(block_hash)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load normalized-event audit count for chain {chain_id} block {block_hash}"
        )
    })?;

    decode_count(&row, "normalized_event_count")
}

fn decode_stored_lineage_block(row: sqlx::postgres::PgRow) -> Result<StoredLineageRangeBlock> {
    Ok(ChainLineageBlock {
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_hash: row.try_get("block_hash").context("missing block_hash")?,
        parent_hash: row.try_get("parent_hash").context("missing parent_hash")?,
        block_number: row
            .try_get("block_number")
            .context("missing block_number")?,
        block_timestamp: row
            .try_get("block_timestamp")
            .context("missing block_timestamp")?,
        logs_bloom: row.try_get("logs_bloom").context("missing logs_bloom")?,
        transactions_root: row
            .try_get("transactions_root")
            .context("missing transactions_root")?,
        receipts_root: row
            .try_get("receipts_root")
            .context("missing receipts_root")?,
        state_root: row.try_get("state_root").context("missing state_root")?,
        canonicality_state: CanonicalityState::parse(
            &row.try_get::<String, _>("canonicality_state")
                .context("missing canonicality_state")?,
        )?,
    })
}

fn decode_manifest_drift_alert_observation(
    row: sqlx::postgres::PgRow,
) -> Result<ManifestDriftAlertObservation> {
    let observation_kind = row
        .try_get::<String, _>("observation_kind")
        .context("missing observation_kind")?;
    let alert_kind = ManifestDriftAlertKind::parse_observation_kind(&observation_kind)?;
    let lifecycle_status = ManifestDriftAlertLifecycleStatus::parse(
        &row.try_get::<String, _>("lifecycle_status")
            .context("missing lifecycle_status")?,
    )?;
    let observed_canonicality_state = row
        .try_get::<Option<String>, _>("observed_canonicality_state")
        .context("missing observed_canonicality_state")?
        .map(|value| CanonicalityState::parse(&value))
        .transpose()?;
    let last_observed_at = row
        .try_get("last_observed_at")
        .context("missing last_observed_at")?;
    let raw_fact_ref = row
        .try_get("raw_fact_ref")
        .context("missing raw_fact_ref")?;
    let alert_state = build_manifest_alert_state(
        alert_kind,
        lifecycle_status,
        &row,
        observed_canonicality_state,
    )?;

    Ok(ManifestDriftAlertObservation {
        normalized_event_id: row
            .try_get("manifest_alert_observation_id")
            .context("missing manifest_alert_observation_id")?,
        event_identity: row
            .try_get("observation_identity")
            .context("missing observation_identity")?,
        alert_kind,
        namespace: row.try_get("namespace").context("missing namespace")?,
        source_family: row
            .try_get("source_family")
            .context("missing source_family")?,
        manifest_version: row
            .try_get("manifest_version")
            .context("missing manifest_version")?,
        source_manifest_id: row
            .try_get("source_manifest_id")
            .context("missing source_manifest_id")?,
        chain_id: row.try_get("chain_id").context("missing chain_id")?,
        block_number: row
            .try_get("observed_block_number")
            .context("missing observed_block_number")?,
        block_hash: row
            .try_get("observed_block_hash")
            .context("missing observed_block_hash")?,
        raw_fact_ref,
        canonicality_state: observed_canonicality_state.unwrap_or(CanonicalityState::Observed),
        alert_state,
        observed_at: last_observed_at,
    })
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

fn build_manifest_alert_state(
    alert_kind: ManifestDriftAlertKind,
    lifecycle_status: ManifestDriftAlertLifecycleStatus,
    row: &sqlx::postgres::PgRow,
    observed_canonicality_state: Option<CanonicalityState>,
) -> Result<Value> {
    let mut state = json_object(
        row.try_get("alert_metadata")
            .context("missing alert_metadata")?,
    )?;
    let expected_material: Value = row
        .try_get("expected_material")
        .context("missing expected_material")?;
    let observed_material: Value = row
        .try_get("observed_material")
        .context("missing observed_material")?;
    let watch_plan_metadata: Value = row
        .try_get("watch_plan_metadata")
        .context("missing watch_plan_metadata")?;

    insert_json(&mut state, "alert_type", alert_kind.alert_type());
    insert_json(&mut state, "alert_status", lifecycle_status.as_str());
    insert_json(
        &mut state,
        "source_family",
        row.try_get::<String, _>("source_family")
            .context("missing source_family")?,
    );
    insert_json(
        &mut state,
        "chain",
        row.try_get::<String, _>("chain_id")
            .context("missing chain_id")?,
    );
    insert_optional_json(
        &mut state,
        "source_manifest_id",
        row.try_get::<Option<i64>, _>("source_manifest_id")
            .context("missing source_manifest_id")?,
    );
    insert_optional_json(
        &mut state,
        "remediation_status",
        row.try_get::<Option<String>, _>("remediation_status")
            .context("missing remediation_status")?,
    );
    insert_optional_json(
        &mut state,
        "remediation_metadata",
        row.try_get::<Option<Value>, _>("remediation_metadata")
            .context("missing remediation_metadata")?,
    );

    merge_json_object(&mut state, "expected_material", expected_material)?;
    merge_json_object(&mut state, "observed_material", observed_material)?;
    merge_json_object(&mut state, "watch_plan_metadata", watch_plan_metadata)?;

    match alert_kind {
        ManifestDriftAlertKind::CodeHashDrift => {
            insert_uuid(
                &mut state,
                "contract_instance_id",
                row.try_get::<Option<Uuid>, _>("contract_instance_id")
                    .context("missing contract_instance_id")?,
            );
            insert_optional_json(
                &mut state,
                "expected_code_hash",
                row.try_get::<Option<String>, _>("expected_code_hash")
                    .context("missing expected_code_hash")?,
            );
            insert_optional_json(
                &mut state,
                "observed_code_hash",
                row.try_get::<Option<String>, _>("observed_code_hash")
                    .context("missing observed_code_hash")?,
            );
            insert_optional_json(
                &mut state,
                "observed_code_byte_length",
                row.try_get::<Option<i64>, _>("observed_code_byte_length")
                    .context("missing observed_code_byte_length")?,
            );
            insert_optional_json(
                &mut state,
                "observed_block_number",
                row.try_get::<Option<i64>, _>("observed_block_number")
                    .context("missing observed_block_number")?,
            );
            insert_optional_json(
                &mut state,
                "observed_block_hash",
                row.try_get::<Option<String>, _>("observed_block_hash")
                    .context("missing observed_block_hash")?,
            );
            insert_optional_json(
                &mut state,
                "observed_canonicality_state",
                observed_canonicality_state.map(CanonicalityState::as_str),
            );
        }
        ManifestDriftAlertKind::ProxyImplementation => {
            insert_uuid(
                &mut state,
                "proxy_contract_instance_id",
                row.try_get::<Option<Uuid>, _>("proxy_contract_instance_id")
                    .context("missing proxy_contract_instance_id")?,
            );
            insert_uuid(
                &mut state,
                "expected_implementation_contract_instance_id",
                row.try_get::<Option<Uuid>, _>("expected_implementation_contract_instance_id")
                    .context("missing expected_implementation_contract_instance_id")?,
            );
            insert_uuid(
                &mut state,
                "observed_implementation_contract_instance_id",
                row.try_get::<Option<Uuid>, _>("observed_implementation_contract_instance_id")
                    .context("missing observed_implementation_contract_instance_id")?,
            );
            insert_uuid(
                &mut state,
                "implementation_contract_instance_id",
                row.try_get::<Option<Uuid>, _>("observed_implementation_contract_instance_id")
                    .context("missing observed_implementation_contract_instance_id")?,
            );
            insert_optional_json(
                &mut state,
                "discovery_edge_id",
                row.try_get::<Option<i64>, _>("discovery_edge_id")
                    .context("missing discovery_edge_id")?,
            );
        }
    }

    Ok(Value::Object(state))
}

fn validate_manifest_drift_alert_observation_create(
    observation: &ManifestDriftAlertObservationCreate,
) -> Result<()> {
    if observation.observation_identity.trim().is_empty() {
        bail!("manifest drift alert observation_identity must not be empty");
    }
    if observation.namespace.trim().is_empty() {
        bail!("manifest drift alert namespace must not be empty");
    }
    if observation.source_family.trim().is_empty() {
        bail!("manifest drift alert source_family must not be empty");
    }
    if observation.manifest_version <= 0 {
        bail!(
            "manifest drift alert {} has non-positive manifest_version {}",
            observation.observation_identity,
            observation.manifest_version
        );
    }
    if observation.chain_id.trim().is_empty() {
        bail!("manifest drift alert chain_id must not be empty");
    }
    if observation
        .observed_code_byte_length
        .is_some_and(|value| value < 0)
    {
        bail!(
            "manifest drift alert {} has negative observed_code_byte_length",
            observation.observation_identity
        );
    }
    if observation
        .observed_block_number
        .is_some_and(|value| value < 0)
    {
        bail!(
            "manifest drift alert {} has negative observed_block_number",
            observation.observation_identity
        );
    }
    if observation.observed_block_number.is_some() != observation.observed_block_hash.is_some() {
        bail!(
            "manifest drift alert {} must include observed_block_number and observed_block_hash together",
            observation.observation_identity
        );
    }
    if observation.last_observed_at < observation.first_observed_at {
        bail!(
            "manifest drift alert {} last_observed_at is before first_observed_at",
            observation.observation_identity
        );
    }
    if observation
        .remediated_at
        .is_some_and(|value| value < observation.first_observed_at)
    {
        bail!(
            "manifest drift alert {} remediated_at is before first_observed_at",
            observation.observation_identity
        );
    }
    ensure_json_object(
        "manifest drift alert raw_fact_ref",
        &observation.raw_fact_ref,
    )?;
    ensure_json_object(
        "manifest drift alert expected_material",
        &observation.expected_material,
    )?;
    ensure_json_object(
        "manifest drift alert observed_material",
        &observation.observed_material,
    )?;
    ensure_json_object(
        "manifest drift alert watch_plan_metadata",
        &observation.watch_plan_metadata,
    )?;
    ensure_json_object(
        "manifest drift alert alert_metadata",
        &observation.alert_metadata,
    )?;
    if let Some(metadata) = &observation.remediation_metadata {
        ensure_json_object("manifest drift alert remediation_metadata", metadata)?;
    }

    match observation.alert_kind {
        ManifestDriftAlertKind::CodeHashDrift => {
            if observation.proxy_contract_instance_id.is_some() {
                bail!(
                    "manifest code-hash drift alert {} must not set proxy_contract_instance_id",
                    observation.observation_identity
                );
            }
            if observation.expected_code_hash.is_none()
                || observation.observed_code_hash.is_none()
                || observation.observed_canonicality_state.is_none()
            {
                bail!(
                    "manifest code-hash drift alert {} must include expected and observed code-hash material",
                    observation.observation_identity
                );
            }
        }
        ManifestDriftAlertKind::ProxyImplementation => {
            if observation.proxy_contract_instance_id != Some(observation.contract_instance_id) {
                bail!(
                    "manifest proxy implementation alert {} must preserve the proxy contract_instance_id as the alert subject",
                    observation.observation_identity
                );
            }
        }
    }

    Ok(())
}

fn manifest_alert_observation_create_from_rendered(
    observation: &ManifestDriftAlertObservation,
) -> Result<ManifestDriftAlertObservationCreate> {
    let lifecycle_status = ManifestDriftAlertLifecycleStatus::parse(
        observation
            .alert_state
            .get("alert_status")
            .and_then(Value::as_str)
            .unwrap_or(ManifestDriftAlertLifecycleStatus::Active.as_str()),
    )?;
    let chain_id = observation
        .chain_id
        .clone()
        .or_else(|| alert_state_string_owned(observation, "chain"))
        .context("manifest drift alert observation is missing chain_id")?;
    let source_manifest_id = observation.source_manifest_id.or_else(|| {
        observation
            .alert_state
            .get("source_manifest_id")
            .and_then(Value::as_i64)
    });
    let contract_instance_id = match observation.alert_kind {
        ManifestDriftAlertKind::CodeHashDrift => {
            parse_required_alert_uuid(observation, "contract_instance_id")?
        }
        ManifestDriftAlertKind::ProxyImplementation => {
            parse_required_alert_uuid(observation, "proxy_contract_instance_id")?
        }
    };
    let proxy_contract_instance_id = match observation.alert_kind {
        ManifestDriftAlertKind::CodeHashDrift => None,
        ManifestDriftAlertKind::ProxyImplementation => Some(contract_instance_id),
    };
    let observed_implementation_contract_instance_id =
        parse_optional_alert_uuid(observation, "observed_implementation_contract_instance_id")?
            .or_else(|| {
                parse_optional_alert_uuid(observation, "implementation_contract_instance_id")
                    .ok()
                    .flatten()
            });

    Ok(ManifestDriftAlertObservationCreate {
        observation_identity: observation.event_identity.clone(),
        alert_kind: observation.alert_kind,
        lifecycle_status,
        namespace: observation.namespace.clone(),
        source_family: observation.source_family.clone(),
        manifest_version: observation.manifest_version,
        source_manifest_id,
        chain_id,
        contract_instance_id,
        proxy_contract_instance_id,
        expected_implementation_contract_instance_id: parse_optional_alert_uuid(
            observation,
            "expected_implementation_contract_instance_id",
        )?,
        observed_implementation_contract_instance_id,
        discovery_edge_id: observation
            .alert_state
            .get("discovery_edge_id")
            .and_then(Value::as_i64)
            .or_else(|| {
                observation
                    .raw_fact_ref
                    .get("discovery_edge_id")
                    .and_then(Value::as_i64)
            }),
        expected_code_hash: alert_state_string_owned(observation, "expected_code_hash"),
        observed_code_hash: alert_state_string_owned(observation, "observed_code_hash"),
        observed_code_byte_length: observation
            .alert_state
            .get("observed_code_byte_length")
            .and_then(Value::as_i64),
        observed_block_number: observation.block_number.or_else(|| {
            observation
                .alert_state
                .get("observed_block_number")
                .and_then(Value::as_i64)
        }),
        observed_block_hash: observation
            .block_hash
            .clone()
            .or_else(|| alert_state_string_owned(observation, "observed_block_hash")),
        observed_canonicality_state: Some(observation.canonicality_state),
        raw_fact_ref: observation.raw_fact_ref.clone(),
        expected_material: json!({}),
        observed_material: json!({}),
        watch_plan_metadata: json!({}),
        alert_metadata: observation.alert_state.clone(),
        remediation_status: alert_state_string_owned(observation, "remediation_status"),
        remediation_metadata: observation
            .alert_state
            .get("remediation_metadata")
            .cloned()
            .or_else(|| observation.alert_state.get("remediation").cloned()),
        first_observed_at: observation.observed_at,
        last_observed_at: observation.observed_at,
        remediated_at: None,
    })
}

fn alert_state_string_owned(
    observation: &ManifestDriftAlertObservation,
    field: &str,
) -> Option<String> {
    observation
        .alert_state
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn parse_required_alert_uuid(
    observation: &ManifestDriftAlertObservation,
    field: &str,
) -> Result<Uuid> {
    let value = alert_state_string_owned(observation, field)
        .with_context(|| format!("manifest drift alert observation is missing {field}"))?;
    Uuid::parse_str(&value)
        .with_context(|| format!("manifest drift alert observation has invalid {field}"))
}

fn parse_optional_alert_uuid(
    observation: &ManifestDriftAlertObservation,
    field: &str,
) -> Result<Option<Uuid>> {
    alert_state_string_owned(observation, field)
        .map(|value| {
            Uuid::parse_str(&value)
                .with_context(|| format!("manifest drift alert observation has invalid {field}"))
        })
        .transpose()
}

fn ensure_existing_manifest_alert_matches_request(
    stored: &ManifestDriftAlertObservation,
    request: &ManifestDriftAlertObservationCreate,
) -> Result<()> {
    let expected_alert_state = manifest_alert_state_from_create(request)?;
    let expected_canonicality = request
        .observed_canonicality_state
        .unwrap_or(CanonicalityState::Observed);

    if stored.event_identity != request.observation_identity
        || stored.alert_kind != request.alert_kind
        || stored.namespace != request.namespace
        || stored.source_family != request.source_family
        || stored.manifest_version != request.manifest_version
        || stored.source_manifest_id != request.source_manifest_id
        || stored.chain_id.as_deref() != Some(request.chain_id.as_str())
        || stored.block_number != request.observed_block_number
        || stored.block_hash != request.observed_block_hash
        || stored.raw_fact_ref != request.raw_fact_ref
        || stored.canonicality_state != expected_canonicality
        || stored.alert_state != expected_alert_state
        || stored.observed_at != request.last_observed_at
    {
        bail!(
            "manifest drift alert observation {} already exists with different persisted material",
            request.observation_identity
        );
    }

    Ok(())
}

fn manifest_alert_state_from_create(
    observation: &ManifestDriftAlertObservationCreate,
) -> Result<Value> {
    let mut state = json_object(observation.alert_metadata.clone())?;

    insert_json(
        &mut state,
        "alert_type",
        observation.alert_kind.alert_type(),
    );
    insert_json(
        &mut state,
        "alert_status",
        observation.lifecycle_status.as_str(),
    );
    insert_json(
        &mut state,
        "source_family",
        observation.source_family.clone(),
    );
    insert_json(&mut state, "chain", observation.chain_id.clone());
    insert_optional_json(
        &mut state,
        "source_manifest_id",
        observation.source_manifest_id,
    );
    insert_optional_json(
        &mut state,
        "remediation_status",
        observation.remediation_status.clone(),
    );
    insert_optional_json(
        &mut state,
        "remediation_metadata",
        observation.remediation_metadata.clone(),
    );
    merge_json_object(
        &mut state,
        "expected_material",
        observation.expected_material.clone(),
    )?;
    merge_json_object(
        &mut state,
        "observed_material",
        observation.observed_material.clone(),
    )?;
    merge_json_object(
        &mut state,
        "watch_plan_metadata",
        observation.watch_plan_metadata.clone(),
    )?;

    match observation.alert_kind {
        ManifestDriftAlertKind::CodeHashDrift => {
            insert_json(
                &mut state,
                "contract_instance_id",
                observation.contract_instance_id.to_string(),
            );
            insert_optional_json(
                &mut state,
                "expected_code_hash",
                observation.expected_code_hash.clone(),
            );
            insert_optional_json(
                &mut state,
                "observed_code_hash",
                observation.observed_code_hash.clone(),
            );
            insert_optional_json(
                &mut state,
                "observed_code_byte_length",
                observation.observed_code_byte_length,
            );
            insert_optional_json(
                &mut state,
                "observed_block_number",
                observation.observed_block_number,
            );
            insert_optional_json(
                &mut state,
                "observed_block_hash",
                observation.observed_block_hash.clone(),
            );
            insert_optional_json(
                &mut state,
                "observed_canonicality_state",
                observation
                    .observed_canonicality_state
                    .map(CanonicalityState::as_str),
            );
        }
        ManifestDriftAlertKind::ProxyImplementation => {
            insert_json(
                &mut state,
                "proxy_contract_instance_id",
                observation.contract_instance_id.to_string(),
            );
            insert_optional_json(
                &mut state,
                "expected_implementation_contract_instance_id",
                observation
                    .expected_implementation_contract_instance_id
                    .map(|value| value.to_string()),
            );
            insert_optional_json(
                &mut state,
                "observed_implementation_contract_instance_id",
                observation
                    .observed_implementation_contract_instance_id
                    .map(|value| value.to_string()),
            );
            insert_optional_json(
                &mut state,
                "implementation_contract_instance_id",
                observation
                    .observed_implementation_contract_instance_id
                    .map(|value| value.to_string()),
            );
            insert_optional_json(
                &mut state,
                "discovery_edge_id",
                observation.discovery_edge_id,
            );
        }
    }

    Ok(Value::Object(state))
}

fn serialize_json_object(context: &str, value: &Value) -> Result<String> {
    ensure_json_object(context, value)?;
    serde_json::to_string(value).with_context(|| format!("failed to serialize {context}"))
}

fn ensure_json_object(context: &str, value: &Value) -> Result<()> {
    if !value.is_object() {
        bail!("{context} must be a JSON object");
    }
    Ok(())
}

fn json_object(value: Value) -> Result<Map<String, Value>> {
    match value {
        Value::Object(object) => Ok(object),
        _ => bail!("manifest drift alert JSON material must be an object"),
    }
}

fn merge_json_object(state: &mut Map<String, Value>, context: &str, value: Value) -> Result<()> {
    for (key, value) in
        json_object(value).with_context(|| format!("{context} must be an object"))?
    {
        state.insert(key, value);
    }
    Ok(())
}

fn insert_json<T>(state: &mut Map<String, Value>, key: &str, value: T)
where
    T: Into<Value>,
{
    state.insert(key.to_owned(), value.into());
}

fn insert_optional_json<T>(state: &mut Map<String, Value>, key: &str, value: Option<T>)
where
    T: Into<Value>,
{
    if let Some(value) = value {
        insert_json(state, key, value);
    }
}

fn insert_uuid(state: &mut Map<String, Value>, key: &str, value: Option<Uuid>) {
    if let Some(value) = value {
        insert_json(state, key, value.to_string());
    }
}

fn render_live_code_hash_drift_candidate(row: sqlx::postgres::PgRow) -> Result<Value> {
    let contract_instance_id: Uuid = row
        .try_get("contract_instance_id")
        .context("missing live code-hash contract_instance_id")?;
    let manifest_id: i64 = row
        .try_get("manifest_id")
        .context("missing live code-hash manifest_id")?;
    let raw_code_hash_id: i64 = row
        .try_get("raw_code_hash_id")
        .context("missing live code-hash raw_code_hash_id")?;
    let raw_observed_at: OffsetDateTime = row
        .try_get("raw_observed_at")
        .context("missing live code-hash raw_observed_at")?;

    Ok(json!({
        "alert_type": ManifestDriftAlertKind::CodeHashDrift.alert_type(),
        "event_kind": ManifestDriftAlertKind::CodeHashDrift.event_kind(),
        "candidate_identity": format!(
            "live_manifest_drift:code_hash:{manifest_id}:{contract_instance_id}:{raw_code_hash_id}"
        ),
        "namespace": row.try_get::<String, _>("namespace").context("missing live code-hash namespace")?,
        "source_family": row.try_get::<String, _>("source_family").context("missing live code-hash source_family")?,
        "manifest_version": row.try_get::<i64, _>("manifest_version").context("missing live code-hash manifest_version")?,
        "source_manifest_id": manifest_id,
        "chain": row.try_get::<String, _>("chain").context("missing live code-hash chain")?,
        "deployment_epoch": row.try_get::<String, _>("deployment_epoch").context("missing live code-hash deployment_epoch")?,
        "lifecycle": {
            "status": "candidate",
            "active": true,
            "persisted": false,
        },
        "declaration": {
            "kind": row.try_get::<String, _>("declaration_kind").context("missing live code-hash declaration_kind")?,
            "name": row.try_get::<String, _>("declaration_name").context("missing live code-hash declaration_name")?,
        },
        "contract": {
            "contract_instance_id": contract_instance_id.to_string(),
            "address": row.try_get::<String, _>("declared_address").context("missing live code-hash declared_address")?,
        },
        "code_hash": {
            "expected": row.try_get::<String, _>("expected_code_hash").context("missing live code-hash expected_code_hash")?,
            "observed": row.try_get::<String, _>("observed_code_hash").context("missing live code-hash observed_code_hash")?,
            "observed_byte_length": row.try_get::<i64, _>("observed_code_byte_length").context("missing live code-hash observed_code_byte_length")?,
        },
        "observed_block": {
            "number": row.try_get::<i64, _>("observed_block_number").context("missing live code-hash observed_block_number")?,
            "hash": row.try_get::<String, _>("observed_block_hash").context("missing live code-hash observed_block_hash")?,
            "canonicality_state": row.try_get::<String, _>("observed_canonicality_state").context("missing live code-hash observed_canonicality_state")?,
        },
        "watched_target": {
            "source": row.try_get::<String, _>("watched_source").context("missing live code-hash watched_source")?,
            "source_manifest_id": manifest_id,
            "active_block_range": {
                "from_block_number": row.try_get::<Option<i64>, _>("active_from_block_number").context("missing live code-hash active_from_block_number")?,
                "to_block_number": row.try_get::<Option<i64>, _>("active_to_block_number").context("missing live code-hash active_to_block_number")?,
            },
            "raw_fact_ref": {
                "raw_code_hash_id": raw_code_hash_id,
            },
        },
        "timestamps": {
            "observed_at": format_timestamp(raw_observed_at),
        },
        "remediation": Value::Null,
    }))
}

fn render_live_proxy_implementation_candidate(row: sqlx::postgres::PgRow) -> Result<Value> {
    let manifest_id: i64 = row
        .try_get("manifest_id")
        .context("missing live proxy manifest_id")?;
    let proxy_contract_instance_id: Uuid = row
        .try_get("proxy_contract_instance_id")
        .context("missing live proxy proxy_contract_instance_id")?;
    let expected_implementation_contract_instance_id: Uuid = row
        .try_get("expected_implementation_contract_instance_id")
        .context("missing live proxy expected_implementation_contract_instance_id")?;
    let observed_implementation_contract_instance_id: Option<Uuid> = row
        .try_get("observed_implementation_contract_instance_id")
        .context("missing live proxy observed_implementation_contract_instance_id")?;
    let discovery_edge_id: Option<i64> = row
        .try_get("discovery_edge_id")
        .context("missing live proxy discovery_edge_id")?;

    let candidate_reason = if discovery_edge_id.is_some() {
        "implementation_mismatch"
    } else {
        "missing_proxy_implementation_edge"
    };

    Ok(json!({
        "alert_type": ManifestDriftAlertKind::ProxyImplementation.alert_type(),
        "event_kind": ManifestDriftAlertKind::ProxyImplementation.event_kind(),
        "candidate_identity": format!(
            "live_manifest_drift:proxy_implementation:{manifest_id}:{proxy_contract_instance_id}:{}",
            discovery_edge_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "missing".to_owned())
        ),
        "candidate_reason": candidate_reason,
        "namespace": row.try_get::<String, _>("namespace").context("missing live proxy namespace")?,
        "source_family": row.try_get::<String, _>("source_family").context("missing live proxy source_family")?,
        "manifest_version": row.try_get::<i64, _>("manifest_version").context("missing live proxy manifest_version")?,
        "source_manifest_id": manifest_id,
        "chain": row.try_get::<String, _>("chain").context("missing live proxy chain")?,
        "lifecycle": {
            "status": "candidate",
            "active": true,
            "persisted": false,
        },
        "declaration": {
            "name": row.try_get::<String, _>("declaration_name").context("missing live proxy declaration_name")?,
            "role": row.try_get::<Option<String>, _>("role").context("missing live proxy role")?,
            "proxy_kind": row.try_get::<Option<String>, _>("proxy_kind").context("missing live proxy proxy_kind")?,
        },
        "proxy": {
            "contract_instance_id": proxy_contract_instance_id.to_string(),
            "address": row.try_get::<String, _>("proxy_address").context("missing live proxy proxy_address")?,
        },
        "expected_implementation": {
            "contract_instance_id": expected_implementation_contract_instance_id.to_string(),
            "address": row.try_get::<Option<String>, _>("expected_implementation_address").context("missing live proxy expected_implementation_address")?,
        },
        "observed_implementation": {
            "contract_instance_id": observed_implementation_contract_instance_id
                .map(|value| value.to_string()),
            "address": row.try_get::<Option<String>, _>("observed_implementation_address").context("missing live proxy observed_implementation_address")?,
        },
        "implementation_edge": {
            "discovery_edge_id": discovery_edge_id,
            "admission": row.try_get::<Option<String>, _>("admission").context("missing live proxy admission")?,
            "active_from_block_number": row.try_get::<Option<i64>, _>("active_from_block_number").context("missing live proxy active_from_block_number")?,
            "active_to_block_number": row.try_get::<Option<i64>, _>("active_to_block_number").context("missing live proxy active_to_block_number")?,
            "provenance": row.try_get::<Option<Value>, _>("provenance").context("missing live proxy provenance")?.unwrap_or(Value::Null),
        },
        "remediation": Value::Null,
    }))
}

fn decode_count(row: &sqlx::postgres::PgRow, column_name: &str) -> Result<u64> {
    let count = row
        .try_get::<i64, _>(column_name)
        .with_context(|| format!("missing {column_name}"))?;
    u64::try_from(count).with_context(|| format!("{column_name} does not fit in u64"))
}

fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(sqlx::types::time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

fn validate_block_identity(chain_id: &str, block_hash: &str) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    if block_hash.trim().is_empty() {
        bail!("block_hash must not be empty");
    }
    Ok(())
}

fn validate_range(chain_id: &str, start: i64, end: i64) -> Result<()> {
    if chain_id.trim().is_empty() {
        bail!("chain_id must not be empty");
    }
    if start < 0 {
        bail!("canonicality inspection range start {start} is negative");
    }
    if end < start {
        bail!("canonicality inspection range end {end} is before start {start}");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
