use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use bigname_manifests::{
    ManifestCodeHashObservation, ManifestDeclaredContractDriftInput, ManifestDriftActiveManifest,
    ManifestDriftInputs, ManifestProxyImplementationDriftEdge, load_manifest_drift_inputs,
};
use bigname_storage::{CanonicalityState, NormalizedEvent, upsert_normalized_events};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::Uuid};

const DERIVATION_KIND_MANIFEST_SYNC: &str = "manifest_sync";
const DERIVATION_KIND_MANIFEST_ALERT: &str = "manifest_alert";
const EVENT_KIND_SOURCE_MANIFEST_UPDATED: &str = "SourceManifestUpdated";
const EVENT_KIND_CAPABILITY_CHANGED: &str = "CapabilityChanged";
const EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED: &str = "ProxyImplementationChanged";
const EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT: &str = "ManifestCodeHashDriftAlert";
const EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT: &str = "ManifestProxyImplementationAlert";

/// Sync summary for normalized events derived from stored active manifests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestNormalizedEventSyncSummary {
    pub total_synced_count: usize,
    pub total_inserted_count: usize,
    pub by_kind: BTreeMap<String, ManifestNormalizedEventKindSyncSummary>,
}

/// Per-kind sync summary for logging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestNormalizedEventKindSyncSummary {
    pub synced_count: usize,
    pub inserted_count: usize,
}

#[derive(Clone, Debug)]
struct ActiveCapabilityRow {
    capability_name: String,
    status: String,
    notes: Option<String>,
}

/// Sync manifest-derived normalized events from stored active manifest state.
pub async fn sync_manifest_normalized_events(
    pool: &PgPool,
) -> Result<ManifestNormalizedEventSyncSummary> {
    let drift_inputs = load_manifest_drift_inputs(pool).await?;
    if drift_inputs.active_manifests.is_empty() {
        return Ok(ManifestNormalizedEventSyncSummary {
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let capabilities = load_active_capabilities(pool).await?;
    let contracts = active_proxy_contracts_by_manifest(&drift_inputs);
    let before_counts = load_normalized_event_counts_by_kind(pool).await?;
    let events = build_normalized_events(&drift_inputs, &capabilities, &contracts)?;

    if events.is_empty() {
        return Ok(ManifestNormalizedEventSyncSummary {
            total_synced_count: 0,
            total_inserted_count: 0,
            by_kind: BTreeMap::new(),
        });
    }

    let synced_by_kind = count_events_by_kind(&events);
    upsert_normalized_events(pool, &events).await?;
    let after_counts = load_normalized_event_counts_by_kind(pool).await?;

    let mut by_kind = BTreeMap::new();
    let mut total_inserted_count = 0;
    for (kind, synced_count) in synced_by_kind {
        let inserted_count = after_counts
            .get(&kind)
            .copied()
            .unwrap_or(0)
            .saturating_sub(before_counts.get(&kind).copied().unwrap_or(0));
        total_inserted_count += inserted_count;
        by_kind.insert(
            kind,
            ManifestNormalizedEventKindSyncSummary {
                synced_count,
                inserted_count,
            },
        );
    }

    Ok(ManifestNormalizedEventSyncSummary {
        total_synced_count: events.len(),
        total_inserted_count,
        by_kind,
    })
}

fn build_normalized_events(
    drift_inputs: &ManifestDriftInputs,
    capabilities: &HashMap<i64, Vec<ActiveCapabilityRow>>,
    contracts: &HashMap<i64, Vec<ManifestDeclaredContractDriftInput>>,
) -> Result<Vec<NormalizedEvent>> {
    let mut events = Vec::new();

    for manifest in &drift_inputs.active_manifests {
        events.push(build_source_manifest_updated_event(manifest)?);

        if let Some(capability_rows) = capabilities.get(&manifest.manifest_id) {
            for capability in capability_rows {
                events.push(build_capability_changed_event(manifest, capability)?);
            }
        }

        if let Some(contract_rows) = contracts.get(&manifest.manifest_id) {
            for contract in contract_rows {
                events.push(build_proxy_implementation_changed_event(
                    manifest, contract,
                )?);
            }
        }
    }

    events.extend(build_code_hash_drift_alert_events(drift_inputs)?);
    for edge in &drift_inputs.proxy_implementation_edges {
        events.push(build_proxy_implementation_alert_event(edge)?);
    }

    Ok(events)
}

fn build_source_manifest_updated_event(
    manifest: &ManifestDriftActiveManifest,
) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let deployment_epoch = manifest.deployment_epoch.clone();
    let normalizer_version = manifest.normalizer_version.clone();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:source_manifest_updated",
            json!([
                manifest.manifest_id,
                manifest.manifest_version,
                namespace.clone(),
                source_family.clone(),
                chain.clone(),
                deployment_epoch.clone(),
                normalizer_version.clone(),
            ]),
        )?,
        namespace: namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(),
        source_family: source_family.clone(),
        manifest_version: manifest_version_i64(manifest.manifest_version)?,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain.clone()),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "namespace": namespace.clone(),
            "source_family": source_family.clone(),
            "chain": chain.clone(),
            "deployment_epoch": deployment_epoch.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "manifest_version": manifest.manifest_version,
            "normalizer_version": normalizer_version,
        }),
    })
}

fn build_capability_changed_event(
    manifest: &ManifestDriftActiveManifest,
    capability: &ActiveCapabilityRow,
) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let capability_name = capability.capability_name.clone();
    let status = capability.status.clone();
    let notes = capability.notes.clone();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:capability_changed",
            json!([
                manifest.manifest_id,
                capability_name.clone(),
                status.clone(),
                notes.clone(),
            ]),
        )?,
        namespace,
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_CAPABILITY_CHANGED.to_owned(),
        source_family,
        manifest_version: manifest_version_i64(manifest.manifest_version)?,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "capability_name": capability_name.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "capability_name": capability_name,
            "status": status,
            "notes": notes,
        }),
    })
}

fn build_proxy_implementation_changed_event(
    manifest: &ManifestDriftActiveManifest,
    contract: &ManifestDeclaredContractDriftInput,
) -> Result<NormalizedEvent> {
    let namespace = manifest.namespace.clone();
    let source_family = manifest.source_family.clone();
    let chain = manifest.chain.clone();
    let role = contract
        .role
        .clone()
        .unwrap_or_else(|| contract.declaration_name.clone());
    let address = contract.declared_address.clone();
    let proxy_kind = contract.proxy_kind.clone().unwrap_or_default();
    let implementation = contract
        .declared_implementation_address
        .clone()
        .unwrap_or_default();
    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_sync:proxy_implementation_changed",
            json!([
                manifest.manifest_id,
                role.clone(),
                address.clone(),
                proxy_kind.clone(),
                implementation.clone(),
            ]),
        )?,
        namespace,
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(),
        source_family,
        manifest_version: manifest_version_i64(manifest.manifest_version)?,
        source_manifest_id: Some(manifest.manifest_id),
        chain_id: Some(chain),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": manifest.manifest_id,
            "role": role.clone(),
            "address": address.clone(),
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_SYNC.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "role": role,
            "address": address,
            "proxy_kind": proxy_kind,
            "implementation": implementation,
        }),
    })
}

fn build_code_hash_drift_alert_events(
    drift_inputs: &ManifestDriftInputs,
) -> Result<Vec<NormalizedEvent>> {
    let observations = drift_inputs
        .code_hash_observations
        .iter()
        .map(|observation| {
            (
                code_hash_observation_key(
                    &observation.chain,
                    observation.contract_instance_id,
                    &observation.address,
                ),
                observation,
            )
        })
        .collect::<HashMap<_, _>>();

    let mut events = Vec::new();
    for declared_contract in &drift_inputs.declared_contracts {
        let Some(expected_code_hash) = declared_contract.code_hash.as_ref() else {
            continue;
        };
        let Some(observation) = observations.get(&code_hash_observation_key(
            &declared_contract.chain,
            declared_contract.contract_instance_id,
            &declared_contract.declared_address,
        )) else {
            continue;
        };
        if expected_code_hash.eq_ignore_ascii_case(&observation.code_hash) {
            continue;
        }
        events.push(build_code_hash_drift_alert_event(
            declared_contract,
            observation,
            expected_code_hash,
        )?);
    }

    Ok(events)
}

fn build_code_hash_drift_alert_event(
    declared_contract: &ManifestDeclaredContractDriftInput,
    observation: &ManifestCodeHashObservation,
    expected_code_hash: &str,
) -> Result<NormalizedEvent> {
    let canonicality_state = canonicality_state_from_view(&observation.canonicality_state)?;
    let contract_instance_id = declared_contract.contract_instance_id.to_string();
    let source_manifest_id = declared_contract.manifest_id;
    let namespace = declared_contract.namespace.clone();
    let source_family = declared_contract.source_family.clone();
    let chain = declared_contract.chain.clone();
    let address = declared_contract.declared_address.clone();

    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_alert:code_hash_drift",
            json!([
                source_manifest_id,
                declared_contract.declaration_kind,
                declared_contract.declaration_name,
                contract_instance_id,
                address,
                expected_code_hash,
                observation.code_hash,
                observation.block_hash,
            ]),
        )?,
        namespace,
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_MANIFEST_CODE_HASH_DRIFT_ALERT.to_owned(),
        source_family,
        manifest_version: manifest_version_i64(declared_contract.manifest_version)?,
        source_manifest_id: Some(source_manifest_id),
        chain_id: Some(chain.clone()),
        block_number: Some(observation.block_number),
        block_hash: Some(observation.block_hash.clone()),
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": source_manifest_id,
            "declaration_kind": declared_contract.declaration_kind,
            "declaration_name": declared_contract.declaration_name,
            "contract_instance_id": contract_instance_id,
            "address": address,
            "observed_block_number": observation.block_number,
            "observed_block_hash": observation.block_hash,
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_ALERT.to_owned(),
        canonicality_state,
        before_state: json!({}),
        after_state: json!({
            "alert_type": "manifest_code_hash_drift",
            "alert_status": "active",
            "chain": chain,
            "source_family": declared_contract.source_family,
            "declaration_kind": declared_contract.declaration_kind,
            "declaration_name": declared_contract.declaration_name,
            "contract_instance_id": contract_instance_id,
            "address": declared_contract.declared_address,
            "expected_code_hash": expected_code_hash,
            "observed_code_hash": observation.code_hash,
            "observed_code_byte_length": observation.code_byte_length,
            "observed_block_number": observation.block_number,
            "observed_block_hash": observation.block_hash,
            "observed_canonicality_state": observation.canonicality_state,
            "watched_source": watched_contract_source_name(observation),
            "source_manifest_id": observation.source_manifest_id,
        }),
    })
}

fn build_proxy_implementation_alert_event(
    edge: &ManifestProxyImplementationDriftEdge,
) -> Result<NormalizedEvent> {
    let proxy_contract_instance_id = edge.proxy_contract_instance_id.to_string();
    let implementation_contract_instance_id = edge.implementation_contract_instance_id.to_string();

    Ok(NormalizedEvent {
        event_identity: event_identity(
            "manifest_alert:proxy_implementation",
            json!([
                edge.source_manifest_id,
                edge.discovery_edge_id,
                proxy_contract_instance_id,
                edge.proxy_address,
                implementation_contract_instance_id,
                edge.implementation_address,
            ]),
        )?,
        namespace: edge.namespace.clone(),
        logical_name_id: None,
        resource_id: None,
        event_kind: EVENT_KIND_MANIFEST_PROXY_IMPLEMENTATION_ALERT.to_owned(),
        source_family: edge.source_family.clone(),
        manifest_version: manifest_version_i64(edge.manifest_version)?,
        source_manifest_id: Some(edge.source_manifest_id),
        chain_id: Some(edge.chain.clone()),
        block_number: None,
        block_hash: None,
        transaction_hash: None,
        log_index: None,
        raw_fact_ref: json!({
            "manifest_id": edge.source_manifest_id,
            "discovery_edge_id": edge.discovery_edge_id,
            "proxy_contract_instance_id": proxy_contract_instance_id,
            "implementation_contract_instance_id": implementation_contract_instance_id,
        }),
        derivation_kind: DERIVATION_KIND_MANIFEST_ALERT.to_owned(),
        canonicality_state: CanonicalityState::Finalized,
        before_state: json!({}),
        after_state: json!({
            "alert_type": "manifest_proxy_implementation_edge",
            "alert_status": "active",
            "chain": edge.chain,
            "source_family": edge.source_family,
            "proxy_contract_instance_id": edge.proxy_contract_instance_id.to_string(),
            "proxy_address": edge.proxy_address,
            "implementation_contract_instance_id": edge.implementation_contract_instance_id.to_string(),
            "implementation_address": edge.implementation_address,
            "declaration_name": edge.declaration_name,
            "role": edge.role,
            "proxy_kind": edge.proxy_kind,
            "admission": edge.admission,
            "active_from_block_number": edge.active_from_block_number,
            "active_to_block_number": edge.active_to_block_number,
            "provenance": edge.provenance,
        }),
    })
}

fn event_identity(prefix: &str, key: Value) -> Result<String> {
    Ok(format!(
        "{prefix}:{}",
        serde_json::to_string(&key).context("failed to serialize normalized-event identity")?
    ))
}

fn count_events_by_kind(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.event_kind.clone()).or_insert(0) += 1;
    }
    counts
}

async fn load_active_capabilities(pool: &PgPool) -> Result<HashMap<i64, Vec<ActiveCapabilityRow>>> {
    let rows = sqlx::query(
        r#"
        SELECT
            mv.manifest_id AS manifest_id,
            mcf.capability_name AS capability_name,
            mcf.status::text AS status,
            mcf.notes AS notes
        FROM manifest_versions mv
        JOIN manifest_capability_flags mcf ON mcf.manifest_id = mv.manifest_id
        WHERE mv.rollout_status = 'active'
        ORDER BY mv.namespace, mv.source_family, mv.chain, mv.deployment_epoch, mv.manifest_version, mcf.capability_name
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active capability flags for normalized-event sync")?;

    let mut grouped = HashMap::<i64, Vec<ActiveCapabilityRow>>::new();
    for row in rows {
        let manifest_id = row
            .try_get("manifest_id")
            .context("missing capability manifest_id")?;
        grouped
            .entry(manifest_id)
            .or_default()
            .push(ActiveCapabilityRow {
                capability_name: row
                    .try_get("capability_name")
                    .context("missing capability_name")?,
                status: row.try_get("status").context("missing status")?,
                notes: row.try_get("notes").context("missing notes")?,
            });
    }

    Ok(grouped)
}

fn active_proxy_contracts_by_manifest(
    drift_inputs: &ManifestDriftInputs,
) -> HashMap<i64, Vec<ManifestDeclaredContractDriftInput>> {
    let mut grouped = HashMap::<i64, Vec<ManifestDeclaredContractDriftInput>>::new();
    for contract in &drift_inputs.declared_contracts {
        if contract.declaration_kind == "contract"
            && contract.implementation_contract_instance_id.is_some()
            && contract.declared_implementation_address.is_some()
        {
            grouped
                .entry(contract.manifest_id)
                .or_default()
                .push(contract.clone());
        }
    }
    for rows in grouped.values_mut() {
        rows.sort_by(|left, right| {
            (
                left.role.as_deref().unwrap_or_default(),
                left.declared_address.as_str(),
                left.declared_implementation_address
                    .as_deref()
                    .unwrap_or_default(),
            )
                .cmp(&(
                    right.role.as_deref().unwrap_or_default(),
                    right.declared_address.as_str(),
                    right
                        .declared_implementation_address
                        .as_deref()
                        .unwrap_or_default(),
                ))
        });
    }
    grouped
}

fn code_hash_observation_key(
    chain: &str,
    contract_instance_id: Uuid,
    address: &str,
) -> (String, Uuid, String) {
    (chain.to_owned(), contract_instance_id, address.to_owned())
}

fn watched_contract_source_name(observation: &ManifestCodeHashObservation) -> &'static str {
    match observation.source {
        bigname_manifests::WatchedContractSource::ManifestRoot => "manifest_root",
        bigname_manifests::WatchedContractSource::ManifestContract => "manifest_contract",
        bigname_manifests::WatchedContractSource::DiscoveryEdge => "discovery_edge",
    }
}

fn canonicality_state_from_view(value: &str) -> Result<CanonicalityState> {
    match value {
        "observed" => Ok(CanonicalityState::Observed),
        "canonical" => Ok(CanonicalityState::Canonical),
        "safe" => Ok(CanonicalityState::Safe),
        "finalized" => Ok(CanonicalityState::Finalized),
        "orphaned" => Ok(CanonicalityState::Orphaned),
        _ => anyhow::bail!("failed to parse manifest drift canonicality state {value}"),
    }
}

fn manifest_version_i64(manifest_version: u64) -> Result<i64> {
    i64::try_from(manifest_version).context("manifest_version does not fit in i64")
}

async fn load_normalized_event_counts_by_kind(pool: &PgPool) -> Result<BTreeMap<String, usize>> {
    let rows = sqlx::query(
        r#"
        SELECT event_kind, COUNT(*)::BIGINT AS event_count
        FROM normalized_events
        GROUP BY event_kind
        ORDER BY event_kind
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load normalized-event counts by kind")?;

    let mut counts = BTreeMap::new();
    for row in rows {
        let event_kind = row
            .try_get::<String, _>("event_kind")
            .context("missing event_kind from normalized-event count row")?;
        let event_count = row
            .try_get::<i64, _>("event_count")
            .context("missing event_count from normalized-event count row")?;
        counts.insert(
            event_kind,
            usize::try_from(event_count).context("normalized-event count does not fit in usize")?,
        );
    }

    Ok(counts)
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use anyhow::{Context, Result};
    use bigname_storage::{
        CanonicalityState, default_database_url, load_normalized_event_counts_by_kind,
        load_normalized_events_by_namespace,
    };
    use sqlx::{
        PgPool,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use uuid::Uuid;

    use super::*;

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        admin_pool: PgPool,
        pool: PgPool,
        database_name: String,
    }

    impl TestDatabase {
        async fn new() -> Result<Self> {
            let database_url = std::env::var("BIGNAME_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .unwrap_or_else(|_| default_database_url().to_owned());
            let base_options = PgConnectOptions::from_str(&database_url)
                .context("failed to parse database URL for manifest sync tests")?;
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock is before unix epoch")?
                .as_nanos();
            let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
            let database_name = format!(
                "bigname_adapters_manifest_sync_test_{}_{}_{}",
                std::process::id(),
                unique,
                sequence
            );

            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(base_options.clone().database("postgres"))
                .await
                .context("failed to connect admin pool for manifest sync tests")?;

            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("failed to create test database {database_name}"))?;

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect_with(base_options.database(&database_name))
                .await
                .context("failed to connect test pool for manifest sync tests")?;

            bigname_storage::MIGRATOR
                .run(&pool)
                .await
                .context("failed to apply migrations for manifest sync tests")?;

            Ok(Self {
                admin_pool,
                pool,
                database_name,
            })
        }

        fn pool(&self) -> &PgPool {
            &self.pool
        }

        async fn cleanup(self) -> Result<()> {
            self.pool.close().await;
            sqlx::query(&format!(
                r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
                self.database_name
            ))
            .execute(&self.admin_pool)
            .await
            .with_context(|| format!("failed to drop test database {}", self.database_name))?;
            self.admin_pool.close().await;
            Ok(())
        }
    }

    struct ManifestVersionSeed<'a> {
        manifest_version: i64,
        namespace: &'a str,
        source_family: &'a str,
        chain: &'a str,
        deployment_epoch: &'a str,
        rollout_status: &'a str,
        normalizer_version: &'a str,
        file_path: &'a str,
    }

    async fn insert_manifest_version(pool: &PgPool, seed: ManifestVersionSeed<'_>) -> Result<i64> {
        sqlx::query_scalar(
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
            RETURNING manifest_id
            "#,
        )
        .bind(seed.manifest_version)
        .bind(seed.namespace)
        .bind(seed.source_family)
        .bind(seed.chain)
        .bind(seed.deployment_epoch)
        .bind(seed.rollout_status)
        .bind(seed.normalizer_version)
        .bind(seed.file_path)
        .bind("{}")
        .fetch_one(pool)
        .await
        .context("failed to insert manifest version")
    }

    async fn insert_capability_flag(
        pool: &PgPool,
        manifest_id: i64,
        capability_name: &str,
        status: &str,
        notes: Option<&str>,
    ) -> Result<()> {
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
        .bind(status)
        .bind(notes)
        .execute(pool)
        .await
        .context("failed to insert capability flag")?;
        Ok(())
    }

    async fn insert_contract_instance(
        pool: &PgPool,
        contract_instance_id: Uuid,
        chain_id: &str,
        contract_kind: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO contract_instances (
                contract_instance_id,
                chain_id,
                contract_kind,
                provenance
            )
            VALUES ($1, $2, $3, $4::jsonb)
            "#,
        )
        .bind(contract_instance_id)
        .bind(chain_id)
        .bind(contract_kind)
        .bind("{}")
        .execute(pool)
        .await
        .context("failed to insert contract instance")?;
        Ok(())
    }

    struct ContractSeed<'a> {
        manifest_id: i64,
        contract_instance_id: Uuid,
        declaration_name: &'a str,
        role: &'a str,
        address: &'a str,
        proxy_kind: &'a str,
        implementation_contract_instance_id: Option<Uuid>,
        implementation: Option<&'a str>,
    }

    async fn insert_contract(pool: &PgPool, seed: ContractSeed<'_>) -> Result<()> {
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
            VALUES ($1, 'contract', $2, $3, $4, NULL, NULL, $5, $6, $7, $8)
            "#,
        )
        .bind(seed.manifest_id)
        .bind(seed.declaration_name)
        .bind(seed.contract_instance_id)
        .bind(seed.address)
        .bind(seed.role)
        .bind(seed.proxy_kind)
        .bind(seed.implementation_contract_instance_id)
        .bind(seed.implementation)
        .execute(pool)
        .await
        .context("failed to insert manifest contract instance")?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_manifest_normalized_events_is_idempotent() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let active_manifest_id = insert_manifest_version(
            database.pool(),
            ManifestVersionSeed {
                manifest_version: 1,
                namespace: "ens",
                source_family: "ens_v2_registry_l1",
                chain: "ethereum-mainnet",
                deployment_epoch: "ens_v2",
                rollout_status: "active",
                normalizer_version: "uts46-v1",
                file_path: "manifests/ens/ens_v2_registry_l1/1.toml",
            },
        )
        .await?;
        let inactive_manifest_id = insert_manifest_version(
            database.pool(),
            ManifestVersionSeed {
                manifest_version: 2,
                namespace: "ens",
                source_family: "ens_v2_registry_l1",
                chain: "ethereum-mainnet",
                deployment_epoch: "ens_v2_shadow",
                rollout_status: "draft",
                normalizer_version: "uts46-v1",
                file_path: "manifests/ens/ens_v2_registry_l1/2.toml",
            },
        )
        .await?;

        insert_capability_flag(
            database.pool(),
            active_manifest_id,
            "declared_children",
            "supported",
            Some("live"),
        )
        .await?;
        insert_capability_flag(
            database.pool(),
            active_manifest_id,
            "verified_resolution",
            "shadow",
            None,
        )
        .await?;
        insert_capability_flag(
            database.pool(),
            inactive_manifest_id,
            "declared_children",
            "unsupported",
            Some("ignored"),
        )
        .await?;

        let active_contract_instance_id = Uuid::new_v4();
        let active_implementation_contract_instance_id = Uuid::new_v4();
        let inactive_contract_instance_id = Uuid::new_v4();
        let inactive_implementation_contract_instance_id = Uuid::new_v4();
        insert_contract_instance(
            database.pool(),
            active_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            active_implementation_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            inactive_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            inactive_implementation_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;

        insert_contract(
            database.pool(),
            ContractSeed {
                manifest_id: active_manifest_id,
                contract_instance_id: active_contract_instance_id,
                declaration_name: "registry",
                role: "registry",
                address: "0x00000000000000000000000000000000000000aa",
                proxy_kind: "erc1967",
                implementation_contract_instance_id: Some(
                    active_implementation_contract_instance_id,
                ),
                implementation: Some("0x00000000000000000000000000000000000000dd"),
            },
        )
        .await?;
        insert_contract(
            database.pool(),
            ContractSeed {
                manifest_id: inactive_manifest_id,
                contract_instance_id: inactive_contract_instance_id,
                declaration_name: "registry",
                role: "registry",
                address: "0x00000000000000000000000000000000000000bb",
                proxy_kind: "erc1967",
                implementation_contract_instance_id: Some(
                    inactive_implementation_contract_instance_id,
                ),
                implementation: Some("0x00000000000000000000000000000000000000ee"),
            },
        )
        .await?;

        let first_summary = sync_manifest_normalized_events(database.pool()).await?;
        assert_eq!(first_summary.total_synced_count, 4);
        assert_eq!(first_summary.total_inserted_count, 4);
        assert_eq!(
            first_summary.by_kind,
            BTreeMap::from([
                (
                    EVENT_KIND_CAPABILITY_CHANGED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 2,
                        inserted_count: 2,
                    },
                ),
                (
                    EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 1,
                        inserted_count: 1,
                    },
                ),
                (
                    EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 1,
                        inserted_count: 1,
                    },
                ),
            ])
        );

        let loaded = load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(loaded.len(), 4);
        assert!(loaded.iter().all(|event| {
            event.canonicality_state == CanonicalityState::Finalized
                && event.derivation_kind == DERIVATION_KIND_MANIFEST_SYNC
                && event.source_manifest_id == Some(active_manifest_id)
        }));
        assert_eq!(
            loaded
                .iter()
                .map(|event| event.event_kind.as_str())
                .collect::<Vec<_>>(),
            vec![
                EVENT_KIND_SOURCE_MANIFEST_UPDATED,
                EVENT_KIND_CAPABILITY_CHANGED,
                EVENT_KIND_CAPABILITY_CHANGED,
                EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED,
            ]
        );

        let counts = load_normalized_event_counts_by_kind(database.pool(), "ens").await?;
        assert_eq!(
            counts,
            BTreeMap::from([
                (EVENT_KIND_CAPABILITY_CHANGED.to_owned(), 2_usize),
                (EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(), 1_usize),
            ])
        );

        let second_summary = sync_manifest_normalized_events(database.pool()).await?;
        assert_eq!(second_summary.total_synced_count, 4);
        assert_eq!(second_summary.total_inserted_count, 0);
        assert_eq!(
            second_summary.by_kind,
            BTreeMap::from([
                (
                    EVENT_KIND_CAPABILITY_CHANGED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 2,
                        inserted_count: 0,
                    },
                ),
                (
                    EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 1,
                        inserted_count: 0,
                    },
                ),
                (
                    EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(),
                    ManifestNormalizedEventKindSyncSummary {
                        synced_count: 1,
                        inserted_count: 0,
                    },
                ),
            ])
        );

        let loaded_after_rerun =
            load_normalized_events_by_namespace(database.pool(), "ens").await?;
        assert_eq!(loaded_after_rerun, loaded);

        database.cleanup().await
    }

    #[tokio::test]
    async fn sync_manifest_normalized_events_skips_inactive_manifests() -> Result<()> {
        let _permit = crate::acquire_test_db_permit().await;
        let database = TestDatabase::new().await?;

        let active_manifest_id = insert_manifest_version(
            database.pool(),
            ManifestVersionSeed {
                manifest_version: 1,
                namespace: "ens",
                source_family: "ens_v2_registry_l1",
                chain: "ethereum-mainnet",
                deployment_epoch: "ens_v2",
                rollout_status: "active",
                normalizer_version: "uts46-v1",
                file_path: "manifests/ens/ens_v2_registry_l1/1.toml",
            },
        )
        .await?;
        let inactive_manifest_id = insert_manifest_version(
            database.pool(),
            ManifestVersionSeed {
                manifest_version: 2,
                namespace: "ens",
                source_family: "ens_v2_registry_l1",
                chain: "ethereum-mainnet",
                deployment_epoch: "ens_v2_shadow",
                rollout_status: "deprecated",
                normalizer_version: "uts46-v1",
                file_path: "manifests/ens/ens_v2_registry_l1/2.toml",
            },
        )
        .await?;

        insert_capability_flag(
            database.pool(),
            active_manifest_id,
            "declared_children",
            "supported",
            None,
        )
        .await?;
        insert_capability_flag(
            database.pool(),
            inactive_manifest_id,
            "declared_children",
            "unsupported",
            None,
        )
        .await?;

        let active_contract_instance_id = Uuid::new_v4();
        let active_implementation_contract_instance_id = Uuid::new_v4();
        let inactive_contract_instance_id = Uuid::new_v4();
        let inactive_implementation_contract_instance_id = Uuid::new_v4();
        insert_contract_instance(
            database.pool(),
            active_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            active_implementation_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            inactive_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;
        insert_contract_instance(
            database.pool(),
            inactive_implementation_contract_instance_id,
            "ethereum-mainnet",
            "contract",
        )
        .await?;

        insert_contract(
            database.pool(),
            ContractSeed {
                manifest_id: active_manifest_id,
                contract_instance_id: active_contract_instance_id,
                declaration_name: "registry",
                role: "registry",
                address: "0x00000000000000000000000000000000000000aa",
                proxy_kind: "erc1967",
                implementation_contract_instance_id: Some(
                    active_implementation_contract_instance_id,
                ),
                implementation: Some("0x00000000000000000000000000000000000000dd"),
            },
        )
        .await?;
        insert_contract(
            database.pool(),
            ContractSeed {
                manifest_id: inactive_manifest_id,
                contract_instance_id: inactive_contract_instance_id,
                declaration_name: "registry",
                role: "registry",
                address: "0x00000000000000000000000000000000000000bb",
                proxy_kind: "erc1967",
                implementation_contract_instance_id: Some(
                    inactive_implementation_contract_instance_id,
                ),
                implementation: Some("0x00000000000000000000000000000000000000ee"),
            },
        )
        .await?;

        let summary = sync_manifest_normalized_events(database.pool()).await?;
        assert_eq!(summary.total_synced_count, 3);
        assert_eq!(summary.total_inserted_count, 3);
        assert_eq!(
            load_normalized_events_by_namespace(database.pool(), "ens")
                .await?
                .len(),
            3
        );
        assert_eq!(
            load_normalized_event_counts_by_kind(database.pool(), "ens").await?,
            BTreeMap::from([
                (EVENT_KIND_CAPABILITY_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_PROXY_IMPLEMENTATION_CHANGED.to_owned(), 1_usize),
                (EVENT_KIND_SOURCE_MANIFEST_UPDATED.to_owned(), 1_usize),
            ])
        );

        database.cleanup().await
    }
}
