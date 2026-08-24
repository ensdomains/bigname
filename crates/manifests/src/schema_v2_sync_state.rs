use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::{ManifestRepository, SourceManifest, normalize_address};
use alloy_primitives::{hex, keccak256};
use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{Postgres, Transaction};
const REQUIRED_REDO_PREFIX: &str = "required downstream redo: ";
const MANIFEST_WIDENING_REASON: &str = "manifest watch plan widened over an already-ingested range";
type IngestRedoRow = (Option<i64>, bool, Option<i64>, Option<i64>);
#[derive(Default)]
pub(super) struct AuthoritySnapshot {
    manifests_by_chain: BTreeMap<String, Vec<String>>,
    basenames_execution: Vec<String>,
    watch: super::watch::Snapshot,
}
const PHASE_NAMES: &[&str] = &["ingest", "interpret", "project", "verify", "live"];
const BASENAMES_EXECUTION_CHAIN: &str = "ethereum-mainnet";
const BASENAMES_PROJECT_CHAIN: &str = "base-mainnet";

pub(super) async fn lock_phase_writers(
    transaction: &mut Transaction<'_, Postgres>,
    repository: &ManifestRepository,
) -> Result<()> {
    let active_manifests: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT DISTINCT chain_id, namespace, source_family
         FROM manifest_versions
         WHERE rollout_status = 'active'",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load active manifest chains before schema-v2 sync")?;
    let mut chains = active_manifests
        .iter()
        .map(|(chain_id, _, _)| chain_id.clone())
        .collect::<BTreeSet<_>>();
    chains.extend(
        repository
            .manifests()
            .iter()
            .map(|loaded| loaded.manifest.chain.clone()),
    );
    let has_basenames_execution =
        active_manifests
            .iter()
            .any(|(chain_id, namespace, source_family)| {
                chain_id == BASENAMES_EXECUTION_CHAIN
                    && namespace == "basenames"
                    && source_family == "basenames_execution"
            })
            || repository.manifests().iter().any(|loaded| {
                loaded.manifest.rollout_status.is_active()
                    && loaded.manifest.chain == BASENAMES_EXECUTION_CHAIN
                    && loaded.manifest.namespace == "basenames"
                    && loaded.manifest.source_family == "basenames_execution"
            });
    if has_basenames_execution {
        chains.insert(BASENAMES_PROJECT_CHAIN.to_owned());
    }
    for chain_id in chains {
        for phase in PHASE_NAMES {
            let lock_name = format!("phase-runner:{chain_id}:{phase}");
            let acquired: bool = sqlx::query_scalar(
                "SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, 0::bigint))",
            )
            .bind(&lock_name)
            .fetch_one(&mut **transaction)
            .await
            .with_context(|| {
                format!("failed to acquire phase advisory lock for chain {chain_id} phase {phase}")
            })?;
            if !acquired {
                bail!(
                    "phase advisory lock is held for chain {chain_id} phase {phase}; refusing \
                     schema-v2 manifest sync"
                );
            }
        }
    }
    Ok(())
}
pub(super) async fn active_authority(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<AuthoritySnapshot> {
    let rows: Vec<(String, String, String, Value)> = sqlx::query_as(
        "
        SELECT chain_id, namespace, source_family, manifest_payload
        FROM manifest_versions
        WHERE rollout_status = 'active'
        ",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load active schema-v2 manifest authority")?;
    let mut snapshot = AuthoritySnapshot::default();
    for (chain_id, namespace, source_family, payload) in rows {
        record_authority(
            &mut snapshot,
            &chain_id,
            &namespace,
            &source_family,
            payload,
        )?;
    }
    sort_snapshot(&mut snapshot);
    Ok(snapshot)
}
pub(super) fn repository_authority(repository: &ManifestRepository) -> Result<AuthoritySnapshot> {
    let mut snapshot = AuthoritySnapshot::default();
    for loaded in repository
        .manifests()
        .iter()
        .filter(|loaded| loaded.manifest.rollout_status.is_active())
    {
        let payload = super::watch::manifest_payload(&loaded.manifest)
            .with_context(|| format!("failed to compile {}", loaded.path.display()))?;
        record_authority(
            &mut snapshot,
            &loaded.manifest.chain,
            &loaded.manifest.namespace,
            &loaded.manifest.source_family,
            payload,
        )?;
    }
    sort_snapshot(&mut snapshot);
    Ok(snapshot)
}
pub(super) async fn invalidate_changed_derived_epochs(
    transaction: &mut Transaction<'_, Postgres>,
    previous: &AuthoritySnapshot,
    desired: &AuthoritySnapshot,
    previous_admission_floors: &BTreeMap<String, super::watch::AdmissionFloors>,
    repaired_floor_chains: &HashSet<String>,
) -> Result<()> {
    let mut chains = previous
        .manifests_by_chain
        .keys()
        .chain(desired.manifests_by_chain.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    chains.extend(repaired_floor_chains.iter().cloned());
    for chain_id in chains {
        let previous_manifests = previous
            .manifests_by_chain
            .get(&chain_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let desired_manifests = desired
            .manifests_by_chain
            .get(&chain_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if previous_manifests == desired_manifests && !repaired_floor_chains.contains(&chain_id) {
            continue;
        }
        let admission_floors = previous_admission_floors
            .get(&chain_id)
            .cloned()
            .unwrap_or_default();
        if let Some(widening) = super::watch::resolver_deployment_widening(
            &previous.watch,
            &desired.watch,
            &chain_id,
            &admission_floors,
        ) {
            reject_covered_discovery_widening(transaction, &chain_id, widening, false).await?;
        }
        if let Some(widening) = super::watch::discovery_widening_start(
            &previous.watch,
            &desired.watch,
            &chain_id,
            &admission_floors,
        ) {
            reject_covered_discovery_widening(transaction, &chain_id, widening, false).await?;
        }
        if let Some(widening) = super::watch::unsupported_registry_announcement_widening(
            &previous.watch,
            &desired.watch,
            &chain_id,
            &admission_floors,
        ) {
            reject_covered_discovery_widening(transaction, &chain_id, widening, true).await?;
        }
        if let Some(widened_from) = super::watch::registry_announcement_widening_start(
            &previous.watch,
            &desired.watch,
            &chain_id,
            &admission_floors,
        ) {
            let widened_from =
                earliest_retained_registry_announcement(transaction, &chain_id, widened_from)
                    .await?;
            stamp_required_ingest(transaction, &chain_id, widened_from).await?;
        }
        if let Some(widened_from) =
            super::watch::widening_start(&previous.watch, &desired.watch, &chain_id)
        {
            stamp_required_ingest(transaction, &chain_id, widened_from).await?;
        }
        let encoded = serde_json::to_vec(&(chain_id.as_str(), desired_manifests))
            .context("failed to fingerprint manifest authority")?;
        let marker = invalidation_marker(transaction, encoded, &chain_id).await?;
        sqlx::query(
            "
            UPDATE chain_phase_state
            SET input_content_hash = $2,
                updated_at = now()
            WHERE chain_id = $1
              AND phase_name IN ('interpret', 'project')
              AND input_content_hash IS NOT NULL
            ",
        )
        .bind(&chain_id)
        .bind(marker)
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to invalidate derived phase epochs for chain {chain_id}")
        })?;
    }
    if previous.basenames_execution != desired.basenames_execution {
        let encoded = serde_json::to_vec(&(
            BASENAMES_PROJECT_CHAIN,
            "basenames_execution",
            desired.basenames_execution.as_slice(),
        ))
        .context("failed to fingerprint Basenames project manifest authority")?;
        let marker = invalidation_marker(
            transaction,
            encoded,
            "Base project Basenames execution dependency",
        )
        .await?;
        sqlx::query(
            "UPDATE chain_phase_state
             SET input_content_hash = $2,
                 updated_at = now()
             WHERE chain_id = $1
               AND phase_name = 'project'
               AND input_content_hash IS NOT NULL",
        )
        .bind(BASENAMES_PROJECT_CHAIN)
        .bind(marker)
        .execute(&mut **transaction)
        .await
        .context("failed to invalidate Base project epoch for Basenames execution authority")?;
    }
    Ok(())
}
pub(super) async fn active_admission_floors(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<BTreeMap<String, super::watch::AdmissionFloors>> {
    // Retained manifest facts preserve role-specific starts across child replacement and retirement;
    // combine them with persisted address ranges exactly as Interpret does.
    let payloads: Vec<Value> = sqlx::query_scalar(
        "SELECT manifest_payload FROM manifest_versions WHERE rollout_status = 'active' UNION ALL SELECT before_state -> 'manifest_payload' FROM normalized_events WHERE event_kind = 'SourceManifestUpdated' AND derivation_kind = 'manifest_sync' AND before_state ->> 'rollout_status' = 'active' AND before_state ? 'manifest_payload' UNION ALL SELECT after_state -> 'manifest_payload' FROM normalized_events WHERE event_kind = 'SourceManifestUpdated' AND derivation_kind = 'manifest_sync' AND after_state ->> 'rollout_status' = 'active' AND after_state ? 'manifest_payload'",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load retained manifest admission history")?;
    let manifests = payloads
        .into_iter()
        .map(|payload| {
            serde_json::from_value::<SourceManifest>(payload)
                .context("failed to decode retained manifest admission history")
        })
        .collect::<Result<Vec<_>>>()?;
    let address_keys = manifests
        .iter()
        .flat_map(|manifest| {
            manifest
                .roots
                .iter()
                .map(|root| &root.address)
                .chain(manifest.contracts.iter().map(|contract| &contract.address))
                .map(|address| (manifest.chain.clone(), normalize_address(address)))
        })
        .collect::<BTreeSet<_>>();
    let (chains, addresses): (Vec<_>, Vec<_>) = address_keys.into_iter().unzip();
    let rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT target.chain_id, target.address, min(COALESCE(admission.active_from_block_number, 0)) FROM unnest($1::text[], $2::text[]) AS target(chain_id, address) \
         JOIN contract_instance_addresses admission ON admission.chain_id = target.chain_id AND lower(admission.address) = target.address WHERE admission.deactivated_at IS NULL OR admission.active_to_block_number IS NOT NULL \
         GROUP BY target.chain_id, target.address",
    )
    .bind(chains)
    .bind(addresses)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load retained address admission floors")?;
    let address_floors = rows
        .into_iter()
        .map(|(chain_id, address, start)| {
            Ok((
                (chain_id, address),
                u64::try_from(start).context("address floor is negative")?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut by_chain = BTreeMap::<String, super::watch::AdmissionFloors>::new();
    for manifest in manifests {
        for (role, address, declared_start) in manifest
            .roots
            .iter()
            .map(|root| (root.name.as_str(), &root.address, root.start_block))
            .chain(manifest.contracts.iter().map(|contract| {
                (
                    contract.role.as_str(),
                    &contract.address,
                    contract.start_block,
                )
            }))
        {
            let address = normalize_address(address);
            let Some(address_start) =
                address_floors.get(&(manifest.chain.clone(), address.clone()))
            else {
                continue;
            };
            let start = declared_start.map_or(0, |declared| declared.max(*address_start));
            by_chain
                .entry(manifest.chain.clone())
                .or_default()
                .entry((
                    manifest.namespace.clone(),
                    manifest.source_family.clone(),
                    role.to_owned(),
                    address,
                ))
                .and_modify(|floor| *floor = (*floor).min(start))
                .or_insert(start);
        }
    }
    Ok(by_chain)
}
async fn invalidation_marker(
    transaction: &mut Transaction<'_, Postgres>,
    encoded_authority: Vec<u8>,
    context: &str,
) -> Result<String> {
    let generation: String = sqlx::query_scalar("SELECT gen_random_uuid()::text")
        .fetch_one(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to mint manifest-authority invalidation generation for {context}")
        })?;
    Ok(format!(
        "manifest-authority:{}:{generation}",
        hex::encode(keccak256(encoded_authority))
    ))
}
fn record_authority(
    snapshot: &mut AuthoritySnapshot,
    chain_id: &str,
    namespace: &str,
    source_family: &str,
    payload: Value,
) -> Result<()> {
    let manifest: SourceManifest = serde_json::from_value(payload.clone())
        .with_context(|| format!("failed to decode active manifest watch plan for {chain_id}"))?;
    super::watch::record(&mut snapshot.watch, &manifest, &payload)?;
    let payload = authority_payload(payload)?;
    snapshot
        .manifests_by_chain
        .entry(chain_id.to_owned())
        .or_default()
        .push(payload.clone());
    if chain_id == BASENAMES_EXECUTION_CHAIN
        && namespace == "basenames"
        && source_family == "basenames_execution"
    {
        snapshot.basenames_execution.push(payload);
    }
    Ok(())
}
async fn reject_covered_discovery_widening(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    widening: super::watch::DiscoveryWidening,
    registry_announcement: bool,
) -> Result<()> {
    let widened_from = i64::try_from(widening.start)
        .context("manifest discovery start does not fit into BIGINT")?;
    let overlaps: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM chain_phase_state phase
             WHERE phase.chain_id = $1 AND phase.phase_name = 'ingest'
               AND phase.current_block_number IS NOT NULL
               AND EXISTS (
                   SELECT 1 FROM ingest_cursors cursor
                   WHERE cursor.chain_id = $1
                     AND GREATEST(cursor.start_block_number, $2)
                         <= COALESCE(
                             (SELECT latest_block_number FROM chain_heads WHERE chain_id = $1),
                             phase.current_block_number
                         )
               )
         )",
    )
    .bind(chain_id)
    .bind(widened_from)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| format!("failed to inspect discovery coverage for chain {chain_id}"))?;
    if overlaps {
        let subject = match (registry_announcement, widening.kind) {
            (true, _) => "registry announcement ",
            (
                false,
                super::watch::DiscoveryWideningKind::SourceReplacement
                | super::watch::DiscoveryWideningKind::DeploymentEpoch,
            ) => "resolver ",
            _ => "",
        };
        let transition = match widening.kind {
            super::watch::DiscoveryWideningKind::Rule => "discovery rule widening",
            super::watch::DiscoveryWideningKind::SourceReplacement => {
                "discovery source replacement"
            }
            super::watch::DiscoveryWideningKind::DeploymentEpoch => {
                "discovery rule widening from a newly matching deployment epoch"
            }
        };
        bail!(
            "manifest {subject}{transition} overlaps ingested history for chain {chain_id}; this \
             transition is unsupported in place and needs a fresh rebuild or a dedicated \
             discovery backfill mechanism before historical Ingest can be certified"
        );
    }
    Ok(())
}
pub(super) async fn stamp_required_ingest(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    widened_from: u64,
) -> Result<()> {
    let row: Option<IngestRedoRow> = sqlx::query_as(
        "SELECT current_block_number, redo_in_progress,
                redo_from_block_number, redo_to_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'
         FOR UPDATE",
    )
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| format!("failed to inspect Ingest coverage for chain {chain_id}"))?;
    let Some((Some(current), active, active_from, active_to)) = row else {
        return Ok(());
    };
    let bounds: (Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT min(start_block_number),
                (SELECT latest_block_number FROM chain_heads WHERE chain_id = $1)
         FROM ingest_cursors WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| format!("failed to load ingested watch bounds for chain {chain_id}"))?;
    let Some(cursor_start) = bounds.0 else {
        return Ok(());
    };
    let widened_from = i64::try_from(widened_from)
        .context("manifest watch start does not fit into BIGINT")?
        .max(cursor_start);
    // The published head is the readable coverage boundary. After a rewind, the finite Ingest
    // current position may remain ahead on orphaned lineage; taking max(latest, current) here
    // would stamp an unreadable suffix that the required redo cannot discharge.
    let through = bounds.1.unwrap_or(current);
    if widened_from > through {
        return Ok(());
    }
    let reason = format!("{REQUIRED_REDO_PREFIX}{MANIFEST_WIDENING_REASON}");
    if active {
        let (Some(active_from), Some(active_to)) = (active_from, active_to) else {
            bail!("active Ingest redo for chain {chain_id} is missing its persisted range");
        };
        let from = active_from.min(widened_from);
        let to = active_to.max(through);
        let result = sqlx::query(
            "UPDATE chain_phase_state
             SET redo_from_block_number = $2, redo_to_block_number = $3,
                 redo_current_block_number = NULL, redo_current_block_hash = NULL,
                 redo_target_block_number = NULL, redo_target_block_hash = NULL,
                 redo_source_boundary_markers = NULL,
                 redo_manifest_authority_fingerprint = NULL,
                 last_error = $4, updated_at = now()
             WHERE chain_id = $1 AND phase_name = 'ingest' AND redo_in_progress",
        )
        .bind(chain_id)
        .bind(from)
        .bind(to)
        .bind(reason)
        .execute(&mut **transaction)
        .await
        .with_context(|| format!("failed to widen required Ingest redo for chain {chain_id}"))?;
        if result.rows_affected() != 1 {
            bail!("active Ingest redo disappeared while widening chain {chain_id}");
        }
        return Ok(());
    }
    let result = sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'running', redo_in_progress = true, redo_mode = 'redo',
             redo_previous_phase_status = phase_status,
             redo_previous_last_error = last_error,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = $2, redo_to_block_number = $3,
             redo_current_block_number = NULL, redo_current_block_hash = NULL,
             redo_target_block_number = NULL, redo_target_block_hash = NULL,
             redo_source_boundary_markers = NULL,
             redo_manifest_authority_fingerprint = NULL,
             last_error = $4, started_at = now(), finished_at = NULL, updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'ingest'
           AND current_block_number IS NOT NULL AND NOT redo_in_progress",
    )
    .bind(chain_id)
    .bind(widened_from)
    .bind(through)
    .bind(reason)
    .execute(&mut **transaction)
    .await
    .with_context(|| format!("failed to stamp required Ingest redo for chain {chain_id}"))?;
    if result.rows_affected() != 1 {
        bail!("Ingest coverage changed while stamping manifest widening for chain {chain_id}");
    }
    Ok(())
}
async fn earliest_retained_registry_announcement(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    declared_start: u64,
) -> Result<u64> {
    // RegistryCreated is already retained by the all-emitter
    // [watch plan](../../../docs/glossary.md#watch-plan--watched-tuple) from block zero. Mirror the
    // Ingest preload's canonical-lineage test, bounded by the same readable head used for widening.
    let earliest: Option<i64> = sqlx::query_scalar(
        "SELECT min(raw.block_number)
         FROM raw_logs raw
         JOIN chain_lineage lineage
           ON lineage.chain_id = raw.chain_id
          AND lineage.block_hash = raw.block_hash
          AND lineage.block_number = raw.block_number
         JOIN chain_phase_state phase
           ON phase.chain_id = raw.chain_id AND phase.phase_name = 'ingest'
         LEFT JOIN chain_heads head ON head.chain_id = raw.chain_id
         WHERE raw.chain_id = $1
           AND raw.block_number <= COALESCE(
               head.latest_block_number, phase.current_block_number
           )
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lower(raw.topics[1]) = lower($2)",
    )
    .bind(chain_id)
    .bind(crate::registry_announcement_topic0())
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| {
        format!("failed to load retained registry announcements for chain {chain_id}")
    })?;
    let Some(earliest) = earliest else {
        return Ok(declared_start);
    };
    Ok(declared_start
        .min(u64::try_from(earliest).context("retained registry announcement block is negative")?))
}

fn authority_payload(mut payload: Value) -> Result<String> {
    let Value::Object(fields) = &mut payload else {
        bail!("manifest authority payload is not a JSON object");
    };
    fields.remove("normalizer_version");
    serde_json::to_string(&payload).context("failed to encode manifest authority payload")
}

fn sort_snapshot(snapshot: &mut AuthoritySnapshot) {
    for manifests in snapshot.manifests_by_chain.values_mut() {
        manifests.sort();
    }
    snapshot.basenames_execution.sort();
}
