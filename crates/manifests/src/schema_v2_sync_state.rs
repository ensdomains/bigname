use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::{hex, keccak256};
use anyhow::{Context, Result, bail};
use serde_json::Value;
use sqlx::{Postgres, Transaction};

use crate::ManifestRepository;

#[derive(Default)]
pub(super) struct AuthoritySnapshot {
    manifests_by_chain: BTreeMap<String, Vec<String>>,
    basenames_execution: Vec<String>,
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
            authority_payload(payload)?,
        );
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
        let payload = serde_json::to_value(&loaded.manifest)
            .with_context(|| format!("failed to serialize {}", loaded.path.display()))?;
        record_authority(
            &mut snapshot,
            &loaded.manifest.chain,
            &loaded.manifest.namespace,
            &loaded.manifest.source_family,
            authority_payload(payload)?,
        );
    }
    sort_snapshot(&mut snapshot);
    Ok(snapshot)
}

pub(super) async fn invalidate_changed_derived_epochs(
    transaction: &mut Transaction<'_, Postgres>,
    previous: &AuthoritySnapshot,
    desired: &AuthoritySnapshot,
) -> Result<()> {
    let chains = previous
        .manifests_by_chain
        .keys()
        .chain(desired.manifests_by_chain.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
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
        if previous_manifests == desired_manifests {
            continue;
        }
        let encoded = serde_json::to_vec(&(chain_id.as_str(), desired_manifests))
            .context("failed to fingerprint manifest authority")?;
        let marker = format!("manifest-authority:{}", hex::encode(keccak256(encoded)));
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
        let marker = format!("manifest-authority:{}", hex::encode(keccak256(encoded)));
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

fn record_authority(
    snapshot: &mut AuthoritySnapshot,
    chain_id: &str,
    namespace: &str,
    source_family: &str,
    payload: String,
) {
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
