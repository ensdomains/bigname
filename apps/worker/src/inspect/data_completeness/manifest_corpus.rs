use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result};
use bigname_manifests::load_repository;
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Row};

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct ManifestIdentity {
    pub(super) namespace: String,
    pub(super) source_family: String,
    pub(super) chain: String,
    pub(super) deployment_epoch: String,
    pub(super) manifest_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ManifestEvidence {
    identity: ManifestIdentity,
    payload: Value,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct ManifestCorpusInspection {
    pub(super) repository_supplied: bool,
    pub(super) verified: bool,
    pub(super) repository_root: Option<String>,
    pub(super) repository_status: Option<String>,
    pub(super) repository_error: Option<String>,
    pub(super) expected_active_manifest_count: usize,
    pub(super) database_active_manifest_count: usize,
    pub(super) missing_active_manifests: Vec<ManifestIdentity>,
    pub(super) unexpected_active_manifests: Vec<ManifestIdentity>,
    pub(super) mismatched_manifest_payloads: Vec<ManifestIdentity>,
}

impl ManifestCorpusInspection {
    pub(super) fn complete(&self) -> bool {
        !self.repository_supplied
            || (self.verified
                && self.missing_active_manifests.is_empty()
                && self.unexpected_active_manifests.is_empty()
                && self.mismatched_manifest_payloads.is_empty())
    }
}

pub(super) async fn inspect_manifest_corpus(
    pool: &PgPool,
    manifests_root: Option<&Path>,
) -> Result<ManifestCorpusInspection> {
    let database = load_database_active_manifests(pool).await?;
    let Some(root) = manifests_root else {
        return Ok(ManifestCorpusInspection {
            database_active_manifest_count: database.len(),
            ..ManifestCorpusInspection::default()
        });
    };

    let repository = match load_repository(root) {
        Ok(repository) => repository,
        Err(error) => {
            return Ok(ManifestCorpusInspection {
                repository_supplied: true,
                repository_root: Some(root.display().to_string()),
                repository_error: Some(format!("{error:#}")),
                database_active_manifest_count: database.len(),
                ..ManifestCorpusInspection::default()
            });
        }
    };
    let repository_status = repository.summary().status.as_str().to_owned();
    let expected = repository
        .manifests()
        .iter()
        .filter(|loaded| loaded.manifest.rollout_status.is_active())
        .map(|loaded| {
            let manifest = &loaded.manifest;
            Ok(ManifestEvidence {
                identity: ManifestIdentity {
                    namespace: manifest.namespace.clone(),
                    source_family: manifest.source_family.clone(),
                    chain: manifest.chain.clone(),
                    deployment_epoch: manifest.deployment_epoch.clone(),
                    manifest_version: manifest.manifest_version,
                },
                payload: serde_json::to_value(manifest)
                    .context("failed to serialize expected active manifest")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut inspection = compare_manifests(expected, database);
    inspection.repository_supplied = true;
    inspection.verified =
        repository.summary().status == bigname_manifests::ManifestLoadStatus::Loaded;
    inspection.repository_root = Some(repository.root().display().to_string());
    inspection.repository_status = Some(repository_status);
    Ok(inspection)
}

fn compare_manifests(
    expected: Vec<ManifestEvidence>,
    database: Vec<ManifestEvidence>,
) -> ManifestCorpusInspection {
    let expected_count = expected.len();
    let database_count = database.len();
    let expected = expected
        .into_iter()
        .map(|manifest| (manifest.identity.clone(), manifest.payload))
        .collect::<BTreeMap<_, _>>();
    let database = database
        .into_iter()
        .map(|manifest| (manifest.identity.clone(), manifest.payload))
        .collect::<BTreeMap<_, _>>();
    let missing_active_manifests = expected
        .keys()
        .filter(|identity| !database.contains_key(*identity))
        .cloned()
        .collect();
    let unexpected_active_manifests = database
        .keys()
        .filter(|identity| !expected.contains_key(*identity))
        .cloned()
        .collect();
    let mismatched_manifest_payloads = expected
        .iter()
        .filter(|(identity, payload)| {
            database
                .get(*identity)
                .is_some_and(|database_payload| database_payload != *payload)
        })
        .map(|(identity, _)| identity.clone())
        .collect();

    ManifestCorpusInspection {
        expected_active_manifest_count: expected_count,
        database_active_manifest_count: database_count,
        missing_active_manifests,
        unexpected_active_manifests,
        mismatched_manifest_payloads,
        ..ManifestCorpusInspection::default()
    }
}

async fn load_database_active_manifests(pool: &PgPool) -> Result<Vec<ManifestEvidence>> {
    let rows = sqlx::query(
        r#"
        SELECT namespace, source_family, chain, deployment_epoch, manifest_version,
               manifest_payload
        FROM manifest_versions
        WHERE rollout_status = 'active'::manifest_rollout_status
        ORDER BY namespace, source_family, chain, deployment_epoch, manifest_version
        "#,
    )
    .fetch_all(pool)
    .await
    .context("failed to load active database manifests for corpus inspection")?;

    rows.into_iter()
        .map(|row| {
            let manifest_version = row.try_get::<i64, _>("manifest_version")?;
            Ok(ManifestEvidence {
                identity: ManifestIdentity {
                    namespace: row.try_get("namespace")?,
                    source_family: row.try_get("source_family")?,
                    chain: row.try_get("chain")?,
                    deployment_epoch: row.try_get("deployment_epoch")?,
                    manifest_version: u64::try_from(manifest_version)
                        .context("active manifest version must be non-negative")?,
                },
                payload: row.try_get("manifest_payload")?,
            })
        })
        .collect()
}
