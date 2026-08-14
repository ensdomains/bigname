use anyhow::{Context, Result};
use sqlx::PgPool;

use super::InterpretRedoSnapshot;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::api_load) struct ApiDatabaseSnapshot {
    pub(super) redo: InterpretRedoSnapshot,
    project_publications: Vec<ProjectPublication>,
    manifest_authority: Vec<ManifestAuthority>,
}

#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
struct ProjectPublication {
    namespace: String,
    chain_id: String,
    latest_block_number: Option<i64>,
    latest_block_hash: Option<String>,
    phase_status: Option<String>,
    current_block_number: Option<i64>,
    current_block_hash: Option<String>,
    input_content_hash: Option<String>,
    row_generation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
struct ManifestAuthority {
    manifest_id: String,
    namespace: String,
    source_family: String,
    chain_id: String,
    manifest_version: String,
    normalizer_version: String,
    loaded_at: String,
    manifest_payload: String,
    latest_event_identity: Option<String>,
    latest_event_after_state: Option<String>,
}

pub(super) async fn load(pool: &PgPool) -> Result<ApiDatabaseSnapshot> {
    let (redo, project_publications, manifest_authority) = tokio::try_join!(
        super::load_interpret_redo_snapshot(pool),
        load_project_publications(pool),
        load_manifest_authority(pool),
    )?;
    Ok(ApiDatabaseSnapshot {
        redo,
        project_publications,
        manifest_authority,
    })
}

impl ApiDatabaseSnapshot {
    pub(in crate::api_load) fn active_failures(&self) -> Vec<String> {
        self.redo.active_failures()
    }

    pub(super) fn boundary_failures(&self, preflight: &Self) -> Vec<String> {
        let mut failures = self.redo.boundary_failures(&preflight.redo);
        if self.project_publications != preflight.project_publications {
            failures.push(
                "Project publication generation changed during the API benchmark; pause indexing, re-copy the database, and rerun the gate"
                    .to_owned(),
            );
        }
        if self.manifest_authority != preflight.manifest_authority {
            failures.push(
                "active public manifest authority changed during the API benchmark; restore one fixed manifest payload and declaration revision set, re-copy the database, and rerun the gate"
                    .to_owned(),
            );
        }
        failures
    }
}

async fn load_project_publications(pool: &PgPool) -> Result<Vec<ProjectPublication>> {
    sqlx::query_as(
        r#"WITH public_chains AS (
               SELECT DISTINCT namespace, chain_id
               FROM bigname_phase.manifest_versions
               WHERE rollout_status = 'active'
                 AND namespace IN ('ens', 'basenames')
           )
           SELECT public.namespace,
                  public.chain_id,
                  head.latest_block_number,
                  head.latest_block_hash,
                  project.phase_status,
                  project.current_block_number,
                  project.current_block_hash,
                  project.input_content_hash,
                  project.xmin::text AS row_generation
           FROM public_chains public
           LEFT JOIN bigname_phase.chain_heads head
             ON head.chain_id = public.chain_id
           LEFT JOIN bigname_phase.chain_phase_state project
             ON project.chain_id = public.chain_id
            AND project.phase_name = 'project'
           ORDER BY public.namespace, public.chain_id"#,
    )
    .fetch_all(pool)
    .await
    .context("failed to snapshot Project publication generations for API benchmarking")
}

async fn load_manifest_authority(pool: &PgPool) -> Result<Vec<ManifestAuthority>> {
    sqlx::query_as(
        r#"SELECT manifest.manifest_id::text,
                  manifest.namespace,
                  manifest.source_family,
                  manifest.chain_id,
                  manifest.manifest_version::text,
                  manifest.normalizer_version,
                  manifest.loaded_at::text,
                  manifest.manifest_payload::text,
                  latest.event_identity AS latest_event_identity,
                  latest.after_state::text AS latest_event_after_state
           FROM bigname_phase.manifest_versions manifest
           LEFT JOIN LATERAL (
               SELECT event.event_identity, event.after_state
               FROM bigname_phase.normalized_events event
               WHERE event.source_manifest_id = manifest.manifest_id
                 AND event.event_kind = 'SourceManifestUpdated'
                 AND event.derivation_kind = 'manifest_sync'
                 AND event.canonicality_state = 'finalized'
               ORDER BY event.normalized_event_id DESC
               LIMIT 1
           ) latest ON TRUE
           WHERE manifest.rollout_status = 'active'
             AND manifest.namespace IN ('ens', 'basenames')
           ORDER BY manifest.namespace, manifest.source_family, manifest.chain_id,
                    manifest.manifest_version, manifest.manifest_id"#,
    )
    .fetch_all(pool)
    .await
    .context("failed to snapshot active public manifest authority for API benchmarking")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> ApiDatabaseSnapshot {
        ApiDatabaseSnapshot {
            redo: InterpretRedoSnapshot { rows: Vec::new() },
            project_publications: vec![ProjectPublication {
                namespace: "ens".to_owned(),
                chain_id: "ethereum-mainnet".to_owned(),
                latest_block_number: Some(10),
                latest_block_hash: Some("0x10".to_owned()),
                phase_status: Some("completed".to_owned()),
                current_block_number: Some(10),
                current_block_hash: Some("0x10".to_owned()),
                input_content_hash: Some(bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned()),
                row_generation: Some("7".to_owned()),
            }],
            manifest_authority: vec![ManifestAuthority {
                manifest_id: "1".to_owned(),
                namespace: "ens".to_owned(),
                source_family: "ens_v1_registry_l1".to_owned(),
                chain_id: "ethereum-mainnet".to_owned(),
                manifest_version: "3".to_owned(),
                normalizer_version: "ensip15@ens-normalize-0.1.1".to_owned(),
                loaded_at: "2026-08-14 00:00:00+00".to_owned(),
                manifest_payload: r#"{"contracts": []}"#.to_owned(),
                latest_event_identity: Some("manifest-event".to_owned()),
                latest_event_after_state: Some(r#"{"rollout_status": "active"}"#.to_owned()),
            }],
        }
    }

    #[test]
    fn project_or_manifest_content_drift_is_red() {
        let before = snapshot();
        let mut project = before.clone();
        project.project_publications[0].row_generation = Some("8".to_owned());
        assert!(
            project
                .boundary_failures(&before)
                .iter()
                .any(|failure| failure.contains("Project publication generation changed"))
        );

        let mut manifest = before.clone();
        manifest.manifest_authority[0].manifest_payload =
            r#"{"contracts": [{"address": "0x01"}]}"#.to_owned();
        assert!(
            manifest
                .boundary_failures(&before)
                .iter()
                .any(|failure| failure.contains("manifest authority changed"))
        );
    }
}
