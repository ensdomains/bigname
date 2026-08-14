use super::{ApiTargetIdentity, api_identity_failures, load_api_target_identity};
use crate::database;
use anyhow::{Context, Result};
use reqwest::Client;
use sqlx::PgPool;

mod snapshot;
pub(super) use snapshot::ApiDatabaseSnapshot;

pub(super) async fn load_api_database_snapshot(pool: &PgPool) -> Result<ApiDatabaseSnapshot> {
    snapshot::load(pool).await
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct InterpretRedoSnapshot {
    rows: Vec<(String, String, bool, String)>,
}

pub(super) async fn load_interpret_redo_snapshot(pool: &PgPool) -> Result<InterpretRedoSnapshot> {
    let rows = sqlx::query_as(
        "SELECT DISTINCT
             manifest.namespace,
             interpret.chain_id,
             interpret.redo_in_progress,
             interpret.xmin::text
         FROM bigname_phase.manifest_versions manifest
         JOIN bigname_phase.chain_phase_state interpret
           ON interpret.chain_id = manifest.chain_id
          AND interpret.phase_name = 'interpret'
         WHERE manifest.rollout_status = 'active'
           AND (
               manifest.namespace = 'ens'
               OR (
                   manifest.namespace = 'basenames'
                   AND manifest.chain_id = 'base-mainnet'
               )
           )
         ORDER BY manifest.namespace, interpret.chain_id",
    )
    .fetch_all(pool)
    .await
    .context("failed to inspect Interpret redo state before API benchmarking")?;
    Ok(InterpretRedoSnapshot { rows })
}

#[cfg(test)]
async fn interpret_redo_preflight_failures(pool: &PgPool) -> Result<Vec<String>> {
    Ok(load_interpret_redo_snapshot(pool).await?.active_failures())
}

pub(super) struct ApiBoundaryPreflight<'a> {
    target: &'a ApiTargetIdentity,
    database_snapshot: &'a ApiDatabaseSnapshot,
    expected_build_sha: Option<&'a str>,
    database_identity: &'a str,
}

impl<'a> ApiBoundaryPreflight<'a> {
    pub(super) fn new(
        target: &'a ApiTargetIdentity,
        database_snapshot: &'a ApiDatabaseSnapshot,
        expected_build_sha: Option<&'a str>,
        database_identity: &'a str,
    ) -> Self {
        Self {
            target,
            database_snapshot,
            expected_build_sha,
            database_identity,
        }
    }
}

impl InterpretRedoSnapshot {
    pub(super) fn active_failures(&self) -> Vec<String> {
        self.rows
        .iter()
        .filter(|(_, _, redo_in_progress, _)| *redo_in_progress)
        .map(|(namespace, chain_id, _, _)| {
            format!(
                "chain_phase_state.interpret.redo_in_progress=true for active public namespace {namespace:?} on chain {chain_id:?}; complete or roll back the Interpret redo, re-copy the database, and rerun the gate"
            )
        })
        .collect()
    }

    fn boundary_failures(&self, preflight: &Self) -> Vec<String> {
        let mut failures = self.active_failures();
        for (namespace, chain_id, _, _) in &preflight.rows {
            let still_watched =
                self.rows
                    .iter()
                    .any(|(current_namespace, current_chain_id, _, _)| {
                        current_namespace == namespace && current_chain_id == chain_id
                    });
            if !still_watched {
                failures.push(format!(
                    "Interpret redo state for active public namespace {namespace:?} on chain {chain_id:?} is no longer watched after preflight; keep active manifests and phase state fixed, re-copy the database, and rerun the gate"
                ));
            }
        }
        for (namespace, chain_id, redo_in_progress, generation) in &self.rows {
            let Some((_, _, _, before_generation)) =
                preflight
                    .rows
                    .iter()
                    .find(|(before_namespace, before_chain_id, _, _)| {
                        before_namespace == namespace && before_chain_id == chain_id
                    })
            else {
                failures.push(format!(
                    "Interpret redo state for active public namespace {namespace:?} on chain {chain_id:?} is newly watched after preflight; keep active manifests and phase state fixed, re-copy the database, and rerun the gate"
                ));
                continue;
            };
            if *redo_in_progress {
                continue;
            }
            if before_generation != generation {
                failures.push(format!(
                    "chain_phase_state.interpret row generation changed for active public namespace {namespace:?} on chain {chain_id:?}; an Interpret redo may have started or completed during the API benchmark; complete or roll back the Interpret redo, re-copy the database, and rerun the gate"
                ));
            }
        }
        failures
    }
}

pub(super) fn api_postflight_failures(
    preflight: &ApiTargetIdentity,
    postflight: &ApiTargetIdentity,
    preflight_database_identity: &str,
    postflight_database_identity: &str,
) -> Vec<String> {
    let mut failures = Vec::new();
    if preflight != postflight {
        failures.push("target API identity changed during the load benchmark".to_owned());
    }
    if preflight_database_identity != postflight_database_identity {
        failures.push("corpus database identity changed during the load benchmark".to_owned());
    }
    failures
}

pub(super) fn api_boundary_failures(
    endpoint: &str,
    preflight: &ApiTargetIdentity,
    boundary: &ApiTargetIdentity,
    expected_build_sha: Option<&str>,
    preflight_database_identity: &str,
    boundary_database_identity: &str,
) -> Vec<String> {
    let failures = api_identity_failures(boundary, expected_build_sha, boundary_database_identity)
        .into_iter()
        .chain(api_postflight_failures(
            preflight,
            boundary,
            preflight_database_identity,
            boundary_database_identity,
        ));
    failures
        .map(|failure| format!("after {endpoint} endpoint: {failure}"))
        .collect()
}

pub(super) fn classify_api_boundary(
    endpoint: &str,
    preflight: &ApiTargetIdentity,
    expected_build_sha: Option<&str>,
    preflight_database_identity: &str,
    target: Result<ApiTargetIdentity>,
    database: Result<String>,
) -> (Option<ApiTargetIdentity>, Option<String>, Vec<String>) {
    let mut failures = Vec::new();
    match (&target, &database) {
        (Ok(target), Ok(database)) => failures.extend(api_boundary_failures(
            endpoint,
            preflight,
            target,
            expected_build_sha,
            preflight_database_identity,
            database,
        )),
        (Ok(target), Err(error)) => {
            failures.extend(
                api_identity_failures(target, expected_build_sha, preflight_database_identity)
                    .into_iter()
                    .map(|failure| format!("after {endpoint} endpoint: {failure}")),
            );
            if target != preflight {
                failures.push(format!(
                    "after {endpoint} endpoint: target API identity changed during the load benchmark"
                ));
            }
            failures.push(format!(
                "after {endpoint} endpoint: corpus database identity recheck failed: {error:#}"
            ));
        }
        (Err(error), Ok(database)) => {
            failures.push(format!(
                "after {endpoint} endpoint: target API identity recheck failed: {error:#}"
            ));
            if database != preflight_database_identity {
                failures.push(format!(
                    "after {endpoint} endpoint: corpus database identity changed during the load benchmark"
                ));
            }
        }
        (Err(target_error), Err(database_error)) => {
            failures.push(format!(
                "after {endpoint} endpoint: target API identity recheck failed: {target_error:#}"
            ));
            failures.push(format!(
                "after {endpoint} endpoint: corpus database identity recheck failed: {database_error:#}"
            ));
        }
    }
    (target.ok(), database.ok(), failures)
}

pub(super) async fn recheck_api_boundary(
    client: &Client,
    base: &reqwest::Url,
    pool: &PgPool,
    endpoint: &str,
    preflight: &ApiBoundaryPreflight<'_>,
) -> (Option<ApiTargetIdentity>, Option<String>, Vec<String>) {
    let (target, database, database_snapshot) = tokio::join!(
        load_api_target_identity(client, base),
        database::database_instance_identity(pool),
        load_api_database_snapshot(pool),
    );
    let mut result = classify_api_boundary(
        endpoint,
        preflight.target,
        preflight.expected_build_sha,
        preflight.database_identity,
        target,
        database,
    );
    match database_snapshot {
        Ok(snapshot) => result.2.extend(
            snapshot
                .boundary_failures(preflight.database_snapshot)
                .into_iter()
                .map(|failure| format!("after {endpoint} endpoint: {failure}")),
        ),
        Err(error) => result.2.push(format!(
            "after {endpoint} endpoint: database publication/manifest/redo state recheck failed: {error:#}"
        )),
    }
    result
}

#[cfg(test)]
mod tests {
    use super::super::ApiTargetIdentity;
    use super::*;
    use crate::database;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use reqwest::Client;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn boundary_recheck_rejects_redo_watch_membership_changes() {
        let ens = (
            "ens".to_owned(),
            "ethereum-mainnet".to_owned(),
            false,
            "1".to_owned(),
        );
        let basenames = (
            "basenames".to_owned(),
            "base-mainnet".to_owned(),
            false,
            "2".to_owned(),
        );
        let preflight = InterpretRedoSnapshot {
            rows: vec![ens.clone()],
        };
        let removed = InterpretRedoSnapshot { rows: Vec::new() };
        let added = InterpretRedoSnapshot {
            rows: vec![ens, basenames],
        };

        let removed_failures = removed.boundary_failures(&preflight);
        let added_failures = added.boundary_failures(&preflight);

        assert_eq!(removed_failures.len(), 1);
        assert!(removed_failures[0].contains("namespace \"ens\""));
        assert!(removed_failures[0].contains("no longer watched"));
        assert_eq!(added_failures.len(), 1);
        assert!(added_failures[0].contains("namespace \"basenames\""));
        assert!(added_failures[0].contains("newly watched"));
    }

    async fn preflight_database(name: &str, redo_in_progress: bool) -> TestDatabase {
        let database = TestDatabase::create(TestDatabaseConfig::new(name))
            .await
            .unwrap();
        sqlx::raw_sql(&format!(
            "CREATE SCHEMA bigname_phase;
             CREATE TABLE bigname_phase.manifest_versions (
                 manifest_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                 namespace text NOT NULL,
                 chain_id text NOT NULL,
                 rollout_status text NOT NULL,
                 source_family text NOT NULL DEFAULT 'benchmark_registry',
                 manifest_version bigint NOT NULL DEFAULT 1,
                 normalizer_version text NOT NULL DEFAULT 'ensip15@ens-normalize-0.1.1',
                 loaded_at timestamptz NOT NULL DEFAULT now(),
                 manifest_payload jsonb NOT NULL DEFAULT '{{}}'::jsonb
             );
             CREATE TABLE bigname_phase.chain_phase_state (
                 chain_id text NOT NULL,
                 phase_name text NOT NULL,
                 redo_in_progress boolean NOT NULL,
                 phase_status text,
                 current_block_number bigint,
                 current_block_hash text,
                 input_content_hash text
             );
             CREATE TABLE bigname_phase.chain_heads (
                 chain_id text PRIMARY KEY,
                 latest_block_number bigint,
                 latest_block_hash text
             );
             CREATE TABLE bigname_phase.normalized_events (
                 normalized_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                 source_manifest_id bigint,
                 event_kind text NOT NULL,
                 derivation_kind text NOT NULL DEFAULT 'manifest_sync',
                 canonicality_state text NOT NULL DEFAULT 'finalized',
                 event_identity text NOT NULL,
                 after_state jsonb NOT NULL DEFAULT '{{}}'::jsonb
             );
             INSERT INTO bigname_phase.manifest_versions
                 (namespace, chain_id, rollout_status)
             VALUES
                 ('ens', 'ethereum-mainnet', 'active'),
                 ('basenames', 'base-mainnet', 'active');
             INSERT INTO bigname_phase.chain_heads VALUES
                 ('ethereum-mainnet', 16, '0x10'),
                 ('base-mainnet', 16, '0x10');
             INSERT INTO bigname_phase.chain_phase_state
                 (chain_id, phase_name, redo_in_progress, phase_status,
                  current_block_number, current_block_hash, input_content_hash)
             VALUES
                 ('ethereum-mainnet', 'interpret', {redo_in_progress}, 'completed', 16, '0x10', NULL),
                 ('base-mainnet', 'interpret', false, 'completed', 16, '0x10', NULL),
                 ('ethereum-mainnet', 'project', false, 'completed', 16, '0x10', '{content_hash}'),
                 ('base-mainnet', 'project', false, 'completed', 16, '0x10', '{content_hash}');",
            content_hash = bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ))
        .execute(database.pool())
        .await
        .unwrap();
        database
    }

    #[tokio::test]
    async fn api_preflight_refuses_an_active_namespace_interpret_redo() {
        let database = preflight_database("benchmark_api_interpret_redo_preflight", true).await;

        let failures = interpret_redo_preflight_failures(database.pool())
            .await
            .unwrap();

        database.cleanup().await.unwrap();
        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains("interpret.redo_in_progress=true"));
        assert!(failures[0].contains("namespace \"ens\""));
        assert!(failures[0].contains("complete or roll back the Interpret redo"));
        assert!(failures[0].contains("re-copy"));
        assert!(failures[0].contains("rerun the gate"));
    }

    #[tokio::test]
    async fn api_preflight_accepts_completed_interpret_state() {
        let database = preflight_database("benchmark_api_interpret_ready_preflight", false).await;

        let failures = interpret_redo_preflight_failures(database.pool())
            .await
            .unwrap();

        database.cleanup().await.unwrap();
        assert!(failures.is_empty());
    }

    #[tokio::test]
    async fn basenames_redo_check_uses_its_serving_authority_chain() {
        let database = preflight_database("benchmark_api_basenames_redo_scope", false).await;
        sqlx::raw_sql(
            "DELETE FROM bigname_phase.manifest_versions WHERE namespace = 'ens';
             INSERT INTO bigname_phase.manifest_versions
                 (namespace, chain_id, rollout_status)
             VALUES ('basenames', 'ethereum-mainnet', 'active');
             UPDATE bigname_phase.chain_phase_state
             SET redo_in_progress = true
             WHERE chain_id = 'ethereum-mainnet';",
        )
        .execute(database.pool())
        .await
        .unwrap();

        let failures = interpret_redo_preflight_failures(database.pool())
            .await
            .unwrap();

        database.cleanup().await.unwrap();
        assert!(failures.is_empty());
    }

    #[tokio::test]
    async fn basenames_serving_authority_redo_is_refused() {
        let database = preflight_database("benchmark_api_basenames_authority_redo", false).await;
        sqlx::query(
            "UPDATE bigname_phase.chain_phase_state
             SET redo_in_progress = true
             WHERE chain_id = 'base-mainnet'",
        )
        .execute(database.pool())
        .await
        .unwrap();

        let failures = interpret_redo_preflight_failures(database.pool())
            .await
            .unwrap();

        database.cleanup().await.unwrap();
        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains("namespace \"basenames\""));
        assert!(failures[0].contains("chain \"base-mainnet\""));
    }

    #[tokio::test]
    async fn boundary_snapshot_detects_project_and_same_key_manifest_drift() {
        let database =
            preflight_database("benchmark_api_publication_manifest_boundary", false).await;
        let before = load_api_database_snapshot(database.pool()).await.unwrap();
        sqlx::query(
            "UPDATE bigname_phase.chain_phase_state
             SET current_block_number = 17, current_block_hash = '0x11'
             WHERE chain_id = 'ethereum-mainnet' AND phase_name = 'project'",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "UPDATE bigname_phase.manifest_versions
             SET manifest_payload = '{\"contracts\":[{\"address\":\"0x01\"}]}'::jsonb,
                 loaded_at = loaded_at + interval '1 second'
             WHERE namespace = 'ens'",
        )
        .execute(database.pool())
        .await
        .unwrap();
        let after = load_api_database_snapshot(database.pool()).await.unwrap();
        let failures = after.boundary_failures(&before);

        database.cleanup().await.unwrap();
        assert!(
            failures
                .iter()
                .any(|failure| failure.contains("Project publication generation changed"))
        );
        assert!(
            failures
                .iter()
                .any(|failure| failure.contains("manifest authority changed"))
        );
    }

    #[tokio::test]
    async fn boundary_recheck_refuses_a_redo_started_after_preflight() {
        let database = preflight_database("benchmark_api_boundary_redo", false).await;
        let database_snapshot = load_api_database_snapshot(database.pool()).await.unwrap();
        assert!(database_snapshot.active_failures().is_empty());
        let database_identity = database::database_instance_identity(database.pool())
            .await
            .unwrap();
        let identity = ApiTargetIdentity {
            build_sha: "release".to_owned(),
            interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned(),
            database_identity: database_identity.clone(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base =
            reqwest::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let body = serde_json::json!({
            "identity": {
                "build_sha": identity.build_sha,
                "interpreter_content_hash": identity.interpreter_content_hash,
            },
            "database": {"identity": identity.database_identity},
        })
        .to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        sqlx::query(
            "UPDATE bigname_phase.chain_phase_state
             SET redo_in_progress = true
             WHERE chain_id = 'ethereum-mainnet'
               AND phase_name = 'interpret'",
        )
        .execute(database.pool())
        .await
        .unwrap();
        let boundary_preflight = ApiBoundaryPreflight::new(
            &identity,
            &database_snapshot,
            Some("release"),
            &database_identity,
        );

        let (_, _, failures) = recheck_api_boundary(
            &Client::new(),
            &base,
            database.pool(),
            "lookup",
            &boundary_preflight,
        )
        .await;

        server.await.unwrap();
        database.cleanup().await.unwrap();
        assert_eq!(failures.len(), 1);
        assert!(failures[0].starts_with("after lookup endpoint:"));
        assert!(failures[0].contains("interpret.redo_in_progress=true"));
        assert!(failures[0].contains("namespace \"ens\""));
        assert!(failures[0].contains("complete or roll back the Interpret redo"));
    }

    #[tokio::test]
    async fn boundary_recheck_detects_a_redo_completed_inside_the_window() {
        let database = preflight_database("benchmark_api_transient_boundary_redo", false).await;
        let database_snapshot = load_api_database_snapshot(database.pool()).await.unwrap();
        let database_identity = database::database_instance_identity(database.pool())
            .await
            .unwrap();
        let identity = ApiTargetIdentity {
            build_sha: "release".to_owned(),
            interpreter_content_hash: bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned(),
            database_identity: database_identity.clone(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base =
            reqwest::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let body = serde_json::json!({
            "identity": {
                "build_sha": identity.build_sha,
                "interpreter_content_hash": identity.interpreter_content_hash,
            },
            "database": {"identity": identity.database_identity},
        })
        .to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        for redo_in_progress in [true, false] {
            sqlx::query(
                "UPDATE bigname_phase.chain_phase_state
                 SET redo_in_progress = $1
                 WHERE chain_id = 'ethereum-mainnet'
                   AND phase_name = 'interpret'",
            )
            .bind(redo_in_progress)
            .execute(database.pool())
            .await
            .unwrap();
        }
        let boundary_preflight = ApiBoundaryPreflight::new(
            &identity,
            &database_snapshot,
            Some("release"),
            &database_identity,
        );

        let (_, _, failures) = recheck_api_boundary(
            &Client::new(),
            &base,
            database.pool(),
            "lookup",
            &boundary_preflight,
        )
        .await;

        server.await.unwrap();
        database.cleanup().await.unwrap();
        assert_eq!(failures.len(), 1);
        assert!(failures[0].starts_with("after lookup endpoint:"));
        assert!(failures[0].contains("row generation changed"));
        assert!(failures[0].contains("namespace \"ens\""));
        assert!(failures[0].contains("complete or roll back the Interpret redo"));
    }
}
