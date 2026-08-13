use anyhow::{Context, Result};
use sqlx::PgPool;

pub(super) async fn interpret_redo_preflight_failures(pool: &PgPool) -> Result<Vec<String>> {
    let blocked: Vec<(String, String)> = sqlx::query_as(
        "SELECT DISTINCT manifest.namespace, interpret.chain_id
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
           AND interpret.redo_in_progress = true
         ORDER BY manifest.namespace, interpret.chain_id",
    )
    .fetch_all(pool)
    .await
    .context("failed to inspect Interpret redo state before API benchmarking")?;
    Ok(blocked
        .into_iter()
        .map(|(namespace, chain_id)| {
            format!(
                "chain_phase_state.interpret.redo_in_progress=true for active public namespace {namespace:?} on chain {chain_id:?}; complete or roll back the Interpret redo, re-copy the database, and rerun the gate"
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    async fn preflight_database(name: &str, redo_in_progress: bool) -> TestDatabase {
        let database = TestDatabase::create(TestDatabaseConfig::new(name))
            .await
            .unwrap();
        sqlx::raw_sql(&format!(
            "CREATE SCHEMA bigname_phase;
             CREATE TABLE bigname_phase.manifest_versions (
                 namespace text NOT NULL,
                 chain_id text NOT NULL,
                 rollout_status text NOT NULL
             );
             CREATE TABLE bigname_phase.chain_phase_state (
                 chain_id text NOT NULL,
                 phase_name text NOT NULL,
                 redo_in_progress boolean NOT NULL
             );
             INSERT INTO bigname_phase.manifest_versions VALUES
                 ('ens', 'ethereum-mainnet', 'active'),
                 ('basenames', 'base-mainnet', 'active');
             INSERT INTO bigname_phase.chain_phase_state VALUES
                 ('ethereum-mainnet', 'interpret', {redo_in_progress}),
                 ('base-mainnet', 'interpret', false);"
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
             INSERT INTO bigname_phase.manifest_versions VALUES
                 ('basenames', 'ethereum-mainnet', 'active');
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
}
