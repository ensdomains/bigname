use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::{Connection, PgConnection, Postgres, Transaction};

use crate::error::{ErrorKind, RunnerError};

const MANIFEST_STARTUP_LOCK_NAME: &str = "phase-runner:manifest-startup";

pub async fn sync_loaded_manifests(
    pool: &sqlx::PgPool,
    root: &std::path::Path,
    repository: &bigname_manifests::ManifestRepository,
    profile: &str,
) -> Result<()> {
    let mut startup_lock = ManifestStartupLock::acquire(pool).await?;
    let synchronization: Result<_> = async {
        let mut transaction = startup_lock
            .connection()
            .begin()
            .await
            .context("failed to start fenced schema-v2 manifest sync transaction")?;
        validate_retained_ens_profile(&mut transaction, repository).await?;
        let summary = bigname_manifests::sync_schema_v2_repository_in_transaction(
            &mut transaction,
            repository,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit fenced schema-v2 manifest sync")?;
        Ok(summary)
    }
    .await;
    let release = startup_lock.release().await;
    let summary = match (synchronization, release) {
        (Ok(summary), Ok(())) => summary,
        (Err(error), Ok(())) => return Err(error),
        (Ok(_), Err(error)) => return Err(error.into()),
        (Err(error), Err(release_error)) => {
            return Err(error.context(format!(
                "additionally failed to release the manifest synchronization advisory lock: \
                 {release_error}"
            )));
        }
    };

    for notice in &summary.notices {
        tracing::warn!(notice = %notice, "schema-v2 manifest synchronization notice");
    }
    tracing::info!(
        manifests_root = %root.display(),
        manifest_profile = profile,
        manifest_count = summary.manifest_count,
        declaration_count = summary.declaration_count,
        discovery_rule_count = summary.discovery_rule_count,
        proxy_edge_count = summary.proxy_edge_count,
        "schema-v2 manifests synchronized"
    );
    Ok(())
}

async fn validate_retained_ens_profile(
    transaction: &mut Transaction<'_, Postgres>,
    repository: &bigname_manifests::ManifestRepository,
) -> Result<(), RunnerError> {
    let incoming_ens_chains = repository
        .manifests()
        .iter()
        .filter(|loaded| loaded.manifest.namespace == "ens")
        .map(|loaded| loaded.manifest.chain.as_str())
        .collect::<BTreeSet<_>>();
    let retained_ens_chains = sqlx::query_scalar::<_, String>(
        "SELECT chain_id
         FROM bigname_phase.name_surfaces
         WHERE namespace = 'ens'
         UNION
         SELECT chain_id
         FROM bigname_phase.manifest_versions
         WHERE namespace = 'ens' AND rollout_status = 'active'
         ORDER BY chain_id",
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            "failed to inspect retained ENS name surfaces and active manifests",
            error,
        )
    })?
    .into_iter()
    .collect::<BTreeSet<_>>();

    if retained_ens_profile_is_allowed(&retained_ens_chains, &incoming_ens_chains) {
        return Ok(());
    }

    Err(RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "manifest synchronization refused because retained ENS chains {retained_ens_chains:?} \
             do not match incoming ENS manifest chains \
             {incoming_ens_chains:?}; use a separate database/schema for the other ENS \
             deployment. An ordinary redo or recompute-flags does not authorize changing the \
             ENS chain in place; use the explicitly reviewed full phase-schema replacement \
             procedure when its preconditions apply."
        ),
    ))
}

fn retained_ens_profile_is_allowed(retained: &BTreeSet<String>, incoming: &BTreeSet<&str>) -> bool {
    if retained.is_empty() {
        return incoming.len() <= 1;
    }

    retained.len() == 1
        && incoming.len() == 1
        && retained.iter().next().map(String::as_str) == incoming.iter().next().copied()
}

struct ManifestStartupLock {
    connection: PgConnection,
}

impl ManifestStartupLock {
    async fn acquire(pool: &sqlx::PgPool) -> Result<Self, RunnerError> {
        let options = pool.connect_options();
        let mut connection =
            PgConnection::connect_with(options.as_ref())
                .await
                .map_err(|error| {
                    RunnerError::database(
                        "failed to open the manifest synchronization advisory-lock connection",
                        error,
                    )
                })?;
        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1::text, 0::bigint))")
            .bind(MANIFEST_STARTUP_LOCK_NAME)
            .execute(&mut connection)
            .await
            .map_err(|error| {
                RunnerError::database("failed to acquire the manifest synchronization lock", error)
            })?;
        Ok(Self { connection })
    }

    fn connection(&mut self) -> &mut PgConnection {
        &mut self.connection
    }

    async fn release(mut self) -> Result<(), RunnerError> {
        let released: bool =
            sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))")
                .bind(MANIFEST_STARTUP_LOCK_NAME)
                .fetch_one(&mut self.connection)
                .await
                .map_err(|error| {
                    RunnerError::database(
                        "failed to release the manifest synchronization lock",
                        error,
                    )
                })?;
        if !released {
            return Err(RunnerError::data_integrity(
                "manifest synchronization advisory lock was already released",
            ));
        }
        self.connection.close().await.map_err(|error| {
            RunnerError::database(
                "failed to close the manifest synchronization advisory-lock connection",
                error,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_retained_state_refuses_a_multi_chain_ens_profile() {
        let retained = BTreeSet::new();
        let incoming = BTreeSet::from(["ethereum-mainnet", "ethereum-sepolia"]);

        assert!(!retained_ens_profile_is_allowed(&retained, &incoming));
    }
}
