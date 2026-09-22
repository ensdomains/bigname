//! Limit JIT suppression to incremental Sepolia derivation, including redo. Full rebuilds and
//! other chains keep the caller's policy. A savepoint also restores the setting on SQL errors,
//! when an explicit SET cannot execute until the failed work has been rolled back.
use sqlx::{Acquire, Postgres, Transaction};

use super::{BatchRequest, RunMode};
use crate::{ProjectError, Result};

pub(super) async fn applies(
    transaction: &mut Transaction<'_, Postgres>,
    request: &BatchRequest,
) -> Result<bool> {
    let incremental = request.chain_id == "ethereum-sepolia"
        && !(request.mode == RunMode::Normal && request.resume_current.is_none());
    if !incremental {
        return Ok(false);
    }
    #[cfg(test)]
    if crate::reference::enabled(transaction).await? {
        return Ok(false);
    }
    #[cfg(not(test))]
    let _ = transaction;
    Ok(true)
}

pub(super) struct Scope<'a> {
    transaction: Transaction<'a, Postgres>,
    previous: String,
}

impl<'a> Scope<'a> {
    pub(super) async fn begin(parent: &'a mut Transaction<'_, Postgres>) -> Result<Self> {
        let mut transaction = parent.begin().await.map_err(|error| {
            ProjectError::database("failed to begin Project JIT savepoint", error)
        })?;
        let previous = sqlx::query_scalar("SHOW jit")
            .fetch_one(&mut *transaction)
            .await
            .map_err(|error| ProjectError::database("failed to read Project JIT policy", error))?;
        sqlx::query("SET LOCAL jit=off")
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to configure Project JIT policy", error)
            })?;
        Ok(Self {
            transaction,
            previous,
        })
    }

    pub(super) fn transaction(&mut self) -> &mut Transaction<'a, Postgres> {
        &mut self.transaction
    }

    pub(super) async fn finish(mut self, result: Result<u64>) -> Result<u64> {
        match result {
            Ok(rows) => {
                // Restore before RELEASE: transaction-local settings otherwise survive the
                // savepoint and would affect later benchmark/reference or caller queries.
                sqlx::query("SELECT set_config('jit',$1,true)")
                    .bind(&self.previous)
                    .execute(&mut *self.transaction)
                    .await
                    .map_err(|error| {
                        ProjectError::database("failed to restore Project JIT policy", error)
                    })?;
                self.transaction.commit().await.map_err(|error| {
                    ProjectError::database("failed to release Project JIT savepoint", error)
                })?;
                Ok(rows)
            }
            Err(original) => {
                // ROLLBACK TO restores the pre-savepoint GUC even after an SQL error. Keep the
                // original classified error/evidence; callers must abandon their outer batch
                // when rollback itself fails (Engine and the benchmark already propagate it).
                if self.transaction.rollback().await.is_err() {
                    tracing::error!(
                        "Project JIT savepoint rollback failed; outer batch must roll back"
                    );
                }
                Err(original)
            }
        }
    }
}

#[cfg(test)]
#[path = "engine_jit_tests.rs"]
mod tests;
