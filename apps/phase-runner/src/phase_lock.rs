use std::{future::Future, time::Duration};

use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};
use tokio::time::MissedTickBehavior;

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::PhaseName,
};

pub struct PhaseLock {
    connection: PgConnection,
    chain_id: String,
    phase: PhaseName,
}

impl PhaseLock {
    pub async fn acquire(
        options: PgConnectOptions,
        chain_id: &str,
        phase: PhaseName,
    ) -> RunnerResult<Self> {
        let mut connection = PgConnection::connect_with(&options)
            .await
            .map_err(|error| {
                RunnerError::transient(format!(
                    "failed to open advisory-lock connection for chain {chain_id} phase {phase}: \
                 {error}"
                ))
            })?;
        let lock_name = lock_name(chain_id, phase);
        let acquired: bool = sqlx::query_scalar(
            "SELECT pg_try_advisory_lock(hashtextextended($1::text, 0::bigint))",
        )
        .bind(&lock_name)
        .fetch_one(&mut connection)
        .await
        .map_err(|error| {
            RunnerError::transient(format!(
                "failed to acquire advisory lock for chain {chain_id} phase {phase}: {error}"
            ))
        })?;
        if !acquired {
            return Err(RunnerError::new(
                ErrorKind::LockHeld,
                format!(
                    "phase advisory lock is already held for chain {chain_id} phase {phase}; \
                     refusing a second runner"
                ),
            ));
        }
        Ok(Self {
            connection,
            chain_id: chain_id.to_owned(),
            phase,
        })
    }

    pub async fn check_alive(&mut self) -> RunnerResult<()> {
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&mut self.connection)
            .await
            .map(|_| ())
            .map_err(|error| {
                RunnerError::lock_connection_lost(format!(
                    "advisory-lock connection was lost for chain {} phase {}; stopping this phase \
                     attempt before further writes: {error}",
                    self.chain_id, self.phase
                ))
            })
    }

    pub async fn run_while_alive<T>(
        &mut self,
        check_interval: Duration,
        future: impl Future<Output = RunnerResult<T>>,
    ) -> RunnerResult<T> {
        tokio::pin!(future);
        let mut checks = tokio::time::interval(check_interval);
        checks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        checks.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = checks.tick() => self.check_alive().await?,
                result = &mut future => return result,
            }
        }
    }

    pub async fn release(mut self) -> RunnerResult<()> {
        let lock_name = lock_name(&self.chain_id, self.phase);
        let released: bool =
            sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1::text, 0::bigint))")
                .bind(lock_name)
                .fetch_one(&mut self.connection)
                .await
                .map_err(|error| {
                    RunnerError::transient(format!(
                        "failed to release advisory lock for chain {} phase {}: {error}",
                        self.chain_id, self.phase
                    ))
                })?;
        if !released {
            return Err(RunnerError::data_integrity(format!(
                "advisory lock was already released for chain {} phase {}",
                self.chain_id, self.phase
            )));
        }
        self.connection.close().await.map_err(|error| {
            RunnerError::transient(format!(
                "failed to close advisory-lock connection for chain {} phase {}: {error}",
                self.chain_id, self.phase
            ))
        })
    }
}

fn lock_name(chain_id: &str, phase: PhaseName) -> String {
    format!("phase-runner:{chain_id}:{phase}")
}
