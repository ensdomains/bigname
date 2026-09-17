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

    pub(crate) fn connection(&mut self) -> &mut PgConnection {
        &mut self.connection
    }

    pub(crate) fn connection_for(
        &mut self,
        chain_id: &str,
        phase: PhaseName,
    ) -> RunnerResult<&mut PgConnection> {
        if self.chain_id != chain_id || self.phase != phase {
            return Err(RunnerError::data_integrity(format!(
                "phase lock for chain {} phase {} cannot fence completion for chain {chain_id} phase {phase}",
                self.chain_id, self.phase
            )));
        }
        Ok(&mut self.connection)
    }

    /// Run `future`, probing the lock's connection every `check_interval`. The
    /// probe is polled alongside the future, never in its place: a probe that
    /// stalls on a dead connection must not keep a future that would otherwise
    /// finish -- or observe a stop -- from doing so. A probe still pending when
    /// the future finishes is dropped with its query; the caller's next probe
    /// or release finds out what became of the connection.
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
                result = &mut future => return result,
                _ = checks.tick() => {}
            }
            let probe = self.check_alive();
            tokio::pin!(probe);
            tokio::select! {
                biased;
                result = &mut future => return result,
                probed = &mut probe => probed?,
            }
            // A slow probe must leave a full interval before the next one.
            checks.reset();
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

pub(crate) fn lock_name(chain_id: &str, phase: PhaseName) -> String {
    format!("phase-runner:{chain_id}:{phase}")
}
