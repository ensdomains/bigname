use std::str::FromStr;

use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::error::{RunnerError, RunnerResult};

pub const INTERPRETER_CONTENT_HASH_SETTING: &str = "bigname.interpreter_content_hash";

#[derive(Clone)]
pub struct RunnerDatabase {
    pool: PgPool,
    connect_options: PgConnectOptions,
}

impl RunnerDatabase {
    pub async fn connect(database_url: &str, maximum_connections: u32) -> RunnerResult<Self> {
        let options = PgConnectOptions::from_str(database_url).map_err(|error| {
            RunnerError::new(
                crate::error::ErrorKind::Configuration,
                format!("failed to parse phase-runner database URL: {error}"),
            )
        })?;
        Self::connect_with_options(options, maximum_connections).await
    }

    pub async fn connect_with_options(
        options: PgConnectOptions,
        maximum_connections: u32,
    ) -> RunnerResult<Self> {
        let connect_options = stamp_interpreter_content_hash(options);
        let pool = PgPoolOptions::new()
            .max_connections(maximum_connections.max(1))
            .connect_with(connect_options.clone())
            .await
            .map_err(|error| {
                RunnerError::transient(format!(
                    "failed to connect phase-runner database pool: {error}"
                ))
            })?;
        Ok(Self {
            pool,
            connect_options,
        })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub(crate) fn connect_options(&self) -> PgConnectOptions {
        self.connect_options.clone()
    }
}

pub fn stamp_interpreter_content_hash(options: PgConnectOptions) -> PgConnectOptions {
    options.options([(
        INTERPRETER_CONTENT_HASH_SETTING,
        bigname_content_hash::INTERPRETER_CONTENT_HASH,
    )])
}
