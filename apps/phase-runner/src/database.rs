use std::str::FromStr;

use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::error::{RunnerError, RunnerResult};

pub const INTERPRETER_CONTENT_HASH_SETTING: &str = "bigname.interpreter_content_hash";
pub const PHASE_SEARCH_PATH: &str = "bigname_phase";

#[derive(Clone)]
pub struct RunnerDatabase {
    pool: PgPool,
    connect_options: PgConnectOptions,
}

#[derive(Clone)]
pub struct VerificationDatabase {
    pool: PgPool,
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

impl VerificationDatabase {
    pub async fn connect(
        database_url: &str,
        writer_database: &RunnerDatabase,
        maximum_connections: u32,
    ) -> RunnerResult<Self> {
        let options = PgConnectOptions::from_str(database_url).map_err(|error| {
            RunnerError::new(
                crate::error::ErrorKind::Configuration,
                format!("failed to parse verification database URL: {error}"),
            )
        })?;
        Self::connect_with_options(options, writer_database, maximum_connections).await
    }

    pub async fn connect_with_options(
        options: PgConnectOptions,
        writer_database: &RunnerDatabase,
        maximum_connections: u32,
    ) -> RunnerResult<Self> {
        let options = stamp_interpreter_content_hash(options);
        let pool = PgPoolOptions::new()
            .max_connections(maximum_connections.max(1))
            .after_connect(|connection, _metadata| {
                Box::pin(async move {
                    sqlx::query("SET default_transaction_read_only = on")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .map_err(|error| {
                RunnerError::transient(format!(
                    "failed to connect verification read-only database pool: {error}"
                ))
            })?;
        validate_verification_role(&pool).await?;
        validate_database_identity(writer_database.pool(), &pool).await?;
        Ok(VerificationDatabase { pool })
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[derive(Debug, Eq, PartialEq)]
struct DatabaseIdentity {
    system_identifier: String,
    database_oid: String,
    database_name: String,
}

async fn validate_database_identity(
    writer_pool: &PgPool,
    verification_pool: &PgPool,
) -> RunnerResult<()> {
    let writer = load_database_identity(writer_pool, "phase-runner writer").await?;
    let verification = load_database_identity(verification_pool, "verification reader").await?;
    if writer == verification {
        return Ok(());
    }
    verification_pool.close().await;
    Err(RunnerError::new(
        crate::error::ErrorKind::Configuration,
        format!(
            "verification database does not match the phase-runner writer database: writer \
             {writer:?}, verification reader {verification:?}"
        ),
    ))
}

async fn load_database_identity(pool: &PgPool, role: &str) -> RunnerResult<DatabaseIdentity> {
    let (database_name, database_oid, system_identifier): (String, String, String) =
        sqlx::query_as(
            "SELECT pg_catalog.current_database()::text,
                    database.oid::text,
                    control.system_identifier::text
             FROM pg_catalog.pg_database database
             CROSS JOIN pg_catalog.pg_control_system() control
             WHERE database.datname = pg_catalog.current_database()",
        )
        .fetch_one(pool)
        .await
        .map_err(|error| {
            RunnerError::new(
                crate::error::ErrorKind::Configuration,
                format!(
                    "failed to read PostgreSQL cluster/database identity through the {role} \
                     connection: {error}"
                ),
            )
        })?;
    Ok(DatabaseIdentity {
        system_identifier,
        database_oid,
        database_name,
    })
}

async fn validate_verification_role(pool: &PgPool) -> RunnerResult<()> {
    let (
        role,
        session_role,
        elevated,
        member_of_other_role,
        database_create,
        schema_create,
        relation_write,
    ): (String, String, bool, bool, bool, bool, bool) = sqlx::query_as(
        "SELECT current_user::text,
                session_user::text,
                role.rolsuper
                    OR role.rolcreaterole
                    OR role.rolcreatedb
                    OR role.rolreplication
                    OR role.rolbypassrls,
                EXISTS (
                    SELECT 1
                    FROM pg_auth_members membership
                    WHERE membership.member = role.oid
                ),
                has_database_privilege(current_user, current_database(), 'CREATE'),
                EXISTS (
                    SELECT 1
                    FROM pg_namespace namespace
                    WHERE namespace.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'
                      AND namespace.nspname <> 'information_schema'
                      AND has_schema_privilege(current_user, namespace.oid, 'CREATE')
                ),
                EXISTS (
                    SELECT 1
                    FROM pg_class relation
                    JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
                    WHERE namespace.nspname NOT LIKE 'pg\\_%' ESCAPE '\\'
                      AND namespace.nspname <> 'information_schema'
                      AND (
                          (
                              relation.relkind IN ('r', 'p', 'v', 'm', 'f')
                              AND (
                                  has_table_privilege(current_user, relation.oid, 'INSERT')
                                  OR has_table_privilege(current_user, relation.oid, 'UPDATE')
                                  OR has_table_privilege(current_user, relation.oid, 'DELETE')
                                  OR has_table_privilege(current_user, relation.oid, 'TRUNCATE')
                                  OR has_table_privilege(current_user, relation.oid, 'REFERENCES')
                                  OR has_table_privilege(current_user, relation.oid, 'TRIGGER')
                                  OR has_any_column_privilege(
                                      current_user,
                                      relation.oid,
                                      'INSERT, UPDATE, REFERENCES'
                                  )
                              )
                          )
                          OR (
                              relation.relkind = 'S'
                              AND (
                                  has_sequence_privilege(current_user, relation.oid, 'USAGE')
                                  OR has_sequence_privilege(current_user, relation.oid, 'UPDATE')
                              )
                          )
                      )
                )
         FROM pg_roles role
         WHERE role.rolname = current_user",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| {
        RunnerError::database("failed to inspect verification database role", error)
    })?;
    if role != session_role
        || elevated
        || member_of_other_role
        || database_create
        || schema_create
        || relation_write
    {
        pool.close().await;
        return Err(RunnerError::new(
            crate::error::ErrorKind::Configuration,
            format!(
                "verification database role {role:?} (session user {session_role:?}) is not a \
                 directly authenticated SELECT-only login; configure a dedicated login role \
                 with no write authority, role assumption, or role memberships"
            ),
        ));
    }

    let phase_schema_is_readable: bool = sqlx::query_scalar(
        "SELECT to_regnamespace('bigname_phase') IS NOT NULL
                AND has_schema_privilege(current_user, 'bigname_phase', 'USAGE')
                AND NOT EXISTS (
                    SELECT 1
                    FROM pg_class relation
                    JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
                    WHERE namespace.nspname = 'bigname_phase'
                      AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
                      AND NOT has_table_privilege(current_user, relation.oid, 'SELECT')
                )",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| {
        RunnerError::database("failed to inspect verification database read access", error)
    })?;
    if !phase_schema_is_readable {
        pool.close().await;
        return Err(RunnerError::new(
            crate::error::ErrorKind::Configuration,
            format!(
                "verification database role {role:?} requires USAGE on bigname_phase and SELECT \
                 on every relation in that schema"
            ),
        ));
    }
    Ok(())
}

pub fn stamp_interpreter_content_hash(options: PgConnectOptions) -> PgConnectOptions {
    options.options([
        (
            INTERPRETER_CONTENT_HASH_SETTING,
            bigname_content_hash::INTERPRETER_CONTENT_HASH,
        ),
        ("search_path", PHASE_SEARCH_PATH),
    ])
}
