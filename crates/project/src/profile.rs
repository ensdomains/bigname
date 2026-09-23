//! Opt-in test-only query plans. No production build includes this module.
use crate::{ProjectError, Result};
use sqlx::{Postgres, Transaction};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy)]
pub(crate) enum Stage {
    WrapperNames,
    UnnamedLeaseNames,
    InventoryNames,
    Events,
    LinkedRecords,
    ResourcePermissions,
    NameCurrent,
    Resolver,
    Mirror(&'static str),
}

impl Stage {
    fn label(self) -> Result<&'static str> {
        match self {
            Self::WrapperNames => Ok("wrapper_names_from_registrars"),
            Self::UnnamedLeaseNames => Ok("names_from_unnamed_leases"),
            Self::InventoryNames => Ok("inventory_pointer_names"),
            Self::Events => Ok("project_events"),
            Self::LinkedRecords => Ok("linked_records"),
            Self::ResourcePermissions => Ok("resource_permission_summary"),
            Self::NameCurrent => Ok("name_current"),
            Self::Resolver => Ok("resolver_current"),
            Self::Mirror(name) if MIRROR_TABLES.contains(&name) => Ok(name),
            _ => Err(ProjectError::configuration("unrecognized profile stage")),
        }
    }
}

const MIRROR_TABLES: &[&str] = &[
    "project_mirror_resource_nodes",
    "project_mirror_seeds",
    "project_mirror_queried_names",
    "project_mirror_pointer_candidates",
    "project_mirror_new_pointers",
    "project_mirror_walks",
    "project_mirror_suffixes",
    "project_mirror_surfaces",
    "project_mirror_consulted",
    "project_mirror_wanted",
    "project_mirror_cached_nodes",
    "project_mirror_links",
];

pub(crate) fn mirror_stage(statement: &str) -> Option<Stage> {
    MIRROR_TABLES
        .iter()
        .copied()
        .find(|name| {
            statement.contains(&format!("CREATE TEMP TABLE {name} ON COMMIT DROP AS"))
                || statement.contains(&format!("INSERT INTO {name}\n"))
        })
        .map(Stage::Mirror)
}

tokio::task_local! { static DIRECTORY: PathBuf; }

pub(crate) struct Session {
    directory: PathBuf,
}

impl Session {
    pub(crate) fn create(root: &Path) -> Result<Self> {
        let root = root.canonicalize().map_err(|_| io_error())?;
        if !root.is_dir() {
            return Err(io_error());
        }
        let directory = root.join(format!("project-query-plans-{}", uuid::Uuid::new_v4()));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&directory).map_err(|_| io_error())?;
        Ok(Self { directory })
    }

    // Only the explicitly scoped candidate future sees profiling. Concurrent tests,
    // baseline/reference derivation and candidate advance remain unprofiled.
    pub(crate) async fn scope<F: std::future::Future>(&self, future: F) -> F::Output {
        DIRECTORY.scope(self.directory.clone(), future).await
    }

    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }
}

// Returns true only when this call executed the statement. Callers must ignore
// rows_affected and must not execute it again after true. The plan is private data.
pub(crate) async fn execute(
    tx: &mut Transaction<'_, Postgres>,
    chain: &str,
    target: i64,
    statement: &str,
    stage: Stage,
) -> Result<bool> {
    if statement.contains("$1") {
        execute_bound(
            tx,
            statement,
            stage,
            &[Parameter::Text(chain), Parameter::I64(target)],
        )
        .await
    } else {
        execute_bound(tx, statement, stage, &[]).await
    }
}

pub(crate) enum Parameter<'a> {
    Text(&'a str),
    I64(i64),
    I32(i32),
    Bool(bool),
}

pub(crate) async fn execute_bound(
    tx: &mut Transaction<'_, Postgres>,
    statement: &str,
    stage: Stage,
    parameters: &[Parameter<'_>],
) -> Result<bool> {
    let Some(directory) = DIRECTORY.try_with(Clone::clone).ok() else {
        return Ok(false);
    };
    if crate::reference::enabled(tx).await? {
        return Err(ProjectError::configuration(
            "candidate profiling leaked into reference",
        ));
    }
    let label = stage.label()?;
    let metadata = fs::symlink_metadata(&directory).map_err(|_| io_error())?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(io_error());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o777 != 0o700 {
            return Err(io_error());
        }
    }
    let ordinal = fs::read_dir(&directory).map_err(|_| io_error())?.count() + 1;
    if ordinal > 512 {
        return Err(ProjectError::configuration(
            "candidate profile plan limit exceeded",
        ));
    }
    let explained =
        format!("EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON, TIMING OFF) {statement}");
    let mut query = sqlx::query_scalar::<_, serde_json::Value>(&explained);
    for parameter in parameters {
        query = match parameter {
            Parameter::Text(value) => query.bind(*value),
            Parameter::I64(value) => query.bind(*value),
            Parameter::I32(value) => query.bind(*value),
            Parameter::Bool(value) => query.bind(*value),
        };
    }
    let plan = query
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| ProjectError::database("failed to profile candidate statement", e))?;
    let bytes = serde_json::to_vec(&plan).map_err(|_| io_error())?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(ProjectError::configuration(
            "candidate profile plan size limit exceeded",
        ));
    }
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut output = options
        .open(directory.join(format!(
            "{ordinal:04}-{label}-{}.json",
            uuid::Uuid::new_v4()
        )))
        .map_err(|_| io_error())?;
    output.write_all(&bytes).map_err(|_| io_error())?;
    tracing::debug!(
        stage = label,
        ordinal,
        profile = "PROFILED",
        "Project candidate query plan captured privately"
    );
    Ok(true)
}

fn io_error() -> ProjectError {
    ProjectError::transient("private candidate plan evidence I/O failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    #[tokio::test]
    async fn profiled_execution_is_scoped_single_execution_and_preserves_sql_errors()
    -> anyhow::Result<()> {
        let database = TestDatabase::create(TestDatabaseConfig::new("profile_execution")).await?;
        let mut tx = database.pool().begin().await?;
        sqlx::query("CREATE TEMP TABLE profile_once(id integer PRIMARY KEY)")
            .execute(&mut *tx)
            .await?;
        let session = Session::create(&std::env::temp_dir())?;
        let sql = "INSERT INTO profile_once VALUES(1)";
        assert!(!execute(&mut tx, "bench", 10, sql, Stage::InventoryNames).await?);
        assert!(
            session
                .scope(execute(&mut tx, "bench", 10, sql, Stage::InventoryNames))
                .await?
        );
        assert!(!execute(&mut tx, "bench", 10, sql, Stage::InventoryNames).await?);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM profile_once")
                .fetch_one(&mut *tx)
                .await?,
            1
        );
        sqlx::query("SAVEPOINT expected_error")
            .execute(&mut *tx)
            .await?;
        let error = session
            .scope(execute(&mut tx, "bench", 10, sql, Stage::InventoryNames))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::DataIntegrity);
        sqlx::query("ROLLBACK TO SAVEPOINT expected_error")
            .execute(&mut *tx)
            .await?;
        assert_eq!(fs::read_dir(session.directory())?.count(), 1);
        // A task-local profile does not propagate into an independently spawned task.
        assert!(
            session
                .scope(async { tokio::spawn(async { DIRECTORY.try_with(|_| ()).is_err() }).await })
                .await?
        );
        sqlx::query("SET LOCAL bigname.benchmark_reference='on'")
            .execute(&mut *tx)
            .await?;
        assert_eq!(
            session
                .scope(execute(&mut tx, "bench", 10, sql, Stage::InventoryNames))
                .await
                .unwrap_err()
                .kind(),
            crate::ErrorKind::Configuration
        );
        assert_eq!(fs::read_dir(session.directory())?.count(), 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(session.directory())?.permissions().mode() & 0o777,
                0o700
            );
        }
        fs::remove_dir_all(session.directory())?;
        tx.rollback().await?;
        database.cleanup().await?;
        Ok(())
    }
}
