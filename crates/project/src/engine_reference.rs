//! Bounded-memory exact output comparison for the opt-in retained-data benchmark.
use anyhow::{Result, ensure};
use sqlx::{Postgres, Transaction};
use std::{
    fs,
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

pub(super) struct Snapshot {
    directory: PathBuf,
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

impl Snapshot {
    pub(super) async fn capture(
        tx: &mut Transaction<'_, Postgres>,
        tables: &[&str],
        evidence_dir: &Path,
    ) -> Result<Self> {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let directory = evidence_dir.join(format!(
            "bigname-project-reference-{}-{nonce}",
            std::process::id()
        ));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&directory)?;
        let snapshot = Self { directory };
        for table in tables {
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut output = BufWriter::new(options.open(snapshot.directory.join(table))?);
            open_cursor(tx, table).await?;
            loop {
                let rows: Vec<String> =
                    sqlx::query_scalar("FETCH FORWARD 128 FROM benchmark_projection_rows")
                        .fetch_all(&mut **tx)
                        .await?;
                if rows.is_empty() {
                    break;
                }
                for row in rows {
                    writeln!(output, "{row}")?;
                }
            }
            output.flush()?;
            sqlx::query("CLOSE benchmark_projection_rows")
                .execute(&mut **tx)
                .await?;
        }
        Ok(snapshot)
    }

    pub(super) fn cleanup(self) -> Result<()> {
        fs::remove_dir_all(&self.directory)?;
        Ok(())
    }

    pub(super) async fn assert_equal(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        tables: &[&str],
    ) -> Result<()> {
        for table in tables {
            let mut expected = BufReader::new(fs::File::open(self.directory.join(table))?);
            let mut position = 0_u64;
            open_cursor(tx, table).await?;
            loop {
                let rows: Vec<String> =
                    sqlx::query_scalar("FETCH FORWARD 128 FROM benchmark_projection_rows")
                        .fetch_all(&mut **tx)
                        .await?;
                if rows.is_empty() {
                    break;
                }
                for actual in rows {
                    position += 1;
                    let mut line = String::new();
                    ensure!(
                        expected.read_line(&mut line)? > 0
                            && line.strip_suffix('\n') == Some(actual.as_str()),
                        "exact pre-change/candidate output differs in {table} at sorted row {position}"
                    );
                }
            }
            let mut trailing = String::new();
            ensure!(
                expected.read_line(&mut trailing)? == 0,
                "candidate omitted rows in {table}"
            );
            sqlx::query("CLOSE benchmark_projection_rows")
                .execute(&mut **tx)
                .await?;
        }
        Ok(())
    }
}

async fn open_cursor(tx: &mut Transaction<'_, Postgres>, table: &str) -> Result<()> {
    // Keep every position/provenance/expiry timestamp. Only operational insertion and
    // recomputation clock fields differ because the two executions happen sequentially.
    sqlx::query(&format!(
        "DECLARE benchmark_projection_rows NO SCROLL CURSOR FOR
        SELECT value FROM (
            SELECT (to_jsonb(row) - 'last_recomputed_at' - 'inserted_at')::text AS value
            FROM {table} row
        ) serialized ORDER BY value COLLATE \"C\""
    ))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[tokio::test]
async fn exact_reference_snapshot_preserves_duplicates_and_semantic_timestamps() -> Result<()> {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    let database = TestDatabase::create(TestDatabaseConfig::new("project_exact_reference")).await?;
    let mut tx = database.pool().begin().await?;
    sqlx::raw_sql("CREATE TEMP TABLE benchmark_projection_fixture(value jsonb, inserted_at text, last_recomputed_at text);
        INSERT INTO benchmark_projection_fixture VALUES
        ('{\"target_timestamp\":\"2026-09-20\",\"text\":\"a\\nb\"}','first','first'),
        ('{\"target_timestamp\":\"2026-09-20\",\"text\":\"a\\nb\"}','second','second');")
        .execute(&mut *tx).await?;
    let tables = &["benchmark_projection_fixture"];
    let snapshot = Snapshot::capture(&mut tx, tables, &std::env::temp_dir()).await?;
    sqlx::query(
        "UPDATE benchmark_projection_fixture SET inserted_at='later', last_recomputed_at='later'",
    )
    .execute(&mut *tx)
    .await?;
    snapshot.assert_equal(&mut tx, tables).await?;
    sqlx::query("UPDATE benchmark_projection_fixture SET value = jsonb_set(value, '{target_timestamp}', '\"2026-09-21\"')")
        .execute(&mut *tx).await?;
    ensure!(
        snapshot.assert_equal(&mut tx, tables).await.is_err(),
        "meaningful target timestamp drift was hidden"
    );
    sqlx::query("CLOSE benchmark_projection_rows")
        .execute(&mut *tx)
        .await?;
    sqlx::query("TRUNCATE benchmark_projection_fixture")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO benchmark_projection_fixture VALUES ('{\"target_timestamp\":\"2026-09-20\",\"text\":\"a\\nb\"}','first','first')")
        .execute(&mut *tx).await?;
    ensure!(
        snapshot.assert_equal(&mut tx, tables).await.is_err(),
        "duplicate omission was hidden"
    );
    let directory = snapshot.directory.clone();
    snapshot.cleanup()?;
    ensure!(!directory.exists(), "reference rows were left on disk");
    tx.rollback().await?;
    database.cleanup().await?;
    Ok(())
}
