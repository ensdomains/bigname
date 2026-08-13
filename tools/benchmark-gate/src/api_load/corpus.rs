use anyhow::{Context, Result, ensure};
use sqlx::PgPool;

use crate::budgets::GateBudgets;

const PARENT_CORPUS_SQL: &str = "SELECT DISTINCT surface.raw_name FROM children_current child JOIN name_surfaces surface ON surface.logical_name_id = child.parent_logical_name_id WHERE surface.raw_name <> '' ORDER BY surface.raw_name LIMIT $1";

#[derive(Clone, Debug)]
pub(super) struct Corpus {
    pub(super) names: Vec<String>,
    pub(super) address_names: Vec<(String, String, String, String)>,
    pub(super) parents: Vec<String>,
    pub(super) permission_subjects: Vec<String>,
    pub(super) primary_names: Vec<(String, String, String)>,
    pub(super) resolvers: Vec<(String, String)>,
    pub(super) namespaces: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct TableScale {
    pub(super) name_current_rows: u64,
    pub(super) address_names_current_rows: u64,
}

impl Corpus {
    pub(super) async fn load(pool: &PgPool, budgets: &GateBudgets) -> Result<Self> {
        let limit = i64::try_from(budgets.api_corpus_size)
            .context("API corpus size exceeds PostgreSQL limit")?;
        let names: Vec<String> = sqlx::query_scalar(
            "SELECT raw_name FROM name_current WHERE support_status = 'supported' ORDER BY logical_name_id LIMIT $1",
        )
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("failed to load name benchmark corpus")?;
        let address_names: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT address, min(raw_name), namespace, relation
             FROM address_names_current
             WHERE support_status = 'supported'
             GROUP BY address, namespace, relation
             ORDER BY address, namespace, relation
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("failed to load address benchmark corpus")?;
        let parents: Vec<String> = sqlx::query_scalar(PARENT_CORPUS_SQL)
            .bind(limit)
            .fetch_all(pool)
            .await
            .context("failed to load subname-parent benchmark corpus")?;
        let permission_subjects: Vec<String> = sqlx::query_scalar(
            "SELECT subject
             FROM permissions_current
             WHERE subject ~ '^0x[0-9A-Fa-f]{40}$'
             GROUP BY subject
             ORDER BY subject
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("failed to load permission-subject benchmark corpus")?;
        let primary_names: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT address, coin_type, namespace
             FROM primary_names_current
             WHERE claim_status = 'success'
             ORDER BY address, coin_type, namespace
             LIMIT $1",
        )
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("failed to load primary-name benchmark corpus")?;
        let resolvers: Vec<(String, String)> = sqlx::query_as(
            "SELECT chain_id, resolver_address FROM resolver_current WHERE support_status = 'supported' ORDER BY chain_id, resolver_address LIMIT $1",
        )
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("failed to load resolver benchmark corpus")?;
        let namespaces: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT namespace FROM manifest_versions WHERE rollout_status = 'active' AND namespace IN ('ens', 'basenames') ORDER BY namespace",
        )
        .fetch_all(pool)
        .await
        .context("failed to load namespace benchmark corpus")?;

        ensure!(
            names.len() >= budgets.api_corpus_size,
            "name corpus has {} rows; release profile requires {}",
            names.len(),
            budgets.api_corpus_size
        );
        ensure!(
            address_names.len() >= budgets.api_corpus_size,
            "address corpus has {} rows; release profile requires {}",
            address_names.len(),
            budgets.api_corpus_size
        );
        ensure!(
            !namespaces.is_empty(),
            "benchmark database has no active public namespace"
        );
        for (label, actual) in [
            ("subname parent", parents.len()),
            ("permission subject", permission_subjects.len()),
            ("successful primary name", primary_names.len()),
        ] {
            ensure!(
                actual >= budgets.api_min_specialized_corpus_size,
                "{label} corpus has {actual} rows; release profile requires {}",
                budgets.api_min_specialized_corpus_size
            );
        }

        Ok(Self {
            names,
            address_names,
            parents,
            permission_subjects,
            primary_names,
            resolvers,
            namespaces,
        })
    }
}

pub(super) async fn load_table_scale(pool: &PgPool) -> Result<TableScale> {
    Ok(TableScale {
        name_current_rows: table_count(pool, "name_current").await?,
        address_names_current_rows: table_count(pool, "address_names_current").await?,
    })
}

impl TableScale {
    pub(super) fn failures(self, budgets: &GateBudgets) -> Vec<String> {
        table_scale_failures(
            self.name_current_rows,
            self.address_names_current_rows,
            budgets.api_min_name_current_rows,
            budgets.api_min_address_names_current_rows,
        )
    }
}

fn table_scale_failures(
    name_rows: u64,
    address_rows: u64,
    min_name_rows: u64,
    min_address_rows: u64,
) -> Vec<String> {
    let mut failures = Vec::new();
    if name_rows < min_name_rows {
        failures.push(format!(
            "name_current has {name_rows} rows; release profile requires {min_name_rows}"
        ));
    }
    if address_rows < min_address_rows {
        failures.push(format!(
                "address_names_current has {address_rows} rows; release profile requires {min_address_rows}"
            ));
    }
    failures
}

async fn table_count(pool: &PgPool, table: &str) -> Result<u64> {
    let count: i64 = match table {
        "name_current" => {
            sqlx::query_scalar("SELECT count(*) FROM name_current")
                .fetch_one(pool)
                .await
        }
        "address_names_current" => {
            sqlx::query_scalar("SELECT count(*) FROM address_names_current")
                .fetch_one(pool)
                .await
        }
        _ => unreachable!("benchmark table names are fixed"),
    }
    .with_context(|| format!("failed to count {table} benchmark rows"))?;
    u64::try_from(count).with_context(|| format!("{table} returned a negative row count"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_scale_rejects_staging_sized_tables() {
        assert!(!table_scale_failures(50_000, 75_000, 3_000_000, 3_000_000).is_empty());
        assert!(table_scale_failures(3_000_000, 3_000_000, 3_000_000, 3_000_000).is_empty());
    }

    #[test]
    fn subname_parent_corpus_excludes_the_empty_root() {
        assert!(PARENT_CORPUS_SQL.contains("surface.raw_name <> ''"));
    }
}
