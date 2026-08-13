use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use sqlx::PgPool;

use crate::budgets::GateBudgets;

const ACTIVE_NAMESPACES_SQL: &str = "SELECT DISTINCT namespace FROM manifest_versions WHERE rollout_status = 'active' AND namespace IN ('ens', 'basenames') ORDER BY namespace";
const NAME_CORPUS_SQL: &str = r#"
WITH active_namespaces AS (
    SELECT namespace,
           row_number() OVER (ORDER BY namespace) AS quota_rank,
           count(*) OVER () AS namespace_count
    FROM (SELECT DISTINCT namespace FROM manifest_versions WHERE rollout_status = 'active' AND namespace IN ('ens', 'basenames')) active
), ranked AS (
    SELECT name.namespace, name.raw_name, name.logical_name_id,
           row_number() OVER (PARTITION BY name.namespace ORDER BY name.logical_name_id) AS sample_rank,
           active.quota_rank, active.namespace_count
    FROM active_namespaces active
    JOIN name_current name ON name.namespace = active.namespace
    WHERE name.support_status = 'supported'
)
SELECT namespace, raw_name
FROM ranked
WHERE sample_rank <= ($1 / namespace_count)
    + CASE WHEN quota_rank <= ($1 % namespace_count) THEN 1 ELSE 0 END
ORDER BY namespace, logical_name_id"#;
const PARENT_CORPUS_SQL: &str = r#"
WITH active_namespaces AS (
    SELECT namespace,
           row_number() OVER (ORDER BY namespace) AS quota_rank,
           count(*) OVER () AS namespace_count
    FROM (SELECT DISTINCT namespace FROM manifest_versions WHERE rollout_status = 'active' AND namespace IN ('ens', 'basenames')) active
), candidates AS (
    SELECT DISTINCT surface.namespace, surface.raw_name
    FROM children_current child
    JOIN name_current surface ON surface.logical_name_id = child.parent_logical_name_id
    JOIN active_namespaces active ON active.namespace = surface.namespace
    WHERE surface.support_status = 'supported' AND surface.raw_name <> ''
), ranked AS (
    SELECT candidate.namespace, candidate.raw_name,
           row_number() OVER (PARTITION BY candidate.namespace ORDER BY candidate.raw_name) AS sample_rank,
           active.quota_rank, active.namespace_count
    FROM candidates candidate
    JOIN active_namespaces active ON active.namespace = candidate.namespace
)
SELECT namespace, raw_name
FROM ranked
WHERE sample_rank <= ($1 / namespace_count)
    + CASE WHEN quota_rank <= ($1 % namespace_count) THEN 1 ELSE 0 END
ORDER BY namespace, raw_name"#;

#[derive(Clone, Debug)]
pub(super) struct Corpus {
    pub(super) names: Vec<(String, String)>,
    pub(super) address_names: Vec<(String, String, String, String)>,
    pub(super) parents: Vec<(String, String)>,
    pub(super) permission_subjects: Vec<String>,
    pub(super) primary_names: Vec<(String, String, String)>,
    pub(super) resolvers: Vec<(String, String)>,
    pub(super) namespaces: Vec<String>,
    pub(super) names_by_namespace: BTreeMap<String, usize>,
    pub(super) parents_by_namespace: BTreeMap<String, usize>,
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
        let namespaces: Vec<String> = sqlx::query_scalar(ACTIVE_NAMESPACES_SQL)
            .fetch_all(pool)
            .await
            .context("failed to load namespace benchmark corpus")?;
        let names: Vec<(String, String)> = sqlx::query_as(NAME_CORPUS_SQL)
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
        let parents: Vec<(String, String)> = sqlx::query_as(PARENT_CORPUS_SQL)
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
        let names_by_namespace = namespace_counts(&names);
        let parents_by_namespace = namespace_counts(&parents);

        require_active_namespace_coverage(&namespaces, &names_by_namespace, "supported names")?;
        require_active_namespace_coverage(&namespaces, &parents_by_namespace, "supported parents")?;
        require_name_corpus_size(names.len(), budgets.api_corpus_size, &names_by_namespace)?;
        require_minimum_corpus_size("address", address_names.len(), budgets.api_corpus_size)?;
        for (label, actual) in [
            ("subname parent", parents.len()),
            ("permission subject", permission_subjects.len()),
            ("successful primary name", primary_names.len()),
        ] {
            require_minimum_corpus_size(label, actual, budgets.api_min_specialized_corpus_size)?;
        }
        require_minimum_corpus_size(
            "resolver",
            resolvers.len(),
            budgets.api_min_resolver_corpus_size,
        )?;

        Ok(Self {
            names,
            address_names,
            parents,
            permission_subjects,
            primary_names,
            resolvers,
            namespaces,
            names_by_namespace,
            parents_by_namespace,
        })
    }
}

fn namespace_counts(rows: &[(String, String)]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for (namespace, _) in rows {
        *counts.entry(namespace.clone()).or_insert(0) += 1;
    }
    counts
}

fn require_minimum_corpus_size(label: &str, actual: usize, minimum: usize) -> Result<()> {
    ensure!(
        actual >= minimum,
        "{label} corpus has {actual} rows; release profile requires {minimum}"
    );
    Ok(())
}

fn require_name_corpus_size(
    actual: usize,
    minimum: usize,
    names_by_namespace: &BTreeMap<String, usize>,
) -> Result<()> {
    let contributions = names_by_namespace
        .iter()
        .map(|(namespace, count)| format!("{namespace}={count}"))
        .collect::<Vec<_>>()
        .join(", ");
    ensure!(
        actual >= minimum,
        "name corpus has {actual} rows; release profile requires {minimum}; namespace contributions: {contributions}"
    );
    Ok(())
}

fn require_active_namespace_coverage(
    namespaces: &[String],
    counts_by_namespace: &BTreeMap<String, usize>,
    seed_kind: &str,
) -> Result<()> {
    ensure!(
        !namespaces.is_empty(),
        "benchmark database has no active public namespace"
    );
    for namespace in namespaces {
        ensure!(
            counts_by_namespace
                .get(namespace)
                .copied()
                .unwrap_or_default()
                > 0,
            "active namespace {namespace:?} contributed no {seed_kind} to the benchmark corpus"
        );
    }
    Ok(())
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
            "name_current has {name_rows} supported rows; release profile requires {min_name_rows}"
        ));
    }
    if address_rows < min_address_rows {
        failures.push(format!(
                "address_names_current has {address_rows} supported rows; release profile requires {min_address_rows}"
            ));
    }
    failures
}

async fn table_count(pool: &PgPool, table: &str) -> Result<u64> {
    let count: i64 = match table {
        "name_current" => {
            sqlx::query_scalar(
                "SELECT count(*) FROM name_current WHERE support_status = 'supported'",
            )
            .fetch_one(pool)
            .await
        }
        "address_names_current" => {
            sqlx::query_scalar(
                "SELECT count(*) FROM address_names_current WHERE support_status = 'supported'",
            )
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
    use std::path::Path;

    use super::*;
    use crate::budgets::{BudgetProfile, BudgetsFile};
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    #[test]
    fn production_scale_rejects_staging_sized_tables() {
        assert!(!table_scale_failures(50_000, 75_000, 3_000_000, 3_000_000).is_empty());
        assert!(table_scale_failures(3_000_000, 3_000_000, 3_000_000, 3_000_000).is_empty());
    }

    #[test]
    fn subname_parent_corpus_excludes_the_empty_root() {
        assert!(PARENT_CORPUS_SQL.contains("surface.raw_name <> ''"));
        assert!(PARENT_CORPUS_SQL.contains("surface.support_status = 'supported'"));
    }

    #[test]
    fn resolver_corpus_has_an_independent_floor() {
        assert!(require_minimum_corpus_size("resolver", 999, 1_000).is_err());
        assert!(require_minimum_corpus_size("resolver", 1_000, 1_000).is_ok());
    }

    #[test]
    fn every_active_namespace_must_contribute_supported_names() {
        let namespaces = vec!["basenames".to_owned(), "ens".to_owned()];
        let counts = [("basenames".to_owned(), 5_000)].into_iter().collect();
        let error = require_active_namespace_coverage(&namespaces, &counts, "supported names")
            .unwrap_err()
            .to_string();
        assert!(error.contains("active namespace \"ens\""));
    }

    #[test]
    fn name_corpus_shortfall_names_namespace_contributions() {
        let counts = [("basenames".to_owned(), 5_000), ("ens".to_owned(), 1_000)]
            .into_iter()
            .collect();
        let error = require_name_corpus_size(6_000, 10_000, &counts)
            .unwrap_err()
            .to_string();
        assert!(error.contains("basenames=5000"));
        assert!(error.contains("ens=1000"));
    }

    #[tokio::test]
    async fn unsupported_rows_fail_the_supported_scale_preflight() {
        let database = TestDatabase::create(TestDatabaseConfig::new(
            "benchmark_supported_scale_preflight",
        ))
        .await
        .unwrap();
        sqlx::query("CREATE TABLE name_current (support_status text NOT NULL)")
            .execute(database.pool())
            .await
            .unwrap();
        sqlx::query("CREATE TABLE address_names_current (support_status text NOT NULL)")
            .execute(database.pool())
            .await
            .unwrap();
        for table in ["name_current", "address_names_current"] {
            sqlx::query(&format!(
                "INSERT INTO {table} SELECT 'unsupported' FROM generate_series(1, 8)"
            ))
            .execute(database.pool())
            .await
            .unwrap();
        }

        let scale = load_table_scale(database.pool()).await.unwrap();
        let failures = table_scale_failures(
            scale.name_current_rows,
            scale.address_names_current_rows,
            8,
            8,
        );

        assert_eq!(scale.name_current_rows, 0);
        assert_eq!(scale.address_names_current_rows, 0);
        assert_eq!(
            failures,
            [
                "name_current has 0 supported rows; release profile requires 8",
                "address_names_current has 0 supported rows; release profile requires 8",
            ]
        );
        database.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn name_corpus_is_stratified_across_active_namespaces() {
        let database = TestDatabase::create(TestDatabaseConfig::new(
            "benchmark_namespace_stratification",
        ))
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE name_current (
                 namespace text NOT NULL,
                 raw_name text NOT NULL,
                 logical_name_id text NOT NULL,
                 support_status text NOT NULL
             )",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE manifest_versions (
                 namespace text NOT NULL,
                 rollout_status text NOT NULL
             )",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO manifest_versions VALUES
                 ('basenames', 'active'), ('ens', 'active')",
        )
        .execute(database.pool())
        .await
        .unwrap();
        for index in 0..4 {
            sqlx::query(
                "INSERT INTO name_current VALUES
                     ('basenames', $1, $2, 'supported')",
            )
            .bind(format!("base-{index}.base.eth"))
            .bind(format!("basenames:{index:02}"))
            .execute(database.pool())
            .await
            .unwrap();
        }
        for index in 0..2 {
            sqlx::query(
                "INSERT INTO name_current VALUES
                     ('ens', $1, $2, 'supported')",
            )
            .bind(format!("ens-{index}.eth"))
            .bind(format!("ens:{index:02}"))
            .execute(database.pool())
            .await
            .unwrap();
        }

        let names: Vec<(String, String)> = sqlx::query_as(NAME_CORPUS_SQL)
            .bind(4_i64)
            .fetch_all(database.pool())
            .await
            .unwrap();
        let namespaces = names
            .iter()
            .map(|(namespace, _)| namespace.as_str())
            .collect::<std::collections::BTreeSet<_>>();

        database.cleanup().await.unwrap();
        assert_eq!(names.len(), 4);
        assert_eq!(namespaces, ["basenames", "ens"].into_iter().collect());
    }

    #[tokio::test]
    async fn parent_corpus_and_report_counts_cover_active_namespaces() {
        let database = TestDatabase::create(TestDatabaseConfig::new(
            "benchmark_parent_namespace_stratification",
        ))
        .await
        .unwrap();
        for statement in [
            "CREATE TABLE manifest_versions (namespace text NOT NULL, rollout_status text NOT NULL)",
            "CREATE TABLE name_current (namespace text NOT NULL, raw_name text NOT NULL, logical_name_id text PRIMARY KEY, support_status text NOT NULL)",
            "CREATE TABLE children_current (parent_logical_name_id text NOT NULL)",
        ] {
            sqlx::query(statement)
                .execute(database.pool())
                .await
                .unwrap();
        }
        sqlx::query(
            "INSERT INTO manifest_versions VALUES
                 ('basenames', 'active'), ('ens', 'active')",
        )
        .execute(database.pool())
        .await
        .unwrap();
        for index in 0..4 {
            for namespace in ["basenames", "ens"] {
                let logical_name_id = format!("{namespace}:{index:02}");
                sqlx::query("INSERT INTO name_current VALUES ($1, $2, $3, 'supported')")
                    .bind(namespace)
                    .bind(format!("{namespace}-{index}.eth"))
                    .bind(&logical_name_id)
                    .execute(database.pool())
                    .await
                    .unwrap();
                sqlx::query("INSERT INTO children_current VALUES ($1)")
                    .bind(logical_name_id)
                    .execute(database.pool())
                    .await
                    .unwrap();
            }
        }

        let parents: Vec<(String, String)> = sqlx::query_as(PARENT_CORPUS_SQL)
            .bind(4_i64)
            .fetch_all(database.pool())
            .await
            .unwrap();
        let counts = namespace_counts(&parents);

        database.cleanup().await.unwrap();
        assert_eq!(parents.len(), 4);
        assert_eq!(
            counts,
            [("basenames".to_owned(), 2), ("ens".to_owned(), 2)]
                .into_iter()
                .collect()
        );
    }

    #[tokio::test]
    async fn every_active_namespace_must_contribute_supported_parents() {
        let database = TestDatabase::create(TestDatabaseConfig::new(
            "benchmark_parent_namespace_coverage",
        ))
        .await
        .unwrap();
        for statement in [
            "CREATE TABLE manifest_versions (namespace text NOT NULL, rollout_status text NOT NULL)",
            "CREATE TABLE name_current (namespace text NOT NULL, raw_name text NOT NULL, logical_name_id text PRIMARY KEY, support_status text NOT NULL)",
            "CREATE TABLE children_current (parent_logical_name_id text NOT NULL)",
            "CREATE TABLE address_names_current (address text NOT NULL, raw_name text NOT NULL, namespace text NOT NULL, relation text NOT NULL, support_status text NOT NULL)",
            "CREATE TABLE permissions_current (subject text NOT NULL)",
            "CREATE TABLE primary_names_current (address text NOT NULL, coin_type text NOT NULL, namespace text NOT NULL, claim_status text NOT NULL)",
            "CREATE TABLE resolver_current (chain_id text NOT NULL, resolver_address text NOT NULL, support_status text NOT NULL)",
        ] {
            sqlx::query(statement)
                .execute(database.pool())
                .await
                .unwrap();
        }
        sqlx::query(
            "INSERT INTO manifest_versions VALUES
                 ('basenames', 'active'), ('ens', 'active')",
        )
        .execute(database.pool())
        .await
        .unwrap();
        for index in 0..16 {
            sqlx::query(
                "INSERT INTO name_current VALUES
                     ('basenames', $1, $2, 'supported'),
                     ('ens', $3, $4, 'supported')",
            )
            .bind(format!("base-{index}.base.eth"))
            .bind(format!("basenames:{index:02}"))
            .bind(format!("ens-{index}.eth"))
            .bind(format!("ens:{index:02}"))
            .execute(database.pool())
            .await
            .unwrap();
        }
        sqlx::query("INSERT INTO children_current VALUES ('basenames:00')")
            .execute(database.pool())
            .await
            .unwrap();
        for index in 0..32 {
            sqlx::query(
                "INSERT INTO address_names_current VALUES ($1, $2, 'basenames', 'owner', 'supported')",
            )
            .bind(format!("0x{index:040x}"))
            .bind(format!("base-{index}.base.eth"))
            .execute(database.pool())
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO resolver_current VALUES
                 ('base-mainnet', '0x0000000000000000000000000000000000000001', 'supported')",
        )
        .execute(database.pool())
        .await
        .unwrap();

        let budgets_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml");
        let budgets = BudgetsFile::load(&budgets_path).unwrap();
        let result = Corpus::load(database.pool(), budgets.profile(BudgetProfile::Smoke)).await;
        database.cleanup().await.unwrap();

        let error = result.expect_err("ENS with zero parent seeds must be rejected");
        assert!(error.to_string().contains("active namespace \"ens\""));
        assert!(error.to_string().contains("supported parents"));
    }
}
