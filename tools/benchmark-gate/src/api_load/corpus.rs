use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use bigname_storage::{
    DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS, DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER,
    DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS, DEFAULT_CHILDREN_CURRENT_READ_FILTER,
    DEFAULT_NAME_CURRENT_LINEAGE_JOINS, DEFAULT_NAME_CURRENT_READ_FILTER,
    DEFAULT_PERMISSIONS_CURRENT_READ_FILTER, DEFAULT_PRIMARY_NAME_CURRENT_READ_FILTER,
    DEFAULT_RESOLVER_CURRENT_READ_FILTER,
};
use sqlx::PgPool;

use crate::budgets::GateBudgets;

const ACTIVE_NAMESPACES_SQL: &str = "SELECT DISTINCT namespace FROM manifest_versions WHERE rollout_status = 'active' AND namespace IN ('ens', 'basenames') ORDER BY namespace";
fn name_corpus_sql() -> String {
    format!(
        r#"
WITH active_namespaces AS (
    SELECT namespace,
           row_number() OVER (ORDER BY namespace) AS quota_rank,
           count(*) OVER () AS namespace_count
    FROM (SELECT DISTINCT namespace FROM manifest_versions WHERE rollout_status = 'active' AND namespace IN ('ens', 'basenames')) active
), ranked AS (
    SELECT nc.namespace, nc.raw_name, nc.logical_name_id,
           row_number() OVER (PARTITION BY nc.namespace ORDER BY nc.logical_name_id) AS sample_rank,
           active.quota_rank, active.namespace_count
    FROM active_namespaces active
    JOIN bigname_phase.name_current nc ON nc.namespace = active.namespace
    JOIN bigname_phase.name_surfaces surface
      ON surface.logical_name_id = nc.logical_name_id
    LEFT JOIN bigname_phase.resources resource
      ON resource.resource_id = nc.resource_id
    LEFT JOIN bigname_phase.surface_bindings binding
      ON binding.surface_binding_id = nc.surface_binding_id
    LEFT JOIN bigname_phase.token_lineages token_lineage
      ON token_lineage.token_lineage_id = nc.token_lineage_id
    {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
    WHERE nc.support_status = 'supported'
      {DEFAULT_NAME_CURRENT_READ_FILTER}
)
SELECT namespace, raw_name
FROM ranked
WHERE sample_rank <= ($1 / namespace_count)
    + CASE WHEN quota_rank <= ($1 % namespace_count) THEN 1 ELSE 0 END
ORDER BY namespace, logical_name_id"#
    )
}
fn parent_corpus_sql() -> String {
    format!(
        r#"
WITH active_namespaces AS (
    SELECT namespace,
           row_number() OVER (ORDER BY namespace) AS quota_rank,
           count(*) OVER () AS namespace_count
    FROM (SELECT DISTINCT namespace FROM manifest_versions WHERE rollout_status = 'active' AND namespace IN ('ens', 'basenames')) active
), candidates AS (
    SELECT DISTINCT nc.namespace, nc.raw_name
    FROM bigname_phase.children_current cc
    JOIN bigname_phase.name_current nc
      ON nc.logical_name_id = cc.parent_logical_name_id
    JOIN bigname_phase.name_surfaces surface
      ON surface.logical_name_id = nc.logical_name_id
    LEFT JOIN bigname_phase.resources resource
      ON resource.resource_id = nc.resource_id
    LEFT JOIN bigname_phase.surface_bindings binding
      ON binding.surface_binding_id = nc.surface_binding_id
    LEFT JOIN bigname_phase.token_lineages token_lineage
      ON token_lineage.token_lineage_id = nc.token_lineage_id
    {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
    JOIN active_namespaces active ON active.namespace = nc.namespace
    {DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS}
    WHERE nc.support_status = 'supported' AND nc.raw_name <> ''
      {DEFAULT_NAME_CURRENT_READ_FILTER}
      {DEFAULT_CHILDREN_CURRENT_READ_FILTER}
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
ORDER BY namespace, raw_name"#
    )
}

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
        let names: Vec<(String, String)> = sqlx::query_as(&name_corpus_sql())
            .bind(limit)
            .fetch_all(pool)
            .await
            .context("failed to load name benchmark corpus")?;
        let address_names: Vec<(String, String, String, String)> =
            sqlx::query_as(&address_corpus_sql())
                .bind(limit)
                .fetch_all(pool)
                .await
                .context("failed to load address benchmark corpus")?;
        let parents: Vec<(String, String)> = sqlx::query_as(&parent_corpus_sql())
            .bind(limit)
            .fetch_all(pool)
            .await
            .context("failed to load subname-parent benchmark corpus")?;
        let permission_subjects: Vec<String> = sqlx::query_scalar(&permission_subject_corpus_sql())
            .bind(limit)
            .fetch_all(pool)
            .await
            .context("failed to load permission-subject benchmark corpus")?;
        let primary_names: Vec<(String, String, String)> =
            sqlx::query_as(&primary_name_corpus_sql())
                .bind(limit)
                .fetch_all(pool)
                .await
                .context("failed to load primary-name benchmark corpus")?;
        let resolvers: Vec<(String, String)> = sqlx::query_as(&resolver_corpus_sql())
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

fn address_corpus_sql() -> String {
    format!(
        r#"SELECT anc.address, min(anc.raw_name), anc.namespace, anc.relation
           FROM bigname_phase.address_names_current anc
           {DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS}
           JOIN (
               SELECT DISTINCT namespace
               FROM bigname_phase.manifest_versions
               WHERE rollout_status = 'active'
                 AND namespace IN ('ens', 'basenames')
           ) active ON active.namespace = anc.namespace
           WHERE anc.support_status = 'supported'
             {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}
           GROUP BY anc.address, anc.namespace, anc.relation
           ORDER BY anc.address, anc.namespace, anc.relation
           LIMIT $1"#
    )
}

fn resolver_corpus_sql() -> String {
    format!(
        r#"SELECT resolver.chain_id, resolver.resolver_address
           FROM bigname_phase.resolver_current resolver
           WHERE resolver.support_status = 'supported'
             {DEFAULT_RESOLVER_CURRENT_READ_FILTER}
           ORDER BY resolver.chain_id, resolver.resolver_address
           LIMIT $1"#
    )
}

fn permission_subject_corpus_sql() -> String {
    format!(
        r#"SELECT pc.subject
           FROM bigname_phase.permissions_current pc
           WHERE pc.subject ~ '^0x[0-9A-Fa-f]{{40}}$'
             {DEFAULT_PERMISSIONS_CURRENT_READ_FILTER}
           GROUP BY pc.subject
           ORDER BY pc.subject
           LIMIT $1"#
    )
}

fn primary_name_corpus_sql() -> String {
    format!(
        r#"SELECT pnc.address, pnc.coin_type, pnc.namespace
           FROM bigname_phase.primary_names_current pnc
           JOIN (
               SELECT DISTINCT namespace
               FROM bigname_phase.manifest_versions
               WHERE rollout_status = 'active'
                 AND namespace IN ('ens', 'basenames')
           ) active ON active.namespace = pnc.namespace
           WHERE pnc.claim_status = 'success'
             {DEFAULT_PRIMARY_NAME_CURRENT_READ_FILTER}
           ORDER BY pnc.address, pnc.coin_type, pnc.namespace
           LIMIT $1"#
    )
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
            "name_current has {name_rows} API-visible supported rows in active public namespaces after canonical projection and identity filtering; release profile requires {min_name_rows}"
        ));
    }
    if address_rows < min_address_rows {
        failures.push(format!(
                "address_names_current has {address_rows} API-visible supported rows in active public namespaces after canonical projection and identity filtering; release profile requires {min_address_rows}"
            ));
    }
    failures
}

async fn table_count(pool: &PgPool, table: &str) -> Result<u64> {
    let count: i64 = match table {
        "name_current" => sqlx::query_scalar(&name_scale_sql()).fetch_one(pool).await,
        "address_names_current" => {
            sqlx::query_scalar(&address_scale_sql())
                .fetch_one(pool)
                .await
        }
        _ => unreachable!("benchmark table names are fixed"),
    }
    .with_context(|| format!("failed to count {table} benchmark rows"))?;
    u64::try_from(count).with_context(|| format!("{table} returned a negative row count"))
}

fn name_scale_sql() -> String {
    format!(
        r#"SELECT count(*)
           FROM bigname_phase.name_current nc
           JOIN bigname_phase.name_surfaces surface
             ON surface.logical_name_id = nc.logical_name_id
           LEFT JOIN bigname_phase.resources resource
             ON resource.resource_id = nc.resource_id
           LEFT JOIN bigname_phase.surface_bindings binding
             ON binding.surface_binding_id = nc.surface_binding_id
           LEFT JOIN bigname_phase.token_lineages token_lineage
             ON token_lineage.token_lineage_id = nc.token_lineage_id
           {DEFAULT_NAME_CURRENT_LINEAGE_JOINS}
           JOIN (
               SELECT DISTINCT namespace
               FROM bigname_phase.manifest_versions
               WHERE rollout_status = 'active'
                 AND namespace IN ('ens', 'basenames')
           ) active ON active.namespace = nc.namespace
           WHERE nc.support_status = 'supported'
             {DEFAULT_NAME_CURRENT_READ_FILTER}"#
    )
}

fn address_scale_sql() -> String {
    format!(
        r#"SELECT count(*)
           FROM bigname_phase.address_names_current anc
           {DEFAULT_ADDRESS_NAMES_CURRENT_IDENTITY_JOINS}
           JOIN (
               SELECT DISTINCT namespace
               FROM bigname_phase.manifest_versions
               WHERE rollout_status = 'active'
                 AND namespace IN ('ens', 'basenames')
           ) active ON active.namespace = anc.namespace
           WHERE anc.support_status = 'supported'
             {DEFAULT_ADDRESS_NAMES_CURRENT_READ_FILTER}"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    async fn install_name_visibility_schema(pool: &PgPool) {
        for statement in [
            "CREATE SCHEMA bigname_phase",
            "CREATE TYPE bigname_phase.canonicality_state AS ENUM ('canonical', 'safe', 'finalized', 'orphaned')",
            "CREATE TABLE bigname_phase.manifest_versions (namespace text NOT NULL, rollout_status text NOT NULL)",
            "CREATE TABLE bigname_phase.chain_lineage (chain_id text NOT NULL, block_hash text NOT NULL, canonicality_state bigname_phase.canonicality_state NOT NULL)",
            "CREATE TABLE bigname_phase.name_surfaces (logical_name_id text NOT NULL, chain_id text NOT NULL, block_hash text NOT NULL, canonicality_state bigname_phase.canonicality_state NOT NULL)",
            "CREATE TABLE bigname_phase.resources (resource_id uuid NOT NULL, chain_id text NOT NULL, block_hash text NOT NULL, canonicality_state bigname_phase.canonicality_state NOT NULL)",
            "CREATE TABLE bigname_phase.surface_bindings (surface_binding_id uuid NOT NULL, chain_id text NOT NULL, block_hash text NOT NULL, canonicality_state bigname_phase.canonicality_state NOT NULL, active_to timestamptz)",
            "CREATE TABLE bigname_phase.token_lineages (token_lineage_id uuid NOT NULL, chain_id text NOT NULL, block_hash text NOT NULL, canonicality_state bigname_phase.canonicality_state NOT NULL)",
            "CREATE TABLE bigname_phase.name_current (namespace text NOT NULL, raw_name text NOT NULL, logical_name_id text NOT NULL, support_status text NOT NULL, surface_binding_id uuid, resource_id uuid, token_lineage_id uuid, provenance jsonb NOT NULL, canonicality_summary jsonb NOT NULL)",
            "CREATE TABLE bigname_phase.address_names_current (address text NOT NULL, raw_name text NOT NULL, namespace text NOT NULL, relation text NOT NULL, logical_name_id text NOT NULL, support_status text NOT NULL, surface_binding_id uuid NOT NULL, resource_id uuid NOT NULL, token_lineage_id uuid, provenance jsonb NOT NULL, chain_positions jsonb NOT NULL, canonicality_summary jsonb NOT NULL)",
            "CREATE TABLE bigname_phase.resolver_current (chain_id text NOT NULL, resolver_address text NOT NULL, support_status text NOT NULL, chain_positions jsonb NOT NULL, canonicality_summary jsonb NOT NULL)",
            "CREATE TABLE bigname_phase.children_current (parent_logical_name_id text NOT NULL, child_logical_name_id text NOT NULL, provenance jsonb NOT NULL, chain_positions jsonb NOT NULL, canonicality_summary jsonb NOT NULL)",
            "CREATE TABLE bigname_phase.permissions_current (subject text NOT NULL, resource_id uuid NOT NULL, provenance jsonb NOT NULL, chain_positions jsonb NOT NULL, canonicality_summary jsonb NOT NULL)",
            "CREATE TABLE bigname_phase.primary_names_current (address text NOT NULL, coin_type text NOT NULL, namespace text NOT NULL, claim_status text NOT NULL, claim_provenance jsonb NOT NULL)",
            "SET search_path TO bigname_phase, public",
        ] {
            sqlx::query(statement).execute(pool).await.unwrap();
        }
    }

    async fn insert_name_with_visibility(
        pool: &PgPool,
        namespace: &str,
        raw_name: &str,
        logical_name_id: &str,
        support_status: &str,
        surface_state: &str,
        projection_state: &str,
    ) {
        let surface_hash = format!("{logical_name_id}-surface");
        let projection_hash = format!("{logical_name_id}-projection");
        sqlx::query(
            "INSERT INTO chain_lineage VALUES
                 ('ethereum-mainnet', $1, $3::bigname_phase.canonicality_state),
                 ('ethereum-mainnet', $2, $4::bigname_phase.canonicality_state)",
        )
        .bind(&surface_hash)
        .bind(&projection_hash)
        .bind(surface_state)
        .bind(projection_state)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO name_surfaces VALUES
                 ($1, 'ethereum-mainnet', $2, $3::bigname_phase.canonicality_state)",
        )
        .bind(logical_name_id)
        .bind(surface_hash)
        .bind(surface_state)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO name_current VALUES
                 ($1, $2, $3, $4, NULL, NULL, NULL,
                  '{\"chain_id\":\"ethereum-mainnet\"}',
                  jsonb_build_object('state', 'canonical_lineage', 'target_block_hash', $5::text))",
        )
        .bind(namespace)
        .bind(raw_name)
        .bind(logical_name_id)
        .bind(support_status)
        .bind(projection_hash)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_visible_child_parent(pool: &PgPool, parent_logical_name_id: &str) {
        let projection_hash = format!("{parent_logical_name_id}-children-projection");
        sqlx::query(
            "INSERT INTO chain_lineage VALUES
                 ('ethereum-mainnet', $1, 'canonical')",
        )
        .bind(&projection_hash)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO children_current VALUES
                 ($1, $2, '{\"chain_id\":\"ethereum-mainnet\"}',
                  jsonb_build_object('target_block_hash', $3::text),
                  '{\"state\":\"canonical\"}')",
        )
        .bind(parent_logical_name_id)
        .bind(format!("{parent_logical_name_id}-missing-child"))
        .bind(projection_hash)
        .execute(pool)
        .await
        .unwrap();
    }

    #[test]
    fn production_scale_rejects_staging_sized_tables() {
        assert!(!table_scale_failures(50_000, 75_000, 3_000_000, 3_000_000).is_empty());
        assert!(table_scale_failures(3_000_000, 3_000_000, 3_000_000, 3_000_000).is_empty());
    }

    #[test]
    fn subname_parent_corpus_excludes_the_empty_root() {
        let query = parent_corpus_sql();
        assert!(query.contains("nc.raw_name <> ''"));
        assert!(query.contains("nc.support_status = 'supported'"));
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
        let database = TestDatabase::create(
            TestDatabaseConfig::new("benchmark_supported_scale_preflight").pool_max_connections(1),
        )
        .await
        .unwrap();
        install_name_visibility_schema(database.pool()).await;
        for index in 0..8 {
            insert_name_with_visibility(
                database.pool(),
                "ens",
                &format!("unsupported-{index}.eth"),
                &format!("ens:unsupported-{index}"),
                "unsupported",
                "canonical",
                "canonical",
            )
            .await;
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
                "name_current has 0 API-visible supported rows in active public namespaces after canonical projection and identity filtering; release profile requires 8",
                "address_names_current has 0 API-visible supported rows in active public namespaces after canonical projection and identity filtering; release profile requires 8",
            ]
        );
        database.cleanup().await.unwrap();
    }

    #[tokio::test]
    async fn hidden_supported_names_are_excluded_from_corpus_and_scale() {
        let database = TestDatabase::create(
            TestDatabaseConfig::new("benchmark_visible_corpus").pool_max_connections(1),
        )
        .await
        .unwrap();
        install_name_visibility_schema(database.pool()).await;
        sqlx::query("INSERT INTO manifest_versions VALUES ('ens', 'active')")
            .execute(database.pool())
            .await
            .unwrap();
        insert_name_with_visibility(
            database.pool(),
            "ens",
            "healthy.eth",
            "ens:healthy",
            "supported",
            "canonical",
            "canonical",
        )
        .await;
        insert_name_with_visibility(
            database.pool(),
            "ens",
            "orphan-target.eth",
            "ens:orphan-target",
            "supported",
            "canonical",
            "orphaned",
        )
        .await;
        insert_name_with_visibility(
            database.pool(),
            "ens",
            "orphan-surface.eth",
            "ens:orphan-surface",
            "supported",
            "orphaned",
            "canonical",
        )
        .await;

        let names: Vec<(String, String)> = sqlx::query_as(&name_corpus_sql())
            .bind(10_i64)
            .fetch_all(database.pool())
            .await
            .unwrap();
        let scale = load_table_scale(database.pool()).await.unwrap();

        database.cleanup().await.unwrap();
        assert_eq!(names, [("ens".to_owned(), "healthy.eth".to_owned())]);
        assert_eq!(scale.name_current_rows, 1);
    }

    #[tokio::test]
    async fn inactive_namespace_names_do_not_satisfy_the_scale_floor() {
        let database = TestDatabase::create(
            TestDatabaseConfig::new("benchmark_active_name_scale").pool_max_connections(1),
        )
        .await
        .unwrap();
        install_name_visibility_schema(database.pool()).await;
        sqlx::query("INSERT INTO manifest_versions VALUES ('ens', 'active')")
            .execute(database.pool())
            .await
            .unwrap();
        for index in 0..2 {
            insert_name_with_visibility(
                database.pool(),
                "ens",
                &format!("active-{index}.eth"),
                &format!("ens:active-{index}"),
                "supported",
                "canonical",
                "canonical",
            )
            .await;
        }
        for index in 0..5 {
            insert_name_with_visibility(
                database.pool(),
                "ens-sepolia",
                &format!("inactive-{index}.eth"),
                &format!("ens-sepolia:inactive-{index}"),
                "supported",
                "canonical",
                "canonical",
            )
            .await;
        }

        let scale = load_table_scale(database.pool()).await.unwrap();

        database.cleanup().await.unwrap();
        assert_eq!(scale.name_current_rows, 2);
    }

    #[tokio::test]
    async fn hidden_address_and_resolver_rows_are_excluded_from_corpus_and_scale() {
        let database = TestDatabase::create(
            TestDatabaseConfig::new("benchmark_visible_address_resolver").pool_max_connections(1),
        )
        .await
        .unwrap();
        install_name_visibility_schema(database.pool()).await;
        sqlx::query("INSERT INTO manifest_versions VALUES ('ens', 'active')")
            .execute(database.pool())
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO chain_lineage VALUES
                 ('ethereum-mainnet', 'visible-surface', 'canonical'),
                 ('ethereum-mainnet', 'visible-resource', 'canonical'),
                 ('ethereum-mainnet', 'visible-binding', 'canonical'),
                 ('ethereum-mainnet', 'visible-projection', 'canonical'),
                 ('ethereum-mainnet', 'hidden-surface', 'canonical'),
                 ('ethereum-mainnet', 'hidden-resource', 'canonical'),
                 ('ethereum-mainnet', 'hidden-binding', 'canonical'),
                 ('ethereum-mainnet', 'hidden-projection', 'orphaned'),
                 ('ethereum-mainnet', 'visible-resolver-projection', 'canonical'),
                 ('ethereum-mainnet', 'hidden-resolver-projection', 'orphaned')",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::raw_sql(
            "INSERT INTO name_surfaces VALUES
                 ('ens:visible-address', 'ethereum-mainnet', 'visible-surface', 'canonical'),
                 ('ens:hidden-address', 'ethereum-mainnet', 'hidden-surface', 'canonical');
             INSERT INTO resources VALUES
                 ('00000000-0000-0000-0000-000000000011', 'ethereum-mainnet', 'visible-resource', 'canonical'),
                 ('00000000-0000-0000-0000-000000000012', 'ethereum-mainnet', 'hidden-resource', 'canonical');
             INSERT INTO surface_bindings VALUES
                 ('00000000-0000-0000-0000-000000000021', 'ethereum-mainnet', 'visible-binding', 'canonical', NULL),
                 ('00000000-0000-0000-0000-000000000022', 'ethereum-mainnet', 'hidden-binding', 'canonical', NULL)",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::raw_sql(
            "INSERT INTO address_names_current VALUES
                 ('0x0000000000000000000000000000000000000001', 'visible.eth', 'ens', 'effective_controller', 'ens:visible-address', 'supported',
                  '00000000-0000-0000-0000-000000000021', '00000000-0000-0000-0000-000000000011', NULL,
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"visible-projection\"}', '{\"state\":\"canonical_lineage\"}'),
                 ('0x0000000000000000000000000000000000000002', 'hidden.eth', 'ens', 'effective_controller', 'ens:hidden-address', 'supported',
                  '00000000-0000-0000-0000-000000000022', '00000000-0000-0000-0000-000000000012', NULL,
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"hidden-projection\"}', '{\"state\":\"canonical_lineage\"}');
             INSERT INTO resolver_current VALUES
                 ('ethereum-mainnet', '0x0000000000000000000000000000000000000031', 'supported',
                  '{\"target_block_hash\":\"visible-resolver-projection\"}', '{\"state\":\"canonical_lineage\"}'),
                 ('ethereum-mainnet', '0x0000000000000000000000000000000000000032', 'supported',
                  '{\"target_block_hash\":\"hidden-resolver-projection\"}', '{\"state\":\"canonical_lineage\"}')",
        )
        .execute(database.pool())
        .await
        .unwrap();

        let addresses: Vec<(String, String, String, String)> =
            sqlx::query_as(&address_corpus_sql())
                .bind(10_i64)
                .fetch_all(database.pool())
                .await
                .unwrap();
        let resolvers: Vec<(String, String)> = sqlx::query_as(&resolver_corpus_sql())
            .bind(10_i64)
            .fetch_all(database.pool())
            .await
            .unwrap();
        let scale = load_table_scale(database.pool()).await.unwrap();

        database.cleanup().await.unwrap();
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0].1, "visible.eth");
        assert_eq!(resolvers.len(), 1);
        assert_eq!(resolvers[0].1, "0x0000000000000000000000000000000000000031");
        assert_eq!(scale.address_names_current_rows, 1);
    }

    #[tokio::test]
    async fn inactive_namespace_rows_are_excluded_from_address_and_primary_corpora() {
        let database = TestDatabase::create(
            TestDatabaseConfig::new("benchmark_active_public_corpus").pool_max_connections(1),
        )
        .await
        .unwrap();
        install_name_visibility_schema(database.pool()).await;
        sqlx::query("INSERT INTO manifest_versions VALUES ('ens', 'active')")
            .execute(database.pool())
            .await
            .unwrap();
        sqlx::raw_sql(
            "INSERT INTO chain_lineage VALUES
                 ('ethereum-mainnet', 'ens-surface', 'canonical'),
                 ('ethereum-mainnet', 'ens-resource', 'canonical'),
                 ('ethereum-mainnet', 'ens-binding', 'canonical'),
                 ('ethereum-mainnet', 'ens-projection', 'canonical'),
                 ('ethereum-sepolia', 'sepolia-surface', 'canonical'),
                 ('ethereum-sepolia', 'sepolia-resource', 'canonical'),
                 ('ethereum-sepolia', 'sepolia-binding', 'canonical'),
                 ('ethereum-sepolia', 'sepolia-projection', 'canonical'),
                 ('ethereum-mainnet', 'ens-primary-projection', 'canonical'),
                 ('ethereum-sepolia', 'sepolia-primary-projection', 'canonical');
             INSERT INTO name_surfaces VALUES
                 ('ens:visible-address', 'ethereum-mainnet', 'ens-surface', 'canonical'),
                 ('ens-sepolia:inactive-address', 'ethereum-sepolia', 'sepolia-surface', 'canonical');
             INSERT INTO resources VALUES
                 ('00000000-0000-0000-0000-000000000061', 'ethereum-mainnet', 'ens-resource', 'canonical'),
                 ('00000000-0000-0000-0000-000000000062', 'ethereum-sepolia', 'sepolia-resource', 'canonical');
             INSERT INTO surface_bindings VALUES
                 ('00000000-0000-0000-0000-000000000071', 'ethereum-mainnet', 'ens-binding', 'canonical', NULL),
                 ('00000000-0000-0000-0000-000000000072', 'ethereum-sepolia', 'sepolia-binding', 'canonical', NULL);
             INSERT INTO address_names_current VALUES
                 ('0x0000000000000000000000000000000000000061', 'visible.eth', 'ens', 'effective_controller', 'ens:visible-address', 'supported',
                  '00000000-0000-0000-0000-000000000071', '00000000-0000-0000-0000-000000000061', NULL,
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"ens-projection\"}', '{\"state\":\"canonical_lineage\"}'),
                 ('0x0000000000000000000000000000000000000062', 'inactive.eth', 'ens-sepolia', 'effective_controller', 'ens-sepolia:inactive-address', 'supported',
                  '00000000-0000-0000-0000-000000000072', '00000000-0000-0000-0000-000000000062', NULL,
                  '{\"chain_id\":\"ethereum-sepolia\"}', '{\"target_block_hash\":\"sepolia-projection\"}', '{\"state\":\"canonical_lineage\"}');
             INSERT INTO primary_names_current VALUES
                 ('0x0000000000000000000000000000000000000061', '60', 'ens', 'success',
                  '{\"chain_id\":\"ethereum-mainnet\",\"target_block_hash\":\"ens-primary-projection\"}'),
                 ('0x0000000000000000000000000000000000000062', '60', 'ens-sepolia', 'success',
                  '{\"chain_id\":\"ethereum-sepolia\",\"target_block_hash\":\"sepolia-primary-projection\"}')",
        )
        .execute(database.pool())
        .await
        .unwrap();

        let addresses: Vec<(String, String, String, String)> =
            sqlx::query_as(&address_corpus_sql())
                .bind(10_i64)
                .fetch_all(database.pool())
                .await
                .unwrap();
        let primary_names: Vec<(String, String, String)> =
            sqlx::query_as(&primary_name_corpus_sql())
                .bind(10_i64)
                .fetch_all(database.pool())
                .await
                .unwrap();
        let scale = load_table_scale(database.pool()).await.unwrap();

        database.cleanup().await.unwrap();
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0].2, "ens");
        assert_eq!(primary_names.len(), 1);
        assert_eq!(primary_names[0].2, "ens");
        assert_eq!(scale.address_names_current_rows, 1);
    }

    #[tokio::test]
    async fn hidden_specialized_rows_are_excluded_from_the_corpus() {
        let database = TestDatabase::create(
            TestDatabaseConfig::new("benchmark_visible_specialized").pool_max_connections(1),
        )
        .await
        .unwrap();
        install_name_visibility_schema(database.pool()).await;
        sqlx::query("INSERT INTO manifest_versions VALUES ('ens', 'active')")
            .execute(database.pool())
            .await
            .unwrap();
        insert_name_with_visibility(
            database.pool(),
            "ens",
            "visible.eth",
            "ens:visible-parent",
            "supported",
            "canonical",
            "canonical",
        )
        .await;
        insert_name_with_visibility(
            database.pool(),
            "ens",
            "hidden.eth",
            "ens:hidden-parent",
            "supported",
            "canonical",
            "canonical",
        )
        .await;
        insert_name_with_visibility(
            database.pool(),
            "ens",
            "hidden-parent-name.eth",
            "ens:hidden-parent-name",
            "supported",
            "canonical",
            "orphaned",
        )
        .await;
        sqlx::query(
            "INSERT INTO chain_lineage VALUES
                 ('ethereum-mainnet', 'visible-child-projection', 'canonical'),
                 ('ethereum-mainnet', 'hidden-parent-name-child-projection', 'canonical'),
                 ('ethereum-mainnet', 'hidden-child-projection', 'orphaned'),
                 ('ethereum-mainnet', 'permission-resource', 'canonical'),
                 ('ethereum-mainnet', 'visible-permission-projection', 'canonical'),
                 ('ethereum-mainnet', 'hidden-permission-projection', 'orphaned'),
                 ('ethereum-mainnet', 'visible-primary-projection', 'canonical'),
                 ('ethereum-mainnet', 'hidden-primary-projection', 'orphaned')",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::raw_sql(
            "INSERT INTO children_current VALUES
                 ('ens:visible-parent', 'ens:missing-visible-child',
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"visible-child-projection\"}', '{\"state\":\"canonical\"}'),
                 ('ens:hidden-parent-name', 'ens:missing-hidden-parent-name-child',
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"hidden-parent-name-child-projection\"}', '{\"state\":\"canonical\"}'),
                 ('ens:hidden-parent', 'ens:missing-hidden-child',
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"hidden-child-projection\"}', '{\"state\":\"canonical\"}');
             INSERT INTO resources VALUES
                 ('00000000-0000-0000-0000-000000000041', 'ethereum-mainnet', 'permission-resource', 'canonical');
             INSERT INTO permissions_current VALUES
                 ('0x0000000000000000000000000000000000000041', '00000000-0000-0000-0000-000000000041',
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"visible-permission-projection\"}', '{\"state\":\"canonical\"}'),
                 ('0x0000000000000000000000000000000000000042', '00000000-0000-0000-0000-000000000041',
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"hidden-permission-projection\"}', '{\"state\":\"canonical\"}');
             INSERT INTO primary_names_current VALUES
                 ('0x0000000000000000000000000000000000000051', '60', 'ens', 'success',
                  '{\"chain_id\":\"ethereum-mainnet\",\"target_block_hash\":\"visible-primary-projection\"}'),
                 ('0x0000000000000000000000000000000000000052', '60', 'ens', 'success',
                  '{\"chain_id\":\"ethereum-mainnet\",\"target_block_hash\":\"hidden-primary-projection\"}')",
        )
        .execute(database.pool())
        .await
        .unwrap();

        let parents: Vec<(String, String)> = sqlx::query_as(&parent_corpus_sql())
            .bind(10_i64)
            .fetch_all(database.pool())
            .await
            .unwrap();
        let subjects: Vec<String> = sqlx::query_scalar(&permission_subject_corpus_sql())
            .bind(10_i64)
            .fetch_all(database.pool())
            .await
            .unwrap();
        let primary_names: Vec<(String, String, String)> =
            sqlx::query_as(&primary_name_corpus_sql())
                .bind(10_i64)
                .fetch_all(database.pool())
                .await
                .unwrap();

        database.cleanup().await.unwrap();
        assert_eq!(parents, [("ens".to_owned(), "visible.eth".to_owned())]);
        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0], "0x0000000000000000000000000000000000000041");
        assert_eq!(primary_names.len(), 1);
        assert_eq!(
            primary_names[0].0,
            "0x0000000000000000000000000000000000000051"
        );
    }

    #[tokio::test]
    async fn name_corpus_is_stratified_across_active_namespaces() {
        let database = TestDatabase::create(
            TestDatabaseConfig::new("benchmark_namespace_stratification").pool_max_connections(1),
        )
        .await
        .unwrap();
        install_name_visibility_schema(database.pool()).await;
        sqlx::query(
            "INSERT INTO manifest_versions VALUES
                 ('basenames', 'active'), ('ens', 'active')",
        )
        .execute(database.pool())
        .await
        .unwrap();
        for index in 0..4 {
            insert_name_with_visibility(
                database.pool(),
                "basenames",
                &format!("base-{index}.base.eth"),
                &format!("basenames:{index:02}"),
                "supported",
                "canonical",
                "canonical",
            )
            .await;
        }
        for index in 0..2 {
            insert_name_with_visibility(
                database.pool(),
                "ens",
                &format!("ens-{index}.eth"),
                &format!("ens:{index:02}"),
                "supported",
                "canonical",
                "canonical",
            )
            .await;
        }

        let names: Vec<(String, String)> = sqlx::query_as(&name_corpus_sql())
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
        let database = TestDatabase::create(
            TestDatabaseConfig::new("benchmark_parent_namespace_stratification")
                .pool_max_connections(1),
        )
        .await
        .unwrap();
        install_name_visibility_schema(database.pool()).await;
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
                insert_name_with_visibility(
                    database.pool(),
                    namespace,
                    &format!("{namespace}-{index}.eth"),
                    &logical_name_id,
                    "supported",
                    "canonical",
                    "canonical",
                )
                .await;
                insert_visible_child_parent(database.pool(), &logical_name_id).await;
            }
        }

        let parents: Vec<(String, String)> = sqlx::query_as(&parent_corpus_sql())
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
        let database = TestDatabase::create(
            TestDatabaseConfig::new("benchmark_parent_namespace_coverage").pool_max_connections(1),
        )
        .await
        .unwrap();
        install_name_visibility_schema(database.pool()).await;
        sqlx::query(
            "INSERT INTO manifest_versions VALUES
                 ('basenames', 'active'), ('ens', 'active')",
        )
        .execute(database.pool())
        .await
        .unwrap();
        for index in 0..16 {
            insert_name_with_visibility(
                database.pool(),
                "basenames",
                &format!("base-{index}.base.eth"),
                &format!("basenames:{index:02}"),
                "supported",
                "canonical",
                "canonical",
            )
            .await;
            insert_name_with_visibility(
                database.pool(),
                "ens",
                &format!("ens-{index}.eth"),
                &format!("ens:{index:02}"),
                "supported",
                "canonical",
                "canonical",
            )
            .await;
        }
        insert_visible_child_parent(database.pool(), "basenames:00").await;
        let parents: Vec<(String, String)> = sqlx::query_as(&parent_corpus_sql())
            .bind(32_i64)
            .fetch_all(database.pool())
            .await
            .unwrap();
        let counts = namespace_counts(&parents);
        let result = require_active_namespace_coverage(
            &["basenames".to_owned(), "ens".to_owned()],
            &counts,
            "supported parents",
        );
        database.cleanup().await.unwrap();

        let error = result.expect_err("ENS with zero parent seeds must be rejected");
        assert!(error.to_string().contains("active namespace \"ens\""));
        assert!(error.to_string().contains("supported parents"));
    }
}
