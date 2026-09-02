use std::collections::BTreeMap;

use anyhow::{Context, Result};
use bigname_storage::{
    DEFAULT_CHILDREN_CURRENT_IDENTITY_JOINS, DEFAULT_CHILDREN_CURRENT_READ_FILTER,
    DEFAULT_NAME_CURRENT_LINEAGE_JOINS, DEFAULT_NAME_CURRENT_READ_FILTER,
};
use sqlx::PgPool;

use crate::budgets::GateBudgets;

mod permissions;
mod resolver_coverage;
mod scale;
mod stratified;
mod verdict;
pub(super) use permissions::PermissionTarget;
use resolver_coverage::load as load_resolver_coverage;
#[cfg(test)]
use scale::table_scale_failures;
pub(super) use scale::{TableScale, load_table_scale};
use stratified::{
    address_corpus_sql, address_namespace_counts, primary_name_corpus_sql,
    primary_namespace_counts, require_active_namespace_coverage,
};
pub(super) use verdict::require_stratified_size as require_stratified_corpus_size;
use verdict::{
    collect_failure as collect_corpus_failure, require_minimum_size as require_minimum_corpus_size,
};

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
    pub(super) permission_subjects: Vec<PermissionTarget>,
    pub(super) primary_names: Vec<(String, String, String)>,
    pub(super) resolvers: Vec<super::workload::ResolverTarget>,
    pub(super) namespaces: Vec<String>,
    pub(super) names_by_namespace: BTreeMap<String, usize>,
    pub(super) parents_by_namespace: BTreeMap<String, usize>,
    pub(super) resolver_manifest_coverage: Vec<super::ResolverManifestCoverage>,
}

impl Corpus {
    pub(super) async fn load(pool: &PgPool, budgets: &GateBudgets) -> Result<(Self, Vec<String>)> {
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
        let permission_subjects = permissions::load(pool, limit).await?;
        let primary_names: Vec<(String, String, String)> =
            sqlx::query_as(&primary_name_corpus_sql())
                .bind(limit)
                .fetch_all(pool)
                .await
                .context("failed to load primary-name benchmark corpus")?;
        let resolver_coverage = load_resolver_coverage(pool).await?;
        let names_by_namespace = namespace_counts(&names);
        let parents_by_namespace = namespace_counts(&parents);
        let addresses_by_namespace = address_namespace_counts(&address_names);
        let primary_names_by_namespace = primary_namespace_counts(&primary_names);

        let mut failures = resolver_coverage.failures;
        if budgets.api_require_populated_probes
            && !permission_subjects
                .iter()
                .any(|target| target.retained_registration)
        {
            failures.push(
                "permission corpus contains no canonical retained registration absent from name_current; restore production-shaped superseded-registration permission history and rerun the gate"
                    .to_owned(),
            );
        }
        collect_corpus_failure(
            &mut failures,
            require_active_namespace_coverage(&namespaces, &names_by_namespace, "supported names"),
        );
        collect_corpus_failure(
            &mut failures,
            require_active_namespace_coverage(
                &namespaces,
                &parents_by_namespace,
                "supported parents",
            ),
        );
        collect_corpus_failure(
            &mut failures,
            require_active_namespace_coverage(
                &namespaces,
                &addresses_by_namespace,
                "supported address/name relations",
            ),
        );
        if budgets.api_min_specialized_corpus_size > 0 {
            collect_corpus_failure(
                &mut failures,
                require_active_namespace_coverage(
                    &namespaces,
                    &primary_names_by_namespace,
                    "successful primary names",
                ),
            );
        }
        collect_corpus_failure(
            &mut failures,
            require_stratified_corpus_size(
                "name",
                names.len(),
                budgets.api_corpus_size,
                &names_by_namespace,
            ),
        );
        collect_corpus_failure(
            &mut failures,
            require_stratified_corpus_size(
                "address",
                address_names.len(),
                budgets.api_corpus_size,
                &addresses_by_namespace,
            ),
        );
        for (label, actual) in [
            ("subname parent", parents.len()),
            ("permission subject", permission_subjects.len()),
        ] {
            collect_corpus_failure(
                &mut failures,
                require_minimum_corpus_size(label, actual, budgets.api_min_specialized_corpus_size),
            );
        }
        collect_corpus_failure(
            &mut failures,
            require_stratified_corpus_size(
                "successful primary-name",
                primary_names.len(),
                budgets.api_min_specialized_corpus_size,
                &primary_names_by_namespace,
            ),
        );

        Ok((
            Self {
                names,
                address_names,
                parents,
                permission_subjects,
                primary_names,
                resolvers: resolver_coverage.resolvers,
                namespaces,
                names_by_namespace,
                parents_by_namespace,
                resolver_manifest_coverage: resolver_coverage.counts,
            },
            failures,
        ))
    }
}

fn namespace_counts(rows: &[(String, String)]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for (namespace, _) in rows {
        *counts.entry(namespace.clone()).or_insert(0) += 1;
    }
    counts
}

#[cfg(test)]
#[path = "corpus/tests/load.rs"]
mod load_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    pub(super) async fn install_name_visibility_schema(pool: &PgPool) {
        for statement in [
            "CREATE SCHEMA bigname_phase",
            "CREATE TYPE bigname_phase.canonicality_state AS ENUM ('canonical', 'safe', 'finalized', 'orphaned')",
            "CREATE TABLE bigname_phase.manifest_versions (manifest_id bigint GENERATED BY DEFAULT AS IDENTITY, manifest_version bigint NOT NULL DEFAULT 1, namespace text NOT NULL, rollout_status text NOT NULL, source_family text NOT NULL DEFAULT 'benchmark_non_resolver', chain_id text NOT NULL DEFAULT 'ethereum-mainnet', normalizer_version text NOT NULL DEFAULT 'ensip15@ens-normalize-0.1.1', manifest_payload jsonb NOT NULL DEFAULT '{\"contracts\":[]}'::jsonb)",
            "CREATE TABLE bigname_phase.chain_lineage (chain_id text NOT NULL, block_hash text NOT NULL, canonicality_state bigname_phase.canonicality_state NOT NULL, block_number bigint NOT NULL DEFAULT 0)",
            "CREATE TABLE bigname_phase.name_surfaces (logical_name_id text NOT NULL, chain_id text NOT NULL, block_hash text NOT NULL, canonicality_state bigname_phase.canonicality_state NOT NULL)",
            "CREATE TABLE bigname_phase.resources (resource_id uuid NOT NULL, chain_id text NOT NULL, block_hash text NOT NULL, canonicality_state bigname_phase.canonicality_state NOT NULL)",
            "CREATE TABLE bigname_phase.surface_bindings (surface_binding_id uuid NOT NULL, chain_id text NOT NULL, block_hash text NOT NULL, canonicality_state bigname_phase.canonicality_state NOT NULL, active_to timestamptz)",
            "CREATE TABLE bigname_phase.token_lineages (token_lineage_id uuid NOT NULL, chain_id text NOT NULL, block_hash text NOT NULL, canonicality_state bigname_phase.canonicality_state NOT NULL)",
            "CREATE TABLE bigname_phase.name_current (namespace text NOT NULL, raw_name text NOT NULL, logical_name_id text NOT NULL, support_status text NOT NULL, surface_binding_id uuid, resource_id uuid, serving_resource_id uuid, token_lineage_id uuid, provenance jsonb NOT NULL, canonicality_summary jsonb NOT NULL)",
            "CREATE TABLE bigname_phase.address_names_current (address text NOT NULL, raw_name text NOT NULL, namespace text NOT NULL, relation text NOT NULL, logical_name_id text NOT NULL, support_status text NOT NULL, surface_binding_id uuid NOT NULL, resource_id uuid NOT NULL, token_lineage_id uuid, provenance jsonb NOT NULL, chain_positions jsonb NOT NULL, canonicality_summary jsonb NOT NULL)",
            "CREATE TABLE bigname_phase.resolver_current (chain_id text NOT NULL, resolver_address text NOT NULL, support_status text NOT NULL, chain_positions jsonb NOT NULL, canonicality_summary jsonb NOT NULL, provenance jsonb NOT NULL DEFAULT '{}'::jsonb, manifest_version bigint NOT NULL DEFAULT 1)",
            "CREATE TABLE bigname_phase.normalized_events (normalized_event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, event_identity text NOT NULL UNIQUE, namespace text NOT NULL, event_kind text NOT NULL, source_family text NOT NULL, manifest_version bigint NOT NULL, source_manifest_id bigint, chain_id text NOT NULL, block_number bigint, block_hash text, transaction_index bigint, log_index bigint, canonicality_state bigname_phase.canonicality_state NOT NULL, before_state jsonb NOT NULL DEFAULT '{}'::jsonb, after_state jsonb NOT NULL DEFAULT '{}'::jsonb, consumer_visibility text NOT NULL DEFAULT 'activated')",
            "CREATE TABLE bigname_phase.children_current (parent_logical_name_id text NOT NULL, child_logical_name_id text NOT NULL, provenance jsonb NOT NULL, chain_positions jsonb NOT NULL, canonicality_summary jsonb NOT NULL)",
            "CREATE TABLE bigname_phase.permissions_current (subject text NOT NULL, resource_id uuid NOT NULL, provenance jsonb NOT NULL, chain_positions jsonb NOT NULL, canonicality_summary jsonb NOT NULL)",
            "CREATE TABLE bigname_phase.primary_names_current (address text NOT NULL, coin_type text NOT NULL, namespace text NOT NULL, claim_status text NOT NULL, claim_provenance jsonb NOT NULL)",
            "SET search_path TO bigname_phase, public",
        ] {
            sqlx::query(statement).execute(pool).await.unwrap();
        }
    }

    pub(super) async fn insert_name_with_visibility(
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
                 ($1, $2, $3, $4, NULL, NULL, NULL, NULL,
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

    pub(super) async fn insert_visible_child_parent(pool: &PgPool, parent_logical_name_id: &str) {
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
        sqlx::query(
            "INSERT INTO manifest_versions (namespace, rollout_status) VALUES ('ens', 'active')",
        )
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
        sqlx::query(
            "INSERT INTO manifest_versions (namespace, rollout_status) VALUES ('ens', 'active')",
        )
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
        sqlx::query(
            "INSERT INTO manifest_versions (namespace, rollout_status) VALUES ('ens', 'active')",
        )
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
             INSERT INTO resolver_current (chain_id, resolver_address, support_status,
                 chain_positions, canonicality_summary) VALUES ('ethereum-mainnet', '0x0000000000000000000000000000000000000031', 'supported',
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
        let scale = load_table_scale(database.pool()).await.unwrap();

        database.cleanup().await.unwrap();
        assert_eq!(addresses.len(), 1);
        assert_eq!(addresses[0].1, "visible.eth");
        assert_eq!(scale.address_names_current_rows, 1);
    }

    #[tokio::test]
    async fn address_and_primary_corpora_are_stratified_across_active_namespaces() {
        let database = TestDatabase::create(
            TestDatabaseConfig::new("benchmark_active_public_corpus").pool_max_connections(1),
        )
        .await
        .unwrap();
        install_name_visibility_schema(database.pool()).await;
        sqlx::query(
            "INSERT INTO manifest_versions (namespace, rollout_status) VALUES
                 ('basenames', 'active'), ('ens', 'active')",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::raw_sql(
            "INSERT INTO chain_lineage VALUES
                 ('base-mainnet', 'base-surface', 'canonical'),
                 ('base-mainnet', 'base-resource', 'canonical'),
                 ('base-mainnet', 'base-binding', 'canonical'),
                 ('base-mainnet', 'base-projection', 'canonical'),
                 ('base-mainnet', 'base-primary-projection', 'canonical'),
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
                 ('basenames:visible-address', 'base-mainnet', 'base-surface', 'canonical'),
                 ('ens:visible-address', 'ethereum-mainnet', 'ens-surface', 'canonical'),
                 ('ens-sepolia:inactive-address', 'ethereum-sepolia', 'sepolia-surface', 'canonical');
             INSERT INTO resources VALUES
                 ('00000000-0000-0000-0000-000000000060', 'base-mainnet', 'base-resource', 'canonical'),
                 ('00000000-0000-0000-0000-000000000061', 'ethereum-mainnet', 'ens-resource', 'canonical'),
                 ('00000000-0000-0000-0000-000000000062', 'ethereum-sepolia', 'sepolia-resource', 'canonical');
             INSERT INTO surface_bindings VALUES
                 ('00000000-0000-0000-0000-000000000070', 'base-mainnet', 'base-binding', 'canonical', NULL),
                 ('00000000-0000-0000-0000-000000000071', 'ethereum-mainnet', 'ens-binding', 'canonical', NULL),
                 ('00000000-0000-0000-0000-000000000072', 'ethereum-sepolia', 'sepolia-binding', 'canonical', NULL);
             INSERT INTO address_names_current VALUES
                 ('0x0000000000000000000000000000000000000001', 'base-one.base.eth', 'basenames', 'effective_controller', 'basenames:visible-address', 'supported',
                  '00000000-0000-0000-0000-000000000070', '00000000-0000-0000-0000-000000000060', NULL,
                  '{\"chain_id\":\"base-mainnet\"}', '{\"target_block_hash\":\"base-projection\"}', '{\"state\":\"canonical_lineage\"}'),
                 ('0x0000000000000000000000000000000000000002', 'base-two.base.eth', 'basenames', 'effective_controller', 'basenames:visible-address', 'supported',
                  '00000000-0000-0000-0000-000000000070', '00000000-0000-0000-0000-000000000060', NULL,
                  '{\"chain_id\":\"base-mainnet\"}', '{\"target_block_hash\":\"base-projection\"}', '{\"state\":\"canonical_lineage\"}'),
                 ('0x0000000000000000000000000000000000000061', 'visible.eth', 'ens', 'effective_controller', 'ens:visible-address', 'supported',
                  '00000000-0000-0000-0000-000000000071', '00000000-0000-0000-0000-000000000061', NULL,
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"ens-projection\"}', '{\"state\":\"canonical_lineage\"}'),
                 ('0x0000000000000000000000000000000000000062', 'inactive.eth', 'ens-sepolia', 'effective_controller', 'ens-sepolia:inactive-address', 'supported',
                  '00000000-0000-0000-0000-000000000072', '00000000-0000-0000-0000-000000000062', NULL,
                  '{\"chain_id\":\"ethereum-sepolia\"}', '{\"target_block_hash\":\"sepolia-projection\"}', '{\"state\":\"canonical_lineage\"}');
             INSERT INTO primary_names_current VALUES
                 ('0x0000000000000000000000000000000000000001', '60', 'basenames', 'success',
                  '{\"chain_id\":\"base-mainnet\",\"target_block_hash\":\"base-primary-projection\"}'),
                 ('0x0000000000000000000000000000000000000002', '60', 'basenames', 'success',
                  '{\"chain_id\":\"base-mainnet\",\"target_block_hash\":\"base-primary-projection\"}'),
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
                .bind(3_i64)
                .fetch_all(database.pool())
                .await
                .unwrap();
        let primary_names: Vec<(String, String, String)> =
            sqlx::query_as(&primary_name_corpus_sql())
                .bind(3_i64)
                .fetch_all(database.pool())
                .await
                .unwrap();
        let scale = load_table_scale(database.pool()).await.unwrap();

        database.cleanup().await.unwrap();
        assert_eq!(addresses.len(), 3);
        assert_eq!(
            address_namespace_counts(&addresses),
            [("basenames".to_owned(), 2), ("ens".to_owned(), 1)]
                .into_iter()
                .collect()
        );
        assert_eq!(primary_names.len(), 3);
        assert_eq!(
            primary_namespace_counts(&primary_names),
            [("basenames".to_owned(), 2), ("ens".to_owned(), 1)]
                .into_iter()
                .collect()
        );
        assert_eq!(scale.address_names_current_rows, 3);
    }

    #[tokio::test]
    async fn hidden_specialized_rows_are_excluded_from_the_corpus() {
        let database = TestDatabase::create(
            TestDatabaseConfig::new("benchmark_visible_specialized").pool_max_connections(1),
        )
        .await
        .unwrap();
        install_name_visibility_schema(database.pool()).await;
        sqlx::query(
            "INSERT INTO manifest_versions (namespace, rollout_status) VALUES ('ens', 'active')",
        )
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
                 ('ethereum-mainnet', 'historical-permission-resource', 'canonical'),
                 ('ethereum-mainnet', 'visible-permission-projection', 'canonical'),
                 ('ethereum-mainnet', 'historical-permission-projection', 'canonical'),
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
                 ('00000000-0000-0000-0000-000000000041', 'ethereum-mainnet', 'permission-resource', 'canonical'),
                 ('00000000-0000-0000-0000-000000000043', 'ethereum-mainnet', 'historical-permission-resource', 'canonical');
             UPDATE name_current
             SET resource_id = '00000000-0000-0000-0000-000000000041'
             WHERE logical_name_id = 'ens:visible-parent';
             INSERT INTO permissions_current VALUES
                 ('0x0000000000000000000000000000000000000041', '00000000-0000-0000-0000-000000000041',
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"visible-permission-projection\"}', '{\"state\":\"canonical\"}'),
                 ('0x0000000000000000000000000000000000000042', '00000000-0000-0000-0000-000000000041',
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"hidden-permission-projection\"}', '{\"state\":\"canonical\"}'),
                 ('0x0000000000000000000000000000000000000043', '00000000-0000-0000-0000-000000000043',
                  '{\"chain_id\":\"ethereum-mainnet\"}', '{\"target_block_hash\":\"historical-permission-projection\"}', '{\"state\":\"canonical\"}');
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
        let subjects: Vec<String> = sqlx::query_scalar(&permissions::sql())
            .bind(10_i64)
            .fetch_all(database.pool())
            .await
            .unwrap();
        let permission_targets = permissions::load(database.pool(), 10).await.unwrap();
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
        assert_eq!(
            permission_targets[0].registration_id, "00000000-0000-0000-0000-000000000043",
            "registration-id workload must include a canonical retained registration with no current name row"
        );
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
            "INSERT INTO manifest_versions (namespace, rollout_status) VALUES
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
            "INSERT INTO manifest_versions (namespace, rollout_status) VALUES
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
            "INSERT INTO manifest_versions (namespace, rollout_status) VALUES
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
