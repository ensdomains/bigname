use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::PgPool;
use uuid::Uuid;

use super::{Corpus, require_active_namespace_coverage, require_stratified_corpus_size, tests};
use crate::budgets::{BudgetProfile, BudgetsFile, GateBudgets};

fn tiny_budgets() -> GateBudgets {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml");
    let mut budgets = BudgetsFile::load(&path)
        .unwrap()
        .profile(BudgetProfile::Production)
        .clone();
    budgets.api_corpus_size = 2;
    budgets.api_min_specialized_corpus_size = 1;
    budgets.api_min_resolver_corpus_size = 1;
    budgets
}

#[test]
fn active_namespace_coverage_names_the_missing_namespace() {
    let namespaces = vec!["basenames".to_owned(), "ens".to_owned()];
    let counts = [("basenames".to_owned(), 5_000)].into_iter().collect();
    let error = require_active_namespace_coverage(&namespaces, &counts, "supported names")
        .unwrap_err()
        .to_string();
    assert!(error.contains("active namespace \"ens\""));
}

#[test]
fn stratified_corpus_shortfalls_name_namespace_contributions() {
    let counts = [("basenames".to_owned(), 750), ("ens".to_owned(), 125)]
        .into_iter()
        .collect();
    for label in ["name", "address", "successful primary-name"] {
        let error = require_stratified_corpus_size(label, 875, 1_000, &counts)
            .unwrap_err()
            .to_string();
        assert!(error.starts_with(&format!("{label} corpus has 875 rows")));
        assert!(error.contains("basenames=750"));
        assert!(error.contains("ens=125"));
    }
}

async fn seeded_database(label: &str) -> TestDatabase {
    let database = TestDatabase::create(TestDatabaseConfig::new(label).pool_max_connections(1))
        .await
        .unwrap();
    let pool = database.pool();
    tests::install_name_visibility_schema(pool).await;
    sqlx::query("INSERT INTO manifest_versions VALUES ('basenames', 'active'), ('ens', 'active')")
        .execute(pool)
        .await
        .unwrap();

    for namespace in ["basenames", "ens"] {
        for index in 0..2 {
            let logical_name_id = format!("{namespace}:load-{index}");
            tests::insert_name_with_visibility(
                pool,
                namespace,
                &format!("{namespace}-{index}.eth"),
                &logical_name_id,
                "supported",
                "canonical",
                "canonical",
            )
            .await;
            tests::insert_visible_child_parent(pool, &logical_name_id).await;
            insert_address(pool, namespace, index, &logical_name_id).await;
        }
        insert_primary_name(pool, namespace).await;
    }
    insert_permission_subjects_and_resolver(pool).await;
    database
}

async fn insert_address(pool: &PgPool, namespace: &str, index: usize, logical_name_id: &str) {
    let chain_id = if namespace == "basenames" {
        "base-mainnet"
    } else {
        "ethereum-mainnet"
    };
    let resource_id = Uuid::new_v4();
    let binding_id = Uuid::new_v4();
    let resource_hash = format!("{namespace}-resource-{index}");
    let binding_hash = format!("{namespace}-binding-{index}");
    let projection_hash = format!("{namespace}-address-projection-{index}");
    sqlx::query(
        "INSERT INTO chain_lineage VALUES
             ($1, $2, 'canonical'), ($1, $3, 'canonical'), ($1, $4, 'canonical')",
    )
    .bind(chain_id)
    .bind(&resource_hash)
    .bind(&binding_hash)
    .bind(&projection_hash)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO resources VALUES ($1, $2, $3, 'canonical')")
        .bind(resource_id)
        .bind(chain_id)
        .bind(&resource_hash)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO surface_bindings VALUES ($1, $2, $3, 'canonical', NULL)")
        .bind(binding_id)
        .bind(chain_id)
        .bind(&binding_hash)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO address_names_current VALUES
             ($1, $2, $3, 'effective_controller', $4, 'supported', $5, $6, NULL,
              jsonb_build_object('chain_id', $7::text),
              jsonb_build_object('target_block_hash', $8::text),
              '{\"state\":\"canonical_lineage\"}')",
    )
    .bind(format!("0x{index:039x}{}", usize::from(namespace == "ens")))
    .bind(format!("{namespace}-address-{index}.eth"))
    .bind(namespace)
    .bind(logical_name_id)
    .bind(binding_id)
    .bind(resource_id)
    .bind(chain_id)
    .bind(projection_hash)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_primary_name(pool: &PgPool, namespace: &str) {
    let chain_id = if namespace == "basenames" {
        "base-mainnet"
    } else {
        "ethereum-mainnet"
    };
    for index in 0..2 {
        let hash = format!("{namespace}-primary-projection-{index}");
        sqlx::query("INSERT INTO chain_lineage VALUES ($1, $2, 'canonical')")
            .bind(chain_id)
            .bind(&hash)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO primary_names_current VALUES
                 ($1, '60', $2, 'success',
                  jsonb_build_object('chain_id', $3::text, 'target_block_hash', $4::text))",
        )
        .bind(format!(
            "0x{index:038x}{}",
            if namespace == "ens" { "91" } else { "90" }
        ))
        .bind(namespace)
        .bind(chain_id)
        .bind(hash)
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn insert_permission_subjects_and_resolver(pool: &PgPool) {
    let resource_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO chain_lineage VALUES
             ('ethereum-mainnet', 'load-permission-resource', 'canonical'),
             ('ethereum-mainnet', 'load-permission-projection', 'canonical'),
             ('ethereum-mainnet', 'load-resolver-projection', 'canonical')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resources VALUES
             ($1, 'ethereum-mainnet', 'load-permission-resource', 'canonical')",
    )
    .bind(resource_id)
    .execute(pool)
    .await
    .unwrap();
    for index in 0..3 {
        sqlx::query(
            "INSERT INTO permissions_current VALUES
                 ($1, $2, '{\"chain_id\":\"ethereum-mainnet\"}',
                  '{\"target_block_hash\":\"load-permission-projection\"}',
                  '{\"state\":\"canonical\"}')",
        )
        .bind(format!("0x{index:040x}"))
        .bind(resource_id)
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO resolver_current VALUES
             ('ethereum-mainnet', '0x0000000000000000000000000000000000000082', 'supported',
              '{\"target_block_hash\":\"load-resolver-projection\"}',
              '{\"state\":\"canonical_lineage\"}')",
    )
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn corpus_load_keeps_each_namespace_coverage_check_load_bearing() {
    for (label, delete_sql, expected) in [
        (
            "corpus_load_name_coverage",
            "DELETE FROM name_current WHERE namespace = 'ens'",
            "contributed no supported names",
        ),
        (
            "corpus_load_parent_coverage",
            "DELETE FROM children_current WHERE parent_logical_name_id LIKE 'ens:%'",
            "contributed no supported parents",
        ),
        (
            "corpus_load_address_coverage",
            "DELETE FROM address_names_current WHERE namespace = 'ens'",
            "contributed no supported address/name relations",
        ),
        (
            "corpus_load_primary_coverage",
            "DELETE FROM primary_names_current WHERE namespace = 'ens'",
            "contributed no successful primary names",
        ),
    ] {
        let database = seeded_database(label).await;
        sqlx::query(delete_sql)
            .execute(database.pool())
            .await
            .unwrap();

        let error = Corpus::load(database.pool(), &tiny_budgets())
            .await
            .expect_err("missing namespace contribution must be named");
        assert!(error.to_string().contains(expected), "{label}: {error:#}");
        database.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn corpus_load_names_aggregate_size_shortfalls() {
    for (label, delete_sql, expected) in [
        (
            "corpus_load_name_size",
            "DELETE FROM name_current WHERE logical_name_id IN ('basenames:load-1', 'ens:load-1')",
            "name corpus has 2 rows; release profile requires 3",
        ),
        (
            "corpus_load_address_size",
            "DELETE FROM address_names_current WHERE raw_name IN ('basenames-address-1.eth', 'ens-address-1.eth')",
            "address corpus has 2 rows; release profile requires 3",
        ),
    ] {
        let database = seeded_database(label).await;
        sqlx::query(delete_sql)
            .execute(database.pool())
            .await
            .unwrap();
        let mut budgets = tiny_budgets();
        budgets.api_corpus_size = 3;

        let error = Corpus::load(database.pool(), &budgets)
            .await
            .expect_err("aggregate corpus shortfall must be named");
        let error = error.to_string();
        assert!(error.contains(expected), "{label}: {error}");
        assert!(error.contains("basenames=1"), "{label}: {error}");
        assert!(error.contains("ens=1"), "{label}: {error}");
        database.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn corpus_load_keeps_specialized_size_floors_load_bearing() {
    let database = seeded_database("corpus_load_parent_size").await;
    sqlx::query(
        "DELETE FROM children_current
         WHERE parent_logical_name_id IN ('basenames:load-1', 'ens:load-1')",
    )
    .execute(database.pool())
    .await
    .unwrap();
    let mut budgets = tiny_budgets();
    budgets.api_min_specialized_corpus_size = 3;
    let error = Corpus::load(database.pool(), &budgets)
        .await
        .expect_err("subname-parent corpus floor must remain load-bearing");
    assert!(
        error
            .to_string()
            .contains("subname parent corpus has 2 rows; release profile requires 3"),
        "{error:#}"
    );
    database.cleanup().await.unwrap();

    let database = seeded_database("corpus_load_permission_size").await;
    sqlx::query("DELETE FROM permissions_current WHERE subject <> '0x0000000000000000000000000000000000000000'")
        .execute(database.pool())
        .await
        .unwrap();
    let mut budgets = tiny_budgets();
    budgets.api_min_specialized_corpus_size = 2;
    let error = Corpus::load(database.pool(), &budgets)
        .await
        .expect_err("permission-subject corpus floor must remain load-bearing");
    assert!(
        error
            .to_string()
            .contains("permission subject corpus has 1 rows; release profile requires 2"),
        "{error:#}"
    );
    database.cleanup().await.unwrap();

    let database = seeded_database("corpus_load_resolver_size").await;
    let mut budgets = tiny_budgets();
    budgets.api_min_resolver_corpus_size = 2;
    let error = Corpus::load(database.pool(), &budgets)
        .await
        .expect_err("resolver corpus floor must remain load-bearing");
    assert!(
        error
            .to_string()
            .contains("resolver corpus has 1 rows; release profile requires 2"),
        "{error:#}"
    );
    database.cleanup().await.unwrap();

    let database = seeded_database("corpus_load_primary_size").await;
    sqlx::query(
        "DELETE FROM primary_names_current
         WHERE address IN (
             '0x0000000000000000000000000000000000000090',
             '0x0000000000000000000000000000000000000091'
         )",
    )
    .execute(database.pool())
    .await
    .unwrap();
    let mut budgets = tiny_budgets();
    budgets.api_corpus_size = 4;
    budgets.api_min_specialized_corpus_size = 3;
    let error = Corpus::load(database.pool(), &budgets)
        .await
        .expect_err("primary-name corpus floor must remain load-bearing");
    let error = error.to_string();
    assert!(
        error.contains("successful primary-name corpus has 2 rows; release profile requires 3"),
        "{error}"
    );
    assert!(error.contains("basenames=1"), "{error}");
    assert!(error.contains("ens=1"), "{error}");
    database.cleanup().await.unwrap();
}
