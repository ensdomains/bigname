use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::PgPool;
use uuid::Uuid;

use super::{Corpus, require_active_namespace_coverage, require_stratified_corpus_size, tests};
use crate::budgets::{BudgetProfile, BudgetsFile, GateBudgets};

#[derive(Clone)]
struct CheckedInResolverManifest {
    namespace: String,
    chain_id: String,
    source_family: String,
    payload: serde_json::Value,
    addresses: Vec<String>,
}

fn checked_in_mainnet_resolver_manifests() -> Vec<CheckedInResolverManifest> {
    [
        "manifests/mainnet/ethereum/ens/ens_v1_resolver_l1/v1.toml",
        "manifests/mainnet/base/basenames/basenames_base_resolver/v1.toml",
    ]
    .into_iter()
    .map(checked_in_resolver_manifest)
    .collect()
}

fn checked_in_resolver_manifest(relative: &str) -> CheckedInResolverManifest {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = std::fs::read_to_string(root.join(relative)).unwrap();
    let manifest: toml::Value = toml::from_str(&source).unwrap();
    let addresses = manifest["contracts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|contract| contract["address"].as_str().unwrap().to_ascii_lowercase())
        .collect();
    CheckedInResolverManifest {
        namespace: manifest["namespace"].as_str().unwrap().to_owned(),
        chain_id: manifest["chain"].as_str().unwrap().to_owned(),
        source_family: manifest["source_family"].as_str().unwrap().to_owned(),
        payload: serde_json::to_value(manifest).unwrap(),
        addresses,
    }
}

fn tiny_budgets() -> GateBudgets {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benchmarks/release-gate.toml");
    let mut budgets = BudgetsFile::load(&path)
        .unwrap()
        .profile(BudgetProfile::Production)
        .clone();
    budgets.api_corpus_size = 2;
    budgets.api_min_specialized_corpus_size = 1;
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

#[tokio::test]
async fn checked_in_mainnet_resolver_manifest_set_satisfies_coverage() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_mainnet_resolver_manifest_coverage")
            .pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    let manifests = checked_in_mainnet_resolver_manifests();
    let mut address_count = 0;
    for manifest in &manifests {
        insert_resolver_manifest(database.pool(), manifest).await;
        for address in &manifest.addresses {
            insert_resolver_row(
                database.pool(),
                &manifest.chain_id,
                address,
                "supported",
                address_count,
            )
            .await;
            address_count += 1;
        }
    }

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .expect("the complete active mainnet resolver manifest set must satisfy coverage");

    assert_eq!(coverage.resolvers.len(), 8);
    assert!(coverage.failures.is_empty());
    assert_eq!(coverage.counts.len(), 2);
    assert_eq!(
        coverage
            .counts
            .iter()
            .map(|count| (count.source_family.as_str(), count.declared_addresses))
            .collect::<Vec<_>>(),
        [("basenames_base_resolver", 1), ("ens_v1_resolver_l1", 7)]
    );
    database.cleanup().await.unwrap();
}

async fn insert_resolver_manifest(pool: &PgPool, manifest: &CheckedInResolverManifest) {
    sqlx::query(
        "INSERT INTO manifest_versions
             (namespace, rollout_status, source_family, chain_id, manifest_payload)
         VALUES ($1, 'active', $2, $3, $4)",
    )
    .bind(&manifest.namespace)
    .bind(&manifest.source_family)
    .bind(&manifest.chain_id)
    .bind(&manifest.payload)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_resolver_row(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
    support_status: &str,
    index: usize,
) {
    let hash = format!("resolver-manifest-{index}");
    sqlx::query("INSERT INTO chain_lineage VALUES ($1, $2, 'canonical')")
        .bind(chain_id)
        .bind(&hash)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO resolver_current VALUES
             ($1, $2, $3, jsonb_build_object('target_block_hash', $4::text),
              '{\"state\":\"canonical_lineage\"}')",
    )
    .bind(chain_id)
    .bind(address)
    .bind(support_status)
    .bind(hash)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn missing_unsupported_or_invisible_declared_resolver_is_named() {
    for (case, expected_message) in [
        ("missing", "is missing from resolver_current"),
        ("unsupported", "not supported, in resolver_current"),
        (
            "invisible",
            "is not API-visible through canonical projection lineage",
        ),
    ] {
        let database = TestDatabase::create(
            TestDatabaseConfig::new(format!("benchmark_resolver_coverage_{case}"))
                .pool_max_connections(1),
        )
        .await
        .unwrap();
        tests::install_name_visibility_schema(database.pool()).await;
        let manifests = checked_in_mainnet_resolver_manifests();
        for manifest in &manifests {
            insert_resolver_manifest(database.pool(), manifest).await;
        }
        let missing_address = manifests
            .iter()
            .find(|manifest| manifest.source_family == "ens_v1_resolver_l1")
            .unwrap()
            .addresses[0]
            .clone();
        let mut index = 0;
        for manifest in &manifests {
            for address in &manifest.addresses {
                if case == "missing" && address == &missing_address {
                    continue;
                }
                let support_status = if case == "unsupported" && address == &missing_address {
                    "unsupported"
                } else {
                    "supported"
                };
                insert_resolver_row(
                    database.pool(),
                    &manifest.chain_id,
                    address,
                    support_status,
                    index,
                )
                .await;
                if case == "invisible" && address == &missing_address {
                    sqlx::query(
                        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
                         WHERE block_hash = $1",
                    )
                    .bind(format!("resolver-manifest-{index}"))
                    .execute(database.pool())
                    .await
                    .unwrap();
                }
                index += 1;
            }
        }
        insert_resolver_row(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000ff",
            "supported",
            100,
        )
        .await;

        let coverage = super::resolver_coverage::load(database.pool())
            .await
            .expect("resolver coverage refusal must remain available for the JSON report");
        let error = coverage.failures.join("; ");

        assert_eq!(coverage.resolvers.len(), 7, "{case}");
        assert!(error.contains(&missing_address), "{case}: {error}");
        assert!(
            error.contains("chain \"ethereum-mainnet\""),
            "{case}: {error}"
        );
        assert!(
            error.contains("family \"ens_v1_resolver_l1\""),
            "{case}: {error}"
        );
        assert!(error.contains(expected_message), "{case}: {error}");
        database.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn active_resolver_family_without_contracts_is_reportable_as_zero() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_zero_resolver_manifest_coverage")
            .pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    let manifest =
        checked_in_resolver_manifest("manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v2.toml");
    insert_resolver_manifest(database.pool(), &manifest).await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(coverage.resolvers.is_empty());
    assert_eq!(coverage.counts.len(), 1);
    assert_eq!(coverage.counts[0].source_family, "ens_v2_resolver_l1");
    assert_eq!(coverage.counts[0].declared_addresses, 0);
    assert_eq!(coverage.counts[0].exercised_addresses, 0);
    assert!(coverage.failures[0].contains("zero supported, API-visible"));
    database.cleanup().await.unwrap();
}

async fn seeded_database(label: &str) -> TestDatabase {
    let database = TestDatabase::create(TestDatabaseConfig::new(label).pool_max_connections(1))
        .await
        .unwrap();
    let pool = database.pool();
    tests::install_name_visibility_schema(pool).await;
    sqlx::query(
        "INSERT INTO manifest_versions (namespace, rollout_status) VALUES
             ('basenames', 'active'), ('ens', 'active')",
    )
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
        "INSERT INTO manifest_versions
             (namespace, rollout_status, source_family, chain_id, manifest_payload)
         VALUES (
             'ens', 'active', 'ens_v1_resolver_l1', 'ethereum-mainnet',
             '{\"contracts\":[{\"address\":\"0x0000000000000000000000000000000000000082\"}]}'
         )",
    )
    .execute(pool)
    .await
    .unwrap();
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
