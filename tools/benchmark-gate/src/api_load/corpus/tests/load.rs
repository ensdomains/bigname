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
    insert_project_head(database.pool(), "ethereum-mainnet", 30_000_000).await;
    insert_project_head(database.pool(), "base-mainnet", 30_000_000).await;
    let manifests = checked_in_mainnet_resolver_manifests();
    let mut address_count = 0;
    for manifest in &manifests {
        insert_resolver_manifest(database.pool(), manifest).await;
        for address in &manifest.addresses {
            insert_resolver_row_at(
                database.pool(),
                &manifest.chain_id,
                address,
                "supported",
                address_count,
                29_000_000 + address_count as i64,
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
    assert!(
        coverage
            .counts
            .iter()
            .all(|count| count.exercised_addresses == 0),
        "loading an admitted corpus must not claim that requests were constructed"
    );
    assert!(
        coverage
            .counts
            .iter()
            .all(|count| count.applicable_addresses == count.declared_addresses)
    );
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
    ensure_project_state_schema(pool).await;
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions
             (namespace, rollout_status, source_family, chain_id, manifest_payload)
         VALUES ($1, 'active', $2, $3, $4)
         RETURNING manifest_id",
    )
    .bind(&manifest.namespace)
    .bind(&manifest.source_family)
    .bind(&manifest.chain_id)
    .bind(&manifest.payload)
    .fetch_one(pool)
    .await
    .unwrap();
    insert_manifest_event(pool, manifest_id, manifest, &manifest.payload).await;
}

async fn insert_manifest_event(
    pool: &PgPool,
    manifest_id: i64,
    manifest: &CheckedInResolverManifest,
    projected_payload: &serde_json::Value,
) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO normalized_events
             (event_identity, namespace, event_kind, source_family,
              manifest_version, source_manifest_id, chain_id,
              canonicality_state, after_state)
         VALUES ($1, $2, 'SourceManifestUpdated', $3, 1, $4, $5,
                 'finalized', jsonb_build_object(
                     'rollout_status', 'active',
                     'manifest_version', 1,
                     'normalizer_version', 'ensip15@ens-normalize-0.1.1',
                     'manifest_payload', $6::jsonb
                 ))
         RETURNING normalized_event_id",
    )
    .bind(format!("manifest-event-{manifest_id}-{}", Uuid::new_v4()))
    .bind(&manifest.namespace)
    .bind(&manifest.source_family)
    .bind(manifest_id)
    .bind(&manifest.chain_id)
    .bind(projected_payload)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn insert_resolver_row(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
    support_status: &str,
    index: usize,
) {
    insert_resolver_row_at(pool, chain_id, address, support_status, index, index as i64).await;
}

async fn insert_resolver_row_at(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
    support_status: &str,
    index: usize,
    target_block_number: i64,
) {
    let hash = format!("resolver-manifest-{index}");
    sqlx::query(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, canonicality_state, block_number)
         VALUES ($1, $2, 'canonical', $3)",
    )
    .bind(chain_id)
    .bind(&hash)
    .bind(target_block_number)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resolver_current
             (chain_id, resolver_address, support_status, chain_positions,
              canonicality_summary, provenance, manifest_version)
         SELECT $1, $2, $3,
                jsonb_build_object('target_block_number', $5::bigint,
                                   'target_block_hash', $4::text),
                '{\"state\":\"canonical_lineage\"}',
                jsonb_build_object(
                    'manifest_id', manifest_id,
                    'manifest_event_id', (
                        SELECT max(event.normalized_event_id)
                        FROM normalized_events event
                        WHERE event.source_manifest_id = manifest_id
                          AND event.event_kind = 'SourceManifestUpdated'
                    )
                ), manifest_version
         FROM manifest_versions
         WHERE chain_id = $1 AND rollout_status = 'active'
           AND EXISTS (
               SELECT 1
               FROM jsonb_array_elements(manifest_payload -> 'contracts') contract
               WHERE lower(contract ->> 'address') = lower($2)
           )
         LIMIT 1",
    )
    .bind(chain_id)
    .bind(address)
    .bind(support_status)
    .bind(hash)
    .bind(target_block_number)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_upgrade_event(
    pool: &PgPool,
    manifest: &CheckedInResolverManifest,
    proxy_address: &str,
    implementation: &str,
    block_number: i64,
) -> i64 {
    let block_hash = format!("resolver-upgrade-{block_number}-{proxy_address}");
    sqlx::query(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, canonicality_state, block_number)
         VALUES ($1, $2, 'canonical', $3)",
    )
    .bind(&manifest.chain_id)
    .bind(&block_hash)
    .bind(block_number)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query_scalar(
        "INSERT INTO normalized_events
             (event_identity, namespace, event_kind, source_family,
              manifest_version, source_manifest_id, chain_id, block_number,
              block_hash, transaction_index, log_index, canonicality_state,
              after_state)
         SELECT $1, namespace, 'Upgraded', source_family, manifest_version,
                manifest_id, chain_id, $2, $3, 0, 0, 'canonical',
                jsonb_build_object('proxy_address', $4::text,
                                   'implementation', $5::text)
         FROM manifest_versions
         WHERE chain_id = $6 AND source_family = $7
         LIMIT 1
         RETURNING normalized_event_id",
    )
    .bind(format!("upgrade-{}", Uuid::new_v4()))
    .bind(block_number)
    .bind(block_hash)
    .bind(proxy_address)
    .bind(implementation)
    .bind(&manifest.chain_id)
    .bind(&manifest.source_family)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn insert_implementation_resolver_row(
    pool: &PgPool,
    manifest: &CheckedInResolverManifest,
    proxy_address: &str,
    target_block_number: i64,
) {
    let hash = format!("implementation-resolver-{target_block_number}");
    sqlx::query(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, canonicality_state, block_number)
         VALUES ($1, $2, 'canonical', $3)",
    )
    .bind(&manifest.chain_id)
    .bind(&hash)
    .bind(target_block_number)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO resolver_current
             (chain_id, resolver_address, support_status, chain_positions,
              canonicality_summary, provenance, manifest_version)
         SELECT chain_id, $1, 'supported',
                jsonb_build_object('target_block_number', $2::bigint,
                                   'target_block_hash', $3::text,
                                   'block_number', (
                                       SELECT event.block_number
                                       FROM normalized_events event
                                       WHERE event.source_manifest_id = manifest_id
                                         AND event.event_kind = 'Upgraded'
                                       ORDER BY event.normalized_event_id DESC
                                       LIMIT 1
                                   ),
                                   'block_hash', (
                                       SELECT event.block_hash
                                       FROM normalized_events event
                                       WHERE event.source_manifest_id = manifest_id
                                         AND event.event_kind = 'Upgraded'
                                       ORDER BY event.normalized_event_id DESC
                                       LIMIT 1
                                   )),
                '{\"state\":\"canonical_lineage\"}',
                jsonb_build_object(
                    'manifest_id', manifest_id,
                    'manifest_event_id', (
                        SELECT max(event.normalized_event_id)
                        FROM normalized_events event
                        WHERE event.source_manifest_id = manifest_id
                          AND event.event_kind = 'SourceManifestUpdated'
                    ),
                    'upgrade_event_id', (
                        SELECT max(event.normalized_event_id)
                        FROM normalized_events event
                        WHERE event.source_manifest_id = manifest_id
                          AND event.event_kind = 'Upgraded'
                    )
                ), manifest_version
         FROM manifest_versions
         WHERE chain_id = $4 AND source_family = $5
         LIMIT 1",
    )
    .bind(proxy_address)
    .bind(target_block_number)
    .bind(hash)
    .bind(&manifest.chain_id)
    .bind(&manifest.source_family)
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_undeclared_resolver_row(pool: &PgPool, chain_id: &str, address: &str) {
    sqlx::query(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, canonicality_state, block_number)
         VALUES ($1, 'undeclared-resolver', 'canonical', 29000000)",
    )
    .bind(chain_id)
    .execute(pool)
    .await
    .unwrap();
    let inserted = sqlx::query(
        "INSERT INTO resolver_current
             (chain_id, resolver_address, support_status, chain_positions,
              canonicality_summary, provenance, manifest_version)
         SELECT $1, $2, 'supported',
                '{\"target_block_number\":29000000,\"target_block_hash\":\"undeclared-resolver\"}',
                '{\"state\":\"canonical_lineage\"}',
                jsonb_build_object('manifest_id', manifest_id), manifest_version
         FROM manifest_versions
         WHERE chain_id = $1 AND rollout_status = 'active'
         ORDER BY manifest_id
         LIMIT 1",
    )
    .bind(chain_id)
    .bind(address)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(inserted.rows_affected(), 1);
}

#[tokio::test]
async fn missing_unsupported_or_invisible_declared_resolver_is_named() {
    for (case, expected_message) in [
        ("missing", "is missing from resolver_current"),
        ("unsupported", "not supported, in resolver_current"),
        (
            "invisible",
            "fails the resolver benchmark's canonical-read or chain-anchor integrity checks",
        ),
    ] {
        let database = TestDatabase::create(
            TestDatabaseConfig::new(format!("benchmark_resolver_coverage_{case}"))
                .pool_max_connections(1),
        )
        .await
        .unwrap();
        tests::install_name_visibility_schema(database.pool()).await;
        insert_project_head(database.pool(), "ethereum-mainnet", 30_000_000).await;
        insert_project_head(database.pool(), "base-mainnet", 30_000_000).await;
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
                insert_resolver_row_at(
                    database.pool(),
                    &manifest.chain_id,
                    address,
                    support_status,
                    index,
                    29_000_000 + index as i64,
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
        insert_undeclared_resolver_row(
            database.pool(),
            "ethereum-mainnet",
            "0x00000000000000000000000000000000000000ff",
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
async fn ens_v2_implementation_upgrade_derives_the_resolver_corpus() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_ens_v2_implementation_coverage").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    let manifest =
        checked_in_resolver_manifest("manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v2.toml");
    insert_project_head(database.pool(), &manifest.chain_id, 1_000).await;
    insert_resolver_manifest(database.pool(), &manifest).await;
    let implementation = manifest.payload["resolver_implementations"][0]["address"]
        .as_str()
        .unwrap();
    let proxy = "0x0000000000000000000000000000000000000200";
    insert_upgrade_event(database.pool(), &manifest, proxy, implementation, 900).await;
    insert_implementation_resolver_row(database.pool(), &manifest, proxy, 950).await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(coverage.failures.is_empty(), "{:?}", coverage.failures);
    assert_eq!(coverage.resolvers.len(), 1);
    assert_eq!(coverage.resolvers[0].resolver_address, proxy);
    assert_eq!(coverage.counts.len(), 1);
    assert_eq!(coverage.counts[0].source_family, "ens_v2_resolver_l1");
    assert_eq!(coverage.counts[0].declared_addresses, 1);
    assert_eq!(coverage.counts[0].applicable_addresses, 1);
    assert_eq!(coverage.counts[0].exercised_addresses, 0);
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn ens_v2_coverage_binds_the_upgrade_anchor() {
    for (case, mutation) in [
        (
            "target_before_upgrade",
            "UPDATE resolver_current SET chain_positions = chain_positions || jsonb_build_object('target_block_number', 899, 'target_block_hash', 'before-upgrade-target')",
        ),
        (
            "number",
            "UPDATE resolver_current SET chain_positions = jsonb_set(chain_positions, '{block_number}', '899'::jsonb)",
        ),
        (
            "hash",
            "UPDATE resolver_current SET chain_positions = jsonb_set(chain_positions, '{block_hash}', '\"wrong-upgrade\"'::jsonb)",
        ),
    ] {
        let database = TestDatabase::create(
            TestDatabaseConfig::new(format!("benchmark_ens_v2_upgrade_anchor_{case}"))
                .pool_max_connections(1),
        )
        .await
        .unwrap();
        tests::install_name_visibility_schema(database.pool()).await;
        let manifest = checked_in_resolver_manifest(
            "manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v2.toml",
        );
        insert_project_head(database.pool(), &manifest.chain_id, 1_000).await;
        insert_resolver_manifest(database.pool(), &manifest).await;
        let implementation = manifest.payload["resolver_implementations"][0]["address"]
            .as_str()
            .unwrap();
        let proxy = "0x0000000000000000000000000000000000000200";
        insert_upgrade_event(database.pool(), &manifest, proxy, implementation, 900).await;
        insert_implementation_resolver_row(database.pool(), &manifest, proxy, 950).await;
        if case == "target_before_upgrade" {
            sqlx::query(
                "INSERT INTO chain_lineage
                     (chain_id, block_hash, canonicality_state, block_number)
                 VALUES ($1, 'before-upgrade-target', 'canonical', 899)",
            )
            .bind(&manifest.chain_id)
            .execute(database.pool())
            .await
            .unwrap();
        }
        sqlx::query(mutation)
            .execute(database.pool())
            .await
            .unwrap();

        let coverage = super::resolver_coverage::load(database.pool())
            .await
            .unwrap();

        assert!(
            coverage.failures.iter().any(|failure| failure.contains(
                "fails the resolver benchmark's canonical-read or chain-anchor integrity checks"
            )),
            "{case}: {:?}",
            coverage.failures
        );
        database.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn ens_v2_admission_does_not_fall_back_to_concrete_contracts() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_ens_v2_no_contract_fallback").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-sepolia", 1_000).await;
    let resolver = "0x0000000000000000000000000000000000000200";
    let manifest = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-sepolia".to_owned(),
        source_family: "ens_v2_resolver_l1".to_owned(),
        payload: serde_json::json!({
            "contracts": [{"address": resolver}],
            "resolver_implementations": []
        }),
        addresses: vec![resolver.to_owned()],
    };
    insert_resolver_manifest(database.pool(), &manifest).await;
    insert_resolver_row(
        database.pool(),
        &manifest.chain_id,
        resolver,
        "supported",
        1,
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(coverage.resolvers.is_empty());
    assert!(
        coverage.failures.iter().any(|failure| {
            failure.contains("family \"ens_v2_resolver_l1\"")
                && failure.contains("zero currently applicable")
        }),
        "{:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn malformed_ens_v2_implementation_metadata_stays_reportable() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_malformed_ens_v2_implementation_metadata")
            .pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-sepolia", 1_000).await;
    let manifest = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-sepolia".to_owned(),
        source_family: "ens_v2_resolver_l1".to_owned(),
        payload: serde_json::json!({
            "contracts": [],
            "resolver_implementations": null
        }),
        addresses: Vec::new(),
    };
    insert_resolver_manifest(database.pool(), &manifest).await;
    insert_upgrade_event(
        database.pool(),
        &manifest,
        "0x0000000000000000000000000000000000000200",
        "0x0000000000000000000000000000000000000999",
        900,
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .expect("malformed implementation metadata must remain available in the red report");

    assert!(
        coverage.failures.iter().any(
            |failure| failure.contains("resolver_implementations is absent or is not an array")
        ),
        "{:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn non_ens_v2_admission_ignores_incidental_implementation_metadata() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_non_ens_v2_contract_admission").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-mainnet", 1_000).await;
    let resolver = "0x0000000000000000000000000000000000000100";
    let manifest = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        payload: serde_json::json!({
            "contracts": [{"address": resolver}],
            "resolver_implementations": [
                {"address": "0x0000000000000000000000000000000000000999"}
            ]
        }),
        addresses: vec![resolver.to_owned()],
    };
    insert_resolver_manifest(database.pool(), &manifest).await;
    insert_resolver_row(
        database.pool(),
        &manifest.chain_id,
        resolver,
        "supported",
        1,
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(coverage.failures.is_empty(), "{:?}", coverage.failures);
    assert_eq!(coverage.resolvers.len(), 1);
    assert_eq!(coverage.resolvers[0].resolver_address, resolver);
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn every_active_resolver_family_must_contribute_a_workload_target() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_per_family_resolver_coverage").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-mainnet", 1_000).await;
    insert_project_head(database.pool(), "ethereum-sepolia", 1_000).await;
    let ens_v1 = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        payload: serde_json::json!({
            "contracts": [{"address": "0x0000000000000000000000000000000000000100"}]
        }),
        addresses: vec!["0x0000000000000000000000000000000000000100".to_owned()],
    };
    let ens_v2 =
        checked_in_resolver_manifest("manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v2.toml");
    insert_resolver_manifest(database.pool(), &ens_v1).await;
    insert_resolver_manifest(database.pool(), &ens_v2).await;
    insert_resolver_row(
        database.pool(),
        &ens_v1.chain_id,
        &ens_v1.addresses[0],
        "supported",
        1,
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert_eq!(coverage.resolvers.len(), 1);
    assert!(
        coverage.failures.iter().any(|failure| {
            failure.contains("family \"ens_v2_resolver_l1\"")
                && failure.contains("zero currently applicable")
        }),
        "{:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn stored_payload_must_match_the_latest_projected_manifest_event() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_manifest_event_payload_binding").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-mainnet", 1_000).await;
    let retained = "0x0000000000000000000000000000000000000100";
    let removed = "0x0000000000000000000000000000000000000101";
    let stored_b = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        payload: serde_json::json!({"contracts": [{"address": retained}]}),
        addresses: vec![retained.to_owned()],
    };
    insert_resolver_manifest(database.pool(), &stored_b).await;
    let manifest_id: i64 =
        sqlx::query_scalar("SELECT manifest_id FROM manifest_versions WHERE source_family = $1")
            .bind(&stored_b.source_family)
            .fetch_one(database.pool())
            .await
            .unwrap();
    let projected_a = serde_json::json!({
        "contracts": [{"address": retained}, {"address": removed}]
    });
    // A final A -> B event is content-identical to the first transition and is
    // swallowed by manifest sync, so Project's newest persisted event remains B -> A.
    insert_manifest_event(database.pool(), manifest_id, &stored_b, &projected_a).await;
    insert_resolver_row(
        database.pool(),
        &stored_b.chain_id,
        retained,
        "supported",
        1,
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(
        coverage.failures.iter().any(|failure| failure
            .contains("stored active payload diverges from the latest projected manifest event")),
        "{:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn manifest_binding_reports_stored_and_event_versions_accurately() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_manifest_event_version_binding").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-mainnet", 100).await;
    let resolver = "0x0000000000000000000000000000000000000100";
    let manifest = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        payload: serde_json::json!({"contracts": [{"address": resolver}]}),
        addresses: vec![resolver.to_owned()],
    };
    insert_resolver_manifest(database.pool(), &manifest).await;
    sqlx::query("UPDATE manifest_versions SET manifest_version = 2")
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE normalized_events
         SET manifest_version = 3,
             after_state = jsonb_set(after_state, '{manifest_version}', '3'::jsonb)",
    )
    .execute(database.pool())
    .await
    .unwrap();

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(
        coverage.failures.iter().any(|failure| {
            failure.contains("active stored resolver manifest version 2")
                && failure.contains("latest Project event version 3")
        }),
        "{:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn manifest_normalizer_version_must_match_the_latest_projected_event() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_manifest_normalizer_binding").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-mainnet", 100).await;
    let resolver = "0x0000000000000000000000000000000000000100";
    let manifest = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        payload: serde_json::json!({"contracts": [{"address": resolver}]}),
        addresses: vec![resolver.to_owned()],
    };
    insert_resolver_manifest(database.pool(), &manifest).await;
    insert_resolver_row(
        database.pool(),
        &manifest.chain_id,
        resolver,
        "supported",
        1,
    )
    .await;
    let matching = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();
    assert!(matching.failures.is_empty(), "{:?}", matching.failures);

    sqlx::query(
        "UPDATE normalized_events
         SET after_state = jsonb_set(
             after_state,
             '{normalizer_version}',
             to_jsonb('ensip15@future-normalizer'::text)
         )",
    )
    .execute(database.pool())
    .await
    .unwrap();
    let divergent = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();
    assert!(
        divergent
            .failures
            .iter()
            .any(|failure| { failure.contains("stored active normalizer_version diverges") }),
        "{:?}",
        divergent.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn manifest_event_chain_and_family_must_match_the_stored_manifest() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_manifest_event_identity_binding")
            .pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-mainnet", 100).await;
    insert_project_head(database.pool(), "base-mainnet", 100).await;
    let resolver = "0x0000000000000000000000000000000000000100";
    let manifest = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        payload: serde_json::json!({"contracts": [{"address": resolver}]}),
        addresses: vec![resolver.to_owned()],
    };
    insert_resolver_manifest(database.pool(), &manifest).await;
    sqlx::query(
        "UPDATE normalized_events
         SET namespace = 'basenames',
             chain_id = 'base-mainnet',
             source_family = 'basenames_base_resolver'",
    )
    .execute(database.pool())
    .await
    .unwrap();

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(
        coverage
            .failures
            .iter()
            .any(|failure| failure.contains("stored active manifest identity diverges")),
        "{:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn latest_manifest_event_is_selected_before_resolver_family_scoping() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_manifest_latest_event_family").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-mainnet", 100).await;
    let resolver = "0x0000000000000000000000000000000000000100";
    let manifest = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        payload: serde_json::json!({"contracts": [{"address": resolver}]}),
        addresses: vec![resolver.to_owned()],
    };
    insert_resolver_manifest(database.pool(), &manifest).await;
    insert_resolver_row(
        database.pool(),
        &manifest.chain_id,
        resolver,
        "supported",
        1,
    )
    .await;
    let manifest_id: i64 =
        sqlx::query_scalar("SELECT manifest_id FROM manifest_versions WHERE source_family = $1")
            .bind(&manifest.source_family)
            .fetch_one(database.pool())
            .await
            .unwrap();
    let newer_non_resolver = CheckedInResolverManifest {
        source_family: "ens_v1_registry_l1".to_owned(),
        ..manifest.clone()
    };
    insert_manifest_event(
        database.pool(),
        manifest_id,
        &newer_non_resolver,
        &newer_non_resolver.payload,
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(
        coverage
            .failures
            .iter()
            .any(|failure| failure.contains("stored active manifest identity diverges")),
        "the resolver-family filter selected an older event than the projection phase: {:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn resolver_rows_bind_to_the_latest_manifest_event() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_resolver_manifest_event_binding")
            .pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-mainnet", 100).await;
    let resolver = "0x0000000000000000000000000000000000000100";
    let manifest = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        payload: serde_json::json!({"contracts": [{"address": resolver}]}),
        addresses: vec![resolver.to_owned()],
    };
    insert_resolver_manifest(database.pool(), &manifest).await;
    let manifest_id: i64 =
        sqlx::query_scalar("SELECT manifest_id FROM manifest_versions WHERE source_family = $1")
            .bind(&manifest.source_family)
            .fetch_one(database.pool())
            .await
            .unwrap();
    insert_resolver_row(
        database.pool(),
        &manifest.chain_id,
        resolver,
        "supported",
        1,
    )
    .await;
    let normal = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();
    assert!(normal.failures.is_empty(), "{:?}", normal.failures);

    sqlx::query(
        "UPDATE resolver_current
         SET provenance = jsonb_set(provenance, '{manifest_event_id}', '999999'::jsonb)",
    )
    .execute(database.pool())
    .await
    .unwrap();
    let mismatched = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();
    assert!(
        mismatched.failures.iter().any(|failure| {
            failure.contains("does not cite latest projected manifest event")
                && failure.contains(&format!("manifest {manifest_id}"))
                && failure.contains("stored version 1")
                && failure.contains("latest event version 1")
        }),
        "{:?}",
        mismatched.failures
    );
    database.cleanup().await.unwrap();
}

async fn insert_project_head(pool: &PgPool, chain_id: &str, block_number: i64) {
    ensure_project_state_schema(pool).await;
    sqlx::query(
        "INSERT INTO chain_phase_state
             (chain_id, phase_name, phase_status, current_block_number,
              current_block_hash, input_content_hash)
         VALUES ($1, 'project', 'completed', $2, $3, $4)",
    )
    .bind(chain_id)
    .bind(block_number)
    .bind(format!("project-head-{block_number}"))
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO chain_heads
             (chain_id, latest_block_number, latest_block_hash)
         VALUES ($1, $2, $3)",
    )
    .bind(chain_id)
    .bind(block_number)
    .bind(format!("project-head-{block_number}"))
    .execute(pool)
    .await
    .unwrap();
}

async fn ensure_project_state_schema(pool: &PgPool) {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS chain_phase_state (
             chain_id text NOT NULL,
             phase_name text NOT NULL,
             phase_status text NOT NULL,
             current_block_number bigint,
             current_block_hash text,
             input_content_hash text,
             PRIMARY KEY (chain_id, phase_name)
         )",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS chain_heads (
             chain_id text PRIMARY KEY,
             latest_block_number bigint NOT NULL,
             latest_block_hash text NOT NULL
         )",
    )
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn future_resolver_declarations_are_reported_but_not_demanded() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_future_resolver_declaration").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-mainnet", 101).await;
    let admitted = "0x0000000000000000000000000000000000000100";
    let future = "0x0000000000000000000000000000000000000101";
    insert_resolver_manifest(
        database.pool(),
        &CheckedInResolverManifest {
            namespace: "ens".to_owned(),
            chain_id: "ethereum-mainnet".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            payload: serde_json::json!({
                "contracts": [
                    {"address": admitted, "start_block": 100},
                    {"address": future, "start_block": 102}
                ]
            }),
            addresses: vec![admitted.to_owned(), future.to_owned()],
        },
    )
    .await;
    insert_resolver_row(
        database.pool(),
        "ethereum-mainnet",
        admitted,
        "supported",
        100,
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(coverage.failures.is_empty(), "{:?}", coverage.failures);
    assert_eq!(coverage.resolvers.len(), 1);
    assert_eq!(coverage.counts.len(), 1);
    assert_eq!(coverage.counts[0].declared_addresses, 2);
    assert_eq!(coverage.counts[0].applicable_addresses, 1);
    assert_eq!(coverage.counts[0].exercised_addresses, 0);
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn duplicate_resolver_roles_count_one_declared_address() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_duplicate_resolver_roles").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-mainnet", 101).await;
    let resolver = "0x0000000000000000000000000000000000000100";
    insert_resolver_manifest(
        database.pool(),
        &CheckedInResolverManifest {
            namespace: "ens".to_owned(),
            chain_id: "ethereum-mainnet".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            payload: serde_json::json!({
                "contracts": [
                    {"role": "resolver", "address": resolver, "start_block": 100},
                    {"role": "legacy_resolver", "address": resolver, "start_block": 102}
                ]
            }),
            addresses: vec![resolver.to_owned()],
        },
    )
    .await;
    insert_resolver_row(
        database.pool(),
        "ethereum-mainnet",
        resolver,
        "supported",
        100,
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(coverage.failures.is_empty(), "{:?}", coverage.failures);
    assert_eq!(coverage.resolvers.len(), 1);
    assert_eq!(coverage.counts[0].declared_addresses, 1);
    assert_eq!(coverage.counts[0].applicable_addresses, 1);

    sqlx::raw_sql(
        "UPDATE chain_phase_state
         SET current_block_number = 103, current_block_hash = 'project-head-103';
         UPDATE chain_heads
         SET latest_block_number = 103, latest_block_hash = 'project-head-103'",
    )
    .execute(database.pool())
    .await
    .unwrap();
    let advanced = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();
    assert!(
        advanced.failures.iter().any(|failure| failure.contains(
            "fails the resolver benchmark's canonical-read or chain-anchor integrity checks",
        )),
        "{:?}",
        advanced.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn future_only_resolver_declarations_name_the_missing_workload() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_future_only_resolver_declaration")
            .pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_project_head(database.pool(), "ethereum-mainnet", 100).await;
    let future = "0x0000000000000000000000000000000000000101";
    insert_resolver_manifest(
        database.pool(),
        &CheckedInResolverManifest {
            namespace: "ens".to_owned(),
            chain_id: "ethereum-mainnet".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            payload: serde_json::json!({
                "contracts": [{"address": future, "start_block": 101}]
            }),
            addresses: vec![future.to_owned()],
        },
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(coverage.resolvers.is_empty());
    assert_eq!(coverage.counts[0].declared_addresses, 1);
    assert_eq!(coverage.counts[0].applicable_addresses, 0);
    assert!(
        coverage.failures.iter().any(|failure| failure
            .contains("zero currently applicable, supported, API-visible resolver addresses")),
        "{:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn resolver_declarations_require_a_current_project_head() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_resolver_missing_project_head").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    let ens_resolver = "0x0000000000000000000000000000000000000100";
    insert_resolver_manifest(
        database.pool(),
        &CheckedInResolverManifest {
            namespace: "ens".to_owned(),
            chain_id: "ethereum-mainnet".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            payload: serde_json::json!({
                "contracts": [{"address": ens_resolver, "start_block": 100}]
            }),
            addresses: vec![ens_resolver.to_owned()],
        },
    )
    .await;

    let base_resolver = "0x0000000000000000000000000000000000000200";
    insert_resolver_manifest(
        database.pool(),
        &CheckedInResolverManifest {
            namespace: "basenames".to_owned(),
            chain_id: "base-mainnet".to_owned(),
            source_family: "basenames_base_resolver".to_owned(),
            payload: serde_json::json!({"contracts": [{"address": base_resolver}]}),
            addresses: vec![base_resolver.to_owned()],
        },
    )
    .await;
    insert_project_head(database.pool(), "base-mainnet", 100).await;
    insert_resolver_row(
        database.pool(),
        "base-mainnet",
        base_resolver,
        "supported",
        99,
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(
        coverage.failures.iter().any(|failure| failure.contains(
            "chain \"ethereum-mainnet\" in family \"ens_v1_resolver_l1\" has concrete declarations but no current Project head"
        )),
        "{:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn resolver_coverage_requires_a_current_project_publication() {
    for (case, mutation) in [
        (
            "running",
            "UPDATE chain_phase_state SET phase_status = 'running'",
        ),
        (
            "stale_number",
            "UPDATE chain_heads SET latest_block_number = latest_block_number + 1",
        ),
        (
            "stale_hash",
            "UPDATE chain_heads SET latest_block_hash = 'different-head'",
        ),
        (
            "invalidated_input",
            "UPDATE chain_phase_state SET input_content_hash = 'different-generation'",
        ),
    ] {
        let database = TestDatabase::create(
            TestDatabaseConfig::new(format!("benchmark_resolver_project_{case}"))
                .pool_max_connections(1),
        )
        .await
        .unwrap();
        tests::install_name_visibility_schema(database.pool()).await;
        let resolver = "0x0000000000000000000000000000000000000100";
        insert_resolver_manifest(
            database.pool(),
            &CheckedInResolverManifest {
                namespace: "ens".to_owned(),
                chain_id: "ethereum-mainnet".to_owned(),
                source_family: "ens_v1_resolver_l1".to_owned(),
                payload: serde_json::json!({
                    "contracts": [{"address": resolver, "start_block": 100}]
                }),
                addresses: vec![resolver.to_owned()],
            },
        )
        .await;
        insert_project_head(database.pool(), "ethereum-mainnet", 100).await;
        insert_resolver_row(
            database.pool(),
            "ethereum-mainnet",
            resolver,
            "supported",
            200,
        )
        .await;
        sqlx::query(mutation)
            .execute(database.pool())
            .await
            .unwrap();

        let coverage = super::resolver_coverage::load(database.pool())
            .await
            .unwrap();

        assert!(
            coverage.failures.iter().any(|failure| failure.contains(
                "chain \"ethereum-mainnet\" in family \"ens_v1_resolver_l1\" has concrete declarations but no current Project head"
            )),
            "{case}: {:?}",
            coverage.failures
        );
        database.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn resolver_coverage_uses_the_route_snapshot_bounds() {
    for (case, mutation) in [
        (
            "missing_number",
            "UPDATE resolver_current SET chain_positions = chain_positions - 'target_block_number'",
        ),
        (
            "ahead",
            "UPDATE resolver_current SET chain_positions = jsonb_set(chain_positions, '{target_block_number}', '101'::jsonb)",
        ),
        (
            "same_height_wrong_hash",
            "UPDATE resolver_current SET chain_positions = jsonb_build_object('target_block_number', 100, 'target_block_hash', 'other-canonical-head')",
        ),
        (
            "lineage_number_mismatch",
            "UPDATE resolver_current SET chain_positions = jsonb_set(chain_positions, '{target_block_number}', '50'::jsonb)",
        ),
        (
            "predates_declaration",
            "UPDATE resolver_current SET chain_positions = jsonb_build_object('target_block_number', 99, 'target_block_hash', 'older-canonical-head')",
        ),
        (
            "wrong_manifest",
            "UPDATE resolver_current SET provenance = '{\"manifest_id\":999}'::jsonb",
        ),
        (
            "wrong_manifest_version",
            "UPDATE resolver_current SET manifest_version = manifest_version + 1",
        ),
    ] {
        let database = TestDatabase::create(
            TestDatabaseConfig::new(format!("benchmark_resolver_snapshot_{case}"))
                .pool_max_connections(1),
        )
        .await
        .unwrap();
        tests::install_name_visibility_schema(database.pool()).await;
        let resolver = "0x0000000000000000000000000000000000000100";
        insert_resolver_manifest(
            database.pool(),
            &CheckedInResolverManifest {
                namespace: "ens".to_owned(),
                chain_id: "ethereum-mainnet".to_owned(),
                source_family: "ens_v1_resolver_l1".to_owned(),
                payload: serde_json::json!({
                    "contracts": [{"address": resolver, "start_block": 100}]
                }),
                addresses: vec![resolver.to_owned()],
            },
        )
        .await;
        insert_project_head(database.pool(), "ethereum-mainnet", 100).await;
        insert_resolver_row(
            database.pool(),
            "ethereum-mainnet",
            resolver,
            "supported",
            100,
        )
        .await;
        if case == "same_height_wrong_hash" {
            sqlx::query(
                "INSERT INTO chain_lineage
                     (chain_id, block_hash, canonicality_state, block_number)
                 VALUES
                     ('ethereum-mainnet', 'other-canonical-head', 'canonical', 100)",
            )
            .execute(database.pool())
            .await
            .unwrap();
        }
        if case == "predates_declaration" {
            sqlx::query(
                "INSERT INTO chain_lineage
                     (chain_id, block_hash, canonicality_state, block_number)
                 VALUES
                     ('ethereum-mainnet', 'older-canonical-head', 'canonical', 99)",
            )
            .execute(database.pool())
            .await
            .unwrap();
        }
        sqlx::query(mutation)
            .execute(database.pool())
            .await
            .unwrap();

        let coverage = super::resolver_coverage::load(database.pool())
            .await
            .unwrap();

        let expected = if case == "wrong_manifest" {
            "does not cite latest projected manifest event"
        } else {
            "fails the resolver benchmark's canonical-read or chain-anchor integrity checks"
        };
        assert!(
            coverage
                .failures
                .iter()
                .any(|failure| failure.contains(expected)),
            "{case}: {:?}",
            coverage.failures
        );
        database.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn resolver_coverage_binds_anchor_hash_to_its_claimed_block_number() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_resolver_anchor_number_binding").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    let resolver = "0x0000000000000000000000000000000000000100";
    insert_resolver_manifest(
        database.pool(),
        &CheckedInResolverManifest {
            namespace: "ens".to_owned(),
            chain_id: "ethereum-mainnet".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            payload: serde_json::json!({"contracts": [{"address": resolver}]}),
            addresses: vec![resolver.to_owned()],
        },
    )
    .await;
    insert_project_head(database.pool(), "ethereum-mainnet", 100).await;
    insert_resolver_row(
        database.pool(),
        "ethereum-mainnet",
        resolver,
        "supported",
        100,
    )
    .await;
    sqlx::query(
        "UPDATE resolver_current
         SET chain_positions = jsonb_set(
             chain_positions, '{target_block_number}', '99'::jsonb
         )",
    )
    .execute(database.pool())
    .await
    .unwrap();

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(
        coverage.failures.iter().any(|failure| failure.contains(
            "fails the resolver benchmark's canonical-read or chain-anchor integrity checks",
        )),
        "{:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn resolver_coverage_accepts_an_exact_current_head_match() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_resolver_exact_snapshot").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    let resolver = "0x0000000000000000000000000000000000000100";
    insert_resolver_manifest(
        database.pool(),
        &CheckedInResolverManifest {
            namespace: "ens".to_owned(),
            chain_id: "ethereum-mainnet".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            payload: serde_json::json!({
                "contracts": [{"address": resolver, "start_block": 100}]
            }),
            addresses: vec![resolver.to_owned()],
        },
    )
    .await;
    insert_project_head(database.pool(), "ethereum-mainnet", 100).await;
    insert_resolver_row(
        database.pool(),
        "ethereum-mainnet",
        resolver,
        "supported",
        100,
    )
    .await;
    sqlx::query(
        "INSERT INTO chain_lineage
             (chain_id, block_hash, canonicality_state, block_number)
         VALUES ('ethereum-mainnet', 'project-head-100', 'canonical', 100)",
    )
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE resolver_current SET chain_positions =
             jsonb_set(chain_positions, '{target_block_hash}',
                       '\"project-head-100\"'::jsonb)",
    )
    .execute(database.pool())
    .await
    .unwrap();

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(coverage.failures.is_empty(), "{:?}", coverage.failures);
    assert_eq!(coverage.resolvers.len(), 1);
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn malformed_resolver_block_numbers_produce_report_failures() {
    for case in ["manifest_start", "projection_target"] {
        let database = TestDatabase::create(
            TestDatabaseConfig::new(format!("benchmark_resolver_bad_block_{case}"))
                .pool_max_connections(1),
        )
        .await
        .unwrap();
        tests::install_name_visibility_schema(database.pool()).await;
        let resolver = "0x0000000000000000000000000000000000000100";
        let start_block = if case == "manifest_start" {
            serde_json::json!("later")
        } else {
            serde_json::json!(100)
        };
        insert_resolver_manifest(
            database.pool(),
            &CheckedInResolverManifest {
                namespace: "ens".to_owned(),
                chain_id: "ethereum-mainnet".to_owned(),
                source_family: "ens_v1_resolver_l1".to_owned(),
                payload: serde_json::json!({
                    "contracts": [{"address": resolver, "start_block": start_block}]
                }),
                addresses: vec![resolver.to_owned()],
            },
        )
        .await;
        insert_project_head(database.pool(), "ethereum-mainnet", 100).await;
        if case == "projection_target" {
            insert_resolver_row(
                database.pool(),
                "ethereum-mainnet",
                resolver,
                "supported",
                100,
            )
            .await;
            sqlx::query(
                "UPDATE resolver_current SET chain_positions =
                     jsonb_set(chain_positions, '{target_block_number}',
                               '9223372036854775808'::jsonb)",
            )
            .execute(database.pool())
            .await
            .unwrap();
        }

        let coverage = super::resolver_coverage::load(database.pool())
            .await
            .expect("malformed stored block numbers must remain reportable");
        let failures = coverage.failures.join("; ");

        if case == "manifest_start" {
            assert!(
                failures.contains("a contract entry has an invalid start_block"),
                "{failures}"
            );
        } else {
            assert!(
                failures.contains(
                    "fails the resolver benchmark's canonical-read or chain-anchor integrity checks"
                ),
                "{failures}"
            );
        }
        database.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn addressless_resolver_contracts_report_a_zero_declared_family() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_addressless_resolver_declaration")
            .pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_resolver_manifest(
        database.pool(),
        &CheckedInResolverManifest {
            namespace: "ens".to_owned(),
            chain_id: "ethereum-mainnet".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            payload: serde_json::json!({
                "contracts": [{"role": "resolver", "proxy_kind": "none"}]
            }),
            addresses: Vec::new(),
        },
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert_eq!(coverage.counts.len(), 1);
    assert_eq!(coverage.counts[0].declared_addresses, 0);
    assert_eq!(coverage.counts[0].applicable_addresses, 0);
    assert_eq!(coverage.counts[0].exercised_addresses, 0);
    assert!(
        coverage
            .failures
            .iter()
            .any(|failure| failure.contains("a contract entry has no address")),
        "{:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn null_resolver_contracts_report_a_zero_declared_family() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_null_resolver_declaration").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    insert_resolver_manifest(
        database.pool(),
        &CheckedInResolverManifest {
            namespace: "ens".to_owned(),
            chain_id: "ethereum-mainnet".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            payload: serde_json::json!({"contracts": null}),
            addresses: Vec::new(),
        },
    )
    .await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert_eq!(coverage.counts.len(), 1);
    assert_eq!(coverage.counts[0].declared_addresses, 0);
    assert_eq!(coverage.counts[0].applicable_addresses, 0);
    assert_eq!(coverage.counts[0].exercised_addresses, 0);
    assert!(
        coverage
            .failures
            .iter()
            .any(|failure| failure.contains("contracts is absent or is not an array")),
        "{:?}",
        coverage.failures
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn malformed_event_only_resolver_manifest_names_its_actual_authority() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_event_only_malformed_resolver").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    let manifest = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        payload: serde_json::json!({"contracts": null}),
        addresses: Vec::new(),
    };
    insert_resolver_manifest(database.pool(), &manifest).await;
    sqlx::query("UPDATE manifest_versions SET rollout_status = 'deprecated'")
        .execute(database.pool())
        .await
        .unwrap();

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();
    let malformed = coverage
        .failures
        .iter()
        .find(|failure| failure.contains("contracts is absent or is not an array"))
        .expect("the malformed event-side manifest must be reported");

    assert!(
        malformed.contains("latest Project manifest event"),
        "{malformed}"
    );
    assert!(
        !malformed.contains("active stored resolver manifest"),
        "{malformed}"
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn malformed_latest_event_payload_is_not_attributed_to_the_stored_row() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_malformed_latest_event_authority")
            .pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    let manifest = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        payload: serde_json::json!({"contracts": []}),
        addresses: Vec::new(),
    };
    insert_resolver_manifest(database.pool(), &manifest).await;
    sqlx::query(
        "UPDATE normalized_events
         SET after_state = jsonb_set(
             after_state,
             '{manifest_payload}',
             '{\"contracts\": null}'::jsonb
         )",
    )
    .execute(database.pool())
    .await
    .unwrap();

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();
    let malformed = coverage
        .failures
        .iter()
        .find(|failure| failure.contains("contracts is absent or is not an array"))
        .expect("the malformed latest-event payload must be reported");

    assert!(
        malformed.contains("latest Project manifest event"),
        "{malformed}"
    );
    assert!(
        !malformed.contains("active stored resolver manifest"),
        "{malformed}"
    );
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn malformed_resolver_failures_remain_distinct_per_manifest() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_distinct_malformed_resolvers").pool_max_connections(1),
    )
    .await
    .unwrap();
    tests::install_name_visibility_schema(database.pool()).await;
    let manifest = CheckedInResolverManifest {
        namespace: "ens".to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        source_family: "ens_v1_resolver_l1".to_owned(),
        payload: serde_json::json!({"contracts": null}),
        addresses: Vec::new(),
    };
    insert_resolver_manifest(database.pool(), &manifest).await;
    insert_resolver_manifest(database.pool(), &manifest).await;

    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();
    let malformed = coverage
        .failures
        .iter()
        .filter(|failure| failure.contains("contracts is absent or is not an array"))
        .collect::<Vec<_>>();

    assert_eq!(malformed.len(), 2, "{:?}", coverage.failures);
    assert_ne!(malformed[0], malformed[1]);
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
    ensure_project_state_schema(pool).await;
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
    sqlx::query(
        "UPDATE name_current
         SET resource_id = $1
         WHERE logical_name_id = 'ens:load-0'",
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
        "INSERT INTO resolver_current
             (chain_id, resolver_address, support_status, chain_positions,
              canonicality_summary) VALUES
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

        let (_, failures) = Corpus::load(database.pool(), &tiny_budgets())
            .await
            .expect("corpus shortfalls must return reportable evidence");
        let error = failures.join("; ");
        assert!(error.contains(expected), "{label}: {error}");
        database.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn production_corpus_requires_retained_registration_audit_evidence() {
    let database = seeded_database("corpus_load_retained_registration").await;
    let (_, failures) = Corpus::load(database.pool(), &tiny_budgets())
        .await
        .expect("missing retained-registration evidence must stay reportable");
    assert!(
        failures
            .iter()
            .any(|failure| failure.contains("no canonical retained registration")),
        "{failures:?}"
    );

    sqlx::raw_sql(
        "INSERT INTO resources VALUES
             ('00000000-0000-0000-0000-000000000044', 'ethereum-mainnet',
              'load-permission-resource', 'canonical');
         INSERT INTO permissions_current VALUES
             ('0x0000000000000000000000000000000000000044',
              '00000000-0000-0000-0000-000000000044',
              '{\"chain_id\":\"ethereum-mainnet\"}',
              '{\"target_block_hash\":\"load-permission-projection\"}',
              '{\"state\":\"canonical\"}')",
    )
    .execute(database.pool())
    .await
    .unwrap();

    let (corpus, failures) = Corpus::load(database.pool(), &tiny_budgets())
        .await
        .expect("retained-registration evidence must load through the public corpus path");
    assert!(
        !failures
            .iter()
            .any(|failure| failure.contains("no canonical retained registration")),
        "{failures:?}"
    );
    assert!(
        corpus
            .permission_subjects
            .iter()
            .any(|target| target.retained_registration)
    );
    database.cleanup().await.unwrap();
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

        let (_, failures) = Corpus::load(database.pool(), &budgets)
            .await
            .expect("aggregate corpus shortfalls must return reportable evidence");
        let error = failures.join("; ");
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
    let (_, failures) = Corpus::load(database.pool(), &budgets)
        .await
        .expect("subname-parent shortfall must return reportable evidence");
    let error = failures.join("; ");
    assert!(
        error.contains("subname parent corpus has 2 rows; release profile requires 3"),
        "{error}"
    );
    database.cleanup().await.unwrap();

    let database = seeded_database("corpus_load_permission_size").await;
    sqlx::query("DELETE FROM permissions_current WHERE subject <> '0x0000000000000000000000000000000000000000'")
        .execute(database.pool())
        .await
        .unwrap();
    let mut budgets = tiny_budgets();
    budgets.api_min_specialized_corpus_size = 2;
    let (_, failures) = Corpus::load(database.pool(), &budgets)
        .await
        .expect("permission-subject shortfall must return reportable evidence");
    let error = failures.join("; ");
    assert!(
        error.contains("permission subject corpus has 1 rows; release profile requires 2"),
        "{error}"
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
    let (_, failures) = Corpus::load(database.pool(), &budgets)
        .await
        .expect("primary-name shortfall must return reportable evidence");
    let error = failures.join("; ");
    assert!(
        error.contains("successful primary-name corpus has 2 rows; release profile requires 3"),
        "{error}"
    );
    assert!(error.contains("basenames=1"), "{error}");
    assert!(error.contains("ens=1"), "{error}");
    database.cleanup().await.unwrap();
}
