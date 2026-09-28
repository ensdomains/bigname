use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::PgPool;
use uuid::Uuid;

use super::{require_active_namespace_coverage, require_stratified_corpus_size, tests};

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
    _index: usize,
    _target: i64,
) {
    sqlx::query("INSERT INTO project_resolver_classification (chain_id,resolver_address,support_status,manifest_id,manifest_event_id)
        SELECT $1,$2,$3,manifest_id,(SELECT max(normalized_event_id) FROM normalized_events WHERE source_manifest_id=manifest_id AND event_kind='SourceManifestUpdated')
        FROM manifest_versions WHERE chain_id=$1 AND rollout_status='active' AND EXISTS (SELECT 1 FROM jsonb_array_elements(manifest_payload->'contracts') contract WHERE lower(contract->>'address')=lower($2)) LIMIT 1")
        .bind(chain_id).bind(address).bind(support_status).execute(pool).await.unwrap();
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
    proxy: &str,
    _target: i64,
) {
    sqlx::query("INSERT INTO project_resolver_classification (chain_id,resolver_address,support_status,manifest_id,manifest_event_id,classification)
        SELECT chain_id,$1,'supported',manifest_id,
          (SELECT max(normalized_event_id) FROM normalized_events WHERE source_manifest_id=manifest_id AND event_kind='SourceManifestUpdated'),
          jsonb_build_object('upgrade',(SELECT jsonb_build_object('normalized_event_id',normalized_event_id,'block_number',block_number) FROM normalized_events WHERE source_manifest_id=manifest_id AND event_kind='Upgraded' ORDER BY normalized_event_id DESC LIMIT 1))
        FROM manifest_versions WHERE chain_id=$2 AND source_family=$3 LIMIT 1")
        .bind(proxy).bind(&manifest.chain_id).bind(&manifest.source_family).execute(pool).await.unwrap();
}

async fn insert_undeclared_resolver_row(pool: &PgPool, chain_id: &str, address: &str) {
    sqlx::query("INSERT INTO project_resolver_classification (chain_id,resolver_address,support_status,manifest_id)
        SELECT $1,$2,'supported',manifest_id FROM manifest_versions WHERE chain_id=$1 LIMIT 1")
        .bind(chain_id).bind(address).execute(pool).await.unwrap();
}

#[tokio::test]
async fn missing_unsupported_or_invisible_declared_resolver_is_named() {
    for (case, expected_message) in [
        (
            "missing",
            "is missing from the resolver classification family",
        ),
        (
            "unsupported",
            "not supported, in the resolver classification family",
        ),
        ("invisible", "no current Project head"),
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
                         WHERE block_hash = $1 AND chain_id = 'ethereum-mainnet'",
                    )
                    .bind("project-head-30000000")
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

        assert_eq!(
            coverage.resolvers.len(),
            if case == "invisible" { 1 } else { 7 },
            "{case}"
        );
        if case != "invisible" {
            assert!(error.contains(&missing_address), "{case}: {error}");
        }
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
        checked_in_resolver_manifest("manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v1.toml");
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
            "UPDATE project_resolver_classification SET classification = jsonb_set(classification, '{upgrade,normalized_event_id}', '99999')",
        ),
        (
            "number",
            "UPDATE project_resolver_classification SET classification = jsonb_set(classification, '{upgrade,block_number}', '899'::jsonb)",
        ),
        (
            "hash",
            "UPDATE normalized_events SET block_hash = 'wrong-upgrade' WHERE event_kind = 'Upgraded'",
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
            "manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v1.toml",
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
            coverage
                .failures
                .iter()
                .any(|failure| failure.contains(if case == "hash" {
                    "zero currently applicable"
                } else {
                    "fails the resolver benchmark's canonical-read or chain-anchor integrity checks"
                })),
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
        checked_in_resolver_manifest("manifests/sepolia/ethereum/ens/ens_v2_resolver_l1/v1.toml");
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

    sqlx::query("UPDATE project_resolver_classification SET manifest_event_id = 999999")
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
    sqlx::query("INSERT INTO project_family_marker (chain_id,current_block_number,current_block_hash,input_content_hash,state) VALUES ($1,$2,$3,$4,'live')")
        .bind(chain_id).bind(block_number).bind(format!("project-head-{block_number}"))
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,canonicality_state) VALUES ($1,$2,$3,'canonical')")
        .bind(chain_id).bind(format!("project-head-{block_number}")).bind(block_number).execute(pool).await.unwrap();
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
    sqlx::query("CREATE TABLE IF NOT EXISTS project_family_marker (chain_id text PRIMARY KEY, current_block_number bigint, current_block_hash text, input_content_hash text, state text)").execute(pool).await.unwrap();
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS chain_phase_state (
             chain_id text NOT NULL,
             phase_name text NOT NULL,
             phase_status text NOT NULL,
             current_block_number bigint,
             current_block_hash text,
             input_content_hash text,
             redo_in_progress boolean NOT NULL DEFAULT false,
             redo_from_block_number bigint,
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
    for (case, mutation, publication_is_current) in [
        (
            "running",
            "UPDATE chain_phase_state SET phase_status = 'running'",
            true,
        ),
        (
            "stale_number",
            "UPDATE chain_heads SET latest_block_number = latest_block_number + 1",
            false,
        ),
        (
            "stale_hash",
            "UPDATE chain_heads SET latest_block_hash = 'different-head'",
            false,
        ),
        (
            "invalidated_input",
            "UPDATE project_family_marker SET input_content_hash = 'different-generation'",
            false,
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
        insert_project_head(database.pool(), "ethereum-mainnet", 101).await;
        insert_resolver_row(
            database.pool(),
            "ethereum-mainnet",
            resolver,
            "supported",
            100,
        )
        .await;
        let initial = super::resolver_coverage::load(database.pool())
            .await
            .unwrap();
        assert!(
            initial.failures.is_empty(),
            "{case}: {:?}",
            initial.failures
        );
        assert_eq!(initial.resolvers.len(), 1, "{case}");
        sqlx::query(mutation)
            .execute(database.pool())
            .await
            .unwrap();

        let coverage = super::resolver_coverage::load(database.pool())
            .await
            .unwrap();

        if publication_is_current {
            assert!(
                coverage.failures.is_empty(),
                "{case}: {:?}",
                coverage.failures
            );
            assert_eq!(coverage.resolvers.len(), 1, "{case}");
        } else {
            assert!(coverage.resolvers.is_empty(), "{case}");
            assert!(
                coverage.failures.iter().any(|failure| failure.contains(
                    "chain \"ethereum-mainnet\" in family \"ens_v1_resolver_l1\" has concrete declarations but no current Project head"
                )),
                "{case}: {:?}",
                coverage.failures
            );
        }
        database.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn resolver_coverage_uses_the_route_snapshot_bounds() {
    for (case, mutation) in [
        (
            "missing_number",
            "UPDATE project_family_marker SET current_block_number = NULL",
        ),
        (
            "ahead",
            "UPDATE project_family_marker SET current_block_number = 101",
        ),
        (
            "same_height_wrong_hash",
            "UPDATE project_family_marker SET current_block_hash = 'other-canonical-head'",
        ),
        (
            "lineage_number_mismatch",
            "UPDATE chain_lineage SET block_number = 50 WHERE block_hash = 'project-head-100'",
        ),
        (
            "predates_declaration",
            "UPDATE project_family_marker SET current_block_number = 99, current_block_hash = 'older-canonical-head'",
        ),
        (
            "wrong_manifest",
            "UPDATE project_resolver_classification SET manifest_id = 999, manifest_event_id = NULL",
        ),
        (
            "wrong_manifest_version",
            "UPDATE normalized_events SET manifest_version = 2 WHERE event_kind = 'SourceManifestUpdated'",
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

        let expected = match case {
            "wrong_manifest" => "does not cite latest projected manifest event",
            "wrong_manifest_version" => "stored version",
            _ => "no current Project head",
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
        "UPDATE chain_lineage SET block_number = 99 WHERE block_hash = 'project-head-100' ",
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
            .any(|failure| failure.contains("no current Project head",)),
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
    let coverage = super::resolver_coverage::load(database.pool())
        .await
        .unwrap();

    assert!(coverage.failures.is_empty(), "{:?}", coverage.failures);
    assert_eq!(coverage.resolvers.len(), 1);
    database.cleanup().await.unwrap();
}

#[tokio::test]
async fn malformed_resolver_block_numbers_produce_report_failures() {
    for case in ["manifest_start"] {
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
