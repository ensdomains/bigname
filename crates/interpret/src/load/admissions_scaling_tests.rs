//! How `load_admissions` scales with announced registries. Every batch loads the admitted-address
//! set, and any contract can announce itself with `RegistryCreated()`, so the announcement count
//! is attacker-chosen. The test seeds N announced registries, each one an address and a
//! `registry_announcement` self-edge, and times the statement at N = 1, 1,000 and 50,000. A fourth
//! case adds registry-family resolver edges, which the statement reads and then filters out, to
//! separate their cost from N's. Each case prints its timings and its `EXPLAIN (ANALYZE, BUFFERS)`
//! plan. The assertions hold the row counts and keep the growth from 1,000 to 50,000 announced
//! registries within linear-with-a-sort.
use bigname_adapters::schema_v2::seam::ADMISSION_DISCOVERY_EDGE_KINDS;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::PgConnection;
use std::time::{Duration, Instant};

type Result<T = ()> = anyhow::Result<T>;

const CHAIN: &str = "admissions-scaling";
const TIMED_RUNS: usize = 5;

struct Measured {
    announced: i64,
    excluded: i64,
    rows: usize,
    median: Duration,
    plan: String,
}

async fn database() -> Result<TestDatabase> {
    let db = TestDatabase::create(TestDatabaseConfig::new("admissions_scaling")).await?;
    for sql in [
        include_str!("../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../../storage/schema/baseline/04_manifests.sql"),
    ] {
        sqlx::raw_sql(sql).execute(db.pool()).await?;
    }
    Ok(db)
}

/// One active registry manifest declaring one registry, `announced` registries that announced
/// themselves, and `excluded` resolver edges from the declared registry to ten resolvers.
async fn seed(connection: &mut PgConnection, announced: i64, excluded: i64) -> Result {
    let statements = [
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id, deployment_label, rollout_status, normalizer_version, file_path, manifest_payload) VALUES (1, 'ens', 'ens_v2_registry_l1', $1, 'scaling', 'active', 'test', 'scaling.toml', '{}')",
        // The declared registry.
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) VALUES (md5('declared')::uuid, $1, 'contract')",
        "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number, source_manifest_id) VALUES (md5('declared')::uuid, $1, '0x00000000000000000000000000000000000000d0', 0, 1)",
        "INSERT INTO manifest_contract_instances (manifest_id, chain_id, declaration_kind, declaration_name, contract_instance_id, declared_address, role, proxy_kind) VALUES (1, $1, 'contract', 'registry', md5('declared')::uuid, '0x00000000000000000000000000000000000000d0', 'registry', 'none')",
        // The announced registries.
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) SELECT md5('announced' || n)::uuid, $1, 'contract' FROM generate_series(1, $2::bigint) n",
        "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number, source_manifest_id) SELECT md5('announced' || n)::uuid, $1, '0x' || lpad(to_hex(n), 40, '0'), n, 1 FROM generate_series(1, $2::bigint) n",
        "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id, discovery_source, admission_basis, source_manifest_id, active_from_block_number, active_from_block_hash, canonicality_state, provenance) SELECT $1, 'registry_announcement', md5('announced' || n)::uuid, md5('announced' || n)::uuid, 'RegistryCreated', 'protocol_event', 1, n, 'block-' || n, 'canonical', jsonb_build_object('observation_key', 'registry_announcement:' || n) FROM generate_series(1, $2::bigint) n",
        // The resolvers the excluded edges point at.
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind) SELECT md5('resolver' || n)::uuid, $1, 'contract' FROM generate_series(1, 10) n",
        "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address, active_from_block_number, source_manifest_id) SELECT md5('resolver' || n)::uuid, $1, '0x' || lpad(to_hex(n), 38, '0') || 'e0', 0, 1 FROM generate_series(1, 10) n",
        "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id, to_contract_instance_id, discovery_source, admission_basis, source_manifest_id, active_from_block_number, active_from_block_hash, canonicality_state, provenance) SELECT $1, 'resolver', md5('declared')::uuid, md5('resolver' || (n % 10 + 1))::uuid, 'ResolverUpdated', 'protocol_event', 1, n, 'block-' || n, 'canonical', jsonb_build_object('observation_key', 'resolver:' || n) FROM generate_series(1, $3::bigint) n",
    ];
    for statement in statements {
        let mut query = sqlx::query(statement).bind(CHAIN);
        if statement.contains("$2") {
            query = query.bind(announced);
        }
        if statement.contains("$3") {
            query = query.bind(excluded);
        }
        query.execute(&mut *connection).await?;
    }
    sqlx::raw_sql("ANALYZE").execute(&mut *connection).await?;
    Ok(())
}

async fn measure(announced: i64, excluded: i64) -> Result<Measured> {
    let db = database().await?;
    let mut connection = db.pool().acquire().await?;
    seed(&mut connection, announced, excluded).await?;
    let before_block = announced.max(excluded) + 1;
    let rows = super::load_admissions(&mut connection, CHAIN, before_block)
        .await?
        .len();
    let mut times = Vec::with_capacity(TIMED_RUNS);
    for _ in 0..TIMED_RUNS {
        let started = Instant::now();
        super::load_admissions(&mut connection, CHAIN, before_block).await?;
        times.push(started.elapsed());
    }
    times.sort();
    let kinds = ADMISSION_DISCOVERY_EDGE_KINDS
        .iter()
        .map(|kind| format!("'{kind}'"))
        .collect::<Vec<_>>()
        .join(", ");
    sqlx::raw_sql(&format!(
        "PREPARE admissions (text, bigint, text[]) AS {}",
        super::admissions_statement()
    ))
    .execute(&mut *connection)
    .await?;
    let plan = sqlx::query_scalar::<_, String>(&format!(
        "EXPLAIN (ANALYZE, BUFFERS) EXECUTE admissions ('{CHAIN}', {before_block}, ARRAY[{kinds}])"
    ))
    .fetch_all(&mut *connection)
    .await?
    .join("\n");
    Ok(Measured {
        announced,
        excluded,
        rows,
        median: times[TIMED_RUNS / 2],
        plan,
    })
}

#[tokio::test]
async fn admissions_scaling_stays_linear_in_announced_registries() -> Result {
    let mut measured = Vec::new();
    for (announced, excluded) in [(1, 0), (1_000, 0), (50_000, 0), (1_000, 50_000)] {
        let case = measure(announced, excluded).await?;
        // The declared registry plus one row per announced registry. Registry-family resolver
        // edges never admit their target.
        assert_eq!(case.rows, announced as usize + 1);
        println!(
            "load_admissions: announced={} excluded_resolver_edges={} rows={} median_of_{TIMED_RUNS}={:.3} ms\n{}\n",
            case.announced,
            case.excluded,
            case.rows,
            case.median.as_secs_f64() * 1_000.0,
            case.plan
        );
        measured.push(case);
    }
    // Linear growth from 1,000 to 50,000 is 50x and the sort adds a log factor. A plan that turns
    // quadratic lands far past 100x.
    let ratio = measured[2].median.as_secs_f64() / measured[1].median.as_secs_f64();
    println!("load_admissions: 50,000 / 1,000 announced median ratio = {ratio:.1}");
    assert!(
        ratio <= 100.0,
        "load_admissions grew {ratio:.1}x from 1,000 to 50,000 announced registries"
    );
    Ok(())
}
