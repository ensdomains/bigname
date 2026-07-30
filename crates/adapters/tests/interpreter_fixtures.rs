use anyhow::{Context, Result, bail};
use bigname_adapters::{
    sync_block_derived_normalized_events, sync_ens_v1_reverse_claim,
    sync_ens_v1_unwrapped_authority,
};
use bigname_storage::{
    CanonicalityState, MIGRATOR, NormalizedEvent, RawBlock, RawLog,
    load_normalized_events_by_namespace, upsert_raw_blocks, upsert_raw_logs,
};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, types::time::OffsetDateTime};
use uuid::Uuid;

const RAW_EVENTS: &str = include_str!("fixtures/interpreters/raw-events.json");
const EXPECTED_OUTPUTS: &str = include_str!("fixtures/interpreters/expected-outputs.json");

#[derive(Debug, Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    id: String,
    runner: Runner,
    manifests: Vec<Manifest>,
    blocks: Vec<Block>,
    logs: Vec<Log>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Runner {
    ReverseClaim,
    BlockDerived,
    UnwrappedAuthority,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    namespace: String,
    source_family: String,
    chain: String,
    deployment_epoch: String,
    file_path: String,
    declaration_name: String,
    role: String,
    address: String,
    contract_instance_id: Uuid,
    events: Vec<AbiEvent>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AbiEvent {
    name: String,
    fragment: String,
}

#[derive(Debug, Deserialize)]
struct Block {
    chain: String,
    hash: String,
    parent_hash: Option<String>,
    number: i64,
    timestamp: i64,
}

#[derive(Debug, Deserialize)]
struct Log {
    chain: String,
    block_hash: String,
    block_number: i64,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    emitting_address: String,
    topics: Vec<String>,
    data: String,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OutputSuite {
    cases: Vec<CaseOutput>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CaseOutput {
    id: String,
    normalized_events: Vec<NormalizedEvent>,
    name_surfaces: Value,
    surface_bindings: Value,
    resources: Value,
    token_lineages: Value,
}

#[tokio::test]
async fn raw_event_interpreter_outputs_match_committed_expectations() -> Result<()> {
    let corpus: Corpus =
        serde_json::from_str(RAW_EVENTS).context("raw-event fixture is invalid")?;
    let expected: OutputSuite =
        serde_json::from_str(EXPECTED_OUTPUTS).context("expected-output fixture is invalid")?;
    let mut outputs = Vec::with_capacity(corpus.cases.len());

    for case in &corpus.cases {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new(format!("bn_interpreter_{}", case.id)),
            &MIGRATOR,
            "failed to migrate interpreter fixture database",
        )
        .await?;
        let output = run_case(database.pool(), case).await;
        let cleanup = database.cleanup().await;
        cleanup?;
        outputs.push(output?);
    }

    let actual = OutputSuite { cases: outputs };
    if actual != expected {
        bail!(
            "interpreter output changed; update the committed expectation with the semantic change\n\
             expected:\n{}\nactual:\n{}",
            serde_json::to_string_pretty(&expected)?,
            serde_json::to_string_pretty(&actual)?,
        );
    }
    Ok(())
}

async fn run_case(pool: &PgPool, case: &Case) -> Result<CaseOutput> {
    for manifest in &case.manifests {
        seed_manifest(pool, manifest).await?;
    }
    seed_raw_events(pool, case).await?;

    match case.runner {
        Runner::ReverseClaim => {
            let chain = one_chain(case)?;
            sync_ens_v1_reverse_claim(pool, chain).await?;
        }
        Runner::BlockDerived => {
            let chain = one_chain(case)?;
            let block_hashes = case
                .blocks
                .iter()
                .map(|block| block.hash.clone())
                .collect::<Vec<_>>();
            sync_block_derived_normalized_events(pool, chain, &block_hashes, None).await?;
        }
        Runner::UnwrappedAuthority => {
            let chain = one_chain(case)?;
            sync_ens_v1_unwrapped_authority(pool, chain).await?;
        }
    }

    let namespace = case
        .manifests
        .first()
        .context("fixture case must declare a manifest")?
        .namespace
        .as_str();
    Ok(CaseOutput {
        id: case.id.clone(),
        normalized_events: load_normalized_events_by_namespace(pool, namespace).await?,
        name_surfaces: output_rows(pool, "name_surfaces", "logical_name_id").await?,
        surface_bindings: output_rows(pool, "surface_bindings", "surface_binding_id").await?,
        resources: output_rows(pool, "resources", "resource_id").await?,
        token_lineages: output_rows(pool, "token_lineages", "token_lineage_id").await?,
    })
}

fn one_chain(case: &Case) -> Result<&str> {
    let chain = case
        .manifests
        .first()
        .context("fixture case must declare a manifest")?
        .chain
        .as_str();
    if case
        .manifests
        .iter()
        .any(|manifest| manifest.chain != chain)
    {
        bail!("fixture case {} spans more than one chain", case.id);
    }
    Ok(chain)
}

async fn seed_manifest(pool: &PgPool, manifest: &Manifest) -> Result<()> {
    let payload = serde_json::json!({
        "manifest_version": 1,
        "namespace": manifest.namespace,
        "source_family": manifest.source_family,
        "chain": manifest.chain,
        "deployment_epoch": manifest.deployment_epoch,
        "rollout_status": "active",
        "normalizer_version": "ensip15@ens-normalize-0.1.1",
        "capability_flags": {},
        "roots": [],
        "contracts": [],
        "discovery_rules": [],
        "abi": { "events": manifest.events },
    });
    let manifest_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO manifest_versions (
            manifest_version, namespace, source_family, chain, deployment_epoch,
            rollout_status, normalizer_version, file_path, manifest_payload
        )
        VALUES (1, $1, $2, $3, $4, 'active', 'ensip15@ens-normalize-0.1.1', $5, $6)
        RETURNING manifest_id
        "#,
    )
    .bind(&manifest.namespace)
    .bind(&manifest.source_family)
    .bind(&manifest.chain)
    .bind(&manifest.deployment_epoch)
    .bind(&manifest.file_path)
    .bind(payload)
    .fetch_one(pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id, chain_id, contract_kind, provenance
        )
        VALUES ($1, $2, 'contract', '{}'::jsonb)
        "#,
    )
    .bind(manifest.contract_instance_id)
    .bind(&manifest.chain)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO manifest_contract_instances (
            manifest_id, declaration_kind, declaration_name, contract_instance_id,
            declared_address, role, proxy_kind
        )
        VALUES ($1, 'contract', $2, $3, $4, $5, 'none')
        "#,
    )
    .bind(manifest_id)
    .bind(&manifest.declaration_name)
    .bind(manifest.contract_instance_id)
    .bind(&manifest.address)
    .bind(&manifest.role)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id, chain_id, address, source_manifest_id, provenance
        )
        VALUES ($1, $2, $3, $4, '{}'::jsonb)
        "#,
    )
    .bind(manifest.contract_instance_id)
    .bind(&manifest.chain)
    .bind(&manifest.address)
    .bind(manifest_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_raw_events(pool: &PgPool, case: &Case) -> Result<()> {
    let blocks = case
        .blocks
        .iter()
        .map(|block| {
            Ok(RawBlock {
                chain_id: block.chain.clone(),
                block_hash: block.hash.clone(),
                parent_hash: block.parent_hash.clone(),
                block_number: block.number,
                block_timestamp: OffsetDateTime::from_unix_timestamp(block.timestamp)
                    .context("fixture block timestamp is outside the supported range")?,
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let logs = case
        .logs
        .iter()
        .map(|log| {
            Ok(RawLog {
                chain_id: log.chain.clone(),
                block_hash: log.block_hash.clone(),
                block_number: log.block_number,
                transaction_hash: log.transaction_hash.clone(),
                transaction_index: log.transaction_index,
                log_index: log.log_index,
                emitting_address: log.emitting_address.clone(),
                topics: log.topics.clone(),
                data: alloy_primitives::hex::decode(log.data.trim_start_matches("0x"))
                    .context("fixture log data is not hexadecimal")?,
                canonicality_state: CanonicalityState::Canonical,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    upsert_raw_blocks(pool, &blocks).await?;
    upsert_raw_logs(pool, &logs).await?;
    Ok(())
}

async fn output_rows(pool: &PgPool, table: &str, order_column: &str) -> Result<Value> {
    let query = format!(
        "SELECT COALESCE( \
             jsonb_agg(to_jsonb(output_row) - 'observed_at' - 'inserted_at' \
                 ORDER BY output_row.{order_column}), \
             '[]'::jsonb \
         ) \
         FROM {table} output_row"
    );
    sqlx::query_scalar(&query)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to read interpreter output table {table}"))
}
