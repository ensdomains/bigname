use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use alloy_primitives::{B256, keccak256};
use anyhow::{Context, Result, ensure};
use serde::Serialize;
use sqlx::{PgPool, Postgres, QueryBuilder};

use super::{manifests::CHAIN, raw::RawTransaction};

#[derive(Serialize)]
pub(super) struct SeedReport {
    pub(super) from_block: i64,
    pub(super) head: i64,
    pub(super) transactions: u64,
    pub(super) logs: u64,
    pub(super) source_raw_digest_verified: String,
    pub(super) manifest_count: usize,
    pub(super) database_bytes: i64,
    pub(super) feature_gate_complete: bool,
}

pub(super) fn block_hash(block: i64) -> String {
    format!("0x{block:064x}")
}
pub(super) fn transaction_hash(row: &RawTransaction) -> String {
    format!(
        "0x{:064x}",
        ((row.block as u128) << 32) | row.transaction_index as u128
    )
}

/// Validate the complete input digest before the first database write. Intake
/// uses raw tables only; manifest-derived metadata comes from production sync.
pub(super) async fn run(
    pool: &PgPool,
    directory: &Path,
    head: i64,
    append: bool,
) -> Result<SeedReport> {
    let metadata: serde_json::Value =
        serde_json::from_reader(File::open(directory.join("corpus.json"))?)?;
    ensure!(
        metadata["source_head"].as_str() == Some(crate::git_head().as_str()),
        "corpus source differs from the running binary"
    );
    ensure!(
        metadata["interpreter_content_hash"].as_str()
            == Some(bigname_content_hash::INTERPRETER_CONTENT_HASH),
        "corpus interpreter hash differs"
    );
    ensure!(
        ["structural_head", "changed_head", "bytes_head"]
            .iter()
            .any(|key| metadata[*key].as_i64() == Some(head)),
        "head must be a declared corpus epoch"
    );
    let path = directory.join("raw-transactions.jsonl");
    let digest = verify(&path, &metadata)?;
    let repository = bigname_manifests::load_repository(directory.join("manifests"))?;
    // Reject modified fixture declarations before schema initialization.
    for manifest in &repository.manifests()[..] {
        let expected = metadata["manifests"]
            .as_array()
            .context("missing manifest receipts")?
            .iter()
            .find(|receipt| {
                receipt["source_family"].as_str() == Some(manifest.manifest.source_family.as_str())
            })
            .context("manifest has no sealed receipt")?;
        let actual = format!("{:#x}", keccak256(std::fs::read(&manifest.path)?));
        ensure!(
            expected["fixture_keccak256"].as_str() == Some(actual.as_str()),
            "manifest digest differs"
        );
    }
    ensure!(
        repository.manifests().len() == 4,
        "fixture requires four source manifests"
    );
    let from_block = if append {
        let current: i64 =
            sqlx::query_scalar("SELECT latest_block_number FROM chain_heads WHERE chain_id=$1")
                .bind(CHAIN)
                .fetch_one(pool)
                .await?;
        ensure!(current < head, "append requires a later epoch");
        super::oracle::expected(directory, current)?;
        crate::indexing::publication::require_published_head(pool, CHAIN, current).await?;
        current + 1
    } else {
        phase_runner::schema::initialize_schema_v2(pool).await?;
        bigname_manifests::sync_schema_v2_repository(pool, &repository).await?;
        0
    };
    sqlx::query("INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state)
        SELECT $1, '0x' || lpad(to_hex(number),64,'0'), CASE WHEN number=0 THEN NULL ELSE '0x' || lpad(to_hex(number-1),64,'0') END,
        number, to_timestamp(1700000000 + number*12), 'canonical'::canonicality_state FROM generate_series($3::bigint,$2::bigint) number")
        .bind(CHAIN).bind(head).bind(from_block).execute(pool).await?;
    let mut transactions = 0;
    let mut logs = 0;
    let mut batch = Vec::with_capacity(512);
    for line in BufReader::new(File::open(&path)?).lines() {
        let transaction: RawTransaction = serde_json::from_str(&line?)?;
        if transaction.block > head {
            break;
        }
        if transaction.block < from_block {
            continue;
        }
        logs += transaction.logs.len() as u64;
        transactions += 1;
        batch.push(transaction);
        if batch.len() == 512 {
            insert(pool, &batch).await?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        insert(pool, &batch).await?;
    }
    ensure!(transactions > 0 && logs > 0, "empty seeded corpus");
    sqlx::query(
        "INSERT INTO chain_heads(chain_id,latest_block_number,latest_block_hash) VALUES($1,$2,$3)
         ON CONFLICT(chain_id) DO UPDATE SET latest_block_number=EXCLUDED.latest_block_number,latest_block_hash=EXCLUDED.latest_block_hash",
    )
    .bind(CHAIN)
    .bind(head)
    .bind(block_hash(head))
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO ingest_cursors(chain_id,source_key,source_kind,seed_basis,start_block_number,next_block_number,target_block_number,last_processed_block_number,last_processed_block_hash)
        VALUES($1,'node-scale-retained','jsonrpc','new_signature_range',0,$2+1,$2,$2,$3)
        ON CONFLICT(chain_id,source_key) DO UPDATE SET next_block_number=EXCLUDED.next_block_number,target_block_number=EXCLUDED.target_block_number,last_processed_block_number=EXCLUDED.last_processed_block_number,last_processed_block_hash=EXCLUDED.last_processed_block_hash")
        .bind(CHAIN).bind(head).bind(block_hash(head)).execute(pool).await?;
    super::phases::record_intake(pool, head, append.then_some(from_block - 1)).await?;
    let (stored_transactions, stored_logs): (i64,i64) = sqlx::query_as("SELECT
        (SELECT count(*) FROM raw_transactions WHERE chain_id=$1 AND block_number BETWEEN $2 AND $3),
        (SELECT count(*) FROM raw_logs WHERE chain_id=$1 AND block_number BETWEEN $2 AND $3)")
        .bind(CHAIN).bind(from_block).bind(head).fetch_one(pool).await?;
    ensure!(
        stored_transactions as u64 == transactions && stored_logs as u64 == logs,
        "raw persistence count mismatch"
    );
    let database_bytes = sqlx::query_scalar("SELECT pg_database_size(current_database())")
        .fetch_one(pool)
        .await?;
    Ok(SeedReport {
        from_block,
        head,
        transactions,
        logs,
        source_raw_digest_verified: digest,
        manifest_count: repository.manifests().len(),
        database_bytes,
        feature_gate_complete: false,
    })
}

fn verify(path: &Path, metadata: &serde_json::Value) -> Result<String> {
    let mut input = BufReader::new(File::open(path)?);
    let mut digest = B256::ZERO;
    let mut bytes = Vec::new();
    let mut count = 0_u64;
    let mut byte_count = 0_u64;
    loop {
        bytes.clear();
        if input.read_until(b'\n', &mut bytes)? == 0 {
            break;
        }
        let mut digest_input = Vec::with_capacity(32 + bytes.len());
        digest_input.extend_from_slice(digest.as_slice());
        digest_input.extend_from_slice(&bytes);
        digest = keccak256(digest_input);
        byte_count += bytes.len() as u64;
        count += 1;
    }
    let actual = format!("{digest:#x}");
    ensure!(
        metadata["raw"]["rolling_keccak256"].as_str() == Some(actual.as_str())
            && metadata["raw"]["transactions"].as_u64() == Some(count)
            && metadata["raw"]["raw_jsonl_bytes"].as_u64() == Some(byte_count),
        "raw input digest/count mismatch"
    );
    Ok(actual)
}

async fn insert(pool: &PgPool, rows: &[RawTransaction]) -> Result<()> {
    let mut transaction = pool.begin().await?;
    let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO raw_transactions(chain_id,block_hash,block_number,transaction_hash,transaction_index,from_address,to_address) ",
    );
    builder.push_values(rows, |mut row, value| {
        row.push_bind(CHAIN)
            .push_bind(block_hash(value.block))
            .push_bind(value.block)
            .push_bind(transaction_hash(value))
            .push_bind(value.transaction_index)
            .push_bind(&value.from)
            .push_bind(&value.to);
    });
    builder.build().execute(&mut *transaction).await?;
    let decoded = rows
        .iter()
        .flat_map(|row| {
            row.logs
                .iter()
                .enumerate()
                .map(move |(index, log)| (row, index, log))
        })
        .map(|(row, index, log)| {
            Ok((
                row,
                index,
                log,
                alloy_primitives::hex::decode(&log.data_hex)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
        "INSERT INTO raw_logs(chain_id,block_hash,block_number,transaction_hash,transaction_index,log_index,emitting_address,topics,data) ",
    );
    builder.push_values(&decoded, |mut values, (row, index, log, data)| {
        values
            .push_bind(CHAIN)
            .push_bind(block_hash(row.block))
            .push_bind(row.block)
            .push_bind(transaction_hash(row))
            .push_bind(row.transaction_index)
            .push_bind(row.first_log_index + *index as i64)
            .push_bind(&log.emitter)
            .push_bind(&log.topics)
            .push_bind(data);
    });
    builder.build().execute(&mut *transaction).await?;
    transaction.commit().await?;
    Ok(())
}
