//! Bounded operator diagnostics through the same read-only provider used by ingestion.

use std::{collections::BTreeSet, time::Instant};

use anyhow::{Result, bail};
use serde_json::{Value, json};

use crate::provider::RethDbProvider;

/// Reads at most 32 explicitly selected blocks without writing ingestion or node data.
///
/// Returns complete block/transaction/receipt/log values, persisted heads, retention floor,
/// optional topic0-filtered logs, and elapsed milliseconds for independent RPC comparison.
/// Binary fields in the JSON are byte arrays. The datadir must allow MDBX lock updates and
/// RocksDB secondary files; all three underlying storage children are opened read-only.
pub async fn read_reth_sample(
    chain: &str,
    datadir: &str,
    numbers: &[i64],
    topic0s: &[String],
) -> Result<Value> {
    if numbers.is_empty() || numbers.len() > 32 || numbers.iter().any(|number| *number < 0) {
        bail!("select between 1 and 32 nonnegative block numbers");
    }
    if numbers.iter().collect::<BTreeSet<_>>().len() != numbers.len() {
        bail!("selected block numbers must be unique");
    }
    let started = Instant::now();
    let provider = RethDbProvider::new(chain, datadir)?;
    let heads = provider.heads().await?;
    let earliest_available_block = provider.earliest_available_block().await?;
    let opened_ms = started.elapsed().as_millis();
    let resolved = provider.resolve(numbers).await?;
    let headers = provider.headers(&resolved).await?;
    let headers_ms = started.elapsed().as_millis() - opened_ms;
    let bundles = provider.bundles(&resolved).await?;
    let bundles_ms = started.elapsed().as_millis() - opened_ms - headers_ms;
    let mut filtered_logs = Vec::new();
    for block in &resolved {
        if !topic0s.is_empty() {
            filtered_logs.extend(
                provider
                    .logs(std::slice::from_ref(block), &[], topic0s, &[])
                    .await?,
            );
        }
    }
    if provider.resolve(numbers).await? != resolved {
        bail!("selected canonical block hashes changed during the sample");
    }
    Ok(json!({
        "chain": chain,
        "heads": heads,
        "earliest_available_block": earliest_available_block,
        "resolved": resolved,
        "headers": headers,
        "bundles": bundles,
        "topic0s": topic0s,
        "filtered_logs": filtered_logs,
        "elapsed_ms": {
            "open_and_heads": opened_ms,
            "resolve_and_headers": headers_ms,
            "bundles": bundles_ms,
            "total": started.elapsed().as_millis(),
        },
    }))
}
