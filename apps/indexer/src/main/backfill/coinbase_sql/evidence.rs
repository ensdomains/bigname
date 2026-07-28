use std::{collections::BTreeMap, future::Future};

use anyhow::{Context, Result, bail};
use serde_json::Value;

use super::{
    client::{CoinbaseSqlClient, CoinbaseSqlRawQueryResponse},
    error::query_memory_limit_attempt_count,
    query,
};
use crate::backfill::{
    BackfillBlockRange,
    stored_verification::{
        StoredLogIdentityBucket, StoredLogIdentityEvidence, StoredLogIdentityEvidenceRequest,
        StoredLogIdentityEvidenceSource,
    },
};

const MAX_EVIDENCE_WINDOW_HALVINGS: usize = 4;

pub(super) async fn fetch_stored_log_identity_evidence_window(
    client: &CoinbaseSqlClient,
    request: &StoredLogIdentityEvidenceRequest,
    query_range: BackfillBlockRange,
) -> Result<StoredLogIdentityEvidence> {
    let sql = stored_log_identity_evidence_query(request, query_range)?;
    raw_query_response_to_evidence(client.run_raw_query(&sql).await?)
}

pub(in crate::backfill) async fn fetch_windowed_stored_log_identity_evidence(
    source: &dyn StoredLogIdentityEvidenceSource,
    request: &StoredLogIdentityEvidenceRequest,
    initial_window_blocks: i64,
) -> Result<StoredLogIdentityEvidence> {
    fetch_windowed_stored_log_identity_evidence_with(
        request,
        initial_window_blocks,
        |query_range| {
            source.fetch_stored_log_identity_evidence_window(request.clone(), query_range)
        },
    )
    .await
}

async fn fetch_windowed_stored_log_identity_evidence_with<F, Fut>(
    request: &StoredLogIdentityEvidenceRequest,
    initial_window_blocks: i64,
    mut fetch_window: F,
) -> Result<StoredLogIdentityEvidence>
where
    F: FnMut(BackfillBlockRange) -> Fut,
    Fut: Future<Output = Result<StoredLogIdentityEvidence>>,
{
    if initial_window_blocks <= 0 {
        bail!("Coinbase SQL evidence window blocks must be positive");
    }

    let mut buckets = BTreeMap::<i64, StoredLogIdentityBucket>::new();
    let mut query_count = 0usize;
    let mut window_blocks = initial_window_blocks;
    let mut halving_count = 0usize;
    let mut from_block = request.range.from_block;
    loop {
        let to_block = from_block
            .saturating_add(window_blocks.saturating_sub(1))
            .min(request.range.to_block);
        let query_range = BackfillBlockRange::new(from_block, to_block)?;
        let query_blocks = to_block
            .checked_sub(from_block)
            .and_then(|distance| distance.checked_add(1))
            .context("Coinbase SQL evidence sub-window length overflowed")?;

        match fetch_window(query_range).await {
            Ok(evidence) => {
                query_count = query_count
                    .checked_add(evidence.query_count)
                    .context("Coinbase SQL evidence query count overflowed")?;
                for bucket in evidence.buckets {
                    merge_bucket(&mut buckets, bucket)?;
                }
            }
            Err(error) => {
                let Some(attempt_count) = query_memory_limit_attempt_count(&error) else {
                    return Err(error);
                };
                query_count = query_count
                    .checked_add(attempt_count)
                    .context("Coinbase SQL evidence query count overflowed")?;
                if halving_count >= MAX_EVIDENCE_WINDOW_HALVINGS || query_blocks == 1 {
                    return Err(error);
                }
                window_blocks = (query_blocks / 2).max(1);
                halving_count += 1;
                continue;
            }
        }

        if to_block == request.range.to_block {
            break;
        }
        from_block = to_block
            .checked_add(1)
            .context("Coinbase SQL evidence sub-window range overflowed")?;
    }

    Ok(StoredLogIdentityEvidence {
        buckets: buckets.into_values().collect(),
        query_count,
    })
}

fn raw_query_response_to_evidence(
    response: CoinbaseSqlRawQueryResponse,
) -> Result<StoredLogIdentityEvidence> {
    let query_count = response
        .retry_count
        .checked_add(1)
        .context("Coinbase SQL evidence query attempt count overflowed")?;
    let buckets = response
        .rows
        .into_iter()
        .map(stored_log_identity_bucket_from_value)
        .collect::<Result<Vec<_>>>()?;
    Ok(StoredLogIdentityEvidence {
        buckets,
        query_count,
    })
}

fn merge_bucket(
    buckets: &mut BTreeMap<i64, StoredLogIdentityBucket>,
    bucket: StoredLogIdentityBucket,
) -> Result<()> {
    let aggregate = buckets
        .entry(bucket.bucket)
        .or_insert(StoredLogIdentityBucket {
            bucket: bucket.bucket,
            ..StoredLogIdentityBucket::default()
        });
    aggregate.selected_log_count = aggregate
        .selected_log_count
        .checked_add(bucket.selected_log_count)
        .context("Coinbase SQL stored verification bucket count overflowed")?;
    aggregate.digest_left ^= bucket.digest_left;
    aggregate.digest_right ^= bucket.digest_right;
    Ok(())
}

pub(super) fn stored_log_identity_evidence_query(
    request: &StoredLogIdentityEvidenceRequest,
    query_range: BackfillBlockRange,
) -> Result<String> {
    if request.range.from_block > request.range.to_block {
        bail!("Coinbase SQL stored verification range is inverted");
    }
    if query_range.from_block < request.range.from_block
        || query_range.to_block > request.range.to_block
    {
        bail!("Coinbase SQL stored verification sub-window is outside the requested range");
    }
    if request.bucket_blocks <= 0 {
        bail!("Coinbase SQL stored verification bucket size must be positive");
    }
    if request.topic0s.is_empty() {
        bail!("Coinbase SQL stored verification requires at least one topic0");
    }
    let network = query::coinbase_sql_network(&request.chain)?;
    let address = query::sql_string_literals(std::slice::from_ref(&request.address));
    let topics = query::sql_string_literals(&request.topic0s);
    let action = query::active_action_expression("l.action");
    let from_block = query_range.from_block;
    let to_block = query_range.to_block;
    let bucket_origin = request.range.from_block;
    let bucket_blocks = request.bucket_blocks;
    let active_transactions_cte = query::active_transactions_cte(network, from_block, to_block);
    Ok(format!(
        r#"WITH {active_transactions_cte},
selected_rows AS (
  SELECT
    l.block_number,
    l.block_hash,
    l.transaction_hash,
    l.log_index,
    l.address,
    {action} AS action_delta
  FROM {network}.events l
  JOIN active_transactions t
    ON {active_transaction_log_join}
  WHERE l.block_number BETWEEN {from_block} AND {to_block}
    AND lower(l.address) = {address}
    AND lower(l.topics[1]) IN ({topics})
  UNION ALL
  SELECT
    l.block_number,
    l.block_hash,
    l.transaction_hash,
    l.log_index,
    l.address,
    {action} AS action_delta
  FROM {network}.encoded_logs l
  JOIN active_transactions t
    ON {active_transaction_log_join}
  WHERE l.block_number BETWEEN {from_block} AND {to_block}
    AND lower(l.address) = {address}
    AND lower(l.topics[1]) IN ({topics})
),
active_rows AS (
  SELECT
    block_number,
    block_hash,
    transaction_hash,
    log_index,
    address
  FROM selected_rows
  GROUP BY block_number, block_hash, transaction_hash, log_index, address
  HAVING sum(action_delta) > 0
)
SELECT
  intDiv(toInt64(block_number) - {bucket_origin}, {bucket_blocks}) AS bucket,
  count(*) AS selected_log_count,
  groupBitXor(reinterpretAsUInt64(reverse(substring(
    MD5(concat(lower(block_hash), lower(transaction_hash), toString(log_index))),
    1,
    8
  )))) AS digest_left,
  groupBitXor(reinterpretAsUInt64(reverse(substring(
    MD5(concat(lower(block_hash), lower(transaction_hash), toString(log_index))),
    9,
    8
  )))) AS digest_right
FROM active_rows
GROUP BY bucket
ORDER BY bucket"#,
        active_transaction_log_join = query::ACTIVE_TRANSACTION_LOG_JOIN,
    ))
}

pub(super) fn stored_log_identity_bucket_from_value(
    value: Value,
) -> Result<StoredLogIdentityBucket> {
    let object = value
        .as_object()
        .context("Coinbase SQL stored verification row must be an object")?;
    Ok(StoredLogIdentityBucket {
        bucket: json_i64(object.get("bucket"), "bucket")?,
        selected_log_count: json_i64(object.get("selected_log_count"), "selected_log_count")?,
        digest_left: json_u64(object.get("digest_left"), "digest_left")?,
        digest_right: json_u64(object.get("digest_right"), "digest_right")?,
    })
}

fn json_i64(value: Option<&Value>, field: &str) -> Result<i64> {
    match value.context(format!("Coinbase SQL result is missing {field}"))? {
        Value::Number(value) => value
            .as_i64()
            .context(format!("Coinbase SQL field {field} exceeds i64")),
        Value::String(value) => value
            .parse::<i64>()
            .with_context(|| format!("failed to parse Coinbase SQL field {field} value {value}")),
        value => bail!("Coinbase SQL field {field} must be integer-like, got {value}"),
    }
}

fn json_u64(value: Option<&Value>, field: &str) -> Result<u64> {
    match value.context(format!("Coinbase SQL result is missing {field}"))? {
        Value::Number(value) => value
            .as_u64()
            .context(format!("Coinbase SQL field {field} exceeds u64")),
        Value::String(value) => value
            .parse::<u64>()
            .with_context(|| format!("failed to parse Coinbase SQL field {field} value {value}")),
        value => bail!("Coinbase SQL field {field} must be unsigned integer-like, got {value}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use reqwest::StatusCode;
    use serde_json::json;

    use super::*;
    use crate::backfill::coinbase_sql::error::CoinbaseSqlHttpError;

    #[derive(Clone, Copy)]
    struct FixtureIdentity {
        block_number: i64,
        digest_left: u64,
        digest_right: u64,
    }

    #[tokio::test]
    async fn windowed_evidence_composes_to_the_whole_window_aggregate() -> Result<()> {
        let request = request(100, 119, 8)?;
        let fixture = Arc::new(vec![
            FixtureIdentity {
                block_number: 100,
                digest_left: 1,
                digest_right: 2,
            },
            FixtureIdentity {
                block_number: 103,
                digest_left: 4,
                digest_right: 8,
            },
            FixtureIdentity {
                block_number: 106,
                digest_left: 16,
                digest_right: 32,
            },
            FixtureIdentity {
                block_number: 108,
                digest_left: 64,
                digest_right: 128,
            },
            FixtureIdentity {
                block_number: 114,
                digest_left: 256,
                digest_right: 512,
            },
            FixtureIdentity {
                block_number: 119,
                digest_left: 1_024,
                digest_right: 2_048,
            },
        ]);

        let whole = fetch_fixture(&request, 20, Arc::clone(&fixture)).await?;
        let windowed = fetch_fixture(&request, 5, fixture).await?;

        assert_eq!(windowed.buckets, whole.buckets);
        assert_eq!(whole.query_count, 1);
        assert_eq!(windowed.query_count, 4);
        Ok(())
    }

    #[tokio::test]
    async fn memory_limit_400_halves_the_window_and_counts_each_attempt() -> Result<()> {
        const LIVE_MEMORY_LIMIT_BODY: &str = concat!(
            "\"Query memory limit exceeded: would use 14.06 GiB ",
            "(attempt to allocate chunk of 128.00 MiB bytes), maximum: 13.97 GiB.\" ",
            "(errorType invalid_request)"
        );
        let request = request(0, 7, 4)?;
        let attempted_ranges = Arc::new(Mutex::new(Vec::new()));
        let evidence = fetch_windowed_stored_log_identity_evidence_with(&request, 8, {
            let attempted_ranges = Arc::clone(&attempted_ranges);
            move |query_range| {
                let attempted_ranges = Arc::clone(&attempted_ranges);
                async move {
                    attempted_ranges
                        .lock()
                        .expect("attempted-range lock must not be poisoned")
                        .push(query_range);
                    if query_range.to_block - query_range.from_block + 1 > 4 {
                        return Err(CoinbaseSqlHttpError {
                            status: StatusCode::BAD_REQUEST,
                            body: LIVE_MEMORY_LIMIT_BODY.to_owned(),
                            attempt_count: 1,
                        }
                        .into());
                    }
                    raw_query_response_to_evidence(CoinbaseSqlRawQueryResponse {
                        rows: Vec::new(),
                        retry_count: 0,
                    })
                }
            }
        })
        .await?;

        assert_eq!(
            *attempted_ranges
                .lock()
                .expect("attempted-range lock must not be poisoned"),
            vec![
                BackfillBlockRange::new(0, 7)?,
                BackfillBlockRange::new(0, 3)?,
                BackfillBlockRange::new(4, 7)?,
            ]
        );
        assert_eq!(evidence.query_count, 3);
        Ok(())
    }

    #[tokio::test]
    async fn memory_limit_halving_is_bounded() -> Result<()> {
        let request = request(0, 31, 4)?;
        let attempt_count = Arc::new(Mutex::new(0usize));
        let error = fetch_windowed_stored_log_identity_evidence_with(&request, 32, {
            let attempt_count = Arc::clone(&attempt_count);
            move |_query_range| {
                let attempt_count = Arc::clone(&attempt_count);
                async move {
                    *attempt_count
                        .lock()
                        .expect("attempt-count lock must not be poisoned") += 1;
                    Err(CoinbaseSqlHttpError {
                        status: StatusCode::BAD_REQUEST,
                        body: json!({
                            "errorType": "invalid_request",
                            "errorMessage": "Query memory limit exceeded"
                        })
                        .to_string(),
                        attempt_count: 1,
                    }
                    .into())
                }
            }
        })
        .await
        .expect_err("a persistently memory-bound range must fail");

        assert!(error.to_string().contains("Query memory limit exceeded"));
        assert_eq!(
            *attempt_count
                .lock()
                .expect("attempt-count lock must not be poisoned"),
            MAX_EVIDENCE_WINDOW_HALVINGS + 1
        );
        Ok(())
    }

    async fn fetch_fixture(
        request: &StoredLogIdentityEvidenceRequest,
        window_blocks: i64,
        fixture: Arc<Vec<FixtureIdentity>>,
    ) -> Result<StoredLogIdentityEvidence> {
        let bucket_origin = request.range.from_block;
        let bucket_blocks = request.bucket_blocks;
        fetch_windowed_stored_log_identity_evidence_with(
            request,
            window_blocks,
            move |query_range| {
                let fixture = Arc::clone(&fixture);
                async move {
                    raw_query_response_to_evidence(CoinbaseSqlRawQueryResponse {
                        rows: fixture_response(&fixture, bucket_origin, bucket_blocks, query_range),
                        retry_count: 0,
                    })
                }
            },
        )
        .await
    }

    fn fixture_response(
        fixture: &[FixtureIdentity],
        bucket_origin: i64,
        bucket_blocks: i64,
        query_range: BackfillBlockRange,
    ) -> Vec<Value> {
        let mut buckets = BTreeMap::<i64, StoredLogIdentityBucket>::new();
        for identity in fixture.iter().filter(|identity| {
            (query_range.from_block..=query_range.to_block).contains(&identity.block_number)
        }) {
            let bucket = (identity.block_number - bucket_origin) / bucket_blocks;
            let aggregate = buckets.entry(bucket).or_insert(StoredLogIdentityBucket {
                bucket,
                ..StoredLogIdentityBucket::default()
            });
            aggregate.selected_log_count += 1;
            aggregate.digest_left ^= identity.digest_left;
            aggregate.digest_right ^= identity.digest_right;
        }
        buckets
            .into_values()
            .map(|bucket| {
                json!({
                    "bucket": bucket.bucket,
                    "selected_log_count": bucket.selected_log_count,
                    "digest_left": bucket.digest_left,
                    "digest_right": bucket.digest_right,
                })
            })
            .collect()
    }

    fn request(
        from_block: i64,
        to_block: i64,
        bucket_blocks: i64,
    ) -> Result<StoredLogIdentityEvidenceRequest> {
        Ok(StoredLogIdentityEvidenceRequest {
            chain: "base-mainnet".to_owned(),
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            topic0s: vec![
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            ],
            range: BackfillBlockRange::new(from_block, to_block)?,
            bucket_blocks,
        })
    }
}
