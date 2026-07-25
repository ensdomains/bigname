use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use serde_json::Value;

#[path = "coinbase_sql/auth.rs"]
mod auth;
#[path = "coinbase_sql/client.rs"]
mod client;
#[path = "coinbase_sql/pagination.rs"]
mod pagination;
#[path = "coinbase_sql/planner.rs"]
mod planner;
#[path = "coinbase_sql/query.rs"]
mod query;
#[path = "coinbase_sql/rate_limit.rs"]
mod rate_limit;
#[path = "coinbase_sql/rows.rs"]
mod rows;

use crate::provider::{ProviderLog, ProviderReceipt, ProviderTransaction};

use super::{
    CoinbaseSqlBackfillConfig, CoinbaseSqlValidationMode, HistoricalBackfillSourceOps,
    HistoricalLogPayload, HistoricalLogPayloadRequest, HistoricalLogValidationFilter,
    stored_verification::{
        StoredLogIdentityBucket, StoredLogIdentityEvidence, StoredLogIdentityEvidenceRequest,
        StoredLogIdentityEvidenceSource,
    },
};

pub(crate) use planner::load_backfill_topic_plan;
#[cfg(test)]
#[path = "coinbase_sql/tests.rs"]
mod tests;

pub(crate) const DEFAULT_COINBASE_SQL_API_KEY_ID_ENV: &str = "COINBASE_CDP_SQL_API_KEY_ID";
pub(crate) const DEFAULT_COINBASE_SQL_API_KEY_SECRET_ENV: &str = "COINBASE_CDP_SQL_API_KEY_SECRET";
const COINBASE_SQL_DEFAULT_RUN_URL: &str =
    "https://api.cdp.coinbase.com/platform/v2/data/query/run";

#[derive(Clone, Debug)]
pub(crate) struct CoinbaseSqlSourceRegistry {
    urls_by_chain: BTreeMap<String, String>,
    api_key_id_env: String,
    api_key_secret_env: String,
    config: CoinbaseSqlBackfillConfig,
}

impl CoinbaseSqlSourceRegistry {
    pub(crate) fn from_entries(
        entries: &[String],
        api_key_id_env: String,
        api_key_secret_env: String,
        config: CoinbaseSqlBackfillConfig,
    ) -> Result<Self> {
        let mut urls_by_chain = BTreeMap::new();
        for entry in entries {
            let (chain, url) = parse_chain_source_entry(
                entry,
                "Coinbase SQL URL",
                "<chain>=<https://api.cdp.coinbase.com/platform/v2/data/query/run>",
            )?;
            if urls_by_chain.insert(chain.clone(), url).is_some() {
                bail!("duplicate Coinbase SQL source configuration for {chain}");
            }
        }

        Ok(Self {
            urls_by_chain,
            api_key_id_env,
            api_key_secret_env,
            config,
        })
    }

    pub(crate) fn has_source_for(&self, chain: &str) -> bool {
        self.urls_by_chain.contains_key(chain)
    }

    pub(crate) fn source_for(&self, chain: &str) -> Result<Option<CoinbaseSqlBackfillSource>> {
        let Some(url) = self.urls_by_chain.get(chain) else {
            return Ok(None);
        };

        CoinbaseSqlBackfillSource::new(
            chain.to_owned(),
            url.clone(),
            self.api_key_id_env.clone(),
            self.api_key_secret_env.clone(),
            self.config.clone(),
        )
        .map(Some)
    }
}

fn parse_chain_source_entry(
    entry: &str,
    source_label: &str,
    expected_shape: &str,
) -> Result<(String, String)> {
    let (chain, value) = entry.split_once('=').with_context(|| {
        format!("invalid {source_label} source entry {entry}; expected {expected_shape}")
    })?;
    let chain = chain.trim();
    let value = value.trim();
    if chain.is_empty() || value.is_empty() {
        bail!("invalid {source_label} source entry {entry}; expected non-empty {expected_shape}");
    }

    Ok((chain.to_owned(), value.to_owned()))
}

#[derive(Clone)]
pub(crate) struct CoinbaseSqlBackfillSource {
    chain: String,
    config: CoinbaseSqlBackfillConfig,
    client: client::CoinbaseSqlClient,
}

impl CoinbaseSqlBackfillSource {
    fn new(
        chain: String,
        url: String,
        api_key_id_env: String,
        api_key_secret_env: String,
        config: CoinbaseSqlBackfillConfig,
    ) -> Result<Self> {
        config.validate()?;
        let url = if url == "default" {
            COINBASE_SQL_DEFAULT_RUN_URL.to_owned()
        } else {
            url
        };
        let client =
            client::CoinbaseSqlClient::new(&url, &api_key_id_env, &api_key_secret_env, &config)?;
        Ok(Self {
            chain,
            config,
            client,
        })
    }

    pub(crate) fn with_query_attempt_recorder(
        mut self,
        pool: sqlx::PgPool,
        backfill_job_id: i64,
    ) -> Self {
        self.client = self
            .client
            .with_query_attempt_recorder(pool, backfill_job_id);
        self
    }
}

impl HistoricalBackfillSourceOps for CoinbaseSqlBackfillSource {
    fn records_provider_query_attempts_incrementally(&self) -> bool {
        self.client.records_query_attempts_incrementally()
    }

    async fn fetch_selected_log_payloads(
        &self,
        request: HistoricalLogPayloadRequest<'_>,
    ) -> Result<HistoricalLogPayload> {
        if request.chain != self.chain {
            bail!(
                "Coinbase SQL source configured for chain {} was asked to fetch {}",
                self.chain,
                request.chain
            );
        }

        let mut logs_by_block = BTreeMap::<i64, Vec<ProviderLog>>::new();
        let transactions_by_block = BTreeMap::<i64, Vec<ProviderTransaction>>::new();
        let receipts_by_block = BTreeMap::<i64, Vec<ProviderReceipt>>::new();
        let mut stats = super::CoinbaseSqlFetchStats::default();
        let mut validation_filters = Vec::new();
        let mut seen_log_identities = BTreeSet::new();
        let mut logs_filtered_by_selected_target_index = true;
        let mut retained_rows_need_validation_provider_payload = false;
        let resolved_by_number = if request.validation_mode == CoinbaseSqlValidationMode::Sample
            || request.resolved_blocks.is_empty()
        {
            None
        } else {
            Some(
                request
                    .resolved_blocks
                    .iter()
                    .map(|block| (block.block_number, block.block_hash.clone()))
                    .collect::<BTreeMap<_, _>>(),
            )
        };
        let packs = planner::build_filter_packs(&request);
        let page_limit = self.config.effective_page_limit();

        for pack in packs {
            for split_pack in
                query::build_or_split_filter_pack(pack, self.config.sql_char_limit, page_limit)?
            {
                let materializes_all_scan_emitters =
                    materializes_all_scan_all_emitters(&split_pack);
                logs_filtered_by_selected_target_index &=
                    split_pack.scan_all_emitters && !materializes_all_scan_emitters;
                validation_filters.push(HistoricalLogValidationFilter {
                    from_block: split_pack.from_block,
                    to_block: split_pack.to_block,
                    addresses: split_pack.addresses.clone(),
                    topic0s: split_pack.topic0s.clone(),
                });
                let pages = pagination::fetch_all_pages(
                    &self.client,
                    &split_pack,
                    page_limit,
                    self.config.sql_char_limit,
                )
                .await?;
                stats.merge(pages.stats);
                for row in pages.rows {
                    row.validate_against_filter_pack(&split_pack, resolved_by_number.as_ref())?;
                    let requires_validation_provider_data = row.requires_validation_provider_data;
                    let log = row.to_provider_log()?;
                    if split_pack.scan_all_emitters
                        && !materializes_all_scan_emitters
                        && !request
                            .selected_target_index
                            .contains(&log.address, log.block_number)
                    {
                        continue;
                    }
                    retained_rows_need_validation_provider_payload |=
                        requires_validation_provider_data;
                    push_deduped_log(&mut logs_by_block, &mut seen_log_identities, log);
                }
            }
        }

        for logs in logs_by_block.values_mut() {
            logs.sort_by(|left, right| {
                left.transaction_index
                    .cmp(&right.transaction_index)
                    .then_with(|| left.log_index.cmp(&right.log_index))
            });
        }
        let logs_need_validation_provider_payload =
            coinbase_sql_logs_need_validation_provider_payload(
                request.validation_mode,
                !logs_by_block.is_empty(),
                retained_rows_need_validation_provider_payload,
            );

        Ok(HistoricalLogPayload {
            logs_by_block,
            transactions_by_block,
            receipts_by_block,
            logs_need_validation_provider_payload,
            logs_filtered_by_selected_target_index,
            validation_filters,
            validation_mode: request.validation_mode,
            source_stats: stats,
        })
    }
}

impl StoredLogIdentityEvidenceSource for CoinbaseSqlBackfillSource {
    fn records_provider_query_attempts_incrementally(&self) -> bool {
        self.client.records_query_attempts_incrementally()
    }

    fn fetch_stored_log_identity_evidence<'a>(
        &'a self,
        request: StoredLogIdentityEvidenceRequest,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<StoredLogIdentityEvidence>> + Send + 'a>,
    > {
        Box::pin(async move {
            if request.chain != self.chain {
                bail!(
                    "Coinbase SQL source configured for chain {} was asked to verify {}",
                    self.chain,
                    request.chain
                );
            }
            let sql = stored_log_identity_evidence_query(&request)?;
            let response = self.client.run_raw_query(&sql).await?;
            let buckets = response
                .rows
                .into_iter()
                .map(stored_log_identity_bucket_from_value)
                .collect::<Result<Vec<_>>>()?;
            Ok(StoredLogIdentityEvidence {
                buckets,
                query_count: 1 + response.retry_count,
            })
        })
    }
}

fn stored_log_identity_evidence_query(
    request: &StoredLogIdentityEvidenceRequest,
) -> Result<String> {
    if request.range.from_block > request.range.to_block {
        bail!("Coinbase SQL stored verification range is inverted");
    }
    if request.bucket_blocks <= 0 {
        bail!("Coinbase SQL stored verification bucket size must be positive");
    }
    if request.topic0s.is_empty() {
        bail!("Coinbase SQL stored verification requires at least one topic0");
    }
    let network = match request.chain.as_str() {
        "base-mainnet" | "base" => "base",
        "base-sepolia" => "base_sepolia",
        chain => bail!("Coinbase SQL backfill currently supports Base chains only, got {chain}"),
    };
    let address = sql_string_literal(&request.address);
    let topics = request
        .topic0s
        .iter()
        .map(|topic| sql_string_literal(topic))
        .collect::<Vec<_>>()
        .join(", ");
    let action = "CASE WHEN toString(l.action) IN ('1', 'added') THEN 1 \
        WHEN toString(l.action) IN ('-1', 'removed') THEN -1 ELSE 0 END";
    let from_block = request.range.from_block;
    let to_block = request.range.to_block;
    let bucket_blocks = request.bucket_blocks;
    Ok(format!(
        r#"WITH selected_rows AS (
  SELECT
    l.block_number,
    l.block_hash,
    l.transaction_hash,
    l.log_index,
    l.address,
    {action} AS action_delta
  FROM {network}.events l
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
  intDiv(toInt64(block_number) - {from_block}, {bucket_blocks}) AS bucket,
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
ORDER BY bucket"#
    ))
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn stored_log_identity_bucket_from_value(value: Value) -> Result<StoredLogIdentityBucket> {
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

fn coinbase_sql_logs_need_validation_provider_payload(
    validation_mode: CoinbaseSqlValidationMode,
    has_retained_logs: bool,
    retained_rows_need_validation_provider_payload: bool,
) -> bool {
    match validation_mode {
        CoinbaseSqlValidationMode::Full => true,
        CoinbaseSqlValidationMode::Sample => {
            has_retained_logs || retained_rows_need_validation_provider_payload
        }
    }
}

fn materializes_all_scan_all_emitters(pack: &query::CoinbaseSqlFilterPack) -> bool {
    pack.scan_all_emitters
        && pack.source_families.len() == 1
        && pack.source_families[0] == "basenames_base_registry"
}

fn push_deduped_log(
    logs_by_block: &mut BTreeMap<i64, Vec<ProviderLog>>,
    seen_log_identities: &mut BTreeSet<CoinbaseSqlLogIdentity>,
    log: ProviderLog,
) {
    if seen_log_identities.insert(CoinbaseSqlLogIdentity::from_log(&log)) {
        logs_by_block.entry(log.block_number).or_default().push(log);
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CoinbaseSqlLogIdentity {
    block_hash: String,
    transaction_hash: String,
    transaction_index: i64,
    log_index: i64,
    address: String,
}

impl CoinbaseSqlLogIdentity {
    fn from_log(log: &ProviderLog) -> Self {
        Self {
            block_hash: log.block_hash.to_ascii_lowercase(),
            transaction_hash: log.transaction_hash.to_ascii_lowercase(),
            transaction_index: log.transaction_index,
            log_index: log.log_index,
            address: log.address.to_ascii_lowercase(),
        }
    }
}
