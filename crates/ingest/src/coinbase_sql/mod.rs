use std::collections::BTreeMap;

use anyhow::{Result, bail};

use crate::provider::Log;

mod auth;
mod client;
mod error;
pub mod evidence;
mod pagination;
mod query;
mod rate_limit;
mod rows;
mod transport;

use client::CoinbaseSqlClient;
use query::CoinbaseSqlFilterPack;

const DEFAULT_URL: &str = "https://api.cdp.coinbase.com/platform/v2/data/query/run";
const KEY_ID_ENV: &str = "COINBASE_CDP_SQL_API_KEY_ID";
const KEY_SECRET_ENV: &str = "COINBASE_CDP_SQL_API_KEY_SECRET";
const PAGE_LIMIT: usize = 1_000;
const SQL_CHAR_LIMIT: usize = 100_000;
const QUERY_TIMEOUT_SECS: u64 = 120;
const RATE_LIMIT_QPS: u32 = 1;

#[derive(Clone)]
pub struct CoinbaseSqlSource {
    chain_id: String,
    client: CoinbaseSqlClient,
}

impl CoinbaseSqlSource {
    pub fn new(chain_id: &str, endpoint: &str) -> Result<Self> {
        let endpoint = if endpoint == "default" {
            DEFAULT_URL
        } else {
            endpoint
        };
        Ok(Self {
            chain_id: chain_id.to_owned(),
            client: CoinbaseSqlClient::new(
                endpoint,
                KEY_ID_ENV,
                KEY_SECRET_ENV,
                QUERY_TIMEOUT_SECS,
                RATE_LIMIT_QPS,
            )?,
        })
    }

    pub async fn fetch(
        &self,
        from_block: i64,
        to_block: i64,
        addresses: &[String],
        topic0s: &[String],
    ) -> Result<Vec<Log>> {
        if addresses.is_empty() || topic0s.is_empty() {
            return Ok(Vec::new());
        }
        let pack = CoinbaseSqlFilterPack {
            chain: self.chain_id.clone(),
            from_block,
            to_block,
            addresses: addresses.to_vec(),
            topic0s: topic0s.to_vec(),
            event_signatures: Vec::new(),
            scan_all_emitters: false,
            source_families: Vec::new(),
        };
        let packs = query::build_or_split_filter_pack(pack, SQL_CHAR_LIMIT, PAGE_LIMIT)?;
        let mut logs = BTreeMap::<(String, i64), Log>::new();
        for pack in packs {
            let rows = pagination::fetch_all_pages(&self.client, &pack, PAGE_LIMIT, SQL_CHAR_LIMIT)
                .await?;
            for row in rows {
                row.validate(from_block, to_block, addresses, topic0s)?;
                let log = row.identity_log();
                let key = (log.block_hash.clone(), log.log_index);
                if let Some(previous) = logs.insert(key.clone(), log.clone())
                    && previous != log
                {
                    bail!(
                        "Coinbase SQL returned conflicting identities for log {} {}",
                        key.0,
                        key.1
                    );
                }
            }
        }
        Ok(logs.into_values().collect())
    }
}

pub fn source_error(context: &str, error: anyhow::Error) -> crate::IngestError {
    let rendered = format!("{error:#}");
    let transient = rendered.to_ascii_lowercase();
    let kind = if [
        "status 429",
        "status 500",
        "status 502",
        "status 503",
        "status 504",
        "timed out",
        "timeout",
        "connection",
    ]
    .iter()
    .any(|needle| transient.contains(needle))
    {
        crate::ErrorKind::Transient
    } else {
        crate::ErrorKind::DataIntegrity
    };
    crate::IngestError::with_source(kind, context, anyhow::anyhow!(rendered))
}
