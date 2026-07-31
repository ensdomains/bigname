use std::collections::BTreeMap;

use anyhow::{Result, bail};
use tracing::warn;

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
const PAGE_LIMIT: usize = 10_000;
const SQL_CHAR_LIMIT: usize = 10_000;
const QUERY_TIMEOUT_SECS: u64 = 120;
const RATE_LIMIT_QPS: u32 = 1;
const COINBASE_BLOCKS_PER_BATCH: i64 = 1_024;

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
        validate_filter_scope(addresses)?;
        if topic0s.is_empty() {
            return Ok(Vec::new());
        }
        let mut window = AdaptiveWindow::new(from_block, to_block)?;
        let mut logs = BTreeMap::<(String, i64), Log>::new();
        while let Some((window_from, window_to)) = window.current() {
            match self
                .fetch_window(window_from, window_to, addresses, topic0s)
                .await
            {
                Ok(window_logs) => {
                    merge_logs(&mut logs, window_logs)?;
                    window.advance();
                }
                Err(error) if window.halve() => {
                    warn!(
                        component = "coinbase_sql",
                        attempted_from_block = window_from,
                        attempted_to_block = window_to,
                        next_window_blocks = window.blocks,
                        error = %format!("{error:#}"),
                        "Coinbase SQL bulk window failed; retrying with a smaller window"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        Ok(logs.into_values().collect())
    }

    async fn fetch_window(
        &self,
        from_block: i64,
        to_block: i64,
        addresses: &[String],
        topic0s: &[String],
    ) -> Result<Vec<Log>> {
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
                merge_log(&mut logs, log)?;
            }
        }
        Ok(logs.into_values().collect())
    }
}

fn validate_filter_scope(addresses: &[String]) -> Result<()> {
    if addresses.is_empty() {
        bail!("all-emitter scopes are not supported by the Coinbase SQL bulk source");
    }
    Ok(())
}

fn merge_logs(
    logs: &mut BTreeMap<(String, i64), Log>,
    incoming: impl IntoIterator<Item = Log>,
) -> Result<()> {
    for log in incoming {
        merge_log(logs, log)?;
    }
    Ok(())
}

fn merge_log(logs: &mut BTreeMap<(String, i64), Log>, log: Log) -> Result<()> {
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
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdaptiveWindow {
    next: i64,
    through: i64,
    blocks: i64,
}

impl AdaptiveWindow {
    fn new(from_block: i64, to_block: i64) -> Result<Self> {
        if from_block > to_block {
            bail!("Coinbase SQL window start {from_block} is after end {to_block}");
        }
        Ok(Self {
            next: from_block,
            through: to_block,
            blocks: to_block
                .saturating_sub(from_block)
                .saturating_add(1)
                .min(COINBASE_BLOCKS_PER_BATCH),
        })
    }

    fn current(self) -> Option<(i64, i64)> {
        (self.next <= self.through).then(|| {
            (
                self.next,
                self.next
                    .saturating_add(self.blocks.saturating_sub(1))
                    .min(self.through),
            )
        })
    }

    fn halve(&mut self) -> bool {
        let Some((from_block, to_block)) = self.current() else {
            return false;
        };
        let current_blocks = to_block.saturating_sub(from_block).saturating_add(1);
        if current_blocks <= 1 {
            return false;
        }
        self.blocks = (current_blocks / 2).max(1);
        true
    }

    fn advance(&mut self) {
        let (_, to_block) = self.current().expect("an active window can advance");
        self.next = to_block.saturating_add(1);
        self.blocks = self.blocks.saturating_mul(2).min(COINBASE_BLOCKS_PER_BATCH);
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
        "status 520",
        "status 521",
        "status 522",
        "status 524",
        "timed out",
        "timeout",
        "connection",
        "could not resolve host",
        "couldn't resolve host",
        "could not resolve proxy",
        "couldn't resolve proxy",
        "failed to connect",
        "couldn't connect",
        "tls",
        "ssl",
        "proxy",
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn all_emitter_scope_is_a_terminal_bulk_source_error() {
        let error = validate_filter_scope(&[]).expect_err("all-emitter scope must fail");
        let error = source_error("failed to fetch Coinbase SQL logs", error);

        assert_eq!(error.kind(), ErrorKind::DataIntegrity);
        assert!(
            error
                .to_string()
                .contains("all-emitter scopes are not supported")
        );
    }

    #[test]
    fn live_proven_query_limits_are_restored() {
        assert_eq!(PAGE_LIMIT, 10_000);
        assert_eq!(SQL_CHAR_LIMIT, 10_000);
    }

    #[test]
    fn failed_bulk_window_halves_until_one_block() -> Result<()> {
        let mut window = AdaptiveWindow::new(10, 17)?;

        assert_eq!(window.current(), Some((10, 17)));
        assert!(window.halve());
        assert_eq!(window.current(), Some((10, 13)));
        assert!(window.halve());
        assert_eq!(window.current(), Some((10, 11)));
        assert!(window.halve());
        assert_eq!(window.current(), Some((10, 10)));
        assert!(!window.halve());

        window.advance();
        assert_eq!(window.current(), Some((11, 12)));
        Ok(())
    }

    #[test]
    fn successful_bulk_windows_regrow_to_batch_cap_after_halving() -> Result<()> {
        let mut window = AdaptiveWindow::new(0, COINBASE_BLOCKS_PER_BATCH * 4 - 1)?;

        assert_eq!(window.current(), Some((0, COINBASE_BLOCKS_PER_BATCH - 1)));
        assert!(window.halve());
        assert!(window.halve());
        assert_eq!(window.blocks, COINBASE_BLOCKS_PER_BATCH / 4);

        window.advance();
        assert_eq!(window.blocks, COINBASE_BLOCKS_PER_BATCH / 2);
        window.advance();
        assert_eq!(window.blocks, COINBASE_BLOCKS_PER_BATCH);
        window.advance();
        assert_eq!(window.blocks, COINBASE_BLOCKS_PER_BATCH);
        Ok(())
    }

    #[test]
    fn coinbase_edge_gateway_status_is_transient_after_client_retries() {
        let status = reqwest::StatusCode::from_u16(520).expect("520 is a valid HTTP status");
        let error = error::CoinbaseSqlHttpError {
            status,
            body: "edge gateway unavailable".to_owned(),
        };
        let error = source_error("failed to fetch Coinbase SQL logs", error.into());

        assert_eq!(error.kind(), ErrorKind::Transient);
    }

    #[test]
    fn curl_transport_failures_are_transient() {
        for message in [
            "curl: (6) Could not resolve host: api.cdp.coinbase.com",
            "curl: (35) TLS connect error",
            "curl: (7) Failed to connect to proxy port 443: Connection refused",
        ] {
            let error = source_error(
                "failed to fetch Coinbase SQL logs",
                anyhow::anyhow!(message),
            );
            assert_eq!(error.kind(), ErrorKind::Transient, "{message}");
        }
    }
}
