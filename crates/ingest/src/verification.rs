use crate::measurement::{self as memory, Measured};
use std::collections::BTreeMap;

use tokio::sync::Mutex;

use crate::{
    ErrorKind, IngestError, Result,
    manifest::WatchFilter,
    provider::{ChainProvider, Log, ResolvedBlock, provider_error},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationProviderKind {
    IndependentRpc,
    LocalReth,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerificationMarker {
    pub number: i64,
    pub hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerificationLog {
    pub block_hash: String,
    pub block_number: i64,
    pub transaction_hash: String,
    pub transaction_index: i64,
    pub log_index: i64,
    pub address: String,
    pub topics: Vec<String>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerificationBatch {
    pub end: VerificationMarker,
    pub logs: Vec<VerificationLog>,
    pub rpc_request_count: usize,
}

#[derive(Clone)]
pub struct VerificationProvider {
    kind: VerificationProviderKind,
    provider: ChainProvider,
    fetch_lock: std::sync::Arc<Mutex<()>>,
}

impl VerificationProvider {
    pub fn new(chain_id: &str, source_kind: &str, endpoint: &str) -> Result<Self> {
        let normalized = source_kind.trim().to_ascii_lowercase().replace('-', "_");
        let (kind, provider_kind) = match normalized.as_str() {
            "drpc" => (VerificationProviderKind::IndependentRpc, "rpc"),
            "reth" | "reth_db" => (VerificationProviderKind::LocalReth, "reth_db"),
            _ => {
                return Err(IngestError::configuration(format!(
                    "verification source kind {source_kind:?} is unsupported; expected drpc or \
                     reth_db"
                )));
            }
        };
        let provider = ChainProvider::new(chain_id, provider_kind, endpoint).map_err(|error| {
            IngestError::with_source(
                ErrorKind::Configuration,
                "failed to configure verification reference provider",
                error,
            )
        })?;
        Ok(Self {
            kind,
            provider,
            fetch_lock: std::sync::Arc::new(Mutex::new(())),
        })
    }

    pub const fn kind(&self) -> VerificationProviderKind {
        self.kind
    }

    pub async fn fetch(
        &self,
        mut filter: WatchFilter,
        from_block: i64,
        to_block: i64,
    ) -> Result<VerificationBatch> {
        let _fetch_guard = self.fetch_lock.lock().await;
        memory::observe(
            if self.kind == VerificationProviderKind::LocalReth {
                "compared_local_reth"
            } else {
                "compared_rpc"
            },
            Default::default,
        );
        if from_block < 0 || from_block > to_block {
            return Err(IngestError::configuration(format!(
                "verification range {from_block}..={to_block} is invalid"
            )));
        }
        let request_attempts_before = self.provider.verification_rpc_request_attempts();
        let resolved = self
            .provider
            .verification_blocks(from_block, to_block)
            .await
            .map_err(|error| {
                provider_error(
                    &format!("failed to resolve verification range {from_block}..={to_block}"),
                    error,
                )
            })?;
        memory::observe("reference_resolved", || resolved.footprint());
        let end = resolved
            .last()
            .filter(|block| block.number == to_block)
            .cloned()
            .ok_or_else(|| {
                IngestError::data_integrity(format!(
                    "verification reference omitted target block {to_block}"
                ))
            })?;

        let mut selected_by_identity = BTreeMap::new();
        let queries = filter.queries();
        memory::observe("watch_queries_before_mutation", || queries.footprint());
        let mut accounting = memory::Identity::default();
        fetch_queries(
            &self.provider,
            &resolved,
            &queries,
            &mut selected_by_identity,
            &mut accounting,
            "ordinary",
        )
        .await?;
        if let Some(announcement_topic0) = filter.registry_announcement_topic0() {
            let announcements = selected_by_identity
                .values()
                .filter(|log| {
                    log.topics
                        .first()
                        .is_some_and(|topic| topic.eq_ignore_ascii_case(announcement_topic0))
                })
                .map(|log| (log.address.clone(), log.block_number))
                .collect::<Vec<_>>();
            memory::observe("reference_announcements", || {
                let mut f = memory::Footprint::entries::<(String, i64)>(announcements.capacity());
                for (address, _) in &announcements {
                    f = f.combine(address.footprint());
                }
                f
            });
            let supplemental =
                filter.admit_registry_announcements(announcements, from_block, to_block);
            memory::observe("watch_queries_after_mutation", || {
                queries.footprint().combine(supplemental.footprint())
            });
            fetch_queries(
                &self.provider,
                &resolved,
                &supplemental,
                &mut selected_by_identity,
                &mut accounting,
                "supplemental",
            )
            .await?;
        }

        accounting.emit("provider_identity_map");
        let end_after = self
            .provider
            .resolve(&[to_block])
            .await
            .map_err(|error| {
                provider_error(
                    &format!("failed to recheck verification target block {to_block}"),
                    error,
                )
            })?
            .into_iter()
            .next()
            .ok_or_else(|| {
                IngestError::data_integrity(format!(
                    "verification reference omitted target block {to_block} on recheck"
                ))
            })?;
        memory::observe("reference_target_rechecked", || end_after.footprint());
        if end_after != end {
            return Err(IngestError::transient(format!(
                "verification reference target block changed during range lookup: {} became {}",
                end.hash, end_after.hash
            )));
        }
        let rpc_request_count = self
            .provider
            .verification_rpc_request_attempts()
            .saturating_sub(request_attempts_before);

        let mut logs = selected_by_identity
            .into_values()
            .filter(|log| {
                log.topics
                    .first()
                    .is_some_and(|topic0| filter.includes(&log.address, topic0, log.block_number))
            })
            .map(VerificationLog::from)
            .collect::<Vec<_>>();
        memory::observe("provider_before_sort", || logs.footprint());
        logs.sort_by_key(|log| {
            (
                log.block_number,
                log.transaction_index,
                log.log_index,
                log.block_hash.clone(),
            )
        });
        memory::observe("provider_final", || logs.footprint());
        Ok(VerificationBatch {
            end: VerificationMarker {
                number: end.number,
                hash: end.hash,
            },
            logs,
            rpc_request_count,
        })
    }
}

async fn fetch_queries(
    provider: &ChainProvider,
    resolved: &[ResolvedBlock],
    queries: &[crate::manifest::WatchQuery],
    selected_by_identity: &mut BTreeMap<(String, i64), Log>,
    accounting: &mut memory::Identity,
    kind: &'static str,
) -> Result<()> {
    for (ordinal, query) in queries.iter().enumerate() {
        let logs = memory::query(
            kind,
            ordinal,
            query.from_block,
            query.to_block,
            provider.verification_logs(
                resolved,
                query.from_block,
                query.to_block,
                &query.addresses,
                &query.topic0s,
            ),
        )
        .await
        .map_err(|error| provider_error("failed to fetch verification logs", error))?;
        let raw = memory::returned(
            "provider",
            kind,
            ordinal,
            query.from_block,
            query.to_block,
            || logs.footprint(),
        );
        accounting.query(raw, memory::inline(&logs).owned);
        for log in logs {
            let key = (log.block_hash.clone(), log.log_index);
            accounting.consume(&log);
            if let Some(previous) = accounting.inserted(
                selected_by_identity.insert(key.clone(), log.clone()),
                &selected_by_identity,
                &key,
            ) && previous != log
            {
                accounting.emit("provider_identity_conflict");
                return Err(IngestError::data_integrity(format!(
                    "verification reference returned conflicting log identity {} {}",
                    key.0, key.1
                )));
            }
        }
    }
    Ok(())
}

impl From<Log> for VerificationLog {
    fn from(log: Log) -> Self {
        Self {
            block_hash: log.block_hash,
            block_number: log.block_number,
            transaction_hash: log.transaction_hash,
            transaction_index: log.transaction_index,
            log_index: log.log_index,
            address: log.address,
            topics: log.topics,
            data: log.data,
        }
    }
}

impl Measured for crate::manifest::WatchQuery {
    fn footprint(&self) -> memory::Footprint {
        self.addresses
            .footprint()
            .combine(self.topic0s.footprint())
            .combine(memory::Footprint::entries::<i64>(2))
    }
}
