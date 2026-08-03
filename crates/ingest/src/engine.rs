use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use sqlx::PgPool;
use tokio::sync::Mutex;

use crate::{
    IngestError, Result,
    coinbase_sql::CoinbaseSqlSource,
    fetching::{estimated_write_bytes, fetch_selected_facts},
    manifest::load_watch_filter,
    plan::{
        BASE_COINBASE_SEAM_BLOCK, primary_source, publishable_heads, redo_source_target,
        sort_sources, target_number, validate_request,
    },
    provider::{ChainProvider, ProviderKind, SharedProvider, normalized_kind, provider_error},
};

mod live;
mod query;

const BLOCKS_PER_BATCH: i64 = 256;
const COINBASE_BLOCKS_PER_BATCH: i64 = 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Marker {
    pub number: i64,
    pub hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadMarkers {
    pub latest: Marker,
    pub safe: Option<Marker>,
    pub finalized: Option<Marker>,
}

#[derive(Clone, Debug)]
pub struct SourceDescriptor {
    pub key: String,
    pub kind: String,
    pub start_block: i64,
    pub endpoint: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceCursor {
    pub key: String,
    pub next_block: i64,
    pub target_block: Option<i64>,
    pub last_processed: Option<Marker>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceProgress {
    pub key: String,
    pub current: Option<Marker>,
    pub target: Marker,
}

#[derive(Clone, Debug)]
pub struct BatchRequest {
    pub chain_id: String,
    pub sources: Vec<SourceDescriptor>,
    pub cursors: Vec<SourceCursor>,
    pub redo_range: Option<(i64, i64)>,
    pub resume_current: Option<Marker>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchOutcome {
    pub complete: bool,
    pub current: Marker,
    pub target: Marker,
    pub live_handoff: Option<Marker>,
    pub heads: Option<HeadMarkers>,
    pub sources: Vec<SourceProgress>,
    pub estimated_write_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct LiveBatchRequest {
    pub chain_id: String,
    pub sources: Vec<SourceDescriptor>,
    pub live_handoff: Marker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveBatchOutcome {
    pub caught_up: bool,
    pub current: Marker,
    pub target: Marker,
    pub heads: Option<HeadMarkers>,
    pub estimated_write_bytes: u64,
}

pub struct Engine {
    pool: PgPool,
    providers: Mutex<BTreeMap<String, SharedProvider>>,
    coinbase_sources: Mutex<BTreeMap<String, Arc<CoinbaseSqlSource>>>,
}

impl Engine {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            providers: Mutex::new(BTreeMap::new()),
            coinbase_sources: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn run_batch(&self, request: BatchRequest) -> Result<BatchOutcome> {
        validate_request(&request)?;
        if request.redo_range.is_some() {
            self.run_redo_batch(request).await
        } else {
            self.run_normal_batch(request).await
        }
    }

    async fn run_normal_batch(&self, mut request: BatchRequest) -> Result<BatchOutcome> {
        sort_sources(&mut request.sources);
        let primary = primary_source(&request.sources)?;
        let primary_provider = self.provider(&request.chain_id, primary).await?;
        let head_snapshot = primary_provider
            .heads()
            .await
            .map_err(|error| provider_error("failed to fetch ingest target heads", error))?;
        if head_snapshot.safe.is_none() || head_snapshot.finalized.is_none() {
            return Err(IngestError::data_integrity(
                "ingest provider must report safe and finalized checkpoint heads",
            ));
        }
        let cursor_by_key = request
            .cursors
            .iter()
            .map(|cursor| (cursor.key.as_str(), cursor))
            .collect::<BTreeMap<_, _>>();
        let mut states = Vec::with_capacity(request.sources.len());
        for source in &request.sources {
            let cursor = cursor_by_key.get(source.key.as_str()).copied();
            let target_number = cursor
                .and_then(|cursor| cursor.target_block)
                .unwrap_or_else(|| target_number(source, &head_snapshot));
            if request.chain_id == "base-mainnet"
                && normalized_kind(&source.kind) == ProviderKind::Coinbase
                && target_number != BASE_COINBASE_SEAM_BLOCK
            {
                return Err(IngestError::data_integrity(format!(
                    "base-mainnet Coinbase SQL target must be seam block \
                     {BASE_COINBASE_SEAM_BLOCK}, got {target_number}"
                )));
            }
            if target_number < source.start_block {
                return Err(IngestError::configuration(format!(
                    "source {} target {target_number} is below its start block {}",
                    source.key, source.start_block
                )));
            }
            let resolver = self
                .resolver(&request.chain_id, source, &request.sources)
                .await?;
            let target = resolve_marker(&resolver, target_number).await?;
            states.push(NormalSourceState {
                source,
                next: cursor.map_or(source.start_block, |cursor| cursor.next_block),
                current: cursor.and_then(|cursor| cursor.last_processed.clone()),
                target,
            });
        }

        let active_index = states
            .iter()
            .position(|state| state.next <= state.target.number);
        let mut written_bytes = 0;
        if let Some(index) = active_index {
            let state = &mut states[index];
            let to = state
                .next
                .saturating_add(
                    if normalized_kind(&state.source.kind) == ProviderKind::Coinbase {
                        COINBASE_BLOCKS_PER_BATCH
                    } else {
                        BLOCKS_PER_BATCH
                    } - 1,
                )
                .min(state.target.number);
            let result = self
                .load_window(
                    &request.chain_id,
                    state.source,
                    &request.sources,
                    state.next,
                    to,
                )
                .await?;
            state.current = Some(result.marker);
            state.next = to.saturating_add(1);
            written_bytes = result.estimated_write_bytes;
        }
        let complete = states.iter().all(|state| state.next > state.target.number);
        let target = states
            .iter()
            .map(|state| state.target.clone())
            .max_by_key(|marker| marker.number)
            .expect("validated sources are nonempty");
        let current = if complete {
            target.clone()
        } else {
            states
                .iter()
                .filter_map(|state| state.current.clone())
                .max_by_key(|marker| marker.number)
                .ok_or_else(|| {
                    IngestError::data_integrity("ingest batch did not produce a current block")
                })?
        };
        let heads = publishable_heads(&current, &head_snapshot);
        Ok(BatchOutcome {
            complete,
            current: current.clone(),
            target: target.clone(),
            live_handoff: complete.then_some(target),
            heads: Some(heads),
            sources: states
                .into_iter()
                .map(|state| SourceProgress {
                    key: state.source.key.clone(),
                    current: state.current,
                    target: state.target,
                })
                .collect(),
            estimated_write_bytes: written_bytes,
        })
    }

    async fn run_redo_batch(&self, mut request: BatchRequest) -> Result<BatchOutcome> {
        sort_sources(&mut request.sources);
        let (range_from, range_to) = request.redo_range.expect("redo range is present");
        let from = request
            .resume_current
            .as_ref()
            .map_or(range_from, |marker| marker.number.saturating_add(1));
        let to = from.saturating_add(BLOCKS_PER_BATCH - 1).min(range_to);
        let mut written_bytes = 0u64;
        let mut progress = Vec::with_capacity(request.sources.len());

        for source in &request.sources {
            let source_target_number = redo_source_target(source, range_to);
            let resolver = self
                .resolver(&request.chain_id, source, &request.sources)
                .await?;
            let source_target = resolve_marker(&resolver, source_target_number).await?;
            let window_from = from.max(source.start_block);
            let window_to = to.min(source_target_number);
            let current = if window_from <= window_to {
                let loaded = self
                    .load_window(
                        &request.chain_id,
                        source,
                        &request.sources,
                        window_from,
                        window_to,
                    )
                    .await?;
                written_bytes = written_bytes.saturating_add(loaded.estimated_write_bytes);
                Some(loaded.marker)
            } else if to >= source_target_number {
                Some(source_target.clone())
            } else {
                request
                    .cursors
                    .iter()
                    .find(|cursor| cursor.key == source.key)
                    .and_then(|cursor| cursor.last_processed.clone())
            };
            progress.push(SourceProgress {
                key: source.key.clone(),
                current,
                target: source_target,
            });
        }
        let complete = to >= range_to;
        let primary = primary_source(&request.sources)?;
        let provider = self.provider(&request.chain_id, primary).await?;
        let current = resolve_marker(&provider, to).await?;
        let target = resolve_marker(&provider, range_to).await?;
        if complete {
            for source in &mut progress {
                source.current = Some(source.target.clone());
            }
        }
        Ok(BatchOutcome {
            complete,
            current,
            target,
            live_handoff: None,
            heads: None,
            sources: progress,
            estimated_write_bytes: written_bytes,
        })
    }

    async fn load_window(
        &self,
        chain_id: &str,
        source: &SourceDescriptor,
        all_sources: &[SourceDescriptor],
        from: i64,
        to: i64,
    ) -> Result<LoadedWindow> {
        let provider = self.resolver(chain_id, source, all_sources).await?;
        let numbers = (from..=to).collect::<Vec<_>>();
        let resolved = provider.resolve(&numbers).await.map_err(|error| {
            provider_error(
                &format!("failed to resolve ingest blocks {from}..={to}"),
                error,
            )
        })?;
        let mut filter = load_watch_filter(&self.pool, chain_id, from, to).await?;
        let coinbase = normalized_kind(&source.kind) == ProviderKind::Coinbase;
        let mut queries = filter.queries();
        let mut selected_by_identity = BTreeMap::new();
        let coinbase_source = if coinbase {
            Some(self.coinbase_source(chain_id, source).await?)
        } else {
            None
        };
        query::fetch_into(
            &provider,
            &resolved,
            coinbase_source.as_deref(),
            &queries,
            &mut selected_by_identity,
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
                .collect::<BTreeSet<_>>();
            let supplemental = filter.admit_registry_announcements(announcements, from, to);
            query::fetch_into(
                &provider,
                &resolved,
                coinbase_source.as_deref(),
                &supplemental,
                &mut selected_by_identity,
            )
            .await?;
            queries.extend(supplemental);
        }
        let mut selected = selected_by_identity.into_values().collect::<Vec<_>>();
        selected.retain(|log| {
            log.topics
                .first()
                .is_some_and(|topic0| filter.includes(&log.address, topic0, log.block_number))
        });
        let facts = fetch_selected_facts(&provider, &resolved, selected.clone()).await?;
        let estimated_write_bytes = estimated_write_bytes(&facts);
        crate::write::store(
            &self.pool,
            chain_id,
            &facts,
            coinbase.then_some((from, to, selected.as_slice(), queries.as_slice())),
        )
        .await?;
        let last = resolved
            .last()
            .ok_or_else(|| IngestError::data_integrity("ingest window resolved no blocks"))?;
        Ok(LoadedWindow {
            marker: Marker {
                number: last.number,
                hash: last.hash.clone(),
            },
            estimated_write_bytes,
        })
    }

    async fn resolver(
        &self,
        chain_id: &str,
        source: &SourceDescriptor,
        all_sources: &[SourceDescriptor],
    ) -> Result<SharedProvider> {
        if normalized_kind(&source.kind) == ProviderKind::Coinbase {
            let companion = all_sources
                .iter()
                .find(|candidate| normalized_kind(&candidate.kind) == ProviderKind::Rpc)
                .ok_or_else(|| {
                    IngestError::configuration(
                        "Coinbase SQL ingest requires a configured Base RPC source",
                    )
                })?;
            self.provider(chain_id, companion).await
        } else {
            self.provider(chain_id, source).await
        }
    }

    async fn provider(&self, chain_id: &str, source: &SourceDescriptor) -> Result<SharedProvider> {
        let key = format!("{}\0{}\0{}", chain_id, source.kind, source.endpoint);
        if let Some(provider) = self.providers.lock().await.get(&key).cloned() {
            return Ok(provider);
        }
        let provider = Arc::new(
            ChainProvider::new(chain_id, &source.kind, &source.endpoint).map_err(|error| {
                IngestError::with_source(
                    crate::ErrorKind::Configuration,
                    format!("failed to configure source {}", source.key),
                    error,
                )
            })?,
        );
        self.providers.lock().await.insert(key, provider.clone());
        Ok(provider)
    }

    async fn coinbase_source(
        &self,
        chain_id: &str,
        source: &SourceDescriptor,
    ) -> Result<Arc<CoinbaseSqlSource>> {
        let key = format!("{chain_id}\0{}", source.endpoint);
        if let Some(client) = self.coinbase_sources.lock().await.get(&key).cloned() {
            return Ok(client);
        }
        let client = Arc::new(CoinbaseSqlSource::new(chain_id, &source.endpoint).map_err(
            |error| {
                IngestError::with_source(
                    crate::ErrorKind::Configuration,
                    "failed to configure Coinbase SQL source",
                    error,
                )
            },
        )?);
        self.coinbase_sources
            .lock()
            .await
            .insert(key, client.clone());
        Ok(client)
    }
}

struct NormalSourceState<'a> {
    source: &'a SourceDescriptor,
    next: i64,
    current: Option<Marker>,
    target: Marker,
}

struct LoadedWindow {
    marker: Marker,
    estimated_write_bytes: u64,
}

async fn resolve_marker(provider: &ChainProvider, number: i64) -> Result<Marker> {
    let block = provider.resolve(&[number]).await.map_err(|error| {
        provider_error(&format!("failed to resolve target block {number}"), error)
    })?;
    let block = block
        .into_iter()
        .next()
        .ok_or_else(|| IngestError::data_integrity("provider omitted target block"))?;
    Ok(Marker {
        number: block.number,
        hash: block.hash,
    })
}
