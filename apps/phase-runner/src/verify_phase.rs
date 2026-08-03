use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use bigname_ingest::{
    BASE_COINBASE_SEAM_BLOCK, VerificationBatch, VerificationProvider, VerificationProviderKind,
    WatchFilter,
};
use tracing::info;

use crate::{
    config::SourceConfig,
    database::VerificationDatabase,
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{
        Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName, PhaseProgress, RunMode,
        VerificationLevel,
    },
    verify_compare,
    verify_store::VerificationStore,
};

const VERIFICATION_BATCH_BLOCKS: i64 = 131_072;

pub type VerificationReferenceFuture<'a> =
    Pin<Box<dyn Future<Output = RunnerResult<VerificationBatch>> + Send + 'a>>;

pub trait VerificationReferenceProvider: Send + Sync {
    fn preflight(&self, source: &VerificationSource) -> RunnerResult<()>;

    fn fetch<'a>(
        &'a self,
        source: &'a VerificationSource,
        filter: WatchFilter,
        from_block: i64,
        to_block: i64,
    ) -> VerificationReferenceFuture<'a>;
}

#[derive(Clone)]
pub struct VerificationSource {
    chain_id: String,
    source_key: String,
    source_kind: String,
    endpoint: Arc<str>,
    provider_kind: VerificationProviderKind,
    level: VerificationLevel,
    cross_check_through: Option<i64>,
}

impl VerificationSource {
    pub fn chain_id(&self) -> &str {
        &self.chain_id
    }

    pub fn source_key(&self) -> &str {
        &self.source_key
    }

    pub fn source_kind(&self) -> &str {
        &self.source_kind
    }

    pub const fn provider_kind(&self) -> VerificationProviderKind {
        self.provider_kind
    }

    pub const fn verification_level(&self) -> VerificationLevel {
        self.level
    }
}

pub struct VerifyPhase {
    store: VerificationStore,
    reference: Arc<dyn VerificationReferenceProvider>,
}

impl VerifyPhase {
    pub fn new(database: VerificationDatabase) -> Self {
        Self::with_reference_provider(database, Arc::new(ProductionReferences::default()))
    }

    pub fn with_reference_provider(
        database: VerificationDatabase,
        reference: Arc<dyn VerificationReferenceProvider>,
    ) -> Self {
        Self {
            store: VerificationStore::new(database),
            reference,
        }
    }
}

impl Phase for VerifyPhase {
    fn name(&self) -> PhaseName {
        PhaseName::Verify
    }

    fn preflight(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
        mode: &RunMode,
    ) -> RunnerResult<()> {
        let source = select_source(chain_id, sources)?;
        validate_source_range(&source, mode)?;
        self.reference.preflight(&source)
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let source = select_source(&context.chain_id, &context.sources)?;
            let plan = BatchPlan::new(&context, &source, &self.store).await?;
            let stored = self
                .store
                .load_batch(&context.chain_id, plan.from, plan.to)
                .await?;
            let reference = self
                .reference
                .fetch(&source, stored.filter.clone(), plan.from, plan.to)
                .await?;
            if let Some(mismatch) = verify_compare::compare(&stored, &reference) {
                return Err(RunnerError::verification_mismatch(format!(
                    "chain {} source {} range {}..={}: {mismatch}",
                    context.chain_id,
                    source.source_key(),
                    plan.from,
                    plan.to
                )));
            }
            let reported_level = if let Some(range) = context.mode.range() {
                self.store
                    .level_for_redo(&context.chain_id, range, source.verification_level())
                    .await?
            } else {
                normal_extent_level(&context, source.verification_level())?
            };

            info!(
                chain_id = context.chain_id,
                source_key = source.source_key(),
                reference_kind = ?source.provider_kind(),
                reference_verification_level = source.verification_level().as_str(),
                reported_verification_level = reported_level.as_str(),
                from_block = plan.from,
                to_block = plan.to,
                reference_rpc_request_count = reference.rpc_request_count,
                "stored history verification batch matched its reference"
            );
            let progress = PhaseProgress {
                current: Some(stored.end),
                target: Some(plan.target),
                verification_level: Some(reported_level),
                ..PhaseProgress::default()
            };
            if plan.to == progress.target.as_ref().expect("target was set").number {
                Ok(PhaseBatchOutcome::Complete(progress))
            } else {
                Ok(PhaseBatchOutcome::Continue(progress))
            }
        })
    }
}

struct BatchPlan {
    from: i64,
    to: i64,
    target: BlockMarker,
}

impl BatchPlan {
    async fn new(
        context: &PhaseContext,
        source: &VerificationSource,
        store: &VerificationStore,
    ) -> RunnerResult<Self> {
        let range = context.mode.range();
        let start = match range {
            Some(range) => range.from,
            None => store.ingest_start(&context.chain_id).await?,
        };
        let mut target = if let Some(target) = context.resume.target.clone() {
            target
        } else if let Some(range) = range {
            context
                .available_heads
                .as_ref()
                .map(|heads| heads.latest.clone())
                .filter(|marker| marker.number == range.to)
                .ok_or_else(|| {
                    RunnerError::data_integrity(format!(
                        "verification redo target {} for chain {} is unavailable",
                        range.to, context.chain_id
                    ))
                })?
        } else {
            context
                .available_heads
                .as_ref()
                .and_then(|heads| heads.finalized.clone())
                .ok_or_else(|| {
                    RunnerError::transient(format!(
                        "chain {} has no finalized head available for verification",
                        context.chain_id
                    ))
                })?
        };
        validate_source_range(source, &context.mode)?;
        if let Some(cross_check_through) = source.cross_check_through
            && target.number > cross_check_through
        {
            target = store
                .finalized_marker(&context.chain_id, cross_check_through)
                .await?;
        }
        if target.number < start {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!(
                    "verification target {} for chain {} is below scan start {start}",
                    target.number, context.chain_id
                ),
            ));
        }
        let from = context
            .resume
            .current
            .as_ref()
            .map(|marker| {
                marker.number.checked_add(1).ok_or_else(|| {
                    RunnerError::data_integrity("verification cursor block number overflowed")
                })
            })
            .transpose()?
            .unwrap_or(start);
        if from > target.number {
            return Err(RunnerError::data_integrity(format!(
                "verification cursor {from} for chain {} is above target {}",
                context.chain_id, target.number
            )));
        }
        let to = from
            .checked_add(VERIFICATION_BATCH_BLOCKS - 1)
            .unwrap_or(i64::MAX)
            .min(target.number);
        Ok(Self { from, to, target })
    }
}

fn normal_extent_level(
    context: &PhaseContext,
    source_level: VerificationLevel,
) -> RunnerResult<VerificationLevel> {
    match (
        context.resume.current.as_ref(),
        context.resume.verification_level,
    ) {
        (None, _) => Ok(source_level),
        (Some(_), Some(retained)) => Ok(weakest_level(retained, source_level)),
        (Some(current), None) => Err(RunnerError::data_integrity(format!(
            "verification resume for chain {} at block {} has no retained verification level",
            context.chain_id, current.number
        ))),
    }
}

const fn weakest_level(
    retained: VerificationLevel,
    source: VerificationLevel,
) -> VerificationLevel {
    match (retained, source) {
        (VerificationLevel::QuickSynced, _) | (_, VerificationLevel::QuickSynced) => {
            VerificationLevel::QuickSynced
        }
        (VerificationLevel::CrossChecked, _) | (_, VerificationLevel::CrossChecked) => {
            VerificationLevel::CrossChecked
        }
        (VerificationLevel::NodeChecked, VerificationLevel::NodeChecked) => {
            VerificationLevel::NodeChecked
        }
    }
}

fn validate_source_range(source: &VerificationSource, mode: &RunMode) -> RunnerResult<()> {
    let Some((cross_check_through, range)) = source.cross_check_through.zip(mode.range()) else {
        return Ok(());
    };
    if range.to <= cross_check_through {
        return Ok(());
    }
    Err(RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "base-mainnet dRPC verification cannot independently cross-check block {} after its \
             ingest seam {cross_check_through}",
            range.to
        ),
    ))
}

#[derive(Default)]
struct ProductionReferences {
    providers: Mutex<BTreeMap<(String, String, String, String), VerificationProvider>>,
}

impl ProductionReferences {
    fn provider(&self, source: &VerificationSource) -> RunnerResult<VerificationProvider> {
        let key = (
            source.chain_id.clone(),
            source.source_key.clone(),
            source.source_kind.clone(),
            source.endpoint.to_string(),
        );
        let mut providers = self.providers.lock().map_err(|_| {
            RunnerError::data_integrity("verification provider cache lock was poisoned")
        })?;
        if let Some(provider) = providers.get(&key) {
            return Ok(provider.clone());
        }
        let provider =
            VerificationProvider::new(source.chain_id(), source.source_kind(), &source.endpoint)
                .map_err(map_ingest_error)?;
        if provider.kind() != source.provider_kind() {
            return Err(RunnerError::data_integrity(format!(
                "verification provider kind for chain {} source {} changed during construction",
                source.chain_id(),
                source.source_key()
            )));
        }
        providers.insert(key, provider.clone());
        Ok(provider)
    }
}

impl VerificationReferenceProvider for ProductionReferences {
    fn preflight(&self, source: &VerificationSource) -> RunnerResult<()> {
        self.provider(source).map(|_| ())
    }

    fn fetch<'a>(
        &'a self,
        source: &'a VerificationSource,
        filter: WatchFilter,
        from_block: i64,
        to_block: i64,
    ) -> VerificationReferenceFuture<'a> {
        Box::pin(async move {
            self.provider(source)?
                .fetch(filter, from_block, to_block)
                .await
                .map_err(map_ingest_error)
        })
    }
}

fn select_source(chain_id: &str, sources: &[SourceConfig]) -> RunnerResult<VerificationSource> {
    let candidates = sources
        .iter()
        .filter_map(|source| {
            let kind = normalized_kind(&source.source_kind);
            match kind.as_str() {
                "drpc" => Some((
                    source,
                    VerificationProviderKind::IndependentRpc,
                    VerificationLevel::CrossChecked,
                )),
                "reth" | "reth_db" => Some((
                    source,
                    VerificationProviderKind::LocalReth,
                    VerificationLevel::NodeChecked,
                )),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    if candidates.len() != 1 {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            format!(
                "chain {chain_id} must configure exactly one verification reference of kind drpc \
                 or reth_db; found {}",
                candidates.len()
            ),
        ));
    }
    let (source, provider_kind, level) = candidates[0];
    if chain_id == "base-mainnet"
        && provider_kind == VerificationProviderKind::IndependentRpc
        && source.start_block_number != BASE_COINBASE_SEAM_BLOCK
    {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            format!(
                "base-mainnet dRPC verification source must start at the fixed Coinbase-to-dRPC \
                 ingest seam {BASE_COINBASE_SEAM_BLOCK}; got {}",
                source.start_block_number
            ),
        ));
    }
    match (chain_id, provider_kind) {
        ("base-mainnet", VerificationProviderKind::IndependentRpc)
        | ("base-mainnet", VerificationProviderKind::LocalReth)
        | ("ethereum-mainnet", VerificationProviderKind::LocalReth) => {}
        ("ethereum-mainnet", VerificationProviderKind::IndependentRpc) => {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "ethereum-mainnet verification requires the local reth source; dRPC cannot \
                 claim node-checked verification",
            ));
        }
        _ => {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!(
                    "production stored verification is unsupported for chain {chain_id}; \
                     expected base-mainnet or ethereum-mainnet"
                ),
            ));
        }
    }
    Ok(VerificationSource {
        chain_id: chain_id.to_owned(),
        source_key: source.source_key.clone(),
        source_kind: source.source_kind.clone(),
        endpoint: Arc::from(source.endpoint()),
        provider_kind,
        level,
        cross_check_through: (chain_id == "base-mainnet"
            && provider_kind == VerificationProviderKind::IndependentRpc)
            .then_some(BASE_COINBASE_SEAM_BLOCK),
    })
}

pub(crate) fn validate_reported_level(
    chain_id: &str,
    sources: &[SourceConfig],
    reported: Option<VerificationLevel>,
) -> RunnerResult<()> {
    let Some(reported) = reported else {
        return Ok(());
    };
    if chain_id != "base-mainnet" || reported != VerificationLevel::NodeChecked {
        return Ok(());
    }
    let has_drpc = sources
        .iter()
        .any(|source| normalized_kind(&source.source_kind) == "drpc");
    let has_reth = sources.iter().any(|source| {
        matches!(
            normalized_kind(&source.source_kind).as_str(),
            "reth" | "reth_db"
        )
    });
    if has_drpc || !has_reth {
        return Err(RunnerError::data_integrity(
            "base-mainnet cannot record node_checked without an exclusive local reth \
             verification source; dRPC is capped at cross_checked",
        ));
    }
    Ok(())
}

fn normalized_kind(kind: &str) -> String {
    kind.trim().to_ascii_lowercase().replace('-', "_")
}

fn map_ingest_error(error: bigname_ingest::IngestError) -> RunnerError {
    let kind = match error.kind() {
        bigname_ingest::ErrorKind::Transient => ErrorKind::Transient,
        bigname_ingest::ErrorKind::DataIntegrity => ErrorKind::DataIntegrity,
        bigname_ingest::ErrorKind::Configuration => ErrorKind::Configuration,
    };
    RunnerError::new(kind, error.to_string())
}
