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
    config::{SourceConfig, SourceRole, normalized_source_kind},
    database::VerificationDatabase,
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{
        CompletedPhaseFuture, Phase, PhaseBatchOutcome, PhaseContext, PhaseFuture, PhaseName,
        PhaseProgress, RunMode, VerificationLevel,
    },
    verify_compare,
    verify_store::VerificationStore,
};

#[path = "verify_completed.rs"]
mod completed;
#[path = "verify_source.rs"]
mod source_roles;
pub(crate) use completed::provider_trusted_verify_required;
pub(crate) use source_roles::production_verify_chain;
use source_roles::{provider_configuration_error, provider_trusted_source, validate_intake_shape};

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
        let plan = verification_plan(chain_id, sources)?;
        validate_cross_check_range(plan.cross_check_through(), mode)?;
        match &plan {
            VerificationPlan::ProviderTrusted { .. } => Ok(()),
            VerificationPlan::Compared(source) => self.reference.preflight(source),
        }
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let verification = verification_plan(&context.chain_id, &context.sources)?;
            let plan =
                BatchPlan::new(&context, verification.cross_check_through(), &self.store).await?;
            let reported_level = if let Some(range) = context.mode.range() {
                self.store
                    .level_for_redo(&context.chain_id, range, verification.verification_level())
                    .await?
            } else {
                normal_extent_level(&context, verification.verification_level())?
            };
            let completes_target = plan.to == plan.target.number;
            let end = match &verification {
                VerificationPlan::ProviderTrusted { source, .. } => {
                    let end = self
                        .store
                        .finalized_marker(&context.chain_id, plan.to)
                        .await?;
                    if completes_target {
                        self.store
                            .require_provider_trusted_extent(
                                &context.chain_id,
                                source,
                                &end,
                                context.mode.is_redo(),
                            )
                            .await?;
                    }
                    end
                }
                VerificationPlan::Compared(source) => {
                    let stored = self
                        .store
                        .load_batch(&context.chain_id, plan.from, plan.to)
                        .await?;
                    let reference = self
                        .reference
                        .fetch(source, stored.filter.clone(), plan.from, plan.to)
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
                    stored.end
                }
            };
            if let VerificationPlan::ProviderTrusted { source, .. } = &verification {
                info!(
                    chain_id = context.chain_id,
                    source_key = source.source_key,
                    reported_verification_level = reported_level.as_str(),
                    from_block = plan.from,
                    to_block = plan.to,
                    "provider-trusted stored history extent accepted without an independent reference"
                );
            }
            if completes_target {
                completed::require_frozen_target(&context.chain_id, &end, &plan.target)?;
            }
            let progress = PhaseProgress {
                current: Some(end),
                target: Some(plan.target),
                verification_level: Some(reported_level),
                ..PhaseProgress::default()
            };
            if completes_target {
                Ok(PhaseBatchOutcome::Complete(progress))
            } else {
                Ok(PhaseBatchOutcome::Continue(progress))
            }
        })
    }
    fn revalidates_completed(
        &self,
        chain_id: &str,
        sources: &[SourceConfig],
    ) -> RunnerResult<bool> {
        completed::is_required(chain_id, sources)
    }
    fn revalidate_completed(&self, context: PhaseContext) -> CompletedPhaseFuture<'_> {
        Box::pin(completed::revalidate(self, context))
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
        cross_check_through: Option<i64>,
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
        validate_cross_check_range(cross_check_through, &context.mode)?;
        if let Some(cross_check_through) = cross_check_through
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
        (Some(_), Some(retained)) => Ok(crate::verify_level::weakest_level(retained, source_level)),
        (Some(current), None) => Err(RunnerError::data_integrity(format!(
            "verification resume for chain {} at block {} has no retained verification level",
            context.chain_id, current.number
        ))),
    }
}

fn validate_cross_check_range(
    cross_check_through: Option<i64>,
    mode: &RunMode,
) -> RunnerResult<()> {
    let Some((cross_check_through, range)) = cross_check_through.zip(mode.range()) else {
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

enum VerificationPlan {
    ProviderTrusted {
        level: VerificationLevel,
        source: SourceConfig,
    },
    Compared(VerificationSource),
}

impl VerificationPlan {
    const fn verification_level(&self) -> VerificationLevel {
        match self {
            Self::ProviderTrusted { level, .. } => *level,
            Self::Compared(source) => source.verification_level(),
        }
    }
    const fn cross_check_through(&self) -> Option<i64> {
        match self {
            Self::ProviderTrusted { .. } => None,
            Self::Compared(source) => source.cross_check_through,
        }
    }
}

fn verification_plan(chain_id: &str, sources: &[SourceConfig]) -> RunnerResult<VerificationPlan> {
    let intake = sources
        .iter()
        .filter(|source| source.role.serves_intake())
        .collect::<Vec<_>>();
    validate_intake_shape(chain_id, &intake)?;
    let candidates = sources
        .iter()
        .filter(|source| source.role == SourceRole::VerificationOnly)
        .collect::<Vec<_>>();
    if candidates.len() > 1 {
        let source_keys = candidates
            .iter()
            .map(|source| source.source_key.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            format!(
                "chain {chain_id} configures more than one verification-only source: {source_keys}"
            ),
        ));
    }
    if let Some(source) = candidates.first() {
        if chain_id == "ethereum-sepolia" {
            validate_intake_shape(chain_id, &[*source])?;
        }
        if let Some(conflict) = intake
            .iter()
            .find(|intake| intake.endpoint() == source.endpoint())
        {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!(
                    "verification-only source {} resolves to the same endpoint as intake source {}",
                    source.source_key, conflict.source_key
                ),
            ));
        }
        return select_source(chain_id, sources).map(VerificationPlan::Compared);
    }
    Ok(VerificationPlan::ProviderTrusted {
        level: VerificationLevel::QuickSynced,
        source: provider_trusted_source(chain_id, &intake)?.clone(),
    })
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
                .map_err(|_| provider_configuration_error(source))?;
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
        .filter(|source| source.role == SourceRole::VerificationOnly)
        .filter_map(|source| {
            let kind = normalized_source_kind(&source.source_kind);
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
                "chain {chain_id} must configure exactly one verification-only reference of kind \
                 drpc or reth_db; found {}",
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
        ("base-mainnet" | "ethereum-sepolia", VerificationProviderKind::IndependentRpc)
        | ("ethereum-mainnet", VerificationProviderKind::LocalReth) => {}
        ("base-mainnet", VerificationProviderKind::LocalReth) => {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "base-mainnet with reth_db is unsupported: bigname's pinned reader implements \
                 only Ethereum-primitives transaction and receipt decoding; OP Stack decoding is \
                 not implemented; use dRPC for Base verification (tracked by issue #433)",
            ));
        }
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
                     expected base-mainnet, ethereum-mainnet, or ethereum-sepolia"
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
    let declared = if production_verify_chain(chain_id) {
        verification_plan(chain_id, sources)?.verification_level()
    } else {
        // Generic Phase implementations use this seam; production rejects other chains.
        return Ok(());
    };
    let reported = reported.ok_or_else(|| {
        RunnerError::data_integrity(format!("chain {chain_id} Verify has no verification level"))
    })?;
    if verification_level_rank(reported) <= verification_level_rank(declared) {
        return Ok(());
    }
    Err(RunnerError::data_integrity(format!(
        "chain {chain_id} cannot record {} because its chain-specific verification path earns at most {}",
        reported.as_str(),
        declared.as_str()
    )))
}
const fn verification_level_rank(level: VerificationLevel) -> u8 {
    match level {
        VerificationLevel::QuickSynced => 0,
        VerificationLevel::CrossChecked => 1,
        VerificationLevel::NodeChecked => 2,
    }
}

fn map_ingest_error(error: bigname_ingest::IngestError) -> RunnerError {
    let kind = match error.kind() {
        bigname_ingest::ErrorKind::Transient => ErrorKind::Transient,
        bigname_ingest::ErrorKind::DataIntegrity => ErrorKind::DataIntegrity,
        bigname_ingest::ErrorKind::Configuration => ErrorKind::Configuration,
    };
    RunnerError::new(kind, error.to_string())
}
