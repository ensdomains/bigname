use std::{fmt, future::Future, pin::Pin, str::FromStr, sync::Arc};

use crate::{
    config::SourceConfig,
    error::{RunnerError, RunnerResult},
    heads::HeadMarkers,
};

pub type PhaseFuture<'a> =
    Pin<Box<dyn Future<Output = RunnerResult<PhaseBatchOutcome>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PhaseName {
    Ingest,
    Interpret,
    Project,
    Verify,
    Live,
}

impl PhaseName {
    pub const ALL: [Self; 5] = [
        Self::Ingest,
        Self::Interpret,
        Self::Project,
        Self::Verify,
        Self::Live,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ingest => "ingest",
            Self::Interpret => "interpret",
            Self::Project => "project",
            Self::Verify => "verify",
            Self::Live => "live",
        }
    }

    pub const fn prerequisite(self) -> Option<Self> {
        match self {
            Self::Ingest => None,
            Self::Interpret => Some(Self::Ingest),
            Self::Project => Some(Self::Interpret),
            Self::Verify | Self::Live => Some(Self::Project),
        }
    }

    pub const fn writes_derived_data(self) -> bool {
        matches!(self, Self::Interpret | Self::Project | Self::Live)
    }
}

impl fmt::Display for PhaseName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PhaseName {
    type Err = RunnerError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "ingest" => Ok(Self::Ingest),
            "interpret" => Ok(Self::Interpret),
            "project" => Ok(Self::Project),
            "verify" => Ok(Self::Verify),
            "live" => Ok(Self::Live),
            _ => Err(RunnerError::new(
                crate::error::ErrorKind::Configuration,
                format!(
                    "unknown phase {value:?}; expected ingest, interpret, project, verify, or live"
                ),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockRange {
    pub from: i64,
    pub to: i64,
}

impl BlockRange {
    pub fn new(from: i64, to: i64) -> RunnerResult<Self> {
        if from < 0 || to < 0 {
            return Err(RunnerError::new(
                crate::error::ErrorKind::Configuration,
                "redo block numbers must be nonnegative",
            ));
        }
        if from > to {
            return Err(RunnerError::new(
                crate::error::ErrorKind::Configuration,
                format!("redo range start {from} is above range end {to}"),
            ));
        }
        Ok(Self { from, to })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunMode {
    Normal,
    Redo(BlockRange),
    RecomputeFlags(BlockRange),
}

impl RunMode {
    pub const fn is_redo(&self) -> bool {
        !matches!(self, Self::Normal)
    }

    pub const fn range(&self) -> Option<BlockRange> {
        match self {
            Self::Normal => None,
            Self::Redo(range) | Self::RecomputeFlags(range) => Some(*range),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PhaseContext {
    pub chain_id: String,
    pub phase: PhaseName,
    pub mode: RunMode,
    pub sources: Arc<[SourceConfig]>,
    pub available_heads: Option<HeadMarkers>,
    pub live_handoff: Option<crate::heads::BlockMarker>,
    pub resume: PhaseResume,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PhaseResume {
    pub current: Option<crate::heads::BlockMarker>,
    pub target: Option<crate::heads::BlockMarker>,
    pub ingest_cursors: Arc<[IngestCursor]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngestCursor {
    pub source_key: String,
    pub next_block_number: i64,
    pub target_block_number: Option<i64>,
    pub last_processed: Option<crate::heads::BlockMarker>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceProgress {
    pub source_key: String,
    pub current: Option<crate::heads::BlockMarker>,
    pub target: Option<crate::heads::BlockMarker>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationLevel {
    QuickSynced,
    CrossChecked,
    NodeChecked,
}

impl VerificationLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QuickSynced => "quick_synced",
            Self::CrossChecked => "cross_checked",
            Self::NodeChecked => "node_checked",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PhaseProgress {
    pub current: Option<crate::heads::BlockMarker>,
    pub target: Option<crate::heads::BlockMarker>,
    pub live_handoff: Option<crate::heads::BlockMarker>,
    pub heads: Option<HeadMarkers>,
    pub source_progress: Vec<SourceProgress>,
    pub verification_level: Option<VerificationLevel>,
    pub estimated_write_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhaseBatchOutcome {
    Complete(PhaseProgress),
    Continue(PhaseProgress),
}

impl PhaseBatchOutcome {
    pub fn progress(&self) -> &PhaseProgress {
        match self {
            Self::Complete(progress) | Self::Continue(progress) => progress,
        }
    }
}

pub trait Phase: Send + Sync {
    fn name(&self) -> PhaseName;

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_>;
}

#[derive(Clone, Debug)]
pub struct LoopbackPhase {
    name: PhaseName,
}

impl LoopbackPhase {
    pub const fn new(name: PhaseName) -> Self {
        Self { name }
    }
}

impl Phase for LoopbackPhase {
    fn name(&self) -> PhaseName {
        self.name
    }

    fn run_batch(&self, context: PhaseContext) -> PhaseFuture<'_> {
        Box::pin(async move {
            let marker = context
                .mode
                .range()
                .and_then(|range| {
                    context.available_heads.as_ref().and_then(|heads| {
                        (heads.latest.number == range.to).then(|| heads.latest.clone())
                    })
                })
                .or_else(|| {
                    context
                        .available_heads
                        .as_ref()
                        .map(|heads| heads.latest.clone())
                });
            let publish_heads = matches!(context.mode, RunMode::Normal)
                && matches!(context.phase, PhaseName::Ingest | PhaseName::Live);
            let source_progress = if context.phase == PhaseName::Ingest {
                context
                    .sources
                    .iter()
                    .map(|source| SourceProgress {
                        source_key: source.source_key.clone(),
                        current: marker.clone(),
                        target: marker.clone(),
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let progress = PhaseProgress {
                current: marker.clone(),
                target: marker.clone(),
                live_handoff: (context.phase == PhaseName::Ingest)
                    .then_some(marker)
                    .flatten(),
                heads: publish_heads.then_some(context.available_heads).flatten(),
                source_progress,
                verification_level: (context.phase == PhaseName::Verify)
                    .then_some(VerificationLevel::QuickSynced),
                estimated_write_bytes: 0,
            };

            if context.phase == PhaseName::Live && matches!(context.mode, RunMode::Normal) {
                Ok(PhaseBatchOutcome::Continue(progress))
            } else {
                Ok(PhaseBatchOutcome::Complete(progress))
            }
        })
    }
}

#[derive(Clone)]
pub struct PhaseSet {
    phases: [Arc<dyn Phase>; 5],
}

impl PhaseSet {
    pub fn loopback() -> Self {
        Self {
            phases: PhaseName::ALL.map(|name| Arc::new(LoopbackPhase::new(name)) as Arc<dyn Phase>),
        }
    }

    pub fn new(phases: [Arc<dyn Phase>; 5]) -> RunnerResult<Self> {
        for (expected, phase) in PhaseName::ALL.into_iter().zip(&phases) {
            if phase.name() != expected {
                return Err(RunnerError::new(
                    crate::error::ErrorKind::Configuration,
                    format!(
                        "phase set position {expected} contains implementation for {}",
                        phase.name()
                    ),
                ));
            }
        }
        Ok(Self { phases })
    }

    pub fn get(&self, name: PhaseName) -> Arc<dyn Phase> {
        Arc::clone(&self.phases[name as usize])
    }
}
