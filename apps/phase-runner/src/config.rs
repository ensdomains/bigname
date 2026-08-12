use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use crate::error::{ErrorKind, RunnerError, RunnerResult};

#[derive(Clone)]
pub struct SourceConfig {
    pub chain_id: String,
    pub source_key: String,
    pub source_kind: String,
    pub seed_basis: SeedBasis,
    pub start_block_number: i64,
    endpoint: Arc<str>,
}

impl SourceConfig {
    pub fn new(
        chain_id: impl Into<String>,
        source_key: impl Into<String>,
        source_kind: impl Into<String>,
        seed_basis: SeedBasis,
        start_block_number: i64,
        endpoint: impl Into<String>,
    ) -> RunnerResult<Self> {
        let source = Self {
            chain_id: chain_id.into(),
            source_key: source_key.into(),
            source_kind: source_kind.into(),
            seed_basis,
            start_block_number,
            endpoint: Arc::from(endpoint.into()),
        };
        source.validate()?;
        Ok(source)
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn validate(&self) -> RunnerResult<()> {
        for (label, value) in [
            ("chain id", self.chain_id.as_str()),
            ("source key", self.source_key.as_str()),
            ("source kind", self.source_kind.as_str()),
            ("source endpoint", self.endpoint()),
        ] {
            if value.trim().is_empty() {
                return Err(RunnerError::new(
                    ErrorKind::Configuration,
                    format!("{label} must not be empty"),
                ));
            }
        }
        if self.start_block_number < 0 {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!("source {} start block must be nonnegative", self.source_key),
            ));
        }
        Ok(())
    }
}

pub(crate) fn normalized_source_kind(kind: &str) -> String {
    kind.trim().to_ascii_lowercase().replace('-', "_")
}

impl fmt::Debug for SourceConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceConfig")
            .field("chain_id", &self.chain_id)
            .field("source_key", &self.source_key)
            .field("source_kind", &self.source_kind)
            .field("seed_basis", &self.seed_basis)
            .field("start_block_number", &self.start_block_number)
            .field("endpoint", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeedBasis {
    BaseSeam,
    NewSignatureRange,
    EthereumHead,
}

impl SeedBasis {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BaseSeam => "base_seam",
            Self::NewSignatureRange => "new_signature_range",
            Self::EthereumHead => "ethereum_head",
        }
    }

    pub fn parse(value: &str) -> RunnerResult<Self> {
        match value {
            "base_seam" => Ok(Self::BaseSeam),
            "new_signature_range" => Ok(Self::NewSignatureRange),
            "ethereum_head" => Ok(Self::EthereumHead),
            _ => Err(RunnerError::new(
                ErrorKind::Configuration,
                format!(
                    "unknown source seed basis {value:?}; expected base_seam, \
                     new_signature_range, or ethereum_head"
                ),
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChainConfig {
    pub chain_id: String,
    pub sources: Arc<[SourceConfig]>,
    pub verify_before_live: bool,
}

impl ChainConfig {
    pub fn new(
        chain_id: impl Into<String>,
        sources: Vec<SourceConfig>,
        verify_before_live: bool,
    ) -> RunnerResult<Self> {
        let chain_id = chain_id.into();
        if chain_id.trim().is_empty() {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "chain id must not be empty",
            ));
        }
        if sources.iter().any(|source| source.chain_id != chain_id) {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!("source chain does not match configured chain {chain_id}"),
            ));
        }
        let mut keys = BTreeSet::new();
        for source in &sources {
            if !keys.insert(source.source_key.as_str()) {
                return Err(RunnerError::new(
                    ErrorKind::Configuration,
                    format!(
                        "chain {chain_id} configures source key {:?} more than once",
                        source.source_key
                    ),
                ));
            }
        }
        let verify_before_live = verify_before_live
            || sources
                .iter()
                .any(|source| source.seed_basis == SeedBasis::EthereumHead);
        Ok(Self {
            chain_id,
            sources: Arc::from(sources),
            verify_before_live,
        })
    }
}

#[derive(Clone, Debug)]
pub struct CapacityConfig {
    pub database_max_bytes: Option<u64>,
    pub minimum_free_disk_bytes: u64,
    pub writable_path: PathBuf,
    pub poll_interval: Duration,
}

impl Default for CapacityConfig {
    fn default() -> Self {
        Self {
            database_max_bytes: None,
            minimum_free_disk_bytes: 0,
            writable_path: PathBuf::from("."),
            poll_interval: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TimingConfig {
    pub initial_backoff: Duration,
    pub maximum_backoff: Duration,
    pub live_poll_interval: Duration,
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            maximum_backoff: Duration::from_secs(30),
            live_poll_interval: Duration::from_secs(1),
        }
    }
}

impl TimingConfig {
    pub fn validate(&self) -> RunnerResult<()> {
        if self.initial_backoff.is_zero()
            || self.maximum_backoff.is_zero()
            || self.live_poll_interval.is_zero()
        {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "runner timing intervals must be positive",
            ));
        }
        if self.initial_backoff > self.maximum_backoff {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "initial restart backoff must not exceed the capped backoff",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub instance_id: String,
    pub chains: Arc<[ChainConfig]>,
    pub capacity: CapacityConfig,
    pub timing: TimingConfig,
}

impl RuntimeConfig {
    pub fn new(
        instance_id: impl Into<String>,
        chains: Vec<ChainConfig>,
        capacity: CapacityConfig,
        timing: TimingConfig,
    ) -> RunnerResult<Self> {
        let instance_id = instance_id.into();
        if instance_id.trim().is_empty() {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "runner instance id must not be empty",
            ));
        }
        if chains.is_empty() {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "at least one chain must be configured",
            ));
        }
        timing.validate()?;
        if capacity.poll_interval.is_zero() {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "capacity poll interval must be positive",
            ));
        }
        let mut ids = BTreeSet::new();
        for chain in &chains {
            if !ids.insert(chain.chain_id.as_str()) {
                return Err(RunnerError::new(
                    ErrorKind::Configuration,
                    format!("chain {:?} is configured more than once", chain.chain_id),
                ));
            }
        }
        Ok(Self {
            instance_id,
            chains: Arc::from(chains),
            capacity,
            timing,
        })
    }
}

pub fn group_sources(
    chain_ids: &[String],
    sources: Vec<SourceConfig>,
    verify_before_live: &BTreeSet<String>,
) -> RunnerResult<Vec<ChainConfig>> {
    let configured = chain_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut by_chain = BTreeMap::<String, Vec<SourceConfig>>::new();
    for source in sources {
        if !configured.contains(source.chain_id.as_str()) {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!(
                    "source {:?} belongs to unconfigured chain {:?}",
                    source.source_key, source.chain_id
                ),
            ));
        }
        by_chain
            .entry(source.chain_id.clone())
            .or_default()
            .push(source);
    }

    chain_ids
        .iter()
        .map(|chain_id| {
            let sources = by_chain.remove(chain_id).unwrap_or_default();
            if sources.is_empty() {
                return Err(RunnerError::new(
                    ErrorKind::Configuration,
                    format!("chain {chain_id:?} has no configured source"),
                ));
            }
            ChainConfig::new(
                chain_id.clone(),
                sources,
                verify_before_live.contains(chain_id),
            )
        })
        .collect()
}
