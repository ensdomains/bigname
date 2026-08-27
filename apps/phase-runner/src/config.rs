use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use crate::error::{ErrorKind, RunnerError, RunnerResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceRole {
    Intake,
    VerificationOnly,
    Both,
}
impl SourceRole {
    pub fn parse(value: &str) -> RunnerResult<Self> {
        match value {
            "intake" => Ok(Self::Intake),
            "verification-only" => Ok(Self::VerificationOnly),
            "both" => Ok(Self::Both),
            _ => Err(RunnerError::new(
                ErrorKind::Configuration,
                format!("unknown role {value:?}; expected intake, verification-only, or both"),
            )),
        }
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Intake => "intake",
            Self::VerificationOnly => "verification-only",
            Self::Both => "both",
        }
    }
    pub const fn serves_intake(self) -> bool {
        matches!(self, Self::Intake | Self::Both)
    }
    pub const fn serves_verification(self) -> bool {
        matches!(self, Self::VerificationOnly | Self::Both)
    }
}

include!(concat!(env!("OUT_DIR"), "/compiled_chain_namespaces.rs"));

#[derive(Clone)]
pub struct SourceConfig {
    pub chain_id: String,
    pub source_key: String,
    pub source_kind: String,
    pub seed_basis: SeedBasis,
    pub start_block_number: i64,
    pub role: SourceRole,
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
        Self::new_with_role(
            chain_id,
            source_key,
            source_kind,
            seed_basis,
            start_block_number,
            SourceRole::Both,
            endpoint,
        )
    }
    pub fn new_with_role(
        chain_id: impl Into<String>,
        source_key: impl Into<String>,
        source_kind: impl Into<String>,
        seed_basis: SeedBasis,
        start_block_number: i64,
        role: SourceRole,
        endpoint: impl Into<String>,
    ) -> RunnerResult<Self> {
        let source = Self {
            chain_id: chain_id.into(),
            source_key: source_key.into(),
            source_kind: source_kind.into(),
            seed_basis,
            start_block_number,
            role,
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
        ] {
            if value.trim().is_empty() {
                return Err(RunnerError::new(
                    ErrorKind::Configuration,
                    format!("{label} must not be empty"),
                ));
            }
        }
        if self.endpoint().trim().is_empty() {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!(
                    "source descriptor {}:{} has an empty endpoint",
                    self.chain_id, self.source_key
                ),
            ));
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
            .field("role", &self.role)
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
    intake_sources: Arc<[SourceConfig]>,
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
        let intake_sources = sources
            .iter()
            .filter(|source| source.role.serves_intake())
            .cloned()
            .collect::<Vec<_>>();
        let verify_before_live = verify_before_live
            || intake_sources
                .iter()
                .any(|source| source.seed_basis == SeedBasis::EthereumHead);
        Ok(Self {
            chain_id,
            sources: Arc::from(sources),
            intake_sources: Arc::from(intake_sources),
            verify_before_live,
        })
    }
    pub fn intake_sources(&self) -> Arc<[SourceConfig]> {
        Arc::clone(&self.intake_sources)
    }
    pub fn require_intake_sources(&self) -> RunnerResult<()> {
        if self.intake_sources.is_empty() {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                format!(
                    "chain {:?} has zero intake-capable sources; normal run and ingest, verify, or \
                     all-phase redo require intake",
                    self.chain_id
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct CapacityConfig {
    pub database_max_bytes: Option<u64>,
    pub minimum_free_disk_bytes: u64,
    pub writable_path: PathBuf,
    pub poll_interval: Duration,
    pub interpreter_state_cache_entries: usize,
}

impl Default for CapacityConfig {
    fn default() -> Self {
        Self {
            database_max_bytes: None,
            minimum_free_disk_bytes: 0,
            writable_path: PathBuf::from("."),
            poll_interval: Duration::from_secs(5),
            interpreter_state_cache_entries:
                bigname_interpret::DEFAULT_INTERPRETER_STATE_CACHE_ENTRIES,
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
            chain.require_intake_sources()?;
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
            let chain = ChainConfig::new(
                chain_id.clone(),
                sources,
                verify_before_live.contains(chain_id),
            )?;
            chain.require_intake_sources()?;
            Ok(chain)
        })
        .collect()
}

pub fn validate_deployment_table_set<'a>(
    chains: &[ChainConfig],
    manifest_namespaces: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> RunnerResult<()> {
    let configured_chains = chains
        .iter()
        .map(|chain| chain.chain_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut namespaces_by_chain = BTreeMap::<&str, BTreeSet<&str>>::new();
    for (chain_id, namespace) in manifest_namespaces {
        namespaces_by_chain
            .entry(chain_id)
            .or_default()
            .insert(namespace);
    }
    let unknown_chains = configured_chains
        .iter()
        .filter(|chain_id| !namespaces_by_chain.contains_key(*chain_id))
        .copied()
        .collect::<Vec<_>>();
    if !unknown_chains.is_empty() {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            format!(
                "configured chain(s) {} are not declared by the binary-approved deployment \
                 profiles",
                unknown_chains.join(", ")
            ),
        ));
    }

    let ens_chains = configured_chains
        .iter()
        .filter(|chain_id| {
            namespaces_by_chain
                .get(*chain_id)
                .is_some_and(|namespaces| namespaces.contains("ens"))
        })
        .copied()
        .collect::<Vec<_>>();
    if (configured_chains.contains("ethereum-sepolia") && configured_chains.len() > 1)
        || ens_chains.len() > 1
    {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            format!(
                "configured chains {} violate the deployment topology: chains carrying the ens \
                 namespace never share a table set; Sepolia always runs as its own deployment \
                 writing its own tables; two chains in one database are supported only when \
                 their namespaces differ (the existing ethereum-plus-base production shape)",
                configured_chains.into_iter().collect::<Vec<_>>().join(", ")
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(chain_id: &str) -> ChainConfig {
        ChainConfig::new(
            chain_id,
            vec![
                SourceConfig::new(
                    chain_id,
                    "rpc",
                    "rpc",
                    SeedBasis::BaseSeam,
                    0,
                    "http://rpc.invalid",
                )
                .unwrap(),
            ],
            false,
        )
        .unwrap()
    }

    #[test]
    fn two_ens_chains_refuse_to_share_a_table_set() {
        let chains = [chain("ethereum-mainnet"), chain("ethereum-sepolia")];
        let error = validate_deployment_table_set(
            &chains,
            [("ethereum-mainnet", "ens"), ("ethereum-sepolia", "ens")],
        )
        .expect_err("two chains carrying ens must fail startup validation");

        assert_eq!(error.kind(), ErrorKind::Configuration);
        let message = error.to_string();
        assert!(message.contains("chains carrying the ens namespace never share a table set"));
        assert!(message.contains("Sepolia always runs as its own deployment"));
        assert!(message.contains(
            "two chains in one database are supported only when their namespaces differ"
        ));
        assert!(message.contains("ethereum-mainnet"));
        assert!(message.contains("ethereum-sepolia"));
    }

    #[test]
    fn ethereum_and_base_with_different_namespaces_share_a_table_set() {
        let chains = [chain("ethereum-mainnet"), chain("base-mainnet")];

        validate_deployment_table_set(
            &chains,
            [
                ("ethereum-mainnet", "ens"),
                ("ethereum-mainnet", "basenames"),
                ("base-mainnet", "basenames"),
            ],
        )
        .expect("the production ethereum-plus-base shape must remain supported");
    }

    #[test]
    fn single_chain_sepolia_uses_its_own_table_set() {
        let chains = [chain("ethereum-sepolia")];

        validate_deployment_table_set(&chains, [("ethereum-sepolia", "ens")])
            .expect("a single-chain Sepolia deployment must remain supported");
    }

    #[test]
    fn sepolia_and_base_refuse_to_share_a_table_set() {
        let chains = [chain("ethereum-sepolia"), chain("base-mainnet")];
        let error = validate_deployment_table_set(
            &chains,
            [("ethereum-sepolia", "ens"), ("base-mainnet", "basenames")],
        )
        .expect_err("Sepolia must run as its own deployment");

        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert!(
            error
                .to_string()
                .contains("Sepolia always runs as its own deployment")
        );
    }

    #[test]
    fn unknown_chain_is_not_approved_for_runtime_configuration() {
        let chains = [chain("unknown-chain-x")];
        let error =
            validate_deployment_table_set(&chains, COMPILED_CHAIN_NAMESPACES.iter().copied())
                .expect_err("a chain absent from the approved profiles must fail closed");

        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert!(error.to_string().contains("unknown-chain-x"));
        assert!(
            error
                .to_string()
                .contains("binary-approved deployment profiles")
        );
    }
}
