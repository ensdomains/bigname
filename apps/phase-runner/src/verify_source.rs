use crate::{
    config::{SeedBasis, SourceConfig, normalized_source_kind},
    error::{ErrorKind, RunnerError, RunnerResult},
};
use bigname_ingest::BASE_COINBASE_SEAM_BLOCK;
pub(crate) fn production_verify_chain(id: &str) -> bool {
    matches!(id, "base-mainnet" | "ethereum-mainnet" | "ethereum-sepolia")
}
pub(super) fn provider_configuration_error(source: &super::VerificationSource) -> RunnerError {
    let key = source.source_key();
    RunnerError::new(ErrorKind::Configuration, format!("source {key} is invalid"))
}
pub(super) fn validate_intake_shape(chain_id: &str, sources: &[&SourceConfig]) -> RunnerResult<()> {
    let kind = |source: &&SourceConfig| normalized_source_kind(&source.source_kind);
    let valid = match chain_id {
        "base-mainnet" => {
            sources.len() == 2
                && sources.iter().any(|source| {
                    kind(source) == "coinbase_sql"
                        && source.seed_basis == SeedBasis::BaseSeam
                        && source.start_block_number <= BASE_COINBASE_SEAM_BLOCK
                })
                && sources.iter().any(|source| {
                    kind(source) == "drpc"
                        && source.seed_basis == SeedBasis::BaseSeam
                        && source.start_block_number == BASE_COINBASE_SEAM_BLOCK
                })
        }
        "ethereum-mainnet" => {
            sources.len() == 1
                && matches!(kind(&sources[0]), kind if kind == "reth" || kind == "reth_db")
                && sources[0].seed_basis == SeedBasis::EthereumHead
                && sources[0].start_block_number == 0
        }
        "ethereum-sepolia" => {
            sources.len() == 1
                && kind(&sources[0]) == "drpc"
                && sources[0].seed_basis == SeedBasis::EthereumHead
                && sources[0].start_block_number == 0
        }
        _ => true,
    };
    if !valid {
        let message = match chain_id {
            "ethereum-sepolia" => {
                let descriptors = sources
                    .iter()
                    .map(|source| format!("{chain_id}:{}", source.source_key))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "chain {chain_id} intake descriptors [{descriptors}] violate the required \
                     shape: exactly one dRPC intake-capable source with ethereum_head seed basis \
                     and start block 0"
                )
            }
            _ => format!("chain {chain_id} has an unsupported production intake shape"),
        };
        return Err(RunnerError::new(ErrorKind::Configuration, message));
    }
    Ok(())
}
pub(super) fn provider_trusted_source<'a>(
    chain_id: &str,
    intake: &'a [&SourceConfig],
) -> RunnerResult<&'a SourceConfig> {
    let target_kind = match chain_id {
        "base-mainnet" | "ethereum-sepolia" => "drpc",
        "ethereum-mainnet" => "reth_db",
        _ => "",
    };
    let mut candidates = intake.iter().copied().filter(|source| {
        let kind = normalized_source_kind(&source.source_kind);
        kind == target_kind || (target_kind == "reth_db" && kind == "reth")
    });
    if let (Some(source), None) = (candidates.next(), candidates.next()) {
        return Ok(source);
    }
    let message = format!("chain {chain_id} requires one provider-trusted intake for Verify");
    Err(RunnerError::new(ErrorKind::Configuration, message))
}
#[cfg(test)]
mod tests {
    use super::super::VerificationLevel;
    use super::*;
    #[test]
    fn base_provider_trust_selects_drpc_and_quick_synced() -> RunnerResult<()> {
        let source = |key, kind, start| {
            SourceConfig::new(
                "base-mainnet",
                key,
                kind,
                SeedBasis::BaseSeam,
                start,
                "unused",
            )
        };
        let coinbase = source("coinbase-history", "coinbase_sql", 0)?;
        let drpc = source("drpc-intake", "drpc", BASE_COINBASE_SEAM_BLOCK)?;
        let intake = [&coinbase, &drpc];
        let selected = provider_trusted_source("base-mainnet", &intake)?;
        assert_eq!(selected.source_key, "drpc-intake");
        let plan =
            super::super::verification_plan("base-mainnet", &[coinbase.clone(), drpc.clone()])?;
        assert_eq!(plan.verification_level(), VerificationLevel::QuickSynced);
        let chain = crate::config::ChainConfig::new("base-mainnet", vec![coinbase, drpc], false)?;
        assert!(!crate::runner::PhaseRunner::verify_before_live(&chain)?);
        Ok(())
    }
}
