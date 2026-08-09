use std::collections::BTreeMap;

use crate::{
    config::ChainConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
};

pub(super) fn resolve(
    entries: &[String],
    all_chains: bool,
    chains: &[String],
) -> RunnerResult<BTreeMap<String, String>> {
    if entries.is_empty() {
        return Ok(BTreeMap::new());
    }
    if !all_chains && chains.len() == 1 {
        if entries.len() != 1 {
            return Err(configuration_error(
                "a single-chain redo takes exactly one invalidation token",
            ));
        }
        let selected_chain = &chains[0];
        let token = if let Some((named_chain, token)) = entries[0].split_once('=') {
            if named_chain != selected_chain {
                return Err(configuration_error(format!(
                    "attestation names chain {named_chain:?}, but the single-chain redo selects {selected_chain:?}"
                )));
            }
            token
        } else {
            &entries[0]
        };
        require_token(token)?;
        return Ok(BTreeMap::from([(selected_chain.clone(), token.to_owned())]));
    }

    let admitted = (!all_chains).then(|| {
        chains
            .iter()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>()
    });
    let mut resolved = BTreeMap::new();
    for entry in entries {
        let Some((chain_id, token)) = entry.split_once('=') else {
            return Err(configuration_error(
                "a multi-chain redo requires repeated CHAIN=TOKEN attestations; one bare token cannot attest multiple chains",
            ));
        };
        if chain_id.trim().is_empty() {
            return Err(configuration_error("attestation chain must not be empty"));
        }
        require_token(token)?;
        if admitted
            .as_ref()
            .is_some_and(|chains| !chains.contains(chain_id))
        {
            return Err(configuration_error(format!(
                "attestation names chain {chain_id:?}, which is not selected for redo"
            )));
        }
        if resolved
            .insert(chain_id.to_owned(), token.to_owned())
            .is_some()
        {
            return Err(configuration_error(format!(
                "duplicate attestation for chain {chain_id:?}"
            )));
        }
    }
    Ok(resolved)
}

pub(crate) fn validate_resolved_chains(
    attestations: &BTreeMap<String, String>,
    chains: &[ChainConfig],
) -> RunnerResult<()> {
    for chain_id in attestations.keys() {
        if !chains.iter().any(|chain| &chain.chain_id == chain_id) {
            return Err(configuration_error(format!(
                "attestation names chain {chain_id:?}, which is not admitted by the synchronized manifest profile"
            )));
        }
    }
    Ok(())
}

fn require_token(token: &str) -> RunnerResult<()> {
    if token.trim().is_empty() {
        return Err(configuration_error(
            "attestation invalidation token must not be empty",
        ));
    }
    Ok(())
}

fn configuration_error(message: impl Into<String>) -> RunnerError {
    RunnerError::new(ErrorKind::Configuration, message)
}
