//! Which loader restored a chain's prior adapter state, and why. Both loaders must produce
//! identical output; the choice only changes how much history a batch reads.
use std::{collections::HashMap, fmt, sync::Mutex};

use crate::{InterpretError, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StateLoader {
    /// Per-batch ENSv1 lookahead: load only the history of names the batch touches.
    Lookahead,
    /// Restore all retained history once, then carry the session between batches.
    FullState { reason: FullStateReason },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FullStateReason {
    /// The operator forced the full-state loader for the whole process.
    OperatorOverride,
    /// A manifest the batch reads belongs to a source family lookahead does not cover.
    UnsupportedSourceFamily {
        source_family: String,
        rollout_status: &'static str,
    },
}

impl fmt::Display for StateLoader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lookahead => formatter.write_str("lookahead"),
            Self::FullState { .. } => formatter.write_str("full-state"),
        }
    }
}

impl fmt::Display for FullStateReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OperatorOverride => formatter.write_str("forced by the operator"),
            Self::UnsupportedSourceFamily {
                source_family,
                rollout_status,
            } => write!(
                formatter,
                "{rollout_status} source family {source_family} is not covered by lookahead"
            ),
        }
    }
}

/// Remembers the last choice per chain so the log records the first choice and every change,
/// not every batch.
#[derive(Default)]
pub(super) struct LoaderChoices(Mutex<HashMap<String, StateLoader>>);

impl LoaderChoices {
    /// Returns true when this call logged, which is what the tests observe.
    pub(super) fn record(&self, chain_id: &str, choice: StateLoader) -> Result<bool> {
        let mut choices = self
            .0
            .lock()
            .map_err(|_| InterpretError::transient("interpret loader-choice lock was poisoned"))?;
        let previous = choices.get(chain_id);
        if previous == Some(&choice) {
            return Ok(false);
        }
        let reason = match &choice {
            StateLoader::Lookahead => {
                "every source family the chain reads is covered by lookahead".to_owned()
            }
            StateLoader::FullState { reason } => reason.to_string(),
        };
        match previous {
            None => tracing::info!(
                chain_id,
                loader = %choice,
                reason,
                "interpret chose its prior-state loader"
            ),
            Some(previous) => tracing::info!(
                chain_id,
                loader = %choice,
                previous_loader = %previous,
                reason,
                "interpret changed its prior-state loader"
            ),
        }
        choices.insert(chain_id.to_owned(), choice);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loader_choice_is_logged_first_and_on_every_change_only() {
        let choices = LoaderChoices::default();
        let full = |family: &str| StateLoader::FullState {
            reason: FullStateReason::UnsupportedSourceFamily {
                source_family: family.to_owned(),
                rollout_status: "active",
            },
        };
        assert!(
            choices
                .record("ethereum-mainnet", StateLoader::Lookahead)
                .unwrap()
        );
        assert!(
            !choices
                .record("ethereum-mainnet", StateLoader::Lookahead)
                .unwrap()
        );
        // Another chain is tracked on its own.
        assert!(
            choices
                .record("ethereum-sepolia", full("ens_v2_registry_l1"))
                .unwrap()
        );
        assert!(
            choices
                .record("ethereum-mainnet", full("ens_v2_registry_l1"))
                .unwrap()
        );
        assert!(
            !choices
                .record("ethereum-mainnet", full("ens_v2_registry_l1"))
                .unwrap()
        );
        // A different forcing family is a change worth logging.
        assert!(
            choices
                .record("ethereum-mainnet", full("ens_v2_root_l1"))
                .unwrap()
        );
        assert!(
            choices
                .record("ethereum-mainnet", StateLoader::Lookahead)
                .unwrap()
        );
    }
}
