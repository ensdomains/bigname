use std::{cell::RefCell, future::Future};

use crate::{
    config::ChainConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::PhaseName,
};

use super::{PhaseRunner, RedoPhase};

tokio::task_local! {
    static EXPECTED_MANIFEST_ATTESTATION: RefCell<Option<(String, String)>>;
}

impl PhaseRunner {
    pub(super) async fn preflight_watch_set_coverage_attestation(
        &self,
        chain: &ChainConfig,
        selection: RedoPhase,
    ) -> RunnerResult<Option<String>> {
        if !self.attest_watch_set_coverage {
            return Ok(None);
        }
        if !matches!(
            selection,
            RedoPhase::Phase(PhaseName::Interpret) | RedoPhase::All
        ) {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "--attest-watch-set-coverage is only valid for interpret or all-phase redo",
            ));
        }
        crate::redo_manifest_attestation::preflight(self.store.pool(), &chain.chain_id)
            .await
            .map(Some)
    }

    pub(super) async fn scope_manifest_attestation<F>(
        &self,
        chain_id: &str,
        marker: Option<String>,
        future: F,
    ) -> F::Output
    where
        F: Future,
    {
        let expected = marker.map(|marker| (chain_id.to_owned(), marker));
        EXPECTED_MANIFEST_ATTESTATION
            .scope(RefCell::new(expected), future)
            .await
    }

    pub(super) fn expected_manifest_attestation_marker(&self, chain_id: &str) -> Option<String> {
        EXPECTED_MANIFEST_ATTESTATION
            .try_with(|expected| {
                expected
                    .borrow()
                    .as_ref()
                    .filter(|(expected_chain, _)| expected_chain == chain_id)
                    .map(|(_, marker)| marker.clone())
            })
            .ok()
            .flatten()
    }

    pub(super) fn clear_manifest_attestation_marker(&self, chain_id: &str) {
        let _ = EXPECTED_MANIFEST_ATTESTATION.try_with(|expected| {
            if expected
                .borrow()
                .as_ref()
                .is_some_and(|(expected_chain, _)| expected_chain == chain_id)
            {
                *expected.borrow_mut() = None;
            }
        });
    }
}
