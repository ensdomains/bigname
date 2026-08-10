use std::{cell::RefCell, future::Future, sync::Arc};

use crate::{
    config::ChainConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::PhaseName,
};

use super::{PhaseRunner, RedoPhase};

tokio::task_local! {
    static SUPPLIED_MANIFEST_ATTESTATION: RefCell<Option<(String, String)>>;
}

impl PhaseRunner {
    #[doc(hidden)]
    pub fn with_manifest_authority_audit_before_emit(
        mut self,
        hook: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        self.manifest_authority_audit_before_emit = Some(Arc::new(hook));
        self
    }

    pub(super) fn before_manifest_authority_audit_emit(&self) {
        if let Some(hook) = self.manifest_authority_audit_before_emit.as_deref() {
            hook();
        }
    }

    pub(super) async fn preflight_watch_set_coverage_attestation(
        &self,
        chain: &ChainConfig,
        selection: RedoPhase,
    ) -> RunnerResult<Option<String>> {
        let generation_token = self.watch_set_coverage_attestations.get(&chain.chain_id);
        let supports_attestation = matches!(
            selection,
            RedoPhase::Phase(PhaseName::Interpret) | RedoPhase::All
        );
        if generation_token.is_some() && !supports_attestation {
            return Err(RunnerError::new(
                ErrorKind::Configuration,
                "--attest-watch-set-coverage is only valid for interpret or all-phase redo",
            ));
        }
        if !supports_attestation {
            return Ok(None);
        }
        crate::redo_manifest_attestation::preflight(
            self.store.pool(),
            &chain.chain_id,
            generation_token.map(String::as_str),
        )
        .await
    }

    pub(super) async fn scope_manifest_attestation<F>(
        &self,
        chain_id: &str,
        generation_token: Option<String>,
        future: F,
    ) -> F::Output
    where
        F: Future,
    {
        let supplied = generation_token.map(|token| (chain_id.to_owned(), token));
        SUPPLIED_MANIFEST_ATTESTATION
            .scope(RefCell::new(supplied), future)
            .await
    }

    pub(super) fn supplied_manifest_attestation_generation(
        &self,
        chain_id: &str,
    ) -> Option<String> {
        SUPPLIED_MANIFEST_ATTESTATION
            .try_with(|supplied| {
                supplied
                    .borrow()
                    .as_ref()
                    .filter(|(expected_chain, _)| expected_chain == chain_id)
                    .map(|(_, marker)| marker.clone())
            })
            .ok()
            .flatten()
    }
}
