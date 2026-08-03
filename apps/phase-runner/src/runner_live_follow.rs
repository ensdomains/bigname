use std::{sync::Arc, sync::OnceLock};

use tokio_util::sync::CancellationToken;

use crate::{
    config::ChainConfig,
    error::{ErrorKind, RunnerResult},
    phase::{PhaseName, RunMode},
    runner_support::record_live_mismatch_with_lock,
};

use super::{LiveMismatchReason, PhaseRunner};

impl PhaseRunner {
    pub(super) async fn run_live_follow(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            self.run_phase_with_restart(
                chain,
                PhaseName::Live,
                RunMode::Normal,
                cancellation.clone(),
            )
            .await?;
            if cancellation.is_cancelled() {
                return Ok(());
            }
            self.catch_up_for_required_redo(chain, cancellation.clone())
                .await?;
            if cancellation.is_cancelled() {
                return Ok(());
            }
            for phase in [PhaseName::Interpret, PhaseName::Project] {
                self.run_spine_phase(chain, phase, cancellation.clone())
                    .await?;
            }
            if !self.phases.continuous_live_follow() {
                return Ok(());
            }
            tokio::select! {
                () = cancellation.cancelled() => return Ok(()),
                () = tokio::time::sleep(self.timing.live_poll_interval) => {}
            }
        }
    }

    pub(super) async fn run_verify_and_live(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        self.phases.get(PhaseName::Verify).preflight(
            &chain.chain_id,
            &chain.sources,
            &RunMode::Normal,
        )?;
        let pair_cancellation = cancellation.child_token();
        let live_mismatch = Arc::new(OnceLock::new());
        let verify = self.run_phase_with_restart(
            chain,
            PhaseName::Verify,
            RunMode::Normal,
            pair_cancellation.clone(),
        );
        let live = self.run_live_follow_with_mismatch(
            chain,
            pair_cancellation.clone(),
            Arc::clone(&live_mismatch),
        );
        tokio::pin!(verify);
        tokio::pin!(live);

        tokio::select! {
            verify_result = &mut verify => {
                if let Err(error) = verify_result {
                    let verification_mismatch = error.kind() == ErrorKind::VerificationMismatch;
                    if verification_mismatch {
                        let _ = live_mismatch.set(error.to_string());
                    }
                    pair_cancellation.cancel();
                    let live_result = live.await;
                    let error = if verification_mismatch {
                        match self.record_mismatch_if_present(chain, &live_mismatch).await {
                            Ok(()) => error,
                            Err(record_error) => error.with_secondary(
                                "record live stop after verification failed",
                                record_error,
                            ),
                        }
                    } else {
                        error
                    };
                    return match live_result {
                        Ok(()) => Err(error),
                        Err(live_error) => Err(error.with_secondary(
                            "stop live after verification failed",
                            live_error,
                        )),
                    };
                }
                live.await
            }
            live_result = &mut live => {
                if let Err(live_error) = live_result {
                    pair_cancellation.cancel();
                    return match verify.await {
                        Err(verify_error)
                            if verify_error.kind() == ErrorKind::VerificationMismatch =>
                        {
                            let _ = live_mismatch.set(verify_error.to_string());
                            let error = verify_error.with_secondary(
                                "run the paired live phase",
                                live_error,
                            );
                            match self.record_mismatch_if_present(chain, &live_mismatch).await {
                                Ok(()) => Err(error),
                                Err(record_error) => Err(error.with_secondary(
                                    "record live stop after verification failed",
                                    record_error,
                                )),
                            }
                        }
                        Err(verify_error) => Err(live_error.with_secondary(
                            "stop verification after live failed",
                            verify_error,
                        )),
                        Ok(()) => Err(live_error),
                    };
                }
                pair_cancellation.cancel();
                match verify.await {
                    Err(error) if error.kind() == ErrorKind::VerificationMismatch => {
                        let _ = live_mismatch.set(error.to_string());
                        match self.record_mismatch_if_present(chain, &live_mismatch).await {
                            Ok(()) => Err(error),
                            Err(record_error) => Err(error.with_secondary(
                                "record live stop after verification failed",
                                record_error,
                            )),
                        }
                    }
                    result => result,
                }
            }
        }
    }

    async fn run_live_follow_with_mismatch(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
        live_mismatch: LiveMismatchReason,
    ) -> RunnerResult<()> {
        loop {
            if cancellation.is_cancelled() {
                return self.record_mismatch_if_present(chain, &live_mismatch).await;
            }
            self.run_phase_with_restart_inner(
                chain,
                PhaseName::Live,
                RunMode::Normal,
                cancellation.clone(),
                Some(Arc::clone(&live_mismatch)),
            )
            .await?;
            if cancellation.is_cancelled() {
                return self.record_mismatch_if_present(chain, &live_mismatch).await;
            }
            self.catch_up_for_required_redo(chain, cancellation.clone())
                .await?;
            if cancellation.is_cancelled() {
                return self.record_mismatch_if_present(chain, &live_mismatch).await;
            }
            for phase in [PhaseName::Interpret, PhaseName::Project] {
                self.run_spine_phase(chain, phase, cancellation.clone())
                    .await?;
            }
            if !self.phases.continuous_live_follow() {
                return Ok(());
            }
            tokio::select! {
                () = cancellation.cancelled() => {
                    return self.record_mismatch_if_present(chain, &live_mismatch).await;
                },
                () = tokio::time::sleep(self.timing.live_poll_interval) => {}
            }
        }
    }

    async fn record_mismatch_if_present(
        &self,
        chain: &ChainConfig,
        live_mismatch: &OnceLock<String>,
    ) -> RunnerResult<()> {
        if let Some(reason) = live_mismatch.get() {
            record_live_mismatch_with_lock(&self.database, &self.store, &chain.chain_id, reason)
                .await?;
        }
        Ok(())
    }
}
