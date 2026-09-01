use std::{future::Future, pin::Pin, sync::Arc, sync::OnceLock};

use tokio_util::sync::CancellationToken;

use crate::{
    config::ChainConfig,
    error::{ErrorKind, RunnerResult},
    phase::{PhaseName, RunMode},
    phase_lock::PhaseLock,
    runner_support::record_live_mismatch_with_lock,
};

use super::{LiveMismatchReason, PhaseRunner};

pub(super) type AfterRequiredRedoCatchUp =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

enum PostLiveDownstream {
    Complete,
    RepairDiscovery,
}

impl PhaseRunner {
    #[doc(hidden)]
    pub fn with_after_required_redo_catch_up<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.after_required_redo_catch_up = Some(Arc::new(move || Box::pin(hook())));
        self
    }

    async fn after_required_redo_catch_up(&self) {
        if let Some(hook) = self.after_required_redo_catch_up.as_deref() {
            hook().await;
        }
    }

    async fn acquire_post_live_fence(
        &self,
        chain: &ChainConfig,
        phase: PhaseName,
        cancellation: &CancellationToken,
    ) -> RunnerResult<Option<PhaseLock>> {
        loop {
            match PhaseLock::acquire(self.database.connect_options(), &chain.chain_id, phase).await
            {
                Ok(phase_lock) => return Ok(Some(phase_lock)),
                Err(error) if error.kind() == ErrorKind::LockHeld || error.is_retryable() => {
                    tokio::select! {
                        () = cancellation.cancelled() => return Ok(None),
                        () = tokio::time::sleep(self.timing.live_poll_interval) => {}
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn run_post_live_downstream(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        // The paired normal Verify may still be running when Live reaches its downstream
        // boundary. Wait for it before Interpret can admit new intake coverage, and retain this
        // fence until Project is rebuilt. Required Verify redo runs only after releasing the lock.
        let Some(mut verify_fence) = self
            .acquire_post_live_fence(chain, PhaseName::Verify, &cancellation)
            .await?
        else {
            return Ok(());
        };
        let result = verify_fence
            .run_while_alive(
                self.timing.live_poll_interval,
                self.run_post_live_downstream_fenced(chain, cancellation.clone()),
            )
            .await;
        let release = verify_fence.release().await;
        match (result, release) {
            (Ok(()), Ok(())) => {}
            (Ok(()), Err(error)) | (Err(error), Ok(())) => return Err(error),
            (Err(error), Err(release_error)) => {
                return Err(error.with_secondary(
                    "release the post-Live Verify coordination lock",
                    release_error,
                ));
            }
        }
        if cancellation.is_cancelled() {
            return Ok(());
        }
        self.reject_pending_required_ingest(&chain.chain_id).await?;
        self.run_required_verify_redo(chain, cancellation).await
    }

    async fn run_post_live_downstream_fenced(
        &self,
        chain: &ChainConfig,
        cancellation: CancellationToken,
    ) -> RunnerResult<()> {
        let Some((rule_count, iteration_limit)) = self
            .discovery_repair_iteration_limit(&chain.chain_id, &cancellation)
            .await?
        else {
            return Ok(());
        };
        for _iteration in 1..=iteration_limit {
            let Some(mut ingest_fence) = self
                .acquire_post_live_fence(chain, PhaseName::Ingest, &cancellation)
                .await?
            else {
                return Ok(());
            };
            let result = ingest_fence
                .run_while_alive(self.timing.live_poll_interval, async {
                    if let Some(range) = self
                        .store
                        .required_redo_range(&chain.chain_id, PhaseName::Ingest)
                        .await?
                    {
                        self.catch_up_required_range(chain, range, cancellation.clone())
                            .await?;
                        if cancellation.is_cancelled() {
                            return Ok(PostLiveDownstream::Complete);
                        }
                        let Some(discovery_owned) = self
                            .discovery_required_ingest_pending(&chain.chain_id, &cancellation)
                            .await?
                        else {
                            return Ok(PostLiveDownstream::Complete);
                        };
                        if discovery_owned {
                            return Ok(PostLiveDownstream::RepairDiscovery);
                        }
                        return Err(crate::transitions::required_ingest_redo_error(
                            &chain.chain_id,
                            range,
                        ));
                    }
                    self.run_spine_phase(chain, PhaseName::Interpret, cancellation.clone())
                        .await?;
                    if let Some(range) = self
                        .store
                        .required_redo_range(&chain.chain_id, PhaseName::Ingest)
                        .await?
                    {
                        let Some(discovery_owned) = self
                            .discovery_required_ingest_pending(&chain.chain_id, &cancellation)
                            .await?
                        else {
                            return Ok(PostLiveDownstream::Complete);
                        };
                        if discovery_owned {
                            return Ok(PostLiveDownstream::RepairDiscovery);
                        }
                        return Err(crate::transitions::required_ingest_redo_error(
                            &chain.chain_id,
                            range,
                        ));
                    }
                    self.run_spine_phase(chain, PhaseName::Project, cancellation.clone())
                        .await?;
                    self.reject_pending_required_ingest(&chain.chain_id).await?;
                    Ok(PostLiveDownstream::Complete)
                })
                .await;
            let release = ingest_fence.release().await;
            let outcome = match (result, release) {
                (Ok(outcome), Ok(())) => outcome,
                (Ok(_), Err(error)) | (Err(error), Ok(())) => return Err(error),
                (Err(error), Err(release_error)) => {
                    return Err(error.with_secondary(
                        "release the post-Live Ingest coordination lock",
                        release_error,
                    ));
                }
            };
            match outcome {
                PostLiveDownstream::Complete => return Ok(()),
                PostLiveDownstream::RepairDiscovery => {
                    self.run_spine_phase(chain, PhaseName::Ingest, cancellation.clone())
                        .await?;
                    if cancellation.is_cancelled() {
                        return Ok(());
                    }
                }
            }
        }
        Err(Self::discovery_repair_exhausted_error(
            &chain.chain_id,
            rule_count,
            iteration_limit,
        ))
    }

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
            self.after_required_redo_catch_up().await;
            if cancellation.is_cancelled() {
                return Ok(());
            }
            self.run_post_live_downstream(chain, cancellation.clone())
                .await?;
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
                false,
            )
            .await?;
            if cancellation.is_cancelled() {
                return self.record_mismatch_if_present(chain, &live_mismatch).await;
            }
            self.catch_up_for_required_redo(chain, cancellation.clone())
                .await?;
            self.after_required_redo_catch_up().await;
            if cancellation.is_cancelled() {
                return self.record_mismatch_if_present(chain, &live_mismatch).await;
            }
            self.run_post_live_downstream(chain, cancellation.clone())
                .await?;
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
