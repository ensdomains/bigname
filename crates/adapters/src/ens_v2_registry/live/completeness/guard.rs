use super::*;

impl FullSourceRawLogHistoryGuard {
    pub(in crate::ens_v2_registry) async fn acquire(
        mut registry_sync_fence: Transaction<'static, Postgres>,
        chain: &str,
    ) -> Result<Self> {
        // Semantic raw-log mutation triggers take this same transaction-scoped
        // chain lock before advancing the revision/proof state. Other chains
        // remain writable during a full ENSv2 reconciliation.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("raw_log_staging:{chain}"))
            .execute(registry_sync_fence.as_mut())
            .await
            .with_context(|| format!("failed to fence raw-log mutation for {chain}"))?;
        // ACCESS SHARE permits ordinary INSERT/UPDATE/DELETE RowExclusive
        // locks but blocks a global TRUNCATE until this proof user finishes.
        sqlx::query("LOCK TABLE raw_logs IN ACCESS SHARE MODE")
            .execute(registry_sync_fence.as_mut())
            .await
            .with_context(|| format!("failed to fence raw-log truncation for {chain}"))?;
        Ok(Self {
            transaction: registry_sync_fence,
            chain: chain.to_owned(),
        })
    }

    /// Load the currently valid proof without requiring it to reach a newer
    /// live target. A complete live fetch may extend this boundary after it
    /// proves a contiguous selected path back to the stored boundary.
    pub(in crate::ens_v2_registry) async fn load_current_proof(
        &self,
        pool: &PgPool,
    ) -> Result<Option<RawLogClosureProof>> {
        ensure_discovery_epoch_row(pool, &self.chain).await?;
        let mut proof_transaction = pool
            .begin()
            .await
            .context("failed to begin ENSv2 retained-history proof read")?;
        let discovery_epoch = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT epoch
            FROM discovery_admission_epochs
            WHERE chain_id = $1
            FOR SHARE
            "#,
        )
        .bind(&self.chain)
        .fetch_one(proof_transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to lock ENSv2 discovery-admission epoch for {}",
                self.chain
            )
        })?;
        let state =
            load_locked_retained_history_state(proof_transaction.as_mut(), &self.chain).await?;
        let proof = (state.retained_history_complete
            && state.proven_retention_generation == Some(state.retention_generation)
            && state.proven_discovery_admission_epoch == Some(discovery_epoch))
        .then_some(RawLogClosureProof {
            retention_generation: state.retention_generation,
            discovery_admission_epoch: discovery_epoch,
            proven_through_block: state.proven_through_block.unwrap_or(0),
        });
        proof_transaction
            .commit()
            .await
            .context("failed to release retained-history proof read locks")?;
        Ok(proof)
    }

    /// Load a matching proof or rebuild it from generation-bound, gap-free
    /// coverage while the long-lived raw-log fence is held.
    pub(in crate::ens_v2_registry) async fn ensure_proof_through(
        &self,
        pool: &PgPool,
        through_block: i64,
    ) -> Result<RawLogClosureProof> {
        self.ensure_proof_through_inner(pool, through_block, None)
            .await
    }

    pub(in crate::ens_v2_registry) async fn ensure_proof_through_with_progress(
        &self,
        pool: &PgPool,
        through_block: i64,
        progress: &mut dyn StartupAdapterProgress,
    ) -> Result<RawLogClosureProof> {
        self.ensure_proof_through_inner(pool, through_block, Some(progress))
            .await
    }

    async fn ensure_proof_through_inner(
        &self,
        pool: &PgPool,
        through_block: i64,
        mut progress: Option<&mut dyn StartupAdapterProgress>,
    ) -> Result<RawLogClosureProof> {
        ensure!(
            through_block >= 0,
            "ENSv2 retained-history proof boundary cannot be negative"
        );
        ensure_discovery_epoch_row(pool, &self.chain).await?;

        let mut proof_transaction = pool
            .begin()
            .await
            .context("failed to begin ENSv2 retained-history proof")?;
        let discovery_epoch = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT epoch
            FROM discovery_admission_epochs
            WHERE chain_id = $1
            FOR SHARE
            "#,
        )
        .bind(&self.chain)
        .fetch_one(proof_transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to lock ENSv2 discovery-admission epoch for {}",
                self.chain
            )
        })?;
        let state =
            load_locked_retained_history_state(proof_transaction.as_mut(), &self.chain).await?;

        if state.retained_history_complete
            && state.proven_retention_generation == Some(state.retention_generation)
            && state.proven_discovery_admission_epoch == Some(discovery_epoch)
            && state
                .proven_through_block
                .is_some_and(|proven| proven >= through_block)
        {
            let proof = RawLogClosureProof {
                retention_generation: state.retention_generation,
                discovery_admission_epoch: discovery_epoch,
                proven_through_block: state
                    .proven_through_block
                    .context("complete retained-history state is missing its proof boundary")?,
            };
            proof_transaction
                .commit()
                .await
                .context("failed to release retained-history proof locks")?;
            return Ok(proof);
        }

        let source_families = ens_v2_closure_source_families();
        let requirements = if let Some(progress) = progress.as_deref_mut() {
            let mut manifest_progress = StartupManifestProgress::new(progress);
            load_required_watched_tuples_in_transaction_with_progress(
                proof_transaction.as_mut(),
                pool,
                &self.chain,
                0,
                through_block,
                &source_families,
                &mut manifest_progress,
            )
            .await?
        } else {
            load_required_watched_tuples_in_transaction(
                proof_transaction.as_mut(),
                &self.chain,
                0,
                through_block,
                &source_families,
            )
            .await?
        };
        ensure!(
            !requirements.is_empty(),
            "ENSv2 retained history on {} has no authoritative watched closure through block {through_block}",
            self.chain
        );
        ensure_generation_bound_coverage_with_optional_progress(
            pool,
            proof_transaction.as_mut(),
            &self.chain,
            &requirements,
            state.retention_generation,
            &mut progress,
        )
        .await?;
        ensure_retained_semantic_witnesses_with_optional_progress(
            pool,
            proof_transaction.as_mut(),
            &self.chain,
            &requirements,
            through_block,
            &mut progress,
        )
        .await?;

        sqlx::query(
            r#"
            UPDATE raw_log_staging_input_revisions
            SET retained_history_complete = true,
                incomplete_since = NULL,
                proven_retention_generation = retention_generation,
                proven_discovery_admission_epoch = $2,
                proven_through_block = $3
            WHERE chain_id = $1
              AND retention_generation = $4
            "#,
        )
        .bind(&self.chain)
        .bind(discovery_epoch)
        .bind(through_block)
        .bind(state.retention_generation)
        .execute(proof_transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to store ENSv2 retained-history proof for {}",
                self.chain
            )
        })?;
        proof_transaction
            .commit()
            .await
            .context("failed to commit ENSv2 retained-history proof")?;
        Ok(RawLogClosureProof {
            retention_generation: state.retention_generation,
            discovery_admission_epoch: discovery_epoch,
            proven_through_block: through_block,
        })
    }

    /// Establish or extend the retained-history proof from exact provider-live
    /// block bundles. Only watched tuples whose addresses were in the live
    /// fetch selection receive live coverage; every remaining interval still
    /// requires ordinary generation-bound backfill facts.
    pub(in crate::ens_v2_registry) async fn ensure_proof_through_live_selection(
        &self,
        pool: &PgPool,
        through_block: i64,
        selected_addresses: &[String],
        selected_block_hashes: &[String],
    ) -> Result<RawLogClosureProof> {
        self.ensure_proof_through_live_selection_inner(
            pool,
            through_block,
            selected_addresses,
            selected_block_hashes,
            None,
        )
        .await
    }

    pub(in crate::ens_v2_registry) async fn ensure_proof_through_live_selection_with_progress(
        &self,
        pool: &PgPool,
        through_block: i64,
        selected_addresses: &[String],
        selected_block_hashes: &[String],
        progress: &mut dyn StartupAdapterProgress,
    ) -> Result<RawLogClosureProof> {
        self.ensure_proof_through_live_selection_inner(
            pool,
            through_block,
            selected_addresses,
            selected_block_hashes,
            Some(progress),
        )
        .await
    }

    async fn ensure_proof_through_live_selection_inner(
        &self,
        pool: &PgPool,
        through_block: i64,
        selected_addresses: &[String],
        selected_block_hashes: &[String],
        mut progress: Option<&mut dyn StartupAdapterProgress>,
    ) -> Result<RawLogClosureProof> {
        ensure!(
            through_block >= 0,
            "ENSv2 retained-history proof boundary cannot be negative"
        );
        ensure_discovery_epoch_row(pool, &self.chain).await?;
        ensure_retained_history_state_row(pool, &self.chain).await?;
        let selected_block_intervals = load_selected_live_block_intervals(
            pool,
            &self.chain,
            through_block,
            selected_block_hashes,
        )
        .await?;

        let mut proof_transaction = pool
            .begin()
            .await
            .context("failed to begin ENSv2 live retained-history proof")?;
        let discovery_epoch = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT epoch
            FROM discovery_admission_epochs
            WHERE chain_id = $1
            FOR SHARE
            "#,
        )
        .bind(&self.chain)
        .fetch_one(proof_transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to lock ENSv2 discovery-admission epoch for live coverage on {}",
                self.chain
            )
        })?;
        let state =
            load_locked_retained_history_state(proof_transaction.as_mut(), &self.chain).await?;
        let current_proof_is_valid = state.retained_history_complete
            && state.proven_retention_generation == Some(state.retention_generation)
            && state.proven_discovery_admission_epoch == Some(discovery_epoch);
        let required_from_block = if current_proof_is_valid {
            let proven_through = state
                .proven_through_block
                .context("complete retained-history state is missing its proof boundary")?;
            if proven_through >= through_block {
                selected_block_intervals
                    .iter()
                    .map(|(from, _)| *from)
                    .min()
                    .unwrap_or(through_block)
            } else {
                proven_through
                    .checked_add(1)
                    .context("ENSv2 retained-history proof boundary overflow")?
            }
        } else {
            0
        };
        let discovery_history_source_families = ens_v2_discovery_history_source_families();
        let discovery_history_requirements = if required_from_block <= through_block {
            if let Some(progress) = progress.as_deref_mut() {
                let mut manifest_progress = StartupManifestProgress::new(progress);
                load_required_watched_tuples_in_transaction_with_progress(
                    proof_transaction.as_mut(),
                    pool,
                    &self.chain,
                    required_from_block,
                    through_block,
                    &discovery_history_source_families,
                    &mut manifest_progress,
                )
                .await?
            } else {
                load_required_watched_tuples_in_transaction(
                    proof_transaction.as_mut(),
                    &self.chain,
                    required_from_block,
                    through_block,
                    &discovery_history_source_families,
                )
                .await?
            }
        } else {
            Vec::new()
        };
        match progress.as_deref_mut() {
            Some(progress) => {
                ensure_generation_bound_coverage_with_live_selection_with_progress(
                    pool,
                    proof_transaction.as_mut(),
                    &self.chain,
                    &discovery_history_requirements,
                    state.retention_generation,
                    selected_addresses,
                    &selected_block_intervals,
                    progress,
                )
                .await?;
            }
            None => {
                ensure_generation_bound_coverage_with_live_selection(
                    proof_transaction.as_mut(),
                    &self.chain,
                    &discovery_history_requirements,
                    state.retention_generation,
                    selected_addresses,
                    &selected_block_intervals,
                )
                .await?;
            }
        }
        if current_proof_is_valid
            && state
                .proven_through_block
                .is_some_and(|proven| proven >= through_block)
        {
            let proof = RawLogClosureProof {
                retention_generation: state.retention_generation,
                discovery_admission_epoch: discovery_epoch,
                proven_through_block: state
                    .proven_through_block
                    .context("complete retained-history state is missing its proof boundary")?,
            };
            proof_transaction
                .commit()
                .await
                .context("failed to release live retained-history proof locks")?;
            record_startup_adapter_progress(pool, &mut progress).await?;
            return Ok(proof);
        }

        let closure_source_families = ens_v2_closure_source_families();
        let all_requirements = if let Some(progress) = progress.as_deref_mut() {
            let mut manifest_progress = StartupManifestProgress::new(progress);
            load_required_watched_tuples_in_transaction_with_progress(
                proof_transaction.as_mut(),
                pool,
                &self.chain,
                0,
                through_block,
                &closure_source_families,
                &mut manifest_progress,
            )
            .await?
        } else {
            load_required_watched_tuples_in_transaction(
                proof_transaction.as_mut(),
                &self.chain,
                0,
                through_block,
                &closure_source_families,
            )
            .await?
        };
        ensure!(
            !all_requirements.is_empty(),
            "ENSv2 retained history on {} has no authoritative watched closure through block {through_block}",
            self.chain
        );
        ensure_retained_semantic_witnesses_with_optional_progress(
            pool,
            proof_transaction.as_mut(),
            &self.chain,
            &all_requirements,
            through_block,
            &mut progress,
        )
        .await?;

        sqlx::query(
            r#"
            UPDATE raw_log_staging_input_revisions
            SET retained_history_complete = true,
                incomplete_since = NULL,
                proven_retention_generation = retention_generation,
                proven_discovery_admission_epoch = $2,
                proven_through_block = $3
            WHERE chain_id = $1
              AND retention_generation = $4
            "#,
        )
        .bind(&self.chain)
        .bind(discovery_epoch)
        .bind(through_block)
        .bind(state.retention_generation)
        .execute(proof_transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to store ENSv2 live retained-history proof for {}",
                self.chain
            )
        })?;
        proof_transaction
            .commit()
            .await
            .context("failed to commit ENSv2 live retained-history proof")?;
        record_startup_adapter_progress(pool, &mut progress).await?;
        Ok(RawLogClosureProof {
            retention_generation: state.retention_generation,
            discovery_admission_epoch: discovery_epoch,
            proven_through_block: through_block,
        })
    }

    /// Capture the root/registry/resolver discovery-history requirement union
    /// which the caller's complete fetch represents at its new target. A
    /// post-sync epoch may add authority; [`Self::finish`] requires durable
    /// coverage for only the portions not in this pre-sync union. Resolver
    /// requirements remain outside the retained-history proof and semantic
    /// witness set.
    pub(in crate::ens_v2_registry) async fn load_requirements_through(
        &self,
        pool: &PgPool,
        proof: RawLogClosureProof,
        through_block: i64,
    ) -> Result<Vec<RequiredWatchedTuple>> {
        self.load_requirements_through_inner(pool, proof, through_block, None)
            .await
    }

    pub(in crate::ens_v2_registry) async fn load_requirements_through_with_progress(
        &self,
        pool: &PgPool,
        proof: RawLogClosureProof,
        through_block: i64,
        progress: &mut dyn StartupAdapterProgress,
    ) -> Result<Vec<RequiredWatchedTuple>> {
        self.load_requirements_through_inner(pool, proof, through_block, Some(progress))
            .await
    }

    async fn load_requirements_through_inner(
        &self,
        pool: &PgPool,
        proof: RawLogClosureProof,
        through_block: i64,
        mut progress: Option<&mut dyn StartupAdapterProgress>,
    ) -> Result<Vec<RequiredWatchedTuple>> {
        ensure_discovery_epoch_row(pool, &self.chain).await?;
        let mut transaction = pool
            .begin()
            .await
            .context("failed to begin ENSv2 pre-sync requirement capture")?;
        let current_epoch = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT epoch
            FROM discovery_admission_epochs
            WHERE chain_id = $1
            FOR SHARE
            "#,
        )
        .bind(&self.chain)
        .fetch_one(transaction.as_mut())
        .await
        .with_context(|| {
            format!(
                "failed to lock ENSv2 discovery-admission epoch while capturing requirements for {}",
                self.chain
            )
        })?;
        ensure!(
            current_epoch == proof.discovery_admission_epoch,
            "ENSv2 discovery admission changed before requirement capture on {}: expected epoch {}, observed {current_epoch}",
            self.chain,
            proof.discovery_admission_epoch
        );
        let source_families = ens_v2_discovery_history_source_families();
        let requirements = if let Some(progress) = progress.as_deref_mut() {
            let mut manifest_progress = StartupManifestProgress::new(progress);
            load_required_watched_tuples_in_transaction_with_progress(
                transaction.as_mut(),
                pool,
                &self.chain,
                0,
                through_block,
                &source_families,
                &mut manifest_progress,
            )
            .await?
        } else {
            load_required_watched_tuples_in_transaction(
                transaction.as_mut(),
                &self.chain,
                0,
                through_block,
                &source_families,
            )
            .await?
        };
        transaction
            .commit()
            .await
            .context("failed to release ENSv2 pre-sync requirement locks")?;
        Ok(requirements)
    }

    pub(in crate::ens_v2_registry) async fn abort(self) -> Result<()> {
        self.transaction
            .rollback()
            .await
            .context("failed to release ENSv2 raw-log read fence after reconciliation failure")
    }

    pub(in crate::ens_v2_registry) async fn release(self) -> Result<()> {
        self.transaction
            .commit()
            .await
            .context("failed to release ENSv2 raw-log read fence")
    }
}
