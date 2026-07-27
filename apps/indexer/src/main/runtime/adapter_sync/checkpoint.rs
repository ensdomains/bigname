use anyhow::{Result, bail};
use bigname_adapters::StartupAdapterVersion;
use bigname_storage::{
    StartupAdapterSyncCompletion, StartupAdapterSyncDecision, StartupAdapterSyncKey,
    complete_startup_adapter_sync, prepare_startup_adapter_sync,
};
use tracing::{info, warn};

pub(crate) struct StartupFamilySyncAttempt {
    deployment_profile: Option<String>,
    started_key: Option<StartupAdapterSyncKey>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupFamilySyncCompletion {
    Stable,
    Retry,
}

impl StartupFamilySyncAttempt {
    pub(crate) async fn complete(
        self,
        pool: &sqlx::PgPool,
        chain: &str,
        adapter: StartupAdapterVersion,
    ) -> Result<()> {
        if self.complete_or_retry(pool, chain, adapter).await? == StartupFamilySyncCompletion::Retry
        {
            bail!(
                "startup adapter input changed while syncing {adapter_name} for {chain}; \
                 refusing to publish completion so the next startup performs a full re-scan",
                adapter_name = adapter.adapter,
            );
        }
        Ok(())
    }

    pub(crate) async fn complete_or_retry(
        self,
        pool: &sqlx::PgPool,
        chain: &str,
        adapter: StartupAdapterVersion,
    ) -> Result<StartupFamilySyncCompletion> {
        let Some(deployment_profile) = self.deployment_profile else {
            return Ok(StartupFamilySyncCompletion::Stable);
        };
        match complete_startup_adapter_sync(
            pool,
            &deployment_profile,
            chain,
            adapter.adapter,
            adapter.semantic_version,
            self.started_key,
        )
        .await?
        {
            StartupAdapterSyncCompletion::Completed => {
                info!(
                    service = "indexer",
                    command = "startup-adapter-sync",
                    deployment_profile,
                    chain,
                    adapter = adapter.adapter,
                    adapter_semantic_version = adapter.semantic_version,
                    "startup adapter family completion checkpoint published"
                );
                Ok(StartupFamilySyncCompletion::Stable)
            }
            StartupAdapterSyncCompletion::KeyUnknown => {
                warn!(
                    service = "indexer",
                    command = "startup-adapter-sync",
                    deployment_profile,
                    chain,
                    adapter = adapter.adapter,
                    adapter_semantic_version = adapter.semantic_version,
                    "startup adapter family completed with an unknown checkpoint key; \
                     the next boot will run the full sync again"
                );
                Ok(StartupFamilySyncCompletion::Stable)
            }
            StartupAdapterSyncCompletion::InputChanged => {
                if matches!(
                    prepare_startup_adapter_sync(
                        pool,
                        &deployment_profile,
                        chain,
                        adapter.adapter,
                        adapter.semantic_version,
                    )
                    .await?,
                    StartupAdapterSyncDecision::ReuseCompleted
                ) {
                    info!(
                        service = "indexer",
                        command = "startup-adapter-sync",
                        deployment_profile,
                        chain,
                        adapter = adapter.adapter,
                        adapter_semantic_version = adapter.semantic_version,
                        "startup adapter published its own completion after converging on a newer key"
                    );
                    return Ok(StartupFamilySyncCompletion::Stable);
                }
                info!(
                    service = "indexer",
                    command = "startup-adapter-sync",
                    deployment_profile,
                    chain,
                    adapter = adapter.adapter,
                    adapter_semantic_version = adapter.semantic_version,
                    "startup adapter input advanced during the pass; another full pass is required"
                );
                Ok(StartupFamilySyncCompletion::Retry)
            }
        }
    }
}

pub(crate) async fn prepare_startup_family_sync(
    pool: &sqlx::PgPool,
    deployment_profile: Option<&str>,
    chain: &str,
    adapter: StartupAdapterVersion,
) -> Result<Option<StartupFamilySyncAttempt>> {
    let Some(deployment_profile) = deployment_profile else {
        return Ok(Some(StartupFamilySyncAttempt {
            deployment_profile: None,
            started_key: None,
        }));
    };
    match prepare_startup_adapter_sync(
        pool,
        deployment_profile,
        chain,
        adapter.adapter,
        adapter.semantic_version,
    )
    .await?
    {
        StartupAdapterSyncDecision::ReuseCompleted => {
            info!(
                service = "indexer",
                command = "startup-adapter-sync",
                deployment_profile,
                chain,
                adapter = adapter.adapter,
                adapter_semantic_version = adapter.semantic_version,
                "startup adapter family full scan skipped after checkpoint verification"
            );
            Ok(None)
        }
        StartupAdapterSyncDecision::RunFullSync { started_key } => {
            Ok(Some(StartupFamilySyncAttempt {
                deployment_profile: Some(deployment_profile.to_owned()),
                started_key,
            }))
        }
    }
}
