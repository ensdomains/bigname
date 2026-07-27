use std::{future::Future, pin::Pin};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::PgPool;

pub(crate) const FULL_CLOSURE_CHECKPOINT_SCOPE: &str = "full_closure";
const STARTUP_CHECKPOINT_SCOPE: &str = "startup_adapter_sync";
const STARTUP_CHECKPOINT_CURSOR_KIND: &str = "startup_adapter_owned_raw_log_state";
const STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD: &str = "startup_discovery_admission_epoch";
const STARTUP_CHECKPOINT_ADAPTERS: [&str; 2] =
    ["ens_v1_subregistry_discovery", "ens_v1_unwrapped_authority"];

pub type StartupAdapterProgressFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

pub trait StartupAdapterProgress: Send {
    fn record<'a>(&'a mut self, pool: &'a PgPool) -> StartupAdapterProgressFuture<'a>;
}

pub(crate) async fn record_startup_adapter_progress(
    pool: &PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    Ok(())
}

pub(crate) fn reborrow_startup_adapter_progress<'a>(
    progress: &'a mut Option<&mut dyn StartupAdapterProgress>,
) -> Option<&'a mut dyn StartupAdapterProgress> {
    match progress.as_mut() {
        Some(progress) => Some(&mut **progress),
        None => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayAdapterCheckpointContext {
    pub deployment_profile: String,
    pub cursor_kind: String,
    pub range_start_block_number: i64,
    pub target_block_number: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupAdapterCheckpointContext {
    deployment_profile: String,
    range_start_block_number: i64,
    target_block_number: i64,
}

impl StartupAdapterCheckpointContext {
    pub fn new(deployment_profile: impl Into<String>, target_block_number: i64) -> Result<Self> {
        let deployment_profile = deployment_profile.into();
        ensure!(
            !deployment_profile.trim().is_empty(),
            "startup adapter checkpoint deployment profile must not be empty"
        );
        ensure!(
            target_block_number >= 0,
            "startup adapter checkpoint target block must not be negative"
        );
        Ok(Self {
            deployment_profile,
            range_start_block_number: 0,
            target_block_number,
        })
    }

    pub fn deployment_profile(&self) -> &str {
        &self.deployment_profile
    }

    pub fn cursor_kind(&self) -> &'static str {
        STARTUP_CHECKPOINT_CURSOR_KIND
    }

    pub fn checkpoint_scope(&self) -> &'static str {
        STARTUP_CHECKPOINT_SCOPE
    }

    pub fn range_start_block_number(&self) -> i64 {
        self.range_start_block_number
    }

    pub fn target_block_number(&self) -> i64 {
        self.target_block_number
    }

    pub(crate) async fn adapter_context(
        &self,
        pool: &PgPool,
        chain: &str,
        adapter_semantic_version: i64,
    ) -> Result<AdapterCheckpointContext> {
        let discovery_admission_epoch =
            bigname_manifests::try_load_discovery_admission_epoch(pool, chain).await?;
        let schema_migration_state =
            bigname_storage::load_startup_adapter_schema_state(pool).await?;
        Ok(AdapterCheckpointContext {
            deployment_profile: self.deployment_profile.clone(),
            cursor_kind: STARTUP_CHECKPOINT_CURSOR_KIND.to_owned(),
            checkpoint_scope: STARTUP_CHECKPOINT_SCOPE,
            range_start_block_number: self.range_start_block_number,
            target_block_number: self.target_block_number,
            startup_discovery_admission_epoch: discovery_admission_epoch,
            startup_adapter_semantic_version: Some(adapter_semantic_version),
            startup_schema_migration_state: schema_migration_state,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdapterCheckpointContext {
    pub(crate) deployment_profile: String,
    pub(crate) cursor_kind: String,
    pub(crate) checkpoint_scope: &'static str,
    pub(crate) range_start_block_number: i64,
    pub(crate) target_block_number: i64,
    pub(crate) startup_discovery_admission_epoch: Option<i64>,
    pub(crate) startup_adapter_semantic_version: Option<i64>,
    pub(crate) startup_schema_migration_state: Option<(i64, i64)>,
}

impl AdapterCheckpointContext {
    pub(crate) fn for_replay(context: &ReplayAdapterCheckpointContext) -> Self {
        Self {
            deployment_profile: context.deployment_profile.clone(),
            cursor_kind: context.cursor_kind.clone(),
            checkpoint_scope: FULL_CLOSURE_CHECKPOINT_SCOPE,
            range_start_block_number: context.range_start_block_number,
            target_block_number: context.target_block_number,
            startup_discovery_admission_epoch: None,
            startup_adapter_semantic_version: None,
            startup_schema_migration_state: None,
        }
    }

    pub(crate) fn is_startup(&self) -> bool {
        self.checkpoint_scope == STARTUP_CHECKPOINT_SCOPE
    }

    pub(crate) fn startup_authority_changed(&self, state_payload: &Value) -> bool {
        if !self.is_startup() {
            return false;
        }
        let Some(expected_epoch) = self.startup_discovery_admission_epoch else {
            return true;
        };
        state_payload
            .get(STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD)
            .and_then(Value::as_i64)
            != Some(expected_epoch)
    }

    pub(crate) fn startup_version_changed(
        &self,
        adapter_semantic_version: Option<i64>,
        schema_migration_count: Option<i64>,
        schema_migration_max_version: Option<i64>,
    ) -> bool {
        if !self.is_startup() {
            return false;
        }
        let Some(expected_adapter_version) = self.startup_adapter_semantic_version else {
            return true;
        };
        let Some((expected_migration_count, expected_max_migration)) =
            self.startup_schema_migration_state
        else {
            return true;
        };
        adapter_semantic_version != Some(expected_adapter_version)
            || schema_migration_count != Some(expected_migration_count)
            || schema_migration_max_version != Some(expected_max_migration)
    }

    pub(crate) fn startup_adapter_semantic_version(&self) -> Option<i64> {
        self.startup_adapter_semantic_version
    }

    pub(crate) fn startup_schema_migration_count(&self) -> Option<i64> {
        self.startup_schema_migration_state.map(|(count, _)| count)
    }

    pub(crate) fn startup_schema_migration_max_version(&self) -> Option<i64> {
        self.startup_schema_migration_state
            .map(|(_, max_version)| max_version)
    }

    pub(crate) fn bind_startup_authority(&self, mut state_payload: Value) -> Result<Value> {
        let Some(discovery_admission_epoch) = self.startup_discovery_admission_epoch else {
            return Ok(state_payload);
        };
        let payload = state_payload
            .as_object_mut()
            .context("adapter checkpoint state payload must be a JSON object")?;
        payload.insert(
            STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD.to_owned(),
            Value::from(discovery_admission_epoch),
        );
        Ok(state_payload)
    }

    pub(crate) async fn refresh_startup_authority(
        &mut self,
        pool: &PgPool,
        chain: &str,
    ) -> Result<()> {
        if self.is_startup() {
            self.startup_discovery_admission_epoch =
                bigname_manifests::try_load_discovery_admission_epoch(pool, chain).await?;
        }
        Ok(())
    }
}

pub async fn clear_startup_adapter_checkpoints(
    pool: &PgPool,
    chain: &str,
    context: &StartupAdapterCheckpointContext,
) -> Result<()> {
    sqlx::query(
        r#"
        DELETE FROM normalized_replay_adapter_checkpoints
        WHERE deployment_profile = $1
          AND chain_id = $2
          AND cursor_kind = $3
          AND checkpoint_scope = $4
          AND adapter = ANY($5::TEXT[])
          AND status = 'completed'
        "#,
    )
    .bind(context.deployment_profile())
    .bind(chain)
    .bind(context.cursor_kind())
    .bind(context.checkpoint_scope())
    .bind(STARTUP_CHECKPOINT_ADAPTERS.as_slice())
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to clear successful startup adapter checkpoints for {}/{chain}",
            context.deployment_profile()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};

    use super::*;

    #[test]
    fn startup_checkpoint_key_is_distinct_from_full_closure_replay() -> Result<()> {
        let startup = StartupAdapterCheckpointContext::new("mainnet", 42)?;
        assert_ne!(startup.checkpoint_scope(), FULL_CLOSURE_CHECKPOINT_SCOPE);
        assert_ne!(startup.cursor_kind(), "raw_fact_normalized_events");
        assert_eq!(startup.range_start_block_number(), 0);
        assert_eq!(startup.target_block_number(), 42);
        Ok(())
    }

    #[tokio::test]
    async fn missing_discovery_epoch_stays_unknown_for_private_startup_checkpoints() -> Result<()> {
        let database = TestDatabase::create_migrated(
            TestDatabaseConfig::new("missing_startup_discovery_epoch"),
            &bigname_storage::MIGRATOR,
            "failed to migrate missing startup discovery-epoch test database",
        )
        .await?;
        let startup = StartupAdapterCheckpointContext::new("mainnet", 42)?;
        let context = startup
            .adapter_context(database.pool(), "missing-epoch-chain", 1)
            .await?;

        assert_eq!(
            context.startup_discovery_admission_epoch, None,
            "a missing authority row must not become a reusable synthetic epoch"
        );
        assert!(
            context.startup_authority_changed(
                &serde_json::json!({ STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD: 0 })
            ),
            "unknown authority must reset staged startup state even when it stored epoch zero"
        );

        database.cleanup().await
    }
}
