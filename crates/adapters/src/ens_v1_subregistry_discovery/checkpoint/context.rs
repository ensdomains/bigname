use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::PgPool;

#[cfg(test)]
use super::ADAPTER;

pub(in crate::ens_v1_subregistry_discovery) const FULL_CLOSURE_CHECKPOINT_SCOPE: &str =
    "full_closure";
const STARTUP_CHECKPOINT_SCOPE: &str = "startup_adapter_sync";
const STARTUP_CHECKPOINT_CURSOR_KIND: &str = "startup_adapter_owned_raw_log_state";
const STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD: &str = "startup_discovery_admission_epoch";
const STARTUP_CANONICAL_LINEAGE_HEAD_FIELD: &str =
    bigname_storage::STARTUP_CANONICAL_LINEAGE_HEAD_FIELD;
const STARTUP_LINEAGE_MUTATION_REVISION_FIELD: &str =
    bigname_storage::STARTUP_LINEAGE_MUTATION_REVISION_FIELD;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ens_v1_subregistry_discovery) struct ReplayAdapterCheckpointContext {
    pub(in crate::ens_v1_subregistry_discovery) deployment_profile: String,
    pub(in crate::ens_v1_subregistry_discovery) cursor_kind: String,
    pub(in crate::ens_v1_subregistry_discovery) range_start_block_number: i64,
    pub(in crate::ens_v1_subregistry_discovery) target_block_number: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ens_v1_subregistry_discovery) struct StartupAdapterCheckpointContext {
    deployment_profile: String,
    range_start_block_number: i64,
    target_block_number: i64,
}

impl StartupAdapterCheckpointContext {
    pub(in crate::ens_v1_subregistry_discovery) fn new(
        deployment_profile: impl Into<String>,
        target_block_number: i64,
    ) -> Result<Self> {
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

    pub(in crate::ens_v1_subregistry_discovery) fn deployment_profile(&self) -> &str {
        &self.deployment_profile
    }

    pub(in crate::ens_v1_subregistry_discovery) fn cursor_kind(&self) -> &'static str {
        STARTUP_CHECKPOINT_CURSOR_KIND
    }

    pub(in crate::ens_v1_subregistry_discovery) fn checkpoint_scope(&self) -> &'static str {
        STARTUP_CHECKPOINT_SCOPE
    }

    pub(in crate::ens_v1_subregistry_discovery) fn range_start_block_number(&self) -> i64 {
        self.range_start_block_number
    }

    pub(in crate::ens_v1_subregistry_discovery) fn target_block_number(&self) -> i64 {
        self.target_block_number
    }

    pub(in crate::ens_v1_subregistry_discovery) async fn adapter_context(
        &self,
        pool: &PgPool,
        chain: &str,
        adapter_semantic_version: i64,
    ) -> Result<AdapterCheckpointContext> {
        let discovery_admission_epoch =
            bigname_manifests::try_load_discovery_admission_epoch(pool, chain).await?;
        let lineage_state =
            bigname_storage::load_startup_adapter_lineage_state(pool, chain).await?;
        let (lineage_mutation_revision, canonical_lineage_head) = lineage_state
            .map_or((None, None), |state| {
                (Some(state.mutation_revision), state.canonical_lineage_head)
            });
        let schema_migration_state =
            bigname_storage::load_startup_adapter_schema_state(pool).await?;
        Ok(AdapterCheckpointContext {
            deployment_profile: self.deployment_profile.clone(),
            cursor_kind: STARTUP_CHECKPOINT_CURSOR_KIND.to_owned(),
            checkpoint_scope: STARTUP_CHECKPOINT_SCOPE,
            range_start_block_number: self.range_start_block_number,
            target_block_number: self.target_block_number,
            startup_discovery_admission_epoch: discovery_admission_epoch,
            startup_lineage_mutation_revision: lineage_mutation_revision,
            startup_canonical_lineage_head: canonical_lineage_head,
            startup_adapter_semantic_version: Some(adapter_semantic_version),
            startup_schema_migration_state: schema_migration_state,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ens_v1_subregistry_discovery) struct AdapterCheckpointContext {
    pub(in crate::ens_v1_subregistry_discovery) deployment_profile: String,
    pub(in crate::ens_v1_subregistry_discovery) cursor_kind: String,
    pub(in crate::ens_v1_subregistry_discovery) checkpoint_scope: &'static str,
    pub(in crate::ens_v1_subregistry_discovery) range_start_block_number: i64,
    pub(in crate::ens_v1_subregistry_discovery) target_block_number: i64,
    pub(in crate::ens_v1_subregistry_discovery) startup_discovery_admission_epoch: Option<i64>,
    pub(in crate::ens_v1_subregistry_discovery) startup_lineage_mutation_revision: Option<i64>,
    pub(in crate::ens_v1_subregistry_discovery) startup_canonical_lineage_head:
        Option<bigname_storage::StartupCanonicalLineageHead>,
    pub(in crate::ens_v1_subregistry_discovery) startup_adapter_semantic_version: Option<i64>,
    pub(in crate::ens_v1_subregistry_discovery) startup_schema_migration_state: Option<(i64, i64)>,
}

impl AdapterCheckpointContext {
    pub(in crate::ens_v1_subregistry_discovery) fn for_replay(
        context: &ReplayAdapterCheckpointContext,
    ) -> Self {
        Self {
            deployment_profile: context.deployment_profile.clone(),
            cursor_kind: context.cursor_kind.clone(),
            checkpoint_scope: FULL_CLOSURE_CHECKPOINT_SCOPE,
            range_start_block_number: context.range_start_block_number,
            target_block_number: context.target_block_number,
            startup_discovery_admission_epoch: None,
            startup_lineage_mutation_revision: None,
            startup_canonical_lineage_head: None,
            startup_adapter_semantic_version: None,
            startup_schema_migration_state: None,
        }
    }

    pub(in crate::ens_v1_subregistry_discovery) fn is_startup(&self) -> bool {
        self.checkpoint_scope == STARTUP_CHECKPOINT_SCOPE
    }

    pub(in crate::ens_v1_subregistry_discovery) fn startup_authority_changed(
        &self,
        state_payload: &Value,
    ) -> bool {
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

    pub(in crate::ens_v1_subregistry_discovery) fn startup_lineage_changed(
        &self,
        state_payload: &Value,
    ) -> bool {
        if !self.is_startup() {
            return false;
        }
        let Some(expected_revision) = self.startup_lineage_mutation_revision else {
            return true;
        };
        state_payload
            .get(STARTUP_LINEAGE_MUTATION_REVISION_FIELD)
            .and_then(Value::as_i64)
            != Some(expected_revision)
            || state_payload.get(STARTUP_CANONICAL_LINEAGE_HEAD_FIELD)
                != Some(&serde_json::json!(&self.startup_canonical_lineage_head))
    }

    pub(in crate::ens_v1_subregistry_discovery) fn startup_version_changed(
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

    pub(in crate::ens_v1_subregistry_discovery) fn startup_adapter_semantic_version(
        &self,
    ) -> Option<i64> {
        self.startup_adapter_semantic_version
    }

    pub(in crate::ens_v1_subregistry_discovery) fn startup_schema_migration_count(
        &self,
    ) -> Option<i64> {
        self.startup_schema_migration_state.map(|(count, _)| count)
    }

    pub(in crate::ens_v1_subregistry_discovery) fn startup_schema_migration_max_version(
        &self,
    ) -> Option<i64> {
        self.startup_schema_migration_state
            .map(|(_, max_version)| max_version)
    }

    pub(in crate::ens_v1_subregistry_discovery) fn bind_startup_authority(
        &self,
        mut state_payload: Value,
    ) -> Result<Value> {
        if !self.is_startup() {
            return Ok(state_payload);
        }
        let payload = state_payload
            .as_object_mut()
            .context("adapter checkpoint state payload must be a JSON object")?;
        if let Some(discovery_admission_epoch) = self.startup_discovery_admission_epoch {
            payload.insert(
                STARTUP_DISCOVERY_ADMISSION_EPOCH_FIELD.to_owned(),
                Value::from(discovery_admission_epoch),
            );
        }
        if let Some(lineage_mutation_revision) = self.startup_lineage_mutation_revision {
            payload.insert(
                STARTUP_LINEAGE_MUTATION_REVISION_FIELD.to_owned(),
                Value::from(lineage_mutation_revision),
            );
        }
        payload.insert(
            STARTUP_CANONICAL_LINEAGE_HEAD_FIELD.to_owned(),
            serde_json::json!(&self.startup_canonical_lineage_head),
        );
        Ok(state_payload)
    }

    pub(in crate::ens_v1_subregistry_discovery) async fn refresh_startup_authority(
        &mut self,
        pool: &PgPool,
        chain: &str,
    ) -> Result<()> {
        if self.is_startup() {
            self.startup_discovery_admission_epoch =
                bigname_manifests::try_load_discovery_admission_epoch(pool, chain).await?;
            let lineage_state =
                bigname_storage::load_startup_adapter_lineage_state(pool, chain).await?;
            (
                self.startup_lineage_mutation_revision,
                self.startup_canonical_lineage_head,
            ) = lineage_state.map_or((None, None), |state| {
                (Some(state.mutation_revision), state.canonical_lineage_head)
            });
        }
        Ok(())
    }
}

#[cfg(test)]
pub(in crate::ens_v1_subregistry_discovery) async fn clear_startup_adapter_checkpoints(
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
          AND adapter = $5
          AND status = 'completed'
        "#,
    )
    .bind(context.deployment_profile())
    .bind(chain)
    .bind(context.cursor_kind())
    .bind(context.checkpoint_scope())
    .bind(ADAPTER)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to clear successful startup adapter checkpoint for {}/{chain}",
            context.deployment_profile()
        )
    })?;
    Ok(())
}
