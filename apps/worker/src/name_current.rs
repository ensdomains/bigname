mod coverage;
mod json;
mod load;
mod project;
mod resolution;
mod supplemental;
mod types;
mod wildcard;

use anyhow::Result;
use bigname_storage::projection_staging::{
    analyze_name_current_replacement_table, publish_name_current_replacement_table_in_transaction,
    stage_name_current_replacement_rows_in_transaction,
};
use bigname_storage::{NameCurrentRow, delete_name_current, upsert_name_current_rows};
use coverage::build_exact_name_coverage;
use futures_util::{StreamExt, TryStreamExt, pin_mut, stream};
use json::{build_declared_summary, build_provenance};
use load::{
    load_canonical_name_surface, load_canonical_name_surfaces_after, load_current_binding_context,
    load_history_heads, load_relevant_events,
};
use project::{
    build_canonicality_summary, build_chain_positions, max_timestamp, min_timestamp, project_facts,
};
use resolution::build_supported_resolution_projection;
use serde_json::Value;
use sqlx::{PgPool, types::time::OffsetDateTime};
use supplemental::{
    load_active_basenames_execution_manifest, load_supplemental_chain_observations,
};
use types::{NameSurfaceSeed, WildcardSourceContext};
use wildcard::load_wildcard_source_context;

#[cfg(test)]
use bigname_storage::{
    CanonicalityState, HistoryScope, SurfaceBindingKind, load_name_history_head,
};
#[cfg(test)]
use json::{format_timestamp, history_pointer_from_event, history_pointer_json};
#[cfg(test)]
use load::load_name_resource_ids;
#[cfg(test)]
use resolution::{empty_alias_detail, empty_transport_detail, empty_wildcard_detail};
#[cfg(test)]
use serde_json::json;
#[cfg(test)]
use sqlx::Row;
#[cfg(test)]
use types::RelevantEvent;
#[cfg(test)]
use uuid::Uuid;

use crate::primary_name::rebuild_heartbeat::{
    LoopHeartbeat, record_rebuild_progress, run_rebuild_phase,
};

const ENS_NAMESPACE: &str = "ens";
const BASENAMES_NAMESPACE: &str = "basenames";
const ENS_V1_AUTHORITY_DERIVATION_KIND: &str = "ens_v1_unwrapped_authority";
const ENS_V2_REGISTRY_DERIVATION_KIND: &str = "ens_v2_registry_resource_surface";
const ENS_V2_REGISTRAR_DERIVATION_KIND: &str = "ens_v2_registrar";
const ENS_V2_RESOLVER_DERIVATION_KIND: &str = "ens_v2_resolver";
const SOURCE_FAMILY_ENS_V2_REGISTRY_L1: &str = "ens_v2_registry_l1";
const SOURCE_FAMILY_ENS_V2_REGISTRAR_L1: &str = "ens_v2_registrar_l1";
const SELECTED_ENS_V2_EXACT_NAME_DEPLOYMENT_EPOCH: &str = "ens_v2_sepolia_post_audit";
const CAPABILITY_STATUS_SUPPORTED: &str = "supported";
const MANIFEST_ROLLOUT_STATUS_ACTIVE: &str = "active";
const ETHEREUM_SEPOLIA_CHAIN_ID: &str = "ethereum-sepolia";
const ETHEREUM_MAINNET_CHAIN_ID: &str = "ethereum-mainnet";
const BASE_MAINNET_CHAIN_ID: &str = "base-mainnet";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRAR: &str = "basenames_base_registrar";
const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";
const SOURCE_FAMILY_BASENAMES_BASE_RESOLVER: &str = "basenames_base_resolver";
const SOURCE_FAMILY_BASENAMES_EXECUTION: &str = "basenames_execution";
const VERIFIED_RESOLUTION_CAPABILITY: &str = "verified_resolution";
const BASENAMES_V1_DEPLOYMENT_EPOCH: &str = "basenames_v1";
const BASENAMES_L1_RESOLVER_ADDRESS: &str = "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31";
const NAME_CURRENT_DERIVATION_KIND: &str = "name_current_rebuild";
const NAME_CURRENT_REBUILD_CONCURRENCY: usize = 32;
const EVENT_KIND_ALIAS_CHANGED: &str = "AliasChanged";
const EVENT_KIND_RESOLVER_CHANGED: &str = "ResolverChanged";
const EVENT_KIND_RECORD_VERSION_CHANGED: &str = "RecordVersionChanged";
const EVENT_KIND_REGISTRAR_NAME_REGISTERED: &str = "RegistrarNameRegistered";
const RECORD_INVENTORY_UNSUPPORTED_REASON: &str =
    "record_inventory remains unsupported in the ENSv1 name_current rebuild";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
#[cfg(not(test))]
const NAME_CURRENT_REBUILD_STAGE_BATCH_SIZE: usize = 2_000;
#[cfg(test)]
const NAME_CURRENT_REBUILD_STAGE_BATCH_SIZE: usize = 1;
const RELEVANT_EVENT_KINDS: &[&str] = &[
    "AuthorityEpochChanged",
    "AuthorityTransferred",
    EVENT_KIND_ALIAS_CHANGED,
    "ExpiryChanged",
    "RegistrationGranted",
    EVENT_KIND_REGISTRAR_NAME_REGISTERED,
    "RegistrationReleased",
    "RegistrationRenewed",
    EVENT_KIND_RECORD_VERSION_CHANGED,
    EVENT_KIND_RESOLVER_CHANGED,
    "SurfaceBound",
    "SurfaceUnbound",
    "TokenResourceLinked",
    "TokenRegenerated",
    "TokenControlTransferred",
];
const CANONICAL_STATE_FILTER: &str = r#"
  IN (
    'canonical'::canonicality_state,
    'safe'::canonicality_state,
    'finalized'::canonicality_state
  )
"#;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NameCurrentRebuildSummary {
    pub requested_name_count: usize,
    pub upserted_row_count: usize,
    pub deleted_row_count: u64,
}

pub async fn rebuild_name_current(
    pool: &PgPool,
    logical_name_id: Option<&str>,
) -> Result<NameCurrentRebuildSummary> {
    rebuild_name_current_inner(pool, logical_name_id, None).await
}

pub(crate) async fn rebuild_name_current_with_heartbeat(
    pool: &PgPool,
    logical_name_id: Option<&str>,
    loop_heartbeat: &mut LoopHeartbeat,
) -> Result<NameCurrentRebuildSummary> {
    rebuild_name_current_inner(pool, logical_name_id, Some(loop_heartbeat)).await
}

async fn rebuild_name_current_inner(
    pool: &PgPool,
    logical_name_id: Option<&str>,
    loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<NameCurrentRebuildSummary> {
    match logical_name_id {
        Some(logical_name_id) => {
            rebuild_one_name_current(pool, logical_name_id, loop_heartbeat).await
        }
        None => {
            let summary = rebuild_all_name_current(pool, None, loop_heartbeat).await?;
            crate::replay::staging::cleanup_projection_checkpoint(pool, "name_current").await?;
            Ok(summary)
        }
    }
}

pub(crate) async fn rebuild_name_current_for_replay(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<NameCurrentRebuildSummary> {
    rebuild_all_name_current(pool, normalized_target_block, loop_heartbeat).await
}

async fn rebuild_all_name_current(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<NameCurrentRebuildSummary> {
    let mut checkpoint = crate::replay::staging::ProjectionStagingCheckpoint::load_or_start(
        pool,
        "name_current",
        normalized_target_block,
    )
    .await?;
    loop {
        if !checkpoint.staging_complete() {
            loop {
                let input_fence = checkpoint.prepare_next_batch(pool).await?;
                let after_logical_name_id = checkpoint
                    .last_source_key()
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let page = run_rebuild_phase(
                    pool,
                    &mut loop_heartbeat,
                    "name_current.load_inputs",
                    load_canonical_name_surfaces_after(
                        pool,
                        after_logical_name_id.as_deref(),
                        i64::try_from(NAME_CURRENT_REBUILD_STAGE_BATCH_SIZE)?,
                    ),
                )
                .await?;
                if page.is_empty() {
                    if checkpoint.mark_staging_complete(pool, input_fence).await? {
                        break;
                    }
                    continue;
                }
                let last_source_key = Value::String(
                    page.last()
                        .expect("name_current staging page must not be empty")
                        .logical_name_id
                        .clone(),
                );
                let rows = build_name_current_page(pool, &page, &mut loop_heartbeat).await?;
                let mut transaction = pool.begin().await?;
                let staged = stage_name_current_replacement_rows_in_transaction(
                    &mut transaction,
                    checkpoint.stage_table(0)?,
                    &rows,
                )
                .await?;
                let progress =
                    checkpoint.progress_after_batch(page.len(), last_source_key, staged, 0)?;
                checkpoint
                    .persist_progress(&mut transaction, &progress, &input_fence)
                    .await?;
                transaction.commit().await?;
                checkpoint.accept_progress(progress, input_fence);
                let completed_name_count = checkpoint.completed_source_count()?;
                if completed_name_count.is_multiple_of(5_000) {
                    tracing::info!(
                        projection = "name_current",
                        completed_name_count,
                        staged_row_count = checkpoint.staged_row_count()?,
                        "name_current rebuild rows built"
                    );
                }
            }
        }
        let requested_name_count = checkpoint.completed_source_count()?;
        let upserted_row_count = checkpoint.staged_row_count()?;
        let stage_table = checkpoint.stage_table(0)?.to_owned();
        analyze_name_current_replacement_table(pool, &stage_table).await?;
        let published =
            run_rebuild_phase(pool, &mut loop_heartbeat, "name_current.publish", async {
                let Some(mut transaction) =
                    checkpoint.begin_fenced_publish_transaction(pool).await?
                else {
                    return Ok(None);
                };
                let counts = publish_name_current_replacement_table_in_transaction(
                    &mut transaction,
                    &stage_table,
                )
                .await?;
                transaction.commit().await?;
                Ok(Some(counts))
            })
            .await?;
        let Some((published_row_count, deleted_row_count)) = published else {
            continue;
        };
        tracing::info!(
            projection = "name_current",
            requested_name_count,
            completed_name_count = requested_name_count,
            upserted_row_count,
            published_row_count,
            deleted_row_count,
            "name_current rebuild replacement published"
        );
        return Ok(NameCurrentRebuildSummary {
            requested_name_count,
            upserted_row_count,
            deleted_row_count,
        });
    }
}

async fn build_name_current_page(
    pool: &PgPool,
    names: &[NameSurfaceSeed],
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<Vec<NameCurrentRow>> {
    let rows = stream::iter(names.iter().cloned())
        .map(|name| {
            let pool = pool.clone();
            async move { build_name_current_row(&pool, &name).await }
        })
        .buffer_unordered(NAME_CURRENT_REBUILD_CONCURRENCY);
    pin_mut!(rows);
    let mut completed = Vec::with_capacity(names.len());
    while let Some(row) = rows.try_next().await? {
        completed.push(row);
        record_rebuild_progress(pool, loop_heartbeat).await;
    }
    Ok(completed)
}

async fn rebuild_one_name_current(
    pool: &PgPool,
    logical_name_id: &str,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<NameCurrentRebuildSummary> {
    let Some(name) = load_canonical_name_surface(pool, logical_name_id).await? else {
        let deleted_row_count = delete_name_current(pool, logical_name_id).await?;
        record_rebuild_progress(pool, &mut loop_heartbeat).await;
        return Ok(NameCurrentRebuildSummary {
            requested_name_count: 1,
            upserted_row_count: 0,
            deleted_row_count,
        });
    };
    record_rebuild_progress(pool, &mut loop_heartbeat).await;

    let row = build_name_current_row_inner(pool, &name, &mut loop_heartbeat).await?;
    let upserted_row_count = upsert_name_current_rows(pool, &[row]).await?.len();
    record_rebuild_progress(pool, &mut loop_heartbeat).await;
    Ok(NameCurrentRebuildSummary {
        requested_name_count: 1,
        upserted_row_count,
        deleted_row_count: 0,
    })
}

async fn build_name_current_row(pool: &PgPool, name: &NameSurfaceSeed) -> Result<NameCurrentRow> {
    build_name_current_row_inner(pool, name, &mut None).await
}

async fn build_name_current_row_inner(
    pool: &PgPool,
    name: &NameSurfaceSeed,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<NameCurrentRow> {
    let current_binding = load_current_binding_context(pool, &name.logical_name_id).await?;
    record_rebuild_progress(pool, loop_heartbeat).await;
    let events = load_relevant_events(pool, name, current_binding.as_ref()).await?;
    record_rebuild_progress(pool, loop_heartbeat).await;
    let history_heads = load_history_heads(pool, &name.logical_name_id).await?;
    record_rebuild_progress(pool, loop_heartbeat).await;
    let basenames_execution_manifest =
        load_active_basenames_execution_manifest(pool, &name.namespace).await?;
    record_rebuild_progress(pool, loop_heartbeat).await;
    let wildcard_source_context =
        load_wildcard_source_context(pool, name, current_binding.as_ref()).await?;
    record_rebuild_progress(pool, loop_heartbeat).await;
    let supplemental_chain_observations = load_supplemental_chain_observations(
        pool,
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        wildcard_source_context.as_ref(),
        basenames_execution_manifest.as_ref(),
    )
    .await?;
    record_rebuild_progress(pool, loop_heartbeat).await;
    let mut facts = project_facts(&events, current_binding.as_ref(), &history_heads)?;
    // created_at is the first observation of this name. Supplemental observations
    // can come from parent wildcard names or Basenames transport lineage.
    facts.created_at = min_timestamp(name, current_binding.as_ref(), &events, &history_heads, &[])
        .map(|timestamp| timestamp.unix_timestamp());
    let chain_positions = build_chain_positions(
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        &supplemental_chain_observations,
    );
    let supported_resolution_projection = build_supported_resolution_projection(
        name,
        current_binding.as_ref(),
        &facts,
        &events,
        &chain_positions,
        wildcard_source_context.as_ref(),
        basenames_execution_manifest.as_ref(),
    )?;
    let canonicality_summary = build_canonicality_summary(
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        &supplemental_chain_observations,
    );
    let provenance = build_provenance(
        &events,
        &history_heads,
        wildcard_source_context.as_ref(),
        supported_resolution_projection
            .as_ref()
            .map(|projection| projection.manifest_versions.as_slice())
            .unwrap_or(&[]),
    )?;
    let manifest_version = events
        .iter()
        .map(|event| event.manifest_version)
        .chain(
            wildcard_source_context
                .as_ref()
                .into_iter()
                .flat_map(WildcardSourceContext::events)
                .map(|event| event.manifest_version),
        )
        .chain(history_heads.iter().map(|event| event.manifest_version))
        .max()
        .unwrap_or(1);
    let last_recomputed_at = max_timestamp(
        name,
        current_binding.as_ref(),
        &events,
        &history_heads,
        &supplemental_chain_observations,
    )
    .unwrap_or(OffsetDateTime::UNIX_EPOCH);

    Ok(NameCurrentRow {
        logical_name_id: name.logical_name_id.clone(),
        namespace: name.namespace.clone(),
        canonical_display_name: name.canonical_display_name.clone(),
        normalized_name: name.normalized_name.clone(),
        namehash: name.namehash.clone(),
        surface_binding_id: current_binding
            .as_ref()
            .map(|binding| binding.surface_binding_id),
        resource_id: current_binding.as_ref().map(|binding| binding.resource_id),
        token_lineage_id: current_binding
            .as_ref()
            .and_then(|binding| binding.token_lineage_id),
        binding_kind: current_binding.as_ref().map(|binding| binding.binding_kind),
        declared_summary: build_declared_summary(
            facts,
            supported_resolution_projection.map(|projection| projection.topology),
            name.namespace == ENS_NAMESPACE
                && current_binding
                    .as_ref()
                    .and_then(|binding| binding.resource_authority_kind.as_deref())
                    == Some("wrapper"),
        ),
        provenance,
        coverage: build_exact_name_coverage(&name.namespace, &events),
        chain_positions,
        canonicality_summary,
        manifest_version,
        last_recomputed_at,
    })
}

#[cfg(test)]
mod tests;
