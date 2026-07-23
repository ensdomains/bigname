use anyhow::{Context, Result};
use bigname_storage::projection_staging::{
    insert_address_names_current_full_rebuild_rows_in_transaction,
    publish_address_names_current_full_rebuild_in_transaction,
};
use bigname_storage::{
    AddressNamesCurrentAddressReplacement, AddressNamesCurrentFullRebuild,
    begin_address_names_current_address_replacement,
    drop_address_names_current_address_replacement,
    insert_address_names_current_address_replacement_rows,
    publish_address_names_current_address_replacement, replace_address_names_current_logical_names,
};
use futures_util::{StreamExt, TryStreamExt, pin_mut, stream};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::primary_name::rebuild_heartbeat::{
    LoopHeartbeat, record_rebuild_progress, run_rebuild_phase,
};

use super::{
    AddressNamesCurrentRebuildSummary,
    load::{
        load_current_bindings_for_address, load_current_bindings_for_logical_name,
        stream_current_bindings_after,
    },
    model::CurrentBindingSeed,
    projection::build_rows_for_binding,
    util::normalize_address,
};

#[cfg(not(test))]
const ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE: usize = 2_000;
#[cfg(test)]
const ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE: usize = 1;
const ADDRESS_NAMES_CURRENT_REBUILD_CONCURRENCY: usize = 8;

pub async fn rebuild_address_names_current(
    pool: &PgPool,
    address: Option<&str>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    rebuild_address_names_current_inner(pool, address, None).await
}

async fn rebuild_address_names_current_inner(
    pool: &PgPool,
    address: Option<&str>,
    loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    match address {
        Some(address) => rebuild_one_address(pool, address, loop_heartbeat).await,
        None => {
            let summary = rebuild_all_addresses(pool, None, loop_heartbeat).await?;
            crate::replay::staging::cleanup_projection_checkpoint(pool, "address_names_current")
                .await?;
            Ok(summary)
        }
    }
}

pub(crate) async fn rebuild_address_names_current_for_replay(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    rebuild_all_addresses(pool, normalized_target_block, loop_heartbeat).await
}

pub async fn rebuild_address_names_current_logical_name(
    pool: &PgPool,
    address: &str,
    logical_name_id: &str,
) -> Result<AddressNamesCurrentRebuildSummary> {
    rebuild_address_names_current_logical_names(pool, address, &[logical_name_id.to_owned()]).await
}

pub async fn rebuild_address_names_current_logical_names(
    pool: &PgPool,
    address: &str,
    logical_name_ids: &[String],
) -> Result<AddressNamesCurrentRebuildSummary> {
    rebuild_address_names_current_logical_names_inner(pool, address, logical_name_ids, None).await
}

pub(crate) async fn rebuild_address_names_current_logical_names_with_heartbeat(
    pool: &PgPool,
    address: &str,
    logical_name_ids: &[String],
    loop_heartbeat: &mut LoopHeartbeat,
) -> Result<AddressNamesCurrentRebuildSummary> {
    rebuild_address_names_current_logical_names_inner(
        pool,
        address,
        logical_name_ids,
        Some(loop_heartbeat),
    )
    .await
}

async fn rebuild_address_names_current_logical_names_inner(
    pool: &PgPool,
    address: &str,
    logical_name_ids: &[String],
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    let normalized_address = normalize_address(address);
    let mut rows = Vec::new();

    for logical_name_id in logical_name_ids {
        let bindings = load_current_bindings_for_logical_name(pool, logical_name_id).await?;
        record_rebuild_progress(pool, &mut loop_heartbeat).await;
        for binding in &bindings {
            rows.extend(build_rows_for_binding(pool, binding, Some(&normalized_address)).await?);
            record_rebuild_progress(pool, &mut loop_heartbeat).await;
        }
    }

    let (deleted_row_count, inserted_row_count) = replace_address_names_current_logical_names(
        pool,
        &normalized_address,
        logical_name_ids,
        &rows,
    )
    .await?;
    record_rebuild_progress(pool, &mut loop_heartbeat).await;

    tracing::info!(
        projection = "address_names_current",
        address = %normalized_address,
        logical_name_count = logical_name_ids.len(),
        upserted_row_count = inserted_row_count,
        deleted_row_count,
        "address_names_current logical-name batch replacement published projection and refreshed identity sidecars"
    );

    Ok(AddressNamesCurrentRebuildSummary {
        requested_address_count: 1,
        upserted_row_count: inserted_row_count as usize,
        deleted_row_count,
    })
}

async fn rebuild_all_addresses(
    pool: &PgPool,
    normalized_target_block: Option<i64>,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    let deleted_row_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::BIGINT FROM address_names_current")
            .fetch_one(pool)
            .await
            .context("failed to count address_names_current rows before full rebuild")?;
    let deleted_row_count = u64::try_from(deleted_row_count)?;
    let mut checkpoint = crate::replay::staging::ProjectionStagingCheckpoint::load_or_start(
        pool,
        "address_names_current",
        normalized_target_block,
    )
    .await?;
    tracing::info!(
        projection = "address_names_current",
        deleted_row_count,
        "address_names_current full rebuild staging started"
    );

    loop {
        let mut rebuild = AddressNamesCurrentFullRebuild::from_durable_stage(
            checkpoint.stage_table(0)?.to_owned(),
            deleted_row_count,
        )?;
        if !checkpoint.staging_complete() {
            stage_all_address_rows(pool, &mut rebuild, &mut checkpoint, &mut loop_heartbeat)
                .await?;
        }
        let staged = AddressNamesCurrentStagingSummary {
            upserted_row_count: checkpoint.staged_row_count()?,
        };
        let published = run_rebuild_phase(
            pool,
            &mut loop_heartbeat,
            "address_names_current.publish",
            async {
                let Some(mut transaction) =
                    checkpoint.begin_fenced_publish_transaction(pool).await?
                else {
                    return Ok(None);
                };
                let counts = publish_address_names_current_full_rebuild_in_transaction(
                    &mut transaction,
                    &rebuild,
                )
                .await?;
                transaction.commit().await?;
                Ok(Some(counts))
            },
        )
        .await?;
        let Some((_deleted_row_count, published_row_count)) = published else {
            continue;
        };
        tracing::info!(
            projection = "address_names_current",
            upserted_row_count = staged.upserted_row_count,
            published_row_count,
            "address_names_current full rebuild published projection and refreshed identity sidecars"
        );

        let requested_address_count = run_rebuild_phase(
            pool,
            &mut loop_heartbeat,
            "address_names_current.count_published_addresses",
            count_address_names_current_addresses(pool),
        )
        .await?;
        return Ok(AddressNamesCurrentRebuildSummary {
            requested_address_count,
            upserted_row_count: staged.upserted_row_count,
            deleted_row_count,
        });
    }
}

struct AddressNamesCurrentStagingSummary {
    upserted_row_count: usize,
}

async fn stage_all_address_rows(
    pool: &PgPool,
    rebuild: &mut AddressNamesCurrentFullRebuild,
    checkpoint: &mut crate::replay::staging::ProjectionStagingCheckpoint,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<AddressNamesCurrentStagingSummary> {
    loop {
        let input_fence = checkpoint.prepare_next_batch(pool).await?;
        let cursor = address_names_source_cursor(checkpoint.last_source_key())?;
        let bindings = stream_current_bindings_after(
            pool,
            cursor
                .as_ref()
                .map(|(logical_name_id, binding_id)| (logical_name_id.as_str(), *binding_id)),
            i64::try_from(ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE)?,
        );
        pin_mut!(bindings);
        let mut page = Vec::with_capacity(ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE);
        while page.len() < ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE {
            let Some(binding) = bindings.try_next().await? else {
                break;
            };
            page.push(binding);
        }
        if page.is_empty() {
            if checkpoint.mark_staging_complete(pool, input_fence).await? {
                break;
            }
            *rebuild = AddressNamesCurrentFullRebuild::from_durable_stage(
                checkpoint.stage_table(0)?.to_owned(),
                rebuild.previous_row_count(),
            )?;
            continue;
        }
        let last = page
            .last()
            .expect("address_names_current staging page must not be empty");
        let last_source_key = json!([last.logical_name_id, last.surface_binding_id]);
        let rows = build_address_names_page(pool, &page, loop_heartbeat).await?;
        let mut transaction = pool.begin().await?;
        let staged = insert_address_names_current_full_rebuild_rows_in_transaction(
            &mut transaction,
            rebuild,
            &rows,
        )
        .await?
        .len() as u64;
        let progress = checkpoint.progress_after_batch(page.len(), last_source_key, staged, 0)?;
        checkpoint
            .persist_progress(&mut transaction, &progress, &input_fence)
            .await?;
        transaction.commit().await?;
        checkpoint.accept_progress(progress, input_fence);
        let completed_binding_count = checkpoint.completed_source_count()?;
        if completed_binding_count.is_multiple_of(5_000) {
            tracing::info!(
                projection = "address_names_current",
                completed_binding_count,
                upserted_row_count = checkpoint.staged_row_count()?,
                "address_names_current rebuild bindings processed"
            );
        }
    }

    tracing::info!(
        projection = "address_names_current",
        upserted_row_count = checkpoint.staged_row_count()?,
        "address_names_current full rebuild staged projection rows"
    );

    Ok(AddressNamesCurrentStagingSummary {
        upserted_row_count: checkpoint.staged_row_count()?,
    })
}

async fn build_address_names_page(
    pool: &PgPool,
    bindings: &[CurrentBindingSeed],
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<Vec<bigname_storage::AddressNameCurrentRow>> {
    let rows = stream::iter(bindings.iter().cloned().enumerate())
        .map(|(source_index, binding)| {
            let pool = pool.clone();
            async move {
                Ok::<_, anyhow::Error>((
                    source_index,
                    build_rows_for_binding(&pool, &binding, None).await?,
                ))
            }
        })
        .buffer_unordered(ADDRESS_NAMES_CURRENT_REBUILD_CONCURRENCY);
    pin_mut!(rows);
    let mut completed_pages = Vec::with_capacity(bindings.len());
    while let Some(binding_rows) = rows.try_next().await? {
        completed_pages.push(binding_rows);
        record_rebuild_progress(pool, loop_heartbeat).await;
    }
    completed_pages.sort_by_key(|(source_index, _)| *source_index);
    Ok(completed_pages
        .into_iter()
        .flat_map(|(_, rows)| rows)
        .collect())
}

fn spawn_address_names_rebuild_task(
    tasks: &mut JoinSet<Result<Vec<bigname_storage::AddressNameCurrentRow>>>,
    pool: &PgPool,
    binding: CurrentBindingSeed,
    address_filter: Option<String>,
) {
    let pool = pool.clone();
    tasks.spawn(
        async move { build_rows_for_binding(&pool, &binding, address_filter.as_deref()).await },
    );
}

fn address_names_source_cursor(value: Option<&Value>) -> Result<Option<(String, Uuid)>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let [logical_name_id, binding_id]: [String; 2] = serde_json::from_value(value.clone())
        .context("address_names_current staging source key must contain two strings")?;
    Ok(Some((
        logical_name_id,
        Uuid::parse_str(&binding_id)
            .context("address_names_current staging source binding id must be a UUID")?,
    )))
}

async fn rebuild_one_address(
    pool: &PgPool,
    address: &str,
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    let normalized_address = normalize_address(address);
    let bindings = load_current_bindings_for_address(pool, &normalized_address).await?;
    record_rebuild_progress(pool, &mut loop_heartbeat).await;
    let replacement =
        begin_address_names_current_address_replacement(pool, &normalized_address).await?;
    record_rebuild_progress(pool, &mut loop_heartbeat).await;

    let staged = match stage_one_address_rows(
        pool,
        &replacement,
        &normalized_address,
        bindings.as_slice(),
        &mut loop_heartbeat,
    )
    .await
    {
        Ok(staged) => staged,
        Err(error) => {
            if let Err(drop_error) =
                drop_address_names_current_address_replacement(pool, &replacement).await
            {
                tracing::warn!(
                    projection = "address_names_current",
                    address = %normalized_address,
                    error = %drop_error,
                    "failed to drop address_names_current address replacement staging table after error"
                );
            }
            return Err(error);
        }
    };

    let (deleted_row_count, published_row_count) =
        match publish_address_names_current_address_replacement(pool, &replacement).await {
            Ok(summary) => summary,
            Err(error) => {
                if let Err(drop_error) =
                    drop_address_names_current_address_replacement(pool, &replacement).await
                {
                    tracing::warn!(
                        projection = "address_names_current",
                        address = %normalized_address,
                        error = %drop_error,
                        "failed to drop address_names_current address replacement staging table after publish error"
                    );
                }
                return Err(error);
            }
        };
    record_rebuild_progress(pool, &mut loop_heartbeat).await;

    if let Err(error) = drop_address_names_current_address_replacement(pool, &replacement).await {
        tracing::warn!(
            projection = "address_names_current",
            address = %normalized_address,
            error = %error,
            "failed to drop address_names_current address replacement staging table after publish"
        );
    }

    tracing::info!(
        projection = "address_names_current",
        address = %normalized_address,
        upserted_row_count = staged.upserted_row_count,
        published_row_count,
        deleted_row_count,
        "address_names_current address replacement published projection and refreshed identity sidecars"
    );

    Ok(AddressNamesCurrentRebuildSummary {
        requested_address_count: 1,
        upserted_row_count: staged.upserted_row_count,
        deleted_row_count,
    })
}

async fn stage_one_address_rows(
    pool: &PgPool,
    replacement: &AddressNamesCurrentAddressReplacement,
    normalized_address: &str,
    bindings: &[CurrentBindingSeed],
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<AddressNamesCurrentStagingSummary> {
    let mut queued_binding_count = 0usize;
    let mut completed_binding_count = 0usize;
    let mut rows = Vec::with_capacity(ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE);
    let mut upserted_row_count = 0usize;
    let mut bindings = bindings.iter().cloned();
    let mut tasks = JoinSet::new();

    while tasks.len() < ADDRESS_NAMES_CURRENT_REBUILD_CONCURRENCY {
        let Some(binding) = bindings.next() else {
            break;
        };
        queued_binding_count += 1;
        spawn_address_names_rebuild_task(
            &mut tasks,
            pool,
            binding,
            Some(normalized_address.to_owned()),
        );
    }

    while let Some(result) = tasks.join_next().await {
        completed_binding_count += 1;
        let binding_rows = result??;
        record_rebuild_progress(pool, loop_heartbeat).await;
        rows.extend(binding_rows);

        if rows.len() >= ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE {
            upserted_row_count +=
                insert_address_names_current_address_replacement_rows(pool, replacement, &rows)
                    .await?
                    .len();
            rows.clear();
            record_rebuild_progress(pool, loop_heartbeat).await;
        }

        if completed_binding_count.is_multiple_of(5_000) {
            tracing::info!(
                projection = "address_names_current",
                address = %normalized_address,
                queued_binding_count,
                completed_binding_count,
                upserted_row_count,
                "address_names_current address replacement bindings processed"
            );
        }

        while tasks.len() < ADDRESS_NAMES_CURRENT_REBUILD_CONCURRENCY {
            let Some(binding) = bindings.next() else {
                break;
            };
            queued_binding_count += 1;
            spawn_address_names_rebuild_task(
                &mut tasks,
                pool,
                binding,
                Some(normalized_address.to_owned()),
            );
        }
    }

    if !rows.is_empty() {
        upserted_row_count +=
            insert_address_names_current_address_replacement_rows(pool, replacement, &rows)
                .await?
                .len();
        record_rebuild_progress(pool, loop_heartbeat).await;
    }

    Ok(AddressNamesCurrentStagingSummary { upserted_row_count })
}

async fn count_address_names_current_addresses(pool: &PgPool) -> Result<usize> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT address)
        FROM address_names_current
        "#,
    )
    .fetch_one(pool)
    .await
    .context("failed to count address_names_current rebuilt addresses")
    .map(|count| count as usize)
}
