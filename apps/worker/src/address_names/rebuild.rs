use anyhow::{Context, Result};
use bigname_storage::{
    AddressNamesCurrentAddressReplacement, AddressNamesCurrentFullRebuild,
    begin_address_names_current_address_replacement, begin_address_names_current_full_rebuild,
    drop_address_names_current_address_replacement, drop_address_names_current_full_rebuild,
    insert_address_names_current_address_replacement_rows,
    insert_address_names_current_full_rebuild_rows,
    publish_address_names_current_address_replacement, publish_address_names_current_full_rebuild,
    replace_address_names_current_logical_names,
};
use futures_util::{TryStreamExt, pin_mut};
use sqlx::PgPool;
use tokio::task::JoinSet;

use crate::primary_name::rebuild_heartbeat::{
    LoopHeartbeat, record_rebuild_progress, run_rebuild_phase,
};

use super::{
    AddressNamesCurrentRebuildSummary,
    load::{
        load_current_bindings_for_address, load_current_bindings_for_logical_name,
        stream_current_bindings,
    },
    model::CurrentBindingSeed,
    projection::build_rows_for_binding,
    util::normalize_address,
};

const ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE: usize = 2_000;
const ADDRESS_NAMES_CURRENT_REBUILD_CONCURRENCY: usize = 8;

pub async fn rebuild_address_names_current(
    pool: &PgPool,
    address: Option<&str>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    rebuild_address_names_current_inner(pool, address, None).await
}

pub(crate) async fn rebuild_address_names_current_with_heartbeat(
    pool: &PgPool,
    address: Option<&str>,
    loop_heartbeat: &mut LoopHeartbeat,
) -> Result<AddressNamesCurrentRebuildSummary> {
    rebuild_address_names_current_inner(pool, address, Some(loop_heartbeat)).await
}

async fn rebuild_address_names_current_inner(
    pool: &PgPool,
    address: Option<&str>,
    loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    match address {
        Some(address) => rebuild_one_address(pool, address, loop_heartbeat).await,
        None => rebuild_all_addresses(pool, loop_heartbeat).await,
    }
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
    mut loop_heartbeat: Option<&mut LoopHeartbeat>,
) -> Result<AddressNamesCurrentRebuildSummary> {
    let rebuild = run_rebuild_phase(
        pool,
        &mut loop_heartbeat,
        "address_names_current.prepare",
        begin_address_names_current_full_rebuild(pool),
    )
    .await?;
    let deleted_row_count = rebuild.previous_row_count();
    tracing::info!(
        projection = "address_names_current",
        deleted_row_count,
        "address_names_current full rebuild staging started"
    );

    let staged = match stage_all_address_rows(pool, &rebuild, &mut loop_heartbeat).await {
        Ok(staged) => staged,
        Err(error) => {
            if let Err(drop_error) = drop_address_names_current_full_rebuild(pool, &rebuild).await {
                tracing::warn!(
                    projection = "address_names_current",
                    error = %drop_error,
                    "failed to drop address_names_current full rebuild staging table after error"
                );
            }
            return Err(error);
        }
    };

    let (_deleted_row_count, published_row_count) = match run_rebuild_phase(
        pool,
        &mut loop_heartbeat,
        "address_names_current.publish",
        publish_address_names_current_full_rebuild(pool, &rebuild),
    )
    .await
    {
        Ok(published) => published,
        Err(error) => {
            if let Err(drop_error) = drop_address_names_current_full_rebuild(pool, &rebuild).await {
                tracing::warn!(
                    projection = "address_names_current",
                    error = %drop_error,
                    "failed to drop address_names_current full rebuild staging table after publish error"
                );
            }
            return Err(error);
        }
    };
    if let Err(error) = drop_address_names_current_full_rebuild(pool, &rebuild).await {
        tracing::warn!(
            projection = "address_names_current",
            error = %error,
            "failed to drop address_names_current full rebuild staging table after publish"
        );
    }
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

    Ok(AddressNamesCurrentRebuildSummary {
        requested_address_count,
        upserted_row_count: staged.upserted_row_count,
        deleted_row_count,
    })
}

struct AddressNamesCurrentStagingSummary {
    upserted_row_count: usize,
}

async fn stage_all_address_rows(
    pool: &PgPool,
    rebuild: &AddressNamesCurrentFullRebuild,
    loop_heartbeat: &mut Option<&mut LoopHeartbeat>,
) -> Result<AddressNamesCurrentStagingSummary> {
    let mut queued_binding_count = 0usize;
    let mut completed_binding_count = 0usize;
    let mut rows = Vec::with_capacity(ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE);
    let mut upserted_row_count = 0usize;

    let bindings = stream_current_bindings(pool);
    pin_mut!(bindings);
    let mut tasks = JoinSet::new();

    while tasks.len() < ADDRESS_NAMES_CURRENT_REBUILD_CONCURRENCY {
        let Some(binding) = bindings.try_next().await? else {
            break;
        };
        queued_binding_count += 1;
        spawn_address_names_rebuild_task(&mut tasks, pool, binding, None);
    }

    while let Some(result) = tasks.join_next().await {
        completed_binding_count += 1;
        let binding_rows = result??;
        record_rebuild_progress(pool, loop_heartbeat).await;
        rows.extend(binding_rows);

        if rows.len() >= ADDRESS_NAMES_CURRENT_REBUILD_BATCH_SIZE {
            upserted_row_count +=
                insert_address_names_current_full_rebuild_rows(pool, rebuild, &rows)
                    .await?
                    .len();
            rows.clear();
        }

        if completed_binding_count.is_multiple_of(5_000) {
            tracing::info!(
                projection = "address_names_current",
                queued_binding_count,
                completed_binding_count,
                upserted_row_count,
                "address_names_current rebuild bindings processed"
            );
        }

        while tasks.len() < ADDRESS_NAMES_CURRENT_REBUILD_CONCURRENCY {
            let Some(binding) = bindings.try_next().await? else {
                break;
            };
            queued_binding_count += 1;
            spawn_address_names_rebuild_task(&mut tasks, pool, binding, None);
        }
    }

    if !rows.is_empty() {
        upserted_row_count += insert_address_names_current_full_rebuild_rows(pool, rebuild, &rows)
            .await?
            .len();
    }

    tracing::info!(
        projection = "address_names_current",
        upserted_row_count,
        "address_names_current full rebuild staged projection rows"
    );

    Ok(AddressNamesCurrentStagingSummary { upserted_row_count })
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
