use anyhow::Result;
use bigname_storage::{
    NameSurface, NormalizedEvent, Resource, SurfaceBinding, TokenLineage, upsert_name_surfaces,
    upsert_resources, upsert_surface_bindings, upsert_token_lineages,
};
use sqlx::PgPool;

use crate::{
    checkpoint_context::StartupAdapterProgress,
    normalized_event_support::upsert_normalized_events_in_chunks_with_counts_and_progress,
    startup_progress::STARTUP_ADAPTER_PROGRESS_PAGE_ROWS,
};

pub(super) async fn persist_registry_outputs_with_progress(
    pool: &PgPool,
    token_lineages: &[TokenLineage],
    resources: &[Resource],
    surfaces: &[NameSurface],
    bindings: &[SurfaceBinding],
    events: &[NormalizedEvent],
    progress: &mut dyn StartupAdapterProgress,
) -> Result<usize> {
    for chunk in token_lineages.chunks(STARTUP_ADAPTER_PROGRESS_PAGE_ROWS) {
        upsert_token_lineages(pool, chunk).await?;
        progress.record(pool).await?;
    }
    for chunk in resources.chunks(STARTUP_ADAPTER_PROGRESS_PAGE_ROWS) {
        upsert_resources(pool, chunk).await?;
        progress.record(pool).await?;
    }
    for chunk in surfaces.chunks(STARTUP_ADAPTER_PROGRESS_PAGE_ROWS) {
        upsert_name_surfaces(pool, chunk).await?;
        progress.record(pool).await?;
    }

    let closed_bindings = bindings
        .iter()
        .filter(|binding| binding.active_to.is_some())
        .cloned()
        .collect::<Vec<_>>();
    let open_bindings = bindings
        .iter()
        .filter(|binding| binding.active_to.is_none())
        .cloned()
        .collect::<Vec<_>>();
    for chunk in closed_bindings
        .chunks(STARTUP_ADAPTER_PROGRESS_PAGE_ROWS)
        .chain(open_bindings.chunks(STARTUP_ADAPTER_PROGRESS_PAGE_ROWS))
    {
        upsert_surface_bindings(pool, chunk).await?;
        progress.record(pool).await?;
    }

    let counts = upsert_normalized_events_in_chunks_with_counts_and_progress(
        pool,
        events,
        "ENSv2 registry",
        STARTUP_ADAPTER_PROGRESS_PAGE_ROWS,
        Some(progress),
    )
    .await?;
    Ok(counts.total_inserted_count)
}
