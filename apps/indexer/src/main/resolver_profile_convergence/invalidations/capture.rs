use anyhow::{Context, Result, ensure};
use bigname_adapters::StartupAdapterProgress;
use serde_json::{Value, json};
use sqlx::{Postgres, QueryBuilder, Transaction};
use uuid::Uuid;

const SOURCE_PAGE_SIZE: i64 = 1_000;
const TEMP_BOUND_NAMES: &str = "resolver_profile_invalidation_bound_names";

type InvalidationRow = (String, String, String, Value);

/// Capture every projection key from a stable pre-publication snapshot. Raw
/// event and binding scans advance by primary-key pages so no query must first
/// materialize the whole historical target set before progress is observable.
pub(crate) async fn stage_resolver_profile_projection_invalidations(
    pool: &sqlx::PgPool,
    run_id: Uuid,
    chain: &str,
) -> Result<()> {
    stage_resolver_profile_projection_invalidations_inner(pool, run_id, chain, None).await
}

pub(crate) async fn stage_resolver_profile_projection_invalidations_with_progress(
    pool: &sqlx::PgPool,
    run_id: Uuid,
    chain: &str,
    progress: &mut dyn StartupAdapterProgress,
) -> Result<()> {
    stage_resolver_profile_projection_invalidations_inner(pool, run_id, chain, Some(progress)).await
}

async fn stage_resolver_profile_projection_invalidations_inner(
    pool: &sqlx::PgPool,
    run_id: Uuid,
    chain: &str,
    mut progress: Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to begin resolver-profile invalidation capture")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *transaction)
        .await
        .context("failed to establish resolver-profile invalidation snapshot")?;
    sqlx::query(&format!(
        "CREATE TEMP TABLE {TEMP_BOUND_NAMES} (logical_name_id TEXT PRIMARY KEY) ON COMMIT DROP"
    ))
    .execute(&mut *transaction)
    .await
    .context("failed to create resolver-profile invalidation bound-name staging table")?;

    stage_resolver_targets(pool, &mut transaction, run_id, chain, &mut progress).await?;
    materialize_bound_names(pool, &mut transaction, run_id, chain, &mut progress).await?;
    stage_event_resources(pool, &mut transaction, chain, &mut progress).await?;
    stage_binding_resources(pool, &mut transaction, chain, &mut progress).await?;

    transaction
        .commit()
        .await
        .context("failed to commit resolver-profile invalidation capture")?;
    Ok(())
}

async fn stage_resolver_targets(
    pool: &sqlx::PgPool,
    transaction: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    chain: &str,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let mut after = None::<String>;
    loop {
        let targets = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT run.chain_id, target.resolver_address
            FROM resolver_profile_reconciliation_targets target
            JOIN resolver_profile_reconciliation_runs run
              ON run.run_id = target.run_id
            WHERE target.run_id = $1
              AND ($2::TEXT IS NULL OR target.resolver_address > $2)
            ORDER BY target.resolver_address
            LIMIT $3
            "#,
        )
        .bind(run_id)
        .bind(after.as_deref())
        .bind(SOURCE_PAGE_SIZE)
        .fetch_all(&mut **transaction)
        .await
        .with_context(|| format!("failed to load resolver-profile target page on {chain}"))?;
        let Some((_, last_address)) = targets.last() else {
            return Ok(());
        };
        ensure!(
            targets
                .iter()
                .all(|(target_chain, _)| target_chain == chain),
            "resolver-profile invalidation target page crossed its requested chain boundary"
        );
        after = Some(last_address.clone());
        let invalidations = targets
            .into_iter()
            .map(|(target_chain, resolver_address)| {
                (
                    target_chain.clone(),
                    "resolver_current".to_owned(),
                    format!("{target_chain}:{resolver_address}"),
                    json!({
                        "chain_id": target_chain,
                        "resolver_address": resolver_address,
                    }),
                )
            })
            .collect::<Vec<_>>();
        insert_invalidations(transaction, &invalidations, chain).await?;
        record_progress(pool, progress).await?;
    }
}

async fn materialize_bound_names(
    pool: &sqlx::PgPool,
    transaction: &mut Transaction<'_, Postgres>,
    run_id: Uuid,
    chain: &str,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let event_watermark = normalized_event_watermark(transaction).await?;
    let mut after = 0i64;
    while after < event_watermark {
        let rows = sqlx::query_as::<_, (Option<i64>, Option<String>)>(
            r#"
            WITH source_page AS MATERIALIZED (
                SELECT
                    normalized_event_id,
                    chain_id,
                    logical_name_id,
                    event_kind,
                    canonicality_state,
                    before_state,
                    after_state
                FROM normalized_events
                WHERE normalized_event_id > $1
                  AND normalized_event_id <= $2
                ORDER BY normalized_event_id
                LIMIT $3
            ),
            page_end AS (
                SELECT MAX(normalized_event_id) AS last_id
                FROM source_page
            ),
            bound_names AS (
                SELECT DISTINCT event.logical_name_id
                FROM source_page event
                JOIN LATERAL (
                    VALUES
                        (event.before_state ->> 'resolver'),
                        (event.after_state ->> 'resolver')
                ) resolver(resolver_address) ON TRUE
                JOIN resolver_profile_reconciliation_targets target
                  ON target.run_id = $4
                 AND target.resolver_address = lower(resolver.resolver_address)
                WHERE event.chain_id = $5
                  AND event.event_kind = 'ResolverChanged'
                  AND event.logical_name_id IS NOT NULL
                  AND event.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
            )
            SELECT page_end.last_id, bound.logical_name_id
            FROM page_end
            LEFT JOIN bound_names bound ON TRUE
            "#,
        )
        .bind(after)
        .bind(event_watermark)
        .bind(SOURCE_PAGE_SIZE)
        .bind(run_id)
        .bind(chain)
        .fetch_all(&mut **transaction)
        .await
        .with_context(|| format!("failed to scan resolver-profile bound-name page on {chain}"))?;
        let Some(last_id) = rows.first().and_then(|(last_id, _)| *last_id) else {
            break;
        };
        ensure!(
            last_id > after,
            "resolver-profile bound-name scan did not advance"
        );
        after = last_id;
        let names = rows
            .into_iter()
            .filter_map(|(_, logical_name_id)| logical_name_id)
            .collect::<Vec<_>>();
        insert_bound_names(transaction, &names).await?;
        record_progress(pool, progress).await?;
    }
    Ok(())
}

async fn stage_event_resources(
    pool: &sqlx::PgPool,
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let event_watermark = normalized_event_watermark(transaction).await?;
    let mut after = 0i64;
    while after < event_watermark {
        let rows = sqlx::query_as::<_, (Option<i64>, Option<Uuid>)>(&format!(
            r#"
            WITH source_page AS MATERIALIZED (
                SELECT normalized_event_id, logical_name_id, resource_id, canonicality_state
                FROM normalized_events
                WHERE normalized_event_id > $1
                  AND normalized_event_id <= $2
                ORDER BY normalized_event_id
                LIMIT $3
            ),
            page_end AS (
                SELECT MAX(normalized_event_id) AS last_id
                FROM source_page
            ),
            resources AS (
                SELECT DISTINCT event.resource_id
                FROM source_page event
                JOIN {TEMP_BOUND_NAMES} name
                  ON name.logical_name_id = event.logical_name_id
                JOIN resources resource
                  ON resource.resource_id = event.resource_id
                WHERE event.resource_id IS NOT NULL
                  AND event.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND resource.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
            )
            SELECT page_end.last_id, resource.resource_id
            FROM page_end
            LEFT JOIN resources resource ON TRUE
            "#,
        ))
        .bind(after)
        .bind(event_watermark)
        .bind(SOURCE_PAGE_SIZE)
        .fetch_all(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to scan resolver-profile event-resource page on {chain}")
        })?;
        let Some(last_id) = rows.first().and_then(|(last_id, _)| *last_id) else {
            break;
        };
        ensure!(
            last_id > after,
            "resolver-profile event-resource scan did not advance"
        );
        after = last_id;
        let invalidations = record_inventory_invalidations(chain, rows);
        insert_invalidations(transaction, &invalidations, chain).await?;
        record_progress(pool, progress).await?;
    }
    Ok(())
}

async fn stage_binding_resources(
    pool: &sqlx::PgPool,
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    let mut after = None::<Uuid>;
    loop {
        let rows = sqlx::query_as::<_, (Option<Uuid>, Option<Uuid>)>(&format!(
            r#"
            WITH source_page AS MATERIALIZED (
                SELECT
                    surface_binding_id,
                    logical_name_id,
                    resource_id,
                    canonicality_state
                FROM surface_bindings
                WHERE ($1::UUID IS NULL OR surface_binding_id > $1)
                ORDER BY surface_binding_id
                LIMIT $2
            ),
            page_end AS (
                SELECT surface_binding_id AS last_id
                FROM source_page
                ORDER BY surface_binding_id DESC
                LIMIT 1
            ),
            resources AS (
                SELECT DISTINCT binding.resource_id
                FROM source_page binding
                JOIN {TEMP_BOUND_NAMES} name
                  ON name.logical_name_id = binding.logical_name_id
                JOIN resources resource
                  ON resource.resource_id = binding.resource_id
                WHERE binding.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
                  AND resource.canonicality_state IN (
                      'canonical'::canonicality_state,
                      'safe'::canonicality_state,
                      'finalized'::canonicality_state
                  )
            )
            SELECT page_end.last_id, resource.resource_id
            FROM page_end
            LEFT JOIN resources resource ON TRUE
            "#,
        ))
        .bind(after)
        .bind(SOURCE_PAGE_SIZE)
        .fetch_all(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to scan resolver-profile binding-resource page on {chain}")
        })?;
        let Some(last_id) = rows.first().and_then(|(last_id, _)| *last_id) else {
            return Ok(());
        };
        ensure!(
            after.is_none_or(|previous| last_id > previous),
            "resolver-profile binding-resource scan did not advance"
        );
        after = Some(last_id);
        let invalidations = record_inventory_invalidations(chain, rows);
        insert_invalidations(transaction, &invalidations, chain).await?;
        record_progress(pool, progress).await?;
    }
}

async fn normalized_event_watermark(transaction: &mut Transaction<'_, Postgres>) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT COALESCE(MAX(normalized_event_id), 0)::BIGINT FROM normalized_events",
    )
    .fetch_one(&mut **transaction)
    .await
    .context("failed to load resolver-profile invalidation event watermark")
}

async fn insert_bound_names(
    transaction: &mut Transaction<'_, Postgres>,
    names: &[String],
) -> Result<()> {
    if names.is_empty() {
        return Ok(());
    }
    let mut builder =
        QueryBuilder::<Postgres>::new(format!("INSERT INTO {TEMP_BOUND_NAMES} (logical_name_id) "));
    builder.push_values(names, |mut row, name| {
        row.push_bind(name);
    });
    builder.push(" ON CONFLICT (logical_name_id) DO NOTHING");
    builder
        .build()
        .execute(&mut **transaction)
        .await
        .context("failed to stage resolver-profile invalidation bound-name page")?;
    Ok(())
}

fn record_inventory_invalidations<T>(
    chain: &str,
    rows: Vec<(Option<T>, Option<Uuid>)>,
) -> Vec<InvalidationRow> {
    rows.into_iter()
        .filter_map(|(_, resource_id)| resource_id)
        .map(|resource_id| {
            (
                chain.to_owned(),
                "record_inventory_current".to_owned(),
                resource_id.to_string(),
                json!({ "resource_id": resource_id.to_string() }),
            )
        })
        .collect()
}

async fn insert_invalidations(
    transaction: &mut Transaction<'_, Postgres>,
    rows: &[InvalidationRow],
    chain: &str,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO resolver_profile_reconciliation_invalidation_keys \
         (chain_id, projection, projection_key, key_payload) ",
    );
    builder.push_values(rows, |mut row, (chain, projection, key, payload)| {
        row.push_bind(chain)
            .push_bind(projection)
            .push_bind(key)
            .push_bind(payload);
    });
    builder.push(
        " ON CONFLICT (chain_id, projection, projection_key) \
         DO UPDATE SET key_payload = EXCLUDED.key_payload",
    );
    builder
        .build()
        .execute(&mut **transaction)
        .await
        .with_context(|| {
            format!("failed to stage resolver-profile projection invalidation page on {chain}")
        })?;
    Ok(())
}

async fn record_progress(
    pool: &sqlx::PgPool,
    progress: &mut Option<&mut dyn StartupAdapterProgress>,
) -> Result<()> {
    if let Some(progress) = progress.as_deref_mut() {
        progress.record(pool).await?;
    }
    Ok(())
}
