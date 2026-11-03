use anyhow::Result;
use serde_json::Value;
use sqlx::{Arguments, Connection, Execute, PgPool, Postgres, QueryBuilder};

use super::{
    EventHistoryFilter, EventHistoryReadFilter, HistorySummaryMode,
    columns::push_history_select,
    contract_count::contract_count_filter,
    duplicates::push_product_history_duplicate_filter,
    event_history_read_filter,
    paging::{push_history_filters, push_history_order},
    summary::push_history_count_query,
    wrapped_registrar::{
        ResourceNamehashAnchor, push_namehash_surfaces_query, push_registrar_grant_resources_query,
        push_registrar_namehash_anchors_query, push_wrapped_registrar_resources_query,
    },
};

pub(super) async fn explain_history_filter_for_test(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    lookup: HistoryPlanLookup<'_>,
    canonical_only: bool,
) -> Result<String> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *transaction)
        .await?;
    let filter = filter.with_attributed_records(&mut transaction).await?;

    let mut forward = QueryBuilder::<Postgres>::new("EXPLAIN (COSTS OFF) ");
    push_wrapped_registrar_resources_query(&mut forward, lookup.logical_name_id, canonical_only);
    let forward_plan = forward
        .build_query_scalar::<String>()
        .fetch_all(&mut *transaction)
        .await?
        .join("\n");

    let mut grants = QueryBuilder::<Postgres>::new("EXPLAIN (COSTS OFF) ");
    push_registrar_grant_resources_query(&mut grants, lookup.logical_name_id, canonical_only);
    let grant_plan = grants
        .build_query_scalar::<String>()
        .fetch_all(&mut *transaction)
        .await?
        .join("\n");

    let mut anchors = QueryBuilder::<Postgres>::new("EXPLAIN (COSTS OFF) ");
    push_registrar_namehash_anchors_query(&mut anchors, lookup.registration_id, canonical_only);
    let anchor_plan = anchors
        .build_query_scalar::<String>()
        .fetch_all(&mut *transaction)
        .await?
        .join("\n");

    let anchor = ResourceNamehashAnchor {
        chain_id: lookup.chain_id.to_owned(),
        namespace: lookup.namespace.to_owned(),
        namehash: lookup.namehash.to_owned(),
    };
    let mut surfaces = QueryBuilder::<Postgres>::new("EXPLAIN (COSTS OFF) ");
    push_namehash_surfaces_query(&mut surfaces, &anchor, canonical_only);
    let surface_plan = surfaces
        .build_query_scalar::<String>()
        .fetch_all(&mut *transaction)
        .await?
        .join("\n");

    let mut builder = QueryBuilder::<Postgres>::new("EXPLAIN (COSTS OFF) ");
    let resolver_source = push_history_select(&mut builder, &filter, canonical_only, false, false);
    push_history_filters(&mut builder, &filter, canonical_only, resolver_source);
    push_product_history_duplicate_filter(&mut builder, &filter, canonical_only);
    push_history_order(&mut builder, filter.order);
    let plan = builder
        .build_query_scalar::<String>()
        .fetch_all(&mut *transaction)
        .await?
        .join("\n");
    transaction.rollback().await?;
    Ok(format!(
        "name-to-registrar association:\n{forward_plan}\n\nregistrar grants by name:\n{grant_plan}\n\nregistrar namehash anchors:\n{anchor_plan}\n\nexact-namehash surfaces:\n{surface_plan}\n\nhistory page:\n{plan}"
    ))
}

pub(super) struct HistoryPlanLookup<'a> {
    pub(super) logical_name_id: &'a str,
    pub(super) registration_id: uuid::Uuid,
    pub(super) chain_id: &'a str,
    pub(super) namespace: &'a str,
    pub(super) namehash: &'a str,
}

/// `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` of the summary count an app-facing event-history
/// page runs for `mode`, as a canonical product read builds it, executed as a prepared statement
/// on a generic or a custom plan. `None` for a mode that runs no count. A test-support seam,
/// never a production query path.
pub async fn explain_event_history_count_for_test(
    pool: &PgPool,
    filter: EventHistoryFilter,
    mode: HistorySummaryMode,
    generic_plan: bool,
) -> Result<Option<Value>> {
    let limit = match mode {
        HistorySummaryMode::Count => None,
        HistorySummaryMode::CappedCount(cap) => Some(i64::try_from(cap.saturating_add(1))?),
        HistorySummaryMode::None | HistorySummaryMode::Full => return Ok(None),
    };
    let filter = event_history_read_filter(pool, filter, true, false).await?;
    explain_count_query(pool, filter, true, limit, generic_plan)
        .await
        .map(Some)
}

/// `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` of the statement [`super::count_contract_events`]
/// runs for these arguments, executed as a prepared statement on a generic or a custom plan. A
/// test-support seam, never a production query path.
pub async fn explain_contract_event_count_for_test(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
    event_kinds: &[String],
    as_of_block: Option<i64>,
    cap: Option<u64>,
    generic_plan: bool,
) -> Result<Value> {
    let limit = cap
        .map(|cap| i64::try_from(cap.saturating_add(1)))
        .transpose()?;
    let filter = contract_count_filter(chain_id, address, event_kinds, as_of_block);
    explain_count_query(pool, filter, false, limit, generic_plan).await
}

async fn explain_count_query(
    pool: &PgPool,
    filter: EventHistoryReadFilter,
    attribute_records: bool,
    limit: Option<i64>,
    generic_plan: bool,
) -> Result<Value> {
    let mut connection = pool.acquire().await?.detach();
    let mut transaction = connection.begin().await?;
    let filter = if attribute_records {
        filter.with_attributed_records(&mut transaction).await?
    } else {
        filter
    };
    let mut query = QueryBuilder::<Postgres>::new("");
    push_history_count_query(&mut query, &filter, true, limit);
    let (setting, expected) = if generic_plan {
        ("force_generic_plan", (1, 0))
    } else {
        ("force_custom_plan", (0, 1))
    };
    sqlx::query(&format!("SET LOCAL plan_cache_mode = {setting}"))
        .execute(&mut *transaction)
        .await?;
    sqlx::raw_sql(&format!("PREPARE history_count AS {}", query.sql()))
        .execute(&mut *transaction)
        .await?;
    let mut built = query.build();
    let arguments = built
        .take_arguments()
        .map_err(anyhow::Error::from_boxed)?
        .unwrap_or_default();
    // EXECUTE takes no protocol parameters, so PostgreSQL renders each bound value as a quoted
    // literal for it.
    let placeholders = (1..=arguments.len())
        .map(|index| format!("quote_nullable(${index})"))
        .collect::<Vec<_>>()
        .join(", ");
    let literals: Vec<String> =
        sqlx::query_scalar_with(&format!("SELECT ARRAY[{placeholders}]"), arguments)
            .persistent(false)
            .fetch_one(&mut *transaction)
            .await?;
    let plan: Value = sqlx::query_scalar(&format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE history_count({})",
        literals.join(", ")
    ))
    .persistent(false)
    .fetch_one(&mut *transaction)
    .await?;
    let plans: (i64, i64) = sqlx::query_as(
        "SELECT generic_plans, custom_plans FROM pg_prepared_statements
         WHERE name = 'history_count'",
    )
    .fetch_one(&mut *transaction)
    .await?;
    anyhow::ensure!(
        plans == expected,
        "expected {setting} plan counts {expected:?}, found {plans:?}"
    );
    transaction.rollback().await?;
    connection.close().await?;
    Ok(plan)
}

/// Throwaway red-proof seam: while on, every history and contract event count records its
/// statement text and its bound values rendered as SQL literals.
pub static COUNT_SQL_DUMP_ON: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
pub static COUNT_SQL_DUMP: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

pub(super) async fn record_count_statement(
    connection: &mut sqlx::PgConnection,
    mut query: QueryBuilder<'_, Postgres>,
) -> Result<()> {
    let sql = query.sql().to_owned();
    let mut built = query.build();
    let arguments = built
        .take_arguments()
        .map_err(anyhow::Error::from_boxed)?
        .unwrap_or_default();
    let count = arguments.len();
    let placeholders = (1..=count)
        .map(|index| format!("quote_nullable(${index})"))
        .collect::<Vec<_>>()
        .join(", ");
    let literals: Vec<String> = if count == 0 {
        Vec::new()
    } else {
        sqlx::query_scalar_with(&format!("SELECT ARRAY[{placeholders}]"), arguments)
            .persistent(false)
            .fetch_one(&mut *connection)
            .await?
    };
    let params = literals
        .iter()
        .enumerate()
        .map(|(index, literal)| format!("-- ${} = {literal}", index + 1))
        .collect::<Vec<_>>()
        .join("\n");
    COUNT_SQL_DUMP
        .lock()
        .expect("count dump lock")
        .push(format!("{sql}\n{params}"));
    Ok(())
}
