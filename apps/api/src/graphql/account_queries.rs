use anyhow::{Context as _, Result};
use async_graphql::{Context, ID};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};

use bigname_storage::{DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER, NameCurrentListOrder};

use crate::state::AppState;

use super::enums::SubgraphErrorPolicy;
use super::error::internal_error;
use super::inputs::{AccountEntityFilter, BlockHeight};
use super::objects::Account;
use super::snapshot::{
    graphql_snapshot_chain_ids, load_graphql_entity_head, require_account_rows_at_head,
    revalidate_graphql_head,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhaseGraphqlAccountRow {
    pub id: String,
    pub membership_target: Value,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GeneratedAccountFilter {
    pub id: Option<String>,
    pub id_in: Option<Vec<String>>,
}

pub async fn resolve_account(
    ctx: &Context<'_>,
    id: ID,
    block: Option<&BlockHeight>,
    subgraph_error: SubgraphErrorPolicy,
) -> async_graphql::Result<Option<Account>> {
    let state = ctx.data::<AppState>()?;
    let head = load_graphql_entity_head(ctx, block, subgraph_error, "account").await?;
    let chain_ids = graphql_snapshot_chain_ids(head.as_ref());
    let row = load_phase_graphql_account_by_id(&state.pool, "ens", &chain_ids, id.as_str())
        .await
        .map_err(|error| internal_error("account", error))?;
    if let Some(row) = row.as_ref() {
        require_account_rows_at_head(std::slice::from_ref(row), head.as_ref(), "account")?;
    }
    revalidate_graphql_head(state, head.as_ref(), "account").await?;
    Ok(row.map(|row| Account { id: ID(row.id) }))
}

pub fn account_entity_filter_to_storage(
    filter: Option<AccountEntityFilter>,
) -> GeneratedAccountFilter {
    let filter = filter.unwrap_or_default();
    GeneratedAccountFilter {
        id: filter.id.map(|id| id.as_str().to_ascii_lowercase()),
        id_in: filter.id_in.map(|ids| {
            ids.into_iter()
                .map(|id| id.as_str().to_ascii_lowercase())
                .collect()
        }),
    }
}

pub async fn load_phase_graphql_account_by_id(
    pool: &PgPool,
    namespace: &str,
    snapshot_chain_ids: &[String],
    id: &str,
) -> Result<Option<PhaseGraphqlAccountRow>> {
    let rows = load_phase_graphql_account_page_offset(
        pool,
        namespace,
        snapshot_chain_ids,
        &GeneratedAccountFilter {
            id: Some(id.to_ascii_lowercase()),
            id_in: None,
        },
        NameCurrentListOrder::Asc,
        1,
        0,
    )
    .await?;
    Ok(rows.into_iter().next())
}

pub async fn load_phase_graphql_account_page_offset(
    pool: &PgPool,
    namespace: &str,
    snapshot_chain_ids: &[String],
    filter: &GeneratedAccountFilter,
    order: NameCurrentListOrder,
    limit: u64,
    offset: u64,
) -> Result<Vec<PhaseGraphqlAccountRow>> {
    let limit = i64::try_from(limit).context("GraphQL account limit exceeds SQL limit")?;
    let offset = i64::try_from(offset).context("GraphQL account offset exceeds SQL limit")?;
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_accounts(&mut builder, namespace, snapshot_chain_ids, filter, order);
    builder.push(" LIMIT ").push_bind(limit);
    builder.push(" OFFSET ").push_bind(offset);
    builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to load schema-v2 GraphQL accounts")?
        .into_iter()
        .map(decode_account_row)
        .collect()
}

fn push_filtered_accounts<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    namespace: &'a str,
    snapshot_chain_ids: &'a [String],
    filter: &'a GeneratedAccountFilter,
    order: NameCurrentListOrder,
) {
    let direction = match order {
        NameCurrentListOrder::Asc => "ASC",
        NameCurrentListOrder::Desc => "DESC",
    };
    builder.push(
        "SELECT DISTINCT ON (LOWER(anc.address)) LOWER(anc.address) AS id, \
         anc.chain_positions || JSONB_BUILD_OBJECT('chain_id', anc.provenance ->> 'chain_id') \
         AS membership_target \
         FROM bigname_phase.address_names_current anc \
         JOIN LATERAL (SELECT 1 \
         FROM bigname_phase.name_surfaces membership_surface \
         JOIN bigname_phase.resources membership_resource \
           ON membership_resource.resource_id = anc.resource_id \
         JOIN bigname_phase.surface_bindings membership_binding \
           ON membership_binding.surface_binding_id = anc.surface_binding_id \
         LEFT JOIN bigname_phase.token_lineages membership_token_lineage \
           ON membership_token_lineage.token_lineage_id = anc.token_lineage_id \
         JOIN bigname_phase.chain_lineage membership_surface_lineage \
           ON membership_surface_lineage.chain_id = membership_surface.chain_id \
          AND membership_surface_lineage.block_hash = membership_surface.block_hash \
         JOIN bigname_phase.chain_lineage membership_resource_lineage \
           ON membership_resource_lineage.chain_id = membership_resource.chain_id \
          AND membership_resource_lineage.block_hash = membership_resource.block_hash \
         JOIN bigname_phase.chain_lineage membership_binding_lineage \
           ON membership_binding_lineage.chain_id = membership_binding.chain_id \
          AND membership_binding_lineage.block_hash = membership_binding.block_hash \
         LEFT JOIN bigname_phase.chain_lineage membership_token_lineage_lineage \
           ON membership_token_lineage_lineage.chain_id = membership_token_lineage.chain_id \
          AND membership_token_lineage_lineage.block_hash = membership_token_lineage.block_hash \
         WHERE membership_surface.logical_name_id = anc.logical_name_id \
           AND anc.support_status = 'supported' \
           AND anc.provenance ->> 'chain_id' = ANY(",
    );
    builder.push_bind(snapshot_chain_ids).push(")");
    builder.push(DEFAULT_ADDRESS_NAMES_MEMBERSHIP_READ_FILTER);
    builder.push(" OFFSET 0) membership_guard ON TRUE WHERE anc.namespace = ");
    builder.push_bind(namespace);
    if let Some(id) = filter.id.as_deref() {
        builder.push(" AND LOWER(anc.address) = ").push_bind(id);
    }
    if let Some(ids) = filter.id_in.as_deref() {
        if ids.is_empty() {
            builder.push(" AND FALSE");
        } else {
            builder
                .push(" AND LOWER(anc.address) = ANY(")
                .push_bind(ids)
                .push(")");
        }
    }
    builder.push(format!(
        " ORDER BY LOWER(anc.address) {direction}, anc.relation {direction}, \
         anc.namespace {direction}, anc.namehash {direction}, anc.logical_name_id {direction}"
    ));
}

fn decode_account_row(row: PgRow) -> Result<PhaseGraphqlAccountRow> {
    Ok(PhaseGraphqlAccountRow {
        id: row.try_get("id")?,
        membership_target: row.try_get("membership_target")?,
    })
}

#[cfg(test)]
pub async fn explain_phase_graphql_account_page(
    pool: &PgPool,
    namespace: &str,
    snapshot_chain_ids: &[String],
    filter: &GeneratedAccountFilter,
    limit: u64,
    force_index: bool,
) -> Result<Value> {
    let mut transaction = pool.begin().await?;
    if force_index {
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *transaction)
            .await?;
    }
    let mut builder = QueryBuilder::<Postgres>::new("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ");
    push_filtered_accounts(
        &mut builder,
        namespace,
        snapshot_chain_ids,
        filter,
        NameCurrentListOrder::Asc,
    );
    builder
        .push(" LIMIT ")
        .push_bind(i64::try_from(limit)?)
        .push(" OFFSET ")
        .push_bind(0_i64);
    let row = builder.build().fetch_one(&mut *transaction).await?;
    Ok(row.try_get(0)?)
}
