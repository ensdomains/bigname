use anyhow::{Context as _, Result};
use async_graphql::dataloader::DataLoader;
use async_graphql::{Context, ID};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow, types::Uuid};

use bigname_storage::{
    DEFAULT_NAME_CURRENT_LINEAGE_JOINS, DEFAULT_NAME_CURRENT_READ_FILTER, NameCurrentListOrder,
};

use crate::state::AppState;

use super::convert::resolver_from_store;
use super::enums::SubgraphErrorPolicy;
use super::error::internal_error;
use super::inputs::{BlockHeight, ResolverEntityFilter};
use super::loader::{RecordInventoryLoader, record_inventory_key};
use super::objects::Resolver;
use super::snapshot::{
    graphql_snapshot_chain_ids, load_graphql_entity_head, require_inventory_at_head,
    require_resolver_rows_at_head, revalidate_graphql_head,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedResolverId {
    pub address: String,
    pub namehash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhaseGraphqlResolverRow {
    pub id: String,
    pub address: String,
    pub domain_namehash: String,
    pub inventory_resource_id: Option<Uuid>,
    pub record_version_boundary: Option<Value>,
    pub chain_positions: Value,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GeneratedResolverFilter {
    pub id: Option<ParsedResolverId>,
    pub address: Option<String>,
    pub domain: Option<String>,
}

pub async fn resolve_resolver(
    ctx: &Context<'_>,
    id: ID,
    block: Option<&BlockHeight>,
    subgraph_error: SubgraphErrorPolicy,
) -> async_graphql::Result<Option<Resolver>> {
    let state = ctx.data::<AppState>()?;
    let head = load_graphql_entity_head(ctx, block, subgraph_error, "resolver").await?;
    let Some(id) = parse_resolver_id(id.as_str()) else {
        revalidate_graphql_head(state, head.as_ref(), "resolver").await?;
        return Ok(None);
    };
    let chain_ids = graphql_snapshot_chain_ids(head.as_ref());
    let row = load_phase_graphql_resolver_by_id(&state.pool, "ens", &chain_ids, id)
        .await
        .map_err(|error| internal_error("resolver", error))?;
    if let Some(row) = row.as_ref() {
        require_resolver_rows_at_head(std::slice::from_ref(row), head.as_ref(), "resolver")?;
    }
    let mut rows =
        hydrate_resolver_rows(ctx, row.into_iter().collect(), head.as_ref(), "resolver").await?;
    Ok(rows.pop())
}

pub async fn hydrate_resolver_rows(
    ctx: &Context<'_>,
    rows: Vec<PhaseGraphqlResolverRow>,
    head: Option<&crate::v2::lookup::head::ServedHead>,
    operation: &str,
) -> async_graphql::Result<Vec<Resolver>> {
    let state = ctx.data::<AppState>()?;
    let keys = rows
        .iter()
        .filter_map(|row| {
            row.inventory_resource_id
                .map(|id| record_inventory_key(id, row.record_version_boundary.as_ref()))
        })
        .collect::<Vec<_>>();
    let inventories = if keys.is_empty() {
        std::collections::HashMap::new()
    } else {
        ctx.data::<DataLoader<RecordInventoryLoader>>()?
            .load_many(keys)
            .await
            .map_err(|error| {
                internal_error(
                    operation,
                    anyhow::anyhow!("record inventory batch load failed: {error:#}"),
                )
            })?
    };
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let key = row
            .inventory_resource_id
            .map(|id| record_inventory_key(id, row.record_version_boundary.as_ref()));
        let inventory = key.as_ref().and_then(|key| inventories.get(key));
        if let Some(inventory) = inventory {
            require_inventory_at_head(
                &inventory.chain_positions,
                inventory.chain_id.as_deref(),
                head,
                operation,
            )?;
        }
        result.push(
            resolver_from_store(row.address, &row.domain_namehash, inventory)
                .map_err(|error| internal_error(operation, error))?,
        );
    }
    revalidate_graphql_head(state, head, operation).await?;
    Ok(result)
}

pub fn resolver_entity_filter_to_storage(
    filter: Option<ResolverEntityFilter>,
) -> Option<GeneratedResolverFilter> {
    let filter = filter.unwrap_or_default();
    let id = match filter.id {
        Some(id) => Some(parse_resolver_id(id.as_str())?),
        None => None,
    };
    Some(GeneratedResolverFilter {
        id,
        address: filter.address.map(|address| address.as_str().to_owned()),
        domain: filter
            .domain
            .map(|domain| bigname_storage::normalize_evm_b256(&domain)),
    })
}

pub fn parse_resolver_id(value: &str) -> Option<ParsedResolverId> {
    let (address, namehash) = value.split_once('-')?;
    if namehash.contains('-') || !canonical_hex(address, 40) || !canonical_hex(namehash, 64) {
        return None;
    }
    Some(ParsedResolverId {
        address: address.to_ascii_lowercase(),
        namehash: namehash.to_ascii_lowercase(),
    })
}

fn canonical_hex(value: &str, digits: usize) -> bool {
    value
        .strip_prefix("0x")
        .is_some_and(|hex| hex.len() == digits && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

pub async fn load_phase_graphql_resolver_by_id(
    pool: &PgPool,
    namespace: &str,
    snapshot_chain_ids: &[String],
    id: ParsedResolverId,
) -> Result<Option<PhaseGraphqlResolverRow>> {
    let rows = load_phase_graphql_resolver_page_offset(
        pool,
        namespace,
        snapshot_chain_ids,
        &GeneratedResolverFilter {
            id: Some(id),
            ..Default::default()
        },
        NameCurrentListOrder::Asc,
        1,
        0,
    )
    .await?;
    Ok(rows.into_iter().next())
}

pub async fn load_phase_graphql_resolver_page_offset(
    pool: &PgPool,
    namespace: &str,
    snapshot_chain_ids: &[String],
    filter: &GeneratedResolverFilter,
    order: NameCurrentListOrder,
    limit: u64,
    offset: u64,
) -> Result<Vec<PhaseGraphqlResolverRow>> {
    let limit = i64::try_from(limit).context("GraphQL resolver limit exceeds SQL limit")?;
    let offset = i64::try_from(offset).context("GraphQL resolver offset exceeds SQL limit")?;
    let mut builder = QueryBuilder::<Postgres>::new("");
    push_filtered_resolvers(&mut builder, namespace, snapshot_chain_ids, filter, order);
    builder.push(" LIMIT ").push_bind(limit);
    builder.push(" OFFSET ").push_bind(offset);
    builder
        .build()
        .fetch_all(pool)
        .await
        .context("failed to load schema-v2 GraphQL resolvers")?
        .into_iter()
        .map(decode_resolver_row)
        .collect()
}

fn push_filtered_resolvers<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    namespace: &'a str,
    snapshot_chain_ids: &'a [String],
    filter: &'a GeneratedResolverFilter,
    order: NameCurrentListOrder,
) {
    builder.push(
        "WITH resolver_bindings AS (SELECT nc.logical_name_id, \
         nc.namehash, nc.declared_summary, \
         COALESCE(nc.serving_resource_id, nc.resource_id) AS inventory_resource_id, \
         nc.declared_summary #> \
         '{topology,version_boundaries,record_version_boundary}' AS record_version_boundary, \
         nc.chain_positions FROM bigname_phase.name_current nc \
         JOIN bigname_phase.name_surfaces surface \
           ON surface.logical_name_id = nc.logical_name_id \
         LEFT JOIN bigname_phase.resources resource \
           ON resource.resource_id = nc.resource_id \
         LEFT JOIN bigname_phase.surface_bindings binding \
           ON binding.surface_binding_id = nc.surface_binding_id \
         LEFT JOIN bigname_phase.token_lineages token_lineage \
           ON token_lineage.token_lineage_id = nc.token_lineage_id ",
    );
    builder.push(DEFAULT_NAME_CURRENT_LINEAGE_JOINS);
    builder.push(
        " WHERE nc.support_status = 'supported' \
         AND nc.declared_summary #>> '{resolver,address}' IS NOT NULL",
    );
    builder.push(DEFAULT_NAME_CURRENT_READ_FILTER);
    builder.push(" AND nc.namespace = ").push_bind(namespace);
    builder.push(" AND nc.declared_summary #>> '{resolver,chain_id}' ");
    if let [chain_id] = snapshot_chain_ids {
        builder.push("= ").push_bind(chain_id);
    } else {
        builder
            .push("= ANY(")
            .push_bind(snapshot_chain_ids)
            .push(")");
    }
    builder.push(
        " AND nc.chain_positions <> '{}'::JSONB AND NOT EXISTS ( \
         SELECT 1 FROM JSONB_EACH(nc.chain_positions) position \
         WHERE position.value ->> 'chain_id' IS NULL \
            OR position.value ->> 'chain_id' <> ALL(",
    );
    builder.push_bind(snapshot_chain_ids).push(
        "))) SELECT LOWER(nc.declared_summary #>> '{resolver,address}') || '-' || nc.namehash AS id, \
        LOWER(nc.declared_summary #>> '{resolver,address}') AS address, \
        nc.namehash AS domain_namehash, nc.inventory_resource_id, \
        nc.record_version_boundary, \
        nc.chain_positions FROM resolver_bindings nc \
        WHERE nc.declared_summary #>> '{resolver,address}' <> '' \
          AND LOWER(nc.declared_summary #>> '{resolver,address}') ~ '^0x[0-9a-f]{40}$' \
          AND LOWER(nc.declared_summary #>> '{resolver,address}') \
              <> '0x0000000000000000000000000000000000000000'",
    );
    if let Some(id) = filter.id.as_ref() {
        builder
            .push(" AND LOWER(nc.declared_summary #>> '{resolver,address}') = ")
            .push_bind(&id.address);
        builder.push(" AND nc.namehash = ").push_bind(&id.namehash);
        builder
            .push(" AND nc.logical_name_id = ")
            .push_bind(format!("{namespace}:{}", id.namehash));
    }
    if let Some(address) = filter.address.as_deref() {
        // Equal inclusive bounds are exact equality while preserving the indexed address
        // expression and its ordering for the bounded index scan.
        builder
            .push(" AND LOWER(nc.declared_summary #>> '{resolver,address}') >= ")
            .push_bind(address)
            .push(" AND LOWER(nc.declared_summary #>> '{resolver,address}') <= ")
            .push_bind(address);
    }
    if let Some(domain) = filter.domain.as_deref() {
        builder.push(" AND nc.namehash = ").push_bind(domain);
    }
    let direction = match order {
        NameCurrentListOrder::Asc => "ASC",
        NameCurrentListOrder::Desc => "DESC",
    };
    if filter.domain.is_some() {
        // Namespace plus namehash identifies at most one current Domain, so this lookup-index order
        // is equivalent to composite-ID order for the domain-filtered result.
        builder.push(format!(
            " ORDER BY nc.namehash {direction}, nc.logical_name_id {direction}"
        ));
    } else {
        // The interpreter mints each `logical_name_id` as `namespace:namehash` in
        // `crates/adapters/src/schema_v2/identity.rs::materialize`. Within this namespace,
        // logical-name order is therefore namehash order and matches composite Resolver-ID order,
        // while retaining the resolver index's complete `(lower(address), logical_name_id)` key.
        builder.push(format!(
            " ORDER BY LOWER(nc.declared_summary #>> '{{resolver,address}}') {direction}, \
             nc.logical_name_id {direction}"
        ));
    }
}

fn decode_resolver_row(row: PgRow) -> Result<PhaseGraphqlResolverRow> {
    Ok(PhaseGraphqlResolverRow {
        id: row.try_get("id")?,
        address: row.try_get("address")?,
        domain_namehash: row.try_get("domain_namehash")?,
        inventory_resource_id: row.try_get("inventory_resource_id")?,
        record_version_boundary: row.try_get("record_version_boundary")?,
        chain_positions: row.try_get("chain_positions")?,
    })
}

#[cfg(test)]
pub async fn explain_phase_graphql_resolver_page(
    pool: &PgPool,
    namespace: &str,
    snapshot_chain_ids: &[String],
    filter: &GeneratedResolverFilter,
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
    push_filtered_resolvers(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composite_resolver_id_parser_is_exact_and_canonical() {
        let address = "0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD";
        let namehash = format!("0x{}", "AB".repeat(32));
        let parsed = parse_resolver_id(&format!("{address}-{namehash}")).unwrap();
        assert_eq!(parsed.address, address.to_ascii_lowercase());
        assert_eq!(parsed.namehash, namehash.to_ascii_lowercase());
        assert!(parse_resolver_id(&format!("{address}-{namehash}-extra")).is_none());
        assert!(parse_resolver_id("not-an-id").is_none());
    }
}
