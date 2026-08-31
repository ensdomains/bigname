use async_graphql::{Context, Result, SimpleObject};

use crate::AppState;

use super::error::internal_error;
use super::inputs::BlockHeight;
use super::scalars::Bytes;
use super::snapshot::{load_graphql_head, load_graphql_indexing_errors, validate_block_constraint};

/// Block metadata for the indexed state selected for this request.
#[derive(SimpleObject)]
#[graphql(name = "_Block_")]
pub(crate) struct SubgraphBlock {
    pub(crate) number: i32,
    pub(crate) hash: Option<Bytes>,
    pub(crate) timestamp: Option<i32>,
    #[graphql(name = "parentHash")]
    pub(crate) parent_hash: Option<Bytes>,
}

/// Metadata for the currently served GraphQL publication.
#[derive(SimpleObject)]
#[graphql(name = "_Meta_")]
pub(crate) struct SubgraphMeta {
    pub(crate) block: SubgraphBlock,
    pub(crate) deployment: String,
    #[graphql(name = "hasIndexingErrors")]
    pub(crate) has_indexing_errors: bool,
}

pub(crate) async fn resolve_meta(
    ctx: &Context<'_>,
    block: Option<BlockHeight>,
) -> Result<Option<SubgraphMeta>> {
    let state = ctx.data::<AppState>()?;
    let head = load_graphql_head(ctx, "_meta").await?;
    let Some(head) = head else {
        validate_block_constraint(None, block.as_ref(), "_meta")?;
        return Ok(None);
    };
    let constraint = validate_block_constraint(Some(&head), block.as_ref(), "_meta")?;
    // `_meta.block` is bound to the single active chain; revisit this together with block matching
    // when one ENS request can activate a second chain.
    let position = head
        .selected()
        .chain_positions
        .as_map()
        .values()
        .next()
        .ok_or_else(|| internal_error("_meta", anyhow::anyhow!("served head has no block")))?;
    let lineage = bigname_storage::load_chain_lineage_block(
        &state.pool,
        &position.chain_id,
        &position.block_hash,
    )
    .await
    .map_err(|error| internal_error("_meta", error))?
    .ok_or_else(|| internal_error("_meta", anyhow::anyhow!("served head has no lineage row")))?;
    let number = i32::try_from(position.block_number)
        .map_err(|error| internal_error("_meta", anyhow::anyhow!(error)))?;
    let hash = if constraint.hides_hash() {
        None
    } else {
        Some(
            Bytes::parse_string(position.block_hash.clone())
                .map_err(|error| internal_error("_meta", anyhow::anyhow!(error)))?,
        )
    };
    let parent_hash = lineage
        .parent_hash
        .map(Bytes::parse_string)
        .transpose()
        .map_err(|error| internal_error("_meta", anyhow::anyhow!(error)))?;
    let timestamp = i32::try_from(position.timestamp.unix_timestamp()).ok();
    let has_indexing_errors = load_graphql_indexing_errors(state, head.selected(), "_meta").await?;
    super::snapshot::revalidate_graphql_head(state, Some(&head), "_meta").await?;

    Ok(Some(SubgraphMeta {
        block: SubgraphBlock {
            number,
            hash,
            timestamp,
            parent_hash,
        },
        deployment: bigname_content_hash::INTERPRETER_CONTENT_HASH.to_owned(),
        has_indexing_errors,
    }))
}
