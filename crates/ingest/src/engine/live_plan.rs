//! Where live follow resumes: the published chain head's common ancestor with the node.
//!
//! Read-only. [`Engine::run_live_batch`](crate::Engine::run_live_batch) loads the suffix
//! after the block selected here; the source-transport maintenance command judges a proposed
//! reader on the same block without loading anything, so the two cannot disagree about which
//! block live follow needs first.
use std::collections::BTreeMap;

use sqlx::PgPool;

use crate::{
    IngestError, Result,
    engine::{BLOCKS_PER_BATCH, Marker},
    provider::{ChainProvider, ResolvedBlock, provider_error},
    verification::VerificationProvider,
};

pub(super) struct PublishedHead {
    pub(super) latest: Marker,
    #[allow(dead_code)]
    pub(super) safe: Option<Marker>,
    pub(super) finalized: Option<Marker>,
}

/// Where live follow resumes on one node, selected the way a live batch selects it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveContinuation {
    /// The published block the node still holds on the path to its head. Live follow extends
    /// the chain from it, so it is the boundary the node must retain.
    pub ancestor: Marker,
    /// The node's head when the selection was made.
    pub node_head: Marker,
}

impl LiveContinuation {
    /// The first block live follow loads.
    pub const fn next_block(&self) -> i64 {
        self.ancestor.number + 1
    }

    /// Whether the node already holds that block. It does not once live follow has
    /// published the node's head and is waiting for the next one.
    pub const fn next_block_is_available(&self) -> bool {
        self.node_head.number > self.ancestor.number
    }
}

/// Selects where live follow resumes for `chain_id` on the node `provider` reads.
///
/// The rule is the live batch's own: a published head is required; a node standing on a
/// published block behind the published head has nothing to load yet, so that block is the
/// ancestor; otherwise the ancestor is the highest block at or below both heads that the node
/// reports with the hash the chain published, walked back no further than the published
/// finalized block.
pub async fn plan_live_continuation(
    pool: &PgPool,
    chain_id: &str,
    provider: &VerificationProvider,
) -> Result<LiveContinuation> {
    let provider = provider.chain_provider();
    let snapshot = provider
        .heads()
        .await
        .map_err(|error| provider_error("failed to fetch live target heads", error))?;
    let node_head = Marker {
        number: snapshot.latest.number,
        hash: snapshot.latest.hash,
    };
    let (_, ancestor) = published_ancestor(pool, chain_id, provider, &node_head).await?;
    Ok(LiveContinuation {
        ancestor: ancestor.unwrap_or_else(|| node_head.clone()),
        node_head,
    })
}

/// The published head and the block live follow extends from, or `None` for the block when
/// the node stands on a published block behind the published head and nothing is loadable yet.
pub(super) async fn published_ancestor(
    pool: &PgPool,
    chain_id: &str,
    provider: &ChainProvider,
    node_latest: &Marker,
) -> Result<(PublishedHead, Option<Marker>)> {
    let published = load_published_head(pool, chain_id).await?.ok_or_else(|| {
        IngestError::data_integrity(format!(
            "live follow requires a published ingest head for chain {chain_id}"
        ))
    })?;
    if node_latest.number < published.latest.number {
        let stored =
            load_readable_hashes(pool, chain_id, node_latest.number, node_latest.number).await?;
        if stored.get(&node_latest.number) == Some(&node_latest.hash) {
            return Ok((published, None));
        }
    }
    let floor = published
        .finalized
        .as_ref()
        .map_or(0, |marker| marker.number);
    let common = find_common_ancestor(
        pool,
        chain_id,
        provider,
        published.latest.number.min(node_latest.number),
        floor,
    )
    .await?;
    if let Some(finalized) = &published.finalized
        && common.number < finalized.number
    {
        return Err(IngestError::data_integrity(format!(
            "live provider fork for chain {chain_id} does not include finalized block {} at {}",
            finalized.hash, finalized.number
        )));
    }
    Ok((published, Some(common)))
}

async fn find_common_ancestor(
    pool: &PgPool,
    chain_id: &str,
    provider: &ChainProvider,
    mut from: i64,
    floor: i64,
) -> Result<Marker> {
    if from < floor {
        return Err(IngestError::data_integrity(format!(
            "live head for chain {chain_id} is below the finalized boundary {floor}"
        )));
    }
    while from >= floor {
        let chunk_floor = floor.max(from.saturating_sub(BLOCKS_PER_BATCH - 1));
        let numbers = (chunk_floor..=from).collect::<Vec<_>>();
        let resolved = provider.resolve(&numbers).await.map_err(|error| {
            provider_error(
                &format!("failed to walk live head ancestry {chunk_floor}..={from}"),
                error,
            )
        })?;
        let stored = load_readable_hashes(pool, chain_id, chunk_floor, from).await?;
        if let Some(block) = resolved
            .iter()
            .rev()
            .find(|block| stored.get(&block.number) == Some(&block.hash))
        {
            return Ok(marker(block));
        }
        if chunk_floor == floor {
            break;
        }
        from = chunk_floor - 1;
    }
    Err(IngestError::data_integrity(format!(
        "live provider path for chain {chain_id} has no stored canonical ancestor at or above \
         block {floor}"
    )))
}

async fn load_readable_hashes(
    pool: &PgPool,
    chain_id: &str,
    from: i64,
    to: i64,
) -> Result<BTreeMap<i64, String>> {
    sqlx::query_as::<_, (i64, String)>(
        "
        SELECT block_number, block_hash
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number BETWEEN $2 AND $3
          AND canonicality_state IN ('canonical', 'safe', 'finalized')
        ",
    )
    .bind(chain_id)
    .bind(from)
    .bind(to)
    .fetch_all(pool)
    .await
    .map(|rows| rows.into_iter().collect())
    .map_err(|error| {
        IngestError::database(
            format!("failed to load readable ancestry {from}..={to} for chain {chain_id}"),
            error,
        )
    })
}

async fn load_published_head(pool: &PgPool, chain_id: &str) -> Result<Option<PublishedHead>> {
    type Row = (
        i64,
        String,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
    );
    let row: Option<Row> = sqlx::query_as(
        "
        SELECT latest_block_number,
               latest_block_hash,
               safe_block_number,
               safe_block_hash,
               finalized_block_number,
               finalized_block_hash
        FROM chain_heads
        WHERE chain_id = $1
        ",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        IngestError::database(
            format!("failed to load published head for chain {chain_id}"),
            error,
        )
    })?;
    row.map(
        |(latest_number, latest_hash, safe_number, safe_hash, finalized_number, finalized_hash)| {
            Ok(PublishedHead {
                latest: Marker {
                    number: latest_number,
                    hash: latest_hash,
                },
                safe: optional_marker(safe_number, safe_hash)?,
                finalized: optional_marker(finalized_number, finalized_hash)?,
            })
        },
    )
    .transpose()
}

fn marker(block: &ResolvedBlock) -> Marker {
    Marker {
        number: block.number,
        hash: block.hash.clone(),
    }
}

fn optional_marker(number: Option<i64>, hash: Option<String>) -> Result<Option<Marker>> {
    match (number, hash) {
        (Some(number), Some(hash)) => Ok(Some(Marker { number, hash })),
        (None, None) => Ok(None),
        _ => Err(IngestError::data_integrity(
            "stored chain head marker has only a number or only a hash",
        )),
    }
}
