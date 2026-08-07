use std::collections::BTreeMap;

use crate::{
    IngestError, Result,
    engine::{BLOCKS_PER_BATCH, BatchRequest, Engine, LiveBatchOutcome, LiveBatchRequest, Marker},
    plan::{primary_source, publishable_heads, sort_sources, validate_request},
    provider::{ResolvedBlock, SharedProvider, provider_error},
};

impl Engine {
    /// Loads at most one winning-fork suffix batch after the currently published head.
    ///
    /// This is deliberately not a historical scanner. The published chain head and its
    /// finalized boundary anchor every request; normal historical coverage remains ingest's job.
    pub async fn run_live_batch(&self, mut request: LiveBatchRequest) -> Result<LiveBatchOutcome> {
        validate_live_request(&request)?;
        sort_sources(&mut request.sources);
        let primary = primary_source(&request.sources)?;
        let provider = self.provider(&request.chain_id, primary).await?;
        let snapshot = provider
            .heads()
            .await
            .map_err(|error| provider_error("failed to fetch live target heads", error))?;
        if snapshot.safe.is_none() || snapshot.finalized.is_none() {
            return Err(IngestError::data_integrity(
                "live provider must report safe and finalized checkpoint heads",
            ));
        }

        let published = self
            .load_published_head(&request.chain_id)
            .await?
            .ok_or_else(|| {
                IngestError::data_integrity(format!(
                    "live follow requires a published ingest head for chain {}",
                    request.chain_id
                ))
            })?;
        if snapshot.latest.number < published.latest.number {
            let stored = self
                .load_readable_hashes(
                    &request.chain_id,
                    snapshot.latest.number,
                    snapshot.latest.number,
                )
                .await?;
            if stored.get(&snapshot.latest.number) == Some(&snapshot.latest.hash) {
                return Ok(LiveBatchOutcome {
                    caught_up: true,
                    current: published.latest.clone(),
                    target: published.latest,
                    heads: None,
                    estimated_write_bytes: 0,
                });
            }
        }
        let floor = published
            .finalized
            .as_ref()
            .map_or(0, |marker| marker.number);
        let common = self
            .find_common_ancestor(
                &request.chain_id,
                &provider,
                published.latest.number.min(snapshot.latest.number),
                floor,
            )
            .await?;
        if let Some(finalized) = &published.finalized
            && common.number < finalized.number
        {
            return Err(IngestError::data_integrity(format!(
                "live provider fork for chain {} does not include finalized block {} at {}",
                request.chain_id, finalized.hash, finalized.number
            )));
        }

        let load_to = snapshot
            .latest
            .number
            .min(common.number.saturating_add(BLOCKS_PER_BATCH));
        let (current, estimated_write_bytes) = if load_to > common.number {
            // Extending the published head does not imply staying above the node's retention:
            // downtime or a deep reorg can put the ancestor below a floor that moved on.
            self.enforce_window_floor(&request.chain_id, primary, common.number + 1, load_to)
                .await?;
            let loaded = self
                .load_window(
                    &request.chain_id,
                    primary,
                    &request.sources,
                    common.number + 1,
                    load_to,
                )
                .await?;
            self.require_loaded_suffix_descends_from(&request.chain_id, &loaded.marker, &common)
                .await?;
            (loaded.marker, loaded.estimated_write_bytes)
        } else {
            (common, 0)
        };
        let target = Marker {
            number: snapshot.latest.number,
            hash: snapshot.latest.hash.clone(),
        };
        Ok(LiveBatchOutcome {
            caught_up: current == target,
            heads: Some(publishable_heads(&current, &snapshot)),
            current,
            target,
            estimated_write_bytes,
        })
    }

    async fn find_common_ancestor(
        &self,
        chain_id: &str,
        provider: &SharedProvider,
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
            let stored = self
                .load_readable_hashes(chain_id, chunk_floor, from)
                .await?;
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
        &self,
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
        .fetch_all(&self.pool)
        .await
        .map(|rows| rows.into_iter().collect())
        .map_err(|error| {
            IngestError::database(
                format!("failed to load readable ancestry {from}..={to} for chain {chain_id}"),
                error,
            )
        })
    }

    async fn require_loaded_suffix_descends_from(
        &self,
        chain_id: &str,
        loaded: &Marker,
        ancestor: &Marker,
    ) -> Result<()> {
        let connected: bool = sqlx::query_scalar(
            "WITH RECURSIVE loaded_path AS (
                 SELECT block_number, block_hash, parent_hash
                 FROM chain_lineage
                 WHERE chain_id = $1 AND block_number = $2 AND block_hash = $3
                 UNION ALL
                 SELECT parent.block_number, parent.block_hash, parent.parent_hash
                 FROM chain_lineage parent
                 JOIN loaded_path child
                   ON parent.chain_id = $1
                  AND parent.block_hash = child.parent_hash
                  AND parent.block_number = child.block_number - 1
                 WHERE child.block_number > $4
             )
             SELECT EXISTS (
                 SELECT 1 FROM loaded_path
                 WHERE block_number = $4 AND block_hash = $5
             )",
        )
        .bind(chain_id)
        .bind(loaded.number)
        .bind(&loaded.hash)
        .bind(ancestor.number)
        .bind(&ancestor.hash)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| {
            IngestError::database(
                format!("failed to validate loaded live suffix for chain {chain_id}"),
                error,
            )
        })?;
        if !connected {
            return Err(IngestError::transient(format!(
                "live provider changed the canonical path while loading blocks after {} ({}) for chain {chain_id}; retry from a fresh head snapshot",
                ancestor.number, ancestor.hash
            )));
        }
        Ok(())
    }

    async fn load_published_head(&self, chain_id: &str) -> Result<Option<PublishedHead>> {
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
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            IngestError::database(
                format!("failed to load published head for chain {chain_id}"),
                error,
            )
        })?;
        row.map(
            |(
                latest_number,
                latest_hash,
                safe_number,
                safe_hash,
                finalized_number,
                finalized_hash,
            )| {
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
}

struct PublishedHead {
    latest: Marker,
    #[allow(dead_code)]
    safe: Option<Marker>,
    finalized: Option<Marker>,
}

fn validate_live_request(request: &LiveBatchRequest) -> Result<()> {
    validate_request(&BatchRequest {
        chain_id: request.chain_id.clone(),
        sources: request.sources.clone(),
        cursors: Vec::new(),
        redo_range: None,
        resume_current: None,
    })?;
    if request.live_handoff.number < 0 || request.live_handoff.hash.trim().is_empty() {
        return Err(IngestError::configuration(
            "live handoff marker must contain a nonnegative block and a hash",
        ));
    }
    Ok(())
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
