use crate::{
    IngestError, Result,
    engine::{
        BLOCKS_PER_BATCH, BatchRequest, Engine, LiveBatchOutcome, LiveBatchRequest, Marker,
        live_plan::{published_ancestor, require_checkpoint_heads},
    },
    plan::{primary_source, publishable_heads, sort_sources, validate_request},
    provider::provider_error,
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
        require_checkpoint_heads(&snapshot)?;

        let node_latest = Marker {
            number: snapshot.latest.number,
            hash: snapshot.latest.hash.clone(),
        };
        let (published, common) =
            published_ancestor(&self.pool, &request.chain_id, &provider, &node_latest).await?;
        let Some(common) = common else {
            return Ok(LiveBatchOutcome {
                caught_up: true,
                current: published.latest.clone(),
                target: published.latest,
                heads: None,
                estimated_write_bytes: 0,
            });
        };

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
                    // Live-follow works at the unfinalized head, exactly where a
                    // prefetched range could be read before the reorg that changes it.
                    None,
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
