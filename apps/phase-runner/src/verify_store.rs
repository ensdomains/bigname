use std::collections::BTreeMap;

use bigname_ingest::{VerificationLog, WatchFilter};

use crate::{
    config::{SourceConfig, normalized_source_kind},
    database::VerificationDatabase,
    error::{ErrorKind, RunnerError, RunnerResult},
    heads::BlockMarker,
    phase::{BlockRange, VerificationLevel},
};

pub(crate) struct StoredVerificationBatch {
    pub(crate) end: BlockMarker,
    pub(crate) filter: WatchFilter,
    pub(crate) logs: Vec<VerificationLog>,
}

#[derive(Clone)]
pub(crate) struct VerificationStore {
    database: VerificationDatabase,
}

impl VerificationStore {
    pub(crate) fn new(database: VerificationDatabase) -> Self {
        Self { database }
    }

    pub(crate) async fn load_batch(
        &self,
        chain_id: &str,
        from_block: i64,
        to_block: i64,
    ) -> RunnerResult<StoredVerificationBatch> {
        let pool = self.database.pool();
        let filter = bigname_ingest::load_watch_filter(pool, chain_id, from_block, to_block)
            .await
            .map_err(map_ingest_error)?;
        let end = self.finalized_marker(chain_id, to_block).await?;

        let mut by_identity = BTreeMap::new();
        for query in filter.queries() {
            let addresses = query
                .addresses
                .iter()
                .map(|address| address.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let topic0s = query
                .topic0s
                .iter()
                .map(|topic| topic.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let rows =
                sqlx::query_as::<_, (String, i64, String, i64, i64, String, Vec<String>, Vec<u8>)>(
                    "SELECT raw.block_hash,
                        raw.block_number,
                        raw.transaction_hash,
                        raw.transaction_index,
                        raw.log_index,
                        raw.emitting_address,
                        raw.topics,
                        raw.data
                 FROM raw_logs raw
                 JOIN chain_lineage lineage
                   ON lineage.chain_id = raw.chain_id
                  AND lineage.block_hash = raw.block_hash
                  AND lineage.block_number = raw.block_number
                 WHERE raw.chain_id = $1
                   AND raw.block_number BETWEEN $2 AND $3
                   AND lineage.canonicality_state = 'finalized'
                   AND lower(raw.topics[1]) = ANY($4::text[])
                   AND (
                       cardinality($5::text[]) = 0
                       OR lower(raw.emitting_address) = ANY($5::text[])
                   )
                 ORDER BY raw.block_number, raw.transaction_index, raw.log_index",
                )
                .bind(chain_id)
                .bind(query.from_block)
                .bind(query.to_block)
                .bind(topic0s)
                .bind(addresses)
                .fetch_all(pool)
                .await
                .map_err(|error| {
                    RunnerError::database(
                        format!(
                            "failed to scan stored verification logs for chain {chain_id} over \
                         {}..={}",
                            query.from_block, query.to_block
                        ),
                        error,
                    )
                })?;
            for row in rows {
                let log = VerificationLog {
                    block_hash: row.0,
                    block_number: row.1,
                    transaction_hash: row.2,
                    transaction_index: row.3,
                    log_index: row.4,
                    address: row.5,
                    topics: row.6,
                    data: row.7,
                };
                let key = (log.block_hash.clone(), log.log_index);
                if let Some(previous) = by_identity.insert(key.clone(), log.clone())
                    && previous != log
                {
                    return Err(RunnerError::data_integrity(format!(
                        "stored verification scan found conflicting log identity {} {}",
                        key.0, key.1
                    )));
                }
            }
        }
        let mut logs = by_identity
            .into_values()
            .filter(|log| {
                log.topics
                    .first()
                    .is_some_and(|topic0| filter.includes(&log.address, topic0, log.block_number))
            })
            .collect::<Vec<_>>();
        logs.sort_by_key(|log| {
            (
                log.block_number,
                log.transaction_index,
                log.log_index,
                log.block_hash.clone(),
            )
        });
        Ok(StoredVerificationBatch { end, filter, logs })
    }

    pub(crate) async fn finalized_marker(
        &self,
        chain_id: &str,
        block_number: i64,
    ) -> RunnerResult<BlockMarker> {
        let end: Option<(i64, String)> = sqlx::query_as(
            "SELECT block_number, block_hash
             FROM chain_lineage
             WHERE chain_id = $1
               AND block_number = $2
               AND canonicality_state = 'finalized'",
        )
        .bind(chain_id)
        .bind(block_number)
        .fetch_optional(self.database.pool())
        .await
        .map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to load finalized verification boundary for chain {chain_id} at \
                     block {block_number}"
                ),
                error,
            )
        })?;
        let (number, hash) = end.ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "verification target block {block_number} for chain {chain_id} is not finalized"
            ))
        })?;
        Ok(BlockMarker { number, hash })
    }

    pub(crate) async fn ingest_start(&self, chain_id: &str) -> RunnerResult<i64> {
        let start: Option<i64> = sqlx::query_scalar(
            "SELECT min(start_block_number)
             FROM ingest_cursors
             WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_one(self.database.pool())
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to load durable ingest start for chain {chain_id}"),
                error,
            )
        })?;
        start.ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "chain {chain_id} has no durable ingest cursor for verification"
            ))
        })
    }

    pub(crate) async fn require_provider_trusted_extent(
        &self,
        chain_id: &str,
        source: &SourceConfig,
        target: &BlockMarker,
        redo_restores_retained_target: bool,
    ) -> RunnerResult<()> {
        let target = if redo_restores_retained_target {
            self.retained_finalized_target(chain_id).await?
        } else {
            target.clone()
        };
        type CursorRow = (String, String, i64, i64, Option<i64>, Option<i64>, bool);
        let row: Option<CursorRow> = sqlx::query_as(
            "SELECT source_kind, seed_basis, start_block_number, next_block_number,
                    target_block_number, last_processed_block_number,
                    (
                        WITH RECURSIVE cursor_ancestry (
                            block_hash, parent_hash, block_number
                        ) AS (
                            SELECT lineage.block_hash, lineage.parent_hash,
                                   lineage.block_number
                            FROM chain_lineage lineage
                            WHERE lineage.chain_id = cursor.chain_id
                              AND lineage.block_number = cursor.last_processed_block_number
                              AND lineage.block_hash = cursor.last_processed_block_hash
                              AND lineage.canonicality_state <> 'observed'
                            UNION ALL
                            SELECT parent.block_hash, parent.parent_hash,
                                   parent.block_number
                            FROM chain_lineage parent
                            JOIN cursor_ancestry child
                              ON parent.chain_id = cursor.chain_id
                             AND parent.block_hash = child.parent_hash
                             AND parent.block_number = child.block_number - 1
                            WHERE child.block_number > $3
                              AND parent.canonicality_state <> 'observed'
                        )
                        SELECT EXISTS (
                            SELECT 1
                            FROM cursor_ancestry
                            WHERE block_number = $3 AND block_hash = $4
                        )
                    )
             FROM ingest_cursors cursor
             WHERE chain_id = $1 AND source_key = $2",
        )
        .bind(chain_id)
        .bind(&source.source_key)
        .bind(target.number)
        .bind(&target.hash)
        .fetch_optional(self.database.pool())
        .await
        .map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to validate provider-trusted ingest cursor {} for chain {chain_id}",
                    source.source_key
                ),
                error,
            )
        })?;
        let Some((
            kind,
            seed,
            start,
            next,
            cursor_target,
            processed,
            processed_descends_from_target,
        )) = row
        else {
            return Err(RunnerError::data_integrity(format!(
                "chain {chain_id} has no durable ingest cursor for configured source {}",
                source.source_key
            )));
        };
        let configuration_matches = normalized_source_kind(&kind)
            == normalized_source_kind(&source.source_kind)
            && seed == source.seed_basis.as_str()
            && start == source.start_block_number;
        // The numeric fields must agree that intake reached the frozen target. The last processed
        // hash may later be orphaned, but its retained ancestry must still contain this already-
        // frozen finalized block.
        let covers_target = next > target.number
            && cursor_target.is_some_and(|number| number >= target.number)
            && processed.is_some_and(|number| number >= target.number)
            && processed_descends_from_target;
        if configuration_matches && covers_target {
            return Ok(());
        }
        Err(RunnerError::data_integrity(format!(
            "provider-trusted verification for chain {chain_id} requires configured source {} to \
             have a matching durable ingest cursor through finalized block {}",
            source.source_key, target.number
        )))
    }

    async fn retained_finalized_target(&self, chain_id: &str) -> RunnerResult<BlockMarker> {
        let row: Option<(Option<i64>, Option<String>)> = sqlx::query_as(
            "SELECT target_block_number, target_block_hash
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'verify'",
        )
        .bind(chain_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to load retained verification target for chain {chain_id}"),
                error,
            )
        })?;
        let Some((Some(number), Some(hash))) = row else {
            return Err(RunnerError::data_integrity(format!(
                "provider-trusted verification redo for chain {chain_id} has no retained target"
            )));
        };
        let retained = BlockMarker::new(number, hash)?;
        let finalized = self.finalized_marker(chain_id, number).await?;
        if retained != finalized {
            return Err(RunnerError::data_integrity(format!(
                "provider-trusted verification redo target for chain {chain_id} does not match \
                 finalized lineage at block {number}"
            )));
        }
        Ok(retained)
    }

    pub(crate) async fn level_for_redo(
        &self,
        chain_id: &str,
        range: BlockRange,
        full_redo_level: VerificationLevel,
    ) -> RunnerResult<VerificationLevel> {
        let row: (Option<i64>, Option<i64>, Option<String>) = sqlx::query_as(
            "SELECT
                 (SELECT min(start_block_number)
                  FROM ingest_cursors
                  WHERE chain_id = $1),
                 current_block_number,
                 verification_level
             FROM chain_phase_state
             WHERE chain_id = $1 AND phase_name = 'verify'",
        )
        .bind(chain_id)
        .fetch_one(self.database.pool())
        .await
        .map_err(|error| {
            RunnerError::database(
                format!("failed to load retained verification extent for chain {chain_id}"),
                error,
            )
        })?;
        let (Some(extent_from), Some(extent_to), Some(level)) = row else {
            return Err(RunnerError::data_integrity(format!(
                "verify redo for chain {chain_id} has no retained verification extent and level"
            )));
        };
        if range.from <= extent_from && range.to >= extent_to {
            return Ok(full_redo_level);
        }
        let retained = match level.as_str() {
            "quick_synced" => VerificationLevel::QuickSynced,
            "cross_checked" => VerificationLevel::CrossChecked,
            "node_checked" => VerificationLevel::NodeChecked,
            value => Err(RunnerError::data_integrity(format!(
                "verify redo for chain {chain_id} found unknown retained verification level \
                 {value:?}"
            )))?,
        };
        Ok(crate::verify_level::weakest_level(
            retained,
            full_redo_level,
        ))
    }
}

fn map_ingest_error(error: bigname_ingest::IngestError) -> RunnerError {
    let kind = match error.kind() {
        bigname_ingest::ErrorKind::Transient => ErrorKind::Transient,
        bigname_ingest::ErrorKind::DataIntegrity => ErrorKind::DataIntegrity,
        bigname_ingest::ErrorKind::Configuration => ErrorKind::Configuration,
    };
    RunnerError::new(kind, error.to_string())
}
