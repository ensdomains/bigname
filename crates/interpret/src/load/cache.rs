use std::collections::BTreeSet;

use bigname_adapters::schema_v2::NormalizedEvent;
#[cfg(test)]
use bigname_adapters::schema_v2::seam::INTERPRETER_STATE_KEY;
use sqlx::PgConnection;

use crate::{InterpretError, Result};

#[derive(Debug)]
pub(crate) struct PriorCache {
    pub(crate) validated_orphaning_epoch: i64,
    // Only anchors added since the latest validation are retained. An epoch change discards the
    // whole retained in-process state and reloads it from the database.
    pub(crate) pending_dependencies: BTreeSet<(i64, String)>,
}

pub(super) fn freshly_loaded(orphaning_epoch: i64) -> PriorCache {
    PriorCache {
        validated_orphaning_epoch: orphaning_epoch,
        pending_dependencies: BTreeSet::new(),
    }
}

pub(super) async fn orphaning_epoch(connection: &mut PgConnection, chain_id: &str) -> Result<i64> {
    sqlx::query_scalar(
        "
        SELECT COALESCE(
            (
                SELECT lineage_orphaning_epoch
                FROM chain_heads
                WHERE chain_id = $1
            ),
            0
        )
        ",
    )
    .bind(chain_id)
    .fetch_one(&mut *connection)
    .await
    .map_err(|error| {
        InterpretError::database("failed to load chain-lineage orphaning epoch", error)
    })
}

pub(super) async fn revalidate(
    connection: &mut PgConnection,
    chain_id: &str,
    mut cache: PriorCache,
    orphaning_epoch: i64,
) -> Result<Option<PriorCache>> {
    if cache.validated_orphaning_epoch != orphaning_epoch {
        return Ok(None);
    }
    let expected = std::mem::take(&mut cache.pending_dependencies);
    if expected.is_empty() {
        cache.validated_orphaning_epoch = orphaning_epoch;
        cache.pending_dependencies.clear();
        return Ok(Some(cache));
    }
    let block_numbers = expected
        .iter()
        .map(|(block_number, _)| *block_number)
        .collect::<Vec<_>>();
    let block_hashes = expected
        .iter()
        .map(|(_, block_hash)| block_hash.clone())
        .collect::<Vec<_>>();
    let live: Vec<(i64, String)> = sqlx::query_as(
        "
        SELECT dependency.block_number, dependency.block_hash
        FROM unnest($2::bigint[], $3::text[])
             AS dependency(block_number, block_hash)
        JOIN chain_lineage lineage
          ON lineage.chain_id = $1
         AND lineage.block_number = dependency.block_number
         AND lineage.block_hash = dependency.block_hash
         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
        ORDER BY dependency.block_number, dependency.block_hash
        ",
    )
    .bind(chain_id)
    .bind(&block_numbers)
    .bind(&block_hashes)
    .fetch_all(&mut *connection)
    .await
    .map_err(|error| {
        InterpretError::database("failed to validate cached adapter-state lineage", error)
    })?;
    if live.into_iter().collect::<BTreeSet<_>>() != expected {
        return Ok(None);
    }
    cache.validated_orphaning_epoch = orphaning_epoch;
    cache.pending_dependencies.clear();
    Ok(Some(cache))
}

pub(crate) fn fold(mut cache: PriorCache, normalized_events: &[NormalizedEvent]) -> PriorCache {
    for event in normalized_events {
        let (Some(block_number), Some(block_hash)) = (event.block_number, &event.block_hash) else {
            continue;
        };
        cache
            .pending_dependencies
            .insert((block_number, block_hash.clone()));
    }
    cache
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn event(state_key: &str) -> NormalizedEvent {
        NormalizedEvent {
            event_identity: format!("event-{state_key}"),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: "RecordChanged".to_owned(),
            source_family: "test".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            chain_id: "chain".to_owned(),
            block_number: Some(10),
            block_hash: Some("block-10".to_owned()),
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            raw_fact_ref: json!({ INTERPRETER_STATE_KEY: state_key }),
            derivation_kind: "raw_log_preimage_observation".to_owned(),
            canonicality_state: "canonical".to_owned(),
            before_state: json!({}),
            after_state: json!({}),
            migration_correlation_ids: Vec::new(),
            consumer_visibility: "activated".to_owned(),
            before_state_explicit: false,
        }
    }

    fn cache() -> PriorCache {
        PriorCache {
            validated_orphaning_epoch: 7,
            pending_dependencies: BTreeSet::new(),
        }
    }

    #[test]
    fn unchanged_epoch_checks_only_anchors_added_by_the_latest_fold() {
        let mut cache = fold(cache(), &[event("new")]);

        assert_eq!(
            std::mem::take(&mut cache.pending_dependencies),
            BTreeSet::from([(10, "block-10".to_owned())])
        );
        assert!(cache.pending_dependencies.is_empty());
    }

    #[test]
    fn fold_retains_only_distinct_batch_anchors() {
        let cache = fold(cache(), &[event("first"), event("second")]);

        assert_eq!(
            cache.pending_dependencies,
            BTreeSet::from([(10, "block-10".to_owned())])
        );
    }
}
