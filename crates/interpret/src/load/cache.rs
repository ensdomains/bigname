use std::collections::{BTreeMap, BTreeSet};

use bigname_adapters::schema_v2::seam::{INTERPRETER_STATE_KEY, retained_prior_state_key};
use bigname_adapters::schema_v2::{NormalizedEvent, PriorEventInput};
use sqlx::PgConnection;

use crate::{InterpretError, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PriorDependency {
    pub block_number: i64,
    pub block_hash: String,
}

#[derive(Debug)]
pub(crate) struct PriorCache {
    // The adapter owns the retained values. Interpretation keeps only their canonical block
    // anchors so epoch changes can invalidate the opaque adapter session without cloning it.
    pub dependencies: BTreeMap<String, PriorDependency>,
    pub(crate) validated_orphaning_epoch: i64,
    pub(crate) pending_dependencies: BTreeSet<(i64, String)>,
}

pub(crate) struct PriorRestore {
    pub events: Vec<PriorEventInput>,
    pub cache: PriorCache,
}

pub(super) fn freshly_loaded(mut restored: PriorRestore, orphaning_epoch: i64) -> PriorRestore {
    restored.cache.validated_orphaning_epoch = orphaning_epoch;
    restored.cache.pending_dependencies.clear();
    restored
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
    let full_revalidation = cache.validated_orphaning_epoch != orphaning_epoch;
    let expected = validation_candidates(&mut cache, full_revalidation);
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

fn validation_candidates(
    cache: &mut PriorCache,
    full_revalidation: bool,
) -> BTreeSet<(i64, String)> {
    if full_revalidation {
        cache
            .dependencies
            .values()
            .map(|dependency| (dependency.block_number, dependency.block_hash.clone()))
            .collect()
    } else {
        std::mem::take(&mut cache.pending_dependencies)
    }
}

pub(crate) fn fold(mut cache: PriorCache, normalized_events: &[NormalizedEvent]) -> PriorCache {
    for event in normalized_events {
        let (Some(block_number), Some(block_hash)) = (event.block_number, &event.block_hash) else {
            continue;
        };
        let key = retained_prior_state_key(
            event
                .raw_fact_ref
                .get(INTERPRETER_STATE_KEY)
                .and_then(serde_json::Value::as_str),
            &event.event_identity,
        );
        cache.dependencies.insert(
            key,
            PriorDependency {
                block_number,
                block_hash: block_hash.clone(),
            },
        );
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
        }
    }

    fn cache() -> PriorCache {
        PriorCache {
            dependencies: BTreeMap::new(),
            validated_orphaning_epoch: 7,
            pending_dependencies: BTreeSet::new(),
        }
    }

    #[test]
    fn unchanged_epoch_checks_only_anchors_added_by_the_latest_fold() {
        let mut cache = fold(cache(), &[event("new")]);

        assert_eq!(
            validation_candidates(&mut cache, false),
            BTreeSet::from([(10, "block-10".to_owned())])
        );
        assert!(validation_candidates(&mut cache, false).is_empty());
    }

    #[test]
    fn changed_epoch_checks_every_retained_anchor() {
        let mut cache = cache();
        cache.dependencies.insert(
            "old".to_owned(),
            PriorDependency {
                block_number: 3,
                block_hash: "block-3".to_owned(),
            },
        );
        cache.dependencies.insert(
            "new".to_owned(),
            PriorDependency {
                block_number: 10,
                block_hash: "block-10".to_owned(),
            },
        );

        assert_eq!(
            validation_candidates(&mut cache, true),
            BTreeSet::from([(3, "block-3".to_owned()), (10, "block-10".to_owned())])
        );
    }
}
