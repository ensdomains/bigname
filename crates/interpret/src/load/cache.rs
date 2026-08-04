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

#[derive(Clone, Debug)]
pub(crate) struct PriorSnapshot {
    pub events: Vec<PriorEventInput>,
    pub dependencies: BTreeMap<String, PriorDependency>,
    pub(crate) validated_orphaning_epoch: i64,
    pub(crate) pending_dependencies: BTreeSet<(i64, String)>,
}

pub(super) fn freshly_loaded(mut snapshot: PriorSnapshot, orphaning_epoch: i64) -> PriorSnapshot {
    snapshot.validated_orphaning_epoch = orphaning_epoch;
    snapshot.pending_dependencies.clear();
    snapshot
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
    mut snapshot: PriorSnapshot,
    orphaning_epoch: i64,
) -> Result<Option<PriorSnapshot>> {
    if snapshot.events.len() != snapshot.dependencies.len() {
        return Ok(None);
    }
    let full_revalidation = snapshot.validated_orphaning_epoch != orphaning_epoch;
    let expected = validation_candidates(&mut snapshot, full_revalidation);
    if expected.is_empty() {
        snapshot.validated_orphaning_epoch = orphaning_epoch;
        snapshot.pending_dependencies.clear();
        return Ok(Some(snapshot));
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
    snapshot.validated_orphaning_epoch = orphaning_epoch;
    snapshot.pending_dependencies.clear();
    Ok(Some(snapshot))
}

fn validation_candidates(
    snapshot: &mut PriorSnapshot,
    full_revalidation: bool,
) -> BTreeSet<(i64, String)> {
    if full_revalidation {
        snapshot
            .dependencies
            .values()
            .map(|dependency| (dependency.block_number, dependency.block_hash.clone()))
            .collect()
    } else {
        std::mem::take(&mut snapshot.pending_dependencies)
    }
}

pub(crate) fn fold(
    mut snapshot: PriorSnapshot,
    events: Vec<PriorEventInput>,
    normalized_events: &[NormalizedEvent],
) -> PriorSnapshot {
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
        snapshot.dependencies.insert(
            key,
            PriorDependency {
                block_number,
                block_hash: block_hash.clone(),
            },
        );
        snapshot
            .pending_dependencies
            .insert((block_number, block_hash.clone()));
    }
    let retained = events
        .iter()
        .map(|event| event.retained_state_key.as_str())
        .collect::<BTreeSet<_>>();
    snapshot
        .dependencies
        .retain(|state_key, _| retained.contains(state_key.as_str()));
    snapshot.events = events;
    snapshot
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

    fn snapshot() -> PriorSnapshot {
        PriorSnapshot {
            events: Vec::new(),
            dependencies: BTreeMap::new(),
            validated_orphaning_epoch: 7,
            pending_dependencies: BTreeSet::new(),
        }
    }

    fn prior(state_key: &str) -> PriorEventInput {
        PriorEventInput {
            retained_state_key: retained_prior_state_key(Some(state_key), "unused"),
            chain_id: "chain".to_owned(),
            namespace: "ens".to_owned(),
            logical_name_id: None,
            resource_id: None,
            event_kind: "RecordChanged".to_owned(),
            source_family: "test".to_owned(),
            manifest_version: 1,
            source_manifest_id: None,
            state_scope: None,
            block_timestamp: None,
            after_state: json!({}),
        }
    }

    #[test]
    fn unchanged_epoch_checks_only_anchors_added_by_the_latest_fold() {
        let mut snapshot = fold(snapshot(), vec![prior("new")], &[event("new")]);

        assert_eq!(
            validation_candidates(&mut snapshot, false),
            BTreeSet::from([(10, "block-10".to_owned())])
        );
        assert!(validation_candidates(&mut snapshot, false).is_empty());
    }

    #[test]
    fn changed_epoch_checks_every_retained_anchor() {
        let mut snapshot = snapshot();
        snapshot.dependencies.insert(
            "old".to_owned(),
            PriorDependency {
                block_number: 3,
                block_hash: "block-3".to_owned(),
            },
        );
        snapshot.dependencies.insert(
            "new".to_owned(),
            PriorDependency {
                block_number: 10,
                block_hash: "block-10".to_owned(),
            },
        );

        assert_eq!(
            validation_candidates(&mut snapshot, true),
            BTreeSet::from([(3, "block-3".to_owned()), (10, "block-10".to_owned())])
        );
    }
}
