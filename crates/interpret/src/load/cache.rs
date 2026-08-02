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
}

pub(super) async fn dependencies_are_live(
    connection: &mut PgConnection,
    chain_id: &str,
    snapshot: &PriorSnapshot,
) -> Result<bool> {
    if snapshot.events.len() != snapshot.dependencies.len() {
        return Ok(false);
    }
    let expected = snapshot
        .dependencies
        .values()
        .map(|dependency| (dependency.block_number, dependency.block_hash.clone()))
        .collect::<BTreeSet<_>>();
    if expected.is_empty() {
        return Ok(true);
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
    Ok(live.into_iter().collect::<BTreeSet<_>>() == expected)
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
