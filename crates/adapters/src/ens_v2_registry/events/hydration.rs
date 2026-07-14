use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bigname_storage::NormalizedEvent;
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, types::Uuid};

use super::super::{constants::EVENT_KIND_SUBREGISTRY_CHANGED, util::normalize_address};

struct TargetRequest {
    event_index: i64,
    chain_id: String,
    from_contract_instance_id: Uuid,
    target_address: String,
    block_number: i64,
    block_hash: String,
}

pub(in crate::ens_v2_registry) async fn hydrate_subregistry_event_target_ids(
    pool: &PgPool,
    events: &mut [NormalizedEvent],
) -> Result<()> {
    let requests = events
        .iter_mut()
        .enumerate()
        .filter(|(_, event)| event.event_kind == EVENT_KIND_SUBREGISTRY_CHANGED)
        .filter_map(|(event_index, event)| {
            event.after_state["to_contract_instance_id"] = Value::Null;
            let target_address = event
                .after_state
                .get("subregistry")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)?;
            Some((event_index, event, target_address))
        })
        .map(|(event_index, event, target_address)| {
            let from_contract_instance_id = event
                .after_state
                .get("from_contract_instance_id")
                .and_then(Value::as_str)
                .with_context(|| {
                    format!(
                        "SubregistryChanged event {} is missing from_contract_instance_id",
                        event.event_identity
                    )
                })?
                .parse::<Uuid>()
                .with_context(|| {
                    format!(
                        "SubregistryChanged event {} has an invalid from_contract_instance_id",
                        event.event_identity
                    )
                })?;
            Ok(TargetRequest {
                event_index: i64::try_from(event_index)
                    .context("SubregistryChanged event index exceeds i64")?,
                chain_id: event.chain_id.clone().with_context(|| {
                    format!(
                        "SubregistryChanged event {} is missing chain_id",
                        event.event_identity
                    )
                })?,
                from_contract_instance_id,
                target_address: normalize_address(&target_address),
                block_number: event.block_number.with_context(|| {
                    format!(
                        "SubregistryChanged event {} is missing block_number",
                        event.event_identity
                    )
                })?,
                block_hash: event.block_hash.clone().with_context(|| {
                    format!(
                        "SubregistryChanged event {} is missing block_hash",
                        event.event_identity
                    )
                })?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if requests.is_empty() {
        return Ok(());
    }

    let mut query = QueryBuilder::<Postgres>::new(
        r#"
        WITH requested (
            event_index,
            chain_id,
            from_contract_instance_id,
            target_address,
            block_number,
            block_hash
        ) AS (
        "#,
    );
    query.push_values(&requests, |mut row, request| {
        row.push_bind(request.event_index)
            .push_bind(&request.chain_id)
            .push_bind(request.from_contract_instance_id)
            .push_bind(&request.target_address)
            .push_bind(request.block_number)
            .push_bind(&request.block_hash);
    });
    query.push(
        r#"
        )
        SELECT
            requested.event_index,
            discovery_edges.to_contract_instance_id
        FROM requested
        JOIN discovery_edges
          ON discovery_edges.chain_id = requested.chain_id
         AND discovery_edges.edge_kind = 'subregistry'
         AND discovery_edges.from_contract_instance_id = requested.from_contract_instance_id
         AND discovery_edges.active_from_block_number = requested.block_number
         AND discovery_edges.active_from_block_hash = requested.block_hash
         AND lower(discovery_edges.provenance ->> 'to_address') = requested.target_address
        ORDER BY requested.event_index, discovery_edges.discovery_edge_id
        "#,
    );

    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .context("failed to load historical ENSv2 subregistry discovery targets")?;
    let mut targets_by_event = BTreeMap::<i64, Uuid>::new();
    for row in rows {
        let event_index = row
            .try_get::<i64, _>("event_index")
            .context("failed to read SubregistryChanged event index")?;
        let target_id = row
            .try_get::<Uuid, _>("to_contract_instance_id")
            .context("failed to read SubregistryChanged target contract instance")?;
        if targets_by_event
            .insert(event_index, target_id)
            .is_some_and(|existing| existing != target_id)
        {
            bail!(
                "multiple historical discovery targets matched SubregistryChanged event index {event_index}"
            );
        }
    }

    for (event_index, target_id) in targets_by_event {
        let event = events
            .get_mut(usize::try_from(event_index).context("negative event index")?)
            .context("historical discovery target returned an unknown event index")?;
        event.after_state["to_contract_instance_id"] = Value::String(target_id.to_string());
    }

    Ok(())
}
