//! Read reverse tuples together, preserving the single-tuple claim assembly and pointer order.
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, Row};

use super::{FamilyPosition, FamilyReverseClaim, assemble_claim, probe_events};

pub(crate) type TupleKey = (String, String, String, String);
type NodeKey = (String, String, String);
type Pointer = (FamilyPosition, Option<i64>, Option<String>);

pub(crate) async fn load(
    conn: &mut PgConnection,
    keys: &[TupleKey],
) -> Result<BTreeMap<TupleKey, FamilyReverseClaim>> {
    if keys.is_empty() {
        return Ok(BTreeMap::new());
    }
    let requests: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            json!({
                "request_index": index, "chain_id": key.0, "address": key.1,
                "namespace": key.2, "coin_type": key.3,
            })
        })
        .collect();
    let tuples = sqlx::query(
        "/* storage:families.records.reverse_tuples_batch */
         SELECT request.request_index, tuple.*
         FROM jsonb_to_recordset($1::jsonb) AS request(
             request_index bigint, chain_id text, address text, namespace text, coin_type text)
         JOIN bigname_phase.project_reverse_tuple tuple
           USING (chain_id, address, namespace, coin_type)",
    )
    .bind(json!(requests))
    .fetch_all(&mut *conn)
    .await?;
    let mut nodes = BTreeSet::new();
    for tuple in &tuples {
        let key = &keys[tuple.try_get::<i64, _>("request_index")? as usize];
        if let Some(node) = tuple.try_get::<Option<String>, _>("reverse_node")? {
            nodes.insert((key.0.clone(), key.2.clone(), node));
        }
    }
    let pointers = pointers(conn, &nodes.into_iter().collect::<Vec<_>>()).await?;
    let node_requests: Vec<_> = pointers
        .iter()
        .filter_map(|(key, (_, _, resolver))| {
            resolver.as_ref().map(|resolver| {
                json!({
                    "chain_id": key.0, "namespace": key.1, "reverse_node": key.2,
                    "resolver_address": resolver,
                })
            })
        })
        .collect();
    let claims = sqlx::query(
        "/* storage:families.records.reverse_node_claims_batch */
         SELECT request.chain_id, request.namespace, request.reverse_node,
                claim.event_identity,
                EXISTS (SELECT 1 FROM bigname_phase.project_reverse_node_claim other
                        WHERE other.chain_id = request.chain_id
                          AND other.namespace = request.namespace
                          AND other.reverse_node = request.reverse_node) AS other_exists
         FROM jsonb_to_recordset($1::jsonb) AS request(
             chain_id text, namespace text, reverse_node text, resolver_address text)
         LEFT JOIN bigname_phase.project_reverse_node_claim claim
           USING (chain_id, namespace, reverse_node, resolver_address)",
    )
    .bind(json!(node_requests))
    .fetch_all(&mut *conn)
    .await?;
    let mut node_claims = BTreeMap::new();
    for row in claims {
        let key: NodeKey = (
            row.try_get("chain_id")?,
            row.try_get("namespace")?,
            row.try_get("reverse_node")?,
        );
        node_claims.insert(
            key,
            (
                row.try_get::<Option<String>, _>("event_identity")?,
                row.try_get::<bool, _>("other_exists")?,
            ),
        );
    }
    let mut selected = Vec::new();
    let mut identities = BTreeSet::new();
    for tuple in tuples {
        let key = keys[tuple.try_get::<i64, _>("request_index")? as usize].clone();
        let Some(reverse) = tuple
            .try_get::<Option<Value>, _>("reverse_position")?
            .as_ref()
            .and_then(FamilyPosition::from_json)
        else {
            continue;
        };
        let node: Option<String> = tuple.try_get("reverse_node")?;
        let node_key = node.map(|node| (key.0.clone(), key.2.clone(), node));
        let pointer = node_key.as_ref().and_then(|key| pointers.get(key)).cloned();
        let (claim, other) = if tuple
            .try_get::<Option<String>, _>("source_event")?
            .as_deref()
            == Some("ReverseClaimed")
        {
            node_key
                .as_ref()
                .and_then(|key| node_claims.get(key))
                .cloned()
                .unwrap_or_default()
        } else {
            (tuple.try_get("claim_event_identity")?, false)
        };
        identities.insert(reverse.event_identity.clone());
        identities.extend(claim.clone());
        if let Some((position, None, _)) = &pointer {
            identities.insert(position.event_identity.clone());
        }
        selected.push((key, tuple, reverse, pointer, claim, other));
    }
    let identities = identities.into_iter().collect::<Vec<_>>();
    let events = probe_events(conn, &identities).await?;
    let rows = sqlx::query(
        "/* storage:families.records.reverse_normalizations_batch */
         SELECT chain_id, claim_event_identity, status, normalized_name
         FROM bigname_phase.project_claim_normalization
         WHERE claim_event_identity = ANY($1::text[]) AND chain_id = ANY($2::text[])",
    )
    .bind(&identities)
    .bind(
        keys.iter()
            .map(|key| key.0.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut normalizations = BTreeMap::new();
    for row in rows {
        let key: (String, String) = (
            row.try_get("chain_id")?,
            row.try_get("claim_event_identity")?,
        );
        normalizations.insert(key, row);
    }
    let mut out = BTreeMap::new();
    for (key, tuple, reverse, pointer, claim, other) in selected {
        let normalization = claim
            .as_ref()
            .and_then(|claim| normalizations.get(&(key.0.clone(), claim.clone())));
        let pointer = pointer.map(|(position, id, resolver)| {
            (
                id.or_else(|| {
                    events
                        .get(&position.event_identity)
                        .map(|event| event.normalized_event_id)
                }),
                resolver,
            )
        });
        let result = assemble_claim(
            &key.0,
            &key.1,
            &key.2,
            &key.3,
            &tuple,
            &reverse,
            pointer,
            claim.as_deref(),
            normalization,
            &events,
            claim.is_none() && other,
        )?;
        out.insert(key, result);
    }
    Ok(out)
}

async fn pointers(conn: &mut PgConnection, keys: &[NodeKey]) -> Result<BTreeMap<NodeKey, Pointer>> {
    let requests: Vec<_> = keys.iter().enumerate().map(|(index, key)| json!({
        "request_index": index, "chain_id": key.0, "namespace": key.1, "node": key.2.to_ascii_lowercase(),
    })).collect();
    let rows = sqlx::query(
        "/* storage:families.records.reverse_pointers_batch */
         SELECT request.request_index, pointer.*
         FROM jsonb_to_recordset($1::jsonb) AS request(
             request_index bigint, chain_id text, namespace text, node text)
         CROSS JOIN LATERAL (
             SELECT block_number, transaction_index, log_index, event_identity,
                    normalized_event_id, NULLIF(resolver_address, '') AS resolver_address,
                    NULL::jsonb AS pointer_position
             FROM bigname_phase.project_registry_pointer
             WHERE chain_id = request.chain_id AND namespace = request.namespace AND node = request.node
             UNION ALL
             SELECT block_number, transaction_index, log_index, event_identity,
                    normalized_event_id, resolver_address, pointer_position
             FROM bigname_phase.project_resource_pointer
             WHERE chain_id = request.chain_id AND namespace = request.namespace AND namehash = request.node
               AND pointer_position IS NOT NULL
         ) pointer",
    ).bind(json!(requests)).fetch_all(&mut *conn).await.context("failed to batch reverse pointers")?;
    let mut out: BTreeMap<NodeKey, Pointer> = BTreeMap::new();
    for row in rows {
        let key = keys[row.try_get::<i64, _>("request_index")? as usize].clone();
        let own = FamilyPosition::from_row(&row)?;
        let pointer = row
            .try_get::<Option<Value>, _>("pointer_position")?
            .as_ref()
            .and_then(FamilyPosition::from_json);
        let (position, id) = match pointer {
            Some(pointer) if pointer.event_identity != own.event_identity => (pointer, None),
            _ => (own, row.try_get("normalized_event_id")?),
        };
        if out.get(&key).is_none_or(|(at, _, _)| position > *at) {
            out.insert(key, (position, id, row.try_get("resolver_address")?));
        }
    }
    Ok(out)
}
