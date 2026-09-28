//! Historical child registration membership, derived from each block's registration events.
//! The existing history table participates in the same before-image journal as current facts;
//! its rows survive release and undo with the block that registered the child.
use std::collections::BTreeMap;

use alloy_primitives::{B256, keccak256};
use serde_json::{Value, json};
use sqlx::{Postgres, QueryBuilder, Transaction};

use super::{
    input::BlockEvent,
    reduce::{Context, Preload, key_of, load_rows},
    store::{Row, RowSet},
    tables,
};
use crate::{ProjectError, Result};

/// These parents' registration memberships are excluded from product history.
pub const EXCLUDED_CHILD_REGISTRATION_PARENTS: &[&str] = &["eth", "base.eth"];

/// Read only qualifying block events and their own readable surfaces. Ranges share this read
/// across their blocks. The parent is computed from label hashes, without a parent surface.
pub(super) async fn read<'a>(
    transaction: &mut Transaction<'_, Postgres>,
    events: impl Iterator<Item = &'a BlockEvent>,
) -> Result<BTreeMap<i64, Vec<Row>>> {
    let ids = events
        .filter(|event| {
            matches!(
                event.event_kind.as_str(),
                "RegistrationGranted" | "LabelRegistered"
            ) && event.logical_name_id.is_some()
                && !(event.after.get("state_derived") == Some(&Value::Bool(true))
                    && event.after.get("registrar_surface_snapshot") == Some(&Value::Bool(true)))
        })
        .map(|event| event.normalized_event_id)
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut query = QueryBuilder::<Postgres>::new(
        "/* project:families.child_registrations.read */ SELECT
            jsonb_build_object(
                'event_identity', event.event_identity,
                'child_logical_name_id', event.logical_name_id,
                'namespace', child.namespace,
                'chain_id', event.chain_id,
                'block_number', event.block_number,
                'block_hash', event.block_hash,
                'transaction_order_key', COALESCE(event.transaction_index, -1),
                'log_order_key', COALESCE(event.log_index, -1),
                'event_kind', event.event_kind,
                'manifest_version', event.manifest_version,
                'provenance', jsonb_build_object(
                    'normalized_event_id', event.normalized_event_id,
                    'source_family', event.source_family,
                    'derivation_kind', 'child_registration_events_rebuild'),
                'target_block_number', event.block_number,
                'target_block_hash', event.block_hash,
                'last_recomputed_at', now(), 'inserted_at', now()),
            child.labelhashes
         FROM normalized_events event
         JOIN name_surfaces child
           ON child.logical_name_id = event.logical_name_id AND child.chain_id = event.chain_id
         JOIN chain_lineage lineage
           ON lineage.chain_id = child.chain_id AND lineage.block_hash = child.block_hash
          AND lineage.block_number = child.block_number
         WHERE event.normalized_event_id = ANY(",
    );
    query.push_bind(ids);
    query.push(
        "::bigint[])
           AND child.visibility_state = 'active'
           AND child.block_number <= event.block_number
           AND child.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND cardinality(child.labelhashes) >= 2",
    );
    // Do not copy every second-level registrar grant out of Postgres just to exclude it.
    for name in EXCLUDED_CHILD_REGISTRATION_PARENTS {
        let labels = name_labelhashes(name);
        query.push(" AND NOT (cardinality(child.labelhashes) = ");
        query.push_bind(i32::try_from(labels.len() + 1).expect("excluded parent label count"));
        for (offset, label) in labels.iter().enumerate() {
            query.push(format!(" AND lower(child.labelhashes[{}]) = ", offset + 2));
            query.push_bind(format!("{label:#x}"));
        }
        query.push(")");
    }
    let read: Vec<(Value, Vec<String>)> = query
        .build_query_as()
        .fetch_all(&mut **transaction)
        .await
        .map_err(|error| {
            ProjectError::database("failed to read child registration memberships", error)
        })?;
    let excluded = EXCLUDED_CHILD_REGISTRATION_PARENTS
        .iter()
        .map(|name| namehash(&name_labelhashes(name)))
        .collect::<Vec<_>>();
    let mut by_block: BTreeMap<i64, Vec<Row>> = BTreeMap::new();
    for (value, labels) in read {
        let Some(parent) = parent_namehash(&labels).filter(|parent| !excluded.contains(parent))
        else {
            continue;
        };
        let Value::Object(mut row) = value else {
            continue;
        };
        let namespace = row["namespace"].as_str().expect("surface namespace");
        let parent = format!("{namespace}:{parent:#x}");
        row.insert("parent_logical_name_id".into(), json!(parent));
        by_block
            .entry(row["block_number"].as_i64().expect("event block"))
            .or_default()
            .push(row);
    }
    Ok(by_block)
}

fn key(row: &Row) -> Row {
    key_of(
        &tables::CHILD_REGISTRATION_EVENT,
        [
            row["parent_logical_name_id"].clone(),
            row["event_identity"].clone(),
        ],
    )
}

pub(super) fn preload(rows: &[Row], into: &mut Preload) {
    into.add(&tables::CHILD_REGISTRATION_EVENT, rows.iter().map(key));
}

pub(super) async fn apply(
    transaction: &mut Transaction<'_, Postgres>,
    context: &Context<'_>,
    events: &[BlockEvent],
    rows: &mut RowSet,
) -> Result<()> {
    let read;
    let memberships = match context.prefetched {
        Some(prefetched) => &prefetched.child_registrations,
        None => {
            read = self::read(transaction, events.iter())
                .await?
                .remove(&context.block.number)
                .unwrap_or_default();
            &read
        }
    };
    let table = &tables::CHILD_REGISTRATION_EVENT;
    load_rows(
        transaction,
        rows,
        table,
        memberships.iter().map(key).collect(),
    )
    .await?;
    for membership in memberships {
        rows.put(table, membership.clone())?;
    }
    Ok(())
}

/// Label hashes of a dotted name, leaf first, as `name_surfaces.labelhashes` stores them.
fn name_labelhashes(name: &str) -> Vec<B256> {
    name.split('.')
        .map(|label| keccak256(label.as_bytes()))
        .collect()
}

/// The namehash of the name one label above the surface whose leaf-first label hashes are given,
/// or `None` when a label hash is not a 32-byte hex word.
fn parent_namehash(labelhashes: &[String]) -> Option<B256> {
    let parent = labelhashes
        .get(1..)?
        .iter()
        .map(|labelhash| labelhash.parse::<B256>().ok())
        .collect::<Option<Vec<_>>>()?;
    (!parent.is_empty()).then(|| namehash(&parent))
}

/// ENS namehash over leaf-first label hashes.
fn namehash(labelhashes: &[B256]) -> B256 {
    labelhashes
        .iter()
        .rev()
        .fold(B256::ZERO, |node, labelhash| {
            let mut input = [0_u8; 64];
            input[..32].copy_from_slice(node.as_slice());
            input[32..].copy_from_slice(labelhash.as_slice());
            keccak256(input)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_namehash_drops_exactly_the_leaf_label() {
        let child = name_labelhashes("bob.alice.eth")
            .iter()
            .map(|labelhash| format!("{labelhash:#x}"))
            .collect::<Vec<_>>();
        assert_eq!(
            parent_namehash(&child),
            Some(namehash(&name_labelhashes("alice.eth")))
        );
        // `B256` parsing accepts upper-case hex, as stored label hashes may carry it.
        let upper = child
            .iter()
            .map(|labelhash| labelhash.to_uppercase().replace("0X", "0x"));
        assert_eq!(
            parent_namehash(&upper.collect::<Vec<_>>()),
            Some(namehash(&name_labelhashes("alice.eth")))
        );
        assert_eq!(
            parent_namehash(&child[2..]),
            None,
            "a top-level name has no parent here"
        );
        assert_eq!(
            parent_namehash(&["0x01".to_owned(), "0x02".to_owned()]),
            None
        );
    }

    #[test]
    fn namehash_matches_the_ens_definition() {
        // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
        // derives a subnode as keccak256(node, label); `eth` is the well-known constant below.
        assert_eq!(
            format!("{:#x}", namehash(&name_labelhashes("eth"))),
            "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae"
        );
    }
}
