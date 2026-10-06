//! Select a fallback-handoff winner over all eligible copies, independent of the public cursor.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgConnection, Postgres, QueryBuilder};

use super::{
    AddressRead, Witness,
    matches::Membership,
    seams::{self, Live},
    source,
};
use crate::history::EventHistoryReadFilter;

const MARKER: &str = ":ResolverChanged:registry-fallback-handoff:";

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
struct Group {
    chain: Option<String>,
    block: Option<i64>,
    hash: Option<String>,
    node: Option<String>,
    origin: String,
}
impl Group {
    fn of(row: &Witness) -> Option<Self> {
        let (origin, _) = row.event_identity.split_once(MARKER)?;
        Some(Self {
            chain: row.chain_id.clone(),
            block: row.block_number,
            hash: row.block_hash.clone(),
            node: row.node.clone(),
            origin: origin.to_owned(),
        })
    }
    fn json(&self) -> Value {
        json!({"chain":self.chain,"block":self.block,"hash":self.hash,"node":self.node,"origin":self.origin})
    }
}

pub(super) async fn retain_winners(
    connection: &mut PgConnection,
    read: &AddressRead<'_>,
    filter: &EventHistoryReadFilter,
    membership: &mut Membership,
    rows: &[Witness],
    matched: &mut [bool],
) -> Result<()> {
    let groups: BTreeSet<_> = rows
        .iter()
        .zip(matched.iter())
        .filter(|(_, matched)| **matched)
        .filter_map(|(row, _)| Group::of(row))
        .collect();
    if groups.is_empty() {
        return Ok(());
    }
    let input = Value::Array(groups.iter().map(Group::json).collect());
    let _groups = Live::new("handoff_groups", groups.len());
    let mut query =
        QueryBuilder::<Postgres>::new("DECLARE address_history_peers NO SCROLL CURSOR FOR ");
    source::push_handoff_query(&mut query, read, filter, &input);
    query
        .build()
        .persistent(false)
        .execute(&mut *connection)
        .await
        .context("failed to open address-history handoff peer cursor")?;
    let mut winners = BTreeMap::<Group, String>::new();
    loop {
        let peers = sqlx::query_as::<_, Witness>(&format!(
            "FETCH FORWARD {} FROM address_history_peers",
            seams::batch_size()
        ))
        .fetch_all(&mut *connection)
        .await?;
        let _live = Live::new("handoff_peers", peers.len());
        seams::count("handoff_batches", 1);
        let terminal = peers.len() < seams::batch_size();
        let valid = membership.validate(connection, read, &peers, None).await?;
        for (peer, valid) in peers.iter().zip(valid) {
            if !valid {
                continue;
            }
            let group = Group::of(peer).context("handoff peer missing origin")?;
            let winner = winners
                .entry(group)
                .or_insert_with(|| peer.event_identity.clone());
            if peer.event_identity < *winner {
                *winner = peer.event_identity.clone();
            }
        }
        if terminal {
            break;
        }
    }
    sqlx::query("CLOSE address_history_peers")
        .execute(&mut *connection)
        .await?;
    for (row, valid) in rows.iter().zip(matched) {
        if let Some(group) = Group::of(row) {
            *valid &= winners.get(&group) == Some(&row.event_identity);
        }
    }
    Ok(())
}
