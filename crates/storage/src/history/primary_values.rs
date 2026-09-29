//! The name a primary-name event recorded, read from the evidence the claim's own transaction
//! retains.
//!
//! A `ReverseChanged` row keeps the claimed address, coin type and reverse node, but not the name.
//! The name is retained on a sibling resolver record write:
//!
//! - `NameForAddrChanged` carries the name in the same log, and the adapter stores it on the
//!   `RecordChanged` of that log whose `primary_claim_source` names the claimed address and coin
//!   type (`crates/adapters/src/schema_v2/protocol/v1/reverse.rs`).
//!   (upstream: .refs/ens_v1/contracts/reverseRegistrar/StandaloneReverseRegistrar.sol:L28-L31 @ ens_v1@91c966f)
//! - `ReverseClaimed` carries no name. The reverse registrar's `setName` claims the reverse node
//!   and then calls `setName` on the resolver in the same call, which emits `NameChanged` for the
//!   reverse node later in the transaction; `claim` alone writes no name.
//!   (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74-L84 @ ens_v1@91c966f)
//!   (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123-L131 @ ens_v1@91c966f)
//!   (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L13-L29 @ ens_v1@91c966f)
//!   The recorded name is the first `NameChanged` on the claimed reverse node after the claim and
//!   before any later claim of that node in the same transaction.
//!
//! Nothing outside the claim's transaction is read, so a later name write or the address's current
//! primary name never stands in for the value an event recorded.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Row};

const READABLE: &str = "('canonical', 'safe', 'finalized')";

/// For each of `event_ids` that is a `ReverseChanged` row with a recorded name, the raw name value
/// as the resolver record write stored it: a string, or `{"encoding": "hex", "bytes": ...}` when
/// the bytes are not valid UTF-8. An event without an entry recorded no name.
pub async fn load_recorded_primary_names(
    pool: &PgPool,
    event_ids: &[i64],
) -> Result<BTreeMap<i64, Value>> {
    if event_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut snapshot = super::paging::begin_history_snapshot(pool, "primary names").await?;
    let rows = sqlx::query(&format!(
        "SELECT claim.normalized_event_id, written.name_value
         FROM bigname_phase.normalized_events claim
         CROSS JOIN LATERAL (
             SELECT COALESCE(NULLIF(write.after_state -> 'raw_name', 'null'::jsonb),
                             write.after_state -> 'raw_name_bytes') AS name_value
             FROM bigname_phase.normalized_events write
             WHERE write.chain_id = claim.chain_id
               AND write.block_number = claim.block_number
               AND write.block_hash = claim.block_hash
               AND write.transaction_hash = claim.transaction_hash
               AND write.event_kind = 'RecordChanged'
               AND write.consumer_visibility = 'activated'
               AND write.canonicality_state IN {READABLE}
               AND (
                   (claim.after_state ->> 'source_event' = 'NameForAddrChanged'
                    AND write.log_index = claim.log_index
                    AND lower(write.after_state #>> '{{primary_claim_source,address}}')
                        = lower(claim.after_state ->> 'address')
                    AND write.after_state #>> '{{primary_claim_source,coin_type}}'
                        = claim.after_state ->> 'coin_type')
                   OR
                   (claim.after_state ->> 'source_event' = 'ReverseClaimed'
                    AND write.after_state ->> 'source_event' = 'NameChanged'
                    AND write.after_state ->> 'record_key' = 'name'
                    AND lower(write.after_state ->> 'node')
                        = lower(claim.after_state ->> 'reverse_node')
                    AND write.log_index > claim.log_index
                    AND NOT EXISTS (
                        SELECT 1 FROM bigname_phase.normalized_events later_claim
                        WHERE later_claim.chain_id = claim.chain_id
                          AND later_claim.block_number = claim.block_number
                          AND later_claim.block_hash = claim.block_hash
                          AND later_claim.transaction_hash = claim.transaction_hash
                          AND later_claim.event_kind = 'ReverseChanged'
                          AND later_claim.consumer_visibility = 'activated'
                          AND later_claim.canonicality_state IN {READABLE}
                          AND lower(later_claim.after_state ->> 'reverse_node')
                              = lower(claim.after_state ->> 'reverse_node')
                          AND later_claim.log_index > claim.log_index
                          AND later_claim.log_index < write.log_index))
               )
             ORDER BY write.log_index, write.event_identity
             LIMIT 1
         ) written
         WHERE claim.normalized_event_id = ANY($1)
           AND claim.event_kind = 'ReverseChanged'
           AND claim.transaction_hash IS NOT NULL
           AND written.name_value IS NOT NULL"
    ))
    .bind(event_ids)
    .fetch_all(&mut *snapshot)
    .await
    .context("failed to load the names primary-name events recorded")?;
    snapshot.commit().await?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("normalized_event_id")?,
                row.try_get("name_value")?,
            ))
        })
        .collect()
}
