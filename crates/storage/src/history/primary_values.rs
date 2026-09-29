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
//! - `ReverseClaimed` carries no name. The reverse registrar's `setNameForAddr` emits
//!   `ReverseClaimed`, has the registry write the reverse node's owner and resolver (`NewOwner`,
//!   and `NewResolver` and `NewTTL` only when they change), then calls `setName` on that resolver,
//!   which emits `NameChanged` for the reverse node.
//!   (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L74-L84 @ ens_v1@91c966f)
//!   (upstream: .refs/ens_v1/contracts/reverseRegistrar/ReverseRegistrar.sol:L123-L131 @ ens_v1@91c966f)
//!   (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L49-L58 @ ens_v1@91c966f)
//!   (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
//!   (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L174-L188 @ ens_v1@91c966f)
//!   (upstream: .refs/ens_v1/contracts/resolvers/profiles/NameResolver.sol:L13-L29 @ ens_v1@91c966f)
//!   The returned name is the first `NameChanged` on the claimed reverse node after the claim in
//!   its transaction, and only when it meets this evidence rule: it is at most four logs after
//!   the claim (the standard call's registry writes, then the name), no other claim of the node
//!   lies between, and it was emitted by the resolver the registry selected for the reverse node
//!   at that point. Otherwise none is returned, and a later write never stands in. The four-log
//!   bound fits the standard `setNameForAddr` call with a resolver that emits only `NameChanged`;
//!   extra logs before the write give no name only when they push the first `NameChanged` past
//!   the claim plus four logs. Logs carry no call boundaries, so the rule does not prove which
//!   call made the write: a separate call that writes the claim's resolver inside that span meets
//!   it too.
//!
//! No name value is taken from outside the claim's transaction, so a later name write or the
//! address's current primary name never stands in for the value an event recorded. Only the
//! resolver selection may come from an earlier block.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Row};

use super::attribution::ENS_V1_POINTER_FAMILIES;

const READABLE: &str = "('canonical', 'safe', 'finalized')";

/// For each of `event_ids` that is a `ReverseChanged` row with a recorded name, the raw name value
/// as the resolver record write stored it: a string, or `{"encoding": "hex", "bytes": ...}` when
/// the bytes are not valid UTF-8 or contain NUL. An event without an entry recorded no name the
/// retained evidence ties to it.
pub async fn load_recorded_primary_names(
    pool: &PgPool,
    event_ids: &[i64],
) -> Result<BTreeMap<i64, Value>> {
    if event_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut snapshot = super::paging::begin_history_snapshot(pool, "primary names").await?;
    let rows = sqlx::query(&format!(
        "SELECT claim.normalized_event_id,
                COALESCE(NULLIF(write.after_state -> 'raw_name', 'null'::jsonb),
                         write.after_state -> 'raw_name_bytes') AS name_value
         FROM bigname_phase.normalized_events claim
         CROSS JOIN LATERAL (
             SELECT write.after_state
             FROM bigname_phase.normalized_events write
             WHERE write.chain_id = claim.chain_id
               AND write.block_number = claim.block_number
               AND write.block_hash = claim.block_hash
               AND write.transaction_hash = claim.transaction_hash
               AND write.log_index = claim.log_index
               AND write.event_kind = 'RecordChanged'
               AND write.consumer_visibility = 'activated'
               AND write.canonicality_state IN {READABLE}
               AND lower(write.after_state #>> '{{primary_claim_source,address}}')
                   = lower(claim.after_state ->> 'address')
               AND write.after_state #>> '{{primary_claim_source,coin_type}}'
                   = claim.after_state ->> 'coin_type'
             ORDER BY write.event_identity
             LIMIT 1
         ) write
         WHERE claim.normalized_event_id = ANY($1)
           AND claim.event_kind = 'ReverseChanged'
           AND claim.after_state ->> 'source_event' = 'NameForAddrChanged'
           AND claim.transaction_hash IS NOT NULL
         UNION ALL
         SELECT claim.normalized_event_id,
                COALESCE(NULLIF(write.after_state -> 'raw_name', 'null'::jsonb),
                         write.after_state -> 'raw_name_bytes') AS name_value
         FROM bigname_phase.normalized_events claim
         CROSS JOIN LATERAL (
             SELECT write.*
             FROM bigname_phase.normalized_events write
             WHERE write.chain_id = claim.chain_id
               AND write.block_number = claim.block_number
               AND write.block_hash = claim.block_hash
               AND write.transaction_hash = claim.transaction_hash
               AND write.log_index > claim.log_index
               AND write.event_kind = 'RecordChanged'
               AND write.consumer_visibility = 'activated'
               AND write.canonicality_state IN {READABLE}
               AND write.after_state ->> 'source_event' = 'NameChanged'
               AND write.after_state ->> 'record_key' = 'name'
               AND lower(write.after_state ->> 'node') = lower(claim.after_state ->> 'reverse_node')
             ORDER BY write.log_index, write.event_identity
             LIMIT 1
         ) write
         WHERE claim.normalized_event_id = ANY($1)
           AND claim.event_kind = 'ReverseChanged'
           AND claim.after_state ->> 'source_event' = 'ReverseClaimed'
           AND claim.transaction_hash IS NOT NULL
           AND write.log_index <= claim.log_index + 4
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
                 AND later_claim.log_index < write.log_index)
           AND lower(COALESCE(NULLIF(write.after_state ->> 'resolver', ''),
                              write.raw_fact_ref ->> 'emitting_address')) = (
               SELECT lower(pointer.after_state ->> 'resolver')
               FROM bigname_phase.normalized_events pointer
               WHERE pointer.chain_id = claim.chain_id
                 AND pointer.namespace = claim.namespace
                 AND lower(COALESCE(pointer.after_state ->> 'child_node',
                                    pointer.after_state ->> 'namehash',
                                    pointer.after_state ->> 'node'))
                     = lower(claim.after_state ->> 'reverse_node')
                 AND pointer.event_kind = 'ResolverChanged'
                 AND pointer.source_family IN {ENS_V1_POINTER_FAMILIES}
                 AND COALESCE(pointer.after_state ->> 'child_node',
                              pointer.after_state ->> 'namehash',
                              pointer.after_state ->> 'node') IS NOT NULL
                 AND pointer.consumer_visibility = 'activated'
                 AND pointer.canonicality_state IN {READABLE}
                 AND pointer.block_number <= write.block_number
                 AND ROW(pointer.block_number, COALESCE(pointer.transaction_index, -1),
                         COALESCE(pointer.log_index, -1))
                     < ROW(write.block_number, COALESCE(write.transaction_index, -1),
                           COALESCE(write.log_index, -1))
               ORDER BY pointer.block_number DESC,
                        pointer.transaction_index DESC NULLS LAST,
                        pointer.log_index DESC NULLS LAST,
                        pointer.event_identity DESC
               LIMIT 1)"
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
