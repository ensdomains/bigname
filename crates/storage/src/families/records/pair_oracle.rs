//! The coin-60 pairs today's record inventory row serves, rebuilt for the harness from normalized
//! events and today's served tables alone, never from the family tables.
//!
//! Today's builder (record_inventory.rs, `attributed_events` and `coin60_siblings`) pairs a served
//! `AddressChanged` value with an `AddrChanged` write one log later in the same block and
//! transaction when that write is among the events the resource's current pointer attributes to
//! it. That attribution is the union of four arms over the current pointer (its logical name,
//! node, resolver, source family and namespace; for a mirror, the substituted ENSv1 pointer):
//!
//! - named: the pointer's logical name at the pointer's resolver (effective resolver: the payload
//!   resolver, else the emitter);
//! - native: an unnamed ENSv1 or Basenames resolver write at the pointer's node and resolver,
//!   read through a registry family of its own protocol;
//! - guarded: an unnamed write at the node and resolver of an ENSv2 pointer whose resolver is
//!   classified supported and manifest-declared (ENSv1 resolver, or ENSv2 PublicResolverV2), in
//!   the declaration's family, and for an ENSv2 write in the pointer's namespace and the
//!   declaration's manifest, with that manifest admitted in the pointer's namespace;
//! - linked: a record-id write with `storage_model = resolver_record_id`, the pointer's resolver
//!   named explicitly in its payload, and the record id the resource selects (its exact link
//!   unless that selects record id 0, else the default link; `linked_records.rs`).
//!
//! The join is per resource, not per arm: a named value pairs with a node-keyed sibling, and
//! halves of different manifests pair. Only events today stages count (activated, canonical on a
//! canonical lineage, at or below the served block), matched on chain and block number, never
//! block hash. The pointer history today's provenance also lists (`attributed_event_ids`) is not
//! current and pairs nothing. The value side keeps the builder's requirements: an `addr` record of
//! selector 60 from `AddressChanged` with a log position, served by the row.
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Row};

use super::{CompatibilityPair, FamilyPosition};
use crate::RecordInventoryCurrentRow;

/// The current pointer the row was built through.
struct Pointer {
    logical_name_id: String,
    resolver_address: String,
    namehash: Option<String>,
    source_family: Option<String>,
    namespace: Option<String>,
}

fn text(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn ids(row: &RecordInventoryCurrentRow, field: &str) -> Vec<i64> {
    row.provenance
        .get(field)
        .and_then(Value::as_array)
        .map(|ids| ids.iter().filter_map(Value::as_i64).collect())
        .unwrap_or_default()
}

async fn pointer(pool: &PgPool, row: &RecordInventoryCurrentRow) -> Result<Option<Pointer>> {
    let provenance = &row.provenance;
    let Some(logical_name_id) = text(provenance, "logical_name_id") else {
        return Ok(None);
    };
    let mirror = provenance
        .get("mirror")
        .filter(|mirror| mirror.get("mirrored_resolver_address").is_some());
    let (resolver_address, namehash, pointer_event) = match mirror {
        Some(mirror) => (
            text(mirror, "mirrored_resolver_address"),
            text(mirror, "queried_node"),
            mirror
                .get("mirrored_pointer_event_id")
                .and_then(Value::as_i64),
        ),
        None => (
            text(provenance, "resolver_address"),
            None,
            provenance
                .get("resolver_pointer_event_id")
                .and_then(Value::as_i64),
        ),
    };
    let Some(resolver_address) = resolver_address else {
        return Ok(None);
    };
    let found: Option<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT event.source_family, event.namespace,
                (SELECT lower(surface.namehash) FROM bigname_phase.name_surfaces surface
                 WHERE surface.logical_name_id = $2 LIMIT 1)
         FROM (SELECT 1) one
         LEFT JOIN bigname_phase.normalized_events event ON event.normalized_event_id = $1",
    )
    .bind(pointer_event)
    .bind(&logical_name_id)
    .fetch_optional(pool)
    .await
    .context("failed to read the pointer today's row was built through")?;
    let (source_family, namespace, surface_namehash) = found.unwrap_or_default();
    Ok(Some(Pointer {
        logical_name_id,
        resolver_address: resolver_address.to_ascii_lowercase(),
        namehash: namehash
            .map(|namehash| namehash.to_ascii_lowercase())
            .or(surface_namehash),
        source_family,
        namespace,
    }))
}

/// The effective resolver of `sibling`: its payload resolver, else its emitter.
const EFFECTIVE_RESOLVER: &str = "lower(COALESCE(NULLIF(sibling.after_state ->> 'resolver', ''),
                                   NULLIF(sibling.raw_fact_ref ->> 'emitting_address', '')))";

/// The pairs today's row serves at `served_block`: each `AddressChanged` value of the row with the
/// admitted `AddrChanged` one log later in the same transaction, the order one ENSv1 `setAddr`
/// emits them in.
/// (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L59-L62 @ ens_v1@91c966f)
pub(crate) async fn expected_pairs(
    pool: &PgPool,
    chain_id: &str,
    served_block: Option<i64>,
    today: Option<&RecordInventoryCurrentRow>,
) -> Result<Vec<CompatibilityPair>> {
    let Some(today) = today else {
        return Ok(Vec::new());
    };
    let values = ids(today, "record_event_ids");
    if values.is_empty() {
        return Ok(Vec::new());
    }
    let Some(pointer) = pointer(pool, today).await? else {
        return Ok(Vec::new());
    };
    // Candidates are found from the value (same chain and block number, null-safe transaction,
    // next log) and then admitted through the arms, so no arm scans the chain. The sibling is one
    // of today's staged events: activated, canonical on a canonical lineage, at or below the
    // served block.
    let sql = format!(
        "WITH guard AS (
             SELECT resolver.declared_summary #>> '{{classification,source_family}}' AS family,
                    manifest.source_manifest_id AS manifest_id
             FROM bigname_phase.resolver_current resolver
             CROSS JOIN LATERAL (
                 SELECT manifest.source_manifest_id, manifest.namespace,
                        manifest.after_state ->> 'rollout_status' AS rollout_status,
                        manifest.after_state -> 'manifest_payload' AS payload
                 FROM bigname_phase.normalized_events manifest
                 LEFT JOIN bigname_phase.chain_lineage lineage
                   ON lineage.chain_id = manifest.chain_id
                  AND lineage.block_hash = manifest.block_hash
                  AND lineage.block_number = manifest.block_number
                 WHERE manifest.event_kind = 'SourceManifestUpdated'
                   AND manifest.source_manifest_id =
                       (resolver.provenance ->> 'manifest_id')::bigint
                   AND (manifest.chain_id = $2
                        OR ($2 = 'base-mainnet' AND manifest.namespace = 'basenames'
                            AND manifest.source_family = 'basenames_execution'
                            AND manifest.chain_id = 'ethereum-mainnet'))
                   AND manifest.canonicality_state IN ('canonical', 'safe', 'finalized')
                   AND (manifest.block_hash IS NULL
                        OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
                   AND (manifest.block_number IS NULL OR $8::bigint IS NULL
                        OR manifest.block_number <= $8)
                 ORDER BY manifest.normalized_event_id DESC
                 LIMIT 1
             ) manifest
             WHERE resolver.chain_id = $2 AND resolver.resolver_address = $4
               AND resolver.support_status = 'supported'
               AND (resolver.declared_summary #>> '{{classification,source_family}}' =
                        'ens_v1_resolver_l1'
                    OR (resolver.declared_summary #>> '{{classification,source_family}}' =
                            'ens_v2_resolver_l1'
                        AND resolver.declared_summary #>> '{{classification,role}}' =
                            'public_resolver_v2'))
               AND resolver.declared_summary #>> '{{classification,basis}}' =
                   'manifest_declared_address'
               AND manifest.rollout_status = 'active' AND manifest.payload IS NOT NULL
               AND manifest.namespace = $7::text
         ),
         -- The exact link unless it selects record id 0, else the default link; provenance
         -- lists the default link only in that case.
         selected AS (
             SELECT COALESCE(
                 (SELECT link.after_state ->> 'resolver_record_id'
                  FROM bigname_phase.normalized_events link
                  WHERE link.normalized_event_id = ANY($9::bigint[])
                    AND link.event_kind = 'ResolverRecordLinked'
                    AND lower(link.after_state ->> 'node') = $5::text
                    AND link.after_state ->> 'resolver_record_id' <> '0'
                  LIMIT 1),
                 (SELECT link.after_state ->> 'resolver_record_id'
                  FROM bigname_phase.normalized_events link
                  WHERE link.normalized_event_id = ANY($9::bigint[])
                    AND link.event_kind = 'ResolverRecordLinked'
                    AND lower(link.after_state ->> 'node') IS DISTINCT FROM $5::text
                  LIMIT 1)
             ) AS record_id
         )
         SELECT value.normalized_event_id AS value_id, value.block_number AS value_block,
                value.transaction_index AS value_transaction, value.log_index AS value_log,
                value.event_identity AS value_identity,
                sibling.normalized_event_id AS sibling_id, sibling.block_number AS sibling_block,
                sibling.transaction_index AS sibling_transaction,
                sibling.log_index AS sibling_log, sibling.event_identity AS sibling_identity
         FROM bigname_phase.normalized_events value
         JOIN bigname_phase.normalized_events sibling
           ON sibling.chain_id = value.chain_id AND sibling.block_number = value.block_number
          AND sibling.transaction_hash IS NOT DISTINCT FROM value.transaction_hash
          AND sibling.transaction_index IS NOT DISTINCT FROM value.transaction_index
          AND sibling.log_index = value.log_index + 1
         CROSS JOIN selected
         LEFT JOIN guard ON TRUE
         WHERE value.normalized_event_id = ANY($1::bigint[])
           AND value.event_kind = 'RecordChanged'
           AND value.after_state ->> 'source_event' = 'AddressChanged'
           AND value.after_state ->> 'record_key' = 'addr:60'
           AND value.after_state ->> 'record_family' = 'addr'
           AND value.after_state ->> 'selector_key' = '60'
           AND value.log_index IS NOT NULL
           AND sibling.event_kind = 'RecordChanged'
           AND sibling.after_state ->> 'record_key' = 'addr:60'
           AND sibling.after_state ->> 'source_event' = 'AddrChanged'
           AND sibling.chain_id = $2
           AND sibling.consumer_visibility = 'activated'
           AND sibling.canonicality_state IN ('canonical', 'safe', 'finalized')
           AND (($8::bigint IS NULL OR sibling.block_number <= $8)
                AND EXISTS (
                    SELECT 1 FROM bigname_phase.chain_lineage lineage
                    WHERE lineage.chain_id = sibling.chain_id
                      AND lineage.block_hash = sibling.block_hash
                      AND lineage.block_number = sibling.block_number
                      AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
                ))
           AND (
               -- named
               (sibling.logical_name_id = $3 AND {EFFECTIVE_RESOLVER} = $4)
               -- native
               OR (sibling.logical_name_id IS NULL
                   AND lower(sibling.after_state ->> 'node') = $5::text
                   AND {EFFECTIVE_RESOLVER} = $4
                   AND ((sibling.source_family = 'ens_v1_resolver_l1'
                         AND $6::text IN ('ens_v1_registry_l1', 'ens_v1_registrar_l1',
                                          'ens_v1_wrapper_l1'))
                        OR (sibling.source_family = 'basenames_base_resolver'
                            AND $6::text = 'basenames_base_registry')))
               -- guarded
               OR (sibling.logical_name_id IS NULL
                   AND guard.family IS NOT NULL
                   AND sibling.source_family = guard.family
                   AND (sibling.source_family <> 'ens_v2_resolver_l1'
                        OR (sibling.namespace = $7::text
                            AND sibling.source_manifest_id = guard.manifest_id))
                   AND lower(sibling.after_state ->> 'node') = $5::text
                   AND {EFFECTIVE_RESOLVER} = $4
                   AND $6::text IN ('ens_v2_registry_l1', 'ens_v2_root_l1'))
               -- linked
               OR (lower(sibling.after_state ->> 'resolver') = $4
                   AND sibling.after_state ->> 'storage_model' = 'resolver_record_id'
                   AND sibling.after_state ->> 'resolver_record_id' = selected.record_id)
           )
         ORDER BY value.normalized_event_id, sibling.normalized_event_id"
    );
    let rows = sqlx::query(&sql)
        .bind(&values)
        .bind(chain_id)
        .bind(&pointer.logical_name_id)
        .bind(&pointer.resolver_address)
        .bind(&pointer.namehash)
        .bind(&pointer.source_family)
        .bind(&pointer.namespace)
        .bind(served_block)
        .bind(ids(today, "record_link_event_ids"))
        .fetch_all(pool)
        .await
        .context("failed to find the coin-60 pairs today's row serves")?;
    rows.iter()
        .map(|row| {
            let position = |prefix: &str| -> Result<FamilyPosition> {
                Ok(FamilyPosition {
                    block_number: row.try_get(format!("{prefix}_block").as_str())?,
                    transaction_index: row.try_get(format!("{prefix}_transaction").as_str())?,
                    log_index: row.try_get(format!("{prefix}_log").as_str())?,
                    event_identity: row.try_get(format!("{prefix}_identity").as_str())?,
                })
            };
            Ok(CompatibilityPair {
                record_key: "addr:60".to_owned(),
                value_event_id: Some(row.try_get("value_id")?),
                value_position: position("value")?,
                sibling_event_id: Some(row.try_get("sibling_id")?),
                sibling_position: position("sibling")?,
            })
        })
        .collect()
}
