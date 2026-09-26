//! The reverse claim of one (address, coin type, namespace) tuple over F12 (primary_names.rs,
//! `BUILD_PRIMARY_NAMES`): the tuple's latest `ReverseChanged`, the reverse node's current resolver
//! from the registry-node pointer (F4) or a resource pointer (F5) at that node, and the claim a
//! `ReverseClaimed` tuple selects through the node, else the tuple's direct claim, with the claim's
//! stored normalization. Hydration stays with the served rows (D11), so the result is the
//! pre-hydration claim.
use anyhow::{Context, Result};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Row};

use super::{FamilyPosition, facts::probe_events, payload::strip_nulls};
use crate::{PrimaryNameClaimStatus, PrimaryNameCurrentRow, PrimaryNameCurrentSnapshot};

/// A family reverse claim and whether the families could represent it.
#[derive(Clone, Debug)]
pub struct FamilyReverseClaim {
    pub snapshot: PrimaryNameCurrentSnapshot,
    /// The node's latest name record or version change was written at another resolver than the
    /// node's current one. Today's reader then takes the latest record at the current resolver,
    /// which the node claim family, one row per node, does not keep.
    pub node_claim_at_other_resolver: bool,
}

/// The reverse claim of the tuple, `None` when the tuple has no `ReverseChanged`.
pub async fn load_family_reverse_claim(
    pool: &PgPool,
    chain_id: &str,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> Result<Option<FamilyReverseClaim>> {
    let address = address.to_ascii_lowercase();
    let tuple = sqlx::query(
        "SELECT reverse_node, source_event, claim_provenance, reverse_position,
                claim_event_identity
         FROM bigname_phase.project_reverse_tuple
         WHERE address = $1 AND coin_type = $2 AND namespace = $3 AND chain_id = $4",
    )
    .bind(&address)
    .bind(coin_type)
    .bind(namespace)
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .context("failed to load the family reverse tuple")?;
    let Some(tuple) = tuple else {
        return Ok(None);
    };
    let Some(reverse) = tuple
        .try_get::<Option<Value>, _>("reverse_position")?
        .as_ref()
        .and_then(FamilyPosition::from_json)
    else {
        return Ok(None);
    };
    let reverse_node: Option<String> = tuple.try_get("reverse_node")?;
    let node_claimed = tuple
        .try_get::<Option<String>, _>("source_event")?
        .as_deref()
        == Some("ReverseClaimed");
    let pointer = match &reverse_node {
        Some(node) => node_pointer(pool, chain_id, namespace, node).await?,
        None => None,
    };

    // The selected claim: its event identity, stored name and normalization row.
    let mut node_claim_at_other_resolver = false;
    let claim_identity: Option<String> = if node_claimed {
        match (&reverse_node, &pointer) {
            (Some(node), Some((_, Some(resolver)))) => {
                // The claim at the node's current resolver. While the family keeps one row per
                // node, a row at another resolver means the claim today's reader serves is lost.
                let claim = sqlx::query(
                    "SELECT event_identity
                     FROM bigname_phase.project_reverse_node_claim
                     WHERE namespace = $1 AND reverse_node = $2 AND chain_id = $3
                       AND resolver_address = $4",
                )
                .bind(namespace)
                .bind(node)
                .bind(chain_id)
                .bind(resolver)
                .fetch_optional(pool)
                .await
                .context("failed to load the family reverse node claim")?;
                match claim {
                    Some(claim) => Some(claim.try_get("event_identity")?),
                    None => {
                        node_claim_at_other_resolver = sqlx::query_scalar(
                            "SELECT EXISTS (
                                 SELECT 1 FROM bigname_phase.project_reverse_node_claim
                                 WHERE namespace = $1 AND reverse_node = $2 AND chain_id = $3
                             )",
                        )
                        .bind(namespace)
                        .bind(node)
                        .bind(chain_id)
                        .fetch_one(pool)
                        .await
                        .context("failed to probe the family reverse node claims")?;
                        None
                    }
                }
            }
            _ => None,
        }
    } else {
        tuple.try_get("claim_event_identity")?
    };

    let mut identities = vec![reverse.event_identity.clone()];
    identities.extend(claim_identity.clone());
    let probed = probe_events(pool, &identities).await?;
    let claim = claim_identity
        .as_ref()
        .and_then(|identity| probed.get(identity));
    let normalization = match &claim_identity {
        Some(identity) => sqlx::query(
            "SELECT status, normalized_name FROM bigname_phase.project_claim_normalization
             WHERE chain_id = $1 AND claim_event_identity = $2",
        )
        .bind(chain_id)
        .bind(identity)
        .fetch_optional(pool)
        .await
        .context("failed to load the family claim normalization")?,
        None => None,
    };
    let status: String = match &normalization {
        Some(row) => row.try_get("status")?,
        None => "not_found".to_owned(),
    };
    let normalized_name: Option<String> = match &normalization {
        Some(row) => row.try_get("normalized_name")?,
        None => None,
    };
    let raw_name = claim
        .and_then(|claim| claim.after_state.get("raw_name"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let raw_claim_name = match status.as_str() {
        "success" | "invalid_name" => raw_name.clone(),
        _ => None,
    };
    let claim_name_is_normalized = status == "success"
        && normalized_name.is_some()
        && normalized_name.as_deref() == raw_name.as_deref();

    // COALESCE(the claim's own claim provenance, the ReverseChanged's, {}).
    let reverse_provenance: Option<Value> = tuple.try_get("claim_provenance")?;
    let base = claim
        .and_then(|claim| {
            claim
                .after_state
                .pointer("/primary_claim_source/claim_provenance")
        })
        .filter(|value| !value.is_null())
        .cloned()
        .or(reverse_provenance.filter(|value| !value.is_null()))
        .unwrap_or_else(|| json!({}));
    let mut provenance = match base {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    let stamped = strip_nulls(json!({
        "chain_id": chain_id,
        "reverse_event_id": probed.get(&reverse.event_identity)
            .map(|event| event.normalized_event_id),
        "claim_event_id": claim.map(|claim| claim.normalized_event_id),
        "resolver_event_id": pointer.as_ref().and_then(|(id, _)| *id),
        "reverse_node": reverse_node,
        "resolver_address": pointer.as_ref().and_then(|(_, resolver)| resolver.clone()),
        "coverage": {"status": "projected", "exhaustiveness": "not_asserted"},
    }));
    if let Value::Object(stamped) = stamped {
        provenance.extend(stamped);
    }
    let claim_status = match status.as_str() {
        "success" => PrimaryNameClaimStatus::Success,
        "unsupported" => PrimaryNameClaimStatus::Unsupported,
        "invalid_name" => PrimaryNameClaimStatus::InvalidName,
        _ => PrimaryNameClaimStatus::NotFound,
    };
    Ok(Some(FamilyReverseClaim {
        snapshot: PrimaryNameCurrentSnapshot {
            normalized_claim_name: crate::normalized_claim_name(
                claim_status,
                claim_name_is_normalized,
                raw_claim_name.as_deref(),
            ),
            row: PrimaryNameCurrentRow {
                address,
                namespace: namespace.to_owned(),
                coin_type: coin_type.to_owned(),
                claim_status,
                raw_claim_name,
                claim_provenance: Value::Object(provenance),
            },
            claim_name_is_normalized,
        },
        node_claim_at_other_resolver,
    }))
}

/// The reverse node's latest `ResolverChanged` by the family's derived node keys, clears
/// included: the F4 row keyed to the node (child first, `pointer_node` in
/// crates/project/src/families/keys.rs) or an F5 row whose pointer names the node, whichever is
/// later. That is not necessarily the pointer today's raw `after_state ->> 'node'` predicate
/// selects: a state-derived ENSv1 `ResolverChanged` with the parent in `node` and the reverse
/// node in `child_node` is keyed here and skipped there, a declared difference pinned in
/// crates/project/tests/primary_names_reverse_node/reclaim_after_unwrap.rs. Returns its event id
/// and resolver.
async fn node_pointer(
    pool: &PgPool,
    chain_id: &str,
    namespace: &str,
    node: &str,
) -> Result<Option<(Option<i64>, Option<String>)>> {
    let rows = sqlx::query(
        "SELECT block_number, transaction_index, log_index, event_identity,
                normalized_event_id, NULLIF(resolver_address, '') AS resolver_address,
                NULL::jsonb AS pointer_position
         FROM bigname_phase.project_registry_pointer
         WHERE chain_id = $1 AND namespace = $2 AND node = $3
         UNION ALL
         SELECT block_number, transaction_index, log_index, event_identity,
                normalized_event_id, resolver_address, pointer_position
         FROM bigname_phase.project_resource_pointer
         WHERE chain_id = $1 AND namespace = $2 AND namehash = $3
           AND pointer_position IS NOT NULL",
    )
    .bind(chain_id)
    .bind(namespace)
    .bind(node.to_ascii_lowercase())
    .fetch_all(pool)
    .await
    .context("failed to load the family pointers of a reverse node")?;
    let mut latest: Option<(FamilyPosition, Option<i64>, Option<String>)> = None;
    let mut unowned = Vec::new();
    for row in rows {
        let own = FamilyPosition::from_row(&row)?;
        let pointer = row
            .try_get::<Option<Value>, _>("pointer_position")?
            .as_ref()
            .and_then(FamilyPosition::from_json);
        // An F5 row whose last writer is a version change names its pointer only by position.
        let (position, id) = match pointer {
            Some(pointer) if pointer.event_identity != own.event_identity => {
                unowned.push(pointer.event_identity.clone());
                (pointer, None)
            }
            _ => (own, row.try_get("normalized_event_id")?),
        };
        let resolver: Option<String> = row.try_get("resolver_address")?;
        if latest.as_ref().is_none_or(|(at, _, _)| position > *at) {
            latest = Some((position, id, resolver));
        }
    }
    let Some((position, id, resolver)) = latest else {
        return Ok(None);
    };
    let id = match id {
        Some(id) => Some(id),
        None => probe_events(pool, &unowned)
            .await?
            .get(&position.event_identity)
            .map(|event| event.normalized_event_id),
    };
    Ok(Some((id, resolver)))
}
