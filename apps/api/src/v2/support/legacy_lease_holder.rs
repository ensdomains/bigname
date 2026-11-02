//! The owner side of the read-time lease fill (`legacy_lease`). A `.eth` second-level ENSv1
//! name the fill serves a BaseRegistrar lease for is owned by that lease's token holder, not by
//! the registry owner its stored row falls back to, and a name the fill serves as `released`
//! has no `owner` and no `manager` (docs/api-v1.md, `owner`). The registrar keeps the lease and
//! its holder apart from the registry record, and only `reclaim` writes the holder into the
//! registry (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L71-L76 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L174 @ ens_v1@91c966f).
//! So a name whose registry owner is zero keeps its lease: it serves the lease's status, its
//! holder and its registry's `authority`, and no `manager`, since a zero registry owner names no
//! account.
//!
//! The holder is read in its own statement, so a lease with more events than the fill's events
//! window still finds it. Like the rest of the fill it stands until the stored registration
//! carries the lease.
use anyhow::Result;
use serde_json::{Map, Value, json};
use sqlx::PgConnection;

use super::legacy_lease::{LeaseEvent, lease_lifecycle_status};

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// The `eth` label of a name served by its label hashes: `[keccak256("eth")]`.
const ETH_LABEL_PLACEHOLDER: &str =
    "[4f5b812789fc606be1b3b16908db13fc7a9adf7ca72641f84d75b47069d3d7f0]";

/// Whether `name` is a `.eth` second-level name, also when it is served by its label hashes.
pub(super) fn is_eth_second_level(name: &str) -> bool {
    name.split_once('.').is_some_and(|(label, parent)| {
        !label.is_empty() && matches!(parent, "eth" | ETH_LABEL_PLACEHOLDER)
    })
}

/// Whether the fill's lease, the lease of the latest event in `events` (newest first), is the
/// lease the composer's implied lease would read: the lease of the latest event it reads, a
/// registrar-kind row not derived from retained state, at or after the node's first
/// current-registry record (`handoff_block`) when there is one
/// (`crates/storage/src/families/control/lifecycle/admission.rs`, `imply_lease`, on the reconcile
/// line). Only such a lease serves its holder.
pub(super) fn is_current_lease(events: &[LeaseEvent], handoff_block: Option<i64>) -> bool {
    let Some(latest) = events.first() else {
        return false;
    };
    events
        .iter()
        .find(|event| {
            event.implies_lease && handoff_block.is_none_or(|handoff| event.block_number >= handoff)
        })
        .is_some_and(|implied| implied.resource_id == latest.resource_id)
}

/// The latest nonzero holder of a lease: a transfer's recipient, a grant's registrant or a
/// release's before-state registrant, at or below the bound and, when the node has a first
/// current-registry record, at or after that record's block. One row, by
/// `normalized_events_resource_history_idx` in its own order. Binds the lease, the bound, that
/// block or null, and the chain, in that order.
pub(crate) const LEASE_HOLDER_SQL: &str = "/* api:v2.support.legacy_lease.holder */
         SELECT lower(held.holder)
         FROM bigname_phase.normalized_events
         CROSS JOIN LATERAL (
             SELECT CASE event_kind
                        WHEN 'TokenControlTransferred' THEN after_state ->> 'to'
                        WHEN 'RegistrationReleased' THEN before_state ->> 'registrant'
                        ELSE after_state ->> 'registrant'
                    END AS holder
         ) held
         WHERE resource_id = $1
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
           AND block_number <= $2
           AND ($3::bigint IS NULL OR block_number >= $3)
           AND chain_id = $4
           AND source_family = 'ens_v1_registrar_l1'
           AND consumer_visibility = 'activated'
           AND event_kind IN ('TokenControlTransferred', 'RegistrationGranted',
                              'RegistrationReleased')
           AND lower(held.holder) <> '0x0000000000000000000000000000000000000000'
         ORDER BY block_number DESC NULLS LAST, chain_id ASC NULLS LAST,
                  block_hash DESC NULLS LAST, transaction_index DESC NULLS LAST,
                  log_index DESC NULLS LAST, event_identity DESC
         LIMIT 1";

/// The holder of `lease` ([`LEASE_HOLDER_SQL`]). Transfers before the name's first
/// current-registry record (`handoff_block`) name no holder.
pub(super) async fn load_lease_holder(
    conn: &mut PgConnection,
    chain_id: &str,
    lease: &str,
    handoff_block: Option<i64>,
    bound: i64,
) -> Result<Option<String>> {
    let lease: sqlx::types::Uuid = lease.parse()?;
    let holder = sqlx::query_scalar::<_, String>(LEASE_HOLDER_SQL)
        .bind(lease)
        .bind(bound)
        .bind(handoff_block)
        .bind(chain_id)
        .fetch_optional(conn)
        .await?;
    Ok(holder)
}

/// Serves `holder`, the filled lease's holder, on a row the fill healed, at the snapshot's block
/// time `clock`. The served status is the registration's lifecycle status. A `released` row has
/// no owner and no manager, and the lease's own release keeps the holder as the lapsed
/// registration's owner. Any other row is owned by the holder when there is one, unless its
/// lease is released under a stored term that outlives the release, which keeps the composed
/// control. An ownerless row takes the lease's status, unless the fill serves the lease's release,
/// and its arm's `authority`. Without a known clock it stays as composed. A zero registry owner is
/// served as none.
pub(super) fn serve_lease_holder(
    summary: &mut Value,
    provenance: &mut Value,
    holder: Option<&str>,
    clock: Option<i64>,
) {
    let ownerless =
        provenance.pointer("/authority_selection/ownerless_registry") == Some(&Value::Bool(true));
    let Some(registration) = summary
        .get_mut("registration")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    let latest_release = registration
        .get("latest_event_kind")
        .and_then(Value::as_str)
        == Some("RegistrationReleased");
    // The fill serves the lease's own release when no stored term outlives it
    // (`heal_registration`).
    let lease_released =
        latest_release && registration.get("status").and_then(Value::as_str) == Some("released");
    if ownerless && !lease_released {
        let expiry = registration
            .get("ens_v1_expiry")
            .and_then(Value::as_str)
            .and_then(|expiry| expiry.parse::<i64>().ok());
        let (Some(expiry), Some(clock)) = (expiry, clock) else {
            return;
        };
        registration.insert(
            "lifecycle_status".into(),
            json!(lease_lifecycle_status(expiry, clock)),
        );
    }
    if ownerless
        && let Some(selection) = provenance
            .get_mut("authority_selection")
            .and_then(Value::as_object_mut)
    {
        selection.insert("ownerless_registry".into(), json!(false));
    }
    let released = registration.get("lifecycle_status").and_then(Value::as_str) == Some("released");
    if released {
        // A released name serves no resolver or records through a retained registry pointer
        // either, which keys on the registration's own status (`released_under_a_retained_pointer`).
        registration.insert("status".into(), json!("released"));
    }
    // A release the stored term outlives leaves the row as composed: its released lease's holder
    // is not the owner.
    let holder = holder.filter(|_| !latest_release || lease_released);
    if let Some(holder) = holder {
        registration.insert("registrant".into(), json!(holder));
    }
    if lease_released {
        let released_at = registration
            .get("released_at")
            .cloned()
            .unwrap_or(Value::Null);
        registration.insert(
            "lapsed_registration".into(),
            json!({"owner": holder, "released_at": released_at, "release_kind": "expired"}),
        );
    }
    let Some(control) = summary.as_object_mut().and_then(|summary| {
        summary
            .entry("control")
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
    }) else {
        return;
    };
    let zero_registry_owner = control
        .get("registry_owner")
        .and_then(Value::as_str)
        .is_some_and(|owner| owner.eq_ignore_ascii_case(ZERO_ADDRESS));
    if ownerless || zero_registry_owner {
        control.insert("registry_owner".into(), Value::Null);
    }
    if released {
        // No owner, and no manager, which is served from the registry owner.
        for field in ["owner", "registrant", "registry_owner"] {
            control.insert(field.into(), Value::Null);
        }
    } else if let Some(holder) = holder {
        control.insert("owner".into(), json!(holder));
        control.insert("registrant".into(), json!(holder));
    }
}

#[cfg(test)]
#[path = "legacy_lease_holder_tests.rs"]
mod tests;
