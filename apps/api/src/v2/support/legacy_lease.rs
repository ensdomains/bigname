//! A `.eth` second-level ENSv1 name whose stored registration does not carry its BaseRegistrar
//! lease serves, on name detail and the records route, the lease its own registrar events prove.
//! The fill runs at read time on the request's snapshot and persists nothing.
//!
//! Two composed shapes are filled ([`legacy_lease_shape`]):
//!
//! - A registration that is registry-only (docs/glossary.md#registry-only-handoff), or has no
//!   authority kind, with no lease resource and no lease expiry. The latest lease among the
//!   name's activated registrar events at or below the snapshot becomes the registration's
//!   resource and identity. Its newest expiry is the lease expiry, and also the registration's
//!   expiry and grace end when those are null. The registration time is the block time of the
//!   name's first current-registry record, else of the lease's earliest event. A lease whose
//!   latest event is a release is served released at that event's block. Events of an older
//!   lease are left out.
//! - A registrar-selected registration whose lease is known but has no registration time. It is
//!   dated at the block of the lease's earliest activated grant at or below the snapshot, and the
//!   lease becomes its identity.
//!
//! A name with no such events is served as composed. The fill stands until the stored
//! registration carries the lease, which the shape check then no longer matches.
use serde_json::{Map, Value};
use sqlx::{PgConnection, Row};

use super::*;

/// The `.eth` registrar grace period the lifecycle keys `grace_ends_at` on.
const GRACE_SECONDS: i64 = 90 * 24 * 60 * 60;

/// A registrar lifecycle event of the name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LeaseEvent {
    pub(crate) kind: String,
    pub(crate) block_number: i64,
    pub(crate) resource_id: String,
    pub(crate) expiry: Option<i64>,
}

/// The shape of a composed row this read heals.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LegacyLeaseShape {
    /// A registry-only (or ownerless, authority-less) registration with no lease resource and no
    /// lease expiry: the lease is read from the name's registrar events.
    Unleased,
    /// A registrar registration whose lease is known but undated: `registered_at` is the lease's
    /// own grant block.
    UndatedLease(String),
}

/// Which shape, if any, a composed row is: an `ens_v1` `.eth` second-level name whose
/// registration is registry-only (or has no authority kind) with no lease resource and no lease
/// expiry, or registrar-selected with a lease resource and no registration time.
pub(crate) fn legacy_lease_shape(
    namespace: &str,
    normalized_name: &str,
    provenance: &Value,
    declared_summary: &Value,
) -> Option<LegacyLeaseShape> {
    if namespace != "ens" || !is_eth_second_level(normalized_name) {
        return None;
    }
    if provenance
        .pointer("/authority_selection/authority_arm")
        .and_then(Value::as_str)
        != Some("ens_v1")
    {
        return None;
    }
    let registration = declared_summary
        .get("registration")
        .and_then(Value::as_object)?;
    let resource_id = registration.get("resource_id").and_then(Value::as_str);
    match registration.get("authority_kind").and_then(Value::as_str) {
        Some("registry_only") | None
            if resource_id.is_none()
                && registration.get("ens_v1_expiry").is_none_or(Value::is_null) =>
        {
            Some(LegacyLeaseShape::Unleased)
        }
        Some("registrar") if registration.get("registered_at").is_none_or(Value::is_null) => {
            resource_id.map(|lease| LegacyLeaseShape::UndatedLease(lease.to_owned()))
        }
        _ => None,
    }
}

fn is_eth_second_level(name: &str) -> bool {
    name.strip_suffix(".eth")
        .is_some_and(|label| !label.is_empty() && !label.contains('.'))
}

/// Dates an undated registrar registration at its lease's grant block (`grant_block`, the lease's
/// earliest grant at or before the snapshot) and names the lease as its identity. Returns whether
/// anything was written.
pub(crate) fn date_registration(
    registration: &mut Map<String, Value>,
    lease: &str,
    grant_block: Option<i64>,
    block_times: &BTreeMap<i64, i64>,
) -> bool {
    let Some(seconds) = grant_block.and_then(|block| block_times.get(&block)) else {
        return false;
    };
    registration.insert("registered_at".into(), Value::String(seconds.to_string()));
    registration.insert(
        "identity_resource_id".into(),
        Value::String(lease.to_owned()),
    );
    true
}

/// Writes the lease `events` (newest first) prove into `registration`: the latest event's lease is
/// the registration's resource and identity, its newest expiry the lease expiry (and the
/// registration's expiry and grace end when those are null), the handoff block's time (else the
/// lease's first event's) the registration time, and a lease whose latest event is a release is
/// released at that event's block. Events of an older lease are left out. Returns whether anything
/// was written.
pub(crate) fn heal_registration(
    registration: &mut Map<String, Value>,
    events: &[LeaseEvent],
    handoff_block: Option<i64>,
    block_times: &BTreeMap<i64, i64>,
) -> bool {
    let Some(latest) = events.first() else {
        return false;
    };
    let lease: Vec<&LeaseEvent> = events
        .iter()
        .filter(|event| event.resource_id == latest.resource_id)
        .collect();
    let seconds = |block: Option<i64>| {
        block
            .and_then(|block| block_times.get(&block))
            .map(|seconds| Value::String(seconds.to_string()))
    };
    registration.insert(
        "resource_id".into(),
        Value::String(latest.resource_id.clone()),
    );
    registration.insert(
        "identity_resource_id".into(),
        Value::String(latest.resource_id.clone()),
    );
    registration.insert(
        "latest_event_kind".into(),
        Value::String(latest.kind.clone()),
    );
    if let Some(expiry) = lease.iter().find_map(|event| event.expiry) {
        registration.insert("ens_v1_expiry".into(), Value::String(expiry.to_string()));
        if registration.get("expiry").is_none_or(Value::is_null) {
            registration.insert("expiry".into(), Value::String(expiry.to_string()));
            registration.insert(
                "grace_ends_at".into(),
                Value::String((expiry + GRACE_SECONDS).to_string()),
            );
        }
    }
    let start = handoff_block.or_else(|| lease.iter().map(|event| event.block_number).min());
    if let Some(registered_at) = seconds(start) {
        registration.insert("registered_at".into(), registered_at);
    }
    if latest.kind == "RegistrationReleased" {
        registration.insert("status".into(), Value::String("released".into()));
        registration.insert("lifecycle_status".into(), Value::String("released".into()));
        registration.insert(
            "released_at".into(),
            seconds(Some(latest.block_number)).unwrap_or(Value::Null),
        );
    }
    true
}

/// Heals `row`'s registration on the snapshot's position on its chain: the unleased shape from the
/// name's registrar events, the undated shape from its lease's grant. Returns whether the
/// registration changed.
pub(crate) async fn serve_legacy_lease(
    conn: &mut PgConnection,
    row: &mut NameCurrentRow,
    selected_snapshot: &SelectedSnapshot,
) -> Result<bool> {
    let Some(shape) = legacy_lease_shape(
        &row.namespace,
        &row.normalized_name,
        &row.provenance,
        &row.declared_summary,
    ) else {
        return Ok(false);
    };
    let Some(chain_id) = row_chain_id(row) else {
        return Ok(false);
    };
    let Some(bound) = selected_snapshot
        .chain_positions
        .as_map()
        .values()
        .find(|position| position.chain_id == chain_id)
        .map(|position| position.block_number)
    else {
        return Ok(false);
    };
    match shape {
        LegacyLeaseShape::Unleased => {
            let events =
                load_lease_events(&mut *conn, &chain_id, &row.logical_name_id, bound).await?;
            if events.is_empty() {
                return Ok(false);
            }
            let handoff =
                bigname_storage::name_current_registry_handoff_block_number(&row.provenance);
            let mut blocks: Vec<i64> = events.iter().map(|event| event.block_number).collect();
            blocks.extend(handoff);
            let times = load_block_times(&mut *conn, &chain_id, &blocks).await?;
            let Some(registration) = row
                .declared_summary
                .get_mut("registration")
                .and_then(Value::as_object_mut)
            else {
                return Ok(false);
            };
            Ok(heal_registration(registration, &events, handoff, &times))
        }
        LegacyLeaseShape::UndatedLease(lease) => {
            let Some(grant_block) = load_grant_block(&mut *conn, &lease, bound).await? else {
                return Ok(false);
            };
            let times = load_block_times(&mut *conn, &chain_id, &[grant_block]).await?;
            let Some(registration) = row
                .declared_summary
                .get_mut("registration")
                .and_then(Value::as_object_mut)
            else {
                return Ok(false);
            };
            Ok(date_registration(
                registration,
                &lease,
                Some(grant_block),
                &times,
            ))
        }
    }
}

/// The block of the lease's earliest activated grant at or before `bound`, by
/// `normalized_events_history_registrar_lease_idx`.
async fn load_grant_block(conn: &mut PgConnection, lease: &str, bound: i64) -> Result<Option<i64>> {
    let lease: sqlx::types::Uuid = lease.parse()?;
    let row = sqlx::query(
        "/* api:v2.support.legacy_lease.grant_block */
         SELECT min(block_number) AS block_number
         FROM bigname_phase.normalized_events
         WHERE resource_id = $1
           AND source_family = 'ens_v1_registrar_l1'
           AND canonicality_state <> 'orphaned'
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
           AND consumer_visibility = 'activated'
           AND event_kind = 'RegistrationGranted'
           AND block_number <= $2",
    )
    .bind(lease)
    .bind(bound)
    .fetch_one(conn)
    .await?;
    Ok(row.get::<Option<i64>, _>("block_number"))
}

/// The name's activated registrar lifecycle events at or before `bound`, newest first, by
/// `normalized_events_name_history_idx`.
async fn load_lease_events(
    conn: &mut PgConnection,
    chain_id: &str,
    logical_name_id: &str,
    bound: i64,
) -> Result<Vec<LeaseEvent>> {
    let rows = sqlx::query(
        "/* api:v2.support.legacy_lease.events */
         SELECT event_kind, block_number, resource_id::text AS resource_id,
                after_state ->> 'expiry' AS expiry
         FROM bigname_phase.normalized_events
         WHERE logical_name_id = $1
           AND chain_id = $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
           AND source_family = 'ens_v1_registrar_l1'
           AND consumer_visibility = 'activated'
           AND resource_id IS NOT NULL
           AND block_number <= $3
           AND event_kind IN ('RegistrationGranted', 'RegistrationRenewed',
                              'RegistrationReleased', 'ExpiryChanged')
         ORDER BY block_number DESC, transaction_index DESC NULLS LAST,
                  log_index DESC NULLS LAST, normalized_event_id DESC
         LIMIT 256",
    )
    .bind(logical_name_id)
    .bind(chain_id)
    .bind(bound)
    .fetch_all(conn)
    .await?;
    Ok(rows
        .iter()
        .map(|row| LeaseEvent {
            kind: row.get::<String, _>("event_kind"),
            block_number: row.get::<i64, _>("block_number"),
            resource_id: row.get::<String, _>("resource_id"),
            expiry: row
                .get::<Option<String>, _>("expiry")
                .and_then(|expiry| expiry.parse::<i64>().ok()),
        })
        .collect())
}

/// The readable block times of `blocks` on `chain_id`, as Unix seconds.
async fn load_block_times(
    conn: &mut PgConnection,
    chain_id: &str,
    blocks: &[i64],
) -> Result<BTreeMap<i64, i64>> {
    let rows = sqlx::query(
        "/* api:v2.support.legacy_lease.block_times */
         SELECT block_number, floor(extract(epoch FROM block_timestamp))::bigint AS seconds
         FROM bigname_phase.chain_lineage
         WHERE chain_id = $1
           AND block_number = ANY($2)
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(blocks)
    .fetch_all(conn)
    .await?;
    Ok(rows
        .iter()
        .map(|row| {
            (
                row.get::<i64, _>("block_number"),
                row.get::<i64, _>("seconds"),
            )
        })
        .collect())
}

#[cfg(test)]
#[path = "legacy_lease_tests.rs"]
mod tests;
