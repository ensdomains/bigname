use anyhow::{Context, Result, bail};
use sqlx::{PgPool, Postgres, Transaction};

const REQUIRED_REDO_PREFIX: &str = "required downstream redo: ";
#[cfg(test)]
const DISCOVERY_CAUSE_MARKER: &str = "[required-ingest-cause:discovery]";
const MANIFEST_REASON: &str = "manifest watch plan widened over an already-ingested range";
const DISCOVERY_OWNERSHIP: &str = "discovery watch admission added coverage over already-ingested blocks [required-ingest-cause:discovery]";
const MIXED_OPERATOR_REASON: &str =
    "required downstream redo: discovery demand merged with operator-owned Ingest work";

type IngestRedoRow = (Option<i64>, bool, Option<i64>, Option<i64>, Option<String>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequiredIngestCause {
    ManifestWatchWidening,
    DiscoveryWatchAdmission,
}

pub(crate) async fn install_manifest_required_ingest(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    widened_from: i64,
) -> Result<bool> {
    install_required_ingest(
        transaction,
        chain_id,
        widened_from,
        RequiredIngestCause::ManifestWatchWidening,
    )
    .await
}

impl RequiredIngestCause {
    fn reason(self) -> String {
        match self {
            Self::ManifestWatchWidening => MANIFEST_REASON.to_owned(),
            Self::DiscoveryWatchAdmission => DISCOVERY_OWNERSHIP.to_owned(),
        }
    }
}

/// Installs required Ingest work through the one `chain_phase_state` redo path.
///
/// Callers must first prove a semantic delta. Every call invalidates any active attempt,
/// including same-range demand, so repeated observations must be suppressed before this helper.
pub async fn install_required_ingest(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    widened_from: i64,
    cause: RequiredIngestCause,
) -> Result<bool> {
    let row: Option<IngestRedoRow> = sqlx::query_as(
        "SELECT current_block_number, redo_in_progress,
                redo_from_block_number, redo_to_block_number, last_error
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest'
         FOR UPDATE",
    )
    .bind(chain_id)
    .fetch_optional(&mut **transaction)
    .await
    .with_context(|| format!("failed to inspect Ingest coverage for chain {chain_id}"))?;
    let Some((Some(current), active, active_from, active_to, existing_reason)) = row else {
        return Ok(false);
    };
    let bounds: (Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT min(start_block_number),
                (SELECT latest_block_number FROM chain_heads WHERE chain_id = $1)
         FROM ingest_cursors WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| format!("failed to load ingested watch bounds for chain {chain_id}"))?;
    let Some(cursor_start) = bounds.0 else {
        return Ok(false);
    };
    let widened_from = widened_from.max(cursor_start);
    // The published head is the readable coverage boundary. A finite Ingest position may remain
    // ahead after a rewind, so it is only the fallback when no published head exists.
    let through = bounds.1.unwrap_or(current);
    if widened_from > through {
        return Ok(false);
    }
    let requested_reason = format!("{REQUIRED_REDO_PREFIX}{}", cause.reason());
    if active {
        let (Some(active_from), Some(active_to)) = (active_from, active_to) else {
            bail!("active Ingest redo for chain {chain_id} is missing its persisted range");
        };
        let from = active_from.min(widened_from);
        let to = active_to.max(through);
        let reason = merged_reason(existing_reason.as_deref(), requested_reason, cause);
        let result = sqlx::query(
            "UPDATE chain_phase_state
             SET redo_attempt_generation = redo_attempt_generation + 1,
                 redo_from_block_number = $2, redo_to_block_number = $3,
                 redo_current_block_number = NULL, redo_current_block_hash = NULL,
                 redo_target_block_number = NULL, redo_target_block_hash = NULL,
                 redo_source_boundary_markers = NULL,
                 redo_manifest_authority_fingerprint = NULL,
                 last_error = $4, updated_at = now()
             WHERE chain_id = $1 AND phase_name = 'ingest' AND redo_in_progress",
        )
        .bind(chain_id)
        .bind(from)
        .bind(to)
        .bind(reason)
        .execute(&mut **transaction)
        .await
        .with_context(|| format!("failed to widen required Ingest redo for chain {chain_id}"))?;
        if result.rows_affected() != 1 {
            bail!("active Ingest redo disappeared while widening chain {chain_id}");
        }
        return Ok(true);
    }
    let result = sqlx::query(
        "UPDATE chain_phase_state
         SET redo_attempt_generation = redo_attempt_generation + 1,
             phase_status = 'running', redo_in_progress = true, redo_mode = 'redo',
             redo_previous_phase_status = phase_status,
             redo_previous_last_error = last_error,
             redo_previous_started_at = started_at,
             redo_previous_finished_at = finished_at,
             redo_from_block_number = $2, redo_to_block_number = $3,
             redo_current_block_number = NULL, redo_current_block_hash = NULL,
             redo_target_block_number = NULL, redo_target_block_hash = NULL,
             redo_source_boundary_markers = NULL,
             redo_manifest_authority_fingerprint = NULL,
             last_error = $4, started_at = now(), finished_at = NULL, updated_at = now()
         WHERE chain_id = $1 AND phase_name = 'ingest'
           AND current_block_number IS NOT NULL AND NOT redo_in_progress",
    )
    .bind(chain_id)
    .bind(widened_from)
    .bind(through)
    .bind(requested_reason)
    .execute(&mut **transaction)
    .await
    .with_context(|| format!("failed to stamp required Ingest redo for chain {chain_id}"))?;
    if result.rows_affected() != 1 {
        bail!("Ingest coverage changed while stamping required work for chain {chain_id}");
    }
    Ok(true)
}

fn merged_reason(existing: Option<&str>, requested: String, cause: RequiredIngestCause) -> String {
    match cause {
        RequiredIngestCause::ManifestWatchWidening => requested,
        RequiredIngestCause::DiscoveryWatchAdmission => match existing {
            Some(message) if is_discovery_required_ingest(message) => requested,
            Some(message) if message.starts_with(REQUIRED_REDO_PREFIX) => message.to_owned(),
            _ => MIXED_OPERATOR_REASON.to_owned(),
        },
    }
}

pub fn is_discovery_required_ingest(message: &str) -> bool {
    let ownership = message
        .split_once("; last attempt failed: ")
        .map_or(message, |(ownership, _)| ownership);
    [REQUIRED_REDO_PREFIX, "required downstream redo active: "]
        .into_iter()
        .any(|prefix| ownership.strip_prefix(prefix) == Some(DISCOVERY_OWNERSHIP))
}

pub async fn discovery_required_ingest_pending(pool: &PgPool, chain_id: &str) -> Result<bool> {
    let message: Option<String> = sqlx::query_scalar(
        "SELECT last_error FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest' AND redo_in_progress",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .with_context(|| {
        format!("failed to inspect discovery-owned required Ingest work for chain {chain_id}")
    })?
    .flatten();
    Ok(message.as_deref().is_some_and(is_discovery_required_ingest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_ownership_keeps_operator_routing() {
        let operator = "required downstream redo: operator approved manifest backfill";
        assert_eq!(
            merged_reason(
                Some(operator),
                format!("{REQUIRED_REDO_PREFIX}{DISCOVERY_OWNERSHIP}"),
                RequiredIngestCause::DiscoveryWatchAdmission,
            ),
            operator
        );
    }

    #[test]
    fn active_operator_redo_without_a_reason_stays_operator_routed() {
        let discovery = format!("{REQUIRED_REDO_PREFIX}{DISCOVERY_OWNERSHIP}");
        let merged = merged_reason(
            None,
            discovery,
            RequiredIngestCause::DiscoveryWatchAdmission,
        );
        assert_eq!(merged, MIXED_OPERATOR_REASON);
        assert!(!is_discovery_required_ingest(&merged));
    }

    #[test]
    fn manifest_demand_overrides_discovery_auto_routing() {
        let discovery = format!(
            "required downstream redo active: {DISCOVERY_OWNERSHIP}; last attempt failed: rpc"
        );
        let manifest = format!("{REQUIRED_REDO_PREFIX}{MANIFEST_REASON}");
        assert_eq!(
            merged_reason(
                Some(&discovery),
                manifest.clone(),
                RequiredIngestCause::ManifestWatchWidening,
            ),
            manifest
        );
    }

    #[test]
    fn discovery_cause_survives_active_and_failure_text() {
        assert!(is_discovery_required_ingest(&format!(
            "required downstream redo active: {DISCOVERY_OWNERSHIP}; last attempt failed: timeout"
        )));
    }

    #[test]
    fn failure_text_cannot_claim_discovery_ownership() {
        let operator = format!(
            "required downstream redo active: {MANIFEST_REASON}; last attempt failed: provider returned {DISCOVERY_CAUSE_MARKER}"
        );
        assert!(!is_discovery_required_ingest(&operator));
    }
}
