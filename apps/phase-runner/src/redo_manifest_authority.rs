use sqlx::{Postgres, Transaction};

use crate::{
    error::{RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
};

// This matches manifest synchronization's watch-plan comparison: active payloads
// are ordered deterministically, and only the interpretation-only normalizer is
// excluded. Roots, contracts, addresses, discovery rules, and block ranges stay
// in the fingerprint.
pub(crate) const FINGERPRINT_SQL: &str = "(
    SELECT encode(
        public.digest(
            COALESCE(
                jsonb_agg(
                    manifest.manifest_payload - 'normalizer_version'
                    ORDER BY manifest.namespace, manifest.source_family
                )::text,
                '[]'
            ),
            'sha256'
        ),
        'hex'
    )
    FROM manifest_versions manifest
    WHERE manifest.chain_id = $1
      AND manifest.rollout_status = 'active'
)";

pub(crate) async fn for_redo_begin(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    phase: PhaseName,
    same_active_redo: bool,
    stored: Option<&str>,
) -> RunnerResult<(Option<String>, bool)> {
    if phase != PhaseName::Ingest {
        return Ok((None, false));
    }
    let current: String = sqlx::query_scalar(&format!("SELECT {FINGERPRINT_SQL}"))
        .bind(chain_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to fingerprint active manifest/watch-plan inputs for chain {chain_id}"
                ),
                error,
            )
        })?;
    let changed = same_active_redo && stored != Some(current.as_str());
    Ok((Some(current), changed))
}

pub(crate) fn reject_changed(changed: bool, chain_id: &str, range: BlockRange) -> RunnerResult<()> {
    if changed {
        return Err(RunnerError::data_integrity(format!(
            "Ingest redo authority changed for chain {chain_id} range {}..={}: the active \
             manifest/watch-plan inputs differ from the checkpoint; resumable progress was \
             cleared; rerun the Ingest redo so the full range loads under the current inputs",
            range.from, range.to
        )));
    }
    Ok(())
}
