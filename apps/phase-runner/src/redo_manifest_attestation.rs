use crate::{
    config::SourceConfig,
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
};
use sqlx::PgPool;

// This must match the invalidation marker written by
// crates/manifests/src/schema_v2_sync_state.rs.
const MANIFEST_AUTHORITY_INVALIDATION_PREFIX: &str = "manifest-authority:";

fn has_marker_prefix(hash: &str) -> bool {
    hash.starts_with(MANIFEST_AUTHORITY_INVALIDATION_PREFIX)
}

pub(crate) struct ManifestAuthorityAttestation {
    marker: Option<String>,
    expected_marker: Option<String>,
    lineage_from: Option<i64>,
}

pub(crate) struct AttestedManifestAuthority {
    marker: String,
    range: BlockRange,
    lineage_from: i64,
}

impl ManifestAuthorityAttestation {
    pub(crate) fn new(
        chain_id: &str,
        recorded_input_hash: Option<&str>,
        expected_marker: Option<&str>,
    ) -> RunnerResult<Self> {
        let marker = recorded_input_hash
            .filter(|hash| has_marker_prefix(hash))
            .map(str::to_owned);
        if let Some(expected) = expected_marker
            && marker.as_deref() != Some(expected)
        {
            return Err(marker_changed_error(chain_id, expected, marker.as_deref()));
        }
        Ok(Self {
            marker,
            expected_marker: expected_marker.map(str::to_owned),
            lineage_from: None,
        })
    }

    pub(crate) fn record_lineage_suffix(&mut self, from_block: i64) {
        self.lineage_from = Some(
            self.lineage_from
                .map_or(from_block, |current| current.min(from_block)),
        );
    }

    pub(crate) fn finish(
        self,
        chain_id: &str,
        range: BlockRange,
    ) -> RunnerResult<Option<AttestedManifestAuthority>> {
        let Some(marker) = self.marker else {
            return Ok(None);
        };
        let Some(lineage_from) = self.lineage_from else {
            if self.expected_marker.is_some() {
                return Err(fence_not_needed_error(chain_id));
            }
            return Ok(None);
        };
        if self.expected_marker.is_none() {
            return Err(RunnerError::data_integrity(format!(
                "raw-data presence check failed for interpret redo on chain {chain_id}: the \
                 manifest authority changed since blocks {lineage_from}..={} were loaded; \
                 complete the documented mandatory historical fetch for any widened range \
                 (docs/manifests.md § mandatory historical fetch after watch-plan widening), or \
                 confirm that the change widened nothing; then re-run with \
                 --attest-watch-set-coverage; see issue #376",
                range.to
            )));
        }

        Ok(Some(AttestedManifestAuthority {
            marker,
            range,
            lineage_from,
        }))
    }
}

impl AttestedManifestAuthority {
    pub(crate) fn log(self, chain_id: &str) {
        tracing::error!(
            event = "manifest_authority_watch_set_coverage_attested",
            chain_id,
            phase = %PhaseName::Interpret,
            redo_from_block = self.range.from,
            redo_to_block = self.range.to,
            lineage_from_block = self.lineage_from,
            lineage_to_block = self.range.to,
            manifest_authority_marker = self.marker,
            "OPERATOR ATTESTATION: manifest-authority redo began with Live lineage coverage"
        );
    }
}

pub(crate) async fn preflight(
    pool: &PgPool,
    chain_id: &str,
    sources: &[SourceConfig],
) -> RunnerResult<String> {
    let state: Option<(Option<String>, Option<i64>)> = sqlx::query_as(
        "SELECT input_content_hash, current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'interpret'",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to preflight watch-set coverage attestation for chain {chain_id}"),
            error,
        )
    })?;
    let Some((recorded_hash, current_block_number)) = state else {
        return Err(marker_missing_error(chain_id));
    };
    let Some(marker) = recorded_hash.filter(|hash| has_marker_prefix(hash)) else {
        return Err(marker_missing_error(chain_id));
    };
    let Some(redo_end) = current_block_number else {
        return Err(fence_not_needed_error(chain_id));
    };

    for source in sources {
        if source.start_block_number > redo_end {
            continue;
        }
        let cursor: Option<(i64, Option<i64>)> = sqlx::query_as(
            "SELECT next_block_number, target_block_number
             FROM ingest_cursors
             WHERE chain_id = $1 AND source_key = $2",
        )
        .bind(chain_id)
        .bind(&source.source_key)
        .fetch_optional(pool)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to preflight source {} on chain {chain_id}",
                    source.source_key
                ),
                error,
            )
        })?;
        if cursor.is_some_and(|(next, target)| {
            target.is_some_and(|target| target < redo_end) && next <= redo_end
        }) {
            return Ok(marker);
        }
    }
    Err(fence_not_needed_error(chain_id))
}

fn marker_missing_error(chain_id: &str) -> RunnerError {
    RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "--attest-watch-set-coverage is only valid when an interpret redo on chain \
             {chain_id} is discharging a manifest-authority marker and would otherwise rely on \
             Live lineage"
        ),
    )
}

fn marker_changed_error(chain_id: &str, expected: &str, recorded: Option<&str>) -> RunnerError {
    RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "--attest-watch-set-coverage was preflighted for manifest-authority marker \
             {expected} on chain {chain_id}, but the locked phase state now records {}; repeat \
             the historical-fetch or no-widening review for the current marker before retrying; \
             see issue #376",
            recorded.unwrap_or("no manifest-authority marker")
        ),
    )
}

fn fence_not_needed_error(chain_id: &str) -> RunnerError {
    RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "--attest-watch-set-coverage is not valid for interpret redo on chain {chain_id}: \
             finite ingest cursors already cover the redo range, so the manifest-authority fence \
             would not block"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locked_begin_rejects_a_marker_changed_after_preflight() {
        let error = match ManifestAuthorityAttestation::new(
            "ethereum-mainnet",
            Some("manifest-authority:new"),
            Some("manifest-authority:preflight"),
        ) {
            Ok(_) => panic!("a changed marker must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert!(error.to_string().contains("manifest-authority:preflight"));
        assert!(error.to_string().contains("manifest-authority:new"));
    }

    #[test]
    fn locked_begin_rejects_a_marker_cleared_after_preflight() {
        let error = match ManifestAuthorityAttestation::new(
            "ethereum-mainnet",
            None,
            Some("manifest-authority:preflight"),
        ) {
            Ok(_) => panic!("a cleared marker must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert!(error.to_string().contains("no manifest-authority marker"));
    }
}
