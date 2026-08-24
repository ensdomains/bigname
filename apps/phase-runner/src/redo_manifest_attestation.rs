use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::BlockRange,
};
use sqlx::PgPool;
use sqlx::{Postgres, Transaction};

// This must match the invalidation marker written by
// crates/manifests/src/schema_v2_sync_state.rs.
const MANIFEST_AUTHORITY_INVALIDATION_PREFIX: &str = "manifest-authority:";

pub(crate) fn has_marker_prefix(hash: &str) -> bool {
    hash.starts_with(MANIFEST_AUTHORITY_INVALIDATION_PREFIX)
}

pub(crate) struct ManifestAuthorityAttestation {
    marker: Option<ManifestAuthorityMarker>,
    supplied_generation: Option<String>,
}

pub(crate) struct AttestedManifestAuthority {
    pub(crate) authority_fingerprint: String,
    pub(crate) generation_token: String,
    pub(crate) range: BlockRange,
}

struct ManifestAuthorityMarker {
    authority_fingerprint: String,
    generation_token: String,
}

impl ManifestAuthorityAttestation {
    pub(crate) fn new(
        chain_id: &str,
        recorded_input_hash: Option<&str>,
        supplied_generation: Option<&str>,
    ) -> RunnerResult<Self> {
        let marker = recorded_input_hash
            .filter(|hash| has_marker_prefix(hash))
            .map(|hash| parse_marker(chain_id, hash))
            .transpose()?;
        if let Some(supplied) = supplied_generation
            && marker
                .as_ref()
                .map(|marker| marker.generation_token.as_str())
                != Some(supplied)
        {
            return Err(generation_mismatch_error(
                chain_id,
                supplied,
                marker
                    .as_ref()
                    .map(|marker| marker.generation_token.as_str()),
            ));
        }
        Ok(Self {
            marker,
            supplied_generation: supplied_generation.map(str::to_owned),
        })
    }

    pub(crate) fn finish(
        self,
        chain_id: &str,
        range: BlockRange,
    ) -> RunnerResult<Option<AttestedManifestAuthority>> {
        let Some(marker) = self.marker else {
            return Ok(None);
        };
        if self.supplied_generation.is_none() {
            return Err(RunnerError::data_integrity(format!(
                "raw-data presence check failed for interpret redo on chain {chain_id}: the \
                 manifest authority changed since blocks {}..={} were loaded; \
                 invalidation token {}; \
                 complete any required Ingest redo stamped for this authority transition \
                 (docs/manifests.md § mandatory historical fetch after watch-plan widening), \
                 then re-run with \
                 --attest-watch-set-coverage {} (or --attest-watch-set-coverage {chain_id}={} in \
                 a multi-chain redo)",
                range.from,
                range.to,
                marker.generation_token,
                marker.generation_token,
                marker.generation_token
            )));
        }

        Ok(Some(AttestedManifestAuthority {
            authority_fingerprint: marker.authority_fingerprint,
            generation_token: marker.generation_token,
            range,
        }))
    }
}

pub(crate) async fn preflight(
    pool: &PgPool,
    chain_id: &str,
    supplied_generation: Option<&str>,
) -> RunnerResult<Option<String>> {
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
        return supplied_generation
            .map(|supplied| Err(marker_missing_error(chain_id, supplied)))
            .unwrap_or(Ok(None));
    };
    if let Some(marker) = recorded_hash.filter(|hash| has_marker_prefix(hash)) {
        let marker = parse_marker(chain_id, &marker)?;
        if let Some(supplied) = supplied_generation
            && marker.generation_token != supplied
        {
            return Err(generation_mismatch_error(
                chain_id,
                supplied,
                Some(&marker.generation_token),
            ));
        }
        if supplied_generation.is_none() {
            return Ok(None);
        }
    } else {
        let pending_generation =
            crate::redo_manifest_audit::pending_generation(pool, chain_id).await?;
        match (pending_generation.as_deref(), supplied_generation) {
            (Some(pending), Some(supplied)) if pending == supplied => {}
            (Some(pending), Some(supplied)) => {
                return Err(generation_mismatch_error(chain_id, supplied, Some(pending)));
            }
            (Some(pending), None) => {
                return Err(pending_attestation_required_error(chain_id, pending));
            }
            (None, Some(supplied)) => return Err(marker_missing_error(chain_id, supplied)),
            (None, None) => return Ok(None),
        }
    }
    if current_block_number.is_none() {
        return Err(no_recorded_extent_error(chain_id));
    }
    Ok(supplied_generation.map(str::to_owned))
}

pub(crate) async fn resolve_locked_generation(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    recorded_input_hash: Option<&str>,
    supplied_generation: Option<&str>,
) -> RunnerResult<Option<String>> {
    let Some(supplied) = supplied_generation else {
        if !recorded_input_hash.is_some_and(has_marker_prefix)
            && let Some(pending) =
                crate::redo_manifest_audit::pending_generation_locked(transaction, chain_id).await?
        {
            return Err(pending_attestation_required_error(chain_id, &pending));
        }
        return Ok(None);
    };
    if recorded_input_hash.is_some_and(has_marker_prefix) {
        return Ok(Some(supplied.to_owned()));
    }
    let pending_generation =
        crate::redo_manifest_audit::pending_generation_locked(transaction, chain_id).await?;
    if pending_generation.as_deref() != Some(supplied) {
        return Err(generation_mismatch_error(
            chain_id,
            supplied,
            pending_generation.as_deref(),
        ));
    }
    Ok(None)
}

fn marker_missing_error(chain_id: &str, supplied_generation: &str) -> RunnerError {
    RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "--attest-watch-set-coverage supplied invalidation token {supplied_generation} for \
             chain {chain_id}, but its Interpret redo is not discharging a manifest-authority \
             marker"
        ),
    )
}

fn pending_attestation_required_error(chain_id: &str, generation_token: &str) -> RunnerError {
    RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "Interpret redo on chain {chain_id} is resuming the active audited manifest-authority \
             discharge for invalidation token {generation_token}; re-run with \
             --attest-watch-set-coverage {generation_token} (or \
             --attest-watch-set-coverage {chain_id}={generation_token} in a multi-chain redo)"
        ),
    )
}

pub(crate) fn generation_mismatch_error(
    chain_id: &str,
    supplied: &str,
    recorded: Option<&str>,
) -> RunnerError {
    RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "--attest-watch-set-coverage supplied invalidation token {supplied} for chain \
             {chain_id}, but phase state records token {}; complete any required Ingest redo and \
             review the current authority transition before retrying",
            recorded.unwrap_or("<none>")
        ),
    )
}

fn parse_marker(chain_id: &str, marker: &str) -> RunnerResult<ManifestAuthorityMarker> {
    let encoded = marker
        .strip_prefix(MANIFEST_AUTHORITY_INVALIDATION_PREFIX)
        .expect("the caller checked the manifest-authority prefix");
    let Some((authority_fingerprint, generation_token)) = encoded.rsplit_once(':') else {
        return Err(malformed_marker_error(chain_id, marker));
    };
    if authority_fingerprint.len() != 64
        || !authority_fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || generation_token.trim().is_empty()
    {
        return Err(malformed_marker_error(chain_id, marker));
    }
    Ok(ManifestAuthorityMarker {
        authority_fingerprint: authority_fingerprint.to_owned(),
        generation_token: generation_token.to_owned(),
    })
}

fn malformed_marker_error(chain_id: &str, marker: &str) -> RunnerError {
    RunnerError::data_integrity(format!(
        "manifest-authority marker for chain {chain_id} is malformed: {marker}; expected \
         manifest-authority:<authority-fingerprint>:<invalidation-token>"
    ))
}

fn no_recorded_extent_error(chain_id: &str) -> RunnerError {
    RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "--attest-watch-set-coverage is not valid for interpret redo on chain {chain_id}: \
             the manifest-authority marker has no recorded interpreted extent to discharge"
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const FINGERPRINT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn locked_begin_rejects_a_marker_changed_after_preflight() {
        let error = match ManifestAuthorityAttestation::new(
            "ethereum-mainnet",
            Some(&format!("manifest-authority:{FINGERPRINT}:new-token")),
            Some("preflight-token"),
        ) {
            Ok(_) => panic!("a changed marker must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert!(error.to_string().contains("preflight-token"));
        assert!(error.to_string().contains("new-token"));
    }

    #[test]
    fn locked_begin_rejects_a_marker_cleared_after_preflight() {
        let error = match ManifestAuthorityAttestation::new(
            "ethereum-mainnet",
            None,
            Some("preflight-token"),
        ) {
            Ok(_) => panic!("a cleared marker must fail closed"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert!(error.to_string().contains("preflight-token"));
        assert!(error.to_string().contains("token <none>"));
    }
}
