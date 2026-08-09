use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    error::{ErrorKind, RunnerError, RunnerResult},
    phase::{BlockRange, PhaseName},
    redo_manifest_attestation::AttestedManifestAuthority,
};

type AuditRow = (String, String, String, String, i64, i64, String, String);

const PENDING_GENERATION_QUERY: &str = "SELECT audit.generation_token
     FROM manifest_authority_attestations audit
     JOIN chain_phase_state phase
       ON phase.chain_id = audit.chain_id
      AND phase.phase_name = audit.phase_name
      AND phase.redo_in_progress
      AND phase.redo_from_block_number = audit.redo_from_block_number
      AND phase.redo_to_block_number = audit.redo_to_block_number
      AND phase.started_at = audit.attested_at
     WHERE audit.chain_id = $1
     ORDER BY audit.attested_at DESC, audit.generation_token
     LIMIT 1";

pub(crate) struct ManifestAuthorityAttestationAudit {
    chain_id: String,
    phase_name: String,
    generation_token: String,
    authority_fingerprint: String,
    redo_from_block_number: i64,
    redo_to_block_number: i64,
    attested_by: String,
    attested_at: String,
    replayed: bool,
}

impl ManifestAuthorityAttestationAudit {
    pub(crate) fn emit(&self) {
        tracing::error!(
            event = "manifest_authority_watch_set_coverage_attested",
            chain_id = self.chain_id,
            phase = self.phase_name,
            redo_from_block = self.redo_from_block_number,
            redo_to_block = self.redo_to_block_number,
            authority_fingerprint = self.authority_fingerprint,
            generation_token = self.generation_token,
            attested_by = self.attested_by,
            attested_at = self.attested_at,
            replayed = self.replayed,
            "OPERATOR ATTESTATION: manifest-authority redo began after watch-set coverage review"
        );
    }
}

pub(crate) async fn persist(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    attestation: AttestedManifestAuthority,
    attested_by: &str,
) -> RunnerResult<ManifestAuthorityAttestationAudit> {
    let row: AuditRow = sqlx::query_as(
        "INSERT INTO manifest_authority_attestations (
             chain_id, phase_name, generation_token, authority_fingerprint,
             redo_from_block_number, redo_to_block_number, attested_by
         ) VALUES ($1, 'interpret', $2, $3, $4, $5, $6)
         RETURNING chain_id, phase_name, generation_token, authority_fingerprint,
                   redo_from_block_number, redo_to_block_number, attested_by,
                   attested_at::text",
    )
    .bind(chain_id)
    .bind(attestation.generation_token)
    .bind(attestation.authority_fingerprint)
    .bind(attestation.range.from)
    .bind(attestation.range.to)
    .bind(attested_by)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!(
                "failed to record manifest-authority attestation for chain {chain_id} phase {}",
                PhaseName::Interpret
            ),
            error,
        )
    })?;
    Ok(from_row(row, false))
}

pub(crate) async fn record_or_resume(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    attestation: Option<AttestedManifestAuthority>,
    requested_range: BlockRange,
    resume_same_redo: bool,
    supplied_generation: Option<&str>,
    attested_by: &str,
) -> RunnerResult<Option<ManifestAuthorityAttestationAudit>> {
    if let Some(attestation) = attestation {
        return persist(transaction, chain_id, attestation, attested_by)
            .await
            .map(Some);
    }
    let Some(generation) = supplied_generation else {
        return Ok(None);
    };
    let audit = pending_for_restart_locked(transaction, chain_id, generation)
        .await?
        .ok_or_else(|| {
            RunnerError::data_integrity(format!(
                "active manifest-authority attestation {generation} for chain {chain_id} \
                 disappeared during locked redo begin"
            ))
        })?;
    if !resume_same_redo {
        return Err(RunnerError::new(
            ErrorKind::Configuration,
            format!(
                "--attest-watch-set-coverage invalidation token {generation} for chain \
                 {chain_id} belongs to active audited Interpret redo range {}..={}, but this \
                 command resolves to {}..={}; re-run the exact active audited range",
                audit.redo_from_block_number,
                audit.redo_to_block_number,
                requested_range.from,
                requested_range.to
            ),
        ));
    }
    Ok(Some(audit))
}

pub(crate) async fn pending_for_restart_locked(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    generation_token: &str,
) -> RunnerResult<Option<ManifestAuthorityAttestationAudit>> {
    let row: Option<AuditRow> = sqlx::query_as(
        "SELECT audit.chain_id, audit.phase_name, audit.generation_token,
                audit.authority_fingerprint, audit.redo_from_block_number,
                audit.redo_to_block_number, audit.attested_by, audit.attested_at::text
         FROM manifest_authority_attestations audit
         JOIN chain_phase_state phase
           ON phase.chain_id = audit.chain_id
          AND phase.phase_name = audit.phase_name
          AND phase.redo_in_progress
          AND phase.redo_from_block_number = audit.redo_from_block_number
          AND phase.redo_to_block_number = audit.redo_to_block_number
          AND phase.started_at = audit.attested_at
         WHERE audit.chain_id = $1
           AND audit.generation_token = $2",
    )
    .bind(chain_id)
    .bind(generation_token)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| {
        RunnerError::database(
            format!("failed to load durable manifest-authority attestation for chain {chain_id}"),
            error,
        )
    })?;
    Ok(row.map(|row| from_row(row, true)))
}

pub(crate) async fn pending_generation(
    pool: &PgPool,
    chain_id: &str,
) -> RunnerResult<Option<String>> {
    sqlx::query_scalar(PENDING_GENERATION_QUERY)
        .bind(chain_id)
        .fetch_optional(pool)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to load active manifest-authority attestation for chain {chain_id}"
                ),
                error,
            )
        })
}

pub(crate) async fn pending_generation_locked(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> RunnerResult<Option<String>> {
    sqlx::query_scalar(PENDING_GENERATION_QUERY)
        .bind(chain_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| {
            RunnerError::database(
                format!(
                    "failed to recheck active manifest-authority attestation for chain {chain_id}"
                ),
                error,
            )
        })
}

fn from_row(row: AuditRow, replayed: bool) -> ManifestAuthorityAttestationAudit {
    ManifestAuthorityAttestationAudit {
        chain_id: row.0,
        phase_name: row.1,
        generation_token: row.2,
        authority_fingerprint: row.3,
        redo_from_block_number: row.4,
        redo_to_block_number: row.5,
        attested_by: row.6,
        attested_at: row.7,
        replayed,
    }
}
