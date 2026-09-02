use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};

pub(super) async fn load(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<super::watch::PersistedWatchCoverage> {
    let rows: Vec<(String, String, String, String, i64, Option<i64>)> = sqlx::query_as(
        "SELECT manifest.chain_id,
                compiled.entry -> 'emitter' ->> 'family' AS source_family,
                lower(compiled.entry -> 'emitter' ->> 'address') AS address,
                lower(compiled.entry ->> 'topic0') AS topic0,
                GREATEST(
                    COALESCE(declaration.start_block_number, 0),
                    COALESCE(address.active_from_block_number, 0)
                ) AS effective_start,
                address.active_to_block_number AS effective_end
         FROM manifest_versions manifest
         CROSS JOIN LATERAL jsonb_array_elements(
             manifest.manifest_payload -> '_bigname_compiled_watch'
         ) AS compiled(entry)
         JOIN manifest_contract_instances declaration
           ON declaration.manifest_id = manifest.manifest_id
          AND declaration.chain_id = manifest.chain_id
          AND lower(declaration.declared_address) =
              lower(compiled.entry -> 'emitter' ->> 'address')
         JOIN contract_instance_addresses address
           ON address.contract_instance_id = declaration.contract_instance_id
          AND address.chain_id = declaration.chain_id
          AND lower(address.address) = lower(declaration.declared_address)
         WHERE manifest.rollout_status = 'active'
           AND compiled.entry -> 'emitter' ->> 'kind' = 'address'
           AND (address.deactivated_at IS NULL
                OR address.active_to_block_number IS NOT NULL)
           AND (
               address.active_to_block_number IS NULL
               OR GREATEST(
                   COALESCE(declaration.start_block_number, 0),
                   COALESCE(address.active_from_block_number, 0)
               ) <= address.active_to_block_number
           )
         ORDER BY 1, 2, 3, 4, 5, 6 NULLS LAST",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load persisted Ingest interval coverage for compiled-watch comparison")?;
    let mut coverage = super::watch::PersistedWatchCoverage::new();
    for (chain_id, family, address, topic0, start, end) in rows {
        coverage
            .entry((chain_id, family, address, topic0))
            .or_default()
            .push(super::watch::CoverageInterval {
                start: u64::try_from(start)
                    .context("persisted Ingest interval start is negative")?,
                end: end
                    .map(|value| {
                        u64::try_from(value).context("persisted Ingest interval end is negative")
                    })
                    .transpose()?,
            });
    }
    for intervals in coverage.values_mut() {
        super::watch::normalize_coverage(intervals);
    }
    Ok(coverage)
}

pub(super) async fn required_ingest_redo_pending(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<bool> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'ingest' AND redo_in_progress
           AND (last_error LIKE 'required downstream redo: %'
                OR last_error LIKE 'required downstream redo active: %'))",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .with_context(|| format!("failed to inspect required Ingest redo for chain {chain_id}"))
}
