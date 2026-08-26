use anyhow::{Context, Result};
use sqlx::{Postgres, Transaction};

pub(super) async fn load(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<super::watch::PersistedWatchFloors> {
    let rows: Vec<(String, String, String, String, i64)> = sqlx::query_as(
        "SELECT manifest.chain_id,
                compiled.entry -> 'emitter' ->> 'family' AS source_family,
                lower(compiled.entry -> 'emitter' ->> 'address') AS address,
                lower(compiled.entry ->> 'topic0') AS topic0,
                min(GREATEST(
                    COALESCE(declaration.start_block_number, 0),
                    COALESCE(address.active_from_block_number, 0)
                )) AS persisted_floor
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
         WHERE manifest.rollout_status = 'active'
           AND compiled.entry -> 'emitter' ->> 'kind' = 'address'
           AND (
               address.deactivated_at IS NULL
               OR address.active_to_block_number IS NOT NULL
           )
         GROUP BY 1, 2, 3, 4",
    )
    .fetch_all(&mut **transaction)
    .await
    .context("failed to load persisted Ingest floors for compiled-watch comparison")?;
    rows.into_iter()
        .map(|(chain_id, family, address, topic0, floor)| {
            Ok((
                (chain_id, family, address, topic0),
                u64::try_from(floor).context("persisted Ingest floor is negative")?,
            ))
        })
        .collect()
}
