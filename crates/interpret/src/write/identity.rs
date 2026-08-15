use bigname_adapters::schema_v2::BatchOutput;
use bigname_adapters::schema_v2::seam::{
    BINDING_CLOSE_CLAMP_SQL, LOG_INDEX_KEY, TRANSACTION_INDEX_KEY, binding_open_time,
    is_raw_block_provenance,
};
use sqlx::{Postgres, Transaction, types::Uuid};

use crate::{InterpretError, Result};

mod rows;
mod transition;

pub(super) async fn write_rows(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
    preserve_outside_range_closes: bool,
) -> Result<()> {
    transition::validate_boundaries(output)?;
    super::identity_names::write(transaction, output).await?;
    rows::write(transaction, output).await?;
    write_bindings(transaction, output, preserve_outside_range_closes).await
}

pub(super) async fn write_transitions(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
) -> Result<()> {
    transition::write(transaction, &output.migration_authority_transitions).await
}

async fn write_bindings(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
    preserve_outside_range_closes: bool,
) -> Result<()> {
    let mut operations = output
        .binding_closures
        .iter()
        .map(BindingOperation::Close)
        .chain(output.surface_bindings.iter().map(BindingOperation::Open))
        .collect::<Vec<_>>();
    operations.sort_by_key(BindingOperation::order_key);
    for operation in operations {
        match operation {
            BindingOperation::Close(closure) => {
                let statement = format!(
                    "
                    UPDATE surface_bindings
                    SET active_to = {BINDING_CLOSE_CLAMP_SQL},
                        observed_at = now()
                    WHERE logical_name_id = $1
                      AND chain_id = $7
                      AND authority_arm = $8
                      AND canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND (
                          block_number < $4
                          OR (
                              block_number = $4
                              AND (
                                  COALESCE(
                                      (provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint, -1
                                  ),
                                  COALESCE(
                                      (provenance ->> '{LOG_INDEX_KEY}')::bigint, -1
                                  )
                              ) < ($5, $6)
                          )
                      )
                      AND (
                          active_to IS NULL
                          OR active_to > {BINDING_CLOSE_CLAMP_SQL}
                      )
                      AND (
                          $3::uuid IS NULL
                          OR surface_binding_id <> $3
                      )
                    "
                );
                sqlx::query(&statement)
                    .bind(&closure.logical_name_id)
                    .bind(closure.active_to)
                    .bind(closure.except_surface_binding_id)
                    .bind(closure.block_number)
                    .bind(closure.transaction_index)
                    .bind(closure.log_index)
                    .bind(&closure.chain_id)
                    .bind(&closure.authority_arm)
                    .execute(&mut **transaction)
                    .await
                    .map_err(|error| {
                        InterpretError::database("failed to close identity binding", error)
                    })?;
            }
            BindingOperation::Open(binding) => {
                let (transaction_index, log_index) = binding_position(binding)?;
                let predecessor_statement = format!(
                    "
                    SELECT active_from
                    FROM surface_bindings
                    WHERE logical_name_id = $1
                      AND chain_id = $6
                      AND authority_arm = $7
                      AND surface_binding_id <> $2
                      AND canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND (
                          block_number < $3
                          OR (
                              block_number = $3
                              AND (
                                  COALESCE(
                                      (provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint, -1
                                  ),
                                  COALESCE(
                                      (provenance ->> '{LOG_INDEX_KEY}')::bigint, -1
                                  )
                              ) < ($4, $5)
                          )
                      )
                    ORDER BY block_number DESC,
                             COALESCE(
                                 (provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint, -1
                             ) DESC,
                             COALESCE(
                                 (provenance ->> '{LOG_INDEX_KEY}')::bigint, -1
                             ) DESC,
                             surface_binding_id DESC
                    LIMIT 1
                    FOR UPDATE
                    "
                );
                let predecessor: Option<time::OffsetDateTime> =
                    sqlx::query_scalar(&predecessor_statement)
                        .bind(&binding.logical_name_id)
                        .bind(binding.surface_binding_id)
                        .bind(binding.block_number)
                        .bind(transaction_index)
                        .bind(log_index)
                        .bind(&binding.chain_id)
                        .bind(&binding.authority_arm)
                        .fetch_optional(&mut **transaction)
                        .await
                        .map_err(|error| {
                            InterpretError::database(
                                "failed to find predecessor identity binding",
                                error,
                            )
                        })?;
                let effective_start = binding_open_time(binding.active_from, predecessor);
                let successor_statement = format!(
                    "
                    SELECT active_from
                    FROM surface_bindings
                    WHERE logical_name_id = $1
                      AND chain_id = $6
                      AND authority_arm = $7
                      AND surface_binding_id <> $2
                      AND canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND (
                          block_number > $3
                          OR (
                              block_number = $3
                              AND (
                                  COALESCE(
                                      (provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint, -1
                                  ),
                                  COALESCE(
                                      (provenance ->> '{LOG_INDEX_KEY}')::bigint, -1
                                  )
                              ) > ($4, $5)
                          )
                      )
                    ORDER BY block_number,
                             COALESCE(
                                 (provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint, -1
                             ),
                             COALESCE(
                                 (provenance ->> '{LOG_INDEX_KEY}')::bigint, -1
                             ),
                             surface_binding_id
                    LIMIT 1
                    FOR UPDATE
                    "
                );
                let successor: Option<time::OffsetDateTime> =
                    sqlx::query_scalar(&successor_statement)
                        .bind(&binding.logical_name_id)
                        .bind(binding.surface_binding_id)
                        .bind(binding.block_number)
                        .bind(transaction_index)
                        .bind(log_index)
                        .bind(&binding.chain_id)
                        .bind(&binding.authority_arm)
                        .fetch_optional(&mut **transaction)
                        .await
                        .map_err(|error| {
                            InterpretError::database(
                                "failed to find successor identity binding",
                                error,
                            )
                        })?;
                if successor.is_some_and(|successor| successor <= effective_start) {
                    return Err(InterpretError::data_integrity(format!(
                        "surface binding {} has no ordered interval before its successor",
                        binding.surface_binding_id
                    )));
                }
                let cap_predecessor_statement = format!(
                    "
                    UPDATE surface_bindings
                    SET active_to = $3,
                        observed_at = now()
                    WHERE logical_name_id = $1
                      AND chain_id = $7
                      AND authority_arm = $8
                      AND surface_binding_id <> $2
                      AND canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND (
                          block_number < $4
                          OR (
                              block_number = $4
                              AND (
                                  COALESCE(
                                      (provenance ->> '{TRANSACTION_INDEX_KEY}')::bigint, -1
                                  ),
                                  COALESCE(
                                      (provenance ->> '{LOG_INDEX_KEY}')::bigint, -1
                                  )
                              ) < ($5, $6)
                          )
                      )
                      AND active_from < $3
                      AND (
                          active_to IS NULL
                          OR active_to > $3
                      )
                    "
                );
                sqlx::query(&cap_predecessor_statement)
                    .bind(&binding.logical_name_id)
                    .bind(binding.surface_binding_id)
                    .bind(effective_start)
                    .bind(binding.block_number)
                    .bind(transaction_index)
                    .bind(log_index)
                    .bind(&binding.chain_id)
                    .bind(&binding.authority_arm)
                    .execute(&mut **transaction)
                    .await
                    .map_err(|error| {
                        InterpretError::database(
                            "failed to cap predecessor identity binding",
                            error,
                        )
                    })?;
                let written: Option<Uuid> = sqlx::query_scalar(
                    "
                    INSERT INTO surface_bindings (
                        surface_binding_id, logical_name_id, resource_id, binding_kind,
                        authority_arm, active_from, active_to, chain_id, block_hash,
                        block_number, provenance, canonicality_state
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $12, $7, $8, $9, $10,
                            $11::canonicality_state)
                    ON CONFLICT (surface_binding_id) DO UPDATE
                    SET active_from = CASE
                            WHEN surface_bindings.canonicality_state = 'orphaned'
                                THEN EXCLUDED.active_from
                            ELSE surface_bindings.active_from
                        END,
                        active_to = CASE
                            WHEN surface_bindings.canonicality_state = 'orphaned'
                              AND $13
                                THEN CASE
                                    WHEN surface_bindings.active_to IS NULL THEN $12
                                    WHEN $12::timestamptz IS NULL
                                        THEN surface_bindings.active_to
                                    ELSE LEAST(surface_bindings.active_to, $12)
                                END
                            WHEN surface_bindings.canonicality_state = 'orphaned'
                                THEN $12
                            WHEN $12::timestamptz IS NOT NULL
                              AND (
                                  surface_bindings.active_to IS NULL
                                  OR surface_bindings.active_to > $12
                              )
                                THEN $12
                            ELSE surface_bindings.active_to
                        END,
                        block_hash = CASE
                            WHEN surface_bindings.canonicality_state = 'orphaned'
                                THEN EXCLUDED.block_hash
                            ELSE surface_bindings.block_hash
                        END,
                        block_number = CASE
                            WHEN surface_bindings.canonicality_state = 'orphaned'
                                THEN EXCLUDED.block_number
                            ELSE surface_bindings.block_number
                        END,
                        provenance = CASE
                            WHEN surface_bindings.canonicality_state = 'orphaned'
                                THEN EXCLUDED.provenance
                            ELSE surface_bindings.provenance
                        END,
                        canonicality_state = CASE
                            WHEN surface_bindings.canonicality_state = 'orphaned'
                              OR (
                                  EXCLUDED.block_number = surface_bindings.block_number
                                  AND EXCLUDED.block_hash = surface_bindings.block_hash
                              )
                                THEN EXCLUDED.canonicality_state
                            ELSE surface_bindings.canonicality_state
                        END,
                        observed_at = CASE
                            WHEN surface_bindings.canonicality_state = 'orphaned'
                                THEN now()
                            ELSE surface_bindings.observed_at
                        END
                    WHERE surface_bindings.logical_name_id = EXCLUDED.logical_name_id
                      AND surface_bindings.resource_id = EXCLUDED.resource_id
                      AND surface_bindings.binding_kind = EXCLUDED.binding_kind
                      AND surface_bindings.authority_arm = EXCLUDED.authority_arm
                      AND (
                          surface_bindings.canonicality_state = 'orphaned'
                          OR surface_bindings.active_from = EXCLUDED.active_from
                      )
                      AND surface_bindings.chain_id = EXCLUDED.chain_id
                    RETURNING surface_binding_id
                    ",
                )
                .bind(binding.surface_binding_id)
                .bind(&binding.logical_name_id)
                .bind(binding.resource_id)
                .bind(&binding.binding_kind)
                .bind(&binding.authority_arm)
                .bind(effective_start)
                .bind(&binding.chain_id)
                .bind(&binding.block_hash)
                .bind(binding.block_number)
                .bind(&binding.provenance)
                .bind(&binding.canonicality_state)
                .bind(successor)
                .bind(preserve_outside_range_closes)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|error| {
                    InterpretError::database("failed to write identity binding", error)
                })?;
                if written.is_none() {
                    return Err(InterpretError::data_integrity(format!(
                        "surface binding {} is already bound to different identity data",
                        binding.surface_binding_id
                    )));
                }
            }
        }
    }
    Ok(())
}

fn binding_position(binding: &bigname_adapters::schema_v2::SurfaceBinding) -> Result<(i64, i64)> {
    let transaction_index = binding
        .provenance
        .get(TRANSACTION_INDEX_KEY)
        .and_then(serde_json::Value::as_i64);
    let log_index = binding
        .provenance
        .get(LOG_INDEX_KEY)
        .and_then(serde_json::Value::as_i64);
    match (transaction_index, log_index) {
        (Some(transaction_index), Some(log_index)) if transaction_index >= 0 && log_index >= 0 => {
            Ok((transaction_index, log_index))
        }
        (None, None) if is_raw_block_provenance(&binding.provenance) => Ok((-1, -1)),
        _ => Err(InterpretError::data_integrity(
            "surface binding requires both non-negative transaction and log indexes",
        )),
    }
}

enum BindingOperation<'a> {
    Close(&'a bigname_adapters::schema_v2::BindingClosure),
    Open(&'a bigname_adapters::schema_v2::SurfaceBinding),
}

impl BindingOperation<'_> {
    fn order_key(&self) -> (i64, i64, i64, u8) {
        match self {
            Self::Close(closure) => (
                closure.block_number,
                closure.transaction_index,
                closure.log_index,
                0,
            ),
            Self::Open(binding) => {
                let (transaction_index, log_index) =
                    binding_position(binding).unwrap_or((i64::MAX, i64::MAX));
                (binding.block_number, transaction_index, log_index, 1)
            }
        }
    }
}

#[cfg(test)]
mod tests;
