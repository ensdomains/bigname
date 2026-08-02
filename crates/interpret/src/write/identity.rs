use bigname_adapters::schema_v2::BatchOutput;
use sqlx::{Postgres, Transaction, types::Uuid};

use crate::{InterpretError, Result};

pub(super) async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
    preserve_outside_range_closes: bool,
) -> Result<()> {
    super::identity_names::write(transaction, output).await?;
    write_token_lineages(transaction, output).await?;
    write_resources(transaction, output).await?;
    write_bindings(transaction, output, preserve_outside_range_closes).await
}

async fn write_token_lineages(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
) -> Result<()> {
    for lineage in &output.token_lineages {
        sqlx::query(
            "
            INSERT INTO token_lineages (
                token_lineage_id, chain_id, block_hash, block_number,
                provenance, canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6::canonicality_state)
            ON CONFLICT (token_lineage_id) DO UPDATE
            SET block_hash = CASE
                    WHEN token_lineages.canonicality_state = 'orphaned'
                        THEN EXCLUDED.block_hash
                    ELSE token_lineages.block_hash
                END,
                block_number = CASE
                    WHEN token_lineages.canonicality_state = 'orphaned'
                        THEN EXCLUDED.block_number
                    ELSE token_lineages.block_number
                END,
                provenance = CASE
                    WHEN token_lineages.canonicality_state = 'orphaned'
                        THEN EXCLUDED.provenance
                    ELSE token_lineages.provenance
                END,
                canonicality_state = CASE
                    WHEN token_lineages.canonicality_state = 'orphaned'
                      OR (
                          EXCLUDED.block_number = token_lineages.block_number
                          AND EXCLUDED.block_hash = token_lineages.block_hash
                      )
                        THEN EXCLUDED.canonicality_state
                    ELSE token_lineages.canonicality_state
                END,
                observed_at = CASE
                    WHEN token_lineages.canonicality_state = 'orphaned'
                        THEN now()
                    ELSE token_lineages.observed_at
                END
            WHERE token_lineages.chain_id = EXCLUDED.chain_id
            ",
        )
        .bind(lineage.token_lineage_id)
        .bind(&lineage.chain_id)
        .bind(&lineage.block_hash)
        .bind(lineage.block_number)
        .bind(&lineage.provenance)
        .bind(&lineage.canonicality_state)
        .execute(&mut **transaction)
        .await
        .map_err(|error| InterpretError::database("failed to write token lineage", error))?;
    }
    Ok(())
}

async fn write_resources(
    transaction: &mut Transaction<'_, Postgres>,
    output: &BatchOutput,
) -> Result<()> {
    for resource in &output.resources {
        let written: Option<Uuid> = sqlx::query_scalar(
            "
            INSERT INTO resources (
                resource_id, token_lineage_id, chain_id, block_hash,
                block_number, provenance, canonicality_state
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::canonicality_state)
            ON CONFLICT (resource_id) DO UPDATE
            SET block_hash = CASE
                    WHEN resources.canonicality_state = 'orphaned'
                        THEN EXCLUDED.block_hash
                    ELSE resources.block_hash
                END,
                block_number = CASE
                    WHEN resources.canonicality_state = 'orphaned'
                        THEN EXCLUDED.block_number
                    ELSE resources.block_number
                END,
                provenance = CASE
                    WHEN resources.canonicality_state = 'orphaned'
                        THEN EXCLUDED.provenance
                    ELSE resources.provenance
                END,
                canonicality_state = CASE
                    WHEN resources.canonicality_state = 'orphaned'
                      OR (
                          EXCLUDED.block_number = resources.block_number
                          AND EXCLUDED.block_hash = resources.block_hash
                      )
                        THEN EXCLUDED.canonicality_state
                    ELSE resources.canonicality_state
                END,
                observed_at = CASE
                    WHEN resources.canonicality_state = 'orphaned'
                        THEN now()
                    ELSE resources.observed_at
                END
            WHERE resources.chain_id = EXCLUDED.chain_id
              AND resources.token_lineage_id IS NOT DISTINCT FROM EXCLUDED.token_lineage_id
            RETURNING resource_id
            ",
        )
        .bind(resource.resource_id)
        .bind(resource.token_lineage_id)
        .bind(&resource.chain_id)
        .bind(&resource.block_hash)
        .bind(resource.block_number)
        .bind(&resource.provenance)
        .bind(&resource.canonicality_state)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| InterpretError::database("failed to write resource", error))?;
        if written.is_none() {
            return Err(InterpretError::data_integrity(format!(
                "resource {} is already bound to different lineage data",
                resource.resource_id
            )));
        }
    }
    Ok(())
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
                sqlx::query(
                    "
                    UPDATE surface_bindings
                    SET active_to = GREATEST(
                            $2,
                            active_from + interval '1 microsecond'
                        ),
                        observed_at = now()
                    WHERE logical_name_id = $1
                      AND canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND (
                          block_number < $4
                          OR (
                              block_number = $4
                              AND (
                                  COALESCE(
                                      (provenance ->> 'transaction_index')::bigint, -1
                                  ),
                                  COALESCE(
                                      (provenance ->> 'log_index')::bigint, -1
                                  )
                              ) < ($5, $6)
                          )
                      )
                      AND (
                          active_to IS NULL
                          OR active_to > GREATEST(
                              $2,
                              active_from + interval '1 microsecond'
                          )
                      )
                      AND (
                          $3::uuid IS NULL
                          OR surface_binding_id <> $3
                      )
                    ",
                )
                .bind(&closure.logical_name_id)
                .bind(closure.active_to)
                .bind(closure.except_surface_binding_id)
                .bind(closure.block_number)
                .bind(closure.transaction_index)
                .bind(closure.log_index)
                .execute(&mut **transaction)
                .await
                .map_err(|error| {
                    InterpretError::database("failed to close identity binding", error)
                })?;
            }
            BindingOperation::Open(binding) => {
                let (transaction_index, log_index) = binding_position(binding)?;
                let predecessor: Option<time::OffsetDateTime> = sqlx::query_scalar(
                    "
                    SELECT active_from
                    FROM surface_bindings
                    WHERE logical_name_id = $1
                      AND surface_binding_id <> $2
                      AND canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND (
                          block_number < $3
                          OR (
                              block_number = $3
                              AND (
                                  COALESCE(
                                      (provenance ->> 'transaction_index')::bigint, -1
                                  ),
                                  COALESCE(
                                      (provenance ->> 'log_index')::bigint, -1
                                  )
                              ) < ($4, $5)
                          )
                      )
                    ORDER BY block_number DESC,
                             COALESCE(
                                 (provenance ->> 'transaction_index')::bigint, -1
                             ) DESC,
                             COALESCE(
                                 (provenance ->> 'log_index')::bigint, -1
                             ) DESC,
                             surface_binding_id DESC
                    LIMIT 1
                    FOR UPDATE
                    ",
                )
                .bind(&binding.logical_name_id)
                .bind(binding.surface_binding_id)
                .bind(binding.block_number)
                .bind(transaction_index)
                .bind(log_index)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|error| {
                    InterpretError::database("failed to find predecessor identity binding", error)
                })?;
                let effective_start = predecessor
                    .filter(|predecessor| binding.active_from <= *predecessor)
                    .map(|predecessor| predecessor + time::Duration::microseconds(1))
                    .unwrap_or(binding.active_from);
                let successor: Option<time::OffsetDateTime> = sqlx::query_scalar(
                    "
                    SELECT active_from
                    FROM surface_bindings
                    WHERE logical_name_id = $1
                      AND surface_binding_id <> $2
                      AND canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND (
                          block_number > $3
                          OR (
                              block_number = $3
                              AND (
                                  COALESCE(
                                      (provenance ->> 'transaction_index')::bigint, -1
                                  ),
                                  COALESCE(
                                      (provenance ->> 'log_index')::bigint, -1
                                  )
                              ) > ($4, $5)
                          )
                      )
                    ORDER BY block_number,
                             COALESCE(
                                 (provenance ->> 'transaction_index')::bigint, -1
                             ),
                             COALESCE(
                                 (provenance ->> 'log_index')::bigint, -1
                             ),
                             surface_binding_id
                    LIMIT 1
                    FOR UPDATE
                    ",
                )
                .bind(&binding.logical_name_id)
                .bind(binding.surface_binding_id)
                .bind(binding.block_number)
                .bind(transaction_index)
                .bind(log_index)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|error| {
                    InterpretError::database("failed to find successor identity binding", error)
                })?;
                if successor.is_some_and(|successor| successor <= effective_start) {
                    return Err(InterpretError::data_integrity(format!(
                        "surface binding {} has no ordered interval before its successor",
                        binding.surface_binding_id
                    )));
                }
                sqlx::query(
                    "
                    UPDATE surface_bindings
                    SET active_to = $3,
                        observed_at = now()
                    WHERE logical_name_id = $1
                      AND surface_binding_id <> $2
                      AND canonicality_state IN ('canonical', 'safe', 'finalized')
                      AND (
                          block_number < $4
                          OR (
                              block_number = $4
                              AND (
                                  COALESCE(
                                      (provenance ->> 'transaction_index')::bigint, -1
                                  ),
                                  COALESCE(
                                      (provenance ->> 'log_index')::bigint, -1
                                  )
                              ) < ($5, $6)
                          )
                      )
                      AND active_from < $3
                      AND (
                          active_to IS NULL
                          OR active_to > $3
                      )
                    ",
                )
                .bind(&binding.logical_name_id)
                .bind(binding.surface_binding_id)
                .bind(effective_start)
                .bind(binding.block_number)
                .bind(transaction_index)
                .bind(log_index)
                .execute(&mut **transaction)
                .await
                .map_err(|error| {
                    InterpretError::database("failed to cap predecessor identity binding", error)
                })?;
                let written: Option<Uuid> = sqlx::query_scalar(
                    "
                    INSERT INTO surface_bindings (
                        surface_binding_id, logical_name_id, resource_id, binding_kind,
                        active_from, active_to, chain_id, block_hash, block_number,
                        provenance, canonicality_state
                    )
                    VALUES ($1, $2, $3, $4, $5, $11, $6, $7, $8, $9,
                            $10::canonicality_state)
                    ON CONFLICT (surface_binding_id) DO UPDATE
                    SET active_from = CASE
                            WHEN surface_bindings.canonicality_state = 'orphaned'
                                THEN EXCLUDED.active_from
                            ELSE surface_bindings.active_from
                        END,
                        active_to = CASE
                            WHEN surface_bindings.canonicality_state = 'orphaned'
                              AND $12
                                THEN CASE
                                    WHEN surface_bindings.active_to IS NULL THEN $11
                                    WHEN $11::timestamptz IS NULL
                                        THEN surface_bindings.active_to
                                    ELSE LEAST(surface_bindings.active_to, $11)
                                END
                            WHEN surface_bindings.canonicality_state = 'orphaned'
                                THEN $11
                            WHEN $11::timestamptz IS NOT NULL
                              AND (
                                  surface_bindings.active_to IS NULL
                                  OR surface_bindings.active_to > $11
                              )
                                THEN $11
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
        .get("transaction_index")
        .and_then(serde_json::Value::as_i64);
    let log_index = binding
        .provenance
        .get("log_index")
        .and_then(serde_json::Value::as_i64);
    match (transaction_index, log_index) {
        (Some(transaction_index), Some(log_index)) if transaction_index >= 0 && log_index >= 0 => {
            Ok((transaction_index, log_index))
        }
        (None, None)
            if binding
                .provenance
                .get("kind")
                .and_then(serde_json::Value::as_str)
                == Some("raw_block") =>
        {
            Ok((-1, -1))
        }
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
mod tests {
    use super::*;

    #[test]
    fn raw_block_binding_open_orders_before_the_first_log() {
        let binding = bigname_adapters::schema_v2::SurfaceBinding {
            surface_binding_id: Uuid::nil(),
            logical_name_id: "ens:0x00".to_owned(),
            resource_id: Uuid::nil(),
            binding_kind: "declared_registry_path".to_owned(),
            active_from: time::OffsetDateTime::UNIX_EPOCH,
            chain_id: "chain".to_owned(),
            block_hash: "block".to_owned(),
            block_number: 7,
            provenance: serde_json::json!({"kind":"raw_block"}),
            canonicality_state: "canonical".to_owned(),
        };
        let closure = bigname_adapters::schema_v2::BindingClosure {
            logical_name_id: binding.logical_name_id.clone(),
            except_surface_binding_id: None,
            active_to: time::OffsetDateTime::UNIX_EPOCH,
            block_number: 7,
            transaction_index: 0,
            log_index: 0,
        };

        assert!(
            BindingOperation::Open(&binding).order_key()
                < BindingOperation::Close(&closure).order_key()
        );
    }
}
