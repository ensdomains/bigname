use std::collections::HashMap;

use bigname_adapters::schema_v2::ContractAddress;
use serde_json::Value;
use sqlx::{Postgres, Transaction};

use crate::{InterpretError, Result};

#[derive(sqlx::FromRow)]
struct ActiveEpoch {
    row_id: i64,
    start: Option<i64>,
    hash: Option<String>,
    prior_end: Option<i64>,
    retired_through: Option<i64>,
    source_manifest_id: Option<i64>,
    provenance: Value,
}

/// Cache only rows locked or inserted by this transaction. Process observations
/// in their original order so competing address/instance claims keep the same
/// insertion, constraint, and provenance behavior. Missing rows are never cached.
pub(super) async fn write(
    transaction: &mut Transaction<'_, Postgres>,
    addresses: &[ContractAddress],
) -> Result<()> {
    let mut keys = HashMap::new();
    let mut epochs = HashMap::<i64, ActiveEpoch>::new();
    for address in addresses {
        let key = (
            address.contract_instance_id,
            address.chain_id.as_str(),
            address.address.as_str(),
        );
        let row_id = match keys.get(&key) {
            Some(row_id) => *row_id,
            None => {
                let active = load(transaction, address).await?;
                let Some(active) = active else {
                    if let Some(inserted) = insert(transaction, address).await? {
                        keys.insert(key, inserted.row_id);
                        epochs.insert(inserted.row_id, inserted);
                    }
                    continue;
                };
                // Different input spellings can resolve to the same SQL lower()
                // address. Keep one mutable state per actual database row.
                let row_id = active.row_id;
                epochs.insert(row_id, active);
                keys.insert(key, row_id);
                row_id
            }
        };
        refresh(
            transaction,
            epochs.get_mut(&row_id).expect("cached epoch"),
            address,
        )
        .await?;
    }
    Ok(())
}

async fn load(
    transaction: &mut Transaction<'_, Postgres>,
    address: &ContractAddress,
) -> Result<Option<ActiveEpoch>> {
    sqlx::query_as(
        "SELECT current.contract_instance_address_id AS row_id,
                current.active_from_block_number AS start,
                current.active_from_block_hash AS hash,
                (SELECT max(history.active_to_block_number)
                 FROM contract_instance_addresses history
                 WHERE history.contract_instance_id = current.contract_instance_id
                   AND history.chain_id = current.chain_id
                   AND lower(history.address) = lower(current.address)
                   AND history.contract_instance_address_id <> current.contract_instance_address_id
                ) AS prior_end,
                (SELECT max(history.active_to_block_number)
                 FROM contract_instance_addresses history
                 WHERE history.contract_instance_id = current.contract_instance_id
                   AND history.chain_id = current.chain_id
                   AND lower(history.address) = lower(current.address)
                   AND history.deactivated_at IS NOT NULL
                   AND history.provenance ->> 'source' IN
                       ('manifest_declaration', 'manifest_proxy_implementation')
                ) AS retired_through,
                current.source_manifest_id, current.provenance
         FROM contract_instance_addresses current
         WHERE current.contract_instance_id = $1
           AND current.chain_id = $2
           AND lower(current.address) = lower($3)
           AND current.deactivated_at IS NULL
         ORDER BY current.admitted_at DESC LIMIT 1 FOR UPDATE",
    )
    .bind(address.contract_instance_id)
    .bind(&address.chain_id)
    .bind(&address.address)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| InterpretError::database("failed to lock discovered address epoch", error))
}

async fn refresh(
    transaction: &mut Transaction<'_, Postgres>,
    current: &mut ActiveEpoch,
    address: &ContractAddress,
) -> Result<()> {
    if current
        .retired_through
        .is_some_and(|end| address.active_from_block_number <= end)
    {
        return Ok(());
    }
    let bounded_start = bound(address.active_from_block_number, current.prior_end)?;
    let start = current.start.map(|start| start.min(bounded_start));
    let hash = if current.start == start {
        current.hash.clone()
    } else if start == Some(address.active_from_block_number) {
        Some(address.active_from_block_hash.clone())
    } else {
        None
    };
    let protected = matches!(
        current.provenance.get("source").and_then(Value::as_str),
        Some("manifest_declaration" | "manifest_proxy_implementation")
    );
    let metadata_unchanged = protected
        || (current.source_manifest_id == Some(address.source_manifest_id)
            && current.provenance == address.provenance);
    // Skipping an UPDATE must not hide jsonb input errors, even when its CASE
    // would preserve manifest provenance. Complex JSON still goes through SQL.
    if current.start == start
        && current.hash == hash
        && metadata_unchanged
        && flat_jsonb_input(&address.provenance)
    {
        return Ok(());
    }
    sqlx::query(
        "UPDATE contract_instance_addresses
         SET active_from_block_number = $2,
             active_from_block_hash = $3,
             source_manifest_id = CASE
                 WHEN provenance ->> 'source' IN
                     ('manifest_declaration', 'manifest_proxy_implementation')
                 THEN source_manifest_id ELSE $4 END,
             provenance = CASE
                 WHEN provenance ->> 'source' IN
                     ('manifest_declaration', 'manifest_proxy_implementation')
                 THEN provenance ELSE $5 END
         WHERE contract_instance_address_id = $1",
    )
    .bind(current.row_id)
    .bind(start)
    .bind(&hash)
    .bind(address.source_manifest_id)
    .bind(&address.provenance)
    .execute(&mut **transaction)
    .await
    .map_err(|error| InterpretError::database("failed to backdate discovered address", error))?;
    current.start = start;
    current.hash = hash;
    if !protected {
        current.source_manifest_id = Some(address.source_manifest_id);
        current.provenance.clone_from(&address.provenance);
    }
    Ok(())
}

async fn insert(
    transaction: &mut Transaction<'_, Postgres>,
    address: &ContractAddress,
) -> Result<Option<ActiveEpoch>> {
    let (prior_end, retired_through): (Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT max(active_to_block_number),
                max(active_to_block_number) FILTER (
                    WHERE deactivated_at IS NOT NULL AND provenance ->> 'source' IN
                        ('manifest_declaration', 'manifest_proxy_implementation'))
         FROM contract_instance_addresses
         WHERE contract_instance_id = $1 AND chain_id = $2 AND lower(address) = lower($3)",
    )
    .bind(address.contract_instance_id)
    .bind(&address.chain_id)
    .bind(&address.address)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to bound new discovered address epoch", error)
    })?;
    if retired_through.is_some_and(|end| address.active_from_block_number <= end) {
        return Ok(None);
    }
    let start = bound(address.active_from_block_number, prior_end)?;
    let hash =
        (start == address.active_from_block_number).then(|| address.active_from_block_hash.clone());
    let row_id: Option<i64> = sqlx::query_scalar(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address,
             active_from_block_number, active_from_block_hash, source_manifest_id, provenance)
         SELECT $1, $2, lower($3), $4, $5, $6, $7
         WHERE NOT EXISTS (
             SELECT 1 FROM contract_instance_addresses existing
             WHERE existing.chain_id = $2 AND lower(existing.address) = lower($3)
               AND existing.deactivated_at IS NULL)
         RETURNING contract_instance_address_id",
    )
    .bind(address.contract_instance_id)
    .bind(&address.chain_id)
    .bind(&address.address)
    .bind(start)
    .bind(&hash)
    .bind(address.source_manifest_id)
    .bind(&address.provenance)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|error| InterpretError::database("failed to write discovered address", error))?;
    Ok(row_id.map(|row_id| ActiveEpoch {
        row_id,
        start: Some(start),
        hash,
        prior_end,
        retired_through,
        source_manifest_id: Some(address.source_manifest_id),
        provenance: address.provenance.clone(),
    }))
}

fn bound(event_block: i64, prior_end: Option<i64>) -> Result<i64> {
    let Some(prior_end) = prior_end else {
        return Ok(event_block);
    };
    let floor = prior_end.checked_add(1).ok_or_else(|| {
        InterpretError::data_integrity("discovered address epoch end overflowed BIGINT")
    })?;
    Ok(event_block.max(floor))
}

fn flat_jsonb_input(value: &Value) -> bool {
    let scalar = |value: &Value| match value {
        Value::Null | Value::Bool(_) => true,
        Value::Number(value) => value.is_i64() || value.is_u64(),
        Value::String(value) => !value.contains('\0'),
        _ => false,
    };
    match value {
        Value::Object(fields) => fields
            .iter()
            .all(|(key, value)| !key.contains('\0') && scalar(value)),
        _ => false,
    }
}

#[cfg(test)]
#[path = "address_tests.rs"]
mod tests;
