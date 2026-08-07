use std::str;

use bigname_adapters::schema_v2::seam::{LOG_INDEX_KEY, PREIMAGE_OBSERVATION_EVENT_KIND};
use bigname_domain::normalization::{ENS_NORMALIZER_VERSION, normalize_label_under_suffix};
use serde_json::{Value, json};
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use time::OffsetDateTime;

use crate::{InterpretError, Result};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecomputeSummary {
    pub same_class_names: u64,
    pub shadow_to_active_names: u64,
    pub active_to_shadow_names: u64,
    pub shadow_to_active_from_block: Option<i64>,
    pub active_to_shadow_from_block: Option<i64>,
}

impl RecomputeSummary {
    pub fn earliest_transition_block(self) -> Option<i64> {
        match (
            self.shadow_to_active_from_block,
            self.active_to_shadow_from_block,
        ) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(block), None) | (None, Some(block)) => Some(block),
            (None, None) => None,
        }
    }
}

#[derive(Debug, FromRow)]
struct LabelRow {
    labelhash: String,
    raw_label: Vec<u8>,
}

#[derive(Debug, FromRow)]
struct SurfaceRow {
    logical_name_id: String,
    raw_labels: Vec<String>,
    dns_encoded_name: Vec<u8>,
    normalizer_version: String,
    visibility_state: String,
    normalization_errors: Value,
    deactivation_reason: Option<String>,
    deactivated_at: Option<OffsetDateTime>,
    block_number: i64,
    block_timestamp: OffsetDateTime,
    provenance: Value,
    fallback_raw_labels_hex: Option<Value>,
}

#[derive(Debug)]
struct NormalizationFlag {
    normalized: bool,
    error: Option<String>,
}

#[derive(Debug)]
struct SurfaceNormalization {
    visibility_state: &'static str,
    normalization_errors: Value,
    deactivation_reason: Option<&'static str>,
    deactivated_at: Option<OffsetDateTime>,
}

pub(super) async fn run(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<u64> {
    let mut transaction = pool.begin().await.map_err(|error| {
        InterpretError::database("failed to begin normalization-flag recompute", error)
    })?;
    let labels = load_labels(&mut transaction, chain_id, from_block, to_block).await?;
    for label in &labels {
        let flag = normalization_flag(&label.raw_label);
        sqlx::query(
            "UPDATE label_preimages
             SET normalizer_version = $2,
                 normalized_under_version = $3,
                 normalization_error = $4
             WHERE labelhash = $1",
        )
        .bind(&label.labelhash)
        .bind(ENS_NORMALIZER_VERSION)
        .bind(flag.normalized)
        .bind(flag.error)
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to recompute raw label normalization flag", error)
        })?;
    }

    let surfaces = load_surfaces(&mut transaction, chain_id, from_block, to_block).await?;
    let mut same_class_names = 0_u64;
    for surface in &surfaces {
        let desired = surface_normalization(surface)?;
        if desired.visibility_state == surface.visibility_state {
            update_surface(&mut transaction, surface, &desired).await?;
            same_class_names = same_class_names.saturating_add(1);
        }
    }
    transaction.commit().await.map_err(|error| {
        InterpretError::database("failed to commit normalization-flag recompute", error)
    })?;

    let writes = u64::try_from(labels.len())
        .unwrap_or(u64::MAX)
        .saturating_add(same_class_names);
    Ok(writes.saturating_mul(256))
}

pub async fn finalize_recompute_flags(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<RecomputeSummary> {
    let surfaces = load_surfaces(transaction, chain_id, from_block, to_block).await?;
    let mut summary = RecomputeSummary::default();
    for surface in &surfaces {
        let desired = surface_normalization(surface)?;
        match (surface.visibility_state.as_str(), desired.visibility_state) {
            ("shadow", "active") => {
                summary.shadow_to_active_names = summary.shadow_to_active_names.saturating_add(1);
                summary.shadow_to_active_from_block = Some(
                    summary
                        .shadow_to_active_from_block
                        .map_or(surface.block_number, |block| {
                            block.min(surface.block_number)
                        }),
                );
            }
            ("active", "shadow") => {
                summary.active_to_shadow_names = summary.active_to_shadow_names.saturating_add(1);
                summary.active_to_shadow_from_block = Some(
                    summary
                        .active_to_shadow_from_block
                        .map_or(surface.block_number, |block| {
                            block.min(surface.block_number)
                        }),
                );
            }
            (current, desired) if current == desired => {
                summary.same_class_names = summary.same_class_names.saturating_add(1);
            }
            (current, desired) => {
                return Err(InterpretError::data_integrity(format!(
                    "name surface {} has unsupported visibility transition {current} -> {desired}",
                    surface.logical_name_id
                )));
            }
        }
        update_surface(transaction, surface, &desired).await?;
    }
    Ok(summary)
}

async fn load_labels(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<Vec<LabelRow>> {
    sqlx::query_as(&format!(
        "SELECT preimage.labelhash, preimage.raw_label
         FROM label_preimages preimage
         WHERE EXISTS (
                   SELECT 1
                   FROM name_surfaces surface,
                        unnest(surface.labelhashes) AS scoped(labelhash)
                   WHERE surface.chain_id = $1
                     AND surface.block_number BETWEEN $2 AND $3
                     AND scoped.labelhash = preimage.labelhash
               )
            OR (
                preimage.provenance ->> 'chain_id' = $1
                AND preimage.provenance ->> 'block_number' ~ '^[0-9]+$'
                AND (preimage.provenance ->> 'block_number')::bigint BETWEEN $2 AND $3
            )
            OR EXISTS (
                SELECT 1
                FROM normalized_events event
                WHERE event.chain_id = $1
                  AND event.block_number BETWEEN $2 AND $3
                  AND event.event_kind = '{PREIMAGE_OBSERVATION_EVENT_KIND}'
                  AND event.after_state ->> 'labelhash' = preimage.labelhash
            )
         ORDER BY preimage.labelhash
         FOR UPDATE"
    ))
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to load scoped label flags for recompute", error)
    })
}

async fn load_surfaces(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    from_block: i64,
    to_block: i64,
) -> Result<Vec<SurfaceRow>> {
    sqlx::query_as(
        "SELECT surface.logical_name_id, surface.raw_labels,
                surface.dns_encoded_name, surface.normalizer_version,
                surface.visibility_state, surface.normalization_errors,
                surface.deactivation_reason, surface.deactivated_at,
                surface.block_number, lineage.block_timestamp,
                surface.provenance,
                fallback.after_state -> 'raw_labels_hex' AS fallback_raw_labels_hex
         FROM name_surfaces surface
         JOIN chain_lineage lineage
           ON lineage.chain_id = surface.chain_id
          AND lineage.block_hash = surface.block_hash
          AND lineage.block_number = surface.block_number
         LEFT JOIN LATERAL (
             SELECT event.after_state
             FROM normalized_events event
             WHERE event.chain_id = surface.chain_id
               AND event.logical_name_id = surface.logical_name_id
               AND event.after_state ? 'raw_labels_hex'
             ORDER BY event.block_number DESC NULLS LAST,
                      event.transaction_index DESC NULLS LAST,
                      event.log_index DESC NULLS LAST,
                      event.normalized_event_id DESC
             LIMIT 1
         ) fallback ON true
         WHERE surface.chain_id = $1
           AND surface.block_number BETWEEN $2 AND $3
         ORDER BY surface.block_number, surface.logical_name_id
         FOR UPDATE OF surface",
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(to_block)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to load scoped name surfaces for recompute", error)
    })
}

async fn update_surface(
    transaction: &mut Transaction<'_, Postgres>,
    surface: &SurfaceRow,
    desired: &SurfaceNormalization,
) -> Result<()> {
    let result = sqlx::query(
        "UPDATE name_surfaces
         SET normalizer_version = $2,
             visibility_state = $3,
             normalization_errors = $4,
             deactivation_reason = $5,
             deactivated_at = $6
         WHERE logical_name_id = $1
           AND normalizer_version = $7
           AND visibility_state = $8
           AND normalization_errors = $9
           AND deactivation_reason IS NOT DISTINCT FROM $10
           AND deactivated_at IS NOT DISTINCT FROM $11",
    )
    .bind(&surface.logical_name_id)
    .bind(ENS_NORMALIZER_VERSION)
    .bind(desired.visibility_state)
    .bind(&desired.normalization_errors)
    .bind(desired.deactivation_reason)
    .bind(desired.deactivated_at)
    .bind(&surface.normalizer_version)
    .bind(&surface.visibility_state)
    .bind(&surface.normalization_errors)
    .bind(&surface.deactivation_reason)
    .bind(surface.deactivated_at)
    .execute(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to update name-surface normalization flags", error)
    })?;
    if result.rows_affected() != 1 {
        return Err(InterpretError::transient(format!(
            "name surface {} changed while recompute-flags was finalizing; retry the same range",
            surface.logical_name_id
        )));
    }
    Ok(())
}

fn surface_normalization(surface: &SurfaceRow) -> Result<SurfaceNormalization> {
    let raw_labels = raw_surface_labels(surface)?;
    let byte_oriented = surface.fallback_raw_labels_hex.is_some();
    let errors = raw_labels
        .iter()
        .filter_map(|raw_label| {
            let flag = normalization_flag(raw_label);
            flag.error.map(|error| {
                if byte_oriented {
                    let decoded_label = decoded_label(raw_label);
                    json!({
                        "raw_label_hex": hex(raw_label),
                        "decoded_label": decoded_label,
                        "error": error,
                    })
                } else {
                    json!({
                        "raw_label": str::from_utf8(raw_label).unwrap_or_default(),
                        "error": error,
                    })
                }
            })
        })
        .collect::<Vec<_>>();
    let active = errors.is_empty();
    let deactivated_at = if active {
        None
    } else {
        surface.deactivated_at.or_else(|| {
            let log_index = surface
                .provenance
                .get(LOG_INDEX_KEY)
                .and_then(Value::as_i64)
                .unwrap_or(0);
            Some(bigname_adapters::schema_v2::seam::event_time(
                surface.block_timestamp,
                log_index,
            ))
        })
    };
    Ok(SurfaceNormalization {
        visibility_state: if active { "active" } else { "shadow" },
        normalization_errors: Value::Array(errors),
        deactivation_reason: (!active).then_some("normalization_gate"),
        deactivated_at,
    })
}

fn raw_surface_labels(surface: &SurfaceRow) -> Result<Vec<Vec<u8>>> {
    if !surface.raw_labels.is_empty() {
        return Ok(surface
            .raw_labels
            .iter()
            .map(|label| label.as_bytes().to_vec())
            .collect());
    }
    if !surface.dns_encoded_name.is_empty()
        && let Ok(labels) = decode_dns_labels(&surface.dns_encoded_name)
    {
        return Ok(labels);
    }
    let Some(Value::Array(labels)) = surface.fallback_raw_labels_hex.as_ref() else {
        return Err(InterpretError::data_integrity(format!(
            "recompute-flags cannot reconstruct raw labels for name surface {}: the row has no \
             text labels, valid DNS wire name, or retained raw-label event evidence",
            surface.logical_name_id
        )));
    };
    labels
        .iter()
        .map(|label| {
            label
                .as_str()
                .ok_or_else(|| {
                    InterpretError::data_integrity(format!(
                        "name surface {} has a non-string raw-label event value",
                        surface.logical_name_id
                    ))
                })
                .and_then(|label| decode_hex(label, &surface.logical_name_id))
        })
        .collect()
}

fn decode_dns_labels(bytes: &[u8]) -> std::result::Result<Vec<Vec<u8>>, ()> {
    let mut labels = Vec::new();
    let mut cursor = 0_usize;
    loop {
        let length = usize::from(*bytes.get(cursor).ok_or(())?);
        cursor = cursor.checked_add(1).ok_or(())?;
        if length == 0 {
            return (cursor == bytes.len() && !labels.is_empty())
                .then_some(labels)
                .ok_or(());
        }
        let end = cursor.checked_add(length).ok_or(())?;
        labels.push(bytes.get(cursor..end).ok_or(())?.to_vec());
        cursor = end;
    }
}

fn decode_hex(value: &str, logical_name_id: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(InterpretError::data_integrity(format!(
            "name surface {logical_name_id} has an odd-length raw-label hex value"
        )));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0]).ok_or_else(|| {
                InterpretError::data_integrity(format!(
                    "name surface {logical_name_id} has invalid raw-label hex"
                ))
            })?;
            let low = hex_nibble(pair[1]).ok_or_else(|| {
                InterpretError::data_integrity(format!(
                    "name surface {logical_name_id} has invalid raw-label hex"
                ))
            })?;
            Ok((high << 4) | low)
        })
        .collect()
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decoded_label(raw_label: &[u8]) -> Option<&str> {
    str::from_utf8(raw_label)
        .ok()
        .filter(|label| !label.contains('\0'))
}

fn normalization_flag(raw_label: &[u8]) -> NormalizationFlag {
    let Some(raw_label) = decoded_label(raw_label) else {
        return NormalizationFlag {
            normalized: false,
            error: Some("raw label has no PostgreSQL-safe UTF-8 decoding".to_owned()),
        };
    };
    match normalize_label_under_suffix(raw_label, &[]) {
        Ok(normalized) if normalized.normalized_name.as_bytes() == raw_label.as_bytes() => {
            NormalizationFlag {
                normalized: true,
                error: None,
            }
        }
        Ok(_) => NormalizationFlag {
            normalized: false,
            error: Some("raw label is not byte-identical to its normalized form".to_owned()),
        },
        Err(error) => NormalizationFlag {
            normalized: false,
            error: Some(error.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_labels_and_hex_fallback_round_trip_raw_bytes() {
        assert_eq!(
            decode_dns_labels(&[5, b'a', b'l', b'i', b'c', b'e', 3, b'e', b't', b'h', 0]),
            Ok(vec![b"alice".to_vec(), b"eth".to_vec()])
        );
        assert_eq!(decode_hex("00ff41", "ens:test").unwrap(), [0, 255, 65]);
    }

    #[test]
    fn label_flag_matches_the_interpreter_normalization_gate() {
        assert!(normalization_flag(b"alice").normalized);
        assert_eq!(
            normalization_flag(b"Alice").error.as_deref(),
            Some("raw label is not byte-identical to its normalized form")
        );
        assert!(normalization_flag(&[0xff]).error.is_some());
    }
}
