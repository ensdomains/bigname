use anyhow::{Context, Result, bail};
use bigname_domain::normalization::normalize_label_under_suffix;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Row, Transaction};

use crate::identity::NameSurface;
use crate::normalized_events::NormalizedEvent;

mod backfill;

pub use backfill::backfill_label_preimages_from_existing_facts;

const NORMALIZED_EVENT_PREIMAGE_SOURCE_KIND: &str = "normalized_event_preimage";
const NAME_SURFACE_SOURCE_KIND: &str = "name_surface";
const ENS_RAINBOW_SOURCE_KIND: &str = "ens_rainbow_import";
const NORMALIZED_EVENT_PREIMAGE_PRIORITY: i32 = 100;
const NAME_SURFACE_PRIORITY: i32 = 90;
const ENS_RAINBOW_PRIORITY: i32 = 10;
const IMPORT_BATCH_SIZE: i64 = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LabelPreimage {
    pub labelhash: String,
    pub label: String,
    pub normalized_label: String,
    pub canonical_display_label: String,
    pub source_kind: String,
    pub source_priority: i32,
    pub provenance: Value,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LabelPreimageImportSummary {
    pub scanned_row_count: u64,
    pub retained_row_count: u64,
    pub invalidated_parent_count: u64,
}

pub fn label_preimage_from_label(
    label: &str,
    source_kind: &str,
    source_priority: i32,
    provenance: Value,
) -> Result<LabelPreimage> {
    let normalized = normalize_label_under_suffix(label, &[])
        .with_context(|| format!("failed to normalize label preimage {label:?}"))?;
    let normalized_label = normalized
        .normalized_labels
        .first()
        .cloned()
        .context("normalized label preimage is missing the label")?;
    if normalized.normalized_labels.len() != 1 {
        bail!("label preimage {label:?} normalized to more than one label");
    }
    let canonical_display_label = normalized.canonical_display_name;
    let labelhash = labelhash_hex(normalized_label.as_bytes());

    Ok(LabelPreimage {
        labelhash,
        label: label.to_owned(),
        normalized_label,
        canonical_display_label,
        source_kind: source_kind.to_owned(),
        source_priority,
        provenance,
    })
}

pub async fn upsert_label_preimages(
    pool: &PgPool,
    preimages: &[LabelPreimage],
) -> Result<Vec<String>> {
    let (changed_labelhashes, _) = upsert_label_preimages_with_invalidation_count(pool, preimages)
        .await
        .context("failed to upsert label preimages")?;
    Ok(changed_labelhashes)
}

async fn upsert_label_preimages_with_invalidation_count(
    pool: &PgPool,
    preimages: &[LabelPreimage],
) -> Result<(Vec<String>, u64)> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for label preimage upsert")?;
    let (changed_labelhashes, invalidated_parent_count) =
        upsert_label_preimages_with_invalidation_count_in_transaction(&mut transaction, preimages)
            .await
            .context("failed to upsert label preimages")?;
    transaction
        .commit()
        .await
        .context("failed to commit label preimage upsert")?;
    Ok((changed_labelhashes, invalidated_parent_count))
}

pub async fn upsert_label_preimages_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    preimages: &[LabelPreimage],
) -> Result<Vec<String>> {
    let (changed_labelhashes, _) =
        upsert_label_preimages_with_invalidation_count_in_transaction(transaction, preimages)
            .await?;
    Ok(changed_labelhashes)
}

async fn upsert_label_preimages_with_invalidation_count_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    preimages: &[LabelPreimage],
) -> Result<(Vec<String>, u64)> {
    let changed_labelhashes = insert_label_preimages_in_transaction(transaction, preimages).await?;
    let invalidated_parent_count =
        enqueue_children_invalidations_for_labelhashes(transaction, &changed_labelhashes).await?;

    Ok((changed_labelhashes, invalidated_parent_count))
}

pub(super) async fn upsert_label_preimages_without_invalidations(
    pool: &PgPool,
    preimages: &[LabelPreimage],
) -> Result<Vec<String>> {
    let mut transaction = pool
        .begin()
        .await
        .context("failed to open transaction for label preimage upsert")?;
    let changed_labelhashes = insert_label_preimages_in_transaction(&mut transaction, preimages)
        .await
        .context("failed to upsert label preimages")?;
    transaction
        .commit()
        .await
        .context("failed to commit label preimage upsert")?;
    Ok(changed_labelhashes)
}

async fn insert_label_preimages_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    preimages: &[LabelPreimage],
) -> Result<Vec<String>> {
    if preimages.is_empty() {
        return Ok(Vec::new());
    }

    for preimage in preimages {
        validate_label_preimage(preimage)?;
    }

    let labelhashes = preimages
        .iter()
        .map(|preimage| preimage.labelhash.clone())
        .collect::<Vec<_>>();
    let labels = preimages
        .iter()
        .map(|preimage| preimage.label.clone())
        .collect::<Vec<_>>();
    let normalized_labels = preimages
        .iter()
        .map(|preimage| preimage.normalized_label.clone())
        .collect::<Vec<_>>();
    let canonical_display_labels = preimages
        .iter()
        .map(|preimage| preimage.canonical_display_label.clone())
        .collect::<Vec<_>>();
    let source_kinds = preimages
        .iter()
        .map(|preimage| preimage.source_kind.clone())
        .collect::<Vec<_>>();
    let source_priorities = preimages
        .iter()
        .map(|preimage| preimage.source_priority)
        .collect::<Vec<_>>();
    let provenances = preimages
        .iter()
        .map(|preimage| {
            serialize_jsonb_value(
                &preimage.provenance,
                "failed to serialize label preimage provenance",
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let changed_labelhashes = sqlx::query_scalar::<_, String>(
        r#"
        WITH input AS (
            SELECT DISTINCT ON (labelhash)
                labelhash,
                label,
                normalized_label,
                canonical_display_label,
                source_kind,
                source_priority,
                provenance
            FROM unnest(
                $1::TEXT[],
                $2::TEXT[],
                $3::TEXT[],
                $4::TEXT[],
                $5::TEXT[],
                $6::INTEGER[],
                $7::TEXT[]
            ) AS input(
                labelhash,
                label,
                normalized_label,
                canonical_display_label,
                source_kind,
                source_priority,
                provenance
            )
            ORDER BY labelhash, source_priority DESC
        ),
        upserted AS (
            INSERT INTO label_preimages (
                labelhash,
                label,
                normalized_label,
                canonical_display_label,
                source_kind,
                source_priority,
                provenance,
                observed_at
            )
            SELECT
                labelhash,
                label,
                normalized_label,
                canonical_display_label,
                source_kind,
                source_priority,
                provenance::JSONB,
                now()
            FROM input
            ON CONFLICT (labelhash) DO NOTHING
            RETURNING labelhash
        )
        SELECT labelhash
        FROM upserted
        "#,
    )
    .bind(&labelhashes)
    .bind(&labels)
    .bind(&normalized_labels)
    .bind(&canonical_display_labels)
    .bind(&source_kinds)
    .bind(&source_priorities)
    .bind(&provenances)
    .fetch_all(&mut **transaction)
    .await
    .context("failed to insert label preimages")?;

    Ok(changed_labelhashes)
}

pub async fn upsert_label_preimages_from_normalized_events(
    transaction: &mut Transaction<'_, Postgres>,
    events: &[NormalizedEvent],
) -> Result<Vec<String>> {
    let mut preimages = Vec::new();
    for event in events {
        if let Some(event_preimages) = label_preimages_from_normalized_event(event) {
            preimages.extend(event_preimages?);
        }
    }
    upsert_label_preimages_in_transaction(transaction, &preimages).await
}

pub(crate) async fn upsert_label_preimages_from_name_surfaces(
    transaction: &mut Transaction<'_, Postgres>,
    name_surfaces: &[NameSurface],
) -> Result<Vec<String>> {
    let mut preimages = Vec::new();
    for surface in name_surfaces {
        preimages.extend(label_preimages_from_name_surface(surface)?);
    }
    upsert_label_preimages_in_transaction(transaction, &preimages).await
}

pub async fn import_label_preimages_from_ens_names_table(
    pool: &PgPool,
    batch_size: Option<i64>,
    limit: Option<i64>,
) -> Result<LabelPreimageImportSummary> {
    let batch_size = batch_size.unwrap_or(IMPORT_BATCH_SIZE).max(1);
    let mut summary = LabelPreimageImportSummary::default();
    let mut last_hash = String::new();

    loop {
        let remaining_limit =
            limit.map(|limit| limit.saturating_sub(summary.scanned_row_count as i64));
        if remaining_limit == Some(0) {
            break;
        }
        let effective_batch_size = remaining_limit
            .map(|remaining| remaining.min(batch_size))
            .unwrap_or(batch_size);

        let rows = sqlx::query(
            r#"
            SELECT hash, name
            FROM ens_names
            WHERE hash > $1
            ORDER BY hash ASC
            LIMIT $2
            "#,
        )
        .bind(&last_hash)
        .bind(effective_batch_size)
        .fetch_all(pool)
        .await
        .context("failed to load rows from ens_names rainbow table")?;

        if rows.is_empty() {
            break;
        }

        let mut preimages = Vec::new();
        for row in &rows {
            let hash: String = row.try_get("hash")?;
            let name: String = row.try_get("name")?;
            last_hash = hash.clone();
            summary.scanned_row_count += 1;

            let Ok(mut preimage) = label_preimage_from_label(
                &name,
                ENS_RAINBOW_SOURCE_KIND,
                ENS_RAINBOW_PRIORITY,
                json!({
                    "source": "ens_rainbow",
                    "table": "ens_names",
                }),
            ) else {
                continue;
            };
            if preimage.labelhash != hash.to_ascii_lowercase() {
                continue;
            }
            preimage.labelhash = hash.to_ascii_lowercase();
            preimages.push(preimage);
        }

        let changed = upsert_label_preimages_without_invalidations(pool, &preimages).await?;
        summary.retained_row_count += changed.len() as u64;
    }
    summary.invalidated_parent_count +=
        enqueue_children_invalidations_for_existing_label_preimages(pool).await?;

    Ok(summary)
}

pub(super) async fn enqueue_children_invalidations_for_existing_label_preimages(
    pool: &PgPool,
) -> Result<u64> {
    sqlx::query(
        r#"
        WITH candidate_keys AS (
            SELECT DISTINCT
                'children_current'::TEXT AS projection,
                parent.logical_name_id AS projection_key,
                jsonb_build_object('parent_logical_name_id', parent.logical_name_id) AS key_payload
            FROM label_preimages preimage
            JOIN normalized_events ne
              ON lower(ne.after_state ->> 'labelhash') = preimage.labelhash
            JOIN name_surfaces parent
              ON parent.namehash = ne.after_state ->> 'parent_node'
             AND parent.namespace = ne.namespace
             AND parent.chain_id = ne.chain_id
             AND parent.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
             )
            WHERE ne.event_kind = 'SubregistryChanged'
              AND ne.derivation_kind = 'ens_v1_subregistry_changed'
              AND ne.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND ne.after_state ->> 'parent_node' IS NOT NULL
              AND ne.after_state ->> 'child_node' IS NOT NULL
              AND ne.after_state ->> 'labelhash' IS NOT NULL
        )
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            invalidated_at,
            last_changed_at
        )
        SELECT
            projection,
            projection_key,
            key_payload,
            now(),
            now()
        FROM candidate_keys
        ON CONFLICT (projection, projection_key)
        DO UPDATE SET
            key_payload = EXCLUDED.key_payload,
            generation = projection_invalidations.generation + 1,
            invalidated_at = EXCLUDED.invalidated_at,
            last_changed_at = EXCLUDED.last_changed_at,
            claim_token = NULL,
            claimed_at = NULL,
            last_failure_reason = NULL,
            last_failure_at = NULL
        "#,
    )
    .execute(pool)
    .await
    .context("failed to enqueue children_current invalidations for existing label preimages")
    .map(|result| result.rows_affected())
}

async fn enqueue_children_invalidations_for_labelhashes(
    transaction: &mut Transaction<'_, Postgres>,
    labelhashes: &[String],
) -> Result<u64> {
    if labelhashes.is_empty() {
        return Ok(0);
    }

    sqlx::query(
        r#"
        WITH changed_labelhashes AS (
            SELECT DISTINCT lower(labelhash) AS labelhash
            FROM unnest($1::TEXT[]) AS input(labelhash)
        ),
        candidate_keys AS (
            SELECT DISTINCT
                'children_current'::TEXT AS projection,
                parent.logical_name_id AS projection_key,
                jsonb_build_object('parent_logical_name_id', parent.logical_name_id) AS key_payload
            FROM changed_labelhashes changed
            JOIN normalized_events ne
              ON lower(ne.after_state ->> 'labelhash') = changed.labelhash
            JOIN name_surfaces parent
              ON parent.namehash = ne.after_state ->> 'parent_node'
             AND parent.namespace = ne.namespace
             AND parent.chain_id = ne.chain_id
             AND parent.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
             )
            WHERE ne.event_kind = 'SubregistryChanged'
              AND ne.derivation_kind = 'ens_v1_subregistry_changed'
              AND ne.source_family IN ('ens_v1_registry_l1', 'basenames_base_registry')
              AND ne.canonicality_state IN (
                  'canonical'::canonicality_state,
                  'safe'::canonicality_state,
                  'finalized'::canonicality_state
              )
              AND ne.after_state ->> 'parent_node' IS NOT NULL
              AND ne.after_state ->> 'child_node' IS NOT NULL
              AND ne.after_state ->> 'labelhash' IS NOT NULL
        )
        INSERT INTO projection_invalidations (
            projection,
            projection_key,
            key_payload,
            invalidated_at,
            last_changed_at
        )
        SELECT
            projection,
            projection_key,
            key_payload,
            now(),
            now()
        FROM candidate_keys
        ON CONFLICT (projection, projection_key)
        DO UPDATE SET
            key_payload = EXCLUDED.key_payload,
            generation = projection_invalidations.generation + 1,
            invalidated_at = EXCLUDED.invalidated_at,
            last_changed_at = EXCLUDED.last_changed_at,
            claim_token = NULL,
            claimed_at = NULL,
            last_failure_reason = NULL,
            last_failure_at = NULL
        "#,
    )
    .bind(labelhashes)
    .execute(&mut **transaction)
    .await
    .context("failed to enqueue children_current invalidations for label preimages")
    .map(|result| result.rows_affected())
}

fn label_preimages_from_normalized_event(
    event: &NormalizedEvent,
) -> Option<Result<Vec<LabelPreimage>>> {
    if event.event_kind != "PreimageObserved" {
        return None;
    }
    let decoded_name = event.after_state.get("decoded_name")?.as_str()?;
    let labelhashes = event.after_state.get("labelhashes")?.as_array()?;
    let mut preimages = Vec::new();
    for (label_index, (label, labelhash)) in
        decoded_name.split('.').zip(labelhashes.iter()).enumerate()
    {
        let labelhash = labelhash.as_str().with_context(|| {
            format!(
                "normalized preimage labelhash at index {label_index} is not a string for {}",
                event.event_identity
            )
        });
        let Ok(labelhash) = labelhash else {
            continue;
        };
        let labelhash = labelhash.to_ascii_lowercase();
        let Ok(mut preimage) = label_preimage_from_label(
            label,
            NORMALIZED_EVENT_PREIMAGE_SOURCE_KIND,
            NORMALIZED_EVENT_PREIMAGE_PRIORITY,
            json!({
                "source": "normalized_event",
                "normalized_event_identity": event.event_identity,
                "source_family": event.source_family,
                "derivation_kind": event.derivation_kind,
                "label_index": label_index,
            }),
        ) else {
            continue;
        };
        if preimage.labelhash != labelhash {
            continue;
        }
        preimage.labelhash = labelhash;
        preimages.push(preimage);
    }
    Some(Ok(preimages))
}

fn label_preimages_from_name_surface(surface: &NameSurface) -> Result<Vec<LabelPreimage>> {
    let mut preimages = Vec::new();
    for (label_index, (label, labelhash)) in surface
        .normalized_name
        .split('.')
        .zip(surface.labelhashes.iter())
        .enumerate()
    {
        let labelhash = labelhash.to_ascii_lowercase();
        let Ok(mut preimage) = label_preimage_from_label(
            label,
            NAME_SURFACE_SOURCE_KIND,
            NAME_SURFACE_PRIORITY,
            json!({
                "source": "name_surface",
                "logical_name_id": surface.logical_name_id,
                "namespace": surface.namespace,
                "chain_id": surface.chain_id,
                "block_hash": surface.block_hash,
                "block_number": surface.block_number,
                "label_index": label_index,
            }),
        ) else {
            continue;
        };
        if preimage.labelhash != labelhash {
            continue;
        }
        preimage.labelhash = labelhash;
        preimages.push(preimage);
    }
    Ok(preimages)
}

fn validate_label_preimage(preimage: &LabelPreimage) -> Result<()> {
    if !preimage.labelhash.starts_with("0x")
        || preimage.labelhash.len() != 66
        || !preimage.labelhash[2..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || preimage.labelhash != preimage.labelhash.to_ascii_lowercase()
    {
        bail!("invalid label preimage hash {}", preimage.labelhash);
    }
    if preimage.label.trim().is_empty()
        || preimage.normalized_label.trim().is_empty()
        || preimage.canonical_display_label.trim().is_empty()
        || preimage.normalized_label.contains('.')
    {
        bail!("invalid label preimage for {}", preimage.labelhash);
    }
    if preimage.source_kind.trim().is_empty() || preimage.source_priority < 0 {
        bail!("invalid label preimage source for {}", preimage.labelhash);
    }
    if !preimage.provenance.is_object() {
        bail!(
            "label preimage {} must store provenance as a JSON object",
            preimage.labelhash
        );
    }
    let normalized = normalize_label_under_suffix(&preimage.label, &[])
        .with_context(|| format!("failed to normalize label preimage {:?}", preimage.label))?;
    if normalized.normalized_labels.len() != 1 {
        bail!(
            "label preimage {:?} normalized to more than one label",
            preimage.label
        );
    }
    let normalized_label = normalized
        .normalized_labels
        .first()
        .context("normalized label preimage is missing the label")?;
    if normalized_label != &preimage.normalized_label
        || normalized.canonical_display_name != preimage.canonical_display_label
    {
        bail!(
            "label preimage normalized label mismatch for {}",
            preimage.labelhash
        );
    }
    let expected_labelhash = labelhash_hex(preimage.normalized_label.as_bytes());
    if expected_labelhash != preimage.labelhash {
        bail!(
            "label preimage labelhash mismatch for {}: expected {}",
            preimage.labelhash,
            expected_labelhash
        );
    }
    Ok(())
}

fn labelhash_hex(bytes: &[u8]) -> String {
    format!(
        "0x{}",
        alloy_primitives::hex::encode(alloy_primitives::keccak256(bytes))
    )
}

fn serialize_jsonb_value(value: &Value, context: &str) -> Result<String> {
    serde_json::to_string(value).context(context.to_owned())
}

#[cfg(test)]
mod tests;
