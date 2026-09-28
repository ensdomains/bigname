//! The resolver overview row from the F3 classification (`project_resolver_classification`),
//! in the shape of the served `resolver_current` row the resolver routes read (TYR-36 step 7b
//! slice 4, packet E5). The classification, support and declaring manifest are the F3 row's; the
//! section support the routes gate on (`bindings`, `aliases`, `links`, `permissions`,
//! `role_holders`) is derived from it by the served build's rule (builders/resolver/build.sql,
//! `summarized`, and section_summaries.rs), without the counts and samples the routes no longer
//! read:
//!
//! - enumeration is supported for a supported resolver that is neither an ENSv1 resolver nor a
//!   `public_resolver_v2`; such a supported resolver reports
//!   `resolver_binding_enumeration_not_projected`, an unsupported one its own reason;
//! - record links are supported for a supported ENSv2 resolver classified through an `Upgraded`
//!   implementation (neither `public_resolver_v2` nor the ENSv1 mirror) whose declaring
//!   manifest's ABI maps an event to `ResolverRecordLinked`; another supported resolver reports
//!   `record_links_not_applicable`.
//!
//! The declaring manifest's ABI is read by key from the manifest update event F3 names
//! (`manifest_event_id`). A row F3 keeps for a resolver whose family has no active manifest
//! (`resolver_manifest_not_active`) is one the served build leaves out, so it is not served. The
//! row describes the family marker's publication: its `chain_positions` name the marker's block.
use anyhow::{Context, Result};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

use crate::{
    ResolverCurrentRow,
    families::name::{FamilyPublicationUnavailable, publication_on, read_snapshot},
};

/// The `declared_summary` of an F3 row aliased `classification_row`, as a SQL expression: the
/// classification without its F3-only `upgrade`, the section support and the summary version.
pub(crate) const FAMILY_RESOLVER_SUMMARY: &str = r#"(
    WITH facts AS (
        SELECT classification_row.support_status = 'supported' AS supported,
               classification_row.classification ->> 'source_family' AS source_family,
               classification_row.classification ->> 'role' AS role
    ), derived AS (
        SELECT facts.*,
               facts.supported AND facts.source_family <> 'ens_v1_resolver_l1'
                   AND facts.role IS DISTINCT FROM 'public_resolver_v2' AS enumeration_supported,
               CASE WHEN facts.supported
                         AND (facts.source_family = 'ens_v1_resolver_l1'
                              OR facts.role = 'public_resolver_v2')
                        THEN 'resolver_binding_enumeration_not_projected'
                    ELSE classification_row.unsupported_reason END AS enumeration_reason,
               facts.supported AND facts.source_family = 'ens_v2_resolver_l1'
                   AND classification_row.classification ? 'upgrade'
                   AND COALESCE(facts.role, '')
                       NOT IN ('public_resolver_v2', 'ensv1_mirror_resolver')
                   AND EXISTS (
                       SELECT 1
                       FROM bigname_phase.normalized_events manifest
                       CROSS JOIN LATERAL jsonb_array_elements(COALESCE(
                           manifest.after_state #> '{manifest_payload,abi,events}',
                           '[]'::jsonb)) abi_event
                       WHERE manifest.normalized_event_id = classification_row.manifest_event_id
                         AND abi_event -> 'normalized_events' ? 'ResolverRecordLinked'
                   ) AS links_supported
        FROM facts
    ), sections AS (
        SELECT derived.*,
               CASE WHEN derived.enumeration_supported
                    THEN jsonb_build_object('status', 'supported')
                    ELSE jsonb_build_object('status', 'unsupported',
                                            'unsupported_reason', derived.enumeration_reason)
               END AS enumeration
        FROM derived
    )
    SELECT jsonb_build_object(
        'classification', COALESCE(classification_row.classification, '{}'::jsonb) - 'upgrade',
        'bindings', sections.enumeration,
        'aliases', sections.enumeration,
        'permissions', sections.enumeration,
        'role_holders', sections.enumeration,
        'event_summary', sections.enumeration,
        'links', CASE WHEN sections.links_supported
                      THEN jsonb_build_object('status', 'supported')
                      WHEN sections.supported
                      THEN jsonb_build_object('status', 'unsupported',
                                              'unsupported_reason', 'record_links_not_applicable')
                      ELSE sections.enumeration END,
        'summary_version', classification_row.summary_version)
    FROM sections
)"#;

/// The F3 rows the served build would write: every row but a `resolver_manifest_not_active` one.
pub(crate) const FAMILY_RESOLVER_SERVED_ROWS: &str =
    "classification_row.unsupported_reason IS DISTINCT FROM 'resolver_manifest_not_active'";

/// The relation a statement joins as `resolver_current` for a resolver's classification: the
/// served table with the publication switch off; with it on, the F3 rows the served build would
/// write, with the served table's `chain_id`, `resolver_address`, `support_status`,
/// `declared_summary -> 'classification'` and `provenance ->> 'manifest_id'`. Only those columns
/// may be read through it.
pub(crate) fn resolver_classification_relation() -> String {
    if !crate::publication_source::serve_from_families() {
        return "bigname_phase.resolver_current".to_owned();
    }
    format!(
        "(SELECT classification_row.chain_id, classification_row.resolver_address,
                 classification_row.support_status,
                 jsonb_build_object('classification',
                     COALESCE(classification_row.classification, '{{}}'::jsonb) - 'upgrade')
                     AS declared_summary,
                 jsonb_build_object('manifest_id', classification_row.manifest_id) AS provenance
          FROM bigname_phase.project_resolver_classification classification_row
          WHERE {FAMILY_RESOLVER_SERVED_ROWS})"
    )
}

/// The overview row of `resolver_address` on `chain_id` from F3, at the family marker's
/// publication; none when F3 holds no servable row for it.
pub async fn load_family_resolver_current(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<Option<ResolverCurrentRow>> {
    let address = resolver_address.to_ascii_lowercase();
    let mut snapshot = read_snapshot(pool).await?;
    let Some(publication) = publication_on(&mut snapshot, chain_id).await? else {
        return Err(FamilyPublicationUnavailable {
            chain_id: chain_id.to_owned(),
        }
        .into());
    };
    let row = sqlx::query(&format!(
        "/* storage:families.topology.resolver_overview */
         SELECT {FAMILY_RESOLVER_SUMMARY} AS declared_summary,
                classification_row.support_status, classification_row.unsupported_reason,
                jsonb_strip_nulls(jsonb_build_object(
                    'chain_id', classification_row.chain_id,
                    'manifest_id', classification_row.manifest_id,
                    'manifest_event_id', classification_row.manifest_event_id,
                    'classification_admission_namespace',
                        classification_row.admission_namespace)) AS provenance
         FROM bigname_phase.project_resolver_classification classification_row
         WHERE classification_row.chain_id = $1 AND classification_row.resolver_address = $2
           AND {FAMILY_RESOLVER_SERVED_ROWS}"
    ))
    .bind(chain_id)
    .bind(&address)
    .fetch_optional(&mut *snapshot)
    .await
    .with_context(|| format!("failed to load the resolver overview of {chain_id}:{address}"))?;
    snapshot.commit().await?;
    row.map(|row| {
        let support_status: String = row.try_get("support_status")?;
        let unsupported_reason: Option<String> = row.try_get("unsupported_reason")?;
        let coverage = if support_status == "supported" {
            json!({"status": "projected", "exhaustiveness": "not_asserted"})
        } else {
            json!({
                "status": "unsupported",
                "exhaustiveness": "not_asserted",
                "unsupported_reason": unsupported_reason,
            })
        };
        let target = json!({
            "target_block_number": publication.block_number,
            "target_block_hash": publication.block_hash,
        });
        let mut canonicality = target.clone();
        canonicality["state"] = Value::from("canonical_lineage");
        Ok(ResolverCurrentRow {
            chain_id: chain_id.to_owned(),
            resolver_address: address,
            declared_summary: row.try_get("declared_summary")?,
            provenance: row.try_get("provenance")?,
            coverage,
            chain_positions: target,
            canonicality_summary: canonicality,
            manifest_version: 1,
            last_recomputed_at: publication.block_timestamp,
        })
    })
    .transpose()
}
