//! Facts the family readers join that the family rows do not hold: the attribution columns of an
//! event read back by its event identity, the block hash and timestamp of a block, the resolver
//! classification with its declaration manifest, and text ordering in the database collation.
use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{PgPool, Row};

/// Attribution columns of one event, read from `normalized_events` by its event identity, which
/// is unique there. The family rows keep only a secondary position (block, transaction, log and
/// event identity) for the events they do not own, so the normalized event id, the logical name
/// and the payload of such an event are read back this way.
#[derive(Clone, Debug)]
pub(crate) struct ProbedEvent {
    pub(crate) normalized_event_id: i64,
    pub(crate) logical_name_id: Option<String>,
    pub(crate) source_family: String,
    pub(crate) after_state: Value,
}

pub(crate) async fn probe_events(
    pool: &PgPool,
    identities: &[String],
) -> Result<HashMap<String, ProbedEvent>> {
    if identities.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = sqlx::query(
        "SELECT event_identity, normalized_event_id, logical_name_id, source_family,
                after_state
         FROM bigname_phase.normalized_events
         WHERE event_identity = ANY($1::text[])",
    )
    .bind(identities)
    .fetch_all(pool)
    .await
    .context("failed to read family events back by event identity")?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("event_identity")?,
                ProbedEvent {
                    normalized_event_id: row.try_get("normalized_event_id")?,
                    logical_name_id: row.try_get("logical_name_id")?,
                    source_family: row.try_get("source_family")?,
                    after_state: row.try_get("after_state")?,
                },
            ))
        })
        .collect()
}

/// A readable block: its hash and the chain position object the served rows carry for it,
/// `{chain_id, block_number, block_hash, timestamp}` with nulls stripped.
#[derive(Clone, Debug)]
pub(crate) struct BlockStamp {
    pub(crate) block_hash: String,
    pub(crate) chain_position: Value,
}

pub(crate) async fn block_stamps(
    pool: &PgPool,
    chain_id: &str,
    blocks: &[i64],
) -> Result<BTreeMap<i64, BlockStamp>> {
    if blocks.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query(
        "SELECT block_number, block_hash,
                jsonb_strip_nulls(jsonb_build_object(
                    'chain_id', chain_id, 'block_number', block_number,
                    'block_hash', block_hash, 'timestamp', block_timestamp
                )) AS chain_position
         FROM bigname_phase.chain_lineage
         WHERE chain_id = $1 AND block_number = ANY($2::bigint[])
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain_id)
    .bind(blocks)
    .fetch_all(pool)
    .await
    .context("failed to read the readable blocks of family rows")?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("block_number")?,
                BlockStamp {
                    block_hash: row.try_get("block_hash")?,
                    chain_position: row.try_get("chain_position")?,
                },
            ))
        })
        .collect()
}

/// The chain position object of `block`, `{chain_id}` alone when the block is not readable, as
/// the served rows' stripped left join leaves it.
pub(crate) fn chain_position(
    stamps: &BTreeMap<i64, BlockStamp>,
    chain_id: &str,
    block: i64,
) -> Value {
    stamps.get(&block).map_or_else(
        || serde_json::json!({"chain_id": chain_id}),
        |stamp| stamp.chain_position.clone(),
    )
}

/// A resolver's classification as the served record inventory reads it: the classification
/// object, its support status and reason, and the namespace of its declaration manifest when that
/// manifest is admitted (latest `SourceManifestUpdated` active with a payload, at or before the
/// block the families stand at).
///
/// The switch to the owned key family F3 (`project_resolver_classification`) is per resolver: a
/// resolver with an F3 row is read from it, and a resolver without one is read from
/// `resolver_current`. Either way the declaration manifest's namespace is read from the manifest
/// events the way today's builders read it; F3's own `admission_namespace` is the resolver
/// edge's admission, not the declaration's.
#[derive(Clone, Debug, Default)]
pub struct ResolverClassification {
    pub classification: Value,
    pub support_status: Option<String>,
    pub unsupported_reason: Option<String>,
    pub manifest_id: Option<i64>,
    pub declaration_namespace: Option<String>,
}

impl ResolverClassification {
    /// A text field of the classification object.
    pub fn field(&self, field: &str) -> Option<&str> {
        self.classification.get(field).and_then(Value::as_str)
    }

    /// Whether the resolver is supported.
    pub fn supported(&self) -> bool {
        self.support_status.as_deref() == Some("supported")
    }

    /// Whether the classification lists `feature` among its read features.
    pub fn has_read_feature(&self, feature: &str) -> bool {
        self.classification
            .get("read_features")
            .and_then(Value::as_array)
            .is_some_and(|features| features.iter().any(|value| value == feature))
    }

    /// Whether the declaration manifest is admitted in `namespace`.
    pub fn declared_in(&self, namespace: &str) -> bool {
        self.declaration_namespace.as_deref() == Some(namespace)
    }
}

pub async fn load_classification(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
) -> Result<Option<ResolverClassification>> {
    // Both sources key resolvers lower-case. The F3 row when the resolver has one, else
    // resolver_current; either way the declaration's namespace comes from its manifest, admitted
    // at the block the families stand at. F3 keeps a `resolver_manifest_not_active` row for a
    // resolver the served build leaves out; resolver_current has no row for it either, so it
    // reads as unclassified, as today.
    let row = sqlx::query(
        "WITH source AS (
             SELECT classification, support_status, unsupported_reason, manifest_id
             FROM (
                 SELECT family.classification, family.support_status,
                        family.unsupported_reason, family.manifest_id, 0 AS preference
                 FROM bigname_phase.project_resolver_classification family
                 WHERE family.chain_id = $1 AND family.resolver_address = $2
                   AND family.unsupported_reason IS DISTINCT FROM 'resolver_manifest_not_active'
                 UNION ALL
                 SELECT resolver.declared_summary -> 'classification',
                        resolver.support_status, resolver.unsupported_reason,
                        (resolver.provenance ->> 'manifest_id')::bigint, 1
                 FROM bigname_phase.resolver_current resolver
                 WHERE resolver.chain_id = $1 AND resolver.resolver_address = $2
             ) candidates
             ORDER BY preference
             LIMIT 1
         )
         SELECT source.classification, source.support_status, source.unsupported_reason,
                source.manifest_id, declaration.namespace AS declaration_namespace
         FROM source
         LEFT JOIN (
             SELECT current_block_number AS block FROM bigname_phase.project_family_marker
             WHERE chain_id = $1
         ) marker ON TRUE
         LEFT JOIN LATERAL (
             SELECT manifest.namespace,
                    manifest.after_state ->> 'rollout_status' = 'active'
                        AND manifest.after_state -> 'manifest_payload' IS NOT NULL AS active
             FROM bigname_phase.normalized_events manifest
             LEFT JOIN bigname_phase.chain_lineage lineage
               ON lineage.chain_id = manifest.chain_id
              AND lineage.block_hash = manifest.block_hash
              AND lineage.block_number = manifest.block_number
             WHERE manifest.event_kind = 'SourceManifestUpdated'
               AND manifest.source_manifest_id = source.manifest_id
               AND (manifest.chain_id = $1
                    OR ($1 = 'base-mainnet' AND manifest.namespace = 'basenames'
                        AND manifest.source_family = 'basenames_execution'
                        AND manifest.chain_id = 'ethereum-mainnet'))
               AND manifest.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND (manifest.block_hash IS NULL
                    OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
               AND (manifest.block_number IS NULL OR marker.block IS NULL
                    OR manifest.block_number <= marker.block)
             ORDER BY manifest.normalized_event_id DESC
             LIMIT 1
         ) declaration ON declaration.active",
    )
    .bind(chain_id)
    .bind(resolver_address.to_ascii_lowercase())
    .fetch_optional(pool)
    .await
    .with_context(|| format!("failed to load the classification of resolver {resolver_address}"))?;
    row.map(|row| {
        Ok(ResolverClassification {
            classification: row
                .try_get::<Option<Value>, _>("classification")?
                .unwrap_or(Value::Null),
            support_status: row.try_get("support_status")?,
            unsupported_reason: row.try_get("unsupported_reason")?,
            manifest_id: row.try_get("manifest_id")?,
            declaration_namespace: row.try_get("declaration_namespace")?,
        })
    })
    .transpose()
}

/// `texts` in the order `ORDER BY` gives them in the database collation, which the served rows
/// use for their record keys and family names.
pub(crate) async fn collation_order(pool: &PgPool, texts: Vec<String>) -> Result<Vec<String>> {
    if texts.len() < 2 {
        return Ok(texts);
    }
    sqlx::query_scalar("SELECT text FROM unnest($1::text[]) text ORDER BY text")
        .bind(texts)
        .fetch_all(pool)
        .await
        .context("failed to order family record keys")
}
