//! Shared Basenames creation/topology context, read on the caller's publication snapshot.
use super::name::FamilyPublication;
use super::topology::FamilyWildcardSource;
use anyhow::Result;
use serde_json::Value;
use sqlx::{PgConnection, Row};
use std::collections::BTreeMap;
use uuid::Uuid;

pub(crate) struct ExecutionContext {
    pub manifest_version: i64,
    pub block_number: i64,
    pub block_hash: String,
    pub timestamp: Value,
}

/// The Basenames execution admission of the publication's manifest set
/// ([`FamilyPublication::admission_manifests`]), which for `base-mainnet` holds the
/// `ethereum-mainnet` `basenames_execution` manifest, with the latest L1 block at or before the
/// publication's time.
pub(crate) async fn execution(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
) -> Result<Option<ExecutionContext>> {
    let row=sqlx::query(
        "WITH manifests AS (
            SELECT DISTINCT ON (event.source_manifest_id) event.manifest_version,
                event.after_state, event.raw_fact_ref
            FROM bigname_phase.normalized_events event
            LEFT JOIN bigname_phase.chain_lineage lineage ON lineage.chain_id = event.chain_id
                AND lineage.block_number = event.block_number AND lineage.block_hash = event.block_hash
            WHERE event.namespace = 'basenames' AND event.source_family = 'basenames_execution'
                AND event.chain_id = 'ethereum-mainnet' AND event.event_kind = 'SourceManifestUpdated'
                AND event.source_manifest_id IS NOT NULL
                AND event.canonicality_state IN ('canonical','safe','finalized')
                AND (event.block_hash IS NULL OR lineage.canonicality_state IN ('canonical','safe','finalized'))
                AND event.normalized_event_id = ANY($1)
            ORDER BY event.source_manifest_id, event.normalized_event_id DESC
        )
        SELECT manifest.manifest_version, lineage.block_number, lineage.block_hash,
            to_jsonb(lineage.block_timestamp) AS timestamp
        FROM manifests manifest
        CROSS JOIN LATERAL (
            SELECT * FROM bigname_phase.chain_lineage
            WHERE chain_id = 'ethereum-mainnet' AND block_timestamp <= $2
                AND canonicality_state IN ('canonical','safe','finalized')
            ORDER BY block_timestamp DESC, block_number DESC, block_hash DESC LIMIT 1
        ) lineage
        WHERE manifest.manifest_version = 2 AND manifest.after_state ->> 'rollout_status' = 'active'
            AND COALESCE(manifest.after_state #>> '{manifest_payload,deployment_epoch}', manifest.raw_fact_ref ->> 'deployment_epoch') = 'basenames_v1'
            AND manifest.after_state #>> '{manifest_payload,capability_flags,verified_resolution,status}' = 'supported'
            AND EXISTS (SELECT 1 FROM jsonb_array_elements(manifest.after_state #> '{manifest_payload,contracts}') declaration
                WHERE declaration ->> 'role' = 'l1_resolver' AND lower(declaration ->> 'address') = '0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31')
        LIMIT 1")
        .bind(publication.manifest_event_ids()?).bind(publication.block_timestamp)
        .fetch_optional(conn).await?;
    row.map(|row| {
        Ok(ExecutionContext {
            manifest_version: row.try_get("manifest_version")?,
            block_number: row.try_get("block_number")?,
            block_hash: row.try_get("block_hash")?,
            timestamp: row.try_get("timestamp")?,
        })
    })
    .transpose()
}

pub(crate) struct QualifiedPointer {
    pub pointer: FamilyWildcardSource,
    pub event_id: i64,
    pub block_hash: String,
}

/// Same wildcard prerequisites and boundary-event lookup as the single-resource topology path.
/// A manually supplied null address is retained; a missing event omits the pointer, while a
/// present event whose required block hash is null still fails decoding as before.
pub(crate) async fn qualified_pointers(
    conn: &mut PgConnection,
    resources: &[Uuid],
) -> Result<BTreeMap<Uuid, QualifiedPointer>> {
    if resources.is_empty() {
        return Ok(BTreeMap::new());
    }
    let rows = sqlx::query(
        "/* storage:families.name.basenames_creation_pointers */
         SELECT pointer.resource_id, pointer.nonzero_resolver_address, pointer.nonzero_position,
                pointer.boundary_kind, pointer.boundary_position,
                to_jsonb(pointer.boundary_block_timestamp) AS boundary_timestamp,
                event.normalized_event_id AS event_id,event.block_hash
         FROM bigname_phase.project_resource_pointer pointer
         JOIN bigname_phase.normalized_events event
           ON event.event_identity=pointer.boundary_position->>'event_identity'
         WHERE pointer.chain_id='base-mainnet' AND pointer.resource_id=ANY($1::uuid[])
           AND pointer.nonzero_position IS NOT NULL AND pointer.boundary_position IS NOT NULL
           AND pointer.boundary_kind IS NOT NULL",
    )
    .bind(resources)
    .fetch_all(conn)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok((
                row.try_get("resource_id")?,
                QualifiedPointer {
                    pointer: FamilyWildcardSource {
                        nonzero_resolver_address: row.try_get("nonzero_resolver_address")?,
                        nonzero_position: row.try_get("nonzero_position")?,
                        boundary_kind: row.try_get("boundary_kind")?,
                        boundary_position: row.try_get("boundary_position")?,
                        boundary_block_timestamp: row.try_get("boundary_timestamp")?,
                    },
                    event_id: row.try_get("event_id")?,
                    block_hash: row.try_get("block_hash")?,
                },
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use serde_json::json;
    use sqlx::types::time::OffsetDateTime;

    use super::*;

    fn publication(admission_manifests: &str) -> Result<FamilyPublication> {
        Ok(FamilyPublication {
            chain_id: "base-mainnet".into(),
            block_number: 50,
            block_hash: "0xbase".into(),
            block_timestamp: OffsetDateTime::from_unix_timestamp(1_800_000_000)?,
            block_timestamp_json: json!("2027-01-15T08:00:00Z"),
            admission: None,
            admission_manifests: Some(admission_manifests.into()),
        })
    }

    // R21. Manifest sync turns the Ethereum `basenames_execution` manifest to `shadow` with a
    // blockless event. A publication composed before it keeps the L1 transport admission of its
    // own manifest set. The publication the redo writes, whose set holds the new event, has none.
    #[tokio::test]
    async fn the_execution_admission_is_the_publications_manifest_set() -> Result<()> {
        let database =
            TestDatabase::create(TestDatabaseConfig::new("basenames_execution_set")).await?;
        let result = async {
            let pool = database.pool();
            sqlx::raw_sql(
                "CREATE SCHEMA bigname_phase;
                 CREATE TABLE bigname_phase.normalized_events (
                     normalized_event_id bigint PRIMARY KEY, namespace text, source_family text,
                     chain_id text, event_kind text, source_manifest_id bigint,
                     manifest_version bigint, block_number bigint, block_hash text,
                     canonicality_state text, after_state jsonb, raw_fact_ref jsonb);
                 CREATE TABLE bigname_phase.chain_lineage (chain_id text, block_number bigint,
                     block_hash text, block_timestamp timestamptz, canonicality_state text);
                 INSERT INTO bigname_phase.chain_lineage
                 VALUES ('ethereum-mainnet', 7, '0xseven', '2027-01-01T00:00:00Z', 'finalized');",
            )
            .execute(pool)
            .await?;
            for (event, status) in [(1, "active"), (2, "shadow")] {
                sqlx::query(
                    "INSERT INTO bigname_phase.normalized_events VALUES ($1, 'basenames',
                         'basenames_execution', 'ethereum-mainnet', 'SourceManifestUpdated', 9, 2,
                         NULL, NULL, 'finalized', $2, '{}')",
                )
                .bind(event)
                .bind(json!({"rollout_status": status, "manifest_payload": {
                    "deployment_epoch": "basenames_v1",
                    "capability_flags": {"verified_resolution": {"status": "supported"}},
                    "contracts": [{"role": "l1_resolver",
                        "address": "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31"}]}}))
                .execute(pool)
                .await?;
            }
            let mut conn = pool.acquire().await?;
            let published = execution(&mut conn, &publication("9:1")?).await?;
            let redone = execution(&mut conn, &publication("9:2")?).await?;
            anyhow::ensure!(
                published.as_ref().map(|context| context.block_number) == Some(7),
                "the publication's set admits the transport"
            );
            anyhow::ensure!(redone.is_none(), "the redo's set withdraws it");
            Ok(())
        }
        .await;
        database.cleanup().await?;
        result
    }
}
