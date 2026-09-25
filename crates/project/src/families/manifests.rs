//! The active manifest set a family block classifies resolvers under: for every manifest the
//! chain reads, its latest SourceManifestUpdated event at or below the block (or with no block)
//! on the readable lineage, and those of them that are active with a payload. Manifest sync
//! writes these events without a chain position, so the latest per manifest is the latest
//! written, the order stage.rs `create_manifests` uses.
//!
//! A run reads the chain's manifest updates once, before its first block, and every block takes
//! its set from that read: no block statement reads `normalized_events` for manifests. An update
//! written during a run applies from the next run.
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::{ProjectError, Result};

/// One SourceManifestUpdated event.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ManifestEvent {
    pub(crate) manifest_id: i64,
    pub(crate) event_id: i64,
    pub(crate) block_number: Option<i64>,
    pub(crate) namespace: String,
    pub(crate) source_family: String,
    pub(crate) rollout_status: Option<String>,
    pub(crate) payload: Option<Value>,
}

/// The manifest updates one run read, newest first within each manifest.
#[derive(Clone, Debug, Default)]
pub(crate) struct History {
    events: Vec<ManifestEvent>,
}

/// The manifest set active at one block.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ActiveSet {
    /// `manifest_id:event_id` of the latest update of every manifest, by manifest id; a
    /// different key means resolvers were classified under another set.
    pub(crate) key: String,
    /// The active manifests with a payload, as the rows the classification statements read.
    pub(crate) rows: Value,
}

impl History {
    pub(crate) fn new(mut events: Vec<ManifestEvent>) -> Self {
        events.sort_by(|a, b| {
            a.manifest_id
                .cmp(&b.manifest_id)
                .then(b.event_id.cmp(&a.event_id))
        });
        Self { events }
    }

    /// Every manifest update of the chain at or below `through` (or with no block) on the
    /// readable lineage.
    pub(crate) async fn read(pool: &PgPool, chain_id: &str, through: i64) -> Result<Self> {
        let rows: Vec<(
            i64,
            i64,
            Option<i64>,
            String,
            String,
            Option<String>,
            Option<Value>,
        )> = sqlx::query_as(
            "/* project:families.manifests.read */ SELECT event.source_manifest_id,
                    event.normalized_event_id, event.block_number, event.namespace,
                    event.source_family, event.after_state ->> 'rollout_status',
                    event.after_state -> 'manifest_payload'
             FROM normalized_events event
             LEFT JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
              AND lineage.block_number = event.block_number
             WHERE (event.chain_id = $1
                    OR ($1 = 'base-mainnet' AND event.namespace = 'basenames'
                        AND event.source_family = 'basenames_execution'
                        AND event.chain_id = 'ethereum-mainnet'))
               AND event.event_kind = 'SourceManifestUpdated'
               AND event.source_manifest_id IS NOT NULL
               AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
               AND (event.block_hash IS NULL
                    OR lineage.canonicality_state IN ('canonical', 'safe', 'finalized'))
               AND (event.block_number IS NULL OR event.block_number <= $2)",
        )
        .bind(chain_id)
        .bind(through)
        .fetch_all(pool)
        .await
        .map_err(|error| ProjectError::database("failed to read the manifest updates", error))?;
        Ok(Self::new(
            rows.into_iter()
                .map(
                    |(
                        manifest_id,
                        event_id,
                        block_number,
                        namespace,
                        source_family,
                        rollout_status,
                        payload,
                    )| ManifestEvent {
                        manifest_id,
                        event_id,
                        block_number,
                        namespace,
                        source_family,
                        rollout_status,
                        payload,
                    },
                )
                .collect(),
        ))
    }

    /// The set active at block `number`.
    pub(crate) fn at(&self, number: i64) -> ActiveSet {
        let mut key = Vec::new();
        let mut rows = Vec::new();
        let mut last = None;
        for event in &self.events {
            if last == Some(event.manifest_id)
                || event.block_number.is_some_and(|block| block > number)
            {
                continue;
            }
            last = Some(event.manifest_id);
            key.push(format!("{}:{}", event.manifest_id, event.event_id));
            if event.rollout_status.as_deref() == Some("active")
                && event
                    .payload
                    .as_ref()
                    .is_some_and(|payload| !payload.is_null())
            {
                rows.push(json!({
                    "manifest_id": event.manifest_id,
                    "namespace": event.namespace,
                    "source_family": event.source_family,
                    "rollout_status": event.rollout_status,
                    "manifest_payload": event.payload,
                    "manifest_event_id": event.event_id,
                }));
            }
        }
        ActiveSet {
            key: key.join(","),
            rows: Value::Array(rows),
        }
    }
}

/// The `manifests` CTE over the active set bound as parameter `$param`, with the columns the
/// served `create_manifests` gives.
pub(crate) fn input(param: usize) -> String {
    format!(
        "manifests AS (
    SELECT * FROM jsonb_to_recordset(${param}::jsonb) AS manifest(
        manifest_id bigint, namespace text, source_family text, rollout_status text,
        manifest_payload jsonb, manifest_event_id bigint)
)"
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{History, ManifestEvent};

    fn event(manifest_id: i64, event_id: i64, block: Option<i64>, status: &str) -> ManifestEvent {
        ManifestEvent {
            manifest_id,
            event_id,
            block_number: block,
            namespace: "ens".to_owned(),
            source_family: "ens_v1_resolver_l1".to_owned(),
            rollout_status: Some(status.to_owned()),
            payload: Some(json!({"manifest": manifest_id})),
        }
    }

    #[test]
    fn a_block_sees_the_latest_update_of_each_manifest_at_or_below_it() {
        let history = History::new(vec![
            event(1, 10, Some(5), "active"),
            event(1, 30, Some(12), "retired"),
            event(2, 20, None, "active"),
            event(3, 40, Some(20), "active"),
        ]);
        let at_eleven = history.at(11);
        assert_eq!(at_eleven.key, "1:10,2:20");
        assert_eq!(
            at_eleven
                .rows
                .as_array()
                .map(|rows| rows.iter().map(|row| row["manifest_id"].clone()).collect()),
            Some(vec![json!(1), json!(2)])
        );
        let at_twelve = history.at(12);
        assert_eq!(at_twelve.key, "1:30,2:20", "manifest 1 retired at 12");
        assert_eq!(at_twelve.rows.as_array().map(Vec::len), Some(1));
        assert_eq!(history.at(20).key, "1:30,2:20,3:40");
        assert_eq!(
            history.at(1).key,
            "2:20",
            "a blockless update applies everywhere"
        );
    }
}
