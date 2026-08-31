use std::collections::BTreeMap;

use bigname_manifests::{
    DiscoveryWatchInterval, DiscoveryWatchKey, RequiredIngestCause, install_required_ingest,
    load_discovery_watch_coverage, normalize_intervals, subtract_intervals,
};
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};

use crate::{InterpretError, Result};

type SnapshotRow = (String, String, String, String, String, i64, i64);
type CursorRow = (String, i64, i64, Option<i64>);

pub(crate) async fn finalize_empty_completion(pool: &PgPool, chain_id: &str) -> Result<()> {
    let mut transaction = pool.begin().await.map_err(|error| {
        InterpretError::database("failed to begin empty Interpret completion", error)
    })?;
    finalize(&mut transaction, chain_id).await?;
    transaction.commit().await.map_err(|error| {
        InterpretError::database("failed to commit empty Interpret completion", error)
    })
}

pub(super) async fn finalize(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
) -> Result<bool> {
    let coverage = load_discovery_watch_coverage(&mut **transaction, chain_id)
        .await
        .map_err(|error| {
            InterpretError::database_anyhow(
                format!("failed to derive final discovery watch admissions for chain {chain_id}"),
                error,
            )
        })?;
    let snapshot_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM discovery_watch_admissions WHERE chain_id = $1
         )",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database(
            "failed to inspect discovery watch admission snapshot",
            error,
        )
    })?;
    let previous_rows: Vec<SnapshotRow> = sqlx::query_as(
        "SELECT namespace, target_source_family, target_deployment_label,
                lower(address), lower(topic0), active_from_block_number,
                active_to_block_number
         FROM discovery_watch_admissions
         WHERE chain_id = $1
           AND manifest_authority_fingerprint = $2
           AND lineage_orphaning_epoch = $3
         ORDER BY 1, 2, 3, 4, 5, 6, 7",
    )
    .bind(chain_id)
    .bind(&coverage.authority_fingerprint)
    .bind(coverage.lineage_orphaning_epoch)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to load discovery watch admission snapshot", error)
    })?;
    let mut previous = BTreeMap::<DiscoveryWatchKey, Vec<DiscoveryWatchInterval>>::new();
    for (namespace, family, deployment, address, topic0, from, to) in previous_rows {
        previous
            .entry(DiscoveryWatchKey {
                namespace,
                target_family: family,
                deployment_label: deployment,
                address,
                topic0,
            })
            .or_default()
            .push(DiscoveryWatchInterval { from, to });
    }
    for intervals in previous.values_mut() {
        normalize_intervals(intervals);
    }
    let snapshot_changed = previous != coverage.discovered
        || (snapshot_exists && previous.is_empty() && coverage.discovered.is_empty());
    let mut acknowledged_physical =
        BTreeMap::<(String, String), Vec<DiscoveryWatchInterval>>::new();
    for (key, intervals) in &previous {
        acknowledged_physical
            .entry((key.address.clone(), key.topic0.clone()))
            .or_default()
            .extend_from_slice(intervals);
    }
    for intervals in acknowledged_physical.values_mut() {
        normalize_intervals(intervals);
    }

    let cursors: Vec<CursorRow> = sqlx::query_as(
        "SELECT source_key, start_block_number, next_block_number,
                last_processed_block_number
         FROM ingest_cursors
         WHERE chain_id = $1
         ORDER BY source_key",
    )
    .bind(chain_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to load completed source intake coverage", error)
    })?;
    let readable_through: Option<i64> = sqlx::query_scalar(
        "SELECT COALESCE(
             (SELECT latest_block_number FROM chain_heads WHERE chain_id = $1),
             (SELECT current_block_number FROM chain_phase_state
              WHERE chain_id = $1 AND phase_name = 'ingest')
         )",
    )
    .bind(chain_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|error| {
        InterpretError::database("failed to load readable retained intake extent", error)
    })?;
    let completed = completed_source_intervals(cursors, readable_through, chain_id)?;

    let mut earliest_repair = None;
    for (key, desired) in &coverage.discovered {
        let tuple = (key.address.clone(), key.topic0.clone());
        let acknowledged = acknowledged_physical
            .get(&tuple)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let newly_admitted = subtract_intervals(desired, acknowledged);
        let independent = coverage
            .independently_covered
            .get(&tuple)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let physical_delta = if coverage.globally_covered_topics.contains(&key.topic0) {
            Vec::new()
        } else {
            subtract_intervals(&newly_admitted, independent)
        };
        if let Some(overlap) = earliest_completed_overlap(&physical_delta, &completed) {
            earliest_repair =
                Some(earliest_repair.map_or(overlap, |current: i64| current.min(overlap)));
        }
    }

    if let Some(from) = earliest_repair {
        install_required_ingest(
            transaction,
            chain_id,
            from,
            RequiredIngestCause::DiscoveryWatchAdmission,
        )
        .await
        .map_err(|error| {
            InterpretError::database_anyhow(
                format!("failed to install discovery coverage repair for chain {chain_id}"),
                error,
            )
        })?;
    }
    if snapshot_changed {
        replace_snapshot(transaction, chain_id, &coverage).await?;
    }
    Ok(earliest_repair.is_some())
}

fn completed_source_intervals(
    cursors: Vec<CursorRow>,
    readable_through: Option<i64>,
    chain_id: &str,
) -> Result<Vec<DiscoveryWatchInterval>> {
    let Some(readable_through) = readable_through else {
        return Ok(Vec::new());
    };
    let mut intervals = Vec::new();
    for (source, start, next, last) in cursors {
        let Some(last) = last else {
            continue;
        };
        if next != last.saturating_add(1) {
            return Err(InterpretError::data_integrity(format!(
                "ingest cursor {source} for chain {chain_id} has next block {next} but last processed block {last}"
            )));
        }
        let to = last.min(readable_through);
        if start <= to {
            intervals.push(DiscoveryWatchInterval { from: start, to });
        }
    }
    Ok(intervals)
}

fn earliest_completed_overlap(
    delta: &[DiscoveryWatchInterval],
    completed: &[DiscoveryWatchInterval],
) -> Option<i64> {
    delta
        .iter()
        .flat_map(|delta| {
            completed.iter().filter_map(move |source| {
                let from = delta.from.max(source.from);
                let to = delta.to.min(source.to);
                (from <= to).then_some(from)
            })
        })
        .min()
}

async fn replace_snapshot(
    transaction: &mut Transaction<'_, Postgres>,
    chain_id: &str,
    coverage: &bigname_manifests::DiscoveryWatchCoverage,
) -> Result<()> {
    sqlx::query("DELETE FROM discovery_watch_admissions WHERE chain_id = $1")
        .bind(chain_id)
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database(
                "failed to replace discovery watch admission snapshot",
                error,
            )
        })?;
    let rows = coverage
        .discovered
        .iter()
        .flat_map(|(key, intervals)| {
            intervals
                .iter()
                .copied()
                .map(move |interval| (key, interval))
        })
        .collect::<Vec<_>>();
    for chunk in rows.chunks(500) {
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO discovery_watch_admissions (
                 chain_id, manifest_authority_fingerprint, lineage_orphaning_epoch,
                 namespace, target_source_family, target_deployment_label,
                 address, topic0, active_from_block_number, active_to_block_number
             ) ",
        );
        query.push_values(chunk.iter().copied(), |mut row, (key, interval)| {
            row.push_bind(chain_id)
                .push_bind(&coverage.authority_fingerprint)
                .push_bind(coverage.lineage_orphaning_epoch)
                .push_bind(&key.namespace)
                .push_bind(&key.target_family)
                .push_bind(&key.deployment_label)
                .push_bind(&key.address)
                .push_bind(&key.topic0)
                .push_bind(interval.from)
                .push_bind(interval.to);
        });
        query
            .build()
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                InterpretError::database("failed to persist discovery watch admission", error)
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activation_at_last_processed_requires_repair() {
        assert_eq!(
            earliest_completed_overlap(
                &[DiscoveryWatchInterval { from: 10, to: 20 }],
                &[DiscoveryWatchInterval { from: 0, to: 10 }]
            ),
            Some(10)
        );
    }

    #[test]
    fn activation_at_next_block_does_not_require_repair() {
        assert_eq!(
            earliest_completed_overlap(
                &[DiscoveryWatchInterval { from: 11, to: 20 }],
                &[DiscoveryWatchInterval { from: 0, to: 10 }]
            ),
            None
        );
    }

    #[test]
    fn independent_source_ranges_are_not_manufactured_into_one_extent() {
        assert_eq!(
            earliest_completed_overlap(
                &[DiscoveryWatchInterval { from: 11, to: 19 }],
                &[
                    DiscoveryWatchInterval { from: 0, to: 10 },
                    DiscoveryWatchInterval { from: 20, to: 30 }
                ]
            ),
            None
        );
    }
}
