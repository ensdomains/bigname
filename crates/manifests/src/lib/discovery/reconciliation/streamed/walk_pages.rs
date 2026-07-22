//! Fixed-point admission walk over staged observation pages for the
//! streamed full-source reconcile.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use sqlx::{Postgres, QueryBuilder, postgres::PgConnection};

use super::super::super::admission::DiscoveryAdmissionState;
use super::super::super::loading::{
    load_known_contract_instance_addresses, scoped_address_key_vectors,
};
use super::super::super::provenance::is_zero_address;
use super::super::super::types::{AdmittedDiscoveryEdge, ReconciledDiscoveryEdgeSpec};
use super::super::bulk::insert_pending_contract_instance_seeds;
use super::super::walk::DiscoveryAdmissionWalk;
use super::StreamedDiscoveryReconciliationOptions;
use super::staging::{
    STREAMED_OBSERVATION_COLUMNS, STREAMED_OBSERVATION_COLUMNS_QUALIFIED, StreamedObservationRow,
    analyze_temp_table, stage_streamed_derived_contract_keys, streamed_observation_from_row,
};
use crate::normalize_address;

/// Fixed-point admission walk over the staged observations. Pass 1 pages the
/// complete staged set; passes >= 2 revisit only observations emitted from
/// addresses whose active-contract set grew (matching the in-memory walk's
/// requeue of a derived contract's address key). Memory stays bounded by the
/// derived-contract closure plus pending seeds for genuinely new addresses.
///
/// Deliberate divergence from the in-memory walk's mechanics: whenever a NEW
/// derived contract lands at an address — including one whose observations
/// were already admitted — the streamed walk unconditionally revisits that
/// address in the next pass, making the maximal admission fixed point
/// deterministic regardless of page order. The in-memory queue converges on
/// the same closure but schedules address visits in nondeterministic
/// hash-set order; the streamed behavior is intentionally the more-defined
/// of the two.
pub(super) async fn run_streamed_admission_walk(
    executor: &mut PgConnection,
    admission_state: &DiscoveryAdmissionState,
    options: &StreamedDiscoveryReconciliationOptions,
) -> Result<()> {
    let mut walk = DiscoveryAdmissionWalk::new(admission_state);
    let mut desired_buffer = Vec::<ReconciledDiscoveryEdgeSpec>::new();
    let mut admitted_buffer = Vec::<AdmittedDiscoveryEdge>::new();
    let mut pending_derived_keys = BTreeSet::<(String, String)>::new();

    let mut after_key = None::<String>;
    loop {
        let rows = sqlx::query(&format!(
            r#"
            SELECT {STREAMED_OBSERVATION_COLUMNS}
            FROM pg_temp.reconcile_observations
            WHERE ($1::TEXT IS NULL OR observation_key > $1)
            ORDER BY observation_key
            LIMIT $2
            "#
        ))
        .bind(after_key.as_deref())
        .bind(options.observation_page_limit)
        .fetch_all(&mut *executor)
        .await
        .context("failed to page staged streamed discovery observations")?;
        if rows.is_empty() {
            break;
        }
        let rows = rows
            .into_iter()
            .map(streamed_observation_from_row)
            .collect::<Result<Vec<_>>>()?;
        after_key = rows.last().map(|row| row.observation_key.clone());
        admit_streamed_observation_page(
            &mut *executor,
            admission_state,
            &mut walk,
            &rows,
            &mut desired_buffer,
            &mut admitted_buffer,
            &mut pending_derived_keys,
            options,
        )
        .await?;
    }

    let derived_observation_page_query =
        derived_observation_page_query(options.observation_page_limit);
    while !pending_derived_keys.is_empty() {
        let round_keys = std::mem::take(&mut pending_derived_keys);
        stage_streamed_derived_contract_keys(&mut *executor, round_keys).await?;
        let mut after_key = None::<String>;
        loop {
            let rows = sqlx::query(&derived_observation_page_query)
                .bind(after_key.as_deref())
                .fetch_all(&mut *executor)
                .await
                .context("failed to page staged observations for derived discovery contracts")?;
            if rows.is_empty() {
                break;
            }
            let rows = rows
                .into_iter()
                .map(streamed_observation_from_row)
                .collect::<Result<Vec<_>>>()?;
            after_key = rows.last().map(|row| row.observation_key.clone());
            admit_streamed_observation_page(
                &mut *executor,
                admission_state,
                &mut walk,
                &rows,
                &mut desired_buffer,
                &mut admitted_buffer,
                &mut pending_derived_keys,
                options,
            )
            .await?;
        }
    }

    flush_desired_edge_buffer(&mut *executor, &mut desired_buffer).await?;
    flush_admitted_edge_buffer(&mut *executor, &mut admitted_buffer).await?;
    insert_pending_contract_instance_seeds(
        &mut *executor,
        &walk.into_sorted_pending_contract_instance_seeds(),
    )
    .await?;
    analyze_temp_table(&mut *executor, "reconcile_desired_edges").await?;
    analyze_temp_table(&mut *executor, "reconcile_admitted_edges").await?;
    Ok(())
}

fn derived_observation_page_query(observation_page_limit: i64) -> String {
    format!(
        r#"
        SELECT {STREAMED_OBSERVATION_COLUMNS_QUALIFIED}
        FROM pg_temp.reconcile_observations obs
        JOIN pg_temp.reconcile_derived_contract_keys derived
          ON derived.chain_id = obs.chain_id
         AND derived.address = obs.normalized_from_address
        WHERE ($1::TEXT IS NULL OR obs.observation_key > $1)
        ORDER BY obs.observation_key
        LIMIT {observation_page_limit}
        "#
    )
}

#[allow(clippy::too_many_arguments)]
async fn admit_streamed_observation_page(
    executor: &mut PgConnection,
    admission_state: &DiscoveryAdmissionState,
    walk: &mut DiscoveryAdmissionWalk,
    rows: &[StreamedObservationRow],
    desired_buffer: &mut Vec<ReconciledDiscoveryEdgeSpec>,
    admitted_buffer: &mut Vec<AdmittedDiscoveryEdge>,
    pending_derived_keys: &mut BTreeSet<(String, String)>,
    options: &StreamedDiscoveryReconciliationOptions,
) -> Result<()> {
    // Resolve the page's target addresses through the same query and
    // first-row-wins fold the full known-address load uses, scoped to one
    // page instead of the whole `contract_instance_addresses` table.
    let (page_chains, page_addresses) = scoped_address_key_vectors(rows.iter().filter_map(|row| {
        let address = normalize_address(&row.observation.to_address);
        if is_zero_address(&address) {
            None
        } else {
            Some((row.observation.chain.clone(), address))
        }
    }));
    let known_contract_instances_by_address =
        load_known_contract_instance_addresses(&mut *executor, &page_chains, &page_addresses)
            .await?;

    for row in rows {
        if is_zero_address(&row.observation.to_address) {
            continue;
        }
        let contract_key = (
            row.observation.chain.clone(),
            row.normalized_from_address.clone(),
        );
        if !walk.has_contract_address(&contract_key) {
            continue;
        }
        for admitted in walk.admit_observation(
            admission_state,
            &known_contract_instances_by_address,
            &row.observation,
        )? {
            desired_buffer.push(admitted.desired_edge);
            admitted_buffer.push(admitted.admitted_edge);
            if let Some(derived_contract_key) = admitted.derived_contract_key {
                pending_derived_keys.insert(derived_contract_key);
            }
        }
        if desired_buffer.len() >= options.mutation_batch_size {
            flush_desired_edge_buffer(&mut *executor, desired_buffer).await?;
        }
        if admitted_buffer.len() >= options.mutation_batch_size {
            flush_admitted_edge_buffer(&mut *executor, admitted_buffer).await?;
        }
    }
    Ok(())
}

async fn flush_desired_edge_buffer(
    executor: &mut PgConnection,
    buffer: &mut Vec<ReconciledDiscoveryEdgeSpec>,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO pg_temp.reconcile_desired_edges (
            observation_key,
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            active_from_block_number,
            active_from_block_hash,
            active_from_transaction_index,
            active_from_log_index,
            provenance_json
        )
        "#,
    );
    builder.push_values(buffer.iter(), |mut row, edge| {
        row.push_bind(&edge.observation_key)
            .push_bind(&edge.chain)
            .push_bind(&edge.edge_kind)
            .push_bind(edge.from_contract_instance_id)
            .push_bind(edge.to_contract_instance_id)
            .push_bind(&edge.discovery_source)
            .push_bind(edge.source_manifest_id)
            .push_bind(&edge.admission)
            .push_bind(edge.active_from_block_number)
            .push_bind(edge.active_from_block_hash.as_deref())
            .push_bind(
                edge.active_from_event_position
                    .map(|position| position.transaction_index),
            )
            .push_bind(
                edge.active_from_event_position
                    .map(|position| position.log_index),
            )
            .push_bind(&edge.provenance_json);
    });
    builder.push(
        r#"
        ON CONFLICT (
            observation_key,
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            active_from_block_number,
            active_from_block_hash,
            active_from_transaction_index,
            active_from_log_index,
            provenance_json
        ) DO NOTHING
        "#,
    );
    builder
        .build()
        .execute(&mut *executor)
        .await
        .context("failed to stage streamed desired discovery edges")?;
    buffer.clear();
    Ok(())
}

async fn flush_admitted_edge_buffer(
    executor: &mut PgConnection,
    buffer: &mut Vec<AdmittedDiscoveryEdge>,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        INSERT INTO pg_temp.reconcile_admitted_edges (
            source_manifest_id,
            chain_id,
            from_contract_instance_id,
            to_contract_instance_id,
            from_address,
            to_address,
            edge_kind,
            discovery_source,
            admission,
            from_role
        )
        "#,
    );
    builder.push_values(buffer.iter(), |mut row, edge| {
        row.push_bind(edge.source_manifest_id)
            .push_bind(&edge.chain)
            .push_bind(edge.from_contract_instance_id)
            .push_bind(
                edge.to_contract_instance_id
                    .expect("admitted discovery edges are resolved before buffering"),
            )
            .push_bind(&edge.from_address)
            .push_bind(&edge.to_address)
            .push_bind(&edge.edge_kind)
            .push_bind(&edge.discovery_source)
            .push_bind(&edge.admission)
            .push_bind(&edge.from_role);
    });
    builder.push(" ON CONFLICT DO NOTHING ");
    builder
        .build()
        .execute(&mut *executor)
        .await
        .context("failed to stage streamed admitted discovery edges")?;
    buffer.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::derived_observation_page_query;

    #[test]
    fn derived_observation_pages_join_the_staged_set_without_array_binds() {
        let query = derived_observation_page_query(37);

        assert!(query.contains("JOIN pg_temp.reconcile_derived_contract_keys derived"));
        assert!(!query.contains("UNNEST"));
        assert!(!query.contains("TEXT[]"));
        assert!(query.contains("WHERE ($1::TEXT IS NULL OR obs.observation_key > $1)"));
        assert!(!query.contains("$2"));
        assert!(query.contains("LIMIT 37"));
    }
}
