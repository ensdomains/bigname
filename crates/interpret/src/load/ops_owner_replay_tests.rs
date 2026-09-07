use crate::load::{self, CachedPrior, LoadedBatch};
use anyhow::{Context, Result};
use bigname_adapters::schema_v2::seam::{
    INTERPRETER_STATE_KEY, STATE_SCOPE_KEY, TOKEN_CONTROL_TRANSFERRED_EVENT_KIND,
};
use bigname_adapters::{
    SchemaV2AdapterSession, StateCacheCapacity, begin_schema_v2_adapter_restore_with_provenance,
    prepare_schema_v2_batch_incremental_with_provenance,
};
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::Value;
use sqlx::{PgPool, types::Uuid};

const CHAIN: &str = "ethereum-sepolia";
const CAPACITY: StateCacheCapacity = StateCacheCapacity::Unlimited;

// Only actual public-contract receipt/header input and generated manifest bytes
// are retained. No normalized event, current row or phase progress is seeded.
#[tokio::test]
async fn actual_token_sale_live_full_and_compacted_restore_project_identically() -> Result<()> {
    let fixture: Value =
        serde_json::from_str(include_str!("../../tests/fixtures/ops_owner_receipts.json"))?;
    let prefix_end = fixture["prefix_end"].as_i64().context("prefix end")?;
    let suffix_end = fixture["suffix_end"].as_i64().context("suffix end")?;
    let first = fixture["first_block"].as_i64().context("first block")?;
    let logical = fixture["logical_name_id"]
        .as_str()
        .context("logical name")?;
    let previous_registrant = fixture["previous_registrant"]
        .as_str()
        .context("previous registrant")?;
    let next_registrant = fixture["next_registrant"]
        .as_str()
        .context("next registrant")?;
    assert_eq!(
        previous_registrant,
        "0x90f79bf6eb2c4f870365e785982e1f101e93b906"
    );
    assert_eq!(
        next_registrant,
        "0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc"
    );
    let databases = [
        database(&fixture).await?,
        database(&fixture).await?,
        database(&fixture).await?,
    ];
    let prefix = load::batch_input(
        databases[0].pool(),
        CHAIN,
        first,
        prefix_end,
        None,
        None,
        CAPACITY,
    )
    .await?;
    assert_eq!(prefix.restored_event_count, 0);
    let prefix_epoch = prefix.prior_cache.validated_orphaning_epoch;
    let expected = lineage(&prefix);
    assert_eq!(expected, fixture_lineage(&fixture, first, prefix_end));
    assert_eq!(prefix_epoch, 0);
    let predecessor = prefix
        .input
        .blocks
        .last()
        .context("prefix block")?
        .block_timestamp;
    // This genuine live session is returned by the producer, never reconstructed
    // from its output. Full and compacted sessions are compared to it below.
    let prepared = prepare_schema_v2_batch_incremental_with_provenance(
        prefix.input,
        prefix.provenance_manifests,
        None,
        CAPACITY,
    )?;
    let values = load::prior_state_values(
        databases[0].pool(),
        CHAIN,
        first,
        prepared.state_value_requests(),
    )
    .await?;
    let (prefix_output, live_session) = prepared.finish(values)?;
    assert!(!prefix_output.resources.is_empty());
    assert!(!prefix_output.token_lineages.is_empty());
    assert!(!prefix_output.surface_bindings.is_empty());
    let initial = prefix_output
        .normalized_events
        .iter()
        .find(|event| {
            event.logical_name_id.as_deref() == Some(logical)
                && event.event_kind == "RegistrationGranted"
        })
        .context("original sale registration")?;
    assert_eq!(
        initial.after_state["registrant"],
        "0x70997970c51812dc3a010c7d01b50e0d17dc79c8"
    );
    assert_ne!(initial.after_state["registrant"], previous_registrant);
    let mut prefix_markers = Vec::new();
    for database in &databases {
        crate::write::batch(
            database.pool(),
            CHAIN,
            None,
            false,
            true,
            prefix_epoch,
            &expected,
            &prefix_output,
        )
        .await?;
        prefix_markers.push(project(database.pool(), first, prefix_end, None).await?);
        let summary = summary(database.pool(), logical).await?;
        assert_eq!(summary["registration"]["registrant"], previous_registrant);
        assert_eq!(summary["control"]["registry_owner"], previous_registrant);
        let selected: (Uuid, Uuid) = sqlx::query_as(
            "SELECT resource_id, token_lineage_id FROM name_current WHERE logical_name_id = $1",
        )
        .bind(logical)
        .fetch_one(database.pool())
        .await?;
        assert!(
            prefix_output
                .resources
                .iter()
                .any(|resource| resource.resource_id == selected.0
                    && resource.token_lineage_id == Some(selected.1))
        );
        assert!(
            prefix_output
                .token_lineages
                .iter()
                .any(|token| token.token_lineage_id == selected.1)
        );
        assert_eq!(
            prefix_markers.last().context("prefix marker")?.number,
            prefix_end
        );
        assert_eq!(
            prefix_markers.last().context("prefix marker")?.hash,
            fixture_lineage(&fixture, prefix_end, prefix_end)[0].1
        );
        let grants: i64 = sqlx::query_scalar("SELECT count(*) FROM permissions_current WHERE resource_id = $1 AND lower(subject) = $2 AND jsonb_array_length(effective_powers) > 0")
            .bind(selected.0).bind(previous_registrant).fetch_one(database.pool()).await?;
        assert!(
            grants > 0,
            "selected prefix registrant has actual retained permissions"
        );
    }
    let full_input = load::batch_input(
        databases[1].pool(),
        CHAIN,
        prefix_end + 1,
        suffix_end,
        Some((prefix_end, &prefix_markers[1].hash)),
        None,
        CAPACITY,
    )
    .await?;
    let (full_session, full_count) =
        full_restore(databases[1].pool(), &full_input, prefix_end + 1).await?;
    let compact_input = load::batch_input(
        databases[2].pool(),
        CHAIN,
        prefix_end + 1,
        suffix_end,
        Some((prefix_end, &prefix_markers[2].hash)),
        None,
        CAPACITY,
    )
    .await?;
    let mut connection = databases[2].pool().acquire().await?;
    assert_eq!(
        load::resume::predecessor_timestamp(&mut connection, CHAIN, prefix_end + 1).await?,
        Some(predecessor)
    );
    drop(connection);
    let compact_count = compact_input.restored_event_count;
    assert!(full_count > compact_count && compact_count > 0);
    assert_eq!(
        &live_session, &full_session,
        "full producer-output restore before suffix"
    );
    assert_eq!(
        Some(&live_session),
        compact_input.adapter_session.as_ref(),
        "production SQL-compacted restore before suffix"
    );
    let live_input = load::batch_input(
        databases[0].pool(),
        CHAIN,
        prefix_end + 1,
        suffix_end,
        Some((prefix_end, &prefix_markers[0].hash)),
        Some(CachedPrior {
            cache: load::fold_prior_cache(prefix.prior_cache, &prefix_output.normalized_events),
            adapter_session: live_session,
        }),
        CAPACITY,
    )
    .await?;
    assert_eq!(
        live_input.restored_event_count, 0,
        "live continuation must use its validated cache"
    );
    let mut full_session = Some(full_session);
    let mut outputs = Vec::new();
    let mut summaries = Vec::new();
    for (index, mut loaded) in [live_input, full_input, compact_input]
        .into_iter()
        .enumerate()
    {
        if index == 1 {
            loaded.adapter_session = full_session.take();
        }
        let expected = lineage(&loaded);
        let epoch = loaded.prior_cache.validated_orphaning_epoch;
        assert_eq!(epoch, prefix_epoch);
        assert_eq!(
            expected,
            fixture_lineage(&fixture, prefix_end + 1, suffix_end)
        );
        let prepared = prepare_schema_v2_batch_incremental_with_provenance(
            loaded.input,
            loaded.provenance_manifests,
            loaded.adapter_session,
            CAPACITY,
        )?;
        let values = load::prior_state_values(
            databases[index].pool(),
            CHAIN,
            prefix_end + 1,
            prepared.state_value_requests(),
        )
        .await?;
        let (output, _) = prepared.finish(values)?;
        let sale = output
            .normalized_events
            .iter()
            .find(|event| {
                event.event_kind == TOKEN_CONTROL_TRANSFERRED_EVENT_KIND
                    && event.logical_name_id.as_deref() == Some(logical)
            })
            .context("same-name real transfer after the retained prefix")?;
        assert_eq!(sale.before_state["from"], previous_registrant);
        assert_eq!(sale.after_state["to"], next_registrant);
        assert!(sale.source_manifest_id.is_some());
        assert_eq!(
            sale.transaction_hash.as_deref(),
            fixture["sale_transaction"].as_str()
        );
        let selected: Uuid =
            sqlx::query_scalar("SELECT resource_id FROM name_current WHERE logical_name_id = $1")
                .bind(logical)
                .fetch_one(databases[index].pool())
                .await?;
        assert_eq!(sale.resource_id, Some(selected));
        assert!(
            output
                .normalized_events
                .iter()
                .any(|event| event.event_kind == "PermissionChanged"
                    && event.resource_id == Some(selected))
        );
        crate::write::batch(
            databases[index].pool(),
            CHAIN,
            None,
            false,
            true,
            epoch,
            &expected,
            &output,
        )
        .await?;
        project(
            databases[index].pool(),
            prefix_end + 1,
            suffix_end,
            Some(prefix_markers[index].clone()),
        )
        .await?;
        let incremental = summary(databases[index].pool(), logical).await?;
        assert_eq!(incremental["registration"]["registrant"], next_registrant);
        assert_eq!(incremental["control"]["registry_owner"], next_registrant);
        project(databases[index].pool(), first, suffix_end, None).await?;
        assert_eq!(
            summary(databases[index].pool(), logical).await?,
            incremental
        );
        outputs.push(output);
        summaries.push(incremental);
    }
    assert_eq!(outputs[0], outputs[1]);
    assert_eq!(outputs[0], outputs[2]);
    assert_eq!(summaries[0], summaries[1]);
    assert_eq!(summaries[0], summaries[2]);
    eprintln!(
        "OPS full {full_count}, compacted {compact_count}; three producer/Project branches agree"
    );
    for database in databases {
        database.cleanup().await?;
    }
    Ok(())
}

fn fixture_lineage(fixture: &Value, first: i64, last: i64) -> Vec<(i64, String)> {
    fixture["chain_lineage"]
        .as_array()
        .expect("fixture headers")
        .iter()
        .filter_map(|block| {
            let number = block["block_number"].as_i64().expect("block number");
            (first <= number && number <= last).then(|| {
                (
                    number,
                    block["block_hash"].as_str().expect("block hash").to_owned(),
                )
            })
        })
        .collect()
}

fn lineage(loaded: &LoadedBatch) -> Vec<(i64, String)> {
    loaded
        .input
        .blocks
        .iter()
        .map(|block| (block.block_number, block.block_hash.clone()))
        .collect()
}

async fn full_restore(
    pool: &PgPool,
    loaded: &LoadedBatch,
    before: i64,
) -> Result<(SchemaV2AdapterSession, usize)> {
    let mut restore = begin_schema_v2_adapter_restore_with_provenance(
        CHAIN.into(),
        loaded.input.manifests.clone(),
        loaded.provenance_manifests.clone(),
        loaded.input.discovery_rules.clone(),
        loaded.input.admissions.clone(),
        CAPACITY,
    )?;
    // Same columns, lineage membership, ordering and row conversion as the
    // production loader, with only ranking removed for the full-output branch.
    let statement = format!(
        "SELECT event.chain_id, event.namespace, event.logical_name_id, event.resource_id,
         event.event_kind, event.source_family, event.manifest_version, event.source_manifest_id,
         event.raw_fact_ref ->> 'emitting_address', event.raw_fact_ref ->> '{INTERPRETER_STATE_KEY}',
         event.event_identity, event.raw_fact_ref ->> '{STATE_SCOPE_KEY}', event.block_number,
         event.block_hash, lineage.block_timestamp, event.after_state
         FROM normalized_events event JOIN chain_lineage lineage
         ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash AND lineage.block_number = event.block_number
         WHERE event.chain_id = $1 AND event.block_number < $2
         AND event.canonicality_state IN ('canonical', 'safe', 'finalized')
         AND lineage.canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY event.block_number, event.normalized_event_id"
    );
    let rows: Vec<super::Row> = sqlx::query_as(&statement)
        .bind(CHAIN)
        .bind(before)
        .fetch_all(pool)
        .await?;
    let count = rows.len();
    restore.apply_prior_events(rows.into_iter().map(super::row_to_event).collect())?;
    let mut connection = pool.acquire().await?;
    Ok((
        load::resume::finish_restore(&mut connection, CHAIN, before, restore).await?,
        count,
    ))
}

async fn project(
    pool: &PgPool,
    from: i64,
    target: i64,
    previous: Option<Marker>,
) -> Result<Marker> {
    Ok(Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: target,
            affected_from_block: from,
            affected_to_block: target,
            resume_current: previous,
            mode: RunMode::Normal,
        })
        .await?
        .current)
}

async fn summary(pool: &PgPool, logical: &str) -> Result<Value> {
    Ok(
        sqlx::query_scalar("SELECT declared_summary FROM name_current WHERE logical_name_id = $1")
            .bind(logical)
            .fetch_one(pool)
            .await?,
    )
}

async fn database(fixture: &Value) -> Result<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new("ops_owner_replay")).await?;
    for script in [
        include_str!("../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../../schema-v2/baseline/10_phase_state.sql"),
        include_str!("../../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../../schema-v2/baseline/12_project_generation_failures.sql"),
        include_str!("../../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
        include_str!("../../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
    ] {
        sqlx::raw_sql(script).execute(database.pool()).await?;
    }
    let directory = std::env::temp_dir().join(format!("bigname-ops-manifests-{}", Uuid::new_v4()));
    std::fs::create_dir(&directory)?;
    let result = async {
        for (path, content) in fixture["manifests"]
            .as_object()
            .context("manifest input files")?
        {
            let path = directory.join(path);
            std::fs::create_dir_all(path.parent().context("manifest directory")?)?;
            std::fs::write(path, content.as_str().context("manifest bytes")?)?;
        }
        sync_schema_v2_repository(database.pool(), &load_repository(&directory)?).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    std::fs::remove_dir_all(&directory)?;
    result?;
    for table in [
        "chain_lineage",
        "raw_transactions",
        "raw_receipts",
        "raw_logs",
    ] {
        sqlx::query(&format!(
            "INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, $1)"
        ))
        .bind(&fixture[table])
        .execute(database.pool())
        .await?;
    }
    Ok(database)
}
