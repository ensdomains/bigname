//! Manual real-Project follow driver for external HTTP measurements on a disposable clone.
use super::*;

#[tokio::test]
#[ignore = "manual disabled/direct/exact reader check on a retained performance fixture"]
async fn address_history_read_retained_without_count() -> Result<()> {
    use bigname_storage::{
        HistoryCataloguePublication, HistoryCataloguePublicationFence, HistoryOrder,
        HistoryPageOptions, HistoryScope, HistorySummaryMode,
    };
    let url = std::env::var("BIGNAME_HISTORY_PUBLICATION_URL")?;
    let output = PathBuf::from(std::env::var("BIGNAME_HISTORY_PUBLICATION_RECEIPT")?);
    let mode = std::env::var("BIGNAME_HISTORY_RETAINED_COUNT_MODE").unwrap_or_default();
    let summary_mode = match mode.as_str() {
        "" | "record-page" => HistorySummaryMode::None,
        "direct" => HistorySummaryMode::CappedCount(10_000),
        "exact" => HistorySummaryMode::Count,
        _ => anyhow::bail!("unknown retained count mode"),
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            PgConnectOptions::from_str(&url)?
                .options([("search_path", "bigname_phase".to_owned())]),
        )
        .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    anyhow::ensure!(
        database.starts_with("bigname_api_test_"),
        "requires disposable API fixture"
    );
    let (number, hash, generation, content_hash): (i64, String, String, String) = sqlx::query_as(
        "SELECT current_block_number,current_block_hash,sequence::text,input_content_hash FROM project_family_marker WHERE chain_id=$1",
    ).bind(CHAIN).fetch_one(&pool).await?;
    anyhow::ensure!(content_hash == bigname_content_hash::INTERPRETER_CONTENT_HASH);
    let kinds: Vec<String> = match mode.as_str() {
        "direct" => vec!["RegistrationGranted".to_owned()],
        "record-page" => vec![
            "RecordChanged".to_owned(),
            "RecordVersionChanged".to_owned(),
        ],
        _ => Vec::new(),
    };
    let mut results = Vec::new();
    for order in [HistoryOrder::Asc, HistoryOrder::Desc] {
        let options = HistoryPageOptions {
            order,
            event_kinds: kinds.clone(),
            record_key: (mode == "record-page").then(|| "text:description".to_owned()),
            publication_block_bounds: Some([(CHAIN.to_owned(), number)].into()),
            catalogue_publication: Some(HistoryCataloguePublicationFence::Captured {
                publications: vec![HistoryCataloguePublication {
                    chain_id: CHAIN.to_owned(),
                    block_number: number,
                    block_hash: hash.clone(),
                    project_generation: generation.clone(),
                }],
                lag_tolerance_blocks: 10,
                captured_at: std::time::Instant::now(),
            }),
            ..Default::default()
        };
        let stats = std::sync::Arc::new(std::sync::Mutex::new(
            bigname_storage::AddressHistoryWorkingSet::default(),
        ));
        let started = std::time::Instant::now();
        let page = bigname_storage::with_address_history_working_set(
            stats.clone(),
            bigname_storage::load_address_history_page_for_relations(
                &pool,
                ADDRESS,
                Some("ens"),
                Some(&[bigname_storage::AddressNameRelation::TokenHolder]),
                HistoryScope::Both,
                true,
                None,
                200,
                summary_mode,
                &options,
                true,
            ),
        )
        .await?;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(page.rows.len(), 200);
        if summary_mode == HistorySummaryMode::None {
            assert!(page.summary.is_none());
        } else {
            let expected = if mode == "direct" {
                number.min(10_001)
            } else {
                number * 4
            };
            assert_eq!(page.summary.as_ref().unwrap().total_count, expected as u64);
        }
        let stats = stats.lock().unwrap().clone();
        assert_eq!(stats.counters.get("catalogue"), Some(&1));
        assert!(stats.live.values().all(|value| *value == 0));
        let identities: Vec<_> = page
            .rows
            .iter()
            .map(|row| row.event_identity.clone())
            .collect();
        let expected: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT event_identity FROM normalized_events WHERE event_identity LIKE 'perf:%%'
             AND (cardinality($1::text[]) = 0 OR event_kind = ANY($1))
             ORDER BY block_number {direction},block_hash {direction},transaction_index {direction},
                log_index {direction},event_identity {direction} LIMIT 200",
            direction = order.as_str()
        ))
        .bind(&kinds)
        .fetch_all(&pool)
        .await?;
        assert_eq!(identities, expected);
        results.push(
            json!({"order":order.as_str(),"rows":identities,"working_set":format!("{stats:?}"),
            "count_mode":mode,"elapsed_ms":elapsed_ms,"total_count":page.summary.as_ref().map(|summary|summary.total_count),
            "catalogue_receipt":stats.catalogue_receipt}),
        );
    }
    durable_profile_receipt(
        &output,
        &json!({"database":database,"compiled_content_hash":content_hash,"measurement":"Native debug storage reader; not HTTP acceptance","results":results}),
    )?;
    pool.close().await;
    Ok(())
}

#[tokio::test]
#[ignore = "manual publication cadence measurement alongside the Linux release HTTP process"]
async fn address_history_publish_retained_performance_fixture() -> Result<()> {
    let url = std::env::var("BIGNAME_HISTORY_PUBLICATION_URL")?;
    let output = PathBuf::from(std::env::var("BIGNAME_HISTORY_PUBLICATION_RECEIPT")?);
    let blocks: i64 = std::env::var("BIGNAME_HISTORY_PUBLICATION_BLOCKS")?.parse()?;
    let delay_ms: u64 = std::env::var("BIGNAME_HISTORY_PUBLICATION_DELAY_MS")?.parse()?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(
            PgConnectOptions::from_str(&url)?
                .options([("search_path", "bigname_phase".to_owned())]),
        )
        .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    anyhow::ensure!(
        database.starts_with("bigname_api_test_"),
        "requires disposable API fixture"
    );
    let initial: i64 = sqlx::query_scalar(
        "SELECT current_block_number FROM project_family_marker WHERE chain_id=$1",
    )
    .bind(CHAIN)
    .fetch_one(&pool)
    .await?;
    let mut publications = Vec::new();
    let run_started = std::time::Instant::now();
    durable_profile_receipt(
        &output,
        &json!({"database":database,"initial":initial,
        "blocks":blocks,"delay_ms":delay_ms,"publications":publications,"complete":false}),
    )?;
    for number in initial + 1..=initial + blocks {
        let hash = format!("publication-{number}");
        let mut inputs = pool.begin().await?;
        sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state)
            VALUES($1,$2,$3,to_timestamp(1776384000+$3),'canonical')")
            .bind(CHAIN).bind(&hash).bind(number).execute(&mut *inputs).await?;
        sqlx::query("INSERT INTO normalized_events(event_identity,namespace,logical_name_id,resource_id,
            event_kind,source_family,manifest_version,chain_id,block_hash,block_number,transaction_hash,
            transaction_index,log_index,raw_fact_ref,derivation_kind,canonicality_state,after_state)
            SELECT 'publication:'||$3::text,namespace,logical_name_id,resource_id,'RegistrationRenewed',
                source_family,manifest_version,chain_id,$2,$3,'publication-tx-'||$3::text,0,0,
                raw_fact_ref,derivation_kind,canonicality_state,jsonb_build_object('expiry',1900000000+$3)
            FROM normalized_events WHERE event_identity='perf:1:RegistrationGranted' AND chain_id=$1")
            .bind(CHAIN).bind(&hash).bind(number).execute(&mut *inputs).await?;
        sqlx::query(
            "UPDATE chain_heads SET latest_block_number=$2,latest_block_hash=$3 WHERE chain_id=$1",
        )
        .bind(CHAIN)
        .bind(number)
        .bind(&hash)
        .execute(&mut *inputs)
        .await?;
        sqlx::query("UPDATE chain_phase_state SET current_block_number=$2,current_block_hash=$3,
            target_block_number=$2,target_block_hash=$3 WHERE chain_id=$1 AND phase_name='interpret'")
            .bind(CHAIN).bind(number).bind(&hash).execute(&mut *inputs).await?;
        inputs.commit().await?;
        let started = std::time::Instant::now();
        let token = bigname_project::families::input_token(&pool, CHAIN).await?;
        let outcome = bigname_project::families::apply(
            &pool,
            CHAIN,
            &bigname_project::Marker { number, hash },
            bigname_project::families::FamilyMode::Normal,
            &token,
            &bigname_project::families::FamilyOptions::new(
                bigname_content_hash::INTERPRETER_CONTENT_HASH,
            ),
        )
        .await?;
        anyhow::ensure!(
            outcome.marker.as_ref().map(|m| m.number) == Some(number),
            "incomplete follow: {outcome:?}"
        );
        let marker: Value = sqlx::query_scalar(
            "SELECT to_jsonb(marker) FROM project_family_marker marker WHERE chain_id=$1",
        )
        .bind(CHAIN)
        .fetch_one(&pool)
        .await?;
        publications.push(
            json!({"number":number,"project_ms":started.elapsed().as_secs_f64()*1000.,
            "run_ms":run_started.elapsed().as_secs_f64()*1000.,"marker":marker,
            "rows_written":outcome.rows,"undo_rows_written":outcome.undo_rows}),
        );
        durable_profile_receipt(
            &output,
            &json!({"database":database,"initial":initial,
            "blocks":blocks,"delay_ms":delay_ms,"publications":publications,"complete":false}),
        )?;
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }
    durable_profile_receipt(
        &output,
        &json!({"database":database,"initial":initial,
        "blocks":blocks,"delay_ms":delay_ms,"publications":publications,"complete":true}),
    )?;
    pool.close().await;
    Ok(())
}
