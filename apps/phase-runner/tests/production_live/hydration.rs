//! Hydration contracts through ProjectPhase's Follow path; replay/rebuild never call RPC.
use super::*;
#[path = "hydration_inputs.rs"]
mod inputs;
use inputs::*;

pub(super) fn reverse_hydration_node_hex(candidate: i64) -> String {
    reverse_node(&format!("0x{candidate:040x}"))
        .trim_start_matches("0x")
        .to_owned()
}

#[tokio::test]
async fn event_silent_reverse_hydration_refreshes_and_follows_a_fork() -> Result<()> {
    let db = setup("live_family_reverse_fork", 2).await?;
    seed_reverse(db.pool(), ADDRESS).await?;
    let text_resource = seed_text(db.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "alice.eth".into()),
        (block_hash(1, 2), "bob.eth".into()),
        (block_hash(2, 3), String::new()),
    ]))
    .await?;
    follow(db.pool(), &rpc.endpoint, 1, 1).await?;
    assert_primary(
        db.pool(),
        ADDRESS,
        "success",
        Some("alice.eth"),
        Some(&block_hash(1, 1)),
    )
    .await?;
    text_change(db.pool(), 2).await?;
    follow(db.pool(), &rpc.endpoint, 1, 2).await?;
    assert_eq!(
        text_entry(db.pool(), text_resource).await?["value"],
        "bob.eth"
    );
    assert_primary(
        db.pool(),
        ADDRESS,
        "success",
        Some("bob.eth"),
        Some(&block_hash(1, 2)),
    )
    .await?;
    seed_branch(db.pool(), ETHEREUM, 2, 3, Some((1, block_hash(1, 1)))).await?;
    let calls = rpc.calls.lock().unwrap().len();
    follow(db.pool(), &rpc.endpoint, 2, 2).await?;
    assert_eq!(
        rpc.calls.lock().unwrap().len(),
        calls,
        "fork replay performs no RPC"
    );
    assert_primary(
        db.pool(),
        ADDRESS,
        "success",
        Some("alice.eth"),
        Some(&block_hash(1, 1)),
    )
    .await?;
    assert_eq!(
        text_entry(db.pool(), text_resource).await?["value"],
        "alice.eth",
        "undo restores the prior canonical text overlay too"
    );
    follow(db.pool(), &rpc.endpoint, 2, 3).await?;
    assert_primary(
        db.pool(),
        ADDRESS,
        "not_found",
        None,
        Some(&block_hash(2, 3)),
    )
    .await?;
    rpc.server.abort();
    db.cleanup().await
}

#[tokio::test]
async fn event_silent_reverse_hydration_bounds_the_rolling_refresh_batch() -> Result<()> {
    let db = setup("live_family_reverse_bound", 2).await?;
    seed_page(db.pool()).await?;
    let rpc = SelectiveFailureHydrationRpc::spawn(0, 251).await?;
    follow(db.pool(), &rpc.endpoint, 1, 2).await?;
    let batches = rpc.batches.lock().unwrap().clone();
    assert_eq!(
        batches,
        vec![ObservedHydrationBatch {
            poisoned: false,
            call_count: 250,
            contains_last_row: false
        }]
    );
    let refreshed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_reverse_tuple WHERE hydrated_name IS NOT NULL",
    )
    .fetch_one(db.pool())
    .await?;
    assert_eq!(refreshed, 250);
    rpc.server.abort();
    db.cleanup().await
}

/// `poison` names a tuple whose presence fails any aggregate that holds it (zero: none). The
/// endpoint answers every other aggregate, so Project splits the failed page: every other tuple
/// is read, and the poisoned one keeps its (empty) state and only moves back in the rotation.
async fn rolling_progress(poison: i64, cross_head: bool) -> Result<()> {
    let db = setup("live_family_reverse_fairness", 5).await?;
    seed_page(db.pool()).await?;
    let rpc = SelectiveFailureHydrationRpc::spawn(poison, 251).await?;
    // The page of 250; then, with a poisoned tuple, the probe, the halves that hold the tuple
    // down to the tuple alone, and the other half of each split.
    let page: &[usize] = if poison == 0 {
        &[250]
    } else {
        &[250, 1, 125, 62, 31, 15, 7, 3, 1, 2, 4, 8, 16, 31, 63, 125]
    };
    let counts = |rpc: &SelectiveFailureHydrationRpc| {
        rpc.batches
            .lock()
            .unwrap()
            .iter()
            .map(|batch| batch.call_count)
            .collect::<Vec<_>>()
    };
    follow(db.pool(), &rpc.endpoint, 1, 2).await?;
    // A completed head is not another hydration tick. Later Follow blocks advance the cohort.
    project(db.pool(), Some(urls(&rpc.endpoint)?), 1, 2, false).await?;
    assert_eq!(counts(&rpc), page);
    follow(db.pool(), &rpc.endpoint, 1, 3).await?;
    let batches = rpc.batches.lock().unwrap().clone();
    assert_eq!(counts(&rpc), [page, &[1]].concat());
    assert!(batches.last().expect("a batch").contains_last_row);
    let last = "0x00000000000000000000000000000000000000fb";
    assert_primary(
        db.pool(),
        last,
        "success",
        Some("new.eth"),
        Some(&block_hash(1, 3)),
    )
    .await?;
    type Attempt = (String, Option<i64>, Option<String>, i64, bool);
    let attempts:Vec<Attempt>=sqlx::query_as("SELECT address,attempt_block,attempt_hash,attempt_ordinal,hydrated_name IS NOT NULL FROM project_reverse_tuple WHERE address IN ($1,$2) ORDER BY address")
        .bind("0x0000000000000000000000000000000000000001").bind(last).fetch_all(db.pool()).await?;
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[1].1, Some(3));
    assert_eq!(attempts[1].2, Some(block_hash(1, 3)));
    assert!(attempts[0].3 < attempts[1].3);
    assert!(attempts[1].4);
    if poison == 0 {
        assert_eq!(attempts[0].1, Some(2));
        assert_eq!(attempts[0].2, Some(block_hash(1, 2)));
        assert!(attempts[0].4);
    } else {
        // Nothing was observed for the poisoned tuple, so it records no attempt block.
        assert_eq!((attempts[0].1, attempts[0].2.as_deref()), (None, None));
        assert!(!attempts[0].4);
        assert_primary(db.pool(), &attempts[0].0, "not_found", None, None).await?;
        let read: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM project_reverse_tuple WHERE hydrated_name = 'new.eth'",
        )
        .fetch_one(db.pool())
        .await?;
        assert_eq!(read, 250, "every other tuple of the page was read");
    }
    if cross_head {
        follow(db.pool(), &rpc.endpoint, 1, 4).await?;
        follow(db.pool(), &rpc.endpoint, 1, 5).await?;
        assert_eq!(counts(&rpc), [page, &[1], page, &[1]].concat());
    }
    rpc.server.abort();
    db.cleanup().await
}

#[tokio::test]
async fn unreadable_reverse_tuple_does_not_hold_its_page_across_heads() -> Result<()> {
    rolling_progress(1, true).await
}
#[tokio::test]
async fn unreadable_reverse_tuple_does_not_starve_the_next_rolling_row() -> Result<()> {
    rolling_progress(1, false).await
}
#[tokio::test]
async fn successful_reverse_hydration_page_reaches_the_next_rolling_row() -> Result<()> {
    rolling_progress(0, false).await
}

#[tokio::test]
async fn event_silent_reverse_hydration_does_not_serve_or_starve_an_orphaned_batch() -> Result<()> {
    let db = setup("live_family_reverse_orphaned", 2).await?;
    seed_page(db.pool()).await?;
    let rpc = SelectiveFailureHydrationRpc::spawn(0, 251).await?;
    follow(db.pool(), &rpc.endpoint, 1, 2).await?;
    seed_branch(db.pool(), ETHEREUM, 2, 4, Some((1, block_hash(1, 1)))).await?;
    publish(db.pool(), ETHEREUM, 2, 2, 0, 0).await?;
    assert!(
        primary(db.pool(), "0x0000000000000000000000000000000000000001")
            .await
            .is_err(),
        "orphaned publication is not readable"
    );
    project(db.pool(), Some(urls(&rpc.endpoint)?), 2, 2, false).await?;
    assert_eq!(
        rpc.batches.lock().unwrap().len(),
        1,
        "replay does not hydrate"
    );
    follow(db.pool(), &rpc.endpoint, 2, 3).await?;
    let last = "0x00000000000000000000000000000000000000fb";
    assert_primary(db.pool(), last, "not_found", None, None).await?;
    follow(db.pool(), &rpc.endpoint, 2, 4).await?;
    assert_primary(
        db.pool(),
        last,
        "success",
        Some("new.eth"),
        Some(&block_hash(2, 4)),
    )
    .await?;
    assert_eq!(
        rpc.batches
            .lock()
            .unwrap()
            .iter()
            .map(|batch| batch.call_count)
            .collect::<Vec<_>>(),
        vec![250, 250, 1]
    );
    rpc.server.abort();
    db.cleanup().await
}

#[tokio::test]
async fn missing_hydration_rpc_fails_before_retracting_existing_values() -> Result<()> {
    let db = setup("live_family_missing_rpc", 2).await?;
    seed_reverse(db.pool(), ADDRESS).await?;
    let resource = seed_text(db.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([(block_hash(1, 1), "alice.eth".into())])).await?;
    follow(db.pool(), &rpc.endpoint, 1, 1).await?;
    let before = primary(db.pool(), ADDRESS).await?;
    let text_before = text_entry(db.pool(), resource).await?;
    publish(db.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    let error = project(db.pool(), Some(ChainRpcUrls::default()), 1, 2, false)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<RunnerError>()
            .context("runner error")?
            .kind(),
        ErrorKind::Configuration
    );
    assert_eq!(primary(db.pool(), ADDRESS).await?, before);
    assert_eq!(text_entry(db.pool(), resource).await?, text_before);
    let marker: i64 = sqlx::query_scalar(
        "SELECT current_block_number FROM project_family_marker WHERE chain_id=$1",
    )
    .bind(ETHEREUM)
    .fetch_one(db.pool())
    .await?;
    assert_eq!(marker, 1);
    rpc.server.abort();
    db.cleanup().await
}

/// A runner configured without the mainnet hydration URL stops before its first rebuild
/// publishes anything, not at the first follow block with a hydration candidate.
#[tokio::test]
async fn missing_hydration_rpc_fails_before_a_rebuild_publishes() -> Result<()> {
    let db = setup("live_family_missing_rpc_rebuild", 2).await?;
    seed_reverse(db.pool(), ADDRESS).await?;
    publish(db.pool(), ETHEREUM, 1, 1, 0, 0).await?;
    let error = project(db.pool(), Some(ChainRpcUrls::default()), 1, 1, true)
        .await
        .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<RunnerError>()
            .context("runner error")?
            .kind(),
        ErrorKind::Configuration
    );
    let marker: Option<i64> = sqlx::query_scalar(
        "SELECT current_block_number FROM project_family_marker WHERE chain_id=$1",
    )
    .bind(ETHEREUM)
    .fetch_optional(db.pool())
    .await?;
    assert_eq!(marker, Some(0), "the rebuild published before the check");
    db.cleanup().await
}

#[tokio::test]
async fn project_redo_behind_the_canonical_head_defers_hydration() -> Result<()> {
    let db = setup("live_family_redo_rpc_free", 2).await?;
    seed_reverse(db.pool(), ADDRESS).await?;
    publish(db.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    let rpc =
        HydrationRpc::spawn(BTreeMap::from([(block_hash(1, 2), "future.eth".into())])).await?;
    ProjectPhase::with_hydration(db.pool().clone(), urls(&rpc.endpoint)?)
        .run_batch(PhaseContext {
            chain_id: ETHEREUM.into(),
            phase: PhaseName::Project,
            mode: RunMode::Redo(BlockRange::new(1, 1)?),
            redo_attempt: None,
            sources: Arc::from([]),
            available_heads: Some(HeadMarkers {
                latest: BlockMarker::new(1, block_hash(1, 1))?,
                safe: None,
                finalized: None,
            }),
            live_handoff: None,
            resume: PhaseResume::default(),
        })
        .await?;
    assert!(rpc.calls.lock().unwrap().is_empty());
    assert_primary(db.pool(), ADDRESS, "not_found", None, None).await?;
    rpc.server.abort();
    db.cleanup().await
}

/// The one-shot redo builds its phase with an empty URL set and without the URL requirement
/// (the CLI's redo settings). Its undo and replay never read RPC, so it publishes the replayed
/// block on mainnet without the URL the supervised runner needs.
#[tokio::test]
async fn project_redo_runs_without_a_hydration_url() -> Result<()> {
    let db = setup("live_family_redo_no_rpc", 2).await?;
    seed_reverse(db.pool(), ADDRESS).await?;
    publish(db.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    ProjectPhase::with_hydration(db.pool().clone(), ChainRpcUrls::default())
        .with_family_settings(FamilySettings {
            retry_family_failures: false,
            require_hydration_url: false,
            ..FamilySettings::default()
        })
        .run_batch(PhaseContext {
            chain_id: ETHEREUM.into(),
            phase: PhaseName::Project,
            mode: RunMode::Redo(BlockRange::new(1, 1)?),
            redo_attempt: None,
            sources: Arc::from([]),
            available_heads: Some(HeadMarkers {
                latest: BlockMarker::new(1, block_hash(1, 1))?,
                safe: None,
                finalized: None,
            }),
            live_handoff: None,
            resume: PhaseResume::default(),
        })
        .await?;
    let marker: i64 = sqlx::query_scalar(
        "SELECT current_block_number FROM project_family_marker WHERE chain_id=$1",
    )
    .bind(ETHEREUM)
    .fetch_one(db.pool())
    .await?;
    assert_eq!(marker, 1, "the redo replayed its block");
    assert_primary(db.pool(), ADDRESS, "not_found", None, None).await?;
    db.cleanup().await
}

/// What a reverse read that goes wrong at block 2 leaves served.
enum ReverseFailure {
    /// The resolver call fails inside an answered aggregate: the name is cleared (fail closed).
    FailedCall,
    /// Every aggregate at the block fails: nothing is observed and the name stays.
    FailedBatch,
    /// The endpoint answers a two-call aggregate with one result. The aggregate fails as a
    /// whole; the endpoint does answer single calls, so each tuple is read on its own.
    Shortened,
}

async fn reverse_failure(failure: ReverseFailure) -> Result<()> {
    let db = setup("live_family_reverse_failure", 3).await?;
    seed_reverse(db.pool(), ADDRESS).await?;
    let second = "0x00000000000000000000000000000000000000a2";
    let shortened = matches!(failure, ReverseFailure::Shortened);
    if shortened {
        seed_reverse(db.pool(), second).await?;
    }
    let first = if shortened {
        format!("{MULTICALL_RESULTS_PREFIX}alice.eth|bob.eth")
    } else {
        "alice.eth".into()
    };
    let at_two = match failure {
        ReverseFailure::FailedCall => FAILED_MULTICALL,
        ReverseFailure::FailedBatch => FAILED_MULTICALL_BATCH,
        ReverseFailure::Shortened => "only-one.eth",
    };
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), first),
        (block_hash(1, 2), at_two.into()),
        (block_hash(1, 3), "recovered.eth".into()),
    ]))
    .await?;
    follow(db.pool(), &rpc.endpoint, 1, 1).await?;
    assert_primary(
        db.pool(),
        ADDRESS,
        "success",
        Some("alice.eth"),
        Some(&block_hash(1, 1)),
    )
    .await?;
    follow(db.pool(), &rpc.endpoint, 1, 2).await?;
    match failure {
        ReverseFailure::FailedCall => {
            assert_primary(db.pool(), ADDRESS, "not_found", None, None).await?
        }
        // The last observed name is served with the block it was observed at.
        ReverseFailure::FailedBatch => {
            assert_primary(
                db.pool(),
                ADDRESS,
                "success",
                Some("alice.eth"),
                Some(&block_hash(1, 1)),
            )
            .await?
        }
        ReverseFailure::Shortened => {
            for address in [ADDRESS, second] {
                assert_primary(
                    db.pool(),
                    address,
                    "success",
                    Some("only-one.eth"),
                    Some(&block_hash(1, 2)),
                )
                .await?;
            }
        }
    }
    if !shortened {
        follow(db.pool(), &rpc.endpoint, 1, 3).await?;
        assert_primary(
            db.pool(),
            ADDRESS,
            "success",
            Some("recovered.eth"),
            Some(&block_hash(1, 3)),
        )
        .await?;
    }
    rpc.server.abort();
    db.cleanup().await
}
#[tokio::test]
async fn failed_reverse_hydration_retracts_the_previous_head_value() -> Result<()> {
    reverse_failure(ReverseFailure::FailedCall).await
}
#[tokio::test]
async fn reverse_hydration_rpc_failure_keeps_the_previous_head_value() -> Result<()> {
    reverse_failure(ReverseFailure::FailedBatch).await
}
#[tokio::test]
async fn shortened_reverse_multicall_is_read_again_one_call_at_a_time() -> Result<()> {
    reverse_failure(ReverseFailure::Shortened).await
}

#[tokio::test]
async fn reverse_hydration_retracts_when_the_legacy_resolver_becomes_ineligible() -> Result<()> {
    let db = setup("live_family_reverse_admission", 2).await?;
    seed_reverse(db.pool(), ADDRESS).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([(block_hash(1, 1), "alice.eth".into())])).await?;
    follow(db.pool(), &rpc.endpoint, 1, 1).await?;
    event(db.pool(),2,"ResolverChanged","ens_v1_registry_l1",json!({"node":reverse_node(ADDRESS),"resolver":"0x0000000000000000000000000000000000000000"}),None,None).await?;
    publish(db.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    project(db.pool(), Some(urls(&rpc.endpoint)?), 1, 2, false).await?;
    assert_primary(db.pool(), ADDRESS, "not_found", None, None).await?;
    assert_eq!(rpc.calls.lock().unwrap().len(), 1);
    rpc.server.abort();
    db.cleanup().await
}

#[tokio::test]
async fn valueless_legacy_text_hydration_reads_once_until_rebuilt() -> Result<()> {
    let db = setup("live_family_text_rebuild", 3).await?;
    let resource = seed_text(db.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "https://one.test".into()),
        (block_hash(1, 3), String::new()),
    ]))
    .await?;
    follow(db.pool(), &rpc.endpoint, 1, 1).await?;
    let first = text_entry(db.pool(), resource).await?;
    assert_eq!(first["status"], "success");
    assert_eq!(first["value"], "https://one.test");
    assert_eq!(
        first["canonical_head_multicall_hydration"]["block_hash"],
        block_hash(1, 1)
    );
    project(db.pool(), Some(urls(&rpc.endpoint)?), 1, 1, false).await?;
    follow(db.pool(), &rpc.endpoint, 1, 2).await?;
    assert_eq!(text_entry(db.pool(), resource).await?, first);
    assert_eq!(rpc.calls.lock().unwrap().len(), 1);
    project(db.pool(), Some(urls(&rpc.endpoint)?), 1, 2, true).await?;
    assert_eq!(
        text_entry(db.pool(), resource).await?["status"],
        "unsupported"
    );
    assert_eq!(rpc.calls.lock().unwrap().len(), 1, "rebuild is RPC-free");
    follow(db.pool(), &rpc.endpoint, 1, 3).await?;
    let refreshed = text_entry(db.pool(), resource).await?;
    assert_eq!(refreshed["status"], "not_found");
    assert!(refreshed.get("value").is_none());
    assert_eq!(
        refreshed["canonical_head_multicall_hydration"]["block_hash"],
        block_hash(1, 3)
    );
    rpc.server.abort();
    db.cleanup().await
}

#[tokio::test]
async fn text_hydration_rejects_unknown_resolvers_and_restores_ineligible_values() -> Result<()> {
    let db = setup("live_family_text_admission", 2).await?;
    let resource = seed_text(db.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([(block_hash(1, 1), "known".into())])).await?;
    follow(db.pool(), &rpc.endpoint, 1, 1).await?;
    // Actual manifest retirement retracts admission for an existing hydrated selector.
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,derivation_kind,canonicality_state,after_state) SELECT 'text-retired',namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,2,$1,derivation_kind,'canonical',jsonb_set(after_state,'{rollout_status}','\"retired\"') FROM normalized_events WHERE event_identity='text-manifest'")
        .bind(block_hash(1,2)).execute(db.pool()).await?;
    event(db.pool(),2,"RecordChanged","ens_v1_resolver_l1",json!({"node":TEXT_NODE,"resolver":"0x0000000000000000000000000000000000000022","record_key":"text:unknown","record_family":"text","selector_key":"unknown","source_event":"TextChanged"}),None,None).await?;
    publish(db.pool(), ETHEREUM, 1, 2, 0, 0).await?;
    project(db.pool(), Some(urls(&rpc.endpoint)?), 1, 2, false).await?;
    let entry = text_entry(db.pool(), resource).await?;
    assert_eq!(entry["status"], "unsupported");
    assert!(entry.get("value").is_none());
    let overlays: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_node_record_value WHERE hydrated_value IS NOT NULL",
    )
    .fetch_one(db.pool())
    .await?;
    assert_eq!(overlays, 0);
    assert_eq!(rpc.calls.lock().unwrap().len(), 1);
    rpc.server.abort();
    db.cleanup().await
}

/// Block 2 writes the text record again and its read fails, as a failed call or as a failed
/// batch. Either way nothing is served for the new write: a failed call clears the overlay, and
/// a failed batch leaves the overlay read for the replaced write, which the reader rejects.
async fn text_failure(failure: &str) -> Result<()> {
    let db = setup("live_family_text_failure", 3).await?;
    let resource = seed_text(db.pool()).await?;
    let rpc = HydrationRpc::spawn(BTreeMap::from([
        (block_hash(1, 1), "https://one.test".into()),
        (block_hash(1, 2), failure.into()),
        (block_hash(1, 3), "https://recovered.test".into()),
    ]))
    .await?;
    follow(db.pool(), &rpc.endpoint, 1, 1).await?;
    text_change(db.pool(), 2).await?;
    follow(db.pool(), &rpc.endpoint, 1, 2).await?;
    let entry = text_entry(db.pool(), resource).await?;
    assert_eq!(entry["status"], "unsupported");
    assert_eq!(
        entry["unsupported_reason"],
        "value_not_retained_in_normalized_events"
    );
    assert!(entry.get("value").is_none());
    assert!(entry.get("canonical_head_multicall_hydration").is_none());
    follow(db.pool(), &rpc.endpoint, 1, 3).await?;
    assert_eq!(
        text_entry(db.pool(), resource).await?["value"],
        "https://recovered.test"
    );
    rpc.server.abort();
    db.cleanup().await
}
#[tokio::test]
async fn failed_text_hydration_retracts_the_previous_head_value() -> Result<()> {
    text_failure(FAILED_MULTICALL).await
}
#[tokio::test]
async fn text_hydration_rpc_failure_serves_no_value_for_the_new_write() -> Result<()> {
    text_failure(FAILED_MULTICALL_BATCH).await
}
