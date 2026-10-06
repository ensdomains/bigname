use super::*;

#[tokio::test]
async fn two_failed_children_wait_without_restamping_and_new_text_resets_only_its_own_delay()
-> Result<()> {
    let (fixture, rpc) = fixture().await?;
    for (log, key) in [(2, "a"), (3, "b")] {
        keyed_text(&fixture, 1, log, key).await?;
    }
    rpc.answer(1, Some("unused"));
    rpc.fail_call(RESOLVER);
    let outcome = run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    assert_eq!(outcome.hydration.text.failed_calls, 2);
    let failed = fixture.rows("project_node_record_value").await?;
    let work = fixture.rows("project_text_hydration_work").await?;
    for block in [2, 3] {
        rpc.answer(block, Some("unused"));
        let outcome = run(&fixture, block, FamilyMode::Normal, &rpc).await?;
        assert_eq!(outcome.hydration.text.rpc_calls, 0);
        assert_eq!(
            outcome.hydration.text.value_writes + outcome.hydration.text.schedule_writes,
            0
        );
        assert_eq!(fixture.rows("project_node_record_value").await?, failed);
        assert_eq!(fixture.rows("project_text_hydration_work").await?, work);
    }
    assert!(selected(&fixture, 7200).await?.is_empty());
    assert_eq!(
        selected(&fixture, 7201).await?,
        ["a", "b"],
        "eligible exactly 7,200 blocks later"
    );

    // An unrelated key updates the same resolver's classification position, but not its meaning.
    rpc.clear_faults();
    keyed_text(&fixture, 4, 2, "unrelated").await?;
    rpc.answer(4, Some("fresh"));
    let outcome = run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    assert_eq!(outcome.hydration.text.answered, 1);
    assert!(selected(&fixture, 5).await?.is_empty());
    let before_change = fixture.rows("project_node_record_value").await?;
    let before_work = fixture.rows("project_text_hydration_work").await?;

    // The changed key is reset during a catch-up block. The later head reads it, not key b.
    keyed_text(&fixture, 5, 2, "a").await?;
    rpc.answer(6, Some("new a"));
    let outcome = run(&fixture, 6, FamilyMode::Normal, &rpc).await?;
    assert_eq!(outcome.hydration.text.answered, 1);
    assert_eq!(keys_at(&attempts(&fixture).await?, 6), ["a"]);
    let b: (Option<i64>, Option<i32>) = sqlx::query_as(
        "SELECT hydrated_at_block, hydration_failures FROM project_node_record_value WHERE selector_key='b'"
    ).fetch_one(&fixture.pool).await?;
    assert_eq!(b, (Some(1), Some(1)));

    families::undo_to(&fixture.pool, CHAIN, 4).await?;
    assert_eq!(
        fixture.rows("project_node_record_value").await?,
        before_change
    );
    assert_eq!(
        fixture.rows("project_text_hydration_work").await?,
        before_work
    );
    let calls = rpc.calls();
    run(&fixture, 6, FamilyMode::Redo { from: 5, to: 6 }, &rpc).await?;
    assert_eq!(rpc.calls(), calls, "replay resets metadata without RPC");
    assert_eq!(selected(&fixture, 7).await?, ["a"]);
    fixture.cleanup().await
}

#[tokio::test]
async fn old_text_keeps_sixty_three_slots_under_continuing_new_arrivals() -> Result<()> {
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    rpc.answer(1, Some("template"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    // This is the journalled state a deferred aggregate leaves, already covered by the split
    // tests. The old work sorts after every new key and still must make progress.
    copies(&fixture, "zz-old", 130,
        json!({"hydrated_value":null,"hydrated_at_block":1,"hydration_limit":125,"hydration_failures":1})).await?;
    sync_text_work(&fixture, 2).await?;
    for block in 2..=4 {
        for index in 0..260 {
            keyed_text(
                &fixture,
                block,
                index + 2,
                &format!("new-{block}-{index:03}"),
            )
            .await?;
        }
        rpc.answer(block, Some("read"));
        let outcome = run(&fixture, block, FamilyMode::Normal, &rpc).await?;
        assert_eq!(outcome.hydration.text.answered, 250);
        let old_read: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM project_node_record_value
             WHERE selector_key LIKE 'zz-old%' AND hydrated_at_block=$1",
        )
        .bind(block)
        .fetch_one(&fixture.pool)
        .await?;
        assert_eq!(old_read, if block < 4 { 63 } else { 4 });
    }
    fixture.cleanup().await
}

#[tokio::test]
async fn an_old_singleton_is_read_before_nine_new_poisoned_keys_spend_the_calls() -> Result<()> {
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    rpc.answer(1, Some("template"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    copies(&fixture, "zz-old", 3,
        json!({"hydrated_value":null,"hydrated_at_block":1,"hydration_limit":1,"hydration_failures":1})).await?;
    sync_text_work(&fixture, 2).await?;
    for block in 2..=4 {
        for index in 0..9 {
            let key = format!("bad-{block}-{index}");
            keyed_text(&fixture, block, index + 2, &key).await?;
            rpc.poison(&alloy_primitives::hex::encode(&key));
        }
        rpc.answer(block, Some("old read"));
        let outcome = run(&fixture, block, FamilyMode::Normal, &rpc).await?;
        assert!(outcome.hydration.text.rpc_calls <= 17);
        if block == 2 {
            assert_eq!(outcome.hydration.text.answered, 3);
        }
        let observed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM project_node_record_value
             WHERE selector_key LIKE 'zz-old%' AND hydrated_value IS NOT NULL",
        )
        .fetch_one(&fixture.pool)
        .await?;
        assert_eq!(
            observed, 3,
            "the arrival counterexample cannot starve the old singleton"
        );
    }
    fixture.cleanup().await
}

#[tokio::test]
async fn the_old_text_reservation_seeks_the_stamped_index_past_a_large_never_read_backlog()
-> Result<()> {
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    rpc.answer(1, Some("template"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    copies(
        &fixture,
        "new",
        50_000,
        json!({"hydrated_value":null,"hydrated_at_block":null}),
    )
    .await?;
    copies(
        &fixture,
        "zz-old",
        100,
        json!({"hydrated_value":null,"hydrated_at_block":1}),
    )
    .await?;
    // All copies are eligible null overlays; seed only their derived keys so setup does not
    // measure a full 50,100-row refresh. The actual bounded production selector is below.
    sqlx::query(
        "INSERT INTO project_text_hydration_work
        SELECT chain_id, resolver_address, arm, arm_identity, record_key,
            hydrated_at_block, hydration_failures
        FROM project_node_record_value WHERE hydrated_value IS NULL",
    )
    .execute(&fixture.pool)
    .await?;
    sqlx::raw_sql("ANALYZE project_text_hydration_work; ANALYZE project_node_record_value")
        .execute(&fixture.pool)
        .await?;
    let selected = selected(&fixture, 2).await?;
    assert_eq!(selected.len(), 250);
    assert_eq!(
        selected
            .iter()
            .filter(|key| key.starts_with("zz-old"))
            .count(),
        63
    );
    let plan = text_plan(&fixture, 2).await?;
    let generic = plans::generic(
        &fixture.pool,
        &text_selection_sql(),
        &format!("'ethereum-mainnet', 2, '[]', '[]', '[]', ARRAY['{RESOLVER}'], 250, '[]', false"),
    )
    .await?;
    for (label, plan) in [
        ("text-old-reserve-custom", plan),
        ("text-old-reserve-generic", generic),
    ] {
        plans::save(label, &plan)?;
        assert!(
            plans::visits(&plan, "project_text_hydration_work") <= 313.0,
            "{plan}"
        );
        assert!(
            plan.to_string()
                .contains("project_text_hydration_work_order_idx"),
            "{plan}"
        );
    }
    fixture.cleanup().await
}

async fn old_text_with_overlay_cleanup(version_block: i64) -> Result<()> {
    let (fixture, rpc) = fixture().await?;
    let old_node = format!("0x{}", "2".repeat(64));
    for index in 0..63 {
        keyed_text(&fixture, 1, index + 2, &format!("a-clean-{index:03}")).await?;
        let key = format!("z-old-{index:03}");
        fixture
            .event(
                Event::new(
                    &format!("old:{index}"),
                    1,
                    index + 65,
                    "RecordChanged",
                    "ens_v1_resolver_l1",
                )
                .on(CHAIN)
                .after(json!({
                    "node":old_node,"resolver":RESOLVER,"record_key":format!("text:{key}"),
                    "record_family":"text","selector_key":key,"source_event":"TextChanged"
                }))
                .raw(json!({"emitting_address":RESOLVER})),
            )
            .await?;
    }
    rpc.poison(&alloy_primitives::hex::encode("z-old-"));
    rpc.answer(1, Some("to be invalidated"));
    let outcome = run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    assert_eq!(outcome.hydration.text.answered, 63);
    assert_eq!(outcome.hydration.text.deferred, 63);
    fixture
        .event(
            Event::new(
                "cleanup-version",
                version_block,
                1,
                "RecordVersionChanged",
                "ens_v1_resolver_l1",
            )
            .on(CHAIN)
            .after(json!({"node":NODE,"resolver":RESOLVER})),
        )
        .await?;
    rpc::head(&fixture.pool, 3).await?;
    let calls = rpc.calls();
    let (outcome, error) = apply(&fixture, 2, FamilyMode::Normal, &rpc).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.hydration.passes, 0);
    assert_eq!(rpc.calls(), calls);
    if version_block == 2 {
        let cleanup: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM project_text_hydration_work q
             JOIN project_node_record_value v USING
               (chain_id,resolver_address,arm,arm_identity,record_key)
             WHERE v.selector_key LIKE 'a-clean-%' AND v.hydrated_value IS NOT NULL",
        )
        .fetch_one(&fixture.pool)
        .await?;
        assert_eq!(
            cleanup, 63,
            "catch-up leaves real stale overlays for head cleanup"
        );
    }
    for index in 0..250 {
        keyed_text(&fixture, 3, index + 2, &format!("new-{index:03}")).await?;
    }
    rpc.clear_faults();
    rpc.answer(3, Some("read"));
    run(&fixture, 3, FamilyMode::Normal, &rpc).await?;
    let old_read: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_node_record_value
         WHERE selector_key LIKE 'z-old-%' AND hydrated_at_block=3 AND hydrated_value IS NOT NULL",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        old_read, 63,
        "cleanup cannot consume the eligible old reservation"
    );
    // After fresh arrivals stop, ordinary selection still clears the stale overlays.
    rpc.answer(4, Some("rest"));
    run(&fixture, 4, FamilyMode::Normal, &rpc).await?;
    let cleanup: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_node_record_value
         WHERE selector_key LIKE 'a-clean-%' AND
           (hydrated_value IS NOT NULL OR hydrated_at_block IS NOT NULL)",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(cleanup, 0);
    fixture.cleanup().await
}

#[tokio::test]
async fn old_text_reservation_skips_overlay_cleanup_left_by_catch_up() -> Result<()> {
    old_text_with_overlay_cleanup(2).await
}

#[tokio::test]
async fn dependency_fan_out_at_the_head_is_not_reserved_as_unchanged_text() -> Result<()> {
    old_text_with_overlay_cleanup(3).await
}

async fn reassert_reverse_pointers(fixture: &Fixture, block: i64) -> Result<()> {
    for index in [1, 2] {
        fixture
            .event(
                Event::new(
                    &format!("same-pointer:{block}:{index}"),
                    block,
                    index,
                    "ResolverChanged",
                    "ens_v1_registry_l1",
                )
                .on(CHAIN)
                .after(
                    json!({"source_event":"NewResolver", "node":reverse::node(index),
                        "resolver":reverse::SILENT}),
                ),
            )
            .await?;
    }
    Ok(())
}

#[tokio::test]
async fn unchanged_reverse_pointer_events_keep_new_split_progress() -> Result<()> {
    use std::time::Duration;
    let (fixture, rpc) = fixture().await?;
    for index in [1, 2] {
        reverse::seed(&fixture, 1, index).await?;
    }
    rpc.answer(1, Some("before.eth"));
    run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    let observed = [
        reverse::tuple(&fixture, 1).await?,
        reverse::tuple(&fixture, 2).await?,
    ];
    for index in [1, 2] {
        reverse::seed(&fixture, 2, index).await?;
    }
    keyed_text(&fixture, 2, 20, "waiting").await?;
    for block in 2..=5 {
        rpc.answer(block, Some("after.eth"));
    }
    rpc.poison(RESOLVER);
    // Reverse has 2.5 seconds. The 0.8-second failed parent leaves less than the full
    // 2-second call allowance, so it defers two singletons. Each healthy singleton needs
    // 1.5 seconds: one fits, two do not. Failed text remains pending on every head.
    rpc.reject_from(2, Duration::from_millis(800), Duration::from_millis(1500));
    let options = reverse::options(&rpc).with_hydration_time_limits(
        bigname_project::families::HydrationTimeLimits {
            call: Duration::from_secs(2),
            block: Duration::from_secs(5),
        },
    );
    rpc::head(&fixture.pool, 2).await?;
    let (outcome, error) = reverse::apply(&fixture, &marker(2), FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(
        (
            outcome.hydration.reverse.answered,
            outcome.hydration.reverse.deferred
        ),
        (0, 2)
    );
    assert_eq!(outcome.hydration.reverse.rpc_calls, 1);
    assert_eq!(
        outcome.hydration.text.deferred, 1,
        "both kinds remain pending"
    );
    for (index, before) in [1, 2].into_iter().zip(&observed) {
        let deferred = reverse::tuple(&fixture, index).await?;
        assert_eq!(deferred["attempt_limit"], 1);
        assert_eq!(deferred["attempt_failures"], 1);
        assert_eq!(deferred["block_number"], 2);
        for field in ["hydrated_name", "attempt_block", "attempt_hash", "baseline"] {
            assert_eq!(
                deferred[field], before[field],
                "outer failure preserves {field}"
            );
        }
    }
    let deferred = fixture.rows("project_reverse_tuple").await?;
    let work = fixture.rows("project_reverse_hydration_work").await?;

    // An unchanged pointer event in catch-up must not erase the split saved for the fresh claim.
    reassert_reverse_pointers(&fixture, 3).await?;
    rpc::head(&fixture.pool, 4).await?;
    let calls = rpc.calls();
    let (outcome, error) = reverse::apply(&fixture, &marker(3), FamilyMode::Normal, &options).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(outcome.hydration.passes, 0);
    assert_eq!(fixture.rows("project_reverse_tuple").await?, deferred);
    assert_eq!(fixture.rows("project_reverse_hydration_work").await?, work);
    families::undo_to(&fixture.pool, CHAIN, 2).await?;
    assert_eq!(fixture.rows("project_reverse_tuple").await?, deferred);
    assert_eq!(fixture.rows("project_reverse_hydration_work").await?, work);
    let (_, error) = reverse::apply(
        &fixture,
        &marker(3),
        FamilyMode::Redo { from: 3, to: 3 },
        &options,
    )
    .await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(
        rpc.calls(),
        calls,
        "catch-up, undo and replay make no provider call"
    );
    assert_eq!(fixture.rows("project_reverse_tuple").await?, deferred);
    assert_eq!(fixture.rows("project_reverse_hydration_work").await?, work);

    for block in [4, 5] {
        reassert_reverse_pointers(&fixture, block).await?;
        rpc::head(&fixture.pool, block).await?;
        let (outcome, error) =
            reverse::apply(&fixture, &marker(block), FamilyMode::Normal, &options).await;
        assert!(error.is_none(), "{error:?}");
        assert!(
            outcome.hydration.reverse.answered > 0,
            "the saved singleton is reached"
        );
        assert_eq!(outcome.hydration.text.deferred, 1);
    }
    for index in [1, 2] {
        let refreshed = reverse::tuple(&fixture, index).await?;
        assert_eq!(refreshed["hydrated_name"], "after.eth");
        assert!(refreshed["attempt_block"].as_i64().unwrap() >= 4);
        assert!(refreshed["attempt_limit"].is_null());
    }
    fixture.cleanup().await
}

#[tokio::test]
#[ignore = "local characterization of a 50,000-row cooling backlog"]
async fn cooling_text_backlog_plans_with_and_without_eligible_work_behind_it() -> Result<()> {
    let (fixture, rpc) = fixture().await?;
    text(&fixture, 1, None).await?;
    rpc.answer(1, Some("unused"));
    rpc.fail_call(RESOLVER);
    let outcome = run(&fixture, 1, FamilyMode::Normal, &rpc).await?;
    assert_eq!(outcome.hydration.text.failed_calls, 1);
    // Expand the actual failed-child state into the distribution from 250 failed children
    // per head for 200 heads. This characterizes selection, not a 200-block replay benchmark.
    copies(&fixture, "cool", 49_999, json!({})).await?;
    sqlx::query(
        "UPDATE project_node_record_value SET
             hydrated_at_block=1+right(selector_key,5)::bigint/250,
             block_number=1+right(selector_key,5)::bigint/250,
             event_identity='cool-record:'||selector_key,
             log_index=right(selector_key,5)::bigint%250
         WHERE selector_key LIKE 'cool%'",
    )
    .execute(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_lineage
             (chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state)
         SELECT $1,'0x'||lpad(to_hex(b),64,'0'),'0x'||lpad(to_hex(b-1),64,'0'),
             b,to_timestamp(1800000000+b*12),'canonical' FROM generate_series(9,202) b",
    )
    .bind(CHAIN)
    .execute(&fixture.pool)
    .await?;
    // Each copy is eligible, has a null overlay, and is cooling. These are exactly the index
    // columns the production refresh derives; do not time a full fixture-index rebuild here.
    sqlx::query(
        "INSERT INTO project_text_hydration_work
         SELECT chain_id,resolver_address,arm,arm_identity,record_key,
             hydrated_at_block,hydration_failures
         FROM project_node_record_value WHERE selector_key LIKE 'cool%'",
    )
    .execute(&fixture.pool)
    .await?;
    for (label, eligible) in [("empty", 0), ("eligible-behind", 63)] {
        if eligible != 0 {
            // A completed outer failure leaves all 63 members a cap of ceil(63/2)=32.
            copies(
                &fixture,
                "zz-eligible",
                eligible,
                json!({"hydrated_at_block":201,"hydration_limit":32,"hydration_failures":1}),
            )
            .await?;
            sqlx::query(
                "INSERT INTO project_text_hydration_work
                 SELECT chain_id,resolver_address,arm,arm_identity,record_key,
                     hydrated_at_block,hydration_failures
                 FROM project_node_record_value WHERE selector_key LIKE 'zz-eligible%'",
            )
            .execute(&fixture.pool)
            .await?;
        }
        sqlx::raw_sql("ANALYZE project_text_hydration_work; ANALYZE project_node_record_value")
            .execute(&fixture.pool)
            .await?;
        assert_eq!(selected(&fixture, 202).await?.len(), eligible as usize);
        let custom = text_plan(&fixture, 202).await?;
        let generic = plans::generic(
            &fixture.pool,
            &text_selection_sql(),
            &format!(
                "'ethereum-mainnet', 202, '[]', '[]', '[]', ARRAY['{RESOLVER}'], 250, '[]', false"
            ),
        )
        .await?;
        for (mode, plan) in [("custom", custom), ("generic", generic)] {
            plans::save(&format!("text-cooling-{label}-{mode}"), &plan)?;
            assert_eq!(plan[0]["Plan"]["Actual Rows"], eligible);
        }
    }
    fixture.cleanup().await
}
