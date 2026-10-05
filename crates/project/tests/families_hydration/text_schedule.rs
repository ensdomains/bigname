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
