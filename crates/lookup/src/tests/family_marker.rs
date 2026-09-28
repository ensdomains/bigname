//! The verified lookup's publication fence under the publication switch (TYR-36 step 7b): with
//! the switch on, a family marker that is not `live` (a rebuild still populating the owned key
//! families) makes the lookup stale; with the switch off the marker is ignored.

use super::*;

async fn seed_family_marker(pool: &PgPool, state: &str) -> AnyResult<()> {
    sqlx::query(
        "INSERT INTO project_family_marker
             (chain_id, current_block_number, current_block_hash, block_timestamp,
              input_content_hash, sequence, state)
         SELECT project.chain_id, project.current_block_number, project.current_block_hash,
                lineage.block_timestamp, project.input_content_hash, 1, $2
         FROM chain_phase_state project
         JOIN chain_lineage lineage
           ON lineage.chain_id = project.chain_id
          AND lineage.block_number = project.current_block_number
          AND lineage.block_hash = project.current_block_hash
         WHERE project.chain_id = $1 AND project.phase_name = 'project'",
    )
    .bind(ETHEREUM)
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test]
async fn lookup_is_stale_while_the_family_marker_bootstraps_with_the_switch_on() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_family_marker(fixture.pool(), "bootstrap_pending").await?;

    let error = bigname_storage::publication_source::with_serve_from_families(
        true,
        lookup_engine(fixture.pool(), "http://127.0.0.1:1")?
            .lookup(lookup_request(&fixture.logical_name_id)?),
    )
    .await
    .expect_err("a marker still populating the families is not servable");
    assert_eq!(error.kind(), ErrorKind::Stale);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn lookup_serves_beside_a_live_family_marker_and_ignores_it_with_the_switch_off()
-> AnyResult<()> {
    for (on, state) in [(true, "live"), (false, "bootstrap_pending")] {
        let (rpc_url, rpc_handle) =
            spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
        let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
        seed_family_marker(fixture.pool(), state).await?;
        let response = bigname_storage::publication_source::with_serve_from_families(
            on,
            run_lookup(&fixture, &rpc_url),
        )
        .await?;
        assert_eq!(
            response.records[0].value,
            Some(json!(LIVE_VALUE)),
            "switch {on}, marker {state}"
        );
        fixture.cleanup().await?;
        join_rpc(rpc_handle).await?;
    }
    Ok(())
}

#[tokio::test]
async fn lookup_is_stale_without_a_family_marker_row_with_the_switch_on() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    let lookup = |on| {
        bigname_storage::publication_source::with_serve_from_families(
            on,
            run_lookup(&fixture, "http://127.0.0.1:1"),
        )
    };

    let error = lookup(true)
        .await
        .expect_err("a chain without a marker row has no published families");
    assert_eq!(error.kind(), ErrorKind::Stale);
    assert_eq!(
        error.message(),
        format!(
            "owned key families or projected state have not reached the newest processed \
             {ETHEREUM} block"
        )
    );

    // Switch off, the Project row governs and the wording is unchanged.
    sqlx::query(
        "UPDATE chain_phase_state SET input_content_hash = 'another-build' WHERE phase_name = 'project'",
    )
    .execute(fixture.pool())
    .await?;
    let error = lookup(false)
        .await
        .expect_err("a Project row from another build is not servable");
    assert_eq!(error.kind(), ErrorKind::Stale);
    assert_eq!(
        error.message(),
        format!("projected state has not reached the newest processed {ETHEREUM} block")
    );
    fixture.cleanup().await?;
    Ok(())
}

/// Runs a lookup whose provider call has answered, then `mutate`s the database before the guarded
/// comparison write, as a publication landing during provider execution would.
async fn lookup_mutated_during_execution(
    on: bool,
    state: &str,
    mutate: &'static str,
) -> AnyResult<(crate::Result<LookupResponse>, i64)> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_family_marker(fixture.pool(), state).await?;
    let pool = fixture.pool().clone();
    let update_pool = pool.clone();
    let result = bigname_storage::publication_source::with_serve_from_families(
        on,
        lookup_engine(&pool, &rpc_url)?.lookup_with_before_persist(
            lookup_request(&fixture.logical_name_id)?,
            move || async move {
                sqlx::query(mutate)
                    .execute(&update_pool)
                    .await
                    .expect("mutate the publication during provider execution");
            },
        ),
    )
    .await;
    let ledger = ledger_count(&pool).await?;
    fixture.cleanup().await?;
    join_rpc(rpc_handle).await?;
    Ok((result, ledger))
}

const REPUBLISH_PROJECT_ROW: &str = "UPDATE chain_phase_state \
     SET current_block_number = current_block_number WHERE phase_name = 'project'";
const ADVANCE_FAMILY_SEQUENCE: &str = "UPDATE project_family_marker SET sequence = sequence + 1";
const START_FAMILY_REBUILD: &str = "UPDATE project_family_marker \
     SET state = 'bootstrap_pending', sequence = sequence + 1";

/// The flip's guard (TYR-36 step 7b-6): with the switch on, a family block committed
/// while the provider call runs, with the Project row unchanged, is refused with exactly the error
/// the served guard gives when the Project row is republished with the switch off, and no
/// divergence row is written.
#[tokio::test]
async fn a_family_block_during_execution_is_refused_like_a_project_republish() -> AnyResult<()> {
    let (served, served_ledger) =
        lookup_mutated_during_execution(false, "live", REPUBLISH_PROJECT_ROW).await?;
    let served = served.expect_err("the served guard refuses a republished Project row");
    assert_eq!(served.kind(), ErrorKind::ConcurrentState);
    assert_eq!(served_ledger, 0);

    let (family, family_ledger) =
        lookup_mutated_during_execution(true, "live", ADVANCE_FAMILY_SEQUENCE).await?;
    let family = family.expect_err("the marker guard refuses an advanced family sequence");
    assert_eq!(family.kind(), served.kind());
    assert_eq!(family.message(), served.message());
    assert_eq!(
        family_ledger, 0,
        "a refused lookup writes no divergence row"
    );
    Ok(())
}

/// With the switch on the guard reads the marker, so a Project row republished during execution
/// no longer refuses the lookup once the marker is unchanged; with it off the marker is not read,
/// so an advanced family sequence does not refuse it.
#[tokio::test]
async fn each_switch_state_guards_only_its_own_publication() -> AnyResult<()> {
    for (on, mutate) in [
        (true, REPUBLISH_PROJECT_ROW),
        (false, ADVANCE_FAMILY_SEQUENCE),
    ] {
        let (response, ledger) = lookup_mutated_during_execution(on, "live", mutate).await?;
        let response = response.map_err(|error| {
            anyhow::anyhow!("switch {on}: {:?} {}", error.kind(), error.message())
        })?;
        assert_eq!(
            response.records[0].value,
            Some(json!(LIVE_VALUE)),
            "switch {on}"
        );
        assert_eq!(
            response.records[0].ledger_action,
            LedgerAction::Written,
            "switch {on}"
        );
        assert_eq!(ledger, 1, "switch {on}");
    }
    Ok(())
}

/// A family rebuild that starts while the provider call runs leaves the marker
/// `bootstrap_pending`, so with the switch on the lookup is refused (the API's 409 stale) and
/// writes nothing; with the switch off the same rebuild changes nothing.
#[tokio::test]
async fn a_family_rebuild_during_execution_refuses_the_lookup_only_with_the_switch_on()
-> AnyResult<()> {
    let (refused, ledger) =
        lookup_mutated_during_execution(true, "live", START_FAMILY_REBUILD).await?;
    let refused = refused.expect_err("a rebuilding marker is not servable");
    assert_eq!(refused.kind(), ErrorKind::ConcurrentState);
    assert_eq!(ledger, 0);

    let (served, ledger) =
        lookup_mutated_during_execution(false, "live", START_FAMILY_REBUILD).await?;
    let served =
        served.map_err(|error| anyhow::anyhow!("{:?} {}", error.kind(), error.message()))?;
    assert_eq!(served.records[0].ledger_action, LedgerAction::Written);
    assert_eq!(ledger, 1);
    Ok(())
}

async fn guard_status(pool: &PgPool, execution_authority: &Value) -> AnyResult<String> {
    Ok(sqlx::query_scalar(
        "SELECT revalidate_resolution_lookup_state($1, 10, $2, $3, $4, NULL, NULL, NULL)",
    )
    .bind(ETHEREUM)
    .bind(ETHEREUM_HASH)
    .bind(observed_position(10, ETHEREUM_HASH, "2026-08-03T00:00:00Z"))
    .bind(execution_authority)
    .fetch_one(pool)
    .await?)
}

/// The guard's own answers for a captured family publication: unchanged while the marker is, and
/// the served guard's `project_changed` for a stale sequence, a publication without its
/// sequence, or a marker that is no longer `live`.
#[tokio::test]
async fn the_guard_compares_the_captured_family_sequence() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_family_marker(fixture.pool(), "live").await?;
    let request = lookup_request(&fixture.logical_name_id)?;
    let captured = bigname_storage::publication_source::with_serve_from_families(
        true,
        crate::store::load_snapshot(fixture.pool(), &request),
    )
    .await?
    .execution_authority;
    assert_eq!(captured["family_publication"]["sequence"], json!("1"));
    assert_eq!(guard_status(fixture.pool(), &captured).await?, "unchanged");

    let mut stale = captured.clone();
    stale["family_publication"]["sequence"] = json!("0");
    assert_eq!(
        guard_status(fixture.pool(), &stale).await?,
        "project_changed"
    );

    let mut unsequenced = captured.clone();
    unsequenced["family_publication"]
        .as_object_mut()
        .expect("family publication object")
        .remove("sequence");
    // Only the lookup builds this object; a field it lacks fails the match and refuses.
    assert_eq!(
        guard_status(fixture.pool(), &unsequenced).await?,
        "project_changed"
    );

    sqlx::query("UPDATE project_family_marker SET state = 'bootstrap_pending'")
        .execute(fixture.pool())
        .await?;
    assert_eq!(
        guard_status(fixture.pool(), &captured).await?,
        "project_changed"
    );

    // Captured with the switch off, the authority carries no family publication and the guard
    // keeps comparing the Project row, whatever the marker says.
    let served = bigname_storage::publication_source::with_serve_from_families(
        false,
        crate::store::load_snapshot(fixture.pool(), &request),
    )
    .await?
    .execution_authority;
    assert!(served.get("family_publication").is_none());
    assert_eq!(guard_status(fixture.pool(), &served).await?, "unchanged");
    fixture.cleanup().await?;
    Ok(())
}
