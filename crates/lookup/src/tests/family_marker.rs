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
