//! Verified lookups hold the real family publication across provider execution.
use super::*;
use bigname_project::families::{FamilyMode, FamilyOptions};

pub(super) async fn reset_lookup_families(pool: &PgPool) -> AnyResult<()> {
    let token = bigname_project::families::input_token(pool, ETHEREUM).await?;
    let mut options = FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH);
    options.max_blocks_per_run = 0;
    let outcome = bigname_project::families::apply(
        pool,
        ETHEREUM,
        &bigname_project::Marker {
            number: 10,
            hash: ETHEREUM_HASH.into(),
        },
        FamilyMode::Rebuild,
        &token,
        &options,
    )
    .await?;
    anyhow::ensure!(
        outcome.reset && outcome.budget_exhausted && outcome.marker.is_none(),
        "incomplete rebuild: {outcome:?}"
    );
    Ok(())
}

#[tokio::test]
async fn lookup_is_stale_while_the_family_marker_bootstraps() -> AnyResult<()> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    reset_lookup_families(fixture.pool()).await?;
    let error = run_lookup(&fixture, "http://127.0.0.1:1")
        .await
        .expect_err("a reset before replay has no published family state");
    assert_eq!(error.kind(), ErrorKind::Stale);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn lookup_is_stale_before_the_first_family_publication() -> AnyResult<()> {
    let fixture = fixture::setup_unpublished_fixture().await?;
    let error = run_lookup(&fixture, "http://127.0.0.1:1")
        .await
        .expect_err("interpreted inputs without a family publication cannot be served");
    assert_eq!(error.kind(), ErrorKind::Stale);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn a_family_rebuild_during_provider_execution_refuses_the_comparison_write() -> AnyResult<()>
{
    for complete in [false, true] {
        let (rpc_url, rpc_handle) =
            spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(LIVE_VALUE))]).await?;
        let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
        let pool = fixture.pool().clone();
        let result = lookup_engine(fixture.pool(), &rpc_url)?
            .lookup_with_before_persist(
                lookup_request(&fixture.logical_name_id)?,
                move || async move {
                    reset_lookup_families(&pool)
                        .await
                        .expect("reset real family publication");
                    if complete {
                        publish_lookup_families(&pool, ETHEREUM, 10, FamilyMode::Normal)
                            .await
                            .expect("complete replacement publication");
                    }
                },
            )
            .await;
        let error = result.expect_err("a changed held publication must prevent the ledger write");
        assert_eq!(
            error.kind(),
            ErrorKind::ConcurrentState,
            "completed rebuild {complete}: {error}"
        );
        assert_eq!(ledger_count(fixture.pool()).await?, 0);
        fixture.cleanup().await?;
        join_rpc(rpc_handle).await?;
    }
    Ok(())
}
