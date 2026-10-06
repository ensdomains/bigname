//! Verified lookup of a name whose surface stores no raw bytes: the wire name comes from verified
//! label preimages, and without them the lookup is refused before any provider call.
use super::*;

/// Remove the stored bytes of the fixture name's surface, leaving its label-hash path.
async fn drop_surface_bytes(fixture: &Fixture) -> AnyResult<()> {
    let changed = sqlx::query(
        "UPDATE name_surfaces
         SET raw_name = NULL, raw_labels = NULL, dns_encoded_name = NULL,
             preimage_event_identity = NULL
         WHERE logical_name_id = $1",
    )
    .bind(&fixture.logical_name_id)
    .execute(fixture.pool())
    .await?
    .rows_affected();
    anyhow::ensure!(changed == 1, "the fixture name has one surface");
    Ok(())
}

async fn insert_preimage(pool: &PgPool, label: &str, verified: bool) -> AnyResult<()> {
    sqlx::query(
        "INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, normalization_error, source_kind, source_priority)
         VALUES ($1, $2, $3, $4, $5, $6, 'fixture', 0)",
    )
    .bind(format!("{:#x}", keccak256(label.as_bytes())))
    .bind(label.as_bytes())
    .bind(label)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(verified)
    .bind((!verified).then_some("not normalized"))
    .execute(pool)
    .await?;
    Ok(())
}

/// The provider call a lookup of the fixture name makes.
async fn provider_call(fixture: Fixture) -> AnyResult<Value> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(vec![RpcResponse::Result(encoded_text_result(
        INDEXED_VALUE,
    ))])
    .await?;
    let result = run_lookup(&fixture, &rpc_url).await;
    let outcome = finish_fixture(fixture, result).await?;
    assert_eq!(outcome.records[0].value, Some(json!(INDEXED_VALUE)));
    let mut requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests.len(), 1);
    Ok(requests.remove(0)["params"][0].take())
}

#[tokio::test]
async fn a_name_without_stored_bytes_is_refused_before_any_provider_call() -> AnyResult<()> {
    // No preimage at all, one label unknown, and one label known but not normalized.
    for preimages in [
        vec![],
        vec![("eth", true)],
        vec![("eth", true), ("alice", false)],
    ] {
        let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
        drop_surface_bytes(&fixture).await?;
        for (label, verified) in &preimages {
            insert_preimage(fixture.pool(), label, *verified).await?;
        }
        // Nothing listens here: reaching the provider would be a transport error.
        let error = run_lookup(&fixture, "http://127.0.0.1:1")
            .await
            .expect_err("no wire name can be built");
        assert_eq!(
            error.kind(),
            ErrorKind::Unsupported,
            "{preimages:?}: {error}"
        );
        assert_eq!(error.refusal(), None, "{preimages:?}");
        fixture.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn verified_label_bytes_give_the_call_the_stored_bytes_would_make() -> AnyResult<()> {
    let stored = provider_call(setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?).await?;

    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    drop_surface_bytes(&fixture).await?;
    for label in ["alice", "eth"] {
        insert_preimage(fixture.pool(), label, true).await?;
    }
    assert_eq!(provider_call(fixture).await?, stored);
    Ok(())
}
