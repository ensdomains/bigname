//! ENS verified record reads follow CCIP-Read through the same gateway transport as the Basenames
//! records and the ENS/60 forward leg. The Universal Resolver re-raises a resolver's
//! `OffchainLookup` as its own
//! (upstream: .refs/ens_v1/contracts/ccipRead/CCIPReader.sol:L72-L94 @ ens_v1@91c966f),
//! so the lookups here name it as the sender unless a test says otherwise.
use super::*;

use crate::LookupRecordStatus;

const CALLBACK: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
const EFFECTIVE: &str = "0x3000000000000000000000000000000000000003";

fn ens_offchain_lookup(urls: Vec<String>) -> AnyResult<String> {
    Ok(encode_offchain_lookup_for_test(
        Address::from_str(UNIVERSAL_RESOLVER)?,
        urls,
        vec![0x12, 0x34],
        CALLBACK,
        vec![0xab],
    ))
}

async fn ens_fixture(null_exact_resolver: bool) -> AnyResult<Fixture> {
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    if null_exact_resolver {
        make_null_resolver_discovery(&fixture).await?;
    }
    Ok(fixture)
}

async fn answering_gateway() -> AnyResult<(String, Arc<AtomicUsize>, JoinHandle<AnyResult<()>>)> {
    spawn_status_gateway(
        "200 OK",
        String::new(),
        json!({ "data": "0xcafe" }).to_string(),
    )
    .await
}

fn live_answer() -> AnyResult<RpcResponse> {
    Ok(RpcResponse::Result(encoded_text_result_with_resolver(
        LIVE_VALUE, EFFECTIVE,
    )?))
}

fn assert_callbacks_reach_the_universal_resolver(requests: &[Value]) {
    assert_hash_pinned(requests, ETHEREUM_HASH);
    for callback in &requests[1..] {
        assert_eq!(callback["params"][0]["to"], UNIVERSAL_RESOLVER);
        assert!(
            callback["params"][0]["data"]
                .as_str()
                .is_some_and(|data| data.starts_with("0x01020304")),
            "the callback must call the OffchainLookup callback function: {callback}"
        );
    }
}

#[tokio::test]
async fn ens_offchain_lookup_is_followed_served_and_never_persisted() -> AnyResult<()> {
    // The exact-resolver route has an indexed comparison that a direct answer differing from
    // the indexed value would write; the null-exact-resolver route has none.
    for null_exact_resolver in [false, true] {
        let (gateway_url, gateway_hits, gateway) = answering_gateway().await?;
        let (rpc_url, rpc_handle) = spawn_mock_rpc(ccip_chain_responses(
            vec![ens_offchain_lookup(vec![gateway_url])?],
            Some(live_answer()?),
        ))
        .await?;
        let fixture = ens_fixture(null_exact_resolver).await?;

        let response = run_lookup(&fixture, &rpc_url).await?;
        let record = &response.records[0];
        assert_eq!(record.status, LookupRecordStatus::Success, "{record:?}");
        assert_eq!(record.value, Some(json!(LIVE_VALUE)));
        assert!(record.ccip_read);
        assert_eq!(record.ledger_action, LedgerAction::SkippedCcip);
        assert_eq!(ledger_count(fixture.pool()).await?, 0);
        assert_eq!(gateway_hits.load(Ordering::SeqCst), 1);

        fixture.cleanup().await?;
        let requests = join_rpc(rpc_handle).await?;
        assert_eq!(requests.len(), 2, "initial call and one callback");
        assert_callbacks_reach_the_universal_resolver(&requests);
        gateway.abort();
    }
    Ok(())
}

#[tokio::test]
async fn ens_gateway_connect_refusal_fails_the_whole_request() -> AnyResult<()> {
    let (rpc_url, rpc_handle) = spawn_mock_rpc(ccip_chain_responses(
        vec![ens_offchain_lookup(vec![closed_port_url().await?])?],
        None,
    ))
    .await?;
    let fixture = ens_fixture(true).await?;

    let error = run_lookup(&fixture, &rpc_url)
        .await
        .expect_err("a refused gateway connection is a transport failure");
    assert_eq!(error.kind(), ErrorKind::Transport);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    assert_eq!(join_rpc(rpc_handle).await?.len(), 1, "no callback may run");
    Ok(())
}

#[tokio::test]
async fn ens_hanging_gateways_fail_in_band_within_the_ccip_read_budget() -> AnyResult<()> {
    let (first_url, first_handle) = spawn_hanging_rpc().await?;
    let (second_url, second_handle) = spawn_hanging_rpc().await?;
    let (rpc_url, rpc_handle) = spawn_mock_rpc(ccip_chain_responses(
        vec![ens_offchain_lookup(vec![first_url, second_url])?],
        None,
    ))
    .await?;
    let fixture = ens_fixture(true).await?;

    let started = std::time::Instant::now();
    let response = run_lookup(&fixture, &rpc_url).await?;
    let elapsed = started.elapsed();

    let record = &response.records[0];
    assert!(record.ccip_read);
    assert_eq!(record.status, LookupRecordStatus::ExecutionFailed);
    assert_eq!(
        record.failure_reason.as_deref(),
        Some("resolver_call_failed"),
        "{record:?}"
    );
    // One stalled URL alone would take the 1500 ms per-request timeout.
    assert!(
        elapsed < Duration::from_millis(1500),
        "budget did not bound the chain: {elapsed:?}"
    );

    fixture.cleanup().await?;
    assert_eq!(join_rpc(rpc_handle).await?.len(), 1, "no callback may run");
    first_handle.abort();
    second_handle.abort();
    Ok(())
}

#[tokio::test]
async fn ens_gateway_client_error_fails_in_band_without_trying_later_urls() -> AnyResult<()> {
    let (refusing_url, refusing_hits, refusing) =
        spawn_status_gateway("404 Not Found", String::new(), String::new()).await?;
    let (later_url, later) = spawn_untouchable_gateway().await?;
    let (rpc_url, rpc_handle) = spawn_mock_rpc(ccip_chain_responses(
        vec![ens_offchain_lookup(vec![refusing_url, later_url])?],
        None,
    ))
    .await?;
    let fixture = ens_fixture(true).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    let record = &response.records[0];
    assert!(record.ccip_read);
    assert_eq!(record.status, LookupRecordStatus::ExecutionFailed);
    assert_eq!(
        record.failure_reason.as_deref(),
        Some("ccip_read_failed"),
        "{record:?}"
    );
    assert_eq!(refusing_hits.load(Ordering::SeqCst), 1);
    assert!(!later.is_finished(), "a later URL must not be contacted");

    fixture.cleanup().await?;
    assert_eq!(join_rpc(rpc_handle).await?.len(), 1, "no callback may run");
    refusing.abort();
    later.abort();
    Ok(())
}

#[tokio::test]
async fn ens_gateway_server_error_falls_through_to_the_next_url() -> AnyResult<()> {
    let (failing_url, failing_hits, failing) =
        spawn_status_gateway("503 Service Unavailable", String::new(), String::new()).await?;
    let (gateway_url, gateway_hits, gateway) = answering_gateway().await?;
    let (rpc_url, rpc_handle) = spawn_mock_rpc(ccip_chain_responses(
        vec![ens_offchain_lookup(vec![failing_url, gateway_url])?],
        Some(live_answer()?),
    ))
    .await?;
    let fixture = ens_fixture(true).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    let record = &response.records[0];
    assert_eq!(record.status, LookupRecordStatus::Success, "{record:?}");
    assert_eq!(record.value, Some(json!(LIVE_VALUE)));
    assert!(record.ccip_read);
    assert_eq!(failing_hits.load(Ordering::SeqCst), 1);
    assert_eq!(gateway_hits.load(Ordering::SeqCst), 1);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests.len(), 2);
    assert_callbacks_reach_the_universal_resolver(&requests);
    failing.abort();
    gateway.abort();
    Ok(())
}

#[tokio::test]
async fn ens_undecodable_gateway_body_fails_in_band_before_the_callback() -> AnyResult<()> {
    let (gateway_url, gateway_hits, gateway) =
        spawn_status_gateway("200 OK", String::new(), "not hex".to_owned()).await?;
    let (rpc_url, rpc_handle) = spawn_mock_rpc(ccip_chain_responses(
        vec![ens_offchain_lookup(vec![gateway_url])?],
        None,
    ))
    .await?;
    let fixture = ens_fixture(true).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    let record = &response.records[0];
    assert!(record.ccip_read);
    assert_eq!(
        record.failure_reason.as_deref(),
        Some("ccip_read_failed"),
        "{record:?}"
    );
    assert_eq!(gateway_hits.load(Ordering::SeqCst), 1);

    fixture.cleanup().await?;
    assert_eq!(join_rpc(rpc_handle).await?.len(), 1, "no callback may run");
    gateway.abort();
    Ok(())
}

#[tokio::test]
async fn ens_malformed_callback_return_data_fails_in_band() -> AnyResult<()> {
    let (gateway_url, _gateway_hits, gateway) = answering_gateway().await?;
    let (rpc_url, rpc_handle) = spawn_mock_rpc(ccip_chain_responses(
        vec![ens_offchain_lookup(vec![gateway_url])?],
        Some(RpcResponse::Result(json!("0x1234"))),
    ))
    .await?;
    let fixture = ens_fixture(false).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    let record = &response.records[0];
    assert!(record.ccip_read);
    assert_eq!(record.status, LookupRecordStatus::ExecutionFailed);
    assert_eq!(
        record.failure_reason.as_deref(),
        Some("resolver_return_data_malformed"),
        "{record:?}"
    );
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    assert_eq!(join_rpc(rpc_handle).await?.len(), 2);
    gateway.abort();
    Ok(())
}

#[tokio::test]
async fn ens_gateway_redirect_is_not_followed() -> AnyResult<()> {
    let (target_url, target) = spawn_untouchable_gateway().await?;
    let (redirect_url, redirect_hits, redirect) = spawn_status_gateway(
        "302 Found",
        format!("location: {target_url}\r\n"),
        String::new(),
    )
    .await?;
    let (rpc_url, rpc_handle) = spawn_mock_rpc(ccip_chain_responses(
        vec![ens_offchain_lookup(vec![redirect_url])?],
        None,
    ))
    .await?;
    let fixture = ens_fixture(true).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    let record = &response.records[0];
    assert!(record.ccip_read);
    assert_eq!(
        record.failure_reason.as_deref(),
        Some("ccip_read_failed"),
        "{record:?}"
    );
    assert_eq!(redirect_hits.load(Ordering::SeqCst), 1);
    assert!(
        !target.is_finished(),
        "the redirect target must not be contacted"
    );

    fixture.cleanup().await?;
    assert_eq!(join_rpc(rpc_handle).await?.len(), 1, "no callback may run");
    redirect.abort();
    target.abort();
    Ok(())
}

#[tokio::test]
async fn ens_offchain_lookup_from_another_sender_is_refused_before_the_gateway() -> AnyResult<()> {
    let (gateway_url, gateway) = spawn_untouchable_gateway().await?;
    let offchain_data = encode_offchain_lookup_for_test(
        Address::repeat_byte(0x99),
        vec![gateway_url],
        vec![0x12, 0x34],
        CALLBACK,
        vec![0xab],
    );
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(ccip_chain_responses(vec![offchain_data], None)).await?;
    let fixture = ens_fixture(true).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    let record = &response.records[0];
    assert!(record.ccip_read);
    assert_eq!(
        record.failure_reason.as_deref(),
        Some("ccip_read_failed"),
        "{record:?}"
    );
    assert!(!gateway.is_finished(), "the gateway must not be contacted");

    fixture.cleanup().await?;
    assert_eq!(join_rpc(rpc_handle).await?.len(), 1, "no callback may run");
    gateway.abort();
    Ok(())
}

#[tokio::test]
async fn ens_four_nested_offchain_lookups_resolve() -> AnyResult<()> {
    let (gateway_url, gateway_hits, gateway) = answering_gateway().await?;
    let lookups = (0..4)
        .map(|_| ens_offchain_lookup(vec![gateway_url.clone()]))
        .collect::<AnyResult<Vec<_>>>()?;
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(ccip_chain_responses(lookups, Some(live_answer()?))).await?;
    let fixture = ens_fixture(true).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    let record = &response.records[0];
    assert_eq!(record.status, LookupRecordStatus::Success, "{record:?}");
    assert_eq!(record.value, Some(json!(LIVE_VALUE)));
    assert!(record.ccip_read);
    assert_eq!(gateway_hits.load(Ordering::SeqCst), 4);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests.len(), 5, "initial call and four callbacks");
    assert_callbacks_reach_the_universal_resolver(&requests);
    gateway.abort();
    Ok(())
}

#[tokio::test]
async fn ens_fifth_nested_offchain_lookup_fails_in_band_as_ccip_read_failed() -> AnyResult<()> {
    let (gateway_url, gateway_hits, gateway) = answering_gateway().await?;
    let lookups = (0..5)
        .map(|_| ens_offchain_lookup(vec![gateway_url.clone()]))
        .collect::<AnyResult<Vec<_>>>()?;
    let (rpc_url, rpc_handle) = spawn_mock_rpc(ccip_chain_responses(lookups, None)).await?;
    let fixture = ens_fixture(true).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    let record = &response.records[0];
    assert!(record.ccip_read);
    assert_eq!(record.status, LookupRecordStatus::ExecutionFailed);
    assert_eq!(
        record.failure_reason.as_deref(),
        Some("ccip_read_failed"),
        "a callback still asking for a gateway after four steps is a malformed read: {record:?}"
    );
    assert_eq!(gateway_hits.load(Ordering::SeqCst), 4);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(requests.len(), 5, "initial call and four callbacks");
    assert_callbacks_reach_the_universal_resolver(&requests);
    gateway.abort();
    Ok(())
}

#[tokio::test]
async fn ens_gateway_url_list_stops_after_four_unreachable_urls() -> AnyResult<()> {
    let (fifth_url, fifth_hits, fifth) = answering_gateway().await?;
    let mut urls = Vec::with_capacity(5);
    for _ in 0..4 {
        urls.push(closed_port_url().await?);
    }
    urls.push(fifth_url);
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(ccip_chain_responses(vec![ens_offchain_lookup(urls)?], None)).await?;
    let fixture = ens_fixture(true).await?;

    let error = run_lookup(&fixture, &rpc_url)
        .await
        .expect_err("four refused gateway connections are a transport failure");
    assert_eq!(error.kind(), ErrorKind::Transport);
    assert_eq!(
        fifth_hits.load(Ordering::SeqCst),
        0,
        "a fifth URL must not be tried"
    );

    fixture.cleanup().await?;
    assert_eq!(join_rpc(rpc_handle).await?.len(), 1, "no callback may run");
    fifth.abort();
    Ok(())
}

#[tokio::test]
async fn ens_reverting_callback_fails_in_band_and_writes_nothing() -> AnyResult<()> {
    let (gateway_url, _gateway_hits, gateway) = answering_gateway().await?;
    let (rpc_url, rpc_handle) = spawn_mock_rpc(ccip_chain_responses(
        vec![ens_offchain_lookup(vec![gateway_url])?],
        Some(RpcResponse::Error {
            code: 3,
            message: "execution reverted".to_owned(),
            data: json!("0x08c379a0"),
        }),
    ))
    .await?;
    let fixture = ens_fixture(false).await?;

    let response = run_lookup(&fixture, &rpc_url).await?;
    let record = &response.records[0];
    assert!(record.ccip_read);
    assert_eq!(record.status, LookupRecordStatus::ExecutionFailed);
    assert_eq!(
        record.failure_reason.as_deref(),
        Some("resolver_call_reverted"),
        "{record:?}"
    );
    assert_eq!(record.ledger_action, LedgerAction::SkippedCcip);
    assert_eq!(ledger_count(fixture.pool()).await?, 0);

    fixture.cleanup().await?;
    assert_eq!(join_rpc(rpc_handle).await?.len(), 2);
    gateway.abort();
    Ok(())
}

#[tokio::test]
async fn ens_mixed_ccip_and_direct_keys_share_one_effective_resolver() -> AnyResult<()> {
    let fixture = ens_fixture(true).await?;
    for (callback_resolver, consistent) in [
        (EFFECTIVE, true),
        ("0x4000000000000000000000000000000000000004", false),
    ] {
        let (gateway_url, _gateway_hits, gateway) = answering_gateway().await?;
        let (rpc_url, rpc_handle) = spawn_mixed_ccip_rpc(
            ens_offchain_lookup(vec![gateway_url])?,
            encoded_text_result_with_resolver(LIVE_VALUE, EFFECTIVE)?,
            encoded_text_result_with_resolver("ipfs://ccip-avatar", callback_resolver)?,
        )
        .await?;
        let result = lookup_engine(fixture.pool(), &rpc_url)?
            .lookup(LookupRequest::new(
                &fixture.logical_name_id,
                ["avatar", "text:url"],
            )?)
            .await;

        if consistent {
            let response = result?;
            let avatar = response
                .records
                .iter()
                .find(|record| record.record_key == "avatar")
                .context("mixed lookup omitted avatar")?;
            assert!(avatar.ccip_read);
            assert_eq!(
                avatar.value,
                Some(json!("ipfs://ccip-avatar")),
                "{avatar:?}"
            );
            let url = response
                .records
                .iter()
                .find(|record| record.record_key == "text:url")
                .context("mixed lookup omitted text:url")?;
            assert!(!url.ccip_read);
            assert_eq!(url.value, Some(json!(LIVE_VALUE)), "{url:?}");
        } else {
            let error = result.expect_err("a CCIP answer from another resolver must fail closed");
            assert_eq!(error.kind(), ErrorKind::Execution);
        }
        assert_eq!(ledger_count(fixture.pool()).await?, 0);
        assert_eq!(join_rpc(rpc_handle).await?.len(), 3);
        gateway.abort();
    }
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn ens_primary_name_forward_leg_follows_a_gateway() -> AnyResult<()> {
    let target = "0x8e8db5ccef88cca9d624701db544989c996e3216";
    let reverse_resolver = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";
    let (gateway_url, gateway_hits, gateway) = answering_gateway().await?;
    let mut responses = vec![
        RpcResponse::Result(Value::String(hex_string(
            &Address::from_str(reverse_resolver)?.abi_encode(),
        ))),
        RpcResponse::Result(Value::String(hex_string(&"alice.eth".abi_encode()))),
    ];
    responses.extend(ccip_chain_responses(
        vec![ens_offchain_lookup(vec![gateway_url])?],
        Some(RpcResponse::Result(encoded_address_result(target)?)),
    ));
    let (rpc_url, rpc_handle) = spawn_mock_rpc(responses).await?;
    let fixture = setup_fixture(FixtureKind::Ens, INDEXED_VALUE).await?;
    seed_manifest(
        fixture.pool(),
        ENS_NAMESPACE,
        "ens_v1_registry_l1",
        "registry",
        ENS_REGISTRY,
        "00000000-0000-0000-0000-000000000104",
    )
    .await?;
    sqlx::query(
        "UPDATE manifest_versions
         SET manifest_payload = '{}'::jsonb
         WHERE source_family = 'ens_v1_registry_l1'",
    )
    .execute(fixture.pool())
    .await?;

    let result = lookup_engine(fixture.pool(), &rpc_url)?
        .lookup_ens_primary_name(ETHEREUM, target)
        .await?;
    assert_eq!(result.status, EnsPrimaryNameStatus::Success, "{result:?}");
    assert_eq!(result.forward_address.as_deref(), Some(target));
    assert!(result.ccip_read);
    assert_eq!(gateway_hits.load(Ordering::SeqCst), 1);

    fixture.cleanup().await?;
    let requests = join_rpc(rpc_handle).await?;
    assert_eq!(
        requests.len(),
        4,
        "registry, reverse resolver, forward call, callback"
    );
    assert_eq!(requests[2]["params"][0]["to"], UNIVERSAL_RESOLVER);
    assert_callbacks_reach_the_universal_resolver(&requests[2..]);
    assert_hash_pinned(&requests, ETHEREUM_HASH);
    gateway.abort();
    Ok(())
}
