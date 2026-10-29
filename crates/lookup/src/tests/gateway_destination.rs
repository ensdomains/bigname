//! The CCIP-Read gateway destination filter on the verified read paths. A refused destination
//! answers the record in band as `gateway_destination_refused`, tries no later URL, runs no
//! callback and writes nothing. Loopback refusals point at a listener the filter has not
//! admitted, so a hit count of zero proves the refused socket was never contacted. Public but
//! unroutable stand-ins come from 192.0.2.0/24, 203.0.113.0/24 and 2001:db8:2::/48. The
//! process-wide operator list is 198.51.100.0/24 and 2001:db8:1::/48.
use std::net::SocketAddr;

use super::ens_ccip::{
    answering_gateway, ens_fixture, ens_offchain_lookup, live_answer, primary_name_fixture,
};
use super::*;
use crate::{LookupRecordResult, LookupRecordStatus, ccip::GatewayRefusalClass as Class};

const REFUSED: &str = "gateway_destination_refused";
const CALLBACK: [u8; 4] = [0x01, 0x02, 0x03, 0x04];
const PRIMARY_TARGET: &str = "0x8e8db5ccef88cca9d624701db544989c996e3216";
const REVERSE_RESOLVER: &str = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";

/// An unadmitted gateway's address, its count of accepted connections, and its task.
type Unadmitted = (SocketAddr, Arc<AtomicUsize>, JoinHandle<AnyResult<()>>);

/// A loopback gateway the filter has not admitted, bound on `bind`. It answers like a working
/// gateway, so a read that reaches it is served, and counts every connection it accepts.
async fn spawn_unadmitted_gateway(bind: &str) -> AnyResult<Unadmitted> {
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("the test host must be able to bind {bind}"))?;
    let address = listener.local_addr()?;
    seam::withdraw(address);
    let hits = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::clone(&hits);
    let handle = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await?;
            accepted.fetch_add(1, Ordering::SeqCst);
            let _ = read_http_json_body(&mut socket).await;
            write_http_response(&mut socket, &json!({ "data": "0xcafe" }).to_string()).await?;
        }
    });
    Ok((address, hits, handle))
}

/// Scripts the answers for a new, unique `*.gateway.test` name and returns it. `label` must be
/// lowercase, because URL parsing lowercases the host the resolver is asked for.
fn script_host(label: &str, answers: Vec<Vec<SocketAddr>>) -> String {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let host = format!(
        "{label}-{}.gateway.test",
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    seam::script_host(&host, answers);
    host
}

fn peer(text: &str) -> SocketAddr {
    text.parse()
        .unwrap_or_else(|_| panic!("{text} is a socket address"))
}

fn gateway_peer(url: &str) -> AnyResult<SocketAddr> {
    url.trim_start_matches("http://")
        .parse()
        .with_context(|| format!("{url} is a loopback gateway URL"))
}

/// Reads `text:url` on the ENS fixture through one `OffchainLookup` revert, with `callback`
/// answering the callback when given. Returns the served record and the refusal classes the
/// lookup reported, after checking that only the expected calls ran and nothing was written.
async fn ens_read_lookup(
    fixture: &Fixture,
    offchain_data: String,
    callback: Option<RpcResponse>,
) -> AnyResult<(LookupRecordResult, Vec<Class>)> {
    let calls = 1 + usize::from(callback.is_some());
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(ccip_chain_responses(vec![offchain_data], callback)).await?;
    let (response, refusals) = seam::capture_refusals(run_lookup(fixture, &rpc_url)).await;
    let response = response.context("the read must answer in band, not fail the request")?;
    assert_eq!(ledger_count(fixture.pool()).await?, 0);
    assert_eq!(
        join_rpc(rpc_handle).await?.len(),
        calls,
        "a refused read runs no callback"
    );
    Ok((response.records[0].clone(), refusals))
}

async fn ens_read(
    fixture: &Fixture,
    urls: Vec<String>,
    callback: Option<RpcResponse>,
) -> AnyResult<(LookupRecordResult, Vec<Class>)> {
    ens_read_lookup(fixture, ens_offchain_lookup(urls)?, callback).await
}

/// Reads through `urls` and returns the whole-request error the read fails with.
async fn ens_read_error(
    fixture: &Fixture,
    urls: Vec<String>,
) -> AnyResult<(crate::LookupError, Vec<Class>)> {
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(ccip_chain_responses(vec![ens_offchain_lookup(urls)?], None)).await?;
    let (response, refusals) = seam::capture_refusals(run_lookup(fixture, &rpc_url)).await;
    let Err(error) = response else {
        bail!("the read must fail the whole request");
    };
    assert_eq!(join_rpc(rpc_handle).await?.len(), 1, "no callback may run");
    Ok((error, refusals))
}

/// Reads through `url` and checks that the filter judged the peer `judged` admitted and refused
/// nothing. The peer is unroutable, but a middlebox may answer for it, so the read may fail in
/// band for another reason or fail the request as a transport error. Either is accepted.
async fn assert_not_refused(fixture: &Fixture, url: &str, judged: SocketAddr) -> AnyResult<()> {
    let offchain_data = ens_offchain_lookup(vec![url.to_owned()])?;
    let (rpc_url, rpc_handle) =
        spawn_mock_rpc(ccip_chain_responses(vec![offchain_data], None)).await?;
    let ((response, refusals), judged_admitted) =
        seam::capture_judged_admitted(seam::capture_refusals(run_lookup(fixture, &rpc_url))).await;
    assert!(refusals.is_empty(), "{url}: {refusals:?}");
    match response {
        Ok(response) => {
            let record = &response.records[0];
            assert_eq!(
                record.status,
                LookupRecordStatus::ExecutionFailed,
                "{url}: {record:?}"
            );
            assert_ne!(
                record.failure_reason.as_deref(),
                Some(REFUSED),
                "{url}: {record:?}"
            );
        }
        Err(error) => assert_eq!(error.kind(), ErrorKind::Transport, "{url}: {error}"),
    }
    assert_eq!(
        judged_admitted,
        vec![judged],
        "{url}: the filter must judge the literal once"
    );
    assert_eq!(
        join_rpc(rpc_handle).await?.len(),
        1,
        "{url}: no callback may run"
    );
    Ok(())
}

#[track_caller]
fn assert_refused(record: &LookupRecordResult, refusals: &[Class], class: Class, case: &str) {
    assert_eq!(
        record.status,
        LookupRecordStatus::ExecutionFailed,
        "{case}: {record:?}"
    );
    assert_eq!(
        record.failure_reason.as_deref(),
        Some(REFUSED),
        "{case}: {record:?}"
    );
    assert!(record.ccip_read, "{case}");
    assert_eq!(record.value, None, "{case}");
    assert_eq!(refusals.to_vec(), vec![class], "{case}: one observation");
}

#[track_caller]
fn assert_served(record: &LookupRecordResult, refusals: &[Class], case: &str) {
    assert_eq!(
        record.status,
        LookupRecordStatus::Success,
        "{case}: {record:?}"
    );
    assert_eq!(record.value, Some(json!(LIVE_VALUE)), "{case}");
    assert!(record.ccip_read, "{case}");
    assert!(refusals.is_empty(), "{case}: {refusals:?}");
}

/// Each literal URL on one ENS fixture, refused under its class.
async fn assert_literals_refused(cases: &[(String, Class)]) -> AnyResult<()> {
    let fixture = ens_fixture(true).await?;
    for (url, class) in cases {
        let (record, refusals) = ens_read(&fixture, vec![url.clone()], None).await?;
        assert_refused(&record, &refusals, *class, url);
    }
    fixture.cleanup().await
}

fn cases(literals: &[(&str, Class)]) -> Vec<(String, Class)> {
    literals
        .iter()
        .map(|(url, class)| ((*url).to_owned(), *class))
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum VerifiedPath {
    Basenames,
    Ens,
    PrimaryName,
}

const PATHS: [VerifiedPath; 3] = [
    VerifiedPath::Basenames,
    VerifiedPath::Ens,
    VerifiedPath::PrimaryName,
];

struct PathOutcome {
    failed: bool,
    failure_reason: Option<String>,
    ccip_read: bool,
    refusals: Vec<Class>,
}

/// Runs one read on `path` through an `OffchainLookup` carrying `urls`: Basenames records at the
/// L1 resolver, ENS verified records and the ENS/60 forward leg at the Universal Resolver.
async fn read_on(path: VerifiedPath, urls: Vec<String>) -> AnyResult<PathOutcome> {
    let sender = match path {
        VerifiedPath::Basenames => BASE_L1_RESOLVER,
        VerifiedPath::Ens | VerifiedPath::PrimaryName => UNIVERSAL_RESOLVER,
    };
    let offchain_data = encode_offchain_lookup_for_test(
        Address::from_str(sender)?,
        urls,
        vec![0x12, 0x34],
        CALLBACK,
        vec![0xab],
    );
    let mut responses = Vec::new();
    if matches!(path, VerifiedPath::PrimaryName) {
        responses.push(RpcResponse::Result(Value::String(hex_string(
            &Address::from_str(REVERSE_RESOLVER)?.abi_encode(),
        ))));
        responses.push(RpcResponse::Result(Value::String(hex_string(
            &"alice.eth".abi_encode(),
        ))));
    }
    responses.push(offchain_revert(offchain_data));
    let calls = responses.len();
    let (rpc_url, rpc_handle) = spawn_mock_rpc(responses).await?;
    let fixture = match path {
        VerifiedPath::Basenames => setup_fixture(FixtureKind::Basenames, INDEXED_VALUE).await?,
        VerifiedPath::Ens => ens_fixture(true).await?,
        VerifiedPath::PrimaryName => primary_name_fixture().await?,
    };
    let engine = lookup_engine(fixture.pool(), &rpc_url)?;
    let request = lookup_request(&fixture.logical_name_id)?;
    let read = async move {
        match path {
            VerifiedPath::PrimaryName => engine
                .lookup_ens_primary_name(ETHEREUM, PRIMARY_TARGET)
                .await
                .map(|result| {
                    let failed = result.status == EnsPrimaryNameStatus::ExecutionFailed;
                    (failed, result.failure_reason, result.ccip_read)
                }),
            VerifiedPath::Basenames | VerifiedPath::Ens => {
                engine.lookup(request).await.map(|response| {
                    let record = &response.records[0];
                    let failed = record.status == LookupRecordStatus::ExecutionFailed;
                    (failed, record.failure_reason.clone(), record.ccip_read)
                })
            }
        }
    };
    let (outcome, refusals) = seam::capture_refusals(read).await;
    let (failed, failure_reason, ccip_read) = outcome
        .with_context(|| format!("{path:?}: the read must answer in band, not fail the request"))?;
    assert_eq!(ledger_count(fixture.pool()).await?, 0, "{path:?}");
    fixture.cleanup().await?;
    assert_eq!(
        join_rpc(rpc_handle).await?.len(),
        calls,
        "{path:?}: no callback may run"
    );
    Ok(PathOutcome {
        failed,
        failure_reason,
        ccip_read,
        refusals,
    })
}

#[track_caller]
fn assert_path_refused(outcome: &PathOutcome, class: Class, path: VerifiedPath) {
    assert!(outcome.failed, "{path:?}");
    assert_eq!(outcome.failure_reason.as_deref(), Some(REFUSED), "{path:?}");
    assert!(outcome.ccip_read, "{path:?}");
    assert_eq!(outcome.refusals, vec![class], "{path:?}: one observation");
}

// Row 1, and row 27 for its class.
#[tokio::test]
async fn literal_loopback_gateway_is_refused_on_every_verified_path() -> AnyResult<()> {
    for path in PATHS {
        let (address, hits, gateway) = spawn_unadmitted_gateway("127.0.0.1:0").await?;
        let outcome = read_on(path, vec![format!("http://{address}/")]).await?;
        assert_path_refused(&outcome, Class::Loopback, path);
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "{path:?}: the peer was contacted"
        );
        gateway.abort();
    }
    Ok(())
}

// Row 2.
#[tokio::test]
async fn literal_private_gateways_are_refused() -> AnyResult<()> {
    assert_literals_refused(&cases(&[
        ("http://10.0.0.1/", Class::Private),
        ("http://172.16.0.1/", Class::Private),
        ("http://192.168.0.1/", Class::Private),
    ]))
    .await
}

// Row 3.
#[tokio::test]
async fn literal_link_local_gateway_is_refused() -> AnyResult<()> {
    assert_literals_refused(&cases(&[("http://169.254.169.254/", Class::LinkLocal)])).await
}

// Row 4.
#[tokio::test]
async fn literal_shared_address_space_gateway_is_refused() -> AnyResult<()> {
    assert_literals_refused(&cases(&[("http://100.64.0.1/", Class::Cgnat)])).await
}

// Row 5.
#[tokio::test]
async fn literal_unspecified_gateways_are_refused() -> AnyResult<()> {
    let (address, hits, gateway) = spawn_unadmitted_gateway("127.0.0.1:0").await?;
    assert_literals_refused(&[
        (
            format!("http://0.0.0.0:{}/", address.port()),
            Class::Unspecified,
        ),
        ("http://0.1.2.3/".to_owned(), Class::Unspecified),
    ])
    .await?;
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "the local host was contacted"
    );
    gateway.abort();
    Ok(())
}

// Row 6. A runner without IPv6 loopback fails here rather than skipping.
#[tokio::test]
async fn literal_ipv6_loopback_gateway_is_refused() -> AnyResult<()> {
    let (address, hits, gateway) = spawn_unadmitted_gateway("[::1]:0").await?;
    assert_literals_refused(&[(format!("http://{address}/"), Class::Loopback)]).await?;
    assert_eq!(hits.load(Ordering::SeqCst), 0, "the peer was contacted");
    gateway.abort();
    Ok(())
}

// Row 7.
#[tokio::test]
async fn literal_ipv6_local_gateways_are_refused() -> AnyResult<()> {
    assert_literals_refused(&cases(&[
        ("http://[fd00::1]/", Class::UniqueLocal),
        ("http://[fe80::1]/", Class::LinkLocal),
    ]))
    .await
}

// Row 8.
#[tokio::test]
async fn literal_mapped_ipv6_gateways_are_refused_under_the_ipv4_class() -> AnyResult<()> {
    let (address, hits, gateway) = spawn_unadmitted_gateway("127.0.0.1:0").await?;
    assert_literals_refused(&[
        (
            format!("http://[::ffff:127.0.0.1]:{}/", address.port()),
            Class::Loopback,
        ),
        ("http://[::ffff:10.0.0.1]/".to_owned(), Class::Private),
    ])
    .await?;
    assert_eq!(hits.load(Ordering::SeqCst), 0, "the peer was contacted");
    gateway.abort();
    Ok(())
}

// Row 9.
#[tokio::test]
async fn literal_ipv4_compatible_gateway_is_refused() -> AnyResult<()> {
    assert_literals_refused(&cases(&[("http://[::127.0.0.1]/", Class::Ipv4Compatible)])).await
}

// Row 10.
#[tokio::test]
async fn shorthand_ipv4_literals_are_refused_as_the_address_they_canonicalise_to() -> AnyResult<()>
{
    let (address, hits, gateway) = spawn_unadmitted_gateway("127.0.0.1:0").await?;
    let port = address.port();
    assert_literals_refused(&[
        (format!("http://0x7f.1:{port}/"), Class::Loopback),
        (format!("http://2130706433:{port}/"), Class::Loopback),
    ])
    .await?;
    assert_eq!(hits.load(Ordering::SeqCst), 0, "the peer was contacted");
    gateway.abort();
    Ok(())
}

// Row 11.
#[tokio::test]
async fn literal_multicast_and_reserved_gateways_are_refused() -> AnyResult<()> {
    assert_literals_refused(&cases(&[
        ("http://224.0.0.1/", Class::Multicast),
        ("http://[ff02::1]/", Class::Multicast),
        ("http://255.255.255.255/", Class::Reserved),
        ("http://240.0.0.1/", Class::Reserved),
    ]))
    .await
}

// Row 12.
#[tokio::test]
async fn literal_nat64_and_6to4_gateways_are_refused_under_the_embedded_class() -> AnyResult<()> {
    assert_literals_refused(&cases(&[
        ("http://[64:ff9b::7f00:1]/", Class::Loopback),
        ("http://[64:ff9b::a00:1]/", Class::Private),
        ("http://[2002:a00:1::1]/", Class::Private),
    ]))
    .await
}

// Row 13, and row 27 for its class. The refusal is raised inside the connector's resolver, so
// this row also proves it is served in band rather than as a connect failure.
#[tokio::test]
async fn hostname_resolving_to_a_private_address_is_refused_on_every_path() -> AnyResult<()> {
    for path in PATHS {
        let host = script_host("private", vec![vec![peer("10.0.0.1:0")]]);
        let outcome = read_on(path, vec![format!("http://{host}/")]).await?;
        assert_path_refused(&outcome, Class::Private, path);
    }
    Ok(())
}

// Row 14.
#[tokio::test]
async fn hostname_with_one_refused_record_among_public_ones_is_refused_whole() -> AnyResult<()> {
    let (address, hits, gateway) = spawn_unadmitted_gateway("127.0.0.1:0").await?;
    let host = script_host("mixed", vec![vec![peer("203.0.113.1:0"), address]]);
    let fixture = ens_fixture(true).await?;

    let (record, refusals) = ens_read(&fixture, vec![format!("http://{host}/")], None).await?;
    assert_eq!(
        record.failure_reason.as_deref(),
        Some(REFUSED),
        "{record:?}"
    );
    assert_eq!(refusals.len(), 1, "{refusals:?}");
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "the loopback record was dialled"
    );

    fixture.cleanup().await?;
    gateway.abort();
    Ok(())
}

// Row 15. The first resolution answers an admitted gateway that fails with a 500, so the read
// falls through to the second URL, whose resolution answers an unadmitted loopback address. The
// connector dials only the answer the filter judged: the first gateway once, the rebound address
// never.
#[tokio::test]
async fn hostname_that_rebinds_between_urls_is_judged_on_each_resolution() -> AnyResult<()> {
    let (first_url, first_hits, first) =
        spawn_status_gateway("500 Internal Server Error", String::new(), String::new()).await?;
    let (address, hits, gateway) = spawn_unadmitted_gateway("127.0.0.1:0").await?;
    let host = script_host(
        "rebinding",
        vec![vec![gateway_peer(&first_url)?], vec![address]],
    );
    let fixture = ens_fixture(true).await?;

    let urls = vec![format!("http://{host}/"), format!("http://{host}/again")];
    let (record, refusals) = ens_read(&fixture, urls, None).await?;
    assert_refused(&record, &refusals, Class::Loopback, &host);
    assert_eq!(
        first_hits.load(Ordering::SeqCst),
        1,
        "the first answer was not dialled"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "the rebound address was dialled"
    );

    fixture.cleanup().await?;
    first.abort();
    gateway.abort();
    Ok(())
}

// Row 16. The resolver passes an admitted address through to the connector.
#[tokio::test]
async fn hostname_resolving_to_an_admitted_address_is_served() -> AnyResult<()> {
    let (gateway_url, gateway_hits, gateway) = answering_gateway().await?;
    let host = script_host("admitted", vec![vec![gateway_peer(&gateway_url)?]]);
    let fixture = ens_fixture(true).await?;

    let (record, refusals) = ens_read(
        &fixture,
        vec![format!("http://{host}/")],
        Some(live_answer()?),
    )
    .await?;
    assert_served(&record, &refusals, &host);
    assert_eq!(gateway_hits.load(Ordering::SeqCst), 1);

    fixture.cleanup().await?;
    gateway.abort();
    Ok(())
}

// Row 17. Documentation ranges are not built in, so the filter admits the literal.
#[tokio::test]
async fn documentation_range_literal_is_not_refused_by_the_built_in_table() -> AnyResult<()> {
    let fixture = ens_fixture(true).await?;
    assert_not_refused(&fixture, "http://192.0.2.1/", peer("192.0.2.1:80")).await?;
    fixture.cleanup().await
}

// Row 18, and row 27 for its class.
#[tokio::test]
async fn operator_listed_literals_are_refused() -> AnyResult<()> {
    assert_literals_refused(&cases(&[
        ("http://198.51.100.7/", Class::Operator),
        ("http://[2001:db8:1::7]/", Class::Operator),
    ]))
    .await
}

// Row 19.
#[tokio::test]
async fn operator_list_refuses_nothing_outside_its_ranges() -> AnyResult<()> {
    let fixture = ens_fixture(true).await?;
    for (url, judged) in [
        ("http://203.0.113.7/", "203.0.113.7:80"),
        ("http://[2001:db8:2::7]/", "[2001:db8:2::7]:80"),
    ] {
        assert_not_refused(&fixture, url, peer(judged)).await?;
    }
    fixture.cleanup().await
}

// Row 20.
#[tokio::test]
async fn refused_first_url_is_not_recovered_by_a_later_one() -> AnyResult<()> {
    let (gateway_url, gateway_hits, gateway) = answering_gateway().await?;
    let fixture = ens_fixture(true).await?;

    let urls = vec!["http://10.0.0.1/".to_owned(), gateway_url];
    let (record, refusals) = ens_read(&fixture, urls, None).await?;
    assert_refused(&record, &refusals, Class::Private, "refused first");
    assert_eq!(
        gateway_hits.load(Ordering::SeqCst),
        0,
        "a later URL was tried"
    );

    fixture.cleanup().await?;
    gateway.abort();
    Ok(())
}

// Row 21.
#[tokio::test]
async fn admitted_first_url_is_served_before_a_refused_one_is_reached() -> AnyResult<()> {
    let (gateway_url, gateway_hits, gateway) = answering_gateway().await?;
    let fixture = ens_fixture(true).await?;

    let urls = vec![gateway_url, "http://10.0.0.1/".to_owned()];
    let (record, refusals) = ens_read(&fixture, urls, Some(live_answer()?)).await?;
    assert_served(&record, &refusals, "admitted first");
    assert_eq!(gateway_hits.load(Ordering::SeqCst), 1);

    fixture.cleanup().await?;
    gateway.abort();
    Ok(())
}

// Row 22.
#[tokio::test]
async fn batch_gateway_query_with_one_refused_inner_request_is_refused_whole() -> AnyResult<()> {
    let (gateway_url, _gateway_hits, gateway) = answering_gateway().await?;
    let sender = Address::from_str(UNIVERSAL_RESOLVER)?;
    let batch = encode_batch_query_for_test(vec![
        (sender, vec![gateway_url], vec![0x00]),
        (sender, vec!["http://10.0.0.1/".to_owned()], vec![0x01]),
    ]);
    let offchain_data = encode_offchain_lookup_for_test(
        sender,
        vec!["x-batch-gateway:true".to_owned()],
        batch,
        CALLBACK,
        vec![0xab],
    );
    let fixture = ens_fixture(true).await?;

    let (record, refusals) = ens_read_lookup(&fixture, offchain_data, None).await?;
    assert_refused(&record, &refusals, Class::Private, "batch");

    fixture.cleanup().await?;
    gateway.abort();
    Ok(())
}

// Row 23. A refused connection to an admitted peer stays a whole-request transport failure.
#[tokio::test]
async fn connect_refusal_from_an_admitted_peer_is_still_a_transport_failure() -> AnyResult<()> {
    let fixture = ens_fixture(true).await?;
    let (error, refusals) = ens_read_error(&fixture, vec![closed_port_url().await?]).await?;
    assert_eq!(error.kind(), ErrorKind::Transport, "{error}");
    assert!(refusals.is_empty(), "{refusals:?}");
    fixture.cleanup().await
}

// Row 24. Redirects stay unfollowed, so the refused target is never judged or contacted.
#[tokio::test]
async fn redirect_to_a_refused_literal_is_an_unsuccessful_status() -> AnyResult<()> {
    let (redirect_url, redirect_hits, redirect) = spawn_status_gateway(
        "302 Found",
        "location: http://10.0.0.1/\r\n".to_owned(),
        String::new(),
    )
    .await?;
    let fixture = ens_fixture(true).await?;

    let (record, refusals) = ens_read(&fixture, vec![redirect_url], None).await?;
    assert_eq!(
        record.failure_reason.as_deref(),
        Some("ccip_read_failed"),
        "{record:?}"
    );
    assert!(refusals.is_empty(), "{refusals:?}");
    assert_eq!(redirect_hits.load(Ordering::SeqCst), 1);

    fixture.cleanup().await?;
    redirect.abort();
    Ok(())
}
