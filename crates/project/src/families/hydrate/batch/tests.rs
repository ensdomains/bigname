use super::*;

fn sizes(limits: &[Option<usize>]) -> Vec<Vec<usize>> {
    groups(limits)
        .into_iter()
        .map(|group| group.members)
        .collect()
}

#[test]
fn selectors_without_a_limit_fill_whole_aggregates_in_request_order() {
    let plan = sizes(&vec![None; 600]);
    assert_eq!(
        plan.iter().map(Vec::len).collect::<Vec<_>>(),
        [250, 250, 100]
    );
    assert_eq!(plan.concat(), (0..600).collect::<Vec<_>>());
}

#[test]
fn a_limited_selector_is_never_sent_in_a_larger_aggregate_than_its_limit() {
    // Two selectors that failed alone, three left with a limit of two, the rest unlimited.
    let limits = [
        Some(1),
        None,
        Some(2),
        Some(2),
        None,
        Some(1),
        Some(2),
        Some(0),
    ];
    let plan = sizes(&limits);
    assert_eq!(
        plan,
        [vec![0], vec![1, 4], vec![2, 3], vec![5], vec![6], vec![7]],
        "aggregates go out in the order of their earliest request"
    );
    for group in &plan {
        let smallest = group
            .iter()
            .map(|member| limits[*member].map_or(BATCH_LIMIT, |limit| limit.max(1)))
            .min()
            .expect("no aggregate is empty");
        assert!(group.len() <= smallest);
    }
}

#[derive(Default)]
struct Recorder(std::sync::Mutex<Vec<Vec<usize>>>);

impl Aggregate for Recorder {
    type Request = usize;
    type Answer = usize;

    async fn send(&self, chunk: &[usize]) -> Result<Vec<usize>, Failure> {
        self.0.lock().unwrap().push(chunk.to_vec());
        Ok(chunk.to_vec())
    }
}

#[tokio::test]
async fn waiting_work_gets_a_rounded_up_quarter_before_fresh_singletons() {
    let head = BlockHeader {
        number: 1,
        hash: "head".into(),
        predecessor_hash: None,
        timestamp_seconds: 0,
        timestamp: serde_json::Value::Null,
    };
    let urls = ChainRpcUrls::default();
    for (old, expected) in [
        (
            (20..35).collect::<Vec<_>>(),
            (20..25).chain(0..12).collect::<Vec<_>>(),
        ),
        (vec![], (0..17).collect()),
        ((0..35).collect(), (0..17).collect()),
    ] {
        let requests: Vec<_> = (0..35).collect();
        let waiting: Vec<_> = requests
            .iter()
            .map(|request| old.contains(request))
            .collect();
        let call = Recorder::default();
        let mut stats = HydrationOutcome::default();
        let mut session = Session::new(
            "ethereum-mainnet",
            &urls,
            &head,
            HydrationTimeLimits::default(),
            &mut stats,
        );
        let reads = session
            .read(Kind::Text, &requests, &[Some(1); 35], &waiting, &call)
            .await;
        let sent: Vec<_> = call.0.lock().unwrap().iter().flatten().copied().collect();
        assert_eq!(sent, expected, "unused reservation goes to available work");
        assert_eq!(
            reads
                .iter()
                .filter(|read| matches!(read, Read::Answered(_)))
                .count(),
            17
        );
        assert_eq!(
            stats.text.rpc_calls, 17,
            "reservation does not increase the budget"
        );
    }
}

struct RejectOld;

impl Aggregate for RejectOld {
    type Request = usize;
    type Answer = usize;

    async fn send(&self, chunk: &[usize]) -> Result<Vec<usize>, Failure> {
        if chunk.iter().any(|request| *request < 16) {
            Err(Failure {
                message: "old aggregate rejected".into(),
                block_unavailable: false,
            })
        } else {
            Ok(chunk.to_vec())
        }
    }
}

#[tokio::test]
async fn yielding_reserved_calls_records_completed_old_splits_before_fresh_work() {
    let head = BlockHeader {
        number: 1,
        hash: "head".into(),
        predecessor_hash: None,
        timestamp_seconds: 0,
        timestamp: serde_json::Value::Null,
    };
    let urls = ChainRpcUrls::default();
    let mut stats = HydrationOutcome::default();
    let mut session = Session::new(
        "ethereum-mainnet",
        &urls,
        &head,
        HydrationTimeLimits::default(),
        &mut stats,
    );
    // Earlier answered work in this pass establishes availability; the fake transport models
    // content failures without making a real availability probe.
    session.serves = Some(true);
    let requests: Vec<_> = (0..17).collect();
    let waiting: Vec<_> = requests.iter().map(|request| *request < 16).collect();
    let reads = session
        .read(Kind::Text, &requests, &[None; 17], &waiting, &RejectOld)
        .await;
    assert!(
        reads[..16]
            .iter()
            .all(|read| matches!(read, Read::Deferred { limit: 1..=8 }))
    );
    assert!(matches!(reads[16], Read::Answered(16)));
    assert_eq!(
        stats.text.rpc_calls, 6,
        "five old calls then the fresh aggregate"
    );
    assert_eq!(
        stats.text.deferred, 16,
        "completed parent progress survives the switch"
    );
}

struct LoseBlock(std::sync::atomic::AtomicUsize);

impl Aggregate for LoseBlock {
    type Request = usize;
    type Answer = usize;

    async fn send(&self, _chunk: &[usize]) -> Result<Vec<usize>, Failure> {
        let call = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        Err(Failure {
            message: "lost at the reservation boundary".into(),
            block_unavailable: call == 5,
        })
    }
}

#[tokio::test]
async fn losing_the_block_at_the_reservation_boundary_does_not_defer_pending_halves() {
    let head = BlockHeader {
        number: 1,
        hash: "head".into(),
        predecessor_hash: None,
        timestamp_seconds: 0,
        timestamp: serde_json::Value::Null,
    };
    let urls = ChainRpcUrls::default();
    let mut stats = HydrationOutcome::default();
    let mut session = Session::new(
        "ethereum-mainnet",
        &urls,
        &head,
        HydrationTimeLimits::default(),
        &mut stats,
    );
    session.serves = Some(true);
    let requests: Vec<_> = (0..17).collect();
    let waiting: Vec<_> = requests.iter().map(|request| *request < 16).collect();
    let reads = session
        .read(
            Kind::Text,
            &requests,
            &[None; 17],
            &waiting,
            &LoseBlock(std::sync::atomic::AtomicUsize::new(0)),
        )
        .await;
    assert!(reads.iter().all(|read| matches!(read, Read::Unobserved)));
    session.finish();
    assert_eq!(stats.text.rpc_calls, 5);
    assert_eq!(stats.text.deferred, 0);
    assert_eq!(stats.unserved_passes, 1);
}
