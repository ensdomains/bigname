use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use bigname_ingest::{VerificationLog, VerificationMarker, WatchFilter};

use super::*;
use crate::{
    error::ErrorKind, heads::BlockMarker, phase::VerificationLevel,
    verify_phase::VerificationReferenceFuture,
};

struct Responses(Mutex<VecDeque<RunnerResult<VerificationBatch>>>);

impl VerificationReferenceProvider for Responses {
    fn preflight(&self, _: &VerificationSource) -> RunnerResult<()> {
        Ok(())
    }
    fn fetch<'a>(
        &'a self,
        _: &'a VerificationSource,
        filter: WatchFilter,
        from: i64,
        to: i64,
    ) -> VerificationReferenceFuture<'a> {
        assert_eq!((from, to), (0, 10));
        assert_eq!(filter, WatchFilter::default());
        let response = self
            .0
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra fetch");
        Box::pin(async move { response })
    }
}

fn source(kind: VerificationProviderKind) -> VerificationSource {
    VerificationSource {
        chain_id: "ethereum-sepolia".to_owned(),
        source_key: "reference".to_owned(),
        source_kind: "drpc".to_owned(),
        endpoint: Arc::from("https://unused.invalid"),
        provider_kind: kind,
        level: VerificationLevel::CrossChecked,
        cross_check_through: None,
    }
}

fn batch(index: i64) -> VerificationBatch {
    VerificationBatch {
        end: VerificationMarker {
            number: 10,
            hash: "0xend".to_owned(),
        },
        logs: vec![VerificationLog {
            block_hash: "0xblock".to_owned(),
            block_number: 6,
            transaction_hash: "0xtx".to_owned(),
            transaction_index: 44,
            log_index: index,
            address: "0xaddress".to_owned(),
            topics: vec!["0xtopic".to_owned()],
            data: vec![1],
        }],
        rpc_request_count: 3,
    }
}

fn stored() -> StoredVerificationBatch {
    StoredVerificationBatch {
        end: BlockMarker {
            number: 10,
            hash: "0xend".to_owned(),
        },
        filter: WatchFilter::default(),
        logs: batch(121).logs,
    }
}

#[tokio::test]
async fn correct_first_response_needs_no_retry() {
    let responses = Responses(Mutex::new(VecDeque::from([Ok(batch(121))])));
    let result = fetch_matching(
        &responses,
        &source(VerificationProviderKind::IndependentRpc),
        &stored(),
        0,
        10,
    )
    .await
    .unwrap();
    assert_eq!(result.rpc_request_count, 3);
}

#[tokio::test]
async fn second_response_must_match_and_counts_both_requests() {
    let responses = Responses(Mutex::new(VecDeque::from([Ok(batch(115)), Ok(batch(121))])));
    let result = fetch_matching(
        &responses,
        &source(VerificationProviderKind::IndependentRpc),
        &stored(),
        0,
        10,
    )
    .await
    .unwrap();
    assert_eq!(result.logs[0].log_index, 121);
    assert_eq!(result.rpc_request_count, 6);
}

#[tokio::test]
async fn two_different_mismatches_still_fail_without_a_third_fetch() {
    let responses = Responses(Mutex::new(VecDeque::from([Ok(batch(115)), Ok(batch(120))])));
    let error = fetch_matching(
        &responses,
        &source(VerificationProviderKind::IndependentRpc),
        &stored(),
        0,
        10,
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::VerificationMismatch);
    assert!(!error.is_retryable());
    assert!(error.to_string().contains("raw_logs[120]"));
}

#[tokio::test]
async fn local_reth_mismatch_remains_immediately_fatal() {
    let responses = Responses(Mutex::new(VecDeque::from([Ok(batch(115))])));
    let error = fetch_matching(
        &responses,
        &source(VerificationProviderKind::LocalReth),
        &stored(),
        0,
        10,
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::VerificationMismatch);
}

#[tokio::test]
async fn failed_second_fetch_cannot_turn_a_mismatch_into_success() {
    let responses = Responses(Mutex::new(VecDeque::from([
        Ok(batch(115)),
        Err(RunnerError::data_integrity("invalid reference response")),
    ])));
    let error = fetch_matching(
        &responses,
        &source(VerificationProviderKind::IndependentRpc),
        &stored(),
        0,
        10,
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::DataIntegrity);
}
