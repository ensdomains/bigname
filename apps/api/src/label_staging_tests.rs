use std::time::Duration;

use super::*;

fn config(per_second: u32, burst: u32, max_labels: usize, max_in_flight: usize) -> LabelStaging {
    LabelStaging::new(LabelStagingConfig {
        per_second,
        burst,
        max_labels,
        max_in_flight,
    })
}

fn labels(bytes: &[u8]) -> BTreeMap<[u8; 32], String> {
    bytes
        .iter()
        .map(|byte| ([*byte; 32], format!("label{byte}")))
        .collect()
}

fn admitted(decision: Decision) -> Admitted {
    match decision {
        Decision::Write(admitted) => admitted,
        Decision::Skip(outcome) => panic!("expected a write, got {outcome:?}"),
    }
}

fn skipped(decision: Decision) -> Outcome {
    match decision {
        Decision::Write(admitted) => panic!("expected a skip, wrote {:?}", admitted.labels),
        Decision::Skip(outcome) => outcome,
    }
}

#[test]
fn the_bucket_spends_its_burst_then_refills_at_its_rate_up_to_the_burst() {
    let start = Instant::now();
    let mut bucket = Bucket::full(start, 2);
    assert!(bucket.take(start, 10, 2));
    assert!(bucket.take(start, 10, 2));
    assert!(!bucket.take(start, 10, 2), "the burst is spent");
    assert!(
        !bucket.take(start + Duration::from_millis(50), 10, 2),
        "half a token"
    );
    assert!(
        bucket.take(start + Duration::from_millis(100), 10, 2),
        "one token refilled"
    );
    let later = start + Duration::from_secs(60);
    assert!(bucket.take(later, 10, 2));
    assert!(bucket.take(later, 10, 2));
    assert!(!bucket.take(later, 10, 2), "refill stops at the burst");
}

#[test]
fn a_bucket_read_with_an_earlier_instant_neither_refills_nor_rewinds() {
    let start = Instant::now();
    let mut bucket = Bucket::full(start + Duration::from_secs(1), 1);
    assert!(bucket.take(start, 10, 1));
    assert!(!bucket.take(start, 10, 1));
}

#[tokio::test]
async fn staging_configured_off_writes_nothing_and_says_so() {
    let staging = config(0, 0, 0, 0);
    assert_eq!(
        skipped(staging.decide(labels(&[1]), Instant::now())),
        Outcome::Disabled
    );
    staging.settled().await;
}

#[tokio::test]
async fn a_label_staged_once_is_not_written_again_by_the_process() {
    let staging = config(10, 20, 16, 2);
    let now = Instant::now();
    let first = admitted(staging.decide(labels(&[1, 2]), now));
    assert_eq!(first.labels.len(), 2);
    assert_eq!(
        skipped(staging.decide(labels(&[1, 2]), now)),
        Outcome::Duplicate,
        "a concurrent request for the same labels is a duplicate while the first is in flight"
    );
    drop(first);
    assert_eq!(
        skipped(staging.decide(labels(&[2]), now)),
        Outcome::Duplicate
    );
    let partly = admitted(staging.decide(labels(&[2, 3]), now));
    assert_eq!(partly.labels.keys().copied().collect::<Vec<_>>(), [[3; 32]]);
}

#[tokio::test]
async fn writes_beyond_the_budget_are_throttled_and_stay_unreserved() {
    let staging = config(1, 1, 16, 2);
    let now = Instant::now();
    drop(admitted(staging.decide(labels(&[1]), now)));
    assert_eq!(
        skipped(staging.decide(labels(&[2]), now)),
        Outcome::Throttled
    );
    // The throttled label was not reserved, so the refilled budget writes it.
    let refilled = admitted(staging.decide(labels(&[2]), now + Duration::from_secs(1)));
    assert_eq!(
        refilled.labels.keys().copied().collect::<Vec<_>>(),
        [[2; 32]]
    );
}

#[tokio::test]
async fn writes_beyond_the_in_flight_limit_are_throttled_and_spend_no_budget() {
    let staging = config(1, 2, 16, 1);
    let now = Instant::now();
    let held = admitted(staging.decide(labels(&[1]), now));
    assert_eq!(
        skipped(staging.decide(labels(&[2]), now)),
        Outcome::Throttled
    );
    drop(held);
    // One token of the burst of two is left: the refused write did not take it.
    drop(admitted(staging.decide(labels(&[2]), now)));
    assert_eq!(
        skipped(staging.decide(labels(&[3]), now)),
        Outcome::Throttled
    );
}

#[tokio::test]
async fn the_staged_set_starts_again_when_it_would_pass_its_bound() {
    let staging = config(100, 100, 3, 2);
    let now = Instant::now();
    drop(admitted(staging.decide(labels(&[1, 2]), now)));
    drop(admitted(staging.decide(labels(&[3]), now)));
    assert_eq!(
        skipped(staging.decide(labels(&[1]), now)),
        Outcome::Duplicate
    );
    // A fourth label passes the bound of three: the set is cleared, then holds only it.
    drop(admitted(staging.decide(labels(&[4]), now)));
    assert_eq!(staging.inner.lock_staged().len(), 1);
    drop(admitted(staging.decide(labels(&[1]), now)));
}

#[test]
fn a_write_is_classified_by_its_rows_and_its_error() {
    assert_eq!(Outcome::of(&Ok(2)), Outcome::Staged);
    assert_eq!(Outcome::of(&Ok(0)), Outcome::Duplicate);
    assert_eq!(
        Outcome::of(&Err(sqlx::Error::PoolTimedOut)),
        Outcome::Failed
    );
    assert_eq!(
        Outcome::ALL.map(Outcome::as_str),
        [
            "staged",
            "duplicate",
            "throttled",
            "denied",
            "read_only",
            "disabled",
            "failed"
        ]
    );
}

#[test]
fn an_enabled_configuration_needs_a_burst_a_set_and_a_write_slot() {
    LabelStagingConfig::default()
        .validate()
        .expect("the defaults are valid");
    let off = LabelStagingConfig {
        per_second: 0,
        burst: 0,
        max_labels: 0,
        max_in_flight: 0,
    };
    off.validate().expect("staging off needs no bounds");
    for (config, variable) in [
        (
            LabelStagingConfig {
                burst: 0,
                ..LabelStagingConfig::default()
            },
            "BIGNAME_API_LABEL_STAGING_BURST",
        ),
        (
            LabelStagingConfig {
                max_labels: 0,
                ..LabelStagingConfig::default()
            },
            "BIGNAME_API_LABEL_STAGING_MAX_LABELS",
        ),
        (
            LabelStagingConfig {
                max_in_flight: 0,
                ..LabelStagingConfig::default()
            },
            "BIGNAME_API_LABEL_STAGING_MAX_IN_FLIGHT",
        ),
    ] {
        let error = config
            .validate()
            .expect_err("an enabled zero bound is refused");
        assert!(error.to_string().contains(variable), "{error}");
    }
}
