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
