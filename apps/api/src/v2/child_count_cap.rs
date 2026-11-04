//! The cap on a parent's child totals, shared by the subnames page and the registry overview.

#[cfg(test)]
tokio::task_local! {
    static CHILD_COUNT_CAP: u64;
}

/// Runs `future` with `cap` in place of the cap on child totals.
#[cfg(test)]
pub(crate) async fn with_child_count_cap<F: std::future::Future>(cap: u64, future: F) -> F::Output {
    CHILD_COUNT_CAP.scope(cap, future).await
}

/// The subnames `page.total_count` and the registry overview's `counts.labels` count at most
/// this many children (registrations, for the label count) and report `null` above it, the
/// route default count cap. The label count reads only that many, which bounds it for a registry
/// such as the `.eth` one; the subnames page still composes every child to order its rows.
pub(crate) fn child_count_cap() -> u64 {
    #[cfg(test)]
    if let Ok(cap) = CHILD_COUNT_CAP.try_with(|cap| *cap) {
        return cap;
    }
    super::HISTORY_TOTAL_COUNT_CAP
}
