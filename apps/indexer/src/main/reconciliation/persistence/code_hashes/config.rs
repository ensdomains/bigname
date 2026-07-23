use std::sync::OnceLock;

/// Per-chain watched-address cap for one baseline poll tick. The sweep walks
/// the sorted watch surface behind a process-lifetime cursor, so a
/// multi-million-address surface is baselined across ticks.
pub(super) const DEFAULT_RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK: usize = 2_048;
const RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK_ENV: &str =
    "BIGNAME_INDEXER_RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK";
static RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK: OnceLock<usize> = OnceLock::new();

pub(super) fn raw_code_baseline_max_addresses_per_tick() -> usize {
    *RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK.get_or_init(|| {
        parse_raw_code_baseline_max_addresses_per_tick(
            std::env::var(RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK_ENV)
                .ok()
                .as_deref(),
        )
    })
}

pub(super) fn parse_raw_code_baseline_max_addresses_per_tick(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_RAW_CODE_BASELINE_MAX_ADDRESSES_PER_TICK)
}
