mod decode;
mod reads;
mod types;

#[cfg(any(test, feature = "test-support"))]
mod fixture_writes;

#[cfg(any(test, feature = "test-support"))]
pub use fixture_writes::insert_normalized_event_fixtures;
pub use reads::{load_normalized_event_counts_by_kind, load_normalized_events_by_namespace};
pub use types::NormalizedEvent;
