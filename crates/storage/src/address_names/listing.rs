//! `GET /v1/addresses/{address}/names` over the stored address listing (`project_address_listing`).
mod anchors;

#[cfg(any(test, feature = "test-support"))]
pub use anchors::load_composed_address_anchors;
pub use anchors::load_stored_address_anchors;
