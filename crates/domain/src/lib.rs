//! Narrow shared domain primitives for the repo.

pub mod block_interval;
pub mod normalization;

/// The current repository phase.
pub const fn bootstrap_phase() -> &'static str {
    "phase-1-bootstrap"
}
