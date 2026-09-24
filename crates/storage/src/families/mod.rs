//! Readers over the owned key families (docs/projections.md, "Owned key families"). They are
//! shadow reads: nothing that serves a response calls them yet, and the harness compares what
//! they return with what today's readers serve.
pub mod records;
