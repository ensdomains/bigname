//! Shadow readers over the Project owned key families (docs/projections.md, "Owned key
//! families"). No served response reads them yet: the phase-runner harness runs them beside the
//! served readers at one publication and asserts they agree, until the per-block publication
//! switches the served reads over.
pub mod topology;
