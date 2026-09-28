//! Readers over the owned key families (docs/projections.md, "Owned key families"). The harness
//! compares what they return with what today's readers serve; under the
//! [publication switch](crate::publication_source) the routes TYR-36 step 7b has moved serve
//! from them (the composed names of `name`, the permission pages of `control::permissions::page`
//! and the resolver overview and collections of `topology`).
pub mod control;
pub mod name;
pub mod records;
pub mod topology;

pub(crate) use name::read_snapshot;
