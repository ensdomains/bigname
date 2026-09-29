//! Readers over the owned key families (docs/projections.md, "Owned key families"), first
//! group: who controls a name. They compute the registration and control blocks of the composed
//! name row (F2a with F1 facts), the NameWrapper masks (F2b), registry ownership and the registry
//! binding (F2c), and raw permissions with account approvals (F8, F9) from the per-key family rows
//! the Project phase writes for each block.
pub mod cutover;
pub mod lifecycle;
pub mod permissions;
pub mod position;
pub mod registry;
pub mod rows;
pub mod wrapper;
