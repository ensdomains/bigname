//! Owned key families (docs/projections.md, "Owned key families"): per-key current-state tables
//! that step 2 of TYR-36 fills block by block beside the served tables. Nothing reads them yet.
// The loop that uses the block input and the key derivation lands in the next commit.
#![allow(dead_code)]
mod input;
mod keys;
