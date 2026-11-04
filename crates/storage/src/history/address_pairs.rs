//! One product row for a source-proven adjacent ETH-address event pair. Physical logs and
//! normalized rows stay unchanged; the AddressChanged row keeps its identity and position. The
//! rule and its proven resolver generations live in `count_contract.rs`
//! (`push_address_pair_filter`), because stored event counts apply it too.

#[cfg(test)]
#[path = "address_pairs_tests.rs"]
mod tests;
