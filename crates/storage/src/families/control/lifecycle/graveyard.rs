//! A name whose current ENSv1 registry record the admitted Graveyard holds: its ENSv1
//! registration has ended, whatever its lease, binding or schedule says.
//!
//! The Graveyard is the ENSv1 `.eth` controller the ENSv2 launch leaves in place. Anyone may call
//! its `clear`. For a `.eth` second-level name it registers the name to itself through the
//! BaseRegistrar, which only succeeds once the lease is past its expiry and grace, with a
//! far-future expiry that is no lease of anyone's. That takes the registry record and burns the
//! lapsed token. A subname of a name it holds is cleared by giving the Graveyard its record. No
//! ENSv1 controller can register the name again, and the Graveyard never transfers what it holds.
//! So the name is served `released` with no expiry (`expires_at_reason: released`), no owner,
//! manager or registrant, released at the block of the Graveyard's registry write, and with the
//! holder it had before the claim as `lapsed_registration.owner`
//! (docs/upstream.md).
//! (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L98-L102 @ ens_v2_mainnet_20261002@4f47cbcc)
//! (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_mainnet_20261002@4f47cbcc)
//! (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L186-L201 @ ens_v2_mainnet_20261002@4f47cbcc)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L100-L104 @ ens_v1@91c966f)
#[cfg(test)]
#[path = "graveyard_tests.rs"]
mod tests;
