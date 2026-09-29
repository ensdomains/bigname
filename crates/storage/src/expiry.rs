use crate::UnixSeconds;

/// Bigname's no-expiry presentation for a known contract and retained value. Other values,
/// including every generic user-registry uint64 word, are finite.
/// Root eth/reverse use MAX_EXPIRY in their deployment:
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/deploy/01_ETHRegistry.ts:L36 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/deploy/01_ReverseMirror.ts:L25 @ ens_v2_sepolia_20260916@366de741)
/// Wrapper root/eth max and parent-capped child expiry:
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L68 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L978 @ ens_v1@91c966f)
pub fn contract_expiry_reason(
    expiry: UnixSeconds,
    source_family: &str,
    namehash: Option<&str>,
) -> Option<&'static str> {
    let wrapper = source_family == "ens_v1_wrapper_l1";
    if wrapper && expiry.unix_timestamp() == 0 && expiry.nanosecond() == 0 {
        return Some("not_set");
    }
    if expiry.unix_timestamp() != i128::from(u64::MAX) || expiry.nanosecond() != 0 {
        return None;
    }
    let root = source_family == "ens_v2_root_l1"
        && namehash.is_some_and(|namehash| {
            ["eth", "reverse"].iter().any(|label| {
                let mut input = [0_u8; 64];
                input[32..]
                    .copy_from_slice(alloy_primitives::keccak256(label.as_bytes()).as_slice());
                format!("{:#x}", alloy_primitives::keccak256(input)) == namehash
            })
        });
    (wrapper || root).then_some("no_expiry")
}
