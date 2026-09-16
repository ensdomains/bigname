//! Ethereum header log-bloom membership.
//!
//! A block header commits to the addresses and topics of every log it contains through a
//! 2048-bit bloom filter. Ingest stores logs that a range query reported and a receipt
//! confirmed; testing each stored value against the header bloom pins those logs to the
//! header the window already resolved, without refetching the block body.

use alloy_primitives::keccak256;

/// Byte width of a header `logsBloom`.
pub const BLOOM_BYTES: usize = 256;

/// Returns whether `value` is admitted by a 2048-bit header bloom.
///
/// Ethereum sets three bits per accrued value: keccak256 the value, then read three
/// big-endian 16-bit words from the first six bytes of the digest and keep the low 11 bits
/// of each. Bit `n` lives in byte `255 - n / 8` counted from the front of the bloom, at
/// position `n % 8` within that byte.
///
/// A bloom of the wrong width admits nothing; callers treat that as a provider fault.
pub fn bloom_contains(bloom: &[u8], value: &[u8]) -> bool {
    if bloom.len() != BLOOM_BYTES {
        return false;
    }
    let digest = keccak256(value);
    (0..3).all(|word| {
        let bit = (u16::from_be_bytes([digest[word * 2], digest[word * 2 + 1]]) & 0x07ff) as usize;
        bloom[BLOOM_BYTES - 1 - bit / 8] & (1 << (bit % 8)) != 0
    })
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, Bloom, BloomInput};

    use super::*;

    fn accrued(values: &[&[u8]]) -> Bloom {
        let mut bloom = Bloom::ZERO;
        for value in values {
            bloom.accrue(BloomInput::Raw(value));
        }
        bloom
    }

    #[test]
    fn membership_matches_the_reference_bloom() {
        let address = Address::repeat_byte(0x11);
        let topic = B256::repeat_byte(0x22);
        let absent = B256::repeat_byte(0x33);
        let bloom = accrued(&[address.as_slice(), topic.as_slice()]);

        assert!(bloom_contains(bloom.as_slice(), address.as_slice()));
        assert!(bloom_contains(bloom.as_slice(), topic.as_slice()));
        assert_eq!(
            bloom_contains(bloom.as_slice(), absent.as_slice()),
            bloom.contains_input(BloomInput::Raw(absent.as_slice()))
        );
    }

    #[test]
    fn an_empty_bloom_admits_nothing_and_a_full_bloom_admits_everything() {
        let value = B256::repeat_byte(0x44);
        assert!(!bloom_contains(&[0u8; BLOOM_BYTES], value.as_slice()));
        assert!(bloom_contains(&[0xffu8; BLOOM_BYTES], value.as_slice()));
    }

    #[test]
    fn a_misshapen_bloom_admits_nothing() {
        let value = B256::repeat_byte(0x55);
        assert!(!bloom_contains(&[0xffu8; 128], value.as_slice()));
        assert!(!bloom_contains(&[], value.as_slice()));
    }
}
