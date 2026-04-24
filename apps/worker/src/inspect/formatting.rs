use bigname_storage::CanonicalityState;
use sqlx::types::time::{OffsetDateTime, UtcOffset};

pub(in crate::inspect) fn format_timestamp(value: OffsetDateTime) -> String {
    let value = value.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

pub(in crate::inspect) fn format_bytes_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(2 + bytes.len() * 2);
    encoded.push_str("0x");
    for byte in bytes {
        encoded.push(hex_digit(byte >> 4));
        encoded.push(hex_digit(byte & 0x0f));
    }
    encoded
}

const fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => '?',
    }
}

pub(in crate::inspect) const fn canonicality_state_label(state: CanonicalityState) -> &'static str {
    match state {
        CanonicalityState::Observed => "observed",
        CanonicalityState::Canonical => "canonical",
        CanonicalityState::Safe => "safe",
        CanonicalityState::Finalized => "finalized",
        CanonicalityState::Orphaned => "orphaned",
    }
}
