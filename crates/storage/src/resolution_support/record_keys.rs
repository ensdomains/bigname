use anyhow::{Result, bail};

use crate::name_current::NameCurrentRow;

use super::{
    boundaries::resolution_supports_avatar_readback, support_classes::VerifiedResolutionRecord,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SupportedVerifiedResolutionRecordKey {
    Addr { coin_type: String },
    Avatar,
    Contenthash,
    Text,
}

pub fn parse_supported_verified_resolution_record_key(
    record_key: &str,
) -> Result<SupportedVerifiedResolutionRecordKey> {
    if let Some(coin_type) = record_key.strip_prefix("addr:")
        && !coin_type.is_empty()
        && coin_type.as_bytes().iter().all(u8::is_ascii_digit)
    {
        return Ok(SupportedVerifiedResolutionRecordKey::Addr {
            coin_type: coin_type.to_owned(),
        });
    }

    if record_key == "contenthash" {
        return Ok(SupportedVerifiedResolutionRecordKey::Contenthash);
    }

    if record_key == "avatar" {
        return Ok(SupportedVerifiedResolutionRecordKey::Avatar);
    }

    if let Some(text_key) = record_key.strip_prefix("text:")
        && !text_key.is_empty()
    {
        return Ok(SupportedVerifiedResolutionRecordKey::Text);
    }

    bail!(
        "ENS direct-path verified resolution only supports addr:<coin_type>, avatar, contenthash, and text:<key> selectors, found {}",
        record_key
    );
}

pub fn supported_resolution_verified_lookup_records<R>(records: &[R]) -> Vec<R>
where
    R: VerifiedResolutionRecord + Clone,
{
    records
        .iter()
        .filter(|record| supports_resolution_verified_lookup_record(*record))
        .cloned()
        .collect()
}

pub fn supported_resolution_verified_readback_records<R>(
    row: &NameCurrentRow,
    records: &[R],
) -> Vec<R>
where
    R: VerifiedResolutionRecord + Clone,
{
    records
        .iter()
        .filter(|record| {
            supports_resolution_verified_lookup_record(*record)
                || (resolution_supports_avatar_readback(row, None)
                    && is_resolution_avatar_record(*record))
        })
        .cloned()
        .collect()
}

pub fn supports_resolution_verified_lookup_record(record: &impl VerifiedResolutionRecord) -> bool {
    match record.record_family() {
        "addr" => record
            .selector_key()
            .is_some_and(|selector| selector.as_bytes().iter().all(u8::is_ascii_digit)),
        "contenthash" => record.record_key() == "contenthash" && record.selector_key().is_none(),
        "text" => record.selector_key().is_some(),
        _ => false,
    }
}

pub fn is_resolution_avatar_record(record: &impl VerifiedResolutionRecord) -> bool {
    record.record_key() == "avatar"
        && record.record_family() == "avatar"
        && record.selector_key().is_none()
}

pub fn resolution_execution_cache_lookup_records<R>(row: &NameCurrentRow, records: &[R]) -> Vec<R>
where
    R: VerifiedResolutionRecord + Clone,
{
    if !resolution_supports_avatar_readback(row, None) {
        return records.to_vec();
    }

    let lookup_records = records
        .iter()
        .filter(|record| !is_resolution_avatar_record(*record))
        .cloned()
        .collect::<Vec<_>>();

    if lookup_records.is_empty() || lookup_records.len() == records.len() {
        records.to_vec()
    } else {
        lookup_records
    }
}
