use alloy_primitives::Address;

use super::*;

pub(crate) fn parse_evm_address(address: &str, field: &'static str) -> ApiResult<String> {
    if let Some(normalized) = normalize_standard_evm_address(address.trim()) {
        Ok(normalized)
    } else {
        Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: format!("{field} must be a 0x-prefixed 20-byte hex string"),
        })
    }
}

pub(crate) fn parse_primary_name_namespace(namespace: Option<&str>) -> ApiResult<String> {
    let Some(namespace) = namespace.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "namespace is required".to_owned(),
        });
    };

    ensure_public_namespace(namespace)?;
    Ok(namespace.to_owned())
}

pub(crate) fn parse_primary_name_coin_type(coin_type: Option<&str>) -> ApiResult<String> {
    let Some(coin_type) = coin_type.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "coin_type is required".to_owned(),
        });
    };

    if !coin_type.as_bytes().iter().all(u8::is_ascii_digit) {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "coin_type must contain only decimal digits".to_owned(),
        });
    }

    bigname_storage::canonical_addr_coin_type(coin_type).ok_or_else(|| ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "coin_type must fit in an unsigned 64-bit integer".to_owned(),
    })
}

pub(crate) fn parse_resolution_record_key(record_key: &str) -> Option<ResolutionRecordKey> {
    if record_key.is_empty()
        || record_key
            .chars()
            .any(|character| character.is_ascii_whitespace() || character == ',')
    {
        return None;
    }

    let is_valid_family = |family: &str| {
        !family.is_empty()
            && family.chars().all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
            })
    };

    match record_key.split_once(':') {
        None if is_valid_family(record_key) => Some(ResolutionRecordKey {
            record_key: record_key.to_owned(),
            record_family: record_key.to_owned(),
            selector_key: None,
        }),
        Some(("addr", selector)) if !selector.is_empty() => {
            let selector_key = bigname_storage::canonical_addr_coin_type(selector)?;
            Some(ResolutionRecordKey {
                record_key: format!("addr:{selector_key}"),
                record_family: "addr".to_owned(),
                selector_key: Some(selector_key),
            })
        }
        Some((family, selector)) if is_valid_family(family) && !selector.is_empty() => {
            Some(ResolutionRecordKey {
                record_key: record_key.to_owned(),
                record_family: family.to_owned(),
                selector_key: Some(selector.to_owned()),
            })
        }
        _ => None,
    }
}

fn normalize_standard_evm_address(value: &str) -> Option<String> {
    if value.len() != 42 || (!value.starts_with("0x") && !value.starts_with("0X")) {
        return None;
    }

    let address = format!("0x{}", &value[2..]).parse::<Address>().ok()?;
    Some(format!("0x{}", hex::encode(address.as_slice())))
}

pub(crate) fn ensure_public_namespace(namespace: &str) -> ApiResult<()> {
    if crate::state::is_recognized_public_namespace(namespace) {
        Ok(())
    } else {
        Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("namespace {namespace} is not supported"),
        })
    }
}

#[cfg(test)]
#[path = "query_parsing/tests.rs"]
mod tests;
