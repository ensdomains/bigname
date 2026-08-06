//! Record-selector vocabulary.
//!
//! Split out of `types` because project hydration encodes calls from these selectors before rows
//! are persisted, which puts this module inside the interpreter content hash. The rest of `types`
//! is the request-scoped verified-lookup response shape and must stay outside it.

use crate::{LookupError, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordSelector {
    pub record_key: String,
    pub record_family: String,
    pub selector_key: Option<String>,
}

impl RecordSelector {
    pub fn parse(record_key: &str) -> Result<Self> {
        if let Some(coin_type) = record_key.strip_prefix("addr:") {
            let coin_type = coin_type.parse::<u64>().map_err(|_| {
                LookupError::unsupported(format!(
                    "unsupported address record selector {record_key}"
                ))
            })?;
            let coin_type = coin_type.to_string();
            return Ok(Self {
                record_key: format!("addr:{coin_type}"),
                record_family: "addr".to_owned(),
                selector_key: Some(coin_type),
            });
        }
        if let Some(key) = record_key
            .strip_prefix("text:")
            .filter(|key| !key.is_empty())
        {
            if key.contains('\0') {
                return Err(LookupError::unsupported(
                    "text record selector cannot contain NUL",
                ));
            }
            return Ok(Self {
                record_key: format!("text:{key}"),
                record_family: "text".to_owned(),
                selector_key: Some(key.to_owned()),
            });
        }
        match record_key {
            "avatar" => Ok(Self {
                record_key: record_key.to_owned(),
                record_family: "avatar".to_owned(),
                selector_key: None,
            }),
            "contenthash" => Ok(Self {
                record_key: record_key.to_owned(),
                record_family: "contenthash".to_owned(),
                selector_key: None,
            }),
            _ => Err(LookupError::unsupported(format!(
                "unsupported verified record selector {record_key}"
            ))),
        }
    }

    pub(crate) fn exact_text(text_key: &str) -> Result<Self> {
        if text_key.is_empty() {
            return Err(LookupError::unsupported(
                "text record selector requires a nonempty exact key",
            ));
        }
        Ok(Self {
            record_key: format!("text:{text_key}"),
            record_family: "text".to_owned(),
            selector_key: Some(text_key.to_owned()),
        })
    }
}
