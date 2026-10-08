//! Independent expectation for the test oracle; never reads prepared selector flags.
use anyhow::Result;
use bigname_storage::UnixSeconds;
use serde_json::Value;

pub fn expected_registration_expiry(
    coverage: Option<&Value>,
    summary: Option<&Value>,
) -> Result<Option<UnixSeconds>> {
    let (Some(coverage), Some(summary)) = (coverage, summary) else {
        return Ok(None);
    };
    let expiry = match &summary["registration"]["expiry"] {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
    .filter(|text| {
        let digits = text.strip_prefix('-').unwrap_or(text);
        let (whole, fraction) = digits.split_once('.').unwrap_or((digits, "0"));
        [whole, fraction]
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
    });
    let Some(expiry) = expiry else {
        return Ok(None);
    };

    // Accepted contract: current-control coverage and allocation discovery are separate.
    // Only this precise unsupported reason plus positive allocation proof admits discovery.
    let admissible = match (
        coverage["status"].as_str(),
        coverage["unsupported_reason"].as_str(),
        summary["registration"]["canonical_allocation"].as_bool(),
    ) {
        (Some("unsupported"), Some("current_authority_not_projected"), Some(true)) => true,
        (Some("unsupported"), _, _) => false,
        _ => true,
    };
    if !admissible {
        return Ok(None);
    }
    expiry
        .parse()
        .map(Some)
        .map_err(|_| anyhow::anyhow!("expiry {expiry} is not exact seconds"))
}
