use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};

use super::types::{
    PrimaryNameClaimStatus, PrimaryNameCurrentRow, VERIFIED_PRIMARY_NAME_INVALIDATION_KEY,
    VERIFIED_PRIMARY_NAME_LOOKUP_KEY, VerifiedPrimaryNameClaimHooks,
    VerifiedPrimaryNameInvalidationHook, VerifiedPrimaryNameLookupHook, normalize_address,
};

/// Decode the persisted claim-side hooks later execution readers need for verified
/// primary-name lookup and request-matching invalidation.
pub fn verified_primary_name_claim_hooks(
    row: &PrimaryNameCurrentRow,
) -> Result<VerifiedPrimaryNameClaimHooks> {
    let claim_provenance = row
        .claim_provenance
        .as_object()
        .context("primary_names_current claim_provenance must be a JSON object")?;

    let lookup = match claim_provenance.get(VERIFIED_PRIMARY_NAME_LOOKUP_KEY) {
        Some(value) => decode_verified_primary_name_lookup_hook(value, row)?,
        None => VerifiedPrimaryNameLookupHook {
            address: normalize_address(&row.address),
            namespace: row.namespace.clone(),
            coin_type: row.coin_type.clone(),
        },
    };
    let invalidation = match claim_provenance.get(VERIFIED_PRIMARY_NAME_INVALIDATION_KEY) {
        Some(value) => {
            decode_verified_primary_name_invalidation_hook(value, row, claim_provenance)?
        }
        None => VerifiedPrimaryNameInvalidationHook {
            claim_status: row.claim_status,
            reverse_claim_provenance: strip_verified_primary_name_hook_fields(claim_provenance),
            primary_claim_source: None,
        },
    };

    Ok(VerifiedPrimaryNameClaimHooks {
        lookup,
        invalidation,
    })
}

fn decode_verified_primary_name_lookup_hook(
    value: &Value,
    row: &PrimaryNameCurrentRow,
) -> Result<VerifiedPrimaryNameLookupHook> {
    let object = value
        .as_object()
        .context("verified_primary_name_lookup must be a JSON object")?;
    let address = required_string_field(
        object,
        "address",
        VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
        Some(&row.address),
    )?;
    let namespace = required_string_field(
        object,
        "namespace",
        VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
        Some(&row.address),
    )?;
    let coin_type = required_string_field(
        object,
        "coin_type",
        VERIFIED_PRIMARY_NAME_LOOKUP_KEY,
        Some(&row.address),
    )?;

    let lookup = VerifiedPrimaryNameLookupHook {
        address: normalize_address(address),
        namespace: namespace.to_owned(),
        coin_type: coin_type.to_owned(),
    };
    if lookup.address != normalize_address(&row.address)
        || lookup.namespace != row.namespace
        || lookup.coin_type != row.coin_type
    {
        bail!(
            "verified_primary_name_lookup must match primary_names_current tuple for address {} namespace {} coin_type {}",
            row.address,
            row.namespace,
            row.coin_type
        );
    }

    Ok(lookup)
}

fn decode_verified_primary_name_invalidation_hook(
    value: &Value,
    row: &PrimaryNameCurrentRow,
    claim_provenance: &Map<String, Value>,
) -> Result<VerifiedPrimaryNameInvalidationHook> {
    let object = value
        .as_object()
        .context("verified_primary_name_invalidation must be a JSON object")?;
    let claim_status = PrimaryNameClaimStatus::parse(required_string_field(
        object,
        "claim_status",
        VERIFIED_PRIMARY_NAME_INVALIDATION_KEY,
        Some(&row.address),
    )?)?;
    if claim_status != row.claim_status {
        bail!(
            "verified_primary_name_invalidation claim_status must match primary_names_current claim_status for address {} namespace {} coin_type {}",
            row.address,
            row.namespace,
            row.coin_type
        );
    }

    let primary_claim_source = match object.get("primary_claim_source") {
        Some(primary_claim_source) => {
            let source = primary_claim_source.as_object().with_context(|| {
                format!(
                    "verified_primary_name_invalidation primary_claim_source for address {} namespace {} coin_type {} must be a JSON object",
                    row.address, row.namespace, row.coin_type
                )
            })?;
            let source_address = required_string_field(
                source,
                "address",
                "verified_primary_name_invalidation.primary_claim_source",
                Some(&row.address),
            )?;
            let source_namespace = required_string_field(
                source,
                "namespace",
                "verified_primary_name_invalidation.primary_claim_source",
                Some(&row.address),
            )?;
            let source_coin_type = required_string_field(
                source,
                "coin_type",
                "verified_primary_name_invalidation.primary_claim_source",
                Some(&row.address),
            )?;
            if normalize_address(source_address) != normalize_address(&row.address)
                || source_namespace != row.namespace
                || source_coin_type != row.coin_type
            {
                bail!(
                    "verified_primary_name_invalidation primary_claim_source must match primary_names_current tuple for address {} namespace {} coin_type {}",
                    row.address,
                    row.namespace,
                    row.coin_type
                );
            }

            Some(primary_claim_source.clone())
        }
        None => None,
    };

    Ok(VerifiedPrimaryNameInvalidationHook {
        claim_status,
        reverse_claim_provenance: strip_verified_primary_name_hook_fields(claim_provenance),
        primary_claim_source,
    })
}

fn strip_verified_primary_name_hook_fields(claim_provenance: &Map<String, Value>) -> Value {
    let mut stripped = claim_provenance.clone();
    stripped.remove(VERIFIED_PRIMARY_NAME_LOOKUP_KEY);
    stripped.remove(VERIFIED_PRIMARY_NAME_INVALIDATION_KEY);
    Value::Object(stripped)
}

fn required_string_field<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    context: &str,
    address: Option<&str>,
) -> Result<&'a str> {
    let value = object.get(field).with_context(|| match address {
        Some(address) => format!("{context} for address {address} must include {field}"),
        None => format!("{context} must include {field}"),
    })?;
    let string = value.as_str().with_context(|| match address {
        Some(address) => format!("{context} for address {address} {field} must be a string"),
        None => format!("{context} {field} must be a string"),
    })?;
    if string.trim().is_empty() {
        match address {
            Some(address) => bail!("{context} for address {address} {field} must not be blank"),
            None => bail!("{context} {field} must not be blank"),
        }
    }

    Ok(string)
}
