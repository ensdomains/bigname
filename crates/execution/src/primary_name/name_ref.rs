use anyhow::{Result, bail};
use serde_json::Value;

use crate::json_helpers::{
    ensure_only_allowed_fields, optional_nonempty_string_field, required_object, required_string,
};

pub(crate) fn validate_verified_primary_name_ref(
    value: Option<&Value>,
    context: &str,
    expected_namespace: &str,
) -> Result<()> {
    let name = required_object(value, context)?;
    ensure_only_allowed_fields(
        name,
        &[
            "logical_name_id",
            "namespace",
            "normalized_name",
            "canonical_display_name",
            "namehash",
            "resource_id",
            "binding_kind",
        ],
        context,
    )?;

    let logical_name_id = required_string(name, "logical_name_id", context)?;
    let namespace = required_string(name, "namespace", context)?;
    let normalized_name = required_string(name, "normalized_name", context)?;
    required_string(name, "canonical_display_name", context)?;
    required_string(name, "namehash", context)?;
    optional_nonempty_string_field(name, "resource_id", context)?;
    optional_nonempty_string_field(name, "binding_kind", context)?;

    if namespace != expected_namespace {
        bail!("{context}.namespace must be {expected_namespace}");
    }
    if logical_name_id != format!("{expected_namespace}:{normalized_name}") {
        bail!(
            "{context}.logical_name_id {} does not match normalized_name {}",
            logical_name_id,
            normalized_name
        );
    }

    Ok(())
}
