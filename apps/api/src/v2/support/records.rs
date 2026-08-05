use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedRouteNameInput {
    pub(crate) namespace: &'static str,
    pub(crate) normalized_name: String,
    pub(crate) corrected_input_normalization: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RouteNameNormalizationError {
    pub(crate) message: String,
}

fn infer_resolution_namespace(name: &str) -> &'static str {
    if name == "base.eth" {
        return bigname_storage::ENS_NAMESPACE;
    }

    if name
        .strip_suffix(".base.eth")
        .is_some_and(|prefix| !prefix.is_empty())
    {
        BASENAMES_NAMESPACE
    } else {
        bigname_storage::ENS_NAMESPACE
    }
}

pub(crate) fn normalize_inferred_route_name(
    name: &str,
) -> Result<NormalizedRouteNameInput, RouteNameNormalizationError> {
    if name.is_empty() {
        return Err(RouteNameNormalizationError {
            message: "name must not be empty".to_owned(),
        });
    }
    let normalized = bigname_domain::normalization::normalize_name(name).map_err(|error| {
        RouteNameNormalizationError {
            message: error.message().to_owned(),
        }
    })?;
    Ok(NormalizedRouteNameInput {
        namespace: infer_resolution_namespace(&normalized.normalized_name),
        corrected_input_normalization: name != normalized.normalized_name,
        normalized_name: normalized.normalized_name,
    })
}

pub(crate) const PROFILE_FALLBACK_RECORD_KEYS: &[&str] = &[
    "addr:60",
    "avatar",
    "contenthash",
    "text:description",
    "text:url",
    "text:email",
];
