use super::*;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ExactNameSnapshotSelector<'a> {
    at: Option<&'a str>,
    chain_positions: Option<&'a str>,
    consistency: Option<&'a str>,
}

impl<'a> From<&'a ExactNameSnapshotQuery> for ExactNameSnapshotSelector<'a> {
    fn from(query: &'a ExactNameSnapshotQuery) -> Self {
        Self {
            at: query.at.as_deref(),
            chain_positions: query.chain_positions.as_deref(),
            consistency: query.consistency.as_deref(),
        }
    }
}

impl<'a> From<&'a ResolutionQuery> for ExactNameSnapshotSelector<'a> {
    fn from(query: &'a ResolutionQuery) -> Self {
        Self {
            at: query.at.as_deref(),
            chain_positions: query.chain_positions.as_deref(),
            consistency: query.consistency.as_deref(),
        }
    }
}

pub(super) async fn resolve_exact_name_selected_snapshot(
    pool: &PgPool,
    namespace: &str,
    selector: ExactNameSnapshotSelector<'_>,
    include_resolution_auxiliary: bool,
) -> ApiResult<SelectedSnapshot> {
    let scope =
        exact_name_snapshot_scope(pool, namespace, selector, include_resolution_auxiliary).await?;
    let input = parse_exact_name_snapshot_selector(selector, &scope)?;
    resolve_exact_name_snapshot_selection(pool, &scope, &input)
        .await
        .map_err(snapshot_selection_api_error)
}

pub(super) async fn load_name_current_for_selected_snapshot(
    pool: &PgPool,
    namespace: &str,
    name: &str,
    selected_snapshot: &SelectedSnapshot,
) -> ApiResult<NameCurrentRow> {
    let logical_name_id = format!("{namespace}:{name}");
    match load_name_current_for_snapshot(pool, &logical_name_id, &selected_snapshot.chain_positions)
        .await
        .map_err(snapshot_selection_api_error)?
    {
        SnapshotProjectionRead::Found(row) => Ok(row),
        SnapshotProjectionRead::NotFound => Err(name_not_found_error(namespace, name)),
    }
}

pub(super) fn map_internal_api_error(error: ApiError, message: impl Into<String>) -> ApiError {
    if error.status == StatusCode::INTERNAL_SERVER_ERROR && error.code == "internal_error" {
        ApiError::internal_error(message)
    } else {
        error
    }
}

pub(super) fn name_not_found_error(namespace: &str, name: &str) -> ApiError {
    ApiError {
        status: StatusCode::NOT_FOUND,
        code: "not_found",
        message: format!("name {name} was not found in namespace {namespace}"),
    }
}

pub(super) async fn exact_name_snapshot_scope(
    pool: &PgPool,
    namespace: &str,
    selector: ExactNameSnapshotSelector<'_>,
    include_resolution_auxiliary: bool,
) -> ApiResult<SnapshotSelectionScope> {
    let (authoritative_slot, authoritative_chain_id) = match namespace {
        "ens" => ens_snapshot_position_profile(pool, selector).await?,
        BASENAMES_NAMESPACE => ("base", BASENAMES_COMPAT_SOURCE_CHAIN_ID),
        _ => {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: format!("namespace {namespace} is not supported"),
            });
        }
    };

    let mut requirements = vec![SnapshotPositionRequirement::new(
        authoritative_slot,
        authoritative_chain_id,
    )];
    if namespace == BASENAMES_NAMESPACE && include_resolution_auxiliary {
        requirements.push(SnapshotPositionRequirement::new(
            "ethereum",
            BASENAMES_COMPAT_TARGET_CHAIN_ID,
        ));
    }

    SnapshotSelectionScope::new(requirements, Some(authoritative_slot.to_owned()))
        .map_err(snapshot_selection_api_error)
}

pub(super) async fn ens_snapshot_position_profile(
    pool: &PgPool,
    selector: ExactNameSnapshotSelector<'_>,
) -> ApiResult<(&'static str, &'static str)> {
    if selector_mentions_ens_sepolia_profile(selector) {
        return Ok(("ethereum-sepolia", "ethereum-sepolia"));
    }
    if selector_mentions_ens_mainnet_profile(selector) {
        return Ok(("ethereum", "ethereum-mainnet"));
    }

    let mainnet_checkpoint = load_chain_checkpoint(pool, "ethereum-mainnet")
        .await
        .map_err(|error| {
            snapshot_selection_api_error(SnapshotSelectionError::internal(format!(
                "failed to load ethereum-mainnet checkpoint for ENS snapshot selection: {error}"
            )))
        })?;
    if mainnet_checkpoint.is_some() {
        return Ok(("ethereum", "ethereum-mainnet"));
    }

    let sepolia_checkpoint = load_chain_checkpoint(pool, "ethereum-sepolia")
        .await
        .map_err(|error| {
            snapshot_selection_api_error(SnapshotSelectionError::internal(format!(
                "failed to load ethereum-sepolia checkpoint for ENS snapshot selection: {error}"
            )))
        })?;
    if sepolia_checkpoint.is_some() {
        return Ok(("ethereum-sepolia", "ethereum-sepolia"));
    }

    Ok(("ethereum", "ethereum-mainnet"))
}

pub(super) fn selector_mentions_ens_sepolia_profile(
    selector: ExactNameSnapshotSelector<'_>,
) -> bool {
    selector
        .chain_positions
        .or_else(|| {
            selector
                .at
                .filter(|value| value.trim_start().starts_with('{'))
        })
        .is_some_and(|value| value.contains("ethereum-sepolia"))
}

pub(super) fn selector_mentions_ens_mainnet_profile(
    selector: ExactNameSnapshotSelector<'_>,
) -> bool {
    selector
        .chain_positions
        .or_else(|| {
            selector
                .at
                .filter(|value| value.trim_start().starts_with('{'))
        })
        .is_some_and(|value| value.contains("ethereum-mainnet"))
}

pub(super) fn parse_exact_name_snapshot_selector(
    selector: ExactNameSnapshotSelector<'_>,
    scope: &SnapshotSelectionScope,
) -> ApiResult<SnapshotSelectorInput> {
    let consistency = SnapshotConsistency::parse(selector.consistency.map(str::trim))
        .map_err(snapshot_selection_api_error)?;
    let at = selector
        .at
        .map(str::trim)
        .map(|value| parse_snapshot_at(value, scope))
        .transpose()?;
    let chain_positions = selector
        .chain_positions
        .map(str::trim)
        .map(|value| ChainPositions::parse_explicit_json(value, scope))
        .transpose()
        .map_err(snapshot_selection_api_error)?;

    SnapshotSelectorInput::new(at, chain_positions, consistency)
        .map_err(snapshot_selection_api_error)
}

pub(super) fn parse_snapshot_at(
    value: &str,
    scope: &SnapshotSelectionScope,
) -> ApiResult<SnapshotAt> {
    if value.starts_with('{') {
        return ChainPositions::parse_explicit_json(value, scope)
            .map(SnapshotAt::ResolvedPositions)
            .map_err(snapshot_selection_api_error);
    }

    parse_rfc3339_utc_timestamp(value)
        .map(SnapshotAt::Timestamp)
        .map_err(snapshot_selection_api_error)
}

pub(super) fn snapshot_selection_api_error(error: SnapshotSelectionError) -> ApiError {
    let status = match error.kind() {
        SnapshotSelectionErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
        SnapshotSelectionErrorKind::Conflict => StatusCode::CONFLICT,
        SnapshotSelectionErrorKind::Stale => StatusCode::CONFLICT,
        SnapshotSelectionErrorKind::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
    };

    ApiError {
        status,
        code: error.api_error_code(),
        message: error.message().to_owned(),
    }
}
