use super::*;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ExactNameSnapshotSelector<'a> {
    at: Option<&'a str>,
}

impl<'a> ExactNameSnapshotSelector<'a> {
    pub(crate) fn from_at(at: &'a str) -> Self {
        Self { at: Some(at) }
    }
}

pub(crate) async fn load_name_current_for_selected_snapshot(
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
        SnapshotProjectionRead::NotFound => Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("name {name} was not found in namespace {namespace}"),
        }),
    }
}

pub(crate) fn map_internal_api_error(error: ApiError, message: impl Into<String>) -> ApiError {
    if error.status == StatusCode::INTERNAL_SERVER_ERROR && error.code == "internal_error" {
        error!(
            service = "api",
            status = %error.status,
            code = %error.code,
            message = %error.message,
            "sanitized internal API error"
        );
        ApiError::internal_error(message)
    } else {
        error
    }
}

pub(crate) async fn exact_name_snapshot_scope(
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

async fn ens_snapshot_position_profile(
    pool: &PgPool,
    selector: ExactNameSnapshotSelector<'_>,
) -> ApiResult<(&'static str, &'static str)> {
    if selector
        .at
        .is_some_and(|value| value.contains("ethereum-sepolia"))
    {
        return Ok(("ethereum-sepolia", "ethereum-sepolia"));
    }
    if selector
        .at
        .is_some_and(|value| value.contains("ethereum-mainnet"))
    {
        return Ok(("ethereum", "ethereum-mainnet"));
    }

    let mainnet_has_head = snapshot_chain_has_head(pool, "ethereum-mainnet")
        .await
        .map_err(snapshot_selection_api_error)?;
    if mainnet_has_head {
        return Ok(("ethereum", "ethereum-mainnet"));
    }

    let sepolia_has_head = snapshot_chain_has_head(pool, "ethereum-sepolia")
        .await
        .map_err(snapshot_selection_api_error)?;
    if sepolia_has_head {
        return Ok(("ethereum-sepolia", "ethereum-sepolia"));
    }

    Ok(("ethereum", "ethereum-mainnet"))
}

pub(crate) fn snapshot_selection_api_error(error: SnapshotSelectionError) -> ApiError {
    let status = match error.kind() {
        SnapshotSelectionErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
        SnapshotSelectionErrorKind::Conflict | SnapshotSelectionErrorKind::Stale => {
            StatusCode::CONFLICT
        }
        SnapshotSelectionErrorKind::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
    };

    ApiError {
        status,
        code: error.api_error_code(),
        message: error.message().to_owned(),
    }
}
