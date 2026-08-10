use super::*;
use std::sync::Arc;

use bigname_manifests::load_namespace_manifest_snapshot;

#[derive(Clone, Debug)]
pub(crate) struct PublicNamespaceDeployment {
    namespace: String,
    scope: SnapshotSelectionScope,
}

impl PublicNamespaceDeployment {
    pub(crate) fn scope(&self) -> &SnapshotSelectionScope {
        &self.scope
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PublicNamespaceSet {
    deployments: Arc<[PublicNamespaceDeployment]>,
    names: Arc<[String]>,
}

impl PublicNamespaceSet {
    fn new(deployments: Vec<PublicNamespaceDeployment>) -> Self {
        let names = deployments
            .iter()
            .map(|deployment| deployment.namespace.clone())
            .collect::<Vec<_>>();
        Self {
            deployments: Arc::from(deployments),
            names: Arc::from(names),
        }
    }

    pub(crate) fn deployments(&self) -> &[PublicNamespaceDeployment] {
        &self.deployments
    }

    pub(crate) fn names(&self) -> &[String] {
        &self.names
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.deployments.is_empty()
    }

    pub(crate) fn contains(&self, namespace: &str) -> bool {
        self.deployments
            .iter()
            .any(|deployment| deployment.namespace == namespace)
    }
}

pub(crate) async fn derive_public_namespace_set(state: &AppState) -> ApiResult<PublicNamespaceSet> {
    if let Some(namespaces) = state.public_namespaces_override() {
        let mut deployments = Vec::new();
        for namespace in namespaces.iter() {
            let active_chains = match namespace.as_str() {
                "ens" => BTreeSet::from(["ethereum-mainnet"]),
                BASENAMES_NAMESPACE => BTreeSet::from([BASENAMES_COMPAT_SOURCE_CHAIN_ID]),
                _ => BTreeSet::new(),
            };
            if let Some(scope) =
                public_namespace_snapshot_scope(&state.pool, namespace, &active_chains).await?
            {
                deployments.push(PublicNamespaceDeployment {
                    namespace: namespace.clone(),
                    scope,
                });
            }
        }
        return Ok(PublicNamespaceSet::new(deployments));
    }

    let mut deployments = Vec::new();
    for namespace in [BASENAMES_NAMESPACE, "ens"] {
        let snapshot = load_namespace_manifest_snapshot(&state.pool, namespace)
            .await
            .map_err(|_| ApiError::internal_error("failed to load public namespace manifests"))?;
        let active_chains = snapshot
            .manifests
            .iter()
            .map(|manifest| manifest.chain.as_str())
            .collect::<BTreeSet<_>>();
        let Some(scope) =
            public_namespace_snapshot_scope(&state.pool, namespace, &active_chains).await?
        else {
            continue;
        };
        let input = SnapshotSelectorInput::new(None, None, SnapshotConsistency::Head)
            .map_err(snapshot_selection_api_error)?;
        match resolve_exact_name_snapshot_selection(&state.pool, &scope, &input).await {
            Ok(_) => deployments.push(PublicNamespaceDeployment {
                namespace: namespace.to_owned(),
                scope,
            }),
            Err(error)
                if matches!(
                    error.kind(),
                    SnapshotSelectionErrorKind::Conflict | SnapshotSelectionErrorKind::Stale
                ) => {}
            Err(error) => return Err(snapshot_selection_api_error(error)),
        }
    }

    Ok(PublicNamespaceSet::new(deployments))
}

async fn public_namespace_snapshot_scope(
    pool: &PgPool,
    namespace: &str,
    active_chains: &BTreeSet<&str>,
) -> ApiResult<Option<SnapshotSelectionScope>> {
    let selector = match namespace {
        "ens" => {
            let chains = active_chains
                .iter()
                .copied()
                .filter(|chain_id| matches!(*chain_id, "ethereum-mainnet" | "ethereum-sepolia"))
                .collect::<Vec<_>>();
            match chains.as_slice() {
                [] => return Ok(None),
                [chain_id] => ExactNameSnapshotSelector::from_at(chain_id),
                _ => {
                    return Err(ApiError::internal_error(
                        "ENS manifests span multiple deployment profiles",
                    ));
                }
            }
        }
        BASENAMES_NAMESPACE if active_chains.contains(BASENAMES_COMPAT_SOURCE_CHAIN_ID) => {
            ExactNameSnapshotSelector::default()
        }
        BASENAMES_NAMESPACE => return Ok(None),
        _ => return Ok(None),
    };

    exact_name_snapshot_scope(pool, namespace, selector, false)
        .await
        .map(Some)
}

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
    let logical_name_id = bigname_storage::logical_name_id_for_name(namespace, name);
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
