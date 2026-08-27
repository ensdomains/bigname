use super::*;
use std::sync::Arc;

use bigname_manifests::{ActiveManifestVersion, load_namespace_manifest_snapshot};

#[derive(Clone, Debug, Eq, PartialEq)]
struct PublicNamespaceReadToken {
    selected: SelectedSnapshot,
    project_generations: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PublicNamespaceManifestToken {
    namespace: String,
    manifests: Vec<ActiveManifestVersion>,
    declaration_revisions: Vec<Option<String>>,
}

#[derive(Clone, Debug)]
pub(crate) struct PublicNamespaceDeployment {
    namespace: String,
    scope: SnapshotSelectionScope,
    read_token: Option<PublicNamespaceReadToken>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestScopeSnapshot {
    pub(super) scope: SnapshotSelectionScope,
    pub(super) selected: Option<SelectedSnapshot>,
}

impl RequestScopeSnapshot {
    pub(crate) fn scope(&self) -> &SnapshotSelectionScope {
        &self.scope
    }

    pub(crate) fn selected(&self) -> Option<&SelectedSnapshot> {
        self.selected.as_ref()
    }
}

impl PublicNamespaceDeployment {
    pub(crate) fn scope(&self) -> &SnapshotSelectionScope {
        &self.scope
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PublicNamespaceSet {
    deployments: Arc<[PublicNamespaceDeployment]>,
    request_scope: Arc<[RequestScopeSnapshot]>,
    manifest_tokens: Arc<[PublicNamespaceManifestToken]>,
    names: Arc<[String]>,
}

impl PublicNamespaceSet {
    fn new(
        deployments: Vec<PublicNamespaceDeployment>,
        request_scope: Vec<RequestScopeSnapshot>,
        manifest_tokens: Vec<PublicNamespaceManifestToken>,
    ) -> Self {
        let names = deployments
            .iter()
            .map(|deployment| deployment.namespace.clone())
            .collect::<Vec<_>>();
        Self {
            deployments: Arc::from(deployments),
            request_scope: Arc::from(request_scope),
            manifest_tokens: Arc::from(manifest_tokens),
            names: Arc::from(names),
        }
    }

    pub(crate) fn deployments(&self) -> &[PublicNamespaceDeployment] {
        &self.deployments
    }

    pub(crate) fn request_scope(&self) -> &[RequestScopeSnapshot] {
        &self.request_scope
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

    fn shares_read_view(&self, other: &Self) -> bool {
        self.manifest_tokens == other.manifest_tokens
            && self.deployments.len() == other.deployments.len()
            && self
                .deployments
                .iter()
                .zip(other.deployments.iter())
                .all(|(expected, current)| {
                    expected.namespace == current.namespace
                        && expected.read_token == current.read_token
                })
    }
}

pub(crate) async fn derive_public_namespace_set(state: &AppState) -> ApiResult<PublicNamespaceSet> {
    if let Some(namespaces) = state.public_namespaces_override() {
        let mut deployments = Vec::new();
        let mut request_scope = Vec::new();
        for namespace in namespaces.iter() {
            let active_chains = match namespace.as_str() {
                "ens" => BTreeSet::from(["ethereum-mainnet"]),
                BASENAMES_NAMESPACE => BTreeSet::from([BASENAMES_COMPAT_SOURCE_CHAIN_ID]),
                _ => BTreeSet::new(),
            };
            if let Some(scope) =
                public_namespace_snapshot_scope(&state.pool, namespace, &active_chains).await?
            {
                let read_token = load_request_scope_snapshot(&state.pool, &scope).await?;
                request_scope.push(RequestScopeSnapshot {
                    scope: scope.clone(),
                    selected: read_token.as_ref().map(|token| token.selected.clone()),
                });
                if let Some(read_token) = read_token {
                    deployments.push(PublicNamespaceDeployment {
                        namespace: namespace.clone(),
                        scope,
                        read_token: Some(read_token),
                    });
                }
            }
        }
        return Ok(PublicNamespaceSet::new(
            deployments,
            request_scope,
            Vec::new(),
        ));
    }

    let manifest_tokens = load_public_namespace_manifest_tokens(&state.pool).await?;
    derive_public_namespace_set_from_manifests(state, manifest_tokens).await
}

async fn derive_public_namespace_set_from_manifests(
    state: &AppState,
    manifest_tokens: Vec<PublicNamespaceManifestToken>,
) -> ApiResult<PublicNamespaceSet> {
    let mut deployments = Vec::new();
    let mut request_scope = Vec::new();
    for manifest_token in &manifest_tokens {
        let active_chains = manifest_token
            .manifests
            .iter()
            .map(|manifest| manifest.chain.as_str())
            .collect::<BTreeSet<_>>();
        let Some(scope) =
            public_namespace_snapshot_scope(&state.pool, &manifest_token.namespace, &active_chains)
                .await?
        else {
            continue;
        };
        let read_token = load_request_scope_snapshot(&state.pool, &scope).await?;
        request_scope.push(RequestScopeSnapshot {
            scope: scope.clone(),
            selected: read_token.as_ref().map(|token| token.selected.clone()),
        });
        if let Some(read_token) = read_token {
            deployments.push(PublicNamespaceDeployment {
                namespace: manifest_token.namespace.clone(),
                scope,
                read_token: Some(read_token),
            });
        }
    }

    Ok(PublicNamespaceSet::new(
        deployments,
        request_scope,
        manifest_tokens,
    ))
}

async fn load_request_scope_snapshot(
    pool: &PgPool,
    scope: &SnapshotSelectionScope,
) -> ApiResult<Option<PublicNamespaceReadToken>> {
    let input = SnapshotSelectorInput::new(None, None, SnapshotConsistency::Head)
        .map_err(snapshot_selection_api_error)?;
    let selected = match resolve_exact_name_snapshot_selection(pool, scope, &input).await {
        Ok(selected) => selected,
        Err(error)
            if matches!(
                error.kind(),
                SnapshotSelectionErrorKind::Conflict | SnapshotSelectionErrorKind::Stale
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(snapshot_selection_api_error(error)),
    };
    Ok(load_public_namespace_project_generations(pool, &selected)
        .await?
        .map(|project_generations| PublicNamespaceReadToken {
            selected,
            project_generations,
        }))
}

pub(crate) async fn revalidate_public_namespace_set(
    state: &AppState,
    expected: &PublicNamespaceSet,
) -> ApiResult<()> {
    let current = if state.public_namespaces_override().is_some() {
        derive_public_namespace_set(state).await?
    } else {
        let manifest_tokens = load_public_namespace_manifest_tokens(&state.pool).await?;
        if expected.manifest_tokens.as_ref() != manifest_tokens.as_slice() {
            return Err(public_namespace_manifest_conflict());
        }
        let current =
            derive_public_namespace_set_from_manifests(state, manifest_tokens.clone()).await?;
        let reloaded_manifest_tokens = load_public_namespace_manifest_tokens(&state.pool).await?;
        if manifest_tokens != reloaded_manifest_tokens {
            return Err(public_namespace_manifest_conflict());
        }
        current
    };
    if expected.shares_read_view(&current) {
        return Ok(());
    }

    Err(ApiError {
        status: StatusCode::CONFLICT,
        code: "conflict",
        message: "public namespace authority changed while the request was being read".to_owned(),
    })
}

pub(crate) async fn revalidate_lookup_public_namespace_set(
    state: &AppState,
    expected: &PublicNamespaceSet,
) -> ApiResult<()> {
    if state.public_namespaces_override().is_some() {
        let current = derive_public_namespace_set(state).await?;
        return require_same_public_namespace_names(expected, &current);
    }

    let manifest_tokens = load_public_namespace_manifest_tokens(&state.pool).await?;
    if expected.manifest_tokens.as_ref() != manifest_tokens.as_slice() {
        return Err(public_namespace_manifest_conflict());
    }
    let current =
        derive_public_namespace_set_from_manifests(state, manifest_tokens.clone()).await?;
    let reloaded_manifest_tokens = load_public_namespace_manifest_tokens(&state.pool).await?;
    if manifest_tokens != reloaded_manifest_tokens {
        return Err(public_namespace_manifest_conflict());
    }
    require_same_public_namespace_names(expected, &current)
}

fn require_same_public_namespace_names(
    expected: &PublicNamespaceSet,
    current: &PublicNamespaceSet,
) -> ApiResult<()> {
    if expected.names == current.names {
        return Ok(());
    }

    Err(ApiError {
        status: StatusCode::CONFLICT,
        code: "stale",
        message: "public namespace readiness changed while the request was being read".to_owned(),
    })
}

fn public_namespace_manifest_conflict() -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        code: "conflict",
        message: "active public namespace manifests changed while the request was being read"
            .to_owned(),
    }
}

async fn load_public_namespace_manifest_tokens(
    pool: &PgPool,
) -> ApiResult<Vec<PublicNamespaceManifestToken>> {
    let mut tokens = Vec::new();
    for namespace in [BASENAMES_NAMESPACE, "ens"] {
        let snapshot = load_namespace_manifest_snapshot(pool, namespace)
            .await
            .map_err(|_| ApiError::internal_error("failed to load public namespace manifests"))?;
        if !snapshot.manifests.is_empty() {
            let revision_rows = sqlx::query_as::<_, (String, String, String, String, String)>(
                r#"
                SELECT DISTINCT ON (
                    source_family,
                    chain_id,
                    manifest_version,
                    raw_fact_ref ->> 'deployment_epoch'
                )
                    source_family,
                    chain_id,
                    manifest_version::TEXT,
                    raw_fact_ref ->> 'deployment_epoch',
                    event_identity
                FROM bigname_phase.normalized_events
                WHERE namespace = $1
                  AND event_kind = 'SourceManifestUpdated'
                  AND derivation_kind = 'manifest_sync'
                  AND canonicality_state = 'finalized'
                ORDER BY
                    source_family,
                    chain_id,
                    manifest_version,
                    raw_fact_ref ->> 'deployment_epoch',
                    normalized_event_id DESC
                "#,
            )
            .bind(namespace)
            .fetch_all(pool)
            .await
            .map_err(|_| {
                ApiError::internal_error("failed to load public namespace declaration revisions")
            })?;
            let revisions = revision_rows
                .into_iter()
                .map(
                    |(source_family, chain, version, deployment_epoch, event_identity)| {
                        (
                            (source_family, chain, version, deployment_epoch),
                            event_identity,
                        )
                    },
                )
                .collect::<BTreeMap<_, _>>();
            let declaration_revisions = snapshot
                .manifests
                .iter()
                .map(|manifest| {
                    revisions
                        .get(&(
                            manifest.source_family.clone(),
                            manifest.chain.clone(),
                            manifest.manifest_version.to_string(),
                            manifest.deployment_epoch.clone(),
                        ))
                        .cloned()
                })
                .collect();
            tokens.push(PublicNamespaceManifestToken {
                namespace: namespace.to_owned(),
                manifests: snapshot.manifests,
                declaration_revisions,
            });
        }
    }
    Ok(tokens)
}

async fn load_public_namespace_project_generations(
    pool: &PgPool,
    selected: &SelectedSnapshot,
) -> ApiResult<Option<BTreeMap<String, String>>> {
    load_selected_project_generations_for_read(pool, selected, true)
        .await
        .map_err(|_| ApiError::internal_error("failed to validate public namespace data"))
}

pub(crate) async fn load_selected_project_generations_for_read(
    pool: &PgPool,
    selected: &SelectedSnapshot,
    require_interpret_not_redo: bool,
) -> std::result::Result<Option<BTreeMap<String, String>>, sqlx::Error> {
    let mut generations = BTreeMap::new();
    for position in selected.chain_positions.as_map().values() {
        // Do not compare interpret.xmin: normal forward batches update it. History-rewriting redos
        // hold this flag until Project is stamped; canonical-head orphaning stamps both phases.
        let generation = sqlx::query_scalar::<_, String>(
            r#"
            SELECT project.xmin::TEXT
            FROM chain_heads head
            JOIN chain_phase_state project
              ON project.chain_id = head.chain_id
             AND project.phase_name = 'project'
             AND project.phase_status = 'completed'
             AND project.current_block_number = head.latest_block_number
             AND project.current_block_hash = head.latest_block_hash
             AND project.input_content_hash = $4
            WHERE head.chain_id = $1
              AND head.latest_block_number = $2
              AND head.latest_block_hash = $3
              AND (
                  NOT $5
                  OR EXISTS (
                      SELECT 1
                      FROM chain_phase_state interpret
                      WHERE interpret.chain_id = head.chain_id
                        AND interpret.phase_name = 'interpret'
                        AND interpret.redo_in_progress = false
                  )
              )
            "#,
        )
        .bind(&position.chain_id)
        .bind(position.block_number)
        .bind(&position.block_hash)
        .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
        .bind(require_interpret_not_redo)
        .fetch_optional(pool)
        .await?;
        let Some(generation) = generation else {
            return Ok(None);
        };
        generations.insert(position.chain_id.clone(), generation);
    }
    Ok(Some(generations))
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
