//! Which namespaces a request with no `namespace` covers. Prepare manifest authority before
//! holding a page transaction, then select on that transaction.
use super::*;

pub(crate) struct PreparedPublicNamespaces {
    scopes: Vec<(String, SnapshotSelectionScope)>,
    manifests: Vec<PublicNamespaceManifestToken>,
}

pub(crate) async fn prepare_public_namespace_admission(
    state: &AppState,
) -> ApiResult<PreparedPublicNamespaces> {
    let followed = load_followed_chains(&state.pool).await?;
    let mut scopes = Vec::new();
    let manifests = if let Some(namespaces) = state.public_namespaces_override() {
        for namespace in namespaces.iter() {
            let chains = test_override_chains(namespace);
            if let Some(scope) =
                public_namespace_snapshot_scope(&state.pool, namespace, &chains, &followed).await?
            {
                scopes.push((namespace.clone(), scope));
            }
        }
        Vec::new()
    } else {
        let manifests = load_public_namespace_manifest_tokens(&state.pool).await?;
        for manifest in &manifests {
            let chains = manifest
                .manifests
                .iter()
                .map(|entry| entry.chain.as_str())
                .collect();
            if let Some(scope) = public_namespace_snapshot_scope(
                &state.pool,
                &manifest.namespace,
                &chains,
                &followed,
            )
            .await?
            {
                scopes.push((manifest.namespace.clone(), scope));
            }
        }
        manifests
    };
    Ok(PreparedPublicNamespaces { scopes, manifests })
}

/// The chains this deployment follows: those with phase state or a stored head, the chain set
/// `/v1/status` reports. The runner writes phase state for each configured chain at start-up.
pub(super) async fn load_followed_chains(pool: &PgPool) -> ApiResult<BTreeSet<String>> {
    bigname_storage::load_phase_expected_status_chain_ids(pool)
        .await
        .map(BTreeSet::from_iter)
        .map_err(|_| ApiError::internal_error("failed to load the deployment's chains"))
}

/// The test override's stand-in for each namespace's active manifest chains.
pub(super) fn test_override_chains(namespace: &str) -> BTreeSet<&'static str> {
    match namespace {
        "ens" => BTreeSet::from(["ethereum-mainnet"]),
        BASENAMES_NAMESPACE => BTreeSet::from([BASENAMES_COMPAT_SOURCE_CHAIN_ID]),
        _ => BTreeSet::new(),
    }
}

/// A namespace is in scope only on the chains this deployment follows.
pub(super) async fn public_namespace_snapshot_scope(
    pool: &PgPool,
    namespace: &str,
    active_chains: &BTreeSet<&str>,
    followed_chains: &BTreeSet<String>,
) -> ApiResult<Option<SnapshotSelectionScope>> {
    let active_chains = active_chains
        .iter()
        .copied()
        .filter(|chain_id| followed_chains.contains(*chain_id))
        .collect::<BTreeSet<_>>();
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

impl PreparedPublicNamespaces {
    pub(crate) async fn select_on(
        self,
        connection: &mut sqlx::PgConnection,
    ) -> ApiResult<PublicNamespaceSet> {
        let mut deployments = Vec::new();
        let mut request_scope = Vec::new();
        for (namespace, scope) in self.scopes {
            let read_token = load_request_scope_snapshot_on(&mut *connection, &scope).await?;
            request_scope.push(RequestScopeSnapshot {
                scope: scope.clone(),
                selected: read_token.as_ref().map(|token| token.selected.clone()),
            });
            if let Some(read_token) = read_token {
                deployments.push(PublicNamespaceDeployment {
                    namespace,
                    scope,
                    read_token: Some(read_token),
                });
            }
        }
        Ok(PublicNamespaceSet::new(
            deployments,
            request_scope,
            self.manifests,
        ))
    }
}
