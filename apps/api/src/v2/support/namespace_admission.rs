//! Prepare manifest authority before holding a page transaction, then select on that transaction.
use super::*;

pub(crate) struct PreparedPublicNamespaces {
    scopes: Vec<(String, SnapshotSelectionScope)>,
    manifests: Vec<PublicNamespaceManifestToken>,
}

pub(crate) async fn prepare_public_namespace_admission(
    state: &AppState,
) -> ApiResult<PreparedPublicNamespaces> {
    let mut scopes = Vec::new();
    let manifests = if let Some(namespaces) = state.public_namespaces_override() {
        for namespace in namespaces.iter() {
            let chains = match namespace.as_str() {
                "ens" => BTreeSet::from(["ethereum-mainnet"]),
                BASENAMES_NAMESPACE => BTreeSet::from([BASENAMES_COMPAT_SOURCE_CHAIN_ID]),
                _ => BTreeSet::new(),
            };
            if let Some(scope) =
                public_namespace_snapshot_scope(&state.pool, namespace, &chains).await?
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
            if let Some(scope) =
                public_namespace_snapshot_scope(&state.pool, &manifest.namespace, &chains).await?
            {
                scopes.push((manifest.namespace.clone(), scope));
            }
        }
        manifests
    };
    Ok(PreparedPublicNamespaces { scopes, manifests })
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
