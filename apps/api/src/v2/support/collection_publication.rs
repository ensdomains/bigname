//! API-owned publication identity and response revalidation; storage owns all underlying reads.
use super::*;

impl PublicNamespaceSet {
    pub(crate) fn for_namespace(self, namespace: Option<&str>) -> Self {
        let Some(namespace) = namespace else {
            return self;
        };
        let deployments = self
            .deployments
            .iter()
            .filter(|entry| entry.namespace == namespace)
            .cloned()
            .collect::<Vec<_>>();
        let request_scope = self
            .request_scope
            .iter()
            .filter(|scope| {
                deployments
                    .iter()
                    .any(|deployment| deployment.scope == scope.scope)
            })
            .cloned()
            .collect();
        let manifests = self
            .manifest_tokens
            .iter()
            .filter(|entry| entry.namespace == namespace)
            .cloned()
            .collect();
        Self::new(deployments, request_scope, manifests)
    }

    pub(crate) fn collection_fingerprint(&self) -> String {
        let deployments = self
            .deployments
            .iter()
            .map(|deployment| {
                let read = deployment.read_token.as_ref().map(|token| {
                    json!({
                        "positions": token.selected.chain_positions.to_value(),
                        "generations": token.project_generations,
                    })
                });
                json!({"namespace": deployment.namespace, "read": read})
            })
            .collect::<Vec<_>>();
        let manifests = self
            .manifest_tokens
            .iter()
            .map(|token| {
                json!({
                    "namespace": token.namespace,
                    "manifests": token.manifests,
                    "revisions": token.declaration_revisions,
                })
            })
            .collect::<Vec<_>>();
        let encoded = serde_json::to_vec(&json!({
            "version": 1, "deployments": deployments, "manifests": manifests,
        }))
        .expect("collection read identity must serialize");
        format!("publication-{}", alloy_primitives::keccak256(encoded))
    }
}

pub(crate) async fn revalidate_collection_namespace_set(
    state: &AppState,
    expected: &PublicNamespaceSet,
    namespace: Option<&str>,
) -> ApiResult<()> {
    let current = if state.public_namespaces_override().is_some() {
        derive_public_namespace_set(state)
            .await?
            .for_namespace(namespace)
    } else {
        let mut tokens = load_public_namespace_manifest_tokens(&state.pool).await?;
        tokens.retain(|token| namespace.is_none_or(|namespace| token.namespace == namespace));
        if expected.manifest_tokens.as_ref() != tokens.as_slice() {
            return Err(public_namespace_manifest_conflict());
        }
        let current = derive_public_namespace_set_from_manifests(state, tokens.clone()).await?;
        let mut reloaded = load_public_namespace_manifest_tokens(&state.pool).await?;
        reloaded.retain(|token| namespace.is_none_or(|namespace| token.namespace == namespace));
        if tokens != reloaded {
            return Err(public_namespace_manifest_conflict());
        }
        current
    };
    if expected.shares_read_view(&current) {
        return Ok(());
    }
    Err(public_namespace_manifest_conflict())
}
