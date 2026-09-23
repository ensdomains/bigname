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

    /// `0x`-prefixed keccak256 of every manifest this read is served under: per namespace, each
    /// active manifest version paired with the revision of its latest finalized declaration.
    /// Namespaces and manifests are sorted and object keys are written in order, so the digest
    /// depends only on the manifest set.
    pub(crate) fn manifest_digest(&self) -> String {
        let mut namespaces = self
            .manifest_tokens
            .iter()
            .map(|token| {
                let mut manifests = token
                    .manifests
                    .iter()
                    .zip(token.declaration_revisions.iter())
                    .collect::<Vec<_>>();
                manifests.sort_by(|(left, _), (right, _)| {
                    (
                        &left.source_family,
                        &left.chain,
                        left.manifest_version,
                        &left.deployment_epoch,
                    )
                        .cmp(&(
                            &right.source_family,
                            &right.chain,
                            right.manifest_version,
                            &right.deployment_epoch,
                        ))
                });
                let manifests = manifests
                    .into_iter()
                    .map(|(manifest, revision)| json!({"manifest": manifest, "revision": revision}))
                    .collect::<Vec<_>>();
                (token.namespace.clone(), manifests)
            })
            .collect::<Vec<_>>();
        namespaces.sort_by(|(left, _), (right, _)| left.cmp(right));
        let namespaces = namespaces
            .into_iter()
            .map(|(namespace, manifests)| json!({"namespace": namespace, "manifests": manifests}))
            .collect::<Vec<_>>();
        let mut input = b"bigname-collection-manifests-v1\0".to_vec();
        input.extend(
            serde_json::to_vec(&sorted_keys(json!(namespaces)))
                .expect("collection manifest set must serialize"),
        );
        alloy_primitives::keccak256(input).to_string()
    }
}

/// The same JSON value with every object's keys in ascending order, whatever map order
/// `serde_json` was built with.
fn sorted_keys(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let sorted = object
                .into_iter()
                .map(|(key, value)| (key, sorted_keys(value)))
                .collect::<BTreeMap<_, _>>();
            serde_json::Value::Object(sorted.into_iter().collect())
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(sorted_keys).collect())
        }
        value => value,
    }
}

pub(crate) async fn revalidate_collection_namespace_set(
    state: &AppState,
    expected: &PublicNamespaceSet,
    namespace: Option<&str>,
) -> ApiResult<()> {
    let current = reload_collection_namespace_set(state, expected, namespace).await?;
    if expected.shares_read_view(&current) {
        return Ok(());
    }
    Err(public_namespace_manifest_conflict())
}

/// The request scope as it stands after a read, with the manifests checked against `expected`
/// before and after it is derived. The Project row version is not compared: a history read
/// compares the block it is bound to instead (`collection_binding`).
pub(crate) async fn reload_collection_namespace_set(
    state: &AppState,
    expected: &PublicNamespaceSet,
    namespace: Option<&str>,
) -> ApiResult<PublicNamespaceSet> {
    if state.public_namespaces_override().is_some() {
        Ok(derive_public_namespace_set(state)
            .await?
            .for_namespace(namespace))
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
        Ok(current)
    }
}
