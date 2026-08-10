use std::collections::BTreeSet;

use bigname_storage::{SnapshotPositionRequirement, SnapshotSelectionScope};
use tracing::error;

use crate::{
    AppState,
    v2::support::{PublicNamespaceSet, derive_public_namespace_set},
};

use super::{super::chains::deployment_profile_for_slug, parse::ParsedNameLookup};
use crate::v2::{V2Error, V2Result, v2_exact_name_snapshot_scope};

pub(super) async fn lookup_snapshot_scope(
    state: &AppState,
    namespace: Option<&str>,
    name_inputs: &[ParsedNameLookup],
    has_address_inputs: bool,
    public_namespaces: Option<&PublicNamespaceSet>,
) -> V2Result<Option<SnapshotSelectionScope>> {
    let has_valid_name_inputs = name_inputs.iter().any(|input| input.lookup.is_some());
    if !has_address_inputs && !has_valid_name_inputs {
        return Ok(None);
    }

    if let Some(namespace) = namespace {
        return v2_exact_name_snapshot_scope(state, namespace, None)
            .await
            .map(Some);
    }

    let namespaces = name_inputs
        .iter()
        .filter_map(parsed_name_lookup_namespace)
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();

    if has_address_inputs {
        let public_namespaces = public_namespaces.ok_or_else(|| {
            V2Error::internal_error("public reverse lookup is missing its namespace set")
        })?;
        return lookup_public_union_snapshot_scope(state, public_namespaces, &namespaces)
            .await
            .map(Some);
    }

    if namespaces.len() == 1 {
        let namespace = namespaces
            .iter()
            .next()
            .expect("length check ensures one namespace");
        return v2_exact_name_snapshot_scope(state, namespace, None)
            .await
            .map(Some);
    }

    let namespaces = if namespaces.is_empty() {
        return lookup_public_snapshot_scope(state).await.map(Some);
    } else {
        namespaces.into_iter().collect()
    };
    lookup_union_snapshot_scope(state, namespaces)
        .await
        .map(Some)
}

async fn lookup_public_snapshot_scope(state: &AppState) -> V2Result<SnapshotSelectionScope> {
    let namespaces = lookup_public_namespaces(state).await?;
    lookup_public_union_snapshot_scope(state, &namespaces, &BTreeSet::new()).await
}

pub(super) async fn lookup_public_namespaces(state: &AppState) -> V2Result<PublicNamespaceSet> {
    let namespaces = derive_public_namespace_set(state)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                status = %load_error.status,
                code = load_error.code,
                message = %load_error.message,
                "failed to derive the public namespace set"
            );
            V2Error::internal_error("failed to select lookup namespaces")
        })?;
    Ok(namespaces)
}

async fn lookup_public_union_snapshot_scope(
    state: &AppState,
    public_namespaces: &PublicNamespaceSet,
    name_namespaces: &BTreeSet<String>,
) -> V2Result<SnapshotSelectionScope> {
    let mut requirements = public_namespaces
        .deployments()
        .iter()
        .flat_map(|deployment| deployment.scope().required_positions().iter().cloned())
        .collect::<Vec<_>>();

    for namespace in name_namespaces {
        if public_namespaces.contains(namespace) {
            continue;
        }
        let scope = v2_exact_name_snapshot_scope(state, namespace, None).await?;
        requirements.extend(scope.required_positions().iter().cloned());
    }

    build_lookup_union_snapshot_scope(requirements)
}

async fn lookup_union_snapshot_scope(
    state: &AppState,
    namespaces: Vec<String>,
) -> V2Result<SnapshotSelectionScope> {
    let mut requirements = Vec::new();
    for namespace in namespaces {
        let scope = v2_exact_name_snapshot_scope(state, &namespace, None).await?;
        requirements.extend(scope.required_positions().iter().cloned());
    }
    build_lookup_union_snapshot_scope(requirements)
}

fn build_lookup_union_snapshot_scope(
    requirements: Vec<SnapshotPositionRequirement>,
) -> V2Result<SnapshotSelectionScope> {
    validate_lookup_single_deployment_profile(&requirements)?;

    SnapshotSelectionScope::new(requirements, None).map_err(|error| {
        error!(
            service = "api",
            message = %error.message(),
            "failed to build v2 lookup snapshot scope"
        );
        V2Error::internal_error("failed to build lookup snapshot scope")
    })
}

fn parsed_name_lookup_namespace(input: &ParsedNameLookup) -> Option<&str> {
    input
        .lookup
        .as_ref()
        .and_then(|lookup| lookup.logical_name_id.split_once(':'))
        .map(|(namespace, _)| namespace)
}

fn validate_lookup_single_deployment_profile(
    requirements: &[SnapshotPositionRequirement],
) -> V2Result<()> {
    let mut profile = None;
    for requirement in requirements {
        let requirement_profile =
            deployment_profile_for_slug(&requirement.chain_id).ok_or_else(|| {
                V2Error::internal_error("snapshot scope contains an unregistered deployment chain")
            })?;
        if profile.is_some_and(|profile| profile != requirement_profile) {
            return Err(V2Error::conflict(
                "snapshot selector cannot form one canonical snapshot across deployment profiles",
            ));
        }
        profile = Some(requirement_profile);
    }

    Ok(())
}
