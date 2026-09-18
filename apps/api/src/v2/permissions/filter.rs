//! Request-filter resolution for the permissions collection: which registration a request
//! selects, and whether that selection may be read as a current-name claim.

use std::collections::BTreeMap;

use bigname_storage::NameCurrentRow;
use sqlx::types::Uuid;

use crate::AppState;

use super::super::name_record::{name_registration_fields, registration_id, string_field};
use super::super::support::normalize_inferred_route_name;
use super::super::{
    QueryParams, V2Result,
    vocab::{AuthorityContext, RegistrationStatus},
};
use super::{
    ADDRESS_FILTER_KEY, INCLUDE_FILTER_KEY, NAME_FILTER_KEY, NAMESPACE_FILTER_KEY,
    REGISTRATION_ID_FILTER_KEY, V2Error, load_current_name_row,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EmptyPermissionsSelection {
    MissingOrUnsupportedNameAnchor,
    SupersededNameRegistrationPair,
    NamespaceRegistrationMismatch,
    /// The requested id is the NameWrapper resource of a wrapped `.eth` name, whose
    /// registration is its BaseRegistrar lease. History rejects the same value.
    ResourceIsNotARegistration,
}

#[derive(Debug)]
pub(super) struct ResolvedPermissionsFilter {
    pub(super) subject: Option<String>,
    pub(super) resource_id: Option<Uuid>,
    pub(super) empty_selection: Option<EmptyPermissionsSelection>,
    /// The resource whose support summary classifies a superseded name and registration pair:
    /// the requested registration resolved like its own standalone read, so a wrapped `.eth`
    /// lease reads the NameWrapper resource that controls it. An id that is not a registration
    /// keeps its raw resource classification.
    pub(super) pair_support_resource_id: Option<Uuid>,
    pub(super) authority_context: AuthorityContext,
    pub(super) cursor_filters: BTreeMap<String, String>,
}

#[derive(Debug)]
pub(super) struct NormalizedNameFilter {
    pub(super) namespace: String,
    pub(super) normalized_name: String,
}

#[derive(Debug)]
pub(super) struct PermissionsFilterInputs {
    pub(super) namespace: String,
    pub(super) name_filter: Option<NormalizedNameFilter>,
    pub(super) requested_resource_id: Option<Uuid>,
}

pub(super) fn permissions_filter_inputs(params: &QueryParams) -> V2Result<PermissionsFilterInputs> {
    let name_filter = normalized_name_filter(params)?;
    if name_filter.is_none() && params.registration_id.is_none() && params.address.is_none() {
        return Err(V2Error::invalid_input(
            "at least one of name, registration_id, or address is required",
        ));
    }

    let requested_resource_id = params
        .registration_id
        .as_deref()
        .map(|registration_id| {
            Uuid::parse_str(registration_id)
                .map_err(|_| V2Error::invalid_input("registration_id must be a UUID"))
        })
        .transpose()?;

    let namespace = name_filter
        .as_ref()
        .map(|name_filter| name_filter.namespace.clone())
        .or_else(|| params.namespace.clone())
        .unwrap_or_else(|| "ens".to_owned());

    Ok(PermissionsFilterInputs {
        namespace,
        name_filter,
        requested_resource_id,
    })
}

pub(super) async fn resolve_permissions_filter(
    state: &AppState,
    params: &QueryParams,
    include_lineage: bool,
    inputs: &PermissionsFilterInputs,
) -> V2Result<ResolvedPermissionsFilter> {
    let resolved_name_row = match inputs.name_filter.as_ref() {
        Some(name_filter) => Some(
            load_current_name_row(state, &name_filter.namespace, &name_filter.normalized_name)
                .await?,
        ),
        None => None,
    };
    // A name filter selects only the exact-name authority's current registration. An unsupported
    // name has no such registration, so it selects nothing rather than falling back to whatever
    // resource the projection still carries.
    let name_row = resolved_name_row
        .as_ref()
        .and_then(|row| row.as_ref())
        .filter(|row| current_registration_row(row));
    // Permissions live on the resource that currently holds control of the name. For a wrapped
    // `.eth` name that is its NameWrapper resource, while the registration the name serves is
    // its BaseRegistrar lease.
    let name_resource_id = name_row.and_then(|row| row.resource_id);
    let name_registration_id = name_row.and_then(registration_uuid);

    // A registration the name filter did not select is a superseded pair: queryable on its own as
    // an audit read, but empty when combined with the name it no longer holds.
    let superseded_pair = matches!(
        (inputs.requested_resource_id, name_registration_id),
        (Some(requested), Some(resolved)) if requested != resolved
    );

    let pair_support_resource_id = match inputs.requested_resource_id {
        Some(requested) if superseded_pair => Some(
            control_resource_for_registration(state, requested)
                .await?
                .unwrap_or(requested),
        ),
        _ => None,
    };

    let namespace = inputs.namespace.clone();
    let mut resource_is_not_a_registration = false;
    let resource_id = match (name_resource_id, inputs.requested_resource_id) {
        (Some(name_resource_id), _) => Some(name_resource_id),
        (None, Some(requested)) if inputs.name_filter.is_none() => {
            let control_resource = control_resource_for_registration(state, requested).await?;
            resource_is_not_a_registration = control_resource.is_none();
            Some(control_resource.unwrap_or(requested))
        }
        (None, requested) => requested,
    };
    let empty_selection = if superseded_pair {
        Some(EmptyPermissionsSelection::SupersededNameRegistrationPair)
    } else if resource_is_not_a_registration {
        Some(EmptyPermissionsSelection::ResourceIsNotARegistration)
    } else if inputs.name_filter.is_some() && name_resource_id.is_none() {
        Some(EmptyPermissionsSelection::MissingOrUnsupportedNameAnchor)
    } else {
        None
    };
    let empty_selection = if empty_selection.is_none()
        && inputs.name_filter.is_none()
        && let (Some(resource_id), Some(namespace)) = (resource_id, params.namespace.as_deref())
        && !bigname_storage::permission_resource_matches_namespace(
            &state.pool,
            resource_id,
            namespace,
        )
        .await
        .map_err(|_| V2Error::internal_error("failed to check permission namespace"))?
    {
        Some(EmptyPermissionsSelection::NamespaceRegistrationMismatch)
    } else {
        empty_selection
    };
    let authority_context = if inputs.name_filter.is_some() {
        AuthorityContext::CurrentForName
    } else {
        AuthorityContext::ResourceAudit
    };
    let namespace_filter =
        (params.namespace.is_some() || inputs.name_filter.is_some()).then_some(namespace.clone());
    let mut cursor_filters = BTreeMap::new();
    if let Some(namespace) = namespace_filter.as_ref() {
        cursor_filters.insert(NAMESPACE_FILTER_KEY.to_owned(), namespace.clone());
    }
    if let Some(name_filter) = inputs.name_filter.as_ref() {
        cursor_filters.insert(
            NAME_FILTER_KEY.to_owned(),
            name_filter.normalized_name.clone(),
        );
    }
    if let Some(address) = params.address.as_ref() {
        cursor_filters.insert(ADDRESS_FILTER_KEY.to_owned(), address.clone());
    }
    // The cursor is bound to the registration handle the request named; the resource that
    // handle reads is a storage detail, and a read of that resource is a different request.
    if let Some(registration_id) = inputs.requested_resource_id.or(resource_id) {
        cursor_filters.insert(
            REGISTRATION_ID_FILTER_KEY.to_owned(),
            registration_id.to_string(),
        );
    }
    if include_lineage {
        cursor_filters.insert(INCLUDE_FILTER_KEY.to_owned(), "lineage".to_owned());
    }

    Ok(ResolvedPermissionsFilter {
        subject: params.address.clone(),
        resource_id,
        empty_selection,
        pair_support_resource_id,
        authority_context,
        cursor_filters,
    })
}

/// Whether `row` currently serves a supported registration, the condition under which a
/// `registration_id` read resolves to the resource that controls the name.
pub(crate) fn current_registration_row(row: &NameCurrentRow) -> bool {
    string_field(row.coverage.get("status")).as_deref() != Some("unsupported")
        && matches!(
            name_registration_fields(Some(row), &row.namespace).registration_status,
            RegistrationStatus::Active
                | RegistrationStatus::Wrapped
                | RegistrationStatus::Registered
        )
}

fn registration_uuid(row: &NameCurrentRow) -> Option<Uuid> {
    registration_id(&row.declared_summary, row.resource_id)
        .and_then(|registration_id| Uuid::parse_str(&registration_id).ok())
}

/// The resource whose permission rows belong to `registration_id`. A BaseRegistrar lease that is
/// currently wrapped is controlled through its name's NameWrapper resource; every other
/// registration, and every audit read of a resource no current name serves, is its own resource.
///
/// The lease is matched to its name through Project's current registration identity: a
/// candidate name (one the resource is bound to, or an exact name of the node the resource's
/// own rows carry) belongs to the lease only when its current row names the lease as its
/// registration. A name registered through the NameWrapper, whose wrap recorded no lease and
/// whose lease has no binding of its own, is found this way: the NameWrapper registers the lease
/// to itself and wraps it in one call, the registrar mints and emits before the wrap, so the
/// controller event a manifest creates the registration from comes after `NameWrapped`, and the
/// registry write happens through the wrapper rather than through the lease's own binding.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L305 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L902 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f)
///
/// `None` means the id is not a registration: it is the NameWrapper resource of a current name
/// whose registration is a different resource, its BaseRegistrar lease. A wrapped subname has no
/// lease, so its NameWrapper resource is its registration and still resolves to itself.
async fn control_resource_for_registration(
    state: &AppState,
    registration_id: Uuid,
) -> V2Result<Option<Uuid>> {
    let failed = |error| {
        tracing::error!(
            error = ?error,
            %registration_id,
            "failed to resolve the resource that controls a registration"
        );
        V2Error::internal_error("failed to resolve registration resource")
    };
    let mut stands_in_for_a_lease = false;
    let logical_name_ids = bigname_storage::load_candidate_logical_name_ids_for_registration_id(
        &state.pool,
        registration_id,
    )
    .await
    .map_err(failed)?;
    for logical_name_id in logical_name_ids {
        let row = bigname_storage::load_name_current(&state.pool, &logical_name_id)
            .await
            .map_err(failed)?;
        let Some(row) = row.as_ref().filter(|row| current_registration_row(row)) else {
            continue;
        };
        if registration_uuid(row) == Some(registration_id) {
            if let Some(resource_id) = row.resource_id {
                return Ok(Some(resource_id));
            }
        } else if row.resource_id == Some(registration_id) {
            stands_in_for_a_lease = true;
        }
    }
    Ok((!stands_in_for_a_lease).then_some(registration_id))
}

fn normalized_name_filter(params: &QueryParams) -> V2Result<Option<NormalizedNameFilter>> {
    let Some(name) = params.name.as_deref() else {
        return Ok(None);
    };
    let normalized = normalize_inferred_route_name(name)
        .map_err(|error| V2Error::invalid_input(error.message))?;
    let namespace = params
        .namespace
        .clone()
        .unwrap_or_else(|| normalized.namespace.to_owned());

    Ok(Some(NormalizedNameFilter {
        namespace,
        normalized_name: normalized.normalized_name.to_owned(),
    }))
}
