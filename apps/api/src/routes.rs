#[derive(Clone, Copy)]
pub(crate) struct ApiRouteDefinition {
    pub(crate) id: ApiRouteId,
    pub(crate) method: ApiRouteMethod,
    pub(crate) path: &'static str,
    pub(crate) contract: Option<ApiRouteContract>,
}

#[derive(Clone, Copy)]
pub(crate) enum ApiRouteMethod {
    Get,
}

#[derive(Clone, Copy)]
pub(crate) struct ApiRouteContract {
    pub(crate) operation_id: &'static str,
    pub(crate) summary: &'static str,
    pub(crate) tag: &'static str,
    pub(crate) parameters: ApiRouteParameters,
    pub(crate) success_schema: &'static str,
    pub(crate) errors: ApiRouteErrorResponses,
}

#[derive(Clone, Copy)]
pub(crate) enum ApiRouteParameters {
    Names,
    AddressNames,
    AddressNamesCount,
    AddressHistory,
    PrimaryNames,
    ExactNameSnapshot(&'static str),
    ExplainResolutionExecution,
    NamespacePath,
    NameChildren,
    NameRecords,
    NameRoles,
    Events,
    Roles,
    ResourceLookup,
    ResolveCurrent,
    ResolveRecords,
    ResolutionCurrent,
    ResolverPath,
    ResolverOverview,
    NameHistory,
    ResourceHistory,
    ResourcePermissions,
}

#[derive(Clone, Copy)]
pub(crate) struct ApiRouteErrorResponses {
    pub(crate) include_bad_request: bool,
    pub(crate) include_not_found: bool,
    pub(crate) bad_request_description: Option<&'static str>,
    pub(crate) include_conflict: bool,
}

impl ApiRouteDefinition {
    const fn private_get(id: ApiRouteId, path: &'static str) -> Self {
        Self {
            id,
            method: ApiRouteMethod::Get,
            path,
            contract: None,
        }
    }

    const fn public_get(id: ApiRouteId, path: &'static str, contract: ApiRouteContract) -> Self {
        Self {
            id,
            method: ApiRouteMethod::Get,
            path,
            contract: Some(contract),
        }
    }
}

impl ApiRouteContract {
    const fn new(
        operation_id: &'static str,
        summary: &'static str,
        tag: &'static str,
        parameters: ApiRouteParameters,
        success_schema: &'static str,
        errors: ApiRouteErrorResponses,
    ) -> Self {
        Self {
            operation_id,
            summary,
            tag,
            parameters,
            success_schema,
            errors,
        }
    }
}

impl ApiRouteErrorResponses {
    const fn new(include_bad_request: bool, include_not_found: bool) -> Self {
        Self {
            include_bad_request,
            include_not_found,
            bad_request_description: None,
            include_conflict: false,
        }
    }

    const fn snapshot() -> Self {
        Self {
            include_bad_request: true,
            include_not_found: true,
            bad_request_description: Some("Invalid snapshot selector"),
            include_conflict: true,
        }
    }

    const fn conflict(include_not_found: bool) -> Self {
        Self {
            include_bad_request: true,
            include_not_found,
            bad_request_description: None,
            include_conflict: true,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ApiRouteId {
    Health,
    Names,
    AddressNames,
    AddressNamesCount,
    AddressHistory,
    PrimaryNames,
    Coverage,
    ExplainSurfaceBinding,
    ExplainAuthorityControl,
    ExplainResolutionExecution,
    NamespaceMetadata,
    NameChildren,
    NameCurrent,
    NameRecords,
    NameRoles,
    Events,
    Roles,
    ResourceLookup,
    ResolveCurrent,
    ResolveRecords,
    ResolutionCurrent,
    ResolverCurrent,
    ResolverOverview,
    NameHistory,
    ResourceHistory,
    ResourcePermissions,
    NamespaceManifests,
}

pub(crate) const API_ROUTE_DEFINITIONS: &[ApiRouteDefinition] = &[
    ApiRouteDefinition::private_get(ApiRouteId::Health, "/healthz"),
    ApiRouteDefinition::public_get(
        ApiRouteId::Names,
        "/v1/names",
        ApiRouteContract::new(
            "names",
            "App-facing compact name search and exact lookup",
            "Names",
            ApiRouteParameters::Names,
            "CompactNamesResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::AddressNames,
        "/v1/addresses/{address}/names",
        ApiRouteContract::new(
            "address_names",
            "Address-to-surface collection",
            "Collections",
            ApiRouteParameters::AddressNames,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::AddressNamesCount,
        "/v1/addresses/{address}/names/count",
        ApiRouteContract::new(
            "address_names_count",
            "App-facing count for address relation filters",
            "Collections",
            ApiRouteParameters::AddressNamesCount,
            "AddressNamesCountResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::AddressHistory,
        "/v1/history/addresses/{address}",
        ApiRouteContract::new(
            "address_history",
            "Address activity across related surfaces and resources",
            "History",
            ApiRouteParameters::AddressHistory,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::PrimaryNames,
        "/v1/primary-names/{address}",
        ApiRouteContract::new(
            "primary_names",
            "Claimed and verified primary-name answer",
            "Resolution",
            ApiRouteParameters::PrimaryNames,
            "PrimaryNameResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::Coverage,
        "/v1/coverage/{namespace}/{name}",
        ApiRouteContract::new(
            "coverage_current",
            "Single-name coverage and explain details",
            "Coverage",
            ApiRouteParameters::ExactNameSnapshot(
                "Point-in-time selector for the exact-name snapshot. Mutually exclusive with `chain_positions`.",
            ),
            "ExactNameResponse",
            ApiRouteErrorResponses::snapshot(),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ExplainSurfaceBinding,
        "/v1/explain/names/{namespace}/{name}/surface-binding",
        ApiRouteContract::new(
            "explain_surface_binding_current",
            "Current surface-binding explain view for one exact name",
            "Explain",
            ApiRouteParameters::ExactNameSnapshot(
                "Point-in-time selector for the exact-name snapshot. Mutually exclusive with `chain_positions`.",
            ),
            "ExactNameResponse",
            ApiRouteErrorResponses::snapshot(),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ExplainAuthorityControl,
        "/v1/explain/names/{namespace}/{name}/authority-control",
        ApiRouteContract::new(
            "explain_authority_control_current",
            "Current authority/control explain view for one exact name",
            "Explain",
            ApiRouteParameters::ExactNameSnapshot(
                "Point-in-time selector for the exact-name snapshot. Mutually exclusive with `chain_positions`.",
            ),
            "ExactNameResponse",
            ApiRouteErrorResponses::snapshot(),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ExplainResolutionExecution,
        "/v1/explain/resolutions/{namespace}/{name}/execution",
        ApiRouteContract::new(
            "explain_resolution_execution_current",
            "Persisted verified execution explain for one exact-name resolution request",
            "Explain",
            ApiRouteParameters::ExplainResolutionExecution,
            "ResolutionResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NamespaceMetadata,
        "/v1/namespaces/{namespace}",
        ApiRouteContract::new(
            "namespace_metadata",
            "Namespace metadata and support status",
            "Namespaces",
            ApiRouteParameters::NamespacePath,
            "NamespaceMetadataResponse",
            ApiRouteErrorResponses::new(false, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NameChildren,
        "/v1/names/{namespace}/{name}/children",
        ApiRouteContract::new(
            "name_children",
            "Declared child collection by default",
            "Collections",
            ApiRouteParameters::NameChildren,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NameRecords,
        "/v1/names/{namespace}/{name}/records",
        ApiRouteContract::new(
            "name_records",
            "App-facing compact resolver records",
            "Resolution",
            ApiRouteParameters::NameRecords,
            "CompactNameRecordsResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NameRoles,
        "/v1/names/{namespace}/{name}/roles",
        ApiRouteContract::new(
            "name_roles",
            "App-facing role rows for a name's current resource",
            "Collections",
            ApiRouteParameters::NameRoles,
            "CompactRolesResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NameCurrent,
        "/v1/names/{namespace}/{name}",
        ApiRouteContract::new(
            "name_current",
            "Exact name lookup",
            "Names",
            ApiRouteParameters::ExactNameSnapshot(
                "Point-in-time selector for the exact-name snapshot. Mutually exclusive with `chain_positions`.",
            ),
            "ExactNameResponse",
            ApiRouteErrorResponses::snapshot(),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::Events,
        "/v1/events",
        ApiRouteContract::new(
            "events",
            "App-facing compact event search",
            "History",
            ApiRouteParameters::Events,
            "CompactEventsResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::Roles,
        "/v1/roles",
        ApiRouteContract::new(
            "roles",
            "App-facing role rows by account, resource, or name filters",
            "Collections",
            ApiRouteParameters::Roles,
            "CompactRolesResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResourceLookup,
        "/v1/resources/lookup",
        ApiRouteContract::new(
            "resource_lookup",
            "App-facing lookup from name to current resource identity",
            "Resources",
            ApiRouteParameters::ResourceLookup,
            "ResourceLookupResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResolveCurrent,
        "/v1/resolve/{name}",
        ApiRouteContract::new(
            "resolve_current",
            "Namespace-inferred resolution topology, inventory, and verified reads",
            "Resolution",
            ApiRouteParameters::ResolveCurrent,
            "ResolutionResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResolveRecords,
        "/v1/resolve/{name}/records",
        ApiRouteContract::new(
            "resolve_records",
            "Namespace-inferred compact resolver records",
            "Resolution",
            ApiRouteParameters::ResolveRecords,
            "CompactNameRecordsResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResolutionCurrent,
        "/v1/resolutions/{namespace}/{name}",
        ApiRouteContract::new(
            "resolution_current",
            "Resolution topology, inventory, and verified reads",
            "Resolution",
            ApiRouteParameters::ResolutionCurrent,
            "ResolutionResponse",
            ApiRouteErrorResponses::conflict(true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResolverCurrent,
        "/v1/resolvers/{chain_id}/{resolver_address}",
        ApiRouteContract::new(
            "resolver_current",
            "Resolver overview",
            "Resolvers",
            ApiRouteParameters::ResolverPath,
            "ResolverResponse",
            ApiRouteErrorResponses::new(false, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResolverOverview,
        "/v1/resolvers/{chain_id}/{resolver_address}/overview",
        ApiRouteContract::new(
            "resolver_overview",
            "App-facing compact resolver overview",
            "Resolvers",
            ApiRouteParameters::ResolverOverview,
            "CompactResolverOverviewResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NameHistory,
        "/v1/history/names/{namespace}/{name}",
        ApiRouteContract::new(
            "name_history",
            "Surface or combined history",
            "History",
            ApiRouteParameters::NameHistory,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResourceHistory,
        "/v1/history/resources/{resource_id}",
        ApiRouteContract::new(
            "resource_history",
            "Resource history",
            "History",
            ApiRouteParameters::ResourceHistory,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, true),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::ResourcePermissions,
        "/v1/resources/{resource_id}/permissions",
        ApiRouteContract::new(
            "resource_permissions",
            "Resource-centric effective permissions",
            "Collections",
            ApiRouteParameters::ResourcePermissions,
            "CollectionResponse",
            ApiRouteErrorResponses::new(true, false),
        ),
    ),
    ApiRouteDefinition::public_get(
        ApiRouteId::NamespaceManifests,
        "/v1/manifests/{namespace}",
        ApiRouteContract::new(
            "namespace_manifests",
            "Active manifest versions and capabilities",
            "Namespaces",
            ApiRouteParameters::NamespacePath,
            "NamespaceManifestsResponse",
            ApiRouteErrorResponses::new(false, true),
        ),
    ),
];
