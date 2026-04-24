#[derive(Clone, Copy)]
pub(crate) struct ApiRouteDefinition {
    pub(crate) id: ApiRouteId,
    pub(crate) path: &'static str,
    pub(crate) published_in_contract: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum ApiRouteId {
    Health,
    AddressNames,
    AddressHistory,
    PrimaryNames,
    Coverage,
    ExplainSurfaceBinding,
    ExplainAuthorityControl,
    ExplainResolutionExecution,
    NamespaceMetadata,
    NameChildren,
    NameCurrent,
    ResolveCurrent,
    ResolutionCurrent,
    ResolverCurrent,
    NameHistory,
    ResourceHistory,
    ResourcePermissions,
    NamespaceManifests,
}

pub(crate) const API_ROUTE_DEFINITIONS: &[ApiRouteDefinition] = &[
    ApiRouteDefinition {
        id: ApiRouteId::Health,
        path: "/healthz",
        published_in_contract: false,
    },
    ApiRouteDefinition {
        id: ApiRouteId::AddressNames,
        path: "/v1/addresses/{address}/names",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::AddressHistory,
        path: "/v1/history/addresses/{address}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::PrimaryNames,
        path: "/v1/primary-names/{address}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::Coverage,
        path: "/v1/coverage/{namespace}/{name}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ExplainSurfaceBinding,
        path: "/v1/explain/names/{namespace}/{name}/surface-binding",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ExplainAuthorityControl,
        path: "/v1/explain/names/{namespace}/{name}/authority-control",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ExplainResolutionExecution,
        path: "/v1/explain/resolutions/{namespace}/{name}/execution",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::NamespaceMetadata,
        path: "/v1/namespaces/{namespace}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::NameChildren,
        path: "/v1/names/{namespace}/{name}/children",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::NameCurrent,
        path: "/v1/names/{namespace}/{name}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ResolveCurrent,
        path: "/v1/resolve/{name}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ResolutionCurrent,
        path: "/v1/resolutions/{namespace}/{name}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ResolverCurrent,
        path: "/v1/resolvers/{chain_id}/{resolver_address}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::NameHistory,
        path: "/v1/history/names/{namespace}/{name}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ResourceHistory,
        path: "/v1/history/resources/{resource_id}",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::ResourcePermissions,
        path: "/v1/resources/{resource_id}/permissions",
        published_in_contract: true,
    },
    ApiRouteDefinition {
        id: ApiRouteId::NamespaceManifests,
        path: "/v1/manifests/{namespace}",
        published_in_contract: true,
    },
];
