use serde_json::json;
use sqlx::types::JsonValue;

use crate::{
    ApiRouteDefinition, ApiRouteId, AppState, Router, address_history, address_names,
    coverage_current, explain_authority_control_current, explain_resolution_execution_current,
    explain_surface_binding_current, get, health, name_children, name_current, name_history,
    namespace_manifests, namespace_metadata, primary_names, resolution_current, resolve_current,
    resolver_current, resource_history, resource_permissions,
};

use super::{
    parameters::{
        address_path_parameter, chain_id_path_parameter, csv_query_parameter,
        cursor_query_parameter, dedupe_by_query_parameter, exact_name_snapshot_parameters,
        history_scope_query_parameter, name_path_parameter, namespace_path_parameter,
        namespace_query_parameter, page_size_query_parameter, primary_name_mode_query_parameter,
        query_parameter, relation_query_parameter, required_coin_type_query_parameter,
        required_csv_query_parameter, required_namespace_query_parameter,
        resolution_current_parameters, resolution_mode_query_parameter,
        resolver_address_path_parameter, resource_id_path_parameter,
    },
    responses::{OpenApiOperationExt, openapi_json_get_operation},
};

impl ApiRouteDefinition {
    pub(super) fn register(self, router: Router<AppState>) -> Router<AppState> {
        match self.id {
            ApiRouteId::Health => router.route(self.path, get(health)),
            ApiRouteId::AddressNames => router.route(self.path, get(address_names)),
            ApiRouteId::AddressHistory => router.route(self.path, get(address_history)),
            ApiRouteId::PrimaryNames => router.route(self.path, get(primary_names)),
            ApiRouteId::Coverage => router.route(self.path, get(coverage_current)),
            ApiRouteId::ExplainSurfaceBinding => {
                router.route(self.path, get(explain_surface_binding_current))
            }
            ApiRouteId::ExplainAuthorityControl => {
                router.route(self.path, get(explain_authority_control_current))
            }
            ApiRouteId::ExplainResolutionExecution => {
                router.route(self.path, get(explain_resolution_execution_current))
            }
            ApiRouteId::NamespaceMetadata => router.route(self.path, get(namespace_metadata)),
            ApiRouteId::NameChildren => router.route(self.path, get(name_children)),
            ApiRouteId::NameCurrent => router.route(self.path, get(name_current)),
            ApiRouteId::ResolveCurrent => router.route(self.path, get(resolve_current)),
            ApiRouteId::ResolutionCurrent => router.route(self.path, get(resolution_current)),
            ApiRouteId::ResolverCurrent => router.route(self.path, get(resolver_current)),
            ApiRouteId::NameHistory => router.route(self.path, get(name_history)),
            ApiRouteId::ResourceHistory => router.route(self.path, get(resource_history)),
            ApiRouteId::ResourcePermissions => router.route(self.path, get(resource_permissions)),
            ApiRouteId::NamespaceManifests => router.route(self.path, get(namespace_manifests)),
        }
    }

    pub(super) fn openapi_path_item(self) -> Option<JsonValue> {
        self.published_in_contract
            .then(|| json!({ "get": self.id.openapi_operation() }))
    }
}

impl ApiRouteId {
    fn operation_id(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::AddressNames => "address_names",
            Self::AddressHistory => "address_history",
            Self::PrimaryNames => "primary_names",
            Self::Coverage => "coverage_current",
            Self::ExplainSurfaceBinding => "explain_surface_binding_current",
            Self::ExplainAuthorityControl => "explain_authority_control_current",
            Self::ExplainResolutionExecution => "explain_resolution_execution_current",
            Self::NamespaceMetadata => "namespace_metadata",
            Self::NameChildren => "name_children",
            Self::NameCurrent => "name_current",
            Self::ResolveCurrent => "resolve_current",
            Self::ResolutionCurrent => "resolution_current",
            Self::ResolverCurrent => "resolver_current",
            Self::NameHistory => "name_history",
            Self::ResourceHistory => "resource_history",
            Self::ResourcePermissions => "resource_permissions",
            Self::NamespaceManifests => "namespace_manifests",
        }
    }

    fn openapi_operation(self) -> JsonValue {
        match self {
            Self::Health => openapi_json_get_operation(
                self.operation_id(),
                "Health check",
                "Health",
                Vec::new(),
                "HealthResponse",
                false,
                false,
            ),
            Self::AddressNames => openapi_json_get_operation(
                self.operation_id(),
                "Address-to-surface collection",
                "Collections",
                vec![
                    address_path_parameter(),
                    namespace_query_parameter(),
                    relation_query_parameter(),
                    dedupe_by_query_parameter(),
                    csv_query_parameter(
                        "include",
                        "Optional collection expansions. `role_summary` is the only shipped expansion.",
                        json!({
                            "type": "string",
                            "enum": ["role_summary"],
                        }),
                    ),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                false,
            ),
            Self::AddressHistory => openapi_json_get_operation(
                self.operation_id(),
                "Address activity across related surfaces and resources",
                "History",
                vec![
                    address_path_parameter(),
                    namespace_query_parameter(),
                    relation_query_parameter(),
                    history_scope_query_parameter(),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                false,
            ),
            Self::PrimaryNames => openapi_json_get_operation(
                self.operation_id(),
                "Claimed and verified primary-name answer",
                "Resolution",
                vec![
                    address_path_parameter(),
                    required_namespace_query_parameter(),
                    required_coin_type_query_parameter(),
                    primary_name_mode_query_parameter(),
                ],
                "PrimaryNameResponse",
                true,
                true,
            ),
            Self::Coverage => openapi_json_get_operation(
                self.operation_id(),
                "Single-name coverage and explain details",
                "Coverage",
                exact_name_snapshot_parameters(
                    "Point-in-time selector for the exact-name snapshot. Mutually exclusive with `chain_positions`.",
                ),
                "ExactNameResponse",
                true,
                true,
            )
            .with_bad_request_description("Invalid snapshot selector")
            .with_conflict_response(),
            Self::ExplainSurfaceBinding => openapi_json_get_operation(
                self.operation_id(),
                "Current surface-binding explain view for one exact name",
                "Explain",
                exact_name_snapshot_parameters(
                    "Point-in-time selector for the exact-name snapshot. Mutually exclusive with `chain_positions`.",
                ),
                "ExactNameResponse",
                true,
                true,
            )
            .with_bad_request_description("Invalid snapshot selector")
            .with_conflict_response(),
            Self::ExplainAuthorityControl => openapi_json_get_operation(
                self.operation_id(),
                "Current authority/control explain view for one exact name",
                "Explain",
                exact_name_snapshot_parameters(
                    "Point-in-time selector for the exact-name snapshot. Mutually exclusive with `chain_positions`.",
                ),
                "ExactNameResponse",
                true,
                true,
            )
            .with_bad_request_description("Invalid snapshot selector")
            .with_conflict_response(),
            Self::ExplainResolutionExecution => openapi_json_get_operation(
                self.operation_id(),
                "Persisted verified execution explain for one exact-name resolution request",
                "Explain",
                vec![
                    namespace_path_parameter(),
                    name_path_parameter(),
                    required_csv_query_parameter(
                        "records",
                        "Comma-separated record selectors. Required for the persisted execution explain lookup.",
                        json!({
                            "type": "string",
                        }),
                    ),
                ],
                "ResolutionResponse",
                true,
                true,
            ),
            Self::NamespaceMetadata => openapi_json_get_operation(
                self.operation_id(),
                "Namespace metadata and support status",
                "Namespaces",
                vec![namespace_path_parameter()],
                "NamespaceMetadataResponse",
                false,
                true,
            ),
            Self::NameChildren => openapi_json_get_operation(
                self.operation_id(),
                "Declared child collection by default",
                "Collections",
                vec![
                    namespace_path_parameter(),
                    name_path_parameter(),
                    csv_query_parameter(
                        "surface_classes",
                        "Requested child surface classes. Only `declared` is currently supported.",
                        json!({
                            "type": "string",
                            "default": "declared",
                        }),
                    ),
                    csv_query_parameter(
                        "include",
                        "Optional collection expansions. `counts` includes `declared_state.subname_count`.",
                        json!({
                            "type": "string",
                            "enum": ["counts"],
                        }),
                    ),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                true,
            ),
            Self::NameCurrent => openapi_json_get_operation(
                self.operation_id(),
                "Exact name lookup",
                "Names",
                exact_name_snapshot_parameters(
                    "Point-in-time selector for the exact-name snapshot. Mutually exclusive with `chain_positions`.",
                ),
                "ExactNameResponse",
                true,
                true,
            )
            .with_bad_request_description("Invalid snapshot selector")
            .with_conflict_response(),
            Self::ResolveCurrent => openapi_json_get_operation(
                self.operation_id(),
                "Namespace-inferred resolution topology, inventory, and verified reads",
                "Resolution",
                vec![
                    name_path_parameter(),
                    resolution_mode_query_parameter(),
                    csv_query_parameter(
                        "records",
                        "Comma-separated record selectors. Required when `mode` is `verified` or `both`.",
                        json!({
                            "type": "string",
                        }),
                    ),
                ],
                "ResolutionResponse",
                true,
                true,
            ),
            Self::ResolutionCurrent => openapi_json_get_operation(
                self.operation_id(),
                "Resolution topology, inventory, and verified reads",
                "Resolution",
                resolution_current_parameters(),
                "ResolutionResponse",
                true,
                true,
            )
            .with_conflict_response(),
            Self::ResolverCurrent => openapi_json_get_operation(
                self.operation_id(),
                "Resolver overview",
                "Resolvers",
                vec![chain_id_path_parameter(), resolver_address_path_parameter()],
                "ResolverResponse",
                false,
                true,
            ),
            Self::NameHistory => openapi_json_get_operation(
                self.operation_id(),
                "Surface or combined history",
                "History",
                vec![
                    namespace_path_parameter(),
                    name_path_parameter(),
                    history_scope_query_parameter(),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                true,
            ),
            Self::ResourceHistory => openapi_json_get_operation(
                self.operation_id(),
                "Resource history",
                "History",
                vec![
                    resource_id_path_parameter(),
                    history_scope_query_parameter(),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                true,
            ),
            Self::ResourcePermissions => openapi_json_get_operation(
                self.operation_id(),
                "Resource-centric effective permissions",
                "Collections",
                vec![
                    resource_id_path_parameter(),
                    query_parameter(
                        "subject",
                        "Optional subject filter for the current effective permissions rows.",
                        json!({
                            "type": "string",
                        }),
                    ),
                    query_parameter(
                        "scope",
                        "Optional scope filter. Accepts `root`, `registry`, `resource`, `resolver:{chain_id}:{resolver_address}`, `record_manager:{chain_id}:{manager_address}`, `migration_derived:{resource_id}`, or `transport_derived:{transport}`.",
                        json!({
                            "type": "string",
                        }),
                    ),
                    cursor_query_parameter(),
                    page_size_query_parameter(),
                ],
                "CollectionResponse",
                true,
                false,
            ),
            Self::NamespaceManifests => openapi_json_get_operation(
                self.operation_id(),
                "Active manifest versions and capabilities",
                "Namespaces",
                vec![namespace_path_parameter()],
                "NamespaceManifestsResponse",
                false,
                true,
            ),
        }
    }
}
