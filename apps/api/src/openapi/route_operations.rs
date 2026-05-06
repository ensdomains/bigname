use serde_json::{Map as JsonMap, json};
use sqlx::types::JsonValue;

use crate::routes::{
    ApiRouteContract, ApiRouteDefinition, ApiRouteId, ApiRouteMethod, ApiRouteParameters,
};
use crate::{
    AppState, Router, address_history, address_names, address_names_count, coverage_current, events,
    explain_authority_control_current, explain_resolution_execution_current,
    explain_surface_binding_current, get, health, name_children, name_current, name_history,
    name_records, name_roles, names, namespace_manifests, namespace_metadata, primary_names,
    resolution_current, resolve_current, resolve_records, resolver_current, resolver_overview,
    resource_history, resource_lookup, resource_permissions, roles,
};

use super::{
    parameters::{
        address_names_count_parameters, address_path_parameter, chain_id_path_parameter,
        csv_query_parameter, cursor_query_parameter, dedupe_by_query_parameter, events_parameters,
        exact_name_snapshot_parameters, history_meta_query_parameter,
        history_scope_query_parameter, history_view_query_parameter, inferred_name_records_parameters,
        meta_query_parameter, name_path_parameter, name_records_parameters, name_roles_parameters,
        names_parameters, namespace_path_parameter, namespace_query_parameter,
        page_size_query_parameter, primary_name_mode_query_parameter, query_parameter,
        relation_query_parameter, required_coin_type_query_parameter, required_csv_query_parameter,
        required_namespace_query_parameter, resolution_current_parameters,
        resolution_mode_query_parameter, resolver_address_path_parameter,
        resolver_overview_parameters, resource_id_path_parameter, resource_lookup_parameters,
        roles_parameters, view_query_parameter,
    },
    responses::{OpenApiOperationExt, openapi_json_get_operation},
};

impl ApiRouteDefinition {
    pub(super) fn register(self, router: Router<AppState>) -> Router<AppState> {
        match self.method {
            ApiRouteMethod::Get => self.register_get(router),
        }
    }

    fn register_get(self, router: Router<AppState>) -> Router<AppState> {
        match self.id {
            ApiRouteId::Health => router.route(self.path, get(health)),
            ApiRouteId::Names => router.route(self.path, get(names)),
            ApiRouteId::AddressNames => router.route(self.path, get(address_names)),
            ApiRouteId::AddressNamesCount => router.route(self.path, get(address_names_count)),
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
            ApiRouteId::NameRecords => router.route(self.path, get(name_records)),
            ApiRouteId::NameRoles => router.route(self.path, get(name_roles)),
            ApiRouteId::Events => router.route(self.path, get(events)),
            ApiRouteId::Roles => router.route(self.path, get(roles)),
            ApiRouteId::ResourceLookup => router.route(self.path, get(resource_lookup)),
            ApiRouteId::ResolveCurrent => router.route(self.path, get(resolve_current)),
            ApiRouteId::ResolveRecords => router.route(self.path, get(resolve_records)),
            ApiRouteId::ResolutionCurrent => router.route(self.path, get(resolution_current)),
            ApiRouteId::ResolverCurrent => router.route(self.path, get(resolver_current)),
            ApiRouteId::ResolverOverview => router.route(self.path, get(resolver_overview)),
            ApiRouteId::NameHistory => router.route(self.path, get(name_history)),
            ApiRouteId::ResourceHistory => router.route(self.path, get(resource_history)),
            ApiRouteId::ResourcePermissions => router.route(self.path, get(resource_permissions)),
            ApiRouteId::NamespaceManifests => router.route(self.path, get(namespace_manifests)),
        }
    }

    pub(super) fn openapi_path_item(self) -> Option<JsonValue> {
        let contract = self.contract?;
        let mut path_item = JsonMap::new();
        path_item.insert(
            self.method.openapi_key().to_owned(),
            contract.openapi_operation(),
        );
        Some(JsonValue::Object(path_item))
    }
}

impl ApiRouteMethod {
    fn openapi_key(self) -> &'static str {
        match self {
            Self::Get => "get",
        }
    }
}

impl ApiRouteContract {
    fn openapi_operation(self) -> JsonValue {
        let errors = self.errors;
        let mut operation = openapi_json_get_operation(
            self.operation_id,
            self.summary,
            self.tag,
            self.parameters.openapi_parameters(),
            self.success_schema,
            errors.include_bad_request,
            errors.include_not_found,
        );
        if let Some(description) = errors.bad_request_description {
            operation = operation.with_bad_request_description(description);
        }
        if errors.include_conflict {
            operation = operation.with_conflict_response();
        }
        operation
    }
}

impl ApiRouteParameters {
    fn openapi_parameters(self) -> Vec<JsonValue> {
        match self {
            Self::Names => names_parameters(),
            Self::AddressNames => address_names_parameters(),
            Self::AddressNamesCount => address_names_count_parameters(),
            Self::AddressHistory => address_history_parameters(),
            Self::PrimaryNames => primary_names_parameters(),
            Self::ExactNameSnapshot(at_description) => {
                exact_name_snapshot_parameters(at_description)
            }
            Self::ExplainResolutionExecution => explain_resolution_execution_parameters(),
            Self::NamespacePath => vec![namespace_path_parameter()],
            Self::NameChildren => name_children_parameters(),
            Self::NameRecords => name_records_parameters(),
            Self::NameRoles => name_roles_parameters(),
            Self::Events => events_parameters(),
            Self::Roles => roles_parameters(),
            Self::ResourceLookup => resource_lookup_parameters(),
            Self::ResolveCurrent => resolve_current_parameters(),
            Self::ResolveRecords => inferred_name_records_parameters(),
            Self::ResolutionCurrent => resolution_current_parameters(),
            Self::ResolverPath => resolver_path_parameters(),
            Self::ResolverOverview => resolver_overview_parameters(),
            Self::NameHistory => name_history_parameters(),
            Self::ResourceHistory => resource_history_parameters(),
            Self::ResourcePermissions => resource_permissions_parameters(),
        }
    }
}

fn address_names_parameters() -> Vec<JsonValue> {
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
    ]
}

fn address_history_parameters() -> Vec<JsonValue> {
    vec![
        address_path_parameter(),
        namespace_query_parameter(),
        relation_query_parameter(),
        history_scope_query_parameter(),
        history_view_query_parameter(),
        history_meta_query_parameter(),
        cursor_query_parameter(),
        page_size_query_parameter(),
    ]
}

fn primary_names_parameters() -> Vec<JsonValue> {
    vec![
        address_path_parameter(),
        required_namespace_query_parameter(),
        required_coin_type_query_parameter(),
        primary_name_mode_query_parameter(),
    ]
}

fn explain_resolution_execution_parameters() -> Vec<JsonValue> {
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
    ]
}

fn name_children_parameters() -> Vec<JsonValue> {
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
            "Optional collection expansions. `counts` includes compact row `subname_count` or full-envelope `declared_state.subname_count`.",
            json!({
                "type": "string",
                "enum": ["counts"],
            }),
        ),
        view_query_parameter("compact"),
        meta_query_parameter("summary"),
        cursor_query_parameter(),
        page_size_query_parameter(),
    ]
}

fn resolve_current_parameters() -> Vec<JsonValue> {
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
    ]
}

fn resolver_path_parameters() -> Vec<JsonValue> {
    vec![chain_id_path_parameter(), resolver_address_path_parameter()]
}

fn name_history_parameters() -> Vec<JsonValue> {
    vec![
        namespace_path_parameter(),
        name_path_parameter(),
        history_scope_query_parameter(),
        history_view_query_parameter(),
        history_meta_query_parameter(),
        cursor_query_parameter(),
        page_size_query_parameter(),
    ]
}

fn resource_history_parameters() -> Vec<JsonValue> {
    vec![
        resource_id_path_parameter(),
        history_scope_query_parameter(),
        history_view_query_parameter(),
        history_meta_query_parameter(),
        cursor_query_parameter(),
        page_size_query_parameter(),
    ]
}

fn resource_permissions_parameters() -> Vec<JsonValue> {
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
    ]
}
