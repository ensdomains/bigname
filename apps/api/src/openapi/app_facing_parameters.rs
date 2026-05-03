pub(super) fn names_parameters() -> Vec<JsonValue> {
    vec![
        namespace_query_parameter(),
        query_parameter(
            "name",
            "Exact normalized-name lookup filter.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "prefix",
            "Normalized-name prefix search filter.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "contains",
            "Normalized-name contains search filter.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "contains_nocase",
            "Case-insensitive normalized-name contains search filter.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "owner",
            "Token-holder / owner address filter.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "account",
            "Address relation filter anchor.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "registrant",
            "Registrant address filter.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "resolver",
            "Current declared resolver address filter.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "resolved_address",
            "Declared record-value equality filter when projected.",
            json!({"type": "string"}),
        ),
        app_relation_query_parameter(),
        query_parameter(
            "sort",
            "Stable compact names sort key.",
            json!({
                "type": "string",
                "enum": ["name", "expiry_date", "registration_date", "created_at"],
                "default": "name",
            }),
        ),
        order_query_parameter(),
        csv_query_parameter(
            "include",
            "Optional compact name expansions.",
            json!({
                "type": "string",
                "enum": ["record_summaries", "total_count"],
            }),
        ),
        view_query_parameter("compact"),
        meta_query_parameter("summary"),
        cursor_query_parameter(),
        page_size_query_parameter(),
    ]
}

pub(super) fn address_names_count_parameters() -> Vec<JsonValue> {
    vec![
        address_path_parameter(),
        namespace_query_parameter(),
        app_relation_query_parameter(),
        query_parameter(
            "prefix",
            "Normalized-name prefix search filter.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "contains",
            "Normalized-name contains search filter.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "contains_nocase",
            "Case-insensitive normalized-name contains search filter.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "resolver",
            "Current declared resolver address filter.",
            json!({"type": "string"}),
        ),
    ]
}

pub(super) fn resource_lookup_parameters() -> Vec<JsonValue> {
    vec![
        required_namespace_query_parameter(),
        required_query_parameter(
            "name",
            "Required normalized name to resolve to a current resource identity.",
            json!({"type": "string"}),
        ),
        view_query_parameter("compact"),
        meta_query_parameter("summary"),
    ]
}

fn app_relation_query_parameter() -> JsonValue {
    query_parameter(
        "relation",
        "Optional app-facing relation facet filter.",
        json!({
            "type": "string",
            "enum": ["token_holder", "registrant", "effective_controller", "any"],
        }),
    )
}

pub(super) fn name_records_parameters() -> Vec<JsonValue> {
    let mut parameters = vec![namespace_path_parameter(), name_path_parameter()];
    parameters.extend(name_records_query_parameters("declared", "resolver_address"));
    parameters
}

pub(super) fn inferred_name_records_parameters() -> Vec<JsonValue> {
    let mut parameters = vec![name_path_parameter()];
    parameters.extend(name_records_query_parameters(
        "auto",
        "resolver_address,known_text_keys,avatar,content_hash,coins",
    ));
    parameters
}

fn name_records_query_parameters(default_mode: &'static str, default_include: &'static str) -> Vec<JsonValue> {
    vec![
        query_parameter(
            "mode",
            "Compact records read mode. `auto` uses declared cache when the resolver profile is authoritative, otherwise verified resolution for requested selectors. When no declared selectors are available, app-facing defaults probe only a bounded basic profile set.",
            json!({
                "type": "string",
                "enum": ["auto", "declared", "verified", "both"],
                "default": default_mode,
            }),
        ),
        csv_query_parameter(
            "texts",
            "Requested text record keys.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "known_text_keys",
            "Whether to return projected known text-key inventory.",
            json!({"type": "boolean"}),
        ),
        query_parameter(
            "avatar",
            "Whether to request the avatar text convenience field.",
            json!({"type": "boolean"}),
        ),
        query_parameter(
            "content_hash",
            "Whether to request the content-hash selector.",
            json!({"type": "boolean"}),
        ),
        csv_query_parameter(
            "coin_types",
            "Requested textual coin-type selector keys.",
            json!({"type": "string"}),
        ),
        csv_query_parameter(
            "include",
            "Optional compact record sections.",
            json!({
                "type": "string",
                "enum": ["resolver_address", "known_text_keys", "avatar", "content_hash", "coins"],
                "default": default_include,
            }),
        ),
        view_query_parameter("compact"),
        meta_query_parameter("summary"),
    ]
}

pub(super) fn events_parameters() -> Vec<JsonValue> {
    vec![
        namespace_query_parameter(),
        query_parameter(
            "name",
            "Normalized name event anchor filter.",
            json!({"type": "string"}),
        ),
        address_path_like_query_parameter("address", "Address relation event filter."),
        query_parameter(
            "resource",
            "Opaque resource identifier filter.",
            json!({"type": "string", "format": "uuid"}),
        ),
        query_parameter(
            "resource_id",
            "Opaque resource identifier filter.",
            json!({"type": "string", "format": "uuid"}),
        ),
        query_parameter(
            "type",
            "Normalized event type or compact type alias filter.",
            json!({"type": "string"}),
        ),
        app_relation_query_parameter(),
        query_parameter(
            "from_block",
            "Inclusive canonical block lower bound.",
            json!({"type": "integer", "minimum": 0}),
        ),
        query_parameter(
            "to_block",
            "Inclusive canonical block upper bound.",
            json!({"type": "integer", "minimum": 0}),
        ),
        view_query_parameter("compact"),
        meta_query_parameter("summary"),
        cursor_query_parameter(),
        page_size_query_parameter(),
    ]
}

pub(super) fn roles_parameters() -> Vec<JsonValue> {
    vec![
        address_path_like_query_parameter("account", "Effective permission subject filter."),
        query_parameter(
            "resource_id",
            "Opaque resource identifier filter.",
            json!({"type": "string", "format": "uuid"}),
        ),
        namespace_query_parameter(),
        query_parameter(
            "name",
            "Normalized name lookup filter paired with namespace.",
            json!({"type": "string"}),
        ),
        query_parameter(
            "role_bitmap",
            "Projected role bitmap filter when supported.",
            json!({"type": "string"}),
        ),
        view_query_parameter("compact"),
        meta_query_parameter("summary"),
        cursor_query_parameter(),
        page_size_query_parameter(),
    ]
}

pub(super) fn name_roles_parameters() -> Vec<JsonValue> {
    vec![
        namespace_path_parameter(),
        name_path_parameter(),
        address_path_like_query_parameter("account", "Effective permission subject filter."),
        query_parameter(
            "role_bitmap",
            "Projected role bitmap filter when supported.",
            json!({"type": "string"}),
        ),
        view_query_parameter("compact"),
        meta_query_parameter("summary"),
        cursor_query_parameter(),
        page_size_query_parameter(),
    ]
}

pub(super) fn resolver_overview_parameters() -> Vec<JsonValue> {
    vec![
        chain_id_path_parameter(),
        resolver_address_path_parameter(),
        csv_query_parameter(
            "include",
            "Requested compact resolver overview sections.",
            json!({
                "type": "string",
                "enum": ["nodes", "aliases", "roles", "events"],
                "default": "nodes,aliases,roles,events",
            }),
        ),
        view_query_parameter("compact"),
        meta_query_parameter("summary"),
    ]
}

fn order_query_parameter() -> JsonValue {
    query_parameter(
        "order",
        "Stable sort order.",
        json!({
            "type": "string",
            "enum": ["asc", "desc"],
            "default": "asc",
        }),
    )
}

fn address_path_like_query_parameter(name: &'static str, description: &'static str) -> JsonValue {
    query_parameter(
        name,
        description,
        json!({
            "type": "string",
        }),
    )
}
