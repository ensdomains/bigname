pub(super) fn names_parameters() -> Vec<JsonValue> {
    vec![
        namespace_query_parameter(),
        query_parameter(
            "name",
            "Exact normalized-name lookup filter.",
            string_schema(),
        ),
        query_parameter(
            "prefix",
            "Normalized-name prefix search filter.",
            string_schema(),
        ),
        query_parameter(
            "contains",
            "Normalized-name contains search filter.",
            string_schema(),
        ),
        query_parameter(
            "contains_nocase",
            "Case-insensitive normalized-name contains search filter.",
            string_schema(),
        ),
        query_parameter(
            "owner",
            "Token-holder / owner address filter.",
            string_schema(),
        ),
        query_parameter(
            "account",
            "Address relation filter anchor.",
            string_schema(),
        ),
        query_parameter(
            "registrant",
            "Registrant address filter.",
            string_schema(),
        ),
        query_parameter(
            "resolver",
            "Current declared resolver address filter.",
            string_schema(),
        ),
        query_parameter(
            "resolved_address",
            "Declared record-value equality filter when projected.",
            string_schema(),
        ),
        app_relation_query_parameter(),
        query_parameter(
            "sort",
            "Stable compact names sort key.",
            string_enum_default_schema(
                &["name", "expiry_date", "registration_date", "created_at"],
                "name",
            ),
        ),
        order_query_parameter(),
        csv_query_parameter(
            "include",
            "Optional compact name expansions.",
            string_enum_schema(&["record_summaries", "total_count"]),
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
            string_schema(),
        ),
        query_parameter(
            "contains",
            "Normalized-name contains search filter.",
            string_schema(),
        ),
        query_parameter(
            "contains_nocase",
            "Case-insensitive normalized-name contains search filter.",
            string_schema(),
        ),
        query_parameter(
            "resolver",
            "Current declared resolver address filter.",
            string_schema(),
        ),
    ]
}

pub(super) fn resource_lookup_parameters() -> Vec<JsonValue> {
    vec![
        required_namespace_query_parameter(),
        required_query_parameter(
            "name",
            "Required normalized name to resolve to a current resource identity.",
            string_schema(),
        ),
        view_query_parameter("compact"),
        meta_query_parameter("summary"),
    ]
}

fn app_relation_query_parameter() -> JsonValue {
    query_parameter(
        "relation",
        "Optional app-facing relation facet filter.",
        string_enum_schema(&["token_holder", "registrant", "effective_controller", "any"]),
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
            string_enum_default_schema(&["auto", "declared", "verified", "both"], default_mode),
        ),
        csv_query_parameter(
            "texts",
            "Requested text record keys.",
            string_schema(),
        ),
        query_parameter(
            "known_text_keys",
            "Whether to return projected known text-key inventory.",
            boolean_schema(),
        ),
        query_parameter(
            "avatar",
            "Whether to request the avatar text convenience field.",
            boolean_schema(),
        ),
        query_parameter(
            "content_hash",
            "Whether to request the content-hash selector.",
            boolean_schema(),
        ),
        csv_query_parameter(
            "coin_types",
            "Requested textual coin-type selector keys.",
            string_schema(),
        ),
        csv_query_parameter(
            "include",
            "Optional compact record sections.",
            string_enum_default_schema(
                &[
                    "resolver_address",
                    "known_text_keys",
                    "avatar",
                    "content_hash",
                    "coins",
                ],
                default_include,
            ),
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
            string_schema(),
        ),
        address_path_like_query_parameter("address", "Address relation event filter."),
        query_parameter(
            "resource",
            "Opaque resource identifier filter.",
            uuid_string_schema(),
        ),
        query_parameter(
            "resource_id",
            "Opaque resource identifier filter.",
            uuid_string_schema(),
        ),
        query_parameter(
            "type",
            "Normalized event type or compact type alias filter.",
            string_schema(),
        ),
        app_relation_query_parameter(),
        query_parameter(
            "from_block",
            "Inclusive canonical block lower bound.",
            integer_min_schema(0),
        ),
        query_parameter(
            "to_block",
            "Inclusive canonical block upper bound.",
            integer_min_schema(0),
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
            uuid_string_schema(),
        ),
        namespace_query_parameter(),
        query_parameter(
            "name",
            "Normalized name lookup filter paired with namespace.",
            string_schema(),
        ),
        query_parameter(
            "role_bitmap",
            "Projected role bitmap filter when supported.",
            string_schema(),
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
            string_schema(),
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
            string_enum_default_schema(
                &["nodes", "aliases", "roles", "events"],
                "nodes,aliases,roles,events",
            ),
        ),
        view_query_parameter("compact"),
        meta_query_parameter("summary"),
    ]
}

fn order_query_parameter() -> JsonValue {
    query_parameter(
        "order",
        "Stable sort order.",
        string_enum_default_schema(&["asc", "desc"], "asc"),
    )
}

fn address_path_like_query_parameter(name: &'static str, description: &'static str) -> JsonValue {
    query_parameter(
        name,
        description,
        string_schema(),
    )
}
