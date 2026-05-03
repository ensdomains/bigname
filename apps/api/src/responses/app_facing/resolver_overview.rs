pub(crate) type CompactResolverOverviewResponse = JsonValue;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolverOverviewInclude {
    nodes: bool,
    aliases: bool,
    roles: bool,
    events: bool,
}

impl ResolverOverviewInclude {
    fn all() -> Self {
        Self {
            nodes: true,
            aliases: true,
            roles: true,
            events: true,
        }
    }

    fn empty() -> Self {
        Self {
            nodes: false,
            aliases: false,
            roles: false,
            events: false,
        }
    }

    fn requests(self, section: ResolverOverviewSection) -> bool {
        match section {
            ResolverOverviewSection::Nodes => self.nodes,
            ResolverOverviewSection::Aliases => self.aliases,
            ResolverOverviewSection::Roles => self.roles,
            ResolverOverviewSection::Events => self.events,
        }
    }
}

pub(crate) fn parse_resolver_overview_include(
    include: Option<&str>,
) -> ApiResult<ResolverOverviewInclude> {
    let mut parsed = ResolverOverviewInclude::empty();
    let mut saw_value = false;

    for value in include
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        saw_value = true;
        match value {
            "nodes" => parsed.nodes = true,
            "aliases" => parsed.aliases = true,
            "roles" => parsed.roles = true,
            "events" => parsed.events = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "include must contain only nodes, aliases, roles, or events"
                        .to_owned(),
                });
            }
        }
    }

    Ok(if saw_value {
        parsed
    } else {
        ResolverOverviewInclude::all()
    })
}

pub(crate) fn build_compact_resolver_overview_response(
    row: ResolverCurrentRow,
    include: ResolverOverviewInclude,
    meta_mode: MetaMode,
) -> CompactResolverOverviewResponse {
    let CompactResolverOverviewData {
        data,
        unsupported_sections,
    } = build_compact_resolver_overview_data(&row, include);

    let mut response = empty_object();
    insert_value_field(&mut response, "data", data);
    if meta_mode != MetaMode::None {
        insert_value_field(
            &mut response,
            "meta",
            build_compact_resolver_overview_meta(&row, &unsupported_sections, meta_mode),
        );
    }
    response
}

struct CompactResolverOverviewData {
    data: JsonValue,
    unsupported_sections: Vec<String>,
}

#[derive(Clone, Copy)]
enum ResolverOverviewSection {
    Nodes,
    Aliases,
    Roles,
    Events,
}

impl ResolverOverviewSection {
    const ALL: [Self; 4] = [Self::Nodes, Self::Aliases, Self::Roles, Self::Events];

    fn field_key(self) -> &'static str {
        match self {
            Self::Nodes => "nodes",
            Self::Aliases => "aliases",
            Self::Roles => "roles",
            Self::Events => "events",
        }
    }

    fn count_key(self) -> &'static str {
        match self {
            Self::Nodes => "nodes",
            Self::Aliases => "aliases",
            Self::Roles => "role_holders",
            Self::Events => "events",
        }
    }

    fn summary_key(self) -> &'static str {
        match self {
            Self::Nodes => "bindings",
            Self::Aliases => "aliases",
            Self::Roles => "role_holders",
            Self::Events => "event_summary",
        }
    }
}

fn build_compact_resolver_overview_data(
    row: &ResolverCurrentRow,
    include: ResolverOverviewInclude,
) -> CompactResolverOverviewData {
    let mut data = empty_object();
    insert_string_field(&mut data, "chain_id", row.chain_id.clone());
    insert_string_field(
        &mut data,
        "resolver_address",
        row.resolver_address.clone(),
    );

    let mut counts = empty_object();
    let mut unsupported_sections = Vec::new();

    for section in ResolverOverviewSection::ALL {
        let section_summary = resolver_overview_summary(row, section);
        let count = section_summary.and_then(projected_section_count);
        let items = section_summary.and_then(|summary| projected_section_items(summary, section));

        if let Some(count) = count {
            insert_value_field(&mut counts, section.count_key(), count);
        } else {
            push_unsupported_section(&mut unsupported_sections, section);
        }

        if include.requests(section) {
            match items {
                Some(items) => insert_value_field(&mut data, section.field_key(), items),
                None => {
                    insert_value_field(&mut data, section.field_key(), JsonValue::Null);
                    push_unsupported_section(&mut unsupported_sections, section);
                }
            }
        }
    }

    insert_value_field(&mut data, "counts", counts);
    CompactResolverOverviewData {
        data,
        unsupported_sections,
    }
}

fn build_compact_resolver_overview_meta(
    row: &ResolverCurrentRow,
    unsupported_sections: &[String],
    meta_mode: MetaMode,
) -> JsonValue {
    let mut meta = empty_object();
    insert_string_field(
        &mut meta,
        "support_status",
        compact_resolver_overview_support_status(unsupported_sections).to_owned(),
    );
    insert_value_field(&mut meta, "unsupported_filters", JsonValue::Array(Vec::new()));
    insert_value_field(
        &mut meta,
        "unsupported_fields",
        JsonValue::Array(
            unsupported_sections
                .iter()
                .cloned()
                .map(JsonValue::String)
                .collect(),
        ),
    );

    if meta_mode == MetaMode::Full {
        insert_value_field(&mut meta, "provenance", build_name_provenance(&row.provenance));
        insert_value_field(&mut meta, "coverage", build_name_coverage(&row.coverage));
        insert_value_field(
            &mut meta,
            "chain_positions",
            ensure_object(&row.chain_positions),
        );
        insert_string_field(
            &mut meta,
            "consistency",
            canonicality_consistency(&row.canonicality_summary).to_owned(),
        );
        insert_string_field(
            &mut meta,
            "last_updated",
            format_timestamp(row.last_recomputed_at),
        );
    }

    meta
}

fn resolver_overview_summary(
    row: &ResolverCurrentRow,
    section: ResolverOverviewSection,
) -> Option<&JsonValue> {
    provenance_field(&row.declared_summary, section.summary_key()).filter(|value| value.is_object())
}

fn projected_section_count(summary: &JsonValue) -> Option<JsonValue> {
    if !summary_is_supported(summary) {
        return None;
    }

    provenance_field(summary, "count")
        .filter(|value| value.is_number())
        .cloned()
        .or_else(|| {
            provenance_field(summary, "items")
                .and_then(JsonValue::as_array)
                .map(|items| json!(items.len()))
        })
}

fn projected_section_items(summary: &JsonValue, section: ResolverOverviewSection) -> Option<JsonValue> {
    if !summary_is_supported(summary) {
        return None;
    }

    provenance_field(summary, "items")
        .and_then(JsonValue::as_array)
        .map(|items| match section {
            ResolverOverviewSection::Nodes | ResolverOverviewSection::Aliases => JsonValue::Array(
                items.iter().map(compact_resolver_binding_item).collect(),
            ),
            ResolverOverviewSection::Roles | ResolverOverviewSection::Events => {
                JsonValue::Array(items.clone())
            }
        })
}

fn compact_resolver_binding_item(item: &JsonValue) -> JsonValue {
    let mut compact = empty_object();
    if let Some(logical_name_id) =
        provenance_field(item, "logical_name_id").and_then(JsonValue::as_str)
        && let Some((namespace, _)) = logical_name_id.split_once(':')
    {
        insert_string_field(&mut compact, "namespace", namespace.to_owned());
    }
    insert_optional_string_field(
        &mut compact,
        "name",
        provenance_field(item, "canonical_display_name")
            .and_then(JsonValue::as_str)
            .map(str::to_owned),
    );
    insert_optional_string_field(
        &mut compact,
        "normalized_name",
        provenance_field(item, "normalized_name")
            .and_then(JsonValue::as_str)
            .map(str::to_owned),
    );
    insert_optional_string_field(
        &mut compact,
        "namehash",
        provenance_field(item, "namehash")
            .and_then(JsonValue::as_str)
            .map(str::to_owned),
    );
    compact
}

fn summary_is_supported(summary: &JsonValue) -> bool {
    string_field(provenance_field(summary, "status")).as_deref() == Some("supported")
}

fn compact_resolver_overview_support_status(unsupported_sections: &[String]) -> &'static str {
    match unsupported_sections.len() {
        0 => "supported",
        4 => "unsupported",
        _ => "partial",
    }
}

fn push_unsupported_section(
    unsupported_sections: &mut Vec<String>,
    section: ResolverOverviewSection,
) {
    let section = section.field_key();
    if !unsupported_sections.iter().any(|value| value == section) {
        unsupported_sections.push(section.to_owned());
    }
}
