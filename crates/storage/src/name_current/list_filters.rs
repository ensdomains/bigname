// The `filtered_names` predicates of the list reader, included into list.rs.



fn push_name_current_filter_predicates<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    filter: &'a NameCurrentListFilter,
) {
    if filter.supported_only {
        builder.push(" AND nc.support_status = 'supported'");
    }
    if let Some(namespaces) = filter
        .namespaces
        .as_ref()
        .filter(|namespaces| !namespaces.is_empty())
    {
        builder.push(" AND nc.namespace = ANY(");
        builder.push_bind(namespaces.as_slice());
        builder.push(")");
    } else if let Some(namespace) = filter.namespace.as_deref() {
        builder.push(" AND nc.namespace = ");
        builder.push_bind(namespace);
    }
    if let Some(name) = filter.name.as_deref() {
        builder.push(" AND nc.raw_name = ");
        builder.push_bind(name);
    }
    if let Some(prefix) = filter.prefix.as_deref() {
        builder.push(" AND nc.raw_name LIKE ");
        builder.push_bind(format!("{}%", escape_like_pattern(prefix)));
        builder.push(" ESCAPE '\\'");
    }
    if let Some(contains) = filter.contains.as_deref() {
        builder.push(" AND nc.raw_name LIKE ");
        builder.push_bind(format!("%{}%", escape_like_pattern(contains)));
        builder.push(" ESCAPE '\\'");
    }
    if let Some(contains_nocase) = filter.contains_nocase.as_deref() {
        builder.push(" AND nc.raw_name LIKE ");
        builder.push_bind(format!(
            "%{}%",
            escape_like_pattern(&contains_nocase.to_ascii_lowercase())
        ));
        builder.push(" ESCAPE '\\'");
    }
    if let Some(resolver) = filter.resolver.as_deref() {
        builder.push(" AND LOWER(nc.declared_summary #>> '{resolver,address}') = ");
        builder.push_bind(resolver);
    }
    if filter.is_migrated == Some(true) {
        builder.push(
            " AND (nc.declared_summary #>> '{registration,authority_kind}') = 'ens_v2_registry'",
        );
    }
}
