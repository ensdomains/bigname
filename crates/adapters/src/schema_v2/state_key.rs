use uuid::Uuid;

pub(super) fn interpreter_state_key(
    namespace: &str,
    logical_name_id: Option<&str>,
    resource_id: Option<Uuid>,
    event_kind: &str,
    source_family: &str,
    state_scope: &str,
) -> String {
    let facet = state_facet(event_kind);
    format!(
        "{namespace}:{source_family}:{}:{}:{facet}:{state_scope}",
        logical_name_id.unwrap_or("-"),
        resource_id.map_or_else(|| "-".to_owned(), |id| id.to_string())
    )
}

fn state_facet(event_kind: &str) -> &str {
    match event_kind {
        "RegistrationGranted"
        | "RegistrarNameRegistered"
        | "RegistrationReleased"
        | "RegistrationRenewed"
        | "RegistrationReserved" => "registration",
        "ResolverChanged" => "resolver",
        "SubregistryChanged" => "subregistry",
        "AuthorityTransferred" => "authority",
        "ExpiryChanged" => "expiry",
        "PermissionChanged" | "RootPermissionChanged" => "permission",
        "RecordChanged" | "RecordVersionChanged" => "records",
        other => other,
    }
}
