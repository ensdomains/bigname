/// SQL `VALUES` rows for resolver addresses carried by a `PermissionChanged` scope.
///
/// Revokes can identify the resolver through `before_state`, while grants identify it through
/// `after_state`. Keep every resolver-scoping query on this shared pair.
pub(crate) const PERMISSION_CHANGED_RESOLVER_ADDRESS_VALUES: &str = r#"
    (CASE WHEN event.event_kind = 'PermissionChanged'
          THEN event.after_state #>> '{scope,resolver_address}' END),
    (CASE WHEN event.event_kind = 'PermissionChanged'
          THEN event.before_state #>> '{scope,resolver_address}' END)
"#;
