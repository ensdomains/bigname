// Keep each history lookup correlated to one changed key. OFFSET 0 prevents the
// planner from flattening the lateral branches back into a chain-wide history scan.
// Visibility is deliberately unrestricted here; create_events applies the serving filter.
pub(super) const SCOPED_NAME_HISTORY_SQL: &str = include_str!("history/names.sql");
pub(super) const SCOPED_PRIMARY_HISTORY_SQL: &str = include_str!("history/primary.sql");
pub(super) const ANALYZE_HISTORY_SCOPES_SQL: &str = include_str!("history/analyze_scopes.sql");

#[cfg(test)]
mod tests;
