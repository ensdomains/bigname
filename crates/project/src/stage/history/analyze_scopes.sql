-- Temporary tables are not auto-analyzed. Measure the completed scope before
-- costing its correlated history probes, including when the scope is empty.
ANALYZE project_scope_names;
ANALYZE project_scope_children;
ANALYZE project_scope_primary;
