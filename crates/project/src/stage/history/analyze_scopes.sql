/* project:stage.history.analyze_scopes.analyze_scope_names */
-- Temporary tables are not auto-analyzed. Measure the completed scope before
-- costing its correlated history probes, including when the scope is empty.
ANALYZE project_scope_names;
/* project:stage.history.analyze_scopes.analyze_scope_children */
ANALYZE project_scope_children;
/* project:stage.history.analyze_scopes.analyze_scope_primary */
ANALYZE project_scope_primary;
