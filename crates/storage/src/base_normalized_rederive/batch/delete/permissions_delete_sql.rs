pub(super) fn permissions_current_sql() -> &'static str {
    r#"
    WITH deleted_resource_summaries AS (
        DELETE FROM permissions_current_resource_summary summary
        USING base_rederive_scope_resources identity_scope
        WHERE summary.resource_id = identity_scope.resource_id
        RETURNING summary.resource_id
    ),
    candidate_rows AS (
        SELECT ctid, resource_id, subject, scope
        FROM base_rederive_delete_permissions_current_candidates
        ORDER BY resource_id, subject, scope
        LIMIT $1
    ),
    removed_candidates AS (
        DELETE FROM base_rederive_delete_permissions_current_candidates candidate
        USING candidate_rows selected
        WHERE candidate.ctid = selected.ctid
        RETURNING candidate.resource_id, candidate.subject, candidate.scope
    ),
    deleted AS (
        DELETE FROM permissions_current p
        USING removed_candidates c
        WHERE p.resource_id = c.resource_id
          AND p.subject = c.subject
          AND p.scope = c.scope
        RETURNING p.resource_id::TEXT || '|' || p.subject || '|' || p.scope AS key_text
    )
    SELECT (COUNT(*) + 0 * (SELECT COUNT(*) FROM deleted_resource_summaries))::BIGINT AS row_count,
           MIN(key_text)::TEXT AS range_start,
           MAX(key_text)::TEXT AS range_end
    FROM deleted
    "#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permissions_delete_removes_resource_summaries_in_the_same_statement() {
        let sql = permissions_current_sql();

        assert!(sql.contains("DELETE FROM permissions_current_resource_summary"));
        assert!(sql.contains("USING base_rederive_scope_resources"));
        assert!(sql.contains("deleted_resource_summaries"));
    }
}
