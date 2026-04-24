pub(super) fn backfill_job_select_sql(where_clause: &str, suffix: &str) -> String {
    format!(
        r#"
        SELECT
            backfill_job_id,
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            status::TEXT AS status,
            failure_reason,
            failure_metadata,
            created_at,
            updated_at,
            completed_at
        FROM backfill_jobs
        {where_clause}
        {suffix}
        "#
    )
}

pub(super) fn backfill_job_returning_sql(prefix: &str) -> String {
    format!(
        r#"
        {prefix}
        RETURNING
            backfill_job_id,
            deployment_profile,
            chain_id,
            source_identity,
            scan_mode,
            range_start_block_number,
            range_end_block_number,
            idempotency_key,
            status::TEXT AS status,
            failure_reason,
            failure_metadata,
            created_at,
            updated_at,
            completed_at
        "#
    )
}

pub(super) fn backfill_range_select_sql(where_clause: &str, suffix: &str) -> String {
    format!(
        r#"
        SELECT
            backfill_range_id,
            backfill_job_id,
            range_start_block_number,
            range_end_block_number,
            checkpoint_block_number,
            status::TEXT AS status,
            lease_token,
            lease_owner,
            lease_expires_at,
            attempt_count,
            failure_reason,
            failure_metadata,
            created_at,
            updated_at,
            completed_at
        FROM backfill_ranges
        {where_clause}
        {suffix}
        "#
    )
}

pub(super) fn backfill_range_returning_sql(prefix: &str) -> String {
    format!(
        r#"
        {prefix}
        RETURNING
            backfill_range_id,
            backfill_job_id,
            range_start_block_number,
            range_end_block_number,
            checkpoint_block_number,
            status::TEXT AS status,
            lease_token,
            lease_owner,
            lease_expires_at,
            attempt_count,
            failure_reason,
            failure_metadata,
            created_at,
            updated_at,
            completed_at
        "#
    )
}
