//! SQL timestamp representations used by collection ordering and the name summary writer.
use sqlx::{Postgres, QueryBuilder};

/// Preserve each reader's existing alias order, but never revive an expiry the composition
/// deliberately set to null (released registrations and contextual no-expiry sentinels).
pub(crate) fn push_expiry_paths_expr(builder: &mut QueryBuilder<'_, Postgres>, paths: &[&[&str]]) {
    builder.push("CASE WHEN nc.declared_summary #> '{registration,expiry}' = 'null'::jsonb THEN NULL::NUMERIC ELSE COALESCE(");
    for (index, path) in paths.iter().enumerate() {
        if index > 0 {
            builder.push(", ");
        }
        push_json_seconds_expr(builder, path);
    }
    builder.push(") END");
}

/// Decimal JSON strings and numbers retain their exact value; historical clock-shaped aliases
/// remain comparable by extracting their numeric epoch, without a floating-point conversion.
fn push_json_seconds_expr(builder: &mut QueryBuilder<'_, Postgres>, path: &[&str]) {
    let value = format!("(nc.declared_summary #>> '{{{}}}')", path.join(","));
    builder.push(format!(
        "CASE WHEN {value} ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN {value}::NUMERIC \
         WHEN {value} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}(\\.[0-9]+)?(Z|[+-][0-9]{{2}}:[0-9]{{2}})$' \
         THEN EXTRACT(EPOCH FROM {value}::TIMESTAMPTZ) ELSE NULL::NUMERIC END"
    ));
}

pub(crate) fn push_json_timestamp_expr(builder: &mut QueryBuilder<'_, Postgres>, path: &[&str]) {
    let path_literal = format!("'{{{}}}'", path.join(","));
    builder.push("CASE WHEN JSONB_TYPEOF(nc.declared_summary #> ");
    builder.push(path_literal.as_str());
    // These are observation/registration clocks, whose public range is a calendar instant.
    // Expiries use the numeric expression above, including values beyond the calendar range.
    builder.push(") = 'number' THEN CASE WHEN (nc.declared_summary #>> ");
    builder.push(path_literal.as_str());
    builder.push(
        ")::NUMERIC BETWEEN 0 AND 253402300799 THEN TO_TIMESTAMP(FLOOR((nc.declared_summary #>> ",
    );
    builder.push(path_literal.as_str());
    builder.push(")::NUMERIC)::DOUBLE PRECISION) END WHEN JSONB_TYPEOF(nc.declared_summary #> ");
    builder.push(path_literal.as_str());
    builder.push(") = 'string' AND nc.declared_summary #>> ");
    builder.push(path_literal.as_str());
    builder.push(" ~ '^[0-9]+(\\.[0-9]+)?$' THEN CASE WHEN (nc.declared_summary #>> ");
    builder.push(path_literal.as_str());
    builder.push(")::NUMERIC <= 253402300799 THEN TO_TIMESTAMP(FLOOR((nc.declared_summary #>> ");
    builder.push(path_literal.as_str());
    builder.push(")::NUMERIC)::DOUBLE PRECISION) END WHEN JSONB_TYPEOF(nc.declared_summary #> ");
    builder.push(path_literal.as_str());
    builder.push(") = 'string' AND nc.declared_summary #>> ");
    builder.push(path_literal.as_str());
    // Lifecycle timestamps serialized by PostgreSQL include a UTC offset, and may include fractions.
    builder.push(" ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$' THEN (nc.declared_summary #>> ");
    builder.push(path_literal.as_str());
    builder.push(")::TIMESTAMPTZ ELSE NULL END");
}
