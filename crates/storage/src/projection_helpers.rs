use std::fmt::Display;

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};

pub(crate) fn require_json_object(
    value: &Value,
    error: impl FnOnce() -> String,
) -> Result<&Map<String, Value>> {
    value.as_object().with_context(error)
}

pub(crate) fn require_json_array(
    value: &Value,
    error: impl FnOnce() -> String,
) -> Result<&Vec<Value>> {
    value.as_array().with_context(error)
}

pub(crate) fn require_resource_json_object<'a>(
    value: &'a Value,
    field_name: &str,
    table_name: &str,
    resource_id: impl Display,
) -> Result<&'a Map<String, Value>> {
    require_json_object(value, || {
        format!(
            "{table_name} row for resource_id {resource_id} field {field_name} must be a JSON object"
        )
    })
}

pub(crate) fn require_resource_json_array<'a>(
    value: &'a Value,
    field_name: &str,
    table_name: &str,
    resource_id: impl Display,
) -> Result<&'a Vec<Value>> {
    require_json_array(value, || {
        format!(
            "{table_name} row for resource_id {resource_id} field {field_name} must be a JSON array"
        )
    })
}

pub(crate) fn take_json_array(value: Value, error: impl FnOnce() -> String) -> Result<Vec<Value>> {
    match value {
        Value::Array(values) => Ok(values),
        _ => bail!(error()),
    }
}

pub(crate) fn checked_page_size_usize(
    page_size: u64,
    zero_error: &'static str,
    overflow_error: &'static str,
) -> Result<usize> {
    if page_size == 0 {
        bail!(zero_error);
    }
    usize::try_from(page_size).context(overflow_error)
}

pub(crate) fn checked_page_limit_i64(
    page_size: u64,
    zero_error: &'static str,
    overflow_error: &'static str,
) -> Result<i64> {
    if page_size == 0 {
        bail!(zero_error);
    }
    let limit = page_size
        .checked_add(1)
        .filter(|limit| *limit <= i64::MAX as u64)
        .context(overflow_error)?;
    Ok(limit as i64)
}

pub(crate) fn checked_page_limit_i64_from_usize(
    page_size: usize,
    add_overflow_error: &'static str,
    sql_overflow_error: &'static str,
) -> Result<i64> {
    let limit = page_size.checked_add(1).context(add_overflow_error)?;
    i64::try_from(limit).context(sql_overflow_error)
}

pub(crate) fn split_keyset_page<T, C>(
    mut rows: Vec<T>,
    page_size: usize,
    cursor_from_row: impl FnOnce(&T) -> C,
) -> (Vec<T>, Option<C>) {
    let has_next_page = rows.len() > page_size;
    if has_next_page {
        rows.truncate(page_size);
    }
    let next_cursor = has_next_page
        .then(|| rows.last().map(cursor_from_row))
        .flatten();
    (rows, next_cursor)
}
