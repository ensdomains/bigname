pub(super) fn push_name_current_list_cursor_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    sort: NameCurrentListSort,
    order: NameCurrentListOrder,
    cursor: &'a NameCurrentListCursor,
) {
    match sort {
        NameCurrentListSort::Name => {
            // The name order is the normalized name; the cursor's display name only goes back to
            // the client, since a display form can sort apart from its normalized name.
            let NameCurrentListCursorValue::Name(_) = &cursor.sort_value else {
                return;
            };
            let sort_value = cursor.normalized_name.as_str();
            match order {
                NameCurrentListOrder::Asc => {
                    builder.push(
                        r#"
                        AND (
                            normalized_name > "#,
                    );
                    builder.push_bind(sort_value);
                    push_name_tie_after(builder, "normalized_name", sort_value, cursor);
                    builder.push(")");
                }
                NameCurrentListOrder::Desc => {
                    builder.push(
                        r#"
                        AND (
                            normalized_name < "#,
                    );
                    builder.push_bind(sort_value);
                    push_name_tie_after(builder, "normalized_name", sort_value, cursor);
                    builder.push(")");
                }
            }
        }
        NameCurrentListSort::ExpiryDate
        | NameCurrentListSort::RegistrationDate
        | NameCurrentListSort::CreatedAt => {
            let sort_value = match &cursor.sort_value {
                NameCurrentListCursorValue::Timestamp(sort_value) => *sort_value,
                NameCurrentListCursorValue::Name(_) => return,
            };
            let column = timestamp_sort_column(sort);
            let cursor_rank = timestamp_null_rank(sort_value, order);
            builder.push(" AND (");
            builder.push(timestamp_rank_expr(column, order));
            builder.push(" > ");
            builder.push_bind(cursor_rank);
            builder.push(" OR (");
            builder.push(timestamp_rank_expr(column, order));
            builder.push(" = ");
            builder.push_bind(cursor_rank);
            builder.push(" AND ");
            match sort_value {
                None => {
                    push_timestamp_tie_after(builder, column, None, cursor);
                }
                Some(value) => match order {
                    NameCurrentListOrder::Asc => {
                        builder.push("(");
                        builder.push(column);
                        builder.push(" > ");
                        builder.push_bind(value);
                        builder.push(" OR (");
                        builder.push(column);
                        builder.push(" = ");
                        builder.push_bind(value);
                        builder.push(" AND ");
                        push_timestamp_tie_after(builder, column, Some(value), cursor);
                        builder.push("))");
                    }
                    NameCurrentListOrder::Desc => {
                        builder.push("(");
                        builder.push(column);
                        builder.push(" < ");
                        builder.push_bind(value);
                        builder.push(" OR (");
                        builder.push(column);
                        builder.push(" = ");
                        builder.push_bind(value);
                        builder.push(" AND ");
                        push_timestamp_tie_after(builder, column, Some(value), cursor);
                        builder.push("))");
                    }
                },
            }
            builder.push("))");
        }
    }
}

fn push_name_tie_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    sort_column: &str,
    sort_value: &'a str,
    cursor: &'a NameCurrentListCursor,
) {
    builder.push(" OR (");
    builder.push(sort_column);
    builder.push(" = ");
    builder.push_bind(sort_value);
    builder.push(" AND (namespace, normalized_name, namehash) > (");
    builder.push_bind(&cursor.namespace);
    builder.push(", ");
    builder.push_bind(&cursor.normalized_name);
    builder.push(", ");
    builder.push_bind(&cursor.namehash);
    builder.push("))");
}

fn push_timestamp_tie_after<'a>(
    builder: &mut QueryBuilder<'a, Postgres>,
    column: &str,
    value: Option<UnixSeconds>,
    cursor: &'a NameCurrentListCursor,
) {
    match value {
        None => {
            builder.push(column);
            builder.push(" IS NULL AND ");
        }
        Some(value) => {
            builder.push(column);
            builder.push(" = ");
            builder.push_bind(value);
            builder.push(" AND ");
        }
    }
    builder.push("(namespace, normalized_name, namehash) > (");
    builder.push_bind(&cursor.namespace);
    builder.push(", ");
    builder.push_bind(&cursor.normalized_name);
    builder.push(", ");
    builder.push_bind(&cursor.namehash);
    builder.push(")");
}

pub(super) fn push_name_current_list_order(
    builder: &mut QueryBuilder<'_, Postgres>,
    sort: NameCurrentListSort,
    order: NameCurrentListOrder,
) {
    match sort {
        NameCurrentListSort::Name => {
            builder.push(" ORDER BY normalized_name ");
            builder.push(match order {
                NameCurrentListOrder::Asc => "ASC",
                NameCurrentListOrder::Desc => "DESC",
            });
            builder.push(", namespace ASC, normalized_name ASC, namehash ASC");
        }
        NameCurrentListSort::ExpiryDate
        | NameCurrentListSort::RegistrationDate
        | NameCurrentListSort::CreatedAt => {
            let column = timestamp_sort_column(sort);
            builder.push(" ORDER BY ");
            builder.push(timestamp_rank_expr(column, order));
            builder.push(" ASC, ");
            builder.push(column);
            builder.push(" ");
            builder.push(match order {
                NameCurrentListOrder::Asc => "ASC",
                NameCurrentListOrder::Desc => "DESC",
            });
            builder.push(", namespace ASC, normalized_name ASC, namehash ASC");
        }
    }
}

fn timestamp_sort_column(sort: NameCurrentListSort) -> &'static str {
    match sort {
        NameCurrentListSort::Name => "normalized_name",
        NameCurrentListSort::ExpiryDate => "expiry_date",
        NameCurrentListSort::RegistrationDate => "EXTRACT(EPOCH FROM registration_date)",
        NameCurrentListSort::CreatedAt => "EXTRACT(EPOCH FROM created_at)",
    }
}

fn timestamp_rank_expr(column: &str, order: NameCurrentListOrder) -> String {
    match order {
        NameCurrentListOrder::Asc => {
            format!("CASE WHEN {column} IS NULL THEN 1 ELSE 0 END")
        }
        NameCurrentListOrder::Desc => {
            format!("CASE WHEN {column} IS NULL THEN 0 ELSE 1 END")
        }
    }
}

fn timestamp_null_rank(value: Option<UnixSeconds>, order: NameCurrentListOrder) -> i32 {
    match (value.is_none(), order) {
        (true, NameCurrentListOrder::Asc) => 1,
        (false, NameCurrentListOrder::Asc) => 0,
        (true, NameCurrentListOrder::Desc) => 0,
        (false, NameCurrentListOrder::Desc) => 1,
    }
}

pub(super) fn decode_name_current_list_row(row: PgRow) -> Result<NameCurrentListRow> {
    let labelhash = crate::sql_row::get(&row, "labelhash")?;
    let token_id = crate::sql_row::get(&row, "token_id")?;
    let owner = crate::sql_row::get(&row, "owner")?;
    let registrant = crate::sql_row::get(&row, "registrant")?;
    let created_at = crate::sql_row::get(&row, "created_at")?;
    let registration_date = crate::sql_row::get(&row, "registration_date")?;
    let expiry_date = crate::sql_row::get(&row, "expiry_date")?;
    let resolver_address = crate::sql_row::get(&row, "resolver_address")?;
    let row = decode_name_current_row(row)?;

    Ok(NameCurrentListRow {
        row,
        labelhash,
        token_id,
        owner,
        registrant,
        created_at,
        registration_date,
        expiry_date,
        resolver_address,
    })
}

pub fn name_current_list_cursor_from_row(
    row: &NameCurrentListRow,
    sort: NameCurrentListSort,
) -> NameCurrentListCursor {
    NameCurrentListCursor {
        sort_value: match sort {
            NameCurrentListSort::Name => {
                NameCurrentListCursorValue::Name(row.row.canonical_display_name.clone())
            }
            NameCurrentListSort::ExpiryDate => {
                NameCurrentListCursorValue::Timestamp(row.expiry_date)
            }
            NameCurrentListSort::RegistrationDate => {
                NameCurrentListCursorValue::Timestamp(row.registration_date.map(Into::into))
            }
            NameCurrentListSort::CreatedAt => {
                NameCurrentListCursorValue::Timestamp(row.created_at.map(Into::into))
            }
        },
        namespace: row.row.namespace.clone(),
        normalized_name: row.row.normalized_name.clone(),
        namehash: row.row.namehash.clone(),
    }
}

pub(crate) fn escape_like_pattern(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('%', r"\%")
        .replace('_', r"\_")
}
