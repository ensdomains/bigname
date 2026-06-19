pub(super) async fn warm_compact_records_route_sql_path(
    state: &AppState,
    max_connections: u32,
) -> Result<()> {
    let Some((namespace, name)) = compact_records_warmup_sample(&state.pool).await? else {
        warn!(
            service = "api",
            "skipped compact records route SQL warm-up because no current name rows exist"
        );
        return Ok(());
    };

    let warm_count = max_connections.clamp(1, 16) as usize;
    let mut tasks = Vec::with_capacity(warm_count);
    for _ in 0..warm_count {
        let state = state.clone();
        let namespace = namespace.clone();
        let name = name.clone();
        tasks.push(tokio::spawn(async move {
            compact_name_records_response_for_name(
                &state,
                &namespace,
                &name,
                compact_records_warmup_query(),
                CompactNameRecordsDefaultMode::Declared,
            )
            .await
        }));
    }

    for task in tasks {
        let result = match task.await {
            Ok(result) => result,
            Err(join_error) => {
                warn!(
                    service = "api",
                    namespace = %namespace,
                    name = %name,
                    error = ?join_error,
                    "skipped compact records route SQL warm-up because a warm-up task failed"
                );
                return Ok(());
            }
        };
        if let Err(error) = result
            && is_skippable_compact_records_warmup_error(&error)
        {
            warn!(
                service = "api",
                namespace = %namespace,
                name = %name,
                status = %error.status,
                code = error.code,
                message = %error.message,
                "skipped compact records route SQL warm-up because the warm-up request failed"
            );
            return Ok(());
        }
    }

    info!(
        service = "api",
        namespace = %namespace,
        name = %name,
        connections_warmed = warm_count,
        "warmed compact records route SQL path"
    );
    Ok(())
}

fn is_skippable_compact_records_warmup_error(error: &ApiError) -> bool {
    error.status == StatusCode::NOT_FOUND
        || error.status.is_client_error()
        || error.status.is_server_error()
}

async fn compact_records_warmup_sample(pool: &PgPool) -> Result<Option<(String, String)>> {
    let inventory_backed_sample = sqlx::query(
        r#"
        WITH inventory_sample AS (
            SELECT record_version_boundary ->> 'logical_name_id' AS logical_name_id
            FROM record_inventory_current
            WHERE record_version_boundary ? 'logical_name_id'
            LIMIT 128
        )
        SELECT nc.namespace, nc.normalized_name
        FROM inventory_sample sample
        JOIN name_current nc
          ON nc.logical_name_id = sample.logical_name_id
        WHERE nc.namespace IN ('ens', 'basenames')
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await
    .context("failed to select inventory-backed compact records warm-up sample")?;

    if let Some(row) = inventory_backed_sample {
        return Ok(Some((
            row.try_get("namespace")
                .context("compact records warm-up sample missing namespace")?,
            row.try_get("normalized_name")
                .context("compact records warm-up sample missing normalized_name")?,
        )));
    }

    sqlx::query(
        r#"
        SELECT namespace, normalized_name
        FROM name_current
        WHERE namespace IN ('ens', 'basenames')
        ORDER BY logical_name_id
        LIMIT 1
        "#,
    )
    .fetch_optional(pool)
    .await
    .context("failed to select fallback compact records warm-up sample")?
    .map(|row| {
        Ok((
            row.try_get("namespace")
                .context("compact records fallback warm-up sample missing namespace")?,
            row.try_get("normalized_name")
                .context("compact records fallback warm-up sample missing normalized_name")?,
        ))
    })
    .transpose()
}

fn compact_records_warmup_query() -> NameRecordsQuery {
    NameRecordsQuery {
        mode: Some("declared".to_owned()),
        known_text_keys: Some("true".to_owned()),
        avatar: Some("true".to_owned()),
        content_hash: Some("true".to_owned()),
        include: Some("resolver_address,known_text_keys,avatar,content_hash,coins".to_owned()),
        meta: Some("none".to_owned()),
        ..NameRecordsQuery::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_records_warmup_skips_not_found_samples() {
        let error = ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: "name missing".to_owned(),
        };

        assert!(is_skippable_compact_records_warmup_error(&error));
    }

    #[test]
    fn compact_records_warmup_treats_internal_errors_as_nonfatal() {
        let error = ApiError::internal_error("projection read failed");

        assert!(is_skippable_compact_records_warmup_error(&error));
    }
}
