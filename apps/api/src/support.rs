fn parse_history_scope(scope: Option<&str>) -> ApiResult<HistoryScope> {
    match scope.unwrap_or("both") {
        "surface" => Ok(HistoryScope::Surface),
        "resource" => Ok(HistoryScope::Resource),
        "both" => Ok(HistoryScope::Both),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "scope must be one of: surface, resource, both".to_owned(),
        }),
    }
}

fn parse_resolution_mode(mode: Option<&str>) -> ApiResult<ResolutionMode> {
    match mode.unwrap_or("declared") {
        "declared" => Ok(ResolutionMode::Declared),
        "verified" => Ok(ResolutionMode::Verified),
        "both" => Ok(ResolutionMode::Both),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "mode must be one of: declared, verified, both".to_owned(),
        }),
    }
}

fn parse_primary_name_address(address: &str) -> ApiResult<String> {
    let normalized = normalize_address(address.trim());
    let is_valid = normalized.len() == 42
        && normalized.starts_with("0x")
        && normalized
            .as_bytes()
            .iter()
            .skip(2)
            .all(|byte| byte.is_ascii_hexdigit());

    if is_valid {
        Ok(normalized)
    } else {
        Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "address must be a 0x-prefixed 20-byte hex string".to_owned(),
        })
    }
}

fn parse_primary_name_namespace(namespace: Option<&str>) -> ApiResult<String> {
    let Some(namespace) = namespace.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "namespace is required".to_owned(),
        });
    };

    ensure_public_namespace(namespace)?;
    Ok(namespace.to_owned())
}

fn parse_primary_name_coin_type(coin_type: Option<&str>) -> ApiResult<String> {
    let Some(coin_type) = coin_type.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "coin_type is required".to_owned(),
        });
    };

    if coin_type.as_bytes().iter().all(u8::is_ascii_digit) {
        Ok(coin_type.to_owned())
    } else {
        Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "coin_type must contain only decimal digits".to_owned(),
        })
    }
}

fn parse_resolution_record_keys(
    records: Option<&str>,
    mode: ResolutionMode,
) -> ApiResult<Vec<ResolutionRecordKey>> {
    let Some(records) = records.map(str::trim).filter(|value| !value.is_empty()) else {
        return if mode.includes_verified() {
            Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: "records is required when mode is verified or both".to_owned(),
            })
        } else {
            Ok(Vec::new())
        };
    };

    let mut parsed = Vec::new();
    let mut deduped = BTreeSet::new();

    for record_key in records.split(',').map(str::trim) {
        let Some(record) = parse_resolution_record_key(record_key) else {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: "records must contain only valid record selectors".to_owned(),
            });
        };

        if mode.includes_verified() && !deduped.insert(record.record_key.clone()) {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: "records must not contain duplicate selectors".to_owned(),
            });
        }

        parsed.push(record);
    }

    Ok(parsed)
}

fn parse_resolution_record_key(record_key: &str) -> Option<ResolutionRecordKey> {
    if record_key.is_empty()
        || record_key
            .chars()
            .any(|character| character.is_ascii_whitespace() || character == ',')
    {
        return None;
    }

    let is_valid_family = |family: &str| {
        !family.is_empty()
            && family.chars().all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
            })
    };

    match record_key.split_once(':') {
        None if is_valid_family(record_key) => Some(ResolutionRecordKey {
            record_key: record_key.to_owned(),
            record_family: record_key.to_owned(),
            selector_key: None,
        }),
        Some((family, selector)) if is_valid_family(family) && !selector.is_empty() => {
            Some(ResolutionRecordKey {
                record_key: record_key.to_owned(),
                record_family: family.to_owned(),
                selector_key: Some(selector.to_owned()),
            })
        }
        _ => None,
    }
}

fn parse_permissions_subject(subject: Option<&str>) -> Option<String> {
    subject
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn parse_permission_scope_filter(scope: Option<&str>) -> ApiResult<Option<PermissionScope>> {
    let Some(scope) = scope.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    if scope == "root" {
        return Ok(Some(PermissionScope::Root));
    }
    if scope == "registry" {
        return Ok(Some(PermissionScope::Registry));
    }
    if scope == "resource" {
        return Ok(Some(PermissionScope::Resource));
    }

    let mut parts = scope.split(':');
    let kind = parts.next().unwrap_or_default();
    let first = parts.next();
    let second = parts.next();
    let extra = parts.next();

    let parsed = match (kind, first, second, extra) {
        ("resolver", Some(chain_id), Some(resolver_address), None) => {
            Some(PermissionScope::Resolver {
                chain_id: chain_id.to_owned(),
                resolver_address: resolver_address.to_ascii_lowercase(),
            })
        }
        ("record_manager", Some(chain_id), Some(manager_address), None) => {
            Some(PermissionScope::RecordManager {
                chain_id: chain_id.to_owned(),
                manager_address: manager_address.to_ascii_lowercase(),
            })
        }
        ("migration_derived", Some(predecessor_resource_id), None, None) => {
            Some(PermissionScope::MigrationDerived {
                predecessor_resource_id: Uuid::parse_str(predecessor_resource_id).map_err(
                    |_| ApiError {
                        status: StatusCode::BAD_REQUEST,
                        code: "invalid_input",
                        message: "scope must use a valid permissions scope filter".to_owned(),
                    },
                )?,
            })
        }
        ("transport_derived", Some(transport), None, None) => {
            Some(PermissionScope::TransportDerived {
                transport: transport.to_owned(),
            })
        }
        _ => None,
    };

    parsed
        .ok_or(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "scope must use a valid permissions scope filter".to_owned(),
        })
        .map(Some)
}

fn parse_pagination(cursor: Option<&str>, page_size: Option<u64>) -> ApiResult<PaginationRequest> {
    let cursor = cursor
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let active = cursor.is_some() || page_size.is_some();

    let page_size = match page_size {
        None if !active => DEFAULT_PAGE_SIZE,
        None => DEFAULT_PAGE_SIZE,
        Some(value) if !(1..=MAX_PAGE_SIZE).contains(&value) => {
            return Err(ApiError {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_input",
                message: format!("page_size must be between 1 and {MAX_PAGE_SIZE}"),
            });
        }
        Some(value) => value,
    };

    Ok(PaginationRequest {
        active,
        cursor,
        page_size,
    })
}

fn paginate_window<T>(
    items: &[T],
    request: &PaginationRequest,
    unpaged_page_size: u64,
    spec: &CursorSpec,
    item_cursor_fields: impl Fn(&T) -> BTreeMap<String, String>,
) -> ApiResult<PaginationWindow> {
    if !request.active {
        return Ok(PaginationWindow {
            start: 0,
            end: items.len(),
            page: HistoryPageResponse {
                cursor: None,
                next_cursor: None,
                page_size: unpaged_page_size,
                sort: spec.sort.to_owned(),
            },
        });
    }

    let start = match request.cursor.as_deref() {
        None => 0,
        Some(cursor) => {
            let decoded = decode_cursor(cursor)?;
            validate_cursor(spec, &decoded)?;
            items
                .iter()
                .position(|item| item_cursor_fields(item) == decoded.item)
                .map(|index| index + 1)
                .ok_or_else(invalid_cursor_error)?
        }
    };
    let end = (start + request.page_size as usize).min(items.len());
    let next_cursor = if end < items.len() {
        Some(encode_cursor(
            &spec.envelope(item_cursor_fields(&items[end - 1])),
        ))
    } else {
        None
    };

    Ok(PaginationWindow {
        start,
        end,
        page: HistoryPageResponse {
            cursor: request.cursor.clone(),
            next_cursor,
            page_size: request.page_size,
            sort: spec.sort.to_owned(),
        },
    })
}

fn storage_page_size(request: &PaginationRequest) -> u64 {
    if request.active {
        request.page_size
    } else {
        (i64::MAX as u64) - 1
    }
}

fn page_response_from_storage_cursor(
    request: &PaginationRequest,
    unpaged_page_size: u64,
    spec: &CursorSpec,
    next_cursor_item: Option<BTreeMap<String, String>>,
) -> HistoryPageResponse {
    if !request.active {
        return HistoryPageResponse {
            cursor: None,
            next_cursor: None,
            page_size: unpaged_page_size,
            sort: spec.sort.to_owned(),
        };
    }

    HistoryPageResponse {
        cursor: request.cursor.clone(),
        next_cursor: next_cursor_item.map(|item| encode_cursor(&spec.envelope(item))),
        page_size: request.page_size,
        sort: spec.sort.to_owned(),
    }
}

fn address_names_storage_cursor(
    request: &PaginationRequest,
    spec: &CursorSpec,
) -> ApiResult<Option<bigname_storage::AddressNamesCurrentCursor>> {
    let Some(item) = decoded_cursor_item(request, spec)? else {
        return Ok(None);
    };

    require_cursor_item_fields(
        &item,
        &["canonical_display_name", "logical_name_id", "resource_id"],
    )?;
    let resource_id = Uuid::parse_str(required_cursor_item_field(&item, "resource_id")?)
        .map_err(|_| invalid_cursor_error())?;

    Ok(Some(bigname_storage::AddressNamesCurrentCursor {
        canonical_display_name: required_cursor_item_field(&item, "canonical_display_name")?
            .to_owned(),
        logical_name_id: required_cursor_item_field(&item, "logical_name_id")?.to_owned(),
        resource_id,
    }))
}

fn children_storage_cursor(
    request: &PaginationRequest,
    spec: &CursorSpec,
) -> ApiResult<Option<bigname_storage::ChildrenCurrentKeysetCursor>> {
    let Some(item) = decoded_cursor_item(request, spec)? else {
        return Ok(None);
    };

    require_cursor_item_fields(&item, &["canonical_display_name", "child_logical_name_id"])?;
    Ok(Some(bigname_storage::ChildrenCurrentKeysetCursor {
        canonical_display_name: required_cursor_item_field(&item, "canonical_display_name")?
            .to_owned(),
        child_logical_name_id: required_cursor_item_field(&item, "child_logical_name_id")?
            .to_owned(),
    }))
}

fn permissions_storage_cursor(
    request: &PaginationRequest,
    spec: &CursorSpec,
) -> ApiResult<Option<bigname_storage::PermissionsCurrentKeysetCursor>> {
    let Some(item) = decoded_cursor_item(request, spec)? else {
        return Ok(None);
    };

    require_cursor_item_fields(&item, &["subject", "scope"])?;
    Ok(Some(bigname_storage::PermissionsCurrentKeysetCursor {
        subject: required_cursor_item_field(&item, "subject")?.to_owned(),
        scope: required_cursor_item_field(&item, "scope")?.to_owned(),
    }))
}

fn address_names_cursor_item(
    cursor: &bigname_storage::AddressNamesCurrentCursor,
) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert(
        "canonical_display_name".to_owned(),
        cursor.canonical_display_name.clone(),
    );
    item.insert("logical_name_id".to_owned(), cursor.logical_name_id.clone());
    item.insert("resource_id".to_owned(), cursor.resource_id.to_string());
    item
}

fn children_cursor_item(
    cursor: &bigname_storage::ChildrenCurrentKeysetCursor,
) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert(
        "canonical_display_name".to_owned(),
        cursor.canonical_display_name.clone(),
    );
    item.insert(
        "child_logical_name_id".to_owned(),
        cursor.child_logical_name_id.clone(),
    );
    item
}

fn permissions_cursor_item(
    cursor: &bigname_storage::PermissionsCurrentKeysetCursor,
) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert("subject".to_owned(), cursor.subject.clone());
    item.insert("scope".to_owned(), cursor.scope.clone());
    item
}

async fn ensure_children_cursor_exists(
    pool: &PgPool,
    parent_logical_name_id: &str,
    cursor: &bigname_storage::ChildrenCurrentKeysetCursor,
) -> ApiResult<()> {
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM children_current cc
            JOIN name_surfaces parent
              ON parent.logical_name_id = cc.parent_logical_name_id
            JOIN name_surfaces child
              ON child.logical_name_id = cc.child_logical_name_id
            WHERE cc.parent_logical_name_id = $1
              AND cc.surface_class = 'declared'
              AND cc.canonical_display_name = $2
              AND cc.child_logical_name_id = $3
              AND parent.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
              AND child.canonicality_state IN (
                    'canonical'::canonicality_state,
                    'safe'::canonicality_state,
                    'finalized'::canonicality_state
              )
        )
        "#,
    )
    .bind(parent_logical_name_id)
    .bind(&cursor.canonical_display_name)
    .bind(&cursor.child_logical_name_id)
    .fetch_one(pool)
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            parent_logical_name_id = %parent_logical_name_id,
            cursor = ?cursor,
            error = ?load_error,
            "failed to validate children_current pagination cursor"
        );
        ApiError::internal_error(format!(
            "failed to load child collection for logical name {parent_logical_name_id}"
        ))
    })?;

    if exists {
        Ok(())
    } else {
        Err(invalid_cursor_error())
    }
}

async fn ensure_permissions_cursor_exists(
    pool: &PgPool,
    resource_id: Uuid,
    subject: Option<&str>,
    scope: Option<&PermissionScope>,
    cursor: &bigname_storage::PermissionsCurrentKeysetCursor,
) -> ApiResult<()> {
    let scope_storage_key = scope.map(PermissionScope::storage_key);
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM permissions_current
            WHERE resource_id = $1
              AND ($2::TEXT IS NULL OR subject = $2)
              AND ($3::TEXT IS NULL OR scope = $3)
              AND subject = $4
              AND scope = $5
        )
        "#,
    )
    .bind(resource_id)
    .bind(subject)
    .bind(scope_storage_key.as_deref())
    .bind(&cursor.subject)
    .bind(&cursor.scope)
    .fetch_one(pool)
    .await
    .map_err(|load_error| {
        error!(
            service = "api",
            resource_id = %resource_id,
            subject = ?subject,
            scope = ?scope_storage_key,
            cursor = ?cursor,
            error = ?load_error,
            "failed to validate permissions_current pagination cursor"
        );
        ApiError::internal_error(format!(
            "failed to load permissions for resource {resource_id}"
        ))
    })?;

    if exists {
        Ok(())
    } else {
        Err(invalid_cursor_error())
    }
}

fn decoded_cursor_item(
    request: &PaginationRequest,
    spec: &CursorSpec,
) -> ApiResult<Option<BTreeMap<String, String>>> {
    let Some(cursor) = request.cursor.as_deref() else {
        return Ok(None);
    };

    let decoded = decode_cursor(cursor)?;
    validate_cursor(spec, &decoded)?;
    Ok(Some(decoded.item))
}

fn required_cursor_item_field<'a>(
    item: &'a BTreeMap<String, String>,
    field: &str,
) -> ApiResult<&'a str> {
    item.get(field)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(invalid_cursor_error)
}

fn require_cursor_item_fields(
    item: &BTreeMap<String, String>,
    expected_fields: &[&str],
) -> ApiResult<()> {
    if item.len() != expected_fields.len()
        || expected_fields
            .iter()
            .any(|field| !item.contains_key(*field))
    {
        return Err(invalid_cursor_error());
    }

    Ok(())
}

fn invalid_cursor_error() -> ApiError {
    ApiError {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_input",
        message: "cursor must be a valid pagination cursor".to_owned(),
    }
}

fn validate_cursor(spec: &CursorSpec, cursor: &CursorEnvelope) -> ApiResult<()> {
    if cursor.version != CURSOR_VERSION
        || cursor.route != spec.route
        || cursor.anchor != spec.anchor
        || cursor.sort != spec.sort
        || cursor.filters != spec.filters
    {
        return Err(invalid_cursor_error());
    }

    Ok(())
}

fn decode_cursor(cursor: &str) -> ApiResult<CursorEnvelope> {
    let decoded = decode_hex(cursor).ok_or_else(invalid_cursor_error)?;
    serde_json::from_slice(&decoded).map_err(|_| invalid_cursor_error())
}

fn encode_cursor(cursor: &CursorEnvelope) -> String {
    encode_hex(&serde_json::to_vec(cursor).expect("cursor envelope must serialize for pagination"))
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("hex encoding must write into string");
    }
    encoded
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }

    let mut decoded = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let high = decode_hex_nibble(bytes[index])?;
        let low = decode_hex_nibble(bytes[index + 1])?;
        decoded.push((high << 4) | low);
        index += 2;
    }
    Some(decoded)
}

fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn history_cursor_fields(row: &HistoryEvent) -> BTreeMap<String, String> {
    let mut item = BTreeMap::new();
    item.insert(
        "normalized_event_id".to_owned(),
        row.normalized_event_id.to_string(),
    );
    item.insert("event_identity".to_owned(), row.event_identity.clone());
    item
}

fn parse_children_query(query: &ChildrenQuery) -> ApiResult<bool> {
    parse_children_surface_classes(query.surface_classes.as_deref())?;
    parse_children_include_counts(query.include.as_deref())
}

fn parse_address_names_namespace(namespace: Option<&str>) -> ApiResult<Option<String>> {
    let Some(namespace) = namespace.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    if PUBLIC_NAMESPACES.contains(&namespace) {
        Ok(Some(namespace.to_owned()))
    } else {
        Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "namespace must be one of: ens, basenames".to_owned(),
        })
    }
}

fn parse_address_name_relation(relation: Option<&str>) -> ApiResult<Option<AddressNameRelation>> {
    match relation.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some("registrant") => Ok(Some(AddressNameRelation::Registrant)),
        Some("token_holder") => Ok(Some(AddressNameRelation::TokenHolder)),
        Some("effective_controller") => Ok(Some(AddressNameRelation::EffectiveController)),
        Some(_) => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "relation must be one of: registrant, token_holder, effective_controller"
                .to_owned(),
        }),
    }
}

fn parse_address_names_dedupe_by(dedupe_by: Option<&str>) -> ApiResult<AddressNamesCurrentDedupe> {
    match dedupe_by.unwrap_or("surface") {
        "surface" => Ok(AddressNamesCurrentDedupe::Surface),
        "resource" => Ok(AddressNamesCurrentDedupe::Resource),
        _ => Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_input",
            message: "dedupe_by must be one of: surface, resource".to_owned(),
        }),
    }
}

fn parse_address_names_include(include: Option<&str>) -> ApiResult<AddressNamesIncludeOptions> {
    let mut options = AddressNamesIncludeOptions::default();

    for value in include
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match value {
            "role_summary" => options.role_summary = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "include must contain only role_summary".to_owned(),
                });
            }
        }
    }

    Ok(options)
}

fn parse_children_surface_classes(surface_classes: Option<&str>) -> ApiResult<()> {
    let mut requested_non_declared = false;

    for value in surface_classes
        .unwrap_or("declared")
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match value {
            "declared" => {}
            "linked" | "alias" | "wildcard" => requested_non_declared = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message:
                        "surface_classes must contain only declared, linked, alias, or wildcard"
                            .to_owned(),
                });
            }
        }
    }

    if requested_non_declared {
        return Err(ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "unsupported",
            message: "surface_classes other than declared are not yet supported".to_owned(),
        });
    }

    Ok(())
}

fn parse_children_include_counts(include: Option<&str>) -> ApiResult<bool> {
    let mut include_counts = false;

    for value in include
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match value {
            "counts" => include_counts = true,
            _ => {
                return Err(ApiError {
                    status: StatusCode::BAD_REQUEST,
                    code: "invalid_input",
                    message: "include must contain only counts".to_owned(),
                });
            }
        }
    }

    Ok(include_counts)
}

fn normalize_address(address: &str) -> String {
    address.to_ascii_lowercase()
}

async fn load_primary_name_lookup_state(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
    mode: ResolutionMode,
) -> ApiResult<PrimaryNameLookupState> {
    match load_primary_name_current_snapshot(pool, address, namespace, coin_type).await {
        Ok(Some(snapshot)) => Ok(PrimaryNameLookupState {
            tuple_state: PrimaryNameTupleState::TuplePresent(snapshot.row),
            normalized_claim_name: mode
                .includes_declared()
                .then_some(snapshot.normalized_claim_name)
                .flatten(),
            persisted_verified: if mode.includes_verified() {
                load_persisted_primary_name_verified_readback(pool, address, namespace, coin_type)
                    .await?
            } else {
                None
            },
        }),
        Ok(None) => Ok(PrimaryNameLookupState {
            tuple_state: PrimaryNameTupleState::TupleMissing,
            normalized_claim_name: None,
            persisted_verified: None,
        }),
        Err(load_error) if primary_name_projection_unavailable(&load_error) => {
            Ok(PrimaryNameLookupState {
                tuple_state: PrimaryNameTupleState::ProjectionUnavailable,
                normalized_claim_name: None,
                persisted_verified: None,
            })
        }
        Err(load_error) => {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to load primary-name tuple state"
            );
            Err(ApiError::internal_error(format!(
                "failed to load primary-name tuple for address {address}"
            )))
        }
    }
}

fn primary_name_projection_unavailable(load_error: &anyhow::Error) -> bool {
    load_error.chain().any(|cause| {
        cause
            .downcast_ref::<sqlx::Error>()
            .is_some_and(|sqlx_error| {
                matches!(
                    sqlx_error,
                    sqlx::Error::Database(error) if error.code().as_deref() == Some("42P01")
                )
            })
    })
}

async fn load_persisted_primary_name_verified_readback(
    pool: &PgPool,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<Option<PersistedPrimaryNameVerifiedReadback>> {
    let request_key = primary_name_verified_request_key(namespace, address, coin_type);
    let row = sqlx::query(
        r#"
        SELECT
            request_key,
            requested_chain_positions,
            manifest_versions,
            topology_version_boundary,
            record_version_boundary,
            execution_trace_id,
            request_type,
            namespace,
            outcome_payload,
            failure_payload,
            finished_at
        FROM execution_cache_outcomes
        WHERE request_type = $1
          AND namespace = $2
          AND request_key = $3
        ORDER BY finished_at DESC, execution_trace_id DESC
        LIMIT 1
        "#,
    )
    .bind(VERIFIED_PRIMARY_NAME_REQUEST_TYPE)
    .bind(namespace)
    .bind(&request_key)
    .fetch_optional(pool)
    .await;

    let Some(row) = (match row {
        Ok(row) => row,
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42P01") => {
            return Ok(None);
        }
        Err(load_error) => {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to load persisted verified primary-name outcome"
            );
            return Err(ApiError::internal_error(format!(
                "failed to load persisted verified primary-name outcome for address {address}"
            )));
        }
    }) else {
        return Ok(None);
    };

    let outcome = ExecutionOutcome {
        cache_key: ExecutionCacheKey {
            request_key: row.try_get("request_key").map_err(|load_error| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    error = ?load_error,
                    "failed to decode persisted verified primary-name request_key"
                );
                ApiError::internal_error(format!(
                    "failed to decode persisted verified primary-name outcome for address {address}"
                ))
            })?,
            requested_chain_positions: row
                .try_get("requested_chain_positions")
                .map_err(|load_error| {
                    error!(
                        service = "api",
                        address = %address,
                        namespace = %namespace,
                        coin_type = %coin_type,
                        error = ?load_error,
                        "failed to decode persisted verified primary-name requested_chain_positions"
                    );
                    ApiError::internal_error(format!(
                        "failed to decode persisted verified primary-name outcome for address {address}"
                    ))
                })?,
            manifest_versions: row.try_get("manifest_versions").map_err(|load_error| {
                error!(
                    service = "api",
                    address = %address,
                    namespace = %namespace,
                    coin_type = %coin_type,
                    error = ?load_error,
                    "failed to decode persisted verified primary-name manifest_versions"
                );
                ApiError::internal_error(format!(
                    "failed to decode persisted verified primary-name outcome for address {address}"
                ))
            })?,
            topology_version_boundary: row
                .try_get("topology_version_boundary")
                .map_err(|load_error| {
                    error!(
                        service = "api",
                        address = %address,
                        namespace = %namespace,
                        coin_type = %coin_type,
                        error = ?load_error,
                        "failed to decode persisted verified primary-name topology_version_boundary"
                    );
                    ApiError::internal_error(format!(
                        "failed to decode persisted verified primary-name outcome for address {address}"
                    ))
                })?,
            record_version_boundary: row
                .try_get("record_version_boundary")
                .map_err(|load_error| {
                    error!(
                        service = "api",
                        address = %address,
                        namespace = %namespace,
                        coin_type = %coin_type,
                        error = ?load_error,
                        "failed to decode persisted verified primary-name record_version_boundary"
                    );
                    ApiError::internal_error(format!(
                        "failed to decode persisted verified primary-name outcome for address {address}"
                    ))
                })?,
        },
        execution_trace_id: row.try_get("execution_trace_id").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name execution_trace_id"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
        request_type: row.try_get("request_type").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name request_type"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
        namespace: row.try_get("namespace").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name namespace"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
        outcome_payload: row.try_get("outcome_payload").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name outcome_payload"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
        failure_payload: row.try_get("failure_payload").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name failure_payload"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
        finished_at: row.try_get("finished_at").map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                error = ?load_error,
                "failed to decode persisted verified primary-name finished_at"
            );
            ApiError::internal_error(format!(
                "failed to decode persisted verified primary-name outcome for address {address}"
            ))
        })?,
    };

    if outcome.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE
        || outcome.namespace != namespace
        || outcome.cache_key.request_key != request_key
    {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            request_type = %outcome.request_type,
            cached_namespace = %outcome.namespace,
            cached_request_key = %outcome.cache_key.request_key,
            "persisted verified primary-name outcome identity mismatch"
        );
        return Err(ApiError::internal_error(format!(
            "persisted verified primary-name outcome identity mismatch for address {address}"
        )));
    }

    let trace = load_execution_trace(pool, outcome.execution_trace_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %outcome.execution_trace_id,
                error = ?load_error,
                "failed to load persisted verified primary-name trace"
            );
            ApiError::internal_error(format!(
                "failed to load persisted verified primary-name trace for address {address}"
            ))
        })?
        .ok_or_else(|| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %outcome.execution_trace_id,
                "persisted verified primary-name trace missing"
            );
            ApiError::internal_error(format!(
                "persisted verified primary-name trace missing for address {address}"
            ))
        })?;

    let verified_primary_name =
        persisted_verified_primary_name_section(&trace, &outcome, address, namespace, coin_type)?;
    let provenance =
        primary_name_verified_readback_provenance(&trace, &outcome, address, namespace, coin_type)?;

    Ok(Some(PersistedPrimaryNameVerifiedReadback {
        verified_primary_name,
        provenance,
        finished_at: outcome.finished_at,
    }))
}

fn primary_name_verified_request_key(namespace: &str, address: &str, coin_type: &str) -> String {
    format!("{namespace}:{address}:{coin_type}")
}

fn manifest_versions_contain_source_family(
    manifest_versions: &JsonValue,
    expected_source_family: &str,
    context: &str,
) -> Result<bool> {
    let manifest_versions = manifest_versions
        .as_array()
        .with_context(|| format!("{context} must be a JSON array"))?;

    for (index, manifest_version) in manifest_versions.iter().enumerate() {
        let manifest_version = manifest_version
            .as_object()
            .with_context(|| format!("{context}[{index}] must be a JSON object"))?;
        if manifest_version
            .get("source_family")
            .and_then(JsonValue::as_str)
            .is_some_and(|source_family| source_family == expected_source_family)
        {
            return Ok(true);
        }
    }

    Ok(false)
}

fn ensure_persisted_primary_name_execution_source_family(
    outcome: &ExecutionOutcome,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<()> {
    let expected_source_family = match namespace {
        "ens" => "ens_execution",
        "basenames" => "basenames_execution",
        _ => {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                "persisted verified primary-name namespace unsupported for execution source-family check"
            );
            return Err(ApiError::internal_error(format!(
                "persisted verified primary-name provenance mismatch for address {address}"
            )));
        }
    };

    let includes_expected_source_family = manifest_versions_contain_source_family(
        &outcome.cache_key.manifest_versions,
        expected_source_family,
        "persisted verified primary-name cache_key.manifest_versions",
    )
    .map_err(|load_error| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %outcome.execution_trace_id,
            error = ?load_error,
            manifest_versions = ?outcome.cache_key.manifest_versions,
            "persisted verified primary-name manifest_versions malformed"
        );
        ApiError::internal_error(format!(
            "persisted verified primary-name provenance mismatch for address {address}"
        ))
    })?;

    if !includes_expected_source_family {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %outcome.execution_trace_id,
            expected_source_family = %expected_source_family,
            manifest_versions = ?outcome.cache_key.manifest_versions,
            "persisted verified primary-name execution source-family mismatch"
        );
        return Err(ApiError::internal_error(format!(
            "persisted verified primary-name provenance mismatch for address {address}"
        )));
    }

    Ok(())
}

fn persisted_verified_primary_name_section(
    trace: &ExecutionTrace,
    outcome: &ExecutionOutcome,
    address: &str,
    namespace: &str,
    coin_type: &str,
) -> ApiResult<JsonValue> {
    let request_key = primary_name_verified_request_key(namespace, address, coin_type);
    if trace.request_type != VERIFIED_PRIMARY_NAME_REQUEST_TYPE
        || trace.namespace != namespace
        || trace.request_key != request_key
    {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            request_type = %trace.request_type,
            trace_namespace = %trace.namespace,
            trace_request_key = %trace.request_key,
            "persisted verified primary-name trace identity mismatch"
        );
        return Err(ApiError::internal_error(format!(
            "persisted verified primary-name trace identity mismatch for address {address}"
        )));
    }

    let trace_metadata = trace.request_metadata.as_object().ok_or_else(|| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %trace.execution_trace_id,
            "persisted verified primary-name trace metadata missing"
        );
        ApiError::internal_error(format!(
            "persisted verified primary-name trace metadata missing for address {address}"
        ))
    })?;

    let metadata_address = trace_metadata
        .get("normalized_address")
        .and_then(JsonValue::as_str);
    let metadata_namespace = trace_metadata.get("namespace").and_then(JsonValue::as_str);
    let metadata_coin_type = trace_metadata.get("coin_type").and_then(JsonValue::as_str);
    if metadata_address != Some(address)
        || metadata_namespace != Some(namespace)
        || metadata_coin_type != Some(coin_type)
    {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %trace.execution_trace_id,
            metadata = ?trace.request_metadata,
            "persisted verified primary-name trace tuple mismatch"
        );
        return Err(ApiError::internal_error(format!(
            "persisted verified primary-name trace tuple mismatch for address {address}"
        )));
    }

    ensure_persisted_primary_name_execution_source_family(outcome, address, namespace, coin_type)?;

    let verified_primary_name = extract_persisted_verified_primary_name_section(
        outcome.outcome_payload.as_ref(),
        "persisted verified primary-name outcome_payload",
        namespace,
    )
    .map_err(|load_error| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %trace.execution_trace_id,
            error = ?load_error,
            "persisted verified primary-name outcome section invalid"
        );
        ApiError::internal_error(format!(
            "persisted verified primary-name payload mismatch for address {address}"
        ))
    })?
    .ok_or_else(|| {
        error!(
            service = "api",
            address = %address,
            namespace = %namespace,
            coin_type = %coin_type,
            execution_trace_id = %trace.execution_trace_id,
            "persisted verified primary-name outcome section missing"
        );
        ApiError::internal_error(format!(
            "persisted verified primary-name outcome missing for address {address}"
        ))
    })?;

    let status = verified_primary_name
        .get("status")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                "persisted verified primary-name status missing"
            );
            ApiError::internal_error(format!(
                "persisted verified primary-name status missing for address {address}"
            ))
        })?;

    if status == "execution_failed" {
        if trace.final_payload.is_some()
            || !outcome
                .failure_payload
                .as_ref()
                .is_some_and(JsonValue::is_object)
            || !trace
                .failure_payload
                .as_ref()
                .is_some_and(JsonValue::is_object)
        {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                "persisted verified primary-name execution_failed payload mismatch"
            );
            return Err(ApiError::internal_error(format!(
                "persisted verified primary-name payload mismatch for address {address}"
            )));
        }
    } else {
        let trace_verified_primary_name = extract_persisted_verified_primary_name_section(
            trace.final_payload.as_ref(),
            "persisted verified primary-name trace.final_payload",
            namespace,
        )
        .map_err(|load_error| {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                error = ?load_error,
                "persisted verified primary-name trace final payload invalid"
            );
            ApiError::internal_error(format!(
                "persisted verified primary-name payload mismatch for address {address}"
            ))
        })?;
        if trace.failure_payload.is_some()
            || outcome.failure_payload.is_some()
            || trace_verified_primary_name.as_ref() != Some(&verified_primary_name)
        {
            error!(
                service = "api",
                address = %address,
                namespace = %namespace,
                coin_type = %coin_type,
                execution_trace_id = %trace.execution_trace_id,
                "persisted verified primary-name final payload mismatch"
            );
            return Err(ApiError::internal_error(format!(
                "persisted verified primary-name payload mismatch for address {address}"
            )));
        }
    }

    Ok(verified_primary_name)
}

fn extract_persisted_verified_primary_name_section(
    payload: Option<&JsonValue>,
    context: &str,
    namespace: &str,
) -> Result<Option<JsonValue>> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    let payload = payload
        .as_object()
        .with_context(|| format!("{context} must be a JSON object"))?;
    ensure_allowed_json_fields(payload, &["verified_primary_name"], context)?;

    let section_context = format!("{context}.verified_primary_name");
    let section = payload
        .get("verified_primary_name")
        .and_then(JsonValue::as_object)
        .with_context(|| format!("{section_context} must be a JSON object"))?;
    ensure_allowed_json_fields(
        section,
        &["status", "name", "failure_reason"],
        &section_context,
    )?;

    match required_json_string_field(section, "status", &section_context)? {
        "success" => {
            validate_persisted_verified_primary_name_ref(
                section.get("name"),
                &format!("{section_context}.name"),
                namespace,
            )?;
            ensure_json_field_absent(section, "failure_reason", &section_context)?;
        }
        "not_found" => {
            ensure_json_field_absent(section, "name", &section_context)?;
            optional_nonempty_json_string_field(section, "failure_reason", &section_context)?;
        }
        "mismatch" => {
            validate_persisted_verified_primary_name_ref(
                section.get("name"),
                &format!("{section_context}.name"),
                namespace,
            )?;
            optional_nonempty_json_string_field(section, "failure_reason", &section_context)?;
        }
        "invalid_name" => {
            ensure_json_field_absent(section, "name", &section_context)?;
            optional_nonempty_json_string_field(section, "failure_reason", &section_context)?;
        }
        "execution_failed" => {
            ensure_json_field_absent(section, "name", &section_context)?;
            required_json_string_field(section, "failure_reason", &section_context)?;
        }
        status => {
            bail!(
                "{section_context} only supports success, not_found, mismatch, invalid_name, and execution_failed; found {status}"
            );
        }
    }

    Ok(Some(JsonValue::Object(section.clone())))
}

fn validate_persisted_verified_primary_name_ref(
    value: Option<&JsonValue>,
    context: &str,
    expected_namespace: &str,
) -> Result<()> {
    let name = value
        .and_then(JsonValue::as_object)
        .with_context(|| format!("{context} must be a JSON object"))?;
    ensure_allowed_json_fields(
        name,
        &[
            "logical_name_id",
            "namespace",
            "normalized_name",
            "canonical_display_name",
            "namehash",
            "resource_id",
            "binding_kind",
        ],
        context,
    )?;

    let logical_name_id = required_json_string_field(name, "logical_name_id", context)?;
    let namespace = required_json_string_field(name, "namespace", context)?;
    let normalized_name = required_json_string_field(name, "normalized_name", context)?;
    required_json_string_field(name, "canonical_display_name", context)?;
    required_json_string_field(name, "namehash", context)?;
    optional_nonempty_json_string_field(name, "resource_id", context)?;
    optional_nonempty_json_string_field(name, "binding_kind", context)?;

    if namespace != expected_namespace {
        bail!("{context}.namespace must be {expected_namespace}");
    }
    if logical_name_id != format!("{expected_namespace}:{normalized_name}") {
        bail!(
            "{context}.logical_name_id {logical_name_id} does not match normalized_name {normalized_name}"
        );
    }

    Ok(())
}

fn address_names_dedupe_label(dedupe_by: AddressNamesCurrentDedupe) -> &'static str {
    match dedupe_by {
        AddressNamesCurrentDedupe::Surface => "surface",
        AddressNamesCurrentDedupe::Resource => "resource",
    }
}

async fn resource_ids_for_name(pool: &PgPool, logical_name_id: &str) -> ApiResult<Vec<Uuid>> {
    let bindings = load_surface_bindings_by_logical_name_id(pool, logical_name_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                logical_name_id = %logical_name_id,
                error = ?load_error,
                "failed to load surface bindings for name history"
            );
            ApiError::internal_error(format!(
                "failed to load history bindings for logical name {logical_name_id}"
            ))
        })?;

    Ok(bindings
        .into_iter()
        .map(|binding| binding.resource_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

async fn logical_name_ids_for_resource(pool: &PgPool, resource_id: Uuid) -> ApiResult<Vec<String>> {
    let bindings = load_surface_bindings_by_resource_id(pool, resource_id)
        .await
        .map_err(|load_error| {
            error!(
                service = "api",
                resource_id = %resource_id,
                error = ?load_error,
                "failed to load surface bindings for resource history"
            );
            ApiError::internal_error(format!(
                "failed to load history bindings for resource {resource_id}"
            ))
        })?;

    Ok(bindings
        .into_iter()
        .map(|binding| binding.logical_name_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

fn chain_position_key(chain_id: &str) -> String {
    match chain_id {
        "ethereum-mainnet" => "ethereum".to_owned(),
        "base-mainnet" => "base".to_owned(),
        other => other.to_owned(),
    }
}

fn history_manifest_version(row: &HistoryEvent) -> JsonValue {
    json!({
        "manifest_version": row.manifest_version,
        "source_family": row.source_family.clone(),
        "source_manifest_id": row.source_manifest_id,
    })
}

fn ensure_public_namespace(namespace: &str) -> ApiResult<()> {
    if PUBLIC_NAMESPACES.contains(&namespace) {
        Ok(())
    } else {
        Err(ApiError {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: format!("namespace {namespace} is not supported"),
        })
    }
}

fn collect_unique(values: impl Iterator<Item = String>) -> Vec<String> {
    values.collect::<BTreeSet<_>>().into_iter().collect()
}

async fn shutdown_signal(service: &'static str) {
    match tokio::signal::ctrl_c().await {
        Ok(()) => info!(service = service, "shutdown signal received"),
        Err(error) => tracing::warn!(
            service = service,
            error = ?error,
            "failed to listen for shutdown signal"
        ),
    }
}

fn init_tracing(service: &'static str) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if std::env::var_os("BIGNAME_LOG_JSON").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .json()
            .with_target(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter)
            .compact()
            .with_target(false)
            .init();
    }

    info!(
        service = service,
        phase = bigname_domain::bootstrap_phase(),
        "logging configured"
    );
}
