pub(crate) type CompactRolesResponse = JsonValue;
pub(crate) type ResourceLookupResponse = JsonValue;
pub(crate) const ENSV1_WRAPPER_PERMISSIONS_UNSUPPORTED_REASON: &str =
    "ensv1_wrapper_holder_permissions_not_projected";
pub(crate) const RESOURCE_PERMISSION_AUTHORITY_NOT_PROJECTED_REASON: &str =
    "resource_permission_authority_not_projected";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompactRolesSupport {
    Supported,
    AccountWideWrapperCoveragePartial,
    ResourceAuthorityPartial,
    WrapperHolderPermissionsUnsupported,
}

pub(crate) fn build_compact_roles_response(
    rows: &[PermissionsCurrentRow],
    associated_names: &BTreeMap<Uuid, String>,
    summary: &bigname_storage::PermissionsCurrentFullFilterSummary,
    page: HistoryPageResponse,
    meta_mode: MetaMode,
    support: CompactRolesSupport,
) -> CompactRolesResponse {
    build_roles_response(
        rows,
        associated_names,
        summary.row_count.max(0) as u64,
        page,
        meta_mode,
        support,
    )
}

pub(crate) fn build_empty_compact_roles_response(
    page: HistoryPageResponse,
    meta_mode: MetaMode,
    support: CompactRolesSupport,
) -> CompactRolesResponse {
    build_roles_response(&[], &BTreeMap::new(), 0, page, meta_mode, support)
}

pub(crate) fn build_resource_lookup_response(
    row: &NameCurrentRow,
    resource_id: Uuid,
    meta_mode: MetaMode,
) -> ResourceLookupResponse {
    let mut data = empty_object();
    insert_string_field(&mut data, "namespace", row.namespace.clone());
    insert_string_field(&mut data, "name", row.canonical_display_name.clone());
    insert_string_field(&mut data, "normalized_name", row.normalized_name.clone());
    insert_string_field(&mut data, "resource_id", resource_id.to_string());
    insert_value_field(&mut data, "resource_hex", JsonValue::Null);

    let mut response = empty_object();
    insert_value_field(&mut response, "data", data);
    if meta_mode != MetaMode::None {
        insert_value_field(
            &mut response,
            "meta",
            compact_meta_object(
                "supported",
                None,
                ["resource_hex"].into_iter().map(str::to_owned),
                std::iter::empty(),
            ),
        );
    }
    response
}

fn build_roles_response(
    rows: &[PermissionsCurrentRow],
    associated_names: &BTreeMap<Uuid, String>,
    total_count: u64,
    page: HistoryPageResponse,
    meta_mode: MetaMode,
    support: CompactRolesSupport,
) -> CompactRolesResponse {
    let data = rows
        .iter()
        .map(|row| build_role_row(row, associated_names.get(&row.resource_id)))
        .collect::<Vec<_>>();

    let mut response = empty_object();
    insert_value_field(&mut response, "data", JsonValue::Array(data));
    insert_value_field(
        &mut response,
        "page",
        serde_json::to_value(page).expect("roles page response must serialize"),
    );
    if meta_mode != MetaMode::None {
        let (support_status, total_count, unsupported_fields) = match support {
            CompactRolesSupport::Supported => (
                "supported",
                Some(total_count),
                vec!["resource_hex".to_owned(), "role_bitmap".to_owned()],
            ),
            CompactRolesSupport::AccountWideWrapperCoveragePartial => (
                "partial",
                None,
                vec![
                    "effective_powers".to_owned(),
                    "resource_hex".to_owned(),
                    "role_bitmap".to_owned(),
                ],
            ),
            CompactRolesSupport::ResourceAuthorityPartial => (
                "partial",
                None,
                vec![
                    "effective_powers".to_owned(),
                    "resource_hex".to_owned(),
                    "role_bitmap".to_owned(),
                ],
            ),
            CompactRolesSupport::WrapperHolderPermissionsUnsupported => (
                "unsupported",
                None,
                vec![
                    "effective_powers".to_owned(),
                    "resource_hex".to_owned(),
                    "role_bitmap".to_owned(),
                ],
            ),
        };
        let mut meta = compact_meta_object(
            support_status,
            total_count,
            unsupported_fields,
            std::iter::empty(),
        );
        if support != CompactRolesSupport::Supported {
            insert_string_field(
                &mut meta,
                "exhaustiveness",
                match support {
                    CompactRolesSupport::AccountWideWrapperCoveragePartial => "best_effort",
                    CompactRolesSupport::ResourceAuthorityPartial => "best_effort",
                    CompactRolesSupport::WrapperHolderPermissionsUnsupported => "not_applicable",
                    CompactRolesSupport::Supported => unreachable!(),
                }
                .to_owned(),
            );
            insert_value_field(
                &mut meta,
                "source_classes_considered",
                match support {
                    CompactRolesSupport::AccountWideWrapperCoveragePartial
                    | CompactRolesSupport::WrapperHolderPermissionsUnsupported => {
                        serde_json::json!(["permissions_current", "ens_v1_wrapper_l1"])
                    }
                    CompactRolesSupport::ResourceAuthorityPartial => {
                        serde_json::json!(["permissions_current"])
                    }
                    CompactRolesSupport::Supported => unreachable!(),
                },
            );
            insert_string_field(
                &mut meta,
                "enumeration_basis",
                match support {
                    CompactRolesSupport::AccountWideWrapperCoveragePartial => "account_roles",
                    CompactRolesSupport::ResourceAuthorityPartial => "resource_roles",
                    CompactRolesSupport::WrapperHolderPermissionsUnsupported => "resource_roles",
                    CompactRolesSupport::Supported => unreachable!(),
                }
                .to_owned(),
            );
            insert_string_field(
                &mut meta,
                "unsupported_reason",
                match support {
                    CompactRolesSupport::AccountWideWrapperCoveragePartial
                    | CompactRolesSupport::WrapperHolderPermissionsUnsupported => {
                        ENSV1_WRAPPER_PERMISSIONS_UNSUPPORTED_REASON
                    }
                    CompactRolesSupport::ResourceAuthorityPartial => {
                        RESOURCE_PERMISSION_AUTHORITY_NOT_PROJECTED_REASON
                    }
                    CompactRolesSupport::Supported => unreachable!(),
                }
                .to_owned(),
            );
        }
        insert_value_field(
            &mut response,
            "meta",
            meta,
        );
    }
    response
}

fn build_role_row(row: &PermissionsCurrentRow, name: Option<&String>) -> JsonValue {
    let mut value = empty_object();
    insert_string_field(&mut value, "account", row.subject.clone());
    insert_value_field(&mut value, "resource_hex", JsonValue::Null);
    insert_string_field(&mut value, "resource_id", row.resource_id.to_string());
    insert_nullable_string_field(
        &mut value,
        "name",
        name.cloned(),
    );
    // permissions_current currently exposes post-scope powers, not a raw role bitmap.
    insert_value_field(&mut value, "role_bitmap", JsonValue::Null);
    insert_value_field(&mut value, "effective_powers", row.effective_powers.clone());
    value
}

#[cfg(test)]
mod wrapper_roles_response_tests {
    use super::*;

    #[test]
    fn wrapper_role_page_does_not_claim_supported_complete_results() {
        let response = build_empty_compact_roles_response(
            HistoryPageResponse {
                cursor: None,
                next_cursor: None,
                page_size: 50,
                sort: "account_resource_scope_asc".to_owned(),
            },
            MetaMode::Summary,
            CompactRolesSupport::WrapperHolderPermissionsUnsupported,
        );

        assert_eq!(response["meta"]["support_status"], "unsupported");
        assert_eq!(response["meta"]["total_count"], JsonValue::Null);
        assert_eq!(
            response["meta"]["unsupported_reason"],
            "ensv1_wrapper_holder_permissions_not_projected"
        );
        assert_eq!(response["meta"]["exhaustiveness"], "not_applicable");
    }

    #[test]
    fn account_role_page_is_partial_while_wrapper_holder_roles_are_unprojected() {
        let response = build_empty_compact_roles_response(
            HistoryPageResponse {
                cursor: None,
                next_cursor: None,
                page_size: 50,
                sort: "account_resource_scope_asc".to_owned(),
            },
            MetaMode::Summary,
            CompactRolesSupport::AccountWideWrapperCoveragePartial,
        );

        assert_eq!(response["meta"]["support_status"], "partial");
        assert_eq!(response["meta"]["total_count"], JsonValue::Null);
        assert_eq!(response["meta"]["exhaustiveness"], "best_effort");
        assert_eq!(response["meta"]["enumeration_basis"], "account_roles");
    }
}
