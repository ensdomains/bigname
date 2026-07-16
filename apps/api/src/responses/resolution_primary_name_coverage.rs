fn primary_name_route_coverage(
    namespace: &str,
    lookup_state: &PrimaryNameLookupState,
) -> JsonValue {
    if matches!(
        lookup_state.tuple_state,
        PrimaryNameTupleState::TuplePresent(_)
    ) && primary_name_supported_tuple_namespace(namespace)
    {
        match namespace {
            "ens" => {
                return primary_name_exact_tuple_coverage(&["ens_v1_reverse_l1", "ens_execution"]);
            }
            "basenames" => {
                return primary_name_exact_tuple_coverage(&[
                    "basenames_base_primary",
                    "basenames_execution",
                ]);
            }
            _ => {}
        }
    }

    if matches!(
        lookup_state.on_demand_verified,
        OnDemandPrimaryNameVerificationState::Verified(_)
    ) && namespace == "ens"
    {
        return primary_name_exact_tuple_coverage(&["ens_reverse_rpc", "ens_execution_rpc"]);
    }

    if matches!(
        lookup_state.on_demand_claim,
        OnDemandPrimaryNameClaimState::Found(_)
            | OnDemandPrimaryNameClaimState::InvalidName(_)
            | OnDemandPrimaryNameClaimState::NotFound
    ) && namespace == "ens"
    {
        return primary_name_exact_tuple_coverage(&["ens_reverse_rpc"]);
    }

    primary_name_unsupported_exact_tuple_coverage()
}

fn primary_name_supported_tuple_namespace(namespace: &str) -> bool {
    matches!(namespace, "ens" | "basenames")
}

fn primary_name_exact_tuple_coverage(source_classes: &[&str]) -> JsonValue {
    json!({
        "status": "partial",
        "exhaustiveness": "non_enumerable",
        "source_classes_considered": source_classes,
        "enumeration_basis": "primary_name_lookup",
        "unsupported_reason": null,
    })
}

fn primary_name_unsupported_exact_tuple_coverage() -> JsonValue {
    json!({
        "status": "unsupported",
        "exhaustiveness": "not_applicable",
        "source_classes_considered": [],
        "enumeration_basis": "primary_name_lookup",
        "unsupported_reason": "primary-name exact-tuple persisted readback is not supported for the requested tuple",
    })
}

fn primary_name_last_updated(
    persisted_verified: Option<&PersistedPrimaryNameVerifiedReadback>,
) -> String {
    persisted_verified
        .map(|persisted_verified| format_timestamp(persisted_verified.finished_at))
        .unwrap_or_else(|| format_timestamp(OffsetDateTime::now_utc()))
}
