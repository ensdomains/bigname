        #[derive(Clone, Copy, Debug)]
        struct CapabilityCutoverEvidence {
            capability_group: &'static str,
            route_owner: &'static [&'static str],
            conformance_owner: &'static str,
            rollout_gate: &'static [&'static str],
            rollback_gate: &'static [&'static str],
        }

        const RELEASE_SMOKE_GATE: &str = "scripts/release-smoke";
        const ROLLBACK_SMOKE_GATE: &str = "scripts/rollback-smoke";
        const OPENAPI_ROUTE_OWNER_GATE: &str = "OpenAPI route owner guard";

        const CAPABILITY_CUTOVER_EVIDENCE: &[CapabilityCutoverEvidence] = &[
            CapabilityCutoverEvidence {
                capability_group: "exact name profile",
                route_owner: &["/v1/names/{namespace}/{name}", "/v1/coverage/{namespace}/{name}"],
                conformance_owner: "exact_name.rs::exact_name_contract_* and exact_name.rs::coverage_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused exact-name conformance",
                    RELEASE_SMOKE_GATE,
                    "matching manifest support state where applicable",
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "route envelope returns prior supported / unsupported coverage state",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "names owned / controlled by address",
                route_owner: &["/v1/addresses/{address}/names"],
                conformance_owner: "collections.rs::address_names_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused collection conformance",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable cursor / response-shape rollback for address collection",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "names owned / controlled by address with role summary",
                route_owner: &["/v1/addresses/{address}/names?include=role_summary"],
                conformance_owner: "collections.rs::address_names_role_summary_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "role-summary conformance",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "base address collection remains stable without role-summary widening",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "declared child subnames and counts",
                route_owner: &["/v1/names/{namespace}/{name}/children"],
                conformance_owner: "collections.rs::name_children_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused children conformance",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable child collection pagination / unsupported behavior",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "record inventory for editing",
                route_owner: &["/v1/resolutions/{namespace}/{name}"],
                conformance_owner: "resolution_and_permissions.rs::resolution_record_inventory_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused resolution conformance",
                    "supported resolver-profile evidence",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "explicit unsupported / pending resolver-family state",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "verified record reads",
                route_owner: &[
                    "/v1/resolutions/{namespace}/{name}",
                    "/v1/explain/resolutions/{namespace}/{name}/execution",
                ],
                conformance_owner: "resolution_and_permissions.rs::resolution_execution_explain_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused verified-resolution conformance",
                    "execution trace persistence checks",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "execution cache invalidation / unsupported verified-query behavior remains stable",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "name history",
                route_owner: &["/v1/history/names/{namespace}/{name}"],
                conformance_owner: "history.rs::name_history_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused history conformance",
                    "replay stability checks",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable cursor and canonical-only history behavior",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "address history across names",
                route_owner: &["/v1/history/addresses/{address}"],
                conformance_owner: "history.rs::address_history_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused address-history conformance",
                    "replay stability checks",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable address-anchor selection and pagination behavior",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "role holders for a resource",
                route_owner: &["/v1/resources/{resource_id}/permissions"],
                conformance_owner: "resolution_and_permissions.rs::resource_permissions_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused permissions conformance",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable permission envelope and explicit unsupported summaries",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "role change history",
                route_owner: &["/v1/history/resources/{resource_id}"],
                conformance_owner: "history.rs::resource_history_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused history conformance",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[ROLLBACK_SMOKE_GATE, "stable resource-history cursor behavior"],
            },
            CapabilityCutoverEvidence {
                capability_group: "resolver-centric overview",
                route_owner: &["/v1/resolvers/{chain_id}/{resolver_address}"],
                conformance_owner: "resolution_and_permissions.rs::resolver_overview_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused resolver conformance",
                    "supported resolver-profile evidence",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "UnsupportedSummary for pending / unsupported resolver profiles",
                ],
            },
            CapabilityCutoverEvidence {
                capability_group: "claimed vs verified primary name",
                route_owner: &["/v1/primary-names/{address}"],
                conformance_owner: "primary_names.rs::primary_names_contract_*",
                rollout_gate: &[
                    OPENAPI_ROUTE_OWNER_GATE,
                    "focused primary-name conformance",
                    "persisted execution readback checks",
                    RELEASE_SMOKE_GATE,
                ],
                rollback_gate: &[
                    ROLLBACK_SMOKE_GATE,
                    "stable bootstrap coverage and execution-derived verification rollback",
                ],
            },
        ];

        #[test]
        fn capability_cutover_evidence_is_complete_for_shipped_groups() {
            let expected_groups = [
                "exact name profile",
                "names owned / controlled by address",
                "names owned / controlled by address with role summary",
                "declared child subnames and counts",
                "record inventory for editing",
                "verified record reads",
                "name history",
                "address history across names",
                "role holders for a resource",
                "role change history",
                "resolver-centric overview",
                "claimed vs verified primary name",
            ];
            let openapi_owned_paths = OPENAPI_CONFORMANCE_COVERAGE
                .iter()
                .filter_map(|coverage| match coverage.scope {
                    OpenApiConformanceScope::HarnessOwner(_) => Some(coverage.path),
                    OpenApiConformanceScope::OutOfScope(_) => None,
                })
                .collect::<BTreeSet<_>>();
            let mut evidence_by_group = BTreeMap::new();

            for evidence in CAPABILITY_CUTOVER_EVIDENCE {
                assert!(
                    !evidence.capability_group.trim().is_empty(),
                    "capability group must be explicit"
                );
                assert!(
                    evidence_by_group
                        .insert(evidence.capability_group, evidence)
                        .is_none(),
                    "duplicate cutover evidence for {}",
                    evidence.capability_group
                );
                assert!(
                    !evidence.route_owner.is_empty(),
                    "{} must name at least one route owner",
                    evidence.capability_group
                );
                assert!(
                    !evidence.conformance_owner.trim().is_empty(),
                    "{} must name a conformance owner",
                    evidence.capability_group
                );
                assert!(
                    evidence.rollout_gate.contains(&OPENAPI_ROUTE_OWNER_GATE),
                    "{} rollout gate must include the OpenAPI route owner guard",
                    evidence.capability_group
                );
                assert!(
                    evidence.rollout_gate.contains(&RELEASE_SMOKE_GATE),
                    "{} rollout gate must include release smoke",
                    evidence.capability_group
                );
                assert!(
                    evidence.rollback_gate.contains(&ROLLBACK_SMOKE_GATE),
                    "{} rollback gate must include rollback smoke",
                    evidence.capability_group
                );

                for route_owner in evidence.route_owner {
                    let path = route_owner
                        .split_once('?')
                        .map_or(*route_owner, |(path, _query)| path);
                    assert!(
                        openapi_owned_paths.contains(path),
                        "{} references route owner {route_owner}, but {path} has no OpenAPI conformance owner",
                        evidence.capability_group
                    );
                }
            }

            let missing_groups = expected_groups
                .iter()
                .filter(|group| !evidence_by_group.contains_key(**group))
                .collect::<Vec<_>>();
            assert!(
                missing_groups.is_empty(),
                "missing cutover evidence for shipped capability groups: {missing_groups:#?}"
            );

            let unexpected_groups = evidence_by_group
                .keys()
                .filter(|group| !expected_groups.contains(group))
                .collect::<Vec<_>>();
            assert!(
                unexpected_groups.is_empty(),
                "unexpected capability cutover evidence groups: {unexpected_groups:#?}"
            );
        }
