        fn timestamp(seconds: i64) -> OffsetDateTime {
            OffsetDateTime::from_unix_timestamp(seconds)
                .expect("conformance timestamp must be valid")
        }

        fn raw_block(
            chain_id: &str,
            block_hash: &str,
            parent_hash: Option<&str>,
            block_number: i64,
            block_timestamp: i64,
        ) -> RawBlock {
            RawBlock {
                chain_id: chain_id.to_owned(),
                block_hash: block_hash.to_owned(),
                parent_hash: parent_hash.map(str::to_owned),
                block_number,
                block_timestamp: timestamp(block_timestamp),
                logs_bloom: None,
                transactions_root: None,
                receipts_root: None,
                state_root: None,
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        fn resource(resource_id: Uuid) -> Resource {
            Resource {
                resource_id,
                token_lineage_id: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xresource".to_owned(),
                block_number: 99,
                provenance: json!({"seed": "resource"}),
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        fn name_surface(logical_name_id: &str) -> NameSurface {
            let (namespace, normalized_name) = logical_name_id
                .split_once(':')
                .expect("logical_name_id must include namespace");

            NameSurface {
                logical_name_id: logical_name_id.to_owned(),
                namespace: namespace.to_owned(),
                input_name: normalized_name.to_owned(),
                canonical_display_name: "Alice.eth".to_owned(),
                normalized_name: normalized_name.to_owned(),
                dns_encoded_name: vec![5, b'a', b'l', b'i', b'c', b'e'],
                namehash: format!("namehash:{normalized_name}"),
                labelhashes: vec!["labelhash:alice".to_owned()],
                normalizer_version: "uts46-v1".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xsurface".to_owned(),
                block_number: 98,
                provenance: json!({"seed": "surface"}),
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        fn surface_binding(
            surface_binding_id: Uuid,
            logical_name_id: &str,
            resource_id: Uuid,
            active_from: OffsetDateTime,
        ) -> SurfaceBinding {
            SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from,
                active_to: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: "0xbinding".to_owned(),
                block_number: 100,
                provenance: json!({"seed": "binding"}),
                canonicality_state: CanonicalityState::Canonical,
            }
        }

        async fn seed_basenames_exact_name_rebuild_inputs(
            database: &HarnessDatabase,
            logical_name_id: &str,
            resource_id: Uuid,
            token_lineage_id: Uuid,
            surface_binding_id: Uuid,
        ) -> Result<()> {
            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("base-mainnet", "0xbase-surface", None, 98, 1_717_171_698),
                    raw_block("base-mainnet", "0xbase-resource", None, 99, 1_717_171_699),
                    raw_block("base-mainnet", "0xbase-binding", None, 100, 1_717_171_700),
                    raw_block("base-mainnet", "0xbase-grant", None, 101, 1_717_171_701),
                    raw_block("base-mainnet", "0xbase-authority", None, 102, 1_717_171_702),
                    raw_block("base-mainnet", "0xbase-resolver", None, 103, 1_717_171_703),
                ],
            )
            .await?;
            bigname_storage::upsert_name_surfaces(
                &database.pool,
                &[NameSurface {
                    logical_name_id: logical_name_id.to_owned(),
                    namespace: "basenames".to_owned(),
                    input_name: "alice.base.eth".to_owned(),
                    canonical_display_name: "Alice.base.eth".to_owned(),
                    normalized_name: "alice.base.eth".to_owned(),
                    dns_encoded_name: b"alice.base.eth".to_vec(),
                    namehash: "namehash:alice.base.eth".to_owned(),
                    labelhashes: vec!["labelhash:alice.base.eth".to_owned()],
                    normalizer_version: "ensip15@2026-04-16".to_owned(),
                    normalization_warnings: json!([]),
                    normalization_errors: json!([]),
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: "0xbase-surface".to_owned(),
                    block_number: 98,
                    provenance: json!({"seed": "basenames_exact_name_surface"}),
                    canonicality_state: CanonicalityState::Canonical,
                }],
            )
            .await?;
            bigname_storage::upsert_token_lineages(
                &database.pool,
                &[TokenLineage {
                    token_lineage_id,
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: "0xbase-resource".to_owned(),
                    block_number: 99,
                    provenance: json!({"seed": "basenames_exact_name_token_lineage"}),
                    canonicality_state: CanonicalityState::Canonical,
                }],
            )
            .await?;
            bigname_storage::upsert_resources(
                &database.pool,
                &[Resource {
                    resource_id,
                    token_lineage_id: Some(token_lineage_id),
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: "0xbase-resource".to_owned(),
                    block_number: 99,
                    provenance: json!({"seed": "basenames_exact_name_resource"}),
                    canonicality_state: CanonicalityState::Canonical,
                }],
            )
            .await?;
            bigname_storage::upsert_surface_bindings(
                &database.pool,
                &[SurfaceBinding {
                    surface_binding_id,
                    logical_name_id: logical_name_id.to_owned(),
                    resource_id,
                    binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                    active_from: timestamp(1_717_171_700),
                    active_to: None,
                    chain_id: "base-mainnet".to_owned(),
                    block_hash: "0xbase-binding".to_owned(),
                    block_number: 100,
                    provenance: json!({"seed": "basenames_exact_name_binding"}),
                    canonicality_state: CanonicalityState::Canonical,
                }],
            )
            .await?;
            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:basenames:grant".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "RegistrationGranted".to_owned(),
                        source_family: "basenames_base_registrar".to_owned(),
                        manifest_version: 3,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(101),
                        block_hash: Some("0xbase-grant".to_owned()),
                        transaction_hash: Some("0xtxbasegrant".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:grant"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "authority_kind": "registrar",
                            "authority_key": "registrar:base-mainnet:alice",
                            "registrant": "0x00000000000000000000000000000000000000aa",
                            "expiry": 1_900_000_000_i64,
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:basenames:authority".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "AuthorityTransferred".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        manifest_version: 3,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(102),
                        block_hash: Some("0xbase-authority".to_owned()),
                        transaction_hash: Some("0xtxbaseauthority".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:authority"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "owner": "0x00000000000000000000000000000000000000bb",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:basenames:resolver".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "ResolverChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xbase-resolver".to_owned()),
                        transaction_hash: Some("0xtxbaseresolver".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:resolver"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "resolver": "0x0000000000000000000000000000000000000abc",
                            "namehash": "namehash:alice.base.eth",
                        }),
                    },
                ],
            )
            .await?;

            Ok(())
        }

        async fn seed_basenames_resolution_rebuild_inputs(
            database: &HarnessDatabase,
            logical_name_id: &str,
            resource_id: Uuid,
            token_lineage_id: Uuid,
            surface_binding_id: Uuid,
        ) -> Result<()> {
            seed_basenames_exact_name_rebuild_inputs(
                database,
                logical_name_id,
                resource_id,
                token_lineage_id,
                surface_binding_id,
            )
            .await?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:basenames:record-version".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "RecordVersionChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xbase-resolver".to_owned()),
                        transaction_hash: Some("0xtxbaseresolver".to_owned()),
                        log_index: Some(1),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:record-version"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({
                            "record_version": 6,
                        }),
                        after_state: json!({
                            "record_version": 7,
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:basenames:addr".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xbase-resolver".to_owned()),
                        transaction_hash: Some("0xtxbaseresolver".to_owned()),
                        log_index: Some(2),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:addr"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "record_key": "addr:60",
                            "record_family": "addr",
                            "selector_key": "60",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:basenames:text".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(logical_name_id.to_owned()),
                        resource_id: Some(resource_id),
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xbase-resolver".to_owned()),
                        transaction_hash: Some("0xtxbaseresolver".to_owned()),
                        log_index: Some(3),
                        raw_fact_ref: json!({"kind": "raw_log", "event_identity": "conformance:basenames:text"}),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "record_key": "text",
                            "record_family": "text",
                            "selector_key": null,
                        }),
                    },
                ],
            )
            .await?;

            Ok(())
        }

        async fn rebuild_record_inventory_current(
            database: &HarnessDatabase,
            resource_id: Uuid,
        ) -> Result<()> {
            let database_url = database.database_url.clone();
            let resource_id = resource_id.to_string();
            let worker_manifest_path =
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../apps/worker/Cargo.toml");

            tokio::task::spawn_blocking(move || -> Result<()> {
                let _guard = WORKER_CARGO_LOCK
                    .lock()
                    .expect("worker cargo lock must not be poisoned");
                let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
                let output = Command::new(cargo)
                    .arg("run")
                    .arg("--quiet")
                    .arg("--manifest-path")
                    .arg(worker_manifest_path)
                    .arg("--")
                    .arg("record-inventory-current")
                    .arg("rebuild")
                    .arg("--database-url")
                    .arg(&database_url)
                    .arg("--resource-id")
                    .arg(&resource_id)
                    .output()
                    .with_context(|| {
                        format!(
                            "failed to invoke worker record_inventory_current rebuild for {resource_id}"
                        )
                    })?;

                if !output.status.success() {
                    return Err(anyhow::anyhow!(
                        "worker record_inventory_current rebuild failed for {resource_id}\nstdout:\n{}\nstderr:\n{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr),
                    ));
                }

                Ok(())
            })
            .await
            .context("worker record_inventory_current rebuild task panicked")??;

            Ok(())
        }

        #[allow(clippy::too_many_arguments)]
        fn history_event(
            event_identity: &str,
            logical_name_id: Option<&str>,
            resource_id: Option<Uuid>,
            chain_id: Option<&str>,
            block_number: Option<i64>,
            block_hash: Option<&str>,
            transaction_hash: Option<&str>,
            log_index: Option<i64>,
            canonicality_state: CanonicalityState,
        ) -> NormalizedEvent {
            NormalizedEvent {
                event_identity: event_identity.to_owned(),
                namespace: "ens".to_owned(),
                logical_name_id: logical_name_id.map(str::to_owned),
                resource_id,
                event_kind: "HistoryEvent".to_owned(),
                source_family: "ens_v1_registry_l1".to_owned(),
                manifest_version: 7,
                source_manifest_id: None,
                chain_id: chain_id.map(str::to_owned),
                block_number,
                block_hash: block_hash.map(str::to_owned),
                transaction_hash: transaction_hash.map(str::to_owned),
                log_index,
                raw_fact_ref: json!({
                    "kind": "raw_log",
                    "event_identity": event_identity,
                }),
                derivation_kind: "history_test".to_owned(),
                canonicality_state,
                before_state: json!({
                    "provenance": {
                        "before": event_identity,
                    }
                }),
                after_state: json!({
                    "provenance": {
                        "after": event_identity,
                    },
                    "coverage": {
                        "status": "full",
                        "exhaustiveness": "authoritative",
                        "source_classes_considered": ["normalized_events"],
                        "enumeration_basis": event_identity,
                        "unsupported_reason": null,
                    }
                }),
            }
        }

        fn authority_history_event(
            event_identity: &str,
            namespace: &str,
            logical_name_id: &str,
            resource_id: Uuid,
            event_kind: &str,
            block_number: i64,
            block_hash: &str,
            after_state: Value,
        ) -> NormalizedEvent {
            NormalizedEvent {
                namespace: namespace.to_owned(),
                event_kind: event_kind.to_owned(),
                source_family: "ens_v1_registrar_l1".to_owned(),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                after_state,
                before_state: json!({}),
                ..history_event(
                    event_identity,
                    Some(logical_name_id),
                    Some(resource_id),
                    Some("ethereum-mainnet"),
                    Some(block_number),
                    Some(block_hash),
                    Some(&format!("0xtx{block_number}")),
                    Some(0),
                    CanonicalityState::Canonical,
                )
            }
        }

        fn history_event_identities(payload: &HistoryResponse) -> Vec<&str> {
            payload
                .data
                .iter()
                .map(|row| {
                    row.get("event_identity")
                        .and_then(Value::as_str)
                        .expect("history row must include event_identity")
                })
                .collect()
        }

        fn permission_current_row(
            resource_id: Uuid,
            subject: &str,
            scope: PermissionScope,
            manifest_version: i64,
            block_number: i64,
        ) -> PermissionsCurrentRow {
            PermissionsCurrentRow {
                resource_id,
                subject: subject.to_owned(),
                scope,
                effective_powers: json!([
                    "set_resolver",
                    if manifest_version % 2 == 0 {
                        "create_subnames"
                    } else {
                        "set_records"
                    }
                ]),
                grant_source: json!({
                    "kind": "normalized_event",
                    "manifest_version": manifest_version,
                }),
                revocation_source: None,
                inheritance_path: json!([
                    {
                        "kind": "resource_authority",
                        "resource_id": resource_id,
                    }
                ]),
                transfer_behavior: json!({
                    "kind": "resource_rebound",
                }),
                provenance: json!({
                    "normalized_event_ids": [block_number, block_number + 1],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "block_number": block_number,
                    }],
                    "manifest_versions": [{
                        "manifest_version": manifest_version,
                        "source_family": "ens_v2_registry_l1",
                        "chain": "ethereum-mainnet",
                        "deployment_epoch": "ens_v2",
                    }],
                    "derivation_kind": "permissions_current_rebuild",
                }),
                coverage: json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["permissions_current"],
                    "enumeration_basis": "resource_permissions",
                    "unsupported_reason": null,
                }),
                chain_positions: json!({
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": block_number,
                        "block_hash": format!("0xperm{block_number:02x}"),
                        "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        "ethereum-mainnet": "finalized",
                    }
                }),
                manifest_version,
                last_recomputed_at: timestamp(1_717_174_000 + block_number),
            }
        }

        fn permission_subjects(payload: &ResourcePermissionsResponse) -> Vec<&str> {
            payload
                .data
                .iter()
                .map(|row| {
                    row.get("subject")
                        .and_then(Value::as_str)
                        .expect("permission row must include subject")
                })
                .collect()
        }

        fn assert_primary_name_route_common_invariants(payload: &PrimaryNameResponse) {
            assert_eq!(
                payload.coverage,
                json!({
                    "status": "unsupported",
                    "exhaustiveness": "not_applicable",
                    "source_classes_considered": [],
                    "enumeration_basis": "primary_name_lookup",
                    "unsupported_reason": "primary-name coverage is not yet supported",
                })
            );
            assert_eq!(payload.chain_positions, json!({}));
            assert_eq!(payload.consistency, "head");
        }

        fn assert_primary_name_bootstrap_invariants(payload: &PrimaryNameResponse) {
            assert_eq!(
                payload.provenance,
                json!({
                    "normalized_event_ids": [],
                    "raw_fact_refs": [],
                    "manifest_versions": [],
                    "execution_trace_id": null,
                    "derivation_kind": "primary_name_route_bootstrap",
                })
            );
            assert_primary_name_route_common_invariants(payload);
            assert!(payload.last_updated.ends_with('Z'));
        }

        fn assert_primary_name_persisted_readback_invariants(
            payload: &PrimaryNameResponse,
            execution_trace_id: Uuid,
            finished_at: OffsetDateTime,
        ) {
            assert_eq!(
                payload.provenance,
                json!({
                    "normalized_event_ids": [],
                    "raw_fact_refs": [],
                    "manifest_versions": primary_name_execution_manifest_versions(),
                    "execution_trace_id": execution_trace_id.to_string(),
                    "derivation_kind": "primary_name_route_bootstrap",
                })
            );
            assert_primary_name_route_common_invariants(payload);
            assert_eq!(payload.last_updated, format_timestamp(finished_at));
        }

        fn seeded_primary_name_claim_provenance() -> Value {
            json!({})
        }

        fn stable_row_strings(rows: &[Value]) -> Vec<String> {
            rows.iter()
                .map(|row| serde_json::to_string(row).expect("response rows must serialize"))
                .collect()
        }

        fn assert_replay_stable_pagination(
            base_rows: &[Value],
            base_page: &HistoryPageResponse,
            first_rows: &[Value],
            first_page: &HistoryPageResponse,
            second_rows: &[Value],
            second_page: &HistoryPageResponse,
            replay_rows: &[Value],
            replay_page: &HistoryPageResponse,
            expected_sort: &str,
            expected_unpaged_page_size: u64,
            expected_paged_page_size: u64,
        ) {
            let base_rows = stable_row_strings(base_rows);
            let first_rows = stable_row_strings(first_rows);
            let second_rows = stable_row_strings(second_rows);
            let replay_rows = stable_row_strings(replay_rows);

            assert_eq!(base_page.cursor, None);
            assert_eq!(base_page.next_cursor, None);
            assert_eq!(base_page.page_size, expected_unpaged_page_size);
            assert_eq!(base_page.sort, expected_sort);

            assert_eq!(first_page.cursor, None);
            assert_eq!(first_page.page_size, expected_paged_page_size);
            assert_eq!(first_page.sort, expected_sort);

            let applied_cursor = first_page
                .next_cursor
                .clone()
                .expect("first page must return a cursor for replay assertions");

            assert_eq!(
                first_rows,
                base_rows
                    .iter()
                    .take(first_rows.len())
                    .cloned()
                    .collect::<Vec<_>>()
            );

            assert_eq!(second_page.cursor.as_deref(), Some(applied_cursor.as_str()));
            assert_eq!(second_page.page_size, expected_paged_page_size);
            assert_eq!(second_page.sort, expected_sort);
            assert_eq!(
                second_rows,
                base_rows
                    .iter()
                    .skip(first_rows.len())
                    .take(second_rows.len())
                    .cloned()
                    .collect::<Vec<_>>()
            );

            assert_eq!(replay_page.cursor.as_deref(), Some(applied_cursor.as_str()));
            assert_eq!(replay_page, second_page);
            assert_eq!(replay_rows, second_rows);
        }

        fn collection_name_surface(
            logical_name_id: &str,
            display_name: &str,
            namehash: &str,
            block_number: i64,
        ) -> NameSurface {
            let namespace = logical_name_id
                .split_once(':')
                .map(|(namespace, _)| namespace)
                .expect("logical_name_id must include namespace")
                .to_owned();
            let chain_id = chain_id_for_namespace(&namespace).to_owned();

            NameSurface {
                logical_name_id: logical_name_id.to_owned(),
                namespace,
                input_name: display_name.to_owned(),
                canonical_display_name: display_name.to_owned(),
                normalized_name: display_name.to_owned(),
                dns_encoded_name: display_name.as_bytes().to_vec(),
                namehash: namehash.to_owned(),
                labelhashes: vec![format!("labelhash:{display_name}")],
                normalizer_version: "ensip15@2026-04-16".to_owned(),
                normalization_warnings: json!([]),
                normalization_errors: json!([]),
                chain_id,
                block_hash: format!("0xsurface{block_number:02x}"),
                block_number,
                provenance: json!({"seed": "children_surface"}),
                canonicality_state: CanonicalityState::Finalized,
            }
        }

        fn declared_child_row(
            parent_logical_name_id: &str,
            child_logical_name_id: &str,
            display_name: &str,
            namehash: &str,
            normalized_event_id: i64,
            block_number: i64,
        ) -> bigname_storage::ChildrenCurrentRow {
            let namespace = parent_logical_name_id
                .split_once(':')
                .map(|(namespace, _)| namespace)
                .expect("parent_logical_name_id must include namespace");
            let chain_id = chain_id_for_namespace(namespace);
            let chain_slot = chain_slot_for_namespace(namespace);

            bigname_storage::ChildrenCurrentRow {
                parent_logical_name_id: parent_logical_name_id.to_owned(),
                child_logical_name_id: child_logical_name_id.to_owned(),
                surface_class: "declared".to_owned(),
                namespace: namespace.to_owned(),
                canonical_display_name: display_name.to_owned(),
                normalized_name: display_name.to_owned(),
                namehash: namehash.to_owned(),
                provenance: json!({
                    "normalized_event_ids": [normalized_event_id],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "block_number": block_number,
                    }],
                    "manifest_versions": [{
                        "manifest_version": 1,
                        "source_family": source_family_for_namespace(namespace),
                        "source_manifest_id": null,
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "children_current_rebuild",
                }),
                chain_positions: json!({
                    chain_slot: {
                        "chain_id": chain_id,
                        "block_number": block_number,
                        "block_hash": format!("0xblock{block_number:02x}"),
                        "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        chain_id: "finalized"
                    }
                }),
                manifest_version: 1,
                last_recomputed_at: timestamp(1_717_172_000 + block_number),
            }
        }

        fn chain_id_for_namespace(namespace: &str) -> &'static str {
            match namespace {
                "basenames" => "base-mainnet",
                _ => "ethereum-mainnet",
            }
        }

        fn chain_slot_for_namespace(namespace: &str) -> &'static str {
            match namespace {
                "basenames" => "base",
                _ => "ethereum",
            }
        }

        fn source_family_for_namespace(namespace: &str) -> &'static str {
            match namespace {
                "basenames" => "basenames_base_registry",
                _ => "ens_v1_registry_l1",
            }
        }

        fn address_name_token_lineage(
            token_lineage_id: Uuid,
            block_hash: &str,
            block_number: i64,
        ) -> TokenLineage {
            TokenLineage {
                token_lineage_id,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                provenance: json!({"seed": "address_name_token_lineage"}),
                canonicality_state: CanonicalityState::Finalized,
            }
        }

        fn address_name_resource(
            resource_id: Uuid,
            token_lineage_id: Option<Uuid>,
            block_hash: &str,
            block_number: i64,
        ) -> Resource {
            Resource {
                resource_id,
                token_lineage_id,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                provenance: json!({"seed": "address_name_resource"}),
                canonicality_state: CanonicalityState::Finalized,
            }
        }

        fn address_name_surface_binding(
            surface_binding_id: Uuid,
            logical_name_id: &str,
            resource_id: Uuid,
            block_hash: &str,
            block_number: i64,
            active_from: i64,
        ) -> SurfaceBinding {
            SurfaceBinding {
                surface_binding_id,
                logical_name_id: logical_name_id.to_owned(),
                resource_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                active_from: timestamp(active_from),
                active_to: None,
                chain_id: "ethereum-mainnet".to_owned(),
                block_hash: block_hash.to_owned(),
                block_number,
                provenance: json!({"seed": "address_name_binding"}),
                canonicality_state: CanonicalityState::Finalized,
            }
        }

        fn address_name_current_row(
            address: &str,
            logical_name_id: &str,
            relation: bigname_storage::AddressNameRelation,
            display_name: &str,
            normalized_name: &str,
            namehash: &str,
            surface_binding_id: Uuid,
            resource_id: Uuid,
            token_lineage_id: Option<Uuid>,
            block_number: i64,
        ) -> bigname_storage::AddressNameCurrentRow {
            bigname_storage::AddressNameCurrentRow {
                address: address.to_owned(),
                logical_name_id: logical_name_id.to_owned(),
                relation,
                namespace: logical_name_id
                    .split_once(':')
                    .map(|(namespace, _)| namespace)
                    .expect("logical_name_id must include namespace")
                    .to_owned(),
                canonical_display_name: display_name.to_owned(),
                normalized_name: normalized_name.to_owned(),
                namehash: namehash.to_owned(),
                surface_binding_id,
                resource_id,
                token_lineage_id,
                binding_kind: SurfaceBindingKind::DeclaredRegistryPath,
                provenance: json!({
                    "normalized_event_ids": [block_number],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "block_number": block_number,
                    }],
                    "manifest_versions": [{
                        "manifest_version": 3,
                        "source_family": "ens_v1_registrar_l1",
                        "source_manifest_id": null,
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "address_names_current_rebuild",
                }),
                coverage: json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["ensv1_registry_path"],
                    "unsupported_reason": null,
                    "enumeration_basis": "surface_current_relations",
                }),
                chain_positions: json!({
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": block_number,
                        "block_hash": format!("0xaddr{block_number:02x}"),
                        "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        "ethereum-mainnet": "finalized"
                    }
                }),
                manifest_version: 3,
                last_recomputed_at: timestamp(1_717_173_000 + block_number),
            }
        }

        fn resolver_current_row(chain_id: &str, resolver_address: &str) -> ResolverCurrentRow {
            ResolverCurrentRow {
                chain_id: chain_id.to_owned(),
                resolver_address: resolver_address.to_owned(),
                declared_summary: json!({
                    "bindings": {
                        "status": "supported",
                        "count": 2,
                        "items": [
                            {
                                "logical_name_id": "ens:alice.eth",
                                "canonical_display_name": "Alice.eth",
                                "normalized_name": "alice.eth",
                                "namehash": "namehash:alice.eth",
                                "resource_id": "00000000-0000-0000-0000-00000000b100",
                                "surface_binding_id": "00000000-0000-0000-0000-00000000b101",
                                "binding_kind": "declared_registry_path",
                            },
                            {
                                "logical_name_id": "ens:beta.eth",
                                "canonical_display_name": "Beta.eth",
                                "normalized_name": "beta.eth",
                                "namehash": "namehash:beta.eth",
                                "resource_id": "00000000-0000-0000-0000-00000000b102",
                                "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                                "binding_kind": "resolver_alias_path",
                            }
                        ],
                    },
                    "aliases": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "logical_name_id": "ens:beta.eth",
                            "canonical_display_name": "Beta.eth",
                            "normalized_name": "beta.eth",
                            "namehash": "namehash:beta.eth",
                            "resource_id": "00000000-0000-0000-0000-00000000b102",
                            "surface_binding_id": "00000000-0000-0000-0000-00000000b103",
                            "binding_kind": "resolver_alias_path",
                        }],
                    },
                    "permissions": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "resource_id": "00000000-0000-0000-0000-00000000b100",
                            "subject": "0x0000000000000000000000000000000000000abc",
                            "effective_powers": ["set_resolver", "set_records"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "resolver-permission-1",
                            },
                            "revocation_source": null,
                        }],
                    },
                    "role_holders": {
                        "status": "supported",
                        "count": 1,
                        "items": [{
                            "subject": "0x0000000000000000000000000000000000000abc",
                            "resource_count": 1,
                            "permission_row_count": 1,
                            "effective_powers": ["set_records", "set_resolver"],
                            "resource_ids": ["00000000-0000-0000-0000-00000000b100"],
                        }],
                    },
                    "event_summary": {
                        "status": "supported",
                        "count": 2,
                        "by_kind": {
                            "PermissionChanged": 1,
                            "ResolverChanged": 1,
                        },
                    },
                }),
                provenance: json!({
                    "normalized_event_ids": [101, 202],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "chain_id": chain_id,
                        "block_number": 202,
                    }],
                    "manifest_versions": [{
                        "manifest_version": 7,
                        "source_family": "ens_v2_registry_l1",
                        "chain": chain_id,
                        "deployment_epoch": "ens_v2",
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "resolver_current_rebuild",
                }),
                coverage: json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["ens_v2_registry_l1", "permissions_current"],
                    "unsupported_reason": null,
                    "enumeration_basis": "resolver_target",
                }),
                chain_positions: json!({
                    "ethereum": {
                        "chain_id": chain_id,
                        "block_number": 202,
                        "block_hash": "0xresolverc8",
                        "timestamp": "2026-04-17T00:00:22Z",
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        chain_id: "finalized",
                    }
                }),
                manifest_version: 7,
                last_recomputed_at: timestamp(1_748_800_202),
            }
        }

        fn address_name_name_current_row(
            logical_name_id: &str,
            canonical_display_name: &str,
            normalized_name: &str,
            namehash: &str,
            surface_binding_id: Uuid,
            resource_id: Uuid,
            token_lineage_id: Option<Uuid>,
            block_number: i64,
            declared_summary: Value,
        ) -> bigname_storage::NameCurrentRow {
            bigname_storage::NameCurrentRow {
                logical_name_id: logical_name_id.to_owned(),
                namespace: logical_name_id
                    .split_once(':')
                    .map(|(namespace, _)| namespace)
                    .expect("logical_name_id must include namespace")
                    .to_owned(),
                canonical_display_name: canonical_display_name.to_owned(),
                normalized_name: normalized_name.to_owned(),
                namehash: namehash.to_owned(),
                surface_binding_id: Some(surface_binding_id),
                resource_id: Some(resource_id),
                token_lineage_id,
                binding_kind: Some(bigname_storage::SurfaceBindingKind::DeclaredRegistryPath),
                declared_summary,
                provenance: json!({
                    "normalized_event_ids": [block_number, block_number + 1],
                    "raw_fact_refs": [{
                        "kind": "raw_log",
                        "block_number": block_number,
                    }],
                    "manifest_versions": [{
                        "manifest_version": 3,
                        "source_family": "ens_v1_registry",
                        "chain": "ethereum-mainnet",
                        "deployment_epoch": "ens_v1",
                    }],
                    "execution_trace_id": null,
                    "derivation_kind": "projection_apply",
                }),
                coverage: json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "source_classes_considered": ["ensv1_registry_path"],
                    "unsupported_reason": null,
                    "enumeration_basis": "exact_name",
                }),
                chain_positions: json!({
                    "ethereum": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": block_number,
                        "block_hash": format!("0xname{block_number:02x}"),
                        "timestamp": format!("2026-04-17T00:00:{:02}Z", block_number % 60),
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        "ethereum-mainnet": "finalized"
                    }
                }),
                manifest_version: 3,
                last_recomputed_at: timestamp(1_717_175_000 + block_number),
            }
        }

        fn exact_name_control_summary() -> Value {
            json!({
                "registrant": "0x00000000000000000000000000000000000000aa",
                "registry_owner": "0x00000000000000000000000000000000000000bb",
                "latest_event_kind": "AuthorityTransferred",
            })
        }

        fn exact_name_authority_summary(resource_id: Uuid, token_lineage_id: Uuid) -> Value {
            json!({
                "resource_id": resource_id.to_string(),
                "token_lineage_id": token_lineage_id.to_string(),
                "binding_kind": "declared_registry_path",
            })
        }

        fn exact_name_surface_binding_summary(surface_binding_id: Uuid) -> Value {
            json!({
                "surface_binding_id": surface_binding_id.to_string(),
                "binding_kind": "declared_registry_path",
            })
        }

        fn exact_name_resolver_summary() -> Value {
            json!({
                "chain_id": "ethereum-mainnet",
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged",
            })
        }

        fn basenames_exact_name_control_summary() -> Value {
            json!({
                "registrant": "0x00000000000000000000000000000000000000aa",
                "registry_owner": "0x00000000000000000000000000000000000000bb",
                "latest_event_kind": "AuthorityTransferred",
            })
        }

        fn basenames_exact_name_resolver_summary() -> Value {
            json!({
                "chain_id": "base-mainnet",
                "address": "0x0000000000000000000000000000000000000abc",
                "latest_event_kind": "ResolverChanged",
            })
        }

        fn resolution_record_inventory_boundary(logical_name_id: &str, resource_id: Uuid) -> Value {
            json!({
                "logical_name_id": logical_name_id,
                "resource_id": resource_id.to_string(),
                "normalized_event_id": null,
                "event_kind": null,
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 106,
                    "block_hash": "0xhistorysurface",
                    "timestamp": "2024-05-31T16:08:26Z",
                },
            })
        }

        fn resolution_record_inventory_enumeration_basis() -> Value {
            json!({
                "observed_selectors": true,
                "capability_declared_families": true,
                "globally_enumerable": false,
            })
        }

        fn resolution_record_inventory_selectors() -> Value {
            json!([
                {
                    "record_key": "addr:60",
                    "record_family": "addr",
                    "selector_key": "60",
                    "cacheable": true,
                },
                {
                    "record_key": "avatar",
                    "record_family": "avatar",
                    "selector_key": null,
                    "cacheable": true,
                },
                {
                    "record_key": "text:com.twitter",
                    "record_family": "text",
                    "selector_key": "com.twitter",
                    "cacheable": false,
                }
            ])
        }

        fn resolution_record_inventory_explicit_gaps() -> Value {
            json!([
                {
                    "record_key": "contenthash",
                    "record_family": "contenthash",
                    "selector_key": null,
                    "gap_reason": "not_observed_on_current_resolver",
                }
            ])
        }

        fn resolution_record_inventory_unsupported_families() -> Value {
            json!([
                {
                    "record_family": "abi",
                    "unsupported_reason": "resolver_family_pending",
                },
                {
                    "record_family": "pubkey",
                    "unsupported_reason": "resolver_family_pending",
                }
            ])
        }

        fn resolution_record_inventory_last_change() -> Value {
            json!({
                "normalized_event_id": 1200,
                "event_kind": "RecordsChanged",
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 106,
                    "block_hash": "0xhistorysurface",
                    "timestamp": "2024-05-31T16:08:26Z",
                }
            })
        }

        fn resolution_contenthash_value() -> &'static str {
            "ipfs://bafybeigdyrzt5sfp7udm7hu76fx4f2jv4jvgxk5csodx4d6vshv3zysn7u"
        }

        fn resolution_avatar_value() -> &'static str {
            "https://cdn.example.test/alice.png"
        }

        fn resolution_alias_avatar_value() -> &'static str {
            "https://cdn.example.test/alice-via-alias.png"
        }

        fn resolution_record_cache_entries(record_keys: &[&str]) -> Vec<Value> {
            record_keys
                .iter()
                .map(|record_key| match *record_key {
                    "addr:60" => json!({
                        "record_key": "addr:60",
                        "record_family": "addr",
                        "selector_key": "60",
                        "status": "success",
                        "value": {
                            "coin_type": "60",
                            "value": "0x0000000000000000000000000000000000000abc",
                        }
                    }),
                    "avatar" => json!({
                        "record_key": "avatar",
                        "record_family": "avatar",
                        "selector_key": null,
                        "status": "unsupported",
                        "unsupported_reason": "resolver_family_pending",
                    }),
                    "text:com.twitter" => json!({
                        "record_key": "text:com.twitter",
                        "record_family": "text",
                        "selector_key": "com.twitter",
                        "status": "not_found",
                    }),
                    "contenthash" => json!({
                        "record_key": "contenthash",
                        "record_family": "contenthash",
                        "selector_key": null,
                        "status": "not_found",
                    }),
                    unexpected => panic!("unexpected direct ENS record selector {unexpected}"),
                })
                .collect()
        }

        fn resolution_record_inventory_current_row(
            logical_name_id: &str,
            resource_id: Uuid,
        ) -> RecordInventoryCurrentRow {
            RecordInventoryCurrentRow {
                resource_id,
                record_version_boundary: resolution_record_inventory_boundary(
                    logical_name_id,
                    resource_id,
                ),
                enumeration_basis: resolution_record_inventory_enumeration_basis(),
                selectors: resolution_record_inventory_selectors(),
                explicit_gaps: resolution_record_inventory_explicit_gaps(),
                unsupported_families: resolution_record_inventory_unsupported_families(),
                last_change: Some(resolution_record_inventory_last_change()),
                entries: json!(resolution_record_cache_entries(&["addr:60", "avatar"])),
                provenance: json!({
                    "normalized_event_ids": [1200],
                    "derivation_kind": "record_inventory_current_rebuild",
                }),
                coverage: json!({
                    "status": "full",
                    "exhaustiveness": "authoritative",
                    "enumeration_basis": "declared_record_inventory",
                }),
                chain_positions: json!({
                    "ethereum-mainnet": {
                        "chain_id": "ethereum-mainnet",
                        "block_number": 106,
                        "block_hash": "0xhistorysurface",
                        "timestamp": "2024-05-31T16:08:26Z",
                    }
                }),
                canonicality_summary: json!({
                    "status": "finalized",
                    "chains": {
                        "ethereum-mainnet": "finalized",
                    }
                }),
                manifest_version: 7,
                last_recomputed_at: timestamp(1_717_171_718),
            }
        }

        fn resolution_supported_declared_state(
            logical_name_id: &str,
            resource_id: Uuid,
            record_cache_keys: &[&str],
        ) -> Value {
            let record_version_boundary =
                resolution_record_inventory_boundary(logical_name_id, resource_id);
            json!({
                "topology": {
                    "registry_path": [
                        {
                            "logical_name_id": "ens:alice.eth",
                            "namespace": "ens",
                            "normalized_name": "alice.eth",
                            "canonical_display_name": "Alice.eth",
                            "namehash": "namehash:alice.eth",
                            "resource_id": resource_id.to_string(),
                            "binding_kind": "declared_registry_path",
                        }
                    ],
                    "subregistry_path": [],
                    "resolver_path": [
                        {
                            "logical_name_id": "ens:alice.eth",
                            "namespace": "ens",
                            "normalized_name": "alice.eth",
                            "canonical_display_name": "Alice.eth",
                            "resource_id": resource_id.to_string(),
                            "chain_id": "ethereum-mainnet",
                            "address": "0x0000000000000000000000000000000000000abc",
                            "latest_event_kind": "ResolverChanged",
                        }
                    ],
                    "wildcard": {
                        "source": null,
                        "matched_labels": [],
                    },
                    "alias": {
                        "final_target": null,
                        "hops": [],
                    },
                    "version_boundaries": {
                        "topology_version_boundary": record_version_boundary.clone(),
                        "record_version_boundary": record_version_boundary.clone(),
                    },
                    "transport": {
                        "source_chain_id": null,
                        "target_chain_id": null,
                        "contract_address": null,
                        "latest_event_kind": null,
                    },
                },
                "record_inventory": {
                    "record_version_boundary": record_version_boundary.clone(),
                    "enumeration_basis": resolution_record_inventory_enumeration_basis(),
                    "selectors": resolution_record_inventory_selectors(),
                    "explicit_gaps": resolution_record_inventory_explicit_gaps(),
                    "unsupported_families": resolution_record_inventory_unsupported_families(),
                    "last_change": resolution_record_inventory_last_change(),
                },
                "record_cache": {
                    "record_version_boundary": record_version_boundary,
                    "entries": resolution_record_cache_entries(record_cache_keys),
                }
            })
        }

        fn record_selector_identity_tuple(value: &Value) -> (String, String, Option<String>) {
            let selector_key = match value.get("selector_key") {
                Some(Value::Null) => None,
                Some(Value::String(selector_key)) => Some(selector_key.clone()),
                Some(_) => panic!("selector_key must be a string or null"),
                None => panic!("selector_key must be present"),
            };

            (
                value
                    .get("record_key")
                    .and_then(Value::as_str)
                    .expect("record_key must be present")
                    .to_owned(),
                value
                    .get("record_family")
                    .and_then(Value::as_str)
                    .expect("record_family must be present")
                    .to_owned(),
                selector_key,
            )
        }

        fn resolution_unsupported_verified_state(record_keys: &[&str]) -> Value {
            json!({
                "verified_queries": record_keys
                    .iter()
                    .map(|record_key| {
                        json!({
                            "record_key": record_key,
                            "status": "unsupported",
                            "unsupported_reason": "verified resolution entrypoint is not yet supported",
                        })
                    })
                    .collect::<Vec<_>>()
            })
        }

        fn resolution_execution_verified_queries(
            execution_trace_id: Uuid,
            record_keys: &[&str],
        ) -> Value {
            json!(
                record_keys
                    .iter()
                    .map(|record_key| match *record_key {
                        "avatar" => json!({
                            "record_key": "avatar",
                            "status": "success",
                            "value": {
                                "value": resolution_avatar_value(),
                            },
                            "provenance": {
                                "execution_trace_id": execution_trace_id.to_string(),
                            }
                        }),
                        "addr:60" => json!({
                            "record_key": "addr:60",
                            "status": "success",
                            "value": {
                                "coin_type": "60",
                                "value": "0x00000000000000000000000000000000000000aa",
                            },
                            "provenance": {
                                "execution_trace_id": execution_trace_id.to_string(),
                            }
                        }),
                        "text:com.twitter" => json!({
                            "record_key": "text:com.twitter",
                            "status": "not_found",
                            "failure_reason": "no_text_record",
                            "provenance": {
                                "execution_trace_id": execution_trace_id.to_string(),
                            }
                        }),
                        "contenthash" => json!({
                            "record_key": "contenthash",
                            "status": "success",
                            "value": {
                                "value": resolution_contenthash_value(),
                            },
                            "provenance": {
                                "execution_trace_id": execution_trace_id.to_string(),
                            }
                        }),
                        unexpected => panic!(
                            "unexpected persisted verified resolution selector {unexpected}"
                        ),
                    })
                    .collect::<Vec<_>>()
            )
        }

        fn resolution_alias_only_verified_queries(
            execution_trace_id: Uuid,
            record_keys: &[&str],
        ) -> Value {
            json!(
                record_keys
                    .iter()
                    .map(|record_key| match *record_key {
                        "avatar" => json!({
                            "record_key": "avatar",
                            "status": "success",
                            "value": {
                                "value": resolution_alias_avatar_value(),
                            },
                            "provenance": {
                                "execution_trace_id": execution_trace_id.to_string(),
                            }
                        }),
                        "text:com.twitter" => json!({
                            "record_key": "text:com.twitter",
                            "status": "success",
                            "value": {
                                "value": "@alice-via-alias",
                            },
                            "provenance": {
                                "execution_trace_id": execution_trace_id.to_string(),
                            }
                        }),
                        unexpected => panic!(
                            "unexpected persisted alias-only verified resolution selector {unexpected}"
                        ),
                    })
                    .collect::<Vec<_>>()
            )
        }

        fn resolution_execution_trace(
            execution_trace_id: Uuid,
            request_key: &str,
            request_record_keys: &[&str],
            verified_queries: Value,
        ) -> ExecutionTrace {
            ExecutionTrace {
                execution_trace_id,
                request_type: "verified_resolution".to_owned(),
                request_key: request_key.to_owned(),
                namespace: "ens".to_owned(),
                chain_context: json!({
                    "requested_positions": [{
                        "chain_id": "ethereum-mainnet",
                        "block_number": 106,
                        "block_hash": "0xhistorysurface",
                    }],
                }),
                manifest_context: json!({
                    "manifest_versions": [{
                        "source_family": "ens_execution",
                        "manifest_version": 5,
                    }]
                }),
                contracts_called: json!([
                    {
                        "chain_id": "ethereum-mainnet",
                        "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                        "selector": "0x9061b923",
                    }
                ]),
                gateway_digests: json!([]),
                final_payload: Some(json!({
                    "verified_queries": verified_queries.clone(),
                })),
                failure_payload: None,
                request_metadata: json!({
                    "surface": "alice.eth",
                    "record_keys": request_record_keys,
                    "entrypoint": "universal_resolver",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                }),
                finished_at: Some(timestamp(1_717_171_900)),
                steps: vec![
                    ExecutionTraceStep {
                        step_index: 0,
                        step_kind: "load_declared_topology".to_owned(),
                        input_digest: Some("sha256:topology-input".to_owned()),
                        output_digest: Some("sha256:topology-output".to_owned()),
                        latency_ms: Some(4),
                        canonicality_dependency: json!({
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized",
                            }
                        }),
                        step_payload: json!({
                            "entrypoint": "universal_resolver",
                            "resolver": "0x0000000000000000000000000000000000000abc",
                        }),
                    },
                    ExecutionTraceStep {
                        step_index: 1,
                        step_kind: "call_universal_resolver".to_owned(),
                        input_digest: Some("sha256:resolver-input".to_owned()),
                        output_digest: Some("sha256:resolver-output".to_owned()),
                        latency_ms: Some(28),
                        canonicality_dependency: json!({
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized",
                            }
                        }),
                        step_payload: json!({
                            "name": "alice.eth",
                            "record_count": request_record_keys.len(),
                        }),
                    },
                ],
            }
        }

        fn resolution_execution_outcome(
            execution_trace_id: Uuid,
            cache_key: ExecutionCacheKey,
            verified_queries: Value,
        ) -> ExecutionOutcome {
            ExecutionOutcome {
                cache_key,
                execution_trace_id,
                request_type: "verified_resolution".to_owned(),
                namespace: "ens".to_owned(),
                outcome_payload: Some(json!({
                    "verified_queries": verified_queries,
                })),
                failure_payload: None,
                finished_at: timestamp(1_717_171_900),
            }
        }

        fn resolution_execution_summary(execution_trace_id: Uuid, resource_id: Uuid) -> Value {
            json!({
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "ens_execution",
                    "role": "universal_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                },
                "resolver_discovery_path": [
                    {
                        "logical_name_id": "ens:alice.eth",
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged",
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": [],
                },
                "alias": {
                    "final_target": null,
                    "hops": [],
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized",
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_universal_resolver",
                        "input_digest": "sha256:resolver-input",
                        "output_digest": "sha256:resolver-output",
                        "latency": 28,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbinding",
                                "block_number": 21_000_003,
                                "state": "finalized",
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900)),
            })
        }

        fn basenames_resolution_execution_request_key(records: &[&str]) -> String {
            let mut records = records
                .iter()
                .map(|record| (*record).to_owned())
                .collect::<Vec<_>>();
            records.sort_unstable();
            format!("basenames:alice.base.eth:{}", records.join(","))
        }

        fn requested_chain_positions_from_name_current(chain_positions: &Value) -> Value {
            let mut positions = chain_positions
                .as_object()
                .expect("name_current.chain_positions must be an object")
                .values()
                .map(|position| {
                    json!({
                        "chain_id": position
                            .get("chain_id")
                            .and_then(Value::as_str)
                            .expect("chain_position.chain_id must be present"),
                        "block_number": position
                            .get("block_number")
                            .and_then(Value::as_i64)
                            .expect("chain_position.block_number must be present"),
                        "block_hash": position
                            .get("block_hash")
                            .and_then(Value::as_str)
                            .expect("chain_position.block_hash must be present"),
                    })
                })
                .collect::<Vec<_>>();
            positions.sort_by(|left, right| {
                left.get("chain_id")
                    .and_then(Value::as_str)
                    .cmp(&right.get("chain_id").and_then(Value::as_str))
            });
            Value::Array(positions)
        }

        fn basenames_execution_manifest_version() -> Value {
            json!({
                "source_family": "basenames_execution",
                "manifest_version": 2,
                "chain": "ethereum-mainnet",
                "deployment_epoch": "basenames_v1",
            })
        }

        fn append_basenames_execution_manifest_version(
            name_row: &mut bigname_storage::NameCurrentRow,
        ) {
            let manifest_versions = name_row.provenance["manifest_versions"]
                .as_array_mut()
                .expect("name_current.provenance.manifest_versions must be an array");
            if manifest_versions.iter().any(|item| {
                item.get("source_family").and_then(Value::as_str) == Some("basenames_execution")
                    && item.get("manifest_version").and_then(Value::as_i64) == Some(2)
            }) {
                return;
            }
            manifest_versions.push(basenames_execution_manifest_version());
        }

        fn insert_basenames_supported_ethereum_position(
            name_row: &mut bigname_storage::NameCurrentRow,
        ) {
            let chain_positions = name_row
                .chain_positions
                .as_object_mut()
                .expect("name_current.chain_positions must be an object");
            chain_positions.insert(
                "ethereum".to_owned(),
                json!({
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_000_100,
                    "block_hash": "0xbasenamesl1",
                    "timestamp": "2026-04-17T00:01:40Z",
                }),
            );
        }

        fn basenames_resolution_execution_trace(
            execution_trace_id: Uuid,
            request_key: &str,
            request_record_keys: &[&str],
            requested_chain_positions: Value,
            verified_queries: Value,
        ) -> ExecutionTrace {
            ExecutionTrace {
                execution_trace_id,
                request_type: "verified_resolution".to_owned(),
                request_key: request_key.to_owned(),
                namespace: "basenames".to_owned(),
                chain_context: json!({
                    "requested_positions": requested_chain_positions,
                }),
                manifest_context: json!({
                    "manifest_versions": [{
                        "source_family": "basenames_execution",
                        "manifest_version": 2,
                    }]
                }),
                contracts_called: json!([
                    {
                        "chain_id": "ethereum-mainnet",
                        "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                        "selector": "0x9061b923",
                    }
                ]),
                gateway_digests: json!(["sha256:ccip-request", "sha256:ccip-response"]),
                final_payload: Some(json!({
                    "verified_queries": verified_queries.clone(),
                })),
                failure_payload: None,
                request_metadata: json!({
                    "surface": "alice.base.eth",
                    "record_keys": request_record_keys,
                    "entrypoint": "l1_resolver",
                    "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                    "transport": {
                        "source_chain_id": "base-mainnet",
                        "target_chain_id": "ethereum-mainnet",
                        "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                        "latest_event_kind": null,
                    }
                }),
                finished_at: Some(timestamp(1_717_171_900)),
                steps: vec![
                    ExecutionTraceStep {
                        step_index: 0,
                        step_kind: "load_declared_topology".to_owned(),
                        input_digest: Some("sha256:topology-input".to_owned()),
                        output_digest: Some("sha256:topology-output".to_owned()),
                        latency_ms: Some(4),
                        canonicality_dependency: json!({
                            "base-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized",
                            }
                        }),
                        step_payload: json!({
                            "entrypoint": "l1_resolver",
                            "resolver": "0x0000000000000000000000000000000000000abc",
                        }),
                    },
                    ExecutionTraceStep {
                        step_index: 1,
                        step_kind: "call_l1_resolver".to_owned(),
                        input_digest: Some("sha256:l1-input".to_owned()),
                        output_digest: Some("sha256:l1-output".to_owned()),
                        latency_ms: Some(17),
                        canonicality_dependency: json!({
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized",
                            }
                        }),
                        step_payload: json!({
                            "name": "alice.base.eth",
                            "record_count": request_record_keys.len(),
                        }),
                    },
                    ExecutionTraceStep {
                        step_index: 2,
                        step_kind: "ccip_offchain_lookup".to_owned(),
                        input_digest: Some("sha256:ccip-input".to_owned()),
                        output_digest: Some("sha256:ccip-output".to_owned()),
                        latency_ms: Some(29),
                        canonicality_dependency: json!({
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized",
                            }
                        }),
                        step_payload: json!({
                            "gateway_digest": "sha256:ccip-request",
                        }),
                    },
                    ExecutionTraceStep {
                        step_index: 3,
                        step_kind: "resolve_with_proof".to_owned(),
                        input_digest: Some("sha256:proof-input".to_owned()),
                        output_digest: Some("sha256:proof-output".to_owned()),
                        latency_ms: Some(11),
                        canonicality_dependency: json!({
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized",
                            }
                        }),
                        step_payload: json!({
                            "proof_kind": "signature",
                        }),
                    },
                ],
            }
        }

        fn basenames_resolution_execution_outcome(
            execution_trace_id: Uuid,
            request_key: &str,
            requested_chain_positions: Value,
            manifest_versions: Value,
            record_version_boundary: Value,
            verified_queries: Value,
        ) -> ExecutionOutcome {
            ExecutionOutcome {
                cache_key: ExecutionCacheKey {
                    request_key: request_key.to_owned(),
                    requested_chain_positions,
                    manifest_versions,
                    topology_version_boundary: record_version_boundary.clone(),
                    record_version_boundary,
                },
                execution_trace_id,
                request_type: "verified_resolution".to_owned(),
                namespace: "basenames".to_owned(),
                outcome_payload: Some(json!({
                    "verified_queries": verified_queries,
                })),
                failure_payload: None,
                finished_at: timestamp(1_717_171_900),
            }
        }

        fn basenames_resolution_execution_summary(
            execution_trace_id: Uuid,
            logical_name_id: &str,
            resource_id: Uuid,
        ) -> Value {
            json!({
                "execution_trace_id": execution_trace_id.to_string(),
                "selected_entrypoint": {
                    "source_family": "basenames_execution",
                    "role": "l1_resolver",
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xde9049636F4a1dfE0a64d1bFe3155C0A14C54F31",
                },
                "resolver_discovery_path": [
                    {
                        "logical_name_id": logical_name_id,
                        "namespace": "basenames",
                        "normalized_name": "alice.base.eth",
                        "canonical_display_name": "Alice.base.eth",
                        "resource_id": resource_id.to_string(),
                        "chain_id": "base-mainnet",
                        "address": "0x0000000000000000000000000000000000000abc",
                        "latest_event_kind": "ResolverChanged",
                    }
                ],
                "wildcard": {
                    "source": null,
                    "matched_labels": [],
                },
                "alias": {
                    "final_target": null,
                    "hops": [],
                },
                "steps": [
                    {
                        "step_index": 0,
                        "step_kind": "load_declared_topology",
                        "input_digest": "sha256:topology-input",
                        "output_digest": "sha256:topology-output",
                        "latency": 4,
                        "canonicality_dependency": {
                            "base-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized",
                            }
                        }
                    },
                    {
                        "step_index": 1,
                        "step_kind": "call_l1_resolver",
                        "input_digest": "sha256:l1-input",
                        "output_digest": "sha256:l1-output",
                        "latency": 17,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized",
                            }
                        }
                    },
                    {
                        "step_index": 2,
                        "step_kind": "ccip_offchain_lookup",
                        "input_digest": "sha256:ccip-input",
                        "output_digest": "sha256:ccip-output",
                        "latency": 29,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized",
                            }
                        }
                    },
                    {
                        "step_index": 3,
                        "step_kind": "resolve_with_proof",
                        "input_digest": "sha256:proof-input",
                        "output_digest": "sha256:proof-output",
                        "latency": 11,
                        "canonicality_dependency": {
                            "ethereum-mainnet": {
                                "block_hash": "0xbase-binding",
                                "block_number": 100,
                                "state": "finalized",
                            }
                        }
                    }
                ],
                "finished_at": format_timestamp(timestamp(1_717_171_900)),
            })
        }

        fn resolution_wildcard_source(
            wildcard_source_logical_name_id: &str,
            wildcard_source_resource_id: Uuid,
        ) -> Value {
            json!({
                "logical_name_id": wildcard_source_logical_name_id,
                "namespace": "ens",
                "normalized_name": "eth",
                "canonical_display_name": "Eth",
                "namehash": "namehash:eth",
                "resource_id": wildcard_source_resource_id.to_string(),
                "binding_kind": "observed_wildcard_path",
            })
        }

        fn resolution_wildcard_projected_topology(
            logical_name_id: &str,
            resource_id: Uuid,
            wildcard_source_logical_name_id: &str,
            wildcard_source_resource_id: Uuid,
        ) -> Value {
            let wildcard_source = resolution_wildcard_source(
                wildcard_source_logical_name_id,
                wildcard_source_resource_id,
            );
            let wildcard_boundary = resolution_record_inventory_boundary(
                wildcard_source_logical_name_id,
                wildcard_source_resource_id,
            );

            json!({
                "registry_path": [
                    {
                        "logical_name_id": logical_name_id,
                        "namespace": "ens",
                        "normalized_name": "alice.eth",
                        "canonical_display_name": "Alice.eth",
                        "namehash": "namehash:alice.eth",
                        "resource_id": resource_id.to_string(),
                        "binding_kind": "observed_wildcard_path",
                    }
                ],
                "subregistry_path": [],
                "resolver_path": [
                    {
                        "logical_name_id": wildcard_source_logical_name_id,
                        "namespace": "ens",
                        "normalized_name": "eth",
                        "canonical_display_name": "Eth",
                        "resource_id": wildcard_source_resource_id.to_string(),
                        "chain_id": "ethereum-mainnet",
                        "address": "0x0000000000000000000000000000000000000def",
                        "latest_event_kind": "ResolverChanged",
                    }
                ],
                "wildcard": {
                    "source": wildcard_source,
                    "matched_labels": ["alice"],
                },
                "alias": {
                    "final_target": null,
                    "hops": [],
                },
                "version_boundaries": {
                    "topology_version_boundary": wildcard_boundary.clone(),
                    "record_version_boundary": wildcard_boundary,
                },
                "transport": {
                    "source_chain_id": null,
                    "target_chain_id": null,
                    "contract_address": null,
                    "latest_event_kind": null,
                },
            })
        }

        fn resolution_wildcard_execution_summary(
            execution_trace_id: Uuid,
            wildcard_source_logical_name_id: &str,
            wildcard_source_resource_id: Uuid,
        ) -> Value {
            let wildcard_source = resolution_wildcard_source(
                wildcard_source_logical_name_id,
                wildcard_source_resource_id,
            );
            let mut execution =
                resolution_execution_summary(execution_trace_id, wildcard_source_resource_id);

            execution["resolver_discovery_path"] = json!([
                {
                    "logical_name_id": wildcard_source_logical_name_id,
                    "namespace": "ens",
                    "normalized_name": "eth",
                    "canonical_display_name": "Eth",
                    "resource_id": wildcard_source_resource_id.to_string(),
                    "chain_id": "ethereum-mainnet",
                    "address": "0x0000000000000000000000000000000000000def",
                    "latest_event_kind": "ResolverChanged",
                }
            ]);
            execution["wildcard"] = json!({
                "source": wildcard_source,
                "matched_labels": ["alice"],
            });
            execution["steps"]
                .as_array_mut()
                .expect("resolution execution summary must expose steps")
                .push(json!({
                    "step_index": 2,
                    "step_kind": "call_wildcard_resolver",
                    "input_digest": "sha256:wildcard-input",
                    "output_digest": "sha256:wildcard-output",
                    "latency": 19,
                    "canonicality_dependency": {
                        "ethereum-mainnet": {
                            "block_hash": "0xabc123",
                            "block_number": 21_000_000,
                            "state": "canonical",
                        }
                    }
                }));

            execution
        }

        #[derive(Clone, Copy, Debug)]
        enum UnsupportedEnsVerifiedResolutionPathCase {
            NonAliasAncestorSelected,
            TransportAssisted,
        }

        impl UnsupportedEnsVerifiedResolutionPathCase {
            fn execution_trace_id(self) -> Uuid {
                match self {
                    Self::NonAliasAncestorSelected => {
                        Uuid::from_u128(0x0e7ec7ace00000000000000000000027)
                    }
                    Self::TransportAssisted => Uuid::from_u128(0x0e7ec7ace00000000000000000000028),
                }
            }

            fn label(self) -> &'static str {
                match self {
                    Self::NonAliasAncestorSelected => "non-alias ancestor-selected",
                    Self::TransportAssisted => "transport-assisted",
                }
            }

            fn apply_to_name_row(self, row: &mut bigname_storage::NameCurrentRow) {
                let summary = row
                    .declared_summary
                    .as_object_mut()
                    .expect("resolution negative fixture requires object declared_summary");
                summary.insert(
                    "topology".to_owned(),
                    self.expected_topology(&row.logical_name_id, row.resource_id),
                );

                if let Some(resolver) = summary.get_mut("resolver").and_then(Value::as_object_mut) {
                    resolver.insert(
                        "address".to_owned(),
                        Value::String(
                            match self {
                                Self::NonAliasAncestorSelected => {
                                    "0x0000000000000000000000000000000000000def"
                                }
                                Self::TransportAssisted => {
                                    "0x0000000000000000000000000000000000000abc"
                                }
                            }
                            .to_owned(),
                        ),
                    );
                }
            }

            fn apply_to_trace(self, trace: &mut ExecutionTrace) {
                let metadata = trace
                    .request_metadata
                    .as_object_mut()
                    .expect("resolution negative fixture requires request_metadata object");
                match self {
                    Self::NonAliasAncestorSelected => {
                        metadata.insert(
                            "resolver_path".to_owned(),
                            json!([{
                                "logical_name_id": "ens:eth",
                                "namespace": "ens",
                                "normalized_name": "eth",
                                "canonical_display_name": "eth",
                                "resource_id": Uuid::from_u128(0x2210).to_string(),
                                "chain_id": "ethereum-mainnet",
                                "address": "0x0000000000000000000000000000000000000def",
                                "latest_event_kind": "ResolverChanged",
                            }]),
                        );
                    }
                    Self::TransportAssisted => {
                        metadata.insert(
                            "transport".to_owned(),
                            resolution_transport_assisted_transport(),
                        );
                    }
                }
            }

            fn expected_topology(self, logical_name_id: &str, resource_id: Option<Uuid>) -> Value {
                let resource_id = resource_id
                    .expect("resolution negative fixture requires an exact-surface resource_id");
                match self {
                    Self::NonAliasAncestorSelected => {
                        resolution_non_alias_ancestor_selected_topology(
                            logical_name_id,
                            resource_id,
                        )
                    }
                    Self::TransportAssisted => {
                        resolution_transport_assisted_topology(logical_name_id, resource_id)
                    }
                }
            }
        }

        struct UnsupportedEnsVerifiedResolutionFixture {
            logical_name_id: &'static str,
            resource_id: Uuid,
        }

        async fn run_resolution_negative_verified_path_case(
            path_case: UnsupportedEnsVerifiedResolutionPathCase,
        ) -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let fixture =
                seed_unsupported_ens_verified_resolution_fixture(&database, path_case).await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=both&records=avatar,text:com.twitter",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| format!("{} mixed resolution request failed", path_case.label()))?;

            assert_eq!(
                response.status(),
                StatusCode::OK,
                "{} mixed resolution should keep the declared envelope and explicit unsupported verified results",
                path_case.label()
            );

            let payload: ResolutionResponse = read_json(response).await?;
            let declared_state = payload
                .declared_state
                .as_ref()
                .context("mixed negative resolution response must include declared_state")?;
            let topology = declared_state
                .get("topology")
                .context("mixed negative resolution response must include topology")?;

            assert_negative_verified_resolution_topology(
                path_case,
                topology,
                fixture.logical_name_id,
            );
            assert_eq!(
                payload.provenance.get("execution_trace_id"),
                Some(&Value::Null),
                "{} mixed resolution must not surface the persisted execution trace id",
                path_case.label()
            );
            assert_eq!(
                payload.verified_state,
                Some(resolution_unsupported_verified_state(&[
                    "avatar",
                    "text:com.twitter",
                ])),
                "{} mixed resolution must keep selector-local unsupported results",
                path_case.label()
            );

            let expected_topology =
                path_case.expected_topology(fixture.logical_name_id, Some(fixture.resource_id));
            assert_eq!(
                topology,
                &expected_topology,
                "{} mixed resolution topology should stay visible while verified resolution remains unsupported",
                path_case.label()
            );

            database.cleanup().await?;
            Ok(())
        }

        async fn run_resolution_execution_explain_negative_verified_path_case(
            path_case: UnsupportedEnsVerifiedResolutionPathCase,
        ) -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let _fixture =
                seed_unsupported_ens_verified_resolution_fixture(&database, path_case).await?;

            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=avatar,text:com.twitter",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!("{} resolution execution explain request failed", path_case.label())
                })?;

            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "{} resolution execution explain should stay outside the shipped public explain surface",
                path_case.label()
            );

            let payload: ErrorResponse = read_json(response).await?;
            assert_eq!(payload.error.code, "not_found");
            assert_eq!(
                payload.error.message,
                "persisted resolution execution explain was not found for name alice.eth in namespace ens"
            );
            assert!(
                payload.error.details.is_empty(),
                "{} resolution execution explain should not add extra error details",
                path_case.label()
            );

            database.cleanup().await?;
            Ok(())
        }

        async fn seed_unsupported_ens_verified_resolution_fixture(
            database: &HarnessDatabase,
            path_case: UnsupportedEnsVerifiedResolutionPathCase,
        ) -> Result<UnsupportedEnsVerifiedResolutionFixture> {
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let supported_name_row =
                bigname_storage::load_name_current(&database.pool, logical_name_id)
                    .await?
                    .context("resolution negative fixture requires an exact-name current row")?;
            let records =
                parse_resolution_record_keys(Some("text:com.twitter"), ResolutionMode::Verified)
                    .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &supported_name_row,
                &records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();

            database
                .insert_record_inventory_current_row(record_inventory_row.clone())
                .await?;

            let mut name_row = supported_name_row.clone();
            path_case.apply_to_name_row(&mut name_row);
            database.insert_name_current_row(name_row.clone()).await?;

            let persisted_verified_queries = resolution_execution_verified_queries(
                path_case.execution_trace_id(),
                &["avatar", "text:com.twitter"],
            );

            let mut trace = resolution_execution_trace(
                path_case.execution_trace_id(),
                &request_key,
                &["avatar", "text:com.twitter"],
                persisted_verified_queries.clone(),
            );
            path_case.apply_to_trace(&mut trace);

            upsert_execution_trace(&database.pool, &trace).await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(
                    path_case.execution_trace_id(),
                    cache_key,
                    persisted_verified_queries,
                ),
            )
            .await?;

            Ok(UnsupportedEnsVerifiedResolutionFixture {
                logical_name_id,
                resource_id,
            })
        }

        fn resolution_non_alias_ancestor_selected_topology(
            logical_name_id: &str,
            resource_id: Uuid,
        ) -> Value {
            let mut topology = resolution_supported_declared_state(
                logical_name_id,
                resource_id,
                &["avatar", "text:com.twitter"],
            )
            .get("topology")
            .cloned()
            .expect("supported declared resolution state must include topology");

            let topology_object = topology
                .as_object_mut()
                .expect("resolution topology must be an object");
            topology_object.insert(
                "resolver_path".to_owned(),
                json!([{
                    "logical_name_id": "ens:eth",
                    "namespace": "ens",
                    "normalized_name": "eth",
                    "canonical_display_name": "eth",
                    "resource_id": Uuid::from_u128(0x2210).to_string(),
                    "chain_id": "ethereum-mainnet",
                    "address": "0x0000000000000000000000000000000000000def",
                    "latest_event_kind": "ResolverChanged",
                }]),
            );
            topology_object.insert(
                "alias".to_owned(),
                json!({
                    "final_target": null,
                    "hops": [],
                }),
            );
            topology_object.insert(
                "wildcard".to_owned(),
                json!({
                    "source": null,
                    "matched_labels": [],
                }),
            );
            topology_object.insert(
                "transport".to_owned(),
                json!({
                    "source_chain_id": null,
                    "target_chain_id": null,
                    "contract_address": null,
                    "latest_event_kind": null,
                }),
            );
            topology
        }

        fn resolution_transport_assisted_transport() -> Value {
            json!({
                "source_chain_id": "base-mainnet",
                "target_chain_id": "ethereum-mainnet",
                "contract_address": "0x0000000000000000000000000000000000000fed",
                "latest_event_kind": "ResolverTransportUpdated",
            })
        }

        fn resolution_transport_assisted_topology(
            logical_name_id: &str,
            resource_id: Uuid,
        ) -> Value {
            let mut topology = resolution_supported_declared_state(
                logical_name_id,
                resource_id,
                &["avatar", "text:com.twitter"],
            )
            .get("topology")
            .cloned()
            .expect("supported declared resolution state must include topology");

            let topology_object = topology
                .as_object_mut()
                .expect("resolution topology must be an object");
            topology_object.insert(
                "alias".to_owned(),
                json!({
                    "final_target": null,
                    "hops": [],
                }),
            );
            topology_object.insert(
                "wildcard".to_owned(),
                json!({
                    "source": null,
                    "matched_labels": [],
                }),
            );
            topology_object.insert(
                "transport".to_owned(),
                resolution_transport_assisted_transport(),
            );
            topology
        }

        fn assert_negative_verified_resolution_topology(
            path_case: UnsupportedEnsVerifiedResolutionPathCase,
            topology: &Value,
            logical_name_id: &str,
        ) {
            assert_eq!(
                topology.get("wildcard"),
                Some(&json!({
                    "source": null,
                    "matched_labels": [],
                })),
                "{} topology should explicitly stay outside wildcard-derived coverage in this slice",
                path_case.label()
            );
            assert_eq!(
                topology.get("alias"),
                Some(&json!({
                    "final_target": null,
                    "hops": [],
                })),
                "{} topology should keep alias rewriting out of this negative case",
                path_case.label()
            );

            match path_case {
                UnsupportedEnsVerifiedResolutionPathCase::NonAliasAncestorSelected => {
                    assert_eq!(
                        topology.get("transport"),
                        Some(&json!({
                            "source_chain_id": null,
                            "target_chain_id": null,
                            "contract_address": null,
                            "latest_event_kind": null,
                        })),
                        "ancestor-selected topology should not rely on transport participation",
                    );
                    assert_eq!(
                        topology
                            .get("resolver_path")
                            .and_then(Value::as_array)
                            .and_then(|resolver_path| resolver_path.first())
                            .and_then(|hop| hop.get("logical_name_id"))
                            .and_then(Value::as_str),
                        Some("ens:eth"),
                        "ancestor-selected topology should expose the selected ancestor hop",
                    );
                    assert_ne!(
                        topology
                            .get("resolver_path")
                            .and_then(Value::as_array)
                            .and_then(|resolver_path| resolver_path.first())
                            .and_then(|hop| hop.get("logical_name_id"))
                            .and_then(Value::as_str),
                        Some(logical_name_id),
                        "ancestor-selected topology must not collapse back to the request surface",
                    );
                }
                UnsupportedEnsVerifiedResolutionPathCase::TransportAssisted => {
                    assert_eq!(
                        topology
                            .get("resolver_path")
                            .and_then(Value::as_array)
                            .and_then(|resolver_path| resolver_path.first())
                            .and_then(|hop| hop.get("logical_name_id"))
                            .and_then(Value::as_str),
                        Some(logical_name_id),
                        "transport-assisted topology should stay exact-surface on the resolver path",
                    );
                    assert_eq!(
                        topology.get("transport"),
                        Some(&resolution_transport_assisted_transport()),
                        "transport-assisted topology should expose the participating compatibility transport",
                    );
                }
            }
        }

        fn primary_name_execution_requested_chain_positions() -> Value {
            json!([{
                "chain_id": "ethereum-mainnet",
                "block_number": 21_000_010,
                "block_hash": "0xprimary",
            }])
        }

        fn primary_name_execution_manifest_versions() -> Value {
            json!([{
                "manifest_version": 3,
                "source_family": "ens_v1_registry",
            }])
        }

        fn primary_name_execution_request_key(
            namespace: &str,
            address: &str,
            coin_type: &str,
        ) -> String {
            format!("{namespace}:{}:{coin_type}", address.to_ascii_lowercase())
        }

        fn primary_name_verified_success(
            logical_name_id: &str,
            normalized_name: &str,
            canonical_display_name: &str,
            namehash: &str,
            resource_id: Uuid,
        ) -> Value {
            json!({
                "status": "success",
                "name": {
                    "logical_name_id": logical_name_id,
                    "namespace": "ens",
                    "normalized_name": normalized_name,
                    "canonical_display_name": canonical_display_name,
                    "namehash": namehash,
                    "resource_id": resource_id.to_string(),
                    "binding_kind": "declared_registry_path",
                }
            })
        }

        fn primary_name_verified_mismatch(
            logical_name_id: &str,
            normalized_name: &str,
            canonical_display_name: &str,
            namehash: &str,
            resource_id: Uuid,
            failure_reason: &str,
        ) -> Value {
            let mut payload = primary_name_verified_success(
                logical_name_id,
                normalized_name,
                canonical_display_name,
                namehash,
                resource_id,
            );
            let object = payload
                .as_object_mut()
                .expect("verified primary-name payload must be an object");
            object.insert("status".to_owned(), Value::String("mismatch".to_owned()));
            object.insert(
                "failure_reason".to_owned(),
                Value::String(failure_reason.to_owned()),
            );
            payload
        }

        fn primary_name_execution_trace(
            execution_trace_id: Uuid,
            namespace: &str,
            address: &str,
            coin_type: &str,
            verified_primary_name: Value,
            finished_at: OffsetDateTime,
        ) -> ExecutionTrace {
            let normalized_address = address.to_ascii_lowercase();
            ExecutionTrace {
                execution_trace_id,
                request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
                request_key: primary_name_execution_request_key(
                    namespace,
                    &normalized_address,
                    coin_type,
                ),
                namespace: namespace.to_owned(),
                chain_context: json!({
                    "requested_positions": primary_name_execution_requested_chain_positions(),
                }),
                manifest_context: json!({
                    "manifest_versions": primary_name_execution_manifest_versions(),
                }),
                contracts_called: json!([{
                    "chain_id": "ethereum-mainnet",
                    "contract_address": "0xeEeEEEeE14D718C2B47D9923Deab1335E144EeEe",
                    "selector": "0x9061b923",
                }]),
                gateway_digests: json!([]),
                final_payload: Some(json!({
                    "verified_primary_name": verified_primary_name.clone(),
                })),
                failure_payload: None,
                request_metadata: json!({
                    "normalized_address": normalized_address,
                    "coin_type": coin_type,
                    "namespace": namespace,
                }),
                finished_at: Some(finished_at),
                steps: vec![ExecutionTraceStep {
                    step_index: 0,
                    step_kind: "call_universal_resolver".to_owned(),
                    input_digest: Some("sha256:primary-input".to_owned()),
                    output_digest: Some("sha256:primary-output".to_owned()),
                    latency_ms: Some(14),
                    canonicality_dependency: json!({
                        "ethereum-mainnet": {
                            "block_hash": "0xprimary",
                            "block_number": 21_000_010,
                            "state": "finalized",
                        }
                    }),
                    step_payload: json!({
                        "address": normalized_address,
                        "coin_type": coin_type,
                    }),
                }],
            }
        }

        fn primary_name_execution_outcome(
            execution_trace_id: Uuid,
            namespace: &str,
            address: &str,
            coin_type: &str,
            verified_primary_name: Value,
            finished_at: OffsetDateTime,
            topology_version_boundary: Value,
            record_version_boundary: Value,
        ) -> ExecutionOutcome {
            let normalized_address = address.to_ascii_lowercase();
            ExecutionOutcome {
                cache_key: ExecutionCacheKey {
                    request_key: primary_name_execution_request_key(
                        namespace,
                        &normalized_address,
                        coin_type,
                    ),
                    requested_chain_positions: primary_name_execution_requested_chain_positions(),
                    manifest_versions: primary_name_execution_manifest_versions(),
                    topology_version_boundary,
                    record_version_boundary,
                },
                execution_trace_id,
                request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE.to_owned(),
                namespace: namespace.to_owned(),
                outcome_payload: Some(json!({
                    "verified_primary_name": verified_primary_name,
                })),
                failure_payload: None,
                finished_at,
            }
        }

        fn primary_name_shared_topology_boundary() -> Value {
            json!({
                "logical_name_id": "ens:alice.eth",
                "resource_id": Uuid::from_u128(0x0e7ec7ace0000000000000000000aca1).to_string(),
                "normalized_event_id": 1510,
                "event_kind": "ResolverChanged",
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_300_010,
                    "block_hash": "0xabd010",
                    "timestamp": "2024-06-04T00:00:27Z",
                },
            })
        }

        fn primary_name_shared_record_boundary() -> Value {
            json!({
                "logical_name_id": "ens:alice.eth",
                "resource_id": Uuid::from_u128(0x0e7ec7ace0000000000000000000aca2).to_string(),
                "normalized_event_id": 1520,
                "event_kind": "RecordsChanged",
                "chain_position": {
                    "chain_id": "ethereum-mainnet",
                    "block_number": 21_300_011,
                    "block_hash": "0xabd011",
                    "timestamp": "2024-06-04T00:00:28Z",
                },
            })
        }

        async fn seed_primary_name_tuple_anchor(
            database: &HarnessDatabase,
            address: &str,
            coin_type: &str,
        ) -> Result<()> {
            database
                .seed_primary_name_reverse_changed(address, coin_type)
                .await?;
            database
                .rebuild_primary_names_current(address, "ens", coin_type)
                .await?;
            Ok(())
        }

        #[derive(Clone, Copy, Debug)]
        enum PersistedResolutionInvalidation {
            Manifest,
            Topology,
            Record,
        }

        impl PersistedResolutionInvalidation {
            fn execution_trace_id(self) -> Uuid {
                match self {
                    Self::Manifest => Uuid::from_u128(0x0e7ec7ace00000000000000000000031),
                    Self::Topology => Uuid::from_u128(0x0e7ec7ace00000000000000000000032),
                    Self::Record => Uuid::from_u128(0x0e7ec7ace00000000000000000000033),
                }
            }

            fn label(self) -> &'static str {
                match self {
                    Self::Manifest => "manifest invalidation",
                    Self::Topology => "topology boundary invalidation",
                    Self::Record => "record boundary invalidation",
                }
            }
        }

        struct PersistedResolutionExecutionFixture {
            logical_name_id: &'static str,
            resource_id: Uuid,
            execution_trace_id: Uuid,
            cache_key: ExecutionCacheKey,
        }

        async fn run_resolution_execution_invalidation_case(
            invalidation: PersistedResolutionInvalidation,
        ) -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let fixture = seed_persisted_resolution_execution_fixture(
                &database,
                invalidation.execution_trace_id(),
            )
            .await?;

            let mixed_before_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=both&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed resolution request failed before invalidation")?;
            let explain_before_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution execution explain request failed before invalidation")?;

            assert_eq!(mixed_before_response.status(), StatusCode::OK);
            assert_eq!(explain_before_response.status(), StatusCode::OK);

            let mixed_before_payload: ResolutionResponse = read_json(mixed_before_response).await?;
            let explain_before_payload: ResolutionResponse =
                read_json(explain_before_response).await?;
            let expected_declared_state = resolution_supported_declared_state(
                fixture.logical_name_id,
                fixture.resource_id,
                &["text:com.twitter", "addr:60"],
            );
            let expected_verified_queries = resolution_execution_verified_queries(
                fixture.execution_trace_id,
                &["text:com.twitter", "addr:60"],
            );

            assert_eq!(
                mixed_before_payload.declared_state.as_ref(),
                Some(&expected_declared_state)
            );
            assert_eq!(
                mixed_before_payload.provenance.get("execution_trace_id"),
                Some(&Value::String(fixture.execution_trace_id.to_string()))
            );
            assert_eq!(
                mixed_before_payload.verified_state,
                Some(json!({
                    "verified_queries": expected_verified_queries.clone(),
                }))
            );
            assert_eq!(
                explain_before_payload.verified_state,
                Some(json!({
                    "execution": resolution_execution_summary(
                        fixture.execution_trace_id,
                        fixture.resource_id,
                    ),
                    "verified_queries": expected_verified_queries,
                }))
            );

            invalidate_persisted_resolution_execution(&database, &fixture.cache_key, invalidation)
                .await?;

            assert_eq!(
                load_execution_outcome(&database.pool, &fixture.cache_key).await?,
                None
            );
            assert!(
                load_execution_trace(&database.pool, fixture.execution_trace_id)
                    .await?
                    .is_some(),
                "execution traces stay durable after cache invalidation",
            );

            let mixed_after_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/resolutions/ens/alice.eth?mode=both&records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("mixed resolution request failed after invalidation")?;
            let explain_after_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(
                            "/v1/explain/resolutions/ens/alice.eth/execution?records=text:com.twitter,addr:60",
                        )
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("resolution execution explain request failed after invalidation")?;

            assert_eq!(mixed_after_response.status(), StatusCode::OK);
            assert_eq!(explain_after_response.status(), StatusCode::NOT_FOUND);

            let mixed_after_payload: ResolutionResponse = read_json(mixed_after_response).await?;
            let explain_after_payload: ErrorResponse = read_json(explain_after_response).await?;

            assert_eq!(mixed_after_payload.data, mixed_before_payload.data);
            assert_eq!(mixed_after_payload.coverage, mixed_before_payload.coverage);
            assert_eq!(
                mixed_after_payload.chain_positions,
                mixed_before_payload.chain_positions
            );
            assert_eq!(
                mixed_after_payload.consistency,
                mixed_before_payload.consistency
            );
            assert_eq!(
                mixed_after_payload.last_updated,
                mixed_before_payload.last_updated
            );
            assert_eq!(
                mixed_after_payload.declared_state,
                mixed_before_payload.declared_state
            );
            assert_eq!(
                mixed_after_payload.provenance.get("execution_trace_id"),
                Some(&Value::Null)
            );
            assert_eq!(
                mixed_after_payload.verified_state,
                Some(resolution_unsupported_verified_state(&[
                    "text:com.twitter",
                    "addr:60",
                ]))
            );
            assert_eq!(explain_after_payload.error.code, "not_found");
            assert_eq!(
                explain_after_payload.error.message,
                "persisted resolution execution explain was not found for name alice.eth in namespace ens"
            );
            assert!(explain_after_payload.error.details.is_empty());

            database.cleanup().await?;
            Ok(())
        }

        async fn seed_persisted_resolution_execution_fixture(
            database: &HarnessDatabase,
            execution_trace_id: Uuid,
        ) -> Result<PersistedResolutionExecutionFixture> {
            let logical_name_id = "ens:alice.eth";
            let resource_id = Uuid::from_u128(0x2200);
            let token_lineage_id = Uuid::from_u128(0x1100);
            let surface_binding_id = Uuid::from_u128(0x3300);

            database
                .seed_exact_name_rebuild_inputs(
                    logical_name_id,
                    resource_id,
                    token_lineage_id,
                    surface_binding_id,
                )
                .await?;
            database.rebuild_name_current(logical_name_id).await?;
            database
                .insert_record_inventory_current_row(resolution_record_inventory_current_row(
                    logical_name_id,
                    resource_id,
                ))
                .await?;

            let name_row = bigname_storage::load_name_current(&database.pool, logical_name_id)
                .await?
                .context("resolution execution invalidation requires an exact-name current row")?;
            let record_inventory_row =
                resolution_record_inventory_current_row(logical_name_id, resource_id);
            let records = parse_resolution_record_keys(
                Some("text:com.twitter,addr:60"),
                ResolutionMode::Verified,
            )
            .map_err(|error| anyhow::anyhow!(error.message))?;
            let cache_key = build_resolution_execution_cache_key(
                &name_row,
                &records,
                Some(&record_inventory_row),
            )?;
            let request_key = cache_key.request_key.clone();
            let persisted_verified_queries = resolution_execution_verified_queries(
                execution_trace_id,
                &["addr:60", "text:com.twitter"],
            );

            upsert_execution_trace(
                &database.pool,
                &resolution_execution_trace(
                    execution_trace_id,
                    &request_key,
                    &["addr:60", "text:com.twitter"],
                    persisted_verified_queries.clone(),
                ),
            )
            .await?;
            upsert_execution_outcome(
                &database.pool,
                &resolution_execution_outcome(
                    execution_trace_id,
                    cache_key.clone(),
                    persisted_verified_queries,
                ),
            )
            .await?;

            Ok(PersistedResolutionExecutionFixture {
                logical_name_id,
                resource_id,
                execution_trace_id,
                cache_key,
            })
        }

        async fn invalidate_persisted_resolution_execution(
            database: &HarnessDatabase,
            cache_key: &ExecutionCacheKey,
            invalidation: PersistedResolutionInvalidation,
        ) -> Result<()> {
            let summary = match invalidation {
                PersistedResolutionInvalidation::Manifest => {
                    let manifest_entry = cache_key
                        .manifest_versions
                        .as_array()
                        .and_then(|entries| entries.first())
                        .context(
                            "persisted verified resolution cache key must expose manifest_versions",
                        )?;
                    let manifest_version = manifest_entry
                        .get("manifest_version")
                        .and_then(Value::as_i64)
                        .context(
                            "persisted verified resolution manifest invalidation requires manifest_version",
                        )?;
                    let source_manifest_id = manifest_entry
                        .get("source_manifest_id")
                        .and_then(Value::as_i64);
                    let source_family = manifest_entry
                        .get("source_family")
                        .and_then(Value::as_str)
                        .map(str::to_owned);

                    if source_manifest_id.is_none() && source_family.is_none() {
                        return Err(anyhow::anyhow!(
                            "persisted verified resolution manifest invalidation requires a manifest identity"
                        ));
                    }

                    invalidate_execution_outcomes_for_manifest_version(
                        &database.pool,
                        &ExecutionManifestInvalidation {
                            request_type: "verified_resolution".to_owned(),
                            namespace: "ens".to_owned(),
                            source_manifest_id,
                            source_family,
                            manifest_version,
                        },
                    )
                    .await?
                }
                PersistedResolutionInvalidation::Topology => {
                    invalidate_execution_outcomes_for_topology_boundary(
                        &database.pool,
                        &ExecutionBoundaryInvalidation {
                            request_type: "verified_resolution".to_owned(),
                            namespace: "ens".to_owned(),
                            boundary: cache_key.topology_version_boundary.clone(),
                        },
                    )
                    .await?
                }
                PersistedResolutionInvalidation::Record => {
                    invalidate_execution_outcomes_for_record_boundary(
                        &database.pool,
                        &ExecutionBoundaryInvalidation {
                            request_type: "verified_resolution".to_owned(),
                            namespace: "ens".to_owned(),
                            boundary: cache_key.record_version_boundary.clone(),
                        },
                    )
                    .await?
                }
            };

            assert_eq!(summary.deleted_outcome_count, 1);
            Ok(())
        }

        #[derive(Clone, Copy, Debug)]
        enum PersistedPrimaryNameInvalidation {
            Manifest,
            Topology,
            Record,
        }

        impl PersistedPrimaryNameInvalidation {
            fn execution_trace_id(self) -> Uuid {
                match self {
                    Self::Manifest => Uuid::from_u128(0x0e7ec7ace00000000000000000000051),
                    Self::Topology => Uuid::from_u128(0x0e7ec7ace00000000000000000000052),
                    Self::Record => Uuid::from_u128(0x0e7ec7ace00000000000000000000053),
                }
            }

            fn sibling_execution_trace_id(self) -> Uuid {
                match self {
                    Self::Manifest => Uuid::from_u128(0x0e7ec7ace00000000000000000000061),
                    Self::Topology => Uuid::from_u128(0x0e7ec7ace00000000000000000000062),
                    Self::Record => Uuid::from_u128(0x0e7ec7ace00000000000000000000063),
                }
            }

            fn label(self) -> &'static str {
                match self {
                    Self::Manifest => "manifest invalidation",
                    Self::Topology => "topology boundary invalidation",
                    Self::Record => "record boundary invalidation",
                }
            }
        }

        struct PersistedPrimaryNameExecutionFixture {
            address: &'static str,
            target_execution_trace_id: Uuid,
            target_cache_key: ExecutionCacheKey,
            sibling_cache_key: ExecutionCacheKey,
            target_verified_primary_name: Value,
            sibling_verified_primary_name: Value,
            target_finished_at: OffsetDateTime,
            sibling_finished_at: OffsetDateTime,
        }

        async fn run_primary_name_execution_invalidation_case(
            invalidation: PersistedPrimaryNameInvalidation,
        ) -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let fixture =
                seed_persisted_primary_name_execution_fixture(&database, invalidation).await?;
            let expected_data = json!({
                "address": fixture.address,
                "namespace": "ens",
                "coin_type": "60",
            });

            let verified_before_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=verified",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "verified primary-name request failed before {}",
                        invalidation.label()
                    )
                })?;
            let both_before_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=both",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "mixed primary-name request failed before {}",
                        invalidation.label()
                    )
                })?;

            assert_eq!(verified_before_response.status(), StatusCode::OK);
            assert_eq!(both_before_response.status(), StatusCode::OK);

            let verified_before_payload: PrimaryNameResponse =
                read_json(verified_before_response).await?;
            let both_before_payload: PrimaryNameResponse = read_json(both_before_response).await?;
            let mut expected_target_verified_primary_name =
                fixture.target_verified_primary_name.clone();
            expected_target_verified_primary_name
                .as_object_mut()
                .expect("target verified primary-name fixture must be an object")
                .insert(
                    "provenance".to_owned(),
                    json!({
                        "manifest_versions": primary_name_execution_manifest_versions(),
                        "execution_trace_id": fixture.target_execution_trace_id.to_string(),
                    }),
                );

            assert_eq!(verified_before_payload.data, expected_data);
            assert_eq!(both_before_payload.data, expected_data);
            assert_eq!(verified_before_payload.declared_state, None);
            assert_eq!(
                verified_before_payload.verified_state,
                Some(json!({
                    "verified_primary_name": expected_target_verified_primary_name,
                }))
            );
            assert_eq!(
                both_before_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                        "provenance": seeded_primary_name_claim_provenance(),
                    }
                }))
            );
            assert_eq!(
                both_before_payload.verified_state,
                verified_before_payload.verified_state
            );
            assert_primary_name_persisted_readback_invariants(
                &verified_before_payload,
                fixture.target_execution_trace_id,
                fixture.target_finished_at,
            );
            assert_primary_name_persisted_readback_invariants(
                &both_before_payload,
                fixture.target_execution_trace_id,
                fixture.target_finished_at,
            );

            invalidate_persisted_primary_name_execution(
                &database,
                &fixture.target_cache_key,
                invalidation,
            )
            .await?;

            assert_eq!(
                load_execution_outcome(&database.pool, &fixture.target_cache_key).await?,
                None
            );
            assert!(
                load_execution_trace(&database.pool, fixture.target_execution_trace_id)
                    .await?
                    .is_some(),
                "execution traces stay durable after verified-primary cache invalidation",
            );
            assert!(
                load_execution_outcome(&database.pool, &fixture.sibling_cache_key)
                    .await?
                    .is_some(),
                "exact-tuple invalidation must keep sibling tuple outcomes",
            );

            let verified_after_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=verified",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "verified primary-name request failed after {}",
                        invalidation.label()
                    )
                })?;
            let both_after_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=60&mode=both",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "mixed primary-name request failed after {}",
                        invalidation.label()
                    )
                })?;
            let sibling_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/primary-names/{}?namespace=ens&coin_type=61&mode=verified",
                            fixture.address
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| {
                    format!(
                        "sibling primary-name request failed after {}",
                        invalidation.label()
                    )
                })?;

            assert_eq!(verified_after_response.status(), StatusCode::OK);
            assert_eq!(both_after_response.status(), StatusCode::OK);
            assert_eq!(sibling_response.status(), StatusCode::OK);

            let verified_after_payload: PrimaryNameResponse =
                read_json(verified_after_response).await?;
            let both_after_payload: PrimaryNameResponse = read_json(both_after_response).await?;
            let sibling_payload: PrimaryNameResponse = read_json(sibling_response).await?;

            assert_eq!(verified_after_payload.data, expected_data);
            assert_eq!(both_after_payload.data, expected_data);
            assert_eq!(verified_after_payload.declared_state, None);
            assert_eq!(
                verified_after_payload.verified_state,
                Some(json!({
                    "verified_primary_name": {
                        "status": "unsupported",
                        "unsupported_reason": "verified primary-name entrypoint is not yet supported",
                    }
                }))
            );
            assert_eq!(
                both_after_payload.declared_state,
                Some(json!({
                    "claimed_primary_name": {
                        "status": "not_found",
                        "provenance": seeded_primary_name_claim_provenance(),
                    }
                }))
            );
            assert_eq!(
                both_after_payload.verified_state,
                verified_after_payload.verified_state
            );
            assert_primary_name_bootstrap_invariants(&verified_after_payload);
            assert_primary_name_bootstrap_invariants(&both_after_payload);
            let mut expected_sibling_verified_primary_name =
                fixture.sibling_verified_primary_name.clone();
            expected_sibling_verified_primary_name
                .as_object_mut()
                .expect("sibling verified primary-name fixture must be an object")
                .insert(
                    "provenance".to_owned(),
                    json!({
                        "manifest_versions": primary_name_execution_manifest_versions(),
                        "execution_trace_id": invalidation
                            .sibling_execution_trace_id()
                            .to_string(),
                    }),
                );

            assert_eq!(
                sibling_payload.verified_state,
                Some(json!({
                    "verified_primary_name": expected_sibling_verified_primary_name,
                }))
            );
            assert_primary_name_persisted_readback_invariants(
                &sibling_payload,
                invalidation.sibling_execution_trace_id(),
                fixture.sibling_finished_at,
            );

            database.cleanup().await?;
            Ok(())
        }

        async fn seed_persisted_primary_name_execution_fixture(
            database: &HarnessDatabase,
            invalidation: PersistedPrimaryNameInvalidation,
        ) -> Result<PersistedPrimaryNameExecutionFixture> {
            let address = "0x0000000000000000000000000000000000000abc";
            seed_primary_name_tuple_anchor(database, address, "60").await?;
            seed_primary_name_tuple_anchor(database, address, "61").await?;

            let target_finished_at = timestamp(1_717_172_401);
            let sibling_finished_at = timestamp(1_717_172_499);
            let target_verified_primary_name = primary_name_verified_success(
                "ens:alice.eth",
                "alice.eth",
                "Alice.eth",
                "0x0000000000000000000000000000000000000000000000000000000000000123",
                Uuid::from_u128(0x456),
            );
            let sibling_verified_primary_name = primary_name_verified_mismatch(
                "ens:other.eth",
                "other.eth",
                "other.eth",
                "0x0000000000000000000000000000000000000000000000000000000000000456",
                Uuid::from_u128(0x999),
                "resolved_address_mismatch",
            );

            let target_outcome = primary_name_execution_outcome(
                invalidation.execution_trace_id(),
                "ens",
                address,
                "60",
                target_verified_primary_name.clone(),
                target_finished_at,
                primary_name_shared_topology_boundary(),
                primary_name_shared_record_boundary(),
            );
            let sibling_outcome = primary_name_execution_outcome(
                invalidation.sibling_execution_trace_id(),
                "ens",
                address,
                "61",
                sibling_verified_primary_name.clone(),
                sibling_finished_at,
                primary_name_shared_topology_boundary(),
                primary_name_shared_record_boundary(),
            );

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    invalidation.execution_trace_id(),
                    "ens",
                    address,
                    "60",
                    target_verified_primary_name.clone(),
                    target_finished_at,
                ),
            )
            .await?;
            upsert_execution_outcome(&database.pool, &target_outcome).await?;

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    invalidation.sibling_execution_trace_id(),
                    "ens",
                    address,
                    "61",
                    sibling_verified_primary_name.clone(),
                    sibling_finished_at,
                ),
            )
            .await?;
            upsert_execution_outcome(&database.pool, &sibling_outcome).await?;

            Ok(PersistedPrimaryNameExecutionFixture {
                address,
                target_execution_trace_id: invalidation.execution_trace_id(),
                target_cache_key: target_outcome.cache_key,
                sibling_cache_key: sibling_outcome.cache_key,
                target_verified_primary_name,
                sibling_verified_primary_name,
                target_finished_at,
                sibling_finished_at,
            })
        }

        async fn invalidate_persisted_primary_name_execution(
            database: &HarnessDatabase,
            cache_key: &ExecutionCacheKey,
            invalidation: PersistedPrimaryNameInvalidation,
        ) -> Result<()> {
            let summary = match invalidation {
                PersistedPrimaryNameInvalidation::Manifest => {
                    let manifest_entry = cache_key
                        .manifest_versions
                        .as_array()
                        .and_then(|entries| entries.first())
                        .context(
                            "persisted verified primary-name cache key must expose manifest_versions",
                        )?;
                    let manifest_version = manifest_entry
                        .get("manifest_version")
                        .and_then(Value::as_i64)
                        .context(
                            "persisted verified primary-name manifest invalidation requires manifest_version",
                        )?;
                    let source_manifest_id = manifest_entry
                        .get("source_manifest_id")
                        .and_then(Value::as_i64);
                    let source_family = manifest_entry
                        .get("source_family")
                        .and_then(Value::as_str)
                        .map(str::to_owned);

                    if source_manifest_id.is_none() && source_family.is_none() {
                        return Err(anyhow::anyhow!(
                            "persisted verified primary-name manifest invalidation requires a manifest identity"
                        ));
                    }

                    invalidate_execution_outcomes_for_manifest_version_and_request_key(
                        &database.pool,
                        &ExecutionManifestInvalidation {
                            request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE
                                .to_owned(),
                            namespace: "ens".to_owned(),
                            source_manifest_id,
                            source_family,
                            manifest_version,
                        },
                        &cache_key.request_key,
                    )
                    .await?
                }
                PersistedPrimaryNameInvalidation::Topology => {
                    invalidate_execution_outcomes_for_topology_boundary_and_request_key(
                        &database.pool,
                        &ExecutionBoundaryInvalidation {
                            request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE
                                .to_owned(),
                            namespace: "ens".to_owned(),
                            boundary: cache_key.topology_version_boundary.clone(),
                        },
                        &cache_key.request_key,
                    )
                    .await?
                }
                PersistedPrimaryNameInvalidation::Record => {
                    invalidate_execution_outcomes_for_record_boundary_and_request_key(
                        &database.pool,
                        &ExecutionBoundaryInvalidation {
                            request_type: bigname_storage::VERIFIED_PRIMARY_NAME_REQUEST_TYPE
                                .to_owned(),
                            namespace: "ens".to_owned(),
                            boundary: cache_key.record_version_boundary.clone(),
                        },
                        &cache_key.request_key,
                    )
                    .await?
                }
            };

            assert_eq!(summary.deleted_outcome_count, 1);
            Ok(())
        }

        async fn assert_exact_name_history_summary_matches_history_route(
            database: &HarnessDatabase,
            namespace: &str,
            name: &str,
            history: &Value,
        ) -> Result<()> {
            let history = history
                .as_object()
                .expect("exact-name history summary must be an object");
            let surface_head = history
                .get("surface_head")
                .context("surface_head must be present")?;
            let resource_head = history
                .get("resource_head")
                .context("resource_head must be present")?;

            let surface_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/names/{namespace}/{name}?scope=surface"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("exact-name surface history request failed")?;
            let resource_response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(format!(
                            "/v1/history/names/{namespace}/{name}?scope=resource"
                        ))
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .context("exact-name resource history request failed")?;

            assert_eq!(surface_response.status(), StatusCode::OK);
            assert_eq!(resource_response.status(), StatusCode::OK);

            let surface_payload: HistoryResponse = read_json(surface_response).await?;
            let resource_payload: HistoryResponse = read_json(resource_response).await?;

            assert_eq!(
                surface_head,
                &history_pointer_from_history_row(
                    surface_payload
                        .data
                        .first()
                        .context("surface history route must return a head row")?,
                )?
            );
            assert_eq!(
                resource_head,
                &history_pointer_from_history_row(
                    resource_payload
                        .data
                        .first()
                        .context("resource history route must return a head row")?,
                )?
            );

            Ok(())
        }

        fn history_pointer_from_history_row(row: &Value) -> Result<Value> {
            let normalized_event_id = row
                .get("normalized_event_id")
                .and_then(Value::as_str)
                .context("history row must include normalized_event_id")?
                .parse::<i64>()
                .context("history row normalized_event_id must parse as i64")?;

            Ok(json!({
                "normalized_event_id": normalized_event_id,
                "event_kind": row
                    .get("event_kind")
                    .cloned()
                    .context("history row must include event_kind")?,
                "chain_position": row
                    .get("chain_position")
                    .cloned()
                    .context("history row must include chain_position")?,
            }))
        }
