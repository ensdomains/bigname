        struct ReplayCorpus {
            logical_name_id: &'static str,
            route_name: &'static str,
            resource_id: Uuid,
            token_lineage_id: Uuid,
            surface_binding_id: Uuid,
            winning_address_names_address: &'static str,
            losing_address_names_address: &'static str,
            winning_control_address: &'static str,
            losing_control_address: &'static str,
            resolver_chain_id: &'static str,
            winning_resolver_address: &'static str,
            losing_resolver_address: &'static str,
            winning_permission_subject: &'static str,
            losing_permission_subject: &'static str,
            primary_name_address: &'static str,
            winning_primary_name: &'static str,
            losing_primary_name: &'static str,
        }

        struct ReplayRoute {
            label: &'static str,
            uri: String,
        }

        pub(crate) async fn run_replay_capability_conformance() -> Result<()> {
            let database = HarnessDatabase::new().await?;
            let corpus = seed_replay_supported_read_corpus(&database).await?;

            let before_replay = snapshot_replay_stale_current_answer_routes(&database, &corpus).await?;
            assert_stale_current_answers_exist(&before_replay, &corpus);
            assert_replay_collection_empty(
                &database,
                ReplayRoute {
                    label: "winning-address-names-before-replay",
                    uri: format!(
                        "/v1/addresses/{}/names?namespace=basenames",
                        corpus.winning_address_names_address
                    ),
                },
            )
            .await?;
            assert_replay_route_status(
                &database,
                ReplayRoute {
                    label: "winning-resolver-before-replay",
                    uri: format!(
                        "/v1/resolvers/{}/{}",
                        corpus.resolver_chain_id, corpus.winning_resolver_address
                    ),
                },
                StatusCode::NOT_FOUND,
            )
            .await?;

            replay_all_current_projections(&database).await?;
            let after_replay = snapshot_replay_supported_read_routes(&database, &corpus).await?;
            assert_replayed_current_answers_are_canonical(&after_replay, &corpus);
            assert_replay_collection_empty(
                &database,
                ReplayRoute {
                    label: "losing-address-names-after-replay",
                    uri: format!(
                        "/v1/addresses/{}/names?namespace=basenames",
                        corpus.losing_address_names_address
                    ),
                },
            )
            .await?;
            assert_replay_collection_empty(
                &database,
                ReplayRoute {
                    label: "losing-address-history-after-replay",
                    uri: format!(
                        "/v1/history/addresses/{}?namespace=basenames&relation=registrant",
                        corpus.losing_address_names_address
                    ),
                },
            )
            .await?;
            assert_replay_route_status(
                &database,
                ReplayRoute {
                    label: "losing-resolver-after-replay",
                    uri: format!(
                        "/v1/resolvers/{}/{}",
                        corpus.resolver_chain_id, corpus.losing_resolver_address
                    ),
                },
                StatusCode::NOT_FOUND,
            )
            .await?;

            database.cleanup().await?;
            Ok(())
        }

        async fn seed_replay_supported_read_corpus(database: &HarnessDatabase) -> Result<ReplayCorpus> {
            let corpus = ReplayCorpus {
                logical_name_id: "basenames:alice.base.eth",
                route_name: "alice.base.eth",
                resource_id: Uuid::from_u128(0xc910),
                token_lineage_id: Uuid::from_u128(0xc911),
                surface_binding_id: Uuid::from_u128(0xc912),
                winning_address_names_address: "0x00000000000000000000000000000000000000cc",
                losing_address_names_address: "0x00000000000000000000000000000000000000aa",
                winning_control_address: "0x00000000000000000000000000000000000000dd",
                losing_control_address: "0x00000000000000000000000000000000000000bb",
                resolver_chain_id: "base-mainnet",
                winning_resolver_address: "0x0000000000000000000000000000000000000def",
                losing_resolver_address: "0x0000000000000000000000000000000000000abc",
                winning_permission_subject: "0x00000000000000000000000000000000000000ee",
                losing_permission_subject: "0x00000000000000000000000000000000000000bb",
                primary_name_address: "0x0000000000000000000000000000000000000bcd",
                winning_primary_name: "alice.base.eth",
                losing_primary_name: "mallory.base.eth",
            };

            seed_basenames_resolution_rebuild_inputs(
                database,
                corpus.logical_name_id,
                corpus.resource_id,
                corpus.token_lineage_id,
                corpus.surface_binding_id,
            )
            .await?;
            seed_replay_permissions(database, &corpus).await?;

            let child_fixture = EnsV2DeclaredChildFixture::new(
                "ens:parent.eth",
                "ens:alice.parent.eth",
                Uuid::from_u128(0xc920),
                Uuid::from_u128(0xc921),
                90,
            );
            child_fixture.seed(database).await?;

            seed_replay_primary_name_claim_observation(
                database,
                &corpus,
                "losing",
                corpus.losing_primary_name,
                "0xreplay-losing-primary-reverse",
                "0xreplay-losing-primary-claim",
                CanonicalityState::Canonical,
            )
            .await?;

            database.rebuild_name_current(corpus.logical_name_id).await?;
            rebuild_children_current(database, None).await?;
            rebuild_record_inventory_current(database, corpus.resource_id).await?;
            rebuild_permissions_current(database, None).await?;
            rebuild_resolver_current(database, None, None).await?;
            rebuild_address_names_current(database, None).await?;
            database
                .rebuild_primary_names_current(corpus.primary_name_address, "basenames", "60")
                .await?;
            seed_replay_primary_name_execution(database, &corpus).await?;
            mark_replay_losing_branch_source_rows_orphaned(database).await?;
            seed_replay_winning_branch_source_rows(database, &corpus).await?;

            Ok(corpus)
        }

        async fn seed_replay_permissions(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<()> {
            let subject = corpus.losing_permission_subject;

            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block("base-mainnet", "0xreplay-permission-1", None, 106, 1_717_181_706),
                    raw_block("base-mainnet", "0xreplay-permission-2", None, 107, 1_717_181_707),
                ],
            )
            .await
            .context("failed to upsert replay permission raw blocks")?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:replay:basenames:resource-permission"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "PermissionChanged".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        manifest_version: 5,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(106),
                        block_hash: Some("0xreplay-permission-1".to_owned()),
                        transaction_hash: Some("0xtxreplaypermission1".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "event_identity": "conformance:replay:basenames:resource-permission",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "subject": subject,
                            "scope": {
                                "kind": "resource",
                            },
                            "effective_powers": ["resource_control"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "conformance:replay:basenames:resource-permission",
                            },
                            "revocation_source": null,
                            "inheritance_path": [],
                            "transfer_behavior": {},
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:basenames:resolver-permission"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "PermissionChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 6,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(107),
                        block_hash: Some("0xreplay-permission-2".to_owned()),
                        transaction_hash: Some("0xtxreplaypermission2".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "event_identity": "conformance:replay:basenames:resolver-permission",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "subject": subject,
                                "scope": {
                                    "kind": "resolver",
                                    "chain_id": corpus.resolver_chain_id,
                                    "resolver_address": corpus.losing_resolver_address,
                                },
                            "effective_powers": ["resolver_control"],
                            "grant_source": {
                                "kind": "normalized_event",
                                "event_identity": "conformance:replay:basenames:resolver-permission",
                            },
                            "revocation_source": null,
                            "inheritance_path": [],
                            "transfer_behavior": {},
                        }),
                    },
                ],
            )
            .await
            .context("failed to upsert replay permission normalized events")?;

            Ok(())
        }

        async fn seed_replay_primary_name_execution(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<()> {
            let execution_trace_id = Uuid::from_u128(0x0e7ec7ace00000000000000000000c91);
            let finished_at = timestamp(1_717_172_410);
            let verified_primary_name = json!({
                "status": "success",
                "name": {
                    "logical_name_id": corpus.logical_name_id,
                    "namespace": "basenames",
                    "normalized_name": corpus.route_name,
                    "canonical_display_name": "Alice.base.eth",
                    "namehash": "namehash:alice.base.eth",
                    "resource_id": corpus.resource_id.to_string(),
                    "binding_kind": "declared_registry_path",
                }
            });

            upsert_execution_trace(
                &database.pool,
                &primary_name_execution_trace(
                    execution_trace_id,
                    "basenames",
                    corpus.primary_name_address,
                    "60",
                    verified_primary_name.clone(),
                    finished_at,
                ),
            )
            .await
            .context("failed to seed replay primary-name execution trace")?;
            upsert_execution_outcome(
                &database.pool,
                &primary_name_execution_outcome(
                    execution_trace_id,
                    "basenames",
                    corpus.primary_name_address,
                    "60",
                    verified_primary_name,
                    finished_at,
                    primary_name_shared_topology_boundary(),
                    primary_name_shared_record_boundary(),
                ),
            )
            .await
            .context("failed to seed replay primary-name execution outcome")?;

            Ok(())
        }

        async fn mark_replay_losing_branch_source_rows_orphaned(
            database: &HarnessDatabase,
        ) -> Result<()> {
            set_normalized_events_canonicality(
                database,
                &[
                    "conformance:basenames:grant",
                    "conformance:basenames:authority",
                    "conformance:basenames:resolver",
                    "conformance:basenames:record-version",
                    "conformance:basenames:addr",
                    "conformance:basenames:text",
                    "conformance:replay:basenames:resource-permission",
                    "conformance:replay:basenames:resolver-permission",
                    "conformance:replay:basenames:primary:losing:reverse",
                    "conformance:replay:basenames:primary:losing:claim",
                ],
                CanonicalityState::Orphaned,
            )
            .await?;
            set_raw_blocks_canonicality(
                database,
                "base-mainnet",
                &[
                    "0xbase-grant",
                    "0xbase-authority",
                    "0xbase-resolver",
                    "0xreplay-permission-1",
                    "0xreplay-permission-2",
                    "0xreplay-losing-primary-reverse",
                    "0xreplay-losing-primary-claim",
                ],
                CanonicalityState::Orphaned,
            )
            .await?;

            Ok(())
        }

        async fn seed_replay_winning_branch_source_rows(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<()> {
            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block(
                        "base-mainnet",
                        "0xreplay-winning-grant",
                        Some("0xbase-binding"),
                        101,
                        1_717_191_701,
                    ),
                    raw_block(
                        "base-mainnet",
                        "0xreplay-winning-authority",
                        Some("0xreplay-winning-grant"),
                        102,
                        1_717_191_702,
                    ),
                    raw_block(
                        "base-mainnet",
                        "0xreplay-winning-resolver",
                        Some("0xreplay-winning-authority"),
                        103,
                        1_717_191_703,
                    ),
                    raw_block(
                        "base-mainnet",
                        "0xreplay-winning-permission-1",
                        Some("0xreplay-winning-resolver"),
                        106,
                        1_717_191_706,
                    ),
                    raw_block(
                        "base-mainnet",
                        "0xreplay-winning-permission-2",
                        Some("0xreplay-winning-permission-1"),
                        107,
                        1_717_191_707,
                    ),
                ],
            )
            .await
            .context("failed to upsert replay winning branch raw blocks")?;

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:grant".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "RegistrationGranted".to_owned(),
                        source_family: "basenames_base_registrar".to_owned(),
                        manifest_version: 3,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(101),
                        block_hash: Some("0xreplay-winning-grant".to_owned()),
                        transaction_hash: Some("0xtxreplaywinninggrant".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:grant",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "authority_kind": "registrar",
                            "authority_key": "registrar:base-mainnet:alice",
                            "registrant": corpus.winning_address_names_address,
                            "expiry": 1_900_000_000_i64,
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:authority"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "AuthorityTransferred".to_owned(),
                        source_family: "basenames_base_registry".to_owned(),
                        manifest_version: 3,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(102),
                        block_hash: Some("0xreplay-winning-authority".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningauthority".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:authority",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "owner": corpus.winning_control_address,
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:resolver"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "ResolverChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xreplay-winning-resolver".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningresolver".to_owned()),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:resolver",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "resolver": corpus.winning_resolver_address,
                            "namehash": "namehash:alice.base.eth",
                        }),
                    },
                    NormalizedEvent {
                        event_identity: "conformance:replay:winning:basenames:record-version"
                            .to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "RecordVersionChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xreplay-winning-resolver".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningresolver".to_owned()),
                        log_index: Some(1),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:record-version",
                        }),
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
                        event_identity: "conformance:replay:winning:basenames:addr".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xreplay-winning-resolver".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningresolver".to_owned()),
                        log_index: Some(2),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:addr",
                        }),
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
                        event_identity: "conformance:replay:winning:basenames:text".to_owned(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: Some(corpus.logical_name_id.to_owned()),
                        resource_id: Some(corpus.resource_id),
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 4,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(103),
                        block_hash: Some("0xreplay-winning-resolver".to_owned()),
                        transaction_hash: Some("0xtxreplaywinningresolver".to_owned()),
                        log_index: Some(3),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": "winning",
                            "event_identity": "conformance:replay:winning:basenames:text",
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state: CanonicalityState::Canonical,
                        before_state: json!({}),
                        after_state: json!({
                            "record_key": "text",
                            "record_family": "text",
                            "selector_key": null,
                        }),
                    },
                    replay_permission_event(
                        "conformance:replay:winning:basenames:resource-permission",
                        corpus,
                        "0xreplay-winning-permission-1",
                        106,
                        "0xtxreplaywinningpermission1",
                        json!({
                            "kind": "resource",
                        }),
                    ),
                    replay_permission_event(
                        "conformance:replay:winning:basenames:resolver-permission",
                        corpus,
                        "0xreplay-winning-permission-2",
                        107,
                        "0xtxreplaywinningpermission2",
                        json!({
                            "kind": "resolver",
                            "chain_id": corpus.resolver_chain_id,
                            "resolver_address": corpus.winning_resolver_address,
                        }),
                    ),
                ],
            )
            .await
            .context("failed to upsert replay winning branch normalized events")?;

            seed_replay_primary_name_claim_observation(
                database,
                corpus,
                "winning",
                corpus.winning_primary_name,
                "0xreplay-winning-primary-reverse",
                "0xreplay-winning-primary-claim",
                CanonicalityState::Canonical,
            )
            .await?;

            Ok(())
        }

        fn replay_permission_event(
            event_identity: &str,
            corpus: &ReplayCorpus,
            block_hash: &str,
            block_number: i64,
            transaction_hash: &str,
            scope: Value,
        ) -> NormalizedEvent {
            let scope_kind = scope.get("kind").and_then(Value::as_str);
            let effective_powers = match scope_kind {
                Some("resolver") => json!(["resolver_control"]),
                _ => json!(["resource_control"]),
            };

            NormalizedEvent {
                event_identity: event_identity.to_owned(),
                namespace: "basenames".to_owned(),
                logical_name_id: Some(corpus.logical_name_id.to_owned()),
                resource_id: Some(corpus.resource_id),
                event_kind: "PermissionChanged".to_owned(),
                source_family: match scope_kind {
                    Some("resolver") => "basenames_base_resolver".to_owned(),
                    _ => "basenames_base_registry".to_owned(),
                },
                manifest_version: match scope_kind {
                    Some("resolver") => 6,
                    _ => 5,
                },
                source_manifest_id: None,
                chain_id: Some("base-mainnet".to_owned()),
                block_number: Some(block_number),
                block_hash: Some(block_hash.to_owned()),
                transaction_hash: Some(transaction_hash.to_owned()),
                log_index: Some(0),
                raw_fact_ref: json!({
                    "kind": "raw_log",
                    "branch": "winning",
                    "event_identity": event_identity,
                }),
                derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                canonicality_state: CanonicalityState::Canonical,
                before_state: json!({}),
                after_state: json!({
                    "subject": corpus.winning_permission_subject,
                    "scope": scope,
                    "effective_powers": effective_powers,
                    "grant_source": {
                        "kind": "normalized_event",
                        "event_identity": event_identity,
                    },
                    "revocation_source": null,
                    "inheritance_path": [],
                    "transfer_behavior": {},
                }),
            }
        }

        async fn seed_replay_primary_name_claim_observation(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
            branch: &str,
            raw_name: &str,
            reverse_block_hash: &str,
            claim_block_hash: &str,
            canonicality_state: CanonicalityState,
        ) -> Result<()> {
            bigname_storage::upsert_raw_blocks(
                &database.pool,
                &[
                    raw_block(
                        "base-mainnet",
                        reverse_block_hash,
                        Some("0xbase-binding"),
                        260,
                        1_717_192_260,
                    ),
                    raw_block(
                        "base-mainnet",
                        claim_block_hash,
                        Some(reverse_block_hash),
                        261,
                        1_717_192_261,
                    ),
                ],
            )
            .await
            .with_context(|| format!("failed to upsert replay {branch} primary-name raw blocks"))?;

            let normalized_address = corpus.primary_name_address.to_ascii_lowercase();
            let reverse_label = normalized_address.trim_start_matches("0x").to_owned();
            let reverse_event_identity =
                format!("conformance:replay:basenames:primary:{branch}:reverse");
            let claim_event_identity =
                format!("conformance:replay:basenames:primary:{branch}:claim");

            bigname_storage::upsert_normalized_events(
                &database.pool,
                &[
                    NormalizedEvent {
                        event_identity: reverse_event_identity.clone(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: None,
                        resource_id: None,
                        event_kind: "ReverseChanged".to_owned(),
                        source_family: "basenames_base_primary".to_owned(),
                        manifest_version: 1,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(260),
                        block_hash: Some(reverse_block_hash.to_owned()),
                        transaction_hash: Some(format!("0xtxreplayprimary{branch}reverse")),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": branch,
                            "event_identity": reverse_event_identity,
                        }),
                        derivation_kind: "ens_v1_reverse_claim".to_owned(),
                        canonicality_state,
                        before_state: json!({}),
                        after_state: json!({
                            "source_event": "ReverseClaimed",
                            "address": normalized_address.clone(),
                            "coin_type": "60",
                            "namespace": "basenames",
                            "reverse_namespace": "basenames",
                            "reverse_label": reverse_label.clone(),
                            "reverse_name": format!("{reverse_label}.addr.reverse"),
                            "reverse_node": format!("0xreplay{branch}reverse"),
                            "claim_provenance": {
                                "source_family": "basenames_base_primary",
                                "contract_role": "reverse_registrar",
                                "contract_instance_id": "00000000-0000-0000-0000-000000000104",
                                "emitting_address": "0x00000000000000000000000000000000000000ad",
                            },
                        }),
                    },
                    NormalizedEvent {
                        event_identity: claim_event_identity.clone(),
                        namespace: "basenames".to_owned(),
                        logical_name_id: None,
                        resource_id: None,
                        event_kind: "RecordChanged".to_owned(),
                        source_family: "basenames_base_resolver".to_owned(),
                        manifest_version: 1,
                        source_manifest_id: None,
                        chain_id: Some("base-mainnet".to_owned()),
                        block_number: Some(261),
                        block_hash: Some(claim_block_hash.to_owned()),
                        transaction_hash: Some(format!("0xtxreplayprimary{branch}claim")),
                        log_index: Some(0),
                        raw_fact_ref: json!({
                            "kind": "raw_log",
                            "branch": branch,
                            "event_identity": claim_event_identity,
                        }),
                        derivation_kind: "ens_v1_unwrapped_authority".to_owned(),
                        canonicality_state,
                        before_state: json!({}),
                        after_state: json!({
                            "record_key": "name",
                            "record_family": "name",
                            "selector_key": null,
                            "raw_name": raw_name,
                            "primary_claim_source": {
                                "address": normalized_address,
                                "namespace": "basenames",
                                "coin_type": "60",
                                "reverse_name": format!("{reverse_label}.addr.reverse"),
                                "reverse_node": format!("0xreplay{branch}claim"),
                                "claim_provenance": {
                                    "source_family": "basenames_base_primary",
                                    "contract_role": "reverse_registrar",
                                    "contract_instance_id": "00000000-0000-0000-0000-000000000105",
                                    "emitting_address": "0x00000000000000000000000000000000000000ad",
                                },
                            },
                        }),
                    },
                ],
            )
            .await
            .with_context(|| {
                format!("failed to upsert replay {branch} primary-name normalized events")
            })?;

            Ok(())
        }

        async fn snapshot_replay_supported_read_routes(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<Vec<(&'static str, Value)>> {
            let mut snapshots = Vec::new();
            for route in replay_supported_read_routes(corpus) {
                let payload = request_replay_route(database, &route).await?;
                snapshots.push((route.label, payload));
            }

            Ok(snapshots)
        }

        async fn snapshot_replay_stale_current_answer_routes(
            database: &HarnessDatabase,
            corpus: &ReplayCorpus,
        ) -> Result<Vec<(&'static str, Value)>> {
            let mut snapshots = Vec::new();
            for route in replay_stale_current_answer_routes(corpus) {
                let payload = request_replay_route(database, &route).await?;
                snapshots.push((route.label, payload));
            }

            Ok(snapshots)
        }

        fn assert_stale_current_answers_exist(
            snapshots: &[(&'static str, Value)],
            corpus: &ReplayCorpus,
        ) {
            let exact_name = replay_route_payload(snapshots, "exact-name");
            assert_declared_exact_name_branch(
                exact_name,
                corpus.losing_address_names_address,
                corpus.losing_control_address,
                corpus.losing_resolver_address,
            );
            assert_json_not_contains(
                exact_name,
                corpus.winning_address_names_address,
                "exact-name route should not expose the winning branch before replay",
            );

            let resolution = replay_route_payload(snapshots, "resolution");
            assert_json_contains(
                resolution,
                corpus.losing_resolver_address,
                "resolution route should expose the stale losing resolver before replay",
            );
            assert_json_not_contains(
                resolution,
                corpus.winning_resolver_address,
                "resolution route should not expose the winning resolver before replay",
            );

            let losing_address_names = replay_route_payload(snapshots, "losing-address-names");
            assert_collection_non_empty(
                losing_address_names,
                "losing address-name collection should be stale before replay",
            );

            let permissions = replay_route_payload(snapshots, "permissions");
            assert_json_contains(
                permissions,
                corpus.losing_permission_subject,
                "permissions route should expose the stale losing subject before replay",
            );
            assert_json_not_contains(
                permissions,
                corpus.winning_permission_subject,
                "permissions route should not expose the winning subject before replay",
            );

            let resolver = replay_route_payload(snapshots, "losing-resolver");
            assert_json_contains(
                resolver,
                corpus.losing_resolver_address,
                "resolver route should expose the stale losing resolver before replay",
            );

            let primary_name = replay_route_payload(snapshots, "primary-name");
            assert_primary_name_claim(primary_name, corpus.losing_primary_name);
        }

        fn assert_replayed_current_answers_are_canonical(
            snapshots: &[(&'static str, Value)],
            corpus: &ReplayCorpus,
        ) {
            let exact_name = replay_route_payload(snapshots, "exact-name");
            assert_declared_exact_name_branch(
                exact_name,
                corpus.winning_address_names_address,
                corpus.winning_control_address,
                corpus.winning_resolver_address,
            );

            let address_names = replay_route_payload(snapshots, "address-names-collection");
            assert_collection_non_empty(
                address_names,
                "winning address-name collection should exist after replay",
            );

            let address_history = replay_route_payload(snapshots, "address-history");
            assert_collection_non_empty(
                address_history,
                "winning address-history route should expose canonical branch events after replay",
            );

            let resolution = replay_route_payload(snapshots, "resolution");
            assert_json_contains(
                resolution,
                corpus.winning_resolver_address,
                "resolution route should expose the canonical winning resolver after replay",
            );

            let permissions = replay_route_payload(snapshots, "permissions");
            assert_json_contains(
                permissions,
                corpus.winning_permission_subject,
                "permissions route should expose the canonical winning subject after replay",
            );

            let resolver = replay_route_payload(snapshots, "resolver");
            assert_json_contains(
                resolver,
                corpus.winning_resolver_address,
                "resolver route should expose the canonical winning resolver after replay",
            );

            let primary_name = replay_route_payload(snapshots, "primary-name");
            assert_primary_name_claim(primary_name, corpus.winning_primary_name);

            for (label, payload) in snapshots {
                for forbidden in [
                    corpus.losing_address_names_address,
                    corpus.losing_control_address,
                    corpus.losing_resolver_address,
                    corpus.losing_permission_subject,
                    corpus.losing_primary_name,
                    "0xbase-grant",
                    "0xbase-authority",
                    "0xbase-resolver",
                    "0xreplay-permission-1",
                    "0xreplay-permission-2",
                    "0xreplay-losing-primary-reverse",
                    "0xreplay-losing-primary-claim",
                    "branch\":\"losing",
                ] {
                    assert_json_not_contains(
                        payload,
                        forbidden,
                        &format!(
                            "{label} route should not expose orphaned losing-branch marker {forbidden} after replay"
                        ),
                    );
                }
            }
        }

        fn replay_route_payload<'a>(
            snapshots: &'a [(&'static str, Value)],
            label: &str,
        ) -> &'a Value {
            snapshots
                .iter()
                .find_map(|(snapshot_label, payload)| {
                    (*snapshot_label == label).then_some(payload)
                })
                .unwrap_or_else(|| panic!("missing replay route snapshot {label}"))
        }

        fn assert_declared_exact_name_branch(
            payload: &Value,
            registrant: &str,
            registry_owner: &str,
            resolver_address: &str,
        ) {
            assert_eq!(
                string_at(payload, &["declared_state", "registration", "registrant"]),
                registrant
            );
            assert_eq!(
                string_at(payload, &["declared_state", "control", "registry_owner"]),
                registry_owner
            );
            assert_eq!(
                string_at(payload, &["declared_state", "resolver", "address"]),
                resolver_address
            );
        }

        fn assert_primary_name_claim(payload: &Value, expected_name: &str) {
            assert_eq!(
                string_at(
                    payload,
                    &["declared_state", "claimed_primary_name", "name"]
                ),
                expected_name
            );
        }

        fn string_at<'a>(payload: &'a Value, path: &[&str]) -> &'a str {
            let mut current = payload;
            for segment in path {
                current = current
                    .get(*segment)
                    .unwrap_or_else(|| panic!("payload missing JSON path segment {segment}"));
            }
            current
                .as_str()
                .unwrap_or_else(|| panic!("payload JSON path {path:?} must be a string"))
        }

        fn assert_collection_non_empty(payload: &Value, message: &str) {
            let data = payload
                .get("data")
                .and_then(Value::as_array)
                .expect("collection payload must include array data");
            assert!(!data.is_empty(), "{message}");
        }

        fn assert_json_contains(payload: &Value, needle: &str, message: &str) {
            let encoded = serde_json::to_string(payload).expect("payload must serialize");
            assert!(encoded.contains(needle), "{message}");
        }

        fn assert_json_not_contains(payload: &Value, needle: &str, message: &str) {
            let encoded = serde_json::to_string(payload).expect("payload must serialize");
            assert!(!encoded.contains(needle), "{message}");
        }

        async fn assert_replay_collection_empty(
            database: &HarnessDatabase,
            route: ReplayRoute,
        ) -> Result<()> {
            let label = route.label;
            let payload = request_replay_route(database, &route).await?;
            let data = payload
                .get("data")
                .and_then(Value::as_array)
                .with_context(|| format!("{label} payload must include array data"))?;
            assert!(data.is_empty(), "{label} should not return collection rows");

            Ok(())
        }

        async fn assert_replay_route_status(
            database: &HarnessDatabase,
            route: ReplayRoute,
            expected_status: StatusCode,
        ) -> Result<()> {
            let label = route.label;
            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(route.uri.as_str())
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| format!("{label} replay route status request failed"))?;

            assert_eq!(
                response.status(),
                expected_status,
                "{label} replay route returned unexpected status",
            );

            Ok(())
        }

        fn replay_stale_current_answer_routes(corpus: &ReplayCorpus) -> Vec<ReplayRoute> {
            vec![
                ReplayRoute {
                    label: "exact-name",
                    uri: format!("/v1/names/basenames/{}", corpus.route_name),
                },
                ReplayRoute {
                    label: "losing-address-names",
                    uri: format!(
                        "/v1/addresses/{}/names?namespace=basenames",
                        corpus.losing_address_names_address
                    ),
                },
                ReplayRoute {
                    label: "resolution",
                    uri: format!(
                        "/v1/resolutions/basenames/{}?mode=declared&records=addr:60,text",
                        corpus.route_name
                    ),
                },
                ReplayRoute {
                    label: "permissions",
                    uri: format!("/v1/resources/{}/permissions", corpus.resource_id),
                },
                ReplayRoute {
                    label: "losing-resolver",
                    uri: format!(
                        "/v1/resolvers/{}/{}",
                        corpus.resolver_chain_id, corpus.losing_resolver_address
                    ),
                },
                ReplayRoute {
                    label: "primary-name",
                    uri: format!(
                        "/v1/primary-names/{}?namespace=basenames&coin_type=60&mode=both",
                        corpus.primary_name_address
                    ),
                },
            ]
        }

        fn replay_supported_read_routes(corpus: &ReplayCorpus) -> Vec<ReplayRoute> {
            vec![
                ReplayRoute {
                    label: "exact-name",
                    uri: format!("/v1/names/basenames/{}", corpus.route_name),
                },
                ReplayRoute {
                    label: "children-collection",
                    uri: "/v1/names/ens/parent.eth/children".to_owned(),
                },
                ReplayRoute {
                    label: "address-names-collection",
                    uri: format!(
                        "/v1/addresses/{}/names?namespace=basenames",
                        corpus.winning_address_names_address
                    ),
                },
                ReplayRoute {
                    label: "name-history",
                    uri: format!(
                        "/v1/history/names/basenames/{}?scope=both",
                        corpus.route_name
                    ),
                },
                ReplayRoute {
                    label: "resource-history",
                    uri: format!("/v1/history/resources/{}?scope=both", corpus.resource_id),
                },
                ReplayRoute {
                    label: "address-history",
                    uri: format!(
                        "/v1/history/addresses/{}?namespace=basenames&relation=registrant",
                        corpus.winning_address_names_address
                    ),
                },
                ReplayRoute {
                    label: "resolution",
                    uri: format!(
                        "/v1/resolutions/basenames/{}?mode=declared&records=addr:60,text",
                        corpus.route_name
                    ),
                },
                ReplayRoute {
                    label: "permissions",
                    uri: format!("/v1/resources/{}/permissions", corpus.resource_id),
                },
                ReplayRoute {
                    label: "resolver",
                    uri: format!(
                        "/v1/resolvers/{}/{}",
                        corpus.resolver_chain_id, corpus.winning_resolver_address
                    ),
                },
                ReplayRoute {
                    label: "primary-name",
                    uri: format!(
                        "/v1/primary-names/{}?namespace=basenames&coin_type=60&mode=both",
                        corpus.primary_name_address
                    ),
                },
            ]
        }

        async fn request_replay_route(
            database: &HarnessDatabase,
            route: &ReplayRoute,
        ) -> Result<Value> {
            let response = app_router(database.app_state())
                .oneshot(
                    Request::builder()
                        .uri(route.uri.as_str())
                        .body(Body::empty())
                        .expect("request must build"),
                )
                .await
                .with_context(|| format!("{} replay route request failed", route.label))?;

            assert_eq!(
                response.status(),
                StatusCode::OK,
                "{} replay route returned unexpected status",
                route.label
            );

            read_json(response)
                .await
                .with_context(|| format!("failed to decode {} replay route response", route.label))
        }
