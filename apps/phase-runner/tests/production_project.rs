#[allow(dead_code)]
mod support;

use std::{sync::Arc, time::Duration};

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::Result;
use bigname_adapters::schema_v2::{
    AddressAdmissionInput, BatchInput as AdapterBatchInput, DiscoveryRuleInput, ManifestInput,
    RawBlockInput as AdapterRawBlockInput, RawLogInput as AdapterRawLogInput,
    interpret_schema_v2_batch,
};
use bigname_interpret::{
    BatchRequest as InterpretRequest, Engine as InterpretEngine, RunMode as InterpretRunMode,
};
use bigname_manifests::load_repository;
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_storage::{NameCurrentRow, SurfaceBindingKind, resolution_verified_support_boundary};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    heads::{BlockMarker, HeadMarkers, publish_heads},
    interpret_phase::InterpretPhase,
    phase::{BlockRange, LoopbackPhase, PhaseName, PhaseSet},
    project_phase::ProjectPhase,
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool, Row,
    types::{Uuid, time},
};
use tokio_util::sync::CancellationToken;

use support::ScratchDatabase;

const CHAIN: &str = "project-fixture";
const BASE_CHAIN: &str = "base-mainnet";
const ETHEREUM_CHAIN: &str = "ethereum-mainnet";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";
const TRANSFER_OWNER: &str = "0x00000000000000000000000000000000000000a2";
const RESOLVER: &str = "0x00000000000000000000000000000000000000b1";
const BASENAMES_RESOLVER: &str = "0xc6d566a56a1aff6508b41f6c90ff131615583bcd";
const BASENAMES_L1_RESOLVER: &str = "0xde9049636f4a1dfe0a64d1bfe3155c0a14c54f31";
const REGISTRAR: &str = "0x0000000000000000000000000000000000000042";
const SENDER: &str = "0x0000000000000000000000000000000000000043";
const REGISTRY: &str = "0x0000000000000000000000000000000000000044";
const WRAPPER: &str = "0x0000000000000000000000000000000000000045";
const REVERSE_REGISTRAR: &str = "0x0000000000000000000000000000000000000046";
const NORMALIZER: &str = "ensip15@ens-normalize-0.1.1";
const RESOURCE: &str = "00000000-0000-0000-0000-000000000011";
const SURFACE_BINDING: &str = "00000000-0000-0000-0000-000000000012";
const TOKEN_LINEAGE: &str = "00000000-0000-0000-0000-000000000013";
const BASENAMES_RESOURCE: &str = "00000000-0000-0000-0000-000000000031";
const EQUIVALENCE_BOB_RESOURCE: &str = "00000000-0000-0000-0000-0000000000b0";
const EQUIVALENCE_BOB_BINDING: &str = "00000000-0000-0000-0000-0000000000b1";
const EQUIVALENCE_PARENT_BINDING: &str = "00000000-0000-0000-0000-0000000000b3";
const EQUIVALENCE_TRANSFER_RESOURCE: &str = "00000000-0000-0000-0000-0000000000b4";
const EQUIVALENCE_TRANSFER_BINDING: &str = "00000000-0000-0000-0000-0000000000b5";
const EQUIVALENCE_V2_RESOLVER: &str = "0x00000000000000000000000000000000000000b2";
const EQUIVALENCE_TRANSFER_RESOLVER: &str = "0x00000000000000000000000000000000000000b3";
const PERMISSION_ONLY_RESOLVER: &str = "0x00000000000000000000000000000000000000b4";
const EQUIVALENCE_V2_IMPLEMENTATION: &str = "0x00000000000000000000000000000000000000c2";
const PERMISSION_ONLY_RESOURCE: &str = "00000000-0000-0000-0000-0000000000c0";
const PERMISSION_ONLY_BINDING: &str = "00000000-0000-0000-0000-0000000000c1";

type ChildLabelRow = (Vec<u8>, Option<String>, Vec<u8>, Option<String>);
type OptionalChildLabelRow = (
    Option<Vec<u8>>,
    Option<String>,
    Option<Vec<u8>>,
    Option<String>,
);
type TopologyOnlyChildRow = (
    Option<Vec<u8>>,
    Option<String>,
    Option<Vec<u8>>,
    Option<String>,
    String,
    String,
);
type PrimaryClaimRow = (String, String, Option<String>, bool, Option<String>);

sol! {
    event NameRegistered(
        string name,
        bytes32 indexed label,
        address indexed owner,
        uint256 expires
    );
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
    event NameWrapped(
        bytes32 indexed node,
        bytes name,
        address owner,
        uint32 fuses,
        uint64 expiry
    );
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event TextChanged(
        bytes32 indexed node,
        string indexed indexedKey,
        string key,
        string value
    );
    event ReverseClaimed(address indexed addr, bytes32 indexed node);
    event LabelRegistered(
        uint256 indexed tokenId,
        bytes32 indexed labelHash,
        string label,
        address owner,
        uint64 expiry,
        address indexed sender
    );
    event TokenResource(uint256 indexed tokenId, uint256 indexed resource);
    event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
}

#[tokio::test]
async fn canonical_fixture_builds_all_seven_projection_families() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_all_builders").await?;
    seed_project_fixture(scratch.pool()).await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    let grouped_builder_snapshot: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'permissions', COALESCE((
                 SELECT jsonb_agg(
                     to_jsonb(permission) - 'last_recomputed_at' - 'inserted_at'
                     ORDER BY resource_id, subject, scope
                 ) FROM permissions_current permission
             ), '[]'::jsonb),
             'permission_summaries', COALESCE((
                 SELECT jsonb_agg(
                     to_jsonb(summary) - 'last_recomputed_at' ORDER BY resource_id
                 ) FROM permissions_current_resource_summary summary
             ), '[]'::jsonb),
             'resolvers', COALESCE((
                 SELECT jsonb_agg(
                     (to_jsonb(resolver) - 'last_recomputed_at' - 'inserted_at' -
                         'declared_summary') || jsonb_build_object(
                             'declared_summary', declared_summary - 'bindings' - 'aliases' -
                                 'permissions' - 'role_holders'
                         )
                     ORDER BY chain_id, resolver_address
                 ) FROM resolver_current resolver
             ), '[]'::jsonb),
             'primary_names', COALESCE((
                 SELECT jsonb_agg(
                     to_jsonb(primary_name) -
                         'reverse_hydration_attempted_block_number' -
                         'reverse_hydration_attempted_block_hash' -
                         'reverse_hydration_attempt_ordinal'
                     ORDER BY address, coin_type, namespace)
                 FROM primary_names_current primary_name
             ), '[]'::jsonb)
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        grouped_builder_snapshot,
        json!({
            "permission_summaries": [{
                "authority_kind": "registrar",
                "canonicality_summary": {
                    "state": "canonical_lineage",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "chain_positions": {
                    "block_hash": "project-fixture-block-1",
                    "block_number": 1,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "manifest_version": 1,
                "provenance": {
                    "chain_id": "project-fixture",
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    }
                },
                "resource_id": RESOURCE,
                "root_resource_id": null,
                "support_status": "supported",
                "unsupported_reason": null
            }],
            "permissions": [{
                "canonicality_summary": {
                    "state": "canonical",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "chain_positions": {
                    "block_hash": "project-fixture-block-1",
                    "block_number": 1,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "effective_powers": ["resource_control"],
                "grant_source": {"kind": "fixture"},
                "inheritance_path": [],
                "manifest_version": 1,
                "provenance": {
                    "chain_id": "project-fixture",
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "derivation_kind": "permissions_current_rebuild",
                    "manifest_versions": [{
                        "manifest_version": 1,
                        "source_family": "ens_v1_registrar_l1",
                        "source_manifest_id": null
                    }],
                    "normalized_event_ids": [6],
                    "raw_fact_refs": [{}]
                },
                "resource_id": RESOURCE,
                "revocation_source": null,
                "scope": "resource",
                "scope_detail": {"kind": "resource"},
                "scope_kind": "resource",
                "subject": OWNER,
                "transfer_behavior": {"mode": "replace_on_authority_change"}
            }],
            "primary_names": [{
                "address": OWNER,
                "claim_name_is_normalized": true,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 10,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "reverse_event_id": 9,
                    "source_family": "ens_v1_reverse_l1",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "success",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": "alice.eth",
                "unsupported_reason": null
            }],
            "resolvers": [{
                "canonicality_summary": {
                    "state": "canonical_lineage",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "chain_id": "project-fixture",
                "chain_positions": {
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "declared_summary": {
                    "classification": {
                        "basis": "manifest_declared_address",
                        "role": "public_resolver",
                        "source_family": "ens_v1_resolver_l1"
                    },
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "event_summary": {
                        "status": "unsupported",
                        "unsupported_reason":
                            "resolver_binding_enumeration_not_projected"
                    }
                },
                "manifest_version": 1,
                "provenance": {
                    "chain_id": "project-fixture",
                    "manifest_event_id": 2,
                    "manifest_id": 2
                },
                "resolver_address": RESOLVER,
                "support_status": "supported",
                "unsupported_reason": null
            }]
        })
    );

    let counts: (i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT count(*) FROM name_current),
            (SELECT count(*) FROM children_current),
            (SELECT count(*) FROM permissions_current),
            (SELECT count(*) FROM permissions_current_resource_summary),
            (SELECT count(*) FROM record_inventory_current),
            (SELECT count(*) FROM resolver_current),
            (SELECT count(*) FROM address_names_current),
            (SELECT count(*) FROM primary_names_current)",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(counts, (2, 2, 1, 1, 1, 1, 3, 1));

    let evidence: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT family, status, exhaustiveness
         FROM (
             SELECT 'name' AS family,
                    min(declared_summary -> 'coverage' ->> 'status') AS status,
                    min(declared_summary -> 'coverage' ->> 'exhaustiveness') AS exhaustiveness
             FROM name_current
             UNION ALL
             SELECT 'children', min(provenance -> 'coverage' ->> 'status'),
                    min(provenance -> 'coverage' ->> 'exhaustiveness')
             FROM children_current
             UNION ALL
             SELECT 'permissions', min(provenance -> 'coverage' ->> 'status'),
                    min(provenance -> 'coverage' ->> 'exhaustiveness')
             FROM permissions_current
             UNION ALL
             SELECT 'permission_summary', min(provenance -> 'coverage' ->> 'status'),
                    min(provenance -> 'coverage' ->> 'exhaustiveness')
             FROM permissions_current_resource_summary
             UNION ALL
             SELECT 'record_inventory', min(provenance -> 'coverage' ->> 'status'),
                    min(provenance -> 'coverage' ->> 'exhaustiveness')
             FROM record_inventory_current
             UNION ALL
             SELECT 'resolver', min(declared_summary -> 'coverage' ->> 'status'),
                    min(declared_summary -> 'coverage' ->> 'exhaustiveness')
             FROM resolver_current
             UNION ALL
             SELECT 'address_names', min(provenance -> 'coverage' ->> 'status'),
                    min(provenance -> 'coverage' ->> 'exhaustiveness')
             FROM address_names_current
             UNION ALL
             SELECT 'primary_names', min(claim_provenance -> 'coverage' ->> 'status'),
                    min(claim_provenance -> 'coverage' ->> 'exhaustiveness')
             FROM primary_names_current
         ) coverage
         ORDER BY family",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(evidence.len(), 8);
    assert!(evidence.iter().all(
        |(_, status, exhaustiveness)| status == "projected" && exhaustiveness == "not_asserted"
    ));
    let resolver_status: (String, String) = sqlx::query_as(
        "SELECT support_status,
                declared_summary -> 'classification' ->> 'basis'
         FROM resolver_current WHERE resolver_address = lower($1)",
    )
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        resolver_status,
        ("supported".into(), "manifest_declared_address".into())
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn recompute_flags_refreshes_same_class_flags_and_primary_projection_without_replay()
-> Result<()> {
    let scratch = ScratchDatabase::create("production_project_recompute_flags").await?;
    seed_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(CHAIN).await?;
    seed_completed_project_extent(scratch.pool(), CHAIN, 3).await?;

    sqlx::query(
        "UPDATE label_preimages
         SET normalizer_version = 'stale-version',
             normalized_under_version = false,
             normalization_error = 'stale flag'
         WHERE decoded_label = 'alice'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE name_surfaces SET normalizer_version = 'stale-version' WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE primary_names_current SET claim_name_is_normalized = false WHERE namespace = 'ens'",
    )
    .execute(scratch.pool())
    .await?;

    let events_before: Vec<(i64, Value, Value, String)> = sqlx::query_as(
        "SELECT normalized_event_id, before_state, after_state, canonicality_state::text
         FROM normalized_events WHERE chain_id = $1 ORDER BY normalized_event_id",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;
    let anchors_before: Vec<(String, String, i64, Value, time::OffsetDateTime)> = sqlx::query_as(
        "SELECT logical_name_id, block_hash, block_number, provenance, observed_at
         FROM name_surfaces WHERE chain_id = $1 ORDER BY logical_name_id",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;
    let bindings_before: Vec<(Uuid, String, i64, Option<time::OffsetDateTime>, String)> =
        sqlx::query_as(
            "SELECT surface_binding_id, block_hash, block_number, active_to,
                    canonicality_state::text
             FROM surface_bindings WHERE chain_id = $1 ORDER BY surface_binding_id",
        )
        .bind(CHAIN)
        .fetch_all(scratch.pool())
        .await?;

    let phases = PhaseSet::with_ingest_interpret_and_project(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(InterpretPhase::new(scratch.pool().clone())),
        Arc::new(ProjectPhase::new(scratch.pool().clone())),
    )?;
    PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-project-recompute-flags",
        test_timing(),
    )?
    .redo(
        &chain_config(CHAIN)?,
        RedoPhase::RecomputeFlags,
        BlockRange::new(0, 3)?,
        CancellationToken::new(),
    )
    .await?;

    let label: (String, bool, Option<String>) = sqlx::query_as(
        "SELECT normalizer_version, normalized_under_version, normalization_error
         FROM label_preimages WHERE decoded_label = 'alice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(label, (NORMALIZER.into(), true, None));
    let stale_surfaces: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM name_surfaces
         WHERE chain_id = $1 AND normalizer_version <> $2",
    )
    .bind(CHAIN)
    .bind(NORMALIZER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(stale_surfaces, 0);
    let claim_is_normalized: bool = sqlx::query_scalar(
        "SELECT claim_name_is_normalized FROM primary_names_current WHERE namespace = 'ens'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(claim_is_normalized);
    let pending_redos: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project') AND redo_in_progress",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(pending_redos, 0, "same-class names must not stamp replay");

    let events_after: Vec<(i64, Value, Value, String)> = sqlx::query_as(
        "SELECT normalized_event_id, before_state, after_state, canonicality_state::text
         FROM normalized_events WHERE chain_id = $1 ORDER BY normalized_event_id",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;
    let anchors_after: Vec<(String, String, i64, Value, time::OffsetDateTime)> = sqlx::query_as(
        "SELECT logical_name_id, block_hash, block_number, provenance, observed_at
         FROM name_surfaces WHERE chain_id = $1 ORDER BY logical_name_id",
    )
    .bind(CHAIN)
    .fetch_all(scratch.pool())
    .await?;
    let bindings_after: Vec<(Uuid, String, i64, Option<time::OffsetDateTime>, String)> =
        sqlx::query_as(
            "SELECT surface_binding_id, block_hash, block_number, active_to,
                    canonicality_state::text
             FROM surface_bindings WHERE chain_id = $1 ORDER BY surface_binding_id",
        )
        .bind(CHAIN)
        .fetch_all(scratch.pool())
        .await?;
    assert_eq!(events_after, events_before);
    assert_eq!(anchors_after, anchors_before);
    assert_eq!(bindings_after, bindings_before);
    scratch.cleanup().await
}

#[tokio::test]
async fn permission_builder_preserves_grouped_history_output_exactly() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_permission_history").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_namespaced_event(
        scratch.pool(),
        "ens",
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registry_l1",
        3,
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control","set_resolver"],
            "grant_source":{"kind":"tie_break_winner"},
            "revocation_source":null,
            "inheritance_path":["tie_break_winner"],
            "transfer_behavior":"retain"
        }),
        json!({"history":"tie_break_winner"}),
    )
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "ens",
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registry_l1",
        2,
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["set_resolver"],
            "grant_source":{"kind":"later_inserted_loser"},
            "revocation_source":null,
            "inheritance_path":["later_inserted_loser"],
            "transfer_behavior":"retain"
        }),
        json!({"history":"later_inserted_loser"}),
    )
    .await?;
    let positioned = sqlx::query(
        "UPDATE normalized_events event
         SET transaction_hash = $2,
             transaction_index = position.transaction_index,
             log_index = position.log_index
         FROM (VALUES
             ('tie_break_winner', 5::bigint, 9::bigint),
             ('later_inserted_loser', 5::bigint, 8::bigint)
         ) position(history, transaction_index, log_index)
         WHERE event.chain_id = $1
           AND event.raw_fact_ref ->> 'history' = position.history",
    )
    .bind(CHAIN)
    .bind(format!("{CHAIN}-permission-history-transaction"))
    .execute(scratch.pool())
    .await?
    .rows_affected();
    assert_eq!(positioned, 2);

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let snapshot: Value = sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'permission', (
                 SELECT to_jsonb(permission) - 'last_recomputed_at' - 'inserted_at'
                 FROM permissions_current permission
                 WHERE resource_id = $1 AND subject = lower($2) AND scope = 'resource'
             ),
             'resource_summary', (
                 SELECT to_jsonb(summary) - 'last_recomputed_at'
                 FROM permissions_current_resource_summary summary
                 WHERE resource_id = $1
             )
         )",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(OWNER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        snapshot,
        json!({
            "permission": {
                "canonicality_summary": {
                    "state": "canonical",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "chain_positions": {
                    "block_hash": "project-fixture-block-3",
                    "block_number": 3,
                    "log_index": 9,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3,
                    "transaction_index": 5
                },
                "effective_powers": ["resource_control", "set_resolver"],
                "grant_source": {"kind": "tie_break_winner"},
                "inheritance_path": ["tie_break_winner"],
                "manifest_version": 3,
                "provenance": {
                    "chain_id": "project-fixture",
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "derivation_kind": "permissions_current_rebuild",
                    "manifest_versions": [
                        {
                            "manifest_version": 1,
                            "source_family": "ens_v1_registrar_l1",
                            "source_manifest_id": null
                        },
                        {
                            "manifest_version": 3,
                            "source_family": "ens_v1_registry_l1",
                            "source_manifest_id": null
                        },
                        {
                            "manifest_version": 2,
                            "source_family": "ens_v1_registry_l1",
                            "source_manifest_id": null
                        }
                    ],
                    "normalized_event_ids": [6, 13, 14],
                    "raw_fact_refs": [
                        {},
                        {"history": "tie_break_winner"},
                        {"history": "later_inserted_loser"}
                    ]
                },
                "resource_id": RESOURCE,
                "revocation_source": null,
                "scope": "resource",
                "scope_detail": {"kind": "resource"},
                "scope_kind": "resource",
                "subject": OWNER,
                "transfer_behavior": {"mode": "retain"}
            },
            "resource_summary": {
                "authority_kind": "registrar",
                "canonicality_summary": {
                    "state": "canonical_lineage",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "chain_positions": {
                    "block_hash": "project-fixture-block-3",
                    "block_number": 3,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "manifest_version": 3,
                "provenance": {
                    "chain_id": "project-fixture",
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "history": "tie_break_winner"
                },
                "resource_id": RESOURCE,
                "root_resource_id": null,
                "support_status": "supported",
                "unsupported_reason": null
            }
        })
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn shadow_identity_labels_retain_bytes_and_decode_only_exact_text() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_shadow_labels").await?;
    seed_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    let labels: Vec<ChildLabelRow> = sqlx::query_as(
        "SELECT raw_label, decoded_label, raw_name, decoded_name
         FROM children_current
         ORDER BY child_logical_name_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        labels,
        vec![
            (
                b"alice".to_vec(),
                Some("alice".into()),
                b"alice.eth".to_vec(),
                Some("alice.eth".into())
            ),
            (
                vec![0xff, 0x00],
                None,
                vec![0xff, 0x00, b'.', b'e', b't', b'h'],
                None
            )
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn primary_name_builder_preserves_success_invalid_blank_and_byte_only_claims() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_primary_claims").await?;
    seed_project_fixture(scratch.pool()).await?;
    for (index, address, raw_name) in [
        (
            10,
            "0x0000000000000000000000000000000000000010",
            Some("Alice.eth"),
        ),
        (
            11,
            "0x0000000000000000000000000000000000000011",
            Some("bad name.eth"),
        ),
        (
            12,
            "0x0000000000000000000000000000000000000012",
            Some("  \t"),
        ),
        (13, "0x0000000000000000000000000000000000000013", None),
    ] {
        insert_event(
            scratch.pool(),
            CHAIN,
            3,
            None,
            None,
            "ReverseChanged",
            "ens_v1_reverse_l1",
            json!({"address":address,"coin_type":"60","namespace":"ens"}),
            json!({}),
        )
        .await?;
        let mut claim = json!({
            "record_key":"name",
            "primary_claim_source":{
                "address":address,
                "coin_type":"60",
                "namespace":"ens"
            }
        });
        if let Some(raw_name) = raw_name {
            claim["raw_name"] = json!(raw_name);
        } else {
            claim["raw_name_bytes"] = json!({"encoding":"hex","bytes":"0xff00"});
        }
        insert_event(
            scratch.pool(),
            CHAIN,
            3,
            None,
            None,
            "RecordChanged",
            "ens_v1_reverse_l1",
            claim,
            json!({"fixture_index":index}),
        )
        .await?;
    }
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        None,
        None,
        "RecordChanged",
        "ens_v1_reverse_l1",
        json!({
            "raw_name":"stale.eth",
            "primary_claim_source":{
                "address":OWNER,
                "coin_type":"60",
                "namespace":"ens",
                "claim_provenance":{"history":"stale"}
            }
        }),
        json!({"history":"stale"}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        None,
        None,
        "RecordChanged",
        "ens_v1_reverse_l1",
        json!({
            "raw_name":"latest.eth",
            "primary_claim_source":{
                "address":OWNER,
                "coin_type":"60",
                "namespace":"ens",
                "claim_provenance":{
                    "history":"latest",
                    "source_family":"ens_v1_reverse_l1"
                }
            }
        }),
        json!({"history":"latest"}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let full_claims: Value = sqlx::query_scalar(
        "SELECT jsonb_agg(
             to_jsonb(primary_name) -
                 'reverse_hydration_attempted_block_number' -
                 'reverse_hydration_attempted_block_hash' -
                 'reverse_hydration_attempt_ordinal'
             ORDER BY address, coin_type, namespace)
         FROM primary_names_current primary_name",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        full_claims,
        json!([
            {
                "address": "0x0000000000000000000000000000000000000010",
                "claim_name_is_normalized": false,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 14,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "reverse_event_id": 13,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "success",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": "Alice.eth",
                "unsupported_reason": null
            },
            {
                "address": "0x0000000000000000000000000000000000000011",
                "claim_name_is_normalized": false,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 16,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "reverse_event_id": 15,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "invalid_name",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": "bad name.eth",
                "unsupported_reason": null
            },
            {
                "address": "0x0000000000000000000000000000000000000012",
                "claim_name_is_normalized": false,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 18,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "reverse_event_id": 17,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "not_found",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": null,
                "unsupported_reason": null
            },
            {
                "address": "0x0000000000000000000000000000000000000013",
                "claim_name_is_normalized": false,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 20,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "reverse_event_id": 19,
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "unsupported",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": null,
                "unsupported_reason": "claim_name_not_decodable"
            },
            {
                "address": OWNER,
                "claim_name_is_normalized": true,
                "claim_provenance": {
                    "chain_id": "project-fixture",
                    "claim_event_id": 22,
                    "coverage": {
                        "exhaustiveness": "not_asserted",
                        "status": "projected"
                    },
                    "history": "latest",
                    "reverse_event_id": 9,
                    "source_family": "ens_v1_reverse_l1",
                    "target_block_hash": "project-fixture-block-3",
                    "target_block_number": 3
                },
                "claim_status": "success",
                "coin_type": "60",
                "namespace": "ens",
                "raw_claim_name": "latest.eth",
                "unsupported_reason": null
            }
        ])
    );
    let claims: Vec<PrimaryClaimRow> = sqlx::query_as(
        "SELECT address, claim_status, raw_claim_name,
                claim_name_is_normalized, unsupported_reason
         FROM primary_names_current
         WHERE address <> lower($1)
         ORDER BY address",
    )
    .bind(OWNER)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        claims,
        vec![
            (
                "0x0000000000000000000000000000000000000010".into(),
                "success".into(),
                Some("Alice.eth".into()),
                false,
                None
            ),
            (
                "0x0000000000000000000000000000000000000011".into(),
                "invalid_name".into(),
                Some("bad name.eth".into()),
                false,
                None
            ),
            (
                "0x0000000000000000000000000000000000000012".into(),
                "not_found".into(),
                None,
                false,
                None
            ),
            (
                "0x0000000000000000000000000000000000000013".into(),
                "unsupported".into(),
                None,
                false,
                Some("claim_name_not_decodable".into())
            )
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_rejects_an_observed_only_range_end_before_claiming_state() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_observed_redo_end").await?;
    let chain = "project-observed-redo-end";
    seed_lineage(scratch.pool(), chain, 2).await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 3, to_timestamp(3), 'observed')",
    )
    .bind(chain)
    .bind(block_hash(chain, 3))
    .bind(block_hash(chain, 2))
    .execute(scratch.pool())
    .await?;
    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_project_extent(scratch.pool(), chain, 3).await?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
            Arc::new(ProjectPhase::new(scratch.pool().clone())),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "production-project-observed-redo-end",
        test_timing(),
    )?;

    let error = runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(3, 3)?,
            CancellationToken::new(),
        )
        .await
        .expect_err("observed staging must not become a project redo marker");
    assert_eq!(error.kind(), phase_runner::error::ErrorKind::DataIntegrity);
    assert!(
        error
            .to_string()
            .contains("not readable (canonical, safe, or finalized)")
    );
    assert!(!error.to_string().contains("not canonical"));
    let state: (String, bool, Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress, redo_mode, redo_current_block_number
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(state, ("completed".into(), false, None, None));
    scratch.cleanup().await
}

#[tokio::test]
async fn record_version_boundary_excludes_prior_records_and_keeps_later_records() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_record_version").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RecordVersionChanged",
        "ens_v1_resolver_l1",
        json!({"resolver":RESOLVER,"record_version":"1"}),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:avatar",
            "record_family":"text",
            "selector_key":"avatar",
            "value_retained":true,
            "value":"ipfs://avatar"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let inventory: (String, Value, Value, Value) = sqlx::query_as(
        "SELECT record_version_boundary_key, record_version_boundary,
                selectors, entries
         FROM record_inventory_current",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(inventory.0.starts_with("ens:0xalice:"));
    assert_eq!(
        inventory.1["event_kind"],
        Value::String("RecordVersionChanged".into())
    );
    assert_eq!(inventory.1["logical_name_id"], "ens:0xalice");
    assert_eq!(inventory.1["resource_id"], RESOURCE);
    assert_eq!(
        inventory.2,
        json!([{
            "record_key":"text:avatar",
            "record_family":"text",
            "selector_key":"avatar",
            "cacheable":true
        }])
    );
    assert_eq!(inventory.3.as_array().map(Vec::len), Some(1));
    assert_eq!(inventory.3[0]["record_key"], "text:avatar");
    scratch.cleanup().await
}

#[tokio::test]
async fn resolver_clear_retracts_inventory_instead_of_reviving_an_older_pointer() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_resolver_clear").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":"0x0000000000000000000000000000000000000000"}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let inventory_count: i64 = sqlx::query_scalar("SELECT count(*) FROM record_inventory_current")
        .fetch_one(scratch.pool())
        .await?;
    let zero_resolver_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM resolver_current
         WHERE resolver_address = '0x0000000000000000000000000000000000000000'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!((inventory_count, zero_resolver_count), (0, 0));
    scratch.cleanup().await
}

#[tokio::test]
async fn undeclared_v1_resolver_is_explicitly_unsupported_without_code_hash_evidence() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_project_undeclared_resolver").await?;
    seed_project_fixture(scratch.pool()).await?;
    let unknown = "0x00000000000000000000000000000000000000b2";
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":unknown}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let classification: (String, Option<String>, String) = sqlx::query_as(
        "SELECT support_status, unsupported_reason,
                declared_summary -> 'classification' ->> 'basis'
         FROM resolver_current WHERE resolver_address = $1",
    )
    .bind(unknown)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        classification,
        (
            "unsupported".into(),
            Some("resolver_not_declared".into()),
            "manifest_declared_address".into()
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn exact_name_support_keeps_mixed_and_unpromoted_v2_status_explicit() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_name_support").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityEpochChanged",
        "ens_v2_registry_l1",
        json!({"authority_kind":"ens_v2_registry"}),
        json!({}),
    )
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let mixed: (String, Option<String>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        mixed,
        (
            "unsupported".into(),
            Some("mixed_ensv1_ensv2_exact_name_corpus".into())
        )
    );

    sqlx::query(
        "UPDATE normalized_events
         SET source_family = CASE
             WHEN event_kind IN ('RegistrationGranted', 'RegistrationRenewed')
                 THEN 'ens_v2_registrar_l1'
             ELSE 'ens_v2_registry_l1'
         END
         WHERE chain_id = $1 AND logical_name_id = 'ens:0xalice'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let shadow: (String, Option<String>) = sqlx::query_as(
        "SELECT support_status, unsupported_reason
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        shadow,
        (
            "unsupported".into(),
            Some("ensv2_exact_name_profile_shadow".into())
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn authority_epoch_rebind_updates_the_current_registration_authority() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_authority_epoch").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityEpochChanged",
        "ens_v1_registry_l1",
        json!({
            "authority_kind":"registry_only",
            "authority_key":"registry:project-fixture:0xalice",
            "registry_owner":OWNER
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let registration: Value = sqlx::query_scalar(
        "SELECT declared_summary -> 'registration'
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(registration["authority_kind"], "registry_only");
    assert_eq!(
        registration["authority_key"],
        "registry:project-fixture:0xalice"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn expiry_fold_ignores_malformed_updates_and_clears_unrepresentable_numbers() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_expiry_fold").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":1_900_000_000_i64}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RegistrationRenewed",
        "ens_v1_registrar_l1",
        json!({"expiry":"not-a-number"}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let summary: Value = sqlx::query_scalar(
        "SELECT declared_summary
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(summary["registration"]["expiry"], 1_900_000_000_i64);
    assert_eq!(summary["control"]["expiry"], "2030-03-17T17:46:40Z");

    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":u64::MAX}),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let summary: Value = sqlx::query_scalar(
        "SELECT declared_summary
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(summary["registration"]["expiry"], Value::Null);
    assert_eq!(
        summary["registration"]["latest_event_kind"],
        "ExpiryChanged"
    );
    assert_eq!(summary["control"]["expiry"], Value::Null);
    scratch.cleanup().await
}

#[tokio::test]
async fn permission_support_preserves_known_wrapper_and_unknown_authority_status() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_permission_support").await?;
    seed_project_fixture(scratch.pool()).await?;
    for (resource_id, authority_kind) in [
        ("00000000-0000-0000-0000-000000000021", "registrar"),
        ("00000000-0000-0000-0000-000000000022", "wrapper"),
        ("00000000-0000-0000-0000-000000000023", "future_authority"),
    ] {
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, chain_id, block_hash, block_number,
                 provenance, canonicality_state
             ) VALUES (
                 $1, $2, $3, 1,
                 jsonb_build_object(
                     'authority_kind', $4::text,
                     'source_family', 'ens_v1_registrar_l1',
                     'manifest_version', 1
                 ),
                 'canonical'
             )",
        )
        .bind(Uuid::parse_str(resource_id)?)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .bind(authority_kind)
        .execute(scratch.pool())
        .await?;
    }

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let support: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT authority_kind, support_status, unsupported_reason
         FROM permissions_current_resource_summary
         WHERE resource_id IN (
             '00000000-0000-0000-0000-000000000021'::uuid,
             '00000000-0000-0000-0000-000000000022'::uuid,
             '00000000-0000-0000-0000-000000000023'::uuid
         )
         ORDER BY resource_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        support,
        vec![
            ("registrar".into(), "supported".into(), None),
            (
                "wrapper".into(),
                "unsupported".into(),
                Some("ensv1_wrapper_holder_permissions_not_projected".into())
            ),
            (
                "future_authority".into(),
                "unsupported".into(),
                Some("resource_permission_authority_not_projected".into())
            ),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn retained_name_and_resolver_summary_sections_are_projected() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_retained_summaries").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_resolver_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resolver","chain_id":CHAIN,"resolver_address":RESOLVER},
            "effective_powers":["resolver_control"],
            "grant_source":{"kind":"fixture"},
            "inheritance_path":[],
            "transfer_behavior":"retain"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AliasChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "active":true,
            "from_name":"alias.alice.eth",
            "to_name":"alice.eth",
            "to_logical_name_id":"ens:0xalice",
            "to_resource_id":RESOURCE
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let summaries: (Value, Value) = sqlx::query_as(
        "SELECT
             (SELECT declared_summary FROM name_current
              WHERE logical_name_id = 'ens:0xalice'),
             (SELECT declared_summary FROM resolver_current
              WHERE resolver_address = $1)",
    )
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;

    assert_eq!(summaries.0["registration"]["registrant"], OWNER);
    assert_eq!(summaries.0["control"]["registry_owner"], OWNER);
    assert_eq!(summaries.0["resolver"]["address"], RESOLVER);
    assert_eq!(summaries.0["record_inventory"]["status"], "unsupported");
    assert!(summaries.0["history"]["surface_head"].is_object());
    assert!(summaries.0["history"]["resource_head"].is_object());

    assert_eq!(
        summaries.1["classification"]["basis"],
        "manifest_declared_address"
    );
    for section in [
        "bindings",
        "aliases",
        "permissions",
        "role_holders",
        "event_summary",
    ] {
        assert_eq!(summaries.1[section]["status"], "unsupported");
        assert_eq!(
            summaries.1[section]["unsupported_reason"],
            "resolver_binding_enumeration_not_projected"
        );
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn name_current_projects_retained_alias_resolution_topology() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_name_alias_topology").await?;
    seed_project_fixture(scratch.pool()).await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET binding_kind = 'resolver_alias_path'
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AliasChanged",
        "ens_v2_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "active":true,
            "alias_state":"active",
            "from_name":"alice.eth",
            "from_namehash":"0xalice",
            "to_name":"profile.alice.eth",
            "to_namehash":"0xprofile-alice",
            "to_logical_name_id":"ens:0xprofile-alice",
            "to_resource_id":RESOURCE
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let topology: Value = sqlx::query_scalar(
        "SELECT declared_summary -> 'topology'
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;

    assert_eq!(
        topology["registry_path"][0]["logical_name_id"],
        "ens:0xalice"
    );
    assert_eq!(topology["resolver_path"][0]["address"], RESOLVER);
    assert_eq!(
        topology["alias"]["final_target"]["logical_name_id"],
        "ens:0xprofile-alice"
    );
    assert_eq!(topology["wildcard"]["source"], Value::Null);
    assert_eq!(topology["transport"]["source_chain_id"], Value::Null);
    assert_eq!(
        topology["version_boundaries"]["record_version_boundary"]["resource_id"],
        RESOURCE
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_ignores_row_local_readable_wildcard_binding_on_orphaned_lineage() -> Result<()>
{
    let scratch = ScratchDatabase::create("production_project_orphaned_wildcard_scope").await?;
    seed_project_fixture(scratch.pool()).await?;

    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, 'project-fixture-losing-block-1', NULL, 1,
                   to_timestamp(1), 'orphaned')",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET binding_kind = 'observed_wildcard_path',
             block_hash = 'project-fixture-losing-block-1'
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "UPDATE name_current
         SET raw_name = 'sentinel.eth'
         WHERE logical_name_id = 'ens:0xeth'",
    )
    .execute(scratch.pool())
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;

    let retained_raw_name: String =
        sqlx::query_scalar("SELECT raw_name FROM name_current WHERE logical_name_id = 'ens:0xeth'")
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(retained_raw_name, "sentinel.eth");
    let binding_state: String = sqlx::query_scalar(
        "SELECT canonicality_state::TEXT
         FROM surface_bindings
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(binding_state, "canonical");

    scratch.cleanup().await
}

#[tokio::test]
async fn alias_projection_keeps_only_latest_state_and_honors_tombstones() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_alias_tombstone").await?;
    seed_basenames_project_fixture(scratch.pool()).await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET binding_kind = 'resolver_alias_path'
         WHERE logical_name_id = 'basenames:0xalice-base'",
    )
    .execute(scratch.pool())
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        2,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        1,
        json!({
            "resolver":BASENAMES_RESOLVER,
            "active":true,
            "from_name":"alice.base.eth",
            "from_namehash":"0xalice-base",
            "to_name":"old.base.eth",
            "to_logical_name_id":"basenames:0xold-base",
            "to_resource_id":BASENAMES_RESOURCE
        }),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        3,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        1,
        json!({
            "resolver":BASENAMES_RESOLVER,
            "active":true,
            "from_name":"alice.base.eth",
            "from_namehash":"0xalice-base",
            "to_name":"new.base.eth",
            "to_logical_name_id":"basenames:0xnew-base",
            "to_resource_id":BASENAMES_RESOURCE
        }),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 3).await?;
    let (topology, aliases): (Value, Value) = sqlx::query_as(
        "SELECT
             (SELECT declared_summary -> 'topology'
              FROM name_current
              WHERE logical_name_id = 'basenames:0xalice-base'),
             (SELECT declared_summary -> 'aliases' -> 'items'
              FROM resolver_current
              WHERE chain_id = $1 AND resolver_address = $2)",
    )
    .bind(BASE_CHAIN)
    .bind(BASENAMES_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        topology["alias"]["final_target"]["logical_name_id"],
        "basenames:0xnew-base"
    );
    let alias_events = aliases
        .as_array()
        .expect("resolver aliases are an array")
        .iter()
        .filter(|item| item["latest_event_kind"] == "AliasChanged")
        .collect::<Vec<_>>();
    assert_eq!(alias_events.len(), 1);
    assert_eq!(
        alias_events[0]["to_logical_name_id"],
        "basenames:0xnew-base"
    );

    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        4,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        1,
        json!({
            "resolver":BASENAMES_RESOLVER,
            "active":false,
            "alias_state":"removed",
            "from_name":"alice.base.eth",
            "from_namehash":"0xalice-base"
        }),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;
    run_project(
        scratch.pool(),
        BASE_CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(BASE_CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let (topology, aliases): (Option<Value>, Value) = sqlx::query_as(
        "SELECT
             (SELECT declared_summary -> 'topology'
              FROM name_current
              WHERE logical_name_id = 'basenames:0xalice-base'),
             (SELECT declared_summary -> 'aliases' -> 'items'
              FROM resolver_current
              WHERE chain_id = $1 AND resolver_address = $2)",
    )
    .bind(BASE_CHAIN)
    .bind(BASENAMES_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert!(topology.is_none() || topology == Some(Value::Null));
    assert!(
        aliases
            .as_array()
            .expect("resolver aliases are an array")
            .iter()
            .all(|item| item["latest_event_kind"] != "AliasChanged")
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn basenames_projection_retains_execution_admission_and_both_chain_positions() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_basenames_support").await?;
    seed_basenames_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 2).await?;

    let projected = sqlx::query(
        "SELECT surface_binding_id, resource_id, token_lineage_id,
                declared_summary, provenance, chain_positions,
                canonicality_summary, manifest_version, last_recomputed_at
         FROM name_current
         WHERE logical_name_id = 'basenames:0xalice-base'",
    )
    .fetch_one(scratch.pool())
    .await?;
    let provenance: Value = projected.try_get("provenance")?;
    let chain_positions: Value = projected.try_get("chain_positions")?;
    assert!(
        provenance["manifest_versions"]
            .as_array()
            .is_some_and(|versions| versions.iter().any(|version| {
                version["source_family"] == "basenames_execution"
                    && version["manifest_version"] == 2
                    && version["chain"] == ETHEREUM_CHAIN
                    && version["deployment_epoch"] == "basenames_v1"
            }))
    );
    assert!(chain_positions.as_object().is_some_and(|positions| {
        positions
            .values()
            .any(|position| position["chain_id"] == BASE_CHAIN)
            && positions
                .values()
                .any(|position| position["chain_id"] == ETHEREUM_CHAIN)
    }));

    let declared_summary: Value = projected.try_get("declared_summary")?;
    assert_eq!(
        declared_summary["topology"]["transport"]["contract_address"], BASENAMES_L1_RESOLVER,
        "Project must publish the domain serializer's lowercase transport address"
    );
    let row = NameCurrentRow {
        logical_name_id: "basenames:0xalice-base".into(),
        namespace: "basenames".into(),
        canonical_display_name: "alice.base.eth".into(),
        normalized_name: "alice.base.eth".into(),
        namehash: "0xalice-base".into(),
        surface_binding_id: projected.try_get("surface_binding_id")?,
        resource_id: projected.try_get("resource_id")?,
        token_lineage_id: projected.try_get("token_lineage_id")?,
        binding_kind: Some(SurfaceBindingKind::DeclaredRegistryPath),
        coverage: declared_summary["coverage"].clone(),
        declared_summary,
        provenance,
        chain_positions,
        canonicality_summary: projected.try_get("canonicality_summary")?,
        manifest_version: projected.try_get("manifest_version")?,
        last_recomputed_at: projected.try_get("last_recomputed_at")?,
    };
    assert!(
        resolution_verified_support_boundary(&row, None).is_some(),
        "projected Basenames rows must remain inside the retained verified-resolution support class"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn target_block_uses_the_last_same_block_surface_binding() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_same_block_binding").await?;
    seed_project_fixture(scratch.pool()).await?;
    let successor_resource = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 3, 'canonical')",
    )
    .bind(successor_resource)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 3))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE surface_bindings
         SET active_to = to_timestamp(3) + interval '5 microseconds'
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'ens:0xalice', $2, 'declared_registry_path',
                   to_timestamp(3) + interval '5 microseconds', $3, $4, 3,
                   'canonical')",
    )
    .bind(Uuid::new_v4())
    .bind(successor_resource)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 3))
    .execute(scratch.pool())
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let projected_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM name_current
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(projected_resource, successor_resource);
    scratch.cleanup().await
}

#[tokio::test]
async fn shared_ensv1_resolver_keeps_fan_in_sections_explicitly_unsupported() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_shared_v1_resolver").await?;
    seed_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    let (support_status, summary): (String, Value) = sqlx::query_as(
        "SELECT support_status, declared_summary
         FROM resolver_current WHERE resolver_address = $1",
    )
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(support_status, "supported");
    for section in [
        "bindings",
        "aliases",
        "permissions",
        "role_holders",
        "event_summary",
    ] {
        assert_eq!(summary[section]["status"], "unsupported", "{section}");
        assert_eq!(
            summary[section]["unsupported_reason"], "resolver_binding_enumeration_not_projected",
            "{section}"
        );
        assert!(summary[section].get("count").is_none(), "{section}");
    }
    scratch.cleanup().await
}

#[tokio::test]
async fn resolver_embedded_collections_report_totals_and_cap_samples() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_resolver_summary_cap").await?;
    seed_basenames_project_fixture(scratch.pool()).await?;

    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         )
         SELECT ('10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                $1, $2, 1, 'canonical'
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name,
             namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state
         )
         SELECT 'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                'basenames', 'sample-' || lpad(ordinal::text, 3, '0') || '.base.eth',
                ARRAY['sample-' || lpad(ordinal::text, 3, '0'), 'base', 'eth'],
                decode('00', 'hex'), '0xsample' || lpad(ordinal::text, 3, '0'),
                ARRAY['0xsample' || lpad(ordinal::text, 3, '0'), '0xbase', '0xeth'],
                $1, 'active', $2, $3, 1, 'canonical'
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(NORMALIZER)
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             active_from, chain_id, block_hash, block_number, canonicality_state
         )
         SELECT ('20000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                ('10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                CASE WHEN ordinal = 100 THEN 'resolver_alias_path'
                     ELSE 'declared_registry_path' END,
                to_timestamp(1), $1, $2, 1, 'canonical'
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             raw_fact_ref, derivation_kind, canonicality_state, before_state, after_state
         )
         SELECT $1 || ':ResolverChanged:sample:' || ordinal,
                'basenames', 'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                ('10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                'ResolverChanged', 'basenames_base_registry', 1, $1, 2, $2,
                '{}'::jsonb, 'ens_v1_unwrapped_authority', 'canonical', '{}'::jsonb,
                jsonb_build_object('resolver', $3::text)
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 2))
    .bind(BASENAMES_RESOLVER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             raw_fact_ref, derivation_kind, canonicality_state, before_state, after_state
         )
         SELECT $1 || ':AliasChanged:sample:' || ordinal,
                'basenames', 'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                ('10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                'AliasChanged', 'basenames_base_resolver', 1, $1, 3, $2,
                jsonb_build_object('emitting_address', $3::text),
                'ens_v1_unwrapped_authority', 'canonical', '{}'::jsonb,
                jsonb_build_object(
                    'resolver', $3::text, 'active', true,
                    'from_name', 'alias-' || lpad(ordinal::text, 3, '0') || '.base.eth',
                    'to_name', 'sample-' || lpad(ordinal::text, 3, '0') || '.base.eth',
                    'to_logical_name_id',
                        'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                    'to_resource_id',
                        '10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0')
                )
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 3))
    .bind(BASENAMES_RESOLVER)
    .execute(scratch.pool())
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        3,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        1,
        json!({
            "resolver":BASENAMES_RESOLVER,
            "active":true,
            "from_name":"alias.alice.base.eth",
            "to_name":"alice.base.eth",
            "to_logical_name_id":"basenames:0xalice-base",
            "to_resource_id":BASENAMES_RESOURCE
        }),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             raw_fact_ref, derivation_kind, canonicality_state, before_state, after_state
         )
         SELECT $1 || ':PermissionChanged:sample:' || ordinal,
                'basenames', 'basenames:0xsample' || lpad(ordinal::text, 3, '0'),
                ('10000000-0000-0000-0000-' || lpad(ordinal::text, 12, '0'))::uuid,
                'PermissionChanged', 'basenames_base_resolver', 1, $1, 4, $2,
                jsonb_build_object('emitting_address', $3::text),
                'ens_v1_unwrapped_authority', 'canonical', '{}'::jsonb,
                jsonb_build_object(
                    'subject', '0x' || lpad(to_hex(ordinal), 40, '0'),
                    'scope', jsonb_build_object(
                        'kind', 'resolver', 'chain_id', $1::text,
                        'resolver_address', $3::text
                    ),
                    'effective_powers', jsonb_build_array('record_write'),
                    'grant_source', jsonb_build_object('kind', 'fixture'),
                    'inheritance_path', '[]'::jsonb,
                    'transfer_behavior', 'replace_on_authority_change'
                )
         FROM generate_series(1, 100) AS ordinal",
    )
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 4))
    .bind(BASENAMES_RESOLVER)
    .execute(scratch.pool())
    .await?;
    insert_namespaced_event(
        scratch.pool(),
        "basenames",
        BASE_CHAIN,
        4,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "PermissionChanged",
        "basenames_base_resolver",
        1,
        json!({
            "subject":"0x0000000000000000000000000000000000000000",
            "scope":{
                "kind":"resolver",
                "chain_id":BASE_CHAIN,
                "resolver_address":BASENAMES_RESOLVER
            },
            "effective_powers":["record_write","set_resolver"],
            "grant_source":{"kind":"fixture"},
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":BASENAMES_RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 4).await?;
    let summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = $2",
    )
    .bind(BASE_CHAIN)
    .bind(BASENAMES_RESOLVER)
    .fetch_one(scratch.pool())
    .await?;

    let bindings = &summary["bindings"];
    assert_eq!(bindings["count"], 101);
    assert_eq!(bindings["total_count"], 101);
    assert_eq!(bindings["sample_limit"], 100);
    assert_eq!(bindings["sample_count"], 100);
    assert_eq!(bindings["truncated"], true);
    let items = bindings["items"].as_array().expect("binding sample array");
    assert_eq!(items.len(), 100);
    assert_eq!(items[0]["raw_name"], "alice.base.eth");
    assert_eq!(items[99]["raw_name"], "sample-099.base.eth");
    assert!(
        items
            .iter()
            .all(|item| item["raw_name"] != "sample-100.base.eth")
    );

    assert_eq!(summary["aliases"]["total_count"], 102);
    assert_eq!(summary["aliases"]["sample_limit"], 100);
    assert_eq!(summary["aliases"]["sample_count"], 100);
    assert_eq!(summary["aliases"]["truncated"], true);
    let aliases = summary["aliases"]["items"]
        .as_array()
        .expect("alias sample array");
    assert_eq!(aliases[0]["binding_kind"], "resolver_alias_path");
    assert_eq!(aliases[0]["raw_name"], "sample-100.base.eth");
    assert_eq!(aliases[1]["from_name"], "alias.alice.base.eth");
    assert_eq!(aliases[99]["from_name"], "alias-098.base.eth");
    assert!(aliases.iter().all(|item| {
        item["from_name"] != "alias-099.base.eth" && item["from_name"] != "alias-100.base.eth"
    }));

    for section in ["permissions", "role_holders"] {
        assert_eq!(summary[section]["total_count"], 101, "{section}");
        assert_eq!(summary[section]["sample_limit"], 100, "{section}");
        assert_eq!(summary[section]["sample_count"], 100, "{section}");
        assert_eq!(summary[section]["truncated"], true, "{section}");
        assert_eq!(
            summary[section]["items"].as_array().map(Vec::len),
            Some(100)
        );
    }
    assert!(
        summary["role_holders"]["items"][0]
            .get("resource_ids")
            .is_none()
    );
    let role_holders = summary["role_holders"]["items"]
        .as_array()
        .expect("role-holder sample array");
    assert_eq!(
        role_holders[0]["subject"],
        "0x0000000000000000000000000000000000000000"
    );
    assert_eq!(
        role_holders[99]["subject"],
        "0x0000000000000000000000000000000000000063"
    );
    assert!(
        role_holders
            .iter()
            .all(|item| { item["subject"] != "0x0000000000000000000000000000000000000064" })
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn children_follow_topology_tombstones_not_surface_suffixes() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_children_tombstone").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xalice",
            "labelhash":"0xalice-label",
            "owner":"0x0000000000000000000000000000000000000000"
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let children: Vec<String> = sqlx::query_scalar(
        "SELECT child_logical_name_id FROM children_current
         ORDER BY child_logical_name_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(children, vec!["ens:0xhostile"]);
    let surface_still_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM name_surfaces
             WHERE logical_name_id = 'ens:0xalice'
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(surface_still_exists);
    scratch.cleanup().await
}

#[tokio::test]
async fn topology_only_child_is_published_then_upgraded_by_a_late_preimage() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_topology_only_child").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 5).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xmystery",
            "labelhash":"0xmystery-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;

    let topology_only: TopologyOnlyChildRow = sqlx::query_as(
        "SELECT raw_name, decoded_name, raw_label, decoded_label, namehash, labelhash
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xeth'
           AND child_logical_name_id = 'ens:0xmystery'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        topology_only,
        (
            None,
            None,
            None,
            None,
            "0xmystery".into(),
            "0xmystery-label".into()
        )
    );

    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES (
             '0xmystery-label', convert_to('mystery', 'UTF8'), 'mystery', $1,
             true, 'fixture', 1
         )",
    )
    .bind(NORMALIZER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             'ens:0xmystery', 'ens', 'mystery.eth', ARRAY['mystery','eth'],
             decode('00', 'hex'), '0xmystery',
             ARRAY['0xmystery-label','0xeth'], $1, 'active', $2, $3, 5,
             'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 5))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        5,
        Some("ens:0xmystery"),
        None,
        "PreimageObserved",
        "ens_v1_registry_l1",
        json!({
            "labelhash":"0xmystery-label",
            "raw_labels_hex":["6d797374657279","657468"]
        }),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;

    let upgraded: OptionalChildLabelRow = sqlx::query_as(
        "SELECT raw_name, decoded_name, raw_label, decoded_label
             FROM children_current
             WHERE parent_logical_name_id = 'ens:0xeth'
               AND child_logical_name_id = 'ens:0xmystery'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        upgraded,
        (
            Some(b"mystery.eth".to_vec()),
            Some("mystery.eth".into()),
            Some(b"mystery".to_vec()),
            Some("mystery".into())
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn parent_preimage_incrementally_publishes_an_existing_child_edge() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_parent_preimage_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 5).await?;
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES (
             '0xleaf-label', convert_to('leaf', 'UTF8'), 'leaf', $1,
             true, 'fixture', 1
         )",
    )
    .bind(NORMALIZER)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xparent",
            "child_node":"0xleaf",
            "labelhash":"0xleaf-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    let absent_before_parent: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS (
             SELECT 1 FROM children_current
             WHERE child_logical_name_id = 'ens:0xleaf'
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(absent_before_parent);

    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             'ens:0xparent', 'ens', 'parent.eth', ARRAY['parent','eth'],
             decode('00', 'hex'), '0xparent',
             ARRAY['0xparent-label','0xeth'], $1, 'active', $2, $3, 5,
             'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 5))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        5,
        Some("ens:0xparent"),
        None,
        "PreimageObserved",
        "ens_v1_registry_l1",
        json!({
            "labelhash":"0xparent-label",
            "raw_labels_hex":["706172656e74","657468"]
        }),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;

    let child: OptionalChildLabelRow = sqlx::query_as(
        "SELECT raw_name, decoded_name, raw_label, decoded_label
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xparent'
           AND child_logical_name_id = 'ens:0xleaf'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        child,
        (
            Some(b"leaf.parent.eth".to_vec()),
            Some("leaf.parent.eth".into()),
            Some(b"leaf".to_vec()),
            Some("leaf".into())
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn incremental_v2_child_renewal_retains_sibling_edges() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_v2_sibling_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    let subregistry_instance = Uuid::parse_str("00000000-0000-0000-0000-0000000000e2")?;
    let subregistry_address = "0x00000000000000000000000000000000000000e2";
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(subregistry_instance)
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number
         ) VALUES ($1, $2, $3, 0)",
    )
    .bind(subregistry_instance)
    .bind(CHAIN)
    .bind(subregistry_address)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             'ens:0xbob', 'ens', 'bob.eth', ARRAY['bob','eth'],
             decode('00', 'hex'), '0xbob', ARRAY['0xbob-label','0xeth'],
             $1, 'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES (
             '0xbob-label', convert_to('bob', 'UTF8'), 'bob', $1,
             true, 'fixture', 1
         )",
    )
    .bind(NORMALIZER)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        1,
        Some("ens:0xeth"),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":subregistry_address}),
        json!({}),
    )
    .await?;
    for (logical_name_id, label, registrant) in [
        ("ens:0xalice", "alice", OWNER),
        (
            "ens:0xbob",
            "bob",
            "0x00000000000000000000000000000000000000b0",
        ),
    ] {
        insert_event(
            scratch.pool(),
            CHAIN,
            1,
            Some(logical_name_id),
            None,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({
                "registry_contract_instance_id":subregistry_instance,
                "label":label,
                "registrant":registrant
            }),
            json!({}),
        )
        .await?;
    }
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let before: Vec<String> = sqlx::query_scalar(
        "SELECT child_logical_name_id FROM children_current
         WHERE parent_logical_name_id = 'ens:0xeth'
           AND child_logical_name_id IN ('ens:0xalice', 'ens:0xbob')
         ORDER BY child_logical_name_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(before, vec!["ens:0xalice", "ens:0xbob"]);

    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        None,
        "RegistrationRenewed",
        "ens_v2_registry_l1",
        json!({
            "registry_contract_instance_id":subregistry_instance,
            "registrant":OWNER
        }),
        json!({}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;

    let after: Vec<String> = sqlx::query_scalar(
        "SELECT child_logical_name_id FROM children_current
         WHERE parent_logical_name_id = 'ens:0xeth'
           AND child_logical_name_id IN ('ens:0xalice', 'ens:0xbob')
         ORDER BY child_logical_name_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(after, before);
    scratch.cleanup().await
}

#[tokio::test]
async fn incremental_topology_closure_rebuilds_the_whole_v1_subtree() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_v1_topology_fixpoint").await?;
    let full = ScratchDatabase::create("production_project_v1_topology_fixpoint_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        seed_deep_v1_topology_fixture(pool).await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 6).await?;
    assert_eq!(
        deep_v1_topology_edges(incremental.pool()).await?,
        expected_deep_v1_topology_edges(),
        "the initial full build must contain the complete v1 subtree"
    );

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 6,
            hash: block_hash(CHAIN, 6),
        }),
        RunMode::Normal,
        7,
        7,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 7).await?;

    let incremental_edges = deep_v1_topology_edges(incremental.pool()).await?;
    let full_edges = deep_v1_topology_edges(full.pool()).await?;
    assert_eq!(full_edges, expected_deep_v1_topology_edges());
    assert_eq!(
        incremental_edges, full_edges,
        "a narrow topology tick dropped an edge outside its frozen event set"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn candidate_topology_cannot_widen_incremental_child_scope() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_candidate_topology_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_deep_v1_topology_fixture(scratch.pool()).await?;
    seed_candidate_topology_bridge(scratch.pool()).await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 6).await?;
    let before: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xremote-parent'
           AND child_logical_name_id = 'ens:0xremote-child'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(before, 6);

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 6,
            hash: block_hash(CHAIN, 6),
        }),
        RunMode::Normal,
        7,
        7,
    )
    .await?;

    let after: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xremote-parent'
           AND child_logical_name_id = 'ens:0xremote-child'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        after, 6,
        "candidate-only topology evidence changed an unrelated projection row"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn orphaned_topology_cannot_widen_incremental_child_scope() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_orphaned_topology_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_deep_v1_topology_fixture(scratch.pool()).await?;
    seed_orphaned_topology_bridge(scratch.pool()).await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 6).await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 6,
            hash: block_hash(CHAIN, 6),
        }),
        RunMode::Normal,
        7,
        7,
    )
    .await?;

    let target: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xremote-parent'
           AND child_logical_name_id = 'ens:0xremote-child'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        target, 6,
        "orphaned topology evidence changed an unrelated projection row"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn current_window_orphaned_topology_cannot_widen_child_scope() -> Result<()> {
    const ORPHANED_HASH: &str = "project-fixture-orphaned-block-7";
    let scratch =
        ScratchDatabase::create("production_project_current_orphaned_topology_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    seed_deep_v1_topology_fixture(scratch.pool()).await?;
    seed_candidate_topology_bridge(scratch.pool()).await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 7, to_timestamp(7), 'orphaned')",
    )
    .bind(CHAIN)
    .bind(ORPHANED_HASH)
    .bind(block_hash(CHAIN, 6))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        7,
        Some("ens:0xremote-parent"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xn1",
            "child_node":"0xremote-parent",
            "labelhash":"0xremote-parent-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET block_hash = $2
         WHERE chain_id = $1
           AND block_number = 7
           AND event_kind = 'SubregistryChanged'
           AND after_state ->> 'node' = '0xn1'",
    )
    .bind(CHAIN)
    .bind(ORPHANED_HASH)
    .execute(scratch.pool())
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 6).await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 6,
            hash: block_hash(CHAIN, 6),
        }),
        RunMode::Normal,
        7,
        7,
    )
    .await?;

    let target: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xremote-parent'
           AND child_logical_name_id = 'ens:0xremote-child'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        target, 6,
        "current-window orphaned topology changed an unrelated projection row"
    );

    scratch.cleanup().await
}

fn expected_deep_v1_topology_edges() -> Vec<(String, String)> {
    (0..4)
        .map(|index| {
            let parent = if index == 0 {
                "ens:0xeth".to_owned()
            } else {
                format!("ens:0xn{index}")
            };
            (parent, format!("ens:0xn{}", index + 1))
        })
        .collect()
}

async fn deep_v1_topology_edges(pool: &PgPool) -> Result<Vec<(String, String)>> {
    Ok(sqlx::query_as(
        "SELECT parent_logical_name_id, child_logical_name_id
         FROM children_current
         WHERE parent_logical_name_id IN (
             'ens:0xeth', 'ens:0xn1', 'ens:0xn2', 'ens:0xn3'
         )
           AND child_logical_name_id IN (
             'ens:0xn1', 'ens:0xn2', 'ens:0xn3', 'ens:0xn4'
         )
         ORDER BY parent_logical_name_id, child_logical_name_id",
    )
    .fetch_all(pool)
    .await?)
}

async fn seed_deep_v1_topology_fixture(pool: &PgPool) -> Result<()> {
    for block in 4..=7 {
        insert_lineage_block(pool, CHAIN, block).await?;
    }
    for index in 1..=4 {
        let node = format!("0xn{index}");
        let logical_name_id = format!("ens:{node}");
        let raw_name = format!("n{index}.eth");
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 $1, 'ens', $2, ARRAY[$2], decode('00', 'hex'), $3,
                 ARRAY[$3], $4, 'active', $5, $6, 1, 'canonical'
             )",
        )
        .bind(logical_name_id)
        .bind(raw_name)
        .bind(node)
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }
    for index in 1..=4 {
        let labelhash = format!("0xn{index}-label");
        let label = format!("n{index}");
        sqlx::query(
            "INSERT INTO label_preimages (
                 labelhash, raw_label, decoded_label, normalizer_version,
                 normalized_under_version, source_kind, source_priority
             ) VALUES ($1, convert_to($2, 'UTF8'), $2, $3, true, 'fixture', 1)",
        )
        .bind(labelhash)
        .bind(label)
        .bind(NORMALIZER)
        .execute(pool)
        .await?;
    }
    for index in 0..4 {
        let parent = if index == 0 {
            "0xeth".to_owned()
        } else {
            format!("0xn{index}")
        };
        let child = format!("0xn{}", index + 1);
        insert_event(
            pool,
            CHAIN,
            index + 3,
            Some(&format!("ens:{parent}")),
            None,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            json!({
                "node":parent,
                "child_node":child,
                "labelhash":format!("0xn{}-label", index + 1),
                "owner":OWNER
            }),
            json!({}),
        )
        .await?;
    }
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xeth"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xn1",
            "labelhash":"0xn1-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    Ok(())
}

async fn seed_candidate_topology_bridge(pool: &PgPool) -> Result<()> {
    seed_remote_topology_bridge(pool).await?;
    sqlx::query(
        "UPDATE normalized_events
         SET consumer_visibility = 'candidate',
             migration_correlation_ids = ARRAY['candidate-topology']::text[]
         WHERE chain_id = $1
           AND block_number = 2
           AND event_kind = 'SubregistryChanged'
           AND after_state ->> 'node' = '0xn1'",
    )
    .bind(CHAIN)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_orphaned_topology_bridge(pool: &PgPool) -> Result<()> {
    const ORPHANED_HASH: &str = "project-fixture-orphaned-block-2";
    seed_remote_topology_bridge(pool).await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 2, to_timestamp(2), 'orphaned')",
    )
    .bind(CHAIN)
    .bind(ORPHANED_HASH)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET block_hash = $2
         WHERE chain_id = $1
           AND block_number = 2
           AND event_kind = 'SubregistryChanged'
           AND after_state ->> 'node' = '0xn1'",
    )
    .bind(CHAIN)
    .bind(ORPHANED_HASH)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_remote_topology_bridge(pool: &PgPool) -> Result<()> {
    for (logical_name_id, raw_name, namehash) in [
        ("ens:0xremote-parent", "remote.eth", "0xremote-parent"),
        ("ens:0xremote-child", "child.remote.eth", "0xremote-child"),
    ] {
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES (
                 $1, 'ens', $2, ARRAY[$2], decode('00', 'hex'), $3,
                 ARRAY[$3], $4, 'active', $5, $6, 1, 'canonical'
             )",
        )
        .bind(logical_name_id)
        .bind(raw_name)
        .bind(namehash)
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }
    for (labelhash, label) in [
        ("0xremote-parent-label", "remote"),
        ("0xremote-child-label", "child"),
    ] {
        sqlx::query(
            "INSERT INTO label_preimages (
                 labelhash, raw_label, decoded_label, normalizer_version,
                 normalized_under_version, source_kind, source_priority
             ) VALUES ($1, convert_to($2, 'UTF8'), $2, $3, true, 'fixture', 1)",
        )
        .bind(labelhash)
        .bind(label)
        .bind(NORMALIZER)
        .execute(pool)
        .await?;
    }
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xremote-parent"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xremote-parent",
            "child_node":"0xremote-child",
            "labelhash":"0xremote-child-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xn1"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xn1",
            "child_node":"0xremote-parent",
            "labelhash":"0xremote-parent-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn incremental_v2_subregistry_flip_replaces_children_in_both_directions() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_v2_subregistry_flip").await?;
    let full = ScratchDatabase::create("production_project_v2_subregistry_flip_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        seed_v2_subregistry_flip_fixture(pool).await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    assert_eq!(
        v2_flip_children(incremental.pool()).await?,
        vec!["ens:0xflip-c0"],
        "the initial S1 pointer must select c0"
    );

    for (target, expected) in [
        (4, vec!["ens:0xflip-c1", "ens:0xflip-shared"]),
        (5, vec!["ens:0xflip-c0"]),
        (6, vec!["ens:0xflip-c0"]),
    ] {
        run_project(
            incremental.pool(),
            CHAIN,
            Some(Marker {
                number: target - 1,
                hash: block_hash(CHAIN, target - 1),
            }),
            RunMode::Normal,
            target,
            target,
        )
        .await?;
        run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, target).await?;

        let incremental_children = v2_flip_children(incremental.pool()).await?;
        let full_children = v2_flip_children(full.pool()).await?;
        assert_eq!(
            full_children, expected,
            "unexpected full rebuild at {target}"
        );
        assert_eq!(
            incremental_children, full_children,
            "incremental child edges diverged after the subregistry tick at {target}"
        );
    }

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn direct_v2_unregister_suppresses_the_selected_registry_child() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_v2_direct_unregister").await?;
    let full = ScratchDatabase::create("production_project_v2_direct_unregister_full").await?;
    let release_after_state = interpreted_direct_v2_release_after_state()?;
    assert_eq!(
        release_after_state
            .get("registry_contract_instance_id")
            .and_then(Value::as_str),
        Some("00000000-0000-0000-0000-000000000f01"),
        "direct unregister must retain its emitting registry identity"
    );
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        seed_v2_subregistry_flip_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            3,
            Some("ens:0xflip-c0"),
            None,
            "RegistrationReleased",
            "ens_v2_registry_l1",
            release_after_state.clone(),
            json!({}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 2).await?;
    assert!(
        v2_flip_children(incremental.pool())
            .await?
            .contains(&"ens:0xflip-c0".to_owned()),
        "the directly registered child must exist before unregister"
    );
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 2,
            hash: block_hash(CHAIN, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    assert_eq!(
        v2_flip_children(incremental.pool()).await?,
        Vec::<String>::new(),
        "incremental Project retained a directly unregistered ENSv2 child"
    );
    assert_eq!(
        v2_flip_children(full.pool()).await?,
        Vec::<String>::new(),
        "a full rebuild retained a directly unregistered ENSv2 child"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

fn interpreted_direct_v2_release_after_state() -> Result<Value> {
    const ADAPTER_CHAIN: &str = "ethereum-sepolia";
    const REGISTRY_ADDRESS: &str = "0x00000000000000000000000000000000000020aa";
    const REGISTRY_INSTANCE: &str = "00000000-0000-0000-0000-000000000f01";
    let repository = load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia"),
    )?;
    let loaded = repository
        .manifests()
        .iter()
        .find(|loaded| {
            loaded.manifest.chain == ADAPTER_CHAIN
                && loaded.manifest.source_family == "ens_v2_registry_l1"
                && loaded.version_tag == "v2"
        })
        .expect("the checked-in post-audit ENSv2 registry manifest must exist");
    let mut payload = serde_json::to_value(&loaded.manifest)?;
    payload["manifest_version"] = Value::from(1);
    let manifest = ManifestInput {
        manifest_id: 1,
        manifest_version: 1,
        namespace: loaded.manifest.namespace.clone(),
        source_family: loaded.manifest.source_family.clone(),
        chain_id: loaded.manifest.chain.clone(),
        deployment_label: loaded.manifest.deployment_epoch.clone(),
        normalizer_version: loaded.manifest.normalizer_version.clone(),
        payload_json: serde_json::to_string(&payload)?,
    };
    let discovery_rules = loaded
        .manifest
        .discovery_rules
        .iter()
        .map(|rule| DiscoveryRuleInput {
            manifest_id: 1,
            edge_kind: rule.edge_kind.clone(),
            from_role: Some(rule.from_role.clone()),
            admission: rule.admission.clone(),
        })
        .collect();
    let token_id = U256::from(101_u64);
    let owner = OWNER.parse::<Address>()?;
    let sender = TRANSFER_OWNER.parse::<Address>()?;
    let label = "direct".to_owned();
    let label_hash = keccak256(label.as_bytes());
    let logs = [
        LabelRegistered {
            tokenId: token_id,
            labelHash: label_hash,
            label,
            owner,
            expiry: 1_800_000_000,
            sender,
        }
        .encode_log_data(),
        TokenResource {
            tokenId: token_id,
            resource: U256::from(5_001_u64),
        }
        .encode_log_data(),
        LabelUnregistered {
            tokenId: token_id,
            sender,
        }
        .encode_log_data(),
    ];
    let raw_logs = logs
        .into_iter()
        .enumerate()
        .map(|(log_index, log)| AdapterRawLogInput {
            chain_id: ADAPTER_CHAIN.into(),
            block_hash: "adapter-direct-unregister-block".into(),
            block_number: 1,
            block_timestamp: time::OffsetDateTime::from_unix_timestamp(1_700_000_000)
                .expect("fixture timestamp must be valid"),
            canonicality_state: "canonical".into(),
            transaction_hash: "adapter-direct-unregister-transaction".into(),
            transaction_index: 0,
            log_index: i64::try_from(log_index).expect("fixture log index fits i64"),
            emitting_address: REGISTRY_ADDRESS.into(),
            topics: log.topics().iter().map(ToString::to_string).collect(),
            data: log.data.to_vec(),
        })
        .collect();
    let output = interpret_schema_v2_batch(AdapterBatchInput {
        chain_id: ADAPTER_CHAIN.into(),
        manifests: vec![manifest],
        discovery_rules,
        admissions: vec![AddressAdmissionInput {
            address: REGISTRY_ADDRESS.into(),
            contract_instance_id: Uuid::parse_str(REGISTRY_INSTANCE)?,
            source_manifest_id: Some(1),
            role: Some("registry".into()),
            discovery_edge_kind: None,
            discovery_from_contract_instance_id: None,
            discovery_observation_key: None,
            active_from_block: Some(0),
            active_to_block: None,
        }],
        prior_events: Vec::new(),
        blocks: vec![AdapterRawBlockInput {
            chain_id: ADAPTER_CHAIN.into(),
            block_hash: "adapter-direct-unregister-block".into(),
            block_number: 1,
            block_timestamp: time::OffsetDateTime::from_unix_timestamp(1_700_000_000)
                .expect("fixture timestamp must be valid"),
            canonicality_state: "canonical".into(),
        }],
        raw_logs,
    })?;
    Ok(output
        .normalized_events
        .into_iter()
        .find(|event| event.event_kind == "RegistrationReleased")
        .expect("LabelUnregistered must derive RegistrationReleased")
        .after_state)
}

async fn v2_flip_children(pool: &PgPool) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT child_logical_name_id
         FROM children_current
         WHERE parent_logical_name_id = 'ens:0xflip-parent'
         ORDER BY child_logical_name_id",
    )
    .fetch_all(pool)
    .await?)
}

async fn seed_v2_subregistry_flip_fixture(pool: &PgPool) -> Result<()> {
    const S1_ADDRESS: &str = "0x0000000000000000000000000000000000000f01";
    const S2_ADDRESS: &str = "0x0000000000000000000000000000000000000f02";
    let s1 = Uuid::parse_str("00000000-0000-0000-0000-000000000f01")?;
    let s2 = Uuid::parse_str("00000000-0000-0000-0000-000000000f02")?;

    for block in 4..=6 {
        insert_lineage_block(pool, CHAIN, block).await?;
    }
    for (instance, address) in [(s1, S1_ADDRESS), (s2, S2_ADDRESS)] {
        sqlx::query(
            "INSERT INTO contract_instances (
                 contract_instance_id, chain_id, contract_kind
             ) VALUES ($1, $2, 'contract')",
        )
        .bind(instance)
        .bind(CHAIN)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO contract_instance_addresses (
                 contract_instance_id, chain_id, address, active_from_block_number
             ) VALUES ($1, $2, $3, 0)",
        )
        .bind(instance)
        .bind(CHAIN)
        .bind(address)
        .execute(pool)
        .await?;
    }
    for (logical_name_id, raw_name, raw_labels, namehash, labelhashes) in [
        (
            "ens:0xflip-parent",
            "flip.eth",
            vec!["flip", "eth"],
            "0xflip-parent",
            vec!["0xflip-parent-label", "0xeth"],
        ),
        (
            "ens:0xflip-c0",
            "c0.flip.eth",
            vec!["c0", "flip", "eth"],
            "0xflip-c0",
            vec!["0xflip-c0-label", "0xflip-parent-label", "0xeth"],
        ),
        (
            "ens:0xflip-c1",
            "c1.flip.eth",
            vec!["c1", "flip", "eth"],
            "0xflip-c1",
            vec!["0xflip-c1-label", "0xflip-parent-label", "0xeth"],
        ),
        (
            "ens:0xflip-shared",
            "shared.flip.eth",
            vec!["shared", "flip", "eth"],
            "0xflip-shared",
            vec!["0xflip-shared-label", "0xflip-parent-label", "0xeth"],
        ),
    ] {
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES ($1, 'ens', $2, $3, decode('00', 'hex'), $4, $5, $6,
                       'active', $7, $8, 1, 'canonical')",
        )
        .bind(logical_name_id)
        .bind(raw_name)
        .bind(raw_labels)
        .bind(namehash)
        .bind(labelhashes)
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES
             ('0xflip-c0-label', convert_to('c0', 'UTF8'), 'c0', $1, true, 'fixture', 1),
             ('0xflip-c1-label', convert_to('c1', 'UTF8'), 'c1', $1, true, 'fixture', 1),
             ('0xflip-shared-label', convert_to('shared', 'UTF8'), 'shared', $1,
              true, 'fixture', 1)",
    )
    .bind(NORMALIZER)
    .execute(pool)
    .await?;

    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xflip-parent"),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":S1_ADDRESS}),
        json!({}),
    )
    .await?;
    for (block, logical_name_id, instance) in [
        (1, "ens:0xflip-c0", s1),
        (2, "ens:0xflip-c1", s2),
        (1, "ens:0xflip-shared", s2),
        (2, "ens:0xflip-shared", s1),
    ] {
        insert_event(
            pool,
            CHAIN,
            block,
            Some(logical_name_id),
            None,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({
                "registry_contract_instance_id":instance,
                "registrant":OWNER
            }),
            json!({}),
        )
        .await?;
    }
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xflip-shared"),
        None,
        "RegistrationReleased",
        "ens_v2_registry_l1",
        json!({
            "registry_contract_instance_id":s1,
            "registrant":OWNER
        }),
        json!({}),
    )
    .await?;
    for (block, before, after) in [(4, S1_ADDRESS, S2_ADDRESS), (5, S2_ADDRESS, S1_ADDRESS)] {
        insert_event(
            pool,
            CHAIN,
            block,
            Some("ens:0xflip-parent"),
            None,
            "SubregistryChanged",
            "ens_v2_registry_l1",
            json!({"subregistry":after}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET before_state = jsonb_build_object('subregistry', lower($1))
             WHERE chain_id = $2 AND block_number = $3
               AND logical_name_id = 'ens:0xflip-parent'
               AND event_kind = 'SubregistryChanged'",
        )
        .bind(before)
        .bind(CHAIN)
        .bind(block)
        .execute(pool)
        .await?;
    }
    Ok(())
}

fn expected_wrapper_fuses(fuses: u32) -> Value {
    json!({
        "fuses": fuses,
        "cannot_unwrap": fuses & 1 != 0,
        "cannot_burn_fuses": fuses & 2 != 0,
        "cannot_transfer": fuses & 4 != 0,
        "cannot_set_resolver": fuses & 8 != 0,
        "cannot_set_ttl": fuses & 16 != 0,
        "cannot_create_subdomain": fuses & 32 != 0,
        "cannot_approve": fuses & 64 != 0,
        "parent_cannot_control": fuses & 65_536 != 0,
        "is_dot_eth": fuses & 131_072 != 0,
        "can_extend_expiry": fuses & 262_144 != 0,
    })
}

#[tokio::test]
async fn wrapper_states_and_expiry_gate_permissions_and_controller_relations() -> Result<()> {
    let cases = [
        (
            "wrapped_expired",
            0,
            "wrapped",
            2,
            Some("wrapped"),
            9,
            true,
            true,
        ),
        (
            "wrapped_expired_parent_fuse",
            262_144,
            "wrapped",
            2,
            Some("wrapped"),
            9,
            true,
            true,
        ),
        (
            "emancipated",
            65_536,
            "emancipated",
            3,
            Some("emancipated"),
            9,
            true,
            true,
        ),
        (
            "dot_eth_grace_boundary",
            196_608,
            "emancipated",
            7_776_003,
            Some("emancipated"),
            9,
            true,
            true,
        ),
        (
            "dot_eth_grace",
            196_608,
            "emancipated",
            7_776_002,
            Some("emancipated"),
            1,
            false,
            true,
        ),
        (
            "locked",
            65_537,
            "locked",
            3,
            Some("locked"),
            7,
            false,
            true,
        ),
        (
            "locked_max_expiry",
            65_537,
            "locked",
            u64::MAX,
            Some("locked"),
            7,
            false,
            true,
        ),
        (
            "cannot_burn_fuses",
            65_539,
            "locked",
            3,
            Some("locked"),
            6,
            false,
            true,
        ),
        (
            "cannot_transfer",
            65_541,
            "locked",
            3,
            Some("locked"),
            6,
            false,
            true,
        ),
        (
            "cannot_set_resolver",
            65_545,
            "locked",
            3,
            Some("locked"),
            5,
            false,
            true,
        ),
        (
            "cannot_set_ttl",
            65_553,
            "locked",
            3,
            Some("locked"),
            6,
            false,
            true,
        ),
        (
            "cannot_create_subdomain",
            65_569,
            "locked",
            3,
            Some("locked"),
            6,
            false,
            true,
        ),
        (
            "cannot_approve",
            65_601,
            "locked",
            3,
            Some("locked"),
            6,
            false,
            true,
        ),
        (
            "can_extend_expiry",
            327_681,
            "locked",
            3,
            Some("locked"),
            7,
            false,
            true,
        ),
        ("locked_expired", 65_537, "locked", 2, None, 0, false, false),
    ];

    for (
        label,
        fuses,
        state,
        expiry,
        expected_state,
        power_count,
        has_controller,
        has_token_holder,
    ) in cases
    {
        let scratch = ScratchDatabase::create(&format!("production_project_fuse_{label}")).await?;
        seed_project_fixture(scratch.pool()).await?;
        insert_event(
            scratch.pool(),
            CHAIN,
            2,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "PermissionChanged",
            "ens_v1_wrapper_l1",
            json!({
                "subject":OWNER,
                "scope":{"kind":"resource"},
                "effective_powers":[
                    "resource_control", "resolver_control", "set_resolver", "set_ttl",
                    "create_subnames", "transfer", "unwrap", "burn_fuses", "approve"
                ],
                "grant_source":{"kind":"fixture"},
                "revocation_source":null,
                "inheritance_path":[],
                "transfer_behavior":"replace_on_authority_change"
            }),
            json!({}),
        )
        .await?;
        insert_event(
            scratch.pool(),
            CHAIN,
            3,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "ExpiryChanged",
            "ens_v1_wrapper_l1",
            json!({"expiry":expiry}),
            json!({}),
        )
        .await?;
        insert_event(
            scratch.pool(),
            CHAIN,
            3,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "PermissionScopeChanged",
            "ens_v1_wrapper_l1",
            json!({"fuses":fuses,"wrapper_state":state}),
            json!({}),
        )
        .await?;

        run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
        let projected_state: Option<String> = sqlx::query_scalar(
            "SELECT declared_summary ->> 'wrapper_state' FROM name_current
             WHERE logical_name_id = 'ens:0xalice'",
        )
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(projected_state.as_deref(), expected_state, "{label}");
        let projected_fuses: Option<Value> = sqlx::query_scalar(
            "SELECT declared_summary -> 'wrapper_fuses' FROM name_current
             WHERE logical_name_id = 'ens:0xalice'",
        )
        .fetch_one(scratch.pool())
        .await?;
        let expected_effective_fuses = expected_state.map(|_| {
            expected_wrapper_fuses(if expiry < 3 {
                0
            } else {
                u32::try_from(fuses).expect("fixture fuses fit uint32")
            })
        });
        assert_eq!(projected_fuses, expected_effective_fuses, "{label}");
        let effective_powers: Option<Value> = sqlx::query_scalar(
            "SELECT effective_powers FROM permissions_current WHERE resource_id = $1",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .fetch_optional(scratch.pool())
        .await?;
        assert_eq!(
            effective_powers
                .as_ref()
                .and_then(Value::as_array)
                .map_or(0, Vec::len),
            power_count,
            "{label}"
        );
        let stored_permissions = bigname_storage::load_permissions_current(
            scratch.pool(),
            Uuid::parse_str(RESOURCE)?,
            None,
            None,
        )
        .await?;
        assert_eq!(
            stored_permissions
                .first()
                .map(|permission| permission.effective_powers.clone()),
            effective_powers,
            "Project-built permission rows must pass phase canonicality admission for {label}"
        );
        assert!(
            bigname_storage::load_permissions_current_resource_summary(
                scratch.pool(),
                Uuid::parse_str(RESOURCE)?,
            )
            .await?
            .is_some(),
            "Project-built permission summaries must pass phase canonicality admission for {label}"
        );
        if label == "dot_eth_grace" {
            assert_eq!(effective_powers, Some(json!(["approve"])));
        }
        let controller_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM address_names_current
                 WHERE logical_name_id = 'ens:0xalice'
                   AND relation = 'effective_controller'
             )",
        )
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(controller_exists, has_controller, "{label}");
        let token_holder_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM address_names_current
                 WHERE logical_name_id = 'ens:0xalice'
                   AND relation = 'token_holder'
             )",
        )
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(token_holder_exists, has_token_holder, "{label}");
        scratch.cleanup().await?;
    }
    Ok(())
}

#[tokio::test]
async fn incremental_project_revisits_wrapper_timestamp_boundaries() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_wrapper_time_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 5, to_timestamp(7776004), 'canonical')",
    )
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 5))
    .bind(block_hash(CHAIN, 4))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_wrapper_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":[
                "resource_control", "resolver_control", "set_resolver", "set_ttl",
                "create_subnames", "transfer", "unwrap", "burn_fuses", "approve"
            ],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"source_event":"NameRenewed","authority_kind":"wrapper","expiry":7_776_003}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196_608,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let grace_powers: Value = sqlx::query_scalar(
        "SELECT effective_powers FROM permissions_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(grace_powers, json!(["approve"]));
    let grace_controller: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM address_names_current
             WHERE logical_name_id = 'ens:0xalice'
               AND relation = 'effective_controller'
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(!grace_controller);

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    let expired_state: Option<String> = sqlx::query_scalar(
        "SELECT declared_summary ->> 'wrapper_state' FROM name_current
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(expired_state, None);
    let expired_relations: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM address_names_current
         WHERE logical_name_id = 'ens:0xalice'
           AND relation IN ('effective_controller', 'token_holder')",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(expired_relations, 0);
    let expired_permissions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM permissions_current WHERE resource_id = $1")
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(expired_permissions, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn event_free_wrapper_boundary_refreshes_resolver_permission_summary() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_wrapper_resolver_summary").await?;
    let full = ScratchDatabase::create("production_project_wrapper_resolver_summary_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        extend_wrapper_resolver_summary_fixture(pool).await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    let event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_number = 5",
    )
    .bind(CHAIN)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(event_count, 0, "the expiry transition must be event-free");

    let pre_boundary_powers: Value = sqlx::query_scalar(
        "SELECT effective_powers
         FROM permissions_current
         WHERE resource_id = $1
           AND scope_kind = 'resolver'
           AND lower(scope_detail ->> 'resolver_address') = lower($2)",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(pre_boundary_powers, json!(["approve"]));
    let record_uses_resolver: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM record_inventory_current
             WHERE resource_id = $1
               AND lower(provenance ->> 'resolver_address') = lower($2)
         )",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    assert!(
        record_uses_resolver,
        "the v2 resolver must also serve records"
    );

    sqlx::query(
        "UPDATE resolver_current
         SET declared_summary = jsonb_set(
             declared_summary, '{passthrough_guard}', 'true'::jsonb
         )
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(incremental.pool())
    .await?;

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;

    for pool in [incremental.pool(), full.pool()] {
        let resolver_permission_count: i64 = sqlx::query_scalar(
            "SELECT count(*)
             FROM permissions_current
             WHERE resource_id = $1
               AND scope_kind = 'resolver'
               AND lower(scope_detail ->> 'resolver_address') = lower($2)",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(EQUIVALENCE_V2_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(resolver_permission_count, 0);
    }

    let incremental_counts =
        resolver_summary_counts(incremental.pool(), CHAIN, EQUIVALENCE_V2_RESOLVER).await?;
    let full_counts = resolver_summary_counts(full.pool(), CHAIN, EQUIVALENCE_V2_RESOLVER).await?;
    assert_eq!(full_counts, (0, 0, None));
    assert_eq!(
        incremental_counts, full_counts,
        "an event-free wrapper boundary left a stale resolver permission summary"
    );

    let incremental_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    let full_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(full.pool())
    .await?;
    assert_eq!(
        incremental_summary, full_summary,
        "event-free binding, alias, permission, role, and event summaries diverged"
    );

    let unscoped_permission_resolver_kept_passthrough: bool = sqlx::query_scalar(
        "SELECT declared_summary @> '{\"passthrough_guard\":true}'::jsonb
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    assert!(
        unscoped_permission_resolver_kept_passthrough,
        "a boundary-scoped resource without resolver permissions lost passthrough"
    );
    let bob_target: i64 = sqlx::query_scalar(
        "SELECT (chain_positions ->> 'target_block_number')::bigint
         FROM record_inventory_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_BOB_RESOURCE)?)
    .fetch_one(incremental.pool())
    .await?;
    assert_eq!(
        bob_target, 5,
        "the passthrough guard resource must be scoped"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn event_free_wrapper_boundary_scopes_permission_named_resolver() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_permission_only_resolver_scope").await?;
    let full =
        ScratchDatabase::create("production_project_permission_only_resolver_scope_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        extend_permission_only_resolver_fixture(pool).await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    assert_eq!(
        resolver_summary_counts(incremental.pool(), CHAIN, PERMISSION_ONLY_RESOLVER).await?,
        (1, 1, Some("approve".into()))
    );
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 5).await?;

    for pool in [incremental.pool(), full.pool()] {
        let live_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM permissions_current
             WHERE resource_id = $1 AND scope_kind = 'resolver'
               AND lower(scope_detail ->> 'resolver_address') = lower($2)",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(PERMISSION_ONLY_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(live_rows, 0);
    }
    let incremental_counts =
        resolver_summary_counts(incremental.pool(), CHAIN, PERMISSION_ONLY_RESOLVER).await?;
    let full_counts = resolver_summary_counts(full.pool(), CHAIN, PERMISSION_ONLY_RESOLVER).await?;
    assert_eq!(full_counts, (0, 0, None));
    assert_eq!(
        incremental_counts, full_counts,
        "a resolver named only by scoped permission history was not rebuilt"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn wrapper_renewal_refreshes_resurrected_resolver_permission_summary() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_resolver_permission_resurrection").await?;
    let full =
        ScratchDatabase::create("production_project_resolver_permission_resurrection_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        extend_wrapper_resolver_summary_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            6,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "ExpiryChanged",
            "ens_v1_registrar_l1",
            json!({
                "source_event":"NameRenewed",
                "authority_kind":"wrapper",
                "expiry":9_000_000
            }),
            json!({}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    assert_eq!(
        resolver_summary_counts(incremental.pool(), CHAIN, EQUIVALENCE_V2_RESOLVER).await?,
        (0, 0, None),
        "the expiry boundary must clear the resolver summary before renewal"
    );

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 5,
            hash: block_hash(CHAIN, 5),
        }),
        RunMode::Normal,
        6,
        6,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 6).await?;
    for pool in [incremental.pool(), full.pool()] {
        let powers: Value = sqlx::query_scalar(
            "SELECT effective_powers FROM permissions_current
             WHERE resource_id = $1 AND scope_kind = 'resolver'
               AND lower(scope_detail ->> 'resolver_address') = lower($2)",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .bind(EQUIVALENCE_V2_RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(powers, json!(["approve"]));
    }
    let incremental_counts =
        resolver_summary_counts(incremental.pool(), CHAIN, EQUIVALENCE_V2_RESOLVER).await?;
    let full_counts = resolver_summary_counts(full.pool(), CHAIN, EQUIVALENCE_V2_RESOLVER).await?;
    assert_eq!(full_counts, (1, 1, Some("approve".into())));
    assert_eq!(
        incremental_counts, full_counts,
        "a renewed resolver permission row was absent from its resolver summary"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

async fn resolver_summary_counts(
    pool: &PgPool,
    chain: &str,
    resolver: &str,
) -> Result<(i64, i64, Option<String>)> {
    Ok(sqlx::query_as(
        "SELECT
             (declared_summary #>> '{permissions,count}')::bigint,
             (declared_summary #>> '{role_holders,count}')::bigint,
             declared_summary #>> '{role_holders,items,0,effective_powers,0}'
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(chain)
    .bind(resolver)
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn incremental_resolver_builder_stages_only_the_scoped_discovered_resolver() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_resolver_candidate_scope").await?;
    seed_project_fixture(scratch.pool()).await?;

    let resolver_manifest: i64 = sqlx::query_scalar(
        "SELECT source_manifest_id
         FROM normalized_events
         WHERE chain_id = $1
           AND event_kind = 'SourceManifestUpdated'
           AND source_family = 'ens_v1_resolver_l1'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    let source_instance = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(source_instance)
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    for address in [
        "0x00000000000000000000000000000000000000B1",
        "0x00000000000000000000000000000000000000C2",
    ] {
        let resolver_instance = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO contract_instances (
                 contract_instance_id, chain_id, contract_kind
             ) VALUES ($1, $2, 'contract')",
        )
        .bind(resolver_instance)
        .bind(CHAIN)
        .execute(scratch.pool())
        .await?;
        sqlx::query(
            "INSERT INTO contract_instance_addresses (
                 contract_instance_id, chain_id, address,
                 active_from_block_number, active_from_block_hash,
                 source_manifest_id
             ) VALUES ($1, $2, $3, 1, $4, $5)",
        )
        .bind(resolver_instance)
        .bind(CHAIN)
        .bind(address)
        .bind(block_hash(CHAIN, 1))
        .bind(resolver_manifest)
        .execute(scratch.pool())
        .await?;
        sqlx::query(
            "INSERT INTO discovery_edges (
                 chain_id, edge_kind, from_contract_instance_id,
                 to_contract_instance_id, discovery_source, admission_basis,
                 source_manifest_id, active_from_block_number,
                 active_from_block_hash, canonicality_state
             ) VALUES (
                 $1, 'resolver', $2, $3, 'fixture', 'reachable_from_root',
                 $4, 1, $5, 'canonical'
             )",
        )
        .bind(CHAIN)
        .bind(source_instance)
        .bind(resolver_instance)
        .bind(resolver_manifest)
        .bind(block_hash(CHAIN, 1))
        .execute(scratch.pool())
        .await?;
    }

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        None,
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({"selector":"text:url","value":"https://example.test"}),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    sqlx::query("CREATE SEQUENCE resolver_stage_candidate_count")
        .execute(scratch.pool())
        .await?;
    sqlx::query(
        "ALTER TABLE resolver_current
         ALTER COLUMN last_recomputed_at SET DEFAULT
             to_timestamp(nextval('resolver_stage_candidate_count')::double precision)",
    )
    .execute(scratch.pool())
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;

    let staged_candidates: (i64, bool) =
        sqlx::query_as("SELECT last_value, is_called FROM resolver_stage_candidate_count")
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(staged_candidates, (1, true));
    let scoped_resolver: (String, String) = sqlx::query_as(
        "SELECT support_status,
                declared_summary -> 'classification' ->> 'source_family'
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        scoped_resolver,
        ("supported".into(), "ens_v1_resolver_l1".into())
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn unwrapped_registration_ignores_the_prior_wrapper_grace_expiry() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_unwrapped_registrar_expiry").await?;
    seed_project_fixture(scratch.pool()).await?;
    let wrapper_resource = Uuid::new_v4();
    let wrapper_resource_text = wrapper_resource.to_string();
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 2, 'canonical')",
    )
    .bind(wrapper_resource)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 2))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"expiry":1_000}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(&wrapper_resource_text),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({
            "source_event":"NameRenewed",
            "authority_kind":"wrapper",
            "registrar_expiry":1_000,
            "expiry":7_777_000
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let projected_expiry: i64 = sqlx::query_scalar(
        "SELECT (declared_summary -> 'registration' ->> 'expiry')::BIGINT
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(projected_expiry, 1_000);
    scratch.cleanup().await
}

#[tokio::test]
async fn wrapped_registration_separates_registrar_and_wrapper_expiry() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_wrapped_expiry_split").await?;
    seed_project_fixture(scratch.pool()).await?;
    let wrapper_resource = Uuid::new_v4();
    let wrapper_resource_text = wrapper_resource.to_string();
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 2, 'canonical')",
    )
    .bind(wrapper_resource)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 2))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE surface_bindings SET resource_id = $1
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .bind(wrapper_resource)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(&wrapper_resource_text),
        "ExpiryChanged",
        "ens_v1_wrapper_l1",
        json!({"authority_kind":"wrapper","expiry":500}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(&wrapper_resource_text),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196_609,"wrapper_state":"locked"}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"source_event":"NameRenewed","authority_kind":"registrar","expiry":1_000}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM name_current
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(summary["registration"]["expiry"], 1_000);
    assert_eq!(summary["wrapper_state"], "locked");
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_without_resume_revisits_wrapper_timestamp_boundaries() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_wrapper_redo_time_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_wrapper_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":[
                "resource_control", "resolver_control", "set_resolver", "set_ttl",
                "create_subnames", "transfer", "unwrap", "burn_fuses", "approve"
            ],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_wrapper_l1",
        json!({"expiry":7_776_003}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196_608,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Redo, 4, 4).await?;

    let powers: Value = sqlx::query_scalar(
        "SELECT effective_powers FROM permissions_current WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(powers, json!(["approve"]));
    let controller: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM address_names_current
             WHERE logical_name_id = 'ens:0xalice'
               AND relation = 'effective_controller'
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(!controller);
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_removes_a_retracted_wrapper_expiry_boundary() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_wrapper_expiry_retraction").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        None,
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_wrapper_l1",
        json!({"expiry":7_776_003}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        None,
        Some(RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196_608,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        None,
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"later_authority"},
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT provenance ? 'wrapper_expiry_boundary'
             FROM permissions_current_resource_summary WHERE resource_id = $1",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .fetch_one(scratch.pool())
        .await?
    );

    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND block_number = 3
           AND event_kind IN ('ExpiryChanged', 'PermissionScopeChanged')",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Redo,
        3,
        3,
    )
    .await?;

    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT provenance ? 'wrapper_expiry_boundary'
             FROM permissions_current_resource_summary WHERE resource_id = $1",
        )
        .bind(Uuid::parse_str(RESOURCE)?)
        .fetch_one(scratch.pool())
        .await?
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn address_relations_fold_token_and_authority_transfers_in_order() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_address_transfer").await?;
    seed_project_fixture(scratch.pool()).await?;
    let holder = "0x00000000000000000000000000000000000000a2";
    let controller = "0x00000000000000000000000000000000000000a3";
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "TokenControlTransferred",
        "ens_v1_registrar_l1",
        json!({"from":OWNER,"to":holder}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        json!({"owner":controller}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let relations: Vec<(String, String)> = sqlx::query_as(
        "SELECT relation, address FROM address_names_current
         WHERE logical_name_id = 'ens:0xalice'
         ORDER BY relation",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        relations,
        vec![
            ("effective_controller".into(), controller.into()),
            ("registrant".into(), holder.into()),
            ("token_holder".into(), holder.into()),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn resource_control_revocation_clears_a_non_token_controller() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_address_revocation").await?;
    seed_project_fixture(scratch.pool()).await?;
    sqlx::query("UPDATE resources SET token_lineage_id = NULL WHERE resource_id = $1")
        .bind(Uuid::parse_str(RESOURCE)?)
        .execute(scratch.pool())
        .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registry_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":[],
            "revocation_source":{"kind":"fixture"},
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let relation_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM address_names_current
         WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(relation_count, 0);
    scratch.cleanup().await
}

#[tokio::test]
async fn record_inventory_restores_same_resolver_records_across_resources() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_cross_resource_records").await?;
    seed_project_fixture(scratch.pool()).await?;
    let predecessor = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(predecessor)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        1,
        Some("ens:0xalice"),
        Some(&predecessor.to_string()),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:email",
            "record_family":"text",
            "selector_key":"email",
            "value_retained":true,
            "value":"alice@example.test"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let entries: Value =
        sqlx::query_scalar("SELECT entries FROM record_inventory_current WHERE resource_id = $1")
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(scratch.pool())
            .await?;
    let keys = entries
        .as_array()
        .expect("record entries are an array")
        .iter()
        .map(|entry| entry["record_key"].as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    assert_eq!(keys, vec!["text:email", "text:url"]);
    scratch.cleanup().await
}

#[tokio::test]
async fn record_inventory_chain_position_keeps_block_number_and_hash_paired() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_record_position_pair").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let position: (i64, String) = sqlx::query_as(
        "SELECT (chain_positions ->> 'block_number')::bigint,
                chain_positions ->> 'block_hash'
         FROM record_inventory_current
         WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(position, (3, block_hash(CHAIN, 3)));
    scratch.cleanup().await
}

#[tokio::test]
async fn phase_runner_redo_repairs_only_the_range_and_retracts_orphaned_output() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_range_redo").await?;
    seed_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "UPDATE name_current SET raw_name = CASE logical_name_id
             WHEN 'ens:0xalice' THEN 'tampered-alice.eth'
             ELSE 'tampered-eth' END",
    )
    .execute(scratch.pool())
    .await?;

    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(CHAIN).await?;
    seed_completed_project_extent(scratch.pool(), CHAIN, 3).await?;
    let phases = PhaseSet::with_ingest_interpret_and_project(
        Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
        Arc::new(LoopbackPhase::new(PhaseName::Interpret)),
        Arc::new(ProjectPhase::new(scratch.pool().clone())),
    )?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        phases,
        CapacityGuard::system(CapacityConfig::default()),
        "production-project-redo",
        test_timing(),
    )?;
    let chain = chain_config(CHAIN)?;
    runner
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(2, 2)?,
            CancellationToken::new(),
        )
        .await?;

    let names: Vec<(String, String)> = sqlx::query_as(
        "SELECT logical_name_id, raw_name FROM name_current ORDER BY logical_name_id",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        names,
        vec![
            ("ens:0xalice".into(), "alice.eth".into()),
            ("ens:0xeth".into(), "tampered-eth".into()),
        ]
    );

    sqlx::query(
        "UPDATE normalized_events
         SET canonicality_state = 'orphaned'
         WHERE chain_id = $1
           AND block_number = 3
           AND event_kind IN ('ReverseChanged', 'RecordChanged')",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    runner
        .redo(
            &chain,
            RedoPhase::Phase(PhaseName::Project),
            BlockRange::new(3, 3)?,
            CancellationToken::new(),
        )
        .await?;
    let primary_count: i64 = sqlx::query_scalar("SELECT count(*) FROM primary_names_current")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(primary_count, 0);
    let state: (String, bool, Option<String>) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress, input_content_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        state,
        (
            "completed".into(),
            false,
            Some(INTERPRETER_CONTENT_HASH.into())
        )
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn incremental_project_does_not_rebuild_unaffected_chain_history() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_incremental_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    let unrelated_lineage = Uuid::new_v4();
    let unrelated_resource = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO token_lineages (
             token_lineage_id, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(unrelated_lineage)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, token_lineage_id, chain_id, block_hash, block_number,
             provenance, canonicality_state
         ) VALUES ($1, $2, $3, $4, 1,
                   '{\"manifest_version\":\"unrelated-history-sentinel\"}',
                   'canonical')",
    )
    .bind(unrelated_resource)
    .bind(unrelated_lineage)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    let successor = "0x00000000000000000000000000000000000000a9";
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":successor}),
        json!({}),
    )
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let owner: String = sqlx::query_scalar(
        "SELECT declared_summary -> 'control' ->> 'registry_owner'
         FROM name_current WHERE logical_name_id = 'ens:0xalice'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(owner, successor);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
             SELECT 1 FROM permissions_current_resource_summary
             WHERE resource_id = $1
         )",
        )
        .bind(unrelated_resource)
        .fetch_one(scratch.pool())
        .await?
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn non_resolver_emitters_do_not_create_resolver_rows() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_non_resolver_emitter").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"fixture"},
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    let projected: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM resolver_current
             WHERE chain_id = $1 AND resolver_address = lower($2)
         )",
    )
    .bind(CHAIN)
    .bind(REGISTRAR)
    .fetch_one(scratch.pool())
    .await?;
    assert!(!projected);
    scratch.cleanup().await
}

#[tokio::test]
async fn non_resolver_resource_delta_preserves_record_inventory_classification() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_resource_delta_classification").await?;
    let full =
        ScratchDatabase::create("production_project_resource_delta_classification_full").await?;
    seed_project_fixture(incremental.pool()).await?;
    seed_project_fixture(full.pool()).await?;
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    for pool in [incremental.pool(), full.pool()] {
        insert_lineage_block(pool, CHAIN, 4).await?;
        insert_event(
            pool,
            CHAIN,
            4,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "AuthorityTransferred",
            "ens_v1_registrar_l1",
            json!({"owner":"0x00000000000000000000000000000000000000a4"}),
            json!({}),
        )
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    let incremental_inventory: Value = sqlx::query_scalar(
        "SELECT to_jsonb(inventory) - 'last_recomputed_at' - 'inserted_at'
         FROM record_inventory_current inventory WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(incremental.pool())
    .await?;
    let full_inventory: Value = sqlx::query_scalar(
        "SELECT to_jsonb(inventory) - 'last_recomputed_at' - 'inserted_at'
         FROM record_inventory_current inventory WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(full.pool())
    .await?;

    assert_eq!(incremental_inventory, full_inventory);
    assert_eq!(incremental_inventory["support_status"], json!("supported"));
    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn project_redo_restores_the_surviving_resolver_pointer_like_a_full_rebuild() -> Result<()> {
    const LOSING_RESOLVER: &str = "0x00000000000000000000000000000000000000b9";

    let incremental = ScratchDatabase::create("production_project_resolver_pointer_redo").await?;
    let full = ScratchDatabase::create("production_project_resolver_pointer_redo_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        insert_lineage_block(pool, CHAIN, 4).await?;
    }
    insert_event(
        incremental.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":LOSING_RESOLVER}),
        json!({}),
    )
    .await?;
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND block_number = 4
           AND event_kind = 'ResolverChanged'",
    )
    .bind(CHAIN)
    .execute(incremental.pool())
    .await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 4,
            hash: block_hash(CHAIN, 4),
        }),
        RunMode::Redo,
        4,
        4,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(full.pool()).await?;
    assert_eq!(
        serving_table_snapshot(incremental.pool()).await?,
        serving_table_snapshot(full.pool()).await?,
        "redo of a removed resolver pointer diverged from the surviving full rebuild"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn candidate_resolver_pointer_cannot_change_resource_delta_scope() -> Result<()> {
    const CANDIDATE_RESOLVER: &str = "0x00000000000000000000000000000000000000b9";

    let scratch = ScratchDatabase::create("production_project_candidate_pointer").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":"0x00000000000000000000000000000000000000a4"}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":CANDIDATE_RESOLVER}),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET consumer_visibility = 'candidate',
             migration_correlation_ids = ARRAY['candidate-fixture']::text[]
         WHERE chain_id = $1 AND block_number = 4
           AND event_kind = 'ResolverChanged'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    sqlx::query(
        "INSERT INTO resolver_current (
             chain_id, resolver_address, declared_summary, support_status,
             unsupported_reason, provenance, chain_positions,
             canonicality_summary, manifest_version
         ) SELECT
             chain_id, lower($2), declared_summary || '{\"sentinel\":true}'::jsonb,
             support_status, unsupported_reason, provenance, chain_positions,
             canonicality_summary, manifest_version
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($3)",
    )
    .bind(CHAIN)
    .bind(CANDIDATE_RESOLVER)
    .bind(RESOLVER)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let sentinel: Option<Value> = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(CANDIDATE_RESOLVER)
    .fetch_optional(scratch.pool())
    .await?;
    assert_eq!(
        sentinel
            .as_ref()
            .and_then(|summary| summary.get("sentinel")),
        Some(&json!(true)),
        "candidate resolver evidence changed an unscoped resolver row"
    );

    scratch.cleanup().await
}

#[tokio::test]
async fn surface_unbind_rebuilds_the_pointer_resolver_binding_summary() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_surface_unbind_scope").await?;
    let full = ScratchDatabase::create("production_project_surface_unbind_scope_full").await?;

    for pool in [incremental.pool(), full.pool()] {
        seed_basenames_project_fixture(pool).await?;
    }
    run_project(incremental.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 2).await?;

    for pool in [incremental.pool(), full.pool()] {
        sqlx::query(
            "UPDATE surface_bindings SET active_to = to_timestamp(3)
             WHERE logical_name_id = 'basenames:0xalice-base'",
        )
        .execute(pool)
        .await?;
        insert_namespaced_event(
            pool,
            "basenames",
            BASE_CHAIN,
            3,
            Some("basenames:0xalice-base"),
            Some(BASENAMES_RESOURCE),
            "SurfaceUnbound",
            "basenames_base_registry",
            1,
            json!({}),
            json!({}),
        )
        .await?;
    }

    run_project(
        incremental.pool(),
        BASE_CHAIN,
        Some(Marker {
            number: 2,
            hash: block_hash(BASE_CHAIN, 2),
        }),
        RunMode::Normal,
        3,
        3,
    )
    .await?;
    run_project(full.pool(), BASE_CHAIN, None, RunMode::Normal, 0, 3).await?;

    let incremental_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(BASE_CHAIN)
    .bind(BASENAMES_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    let full_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(BASE_CHAIN)
    .bind(BASENAMES_RESOLVER)
    .fetch_one(full.pool())
    .await?;

    assert_eq!(incremental_summary, full_summary);
    assert!(full_summary["bindings"]["items"][0]["resource_id"].is_null());
    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn unlinked_record_delta_does_not_discover_a_resolver() -> Result<()> {
    const UNLINKED_RESOLVER: &str = "0x00000000000000000000000000000000000000b9";

    let incremental = ScratchDatabase::create("production_project_unlinked_record_delta").await?;
    let full = ScratchDatabase::create("production_project_unlinked_record_delta_full").await?;
    seed_project_fixture(incremental.pool()).await?;
    seed_project_fixture(full.pool()).await?;
    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    for pool in [incremental.pool(), full.pool()] {
        insert_lineage_block(pool, CHAIN, 4).await?;
        insert_event(
            pool,
            CHAIN,
            4,
            None,
            None,
            "RecordChanged",
            "ens_v1_resolver_l1",
            json!({
                "resolver":UNLINKED_RESOLVER,
                "record_key":"text:url",
                "record_family":"text",
                "selector_key":"url",
                "value_retained":true,
                "value":"https://unlinked.example.test"
            }),
            json!({"emitting_address":UNLINKED_RESOLVER}),
        )
        .await?;
    }

    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;

    let projected: (bool, bool) = (
        sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM resolver_current
                 WHERE chain_id = $1 AND resolver_address = lower($2)
             )",
        )
        .bind(CHAIN)
        .bind(UNLINKED_RESOLVER)
        .fetch_one(incremental.pool())
        .await?,
        sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM resolver_current
                 WHERE chain_id = $1 AND resolver_address = lower($2)
             )",
        )
        .bind(CHAIN)
        .bind(UNLINKED_RESOLVER)
        .fetch_one(full.pool())
        .await?,
    );
    assert_eq!(projected, (false, false));

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn record_delta_rebuilds_emitter_but_not_other_current_resolver_dependents() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_record_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    let bob_resource = "00000000-0000-0000-0000-0000000000b0";
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(bob_resource)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xbob', 'ens', 'bob.eth', ARRAY['bob', 'eth'],
             decode('00', 'hex'), '0xbob', ARRAY['0xbob-label', '0xeth'], $1,
             'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens:0xbob', $2, 'declared_registry_path',
             to_timestamp(1), $3, $4, 1, 'canonical'
         )",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::parse_str(bob_resource)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xbob"),
        Some(bob_resource),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        Some("ens:0xbob"),
        Some(bob_resource),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:description",
            "record_family":"text",
            "selector_key":"description",
            "value_retained":true,
            "value":"bob"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let resolver_summary_before: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE name_current SET last_recomputed_at = to_timestamp(0)
         WHERE logical_name_id = 'ens:0xbob'",
    )
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE record_inventory_current SET last_recomputed_at = to_timestamp(0)
         WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(bob_resource)?)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "UPDATE resolver_current SET last_recomputed_at = to_timestamp(0)
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(scratch.pool())
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:url",
            "record_family":"text",
            "selector_key":"url",
            "value_retained":true,
            "value":"https://changed.example.test"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;

    let clocks: (bool, bool, bool) = sqlx::query_as(
        "SELECT
             (SELECT last_recomputed_at = to_timestamp(0) FROM name_current
              WHERE logical_name_id = 'ens:0xbob'),
             (SELECT last_recomputed_at = to_timestamp(0) FROM record_inventory_current
              WHERE resource_id = $1),
             (SELECT last_recomputed_at > to_timestamp(0) FROM resolver_current
              WHERE chain_id = $2 AND resolver_address = lower($3))",
    )
    .bind(Uuid::parse_str(bob_resource)?)
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(clocks, (true, true, true));
    let resolver_summary_after: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(resolver_summary_after, resolver_summary_before);
    scratch.cleanup().await
}

#[tokio::test]
async fn incremental_resolver_binding_summary_isolated_by_chain() -> Result<()> {
    const OTHER_CHAIN: &str = "other-project-fixture";

    let scratch = ScratchDatabase::create("production_project_resolver_chain_scope").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_lineage_block(scratch.pool(), CHAIN, 4).await?;
    seed_lineage(scratch.pool(), OTHER_CHAIN, 1).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xforeign', 'ens', 'foreign.eth', ARRAY['foreign', 'eth'],
             decode('00', 'hex'), '0xforeign', ARRAY['0xforeign-label', '0xeth'], $1,
             'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(OTHER_CHAIN)
    .bind(block_hash(OTHER_CHAIN, 1))
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO resolver_current (
             chain_id, resolver_address, declared_summary, support_status,
             unsupported_reason, provenance, chain_positions,
             canonicality_summary, manifest_version
         )
         SELECT $1, resolver_address,
                jsonb_set(
                    declared_summary,
                    '{classification,source_family}',
                    '\"basenames_base_resolver\"'::jsonb
                ),
                support_status, unsupported_reason, provenance, chain_positions,
                canonicality_summary, manifest_version
         FROM resolver_current
         WHERE chain_id = $2 AND resolver_address = lower($3)",
    )
    .bind(OTHER_CHAIN)
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(scratch.pool())
    .await?;
    sqlx::query(
        "INSERT INTO name_current (
             logical_name_id, namespace, raw_name, namehash,
             declared_summary, support_status, provenance, chain_positions,
             canonicality_summary, manifest_version
         ) VALUES (
             'ens:0xforeign', 'ens', 'foreign.eth', '0xforeign',
             jsonb_build_object(
                 'resolver', jsonb_build_object('chain_id', $1, 'address', lower($2))
             ),
             'supported', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1
         )",
    )
    .bind(OTHER_CHAIN)
    .bind(RESOLVER)
    .execute(scratch.pool())
    .await?;

    insert_event(
        scratch.pool(),
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_resolver_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resolver","chain_id":CHAIN,"resolver_address":RESOLVER},
            "effective_powers":["record_write"],
            "grant_source":{"kind":"fixture"},
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Normal,
        4,
        4,
    )
    .await?;
    let incremental: Option<Value> = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_optional(scratch.pool())
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 4).await?;
    let full: Option<Value> = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .fetch_optional(scratch.pool())
    .await?;

    assert_eq!(incremental, full);
    scratch.cleanup().await
}

#[tokio::test]
async fn incremental_wrapper_expiry_only_tick_retains_child_families() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_wrapper_child_scope").await?;
    let full = ScratchDatabase::create("production_project_wrapper_child_scope_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
    }

    for target in 1..=4 {
        let previous = (target > 1).then(|| Marker {
            number: target - 1,
            hash: block_hash(CHAIN, target - 1),
        });
        run_project(
            incremental.pool(),
            CHAIN,
            previous,
            RunMode::Normal,
            target,
            target,
        )
        .await?;
    }

    let mut incremental_children = Vec::new();
    let mut full_children = Vec::new();
    for target in [5, 6] {
        let event_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM normalized_events
             WHERE chain_id = $1 AND block_number = $2",
        )
        .bind(CHAIN)
        .bind(target)
        .fetch_one(incremental.pool())
        .await?;
        assert_eq!(event_count, 0, "the {target} tick must be event-free");

        run_project(
            incremental.pool(),
            CHAIN,
            Some(Marker {
                number: target - 1,
                hash: block_hash(CHAIN, target - 1),
            }),
            RunMode::Normal,
            target,
            target,
        )
        .await?;
        run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, target).await?;

        let incremental_v1 = selected_children(
            incremental.pool(),
            "ens:0xeth",
            &["ens:0xalice", "ens:0xhostile"],
        )
        .await?;
        let full_v1 =
            selected_children(full.pool(), "ens:0xeth", &["ens:0xalice", "ens:0xhostile"]).await?;
        let incremental_v2 = selected_children(
            incremental.pool(),
            "ens:0xequivalence-parent",
            &["ens:0xequivalence-c0", "ens:0xequivalence-c1"],
        )
        .await?;
        let full_v2 = selected_children(
            full.pool(),
            "ens:0xequivalence-parent",
            &["ens:0xequivalence-c0", "ens:0xequivalence-c1"],
        )
        .await?;
        incremental_children.push((target, incremental_v1, incremental_v2));
        full_children.push((target, full_v1, full_v2));
    }
    assert_eq!(
        full_children,
        vec![
            (
                5,
                vec!["ens:0xalice".into(), "ens:0xhostile".into()],
                vec!["ens:0xequivalence-c1".into()]
            ),
            (
                6,
                vec!["ens:0xalice".into(), "ens:0xhostile".into()],
                vec!["ens:0xequivalence-c1".into()]
            ),
        ]
    );
    assert_eq!(
        incremental_children, full_children,
        "the expiry tick or its event-free successor lost a child family"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

async fn selected_children(
    pool: &PgPool,
    parent: &str,
    candidates: &[&str],
) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(
        "SELECT child_logical_name_id
         FROM children_current
         WHERE parent_logical_name_id = $1
           AND child_logical_name_id = ANY($2)
         ORDER BY child_logical_name_id",
    )
    .bind(parent)
    .bind(candidates)
    .fetch_all(pool)
    .await?)
}

#[tokio::test]
async fn incremental_ticks_match_one_full_rebuild_across_all_eight_serving_tables() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_incremental_equivalence").await?;
    let full = ScratchDatabase::create("production_project_full_equivalence").await?;
    seed_project_fixture(incremental.pool()).await?;
    seed_project_fixture(full.pool()).await?;
    for pool in [incremental.pool(), full.pool()] {
        extend_incremental_equivalence_fixture(pool).await?;
        extend_equivalence_registrar_transfer_fixture(pool).await?;
    }

    for target in 1..=8 {
        let previous = (target > 1).then(|| Marker {
            number: target - 1,
            hash: block_hash(CHAIN, target - 1),
        });
        run_project(
            incremental.pool(),
            CHAIN,
            previous,
            RunMode::Normal,
            target,
            target,
        )
        .await?;

        if target == 3 {
            let shared_pointer_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM name_current
                 WHERE declared_summary #>> '{resolver,address}' = lower($1)",
            )
            .bind(RESOLVER)
            .fetch_one(incremental.pool())
            .await?;
            assert_eq!(shared_pointer_count, 2, "fixture must share the resolver");
        }
        if target == 4 {
            let pointers: Vec<(String, String)> = sqlx::query_as(
                "SELECT logical_name_id,
                        declared_summary #>> '{resolver,address}'
                 FROM name_current
                 WHERE logical_name_id IN ('ens:0xalice', 'ens:0xbob')
                 ORDER BY logical_name_id",
            )
            .fetch_all(incremental.pool())
            .await?;
            assert_eq!(
                pointers,
                vec![
                    ("ens:0xalice".into(), EQUIVALENCE_V2_RESOLVER.into()),
                    ("ens:0xbob".into(), RESOLVER.into()),
                ],
                "ResolverChanged must replace only Alice's current pointer"
            );
            let grace_powers: Value = sqlx::query_scalar(
                "SELECT effective_powers FROM permissions_current WHERE resource_id = $1",
            )
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(incremental.pool())
            .await?;
            assert_eq!(grace_powers, json!(["approve"]));
            let children: Vec<String> = sqlx::query_scalar(
                "SELECT child_logical_name_id FROM children_current
                 WHERE parent_logical_name_id = 'ens:0xequivalence-parent'
                 ORDER BY child_logical_name_id",
            )
            .fetch_all(incremental.pool())
            .await?;
            assert_eq!(
                children,
                vec!["ens:0xequivalence-c1"],
                "SubregistryChanged must stage the newly selected registry's children"
            );
        }
        if target == 5 {
            let event_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM normalized_events
                 WHERE chain_id = $1 AND block_number = 5",
            )
            .bind(CHAIN)
            .fetch_one(incremental.pool())
            .await?;
            assert_eq!(event_count, 0, "expiry must cross in an event-free window");
            let wrapper_permission_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM permissions_current WHERE resource_id = $1
                 )",
            )
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(incremental.pool())
            .await?;
            assert!(
                !wrapper_permission_exists,
                "expiry boundary must be crossed"
            );
            let eth_children = selected_children(
                incremental.pool(),
                "ens:0xeth",
                &["ens:0xalice", "ens:0xbob", "ens:0xhostile"],
            )
            .await?;
            assert_eq!(
                eth_children,
                vec!["ens:0xalice", "ens:0xbob", "ens:0xhostile"],
                "closure-only expiry scope must retain the whole .eth child family"
            );
            let v2_children = selected_children(
                incremental.pool(),
                "ens:0xequivalence-parent",
                &["ens:0xequivalence-c0", "ens:0xequivalence-c1"],
            )
            .await?;
            assert_eq!(
                v2_children,
                vec!["ens:0xequivalence-c1"],
                "closure-only expiry scope must retain the selected ENSv2 registry family"
            );
        }
        if target == 6 {
            let event_count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM normalized_events
                 WHERE chain_id = $1 AND block_number = 6",
            )
            .bind(CHAIN)
            .fetch_one(incremental.pool())
            .await?;
            assert_eq!(event_count, 0, "the follow-up tick must remain event-free");
        }
        if target == 7 {
            let inventory_status: String = sqlx::query_scalar(
                "SELECT support_status FROM record_inventory_current WHERE resource_id = $1",
            )
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(incremental.pool())
            .await?;
            assert_eq!(
                inventory_status, "supported",
                "Upgraded must rebuild the resolver's current resource dependents"
            );
            let transferred_resolver_permission: (Option<String>, Option<String>, Option<String>) =
                sqlx::query_as(
                    "SELECT
                         declared_summary #>> '{permissions,items,0,subject}',
                         declared_summary #>>
                             '{permissions,items,0,grant_source,source_event_kind}',
                         declared_summary #>> '{role_holders,items,0,subject}'
                     FROM resolver_current
                     WHERE chain_id = $1 AND resolver_address = lower($2)",
                )
                .bind(CHAIN)
                .bind(EQUIVALENCE_TRANSFER_RESOLVER)
                .fetch_one(incremental.pool())
                .await?;
            assert_eq!(
                transferred_resolver_permission,
                (
                    Some(TRANSFER_OWNER.into()),
                    Some("TokenControlTransferred".into()),
                    Some(TRANSFER_OWNER.into())
                ),
                "registrar transfer must refresh the resolver permission summary"
            );
        }
        if target == 8 {
            let children: Vec<String> = sqlx::query_scalar(
                "SELECT child_logical_name_id FROM children_current
                 WHERE parent_logical_name_id = 'ens:0xequivalence-parent'
                 ORDER BY child_logical_name_id",
            )
            .fetch_all(incremental.pool())
            .await?;
            assert_eq!(
                children,
                vec!["ens:0xequivalence-c1"],
                "SubregistryChanged must replace the old registry's children"
            );
        }
    }
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 8).await?;

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(full.pool()).await?;
    assert_eq!(
        serving_table_snapshot(incremental.pool()).await?,
        serving_table_snapshot(full.pool()).await?,
        "incremental Project output diverged from a full rebuild"
    );

    let resolver_states: Vec<(String, String)> = sqlx::query_as(
        "SELECT resolver_address, support_status
         FROM resolver_current
         WHERE chain_id = $1
           AND resolver_address IN (lower($2), lower($3), lower($4))
         ORDER BY resolver_address",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .bind(EQUIVALENCE_TRANSFER_RESOLVER)
    .fetch_all(incremental.pool())
    .await?;
    assert_eq!(
        resolver_states,
        vec![
            (RESOLVER.into(), "supported".into()),
            (EQUIVALENCE_V2_RESOLVER.into(), "supported".into()),
            (EQUIVALENCE_TRANSFER_RESOLVER.into(), "supported".into()),
        ],
        "the shared and moved resolver rows must survive"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn narrow_final_tick_matches_full_rebuild_across_all_eight_serving_tables() -> Result<()> {
    let incremental =
        ScratchDatabase::create("production_project_narrow_final_equivalence").await?;
    let full = ScratchDatabase::create("production_project_narrow_final_equivalence_full").await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        extend_equivalence_registrar_transfer_fixture(pool).await?;
        extend_narrow_final_equivalence_fixture(pool).await?;
    }

    for target in 1..=10 {
        let previous = (target > 1).then(|| Marker {
            number: target - 1,
            hash: block_hash(CHAIN, target - 1),
        });
        run_project(
            incremental.pool(),
            CHAIN,
            previous,
            RunMode::Normal,
            target,
            target,
        )
        .await?;
    }
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 10).await?;

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(full.pool()).await?;
    assert_ne!(
        serving_table_snapshot(incremental.pool()).await?,
        serving_table_snapshot(full.pool()).await?,
        "the narrow final tick must leave at least one unscoped row at its prior stamp"
    );
    assert_eq!(
        serving_table_snapshot_without_vintage_stamps(incremental.pool()).await?,
        serving_table_snapshot_without_vintage_stamps(full.pool()).await?,
        "narrow incremental Project output diverged semantically from a full rebuild"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn topology_staged_sibling_is_not_double_counted_in_resolver_bindings() -> Result<()> {
    let incremental = ScratchDatabase::create("production_project_topology_binding_probe").await?;
    let full = ScratchDatabase::create("production_project_topology_binding_probe_full").await?;

    for pool in [incremental.pool(), full.pool()] {
        seed_project_fixture(pool).await?;
        extend_incremental_equivalence_fixture(pool).await?;
        insert_event(
            pool,
            CHAIN,
            6,
            Some("ens:0xbob"),
            Some(EQUIVALENCE_BOB_RESOURCE),
            "ResolverChanged",
            "ens_v2_registry_l1",
            json!({"resolver":EQUIVALENCE_V2_RESOLVER}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events SET before_state = jsonb_build_object('resolver', lower($1))
             WHERE chain_id = $2 AND block_number = 6
               AND logical_name_id = 'ens:0xbob' AND event_kind = 'ResolverChanged'",
        )
        .bind(RESOLVER)
        .bind(CHAIN)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, 9, to_timestamp(7776008), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 9))
        .bind(block_hash(CHAIN, 8))
        .execute(pool)
        .await?;
        insert_event(
            pool,
            CHAIN,
            9,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "SubregistryChanged",
            "ens_v1_registry_l1",
            json!({
                "node":"0xeth",
                "child_node":"0xalice",
                "labelhash":"0xalice-label",
                "owner":OWNER
            }),
            json!({}),
        )
        .await?;
        insert_event(
            pool,
            CHAIN,
            9,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "PermissionChanged",
            "ens_v2_resolver_l1",
            json!({
                "subject":OWNER,
                "scope":{
                    "kind":"resolver",
                    "chain_id":CHAIN,
                    "resolver_address":EQUIVALENCE_V2_RESOLVER
                },
                "effective_powers":["record_write"],
                "grant_source":{"kind":"fixture"},
                "inheritance_path":[],
                "transfer_behavior":"retain"
            }),
            json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
        )
        .await?;
    }

    run_project(incremental.pool(), CHAIN, None, RunMode::Normal, 0, 8).await?;
    run_project(
        incremental.pool(),
        CHAIN,
        Some(Marker {
            number: 8,
            hash: block_hash(CHAIN, 8),
        }),
        RunMode::Normal,
        9,
        9,
    )
    .await?;
    run_project(full.pool(), CHAIN, None, RunMode::Normal, 0, 9).await?;

    let incremental_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    let full_summary: Value = sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(CHAIN)
    .bind(EQUIVALENCE_V2_RESOLVER)
    .fetch_one(full.pool())
    .await?;

    assert_eq!(
        incremental_summary, full_summary,
        "topology-only staging must not duplicate a retained resolver binding"
    );
    assert_eq!(full_summary["bindings"]["count"], json!(2));

    incremental.cleanup().await?;
    full.cleanup().await
}

#[tokio::test]
async fn atomic_swap_never_exposes_a_partially_replaced_projection_set() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_atomic_swap").await?;
    seed_project_fixture(scratch.pool()).await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let before = projection_counts(scratch.pool()).await?;
    let before_authority = projection_authority_pair(scratch.pool()).await?;
    assert_eq!(before_authority, (OWNER.into(), OWNER.into()));
    let successor = "0x00000000000000000000000000000000000000a2";
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registry_l1",
        json!({"owner":successor}),
        json!({}),
    )
    .await?;

    sqlx::raw_sql(
        "CREATE FUNCTION pause_project_publication() RETURNS trigger AS $$
         BEGIN
             PERFORM pg_sleep(1.0);
             RETURN NEW;
         END $$ LANGUAGE plpgsql;
         CREATE TRIGGER pause_project_publication
         BEFORE INSERT ON name_current
         FOR EACH STATEMENT EXECUTE FUNCTION pause_project_publication();",
    )
    .execute(scratch.pool())
    .await?;

    let engine = Engine::new(scratch.pool().clone());
    let rebuild = tokio::spawn(async move {
        engine
            .run_batch(BatchRequest {
                chain_id: CHAIN.into(),
                target_block: 3,
                affected_from_block: 0,
                affected_to_block: 3,
                resume_current: None,
                mode: RunMode::Normal,
            })
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(projection_counts(scratch.pool()).await?, before);
    assert_eq!(
        projection_authority_pair(scratch.pool()).await?,
        before_authority,
        "reader must retain the complete prior projection set"
    );
    rebuild.await??;
    assert_eq!(projection_counts(scratch.pool()).await?, before);
    assert_eq!(
        projection_authority_pair(scratch.pool()).await?,
        (successor.into(), successor.into()),
        "reader must observe the complete successor projection set"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn manifest_admission_reclassifies_v2_resolver_inline_without_a_queue() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_reconvergence").await?;
    let chain = "project-reconvergence";
    let resolver_manifest = seed_reconvergence_fixture(scratch.pool(), chain).await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 1).await?;
    let before: String = sqlx::query_scalar("SELECT support_status FROM resolver_current")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(before, "unsupported");
    let resolver_count: i64 = sqlx::query_scalar("SELECT count(*) FROM resolver_current")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(
        resolver_count, 1,
        "registry proxy upgrades are not resolvers"
    );

    sqlx::query(
        r#"UPDATE manifest_versions
         SET manifest_payload = jsonb_set(
             manifest_payload,
             '{resolver_implementations}',
             '[{"role":"permissioned_resolver","address":"0x00000000000000000000000000000000000000c1"}]'::jsonb
         )
         WHERE chain_id = $1 AND source_family = 'ens_v2_resolver_l1'"#,
    )
    .bind(chain)
    .execute(scratch.pool())
    .await?;
    let manifest_payload: Value =
        sqlx::query_scalar("SELECT manifest_payload FROM manifest_versions WHERE manifest_id = $1")
            .bind(resolver_manifest)
            .fetch_one(scratch.pool())
            .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 1).await?;
    let without_event: String = sqlx::query_scalar("SELECT support_status FROM resolver_current")
        .fetch_one(scratch.pool())
        .await?;
    assert_eq!(
        without_event, "unsupported",
        "manifest bytes alone do not bypass SourceManifestUpdated"
    );
    insert_manifest_update_event(
        scratch.pool(),
        chain,
        "ens_v2_resolver_l1",
        resolver_manifest,
        manifest_payload,
    )
    .await?;
    run_project(
        scratch.pool(),
        chain,
        Some(Marker {
            number: 1,
            hash: block_hash(chain, 1),
        }),
        RunMode::Normal,
        0,
        0,
    )
    .await?;
    let after: (String, String) = sqlx::query_as(
        "SELECT support_status,
                declared_summary -> 'classification' ->> 'basis'
         FROM resolver_current",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(
        after,
        ("supported".into(), "erc1967_upgraded_history".into())
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn raw_ingest_fixture_flows_through_interpret_then_project() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_raw_flow").await?;
    let chain = "project-raw-flow";
    seed_raw_registration_fixture(scratch.pool(), chain).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;

    let outputs: (i64, i64, i64, i64, i64, i64, i64, i64) = sqlx::query_as(
        "SELECT
            (SELECT count(*) FROM name_current),
            (SELECT count(*) FROM children_current),
            (SELECT count(*) FROM permissions_current),
            (SELECT count(*) FROM permissions_current_resource_summary),
            (SELECT count(*) FROM record_inventory_current),
            (SELECT count(*) FROM resolver_current),
            (SELECT count(*) FROM address_names_current),
            (SELECT count(*) FROM primary_names_current)",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        [
            outputs.0, outputs.1, outputs.2, outputs.3, outputs.4, outputs.5, outputs.6, outputs.7,
        ]
        .into_iter()
        .all(|count| count > 0),
        "every projection table must be populated from interpreted raw facts: {outputs:?}"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn registrar_transfer_nested_resolver_scope_matches_full_rebuild() -> Result<()> {
    assert_registrar_transfer_matches_full_rebuild("project-resolver-transfer", false).await
}

#[tokio::test]
async fn registrar_transfer_authority_flip_matches_full_rebuild() -> Result<()> {
    assert_registrar_transfer_matches_full_rebuild("project-resolver-transfer-flip", true).await
}

async fn assert_registrar_transfer_matches_full_rebuild(
    chain: &str,
    include_registry_owner: bool,
) -> Result<()> {
    let incremental = ScratchDatabase::create(&format!("{chain}-incremental")).await?;
    let full = ScratchDatabase::create(&format!("{chain}-full")).await?;
    for pool in [incremental.pool(), full.pool()] {
        seed_raw_registrar_transfer_fixture(pool, chain, include_registry_owner).await?;
        InterpretEngine::new(pool.clone())
            .run_batch(InterpretRequest {
                chain_id: chain.into(),
                from_block: 0,
                to_block: 5,
                resume_current: None,
                mode: InterpretRunMode::Normal,
            })
            .await?;
    }

    let resolver_permissions: Vec<(Value, Value, String, Option<String>)> = sqlx::query_as(
        "SELECT before_state, after_state, source_family,
                raw_fact_ref ->> 'emitting_address'
         FROM normalized_events
         WHERE chain_id = $1 AND block_number = 5
           AND event_kind = 'PermissionChanged'
           AND COALESCE(
               after_state #>> '{scope,kind}',
               before_state #>> '{scope,kind}'
           ) = 'resolver'
         ORDER BY normalized_event_id",
    )
    .bind(chain)
    .fetch_all(incremental.pool())
    .await?;
    assert_eq!(
        resolver_permissions.len(),
        2,
        "transfer must emit one resolver revoke and one resolver grant"
    );
    assert!(resolver_permissions.iter().all(|(_, _, family, emitter)| {
        family == "basenames_base_registrar" && emitter.as_deref() == Some(REGISTRAR)
    }));
    let revoke = resolver_permissions
        .iter()
        .find(|(before, _, _, _)| before.pointer("/subject").and_then(Value::as_str) == Some(OWNER))
        .expect("old-owner resolver revoke");
    assert_eq!(
        revoke.0.pointer("/scope/resolver_address"),
        Some(&json!(RESOLVER))
    );
    assert_eq!(
        revoke.0.pointer("/effective_powers"),
        Some(&json!(["resolver_control"]))
    );
    let grant = resolver_permissions
        .iter()
        .find(|(_, after, _, _)| {
            after.pointer("/subject").and_then(Value::as_str) == Some(TRANSFER_OWNER)
        })
        .expect("new-owner resolver grant");
    assert_eq!(
        grant.1.pointer("/scope/resolver_address"),
        Some(&json!(RESOLVER))
    );
    assert_eq!(
        grant.1.pointer("/grant_source/source_event_kind"),
        Some(&json!("TokenControlTransferred"))
    );
    assert!(resolver_permissions.iter().all(|(before, after, _, _)| {
        before.get("resolver").is_none() && after.get("resolver").is_none()
    }));

    let surface_event_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND block_number = 5
           AND event_kind IN ('SurfaceBound', 'SurfaceUnbound')",
    )
    .bind(chain)
    .fetch_one(incremental.pool())
    .await?;
    if include_registry_owner {
        assert!(
            surface_event_count > 0,
            "authority mismatch must flip the surface"
        );
    } else {
        assert_eq!(
            surface_event_count, 0,
            "registrar authority must be retained"
        );
    }

    run_project(incremental.pool(), chain, None, RunMode::Normal, 0, 4).await?;
    let pointer_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM record_inventory_current
             WHERE provenance ->> 'chain_id' = $1
               AND lower(provenance ->> 'resolver_address') = lower($2)
         )",
    )
    .bind(chain)
    .bind(RESOLVER)
    .fetch_one(incremental.pool())
    .await?;
    assert!(
        pointer_exists,
        "fixture must scope the resolver through its resource pointer"
    );
    run_project(
        incremental.pool(),
        chain,
        Some(Marker {
            number: 4,
            hash: block_hash(chain, 4),
        }),
        RunMode::Normal,
        5,
        5,
    )
    .await?;
    run_project(full.pool(), chain, None, RunMode::Normal, 0, 5).await?;

    for pool in [incremental.pool(), full.pool()] {
        let current_permission: (String, Option<String>) = sqlx::query_as(
            "SELECT subject, grant_source ->> 'source_event_kind'
             FROM permissions_current
             WHERE scope_kind = 'resolver'
               AND lower(scope_detail ->> 'resolver_address') = lower($1)",
        )
        .bind(RESOLVER)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            current_permission,
            (
                TRANSFER_OWNER.into(),
                Some("TokenControlTransferred".into())
            )
        );
    }

    let incremental_resolver = resolver_permission_summary(incremental.pool(), chain).await?;
    let full_resolver = resolver_permission_summary(full.pool(), chain).await?;
    for summary in [&incremental_resolver, &full_resolver] {
        assert_eq!(
            summary.pointer("/permissions/items/0/subject"),
            Some(&json!(TRANSFER_OWNER))
        );
        assert_eq!(
            summary.pointer("/permissions/items/0/grant_source/source_event_kind"),
            Some(&json!("TokenControlTransferred"))
        );
        assert_eq!(
            summary.pointer("/role_holders/items/0/subject"),
            Some(&json!(TRANSFER_OWNER))
        );
    }

    normalize_projection_clocks(incremental.pool()).await?;
    normalize_projection_clocks(full.pool()).await?;
    assert_eq!(
        serving_table_snapshot(incremental.pool()).await?,
        serving_table_snapshot(full.pool()).await?,
        "registrar-transfer incremental tick diverged from a full rebuild"
    );

    incremental.cleanup().await?;
    full.cleanup().await
}

async fn resolver_permission_summary(pool: &PgPool, chain: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
             'permissions', declared_summary -> 'permissions',
             'role_holders', declared_summary -> 'role_holders'
         )
         FROM resolver_current
         WHERE chain_id = $1 AND resolver_address = lower($2)",
    )
    .bind(chain)
    .bind(RESOLVER)
    .fetch_one(pool)
    .await?)
}

#[tokio::test]
async fn interpret_data_repair_redo_cascades_to_project_without_an_operator_step() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_interpret_redo_cascade").await?;
    let chain = "project-interpret-redo-cascade";
    seed_raw_registration_fixture(scratch.pool(), chain).await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    sqlx::query("UPDATE name_current SET raw_name = 'tampered.eth' WHERE raw_name = 'alice.eth'")
        .execute(scratch.pool())
        .await?;

    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_project_extent(scratch.pool(), chain, 5).await?;
    let runner = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(InterpretPhase::new(scratch.pool().clone())),
            Arc::new(ProjectPhase::new(scratch.pool().clone())),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "production-interpret-redo-cascade",
        test_timing(),
    )?;
    runner
        .redo(
            &chain_config(chain)?,
            RedoPhase::Phase(PhaseName::Interpret),
            BlockRange::new(2, 2)?,
            CancellationToken::new(),
        )
        .await?;

    let names: Vec<String> =
        sqlx::query_scalar("SELECT raw_name FROM name_current ORDER BY raw_name")
            .fetch_all(scratch.pool())
            .await?;
    assert_eq!(names, vec!["alice.eth", "eth"]);
    let project_state: (String, bool) = sqlx::query_as(
        "SELECT phase_status, redo_in_progress FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(project_state, ("completed".into(), false));
    scratch.cleanup().await
}

#[tokio::test]
async fn reorg_below_interpret_cursor_rederives_winning_fork_through_project() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_reorg_cascade").await?;
    let chain = "project-reorg-cascade";
    seed_raw_registration_fixture(scratch.pool(), chain).await?;
    let alice_node = raw_namehash(&[b"alice", b"eth"]);
    let losing_record = TextChanged {
        node: alice_node,
        indexedKey: keccak256(b"avatar"),
        key: "avatar".into(),
        value: "ipfs://losing-fork".into(),
    }
    .encode_log_data();
    insert_raw_event_at(
        scratch.pool(),
        chain,
        5,
        1,
        1,
        RESOLVER,
        losing_record.topics(),
        losing_record.data.as_ref(),
    )
    .await?;
    publish_heads(
        scratch.pool(),
        chain,
        &HeadMarkers {
            latest: BlockMarker::new(5, block_hash(chain, 5))?,
            safe: Some(BlockMarker::new(4, block_hash(chain, 4))?),
            finalized: Some(BlockMarker::new(4, block_hash(chain, 4))?),
        },
    )
    .await?;
    InterpretEngine::new(scratch.pool().clone())
        .run_batch(InterpretRequest {
            chain_id: chain.into(),
            from_block: 0,
            to_block: 5,
            resume_current: None,
            mode: InterpretRunMode::Normal,
        })
        .await?;
    run_project(scratch.pool(), chain, None, RunMode::Normal, 0, 5).await?;
    let before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM primary_names_current WHERE namespace = 'ens'")
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(before, 1);
    let losing_record_before: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM record_inventory_current
             WHERE entries @> '[{\"record_key\":\"text:avatar\"}]'::jsonb
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(losing_record_before);

    let store = PhaseStore::new(scratch.pool().clone());
    store.initialize_chain(chain).await?;
    seed_completed_project_extent(scratch.pool(), chain, 5).await?;
    let winning_hash = format!("{chain}-winning-block-5");
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, 5, to_timestamp(5), 'observed')",
    )
    .bind(chain)
    .bind(&winning_hash)
    .bind(block_hash(chain, 4))
    .execute(scratch.pool())
    .await?;
    publish_heads(
        scratch.pool(),
        chain,
        &HeadMarkers {
            latest: BlockMarker::new(5, winning_hash.clone())?,
            safe: Some(BlockMarker::new(4, block_hash(chain, 4))?),
            finalized: Some(BlockMarker::new(4, block_hash(chain, 4))?),
        },
    )
    .await?;

    let runner = PhaseRunner::new(
        scratch.runner(),
        PhaseSet::with_ingest_interpret_and_project(
            Arc::new(LoopbackPhase::new(PhaseName::Ingest)),
            Arc::new(InterpretPhase::new(scratch.pool().clone())),
            Arc::new(ProjectPhase::new(scratch.pool().clone())),
        )?,
        CapacityGuard::system(CapacityConfig::default()),
        "production-reorg-cascade",
        test_timing(),
    )?;
    let terminal = runner
        .run_chain(&chain_config(chain)?, CancellationToken::new())
        .await
        .expect_err("the intentionally unavailable verify/live slot stops after re-derivation");
    assert_eq!(
        terminal.kind(),
        phase_runner::error::ErrorKind::Configuration
    );

    let readable_reverse: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'ReverseChanged'
           AND canonicality_state IN ('canonical', 'safe', 'finalized')",
    )
    .bind(chain)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(readable_reverse, 0);
    let projected: i64 =
        sqlx::query_scalar("SELECT count(*) FROM primary_names_current WHERE namespace = 'ens'")
            .fetch_one(scratch.pool())
            .await?;
    let reverse_states: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_identity, canonicality_state::text FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'ReverseChanged'",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    let primary_rows: Vec<(String, Value)> = sqlx::query_as(
        "SELECT address, claim_provenance FROM primary_names_current WHERE namespace = 'ens'",
    )
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        projected, 0,
        "reverse events: {reverse_states:?}; primary rows: {primary_rows:?}"
    );
    let losing_record_after: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1 FROM record_inventory_current
             WHERE entries @> '[{\"record_key\":\"text:avatar\"}]'::jsonb
         )",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert!(
        !losing_record_after,
        "record inventory retained a normalized event deleted by interpret redo"
    );
    let states: Vec<(String, String, bool, Option<String>)> = sqlx::query_as(
        "SELECT phase_name, phase_status, redo_in_progress, current_block_hash
         FROM chain_phase_state
         WHERE chain_id = $1 AND phase_name IN ('interpret', 'project')
         ORDER BY phase_name",
    )
    .bind(chain)
    .fetch_all(scratch.pool())
    .await?;
    assert_eq!(
        states,
        vec![
            (
                "interpret".into(),
                "completed".into(),
                false,
                Some(winning_hash.clone()),
            ),
            (
                "project".into(),
                "completed".into(),
                false,
                Some(winning_hash),
            ),
        ]
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_retracts_a_missing_primary_claim_with_surviving_reverse() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_missing_primary_claim").await?;
    seed_project_fixture(scratch.pool()).await?;
    sqlx::query(
        "UPDATE normalized_events
         SET block_number = 2, block_hash = $2
         WHERE chain_id = $1 AND event_kind = 'ReverseChanged'",
    )
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 2))
    .execute(scratch.pool())
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let before: (String, Option<String>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name
         FROM primary_names_current WHERE namespace = 'ens'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(before, ("success".to_owned(), Some("alice.eth".to_owned())));

    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'RecordChanged'
           AND after_state ? 'primary_claim_source'",
    )
    .bind(CHAIN)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        3,
        3,
    )
    .await?;

    let after: (String, Option<String>) = sqlx::query_as(
        "SELECT claim_status, raw_claim_name
         FROM primary_names_current WHERE namespace = 'ens'",
    )
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after, ("not_found".to_owned(), None));
    scratch.cleanup().await
}

#[tokio::test]
async fn project_redo_retracts_a_missing_reverse_resolver_pointer() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_missing_reverse_resolver").await?;
    seed_project_fixture(scratch.pool()).await?;
    let address = "0x00000000000000000000000000000000000000a2";
    let reverse_node = "0x2223456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    insert_event(
        scratch.pool(),
        CHAIN,
        1,
        None,
        None,
        "ReverseChanged",
        "ens_v1_reverse_l1",
        json!({
            "address":address,
            "coin_type":"60",
            "namespace":"ens",
            "reverse_node":reverse_node
        }),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        2,
        None,
        None,
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"node":reverse_node,"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        None,
        None,
        "RecordChanged",
        "ens_v1_reverse_l1",
        json!({
            "raw_name":"alice.eth",
            "primary_claim_source":{
                "address":address,
                "coin_type":"60",
                "namespace":"ens"
            }
        }),
        json!({}),
    )
    .await?;
    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let before: (Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT claim_provenance ->> 'resolver_address',
                NULLIF(claim_provenance ->> 'resolver_event_id', '')::bigint
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(address)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(before.0.as_deref(), Some(RESOLVER));
    assert!(before.1.is_some());

    sqlx::query(
        "DELETE FROM normalized_events
         WHERE chain_id = $1 AND block_number = 2
           AND event_kind = 'ResolverChanged'
           AND after_state ->> 'node' = $2",
    )
    .bind(CHAIN)
    .bind(reverse_node)
    .execute(scratch.pool())
    .await?;
    run_project(
        scratch.pool(),
        CHAIN,
        Some(Marker {
            number: 3,
            hash: block_hash(CHAIN, 3),
        }),
        RunMode::Redo,
        2,
        2,
    )
    .await?;

    let after: (Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT claim_provenance ->> 'resolver_address',
                NULLIF(claim_provenance ->> 'resolver_event_id', '')::bigint
         FROM primary_names_current
         WHERE address = $1 AND coin_type = '60' AND namespace = 'ens'",
    )
    .bind(address)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(after, (None, None));
    scratch.cleanup().await
}

async fn run_project(
    pool: &PgPool,
    chain_id: &str,
    resume_current: Option<Marker>,
    mode: RunMode,
    affected_from_block: i64,
    affected_to_block: i64,
) -> Result<()> {
    let target_block = resume_current.as_ref().map_or(affected_to_block, |marker| {
        marker.number.max(affected_to_block)
    });
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: chain_id.into(),
            target_block,
            affected_from_block,
            affected_to_block,
            resume_current,
            mode,
        })
        .await?;
    assert!(outcome.complete);
    Ok(())
}

async fn projection_counts(pool: &PgPool) -> Result<(i64, i64, i64, i64, i64, i64, i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT
            (SELECT count(*) FROM name_current),
            (SELECT count(*) FROM children_current),
            (SELECT count(*) FROM permissions_current),
            (SELECT count(*) FROM permissions_current_resource_summary),
            (SELECT count(*) FROM record_inventory_current),
            (SELECT count(*) FROM resolver_current),
            (SELECT count(*) FROM address_names_current),
            (SELECT count(*) FROM primary_names_current)",
    )
    .fetch_one(pool)
    .await?)
}

async fn normalize_projection_clocks(pool: &PgPool) -> Result<()> {
    for statement in [
        "UPDATE name_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
        "UPDATE children_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
        "UPDATE permissions_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
        "UPDATE permissions_current_resource_summary SET last_recomputed_at = to_timestamp(0)",
        "UPDATE record_inventory_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
        "UPDATE resolver_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
        "UPDATE address_names_current SET last_recomputed_at = to_timestamp(0), inserted_at = to_timestamp(0)",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}

async fn serving_table_snapshot(pool: &PgPool) -> Result<Vec<(String, Value)>> {
    let tables = [
        ("name_current", "logical_name_id"),
        (
            "children_current",
            "parent_logical_name_id, child_logical_name_id, surface_class",
        ),
        ("permissions_current", "resource_id, subject, scope"),
        ("permissions_current_resource_summary", "resource_id"),
        (
            "record_inventory_current",
            "resource_id, record_version_boundary_key",
        ),
        ("resolver_current", "chain_id, resolver_address"),
        (
            "address_names_current",
            "address, logical_name_id, relation",
        ),
        ("primary_names_current", "address, coin_type, namespace"),
    ];
    let mut snapshot = Vec::with_capacity(tables.len());
    for (table, order) in tables {
        let statement = format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(row) ORDER BY {order}), '[]'::jsonb)
             FROM {table} row"
        );
        snapshot.push((
            table.to_owned(),
            sqlx::query_scalar(&statement).fetch_one(pool).await?,
        ));
    }
    Ok(snapshot)
}

async fn serving_table_snapshot_without_vintage_stamps(
    pool: &PgPool,
) -> Result<Vec<(String, Value)>> {
    let tables = [
        ("name_current", "logical_name_id"),
        (
            "children_current",
            "parent_logical_name_id, child_logical_name_id, surface_class",
        ),
        ("permissions_current", "resource_id, subject, scope"),
        ("permissions_current_resource_summary", "resource_id"),
        (
            "record_inventory_current",
            "resource_id, record_version_boundary_key",
        ),
        ("resolver_current", "chain_id, resolver_address"),
        (
            "address_names_current",
            "address, logical_name_id, relation",
        ),
        ("primary_names_current", "address, coin_type, namespace"),
    ];
    let mut snapshot = Vec::with_capacity(tables.len());
    for (table, order) in tables {
        let statement = format!(
            "SELECT COALESCE(jsonb_agg(
                 (
                     to_jsonb(row)
                     - 'chain_positions'
                     #- '{{canonicality_summary,target_block_number}}'
                     #- '{{canonicality_summary,target_block_hash}}'
                     #- '{{claim_provenance,target_block_number}}'
                     #- '{{claim_provenance,target_block_hash}}'
                 ) ORDER BY {order}
             ), '[]'::jsonb)
             FROM {table} row"
        );
        snapshot.push((
            table.to_owned(),
            sqlx::query_scalar(&statement).fetch_one(pool).await?,
        ));
    }
    Ok(snapshot)
}

async fn projection_authority_pair(pool: &PgPool) -> Result<(String, String)> {
    Ok(sqlx::query_as(
        "SELECT
             (SELECT declared_summary -> 'control' ->> 'registry_owner'
              FROM name_current
              WHERE logical_name_id = 'ens:0xalice'),
             (SELECT address
              FROM address_names_current
              WHERE logical_name_id = 'ens:0xalice'
                AND relation = 'effective_controller')",
    )
    .fetch_one(pool)
    .await?)
}

async fn seed_basenames_project_fixture(pool: &PgPool) -> Result<()> {
    seed_lineage(pool, BASE_CHAIN, 4).await?;
    seed_lineage(pool, ETHEREUM_CHAIN, 4).await?;
    insert_namespaced_manifest(
        pool,
        "basenames",
        BASE_CHAIN,
        "basenames_base_resolver",
        1,
        "basenames_v1",
        "tests/project-basenames-base-resolver.toml",
        json!({
            "manifest_version":1,
            "namespace":"basenames",
            "source_family":"basenames_base_resolver",
            "chain":BASE_CHAIN,
            "deployment_epoch":"basenames_v1",
            "rollout_status":"active",
            "normalizer_version":NORMALIZER,
            "contracts":[{
                "role":"l2_resolver",
                "address":BASENAMES_RESOLVER,
                "proxy_kind":"none",
                "start_block":0
            }]
        }),
    )
    .await?;
    insert_namespaced_manifest(
        pool,
        "basenames",
        ETHEREUM_CHAIN,
        "basenames_execution",
        2,
        "basenames_v1",
        "tests/project-basenames-execution.toml",
        json!({
            "manifest_version":2,
            "namespace":"basenames",
            "source_family":"basenames_execution",
            "chain":ETHEREUM_CHAIN,
            "deployment_epoch":"basenames_v1",
            "rollout_status":"active",
            "normalizer_version":NORMALIZER,
            "capability_flags":{"verified_resolution":{"status":"supported"}},
            "contracts":[{
                "role":"l1_resolver",
                "address":BASENAMES_L1_RESOLVER,
                "proxy_kind":"none",
                "start_block":0
            }]
        }),
    )
    .await?;

    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(BASENAMES_RESOURCE)?)
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             'basenames:0xalice-base', 'basenames', 'alice.base.eth',
             ARRAY['alice','base','eth'], decode('00', 'hex'), '0xalice-base',
             ARRAY['0xalice-label','0xbase','0xeth'], $1, 'active', $2, $3, 1,
             'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'basenames:0xalice-base', $2, 'declared_registry_path',
                   to_timestamp(1), $3, $4, 1, 'canonical')",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::parse_str(BASENAMES_RESOURCE)?)
    .bind(BASE_CHAIN)
    .bind(block_hash(BASE_CHAIN, 1))
    .execute(pool)
    .await?;
    insert_namespaced_event(
        pool,
        "basenames",
        BASE_CHAIN,
        2,
        Some("basenames:0xalice-base"),
        Some(BASENAMES_RESOURCE),
        "ResolverChanged",
        "basenames_base_registry",
        1,
        json!({"resolver":BASENAMES_RESOLVER}),
        json!({}),
    )
    .await?;
    Ok(())
}

async fn extend_narrow_final_equivalence_fixture(pool: &PgPool) -> Result<()> {
    const S1_ADDRESS: &str = "0x0000000000000000000000000000000000000e01";
    const S2_ADDRESS: &str = "0x0000000000000000000000000000000000000e02";

    for (block, timestamp) in [(9, 7_776_008), (10, 7_776_009)] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, block))
        .bind(block_hash(CHAIN, block - 1))
        .bind(block)
        .bind(timestamp)
        .execute(pool)
        .await?;
    }

    insert_event(
        pool,
        CHAIN,
        9,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        json!({
            "resolver":EQUIVALENCE_TRANSFER_RESOLVER,
            "active":true,
            "alias_state":"active",
            "from_name":"transfer.eth",
            "from_namehash":"0xtransfer",
            "to_name":"bob.eth",
            "to_namehash":"0xbob",
            "to_logical_name_id":"ens:0xbob",
            "to_resource_id":EQUIVALENCE_BOB_RESOURCE
        }),
        json!({"emitting_address":EQUIVALENCE_TRANSFER_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        10,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "AliasChanged",
        "basenames_base_resolver",
        json!({
            "resolver":EQUIVALENCE_TRANSFER_RESOLVER,
            "active":false,
            "alias_state":"removed",
            "from_name":"transfer.eth",
            "from_namehash":"0xtransfer"
        }),
        json!({"emitting_address":EQUIVALENCE_TRANSFER_RESOLVER}),
    )
    .await?;

    for (block, before, after) in [(9, OWNER, TRANSFER_OWNER), (10, TRANSFER_OWNER, OWNER)] {
        insert_event(
            pool,
            CHAIN,
            block,
            Some("ens:0xalice"),
            Some(RESOURCE),
            "AuthorityTransferred",
            "ens_v1_registrar_l1",
            json!({"owner":after}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET before_state = jsonb_build_object('owner', lower($1))
             WHERE chain_id = $2 AND block_number = $3
               AND logical_name_id = 'ens:0xalice'
               AND event_kind = 'AuthorityTransferred'",
        )
        .bind(before)
        .bind(CHAIN)
        .bind(block)
        .execute(pool)
        .await?;
    }

    for (block, before, after) in [(9, S2_ADDRESS, S1_ADDRESS), (10, S1_ADDRESS, S2_ADDRESS)] {
        insert_event(
            pool,
            CHAIN,
            block,
            Some("ens:0xequivalence-parent"),
            None,
            "SubregistryChanged",
            "ens_v2_registry_l1",
            json!({"subregistry":after}),
            json!({}),
        )
        .await?;
        sqlx::query(
            "UPDATE normalized_events
             SET before_state = jsonb_build_object('subregistry', lower($1))
             WHERE chain_id = $2 AND block_number = $3
               AND logical_name_id = 'ens:0xequivalence-parent'
               AND event_kind = 'SubregistryChanged'",
        )
        .bind(before)
        .bind(CHAIN)
        .bind(block)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn extend_incremental_equivalence_fixture(pool: &PgPool) -> Result<()> {
    insert_manifest(
        pool,
        CHAIN,
        "ens_v2_resolver_l1",
        "tests/project-equivalence-v2-resolver.toml",
        json!({
            "resolver_implementations":[{
                "role":"permissioned_resolver",
                "address":EQUIVALENCE_V2_IMPLEMENTATION
            }]
        }),
    )
    .await?;
    for (block, timestamp) in [
        (4, 7_776_001),
        (5, 7_776_004),
        (6, 7_776_005),
        (7, 7_776_006),
        (8, 7_776_007),
    ] {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
        )
        .bind(CHAIN)
        .bind(block_hash(CHAIN, block))
        .bind(block_hash(CHAIN, block - 1))
        .bind(block)
        .bind(timestamp)
        .execute(pool)
        .await?;
    }

    seed_equivalence_subregistry_flip(pool).await?;

    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_BOB_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xbob', 'ens', 'bob.eth', ARRAY['bob', 'eth'],
             decode('00', 'hex'), '0xbob', ARRAY['0xbob-label', '0xeth'], $1,
             'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens:0xbob', $2, 'declared_registry_path',
             to_timestamp(1), $3, $4, 1, 'canonical'
         )",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_BOB_BINDING)?)
    .bind(Uuid::parse_str(EQUIVALENCE_BOB_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES (
             '0xbob-label', convert_to('bob', 'UTF8'), 'bob', $1,
             true, 'fixture', 1
         )",
    )
    .bind(NORMALIZER)
    .execute(pool)
    .await?;

    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xbob",
            "labelhash":"0xbob-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":OWNER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        json!({"authority_kind":"registrar","registrant":OWNER,"status":"registered"}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":OWNER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:url",
            "record_family":"text",
            "selector_key":"url",
            "value_retained":true,
            "value":"https://bob.example.test"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;

    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RecordChanged",
        "ens_v1_resolver_l1",
        json!({
            "resolver":RESOLVER,
            "record_key":"text:url",
            "record_family":"text",
            "selector_key":"url",
            "value_retained":true,
            "value":"https://equivalence.example.test"
        }),
        json!({"emitting_address":RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_wrapper_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":[
                "resource_control", "resolver_control", "set_resolver", "set_ttl",
                "create_subnames", "transfer", "unwrap", "burn_fuses", "approve"
            ],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({"source_event":"NameRenewed","authority_kind":"wrapper","expiry":7776003}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196608,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;

    insert_event(
        pool,
        CHAIN,
        4,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v2_registry_l1",
        json!({"resolver":EQUIVALENCE_V2_RESOLVER}),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events SET before_state = jsonb_build_object('resolver', lower($1))
         WHERE chain_id = $2 AND block_number = 4
           AND logical_name_id = 'ens:0xalice' AND event_kind = 'ResolverChanged'",
    )
    .bind(RESOLVER)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AliasChanged",
        "ens_v2_resolver_l1",
        json!({
            "resolver":EQUIVALENCE_V2_RESOLVER,
            "active":true,
            "alias_state":"active",
            "from_name":"alice.eth",
            "from_namehash":"0xalice",
            "to_name":"bob.eth",
            "to_namehash":"0xbob",
            "to_logical_name_id":"ens:0xbob",
            "to_resource_id":EQUIVALENCE_BOB_RESOURCE
        }),
        json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        json!({
            "proxy_address":EQUIVALENCE_V2_RESOLVER,
            "implementation":EQUIVALENCE_V2_IMPLEMENTATION
        }),
        json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":"0x00000000000000000000000000000000000000a7"}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xeth"),
        None,
        "PreimageObserved",
        "ens_v1_registry_l1",
        json!({"labelhash":"0xeth","raw_labels_hex":["657468"]}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        None,
        None,
        "ReverseChanged",
        "ens_v1_reverse_l1",
        json!({
            "address":OWNER,
            "coin_type":"60",
            "namespace":"ens",
            "claim_provenance":{"source_family":"ens_v1_reverse_l1"}
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        None,
        None,
        "RecordChanged",
        "ens_v1_reverse_l1",
        json!({
            "raw_name":"alice.eth",
            "primary_claim_source":{
                "address":OWNER,
                "coin_type":"60",
                "namespace":"ens",
                "claim_provenance":{"source_family":"ens_v1_reverse_l1"}
            }
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":OWNER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":"0x00000000000000000000000000000000000000a7"}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        Some("ens:0xeth"),
        None,
        "PreimageObserved",
        "ens_v1_registry_l1",
        json!({"labelhash":"0xeth","raw_labels_hex":["657468"]}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        None,
        None,
        "ReverseChanged",
        "ens_v1_reverse_l1",
        json!({
            "address":OWNER,
            "coin_type":"60",
            "namespace":"ens",
            "claim_provenance":{"source_family":"ens_v1_reverse_l1"}
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        None,
        None,
        "RecordChanged",
        "ens_v1_reverse_l1",
        json!({
            "raw_name":"alice.eth",
            "primary_claim_source":{
                "address":OWNER,
                "coin_type":"60",
                "namespace":"ens",
                "claim_provenance":{"source_family":"ens_v1_reverse_l1"}
            }
        }),
        json!({}),
    )
    .await?;
    for logical_name_id in [
        "ens:0xequivalence-parent",
        "ens:0xequivalence-c0",
        "ens:0xequivalence-c1",
    ] {
        insert_event(
            pool,
            CHAIN,
            8,
            Some(logical_name_id),
            None,
            "PreimageObserved",
            "ens_v2_registry_l1",
            json!({"raw_labels_hex":[]}),
            json!({}),
        )
        .await?;
    }
    Ok(())
}

async fn extend_wrapper_resolver_summary_fixture(pool: &PgPool) -> Result<()> {
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{
                "kind":"resolver",
                "chain_id":CHAIN,
                "resolver_address":EQUIVALENCE_V2_RESOLVER
            },
            "effective_powers":["approve","resolver_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RecordChanged",
        "ens_v2_resolver_l1",
        json!({
            "resolver":EQUIVALENCE_V2_RESOLVER,
            "record_key":"text:boundary",
            "record_family":"text",
            "selector_key":"boundary",
            "value_retained":true,
            "value":"before-expiry"
        }),
        json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        json!({
            "proxy_address":EQUIVALENCE_V2_RESOLVER,
            "implementation":EQUIVALENCE_V2_IMPLEMENTATION
        }),
        json!({"emitting_address":EQUIVALENCE_V2_RESOLVER}),
    )
    .await?;

    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "PermissionChanged",
        "ens_v1_wrapper_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["approve","resolver_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "ExpiryChanged",
        "ens_v1_registrar_l1",
        json!({
            "source_event":"NameRenewed",
            "authority_kind":"wrapper",
            "expiry":7_776_003
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xbob"),
        Some(EQUIVALENCE_BOB_RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":196_608,"wrapper_state":"emancipated"}),
        json!({}),
    )
    .await?;
    Ok(())
}

async fn extend_permission_only_resolver_fixture(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(PERMISSION_ONLY_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES (
             'ens:0xpermission-only', 'ens', 'permission-only.eth',
             ARRAY['permission-only', 'eth'], decode('00', 'hex'),
             '0xpermission-only', ARRAY['0xpermission-only-label', '0xeth'], $1,
             'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens:0xpermission-only', $2, 'declared_registry_path',
             to_timestamp(1), $3, $4, 1, 'canonical'
         )",
    )
    .bind(Uuid::parse_str(PERMISSION_ONLY_BINDING)?)
    .bind(Uuid::parse_str(PERMISSION_ONLY_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xpermission-only"),
        Some(PERMISSION_ONLY_RESOURCE),
        "ResolverChanged",
        "ens_v2_registry_l1",
        json!({"resolver":PERMISSION_ONLY_RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xpermission-only"),
        Some(PERMISSION_ONLY_RESOURCE),
        "RecordChanged",
        "ens_v2_resolver_l1",
        json!({
            "resolver":PERMISSION_ONLY_RESOLVER,
            "record_key":"text:permission-only",
            "record_family":"text",
            "selector_key":"permission-only",
            "value_retained":true,
            "value":"outside-boundary-scope"
        }),
        json!({"emitting_address":PERMISSION_ONLY_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        json!({
            "proxy_address":PERMISSION_ONLY_RESOLVER,
            "implementation":EQUIVALENCE_V2_IMPLEMENTATION
        }),
        json!({"emitting_address":PERMISSION_ONLY_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{
                "kind":"resolver",
                "chain_id":CHAIN,
                "resolver_address":PERMISSION_ONLY_RESOLVER
            },
            "effective_powers":["approve","resolver_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    Ok(())
}

async fn extend_equivalence_registrar_transfer_fixture(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "INSERT INTO resources (
             resource_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_TRANSFER_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xtransfer', 'ens', 'transfer.eth', ARRAY['transfer', 'eth'],
             decode('00', 'hex'), '0xtransfer', ARRAY['0xtransfer-label', '0xeth'],
             $1, 'active', $2, $3, 1, 'canonical'
         )",
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens:0xtransfer', $2, 'declared_registry_path',
             to_timestamp(1), $3, $4, 1, 'canonical'
         )",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_TRANSFER_BINDING)?)
    .bind(Uuid::parse_str(EQUIVALENCE_TRANSFER_RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    insert_manifest(
        pool,
        CHAIN,
        "basenames_base_resolver",
        "tests/project-equivalence-basenames-resolver.toml",
        json!({
            "contracts":[{
                "role":"l2_resolver",
                "address":EQUIVALENCE_TRANSFER_RESOLVER,
                "proxy_kind":"none"
            }]
        }),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "RegistrationGranted",
        "basenames_base_registrar",
        json!({
            "authority_kind":"registrar",
            "registrant":OWNER,
            "status":"registered"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "AuthorityTransferred",
        "basenames_base_registrar",
        json!({"owner":OWNER}),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "PermissionChanged",
        "basenames_base_registrar",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "ResolverChanged",
        "basenames_base_registry",
        json!({"resolver":EQUIVALENCE_TRANSFER_RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "RecordChanged",
        "basenames_base_resolver",
        json!({
            "resolver":EQUIVALENCE_TRANSFER_RESOLVER,
            "record_key":"text:url",
            "record_family":"text",
            "selector_key":"url",
            "value_retained":true,
            "value":"https://transfer.example.test"
        }),
        json!({"emitting_address":EQUIVALENCE_TRANSFER_RESOLVER}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "PermissionChanged",
        "basenames_base_registry",
        json!({
            "subject":OWNER,
            "scope":{
                "kind":"resolver",
                "chain_id":CHAIN,
                "resolver_address":EQUIVALENCE_TRANSFER_RESOLVER
            },
            "effective_powers":["resolver_control"],
            "grant_source":{
                "kind":"ens_v1_authority",
                "authority_kind":"registrar",
                "authority_key":"equivalence-transfer",
                "source_event_kind":"ResolverChanged"
            },
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":REGISTRY}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "AuthorityTransferred",
        "basenames_base_registrar",
        json!({"owner":TRANSFER_OWNER}),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        8,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "AuthorityTransferred",
        "basenames_base_registrar",
        json!({"owner":TRANSFER_OWNER}),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "PermissionChanged",
        "basenames_base_registrar",
        json!({
            "subject":OWNER,
            "scope":{
                "kind":"resolver",
                "chain_id":CHAIN,
                "resolver_address":EQUIVALENCE_TRANSFER_RESOLVER
            },
            "effective_powers":[],
            "grant_source":null,
            "revocation_source":{
                "kind":"ens_v1_authority",
                "authority_kind":"registrar",
                "authority_key":"equivalence-transfer",
                "source_event_kind":"TokenControlTransferred"
            },
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET before_state = jsonb_build_object(
             'subject', $1::text,
             'scope', jsonb_build_object(
                 'kind', 'resolver', 'chain_id', $2::text,
                 'resolver_address', lower($3::text)
             ),
             'effective_powers', jsonb_build_array('resolver_control'),
             'grant_source', jsonb_build_object(
                 'kind', 'ens_v1_authority',
                 'authority_kind', 'registrar',
                 'authority_key', 'equivalence-transfer',
                 'source_event_kind', 'TokenControlTransferred'
             ),
             'revocation_source', NULL,
             'inheritance_path', '[]'::jsonb,
             'transfer_behavior', 'replace_on_authority_change'
         )
         WHERE chain_id = $2 AND block_number = 7
           AND logical_name_id = 'ens:0xtransfer'
           AND event_kind = 'PermissionChanged'
           AND after_state ->> 'subject' = $1",
    )
    .bind(OWNER)
    .bind(CHAIN)
    .bind(EQUIVALENCE_TRANSFER_RESOLVER)
    .execute(pool)
    .await?;
    insert_event(
        pool,
        CHAIN,
        7,
        Some("ens:0xtransfer"),
        Some(EQUIVALENCE_TRANSFER_RESOURCE),
        "PermissionChanged",
        "basenames_base_registrar",
        json!({
            "subject":TRANSFER_OWNER,
            "scope":{
                "kind":"resolver",
                "chain_id":CHAIN,
                "resolver_address":EQUIVALENCE_TRANSFER_RESOLVER
            },
            "effective_powers":["resolver_control"],
            "grant_source":{
                "kind":"ens_v1_authority",
                "authority_kind":"registrar",
                "authority_key":"equivalence-transfer",
                "source_event_kind":"TokenControlTransferred"
            },
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({"emitting_address":REGISTRAR}),
    )
    .await?;
    Ok(())
}

async fn seed_equivalence_subregistry_flip(pool: &PgPool) -> Result<()> {
    const S1_ADDRESS: &str = "0x0000000000000000000000000000000000000e01";
    const S2_ADDRESS: &str = "0x0000000000000000000000000000000000000e02";
    let s1 = Uuid::parse_str("00000000-0000-0000-0000-000000000e01")?;
    let s2 = Uuid::parse_str("00000000-0000-0000-0000-000000000e02")?;

    for (instance, address) in [(s1, S1_ADDRESS), (s2, S2_ADDRESS)] {
        sqlx::query(
            "INSERT INTO contract_instances (
                 contract_instance_id, chain_id, contract_kind
             ) VALUES ($1, $2, 'contract')",
        )
        .bind(instance)
        .bind(CHAIN)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO contract_instance_addresses (
                 contract_instance_id, chain_id, address, active_from_block_number
             ) VALUES ($1, $2, $3, 0)",
        )
        .bind(instance)
        .bind(CHAIN)
        .bind(address)
        .execute(pool)
        .await?;
    }
    for (logical_name_id, raw_name, raw_labels, namehash, labelhashes) in [
        (
            "ens:0xequivalence-parent",
            "equivalence.eth",
            vec!["equivalence", "eth"],
            "0xequivalence-parent",
            vec!["0xequivalence-parent-label", "0xeth"],
        ),
        (
            "ens:0xequivalence-c0",
            "c0.equivalence.eth",
            vec!["c0", "equivalence", "eth"],
            "0xequivalence-c0",
            vec![
                "0xequivalence-c0-label",
                "0xequivalence-parent-label",
                "0xeth",
            ],
        ),
        (
            "ens:0xequivalence-c1",
            "c1.equivalence.eth",
            vec!["c1", "equivalence", "eth"],
            "0xequivalence-c1",
            vec![
                "0xequivalence-c1-label",
                "0xequivalence-parent-label",
                "0xeth",
            ],
        ),
    ] {
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES ($1, 'ens', $2, $3, decode('00', 'hex'), $4, $5, $6,
                       'active', $7, $8, 1, 'canonical')",
        )
        .bind(logical_name_id)
        .bind(raw_name)
        .bind(raw_labels)
        .bind(namehash)
        .bind(labelhashes)
        .bind(NORMALIZER)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             $1, 'ens:0xequivalence-parent', $2, 'declared_registry_path',
             to_timestamp(7776002), $3, $4, 1, 'canonical'
         )",
    )
    .bind(Uuid::parse_str(EQUIVALENCE_PARENT_BINDING)?)
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, source_kind, source_priority
         ) VALUES
             ('0xequivalence-parent-label', convert_to('equivalence', 'UTF8'), 'equivalence', $1,
              true, 'fixture', 1),
             ('0xequivalence-c0-label', convert_to('c0', 'UTF8'), 'c0', $1,
              true, 'fixture', 1),
             ('0xequivalence-c1-label', convert_to('c1', 'UTF8'), 'c1', $1,
              true, 'fixture', 1)",
    )
    .bind(NORMALIZER)
    .execute(pool)
    .await?;

    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xequivalence-parent"),
        Some(RESOURCE),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xequivalence-parent",
            "labelhash":"0xequivalence-parent-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xequivalence-parent"),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":S1_ADDRESS}),
        json!({}),
    )
    .await?;
    for (block, logical_name_id, instance) in [
        (1, "ens:0xequivalence-c0", s1),
        (2, "ens:0xequivalence-c1", s2),
    ] {
        insert_event(
            pool,
            CHAIN,
            block,
            Some(logical_name_id),
            None,
            "RegistrationGranted",
            "ens_v2_registry_l1",
            json!({
                "registry_contract_instance_id":instance,
                "registrant":OWNER
            }),
            json!({}),
        )
        .await?;
    }
    insert_event(
        pool,
        CHAIN,
        4,
        Some("ens:0xequivalence-parent"),
        None,
        "SubregistryChanged",
        "ens_v2_registry_l1",
        json!({"subregistry":S2_ADDRESS}),
        json!({}),
    )
    .await?;
    sqlx::query(
        "UPDATE normalized_events
         SET before_state = jsonb_build_object('subregistry', lower($1))
         WHERE chain_id = $2 AND block_number = 4
           AND logical_name_id = 'ens:0xequivalence-parent'
           AND event_kind = 'SubregistryChanged'",
    )
    .bind(S1_ADDRESS)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_project_fixture(pool: &PgPool) -> Result<()> {
    seed_lineage(pool, CHAIN, 3).await?;
    let registrar_manifest = insert_manifest(
        pool,
        CHAIN,
        "ens_v1_registrar_l1",
        "tests/project-registrar.toml",
        json!({"contracts":[]}),
    )
    .await?;
    let resolver_manifest = insert_manifest(
        pool,
        CHAIN,
        "ens_v1_resolver_l1",
        "tests/project-resolver.toml",
        json!({"contracts":[{"role":"public_resolver","address":RESOLVER,"proxy_kind":"none"}]}),
    )
    .await?;
    let resolver_instance = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
         VALUES ($1, $2, 'contract')",
    )
    .bind(resolver_instance)
    .bind(CHAIN)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind
         ) VALUES ($1, $2, 'contract', 'public_resolver', $3, $4,
                   'public_resolver', 'none')",
    )
    .bind(resolver_manifest)
    .bind(CHAIN)
    .bind(resolver_instance)
    .bind(RESOLVER)
    .execute(pool)
    .await?;
    let _ = registrar_manifest;

    let token_lineage_id = Uuid::parse_str(TOKEN_LINEAGE)?;
    sqlx::query(
        "INSERT INTO token_lineages (
             token_lineage_id, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, $2, $3, 1, 'canonical')",
    )
    .bind(token_lineage_id)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, token_lineage_id, chain_id, block_hash, block_number,
             canonicality_state
         ) VALUES ($1, $2, $3, $4, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(token_lineage_id)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;
    for (logical, raw_name, raw_labels, namehash, labelhashes, visibility) in [
        (
            "ens:0xeth",
            "eth",
            vec!["eth"],
            "0xeth",
            vec!["0xeth"],
            "active",
        ),
        (
            "ens:0xalice",
            "alice.eth",
            vec!["alice", "eth"],
            "0xalice",
            vec!["0xalice-label", "0xeth"],
            "active",
        ),
    ] {
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version,
                 visibility_state, chain_id, block_hash, block_number,
                 canonicality_state
             ) VALUES ($1, 'ens', $2, $3, decode('00', 'hex'), $4, $5, $6, $7,
                       $8, $9, 1, 'canonical')",
        )
        .bind(logical)
        .bind(raw_name)
        .bind(raw_labels)
        .bind(namehash)
        .bind(labelhashes)
        .bind(NORMALIZER)
        .bind(visibility)
        .bind(CHAIN)
        .bind(block_hash(CHAIN, 1))
        .execute(pool)
        .await?;
    }
    sqlx::query(
        r#"INSERT INTO name_surfaces (
             logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version,
             visibility_state, normalization_errors, deactivation_reason,
             deactivated_at, chain_id, block_hash, block_number, canonicality_state
         ) VALUES (
             'ens:0xhostile', 'ens', '', ARRAY[]::text[], decode('00', 'hex'), '0xhostile',
             ARRAY[]::text[], $1, 'shadow', '[{"error":"hostile"}]',
             'normalization_gate', now(), $2, $3, 3, 'canonical'
         )"#,
    )
    .bind(NORMALIZER)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 3))
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO label_preimages (
             labelhash, raw_label, decoded_label, normalizer_version,
             normalized_under_version, normalization_error, source_kind, source_priority
         ) VALUES
             ('0xalice-label', convert_to('alice', 'UTF8'), 'alice', $1, true, NULL, 'fixture', 1),
             ('0xhostile-label', decode('ff00', 'hex'), NULL, $1, false, 'hostile', 'fixture', 1)",
    )
    .bind(NORMALIZER)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind,
             active_from, chain_id, block_hash, block_number, canonicality_state
         ) VALUES ($1, 'ens:0xalice', $2, 'declared_registry_path',
                   to_timestamp(1), $3, $4, 1, 'canonical')",
    )
    .bind(Uuid::parse_str(SURFACE_BINDING)?)
    .bind(Uuid::parse_str(RESOURCE)?)
    .bind(CHAIN)
    .bind(block_hash(CHAIN, 1))
    .execute(pool)
    .await?;

    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xalice",
            "labelhash":"0xalice-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "RegistrationGranted",
        "ens_v1_registrar_l1",
        json!({"authority_kind":"registrar","registrant":OWNER,"status":"registered"}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "AuthorityTransferred",
        "ens_v1_registrar_l1",
        json!({"owner":OWNER}),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        1,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionChanged",
        "ens_v1_registrar_l1",
        json!({
            "subject":OWNER,
            "scope":{"kind":"resource"},
            "effective_powers":["resource_control"],
            "grant_source":{"kind":"fixture"},
            "revocation_source":null,
            "inheritance_path":[],
            "transfer_behavior":"replace_on_authority_change"
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        2,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "ResolverChanged",
        "ens_v1_registry_l1",
        json!({"resolver":RESOLVER}),
        json!({}),
    )
    .await?;
    insert_event(pool, CHAIN, 2, Some("ens:0xalice"), Some(RESOURCE), "RecordChanged", "ens_v1_resolver_l1", json!({"resolver":RESOLVER,"record_key":"text:url","record_family":"text","selector_key":"url","value_retained":true,"value":"https://example.test"}), json!({"emitting_address":RESOLVER})).await?;
    insert_event(pool, CHAIN, 3, None, None, "ReverseChanged", "ens_v1_reverse_l1", json!({"address":OWNER,"coin_type":"60","namespace":"ens","claim_provenance":{"source_family":"ens_v1_reverse_l1"}}), json!({})).await?;
    insert_event(pool, CHAIN, 3, None, None, "RecordChanged", "ens_v1_reverse_l1", json!({"raw_name":"alice.eth","primary_claim_source":{"address":OWNER,"coin_type":"60","namespace":"ens","claim_provenance":{"source_family":"ens_v1_reverse_l1"}}}), json!({})).await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xhostile"),
        None,
        "SubregistryChanged",
        "ens_v1_registry_l1",
        json!({
            "node":"0xeth",
            "child_node":"0xhostile",
            "labelhash":"0xhostile-label",
            "owner":OWNER
        }),
        json!({}),
    )
    .await?;
    insert_event(
        pool,
        CHAIN,
        3,
        Some("ens:0xhostile"),
        None,
        "PreimageObserved",
        "ens_v1_wrapper_l1",
        json!({"raw_labels_hex":["ff00","657468"]}),
        json!({}),
    )
    .await?;
    Ok(())
}

async fn seed_reconvergence_fixture(pool: &PgPool, chain: &str) -> Result<i64> {
    seed_lineage(pool, chain, 1).await?;
    let resolver_manifest = insert_manifest(
        pool,
        chain,
        "ens_v2_resolver_l1",
        "tests/project-v2-resolver.toml",
        json!({"resolver_implementations":[]}),
    )
    .await?;
    insert_event(
        pool,
        chain,
        1,
        None,
        None,
        "Upgraded",
        "ens_v2_resolver_l1",
        json!({
            "proxy_address":"0x00000000000000000000000000000000000000d1",
            "implementation":"0x00000000000000000000000000000000000000c1"
        }),
        json!({"emitting_address":"0x00000000000000000000000000000000000000d1"}),
    )
    .await?;
    insert_event(
        pool,
        chain,
        1,
        None,
        None,
        "Upgraded",
        "ens_v2_registry_l1",
        json!({
            "proxy_address":"0x00000000000000000000000000000000000000e1",
            "implementation":"0x00000000000000000000000000000000000000e2"
        }),
        json!({"emitting_address":"0x00000000000000000000000000000000000000e1"}),
    )
    .await?;
    Ok(resolver_manifest)
}

async fn seed_raw_registrar_transfer_fixture(
    pool: &PgPool,
    chain: &str,
    include_registry_owner: bool,
) -> Result<()> {
    // BaseRegistrar inherits this ERC-721 transfer path, which updates token ownership and emits
    // Transfer; registry ownership changes through the separate reclaim call.
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L24 @ basenames@1809bbc)
    // (upstream: .refs/basenames/lib/solady/src/tokens/ERC721.sol:L287 @ basenames@1809bbc)
    // (upstream: .refs/basenames/lib/solady/src/tokens/ERC721.sol:L307 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L321 @ basenames@1809bbc)
    // (upstream: .refs/basenames/src/L2/BaseRegistrar.sol:L329 @ basenames@1809bbc)
    seed_lineage(pool, chain, 5).await?;
    insert_declared_source_manifest_events(
        pool,
        "basenames",
        chain,
        "basenames_base_registrar",
        "registrar",
        REGISTRAR,
        &[
            (
                "NameRegistered",
                "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
                &["registrar"],
                &[
                    "RegistrationGranted",
                    "ExpiryChanged",
                    "PermissionChanged",
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                    "PreimageObserved",
                ],
            ),
            (
                "Transfer",
                "event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)",
                &["registrar"],
                &[
                    "TokenControlTransferred",
                    "PermissionChanged",
                    "SurfaceUnbound",
                    "SurfaceBound",
                    "AuthorityEpochChanged",
                ],
            ),
        ],
    )
    .await?;
    insert_declared_source_manifest_events(
        pool,
        "basenames",
        chain,
        "basenames_base_registry",
        "registry",
        REGISTRY,
        &[
            (
                "NewOwner",
                "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
                &["registry"],
                &[
                    "SubregistryChanged",
                    "AuthorityTransferred",
                    "PermissionChanged",
                ],
            ),
            (
                "NewResolver",
                "event NewResolver(bytes32 indexed node, address resolver)",
                &["registry"],
                &["ResolverChanged", "PermissionChanged"],
            ),
        ],
    )
    .await?;
    insert_declared_source_manifest_events(
        pool,
        "basenames",
        chain,
        "basenames_base_resolver",
        "l2_resolver",
        RESOLVER,
        &[(
            "TextChanged",
            "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
            &[],
            &["RecordChanged"],
        )],
    )
    .await?;

    let base_eth_node = raw_namehash(&[b"base", b"eth"]);
    let alice_node = raw_namehash(&[b"alice", b"base", b"eth"]);
    let alice_label = B256::from(keccak256(b"alice"));
    if include_registry_owner {
        let registry_owner = NewOwner {
            node: base_eth_node,
            label: alice_label,
            owner: OWNER.parse::<Address>()?,
        }
        .encode_log_data();
        insert_raw_event(
            pool,
            chain,
            1,
            REGISTRY,
            registry_owner.topics(),
            registry_owner.data.as_ref(),
        )
        .await?;
    }
    let registration = NameRegistered {
        name: "alice".into(),
        label: alice_label,
        owner: OWNER.parse::<Address>()?,
        expires: U256::from(4_000_000_000_u64),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        2,
        REGISTRAR,
        registration.topics(),
        registration.data.as_ref(),
    )
    .await?;
    let resolver = NewResolver {
        node: alice_node,
        resolver: RESOLVER.parse::<Address>()?,
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        3,
        REGISTRY,
        resolver.topics(),
        resolver.data.as_ref(),
    )
    .await?;
    let record = TextChanged {
        node: alice_node,
        indexedKey: keccak256(b"url"),
        key: "url".into(),
        value: "https://resolver-transfer.example.test".into(),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        4,
        RESOLVER,
        record.topics(),
        record.data.as_ref(),
    )
    .await?;
    let transfer = Transfer {
        from: OWNER.parse::<Address>()?,
        to: TRANSFER_OWNER.parse::<Address>()?,
        tokenId: U256::from_be_bytes(alice_label.0),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        5,
        REGISTRAR,
        transfer.topics(),
        transfer.data.as_ref(),
    )
    .await?;
    Ok(())
}

async fn seed_raw_registration_fixture(pool: &PgPool, chain: &str) -> Result<()> {
    seed_lineage(pool, chain, 5).await?;
    insert_declared_source_manifest(
        pool,
        chain,
        "ens_v1_wrapper_l1",
        "name_wrapper",
        WRAPPER,
        "NameWrapped",
        "event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry)",
        &["name_wrapper"],
        &[
            "TokenControlTransferred",
            "ExpiryChanged",
            "PermissionScopeChanged",
            "SurfaceUnbound",
            "SurfaceBound",
            "AuthorityEpochChanged",
            "ResolverChanged",
            "PreimageObserved",
        ],
    )
    .await?;
    insert_declared_source_manifest(
        pool,
        chain,
        "ens_v1_registrar_l1",
        "registrar",
        REGISTRAR,
        "NameRegistered",
        "event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 expires)",
        &["registrar"],
        &[
            "RegistrationGranted",
            "ExpiryChanged",
            "PermissionChanged",
            "SurfaceUnbound",
            "SurfaceBound",
            "AuthorityEpochChanged",
            "ResolverChanged",
            "PreimageObserved",
        ],
    )
    .await?;
    insert_declared_source_manifest(
        pool,
        chain,
        "ens_v1_registry_l1",
        "registry",
        REGISTRY,
        "NewResolver",
        "event NewResolver(bytes32 indexed node, address resolver)",
        &["registry"],
        &["ResolverChanged", "PermissionChanged"],
    )
    .await?;
    insert_declared_source_manifest(
        pool,
        chain,
        "ens_v1_resolver_l1",
        "public_resolver",
        RESOLVER,
        "TextChanged",
        "event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value)",
        &[],
        &["RecordChanged"],
    )
    .await?;
    insert_declared_source_manifest(
        pool,
        chain,
        "ens_v1_reverse_l1",
        "reverse_registrar",
        REVERSE_REGISTRAR,
        "ReverseClaimed",
        "event ReverseClaimed(address indexed addr, bytes32 indexed node)",
        &["reverse_registrar"],
        &["ReverseChanged"],
    )
    .await?;

    let eth_node = raw_namehash(&[b"eth"]);
    let alice_node = raw_namehash(&[b"alice", b"eth"]);
    let wrapped = NameWrapped {
        node: eth_node,
        name: b"\x03eth\0".to_vec().into(),
        owner: OWNER.parse::<Address>()?,
        fuses: 0,
        expiry: 4_000_000_000,
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        1,
        WRAPPER,
        wrapped.topics(),
        wrapped.data.as_ref(),
    )
    .await?;

    let registration = NameRegistered {
        name: "alice".into(),
        label: B256::from(keccak256(b"alice")),
        owner: OWNER.parse::<Address>()?,
        expires: U256::from(4_000_000_000u64),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        2,
        REGISTRAR,
        registration.topics(),
        registration.data.as_ref(),
    )
    .await?;

    let child = NewOwner {
        node: eth_node,
        label: B256::from(keccak256(b"alice")),
        owner: OWNER.parse::<Address>()?,
    }
    .encode_log_data();
    insert_raw_event_at(
        pool,
        chain,
        2,
        1,
        1,
        REGISTRY,
        child.topics(),
        child.data.as_ref(),
    )
    .await?;

    let resolver = NewResolver {
        node: alice_node,
        resolver: RESOLVER.parse::<Address>()?,
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        3,
        REGISTRY,
        resolver.topics(),
        resolver.data.as_ref(),
    )
    .await?;

    let record = TextChanged {
        node: alice_node,
        indexedKey: keccak256(b"url"),
        key: "url".into(),
        value: "https://example.test".into(),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        4,
        RESOLVER,
        record.topics(),
        record.data.as_ref(),
    )
    .await?;

    let reverse_label = OWNER.trim_start_matches("0x").as_bytes();
    let reverse = ReverseClaimed {
        addr: OWNER.parse::<Address>()?,
        node: raw_namehash(&[reverse_label, b"addr", b"reverse"]),
    }
    .encode_log_data();
    insert_raw_event(
        pool,
        chain,
        5,
        REVERSE_REGISTRAR,
        reverse.topics(),
        reverse.data.as_ref(),
    )
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_declared_source_manifest_events(
    pool: &PgPool,
    namespace: &str,
    chain: &str,
    source_family: &str,
    role: &str,
    address: &str,
    events: &[(&str, &str, &[&str], &[&str])],
) -> Result<()> {
    let abi_events: Vec<Value> = events
        .iter()
        .map(|(name, fragment, emitter_roles, normalized_events)| {
            json!({
                "name": name,
                "fragment": fragment,
                "emitter_roles": emitter_roles,
                "normalized_events": normalized_events
            })
        })
        .collect();
    let payload = json!({
        "manifest_version": 1,
        "namespace": namespace,
        "source_family": source_family,
        "chain": chain,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": role,
            "address": address,
            "proxy_kind": "none",
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": {"events": abi_events, "calls": []}
    });
    let manifest = insert_namespaced_manifest(
        pool,
        namespace,
        chain,
        source_family,
        1,
        "fixture",
        &format!("tests/raw-{source_family}.toml"),
        payload,
    )
    .await?;
    let instance = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(instance)
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind,
             start_block_number
         ) VALUES ($1, $2, 'contract', $3, $4, $5, $3, 'none', 0)",
    )
    .bind(manifest)
    .bind(chain)
    .bind(role)
    .bind(instance)
    .bind(address)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id
         ) VALUES ($1, $2, $3, 0, $4)",
    )
    .bind(instance)
    .bind(chain)
    .bind(address)
    .bind(manifest)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_declared_source_manifest(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    role: &str,
    address: &str,
    event_name: &str,
    event_fragment: &str,
    emitter_roles: &[&str],
    normalized_events: &[&str],
) -> Result<()> {
    let mut abi_events = vec![json!({
        "name": event_name,
        "fragment": event_fragment,
        "emitter_roles": emitter_roles,
        "normalized_events": normalized_events
    })];
    if source_family == "ens_v1_registry_l1" {
        abi_events.push(json!({
            "name": "NewOwner",
            "fragment": "event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner)",
            "emitter_roles": ["registry"],
            "normalized_events": ["SubregistryChanged", "AuthorityTransferred"]
        }));
    }
    let payload = json!({
        "manifest_version": 1,
        "namespace": "ens",
        "source_family": source_family,
        "chain": chain,
        "deployment_epoch": "fixture",
        "rollout_status": "active",
        "normalizer_version": NORMALIZER,
        "capability_flags": {},
        "roots": [],
        "contracts": [{
            "role": role,
            "address": address,
            "proxy_kind": "none",
            "start_block": 0
        }],
        "discovery_rules": [],
        "abi": {"events": abi_events, "calls": []}
    });
    let manifest = insert_manifest(
        pool,
        chain,
        source_family,
        &format!("tests/raw-{source_family}.toml"),
        payload,
    )
    .await?;
    let instance = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO contract_instances (
             contract_instance_id, chain_id, contract_kind
         ) VALUES ($1, $2, 'contract')",
    )
    .bind(instance)
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO manifest_contract_instances (
             manifest_id, chain_id, declaration_kind, declaration_name,
             contract_instance_id, declared_address, role, proxy_kind,
             start_block_number
         ) VALUES ($1, $2, 'contract', $3, $4, $5, $3, 'none', 0)",
    )
    .bind(manifest)
    .bind(chain)
    .bind(role)
    .bind(instance)
    .bind(address)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO contract_instance_addresses (
             contract_instance_id, chain_id, address, active_from_block_number,
             source_manifest_id
         ) VALUES ($1, $2, $3, 0, $4)",
    )
    .bind(instance)
    .bind(chain)
    .bind(address)
    .bind(manifest)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_raw_event(
    pool: &PgPool,
    chain: &str,
    block_number: i64,
    emitting_address: &str,
    topics: &[B256],
    data: &[u8],
) -> Result<()> {
    insert_raw_event_at(
        pool,
        chain,
        block_number,
        0,
        0,
        emitting_address,
        topics,
        data,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_raw_event_at(
    pool: &PgPool,
    chain: &str,
    block_number: i64,
    transaction_index: i64,
    log_index: i64,
    emitting_address: &str,
    topics: &[B256],
    data: &[u8],
) -> Result<()> {
    let transaction_hash = format!("{chain}-transaction-{block_number}-{transaction_index}");
    sqlx::query(
        "INSERT INTO raw_transactions (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, from_address, to_address
         ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(chain)
    .bind(block_hash(chain, block_number))
    .bind(block_number)
    .bind(&transaction_hash)
    .bind(transaction_index)
    .bind(SENDER)
    .bind(emitting_address)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO raw_logs (
             chain_id, block_hash, block_number, transaction_hash,
             transaction_index, log_index, emitting_address, topics, data
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(chain)
    .bind(block_hash(chain, block_number))
    .bind(block_number)
    .bind(transaction_hash)
    .bind(transaction_index)
    .bind(log_index)
    .bind(emitting_address)
    .bind(
        topics
            .iter()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>(),
    )
    .bind(data)
    .execute(pool)
    .await?;
    Ok(())
}

fn raw_namehash(labels: &[&[u8]]) -> B256 {
    let mut node = B256::ZERO;
    for label in labels.iter().rev() {
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(node.as_slice());
        input[32..].copy_from_slice(keccak256(label).as_slice());
        node = keccak256(input);
    }
    node
}

async fn seed_lineage(pool: &PgPool, chain: &str, through: i64) -> Result<()> {
    for number in 0..=through {
        sqlx::query(
            "INSERT INTO chain_lineage (
                 chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state
             ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
        )
        .bind(chain)
        .bind(block_hash(chain, number))
        .bind((number > 0).then(|| block_hash(chain, number - 1)))
        .bind(number)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn insert_lineage_block(pool: &PgPool, chain: &str, number: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
    )
    .bind(chain)
    .bind(block_hash(chain, number))
    .bind((number > 0).then(|| block_hash(chain, number - 1)))
    .bind(number)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_completed_project_extent(pool: &PgPool, chain: &str, head: i64) -> Result<()> {
    let hash = block_hash(chain, head);
    sqlx::query(
        "UPDATE chain_phase_state
         SET phase_status = 'completed',
             current_block_number = $2,
             current_block_hash = $3,
             target_block_number = $2,
             target_block_hash = $3,
             live_handoff_block_number = CASE
                 WHEN phase_name = 'ingest' THEN $2
             END,
             live_handoff_block_hash = CASE
                 WHEN phase_name = 'ingest' THEN $3
             END,
             input_content_hash = CASE
                 WHEN phase_name IN ('interpret', 'project') THEN $4
             END,
             started_at = now(),
             finished_at = now()
         WHERE chain_id = $1
           AND phase_name IN ('ingest', 'interpret', 'project')",
    )
    .bind(chain)
    .bind(head)
    .bind(&hash)
    .bind(INTERPRETER_CONTENT_HASH)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO ingest_cursors (
             chain_id, source_key, source_kind, seed_basis, start_block_number,
             next_block_number, target_block_number, last_processed_block_number,
             last_processed_block_hash
         ) VALUES ($1, 'source', 'test', 'new_signature_range', 0,
                   $2, $3, $3, $4)",
    )
    .bind(chain)
    .bind(head.saturating_add(1))
    .bind(head)
    .bind(hash)
    .execute(pool)
    .await?;
    Ok(())
}

fn chain_config(chain: &str) -> phase_runner::error::RunnerResult<ChainConfig> {
    ChainConfig::new(
        chain,
        vec![SourceConfig::new(
            chain,
            "source",
            "test",
            SeedBasis::NewSignatureRange,
            0,
            "http://source.invalid",
        )?],
        false,
    )
}

fn test_timing() -> TimingConfig {
    TimingConfig {
        initial_backoff: Duration::from_millis(1),
        maximum_backoff: Duration::from_millis(4),
        live_poll_interval: Duration::from_millis(1),
    }
}

async fn insert_manifest(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    file_path: &str,
    payload: Value,
) -> Result<i64> {
    let manifest_id = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version,
             file_path, manifest_payload
         ) VALUES (1, 'ens', $1, $2, 'fixture', 'active', $3, $4, $5)
         RETURNING manifest_id",
    )
    .bind(source_family)
    .bind(chain)
    .bind(NORMALIZER)
    .bind(file_path)
    .bind(&payload)
    .fetch_one(pool)
    .await?;
    insert_manifest_update_event(pool, chain, source_family, manifest_id, payload).await?;
    Ok(manifest_id)
}

#[allow(clippy::too_many_arguments)]
async fn insert_namespaced_manifest(
    pool: &PgPool,
    namespace: &str,
    chain: &str,
    source_family: &str,
    manifest_version: i64,
    deployment_label: &str,
    file_path: &str,
    payload: Value,
) -> Result<i64> {
    let manifest_id = sqlx::query_scalar(
        "INSERT INTO manifest_versions (
             manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version,
             file_path, manifest_payload
         ) VALUES ($1, $2, $3, $4, $5, 'active', $6, $7, $8)
         RETURNING manifest_id",
    )
    .bind(manifest_version)
    .bind(namespace)
    .bind(source_family)
    .bind(chain)
    .bind(deployment_label)
    .bind(NORMALIZER)
    .bind(file_path)
    .bind(&payload)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, raw_fact_ref, derivation_kind,
             canonicality_state, before_state, after_state
         ) VALUES ($1, $2, 'SourceManifestUpdated', $3, $4, $5, $6, $7,
                   'manifest_sync', 'finalized', '{}'::jsonb, $8)",
    )
    .bind(format!("{chain}:SourceManifestUpdated:{}", Uuid::new_v4()))
    .bind(namespace)
    .bind(source_family)
    .bind(manifest_version)
    .bind(manifest_id)
    .bind(chain)
    .bind(json!({
        "manifest_id": manifest_id,
        "namespace": namespace,
        "source_family": source_family,
        "chain": chain,
        "deployment_epoch": deployment_label
    }))
    .bind(json!({
        "manifest_version": manifest_version,
        "normalizer_version": NORMALIZER,
        "rollout_status": "active",
        "manifest_payload": payload
    }))
    .execute(pool)
    .await?;
    Ok(manifest_id)
}

async fn insert_manifest_update_event(
    pool: &PgPool,
    chain: &str,
    source_family: &str,
    manifest_id: i64,
    manifest_payload: Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             source_manifest_id, chain_id, raw_fact_ref, derivation_kind,
             canonicality_state, before_state, after_state
         ) VALUES ($1, 'ens', 'SourceManifestUpdated', $2, 1, $3, $4, $5,
                   'manifest_sync', 'finalized', '{}'::jsonb, $6)",
    )
    .bind(format!("{chain}:SourceManifestUpdated:{}", Uuid::new_v4()))
    .bind(source_family)
    .bind(manifest_id)
    .bind(chain)
    .bind(json!({
        "manifest_id": manifest_id,
        "namespace": "ens",
        "source_family": source_family,
        "chain": chain,
        "deployment_epoch": "fixture"
    }))
    .bind(json!({
        "manifest_version": 1,
        "normalizer_version": NORMALIZER,
        "rollout_status": "active",
        "manifest_payload": manifest_payload
    }))
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_event(
    pool: &PgPool,
    chain: &str,
    ordinal: i64,
    logical_name_id: Option<&str>,
    resource_id: Option<&str>,
    event_kind: &str,
    source_family: &str,
    after_state: Value,
    raw_fact_ref: Value,
) -> Result<()> {
    insert_namespaced_event(
        pool,
        "ens",
        chain,
        ordinal,
        logical_name_id,
        resource_id,
        event_kind,
        source_family,
        1,
        after_state,
        raw_fact_ref,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_namespaced_event(
    pool: &PgPool,
    namespace: &str,
    chain: &str,
    ordinal: i64,
    logical_name_id: Option<&str>,
    resource_id: Option<&str>,
    event_kind: &str,
    source_family: &str,
    manifest_version: i64,
    after_state: Value,
    raw_fact_ref: Value,
) -> Result<()> {
    let blockless = event_kind == "SourceManifestUpdated";
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, chain_id, block_number, block_hash,
             raw_fact_ref, derivation_kind, canonicality_state, before_state, after_state
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                   $12, $13::canonicality_state, '{}'::jsonb, $14)",
    )
    .bind(format!("{chain}:{event_kind}:{ordinal}:{}", Uuid::new_v4()))
    .bind(namespace)
    .bind(logical_name_id)
    .bind(resource_id.map(Uuid::parse_str).transpose()?)
    .bind(event_kind)
    .bind(source_family)
    .bind(manifest_version)
    .bind(chain)
    .bind((!blockless).then_some(ordinal))
    .bind((!blockless).then(|| block_hash(chain, ordinal)))
    .bind(raw_fact_ref)
    .bind(if blockless {
        "manifest_sync"
    } else {
        "ens_v1_unwrapped_authority"
    })
    .bind(if blockless { "finalized" } else { "canonical" })
    .bind(after_state)
    .execute(pool)
    .await?;
    Ok(())
}

fn block_hash(chain: &str, number: i64) -> String {
    format!("{chain}-block-{number}")
}
