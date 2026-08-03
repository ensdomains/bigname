#[allow(dead_code)]
mod support;

use std::{sync::Arc, time::Duration};

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use anyhow::Result;
use bigname_interpret::{
    BatchRequest as InterpretRequest, Engine as InterpretEngine, RunMode as InterpretRunMode,
};
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_storage::{NameCurrentRow, SurfaceBindingKind, resolution_verified_support_boundary};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    capacity::CapacityGuard,
    config::{CapacityConfig, ChainConfig, SeedBasis, SourceConfig, TimingConfig},
    phase::{BlockRange, LoopbackPhase, PhaseName, PhaseSet},
    project_phase::ProjectPhase,
    runner::{PhaseRunner, RedoPhase},
    state::PhaseStore,
};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::Uuid};
use tokio_util::sync::CancellationToken;

use support::ScratchDatabase;

const CHAIN: &str = "project-fixture";
const BASE_CHAIN: &str = "base-mainnet";
const ETHEREUM_CHAIN: &str = "ethereum-mainnet";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";
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
const BASENAMES_RESOURCE: &str = "00000000-0000-0000-0000-000000000031";

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
}

#[tokio::test]
async fn canonical_fixture_builds_all_seven_projection_families() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_all_builders").await?;
    seed_project_fixture(scratch.pool()).await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;

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

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
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
async fn wrapper_fuses_mask_resource_control_permissions() -> Result<()> {
    let scratch = ScratchDatabase::create("production_project_fuse_mask").await?;
    seed_project_fixture(scratch.pool()).await?;
    insert_event(
        scratch.pool(),
        CHAIN,
        3,
        Some("ens:0xalice"),
        Some(RESOURCE),
        "PermissionScopeChanged",
        "ens_v1_wrapper_l1",
        json!({"fuses":8}),
        json!({}),
    )
    .await?;

    run_project(scratch.pool(), CHAIN, None, RunMode::Normal, 0, 3).await?;
    let permission_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM permissions_current WHERE resource_id = $1")
            .bind(Uuid::parse_str(RESOURCE)?)
            .fetch_one(scratch.pool())
            .await?;
    assert_eq!(permission_count, 0);
    let support: String = sqlx::query_scalar(
        "SELECT support_status FROM permissions_current_resource_summary
         WHERE resource_id = $1",
    )
    .bind(Uuid::parse_str(RESOURCE)?)
    .fetch_one(scratch.pool())
    .await?;
    assert_eq!(support, "supported");
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

    let token_lineage_id = Uuid::new_v4();
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
    .bind(Uuid::new_v4())
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
