use std::{
    collections::{BTreeSet, HashMap},
    path::PathBuf,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use bigname_manifests::{
    load_active_manifest_abi_events_by_chain_and_source_families, load_repository, sync_repository,
};
use bigname_storage::{
    CanonicalityState, RawBlock, RawLog, default_database_url, load_normalized_events_by_namespace,
    upsert_raw_blocks, upsert_raw_logs,
};
use serde_json::{Value, json};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
    types::{Uuid, time::OffsetDateTime},
};

use crate::adapter_manifest::ActiveManifestEventTopic0sBySignature;
use crate::ens_v2_common::normalize_address;
use crate::evm_abi::keccak_signature_hex;

use super::constants::*;
use super::decode::build_permissions_observation;
use super::hints::{
    fallback_resource_hint, observed_dns_encoded_name, resolver_resource_hint,
    validated_persisted_selector_fields,
};
use super::normalized::{
    RoleVocabulary, permission_changed_event, permission_resource_id, role_bitmap_powers,
};
use super::types::{PermissionsObservation, PermissionsRawLogRow};
use super::util::hex_string;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    admin_pool: PgPool,
    pool: PgPool,
    database_name: String,
}

impl TestDatabase {
    async fn new() -> Result<Self> {
        let database_url = std::env::var("BIGNAME_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| default_database_url().to_owned());
        let base_options = PgConnectOptions::from_str(&database_url)
            .context("failed to parse database URL for ENSv2 permissions tests")?;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_nanos();
        let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let database_name = format!(
            "bn_ad_ensv2_perm_{}_{}_{}",
            std::process::id(),
            unique,
            sequence
        );

        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(base_options.clone().database("postgres"))
            .await
            .context("failed to connect admin pool for ENSv2 permissions tests")?;
        sqlx::query(&format!(r#"CREATE DATABASE "{}""#, database_name))
            .execute(&admin_pool)
            .await
            .with_context(|| format!("failed to create test database {database_name}"))?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(base_options.database(&database_name))
            .await
            .context("failed to connect test pool for ENSv2 permissions tests")?;
        bigname_storage::MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply migrations for ENSv2 permissions tests")?;

        Ok(Self {
            admin_pool,
            pool,
            database_name,
        })
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn cleanup(self) -> Result<()> {
        self.pool.close().await;
        sqlx::query(&format!(
            r#"DROP DATABASE IF EXISTS "{}" WITH (FORCE)"#,
            self.database_name
        ))
        .execute(&self.admin_pool)
        .await
        .with_context(|| format!("failed to drop test database {}", self.database_name))?;
        self.admin_pool.close().await;
        Ok(())
    }
}

#[test]
fn decodes_named_text_resource_observation() -> Result<()> {
    let resource = topic_word(0x42);
    let key_hash = topic_word(0xab);
    let name = dns_name("Alice", "ETH");
    let key = b"avatar";
    let raw_log = raw_log(
        vec![
            keccak_signature_hex("NamedTextResource(uint256,bytes,bytes32,string)"),
            resource.clone(),
            key_hash.clone(),
        ],
        encode_two_dynamic(&name, key),
    );

    let event_topics = test_permissions_event_topics();
    let observation = build_permissions_observation(&raw_log, &event_topics)?;

    match observation {
        Some(PermissionsObservation::NamedTextResource {
            resource: decoded_resource,
            name: decoded_name,
            key_hash: decoded_key_hash,
            key: decoded_key,
        }) => {
            assert_eq!(decoded_resource, resource);
            assert_eq!(decoded_name, name);
            assert_eq!(decoded_key_hash, key_hash);
            assert_eq!(decoded_key, "avatar");
        }
        _ => panic!("expected NamedTextResource observation"),
    }
    Ok(())
}

#[test]
fn persisted_hint_matching_covers_each_named_resource_selector() {
    let resource = topic_word(0x42);
    let name = dns_name("Alice", "ETH");
    let key_hash = topic_word(0xab);
    let cases = [
        (
            PermissionsObservation::NamedResource {
                resource: resource.clone(),
                name: name.clone(),
            },
            "name",
            None,
            None,
        ),
        (
            PermissionsObservation::NamedTextResource {
                resource: resource.clone(),
                name: name.clone(),
                key_hash: key_hash.clone(),
                key: "avatar".to_owned(),
            },
            "text",
            Some("avatar"),
            Some(key_hash.as_str()),
        ),
        (
            PermissionsObservation::NamedAddrResource {
                resource: resource.clone(),
                name: name.clone(),
                coin_type: "60".to_owned(),
            },
            "addr",
            Some("60"),
            None,
        ),
    ];

    for (observation, selector_kind, selector_key, selector_hash) in cases {
        assert_eq!(
            observed_dns_encoded_name(
                &observation,
                &resource,
                selector_kind,
                selector_key,
                selector_hash,
            )
            .as_deref(),
            Some(name.as_slice())
        );
    }
}

#[test]
fn persisted_selector_fields_require_exact_null_and_string_shapes() {
    assert_eq!(
        validated_persisted_selector_fields(
            &json!({"selector_key": null, "selector_hash": null}),
            "name",
        ),
        Some((None, None))
    );
    assert_eq!(
        validated_persisted_selector_fields(
            &json!({"selector_key": "avatar", "selector_hash": topic_word(0xab)}),
            "text",
        ),
        Some((Some("avatar".to_owned()), Some(topic_word(0xab))))
    );
    assert_eq!(
        validated_persisted_selector_fields(
            &json!({"selector_key": "60", "selector_hash": null}),
            "addr",
        ),
        Some((Some("60".to_owned()), None))
    );

    for malformed in [
        json!({"selector_hash": null}),
        json!({"selector_key": true, "selector_hash": null}),
        json!({"selector_key": null}),
        json!({"selector_key": null, "selector_hash": false}),
    ] {
        assert_eq!(
            validated_persisted_selector_fields(&malformed, "name"),
            None
        );
    }
    for malformed in [
        json!({"selector_key": "60"}),
        json!({"selector_key": "60", "selector_hash": false}),
    ] {
        assert_eq!(
            validated_persisted_selector_fields(&malformed, "addr"),
            None
        );
    }
}

#[test]
fn builds_resource_hints_for_observed_and_fallback_resources() -> Result<()> {
    let raw_log = raw_log(Vec::new(), Vec::new());
    let resource = topic_word(0x33);
    let name = dns_name("Alice", "ETH");

    let hint = resolver_resource_hint(
        &raw_log,
        resource.clone(),
        name.clone(),
        "addr",
        Some("60".to_owned()),
        None,
    )?;

    assert_eq!(hint.upstream_resource, resource);
    assert_eq!(hint.logical_name_id.as_deref(), Some("ens:alice.eth"));
    assert_eq!(hint.normalized_name.as_deref(), Some("alice.eth"));
    assert_eq!(hint.dns_encoded_name.as_deref(), Some(name.as_slice()));
    assert_eq!(hint.selector_kind, "addr");
    assert_eq!(hint.selector_key.as_deref(), Some("60"));

    let invalid_name = dns_name("Ni\u{200d}ck", "eth");
    let invalid_hint = resolver_resource_hint(
        &raw_log,
        resource.clone(),
        invalid_name.clone(),
        "addr",
        Some("60".to_owned()),
        None,
    )?;
    assert_eq!(invalid_hint.upstream_resource, resource);
    assert_eq!(invalid_hint.logical_name_id, None);
    assert_eq!(invalid_hint.normalized_name, None);
    assert_eq!(
        invalid_hint.dns_encoded_name.as_deref(),
        Some(invalid_name.as_slice())
    );

    let root = "0x0000000000000000000000000000000000000000000000000000000000000000";
    let fallback = fallback_resource_hint(&raw_log, root.to_owned(), true);
    assert_eq!(fallback.logical_name_id, None);
    assert_eq!(fallback.selector_kind, "root");

    Ok(())
}

#[test]
fn builds_permission_changed_event_payload() -> Result<()> {
    let account = "0x1111111111111111111111111111111111111111";
    let resource = topic_word(0x33);
    let name = dns_name("alice", "eth");
    let raw_log = raw_log(
        vec![
            keccak_signature_hex("EACRolesChanged(uint256,address,uint256,uint256)"),
            resource.clone(),
            address_topic(account),
        ],
        [hex_word(0), bitmap_with_last_byte(0x11)]
            .into_iter()
            .flatten()
            .collect(),
    );
    let hint =
        resolver_resource_hint(&raw_log, resource.clone(), name.clone(), "name", None, None)?;
    let resource_id = Uuid::parse_str("11111111-1111-5111-8111-111111111111")?;
    let event = permission_changed_event(
        &raw_log,
        &hint,
        resource_id,
        account.to_owned(),
        topic_word(0),
        bitmap_hex_with_last_byte(0x11),
    )?;

    assert_eq!(
        event.event_identity,
        format!("ens_v2_permissions:42:0xblock:0xtx:9:{EVENT_KIND_PERMISSION_CHANGED}:{resource}")
    );
    assert_eq!(event.namespace, "ens");
    assert_eq!(event.logical_name_id.as_deref(), Some("ens:alice.eth"));
    assert_eq!(event.resource_id, Some(resource_id));
    assert_eq!(event.event_kind, EVENT_KIND_PERMISSION_CHANGED);
    assert_eq!(event.source_family, "ens_v2_resolver_l1");
    assert_eq!(event.manifest_version, 7);
    assert_eq!(event.source_manifest_id, Some(42));
    assert_eq!(event.chain_id.as_deref(), Some("sepolia-dev"));
    assert_eq!(event.block_number, Some(123));
    assert_eq!(event.block_hash.as_deref(), Some("0xblock"));
    assert_eq!(event.transaction_hash.as_deref(), Some("0xtx"));
    assert_eq!(event.log_index, Some(9));
    assert_eq!(event.derivation_kind, "ens_v2_permissions");
    assert_eq!(event.canonicality_state, CanonicalityState::Canonical);
    assert_eq!(
        event.raw_fact_ref,
        json!({
            "kind": "raw_log",
            "chain_id": "sepolia-dev",
            "block_hash": "0xblock",
            "block_number": 123,
            "transaction_hash": "0xtx",
            "transaction_index": 4,
            "log_index": 9,
            "emitting_address": "0x2222222222222222222222222222222222222222",
        })
    );
    assert_eq!(
        event.before_state,
        json!({
            "subject": account,
            "role_bitmap": topic_word(0),
            "effective_powers": [],
        })
    );
    assert_eq!(event.after_state["subject"], json!(account));
    assert_eq!(
        event.after_state["scope"],
        json!({
            "kind": "resolver",
            "chain_id": "sepolia-dev",
            "resolver_address": "0x2222222222222222222222222222222222222222",
        })
    );
    assert_eq!(
        event.after_state["effective_powers"],
        json!(["set_addr", "set_text"])
    );
    assert_eq!(
        event.after_state["grant_source"]["source_event"],
        "EACRolesChanged"
    );
    assert_eq!(
        event.after_state["grant_source"]["upstream_resource"],
        resource
    );
    assert_eq!(event.after_state["grant_source"]["root_resource"], false);
    assert_eq!(
        event.after_state["grant_source"]["changed_powers"],
        json!(["set_addr", "set_text"])
    );
    assert_eq!(event.after_state["revocation_source"], json!(null));
    assert_eq!(event.after_state["inheritance_path"], json!([]));
    assert_eq!(
        event.after_state["selector"],
        json!({
            "kind": "name",
            "key": null,
            "hash": null,
            "normalized_name": "alice.eth",
            "dns_encoded_name": format!("0x{}", hex_string(&name)),
        })
    );

    Ok(())
}

#[test]
fn ens_v2_registry_permission_resource_id_matches_legacy_golden() -> Result<()> {
    let raw_log = raw_log_with_source_family(SOURCE_FAMILY_ENS_V2_REGISTRY_L1, vec![], vec![]);

    assert_eq!(
        permission_resource_id(
            &raw_log.chain_id,
            raw_log.emitting_contract_instance_id,
            &topic_word(0),
            true,
        ),
        Uuid::parse_str("9dc2aecc-e987-52e2-b6c7-823eb71231bc")?
    );

    Ok(())
}

#[test]
fn registry_root_resource_builds_root_permission_changed_event_payload() -> Result<()> {
    let account = "0x1111111111111111111111111111111111111111";
    let root_resource = topic_word(0);
    let raw_log = raw_log_with_source_family(
        "ens_v2_registry_l1",
        vec![
            keccak_signature_hex("EACRolesChanged(uint256,address,uint256,uint256)"),
            root_resource.clone(),
            address_topic(account),
        ],
        [hex_word(0), bitmap_with_last_byte(0x11)]
            .into_iter()
            .flatten()
            .collect(),
    );
    let hint = fallback_resource_hint(&raw_log, root_resource.clone(), true);
    let resource_id = Uuid::parse_str("11111111-1111-5111-8111-111111111111")?;
    let event = permission_changed_event(
        &raw_log,
        &hint,
        resource_id,
        account.to_owned(),
        topic_word(0),
        bitmap_hex_with_last_byte(0x11),
    )?;

    assert_eq!(event.event_kind, "RootPermissionChanged");
    assert_eq!(
        event.event_identity,
        format!("ens_v2_permissions:42:0xblock:0xtx:9:RootPermissionChanged:{root_resource}")
    );
    assert_eq!(
        event.after_state["scope"],
        json!({
            "kind": "registry_root",
            "chain_id": "sepolia-dev",
            "registry_address": "0x2222222222222222222222222222222222222222",
        })
    );
    assert_eq!(
        event.after_state["grant_source"]["registry_contract_instance_id"],
        raw_log.emitting_contract_instance_id.to_string()
    );
    assert_eq!(event.after_state["grant_source"]["root_resource"], true);
    assert_eq!(
        event.after_state["effective_powers"],
        json!(["registrar", "register_reserved"])
    );
    assert_eq!(
        event.after_state["grant_source"]["changed_powers"],
        json!(["registrar", "register_reserved"])
    );
    assert_eq!(
        event.after_state["inheritance_path"][0]["kind"],
        "registry_root_fallback"
    );

    Ok(())
}

#[test]
fn root_source_root_resource_uses_registry_role_vocabulary() -> Result<()> {
    let account = "0x1111111111111111111111111111111111111111";
    let root_resource = topic_word(0);
    let raw_log = raw_log_with_source_family(
        SOURCE_FAMILY_ENS_V2_ROOT_L1,
        vec![
            keccak_signature_hex("EACRolesChanged(uint256,address,uint256,uint256)"),
            root_resource.clone(),
            address_topic(account),
        ],
        [hex_word(0), bitmap_with_last_byte(0x11)]
            .into_iter()
            .flatten()
            .collect(),
    );
    let hint = fallback_resource_hint(&raw_log, root_resource, true);
    let resource_id = Uuid::parse_str("11111111-1111-5111-8111-111111111111")?;
    let event = permission_changed_event(
        &raw_log,
        &hint,
        resource_id,
        account.to_owned(),
        topic_word(0),
        bitmap_hex_with_last_byte(0x11),
    )?;

    assert_eq!(event.event_kind, EVENT_KIND_ROOT_PERMISSION_CHANGED);
    assert_eq!(
        event.after_state["effective_powers"],
        json!(["registrar", "register_reserved"])
    );
    assert_eq!(
        event.after_state["grant_source"]["changed_powers"],
        json!(["registrar", "register_reserved"])
    );

    Ok(())
}

#[test]
fn reserved_marker_and_unknown_role_bits_are_not_published_as_powers() -> Result<()> {
    let account = "0x1111111111111111111111111111111111111111";
    let resource = topic_word(0);
    let registry_raw_log = raw_log_with_source_family(
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        vec![
            keccak_signature_hex("EACRolesChanged(uint256,address,uint256,uint256)"),
            resource.clone(),
            address_topic(account),
        ],
        [hex_word(0), bitmap_with_bits(&[32])]
            .into_iter()
            .flatten()
            .collect(),
    );
    let registry_hint = fallback_resource_hint(&registry_raw_log, resource.clone(), true);
    let resolver_raw_log = raw_log(
        vec![
            keccak_signature_hex("EACRolesChanged(uint256,address,uint256,uint256)"),
            resource.clone(),
            address_topic(account),
        ],
        [hex_word(0), bitmap_with_bits(&[40])]
            .into_iter()
            .flatten()
            .collect(),
    );
    let resolver_hint = fallback_resource_hint(&resolver_raw_log, resource, false);
    let resource_id = Uuid::parse_str("11111111-1111-5111-8111-111111111111")?;

    let registry_event = permission_changed_event(
        &registry_raw_log,
        &registry_hint,
        resource_id,
        account.to_owned(),
        topic_word(0),
        bitmap_hex_with_bits(&[32]),
    )?;
    let resolver_event = permission_changed_event(
        &resolver_raw_log,
        &resolver_hint,
        resource_id,
        account.to_owned(),
        topic_word(0),
        bitmap_hex_with_bits(&[40]),
    )?;

    assert_eq!(registry_event.after_state["effective_powers"], json!([]));
    assert_eq!(registry_event.after_state["grant_source"], json!({}));
    assert_eq!(resolver_event.after_state["effective_powers"], json!([]));
    assert_eq!(resolver_event.after_state["grant_source"], json!({}));

    Ok(())
}

#[test]
fn post_audit_registry_and_resolver_roles_publish_named_powers() -> Result<()> {
    assert_eq!(
        role_bitmap_powers(
            &bitmap_hex_with_bits(&[36, 120, 164, 248]),
            RoleVocabulary::Registry,
        )?,
        vec!["set_uri", "can_name", "admin_set_uri", "admin_can_name"]
    );
    assert_eq!(
        role_bitmap_powers(
            &bitmap_hex_with_bits(&[36, 120, 164, 248]),
            RoleVocabulary::Resolver,
        )?,
        vec!["set_data", "can_name", "admin_set_data", "admin_can_name"]
    );

    Ok(())
}

#[tokio::test]
async fn sync_ens_v2_permissions_consumes_registry_root_eac_roles_changed() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let block_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let root_address = "0x11b5bfbe9078d826b1edbdd1cfc12f5828d9f50c";
    let registry_address = "0x67b728a792e789a8978b30cf1b3b641f19354b43";
    let account = "0x1111111111111111111111111111111111111111";

    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;
    assert_registry_and_root_eac_roles_changed_are_admitted(database.pool(), chain).await?;

    upsert_raw_blocks(
        database.pool(),
        &[permissions_test_raw_block(chain, block_hash, 11_163_500)],
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 11_163_500,
                transaction_hash: "0xtx420".to_owned(),
                transaction_index: 0,
                log_index: 0,
                emitting_address: normalize_address(root_address),
                topics: vec![
                    keccak_signature_hex(ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE),
                    topic_word(0),
                    address_topic(account),
                ],
                data: [hex_word(0), bitmap_with_last_byte(0x11)]
                    .into_iter()
                    .flatten()
                    .collect(),
                canonicality_state: CanonicalityState::Finalized,
            },
            RawLog {
                chain_id: chain.to_owned(),
                block_hash: block_hash.to_owned(),
                block_number: 11_163_500,
                transaction_hash: "0xtx420".to_owned(),
                transaction_index: 0,
                log_index: 1,
                emitting_address: normalize_address(registry_address),
                topics: vec![
                    keccak_signature_hex(ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE),
                    topic_word(0),
                    address_topic(account),
                ],
                data: [hex_word(0), bitmap_with_last_byte(0x11)]
                    .into_iter()
                    .flatten()
                    .collect(),
                canonicality_state: CanonicalityState::Finalized,
            },
        ],
    )
    .await?;

    let summary = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        &[block_hash.to_owned()],
    )
    .await?;

    assert_eq!(summary.scanned_log_count, 2);
    assert_eq!(summary.matched_log_count, 2);
    assert_eq!(summary.total_synced_count, 2);
    assert_eq!(
        summary
            .by_kind
            .get(EVENT_KIND_ROOT_PERMISSION_CHANGED)
            .map(|entry| entry.synced_count),
        Some(2)
    );

    let events = load_normalized_events_by_namespace(database.pool(), "ens").await?;
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .all(|event| event.event_kind == EVENT_KIND_ROOT_PERMISSION_CHANGED)
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.source_family.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            SOURCE_FAMILY_ENS_V2_ROOT_L1,
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1,
        ])
    );
    assert!(
        events
            .iter()
            .all(|event| event.after_state["scope"]["kind"] == "registry_root")
    );
    assert!(events.iter().all(|event| {
        event.after_state["registry_contract_instance_id"]
            .as_str()
            .is_some()
    }));
    assert!(events.iter().all(|event| {
        event.after_state["effective_powers"] == json!(["registrar", "register_reserved"])
    }));
    assert!(events.iter().all(|event| {
        event.after_state["grant_source"]["changed_powers"]
            == json!(["registrar", "register_reserved"])
    }));

    database.cleanup().await
}

#[tokio::test]
async fn sync_ens_v2_permissions_reuses_persisted_resolver_hints_across_calls() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let resolver_address = "0x0000000000000000000000000000000000000260";
    let account = "0x1111111111111111111111111111111111111111";
    let first_block = 11_163_600;
    let block_hashes = (first_block..first_block + 14)
        .map(permission_test_block_hash)
        .collect::<Vec<_>>();
    let unadmitted_block = first_block - 1;
    let unadmitted_block_hash = permission_test_block_hash(unadmitted_block);
    let resources = [
        topic_word(0x101),
        topic_word(0x102),
        topic_word(0x103),
        topic_word(0x104),
        topic_word(0x105),
        topic_word(0x106),
        topic_word(0x107),
    ];
    let name = dns_name("Alice", "ETH");
    let unadmitted_name = dns_name("aLiCe", "eTH");
    let key_hash = topic_word(0xab);

    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;
    insert_test_discovered_resolver(
        database.pool(),
        chain,
        resolver_address,
        first_block,
        &block_hashes[0],
    )
    .await?;

    let blocks = block_hashes
        .iter()
        .enumerate()
        .map(|(offset, block_hash)| {
            permissions_fork_raw_block(
                chain,
                block_hash,
                Some(if offset == 0 {
                    unadmitted_block_hash.as_str()
                } else {
                    block_hashes[offset - 1].as_str()
                }),
                first_block + offset as i64,
                CanonicalityState::Finalized,
            )
        })
        .collect::<Vec<_>>();
    upsert_raw_blocks(
        database.pool(),
        &[
            vec![permissions_test_raw_block(
                chain,
                &unadmitted_block_hash,
                unadmitted_block,
            )],
            blocks,
        ]
        .concat(),
    )
    .await?;
    upsert_raw_logs(
        database.pool(),
        &[
            named_text_permission_raw_log(
                chain,
                &unadmitted_block_hash,
                unadmitted_block,
                resolver_address,
                &resources[3],
                &unadmitted_name,
                &key_hash,
                "avatar",
            ),
            named_text_permission_raw_log(
                chain,
                &block_hashes[0],
                first_block,
                resolver_address,
                &resources[0],
                &name,
                &key_hash,
                "avatar",
            ),
            eac_roles_changed_permission_raw_log(
                chain,
                &block_hashes[1],
                first_block + 1,
                resolver_address,
                &resources[0],
                account,
            ),
            named_text_permission_raw_log(
                chain,
                &block_hashes[2],
                first_block + 2,
                resolver_address,
                &resources[1],
                &name,
                &key_hash,
                "avatar",
            ),
            eac_roles_changed_permission_raw_log(
                chain,
                &block_hashes[3],
                first_block + 3,
                resolver_address,
                &resources[1],
                account,
            ),
            named_text_permission_raw_log(
                chain,
                &block_hashes[4],
                first_block + 4,
                resolver_address,
                &resources[2],
                &name,
                &key_hash,
                "avatar",
            ),
            eac_roles_changed_permission_raw_log(
                chain,
                &block_hashes[5],
                first_block + 5,
                resolver_address,
                &resources[2],
                account,
            ),
            named_text_permission_raw_log(
                chain,
                &block_hashes[6],
                first_block + 6,
                resolver_address,
                &resources[3],
                &name,
                &key_hash,
                "avatar",
            ),
            eac_roles_changed_permission_raw_log(
                chain,
                &block_hashes[7],
                first_block + 7,
                resolver_address,
                &resources[3],
                account,
            ),
            named_text_permission_raw_log(
                chain,
                &block_hashes[8],
                first_block + 8,
                resolver_address,
                &resources[4],
                &name,
                &key_hash,
                "avatar",
            ),
            eac_roles_changed_permission_raw_log(
                chain,
                &block_hashes[9],
                first_block + 9,
                resolver_address,
                &resources[4],
                account,
            ),
            named_text_permission_raw_log(
                chain,
                &block_hashes[10],
                first_block + 10,
                resolver_address,
                &resources[5],
                &name,
                &key_hash,
                "avatar",
            ),
            eac_roles_changed_permission_raw_log(
                chain,
                &block_hashes[11],
                first_block + 11,
                resolver_address,
                &resources[5],
                account,
            ),
            named_permission_raw_log(
                chain,
                &block_hashes[12],
                first_block + 12,
                resolver_address,
                &resources[6],
                &name,
            ),
            eac_roles_changed_permission_raw_log(
                chain,
                &block_hashes[13],
                first_block + 13,
                resolver_address,
                &resources[6],
                account,
            ),
        ],
    )
    .await?;

    let preimages = crate::sync_block_derived_normalized_events(
        database.pool(),
        chain,
        &block_hashes[..10],
        None,
    )
    .await?;
    assert_eq!(preimages.matched_log_count, resources.len() - 2);
    assert_eq!(preimages.total_synced_count, resources.len() - 2);

    let baseline = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        &block_hashes[..2],
    )
    .await?;
    assert_eq!(baseline.matched_log_count, 2);
    assert_eq!(baseline.total_synced_count, 1);
    let in_memory_selector =
        permission_selector_for_upstream_resource(database.pool(), &resources[0]).await?;

    let persisted_named = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[2]),
    )
    .await?;
    assert_eq!(persisted_named.matched_log_count, 1);
    assert_eq!(persisted_named.total_synced_count, 0);
    let persisted_eac = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[3]),
    )
    .await?;
    assert_eq!(persisted_eac.matched_log_count, 1);
    assert_eq!(persisted_eac.total_synced_count, 1);
    assert_eq!(
        permission_selector_for_upstream_resource(database.pool(), &resources[1]).await?,
        in_memory_selector
    );

    let legacy_named = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[4]),
    )
    .await?;
    assert_eq!(legacy_named.matched_log_count, 1);
    assert_eq!(legacy_named.total_synced_count, 0);
    let legacy_update = sqlx::query(
        r#"
        UPDATE resources
        SET provenance = provenance - 'dns_encoded_name'
        WHERE provenance ->> 'adapter' = 'ens_v2_permissions'
          AND provenance ->> 'upstream_resource' = $1
        "#,
    )
    .bind(&resources[2])
    .execute(database.pool())
    .await?;
    assert_eq!(legacy_update.rows_affected(), 1);

    let legacy_eac = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[5]),
    )
    .await?;
    assert_eq!(legacy_eac.matched_log_count, 1);
    assert_eq!(legacy_eac.total_synced_count, 1);
    assert_eq!(
        permission_selector_for_upstream_resource(database.pool(), &resources[2]).await?,
        in_memory_selector
    );

    let compacted_named = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[6]),
    )
    .await?;
    assert_eq!(compacted_named.matched_log_count, 1);
    assert_eq!(compacted_named.total_synced_count, 0);
    let compacted_legacy_update = sqlx::query(
        r#"
        UPDATE resources
        SET provenance = provenance - 'dns_encoded_name'
        WHERE provenance ->> 'adapter' = 'ens_v2_permissions'
          AND provenance ->> 'upstream_resource' = $1
        "#,
    )
    .bind(&resources[3])
    .execute(database.pool())
    .await?;
    assert_eq!(compacted_legacy_update.rows_affected(), 1);
    let compacted_raw_delete = sqlx::query(
        r#"
        DELETE FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(chain)
    .bind(&block_hashes[6])
    .execute(database.pool())
    .await?;
    assert_eq!(compacted_raw_delete.rows_affected(), 1);

    let compacted_eac = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[7]),
    )
    .await?;
    assert_eq!(compacted_eac.matched_log_count, 1);
    assert_eq!(compacted_eac.total_synced_count, 1);
    assert_eq!(
        permission_selector_for_upstream_resource(database.pool(), &resources[3]).await?,
        in_memory_selector
    );

    let missing_named = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[8]),
    )
    .await?;
    assert_eq!(missing_named.matched_log_count, 1);
    assert_eq!(missing_named.total_synced_count, 0);
    let missing_legacy_update = sqlx::query(
        r#"
        UPDATE resources
        SET provenance = provenance - 'dns_encoded_name'
        WHERE provenance ->> 'adapter' = 'ens_v2_permissions'
          AND provenance ->> 'upstream_resource' = $1
        "#,
    )
    .bind(&resources[4])
    .execute(database.pool())
    .await?;
    assert_eq!(missing_legacy_update.rows_affected(), 1);
    let missing_raw_delete = sqlx::query(
        r#"
        DELETE FROM raw_logs
        WHERE chain_id = $1
          AND block_hash = $2
        "#,
    )
    .bind(chain)
    .bind(&block_hashes[8])
    .execute(database.pool())
    .await?;
    assert_eq!(missing_raw_delete.rows_affected(), 1);
    sqlx::query(
        r#"
        DELETE FROM projection_normalized_event_changes
        WHERE normalized_event_id IN (
            SELECT normalized_event_id
            FROM normalized_events
            WHERE derivation_kind = 'raw_log_preimage_observation'
              AND block_hash = $1
        )
        "#,
    )
    .bind(&block_hashes[8])
    .execute(database.pool())
    .await?;
    let missing_preimage_delete = sqlx::query(
        r#"
        DELETE FROM normalized_events
        WHERE derivation_kind = 'raw_log_preimage_observation'
          AND block_hash = $1
        "#,
    )
    .bind(&block_hashes[8])
    .execute(database.pool())
    .await?;
    assert_eq!(missing_preimage_delete.rows_affected(), 1);

    let missing_eac = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[9]),
    )
    .await?;
    assert_eq!(missing_eac.matched_log_count, 1);
    assert_eq!(missing_eac.total_synced_count, 1);
    assert_eq!(
        permission_selector_for_upstream_resource(database.pool(), &resources[4]).await?,
        json!({
            "kind": "unknown",
            "key": null,
            "hash": null,
            "normalized_name": null,
            "dns_encoded_name": null,
        })
    );

    let upgrade_named = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[10]),
    )
    .await?;
    assert_eq!(upgrade_named.matched_log_count, 1);
    assert_eq!(upgrade_named.total_synced_count, 0);
    let upgrade_legacy_update = sqlx::query(
        r#"
        UPDATE resources
        SET provenance = provenance - 'dns_encoded_name'
        WHERE provenance ->> 'adapter' = 'ens_v2_permissions'
          AND provenance ->> 'upstream_resource' = $1
        "#,
    )
    .bind(&resources[5])
    .execute(database.pool())
    .await?;
    assert_eq!(upgrade_legacy_update.rows_affected(), 1);
    let upgrade_eac = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[11]),
    )
    .await?;
    assert_eq!(upgrade_eac.matched_log_count, 1);
    assert_eq!(upgrade_eac.total_synced_count, 1);
    assert_eq!(
        permission_selector_for_upstream_resource(database.pool(), &resources[5]).await?,
        json!({
            "kind": "unknown",
            "key": null,
            "hash": null,
            "normalized_name": null,
            "dns_encoded_name": null,
        })
    );

    let upgrade_replay = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        &block_hashes[10..12],
    )
    .await?;
    assert_eq!(upgrade_replay.matched_log_count, 2);
    assert_eq!(upgrade_replay.total_synced_count, 1);
    assert_eq!(
        permission_selector_for_upstream_resource(database.pool(), &resources[5]).await?,
        in_memory_selector
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM projection_normalized_event_changes change
            JOIN normalized_events event
              ON event.normalized_event_id = change.normalized_event_id
            WHERE change.change_kind = 'content_update'
              AND event.derivation_kind = 'ens_v2_permissions'
              AND event.after_state ->> 'upstream_resource' = $1
            "#,
        )
        .bind(&resources[5])
        .fetch_one(database.pool())
        .await?,
        1,
        "the unknown-to-named upgrade must be journaled for projection replay"
    );

    let malformed_named = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[12]),
    )
    .await?;
    assert_eq!(malformed_named.matched_log_count, 1);
    assert_eq!(malformed_named.total_synced_count, 0);
    let malformed_preimage = crate::sync_block_derived_normalized_events(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[12]),
        None,
    )
    .await?;
    assert_eq!(malformed_preimage.matched_log_count, 1);
    assert_eq!(malformed_preimage.total_synced_count, 1);
    let malformed_update = sqlx::query(
        r#"
        UPDATE resources
        SET provenance =
            (provenance - 'dns_encoded_name')
            || jsonb_build_object('selector_key', TRUE)
        WHERE provenance ->> 'adapter' = 'ens_v2_permissions'
          AND provenance ->> 'upstream_resource' = $1
        "#,
    )
    .bind(&resources[6])
    .execute(database.pool())
    .await?;
    assert_eq!(malformed_update.rows_affected(), 1);

    let malformed_eac = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&block_hashes[13]),
    )
    .await?;
    assert_eq!(malformed_eac.matched_log_count, 1);
    assert_eq!(malformed_eac.total_synced_count, 1);
    assert_eq!(
        permission_selector_for_upstream_resource(database.pool(), &resources[6]).await?,
        json!({
            "kind": "unknown",
            "key": null,
            "hash": null,
            "normalized_name": null,
            "dns_encoded_name": null,
        }),
        "malformed legacy selector provenance must fail closed"
    );

    database.cleanup().await
}

#[tokio::test]
async fn forked_preimages_require_current_ancestry() -> Result<()> {
    let _permit = crate::acquire_test_db_permit().await;
    let database = TestDatabase::new().await?;
    let chain = "ethereum-sepolia";
    let resolver_address = "0x0000000000000000000000000000000000000261";
    let account = "0x1111111111111111111111111111111111111111";
    let common_block_numbers = [11_163_600, 11_163_601, 11_163_602];
    let fork_a_block_numbers = [11_163_603, 11_163_604, 11_163_605];
    let fork_b_block_numbers = [
        11_163_603, 11_163_604, 11_163_605, 11_163_606, 11_163_607, 11_163_608,
    ];
    let common_hashes =
        common_block_numbers.map(|block_number| permission_fork_block_hash("c", block_number));
    let fork_a_hashes =
        fork_a_block_numbers.map(|block_number| permission_fork_block_hash("a", block_number));
    let fork_b_hashes =
        fork_b_block_numbers.map(|block_number| permission_fork_block_hash("b", block_number));
    let resources = [topic_word(0x201), topic_word(0x202), topic_word(0x203)];
    let alice_name = dns_name("Alice", "ETH");
    let bob_name = dns_name("Bob", "ETH");
    let cross_branch_alice_name = dns_name("ALICE", "eth");
    let avatar_hash = topic_word(0xab);
    let url_hash = topic_word(0xcd);

    // NamedTextResource and EACRolesChanged are the upstream evidence/current-log pair exercised
    // across the fork below.
    // (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L149 @ ens_v2@48b3e2d)
    // (upstream: .refs/ens_v2/contracts/src/access-control/interfaces/IEnhancedAccessControl.sol:L22 @ ens_v2@48b3e2d)
    let repository = load_repository(checked_in_manifest_root("manifests/sepolia"))?;
    sync_repository(database.pool(), &repository).await?;
    insert_test_discovered_resolver(
        database.pool(),
        chain,
        resolver_address,
        common_block_numbers[0],
        &common_hashes[0],
    )
    .await?;

    let mut blocks = Vec::new();
    for (index, block_number) in common_block_numbers.into_iter().enumerate() {
        blocks.push(permissions_fork_raw_block(
            chain,
            &common_hashes[index],
            (index > 0).then(|| common_hashes[index - 1].as_str()),
            block_number,
            CanonicalityState::Canonical,
        ));
    }
    for (index, block_number) in fork_a_block_numbers.into_iter().enumerate() {
        blocks.push(permissions_fork_raw_block(
            chain,
            &fork_a_hashes[index],
            Some(if index == 0 {
                common_hashes[2].as_str()
            } else {
                fork_a_hashes[index - 1].as_str()
            }),
            block_number,
            CanonicalityState::Canonical,
        ));
    }
    for (index, block_number) in fork_b_block_numbers.into_iter().enumerate() {
        blocks.push(permissions_fork_raw_block(
            chain,
            &fork_b_hashes[index],
            Some(if index == 0 {
                common_hashes[2].as_str()
            } else {
                fork_b_hashes[index - 1].as_str()
            }),
            block_number,
            if index == fork_b_block_numbers.len() - 1 {
                CanonicalityState::Observed
            } else {
                CanonicalityState::Canonical
            },
        ));
    }
    upsert_raw_blocks(database.pool(), &blocks).await?;

    let common_named_logs = resources
        .iter()
        .enumerate()
        .map(|(index, resource)| {
            let mut raw_log = named_text_permission_raw_log(
                chain,
                &common_hashes[index],
                common_block_numbers[index],
                resolver_address,
                resource,
                &alice_name,
                &avatar_hash,
                "avatar",
            );
            raw_log.canonicality_state = CanonicalityState::Canonical;
            raw_log
        })
        .collect::<Vec<_>>();
    upsert_raw_logs(database.pool(), &common_named_logs).await?;
    let common_hints = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        &common_hashes,
    )
    .await?;
    assert_eq!(common_hints.matched_log_count, resources.len());
    assert_eq!(common_hints.total_synced_count, 0);
    let legacy_update = sqlx::query(
        r#"
        UPDATE resources
        SET provenance = provenance - 'dns_encoded_name'
        WHERE provenance ->> 'adapter' = 'ens_v2_permissions'
          AND provenance ->> 'upstream_resource' = ANY($1)
        "#,
    )
    .bind(resources.to_vec())
    .execute(database.pool())
    .await?;
    assert_eq!(legacy_update.rows_affected(), resources.len() as u64);

    let mut wrong_name = named_text_permission_raw_log(
        chain,
        &fork_a_hashes[0],
        fork_a_block_numbers[0],
        resolver_address,
        &resources[0],
        &bob_name,
        &avatar_hash,
        "avatar",
    );
    wrong_name.canonicality_state = CanonicalityState::Canonical;
    let mut wrong_selector = named_text_permission_raw_log(
        chain,
        &fork_a_hashes[1],
        fork_a_block_numbers[1],
        resolver_address,
        &resources[1],
        &alice_name,
        &url_hash,
        "url",
    );
    wrong_selector.canonicality_state = CanonicalityState::Canonical;
    let mut matching_cross_branch = named_text_permission_raw_log(
        chain,
        &fork_a_hashes[2],
        fork_a_block_numbers[2],
        resolver_address,
        &resources[2],
        &cross_branch_alice_name,
        &avatar_hash,
        "avatar",
    );
    matching_cross_branch.canonicality_state = CanonicalityState::Canonical;
    upsert_raw_logs(
        database.pool(),
        &[wrong_name, wrong_selector, matching_cross_branch],
    )
    .await?;
    let preimages =
        crate::sync_block_derived_normalized_events(database.pool(), chain, &fork_a_hashes, None)
            .await?;
    assert_eq!(preimages.matched_log_count, resources.len());
    assert_eq!(preimages.total_synced_count, resources.len());

    let mut wrong_name_role = eac_roles_changed_permission_raw_log(
        chain,
        &fork_b_hashes[2],
        fork_b_block_numbers[2],
        resolver_address,
        &resources[0],
        account,
    );
    wrong_name_role.canonicality_state = CanonicalityState::Canonical;
    let mut wrong_selector_role = eac_roles_changed_permission_raw_log(
        chain,
        &fork_b_hashes[3],
        fork_b_block_numbers[3],
        resolver_address,
        &resources[1],
        account,
    );
    wrong_selector_role.canonicality_state = CanonicalityState::Canonical;
    let mut matching_cross_branch_role = eac_roles_changed_permission_raw_log(
        chain,
        &fork_b_hashes[4],
        fork_b_block_numbers[4],
        resolver_address,
        &resources[2],
        account,
    );
    matching_cross_branch_role.canonicality_state = CanonicalityState::Canonical;
    let mut observed_role = eac_roles_changed_permission_raw_log(
        chain,
        &fork_b_hashes[5],
        fork_b_block_numbers[5],
        resolver_address,
        &resources[2],
        account,
    );
    observed_role.canonicality_state = CanonicalityState::Observed;
    upsert_raw_logs(
        database.pool(),
        &[
            wrong_name_role,
            wrong_selector_role,
            matching_cross_branch_role,
            observed_role,
        ],
    )
    .await?;

    let canonical_roles = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        &fork_b_hashes[2..5],
    )
    .await?;
    assert_eq!(canonical_roles.matched_log_count, resources.len());
    assert_eq!(canonical_roles.total_synced_count, resources.len());
    for resource in &resources {
        assert_eq!(
            permission_selector_for_upstream_resource(database.pool(), resource).await?,
            json!({
                "kind": "unknown",
                "key": null,
                "hash": null,
                "normalized_name": null,
                "dns_encoded_name": null,
            }),
            "mismatched or matching sibling-branch evidence must not repair the selector"
        );
    }

    let observed = super::EnsV2PermissionsSyncSummary::sync_for_block_hashes(
        database.pool(),
        chain,
        std::slice::from_ref(&fork_b_hashes[5]),
    )
    .await?;
    assert_eq!(observed.scanned_log_count, 0);
    assert_eq!(observed.matched_log_count, 0);
    assert_eq!(observed.total_synced_count, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::BIGINT
            FROM normalized_events
            WHERE derivation_kind = 'ens_v2_permissions'
              AND block_hash = $1
            "#,
        )
        .bind(&fork_b_hashes[5])
        .fetch_one(database.pool())
        .await?,
        0,
        "an observed current role log cannot consume even matching canonical evidence"
    );

    database.cleanup().await
}

async fn assert_registry_and_root_eac_roles_changed_are_admitted(
    pool: &PgPool,
    chain: &str,
) -> Result<()> {
    let source_families = vec![
        SOURCE_FAMILY_ENS_V2_ROOT_L1.to_owned(),
        SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
    ];
    let admitted =
        load_active_manifest_abi_events_by_chain_and_source_families(pool, chain, &source_families)
            .await?
            .into_iter()
            .filter(|event| event.canonical_signature == ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE)
            .filter(|event| event.topic0.is_some())
            .map(|event| event.source_family)
            .collect::<BTreeSet<_>>();

    assert_eq!(
        admitted,
        BTreeSet::from([
            SOURCE_FAMILY_ENS_V2_ROOT_L1.to_owned(),
            SOURCE_FAMILY_ENS_V2_REGISTRY_L1.to_owned(),
        ])
    );
    Ok(())
}

async fn insert_test_discovered_resolver(
    pool: &PgPool,
    chain: &str,
    address: &str,
    active_from_block_number: i64,
    active_from_block_hash: &str,
) -> Result<Uuid> {
    let source_manifest_id = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT manifest_id
        FROM manifest_versions
        WHERE chain = $1
          AND source_family = $2
          AND rollout_status = 'active'
        "#,
    )
    .bind(chain)
    .bind(SOURCE_FAMILY_ENS_V2_RESOLVER_L1)
    .fetch_one(pool)
    .await?;
    let from_contract_instance_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT contract_instance_id
        FROM contract_instances
        WHERE chain_id = $1
        ORDER BY contract_instance_id
        LIMIT 1
        "#,
    )
    .bind(chain)
    .fetch_one(pool)
    .await?;
    let resolver_contract_instance_id = Uuid::from_u128(0x260);
    sqlx::query(
        r#"
        INSERT INTO contract_instances (
            contract_instance_id,
            chain_id,
            contract_kind,
            provenance
        )
        VALUES ($1, $2, 'resolver', '{}'::JSONB)
        "#,
    )
    .bind(resolver_contract_instance_id)
    .bind(chain)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO contract_instance_addresses (
            contract_instance_id,
            chain_id,
            address,
            active_from_block_number,
            active_from_block_hash,
            source_manifest_id,
            provenance
        )
        VALUES ($1, $2, $3, $4, $5, $6, '{}'::JSONB)
        "#,
    )
    .bind(resolver_contract_instance_id)
    .bind(chain)
    .bind(normalize_address(address))
    .bind(active_from_block_number)
    .bind(active_from_block_hash)
    .bind(source_manifest_id)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO discovery_edges (
            chain_id,
            edge_kind,
            from_contract_instance_id,
            to_contract_instance_id,
            discovery_source,
            source_manifest_id,
            admission,
            active_from_block_number,
            active_from_block_hash,
            provenance
        )
        VALUES (
            $1,
            'resolver',
            $2,
            $3,
            'ens_v2_permissions:test-resolver',
            $4,
            'admitted',
            $5,
            $6,
            jsonb_build_object('to_address', $7::TEXT)
        )
        "#,
    )
    .bind(chain)
    .bind(from_contract_instance_id)
    .bind(resolver_contract_instance_id)
    .bind(source_manifest_id)
    .bind(active_from_block_number)
    .bind(active_from_block_hash)
    .bind(normalize_address(address))
    .execute(pool)
    .await?;
    Ok(resolver_contract_instance_id)
}

async fn permission_selector_for_upstream_resource(
    pool: &PgPool,
    upstream_resource: &str,
) -> Result<Value> {
    sqlx::query_scalar::<_, Value>(
        r#"
        SELECT after_state -> 'selector'
        FROM normalized_events
        WHERE event_kind = 'PermissionChanged'
          AND after_state ->> 'upstream_resource' = $1
        "#,
    )
    .bind(upstream_resource)
    .fetch_one(pool)
    .await
    .context("failed to load ENSv2 permission selector")
}

fn checked_in_manifest_root(profile_root: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(profile_root)
}

fn permissions_test_raw_block(chain: &str, block_hash: &str, block_number: i64) -> RawBlock {
    RawBlock {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        parent_hash: None,
        block_number,
        block_timestamp: OffsetDateTime::from_unix_timestamp(1_717_172_700 + block_number)
            .expect("test timestamp should fit"),
        logs_bloom: None,
        transactions_root: None,
        receipts_root: None,
        state_root: None,
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn permission_test_block_hash(block_number: i64) -> String {
    format!("0x{block_number:064x}")
}

fn permission_fork_block_hash(branch: &str, block_number: i64) -> String {
    let branch_byte = branch
        .as_bytes()
        .first()
        .copied()
        .expect("test branch label must not be empty");
    format!("0x{branch_byte:02x}{block_number:062x}")
}

fn permissions_fork_raw_block(
    chain: &str,
    block_hash: &str,
    parent_hash: Option<&str>,
    block_number: i64,
    canonicality_state: CanonicalityState,
) -> RawBlock {
    let mut block = permissions_test_raw_block(chain, block_hash, block_number);
    block.parent_hash = parent_hash.map(ToOwned::to_owned);
    block.canonicality_state = canonicality_state;
    block
}

#[allow(clippy::too_many_arguments)]
fn named_text_permission_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    resource: &str,
    name: &[u8],
    key_hash: &str,
    key: &str,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xnamedtext{block_number}"),
        transaction_index: 0,
        log_index: 0,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex(ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE),
            resource.to_owned(),
            key_hash.to_owned(),
        ],
        data: encode_two_dynamic(name, key.as_bytes()),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn named_permission_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    resource: &str,
    name: &[u8],
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xnamed{block_number}"),
        transaction_index: 0,
        log_index: 0,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex(ABI_EVENT_NAMED_RESOURCE_SIGNATURE),
            resource.to_owned(),
        ],
        data: [hex_word(32), dynamic_tail(name)].concat(),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn eac_roles_changed_permission_raw_log(
    chain: &str,
    block_hash: &str,
    block_number: i64,
    emitting_address: &str,
    resource: &str,
    account: &str,
) -> RawLog {
    RawLog {
        chain_id: chain.to_owned(),
        block_hash: block_hash.to_owned(),
        block_number,
        transaction_hash: format!("0xeacroles{block_number}"),
        transaction_index: 0,
        log_index: 0,
        emitting_address: normalize_address(emitting_address),
        topics: vec![
            keccak_signature_hex(ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE),
            resource.to_owned(),
            address_topic(account),
        ],
        data: [hex_word(0), bitmap_with_last_byte(0x11)]
            .into_iter()
            .flatten()
            .collect(),
        canonicality_state: CanonicalityState::Finalized,
    }
}

fn raw_log(topics: Vec<String>, data: Vec<u8>) -> PermissionsRawLogRow {
    raw_log_with_source_family("ens_v2_resolver_l1", topics, data)
}

fn raw_log_with_source_family(
    source_family: &str,
    topics: Vec<String>,
    data: Vec<u8>,
) -> PermissionsRawLogRow {
    PermissionsRawLogRow {
        chain_id: "sepolia-dev".to_owned(),
        block_hash: "0xblock".to_owned(),
        block_number: 123,
        transaction_hash: "0xtx".to_owned(),
        transaction_index: 4,
        log_index: 9,
        emitting_address: "0x2222222222222222222222222222222222222222".to_owned(),
        emitting_contract_instance_id: Uuid::parse_str("22222222-2222-5222-8222-222222222222")
            .expect("test uuid should parse"),
        topics,
        data,
        canonicality_state: CanonicalityState::Canonical,
        source_manifest_id: 42,
        namespace: "ens".to_owned(),
        source_family: source_family.to_owned(),
        manifest_version: 7,
    }
}

fn dns_name(first: &str, second: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(first.len() as u8);
    bytes.extend_from_slice(first.as_bytes());
    bytes.push(second.len() as u8);
    bytes.extend_from_slice(second.as_bytes());
    bytes.push(0);
    bytes
}

fn encode_two_dynamic(first: &[u8], second: &[u8]) -> Vec<u8> {
    let first_tail = dynamic_tail(first);
    let second_tail = dynamic_tail(second);
    let second_offset = 64 + first_tail.len();
    let mut data = Vec::new();
    data.extend_from_slice(&hex_word(64));
    data.extend_from_slice(&hex_word(second_offset as u64));
    data.extend_from_slice(&first_tail);
    data.extend_from_slice(&second_tail);
    data
}

fn dynamic_tail(bytes: &[u8]) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&hex_word(bytes.len() as u64));
    data.extend_from_slice(bytes);
    let padding = (32 - (bytes.len() % 32)) % 32;
    data.extend(std::iter::repeat_n(0, padding));
    data
}

fn address_topic(address: &str) -> String {
    format!("0x{:0>64}", address.trim_start_matches("0x"))
}

fn topic_word(value: u64) -> String {
    format!("0x{value:064x}")
}

fn hex_word(value: u64) -> Vec<u8> {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    bytes.to_vec()
}

fn bitmap_with_last_byte(value: u8) -> Vec<u8> {
    let mut bytes = [0u8; 32];
    bytes[31] = value;
    bytes.to_vec()
}

fn bitmap_hex_with_last_byte(value: u8) -> String {
    format!("0x{}", hex_string(bitmap_with_last_byte(value)))
}

fn bitmap_with_bits(bits: &[usize]) -> Vec<u8> {
    let mut bytes = [0u8; 32];
    for bit in bits {
        let byte_index = 31usize.saturating_sub(bit / 8);
        bytes[byte_index] |= 1u8 << (bit % 8);
    }
    bytes.to_vec()
}

fn bitmap_hex_with_bits(bits: &[usize]) -> String {
    format!("0x{}", hex_string(bitmap_with_bits(bits)))
}

fn test_permissions_event_topics() -> ActiveManifestEventTopic0sBySignature {
    ActiveManifestEventTopic0sBySignature::new(HashMap::from([
        (
            ABI_EVENT_NAMED_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_TEXT_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_NAMED_ADDR_RESOURCE_SIGNATURE),
        ),
        (
            ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE.to_owned(),
            keccak_signature_hex(ABI_EVENT_EAC_ROLES_CHANGED_SIGNATURE),
        ),
    ]))
}
