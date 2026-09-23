//! A registrar token transfer without `reclaim` above the published block rebinds an ENSv1 name
//! onto its registry-only resource, which then carries the registration-time registry owner as
//! the controller. The transfer writes no registry state
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L148-L150 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f),
//! so the owner's relation outlives the rebinding. A bounded read must not take the rebound row
//! as a relation held at the bound, nor read the new resource's events into the owner's history
//! at the bound. The events come from the ENSv1 adapter and the relation rows from Project.

use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};
use bigname_adapters::schema_v2::{
    AdapterSession, AddressAdmissionInput, BatchInput, BatchOutput, ManifestInput, RawBlockInput,
    RawLogInput, StateCacheCapacity, prepare_schema_v2_batch_incremental,
};
use bigname_project::{BatchRequest, Engine, RunMode};

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY_MANIFEST: i64 = 961;
const REGISTRAR_MANIFEST: i64 = 962;
const REGISTRY: &str = "0x0000000000000000000000000000000000000091";
const REGISTRAR: &str = "0x0000000000000000000000000000000000000042";
const CONTROLLER: &str = "0x0000000000000000000000000000000000000092";
const OWNER: &str = "0x00000000000000000000000000000000000000ab";
const HOLDER: &str = "0x00000000000000000000000000000000000000cd";
const LABEL: &str = "rebound";
const REGISTERED: i64 = 130;
const REBOUND: i64 = 131;

sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
}

mod registrar_lifecycle {
    alloy_sol_types::sol! {
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    }
}

mod legacy_controller {
    alloy_sol_types::sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 cost, uint256 expires);
    }
}

fn eth_node() -> B256 {
    keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat())
}

fn token_id() -> U256 {
    U256::from_be_bytes(*keccak256(LABEL.as_bytes()))
}

fn raw(data: alloy_primitives::LogData, block: i64, index: i64, emitter: &str) -> RawLogInput {
    RawLogInput {
        chain_id: CHAIN.into(),
        block_hash: format!("0xhistory{block}"),
        block_number: block,
        block_timestamp: timestamp(1_700_000_000 + block),
        canonicality_state: "canonical".into(),
        transaction_hash: format!("0x{block:064x}"),
        transaction_index: 0,
        log_index: index,
        emitting_address: emitter.into(),
        topics: data.topics().iter().map(|t| format!("{t:#x}")).collect(),
        data: data.data.to_vec(),
    }
}

/// The checked-in mainnet registry and registrar manifests under fixture ids.
fn manifests() -> Vec<ManifestInput> {
    let repository = bigname_manifests::load_repository(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/mainnet"),
    )
    .unwrap();
    [
        (REGISTRY_MANIFEST, "ens_v1_registry_l1", "v3"),
        (REGISTRAR_MANIFEST, "ens_v1_registrar_l1", "v1"),
    ]
    .into_iter()
    .map(|(manifest_id, family, version)| {
        let loaded = repository
            .manifests()
            .iter()
            .find(|loaded| {
                loaded.manifest.chain == CHAIN
                    && loaded.manifest.source_family == family
                    && loaded.version_tag == version
            })
            .unwrap();
        ManifestInput {
            manifest_id,
            manifest_version: loaded.manifest.manifest_version as i64,
            namespace: loaded.manifest.namespace.clone(),
            source_family: loaded.manifest.source_family.clone(),
            chain_id: CHAIN.into(),
            deployment_label: loaded.manifest.deployment_epoch.clone(),
            normalizer_version: loaded.manifest.normalizer_version.clone(),
            payload_json: serde_json::to_string(&loaded.manifest).unwrap(),
        }
    })
    .collect()
}

fn admission(manifest_id: i64, instance: u128, role: &str, address: &str) -> AddressAdmissionInput {
    AddressAdmissionInput {
        address: address.into(),
        contract_instance_id: Uuid::from_u128(instance),
        source_manifest_id: Some(manifest_id),
        role: Some(role.into()),
        discovery_edge_kind: None,
        discovery_from_contract_instance_id: None,
        discovery_observation_key: None,
        active_from_block: Some(0),
        active_to_block: None,
    }
}

fn interpret(
    block: i64,
    raw_logs: Vec<RawLogInput>,
    session: Option<AdapterSession>,
) -> Result<(BatchOutput, AdapterSession)> {
    let input = BatchInput {
        chain_id: CHAIN.into(),
        manifests: manifests(),
        discovery_rules: Vec::new(),
        admissions: vec![
            admission(REGISTRY_MANIFEST, 961, "registry", REGISTRY),
            admission(REGISTRAR_MANIFEST, 962, "registrar", REGISTRAR),
            admission(
                REGISTRAR_MANIFEST,
                9621,
                "legacy_registrar_controller",
                CONTROLLER,
            ),
        ],
        prior_events: Vec::new(),
        blocks: vec![RawBlockInput {
            chain_id: CHAIN.into(),
            block_hash: format!("0xhistory{block}"),
            block_number: block,
            block_timestamp: timestamp(1_700_000_000 + block),
            canonicality_state: "canonical".into(),
        }],
        raw_logs,
    };
    let (output, session) =
        prepare_schema_v2_batch_incremental(input, session, StateCacheCapacity::Unlimited)?
            .finish(Vec::new())?;
    assert!(output.decode_skips.is_empty(), "{:?}", output.decode_skips);
    Ok((output, session))
}

/// The registration in the order the ENSv1 contracts emit it: the registrar mints the token,
/// sets the registry owner, and emits its numeric `NameRegistered`; the controller then emits its
/// label-bearing one
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L131-L153 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L333-L341 @ ens_v1@91c966f).
fn registration() -> Vec<RawLogInput> {
    let owner: Address = OWNER.parse().unwrap();
    let label = keccak256(LABEL.as_bytes());
    vec![
        raw(
            Transfer {
                from: Address::ZERO,
                to: owner,
                tokenId: token_id(),
            }
            .encode_log_data(),
            REGISTERED,
            0,
            REGISTRAR,
        ),
        raw(
            NewOwner {
                node: eth_node(),
                label,
                owner,
            }
            .encode_log_data(),
            REGISTERED,
            1,
            REGISTRY,
        ),
        raw(
            registrar_lifecycle::NameRegistered {
                id: token_id(),
                owner,
                expires: U256::from(4_102_444_800_u64),
            }
            .encode_log_data(),
            REGISTERED,
            2,
            REGISTRAR,
        ),
        raw(
            legacy_controller::NameRegistered {
                name: LABEL.to_owned(),
                label,
                owner,
                cost: U256::from(7),
                expires: U256::from(4_102_444_800_u64),
            }
            .encode_log_data(),
            REGISTERED,
            3,
            CONTROLLER,
        ),
    ]
}

/// A registrar token transfer with no registry write.
fn handoff() -> Vec<RawLogInput> {
    vec![raw(
        Transfer {
            from: OWNER.parse().unwrap(),
            to: HOLDER.parse().unwrap(),
            tokenId: token_id(),
        }
        .encode_log_data(),
        REBOUND,
        0,
        REGISTRAR,
    )]
}

/// Persists adapter output as Interpret does for the rows Project and the history read use.
async fn persist(pool: &PgPool, output: &BatchOutput) -> Result<()> {
    for manifest in manifests() {
        sqlx::query(
            "INSERT INTO manifest_versions (
                 manifest_id, manifest_version, namespace, source_family, chain_id,
                 deployment_label, rollout_status, normalizer_version, file_path,
                 manifest_payload
             ) OVERRIDING SYSTEM VALUE
             VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, $8, $9::jsonb)
             ON CONFLICT (manifest_id) DO NOTHING",
        )
        .bind(manifest.manifest_id)
        .bind(manifest.manifest_version)
        .bind(&manifest.namespace)
        .bind(&manifest.source_family)
        .bind(&manifest.chain_id)
        .bind(&manifest.deployment_label)
        .bind(&manifest.normalizer_version)
        .bind(format!("fixture/rebinding/{}.toml", manifest.source_family))
        .bind(&manifest.payload_json)
        .execute(pool)
        .await?;
    }
    for lineage in &output.token_lineages {
        sqlx::query(
            "INSERT INTO token_lineages (
                 token_lineage_id, chain_id, block_hash, block_number, provenance,
                 canonicality_state
             ) VALUES ($1, $2, $3, $4, $5, $6::canonicality_state)
             ON CONFLICT (token_lineage_id) DO NOTHING",
        )
        .bind(lineage.token_lineage_id)
        .bind(&lineage.chain_id)
        .bind(&lineage.block_hash)
        .bind(lineage.block_number)
        .bind(&lineage.provenance)
        .bind(&lineage.canonicality_state)
        .execute(pool)
        .await?;
    }
    for resource in &output.resources {
        sqlx::query(
            "INSERT INTO resources (
                 resource_id, token_lineage_id, chain_id, block_hash, block_number, provenance,
                 canonicality_state
             ) VALUES ($1, $2, $3, $4, $5, $6, $7::canonicality_state)
             ON CONFLICT (resource_id) DO NOTHING",
        )
        .bind(resource.resource_id)
        .bind(resource.token_lineage_id)
        .bind(&resource.chain_id)
        .bind(&resource.block_hash)
        .bind(resource.block_number)
        .bind(&resource.provenance)
        .bind(&resource.canonicality_state)
        .execute(pool)
        .await?;
    }
    for surface in &output.name_surfaces {
        sqlx::query(
            "INSERT INTO name_surfaces (
                 logical_name_id, namespace, raw_name, raw_labels, dns_encoded_name, namehash,
                 labelhashes, normalizer_version, visibility_state, normalization_errors,
                 deactivation_reason, deactivated_at, chain_id, block_hash, block_number,
                 provenance, canonicality_state
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                 $17::canonicality_state
             )
             ON CONFLICT (logical_name_id) DO NOTHING",
        )
        .bind(&surface.logical_name_id)
        .bind(&surface.namespace)
        .bind(&surface.raw_name)
        .bind(&surface.raw_labels)
        .bind(&surface.dns_encoded_name)
        .bind(&surface.namehash)
        .bind(&surface.labelhashes)
        .bind(&surface.normalizer_version)
        .bind(&surface.visibility_state)
        .bind(&surface.normalization_errors)
        .bind(&surface.deactivation_reason)
        .bind(surface.deactivated_at)
        .bind(&surface.chain_id)
        .bind(&surface.block_hash)
        .bind(surface.block_number)
        .bind(&surface.provenance)
        .bind(&surface.canonicality_state)
        .execute(pool)
        .await?;
    }
    for closure in &output.binding_closures {
        sqlx::query(
            "UPDATE surface_bindings
             SET active_to = $2
             WHERE logical_name_id = $1
               AND chain_id = $3
               AND authority_arm = $4
               AND ($5::uuid IS NULL OR surface_binding_id <> $5)
               AND (
                   block_number < $6
                   OR (
                       block_number = $6
                       AND (
                           COALESCE((provenance ->> 'transaction_index')::bigint, -1),
                           COALESCE((provenance ->> 'log_index')::bigint, -1)
                       ) < ($7, $8)
                   )
               )
               AND (active_to IS NULL OR active_to > $2)",
        )
        .bind(&closure.logical_name_id)
        .bind(closure.active_to)
        .bind(&closure.chain_id)
        .bind(&closure.authority_arm)
        .bind(closure.except_surface_binding_id)
        .bind(closure.block_number)
        .bind(closure.transaction_index)
        .bind(closure.log_index)
        .execute(pool)
        .await?;
    }
    for binding in &output.surface_bindings {
        sqlx::query(
            "INSERT INTO surface_bindings (
                 surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm,
                 active_from, chain_id, block_hash, block_number, provenance, canonicality_state
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::canonicality_state)",
        )
        .bind(binding.surface_binding_id)
        .bind(&binding.logical_name_id)
        .bind(binding.resource_id)
        .bind(&binding.binding_kind)
        .bind(&binding.authority_arm)
        .bind(binding.active_from)
        .bind(&binding.chain_id)
        .bind(&binding.block_hash)
        .bind(binding.block_number)
        .bind(&binding.provenance)
        .bind(&binding.canonicality_state)
        .execute(pool)
        .await?;
    }
    for event in &output.normalized_events {
        sqlx::query(
            "INSERT INTO normalized_events (
                 event_identity, namespace, logical_name_id, resource_id, event_kind,
                 source_family, manifest_version, source_manifest_id, chain_id, block_number,
                 block_hash, transaction_hash, transaction_index, log_index, raw_fact_ref,
                 derivation_kind, canonicality_state, before_state, after_state,
                 migration_correlation_ids, consumer_visibility
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                 $17::canonicality_state, $18, $19, $20, $21
             )",
        )
        .bind(&event.event_identity)
        .bind(&event.namespace)
        .bind(&event.logical_name_id)
        .bind(event.resource_id)
        .bind(&event.event_kind)
        .bind(&event.source_family)
        .bind(event.manifest_version)
        .bind(event.source_manifest_id)
        .bind(&event.chain_id)
        .bind(event.block_number)
        .bind(&event.block_hash)
        .bind(&event.transaction_hash)
        .bind(event.transaction_index)
        .bind(event.log_index)
        .bind(&event.raw_fact_ref)
        .bind(&event.derivation_kind)
        .bind(&event.canonicality_state)
        .bind(&event.before_state)
        .bind(&event.after_state)
        .bind(&event.migration_correlation_ids)
        .bind(&event.consumer_visibility)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn project_to(pool: &PgPool, target: i64, resume: Option<i64>) -> Result<()> {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            target_block: target,
            affected_from_block: target,
            affected_to_block: target,
            resume_current: resume.map(|number| bigname_project::Marker {
                number,
                hash: format!("0xhistory{number}"),
            }),
            mode: RunMode::Normal,
        })
        .await?;
    Ok(())
}

/// The owner's relation rows: relation, resource, binding block, cited event kind and cited
/// block.
async fn owner_rows(pool: &PgPool) -> Result<Vec<(String, Uuid, i64, String, i64)>> {
    Ok(sqlx::query_as(
        "SELECT anc.relation::text, anc.resource_id, binding.block_number, event.event_kind,
                (anc.chain_positions ->> 'block_number')::bigint
         FROM bigname_phase.address_names_current anc
         JOIN surface_bindings binding
           ON binding.surface_binding_id = anc.surface_binding_id
         JOIN normalized_events event
           ON event.normalized_event_id = (anc.provenance ->> 'normalized_event_id')::bigint
         WHERE anc.address = $1
         ORDER BY 1",
    )
    .bind(OWNER)
    .fetch_all(pool)
    .await?)
}

/// The owner's canonical history at `block`, as `kind:resource@block` for each row.
async fn owner_history_at(pool: &PgPool, block: i64) -> Result<Vec<String>> {
    owner_history(pool, block, true).await
}

/// The owner's history at `block`, through the canonical read or the read that includes
/// noncanonical identity rows.
async fn owner_history(pool: &PgPool, block: i64, canonical_only: bool) -> Result<Vec<String>> {
    owner_history_for(pool, block, canonical_only, None).await
}

/// The owner's history at `block`, optionally narrowed to some relations.
async fn owner_history_for(
    pool: &PgPool,
    block: i64,
    canonical_only: bool,
    relations: Option<&[bigname_storage::AddressNameRelation]>,
) -> Result<Vec<String>> {
    let page = bigname_storage::load_address_history_page_for_relations(
        pool,
        OWNER,
        None,
        relations,
        bigname_storage::HistoryScope::Both,
        canonical_only,
        None,
        50,
        bigname_storage::HistorySummaryMode::None,
        &bigname_storage::HistoryPageOptions {
            publication_block_bounds: Some(std::collections::BTreeMap::from([(
                CHAIN.to_owned(),
                block,
            )])),
            block_window: Some(bigname_storage::HistoryBlockWindow {
                ranges: vec![bigname_storage::ChainBlockRange {
                    chain_id: CHAIN.to_owned(),
                    from_block: None,
                    to_block: Some(block),
                }],
            }),
            ..bigname_storage::HistoryPageOptions::default()
        },
        false,
    )
    .await?;
    Ok(page
        .rows
        .into_iter()
        .map(|row| {
            format!(
                "{}:{}@{}",
                row.event_kind,
                row.resource_id.map(|id| id.to_string()).unwrap_or_default(),
                row.block_number.unwrap_or_default()
            )
        })
        .collect())
}

#[tokio::test]
async fn rebinding_above_the_bound_leaves_the_history_at_the_bound_unchanged() -> Result<()> {
    let (registered_output, session) = interpret(REGISTERED, registration(), None)?;
    let (rebound_output, _) = interpret(REBOUND, handoff(), Some(session))?;
    // Anti-vacuity: the transfer moves the name onto a new binding above the bound and writes
    // no registry ownership row.
    assert_eq!(rebound_output.surface_bindings.len(), 1);
    assert!(
        rebound_output
            .normalized_events
            .iter()
            .all(|event| event.event_kind != "AuthorityTransferred")
    );
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, REGISTERED..=REBOUND).await?;
    persist(&database.pool, &registered_output).await?;
    project_to(&database.pool, REGISTERED, None).await?;
    let rows_registered = owner_rows(&database.pool).await?;
    let held = owner_history_at(&database.pool, REGISTERED).await?;
    persist(&database.pool, &rebound_output).await?;
    project_to(&database.pool, REBOUND, Some(REGISTERED)).await?;
    let rows_rebound = owner_rows(&database.pool).await?;
    let after = owner_history_at(&database.pool, REGISTERED).await?;
    database.cleanup().await?;

    let registrar_resource = rows_registered
        .iter()
        .find(|row| row.0 == "registrant")
        .map(|row| row.1)
        .context("the registration projects a registrant row")?;
    assert!(
        rows_registered
            .iter()
            .all(|row| row.1 == registrar_resource && row.2 == REGISTERED && row.4 == REGISTERED),
        "{rows_registered:?}"
    );
    // The rebinding moves the owner's control onto the registry-only resource. The adapter grants
    // the owner control of that resource in the transfer's own log
    // (`crates/adapters/src/schema_v2/protocol/v1/registrar/transfer_permissions.rs`, lines 33-56),
    // so Project cites the grant at the new binding's block and not the registration-time
    // `NewOwner`. The row therefore stays out of a read bounded below the rebinding.
    let [(relation, rebound_resource, binding_block, kind, cited_block)] = rows_rebound.as_slice()
    else {
        panic!("the rebinding keeps one controller row for the owner: {rows_rebound:?}");
    };
    assert_eq!(relation, "effective_controller");
    assert_ne!(*rebound_resource, registrar_resource);
    assert_eq!(
        (*binding_block, kind.as_str(), *cited_block),
        (REBOUND, "PermissionChanged", REBOUND)
    );
    assert!(
        after
            .iter()
            .all(|row| !row.contains(&rebound_resource.to_string())),
        "the history at the bound read the rebound resource: {after:?}"
    );
    assert_eq!(
        after, held,
        "a rebinding above the bound changed the owner's history at the bound"
    );
    Ok(())
}

/// A resource the owner's rows are moved onto by hand, and the event it carries at
/// `REGISTERED`. The event names no address, so only a current row on the resource reads it into
/// the owner's history.
const MOVED_RESOURCE: Uuid = Uuid::from_u128(0x0b0a_6d10);
const MOVED_RESOURCE_TX: &str = "0xmoved-resource-event";

/// Moves the owner's rows onto a new binding at `block` of a resource that has an event at
/// `REGISTERED`, leaving every cited event where it is.
async fn move_owner_rows_to_a_binding_at(pool: &PgPool, block: i64) -> Result<()> {
    let moved = Uuid::from_u128(0x0b0a_6d12);
    let active_from = timestamp(1_700_000_000 + block);
    let previous: Uuid = sqlx::query_scalar(
        "SELECT DISTINCT surface_binding_id FROM bigname_phase.address_names_current
         WHERE address = $1",
    )
    .bind(OWNER)
    .fetch_one(pool)
    .await?;
    sqlx::query(
        "INSERT INTO resources (
             resource_id, token_lineage_id, chain_id, block_hash, block_number, provenance,
             canonicality_state
         ) VALUES ($1, NULL, $2, $3, $4, '{}'::jsonb, 'canonical')",
    )
    .bind(MOVED_RESOURCE)
    .bind(CHAIN)
    .bind(format!("0xhistory{REGISTERED}"))
    .bind(REGISTERED)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, logical_name_id, resource_id, event_kind,
             source_family, manifest_version, source_manifest_id, chain_id, block_number,
             block_hash, transaction_hash, transaction_index, log_index, raw_fact_ref,
             derivation_kind, canonicality_state, before_state, after_state,
             migration_correlation_ids, consumer_visibility
         )
         SELECT 'moved-resource-event', namespace, NULL, $1, 'ResolverChanged', source_family,
                manifest_version, source_manifest_id, chain_id, block_number, block_hash, $2,
                transaction_index, 99, raw_fact_ref, derivation_kind, canonicality_state,
                '{}'::jsonb, '{}'::jsonb, migration_correlation_ids, consumer_visibility
         FROM normalized_events
         WHERE event_kind = 'SubregistryChanged' AND block_number = $3
         LIMIT 1",
    )
    .bind(MOVED_RESOURCE)
    .bind(MOVED_RESOURCE_TX)
    .bind(REGISTERED)
    .execute(pool)
    .await?;
    sqlx::query("UPDATE surface_bindings SET active_to = $2 WHERE surface_binding_id = $1")
        .bind(previous)
        .bind(active_from)
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO surface_bindings (
             surface_binding_id, logical_name_id, resource_id, binding_kind, authority_arm,
             active_from, chain_id, block_hash, block_number, provenance, canonicality_state
         )
         SELECT $2, logical_name_id, $3, binding_kind, authority_arm, $4, chain_id, $5, $6,
                provenance, canonicality_state
         FROM surface_bindings
         WHERE surface_binding_id = $1",
    )
    .bind(previous)
    .bind(moved)
    .bind(MOVED_RESOURCE)
    .bind(active_from)
    .bind(format!("0xhistory{block}"))
    .bind(block)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE bigname_phase.address_names_current
         SET surface_binding_id = $2, resource_id = $3, token_lineage_id = NULL
         WHERE address = $1",
    )
    .bind(OWNER)
    .bind(moved)
    .bind(MOVED_RESOURCE)
    .execute(pool)
    .await?;
    Ok(())
}

/// The reader's own guarantee, independent of Project: a row whose binding was written above the
/// bound is not held at the bound, even when the event it cites lies at or below it. Project
/// re-cites a rebound row at the new binding (the test above), so no adapter output has this
/// shape and it is built by hand: the owner's rows are moved onto a binding at `REBOUND` of a
/// resource with an older event, while the events they cite stay at `REGISTERED`.
#[tokio::test]
async fn a_row_bound_above_the_bound_is_not_held_at_the_bound() -> Result<()> {
    let (registered_output, _) = interpret(REGISTERED, registration(), None)?;
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, REGISTERED..=REBOUND).await?;
    persist(&database.pool, &registered_output).await?;
    project_to(&database.pool, REGISTERED, None).await?;
    move_owner_rows_to_a_binding_at(&database.pool, REBOUND).await?;
    let rows = owner_rows(&database.pool).await?;
    let at_bound = owner_history_at(&database.pool, REGISTERED).await?;
    let at_binding = owner_history_at(&database.pool, REBOUND).await?;
    // The read that includes noncanonical identity rows has no binding join and probes the
    // binding by key instead.
    let noncanonical_at_bound = owner_history(&database.pool, REGISTERED, false).await?;
    let noncanonical_at_binding = owner_history(&database.pool, REBOUND, false).await?;
    database.cleanup().await?;

    // Anti-vacuity: every row sits on the moved binding and still cites the registration block.
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert!(
        rows.iter()
            .all(|row| row.1 == MOVED_RESOURCE && row.2 == REBOUND && row.4 == REGISTERED),
        "{rows:?}"
    );
    let moved = MOVED_RESOURCE.to_string();
    assert!(
        at_bound.iter().all(|row| !row.contains(&moved)),
        "a row bound above the bound was held at the bound: {at_bound:?}"
    );
    assert!(
        at_binding.iter().any(|row| row.contains(&moved)),
        "the row is held once the bound reaches its binding: {at_binding:?}"
    );
    assert!(
        noncanonical_at_bound
            .iter()
            .all(|row| !row.contains(&moved)),
        "the noncanonical read held a row bound above the bound: {noncanonical_at_bound:?}"
    );
    assert!(
        noncanonical_at_binding
            .iter()
            .any(|row| row.contains(&moved)),
        "the noncanonical read holds the row once the bound reaches its binding: \
         {noncanonical_at_binding:?}"
    );
    Ok(())
}

const OTHER: &str = "0x00000000000000000000000000000000000000ef";
const MOVED_AWAY: i64 = 131;
const RESTORED: i64 = 132;

/// `reclaim` from the token holder, which makes the registrar set the registry owner of the name
/// through `setSubnodeOwner`, so the registry emits `NewOwner`
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L171-L175 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f).
fn reclaim(block: i64, owner: &str) -> Vec<RawLogInput> {
    vec![raw(
        NewOwner {
            node: eth_node(),
            label: keccak256(LABEL.as_bytes()),
            owner: owner.parse().unwrap(),
        }
        .encode_log_data(),
        block,
        0,
        REGISTRY,
    )]
}

/// The holder's history at `bound`, unfiltered and narrowed to the token-holder relation.
async fn holder_views(pool: &PgPool, bound: i64) -> Result<(Vec<String>, Vec<String>)> {
    Ok((
        owner_history_for(pool, bound, true, None).await?,
        owner_history_for(
            pool,
            bound,
            true,
            Some(&[bigname_storage::AddressNameRelation::TokenHolder]),
        )
        .await?,
    ))
}

/// A registry owner moved away from the token holder and restored by `reclaim` re-attaches the
/// registrar resource with a fresh binding at the restore, with no registrar transfer, so the
/// holder's rows keep citing the registration. The holder held the token throughout, so its
/// history at the registration block must not change when the restore is projected.
#[tokio::test]
async fn a_restored_registry_owner_keeps_the_holder_at_the_bound() -> Result<()> {
    let (registered_output, session) = interpret(REGISTERED, registration(), None)?;
    let (away_output, session) = interpret(MOVED_AWAY, reclaim(MOVED_AWAY, OTHER), Some(session))?;
    let (restored_output, _) = interpret(RESTORED, reclaim(RESTORED, OWNER), Some(session))?;
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, REGISTERED..=RESTORED).await?;
    persist(&database.pool, &registered_output).await?;
    project_to(&database.pool, REGISTERED, None).await?;
    let held = holder_views(&database.pool, REGISTERED).await?;
    persist(&database.pool, &away_output).await?;
    project_to(&database.pool, MOVED_AWAY, Some(REGISTERED)).await?;
    persist(&database.pool, &restored_output).await?;
    project_to(&database.pool, RESTORED, Some(MOVED_AWAY)).await?;
    let restored_rows = owner_rows(&database.pool).await?;
    let restored = holder_views(&database.pool, REGISTERED).await?;
    database.cleanup().await?;

    // Anti-vacuity: the restore rebinds the registrar resource at `RESTORED` and the holder's
    // rows keep citing the registration below the bound.
    let registrar_resource = restored_rows
        .iter()
        .find(|row| row.0 == "token_holder")
        .map(|row| row.1)
        .context("the restore keeps a token-holder row")?;
    assert!(
        restored_output
            .surface_bindings
            .iter()
            .any(|binding| binding.resource_id == registrar_resource
                && binding.block_number == RESTORED),
        "the restore writes a binding of the registrar resource"
    );
    assert!(
        restored_rows
            .iter()
            .filter(|row| row.0 != "effective_controller")
            .all(|row| row.1 == registrar_resource && row.2 == RESTORED && row.4 == REGISTERED),
        "{restored_rows:?}"
    );
    assert!(
        held.1
            .iter()
            .any(|row| row.starts_with("RegistrationGranted:")),
        "the holder's token-holder history at the bound holds the registration: {held:?}"
    );
    // While the registry owner is away the holder has no current row, and a token holder whose
    // only evidence is the grant is a documented loss, so the comparison is with the read before
    // the owner moved away.
    assert_eq!(
        restored, held,
        "the restore above the bound changed the holder's history at the bound"
    );
    Ok(())
}
