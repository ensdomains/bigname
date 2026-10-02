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

const CHAIN: &str = "ethereum-mainnet";
pub(super) const REGISTRY_MANIFEST: i64 = 961;
pub(super) const REGISTRAR_MANIFEST: i64 = 962;
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
pub(super) fn manifests() -> Vec<ManifestInput> {
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
pub(super) async fn persist(pool: &PgPool, output: &BatchOutput) -> Result<()> {
    persist_with_manifests(pool, &manifests(), output).await
}

/// [`persist`] for output interpreted under other manifest declarations.
pub(super) async fn persist_with_manifests(
    pool: &PgPool,
    manifests: &[ManifestInput],
    output: &BatchOutput,
) -> Result<()> {
    for manifest in manifests {
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

async fn project_to(pool: &PgPool, target: i64, _resume: Option<i64>) -> Result<()> {
    publish_test_families_on(pool, CHAIN, target).await?;
    Ok(())
}

/// The owner's relation rows: relation, resource, binding block, cited event kind and cited
/// block.
async fn owner_rows(pool: &PgPool) -> Result<Vec<(String, Uuid, i64, String, i64)>> {
    let rows = bigname_storage::load_address_names_current(pool, OWNER, None, None).await?;
    let mut out = Vec::new();
    for row in rows {
        let event_id = row.provenance["normalized_event_id"]
            .as_i64()
            .context("family relation evidence")?;
        let (binding_block, kind): (i64, String) = sqlx::query_as(
            "SELECT binding.block_number, event.event_kind FROM surface_bindings binding
             CROSS JOIN normalized_events event WHERE binding.surface_binding_id = $1 AND event.normalized_event_id = $2"
        ).bind(row.surface_binding_id).bind(event_id).fetch_one(pool).await?;
        let cited = row.chain_positions["block_number"]
            .as_i64()
            .context("relation position")?;
        out.push((
            row.relation.as_str().to_owned(),
            row.resource_id,
            binding_block,
            kind,
            cited,
        ));
    }
    out.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(out)
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
    history_for(pool, OWNER, block, canonical_only, relations).await
}

/// The history of `address` at `block`, optionally narrowed to some relations.
async fn history_for(
    pool: &PgPool,
    address: &str,
    block: i64,
    canonical_only: bool,
    relations: Option<&[bigname_storage::AddressNameRelation]>,
) -> Result<Vec<String>> {
    let page = bigname_storage::load_address_history_page_for_relations(
        pool,
        address,
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
        .find(|row| row.0 == "token_holder")
        .map(|row| row.1)
        .context("the registration projects a token holder row")?;
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

/// The holder's history at `bound`, unfiltered and narrowed to the token-holder relation, through
/// the canonical read and through the read that includes noncanonical identity rows.
async fn holder_views(pool: &PgPool, bound: i64) -> Result<Vec<Vec<String>>> {
    let token_holder = [bigname_storage::AddressNameRelation::TokenHolder];
    let mut views = Vec::new();
    for canonical_only in [true, false] {
        views.push(owner_history_for(pool, bound, canonical_only, None).await?);
        views.push(owner_history_for(pool, bound, canonical_only, Some(&token_holder)).await?);
    }
    Ok(views)
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
    // The controller relation ended when the registry owner moved away and began again at the
    // restore, so its row cites the grant at the restore, above the bound, and is not admitted
    // there: the relation counts as ended.
    let controller = restored_rows
        .iter()
        .find(|row| row.0 == "effective_controller")
        .context("the restore keeps a controller row")?;
    assert_eq!(
        (controller.1, controller.3.as_str(), controller.4),
        (registrar_resource, "PermissionChanged", RESTORED),
        "{restored_rows:?}"
    );
    for view in [&held[1], &held[3]] {
        assert!(
            view.iter()
                .any(|row| row.starts_with("RegistrationGranted:")),
            "the holder's token-holder history at the bound holds the registration: {held:?}"
        );
    }
    // While the registry owner is away the holder has no current row, and a token holder whose
    // only evidence is the grant is a documented loss, so the comparison is with the read before
    // the owner moved away.
    assert_eq!(
        restored, held,
        "the restore above the bound changed the holder's history at the bound"
    );
    Ok(())
}

mod registry_events {
    alloy_sol_types::sol! {
        event Transfer(bytes32 indexed node, address owner);
        event NewResolver(bytes32 indexed node, address resolver);
    }
}

const REGISTRY_MOVED: i64 = 132;

/// The registry owner left behind by a token transfer without `reclaim` hands the registry
/// record to a third address with `setOwner`
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f).
fn registry_set_owner(block: i64, owner: &str) -> Vec<RawLogInput> {
    let node = keccak256(
        [
            eth_node().as_slice(),
            keccak256(LABEL.as_bytes()).as_slice(),
        ]
        .concat(),
    );
    vec![raw(
        registry_events::Transfer {
            node,
            owner: owner.parse().unwrap(),
        }
        .encode_log_data(),
        block,
        0,
        REGISTRY,
    )]
}

/// A registry owner that never held the token is the name's manager and not its owner, whether
/// the registry record moved after a token transfer without `reclaim` (the registry-only binding
/// that handoff opens) or while the token holder kept the token: it gains the name's history
/// under `manager` and not under `owner`.
#[tokio::test]
async fn a_registry_owner_without_the_token_is_the_manager_in_history_not_the_owner() -> Result<()>
{
    for (case, token_holder, handed_off) in
        [("divergence", OWNER, false), ("handoff", HOLDER, true)]
    {
        let (registered_output, session) = interpret(REGISTERED, registration(), None)?;
        let (rebound_output, session) = if handed_off {
            interpret(REBOUND, handoff(), Some(session))?
        } else {
            interpret(REBOUND, Vec::new(), Some(session))?
        };
        let (moved_output, _) = interpret(
            REGISTRY_MOVED,
            registry_set_owner(REGISTRY_MOVED, OTHER),
            Some(session),
        )?;
        let database = TestDatabase::new_migrated().await?;
        seed_v2_history_blocks(&database, REGISTERED..=REGISTRY_MOVED).await?;
        for output in [&registered_output, &rebound_output, &moved_output] {
            persist(&database.pool, output).await?;
        }
        project_to(&database.pool, REGISTRY_MOVED, None).await?;
        let pool = &database.pool;
        let owner = [bigname_storage::AddressNameRelation::TokenHolder];
        let manager = [bigname_storage::AddressNameRelation::EffectiveController];
        let mut views = Vec::new();
        for canonical_only in [true, false] {
            views.push((
                history_for(pool, OTHER, REGISTRY_MOVED, canonical_only, Some(&owner)).await?,
                history_for(pool, OTHER, REGISTRY_MOVED, canonical_only, Some(&manager)).await?,
            ));
        }
        let holder_rows =
            bigname_storage::load_address_names_current(pool, token_holder, None, None).await?;
        let other_rows =
            bigname_storage::load_address_names_current(pool, OTHER, None, None).await?;
        database.cleanup().await?;

        // Anti-vacuity: the token holder owns the name and the new registry owner manages it.
        assert!(
            holder_rows
                .iter()
                .any(|row| row.relation.as_str() == "token_holder"),
            "{case}: {holder_rows:?}"
        );
        assert_eq!(
            other_rows
                .iter()
                .map(|row| row.relation.as_str())
                .collect::<Vec<_>>(),
            ["effective_controller"],
            "{case}"
        );
        for (owned, managed) in &views {
            assert!(
                managed
                    .iter()
                    .any(|row| row.starts_with("AuthorityTransferred:")),
                "{case}: the registry owner's manager history holds the transfer: {managed:?}"
            );
            assert!(
                owned.is_empty(),
                "{case}: the registry owner is not the owner: {owned:?}"
            );
        }
    }
    Ok(())
}

/// The registry owner of a subname with no token owns it, so the `NewOwner` that created the
/// subname for it is in its history under `owner` as well as `manager`
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f).
#[tokio::test]
async fn the_registry_owner_of_a_tokenless_subname_owns_it_in_history() -> Result<()> {
    let parent = keccak256(
        [
            eth_node().as_slice(),
            keccak256(LABEL.as_bytes()).as_slice(),
        ]
        .concat(),
    );
    let (registered_output, session) = interpret(REGISTERED, registration(), None)?;
    let (subname_output, _) = interpret(
        REBOUND,
        vec![raw(
            NewOwner {
                node: parent,
                label: keccak256(b"sub"),
                owner: OTHER.parse().unwrap(),
            }
            .encode_log_data(),
            REBOUND,
            0,
            REGISTRY,
        )],
        Some(session),
    )?;
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, REGISTERED..=REBOUND).await?;
    persist(&database.pool, &registered_output).await?;
    persist(&database.pool, &subname_output).await?;
    project_to(&database.pool, REBOUND, None).await?;
    let pool = &database.pool;
    let mut views = Vec::new();
    for relation in [
        bigname_storage::AddressNameRelation::TokenHolder,
        bigname_storage::AddressNameRelation::EffectiveController,
    ] {
        views.push(history_for(pool, OTHER, REBOUND, true, Some(&[relation])).await?);
    }
    database.cleanup().await?;

    for view in &views {
        assert!(
            view.iter()
                .any(|row| row.starts_with("AuthorityTransferred:")),
            "{views:?}"
        );
    }
    assert_eq!(views[0], views[1]);
    Ok(())
}

const LAPSED_EXPIRY: u64 = 1_700_000_000 - 90 * 24 * 60 * 60 - 1_000;
const NEW_HOLDER: &str = "0x00000000000000000000000000000000000000c1";

/// A registration in the order the ENSv1 contracts emit it, for `owner` until `expires`; a
/// re-registration of a lapsed token first burns it
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L131-L153 @ ens_v1@91c966f).
fn registration_at(
    block: i64,
    owner: &str,
    expires: u64,
    burned: Option<&str>,
) -> Vec<RawLogInput> {
    let owner: Address = owner.parse().unwrap();
    let label = keccak256(LABEL.as_bytes());
    let mut logs = Vec::new();
    if let Some(burned) = burned {
        logs.push(
            Transfer {
                from: burned.parse().unwrap(),
                to: Address::ZERO,
                tokenId: token_id(),
            }
            .encode_log_data(),
        );
    }
    logs.push(
        Transfer {
            from: Address::ZERO,
            to: owner,
            tokenId: token_id(),
        }
        .encode_log_data(),
    );
    let mut raws: Vec<RawLogInput> = logs
        .into_iter()
        .enumerate()
        .map(|(index, data)| raw(data, block, index as i64, REGISTRAR))
        .collect();
    let next = raws.len() as i64;
    raws.push(raw(
        NewOwner {
            node: eth_node(),
            label,
            owner,
        }
        .encode_log_data(),
        block,
        next,
        REGISTRY,
    ));
    raws.push(raw(
        registrar_lifecycle::NameRegistered {
            id: token_id(),
            owner,
            expires: U256::from(expires),
        }
        .encode_log_data(),
        block,
        next + 1,
        REGISTRAR,
    ));
    raws.push(raw(
        legacy_controller::NameRegistered {
            name: LABEL.to_owned(),
            label,
            owner,
            cost: U256::from(7),
            expires: U256::from(expires),
        }
        .encode_log_data(),
        block,
        next + 2,
        CONTROLLER,
    ));
    raws
}

/// A lease that lapsed and was released leaves the name on its registry-only resource with no
/// owner and no manager, though its registry owner, `OTHER`, to whom the old registrant handed
/// the record, gains its `manager` history and none under `owner`. The name is then registered again and its new token moves
/// without `reclaim`, which reopens the same registry-only resource as a handoff.
#[tokio::test]
async fn a_released_name_has_no_owner_or_manager() -> Result<()> {
    const RELEASED: i64 = 131;
    const REREGISTERED: i64 = 132;
    const HANDED_OFF: i64 = 133;
    let (lapsed, session) = interpret(
        REGISTERED,
        registration_at(REGISTERED, OWNER, LAPSED_EXPIRY, None),
        None,
    )?;
    let (moved, session) = interpret(RELEASED, registry_set_owner(RELEASED, OTHER), Some(session))?;
    let (reregistered, session) = interpret(
        REREGISTERED,
        registration_at(REREGISTERED, NEW_HOLDER, 4_102_444_800, Some(OWNER)),
        Some(session),
    )?;
    let (handed_off, _) = interpret(HANDED_OFF, token_moved(HANDED_OFF), Some(session))?;
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, REGISTERED..=HANDED_OFF).await?;
    let pool = &database.pool;
    let mut owners = Vec::new();
    let mut other_rows = Vec::new();
    let mut released_detail = Value::Null;
    for (block, output) in [
        (REGISTERED, &lapsed),
        (RELEASED, &moved),
        (REREGISTERED, &reregistered),
        (HANDED_OFF, &handed_off),
    ] {
        persist(pool, output).await?;
        project_to(pool, block, None).await?;
        owners.push(composed_owner(pool).await?);
        other_rows.push(relations_of(pool, OTHER).await?);
        if block == RELEASED {
            released_detail = released_detail_at(&database, RELEASED).await?;
        }
    }
    let mut views = Vec::new();
    for bound in [RELEASED, HANDED_OFF] {
        for canonical_only in [true, false] {
            for relation in [
                bigname_storage::AddressNameRelation::TokenHolder,
                bigname_storage::AddressNameRelation::EffectiveController,
            ] {
                views.push(
                    history_for(pool, OTHER, bound, canonical_only, Some(&[relation])).await?,
                );
            }
        }
    }
    database.cleanup().await?;

    assert_eq!(
        owners,
        [json!(OWNER), Value::Null, json!(NEW_HOLDER), json!(HOLDER)]
    );
    assert_eq!(
        other_rows,
        [Vec::<String>::new(), Vec::new(), Vec::new(), Vec::new()]
    );
    assert_eq!(released_detail["registration_status"], "released");
    assert!(released_detail.get("owner").is_none(), "{released_detail}");
    assert!(
        released_detail.get("manager").is_none(),
        "{released_detail}"
    );
    for pair in views.chunks(2) {
        assert!(pair[0].is_empty(), "{views:?}");
        assert!(
            pair[1]
                .iter()
                .any(|row| row.starts_with("AuthorityTransferred:")
                    && row.ends_with(&format!("@{RELEASED}"))),
            "{views:?}"
        );
    }
    Ok(())
}

/// A lease that lapsed past its grace with no later activity on its registry record keeps the
/// registry custody it left behind but has no owner and no manager; its registrant is kept apart
/// as the lapsed owner and lists it under `former_owner` only.
#[tokio::test]
async fn a_lapsed_name_lists_its_registrant_under_former_owner_only() -> Result<()> {
    const RELEASED: i64 = 131;
    const RESOLVER: &str = "0x00000000000000000000000000000000000000e5";
    let mut registered = registration_at(REGISTERED, OWNER, LAPSED_EXPIRY, None);
    let node = keccak256(
        [
            eth_node().as_slice(),
            keccak256(LABEL.as_bytes()).as_slice(),
        ]
        .concat(),
    );
    let resolver_set = registry_events::NewResolver {
        node,
        resolver: RESOLVER.parse().unwrap(),
    };
    registered.push(raw(
        resolver_set.encode_log_data(),
        REGISTERED,
        registered.len() as i64,
        REGISTRY,
    ));
    let (lapsed, session) = interpret(REGISTERED, registered, None)?;
    let (released, _) = interpret(RELEASED, Vec::new(), Some(session))?;
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, REGISTERED..=RELEASED).await?;
    let pool = &database.pool;
    persist(pool, &lapsed).await?;
    project_to(pool, REGISTERED, None).await?;
    let before = released_detail_at(&database, REGISTERED).await?;
    persist(pool, &released).await?;
    project_to(pool, RELEASED, None).await?;
    let detail = released_detail_at(&database, RELEASED).await?;
    let name = bigname_storage::logical_name_id_for_name("ens", &format!("{LABEL}.eth"));
    let composed = bigname_storage::families::name::load_family_name(pool, &name)
        .await?
        .context("composed name")?;
    let relations = relations_of(pool, OWNER).await?;
    let (status, former) = read_family_response(
        &database,
        &format!("/v1/addresses/{OWNER}/names?relation=former_owner&namespace=ens"),
    )
    .await?;
    database.cleanup().await?;

    assert_eq!(detail["registration_status"], "released", "{detail}");
    // The revived registry custody stays selected; a tombstone would null its authority kind.
    assert_eq!(
        composed.declared_summary["registration"]["authority_kind"],
        "registry_only"
    );
    assert!(detail.get("owner").is_none(), "{detail}");
    assert!(detail.get("manager").is_none(), "{detail}");
    // As on main: the lease resource carries the resolver, and the revived registry-only
    // resource the release selects has none of its own.
    assert_eq!(before["resolver"]["address"], RESOLVER, "{before}");
    assert!(detail.get("resolver").is_none(), "{detail}");
    assert_eq!(
        detail["lapsed_registration"],
        json!({"owner": OWNER, "held_through": "registrar", "released_at": "1700000131",
               "release_kind": "expired"}),
        "{detail}"
    );
    assert!(relations.is_empty(), "{relations:?}");
    assert_eq!(status, StatusCode::OK, "{former}");
    assert_eq!(v2_names_listed(&former), [format!("{LABEL}.eth")]);
    Ok(())
}

/// The lapsed block a plain lapse composes on its revived registry custody needs a supported
/// selection: the same facts read under an unsupported selection carry none.
#[tokio::test]
async fn an_unsupported_lapsed_name_has_no_lapsed_block() -> Result<()> {
    use bigname_storage::families::control::lifecycle::{
        AuthoritySelection, Clock, NameInput, NamePlace, evaluate, load_name_facts,
    };
    const RELEASED: i64 = 131;
    let registered = registration_at(REGISTERED, OWNER, LAPSED_EXPIRY, None);
    let (lapsed, session) = interpret(REGISTERED, registered, None)?;
    let (released, _) = interpret(RELEASED, Vec::new(), Some(session))?;
    let database = TestDatabase::new_migrated().await?;
    seed_v2_history_blocks(&database, REGISTERED..=RELEASED).await?;
    let pool = &database.pool;
    persist(pool, &lapsed).await?;
    persist(pool, &released).await?;
    project_to(pool, RELEASED, None).await?;
    released_detail_at(&database, RELEASED).await?;
    let name = bigname_storage::logical_name_id_for_name("ens", &format!("{LABEL}.eth"));
    let composed = bigname_storage::families::name::load_family_name(pool, &name)
        .await?
        .context("composed name")?;
    let input = NameInput {
        logical_name_id: name,
        namehash: composed.namehash.to_ascii_lowercase(),
        selection: AuthoritySelection::default(),
        place: NamePlace::EthSecondLevel,
    };
    let mut facts = load_name_facts(pool, CHAIN, &[input])
        .await?
        .pop()
        .context("name facts")?;
    database.cleanup().await?;
    let clock = Clock {
        block_number: RELEASED,
        timestamp_seconds: 1_700_000_131,
    };
    let lapsed_block = |facts: &_| -> Result<Option<Value>> {
        Ok(evaluate(facts, &clock)?
            .registration
            .get("lapsed_registration")
            .cloned())
    };

    facts.input.selection = AuthoritySelection::from_provenance(&composed.provenance);
    assert!(facts.input.selection.unsupported_reason.is_none());
    assert_eq!(
        lapsed_block(&facts)?.context("supported lapsed block")?["owner"],
        OWNER
    );
    facts.input.selection.unsupported_reason = Some("current_authority_not_projected".into());
    assert_eq!(lapsed_block(&facts)?, None);
    Ok(())
}

async fn released_detail_at(database: &TestDatabase, block: i64) -> Result<Value> {
    database
        .seed_snapshot_selector_chain_positions(&json!({CHAIN: {
            "chain_id": CHAIN, "block_number": block,
            "block_hash": format!("0xhistory{block}"),
            "timestamp": "2023-11-14T22:15:31Z"
        }}))
        .await?;
    publish_test_families_on(&database.pool, CHAIN, block).await?;
    Ok(v2_names_payload(database, &format!("/v1/names/{LABEL}.eth")).await?["data"].clone())
}

/// A `.eth` name whose registry record `OTHER` held with no token owns it there, so a later
/// registration whose token moves without `reclaim`, which reopens the same registry-only
/// resource as a handoff, leaves `OTHER`'s earlier ownership in its `owner` history. The second
/// case moves that handoff binding into `OTHER`'s block at a later transaction: the exclusion
/// compares positions, not blocks.
#[tokio::test]
async fn a_later_handoff_keeps_an_earlier_tokenless_owner_in_owner_history() -> Result<()> {
    const REGISTERED_LATER: i64 = 131;
    const HANDED_OFF: i64 = 132;
    for same_block in [false, true] {
        let tokenless = raw(
            NewOwner {
                node: eth_node(),
                label: keccak256(LABEL.as_bytes()),
                owner: OTHER.parse().unwrap(),
            }
            .encode_log_data(),
            REGISTERED,
            0,
            REGISTRY,
        );
        let (held, session) = interpret(REGISTERED, vec![tokenless], None)?;
        let (registered, session) = interpret(
            REGISTERED_LATER,
            registration_at(REGISTERED_LATER, NEW_HOLDER, 4_102_444_800, None),
            Some(session),
        )?;
        let (handed_off, _) = interpret(HANDED_OFF, token_moved(HANDED_OFF), Some(session))?;
        let database = TestDatabase::new_migrated().await?;
        seed_v2_history_blocks(&database, REGISTERED..=HANDED_OFF).await?;
        let pool = &database.pool;
        let mut owners = Vec::new();
        for (block, output) in [
            (REGISTERED, &held),
            (REGISTERED_LATER, &registered),
            (HANDED_OFF, &handed_off),
        ] {
            persist(pool, output).await?;
            project_to(pool, block, None).await?;
            owners.push(composed_owner(pool).await.ok());
        }
        let handoffs = sqlx::query(if same_block {
            "UPDATE bigname_phase.project_binding_candidate
             SET block_number = $1, transaction_index = 2, log_index = 0
             WHERE registry_only AND lease_resource_id IS NOT NULL"
        } else {
            "SELECT 1 FROM bigname_phase.project_binding_candidate
             WHERE registry_only AND lease_resource_id IS NOT NULL AND $1 > 0"
        })
        .bind(REGISTERED)
        .execute(pool)
        .await?
        .rows_affected();
        let owner = [bigname_storage::AddressNameRelation::TokenHolder];
        let mut owned = Vec::new();
        for canonical_only in [true, false] {
            owned.push(history_for(pool, OTHER, HANDED_OFF, canonical_only, Some(&owner)).await?);
        }
        database.cleanup().await?;

        assert_eq!(owners, [None, Some(json!(NEW_HOLDER)), Some(json!(HOLDER))]);
        // Anti-vacuity: the handoff reopened the registry-only resource for the lease.
        assert_eq!(handoffs, 1, "same block: {same_block}");
        for view in &owned {
            assert!(
                view.iter()
                    .any(|row| row.starts_with("AuthorityTransferred:")
                        && row.ends_with(&format!("@{REGISTERED}"))),
                "same block: {same_block}: {owned:?}"
            );
        }
    }
    Ok(())
}

/// `NEW_HOLDER` moves the token to `HOLDER` without `reclaim`.
fn token_moved(block: i64) -> Vec<RawLogInput> {
    vec![raw(
        Transfer {
            from: NEW_HOLDER.parse().unwrap(),
            to: HOLDER.parse().unwrap(),
            tokenId: token_id(),
        }
        .encode_log_data(),
        block,
        0,
        REGISTRAR,
    )]
}

async fn composed_owner(pool: &sqlx::PgPool) -> Result<Value> {
    let name = bigname_storage::logical_name_id_for_name("ens", &format!("{LABEL}.eth"));
    let composed = bigname_storage::families::name::load_family_name(pool, &name)
        .await?
        .context("composed name")?;
    Ok(composed.declared_summary["control"]["owner"].clone())
}

async fn relations_of(pool: &sqlx::PgPool, address: &str) -> Result<Vec<String>> {
    let mut relations: Vec<String> =
        bigname_storage::load_address_names_current(pool, address, None, None)
            .await?
            .iter()
            .map(|row| row.relation.as_str().to_owned())
            .collect();
    relations.sort();
    Ok(relations)
}
