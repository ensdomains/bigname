use std::path::{Path, PathBuf};

use bigname_adapters::schema_v2::{
    BatchOutput,
    seam::{MIGRATION_APPLIED_EVENT_KIND, SURFACE_BOUND_EVENT_KIND, SURFACE_UNBOUND_EVENT_KIND},
};

use super::*;

mod wrapped_controller {
    use alloy_sol_types::sol;

    sol! {
        event NameRegistered(string name, bytes32 indexed label, address indexed owner, uint256 baseCost, uint256 premium, uint256 expires);
    }
}

mod base_registrar_lifecycle {
    use alloy_sol_types::sol;

    sol! {
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    }
}

/// The Mainnet `WrappedETHRegistrarController`, the emitter role the Mainnet registrar manifest
/// declares for the label-bearing `NameRegistered`.
/// (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L2 @ ens_v1@91c966f)
const WRAPPED_CONTROLLER: &str = "0x253553366da8546fc250f225fe3d25d0c782303b";
const RESOLVER: &str = "0x922d6956c99e12dfeb3224dea977d0939758a1fe";
/// The Sepolia BaseRegistrar start block; every Mainnet start block is rewritten to it so the
/// re-chained Mainnet declarations are active at the test blocks.
const SEPOLIA_REGISTRAR_START_BLOCK: u64 = 3_702_731;
const REGISTRATION_BLOCK: i64 = SETUP_BLOCK;
const REGISTRAR_EXPIRY: u64 = 1_900_000_000;
/// `registerAndWrapETH2LD` wraps with the registrar expiry plus the 90-day grace period.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
const WRAPPER_EXPIRY: u64 = REGISTRAR_EXPIRY + 90 * 24 * 60 * 60;
/// `PARENT_CANNOT_CONTROL | IS_DOT_ETH`, the fuses `_wrapETH2LD` always adds.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1015 @ ens_v1@91c966f)
const DOT_ETH_FUSES: u32 = 0x10000 | 0x20000;
/// The BaseRegistrar transfer to the Graveyard in `seed_unlocked_wrapped_migration`.
const CLEANUP_LOG_INDEX: i64 = 5;
const NAME_UNWRAPPED_LOG_INDEX: i64 = 4;

/// A `.eth` name registered straight into the NameWrapper on the Mainnet deployment profile, then
/// migrated on the unlocked-wrapped path.
///
/// Mainnet has no `ens_v2_migration_l1` manifest yet, so this test loads the checked-in Sepolia
/// profile and replaces its `ens_v1_registrar_l1` manifest with the checked-in Mainnet one,
/// re-chained to `ethereum-sepolia` with its start blocks lowered. What that changes is the
/// evidence shape: on Mainnet the numeric BaseRegistrar `NameRegistered`/`NameRenewed` are
/// declared as `RegistrationReleased` and `RegistrationRenewed`+`ExpiryChanged`, not as
/// `RegistrationGranted`, so they are not lifecycle-interpreted; the lifecycle `RegistrationGranted`
/// comes from the controller's label-bearing `NameRegistered`, which carries `labelhash` but no
/// `token_id`. The test assumes the Sepolia migration controller addresses stand in for the
/// Mainnet ones; nothing in the writer under test depends on which controller address it is.
///
/// Registration: the wrapped controller calls `NameWrapper.registerAndWrapETH2LD`, which calls
/// `BaseRegistrar.register(tokenId, NameWrapper)` (mint transfer from the zero address, registry
/// `NewOwner`, numeric `NameRegistered`), then `_wrapETH2LD` (ERC-1155 mint, `NameWrapped`,
/// `NewResolver`), and finally the controller emits its own `NameRegistered`. The token never
/// moves at the BaseRegistrar level afterwards.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1020 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/deployments/mainnet/WrappedETHRegistrarController.json:L656 @ ens_v1@91c966f)
///
/// Migration: the owner transfers the wrapped token to the unlocked controller (ERC-1155
/// `TransferSingle`), whose receiver clears the ENSv1 resolver and calls
/// `unwrapETH2LD(labelHash, GRAVEYARD, GRAVEYARD)`: `_unwrap` burns the wrapped token, sets the
/// registry owner to the Graveyard and emits `NameUnwrapped`, then the wrapper calls
/// `registrar.safeTransferFrom(NameWrapper, GRAVEYARD, tokenId)`, and the controller registers
/// the name in ENSv2.
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L128-L150 @ ens_v2@a971bd6)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L269-L279 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L281-L306 @ ens_v1@91c966f)
///
/// The mint transfer is ignored by the adapter, so the lease's only `TokenControlTransferred` is
/// the cleanup itself. The boundary must still resolve exactly one ENSv1 predecessor, close every
/// ENSv1 binding of the name at the cleanup, and leave the ENSv2 binding as the only current one;
/// a Redo pass over the migration block must reproduce the same bindings.
#[tokio::test]
async fn mainnet_declared_registrar_resolves_the_lease_of_a_wrapper_minted_registration()
-> TestResult {
    let database = database("interpret_mainnet_shape_wrapped_mint").await?;
    let pool = database.pool();
    let manifests = MainnetShapedManifests::create()?;
    let repository = load_repository(&manifests.root)?;
    let registrar = repository
        .manifests()
        .iter()
        .find(|loaded| loaded.manifest.source_family == "ens_v1_registrar_l1")
        .expect("the hybrid profile declares the registrar family");
    assert_eq!(registrar.manifest.chain, CHAIN);
    let numeric_registration = registrar
        .manifest
        .abi
        .events
        .iter()
        .find(|event| {
            event
                .fragment
                .starts_with("event NameRegistered(uint256 indexed id")
        })
        .expect("the Mainnet registrar declares the numeric NameRegistered");
    assert_eq!(
        numeric_registration.normalized_events,
        ["RegistrationReleased"],
        "the Mainnet declaration keeps the numeric NameRegistered out of lifecycle interpretation"
    );
    assert!(
        registrar.manifest.contracts.iter().any(|contract| {
            contract.role == "wrapped_registrar_controller"
                && contract.address.eq_ignore_ascii_case(WRAPPED_CONTROLLER)
        }),
        "the Mainnet registrar declares the wrapped controller"
    );
    sync_schema_v2_repository(pool, &repository).await?;

    let label = b"mainnet-wrapped-mint";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    let logical_name_id = format!("ens:{namehash:#x}");
    seed_lineage(pool).await?;
    seed_wrapper_minted_registration(pool, label, labelhash, namehash).await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: SETUP_BLOCK,
            to_block: PREDECESSOR_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    let lease_evidence_before = lease_evidence(pool, &logical_name_id).await?;
    let bindings_before = bindings(pool, &logical_name_id).await?;
    assert_eq!(
        bindings_before
            .iter()
            .filter(|row| row.arm == "ens_v1" && row.active_to.is_none())
            .count(),
        1,
        "the registration leaves the wrapper binding as the one current ENSv1 binding: {bindings_before:#?}"
    );

    seed_unlocked_wrapped_migration(pool, label, labelhash, namehash).await?;
    let loaded = load::batch_input(
        pool,
        CHAIN,
        MIGRATION_BLOCK,
        MIGRATION_BLOCK,
        None,
        None,
        StateCacheCapacity::Entries(65_536),
    )
    .await?;
    let expected_orphaning_epoch = loaded.prior_cache.validated_orphaning_epoch;
    let prepared = prepare_schema_v2_batch_incremental(
        loaded.input,
        loaded.adapter_session,
        StateCacheCapacity::Entries(65_536),
    )?;
    let state_values = load::prior_state_values(
        pool,
        CHAIN,
        MIGRATION_BLOCK,
        prepared.state_value_requests(),
    )
    .await?;
    let (mut output, _) = prepared.finish(state_values)?;
    inject_activated_transition_for_test(&mut output)?;
    let migration_diagnostics = describe_migration_output(&output, &logical_name_id);
    assert_eq!(
        output.migration_authority_transitions.len(),
        1,
        "the unlocked-wrapped transaction activates one boundary\n{migration_diagnostics}"
    );
    assert_eq!(
        output.migration_authority_transitions[0].predecessor_selector["resource"]["anchor_kind"],
        "registrar_backed_registration",
        "the unlocked-wrapped path selects the lease\n{migration_diagnostics}"
    );

    // The production phase runner owns diagnostic correlation persistence; this test exercises
    // production interpretation and the identity writer, as the other unlocked-wrapped gates do.
    output.migration_event_associations.clear();
    output.migration_discovery_associations.clear();
    output.migration_candidate_identity_effects.clear();
    output.migration_candidate_discovery_effects.clear();
    let expected_lineage = [(MIGRATION_BLOCK, block_hash(MIGRATION_BLOCK))];
    let written = write::batch(
        pool,
        CHAIN,
        None,
        false,
        true,
        expected_orphaning_epoch,
        &expected_lineage,
        &output,
    )
    .await;
    if let Err(error) = written {
        panic!(
            "the migration boundary writer rejected a wrapper-minted Mainnet-shaped lease: {error}\n\
             lease evidence before the migration (event_kind, log_index, token_id):\n{lease_evidence_before:#?}\n\
             ENSv1/ENSv2 bindings before the migration:\n{bindings_before:#?}\n\
             {migration_diagnostics}"
        );
    }

    let after = bindings(pool, &logical_name_id).await?;
    let current_v1 = after
        .iter()
        .filter(|row| row.arm == "ens_v1" && row.active_to.is_none())
        .count();
    let current_v2 = after
        .iter()
        .filter(|row| row.arm == "ens_v2" && row.active_to.is_none())
        .count();
    assert_eq!(
        (current_v1, current_v2),
        (0, 1),
        "only the ENSv2 binding stays current after the boundary: {after:#?}"
    );
    let latest_v1_close = after
        .iter()
        .filter(|row| row.arm == "ens_v1")
        .filter_map(|row| row.active_to)
        .max()
        .expect("the name had an ENSv1 binding");
    assert_eq!(
        latest_v1_close,
        at(MIGRATION_BLOCK, CLEANUP_LOG_INDEX)?,
        "the last ENSv1 binding closes at the registrar cleanup: {after:#?}"
    );

    // Redo re-derives the migration block against the state the first pass wrote, with the
    // cleanup transfer already in `normalized_events`, and must reach the same boundary.
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: MIGRATION_BLOCK,
            to_block: MIGRATION_BLOCK,
            resume_current: Some(Marker {
                number: PREDECESSOR_BLOCK,
                hash: block_hash(PREDECESSOR_BLOCK),
            }),
            mode: RunMode::Redo,
        })
        .await?;
    let redone = bindings(pool, &logical_name_id).await?;
    assert_eq!(
        redone
            .iter()
            .filter(|row| row.arm == "ens_v1")
            .collect::<Vec<_>>(),
        after
            .iter()
            .filter(|row| row.arm == "ens_v1")
            .collect::<Vec<_>>(),
        "Redo keeps every ENSv1 binding as the first pass left it"
    );
    assert_eq!(
        redone
            .iter()
            .filter(|row| row.active_to.is_none())
            .map(|row| row.arm.as_str())
            .collect::<Vec<_>>(),
        ["ens_v2"],
        "Redo leaves the ENSv2 binding as the only current one: {redone:#?}"
    );

    drop(manifests);
    database.cleanup().await?;
    Ok(())
}

/// The checked-in Sepolia profile with its registrar manifest replaced by the checked-in Mainnet
/// registrar manifest, re-chained to Sepolia. Removed on drop.
struct MainnetShapedManifests {
    root: PathBuf,
}

impl MainnetShapedManifests {
    fn create() -> TestResult<Self> {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let root = std::env::temp_dir().join(format!(
            "bigname-mainnet-shaped-manifests-{}",
            Uuid::new_v4()
        ));
        let manifests = Self { root };
        copy_directory(&workspace.join("manifests/sepolia"), &manifests.root)?;
        let mainnet_registrar = std::fs::read_to_string(
            workspace.join("manifests/mainnet/ethereum/ens/ens_v1_registrar_l1/v1.toml"),
        )?;
        let rechained = mainnet_registrar
            .lines()
            .map(|line| {
                if line.starts_with("chain = ") {
                    format!("chain = \"{CHAIN}\"")
                } else if line.starts_with("start_block = ") {
                    format!("start_block = {SEPOLIA_REGISTRAR_START_BLOCK}")
                } else {
                    line.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(
            manifests
                .root
                .join("ethereum/ens/ens_v1_registrar_l1/v1.toml"),
            rechained,
        )?;
        Ok(manifests)
    }
}

impl Drop for MainnetShapedManifests {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn copy_directory(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_directory(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
struct BindingRow {
    binding: Uuid,
    resource: Uuid,
    arm: String,
    block_number: i64,
    log_index: Option<i64>,
    active_from: time::OffsetDateTime,
    active_to: Option<time::OffsetDateTime>,
}

type RawBindingRow = (
    Uuid,
    Uuid,
    String,
    i64,
    Option<i64>,
    time::OffsetDateTime,
    Option<time::OffsetDateTime>,
);

/// Every canonical binding of the name, oldest first.
async fn bindings(pool: &PgPool, logical_name_id: &str) -> TestResult<Vec<BindingRow>> {
    let rows: Vec<RawBindingRow> = sqlx::query_as(&format!(
        "SELECT surface_binding_id, resource_id, authority_arm, block_number,
                (provenance ->> '{LOG_INDEX_KEY}')::bigint, active_from, active_to
         FROM surface_bindings
         WHERE chain_id = $1 AND logical_name_id = $2
           AND canonicality_state IN ('canonical', 'safe', 'finalized')
         ORDER BY active_from, block_number, (provenance ->> '{LOG_INDEX_KEY}')::bigint"
    ))
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(binding, resource, arm, block_number, log_index, active_from, active_to)| {
                BindingRow {
                    binding,
                    resource,
                    arm,
                    block_number,
                    log_index,
                    active_from,
                    active_to,
                }
            },
        )
        .collect())
}

/// The name's registrar-family events with their `token_id`, the writer's lease evidence.
async fn lease_evidence(
    pool: &PgPool,
    logical_name_id: &str,
) -> TestResult<Vec<(String, Option<Uuid>, i64, i64, Option<String>)>> {
    Ok(sqlx::query_as(
        "SELECT event_kind, resource_id, block_number, log_index, after_state ->> 'token_id'
         FROM normalized_events
         WHERE chain_id = $1 AND logical_name_id = $2 AND source_family = 'ens_v1_registrar_l1'
         ORDER BY block_number, log_index, event_kind",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_all(pool)
    .await?)
}

/// What the adapter emitted for the name in the migration transaction: registrar-family events
/// with their token id, every binding it opened, every closure it recorded, and the transition.
fn describe_migration_output(output: &BatchOutput, logical_name_id: &str) -> String {
    let mut lines = vec!["migration transaction, registrar-family events:".to_owned()];
    for event in output.normalized_events.iter().filter(|event| {
        event.logical_name_id.as_deref() == Some(logical_name_id)
            && (event.source_family == "ens_v1_registrar_l1"
                || [
                    SURFACE_BOUND_EVENT_KIND,
                    SURFACE_UNBOUND_EVENT_KIND,
                    MIGRATION_APPLIED_EVENT_KIND,
                ]
                .contains(&event.event_kind.as_str()))
    }) {
        lines.push(format!(
            "  log {:?} {} {} resource={:?} visibility={} token_id={:?} surface_binding_id={:?}",
            event.log_index,
            event.source_family,
            event.event_kind,
            event.resource_id,
            event.consumer_visibility,
            event.after_state.get("token_id"),
            event.after_state.get(SURFACE_BINDING_ID_KEY),
        ));
    }
    lines.push("migration transaction, bindings opened:".to_owned());
    for binding in output
        .surface_bindings
        .iter()
        .filter(|binding| binding.logical_name_id == logical_name_id)
    {
        lines.push(format!(
            "  {} arm={} resource={} block={} provenance={} active_from={}",
            binding.surface_binding_id,
            binding.authority_arm,
            binding.resource_id,
            binding.block_number,
            binding.provenance,
            binding.active_from
        ));
    }
    lines.push("migration transaction, binding closures:".to_owned());
    for closure in output
        .binding_closures
        .iter()
        .filter(|closure| closure.logical_name_id == logical_name_id)
    {
        lines.push(format!("  {closure:?}"));
    }
    lines.push("migration transaction, authority transitions:".to_owned());
    for transition in &output.migration_authority_transitions {
        lines.push(format!(
            "  boundary={} log={} selector={}",
            transition.boundary_event_identity,
            transition.log_index,
            transition.predecessor_selector
        ));
    }
    lines.join("\n")
}

fn at(block_number: i64, log_index: i64) -> TestResult<time::OffsetDateTime> {
    Ok(time::OffsetDateTime::from_unix_timestamp(block_number)?
        + time::Duration::microseconds(log_index))
}

/// The wrapped controller registers the name straight into the NameWrapper.
async fn seed_wrapper_minted_registration(
    pool: &PgPool,
    label: &[u8],
    labelhash: B256,
    namehash: B256,
) -> TestResult {
    let owner = OWNER.parse::<Address>()?;
    let name_wrapper = NAME_WRAPPER.parse::<Address>()?;
    let mut dns_name = Vec::with_capacity(label.len() + 6);
    dns_name.push(u8::try_from(label.len())?);
    dns_name.extend_from_slice(label);
    dns_name.extend_from_slice(b"\x03eth\0");

    insert_transaction(pool, REGISTRATION_BLOCK, WRAPPED_CONTROLLER).await?;
    // BaseRegistrar._register: mint to the NameWrapper, registry NewOwner, numeric NameRegistered.
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        0,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: Address::ZERO,
            to: name_wrapper,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        1,
        ENS_REGISTRY,
        ens_registry::NewOwner {
            node: eth_node(),
            label: labelhash,
            owner: name_wrapper,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        2,
        BASE_REGISTRAR,
        base_registrar_lifecycle::NameRegistered {
            id: U256::from_be_bytes(labelhash.0),
            owner: name_wrapper,
            expires: U256::from(REGISTRAR_EXPIRY),
        }
        .encode_log_data(),
    )
    .await?;
    // NameWrapper._wrapETH2LD: ERC-1155 mint, NameWrapped, then the resolver.
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        3,
        NAME_WRAPPER,
        TransferSingle {
            operator: name_wrapper,
            from: Address::ZERO,
            to: owner,
            id: U256::from_be_bytes(namehash.0),
            value: U256::from(1_u64),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        4,
        NAME_WRAPPER,
        NameWrapped {
            node: namehash,
            name: dns_name.into(),
            owner,
            fuses: DOT_ETH_FUSES,
            expiry: WRAPPER_EXPIRY,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        5,
        ENS_REGISTRY,
        ens_registry::NewResolver {
            node: namehash,
            resolver: RESOLVER.parse()?,
        }
        .encode_log_data(),
    )
    .await?;
    // The controller's own label-bearing NameRegistered.
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        6,
        WRAPPED_CONTROLLER,
        wrapped_controller::NameRegistered {
            name: std::str::from_utf8(label)?.to_owned(),
            label: labelhash,
            owner,
            baseCost: U256::from(1_u64),
            premium: U256::ZERO,
            expires: U256::from(REGISTRAR_EXPIRY),
        }
        .encode_log_data(),
    )
    .await
}

/// The owner hands the wrapped token to the unlocked controller, which unwraps it into the
/// Graveyard and registers the name in ENSv2.
async fn seed_unlocked_wrapped_migration(
    pool: &PgPool,
    label: &[u8],
    labelhash: B256,
    namehash: B256,
) -> TestResult {
    let owner = OWNER.parse::<Address>()?;
    let controller = UNLOCKED_CONTROLLER.parse::<Address>()?;
    let graveyard = GRAVEYARD.parse::<Address>()?;
    let wrapped_token = U256::from_be_bytes(namehash.0);
    let mut versioned = labelhash.0;
    versioned[28..].fill(0);
    let token = U256::from_be_bytes(versioned);

    insert_transaction(pool, MIGRATION_BLOCK, NAME_WRAPPER).await?;
    // ERC1155Fuse.safeTransferFrom emits before the receiver runs.
    insert_log(
        pool,
        MIGRATION_BLOCK,
        0,
        NAME_WRAPPER,
        TransferSingle {
            operator: owner,
            from: owner,
            to: controller,
            id: wrapped_token,
            value: U256::from(1_u64),
        }
        .encode_log_data(),
    )
    .await?;
    // _migrateWrapped: NAME_WRAPPER.setResolver(id, 0).
    insert_log(
        pool,
        MIGRATION_BLOCK,
        1,
        ENS_REGISTRY,
        ens_registry::NewResolver {
            node: namehash,
            resolver: Address::ZERO,
        }
        .encode_log_data(),
    )
    .await?;
    // unwrapETH2LD -> _unwrap: burn, ens.setOwner(node, GRAVEYARD), NameUnwrapped.
    insert_log(
        pool,
        MIGRATION_BLOCK,
        2,
        NAME_WRAPPER,
        TransferSingle {
            operator: controller,
            from: controller,
            to: Address::ZERO,
            id: wrapped_token,
            value: U256::from(1_u64),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        3,
        ENS_REGISTRY,
        ens_registry::Transfer {
            node: namehash,
            owner: graveyard,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        NAME_UNWRAPPED_LOG_INDEX,
        NAME_WRAPPER,
        NameUnwrapped {
            node: namehash,
            owner: graveyard,
        }
        .encode_log_data(),
    )
    .await?;
    // unwrapETH2LD: registrar.safeTransferFrom(NameWrapper, GRAVEYARD, tokenId).
    insert_log(
        pool,
        MIGRATION_BLOCK,
        CLEANUP_LOG_INDEX,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: NAME_WRAPPER.parse()?,
            to: graveyard,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await?;
    // _inject: ETH_REGISTRY.register.
    insert_log(
        pool,
        MIGRATION_BLOCK,
        6,
        ETH_REGISTRY,
        LabelRegistered {
            tokenId: token,
            labelHash: labelhash,
            label: std::str::from_utf8(label)?.to_owned(),
            owner,
            expiry: 1_900_000_000,
            sender: controller,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        7,
        ETH_REGISTRY,
        TransferSingle {
            operator: controller,
            from: Address::ZERO,
            to: owner,
            id: token,
            value: U256::from(1_u64),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        8,
        ETH_REGISTRY,
        TokenResource {
            tokenId: token,
            resource: token,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        9,
        ETH_REGISTRY,
        EACRolesChanged {
            resource: token,
            account: owner,
            oldRoleBitmap: U256::ZERO,
            newRoleBitmap: "97409655027181761882228017414928043062435250176".parse()?,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        MIGRATION_BLOCK,
        10,
        ETH_REGISTRY,
        ResolverUpdated {
            tokenId: token,
            resolver: RESOLVER.parse()?,
            sender: controller,
        }
        .encode_log_data(),
    )
    .await
}
