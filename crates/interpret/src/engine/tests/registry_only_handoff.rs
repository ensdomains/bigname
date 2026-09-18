use super::*;

sol! {
    event NewTTL(bytes32 indexed node, uint64 ttl);
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event LabelReserved(uint256 indexed tokenId, bytes32 indexed labelHash, string label, uint64 expiry, address indexed sender);
}

/// The Sepolia ENSv2 `.eth` registry reserved the name (block 11710002 log 260), which is how the
/// label preimage became known before the ENSv1 registrar facts; the plain BaseRegistrar
/// `NameRegistered` carries only the token id.
const RESERVATION_BLOCK: i64 = SETUP_BLOCK;
const REGISTRATION_BLOCK: i64 = PREDECESSOR_BLOCK;
const HANDOFF_BLOCK: i64 = MIGRATION_BLOCK;
const HANDOFF_MIGRATION_BLOCK: i64 = MIGRATION_BLOCK + 1;

/// The party that received the registrar token without `reclaim`, and migrates it.
const HOLDER: &str = "0x0000000000000000000000000000000000000052";
const RESOLVER: &str = "0x922d6956c99e12dfeb3224dea977d0939758a1fe";
/// The ENSv1 registrar controller that registered the name; Sepolia admits only the BaseRegistrar.
const REGISTRATION_CONTROLLER: &str = "0x0000000000000000000000000000000000000098";
const HANDOFF_LOG_INDEX: i64 = 0;
const CLEANUP_LOG_INDEX: i64 = 5;

/// Mirrors the Sepolia name `bnmig-0109-pw-unwrapped-012-r02.eth` (block 11717342 log 118, then
/// block 11723340 logs 48-58): a plain `.eth` registration, a BaseRegistrar transfer without
/// `reclaim`, then the unlocked controller's migration transaction.
///
/// The transfer without `reclaim` leaves the registry owner untouched, so ordinary ENSv1
/// interpretation closes the lease binding and opens a registry-only binding
/// ([registry-only handoff](../../../../../docs/glossary.md)). The token is still the lease.
/// At migration the unlocked controller receives the token, reclaims the registry record for
/// itself, writes the Graveyard as owner with empty resolver and TTL, parks the token in the
/// Graveyard, and registers the name in ENSv2.
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L92-L121 @ ens_v2@a971bd6)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
///
/// The migration predecessor is the token: the lease found by its own token evidence, not the
/// registry-only binding that stood in for it. The boundary must resolve, every ENSv1 binding of
/// the name must be closed after it, and the ENSv2 binding must be the only current one.
#[tokio::test]
async fn registry_only_handoff_migrates_the_lease_and_closes_every_v1_binding() -> TestResult {
    let database = database("interpret_registry_only_handoff_migration").await?;
    let pool = database.pool();
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(manifest_root)?).await?;

    let label = b"pw-unwrapped-012";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    let logical_name_id = format!("ens:{namehash:#x}");
    seed_lineage(pool).await?;
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, $4, to_timestamp($4), 'canonical')",
    )
    .bind(CHAIN)
    .bind(block_hash(HANDOFF_MIGRATION_BLOCK))
    .bind(block_hash(HANDOFF_BLOCK))
    .bind(HANDOFF_MIGRATION_BLOCK)
    .execute(pool)
    .await?;
    seed_reservation(pool, label, labelhash).await?;
    seed_plain_registration(pool, labelhash, namehash).await?;
    seed_handoff_without_reclaim(pool, labelhash).await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: RESERVATION_BLOCK,
            to_block: HANDOFF_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;

    let lease_resource: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events
         WHERE chain_id = $1 AND logical_name_id = $2
           AND source_family = 'ens_v1_registrar_l1' AND event_kind = 'RegistrationGranted'",
    )
    .bind(CHAIN)
    .bind(&logical_name_id)
    .fetch_one(pool)
    .await?;
    let before = v1_bindings(pool, &logical_name_id).await?;
    let (registry_only_binding, registry_only_from, registry_only_to) = before
        .last()
        .cloned()
        .expect("the handoff leaves a current ENSv1 binding");
    assert_ne!(
        registry_only_binding.1, lease_resource,
        "the handoff binds the name to a registry-only resource: {before:?}"
    );
    assert_eq!(registry_only_from, at(HANDOFF_BLOCK, HANDOFF_LOG_INDEX)?);
    assert_eq!(
        registry_only_to, None,
        "the registry-only binding is current"
    );
    let lease_binding = before
        .iter()
        .rev()
        .find(|((_, resource), _, _)| *resource == lease_resource)
        .cloned()
        .expect("the lease was bound before the handoff");
    assert_eq!(
        lease_binding.2,
        Some(at(HANDOFF_BLOCK, HANDOFF_LOG_INDEX)?),
        "the handoff closed the lease binding without releasing the token"
    );

    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    seed_migration_transaction(pool, label, labelhash, namehash).await?;
    for mode in [RunMode::Normal, RunMode::Redo] {
        Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block: HANDOFF_MIGRATION_BLOCK,
                to_block: HANDOFF_MIGRATION_BLOCK,
                resume_current: Some(Marker {
                    number: HANDOFF_BLOCK,
                    hash: block_hash(HANDOFF_BLOCK),
                }),
                mode,
            })
            .await?;

        let after = v1_bindings(pool, &logical_name_id).await?;
        assert_eq!(
            after.len(),
            before.len(),
            "{mode:?}: the migration transaction opens no ENSv1 binding: {after:?}"
        );
        assert_eq!(
            &after[..after.len() - 1],
            &before[..before.len() - 1],
            "{mode:?}: earlier ENSv1 bindings, the lease's handoff close included, stay as they were"
        );
        assert_eq!(
            after.last().cloned(),
            Some((
                registry_only_binding,
                registry_only_from,
                Some(at(HANDOFF_MIGRATION_BLOCK, CLEANUP_LOG_INDEX)?)
            )),
            "{mode:?}: the boundary closes the registry-only binding at the registrar cleanup"
        );
        let (v1, v2): (i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE authority_arm = 'ens_v1'),
                    count(*) FILTER (WHERE authority_arm = 'ens_v2')
             FROM surface_bindings WHERE chain_id = $1 AND logical_name_id = $2
               AND active_to IS NULL AND canonicality_state = 'canonical'",
        )
        .bind(CHAIN)
        .bind(&logical_name_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            (v1, v2),
            (0, 1),
            "{mode:?}: only the ENSv2 binding stays current"
        );

        let (boundaries, lease_transfers, v1_openings): (i64, i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (
                        WHERE event_kind = 'MigrationApplied'
                          AND consumer_visibility = 'activated'
                          AND after_state #>> '{predecessor_binding,resource,token_id}' = $5),
                    count(*) FILTER (
                        WHERE event_kind = $4
                          AND source_family = 'ens_v1_registrar_l1'
                          AND resource_id = $6),
                    count(*) FILTER (
                        WHERE source_family LIKE 'ens_v1_%'
                          AND event_kind IN ('SurfaceBound', 'SurfaceUnbound',
                                             'AuthorityEpochChanged'))
             FROM normalized_events
             WHERE chain_id = $1 AND logical_name_id = $2 AND block_number = $3",
        )
        .bind(CHAIN)
        .bind(&logical_name_id)
        .bind(HANDOFF_MIGRATION_BLOCK)
        .bind(bigname_adapters::schema_v2::seam::TOKEN_CONTROL_TRANSFERRED_EVENT_KIND)
        .bind(format!("{labelhash:#x}"))
        .bind(lease_resource)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            (boundaries, lease_transfers, v1_openings),
            (1, 2, 0),
            "{mode:?}: the boundary names the token, both registrar transfers stay on the lease, \
             and no ENSv1 authority flips survive the migration transaction"
        );
    }
    database.cleanup().await?;
    Ok(())
}

type BindingRow = (
    (Uuid, Uuid),
    time::OffsetDateTime,
    Option<time::OffsetDateTime>,
);

/// Every canonical ENSv1 binding of the name as `((binding, resource), active_from, active_to)`,
/// oldest first.
async fn v1_bindings(pool: &PgPool, logical_name_id: &str) -> TestResult<Vec<BindingRow>> {
    let rows: Vec<(
        Uuid,
        Uuid,
        time::OffsetDateTime,
        Option<time::OffsetDateTime>,
    )> = sqlx::query_as(
        "SELECT surface_binding_id, resource_id, active_from, active_to
             FROM surface_bindings
             WHERE chain_id = $1 AND logical_name_id = $2 AND authority_arm = 'ens_v1'
               AND canonicality_state = 'canonical'
             ORDER BY active_from",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(binding, resource, from, to)| ((binding, resource), from, to))
        .collect())
}

fn at(block_number: i64, log_index: i64) -> TestResult<time::OffsetDateTime> {
    Ok(time::OffsetDateTime::from_unix_timestamp(block_number)?
        + time::Duration::microseconds(log_index))
}

/// The ENSv2 `.eth` registry reserves the name for its ENSv1 registrant, which publishes the label.
async fn seed_reservation(pool: &PgPool, label: &[u8], labelhash: B256) -> TestResult {
    let mut versioned = labelhash.0;
    versioned[28..].fill(0);
    insert_transaction(pool, RESERVATION_BLOCK, ETH_REGISTRY).await?;
    insert_log(
        pool,
        RESERVATION_BLOCK,
        0,
        ETH_REGISTRY,
        LabelReserved {
            tokenId: U256::from_be_bytes(versioned),
            labelHash: labelhash,
            label: std::str::from_utf8(label)?.to_owned(),
            expiry: 1_900_000_000,
            sender: Address::from([0x22; 20]),
        }
        .encode_log_data(),
    )
    .await
}

/// A `.eth` registration through the ENSv1 controller with a resolver: the registrar mints the
/// token to the controller and writes it as registry owner, the controller writes the registrant,
/// resolver and TTL with `setRecord`, then hands the token to the registrant. The registrant then
/// sets a TTL, so the migration later has a TTL to clear (Sepolia block 11723340 log 52).
/// (upstream: .refs/ens_v1/contracts/ethregistrar/ETHRegistrarController.sol:L290-L313 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L133-L152 @ ens_v1@91c966f)
async fn seed_plain_registration(pool: &PgPool, labelhash: B256, namehash: B256) -> TestResult {
    let owner = OWNER.parse::<Address>()?;
    let controller = REGISTRATION_CONTROLLER.parse::<Address>()?;
    insert_transaction(pool, REGISTRATION_BLOCK, REGISTRATION_CONTROLLER).await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        0,
        ENS_REGISTRY,
        ens_registry::NewOwner {
            node: eth_node(),
            label: labelhash,
            owner: controller,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        1,
        BASE_REGISTRAR,
        NameRegistered {
            id: U256::from_be_bytes(labelhash.0),
            owner: controller,
            expires: U256::from(1_900_000_000_u64),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        2,
        ENS_REGISTRY,
        ens_registry::Transfer {
            node: namehash,
            owner,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        3,
        ENS_REGISTRY,
        ens_registry::NewResolver {
            node: namehash,
            resolver: RESOLVER.parse()?,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        4,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: controller,
            to: owner,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        5,
        ENS_REGISTRY,
        NewTTL {
            node: namehash,
            ttl: 60,
        }
        .encode_log_data(),
    )
    .await
}

/// The registrar token moves to another holder without `reclaim`, so the registry owner stays.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
async fn seed_handoff_without_reclaim(pool: &PgPool, labelhash: B256) -> TestResult {
    insert_transaction(pool, HANDOFF_BLOCK, BASE_REGISTRAR).await?;
    insert_log(
        pool,
        HANDOFF_BLOCK,
        HANDOFF_LOG_INDEX,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: OWNER.parse()?,
            to: HOLDER.parse()?,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await
}

/// Sepolia block 11723340 logs 48-58 with the holder as the migrating party.
/// (upstream: .refs/ens_v2/contracts/src/migration/UnlockedMigrationController.sol:L92-L121 @ ens_v2@a971bd6)
async fn seed_migration_transaction(
    pool: &PgPool,
    label: &[u8],
    labelhash: B256,
    namehash: B256,
) -> TestResult {
    let holder = HOLDER.parse::<Address>()?;
    let controller = UNLOCKED_CONTROLLER.parse::<Address>()?;
    let graveyard = GRAVEYARD.parse::<Address>()?;
    let mut versioned = labelhash.0;
    versioned[28..].fill(0);
    let token = U256::from_be_bytes(versioned);
    insert_transaction(pool, HANDOFF_MIGRATION_BLOCK, UNLOCKED_CONTROLLER).await?;
    insert_log(
        pool,
        HANDOFF_MIGRATION_BLOCK,
        0,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: holder,
            to: controller,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        HANDOFF_MIGRATION_BLOCK,
        1,
        ENS_REGISTRY,
        ens_registry::NewOwner {
            node: eth_node(),
            label: labelhash,
            owner: controller,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        HANDOFF_MIGRATION_BLOCK,
        2,
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
        HANDOFF_MIGRATION_BLOCK,
        3,
        ENS_REGISTRY,
        ens_registry::NewResolver {
            node: namehash,
            resolver: Address::ZERO,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        HANDOFF_MIGRATION_BLOCK,
        4,
        ENS_REGISTRY,
        NewTTL {
            node: namehash,
            ttl: 0,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        HANDOFF_MIGRATION_BLOCK,
        CLEANUP_LOG_INDEX,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: controller,
            to: graveyard,
            tokenId: U256::from_be_bytes(labelhash.0),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        HANDOFF_MIGRATION_BLOCK,
        6,
        ETH_REGISTRY,
        LabelRegistered {
            tokenId: token,
            labelHash: labelhash,
            label: std::str::from_utf8(label)?.to_owned(),
            owner: holder,
            expiry: 1_900_000_000,
            sender: controller,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        HANDOFF_MIGRATION_BLOCK,
        7,
        ETH_REGISTRY,
        TransferSingle {
            operator: controller,
            from: Address::ZERO,
            to: holder,
            id: token,
            value: U256::from(1_u64),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        HANDOFF_MIGRATION_BLOCK,
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
        HANDOFF_MIGRATION_BLOCK,
        9,
        ETH_REGISTRY,
        EACRolesChanged {
            resource: token,
            account: holder,
            oldRoleBitmap: U256::ZERO,
            newRoleBitmap: "97409655027181761882228017414928043062435250176".parse()?,
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        HANDOFF_MIGRATION_BLOCK,
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
