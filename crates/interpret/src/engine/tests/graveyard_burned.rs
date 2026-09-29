//! An ENSv1 `.eth` grant the admitted Graveyard holds is burned: it never decides the name's
//! authority, is never served as an ENSv1 registration, owner or registrant, and its expiry is
//! never served. These fixtures run production Interpret over the checked-in Sepolia manifests,
//! publish the families and read the served name.
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L20-L25 @ ens_v2_sepolia_20260916@366de741)
use bigname_project::families::{self, FamilyMode, FamilyOptions};
use serde_json::Value;

use super::*;

/// The block after the migration fixture: the ENSv2 unregister, or the Graveyard claim.
const LATER_BLOCK: i64 = MIGRATION_BLOCK + 1;
/// `BaseRegistrarImplementation.GRACE_PERIOD`.
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L17 @ ens_v1@91c966f)
const GRACE_PERIOD: i64 = 90 * 24 * 60 * 60;
/// The expiry `Graveyard._clear` registers: `type(uint64).max - GRACE_PERIOD`.
const CLEANUP_EXPIRY: u64 = u64::MAX - GRACE_PERIOD as u64;
/// The owner a subname of the claimed name is given before the claim.
const SUB_OWNER: &str = "0x0000000000000000000000000000000000000052";

mod ens_v2_registry {
    use alloy_sol_types::sol;

    sol! {
        event LabelUnregistered(uint256 indexed tokenId, address indexed sender);
    }
}

mod registrar {
    use alloy_sol_types::sol;

    sol! {
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    }
}

#[tokio::test]
async fn a_migrated_grant_held_by_the_graveyard_never_serves_after_an_ens_v2_unregister()
-> TestResult {
    let database = family_database("interpret_graveyard_migrated").await?;
    let pool = database.pool();
    sync_sepolia_manifests(pool).await?;
    let label = b"activation-gate";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    let logical_name_id = format!("ens:{namehash:#x}");
    seed_lineage(pool).await?;
    insert_lineage(pool, LATER_BLOCK, LATER_BLOCK).await?;
    seed_predecessor_facts(pool, labelhash, namehash).await?;
    run(pool, SETUP_BLOCK, PREDECESSOR_BLOCK, None).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    seed_faithful_unwrapped_migration(pool, label, labelhash, namehash).await?;
    run(
        pool,
        MIGRATION_BLOCK,
        MIGRATION_BLOCK,
        Some(PREDECESSOR_BLOCK),
    )
    .await?;
    publish(pool, MIGRATION_BLOCK, FamilyMode::Rebuild).await?;
    let migrated = summary(pool, &logical_name_id).await?;
    assert_eq!(
        migrated["registration"]["authority_kind"], "ens_v2_registry",
        "{migrated:#}"
    );
    assert_eq!(migrated["control"]["registry_owner"], OWNER, "{migrated:#}");
    assert_not_graveyard_served(&migrated);

    // The ENSv2 owner unregisters the migrated name: it follows ENSv2 alone and reads as
    // released, never as the ENSv1 grant the Graveyard holds.
    // (upstream: .refs/ens_v2/contracts/src/registry/PermissionedRegistry.sol:L255-L258 @ ens_v2@a971bd64)
    let mut versioned = labelhash.0;
    versioned[28..].fill(0);
    insert_transaction(pool, LATER_BLOCK, ETH_REGISTRY).await?;
    insert_log(
        pool,
        LATER_BLOCK,
        0,
        ETH_REGISTRY,
        ens_v2_registry::LabelUnregistered {
            tokenId: U256::from_be_bytes(versioned),
            sender: OWNER.parse()?,
        }
        .encode_log_data(),
    )
    .await?;
    run(pool, LATER_BLOCK, LATER_BLOCK, Some(MIGRATION_BLOCK)).await?;
    publish(pool, LATER_BLOCK, FamilyMode::Normal).await?;
    let unregistered = summary(pool, &logical_name_id).await?;
    assert_eq!(
        unregistered["registration"]["status"], "released",
        "{unregistered:#}"
    );
    assert_eq!(
        unregistered["registration"]["registrant"],
        Value::Null,
        "{unregistered:#}"
    );
    assert_eq!(
        unregistered["registration"]["expiry"],
        Value::Null,
        "{unregistered:#}"
    );
    assert_eq!(
        unregistered["control"]["status"], "unregistered",
        "{unregistered:#}"
    );
    assert_not_graveyard_served(&unregistered);

    database.cleanup().await?;
    Ok(())
}

/// `Graveyard.clear` claims an expired `.eth` name and clears a subname of it by making itself
/// their registry owner. The claim's grant is cleanup evidence and never recorded; the registry
/// records the Graveyard holds are served with no owner, on the name and on its parent's
/// subnames page.
#[tokio::test]
async fn a_graveyard_claim_serves_no_owner_for_the_name_or_its_cleared_subname() -> TestResult {
    let database = family_database("interpret_graveyard_claim").await?;
    let pool = database.pool();
    sync_sepolia_manifests(pool).await?;
    let label = b"claimed-name";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    let logical_name_id = format!("ens:{namehash:#x}");
    let sub_label = keccak256(b"sub");
    let token = U256::from_be_bytes(labelhash.0);
    let owner = OWNER.parse::<Address>()?;
    let graveyard = GRAVEYARD.parse::<Address>()?;
    let sub_owner = SUB_OWNER.parse::<Address>()?;
    let expires = SETUP_BLOCK + 10;
    // The claim is past the old registration's grace period, so the name is available.
    let claimed_at = expires + GRACE_PERIOD + 1;
    seed_lineage(pool).await?;
    insert_lineage(pool, LATER_BLOCK, claimed_at).await?;
    seed_wrapped_then_unwrapped(pool, label, labelhash, namehash, expires).await?;
    // The owner gives the name a subname (`setSubnodeOwner`).
    // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L83 @ ens_v1@91c966f)
    insert_log(
        pool,
        PREDECESSOR_BLOCK,
        3,
        ENS_REGISTRY,
        ens_registry::NewOwner {
            node: namehash,
            label: sub_label,
            owner: sub_owner,
        }
        .encode_log_data(),
    )
    .await?;
    run(pool, SETUP_BLOCK, MIGRATION_BLOCK, None).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    publish(pool, MIGRATION_BLOCK, FamilyMode::Rebuild).await?;
    assert_eq!(
        subname_owners(pool, &logical_name_id).await?,
        [Some(SUB_OWNER.to_owned())]
    );

    // `clear(["sub.claimed-name.eth"])`: the registrar burns the lapsed token, mints it to the
    // Graveyard, gives it the node and registers it until `CLEANUP_EXPIRY`; the Graveyard then
    // takes the subname with `setSubnodeRecord`.
    // (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_sepolia_20260916@366de741)
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L155 @ ens_v1@91c966f)
    insert_transaction(pool, LATER_BLOCK, GRAVEYARD).await?;
    for (log, emitter, encoded) in [
        (
            0,
            BASE_REGISTRAR,
            base_registrar::Transfer {
                from: owner,
                to: Address::ZERO,
                tokenId: token,
            }
            .encode_log_data(),
        ),
        (
            1,
            BASE_REGISTRAR,
            base_registrar::Transfer {
                from: Address::ZERO,
                to: graveyard,
                tokenId: token,
            }
            .encode_log_data(),
        ),
        (
            2,
            ENS_REGISTRY,
            ens_registry::NewOwner {
                node: eth_node(),
                label: labelhash,
                owner: graveyard,
            }
            .encode_log_data(),
        ),
        (
            3,
            BASE_REGISTRAR,
            registrar::NameRegistered {
                id: token,
                owner: graveyard,
                expires: U256::from(CLEANUP_EXPIRY),
            }
            .encode_log_data(),
        ),
        (
            4,
            ENS_REGISTRY,
            ens_registry::NewOwner {
                node: namehash,
                label: sub_label,
                owner: graveyard,
            }
            .encode_log_data(),
        ),
    ] {
        insert_log(pool, LATER_BLOCK, log, emitter, encoded).await?;
    }
    run(pool, LATER_BLOCK, LATER_BLOCK, Some(MIGRATION_BLOCK)).await?;
    for mode in [FamilyMode::Normal, FamilyMode::Rebuild] {
        publish(pool, LATER_BLOCK, mode).await?;
        let claimed = summary(pool, &logical_name_id).await?;
        assert_eq!(
            claimed["control"]["registry_owner"],
            Value::Null,
            "{claimed:#}"
        );
        assert_not_graveyard_served(&claimed);
        // The cleared subname has no owner and no name row, so, like any such child, its
        // parent no longer lists it.
        assert!(subname_owners(pool, &logical_name_id).await?.is_empty());
    }

    database.cleanup().await?;
    Ok(())
}

/// A registrant that sends a live token to the Graveyard, which accepts any ERC721, keeps the
/// lease running: the name cannot be registered again before its expiry and grace period, so it
/// is served as the chain holds it, the Graveyard as registrant, until the lease lapses.
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L25 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L101-L104 @ ens_v1@91c966f)
#[tokio::test]
async fn a_live_grant_sent_to_the_graveyard_is_served_as_the_chain_holds_it() -> TestResult {
    let database = family_database("interpret_graveyard_sent").await?;
    let pool = database.pool();
    sync_sepolia_manifests(pool).await?;
    let label = b"sent-name";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    let logical_name_id = format!("ens:{namehash:#x}");
    let token = U256::from_be_bytes(labelhash.0);
    let expires = 1_900_000_000_i64;
    let lapsed_block = LATER_BLOCK + 1;
    seed_lineage(pool).await?;
    insert_lineage(pool, LATER_BLOCK, LATER_BLOCK).await?;
    insert_lineage(pool, lapsed_block, expires + GRACE_PERIOD + 1).await?;
    seed_wrapped_then_unwrapped(pool, label, labelhash, namehash, expires).await?;
    run(pool, SETUP_BLOCK, MIGRATION_BLOCK, None).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;

    insert_transaction(pool, LATER_BLOCK, BASE_REGISTRAR).await?;
    insert_log(
        pool,
        LATER_BLOCK,
        0,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: OWNER.parse()?,
            to: GRAVEYARD.parse()?,
            tokenId: token,
        }
        .encode_log_data(),
    )
    .await?;
    run(pool, LATER_BLOCK, LATER_BLOCK, Some(MIGRATION_BLOCK)).await?;
    publish(pool, LATER_BLOCK, FamilyMode::Rebuild).await?;
    let sent = summary(pool, &logical_name_id).await?;
    assert_eq!(sent["registration"]["status"], "active", "{sent:#}");
    assert_eq!(sent["registration"]["registrant"], GRAVEYARD, "{sent:#}");
    assert_eq!(sent["registration"]["expiry"], expires, "{sent:#}");
    // No `reclaim`: the registry record stays the owner's.
    assert_eq!(sent["control"]["registry_owner"], OWNER, "{sent:#}");

    run(pool, lapsed_block, lapsed_block, Some(LATER_BLOCK)).await?;
    publish(pool, lapsed_block, FamilyMode::Normal).await?;
    let lapsed = summary(pool, &logical_name_id).await?;
    assert_eq!(lapsed["registration"]["status"], "released", "{lapsed:#}");
    assert_eq!(
        lapsed["registration"]["released_at"],
        expires + GRACE_PERIOD + 1,
        "{lapsed:#}"
    );
    assert_eq!(
        lapsed["registration"]["lapsed_registration"]["registrant"], GRAVEYARD,
        "{lapsed:#}"
    );

    database.cleanup().await?;
    Ok(())
}

/// Nothing the served row says names the Graveyard or its cleanup expiry.
fn assert_not_graveyard_served(summary: &Value) {
    let text = summary.to_string().to_lowercase();
    assert!(!text.contains(GRAVEYARD), "{summary:#}");
    assert!(!text.contains(&CLEANUP_EXPIRY.to_string()), "{summary:#}");
    assert!(!text.contains(&i64::MAX.to_string()), "{summary:#}");
}

/// The owners the parent's subnames page serves, in page order.
async fn subname_owners(pool: &PgPool, parent: &str) -> TestResult<Vec<Option<String>>> {
    let page = bigname_storage::families::topology::load_children_shadow_page(
        pool,
        parent,
        &bigname_storage::ChildrenCurrentPageFilter::default(),
        None,
        10,
    )
    .await?;
    Ok(page.rows.into_iter().map(|row| row.owner).collect())
}

/// A database in the `bigname_phase` schema the family reads name, with the Interpret baseline.
async fn family_database(prefix: &str) -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(prefix)).await?;
    let pool = database.pool();
    sqlx::raw_sql("CREATE SCHEMA bigname_phase")
        .execute(pool)
        .await?;
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        let mut connection = pool.acquire().await?;
        sqlx::raw_sql("SET search_path TO bigname_phase, public")
            .execute(&mut *connection)
            .await?;
        connections.push(connection);
    }
    drop(connections);
    for script in [
        include_str!("../../../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../../../schema-v2/baseline/10_phase_state.sql"),
        include_str!("../../../../../schema-v2/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../../../schema-v2/baseline/12_project_generation_failures.sql"),
        include_str!("../../../../../schema-v2/baseline/13_interpret_decode_skips.sql"),
        include_str!("../../../../../schema-v2/baseline/14_discovery_watch_admissions.sql"),
    ] {
        sqlx::raw_sql(script).execute(pool).await?;
    }
    Ok(database)
}

/// A wrapped registration at `SETUP_BLOCK`, the admitted Sepolia controllers' only kind, which
/// names the surface, then an unwrap back to the owner at `PREDECESSOR_BLOCK`. The registrar
/// lease expires at `expires`.
async fn seed_wrapped_then_unwrapped(
    pool: &PgPool,
    label: &[u8],
    labelhash: B256,
    namehash: B256,
    expires: i64,
) -> TestResult {
    // (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L155 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L396 @ ens_v1@91c966f)
    let wrapper = NAME_WRAPPER.parse::<Address>()?;
    let owner = OWNER.parse::<Address>()?;
    let token = U256::from_be_bytes(labelhash.0);
    let mut dns_name = vec![u8::try_from(label.len())?];
    dns_name.extend_from_slice(label);
    dns_name.extend_from_slice(b"\x03eth\0");
    insert_transaction(pool, SETUP_BLOCK, BASE_REGISTRAR).await?;
    for (log, emitter, encoded) in [
        (
            0,
            BASE_REGISTRAR,
            base_registrar::Transfer {
                from: Address::ZERO,
                to: wrapper,
                tokenId: token,
            }
            .encode_log_data(),
        ),
        (
            1,
            ENS_REGISTRY,
            ens_registry::NewOwner {
                node: eth_node(),
                label: labelhash,
                owner: wrapper,
            }
            .encode_log_data(),
        ),
        (
            2,
            BASE_REGISTRAR,
            registrar::NameRegistered {
                id: token,
                owner: wrapper,
                expires: U256::from(expires),
            }
            .encode_log_data(),
        ),
        (
            3,
            NAME_WRAPPER,
            NameWrapped {
                node: namehash,
                name: dns_name.into(),
                owner,
                fuses: (1 << 16) | (1 << 17),
                expiry: u64::try_from(expires + GRACE_PERIOD)?,
            }
            .encode_log_data(),
        ),
    ] {
        insert_log(pool, SETUP_BLOCK, log, emitter, encoded).await?;
    }
    insert_transaction(pool, PREDECESSOR_BLOCK, NAME_WRAPPER).await?;
    for (log, emitter, encoded) in [
        (
            0,
            ENS_REGISTRY,
            ens_registry::Transfer {
                node: namehash,
                owner,
            }
            .encode_log_data(),
        ),
        (
            1,
            NAME_WRAPPER,
            NameUnwrapped {
                node: namehash,
                owner,
            }
            .encode_log_data(),
        ),
        (
            2,
            BASE_REGISTRAR,
            base_registrar::Transfer {
                from: wrapper,
                to: owner,
                tokenId: token,
            }
            .encode_log_data(),
        ),
    ] {
        insert_log(pool, PREDECESSOR_BLOCK, log, emitter, encoded).await?;
    }
    Ok(())
}

async fn sync_sepolia_manifests(pool: &PgPool) -> TestResult {
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(manifest_root)?).await?;
    Ok(())
}

async fn insert_lineage(pool: &PgPool, number: i64, timestamp: i64) -> TestResult {
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, parent_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, $4, to_timestamp($5), 'canonical')",
    )
    .bind(CHAIN)
    .bind(block_hash(number))
    .bind(block_hash(number - 1))
    .bind(number)
    .bind(timestamp)
    .execute(pool)
    .await?;
    Ok(())
}

async fn run(pool: &PgPool, from: i64, to: i64, resume: Option<i64>) -> TestResult {
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: from,
            to_block: to,
            resume_current: resume.map(|number| Marker {
                number,
                hash: block_hash(number),
            }),
            mode: RunMode::Normal,
        })
        .await?;
    Ok(())
}

async fn publish(pool: &PgPool, target: i64, mode: FamilyMode) -> TestResult {
    let marker = bigname_project::Marker {
        number: target,
        hash: block_hash(target),
    };
    let token = families::input_token(pool, CHAIN).await?;
    let outcome = families::apply(
        pool,
        CHAIN,
        &marker,
        mode,
        &token,
        &FamilyOptions::new(bigname_content_hash::INTERPRETER_CONTENT_HASH),
    )
    .await?;
    assert_eq!(outcome.marker, Some(marker));
    Ok(())
}

async fn summary(pool: &PgPool, logical_name_id: &str) -> TestResult<Value> {
    Ok(
        bigname_storage::families::name::load_family_name(pool, logical_name_id)
            .await?
            .ok_or("published family name")?
            .declared_summary,
    )
}
