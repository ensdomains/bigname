//! A `.eth` second-level name the 2017 registry recorded and the admitted Graveyard then claimed,
//! from admitted logs through Interpret, Project and the public routes. The Graveyard registers
//! the expired name to itself with an expiry that never reaches grace, which makes it the token
//! holder and writes the node's first current-registry record. The name is not one only the 2017
//! registry recorded, so its served shape is the one a Graveyard-held record already has.
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L60-L84 @ ens_v1@91c966f)
use super::*;

mod registrar {
    use super::*;
    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    }
}

/// The expiry the Graveyard registers a claimed name with: the largest `uint64` less the 90-day
/// grace period.
const CLEANUP_EXPIRY: u64 = u64::MAX - 90 * 86_400;

/// The Mainnet migration manifest, with every declaration starting at block 0 so this fixture's
/// blocks are past the Graveyard's launch.
async fn admit_migration(database: &TestDatabase) -> Result<Value> {
    let mut migration =
        admit_family_from(database, "mainnet", CHAIN, "ens_v2_migration_l1", 2283).await?;
    for contract in migration["contracts"].as_array_mut().unwrap() {
        contract["start_block"] = json!(0);
    }
    sqlx::query("UPDATE manifest_versions SET manifest_payload=$2 WHERE manifest_id=$1")
        .bind(2283_i64)
        .bind(&migration)
        .execute(&database.pool)
        .await?;
    seed_fixture_manifest_update(
        &database.pool,
        2283,
        CHAIN,
        "ens",
        "ens_v2_migration_l1",
        &migration,
    )
    .await?;
    Ok(migration)
}

#[tokio::test]
async fn a_graveyard_held_name_keeps_its_served_shape() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (registry, _) = admit_registry_and_resolvers(&database).await?;
    let migration = admit_migration(&database).await?;
    let registrar_manifest =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registrar_l1", 2284).await?;
    let old = role_address(&registry, "registry_old");
    let current = role_address(&registry, "registry");
    let base_registrar = role_address(&registrar_manifest, "registrar");
    let graveyard = role_address(&migration, "graveyard");
    let eth = node(&[b"eth"]);
    let id = U256::from_be_bytes(*keccak256(b"burned"));
    let logs = vec![
        owner(B256::ZERO, b"eth", HOLDER.parse()?, old, 120, 0),
        owner(eth, b"burned", HOLDER.parse()?, old, 121, 0),
        log(
            registrar::Transfer {
                from: Address::ZERO,
                to: graveyard,
                tokenId: id,
            }
            .encode_log_data(),
            base_registrar,
            122,
            0,
        ),
        owner(eth, b"burned", graveyard, current, 122, 1),
        log(
            registrar::NameRegistered {
                id,
                owner: graveyard,
                expires: U256::from(CLEANUP_EXPIRY),
            }
            .encode_log_data(),
            base_registrar,
            122,
            2,
        ),
    ];
    intake(&database, &logs, 122).await?;
    run(
        &bigname_interpret::Engine::new(database.pool.clone()),
        120,
        122,
        bigname_interpret::RunMode::Normal,
    )
    .await?;
    publish(&database, 122).await?;

    // The fixture reaches the Graveyard: the registry write names it, and the registration is
    // its claim, not a lease.
    let named = node(&[b"burned", b"eth"]);
    let held: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_registry_owner_event
         WHERE chain_id = $1 AND node = $2 AND owner_getter_reason = 'graveyard'",
    )
    .bind(CHAIN)
    .bind(format!("{named:#x}"))
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(held, 1);
    let claims: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM normalized_events
         WHERE chain_id = $1 AND source_family = 'ens_v2_migration_l1'
           AND after_state ->> 'lifecycle_classification' = 'graveyard_cleanup'",
    )
    .bind(CHAIN)
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(claims, 1);

    let selection = bigname_storage::load_name_current(&database.pool, &format!("ens:{named:#x}"))
        .await?
        .context("the name has a composed row")?
        .provenance["authority_selection"]
        .clone();
    assert_eq!(selection["registry_generation"], "current", "{selection:#}");
    assert!(
        selection.get("old_record_without_lease").is_none(),
        "{selection:#}"
    );
    let row = detail(&database, &[b"burned", b"eth"]).await?;
    assert_eq!(row["status"], "active", "{row:#}");
    assert_eq!(row["authority"], "ens_v1", "{row:#}");
    for field in ["owner", "manager", "expires_at", "registered_at"] {
        assert!(row.get(field).is_none(), "{field}: {row:#}");
    }
    database.cleanup().await
}
