//! A `.eth` second-level name the 2017 registry recorded, later registered through the
//! BaseRegistrar, then claimed by the admitted Graveyard once the lease was past its expiry and
//! grace, from admitted logs through Interpret, Project and the public routes. The registration
//! wrote the node's first current-registry record, so the name is not one only the 2017 registry
//! recorded, and it serves the shape of a name the Graveyard holds.
//!
//! No name only the 2017 registry recorded is ever claimed. `clear` reverts on a label whose
//! `nameExpires` is zero, and only a BaseRegistrar registration sets it. The claim is itself a
//! registration, which burns the lapsed token, mints one to the Graveyard and writes the node's
//! current-registry record to it.
//! (upstream: .refs/ens_v2_mainnet_20261002/contracts/src/migration/Graveyard.sol:L157-L164 @ ens_v2_mainnet_20261002@4f47cbcc)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L96-L104 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L110-L152 @ ens_v1@91c966f)
use super::*;

mod registrar {
    use super::*;
    sol! {
        event Transfer(address indexed from, address indexed to, uint256 indexed tokenId);
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    }
}

/// The 2017 record's owner, who never holds the lease.
const RECORD_OWNER: &str = GRANTEE;
/// The block of the lease's registration.
const REGISTERED: i64 = 122;
/// The block of the Graveyard's claim.
const CLAIMED: i64 = 124;
/// The lease's expiry. Fixture blocks are a second apart, so it lies past its 90-day grace at
/// the claim's block, as `available` requires of the claim's registration.
const LAPSED_EXPIRY: u64 = 1_600_000_000;
/// The registration expiry the adapter reads as the Graveyard's: the largest `uint64` less the
/// 90-day grace period.
const CLEANUP_EXPIRY: u64 = u64::MAX - 90 * 86_400;

/// One BaseRegistrar `register` of `label` to `to`: the burn of a lapsed holder's token, the
/// mint, the current-registry write and `NameRegistered`, from log `index` on.
fn registration(
    (base_registrar, current): (Address, Address),
    label: &[u8],
    (from, to): (Option<Address>, Address),
    expires: u64,
    (block, index): (i64, i64),
) -> Vec<RawLogInput> {
    let id = U256::from_be_bytes(*keccak256(label));
    let transfer = |from: Address, to: Address, index: i64| {
        log(
            registrar::Transfer {
                from,
                to,
                tokenId: id,
            }
            .encode_log_data(),
            base_registrar,
            block,
            index,
        )
    };
    let mut logs = Vec::new();
    if let Some(from) = from {
        logs.push(transfer(from, Address::ZERO, index));
    }
    let index = index + i64::from(from.is_some());
    logs.extend([
        transfer(Address::ZERO, to, index),
        owner(node(&[b"eth"]), label, to, current, block, index + 1),
        log(
            registrar::NameRegistered {
                id,
                owner: to,
                expires: U256::from(expires),
            }
            .encode_log_data(),
            base_registrar,
            block,
            index + 2,
        ),
    ]);
    logs
}

#[tokio::test]
async fn a_graveyard_claim_of_a_lapsed_lease_over_a_2017_record_is_released() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let (registry, _) = admit_registry_and_resolvers(&database).await?;
    let migration =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v2_migration_l1", 2283).await?;
    let registrar_manifest =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registrar_l1", 2284).await?;
    let old = role_address(&registry, "registry_old");
    let current = role_address(&registry, "registry");
    let base_registrar = role_address(&registrar_manifest, "registrar");
    let graveyard = role_address(&migration, "graveyard");
    let holder: Address = HOLDER.parse()?;
    let mut logs = vec![
        owner(B256::ZERO, b"eth", holder, old, 120, 0),
        owner(node(&[b"eth"]), b"claimed", RECORD_OWNER.parse()?, old, 121, 0),
    ];
    logs.extend(registration(
        (base_registrar, current),
        b"claimed",
        (None, holder),
        LAPSED_EXPIRY,
        (REGISTERED, 0),
    ));
    logs.extend(registration(
        (base_registrar, current),
        b"claimed",
        (Some(holder), graveyard),
        CLEANUP_EXPIRY,
        (CLAIMED, 0),
    ));
    intake(&database, &logs, CLAIMED).await?;
    run(
        &bigname_interpret::Engine::new(database.pool.clone()),
        120,
        CLAIMED,
        bigname_interpret::RunMode::Normal,
    )
    .await?;
    publish(&database, CLAIMED).await?;

    // The fixture reaches the Graveyard: one registry write names it, and its registration is
    // the claim, not a lease.
    let named = node(&[b"claimed", b"eth"]);
    let held: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM project_registry_owner_event
         WHERE chain_id = $1 AND node = $2 AND event_kind = 'AuthorityTransferred'
           AND owner_getter_reason = 'graveyard'",
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
    let row = detail(&database, &[b"claimed", b"eth"]).await?;
    assert_eq!(row["status"], "released", "{row:#}");
    assert_eq!(row["authority"], "ens_v1", "{row:#}");
    assert_eq!(row["expires_at"], Value::Null, "{row:#}");
    assert_eq!(row["expires_at_reason"], "released", "{row:#}");
    for field in ["owner", "manager"] {
        assert!(row.get(field).is_none(), "{field}: {row:#}");
    }
    assert_eq!(row["lapsed_registration"]["owner"], HOLDER, "{row:#}");
    assert_eq!(
        row["lapsed_registration"]["released_at"],
        (1_700_000_000 + CLAIMED).to_string(),
        "{row:#}"
    );
    database.cleanup().await
}
