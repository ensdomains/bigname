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
/// The holders a wrapped subname's token moves to after the Graveyard cleared it.
const CAROL: &str = "0x0000000000000000000000000000000000000053";
const DAVE: &str = "0x0000000000000000000000000000000000000054";

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

mod name_wrapper {
    use alloy_sol_types::sol;

    sol! {
        event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values);
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
    assert_eq!(
        sent["registration"]["expiry"],
        expires.to_string(),
        "{sent:#}"
    );
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
        lapsed["registration"]["lapsed_registration"]["owner"], GRAVEYARD,
        "{lapsed:#}"
    );
    assert!(
        lapsed["registration"]["lapsed_registration"]
            .get("registrant")
            .is_none(),
        "{lapsed:#}"
    );

    database.cleanup().await?;
    Ok(())
}

/// A subname wrapped without `PARENT_CANNOT_CONTROL` under an unwrapped parent keeps a live
/// NameWrapper token when the Graveyard claims the lapsed parent and clears the subname's
/// registry record, and its holder can still transfer that token. The registry record the
/// Graveyard holds decides: no owner is served after the clear, whatever single or batch
/// transfer follows, in both normal and rebuild runs.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L374 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L155-L197 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L281-L303 @ ens_v1@91c966f)
#[tokio::test]
async fn a_wrapped_subname_the_graveyard_cleared_serves_no_owner_after_wrapper_transfers()
-> TestResult {
    let database = family_database("interpret_graveyard_wrapped_sub").await?;
    let pool = database.pool();
    sync_sepolia_manifests(pool).await?;
    let fixture = ClearedSubname::seed(pool).await?;
    // `wrap("sub.claimed-name.eth", SUB_OWNER, 0)`: the registry record moves to the NameWrapper,
    // which mints the token with no fuses and no expiry.
    fixture.wrap(pool, MIGRATION_BLOCK).await?;
    run(pool, SETUP_BLOCK, MIGRATION_BLOCK, None).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    publish(pool, MIGRATION_BLOCK, FamilyMode::Rebuild).await?;
    let wrapped = summary(pool, &fixture.sub_id).await?;
    assert_eq!(
        wrapped["control"]["registry_owner"],
        Value::String(SUB_OWNER.to_owned()),
        "{wrapped:#}"
    );
    assert_eq!(
        wrapped["control"]["owner"],
        Value::String(SUB_OWNER.to_owned()),
        "{wrapped:#}"
    );

    let claimed_at = fixture.claim(pool, LATER_BLOCK).await?;
    let carol = CAROL.parse::<Address>()?;
    let dave = DAVE.parse::<Address>()?;
    let sub_owner = SUB_OWNER.parse::<Address>()?;
    insert_lineage(pool, LATER_BLOCK + 1, claimed_at + 12).await?;
    insert_transaction(pool, LATER_BLOCK + 1, NAME_WRAPPER).await?;
    insert_log(
        pool,
        LATER_BLOCK + 1,
        0,
        NAME_WRAPPER,
        TransferSingle {
            operator: sub_owner,
            from: sub_owner,
            to: carol,
            id: fixture.sub_token(),
            value: U256::from(1),
        }
        .encode_log_data(),
    )
    .await?;
    insert_lineage(pool, LATER_BLOCK + 2, claimed_at + 24).await?;
    insert_transaction(pool, LATER_BLOCK + 2, NAME_WRAPPER).await?;
    insert_log(
        pool,
        LATER_BLOCK + 2,
        0,
        NAME_WRAPPER,
        name_wrapper::TransferBatch {
            operator: carol,
            from: carol,
            to: dave,
            ids: vec![fixture.sub_token()],
            values: vec![U256::from(1)],
        }
        .encode_log_data(),
    )
    .await?;
    let mut resume = MIGRATION_BLOCK;
    for block in [LATER_BLOCK, LATER_BLOCK + 1, LATER_BLOCK + 2] {
        run(pool, block, block, Some(resume)).await?;
        resume = block;
        for mode in [FamilyMode::Normal, FamilyMode::Rebuild] {
            let run = format!("block {block} {mode:?}");
            publish(pool, block, mode).await?;
            let served = summary(pool, &fixture.sub_id).await?;
            assert_eq!(
                served["control"]["registry_owner"],
                Value::Null,
                "{run}: {served:#}"
            );
            // The surviving NameWrapper token's holder is not the owner of a burned record.
            assert_eq!(served["control"]["owner"], Value::Null, "{run}: {served:#}");
            for holder in [SUB_OWNER, CAROL, DAVE] {
                let relations = address_relations(pool, holder, &fixture.sub_id).await?;
                assert!(
                    !relations.contains(&bigname_storage::AddressNameRelation::TokenHolder),
                    "{run}: {holder} owns a burned record: {relations:?}"
                );
            }
        }
    }

    database.cleanup().await?;
    Ok(())
}

/// A subname that was wrapped, unwrapped and then given to another owner is a registry-only
/// name with a surface, so it has an ordinary address-names row. Once the Graveyard clears its
/// registry record it is listed under no one: not under the Graveyard, and no longer under its
/// earlier owner. The parent the Graveyard claimed is not listed under it either.
#[tokio::test]
async fn a_surfaced_subname_the_graveyard_cleared_is_not_listed_under_the_graveyard() -> TestResult
{
    let database = family_database("interpret_graveyard_surfaced_sub").await?;
    let pool = database.pool();
    sync_sepolia_manifests(pool).await?;
    let fixture = ClearedSubname::seed(pool).await?;
    fixture.wrap(pool, MIGRATION_BLOCK).await?;
    let carol = CAROL.parse::<Address>()?;
    let sub_owner = SUB_OWNER.parse::<Address>()?;
    // `unwrap(parent, "sub", SUB_OWNER)`, then SUB_OWNER gives the record to CAROL.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1032 @ ens_v1@91c966f)
    insert_lineage(pool, LATER_BLOCK, MIGRATION_BLOCK + 1).await?;
    insert_transaction(pool, LATER_BLOCK, NAME_WRAPPER).await?;
    for (log, emitter, encoded) in [
        (
            0,
            NAME_WRAPPER,
            TransferSingle {
                operator: sub_owner,
                from: sub_owner,
                to: Address::ZERO,
                id: fixture.sub_token(),
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            1,
            ENS_REGISTRY,
            ens_registry::Transfer {
                node: fixture.sub_node,
                owner: sub_owner,
            }
            .encode_log_data(),
        ),
        (
            2,
            NAME_WRAPPER,
            NameUnwrapped {
                node: fixture.sub_node,
                owner: sub_owner,
            }
            .encode_log_data(),
        ),
    ] {
        insert_log(pool, LATER_BLOCK, log, emitter, encoded).await?;
    }
    insert_lineage(pool, LATER_BLOCK + 1, MIGRATION_BLOCK + 2).await?;
    insert_transaction(pool, LATER_BLOCK + 1, ENS_REGISTRY).await?;
    insert_log(
        pool,
        LATER_BLOCK + 1,
        0,
        ENS_REGISTRY,
        ens_registry::Transfer {
            node: fixture.sub_node,
            owner: carol,
        }
        .encode_log_data(),
    )
    .await?;
    run(pool, SETUP_BLOCK, LATER_BLOCK + 1, None).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    publish(pool, LATER_BLOCK + 1, FamilyMode::Rebuild).await?;
    assert!(
        address_names(pool, CAROL).await?.contains(&fixture.sub_id),
        "the surfaced subname is listed under its registry owner"
    );

    fixture.claim(pool, LATER_BLOCK + 2).await?;
    run(
        pool,
        LATER_BLOCK + 2,
        LATER_BLOCK + 2,
        Some(LATER_BLOCK + 1),
    )
    .await?;
    for mode in [FamilyMode::Normal, FamilyMode::Rebuild] {
        let run = format!("{mode:?}");
        publish(pool, LATER_BLOCK + 2, mode).await?;
        let served = summary(pool, &fixture.sub_id).await?;
        assert_eq!(
            served["control"]["registry_owner"],
            Value::Null,
            "{run}: {served:#}"
        );
        // Neither the cleared subname nor the claimed parent is listed under the Graveyard.
        let graveyard_names = address_names(pool, GRAVEYARD).await?;
        assert!(graveyard_names.is_empty(), "{run}: {graveyard_names:?}");
        assert!(
            !address_names(pool, CAROL).await?.contains(&fixture.sub_id),
            "{run}"
        );
    }
    // The Graveyard's registry write names no owner, so it is no owner-history anchor, while
    // CAROL's earlier tokenless ownership is.
    assert!(
        owner_history(pool, GRAVEYARD, LATER_BLOCK + 2)
            .await?
            .is_empty()
    );
    assert!(
        owner_history(pool, CAROL, LATER_BLOCK + 2)
            .await?
            .iter()
            .any(|row| row == &format!("AuthorityTransferred@{}", LATER_BLOCK + 1))
    );

    database.cleanup().await?;
    Ok(())
}

pub(super) async fn owner_history(
    pool: &PgPool,
    address: &str,
    block: i64,
) -> TestResult<Vec<String>> {
    let page = bigname_storage::load_address_history_page_for_relations(
        pool,
        address,
        None,
        Some(&[bigname_storage::AddressNameRelation::TokenHolder]),
        bigname_storage::HistoryScope::Both,
        false,
        None,
        50,
        bigname_storage::HistorySummaryMode::None,
        &bigname_storage::HistoryPageOptions {
            publication_block_bounds: Some(std::collections::BTreeMap::from([(
                CHAIN.to_owned(),
                block,
            )])),
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
                "{}@{}",
                row.event_kind,
                row.block_number.unwrap_or_default()
            )
        })
        .collect())
}

/// The Graveyard clears the registry record of a subname wrapped without
/// `PARENT_CANNOT_CONTROL`, and the holder then sends the surviving token to the Graveyard, which
/// accepts ERC1155 tokens. The registry write already ended the NameWrapper authority, so the
/// stale token's transfer lists the Graveyard as nothing.
/// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L25 @ ens_v2_sepolia_20260916@366de741)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1076-L1079 @ ens_v1@91c966f)
#[tokio::test]
async fn a_surviving_wrapper_token_sent_to_the_graveyard_does_not_list_it_as_manager() -> TestResult
{
    let database = family_database("interpret_graveyard_token_back").await?;
    let pool = database.pool();
    sync_sepolia_manifests(pool).await?;
    let fixture = ClearedSubname::seed(pool).await?;
    fixture.wrap(pool, MIGRATION_BLOCK).await?;
    run(pool, SETUP_BLOCK, MIGRATION_BLOCK, None).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    let claimed_at = fixture.claim(pool, LATER_BLOCK).await?;
    let sub_owner = SUB_OWNER.parse::<Address>()?;
    insert_lineage(pool, LATER_BLOCK + 1, claimed_at + 12).await?;
    insert_transaction(pool, LATER_BLOCK + 1, NAME_WRAPPER).await?;
    insert_log(
        pool,
        LATER_BLOCK + 1,
        0,
        NAME_WRAPPER,
        TransferSingle {
            operator: sub_owner,
            from: sub_owner,
            to: GRAVEYARD.parse::<Address>()?,
            id: fixture.sub_token(),
            value: U256::from(1),
        }
        .encode_log_data(),
    )
    .await?;
    run(pool, LATER_BLOCK, LATER_BLOCK + 1, Some(MIGRATION_BLOCK)).await?;
    for mode in [FamilyMode::Normal, FamilyMode::Rebuild] {
        let run = format!("{mode:?}");
        publish(pool, LATER_BLOCK + 1, mode).await?;
        let served = summary(pool, &fixture.sub_id).await?;
        assert_eq!(
            served["control"]["registry_owner"],
            Value::Null,
            "{run}: {served:#}"
        );
        let relations = address_relations(pool, GRAVEYARD, &fixture.sub_id).await?;
        assert!(relations.is_empty(), "{run}: {relations:?}");
    }

    database.cleanup().await?;
    Ok(())
}

/// The parent's owner reassigns a subname wrapped without `PARENT_CANNOT_CONTROL` with the
/// registry's `setSubnodeOwner`. No `NameUnwrapped` follows and the token is not burned, but the
/// NameWrapper no longer holds the registry record, so the name is no longer wrapped: owner,
/// manager and both relations follow the new registry owner, and later transfers of the stale
/// token move nothing.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1076-L1079 @ ens_v1@91c966f)
#[tokio::test]
async fn a_parent_reassigning_a_wrapped_subname_serves_the_new_registry_owner() -> TestResult {
    let database = family_database("interpret_wrapped_sub_reassigned").await?;
    let pool = database.pool();
    sync_sepolia_manifests(pool).await?;
    let fixture = ClearedSubname::seed(pool).await?;
    fixture.wrap(pool, MIGRATION_BLOCK).await?;
    run(pool, SETUP_BLOCK, MIGRATION_BLOCK, None).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    publish(pool, MIGRATION_BLOCK, FamilyMode::Rebuild).await?;
    let wrapped = summary(pool, &fixture.sub_id).await?;
    assert_eq!(wrapped["control"]["owner"], SUB_OWNER, "{wrapped:#}");
    assert!(wrapped.to_string().contains("wrapper_state"), "{wrapped:#}");

    let carol = CAROL.parse::<Address>()?;
    let sub_owner = SUB_OWNER.parse::<Address>()?;
    insert_lineage(pool, LATER_BLOCK, MIGRATION_BLOCK + 1).await?;
    insert_transaction(pool, LATER_BLOCK, OWNER).await?;
    insert_log(
        pool,
        LATER_BLOCK,
        0,
        ENS_REGISTRY,
        ens_registry::NewOwner {
            node: fixture.namehash,
            label: fixture.sub_label,
            owner: carol,
        }
        .encode_log_data(),
    )
    .await?;
    insert_lineage(pool, LATER_BLOCK + 1, MIGRATION_BLOCK + 2).await?;
    insert_transaction(pool, LATER_BLOCK + 1, NAME_WRAPPER).await?;
    insert_log(
        pool,
        LATER_BLOCK + 1,
        0,
        NAME_WRAPPER,
        TransferSingle {
            operator: sub_owner,
            from: sub_owner,
            to: DAVE.parse::<Address>()?,
            id: fixture.sub_token(),
            value: U256::from(1),
        }
        .encode_log_data(),
    )
    .await?;
    let mut resume = MIGRATION_BLOCK;
    for block in [LATER_BLOCK, LATER_BLOCK + 1] {
        run(pool, block, block, Some(resume)).await?;
        resume = block;
        for mode in [FamilyMode::Normal, FamilyMode::Rebuild] {
            let run = format!("block {block} {mode:?}");
            publish(pool, block, mode).await?;
            let served = summary(pool, &fixture.sub_id).await?;
            assert_eq!(
                served["control"]["registry_owner"], CAROL,
                "{run}: {served:#}"
            );
            assert_eq!(served["control"]["owner"], CAROL, "{run}: {served:#}");
            assert!(
                !served.to_string().contains("wrapper_state"),
                "{run}: {served:#}"
            );
            let relations = address_relations(pool, CAROL, &fixture.sub_id).await?;
            for relation in [
                bigname_storage::AddressNameRelation::TokenHolder,
                bigname_storage::AddressNameRelation::EffectiveController,
            ] {
                assert!(relations.contains(&relation), "{run}: {relations:?}");
            }
            for holder in [SUB_OWNER, DAVE] {
                let relations = address_relations(pool, holder, &fixture.sub_id).await?;
                assert!(relations.is_empty(), "{run}: {holder}: {relations:?}");
            }
        }
    }

    database.cleanup().await?;
    Ok(())
}

/// The parent's owner deletes a subname wrapped without `PARENT_CANNOT_CONTROL` by setting its
/// registry owner to zero. The NameWrapper no longer holds the record, so the name has no owner
/// and the old token holder is listed under nothing, even after moving the stale token.
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1076-L1079 @ ens_v1@91c966f)
#[tokio::test]
async fn a_parent_deleting_a_wrapped_subname_serves_no_owner() -> TestResult {
    let database = family_database("interpret_wrapped_sub_deleted").await?;
    let pool = database.pool();
    sync_sepolia_manifests(pool).await?;
    let fixture = ClearedSubname::seed(pool).await?;
    fixture.wrap(pool, MIGRATION_BLOCK).await?;
    let sub_owner = SUB_OWNER.parse::<Address>()?;
    insert_lineage(pool, LATER_BLOCK, MIGRATION_BLOCK + 1).await?;
    insert_transaction(pool, LATER_BLOCK, OWNER).await?;
    insert_log(
        pool,
        LATER_BLOCK,
        0,
        ENS_REGISTRY,
        ens_registry::NewOwner {
            node: fixture.namehash,
            label: fixture.sub_label,
            owner: Address::ZERO,
        }
        .encode_log_data(),
    )
    .await?;
    insert_lineage(pool, LATER_BLOCK + 1, MIGRATION_BLOCK + 2).await?;
    insert_transaction(pool, LATER_BLOCK + 1, NAME_WRAPPER).await?;
    insert_log(
        pool,
        LATER_BLOCK + 1,
        0,
        NAME_WRAPPER,
        TransferSingle {
            operator: sub_owner,
            from: sub_owner,
            to: DAVE.parse::<Address>()?,
            id: fixture.sub_token(),
            value: U256::from(1),
        }
        .encode_log_data(),
    )
    .await?;
    run(pool, SETUP_BLOCK, LATER_BLOCK + 1, None).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    for mode in [FamilyMode::Rebuild, FamilyMode::Normal] {
        let run = format!("{mode:?}");
        publish(pool, LATER_BLOCK + 1, mode).await?;
        let served = summary(pool, &fixture.sub_id).await?;
        assert_eq!(served["control"]["owner"], Value::Null, "{run}: {served:#}");
        assert!(
            !served.to_string().contains("wrapper_state"),
            "{run}: {served:#}"
        );
        for holder in [SUB_OWNER, DAVE] {
            let relations = address_relations(pool, holder, &fixture.sub_id).await?;
            assert!(relations.is_empty(), "{run}: {holder}: {relations:?}");
        }
    }

    database.cleanup().await?;
    Ok(())
}

/// A wrapped subname unwrapped to its holder, whose registry record that holder then gives to
/// another owner, is owned by the new registry owner: the closed NameWrapper binding's token
/// names no registrant of the registry-only binding that follows it.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1022-L1031 @ ens_v1@91c966f)
#[tokio::test]
async fn an_unwrapped_subname_given_to_another_owner_serves_that_owner() -> TestResult {
    let database = family_database("interpret_unwrapped_sub_transferred").await?;
    let pool = database.pool();
    sync_sepolia_manifests(pool).await?;
    let fixture = ClearedSubname::seed(pool).await?;
    fixture.wrap(pool, MIGRATION_BLOCK).await?;
    let sub_owner = SUB_OWNER.parse::<Address>()?;
    insert_lineage(pool, LATER_BLOCK, MIGRATION_BLOCK + 1).await?;
    insert_transaction(pool, LATER_BLOCK, NAME_WRAPPER).await?;
    for (log, emitter, encoded) in [
        (
            0,
            NAME_WRAPPER,
            TransferSingle {
                operator: sub_owner,
                from: sub_owner,
                to: Address::ZERO,
                id: fixture.sub_token(),
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            1,
            ENS_REGISTRY,
            ens_registry::Transfer {
                node: fixture.sub_node,
                owner: sub_owner,
            }
            .encode_log_data(),
        ),
        (
            2,
            NAME_WRAPPER,
            NameUnwrapped {
                node: fixture.sub_node,
                owner: sub_owner,
            }
            .encode_log_data(),
        ),
    ] {
        insert_log(pool, LATER_BLOCK, log, emitter, encoded).await?;
    }
    insert_lineage(pool, LATER_BLOCK + 1, MIGRATION_BLOCK + 2).await?;
    insert_transaction(pool, LATER_BLOCK + 1, ENS_REGISTRY).await?;
    insert_log(
        pool,
        LATER_BLOCK + 1,
        0,
        ENS_REGISTRY,
        ens_registry::Transfer {
            node: fixture.sub_node,
            owner: CAROL.parse::<Address>()?,
        }
        .encode_log_data(),
    )
    .await?;
    run(pool, SETUP_BLOCK, LATER_BLOCK + 1, None).await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    for mode in [FamilyMode::Rebuild, FamilyMode::Normal] {
        let run = format!("{mode:?}");
        publish(pool, LATER_BLOCK + 1, mode).await?;
        let served = summary(pool, &fixture.sub_id).await?;
        assert_eq!(
            served["control"]["registry_owner"], CAROL,
            "{run}: {served:#}"
        );
        assert_eq!(served["control"]["owner"], CAROL, "{run}: {served:#}");
        let relations = address_relations(pool, SUB_OWNER, &fixture.sub_id).await?;
        assert!(relations.is_empty(), "{run}: {relations:?}");
    }

    database.cleanup().await?;
    Ok(())
}

/// `claimed-name.eth`, wrapped then unwrapped back to `OWNER`, with a subname `sub` whose
/// registry record `OWNER` holds; the Graveyard can claim the parent once its lease lapses.
struct ClearedSubname {
    labelhash: B256,
    namehash: B256,
    sub_label: B256,
    sub_node: B256,
    sub_id: String,
    expires: i64,
}

impl ClearedSubname {
    async fn seed(pool: &PgPool) -> TestResult<Self> {
        let label = b"claimed-name";
        let labelhash = keccak256(label);
        let namehash = eth_namehash(labelhash);
        let sub_label = keccak256(b"sub");
        let sub_node = keccak256([namehash.as_slice(), sub_label.as_slice()].concat());
        let expires = SETUP_BLOCK + 10;
        seed_lineage(pool).await?;
        seed_wrapped_then_unwrapped(pool, label, labelhash, namehash, expires).await?;
        insert_log(
            pool,
            PREDECESSOR_BLOCK,
            3,
            ENS_REGISTRY,
            ens_registry::NewOwner {
                node: namehash,
                label: sub_label,
                owner: OWNER.parse::<Address>()?,
            }
            .encode_log_data(),
        )
        .await?;
        Ok(Self {
            labelhash,
            namehash,
            sub_label,
            sub_node,
            sub_id: format!("ens:{sub_node:#x}"),
            expires,
        })
    }

    fn sub_token(&self) -> U256 {
        U256::from_be_bytes(self.sub_node.0)
    }

    /// `wrap("sub.claimed-name.eth", SUB_OWNER, 0)` by the record's owner at `block`.
    async fn wrap(&self, pool: &PgPool, block: i64) -> TestResult {
        let wrapper = NAME_WRAPPER.parse::<Address>()?;
        let owner = OWNER.parse::<Address>()?;
        let sub_owner = SUB_OWNER.parse::<Address>()?;
        insert_transaction(pool, block, NAME_WRAPPER).await?;
        for (log, emitter, encoded) in [
            (
                0,
                ENS_REGISTRY,
                ens_registry::Transfer {
                    node: self.sub_node,
                    owner: wrapper,
                }
                .encode_log_data(),
            ),
            (
                1,
                NAME_WRAPPER,
                TransferSingle {
                    operator: owner,
                    from: Address::ZERO,
                    to: sub_owner,
                    id: self.sub_token(),
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
            (
                2,
                NAME_WRAPPER,
                NameWrapped {
                    node: self.sub_node,
                    name: b"\x03sub\x0cclaimed-name\x03eth\0".to_vec().into(),
                    owner: sub_owner,
                    fuses: 0,
                    expiry: 0,
                }
                .encode_log_data(),
            ),
        ] {
            insert_log(pool, block, log, emitter, encoded).await?;
        }
        Ok(())
    }

    /// `clear(["sub.claimed-name.eth"])` at `block`, past the parent's grace period: the
    /// Graveyard claims the parent, then takes the subname's record with `setSubnodeRecord`.
    /// Returns the block's timestamp.
    /// (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/migration/Graveyard.sol:L142-L172 @ ens_v2_sepolia_20260916@366de741)
    async fn claim(&self, pool: &PgPool, block: i64) -> TestResult<i64> {
        let owner = OWNER.parse::<Address>()?;
        let graveyard = GRAVEYARD.parse::<Address>()?;
        let token = U256::from_be_bytes(self.labelhash.0);
        let claimed_at = self.expires + GRACE_PERIOD + 1;
        insert_lineage(pool, block, claimed_at).await?;
        insert_transaction(pool, block, GRAVEYARD).await?;
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
                    label: self.labelhash,
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
                    node: self.namehash,
                    label: self.sub_label,
                    owner: graveyard,
                }
                .encode_log_data(),
            ),
        ] {
            insert_log(pool, block, log, emitter, encoded).await?;
        }
        Ok(claimed_at)
    }
}

/// The names the address-names read lists for `address` in the `ens` namespace.
async fn address_names(pool: &PgPool, address: &str) -> TestResult<Vec<String>> {
    let page = bigname_storage::load_address_names_current_page_filtered(
        pool,
        address,
        Some("ens"),
        None,
        bigname_storage::AddressNamesCurrentDedupe::Surface,
        None,
        None,
        None,
        bigname_storage::AddressNamesCurrentSort::Name,
        bigname_storage::AddressNamesCurrentOrder::Asc,
        None,
        50,
    )
    .await?;
    Ok(page
        .entries
        .into_iter()
        .map(|entry| entry.logical_name_id)
        .collect())
}

/// The relations the address-names read lists `address` under for `logical_name_id`.
async fn address_relations(
    pool: &PgPool,
    address: &str,
    logical_name_id: &str,
) -> TestResult<Vec<bigname_storage::AddressNameRelation>> {
    let page = bigname_storage::load_address_names_current_page_filtered(
        pool,
        address,
        Some("ens"),
        None,
        bigname_storage::AddressNamesCurrentDedupe::Surface,
        None,
        None,
        None,
        bigname_storage::AddressNamesCurrentSort::Name,
        bigname_storage::AddressNamesCurrentOrder::Asc,
        None,
        50,
    )
    .await?;
    Ok(page
        .entries
        .into_iter()
        .filter(|entry| entry.logical_name_id == logical_name_id)
        .flat_map(|entry| entry.relations)
        .collect())
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
        include_str!("../../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../../storage/schema/baseline/02_raw_facts.sql"),
        include_str!("../../../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../../../storage/schema/baseline/05_normalized_events.sql"),
        include_str!("../../../../storage/schema/baseline/06_projections.sql"),
        include_str!("../../../../storage/schema/baseline/07_labels.sql"),
        include_str!("../../../../storage/schema/baseline/08_heartbeats.sql"),
        include_str!("../../../../storage/schema/baseline/09_divergence.sql"),
        include_str!("../../../../storage/schema/baseline/10_phase_state.sql"),
        include_str!("../../../../storage/schema/baseline/11_manifest_authority_attestations.sql"),
        include_str!("../../../../storage/schema/baseline/12_project_generation_failures.sql"),
        include_str!("../../../../storage/schema/baseline/13_interpret_decode_skips.sql"),
        include_str!("../../../../storage/schema/baseline/14_discovery_watch_admissions.sql"),
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
