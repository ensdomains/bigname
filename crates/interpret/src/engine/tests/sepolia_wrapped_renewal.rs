use super::*;

mod renewal_events {
    use alloy_sol_types::sol;

    sol! {
        event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
        event NameRenewed(uint256 indexed id, uint256 expires);
    }
}

mod wrapped_controller {
    use alloy_sol_types::sol;

    sol! {
        event NameRenewed(string name, bytes32 indexed label, uint256 cost, uint256 expires);
    }
}

/// The Sepolia `WrappedETHRegistrarController` the checked-in registrar manifest admits for its
/// renewals.
/// (upstream: .refs/basenames/lib/ens-contracts/deployments/sepolia/ETHRegistrarController.json:L2 @ basenames@1809bbc)
const WRAPPED_CONTROLLER: &str = "0xfed6a969aaa60e4961fcd3ebf1a2e8913ac65b72";
/// The second NameWrapper-enabled wrapped controller, admitted from chain evidence under its own
/// role (docs/upstream.md, "Second Sepolia wrapped registrar controller admitted from chain
/// evidence").
const SECOND_WRAPPED_CONTROLLER: &str = "0x4477cac137f3353ca35060e01e5aeb777a1ca01b";
const REGISTRATION_BLOCK: i64 = SETUP_BLOCK;
const RENEWAL_BLOCK: i64 = PREDECESSOR_BLOCK;
const REGISTRAR_EXPIRY: u64 = 1_900_000_000;
const RENEWED_EXPIRY: u64 = 1_950_000_000;
const GRACE_PERIOD: u64 = 90 * 24 * 60 * 60;
/// `PARENT_CANNOT_CONTROL | IS_DOT_ETH`, the fuses `_wrapETH2LD` always adds.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1015 @ ens_v1@91c966f)
const DOT_ETH_FUSES: u32 = 0x10000 | 0x20000;

/// An `ExpiryChanged` row: resource, source family, expiry and authority kind.
type ExpiryRow = (Option<Uuid>, String, Option<i64>, Option<String>);

/// Mirrors `nick.eth` on Sepolia: a `.eth` name registered straight into the NameWrapper, then
/// renewed through the wrapped controller (block 8177214). `NameWrapper.renew` renews the
/// registrar and writes the wrapper expiry with no event of its own, so the BaseRegistrar
/// `NameRenewed` moves only the registrar expiry and the controller's label-bearing `NameRenewed`
/// is the only fact that carries the wrapper renewal.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L312-L337 @ ens_v1@91c966f)
///
/// With the controller admitted by the checked-in Sepolia manifest, the renewal must move the
/// wrapper resource's expiry to the renewed registrar expiry plus the grace period, the same in
/// a redo.
#[tokio::test]
async fn sepolia_wrapped_controller_renewal_moves_the_wrapper_expiry() -> TestResult {
    renewal_moves_the_wrapper_expiry(WRAPPED_CONTROLLER, "interpret_sepolia_wrapped_renewal").await
}

/// The same for a renewal through the second wrapped controller, which the manifest declares
/// under its own role.
#[tokio::test]
async fn sepolia_second_wrapped_controller_renewal_moves_the_wrapper_expiry() -> TestResult {
    renewal_moves_the_wrapper_expiry(
        SECOND_WRAPPED_CONTROLLER,
        "interpret_sepolia_second_wrapped_renewal",
    )
    .await
}

async fn renewal_moves_the_wrapper_expiry(controller: &str, database_name: &str) -> TestResult {
    let database = database(database_name).await?;
    let pool = database.pool();
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(manifest_root)?).await?;
    seed_lineage(pool).await?;

    let label = b"renewedwrapped";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    let logical_name_id = format!("ens:{namehash:#x}");
    seed_wrapped_registration(pool, label, labelhash, namehash).await?;
    seed_wrapped_renewal(pool, controller, label, labelhash).await?;

    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: REGISTRATION_BLOCK,
            to_block: REGISTRATION_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    let wrapper: Uuid = sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events
         WHERE chain_id = $1 AND logical_name_id = $2
           AND source_family = 'ens_v1_wrapper_l1' AND event_kind = $3",
    )
    .bind(CHAIN)
    .bind(&logical_name_id)
    .bind(bigname_adapters::schema_v2::seam::SURFACE_BOUND_EVENT_KIND)
    .fetch_one(pool)
    .await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;

    for mode in [RunMode::Normal, RunMode::Redo] {
        Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.to_owned(),
                from_block: RENEWAL_BLOCK,
                to_block: RENEWAL_BLOCK,
                resume_current: Some(Marker {
                    number: REGISTRATION_BLOCK,
                    hash: block_hash(REGISTRATION_BLOCK),
                }),
                mode,
            })
            .await?;
        let renewals: Vec<ExpiryRow> = sqlx::query_as(
            "SELECT resource_id, source_family, (after_state ->> 'expiry')::bigint,
                    after_state ->> 'authority_kind'
             FROM normalized_events
             WHERE chain_id = $1 AND logical_name_id = $2 AND block_number = $3
               AND event_kind = 'ExpiryChanged'
             ORDER BY log_index, normalized_event_id",
        )
        .bind(CHAIN)
        .bind(&logical_name_id)
        .bind(RENEWAL_BLOCK)
        .fetch_all(pool)
        .await?;
        let wrapper_expiry = i64::try_from(RENEWED_EXPIRY + GRACE_PERIOD)?;
        assert!(
            renewals.iter().any(|(resource, family, expiry, kind)| {
                *resource == Some(wrapper)
                    && family == "ens_v1_registrar_l1"
                    && *expiry == Some(wrapper_expiry)
                    && kind.as_deref() == Some("wrapper")
            }),
            "{mode:?}: the controller renewal moves the wrapper expiry: {renewals:?}"
        );
        assert!(
            renewals.iter().any(|(resource, _, expiry, kind)| {
                *resource != Some(wrapper)
                    && *expiry == Some(i64::try_from(RENEWED_EXPIRY).unwrap_or_default())
                    && kind.as_deref() != Some("wrapper")
            }),
            "{mode:?}: the BaseRegistrar renewal still moves the registrar expiry: {renewals:?}"
        );
    }
    database.cleanup().await?;
    Ok(())
}

/// A name registered straight into the NameWrapper names the NameWrapper as its BaseRegistrar
/// registrant, but the NameWrapper never owns it: the wrapped token's holder does. Its owner
/// history has no anchor for that name.
#[tokio::test]
async fn a_wrapper_minted_registration_is_no_owner_history_of_the_name_wrapper() -> TestResult {
    let database = database("interpret_wrapper_minted_owner_history").await?;
    let pool = database.pool();
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(manifest_root)?).await?;
    seed_lineage(pool).await?;
    let label = b"mintedwrapped";
    let labelhash = keccak256(label);
    seed_wrapped_registration(pool, label, labelhash, eth_namehash(labelhash)).await?;
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: REGISTRATION_BLOCK,
            to_block: REGISTRATION_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    let wrapper =
        super::graveyard_burned::owner_history(pool, NAME_WRAPPER, REGISTRATION_BLOCK).await?;
    let holder = super::graveyard_burned::owner_history(pool, OWNER, REGISTRATION_BLOCK).await?;
    database.cleanup().await?;
    assert!(wrapper.is_empty(), "{wrapper:?}");
    assert!(!holder.is_empty(), "{holder:?}");
    Ok(())
}

/// The same registration when the wrapped token's receiver unwraps the name to `B` inside its
/// mint callback, before the outer `NameWrapped`: that wrap records no registrar resource, and
/// the NameWrapper still has no owner history of the name while `B`, who took the token, has.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L382-L395 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L894-L902 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L257-L258 @ ens_v1@91c966f)
#[tokio::test]
async fn a_wrapper_minted_registration_unwrapped_in_its_mint_callback_is_no_owner_history_of_the_name_wrapper()
-> TestResult {
    const CONTROLLER: &str = "0x00000000000000000000000000000000000000a1";
    const REGISTRANT: &str = "0x00000000000000000000000000000000000000b2";
    let database = database("interpret_wrapper_minted_callback_owner_history").await?;
    let pool = database.pool();
    let manifest_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(manifest_root)?).await?;
    seed_lineage(pool).await?;
    let label = b"callbackunwrapped";
    let labelhash = keccak256(label);
    let namehash = eth_namehash(labelhash);
    let receiver = OWNER.parse::<Address>()?;
    let name_wrapper = NAME_WRAPPER.parse::<Address>()?;
    let token = U256::from_be_bytes(labelhash.0);
    let mut dns_name = vec![u8::try_from(label.len())?];
    dns_name.extend_from_slice(label);
    dns_name.extend_from_slice(b"\x03eth\0");

    insert_transaction(pool, REGISTRATION_BLOCK, WRAPPED_CONTROLLER).await?;
    let logs = [
        (
            BASE_REGISTRAR,
            base_registrar::Transfer {
                from: Address::ZERO,
                to: name_wrapper,
                tokenId: token,
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            ens_registry::NewOwner {
                node: eth_node(),
                label: labelhash,
                owner: name_wrapper,
            }
            .encode_log_data(),
        ),
        (
            BASE_REGISTRAR,
            renewal_events::NameRegistered {
                id: token,
                owner: name_wrapper,
                expires: U256::from(REGISTRAR_EXPIRY),
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: name_wrapper,
                from: Address::ZERO,
                to: receiver,
                id: U256::from_be_bytes(namehash.0),
                value: U256::from(1_u64),
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: receiver,
                from: receiver,
                to: Address::ZERO,
                id: U256::from_be_bytes(namehash.0),
                value: U256::from(1_u64),
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            ens_registry::Transfer {
                node: namehash,
                owner: CONTROLLER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameUnwrapped {
                node: namehash,
                owner: CONTROLLER.parse()?,
            }
            .encode_log_data(),
        ),
        (
            BASE_REGISTRAR,
            base_registrar::Transfer {
                from: name_wrapper,
                to: REGISTRANT.parse()?,
                tokenId: token,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameWrapped {
                node: namehash,
                name: dns_name.into(),
                owner: receiver,
                fuses: DOT_ETH_FUSES,
                expiry: REGISTRAR_EXPIRY + GRACE_PERIOD,
            }
            .encode_log_data(),
        ),
    ];
    for (index, (emitter, data)) in logs.into_iter().enumerate() {
        insert_log(
            pool,
            REGISTRATION_BLOCK,
            i64::try_from(index)?,
            emitter,
            data,
        )
        .await?;
    }
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            from_block: REGISTRATION_BLOCK,
            to_block: REGISTRATION_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    let unlinked: Option<serde_json::Value> = sqlx::query_scalar(
        "SELECT after_state -> 'wrapped_registrar_resource_id' FROM normalized_events
         WHERE source_family = 'ens_v1_wrapper_l1' AND event_kind = $1
           AND after_state ->> 'source_event' = 'NameWrapped'",
    )
    .bind(bigname_adapters::schema_v2::seam::TOKEN_CONTROL_TRANSFERRED_EVENT_KIND)
    .fetch_optional(pool)
    .await?;
    let wrapper =
        super::graveyard_burned::owner_history(pool, NAME_WRAPPER, REGISTRATION_BLOCK).await?;
    let registrant =
        super::graveyard_burned::owner_history(pool, REGISTRANT, REGISTRATION_BLOCK).await?;
    database.cleanup().await?;
    assert_eq!(unlinked, Some(serde_json::Value::Null));
    assert!(wrapper.is_empty(), "{wrapper:?}");
    assert!(!registrant.is_empty(), "{registrant:?}");
    Ok(())
}

/// `registerAndWrapETH2LD` from the wrapped controller: the BaseRegistrar mints to the
/// NameWrapper, the registry names the NameWrapper owner, the BaseRegistrar emits its numeric
/// `NameRegistered`, and the NameWrapper mints the ERC-1155 token and emits `NameWrapped`. The
/// controller's own `NameRegistered` is not admitted on Sepolia and is left out.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
async fn seed_wrapped_registration(
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
    let token = U256::from_be_bytes(labelhash.0);

    insert_transaction(pool, REGISTRATION_BLOCK, WRAPPED_CONTROLLER).await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        0,
        BASE_REGISTRAR,
        base_registrar::Transfer {
            from: Address::ZERO,
            to: name_wrapper,
            tokenId: token,
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
        renewal_events::NameRegistered {
            id: token,
            owner: name_wrapper,
            expires: U256::from(REGISTRAR_EXPIRY),
        }
        .encode_log_data(),
    )
    .await?;
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
            expiry: REGISTRAR_EXPIRY + GRACE_PERIOD,
        }
        .encode_log_data(),
    )
    .await
}

/// `renew` on the wrapped controller: `NameWrapper.renew` calls `BaseRegistrar.renew`, which emits
/// the numeric `NameRenewed`, then the controller emits its label-bearing `NameRenewed`.
async fn seed_wrapped_renewal(
    pool: &PgPool,
    controller: &str,
    label: &[u8],
    labelhash: B256,
) -> TestResult {
    insert_transaction(pool, RENEWAL_BLOCK, controller).await?;
    insert_log(
        pool,
        RENEWAL_BLOCK,
        0,
        BASE_REGISTRAR,
        renewal_events::NameRenewed {
            id: U256::from_be_bytes(labelhash.0),
            expires: U256::from(RENEWED_EXPIRY),
        }
        .encode_log_data(),
    )
    .await?;
    insert_log(
        pool,
        RENEWAL_BLOCK,
        1,
        controller,
        wrapped_controller::NameRenewed {
            name: std::str::from_utf8(label)?.to_owned(),
            label: labelhash,
            cost: U256::from(1_u64),
            expires: U256::from(RENEWED_EXPIRY),
        }
        .encode_log_data(),
    )
    .await
}
