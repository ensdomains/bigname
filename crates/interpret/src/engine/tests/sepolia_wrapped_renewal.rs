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
    let database = database("interpret_sepolia_wrapped_renewal").await?;
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
    seed_wrapped_renewal(pool, label, labelhash).await?;

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
           AND source_family = 'ens_v1_wrapper_l1' AND event_kind = 'SurfaceBound'",
    )
    .bind(CHAIN)
    .bind(&logical_name_id)
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
async fn seed_wrapped_renewal(pool: &PgPool, label: &[u8], labelhash: B256) -> TestResult {
    insert_transaction(pool, RENEWAL_BLOCK, WRAPPED_CONTROLLER).await?;
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
        WRAPPED_CONTROLLER,
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
