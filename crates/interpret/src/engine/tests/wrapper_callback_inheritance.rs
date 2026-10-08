//! Complete source-valid setSubnodeOwner -> callback unwrap/generic-wrap receipt. The generic
//! inner completion emits 0/0 although `_mint` inherits the outer parent-controlled fuse/expiry.
//! (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L228-L278 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L347-L374 @ ens_v1@91c966f)
use super::*;

alloy_sol_types::sol! {
    event FusesSet(bytes32 indexed node, uint32 fuses);
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
}

const EXPIRY: u64 = 1_800_000_000;
const PCC: u32 = 1 << 16;

#[tokio::test]
async fn generic_callback_rewrap_keeps_inherited_fuses_and_expiry_through_lapse() -> TestResult {
    callback_inheritance(false).await
}

#[tokio::test]
async fn generic_callback_transfer_survives_both_late_completions() -> TestResult {
    callback_inheritance(true).await
}

async fn callback_inheritance(transfer: bool) -> TestResult {
    const SUCCESSOR: &str = "0x00000000000000000000000000000000000000b2";
    let database = database(if transfer {
        "interpret_generic_callback_transfer"
    } else {
        "interpret_generic_callback_inheritance"
    })
    .await?;
    let pool = database.pool();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../manifests/sepolia");
    sync_schema_v2_repository(pool, &load_repository(root)?).await?;
    for block in REGISTRATION_BLOCK..=RENEWAL_BLOCK {
        sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state)
            VALUES($1,$2,$3,$4,to_timestamp($4),'canonical')")
            .bind(CHAIN).bind(block_hash(block)).bind((block>REGISTRATION_BLOCK).then(|| block_hash(block-1)))
            .bind(block).execute(pool).await?;
    }
    let parent_label = b"callbackparent";
    let parent_hash = eth_namehash(keccak256(parent_label));
    seed_wrapped_registration(pool, parent_label, keccak256(parent_label), parent_hash).await?;
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        5,
        NAME_WRAPPER,
        FusesSet {
            node: parent_hash,
            fuses: DOT_ETH_FUSES | 1,
        }
        .encode_log_data(),
    )
    .await?;
    // R may give custody back to NameWrapper through generic wrap after its unwrap.
    insert_log(
        pool,
        REGISTRATION_BLOCK,
        6,
        ENS_REGISTRY,
        ApprovalForAll {
            owner: OWNER.parse()?,
            operator: NAME_WRAPPER.parse()?,
            approved: true,
        }
        .encode_log_data(),
    )
    .await?;
    let node = keccak256([parent_hash.as_slice(), keccak256(b"child").as_slice()].concat());
    let token = U256::from_be_bytes(node.0);
    let receiver = OWNER.parse::<Address>()?;
    let wrapper = NAME_WRAPPER.parse::<Address>()?;
    let mut dns = b"\x05child\x0ecallbackparent\x03eth\0".to_vec();
    // `callbackparent` has fourteen bytes; keep the exact raw DNS bytes in both completions.
    assert_eq!(parent_label.len(), 14);
    insert_transaction(pool, RENEWAL_BLOCK, NAME_WRAPPER).await?;
    let mut logs = vec![
        (
            ENS_REGISTRY,
            ens_registry::NewOwner {
                node: parent_hash,
                label: keccak256(b"child"),
                owner: wrapper,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: receiver,
                from: Address::ZERO,
                to: receiver,
                id: token,
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: receiver,
                from: receiver,
                to: Address::ZERO,
                id: token,
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            ens_registry::Transfer {
                node,
                owner: receiver,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameUnwrapped {
                node,
                owner: receiver,
            }
            .encode_log_data(),
        ),
        (
            ENS_REGISTRY,
            ens_registry::Transfer {
                node,
                owner: wrapper,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: receiver,
                from: Address::ZERO,
                to: receiver,
                id: token,
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameWrapped {
                node,
                name: dns.clone().into(),
                owner: receiver,
                fuses: 0,
                expiry: 0,
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameWrapped {
                node,
                name: std::mem::take(&mut dns).into(),
                owner: receiver,
                fuses: PCC,
                expiry: EXPIRY,
            }
            .encode_log_data(),
        ),
    ];
    if transfer {
        logs.insert(
            7,
            (
                NAME_WRAPPER,
                TransferSingle {
                    operator: receiver,
                    from: receiver,
                    to: SUCCESSOR.parse()?,
                    id: token,
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
        );
    }
    for (index, (emitter, data)) in logs.into_iter().enumerate() {
        insert_log(pool, RENEWAL_BLOCK, i64::try_from(index)?, emitter, data).await?;
    }
    Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.into(),
            from_block: REGISTRATION_BLOCK,
            to_block: RENEWAL_BLOCK,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    stamp_interpreter_hash(pool, bigname_content_hash::INTERPRETER_CONTENT_HASH).await?;
    super::super::registration_lifecycle::publish(
        pool,
        RENEWAL_BLOCK,
        bigname_project::families::FamilyMode::Rebuild,
    )
    .await?;
    let row = bigname_storage::families::name::load_family_name(pool, &format!("ens:{node:#x}"))
        .await?
        .ok_or("nested child")?;
    let fields = bigname_storage::public_name_fields::registration_fields(
        "ens",
        &row.declared_summary,
        row.resource_id.is_some(),
    );
    let ens_v1 =
        bigname_storage::public_name_fields::ens_v1(Some("ens_v1"), &row.declared_summary)?
            .ok_or("wrapper fields")?;
    assert_eq!(
        fields.owner.as_deref(),
        Some(if transfer { SUCCESSOR } else { OWNER })
    );
    assert_eq!(
        fields.manager.as_deref(),
        Some(if transfer { SUCCESSOR } else { OWNER })
    );
    assert_eq!(
        row.declared_summary["registration"]["expiry"],
        EXPIRY.to_string(),
        "inner generic 0/0 must retain the outer mint's expiry: {:?}",
        row.declared_summary
    );
    assert_eq!(
        ens_v1.wrapper_state,
        Some(bigname_storage::public_name_fields::WrapperState::Emancipated)
    );
    assert_eq!(ens_v1.wrapper_fuses.map(|fuses| fuses.fuses), Some(PCC));
    // Restore at each complete-block checkpoint, then publish time-only boundaries. No raw
    // wrapper operation occurs at E or E+1, so only the retained effective token data can lapse.
    for (offset, timestamp) in [EXPIRY, EXPIRY + 1].into_iter().enumerate() {
        let block = MIGRATION_BLOCK + offset as i64;
        sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,parent_hash,block_number,block_timestamp,canonicality_state)
            VALUES($1,$2,$3,$4,to_timestamp($5),'canonical')")
            .bind(CHAIN).bind(block_hash(block)).bind(block_hash(block-1)).bind(block)
            .bind(timestamp as f64).execute(pool).await?;
        Engine::new(pool.clone())
            .run_batch(BatchRequest {
                chain_id: CHAIN.into(),
                from_block: block,
                to_block: block,
                resume_current: Some(Marker {
                    number: block - 1,
                    hash: block_hash(block - 1),
                }),
                mode: RunMode::Normal,
            })
            .await?;
        super::super::registration_lifecycle::publish(
            pool,
            block,
            bigname_project::families::FamilyMode::Normal,
        )
        .await?;
        let row =
            bigname_storage::families::name::load_family_name(pool, &format!("ens:{node:#x}"))
                .await?
                .ok_or("child at clock boundary")?;
        let fields = bigname_storage::public_name_fields::registration_fields(
            "ens",
            &row.declared_summary,
            row.resource_id.is_some(),
        );
        assert_eq!(
            row.declared_summary["registration"]["expiry"],
            EXPIRY.to_string()
        );
        assert_eq!(
            row.declared_summary["registration"]["grace_ends_at"],
            EXPIRY.to_string()
        );
        let expected = (offset == 0).then_some(if transfer { SUCCESSOR } else { OWNER });
        assert_eq!(
            fields.owner.as_deref(),
            expected,
            "owner at {timestamp}: {:?}",
            row.declared_summary
        );
        assert_eq!(
            fields.manager.as_deref(),
            expected,
            "manager at {timestamp}: {:?}",
            row.declared_summary
        );
        let permissions = bigname_storage::load_serving_effective_permissions_page(
            pool,
            Some(if transfer { SUCCESSOR } else { OWNER }),
            row.resource_id,
            Some("ens"),
            None,
            100,
        )
        .await?;
        if offset == 1 {
            assert!(
                permissions.rows.is_empty(),
                "lapsed holder permissions: {permissions:?}"
            );
        }
        let ens_v1 =
            bigname_storage::public_name_fields::ens_v1(Some("ens_v1"), &row.declared_summary)?
                .ok_or("wrapper boundary")?;
        assert_eq!(
            ens_v1.wrapper_state,
            Some(if offset == 0 {
                bigname_storage::public_name_fields::WrapperState::Emancipated
            } else {
                bigname_storage::public_name_fields::WrapperState::Lapsed
            })
        );
        super::super::registration_lifecycle::publish(
            pool,
            block,
            bigname_project::families::FamilyMode::Rebuild,
        )
        .await?;
        let rebuilt =
            bigname_storage::families::name::load_family_name(pool, &format!("ens:{node:#x}"))
                .await?
                .ok_or("rebuilt child")?;
        assert_eq!(rebuilt.declared_summary, row.declared_summary);
    }
    database.cleanup().await?;
    Ok(())
}
