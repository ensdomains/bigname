//! Complete raw transactions through the checked-in NameWrapper declarations, including
//! receiver-callback reentrancy and restoration at the supported committed transaction boundary.
use super::*;

fn ordered(mut logs: Vec<RawLogInput>) -> Vec<RawLogInput> {
    for (index, log) in logs.iter_mut().enumerate() {
        log.log_index = index as i64;
    }
    logs
}

#[test]
fn callback_burn_and_nested_rewrap_keep_the_actual_final_wrapper_state() -> anyhow::Result<()> {
    let (chain, manifests, admissions) = profile(
        "sepolia",
        &[
            "ens_v1_registry_l1",
            "ens_v1_registrar_l1",
            "ens_v1_wrapper_l1",
        ],
    )?;
    let contracts = Contracts::new(&admissions);
    let node = format!("{:#x}", label().2);
    for nested in [false, true] {
        let mut logs = register_and_wrap(
            &contracts,
            REGISTERED,
            FIRST_OWNER,
            FIRST_EXPIRY,
            None,
            None,
        );
        let outer_completion = logs.pop().expect("outer NameWrapped");
        logs.extend(unwrap(&contracts, REGISTERED, FIRST_OWNER));
        if nested {
            let mut inner = rewrap(&contracts, REGISTERED, FIRST_OWNER, FIRST_EXPIRY);
            // The depositor chooses a different wrapped owner. The inner wrap locks itself;
            // the late outer completion still contains the older owner and unlocked word.
            inner.truncate(2);
            let mut mint = wrapper_mint(&contracts, REGISTERED, 2, SECOND_OWNER, FIRST_EXPIRY);
            mint[1].data = (events::NameWrapped {
                node: label().2,
                name: b"\x08relinked\x03eth\0".to_vec().into(),
                owner: address(SECOND_OWNER),
                fuses: DOT_ETH_FUSES | 1,
                expiry: (FIRST_EXPIRY + GRACE_PERIOD) as u64,
            }
            .encode_log_data()
            .data)
                .to_vec();
            inner.extend(mint);
            logs.extend(inner);
        }
        logs.push(outer_completion);
        let logs = ordered(logs);
        let (output, live) = interpret_test_batch_incremental(
            batch(&chain, &manifests, &admissions, Vec::new(), logs.clone()),
            None,
        )?;
        let name = live.v1_name("ens", &node).expect("final name");
        assert_eq!(
            name.owner.as_deref(),
            Some(if nested { SECOND_OWNER } else { FIRST_OWNER })
        );
        assert_eq!(
            name.authority_source_family,
            if nested {
                "ens_v1_wrapper_l1"
            } else {
                "ens_v1_registrar_l1"
            }
        );
        let completions: Vec<_> = output
            .normalized_events
            .iter()
            .filter(|event| {
                event.event_kind == "TokenControlTransferred"
                    && event.after_state["wrapper_mint"] == true
            })
            .collect();
        assert_eq!(completions.len(), 1 + usize::from(nested));
        if nested {
            assert_eq!(completions[1].after_state["to"], SECOND_OWNER);
            assert_eq!(completions[1].after_state["fuses"], DOT_ETH_FUSES | 1);
        }
        let stale_permissions = output
            .normalized_events
            .iter()
            .filter(|event| {
                event.event_kind == "PermissionChanged"
                    && event.after_state["source_event"] == "NameWrapped"
                    && event.log_index == Some((logs.len() - 1) as i64)
            })
            .count();
        assert_eq!(
            stale_permissions, 0,
            "outer completion restored holder powers"
        );
        for prior in [
            output.normalized_events.iter().map(prior_event).collect(),
            compact_prior(&output.normalized_events),
        ] {
            let (_, restored) = interpret_test_batch_incremental(
                batch(&chain, &manifests, &admissions, prior, Vec::new()),
                None,
            )?;
            assert_eq!(
                restored, live,
                "callback state changed across committed restore"
            );
        }
        let cold = interpret_test_batch(batch(&chain, &manifests, &admissions, Vec::new(), logs))?;
        assert_eq!(cold.normalized_events, output.normalized_events);
    }
    Ok(())
}

#[test]
fn ordinary_same_transaction_rewrap_and_expired_premint_unwrap_complete_normally()
-> anyhow::Result<()> {
    let (chain, manifests, admissions) = profile(
        "sepolia",
        &[
            "ens_v1_registry_l1",
            "ens_v1_registrar_l1",
            "ens_v1_wrapper_l1",
        ],
    )?;
    let contracts = Contracts::new(&admissions);
    let mut logs = register_and_wrap(
        &contracts,
        REGISTERED,
        FIRST_OWNER,
        FIRST_EXPIRY,
        None,
        None,
    );
    logs.extend(unwrap(&contracts, REGISTERED, FIRST_OWNER));
    logs.extend(rewrap(&contracts, REGISTERED, FIRST_OWNER, FIRST_EXPIRY));
    let (first, session) = interpret_test_batch_incremental(
        batch(&chain, &manifests, &admissions, Vec::new(), ordered(logs)),
        None,
    )?;
    assert_eq!(
        first
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "TokenControlTransferred"
                && event.after_state["wrapper_mint"] == true)
            .count(),
        2
    );
    // Expired pre-mint NameUnwrapped(node,0) belongs to the old token, before a new mint.
    let logs = register_and_wrap(
        &contracts,
        REREGISTERED,
        SECOND_OWNER,
        SECOND_EXPIRY,
        Some(FIRST_OWNER),
        None,
    );
    let (output, live) = interpret_test_batch_incremental(
        batch(&chain, &manifests, &admissions, Vec::new(), logs.clone()),
        Some(session),
    )?;
    let name = live
        .v1_name("ens", &format!("{:#x}", label().2))
        .expect("new wrapped name");
    assert_eq!(name.owner.as_deref(), Some(SECOND_OWNER));
    assert_eq!(
        output
            .normalized_events
            .iter()
            .filter(|event| event.event_kind == "TokenControlTransferred"
                && event.after_state["wrapper_mint"] == true)
            .count(),
        1
    );
    for prior in [
        first.normalized_events.iter().map(prior_event).collect(),
        compact_prior(&first.normalized_events),
    ] {
        let (restored_output, restored) = interpret_test_batch_incremental(
            batch(&chain, &manifests, &admissions, prior, logs.clone()),
            None,
        )?;
        assert_eq!(restored_output.normalized_events, output.normalized_events);
        assert_eq!(restored, live);
    }
    Ok(())
}

#[test]
fn generic_callback_inheritance_restores_complete_and_compacted_state() -> anyhow::Result<()> {
    let (chain, manifests, admissions) = profile(
        "sepolia",
        &[
            "ens_v1_registry_l1",
            "ens_v1_registrar_l1",
            "ens_v1_wrapper_l1",
        ],
    )?;
    let contracts = Contracts::new(&admissions);
    let mut parent = register_and_wrap(
        &contracts,
        REGISTERED,
        FIRST_OWNER,
        FIRST_EXPIRY,
        None,
        None,
    );
    let completion = parent.last_mut().expect("parent completion");
    completion.data = events::NameWrapped {
        node: label().2,
        name: b"\x08relinked\x03eth\0".to_vec().into(),
        owner: address(FIRST_OWNER),
        fuses: DOT_ETH_FUSES | 1,
        expiry: (FIRST_EXPIRY + GRACE_PERIOD) as u64,
    }
    .encode_log_data()
    .data
    .to_vec();
    let (parent_output, parent_session) = interpret_test_batch_incremental(
        batch(&chain, &manifests, &admissions, vec![], parent),
        None,
    )?;
    let node = keccak256([label().2.as_slice(), keccak256(b"child").as_slice()].concat());
    let token = U256::from_be_bytes(node.0);
    let holder = address(FIRST_OWNER);
    let wrapper = address(&contracts.wrapper);
    let mint = |from, to| {
        events::TransferSingle {
            operator: holder,
            from,
            to,
            id: token,
            value: U256::from(1),
        }
        .encode_log_data()
    };
    let completion = |fuses, expiry| {
        events::NameWrapped {
            node,
            name: b"\x05child\x08relinked\x03eth\0".to_vec().into(),
            owner: holder,
            fuses,
            expiry,
        }
        .encode_log_data()
    };
    // The authorization is an ENS-registry operator approval, before the one-shot callback.
    alloy_sol_types::sol! { event ApprovalForAll(address indexed owner,address indexed operator,bool approved); }
    let logs = [
        (
            &contracts.registry,
            ApprovalForAll {
                owner: holder,
                operator: wrapper,
                approved: true,
            }
            .encode_log_data(),
        ),
        (
            &contracts.registry,
            events::NewOwner {
                node: label().2,
                label: keccak256(b"child"),
                owner: wrapper,
            }
            .encode_log_data(),
        ),
        (&contracts.wrapper, mint(Address::ZERO, holder)),
        (&contracts.wrapper, mint(holder, Address::ZERO)),
        (
            &contracts.registry,
            events::registry::Transfer {
                node,
                owner: holder,
            }
            .encode_log_data(),
        ),
        (
            &contracts.wrapper,
            events::NameUnwrapped {
                node,
                owner: holder,
            }
            .encode_log_data(),
        ),
        (
            &contracts.registry,
            events::registry::Transfer {
                node,
                owner: wrapper,
            }
            .encode_log_data(),
        ),
        (&contracts.wrapper, mint(Address::ZERO, holder)),
        (&contracts.wrapper, completion(0, 0)),
        (&contracts.wrapper, completion(1 << 16, FIRST_EXPIRY as u64)),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (emitter, data))| raw_at(data, REGISTERED + 1, index as i64, emitter))
    .collect();
    let (output, live) = interpret_test_batch_incremental(
        batch(&chain, &manifests, &admissions, vec![], logs),
        Some(parent_session),
    )?;
    let mut all = parent_output.normalized_events;
    all.extend(output.normalized_events);
    let later = vec![raw_at(
        mint(holder, address(SECOND_OWNER)),
        REGISTERED + 2,
        0,
        &contracts.wrapper,
    )];
    let priors: [Vec<PriorEventInput>; 2] =
        [all.iter().map(prior_event).collect(), compact_prior(&all)];
    for prior in &priors {
        let (_, restored) = interpret_test_batch_incremental(
            batch(&chain, &manifests, &admissions, prior.clone(), vec![]),
            None,
        )?;
        assert_eq!(
            restored, live,
            "generic retained expiry/fuses/authority changed across restore"
        );
    }
    let (expected, expected_state) = interpret_test_batch_incremental(
        batch(&chain, &manifests, &admissions, vec![], later.clone()),
        Some(live),
    )?;
    for prior in priors {
        let (actual, actual_state) = interpret_test_batch_incremental(
            batch(&chain, &manifests, &admissions, prior, later.clone()),
            None,
        )?;
        assert_eq!(actual.normalized_events, expected.normalized_events);
        assert_eq!(
            actual_state, expected_state,
            "later real transfer changed after restore"
        );
    }
    let name = expected_state
        .v1_name("ens", &format!("{node:#x}"))
        .expect("generic name");
    assert_eq!(name.owner.as_deref(), Some(SECOND_OWNER));
    Ok(())
}
