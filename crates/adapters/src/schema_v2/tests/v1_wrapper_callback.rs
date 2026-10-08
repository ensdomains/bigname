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
                    && event.after_state["source_event"] == "NameWrapped"
            })
            .collect();
        assert_eq!(completions.len(), usize::from(nested));
        if nested {
            assert_eq!(completions[0].after_state["to"], SECOND_OWNER);
            assert_eq!(completions[0].after_state["fuses"], DOT_ETH_FUSES | 1);
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
                && event.after_state["source_event"] == "NameWrapped")
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
                && event.after_state["source_event"] == "NameWrapped")
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
