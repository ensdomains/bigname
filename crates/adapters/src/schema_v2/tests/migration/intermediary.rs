use super::*;

fn prefixed_input(prefix_len: i64) -> anyhow::Result<BatchInput> {
    let fixture = fixture()?;
    let mut input = plain_unwrapped_input()?;
    let block = input.raw_logs.last().unwrap().block_number;
    let label = keccak256(fixture["scenarios"]["U-01"]["label"].as_str().unwrap());
    let registrar = fixture["addresses"]["base_registrar"].as_str().unwrap();
    let mut holder = Address::from([0x51; 20]);
    for raw in input
        .raw_logs
        .iter_mut()
        .filter(|raw| raw.block_number == block)
    {
        raw.log_index += prefix_len;
    }
    for log in 0..prefix_len {
        let next = Address::from([0x71 + log as u8; 20]);
        input.raw_logs.push(registrar_transfer_at(
            holder, next, label, block, log, registrar,
        ));
        holder = next;
    }
    let entry = input
        .raw_logs
        .iter_mut()
        .find(|raw| raw.block_number == block && raw.log_index == prefix_len)
        .unwrap();
    entry.topics[1] = format!("{:#x}", B256::left_padding_from(holder.as_slice()));
    input
        .raw_logs
        .sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    Ok(input)
}

fn ordinary_input(mut input: BatchInput) -> anyhow::Result<BatchInput> {
    let fixture = fixture()?;
    let block = input.raw_logs.last().unwrap().block_number;
    let v2 = fixture["addresses"]["eth_registry"].as_str().unwrap();
    // Keep all ordinary ENSv1 observations and the same manifest admissions, including
    // Graveyard disclosure retirement. With no successor receipt there is no migration proof.
    input
        .raw_logs
        .retain(|raw| raw.block_number != block || !raw.emitting_address.eq_ignore_ascii_case(v2));
    Ok(input)
}

fn v1_events(output: &BatchOutput) -> Vec<NormalizedEvent> {
    output
        .normalized_events
        .iter()
        .filter(|event| event.source_family.starts_with("ens_v1_"))
        .cloned()
        .collect()
}

fn assert_ordinary(input: BatchInput, case: &str) -> anyhow::Result<()> {
    let ordinary = interpret_test_batch(ordinary_input(input.clone())?)?;
    let actual = interpret_test_batch(input)?;
    assert_eq!(
        v1_events(&actual),
        v1_events(&ordinary),
        "{case}: preserve complete ordinary ENSv1 observations and permissions"
    );
    assert_eq!(
        actual
            .surface_bindings
            .iter()
            .filter(|binding| binding.authority_arm == "ens_v1")
            .collect::<Vec<_>>(),
        ordinary
            .surface_bindings
            .iter()
            .filter(|binding| binding.authority_arm == "ens_v1")
            .collect::<Vec<_>>(),
        "{case}: preserve ordinary binding openings"
    );
    assert_eq!(
        actual
            .binding_closures
            .iter()
            .filter(|binding| binding.authority_arm == "ens_v1")
            .collect::<Vec<_>>(),
        ordinary
            .binding_closures
            .iter()
            .filter(|binding| binding.authority_arm == "ens_v1")
            .collect::<Vec<_>>(),
        "{case}: preserve ordinary binding closures"
    );
    Ok(())
}

#[test]
fn intermediary_prefix_transfers_preserve_their_complete_ordinary_effects() -> anyhow::Result<()> {
    for prefix_len in [1, 2] {
        let input = prefixed_input(prefix_len)?;
        let block = input.raw_logs.last().unwrap().block_number;
        let mut prefix = input.clone();
        prefix
            .raw_logs
            .retain(|raw| raw.block_number != block || raw.log_index < prefix_len);
        let ordinary = interpret_test_batch(prefix)?;
        let complete = interpret_test_batch(input)?;
        let prefix_events = |output: &BatchOutput| {
            output
                .normalized_events
                .iter()
                .filter(|event| {
                    event.block_number != Some(block)
                        || event.log_index.is_some_and(|log| log < prefix_len)
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        assert_eq!(prefix_events(&complete), ordinary.normalized_events);
        let prefix_bindings = complete
            .surface_bindings
            .iter()
            .filter(|binding| {
                binding.block_number != block
                    || binding.provenance["log_index"]
                        .as_i64()
                        .is_some_and(|log| log < prefix_len)
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(prefix_bindings, ordinary.surface_bindings);
        let prefix_closures = complete
            .binding_closures
            .iter()
            .filter(|binding| binding.block_number != block || binding.log_index < prefix_len)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(prefix_closures, ordinary.binding_closures);
        assert_eq!(complete.migration_authority_transitions.len(), 1);
        assert!(complete.surface_bindings.iter().all(|binding| {
            binding.block_number != block
                || binding.authority_arm != "ens_v1"
                || binding.provenance["log_index"]
                    .as_i64()
                    .is_some_and(|log| log < prefix_len)
        }));
    }
    Ok(())
}

#[test]
fn intermediary_cleanup_leaves_another_names_interleaved_transfer_ordinary() -> anyhow::Result<()> {
    let fixture = fixture()?;
    let mut input = prefixed_input(1)?;
    let block = input.raw_logs.last().unwrap().block_number;
    let label_text = "intermediary-neighbor";
    let label = keccak256(label_text);
    let owner = Address::from([0x51; 20]);
    let registry = "0x0000000000000000000000000000000000000099";
    input.raw_logs.push(raw_at_transaction(
        super::super::v1_registry::NewOwner {
            node: super::super::common::namehash(&["eth".to_owned()]).parse()?,
            label,
            owner,
        }
        .encode_log_data(),
        block - 1,
        0,
        4,
        registry,
    ));
    input.raw_logs.push(raw_at_transaction(
        super::super::NameRegistered {
            name: label_text.to_owned(),
            label,
            owner,
            expires: U256::from(
                fixture["scenarios"]["U-01"]["stored_expiry"]
                    .as_u64()
                    .unwrap(),
            ),
        }
        .encode_log_data(),
        block - 1,
        0,
        5,
        "0x0000000000000000000000000000000000000098",
    ));
    for raw in input
        .raw_logs
        .iter_mut()
        .filter(|raw| raw.block_number == block && raw.log_index >= 2)
    {
        raw.log_index += 1;
    }
    input.raw_logs.push(registrar_transfer_at(
        owner,
        Address::from([0x76; 20]),
        label,
        block,
        2,
        fixture["addresses"]["base_registrar"].as_str().unwrap(),
    ));
    input
        .raw_logs
        .sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
    let ordinary = interpret_test_batch(ordinary_input(input.clone())?)?;
    let complete = interpret_test_batch(input)?;
    let logical = format!(
        "ens:{}",
        super::super::common::namehash(&[label_text.to_owned(), "eth".to_owned()])
    );
    let neighbor = |output: &BatchOutput| {
        output
            .normalized_events
            .iter()
            .filter(|event| event.logical_name_id.as_deref() == Some(&logical))
            .cloned()
            .collect::<Vec<_>>()
    };
    let ordinary_neighbor = neighbor(&ordinary);
    assert!(
        ordinary_neighbor
            .iter()
            .any(|event| event.block_number == Some(block)
                && event.event_kind == "TokenControlTransferred")
    );
    assert_eq!(neighbor(&complete), ordinary_neighbor);
    assert_eq!(complete.migration_authority_transitions.len(), 1);
    let primary = format!(
        "ens:{}",
        fixture["scenarios"]["U-01"]["namehash"].as_str().unwrap()
    );
    let successors = complete
        .surface_bindings
        .iter()
        .filter(|binding| {
            binding.block_number == block
                && binding.logical_name_id == primary
                && binding.authority_arm == "ens_v2"
        })
        .count();
    assert_eq!(successors, 1, "the primary ENSv2 successor remains bound");
    assert!(
        complete
            .surface_bindings
            .iter()
            .all(|binding| binding.block_number != block
                || binding.logical_name_id != primary
                || binding.authority_arm != "ens_v1"
                || binding.provenance["log_index"] == 0)
    );
    Ok(())
}

#[test]
fn incomplete_or_ambiguous_intermediary_pair_keeps_ordinary_interpretation() -> anyhow::Result<()> {
    let fixture = fixture()?;
    // Prefix0, entry1, reclaim2, registry cleanup3, resolver clear4, TTL5, token cleanup6,
    // registration7, mint8, resource link9, role grant10, later resolver11.
    for missing in [1, 2, 3, 4, 6, 7, 8, 9, 10] {
        let mut input = prefixed_input(1)?;
        let block = input.raw_logs.last().unwrap().block_number;
        input
            .raw_logs
            .retain(|raw| raw.block_number != block || raw.log_index != missing);
        assert_ordinary(input, &format!("missing log {missing}"))?;
    }
    for case in [
        "wrong_entry_owner",
        "wrong_entry_instance",
        "wrong_cleanup_instance",
        "inside_transfer",
        "after_cleanup_transfer",
        "duplicate_entry_position",
        "duplicate_cleanup_position",
        "duplicate_pair",
    ] {
        let mut input = prefixed_input(1)?;
        let block = input.raw_logs.last().unwrap().block_number;
        let at =
            |raw: &&mut RawLogInput, index| raw.block_number == block && raw.log_index == index;
        match case {
            "wrong_entry_owner" => {
                input
                    .raw_logs
                    .iter_mut()
                    .find(|raw| at(raw, 1))
                    .unwrap()
                    .topics[1] = format!(
                    "{:#x}",
                    B256::left_padding_from(Address::from([0x51; 20]).as_slice())
                )
            }
            "wrong_entry_instance" | "wrong_cleanup_instance" => {
                let foreign = "0x0000000000000000000000000000000000000097";
                input.admissions.push(admission_at(
                    V1_REGISTRAR_MANIFEST_ID,
                    "registrar",
                    foreign,
                    107,
                ));
                let index = if case == "wrong_entry_instance" { 1 } else { 6 };
                input
                    .raw_logs
                    .iter_mut()
                    .find(|raw| at(raw, index))
                    .unwrap()
                    .emitting_address = foreign.to_owned();
            }
            "inside_transfer" | "after_cleanup_transfer" => {
                let index = if case == "inside_transfer" { 3 } else { 7 };
                for raw in input
                    .raw_logs
                    .iter_mut()
                    .filter(|raw| raw.block_number == block && raw.log_index >= index)
                {
                    raw.log_index += 1;
                }
                input.raw_logs.push(registrar_transfer_at(
                    address(&fixture["addresses"], "unlocked_controller")?,
                    Address::from([0x76; 20]),
                    keccak256(fixture["scenarios"]["U-01"]["label"].as_str().unwrap()),
                    block,
                    index,
                    fixture["addresses"]["base_registrar"].as_str().unwrap(),
                ));
            }
            _ => {
                for index in [1, 6] {
                    if case == "duplicate_pair"
                        || (case == "duplicate_entry_position" && index == 1)
                        || (case == "duplicate_cleanup_position" && index == 6)
                    {
                        let duplicate = input
                            .raw_logs
                            .iter()
                            .find(|raw| raw.block_number == block && raw.log_index == index)
                            .unwrap()
                            .clone();
                        input.raw_logs.push(duplicate);
                    }
                }
            }
        }
        input
            .raw_logs
            .sort_by_key(|raw| (raw.block_number, raw.transaction_index, raw.log_index));
        assert_ordinary(input, case)?;
    }
    Ok(())
}
