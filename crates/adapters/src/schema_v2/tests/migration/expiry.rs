use super::*;

fn migration_and_old_lease_expiry() -> anyhow::Result<BatchInput> {
    let mut input = plain_unwrapped_input()?;
    let migration_block = input.raw_logs.last().unwrap().block_number;
    let fixture = fixture()?;
    let expiry = fixture["scenarios"]["U-01"]["stored_expiry"]
        .as_i64()
        .unwrap();
    input.raw_logs.push(raw_at_transaction(
        super::super::v2_registry::ExpiryUpdated {
            tokenId: decimal_u256(&fixture["scenarios"]["U-01"]["v2_token_id"])?,
            newExpiry: (expiry + 2 * 365 * 86400) as u64,
            sender: address(&fixture["addresses"], "unlocked_controller")?,
        }
        .encode_log_data(),
        migration_block + 1,
        0,
        0,
        fixture["addresses"]["eth_registry"].as_str().unwrap(),
    ));
    let mut blocks = std::collections::BTreeMap::new();
    for raw in &input.raw_logs {
        blocks
            .entry(raw.block_number)
            .or_insert_with(|| RawBlockInput {
                chain_id: raw.chain_id.clone(),
                block_hash: raw.block_hash.clone(),
                block_number: raw.block_number,
                block_timestamp: raw.block_timestamp,
                canonicality_state: raw.canonicality_state.clone(),
            });
    }
    input.blocks = blocks.into_values().collect();
    input.blocks.push(RawBlockInput {
        chain_id: input.chain_id.clone(),
        block_hash: "after-old-v1-grace".to_owned(),
        block_number: migration_block + 2,
        block_timestamp: time::OffsetDateTime::from_unix_timestamp(expiry + 90 * 86400 + 1)?,
        canonicality_state: "canonical".to_owned(),
    });
    Ok(input)
}

fn assert_old_lease_does_not_reopen(output: &BatchOutput, expiry_block: i64) {
    assert!(
        output.normalized_events.iter().any(|event| {
            event.block_number == Some(expiry_block)
                && event.source_family == "ens_v1_registrar_l1"
                && event.event_kind == "RegistrationReleased"
        }),
        "retain the historical registrar lease release"
    );
    assert!(
        output.surface_bindings.iter().all(|binding| {
            binding.block_number != expiry_block || binding.authority_arm != "ens_v1"
        }),
        "expiry must not restore the graveyard as current ENSv1 ownership"
    );
    assert!(output.normalized_events.iter().all(|event| {
        event.block_number != Some(expiry_block)
            || !event.source_family.starts_with("ens_v1_")
            || !matches!(
                event.event_kind.as_str(),
                "SurfaceBound" | "AuthorityEpochChanged"
            )
    }));
}

#[test]
fn migrated_v1_lease_expiry_does_not_reopen_in_one_batch() -> anyhow::Result<()> {
    let input = migration_and_old_lease_expiry()?;
    let block = input.blocks.last().unwrap().block_number;
    let output = interpret_test_batch(input)?;
    assert_eq!(output.migration_authority_transitions.len(), 1);
    assert_old_lease_does_not_reopen(&output, block);
    assert!(
        !output.normalized_events.iter().any(|event| {
            event.block_number == Some(block)
                && event.event_kind == "RegistrationReleased"
                && event.source_family.starts_with("ens_v2_")
        }),
        "the renewed ENSv2 registration remains live"
    );
    Ok(())
}

#[test]
fn migrated_v1_lease_expiry_does_not_reopen_after_restore() -> anyhow::Result<()> {
    let whole = migration_and_old_lease_expiry()?;
    let expiry = whole.blocks.last().unwrap().clone();
    let mut prefix = whole.clone();
    prefix.blocks.clear();
    let (first, session) = interpret_test_batch_incremental(prefix.clone(), None)?;
    let mut suffix = whole.clone();
    suffix.raw_logs.clear();
    suffix.blocks = vec![expiry.clone()];
    let (warm, _) = interpret_test_batch_incremental(suffix.clone(), Some(session))?;
    assert_old_lease_does_not_reopen(&warm, expiry.block_number);
    let prefix_blocks = prefix
        .raw_logs
        .iter()
        .map(|raw| RawBlockInput {
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            block_timestamp: raw.block_timestamp,
            canonicality_state: raw.canonicality_state.clone(),
        })
        .collect::<Vec<_>>();
    suffix.prior_events = super::super::super::seam::fold_prior_events(
        Vec::new(),
        &first.normalized_events,
        &prefix_blocks,
    )?;
    let cold = interpret_test_batch(suffix)?;
    assert_eq!(warm, cold);
    let full = interpret_test_batch(whole)?;
    assert_eq!(
        warm.normalized_events,
        full.normalized_events
            .into_iter()
            .filter(|event| event.block_number == Some(expiry.block_number))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        warm.surface_bindings,
        full.surface_bindings
            .into_iter()
            .filter(|binding| binding.block_number == expiry.block_number)
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn independent_v2_registration_keeps_ordinary_v1_expiry_fallback() -> anyhow::Result<()> {
    let mut input = migration_and_old_lease_expiry()?;
    let migration_block = fixture()?["scenarios"]["U-01"]["migration_block"]
        .as_i64()
        .unwrap();
    let expiry_block = input.blocks.last().unwrap().block_number;
    // No ENSv1 cleanup proves a migration, even though this input retains
    // an independently observed ENSv2 registration.
    input
        .raw_logs
        .retain(|raw| raw.block_number != migration_block || raw.log_index >= 6);
    let output = interpret_test_batch(input)?;
    assert!(output.migration_authority_transitions.is_empty());
    assert!(output.surface_bindings.iter().any(|binding| {
        binding.block_number == expiry_block && binding.authority_arm == "ens_v1"
    }));
    Ok(())
}

#[test]
fn migrated_v1_lease_expiry_does_not_reopen_after_v2_expiry() -> anyhow::Result<()> {
    let mut input = migration_and_old_lease_expiry()?;
    let expiry_block = input.blocks.last().unwrap().block_number;
    input
        .raw_logs
        .retain(|raw| raw.block_number != expiry_block - 1);
    let output = interpret_test_batch(input)?;
    assert_old_lease_does_not_reopen(&output, expiry_block);
    assert!(output.normalized_events.iter().any(|event| {
        event.block_number == Some(expiry_block)
            && event.event_kind == "RegistrationReleased"
            && event.source_family.starts_with("ens_v2_")
    }));
    Ok(())
}
