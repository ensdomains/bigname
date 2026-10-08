use super::*;
sol! {
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event NameUnwrapped(bytes32 indexed node, address owner);
}

#[tokio::test]
async fn history_actions_wrap_and_unwrap_keep_the_explicit_action_and_zero_destination()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let wrapper = admit_family_from(&database, "mainnet", CHAIN, "ens_v1_wrapper_l1", 998).await?;
    let wrapper = role_address(&wrapper, "name_wrapper");
    let node: alloy_primitives::B256 = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let mut dns = vec![LABEL.len() as u8];
    dns.extend_from_slice(LABEL.as_bytes());
    dns.extend_from_slice(b"\x03eth\x00");
    let id = U256::from_be_bytes(*node);
    let mint_or_burn = |block, index, from: Address, to: Address| {
        emitted(
            TransferSingle {
                operator: HOLDER.parse().unwrap(),
                from,
                to,
                id,
                value: U256::from(1),
            }
            .encode_log_data(),
            wrapper,
            block,
            index,
        )
    };
    let wrap = |block, index| {
        emitted(
            NameWrapped {
                node,
                name: dns.clone().into(),
                owner: HOLDER.parse().unwrap(),
                fuses: 196608,
                expiry: 1_900_000_000,
            }
            .encode_log_data(),
            wrapper,
            block,
            index,
        )
    };
    let unwrap = |block, index, owner| {
        emitted(
            NameUnwrapped { node, owner }.encode_log_data(),
            wrapper,
            block,
            index,
        )
    };
    // _mint burns and emits a zero-destination unwrap before a replacement mint/wrap.
    // An ordinary unwrap burns before emitting its destination. The token transfer at122
    // is independent of both actions; no repeated NameWrapped without an intervening unwrap.
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L903 @ ens_v1@91c966f)
    // (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L1021-L1031 @ ens_v1@91c966f)
    let logs = vec![
        mint_or_burn(120, 0, Address::ZERO, HOLDER.parse()?),
        wrap(120, 1),
        mint_or_burn(121, 0, HOLDER.parse()?, Address::ZERO),
        unwrap(121, 1, Address::ZERO),
        mint_or_burn(121, 2, Address::ZERO, HOLDER.parse()?),
        wrap(121, 3),
        mint_or_burn(122, 0, HOLDER.parse()?, GRANTEE.parse()?),
        mint_or_burn(123, 0, GRANTEE.parse()?, Address::ZERO),
        unwrap(123, 1, HOLDER.parse()?),
        mint_or_burn(124, 0, Address::ZERO, HOLDER.parse()?),
        wrap(124, 1),
        mint_or_burn(125, 0, HOLDER.parse()?, Address::ZERO),
        unwrap(125, 1, HOLDER.parse()?),
    ];
    history_v1_payments::seed_and_run(&database, CHAIN, &logs, 125).await?;
    let body = page(
        &database,
        &format!("/v1/events?contract_address={wrapper:#x}"),
    )
    .await?;
    let wrapped = action_rows(&body, "name_wrapped");
    assert_eq!(wrapped.len(), 3, "{body:#}");
    for row in wrapped {
        assert_eq!(row["data"]["node"], format!("{node:#x}"));
        assert_eq!(row["data"]["expires_at"], "1900000000");
        assert_eq!(row["data"]["fuses"], 196608);
        let block = row["block_number"].as_i64().context("wrap block")?;
        assert_eq!(row["log_index"], if block == 121 { 2 } else { 0 });
    }
    let transfers = action_rows(&body, "token_transferred");
    assert_eq!(transfers.len(), 1, "{body:#}");
    assert_eq!(transfers[0]["block_number"], 122);
    assert_eq!(transfers[0]["data"]["from"], HOLDER);
    assert_eq!(transfers[0]["data"]["to"], GRANTEE);
    let unwrapped = action_rows(&body, "name_unwrapped");
    assert_eq!(unwrapped.len(), 3, "{body:#}");
    assert_eq!(
        unwrapped[0]["data"]["owner"],
        format!("{:#x}", Address::ZERO)
    );
    assert_eq!(unwrapped[1]["data"]["owner"], HOLDER);
    let mint_positions: Vec<(i64, i64, String, String, Value)> = sqlx::query_as(
        "SELECT block_number,log_index,block_hash,transaction_hash,after_state FROM normalized_events
         WHERE source_family='ens_v1_wrapper_l1' AND event_kind='TokenControlTransferred'
           AND after_state->'wrapper_mint'='true'::jsonb ORDER BY block_number,log_index")
        .fetch_all(&database.pool).await?;
    assert_eq!(mint_positions.len(), 3, "one normalized mint per wrap");
    for ((block, index, hash, transaction, after), (want_block, want_index)) in
        mint_positions.iter().zip([(120, 0), (121, 2), (124, 0)])
    {
        assert_eq!((*block, *index), (want_block, want_index));
        assert_eq!(after["source_event"], "TransferSingle");
        assert_eq!(
            after["matched_wrapper_completion"]["source_event"],
            "NameWrapped"
        );
        assert_eq!(after["matched_wrapper_completion"]["block_hash"], *hash);
        assert_eq!(
            after["matched_wrapper_completion"]["transaction_hash"],
            *transaction
        );
        assert_eq!(after["matched_wrapper_completion"]["log_index"], index + 1);
    }
    for route in [
        format!("/v1/events?contract_address={wrapper:#x}"),
        format!("/v1/names/{NAME}/history?scope=both"),
        format!("/v1/addresses/{HOLDER}/history?namespace=ens"),
    ] {
        let full = page(&database, &route).await?;
        assert_eq!(action_rows(&full, "name_wrapped").len(), 3, "{full:#}");
        assert_eq!(action_rows(&full, "name_unwrapped").len(), 3, "{full:#}");
        assert_eq!(action_rows(&full, "token_transferred").len(), 1, "{full:#}");
        assert_small_pages_match(&database, &route, &full).await?;
    }
    database.cleanup().await
}
