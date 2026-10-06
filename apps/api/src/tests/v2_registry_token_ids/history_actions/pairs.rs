use super::*;
sol! {
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event AddressChanged(bytes32 indexed node, uint256 coinType, bytes newAddress);
    event AddrChanged(bytes32 indexed node, address a);
    event TextChanged(bytes32 indexed node, string indexed indexedKey, string key, string value);
}

#[tokio::test]
async fn history_actions_produced_address_pairs_page_once_per_write_on_every_route() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let resolver_source =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_resolver_l1", 999).await?;
    let resolver = role_address(&resolver_source, "public_resolver");
    assert_eq!(
        format!("{resolver:#x}"),
        "0xf29100983e058b709f3d539b0c765937b804ac15"
    );
    let node = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let registry_source =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 1001).await?;
    let registry = role_address(&registry_source, "registry");
    let wrapper_source =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_wrapper_l1", 1002).await?;
    let wrapper = role_address(&wrapper_source, "name_wrapper");
    let mut dns = vec![LABEL.len() as u8];
    dns.extend_from_slice(LABEL.as_bytes());
    dns.extend_from_slice(b"\x03eth\x00");
    let mut logs = vec![
        emitted(
            NewOwner {
                node: bigname_lookup::ens_namehash_hex("eth")?.parse()?,
                label: keccak256(LABEL),
                owner: wrapper,
            }
            .encode_log_data(),
            registry,
            120,
            0,
        ),
        emitted(
            NewResolver { node, resolver }.encode_log_data(),
            registry,
            120,
            1,
        ),
        emitted(
            NameWrapped {
                node,
                name: dns.into(),
                owner: HOLDER.parse()?,
                fuses: 0,
                expiry: 1_900_000_000,
            }
            .encode_log_data(),
            wrapper,
            120,
            2,
        ),
    ];
    // Both setter overloads emit this sequence without an intervening external call.
    // Repeating the same value is still a separate write.
    // (upstream: .refs/ens_v1/contracts/resolvers/profiles/AddrResolver.sol:L26-L65 @ ens_v1@91c966f)
    for (write, address) in [HOLDER, HOLDER, GRANTEE, HOLDER].into_iter().enumerate() {
        let address = address.parse::<Address>()?;
        logs.push(emitted(
            AddressChanged {
                node,
                coinType: U256::from(60),
                newAddress: address.to_vec().into(),
            }
            .encode_log_data(),
            resolver,
            121,
            2 * write as i64,
        ));
        logs.push(emitted(
            AddrChanged { node, a: address }.encode_log_data(),
            resolver,
            121,
            2 * write as i64 + 1,
        ));
    }
    logs.push(emitted(
        AddressChanged {
            node,
            coinType: U256::from(60),
            newAddress: vec![].into(),
        }
        .encode_log_data(),
        resolver,
        121,
        8,
    ));
    logs.push(emitted(
        AddrChanged {
            node,
            a: Address::ZERO,
        }
        .encode_log_data(),
        resolver,
        121,
        9,
    ));
    logs.push(emitted(
        AddrChanged {
            node,
            a: GRANTEE.parse()?,
        }
        .encode_log_data(),
        resolver,
        122,
        0,
    ));
    for index in [1, 2] {
        logs.push(emitted(
            TextChanged {
                node,
                indexedKey: keccak256("url"),
                key: "url".into(),
                value: "same".into(),
            }
            .encode_log_data(),
            resolver,
            122,
            index,
        ));
    }
    history_v1_payments::seed_and_run(&database, CHAIN, &logs, 122).await?;
    let raw_count:i64=sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE event_kind='RecordChanged' AND after_state->>'record_key'='addr:60'").fetch_one(&database.pool).await?;
    assert_eq!(raw_count, 11);
    let mut shared_ids = None;
    for route in [
        format!("/v1/events?namespace=ens&resolver=1:{resolver:#x}"),
        format!("/v1/events?contract_address={resolver:#x}"),
        format!("/v1/events?name={NAME}"),
        format!("/v1/names/{NAME}/history?scope=both"),
        format!("/v1/addresses/{HOLDER}/history?namespace=ens"),
    ] {
        let all = page(&database, &format!("{route}&record_key=addr:60")).await?;
        let rows = all["data"].as_array().unwrap();
        assert_eq!(rows.len(), 6, "{route}: {all:#}");
        assert_eq!(all["page"]["total_count"], 6);
        let ids = rows
            .iter()
            .map(|r| r["id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        if let Some(expected) = &shared_ids {
            assert_eq!(&ids, expected, "{route}");
        } else {
            shared_ids = Some(ids.clone());
        }
        assert_eq!(
            rows[4]["data"]["value"], "0x",
            "modern empty clear stays exact"
        );
        for order in ["asc", "desc"] {
            let mut cursor = None;
            let mut walked = Vec::new();
            loop {
                let suffix = cursor
                    .as_ref()
                    .map(|c| format!("&cursor={c}"))
                    .unwrap_or_default();
                let (status,body)=read_family_response(&database,&format!("{route}&record_key=addr:60&include=data,raw,total_count&order={order}&page_size=1{suffix}")).await?;
                assert_eq!(status, StatusCode::OK, "{body}");
                assert_eq!(
                    body["data"].as_array().unwrap().len(),
                    1,
                    "no underfilled page: {body}"
                );
                assert_eq!(body["page"]["total_count"], 6);
                walked.push(body["data"][0]["id"].as_str().unwrap().to_owned());
                cursor = body["page"]["next_cursor"].as_str().map(str::to_owned);
                assert_eq!(body["page"]["has_more"], cursor.is_some());
                if cursor.is_none() {
                    break;
                }
                assert!(walked.len() < 7);
            }
            let mut expected = ids.clone();
            if order == "desc" {
                expected.reverse();
            }
            assert_eq!(walked, expected);
        }
    }
    let texts = page(
        &database,
        &format!("/v1/events?contract_address={resolver:#x}&record_key=text:url"),
    )
    .await?;
    assert_eq!(texts["data"].as_array().unwrap().len(), 2);
    database.cleanup().await
}
