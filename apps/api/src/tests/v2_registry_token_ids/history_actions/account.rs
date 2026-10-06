use super::*;
sol! {
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
    event ReverseClaimed(address indexed addr, bytes32 indexed node);
    event NameForAddrChanged(address indexed addr, string name);
    event NewResolver(bytes32 indexed node, address resolver);
    event NameChanged(bytes32 indexed node, string name);
}

#[tokio::test]
async fn history_actions_account_approvals_and_reverse_claims_have_direct_membership() -> Result<()>
{
    let database = TestDatabase::new_migrated().await?;
    let registry =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_registry_l1", 995).await?;
    let reverse = admit_family_from(&database, "mainnet", CHAIN, "ens_v1_reverse_l1", 996).await?;
    let registry = role_address(&registry, "registry");
    let reverse_contract = role_address(&reverse, "reverse_registrar");
    let default_reverse = role_address(&reverse, "default_reverse_registrar");
    let resolver =
        admit_family_from(&database, "mainnet", CHAIN, "ens_v1_resolver_l1", 994).await?;
    let resolver = role_address(&resolver, "public_resolver");
    let reverse_node = bigname_lookup::ens_namehash_hex(&format!(
        "{}.addr.reverse",
        HOLDER.trim_start_matches("0x")
    ))?
    .parse()?;
    let logs = vec![
        emitted(
            ApprovalForAll {
                owner: HOLDER.parse()?,
                operator: GRANTEE.parse()?,
                approved: true,
            }
            .encode_log_data(),
            registry,
            120,
            0,
        ),
        emitted(
            ApprovalForAll {
                owner: HOLDER.parse()?,
                operator: GRANTEE.parse()?,
                approved: false,
            }
            .encode_log_data(),
            registry,
            120,
            1,
        ),
        emitted(
            ReverseClaimed {
                addr: HOLDER.parse()?,
                node: reverse_node,
            }
            .encode_log_data(),
            reverse_contract,
            121,
            0,
        ),
        emitted(
            NameForAddrChanged {
                addr: HOLDER.parse()?,
                name: NAME.into(),
            }
            .encode_log_data(),
            default_reverse,
            122,
            0,
        ),
        emitted(
            NameForAddrChanged {
                addr: HOLDER.parse()?,
                name: "".into(),
            }
            .encode_log_data(),
            default_reverse,
            123,
            0,
        ),
        emitted(
            ReverseClaimed {
                addr: HOLDER.parse()?,
                node: reverse_node,
            }
            .encode_log_data(),
            reverse_contract,
            124,
            0,
        ),
        emitted(
            NewResolver {
                node: reverse_node,
                resolver,
            }
            .encode_log_data(),
            registry,
            124,
            1,
        ),
        emitted(
            NameChanged {
                node: reverse_node,
                name: NAME.into(),
            }
            .encode_log_data(),
            resolver,
            124,
            2,
        ),
    ];
    history_v1_payments::seed_and_run(&database, CHAIN, &logs, 124).await?;
    let approvals = page(
        &database,
        &format!("/v1/events?contract_address={registry:#x}&kind=AccountPermissionChanged"),
    )
    .await?;
    assert_eq!(
        approvals["data"].as_array().unwrap().len(),
        2,
        "{approvals}"
    );
    for (row, approved) in approvals["data"]
        .as_array()
        .unwrap()
        .iter()
        .zip([true, false])
    {
        assert_eq!(row["data"]["action"], "operator_approval_changed");
        assert_eq!(row["data"]["owner"], HOLDER);
        assert_eq!(row["data"]["address"], GRANTEE);
        assert_eq!(row["data"]["approved"], approved);
        assert_eq!(row["data"]["grant_scope"]["kind"], "account");
        assert_eq!(
            row["data"]["grant_scope"]["detail"]["authority_contract"],
            format!("{registry:#x}")
        );
        assert!(row["name"].is_null() && row["registration_id"].is_null());
    }
    // Neither account needs a currently held name. Both address entry points share the rule.
    for (address, relation, expected) in [
        (HOLDER, "owner", 2),
        (GRANTEE, "role_holder", 2),
        (HOLDER, "role_holder", 0),
        (GRANTEE, "owner", 0),
    ] {
        for route in [format!(
            "/v1/addresses/{address}/history?namespace=ens&relation={relation}"
        )] {
            let body = page(&database, &format!("{route}&kind=AccountPermissionChanged")).await?;
            assert_eq!(body["data"].as_array().unwrap().len(), expected, "{body}");
            assert_eq!(body["page"]["total_count"], expected, "{body}");
        }
    }
    for address in [HOLDER, GRANTEE] {
        let body = page(
            &database,
            &format!("/v1/events?namespace=ens&address={address}&kind=AccountPermissionChanged"),
        )
        .await?;
        assert_eq!(body["data"].as_array().unwrap().len(), 2, "{body}");
        assert_eq!(body["page"]["total_count"], 2, "{body}");
    }
    for scope in ["name", "registration"] {
        let body=page(&database,&format!("/v1/addresses/{HOLDER}/history?namespace=ens&scope={scope}&kind=AccountPermissionChanged,ReverseChanged")).await?;
        assert!(body["data"].as_array().unwrap().is_empty(), "{body}");
    }
    let reverse = page(
        &database,
        &format!("/v1/addresses/{HOLDER}/history?namespace=ens&type=primary_name"),
    )
    .await?;
    assert_eq!(reverse["data"].as_array().unwrap().len(), 4, "{reverse}");
    let claims = action_rows(&reverse, "reverse_claimed");
    assert_eq!(claims.len(), 2);
    assert_eq!(
        claims[0]["data"]["reverse_node"],
        format!("{reverse_node:#x}")
    );
    assert_eq!(claims[0]["data"]["name_status"], "unknown");
    assert!(claims[0]["data"].get("name").is_none());
    assert_eq!(claims[1]["data"]["name"], NAME);
    assert_eq!(claims[1]["data"]["name_status"], "set");
    let any = page(
        &database,
        &format!("/v1/addresses/{HOLDER}/history?namespace=ens&type=primary_name&relation=any"),
    )
    .await?;
    assert_eq!(any["data"], reverse["data"]);
    let names = action_rows(&reverse, "primary_name_recorded");
    assert_eq!(names.len(), 2);
    assert_eq!(names[0]["data"]["name"], NAME);
    assert_eq!(names[0]["data"]["name_status"], "set");
    assert_eq!(names[1]["data"]["name_status"], "cleared");
    let filtered = page(
        &database,
        &format!("/v1/addresses/{HOLDER}/history?namespace=ens&type=primary_name&relation=owner"),
    )
    .await?;
    assert!(filtered["data"].as_array().unwrap().is_empty());
    database.cleanup().await
}
