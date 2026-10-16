//! A NameWrapper `setApprovalForAll` on every route that serves permission powers. The event
//! feeds carry the approval's own `wrapper_control`. Permission reads give the operator the
//! holder's masked powers instead. The test router checks every response against the OpenAPI
//! document, so a served power outside `PermissionPower` fails the request.
//! (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L105-L117 @ ens_v1@91c966f)
use super::*;
sol! {
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
}

/// Every `powers` array anywhere in a response body.
fn served_powers(value: &Value, found: &mut Vec<Value>) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if key == "powers" {
                    found.push(value.clone());
                }
                served_powers(value, found);
            }
        }
        Value::Array(items) => items.iter().for_each(|item| served_powers(item, found)),
        _ => {}
    }
}

fn carries_wrapper_control(body: &Value) -> bool {
    let mut found = Vec::new();
    served_powers(body, &mut found);
    found
        .iter()
        .filter_map(Value::as_array)
        .flatten()
        .any(|power| power == "wrapper_control")
}

async fn current(database: &TestDatabase, route: &str) -> Result<Value> {
    let (status, body) = read_family_response(database, route).await?;
    assert_eq!(status, StatusCode::OK, "{route}: {body:#}");
    Ok(body)
}

#[tokio::test]
async fn a_wrapper_operator_approval_passes_the_schema_on_every_route_that_serves_powers()
-> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let wrapper = admit_family_from(&database, "mainnet", CHAIN, "ens_v1_wrapper_l1", 998).await?;
    let wrapper = role_address(&wrapper, "name_wrapper");
    let node: alloy_primitives::B256 = bigname_lookup::ens_namehash_hex(NAME)?.parse()?;
    let mut dns = vec![LABEL.len() as u8];
    dns.extend_from_slice(LABEL.as_bytes());
    dns.extend_from_slice(b"\x03eth\x00");
    let logs = vec![
        emitted(
            TransferSingle {
                operator: HOLDER.parse()?,
                from: Address::ZERO,
                to: HOLDER.parse()?,
                id: U256::from_be_bytes(*node),
                value: U256::from(1),
            }
            .encode_log_data(),
            wrapper,
            120,
            0,
        ),
        emitted(
            NameWrapped {
                node,
                name: dns.into(),
                owner: HOLDER.parse()?,
                fuses: 196608,
                expiry: 1_900_000_000,
            }
            .encode_log_data(),
            wrapper,
            120,
            1,
        ),
        emitted(
            ApprovalForAll {
                owner: HOLDER.parse()?,
                operator: GRANTEE.parse()?,
                approved: true,
            }
            .encode_log_data(),
            wrapper,
            121,
            0,
        ),
    ];
    history_v1_payments::seed_and_run(&database, CHAIN, &logs, 121).await?;

    // The event feeds serve the approval with its own power.
    for route in [
        format!("/v1/events?type=permission&contract_address={wrapper:#x}"),
        format!("/v1/events?namespace=ens&type=permission&address={GRANTEE}"),
        format!(
            "/v1/addresses/{GRANTEE}/history?namespace=ens&type=permission&relation=role_holder"
        ),
        format!("/v1/addresses/{HOLDER}/history?namespace=ens&type=permission&relation=owner"),
    ] {
        let body = page(&database, &route).await?;
        let approvals = action_rows(&body, "operator_approval_changed");
        assert_eq!(approvals.len(), 1, "{route}: {body:#}");
        let data = &approvals[0]["data"];
        assert_eq!(data["powers"], json!(["wrapper_control"]), "{route}");
        assert_eq!(data["owner"], HOLDER);
        assert_eq!(data["address"], GRANTEE);
        assert_eq!(data["approved"], true);
        assert_eq!(data["grant_scope"]["kind"], "account");
        assert_eq!(data["grant_scope"]["detail"]["authority_kind"], "wrapper");
        assert_eq!(
            data["grant_scope"]["detail"]["authority_contract"],
            format!("{wrapper:#x}")
        );
    }

    // An account-scoped approval names no name, so name history serves only the holder's rows.
    let history = page(
        &database,
        &format!("/v1/names/{NAME}/history?scope=both&type=permission"),
    )
    .await?;
    assert!(
        action_rows(&history, "operator_approval_changed").is_empty(),
        "{history:#}"
    );
    assert!(
        !history["data"].as_array().context("rows")?.is_empty(),
        "{history:#}"
    );
    assert!(!carries_wrapper_control(&history), "{history:#}");

    // Permission reads compose the operator from the holder's served row.
    for route in [
        format!("/v1/permissions?name={NAME}"),
        format!("/v1/permissions?name={NAME}&include=lineage"),
        format!("/v1/permissions?namespace=ens&address={GRANTEE}"),
    ] {
        let body = current(&database, &route).await?;
        let rows = body["data"].as_array().context("permission rows")?;
        let operator = rows
            .iter()
            .find(|row| row["address"] == GRANTEE && row["grant_scope"]["kind"] == "registration")
            .with_context(|| format!("{route}: operator row in {body:#}"))?;
        assert!(!operator["powers"].as_array().context("powers")?.is_empty());
        if let Some(holder) = rows
            .iter()
            .find(|row| row["address"] == HOLDER && row["grant_scope"]["kind"] == "registration")
        {
            assert_eq!(operator["powers"], holder["powers"], "{route}: {body:#}");
        }
        assert!(!carries_wrapper_control(&body), "{route}: {body:#}");
    }

    // An operator grant joins the holder's address-name row and never adds a membership row.
    let names = current(
        &database,
        &format!("/v1/addresses/{HOLDER}/names?namespace=ens&relation=owner&include=role_summary"),
    )
    .await?;
    let grants = names["data"]
        .as_array()
        .context("address names")?
        .iter()
        .filter(|name| name["name"] == NAME)
        .flat_map(|name| name["role_summary"].as_array().cloned().unwrap_or_default())
        .filter(|summary| summary["address"] == GRANTEE)
        .flat_map(|summary| summary["grants"].as_array().cloned().unwrap_or_default())
        .collect::<Vec<_>>();
    assert!(
        grants.iter().any(|grant| grant["powers"]
            .as_array()
            .is_some_and(|powers| !powers.is_empty())),
        "{names:#}"
    );
    assert!(!carries_wrapper_control(&names), "{names:#}");
    database.cleanup().await
}
