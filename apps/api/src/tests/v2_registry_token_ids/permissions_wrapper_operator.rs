//! A NameWrapper `setApprovalForAll` read back through `GET /v1/permissions` and address-name
//! `role_summary`. The reader composes the operator's row from the holder's row, and that row
//! carries `grant_relation=operator`. The holder's own row and the per-token delegate's row are
//! direct rows and omit the field (docs/api-v1.md, `grant_relation`).
//! (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L105-L117 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L374-L377 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L214-L238 @ ens_v1@91c966f)
use super::compatibility::{admit_family_from, role_address};
use super::*;

sol! {
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
    event Approval(address indexed owner, address indexed approved, uint256 indexed tokenId);
}

const DELEGATE: &str = "0x0000000000000000000000000000000000c0ffd1";

fn emitted(
    data: alloy_primitives::LogData,
    emitter: Address,
    block: i64,
    index: i64,
) -> RawLogInput {
    let mut log = raw(data, block, index);
    log.emitting_address = format!("{emitter:#x}");
    log
}

/// A served page, after the API contract's permission vocabulary check.
async fn current(database: &TestDatabase, route: &str) -> Result<Value> {
    let (status, body) = read_family_response(database, route).await?;
    assert_eq!(status, StatusCode::OK, "{route}: {body:#}");
    if let Err(error) = crate::tests::assert_permission_additive_vocabulary(&body) {
        panic!("{route}: {error}: {body:#}");
    }
    Ok(body)
}

/// `HOLDER` wraps the name, approves `GRANTEE` for all its tokens and approves `delegate` for
/// this token.
async fn seed(delegate: &str) -> Result<TestDatabase> {
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
        emitted(
            Approval {
                owner: HOLDER.parse()?,
                approved: delegate.parse()?,
                tokenId: U256::from_be_bytes(*node),
            }
            .encode_log_data(),
            wrapper,
            121,
            1,
        ),
    ];
    history_v1_payments::seed_and_run(&database, CHAIN, &logs, 121).await?;
    Ok(database)
}

/// Every served row on `route` names one of `expected`, each address in `expected` has a row,
/// and each row's `grant_relation` is the one `expected` gives its address.
fn assert_relations(route: &str, body: &Value, expected: &[(&str, Option<&str>)]) {
    let rows = body["data"].as_array().expect("permission rows");
    for (address, relation) in expected {
        let mine: Vec<&Value> = rows
            .iter()
            .filter(|row| row["address"] == *address)
            .collect();
        assert!(
            !mine.is_empty(),
            "{route}: no row for {address} in {body:#}"
        );
        for row in mine {
            assert_eq!(
                row.get("grant_relation").and_then(Value::as_str),
                *relation,
                "{route}: {address} in {row:#}"
            );
            assert_eq!(
                row["grant_scope"]["kind"], "registration",
                "{route}: {row:#}"
            );
        }
    }
    assert!(
        rows.iter().all(|row| expected
            .iter()
            .any(|(address, _)| row["address"] == *address)),
        "{route}: {body:#}"
    );
}

#[tokio::test]
async fn a_wrapper_operator_row_carries_the_operator_relation_on_every_permission_read()
-> Result<()> {
    let database = seed(DELEGATE).await?;
    let all = [
        (HOLDER, None),
        (GRANTEE, Some("operator")),
        (DELEGATE, None),
    ];
    let by_name = current(&database, &format!("/v1/permissions?name={NAME}")).await?;
    let registration = by_name["data"][0]["registration_id"]
        .as_str()
        .context("registration_id")?
        .to_owned();
    for route in [
        format!("/v1/permissions?name={NAME}"),
        format!("/v1/permissions?name={NAME}&include=lineage"),
        format!("/v1/permissions?namespace=ens&registration_id={registration}"),
        format!("/v1/permissions?namespace=ens&registration_id={registration}&include=lineage"),
    ] {
        assert_relations(&route, &current(&database, &route).await?, &all);
    }
    for (address, relation) in all {
        for route in [
            format!("/v1/permissions?namespace=ens&address={address}"),
            format!("/v1/permissions?namespace=ens&address={address}&include=lineage"),
            format!("/v1/permissions?name={NAME}&address={address}"),
        ] {
            assert_relations(
                &route,
                &current(&database, &route).await?,
                &[(address, relation)],
            );
        }
    }

    let route =
        format!("/v1/addresses/{HOLDER}/names?namespace=ens&relation=owner&include=role_summary");
    let names = current(&database, &route).await?;
    let summary = names["data"]
        .as_array()
        .context("address names")?
        .iter()
        .find(|name| name["name"] == NAME)
        .and_then(|name| name["role_summary"].as_array())
        .with_context(|| format!("{route}: role_summary in {names:#}"))?;
    for (address, relation) in all {
        let grants: Vec<&Value> = summary
            .iter()
            .filter(|role| role["address"] == address)
            .flat_map(|role| role["grants"].as_array().into_iter().flatten())
            .collect();
        assert!(
            !grants.is_empty(),
            "{route}: no grant for {address} in {names:#}"
        );
        for grant in grants {
            assert_eq!(
                grant.get("grant_relation").and_then(Value::as_str),
                relation,
                "{route}: {address} in {grant:#}"
            );
        }
    }
    database.cleanup().await
}

/// An operator who is also the token's delegate keeps the operator set on one row, and that row
/// is the operator's.
#[tokio::test]
async fn a_wrapper_operator_who_is_also_the_token_delegate_serves_one_operator_row() -> Result<()> {
    let database = seed(GRANTEE).await?;
    for route in [
        format!("/v1/permissions?name={NAME}"),
        format!("/v1/permissions?namespace=ens&address={GRANTEE}"),
    ] {
        let body = current(&database, &route).await?;
        let operator: Vec<&Value> = body["data"]
            .as_array()
            .context("permission rows")?
            .iter()
            .filter(|row| row["address"] == GRANTEE)
            .collect();
        assert_eq!(operator.len(), 1, "{route}: {body:#}");
        assert_eq!(
            operator[0]["grant_relation"], "operator",
            "{route}: {body:#}"
        );
        assert!(
            operator[0]["powers"]
                .as_array()
                .is_some_and(|powers| powers.len() > 1),
            "{route}: {body:#}"
        );
    }
    database.cleanup().await
}
