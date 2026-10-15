//! Moving a live `.eth` lease into the NameWrapper, through Engine, Project and the public
//! routes. Between the BaseRegistrar transfer to the NameWrapper and the reclaim's `NewOwner`,
//! the registry still names the holder while the token has left it. That state never outlives
//! the transaction, so no route may serve a permission row for a registry-only authority over
//! it, and the lease's control moves to the NameWrapper once, at the transfer.
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L246-L279 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L778-L811 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L878-L902 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L996-L1020 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/wrapper/ERC1155Fuse.sol:L228-L258 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L130-L152 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/ethregistrar/BaseRegistrarImplementation.sol:L172-L175 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L75-L84 @ ens_v1@91c966f)
use super::v2_continuing_lease::{Logs, publish};
use super::v2_sepolia_redeploy::{CHAIN, HEAD, get};
use super::*;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};

const ENS_REGISTRY: &str = "0x00000000000c2e074ec69a0dfb2997ba6c7d2e1e";
const NAME_WRAPPER: &str = "0x0635513f179d50a207757e05759cbd106d7dfce8";
const BASE_REGISTRAR: &str = "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85";
const WRAPPED_CONTROLLER: &str = "0xfed6a969aaa60e4961fcd3ebf1a2e8913ac65b72";
const HOLDER: &str = "0x00000000000000000000000000000000000000c3";
const RESOLVER: &str = "0x00000000000000000000000000000000000000e3";
const LEASE: u64 = 2_000_000_000;
const GRACE_PERIOD: u64 = 90 * 86_400;
const DOT_ETH_FUSES: u32 = 0x30000;
const PERMISSION_KINDS: [&str; 6] = [
    "PermissionChanged",
    "RootPermissionChanged",
    "PermissionScopeChanged",
    "RolesChanged",
    "EACRolesChanged",
    "AccountPermissionChanged",
];

sol! {
    event TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value);
    event NameWrapped(bytes32 indexed node, bytes name, address owner, uint32 fuses, uint64 expiry);
    event NewOwner(bytes32 indexed node, bytes32 indexed label, address owner);
    event NewResolver(bytes32 indexed node, address resolver);
    event NameRegistered(uint256 indexed id, address indexed owner, uint256 expires);
    event ApprovalForAll(address indexed owner, address indexed operator, bool approved);
}
mod erc721 {
    alloy_sol_types::sol! { event Transfer(address indexed from, address indexed to, uint256 indexed tokenId); }
}

struct Name {
    label: &'static str,
    labelhash: B256,
    node: B256,
    token: U256,
}

fn name(label: &'static str) -> Name {
    let labelhash = keccak256(label.as_bytes());
    let eth = eth_node();
    Name {
        label,
        labelhash,
        node: keccak256([eth.as_slice(), labelhash.as_slice()].concat()),
        token: U256::from_be_bytes(labelhash.0),
    }
}

fn eth_node() -> B256 {
    keccak256([B256::ZERO.as_slice(), keccak256(b"eth").as_slice()].concat())
}

/// The adapter's registry-only resource id for `node`.
fn registry_only_resource(node: B256) -> String {
    let hash = keccak256(format!("resource:registry-only:{CHAIN}:{node:#x}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex = hex::encode(bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

fn token_transfer(
    name: &Name,
    from: &str,
    to: &str,
) -> Result<(&'static str, alloy_primitives::LogData)> {
    Ok((
        BASE_REGISTRAR,
        erc721::Transfer {
            from: from.parse()?,
            to: to.parse()?,
            tokenId: name.token,
        }
        .encode_log_data(),
    ))
}

fn new_owner(name: &Name, owner: &str) -> Result<(&'static str, alloy_primitives::LogData)> {
    Ok((
        ENS_REGISTRY,
        NewOwner {
            node: eth_node(),
            label: name.labelhash,
            owner: owner.parse()?,
        }
        .encode_log_data(),
    ))
}

fn name_registered(name: &Name, owner: &str) -> Result<(&'static str, alloy_primitives::LogData)> {
    Ok((
        BASE_REGISTRAR,
        NameRegistered {
            id: name.token,
            owner: owner.parse()?,
            expires: U256::from(LEASE),
        }
        .encode_log_data(),
    ))
}

/// `BaseRegistrar.register` to the holder: `_mint`, `setSubnodeOwner`, `NameRegistered`.
fn registration(name: &Name) -> Result<Logs> {
    Ok(vec![
        token_transfer(name, "0x0000000000000000000000000000000000000000", HOLDER)?,
        new_owner(name, HOLDER)?,
        name_registered(name, HOLDER)?,
    ])
}

/// `_wrap`: the ERC1155 mint (operator is the wrap's caller), then `NameWrapped`.
fn wrap(name: &Name, operator: &str) -> Result<Logs> {
    let mut dns = vec![u8::try_from(name.label.len())?];
    dns.extend_from_slice(name.label.as_bytes());
    dns.extend_from_slice(b"\x03eth\0");
    let holder: Address = HOLDER.parse()?;
    Ok(vec![
        (
            NAME_WRAPPER,
            TransferSingle {
                operator: operator.parse()?,
                from: Address::ZERO,
                to: holder,
                id: U256::from_be_bytes(name.node.0),
                value: U256::from(1_u64),
            }
            .encode_log_data(),
        ),
        (
            NAME_WRAPPER,
            NameWrapped {
                node: name.node,
                name: dns.into(),
                owner: holder,
                fuses: DOT_ETH_FUSES,
                expiry: LEASE + GRACE_PERIOD,
            }
            .encode_log_data(),
        ),
    ])
}

/// The registrar transfer to the NameWrapper and the reclaim to it, then the mint by `operator`.
fn move_into_wrapper(name: &Name, operator: &str) -> Result<Logs> {
    let mut logs = vec![
        token_transfer(name, HOLDER, NAME_WRAPPER)?,
        new_owner(name, NAME_WRAPPER)?,
    ];
    logs.extend(wrap(name, operator)?);
    Ok(logs)
}

fn approval() -> Result<Logs> {
    Ok(vec![(
        BASE_REGISTRAR,
        ApprovalForAll {
            owner: HOLDER.parse()?,
            operator: NAME_WRAPPER.parse()?,
            approved: true,
        }
        .encode_log_data(),
    )])
}

fn set_resolver(name: &Name) -> Result<Logs> {
    Ok(vec![(
        ENS_REGISTRY,
        NewResolver {
            node: name.node,
            resolver: RESOLVER.parse()?,
        }
        .encode_log_data(),
    )])
}

/// Every served `type=permission` row for the name, the holder, the NameWrapper and the
/// namespace event feed, tagged with the route that served it.
async fn permission_rows(database: &TestDatabase, full: &str) -> Result<Vec<(String, Value)>> {
    let mut served = Vec::new();
    for route in [
        format!(
            "/v1/names/{full}/history?type=permission&include=data,raw&order=asc&page_size=200"
        ),
        format!(
            "/v1/addresses/{HOLDER}/history?namespace=ens&type=permission&include=data,raw&order=asc&page_size=200"
        ),
        format!(
            "/v1/addresses/{NAME_WRAPPER}/history?namespace=ens&type=permission&include=data,raw&order=asc&page_size=200"
        ),
        "/v1/events?namespace=ens&type=permission&include=data,raw&order=asc&page_size=200"
            .to_owned(),
    ] {
        let (status, body) = get(database, &route).await?;
        assert_eq!(status, StatusCode::OK, "{route}: {body}");
        let rows = body["data"]
            .as_array()
            .with_context(|| format!("{route}: {body}"))?;
        for row in rows {
            served.push((route.clone(), row.clone()));
        }
    }
    Ok(served)
}

/// The last block's `PermissionChanged` events whose grant or revocation source is a
/// registry-only authority, as (log index, subject, is a grant).
async fn registry_only_changes(database: &TestDatabase) -> Result<Vec<(i64, String, bool)>> {
    let changes: Vec<(i64, String, bool)> = sqlx::query_as(
        "SELECT log_index, lower(after_state->>'subject'), jsonb_array_length(after_state->'effective_powers') > 0
         FROM normalized_events
         WHERE chain_id=$1 AND block_number=$2 AND event_kind='PermissionChanged'
           AND (after_state#>>'{grant_source,authority_kind}'='registry_only'
             OR after_state#>>'{revocation_source,authority_kind}'='registry_only')
         ORDER BY log_index, normalized_event_id",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .fetch_all(&database.pool)
    .await?;
    Ok(changes)
}

/// Normalized events of the last block on the registry-only resource. The registry's read copy
/// of the resolver pointer, replayed when the name surface materializes, shares that resource
/// id, so it is left out.
async fn registry_only_events(database: &TestDatabase, node: B256) -> Result<Vec<(i64, String)>> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT log_index, event_kind FROM normalized_events
         WHERE chain_id=$1 AND block_number=$2 AND resource_id::text=$3
           AND NOT (event_kind = 'ResolverChanged'
                    AND after_state->>'surface_materialization' = 'true')
         ORDER BY log_index, normalized_event_id",
    )
    .bind(CHAIN)
    .bind(HEAD)
    .bind(registry_only_resource(node))
    .fetch_all(&database.pool)
    .await?;
    Ok(rows)
}

/// A normalized event as (block, log index, kind, resource id, after state).
type NormalizedRow = (Option<i64>, Option<i64>, String, Option<String>, Value);

/// Every normalized permission event, printed so a failure shows what Interpret wrote.
async fn print_permission_events(database: &TestDatabase, case: &str) -> Result<()> {
    let events: Vec<NormalizedRow> = sqlx::query_as(
        "SELECT block_number, log_index, event_kind, resource_id::text, after_state
         FROM normalized_events WHERE chain_id=$1 AND event_kind = ANY($2)
         ORDER BY block_number, log_index, normalized_event_id",
    )
    .bind(CHAIN)
    .bind(PERMISSION_KINDS.map(str::to_owned).to_vec())
    .fetch_all(&database.pool)
    .await?;
    for (block, log, kind, resource, after) in &events {
        println!(
            "WRAP {case} normalized block={block:?} log={log:?} kind={kind} resource={} after={after}",
            resource.as_deref().unwrap_or("-")
        );
    }
    Ok(())
}

/// Served last-block permission rows that are one of the registry-only grants or revokes.
fn last_block_registry_only_rows<'a>(
    served: &'a [(String, Value)],
    registry_only: &[(i64, String, bool)],
) -> Vec<&'a (String, Value)> {
    served
        .iter()
        .filter(|(_, row)| {
            let data = &row["data"];
            row["block_number"] == HEAD
                && row["kind"] == "PermissionChanged"
                && registry_only.iter().any(|(log, subject, grant)| {
                    row["log_index"] == *log
                        && data["address"]
                            .as_str()
                            .is_some_and(|address| address.eq_ignore_ascii_case(subject))
                        && data["powers"]
                            .as_array()
                            .is_some_and(|powers| powers.is_empty() != *grant)
                })
        })
        .collect()
}

/// A served history row as `block-offset log kind address powers scope registration`, with
/// the block counted back from the last block and the registration id shown as `lease` when it
/// is the one the registration granted.
fn describe(row: &Value, lease: &Value) -> String {
    let data = &row["data"];
    let registration = &row["registration_id"];
    let registration = if registration.is_null() {
        "null".to_owned()
    } else if registration == lease {
        "lease".to_owned()
    } else {
        registration.to_string()
    };
    format!(
        "{} {} {} {} {} {} {}",
        HEAD - row["block_number"].as_i64().unwrap_or_default(),
        row["log_index"],
        row["kind"].as_str().unwrap_or_default(),
        data["address"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase(),
        data["powers"],
        data["grant_scope"]["kind"].as_str().unwrap_or("-"),
        registration
    )
}

async fn name_history(database: &TestDatabase, full: &str) -> Result<Vec<String>> {
    let route = format!(
        "/v1/names/{full}/history?type=permission&include=data,raw&order=asc&page_size=200"
    );
    let (described, _) = served_history(database, &route, None).await?;
    Ok(described)
}

/// The rows a history route serves, described against the lease's registration id, which is
/// the first row's when none is given.
async fn served_history(
    database: &TestDatabase,
    route: &str,
    lease: Option<Value>,
) -> Result<(Vec<String>, Value)> {
    let (status, body) = get(database, route).await?;
    assert_eq!(status, StatusCode::OK, "{route}: {body}");
    let rows = body["data"]
        .as_array()
        .with_context(|| format!("{route}: {body}"))?;
    let lease = lease.unwrap_or_else(|| {
        rows.first()
            .map(|row| row["registration_id"].clone())
            .unwrap_or(Value::Null)
    });
    let described = rows
        .iter()
        .map(|row| describe(row, &lease))
        .collect::<Vec<_>>();
    for (row, raw) in described.iter().zip(rows) {
        println!("WRAP {route} {row} | {raw}");
    }
    Ok((described, lease))
}

/// The holder's and the NameWrapper's permission history, described against the name's lease.
async fn address_histories(database: &TestDatabase, full: &str) -> Result<[Vec<String>; 2]> {
    let name_route = format!("/v1/names/{full}/history?type=permission&order=asc&page_size=200");
    let (_, lease) = served_history(database, &name_route, None).await?;
    let mut histories = [Vec::new(), Vec::new()];
    for (history, address) in histories.iter_mut().zip([HOLDER, NAME_WRAPPER]) {
        let route = format!(
            "/v1/addresses/{address}/history?namespace=ens&type=permission&include=data,raw&order=asc&page_size=200"
        );
        *history = served_history(database, &route, Some(lease.clone()))
            .await?
            .0;
    }
    Ok(histories)
}

/// The current permission rows for the name and for the holder must not include the
/// registry-only resource.
async fn assert_current_rows_skip_registry_only(
    database: &TestDatabase,
    full: &str,
    node: B256,
) -> Result<()> {
    let registry_only = registry_only_resource(node);
    for route in [
        format!("/v1/permissions?name={full}&namespace=ens"),
        format!("/v1/permissions?address={HOLDER}&namespace=ens"),
        format!("/v1/permissions?address={NAME_WRAPPER}&namespace=ens"),
    ] {
        let (status, body) = get(database, &route).await?;
        assert_eq!(status, StatusCode::OK, "{route}: {body}");
        let rows = body["data"]
            .as_array()
            .with_context(|| format!("{route}: {body}"))?;
        let stale = rows
            .iter()
            .filter(|row| {
                row["registration_id"].is_null() || row["registration_id"] == registry_only.as_str()
            })
            .collect::<Vec<_>>();
        assert!(
            stale.is_empty(),
            "{route} serves a registry-only row: {stale:#?}"
        );
    }
    Ok(())
}

async fn assert_no_registry_only_rows(
    database: &TestDatabase,
    case: &str,
    full: &str,
    node: B256,
) -> Result<()> {
    print_permission_events(database, case).await?;
    let registry_only = registry_only_changes(database).await?;
    let served = permission_rows(database, full).await?;
    let leaked = last_block_registry_only_rows(&served, &registry_only);
    assert!(
        leaked.is_empty(),
        "{case}: registry-only rows served: {leaked:#?}"
    );
    assert_current_rows_skip_registry_only(database, full, node).await?;
    assert_eq!(
        registry_only,
        Vec::new(),
        "{case}: Interpret wrote registry-only permission changes"
    );
    let events = registry_only_events(database, node).await?;
    assert_eq!(
        events,
        Vec::new(),
        "{case}: Interpret wrote events on the registry-only resource"
    );
    Ok(())
}

/// The permission rows of the name's history, oldest first. The transfer moves the lease's
/// control, and with a resolver its `resolver_control`, from the holder to the NameWrapper. The
/// reclaim adds nothing, then the mint grants the holder the wrapper powers and the fuses scope
/// the registration. Without a resolver that is 6 rows.
fn expected_history(resolver: bool) -> Vec<String> {
    expected_history_after(resolver, if resolver { 3 } else { 2 })
}

/// The same rows when the registration is `registered` blocks before the wrap.
fn expected_history_after(resolver: bool, registered: i64) -> Vec<String> {
    let holder = HOLDER;
    let wrapper = NAME_WRAPPER;
    let wrapper_powers = r#"["registration_control","set_resolver","set_ttl","create_subnames","transfer","unwrap","burn_fuses","approve","extend_subname_expiry","extend_expiry"]"#;
    let mut rows = vec![
        format!(
            r#"{registered} 1 PermissionChanged {holder} ["registration_control"] registration lease"#
        ),
        format!(
            r#"{registered} 2 PermissionChanged {holder} ["registration_control"] registration lease"#
        ),
    ];
    if resolver {
        rows.extend([
            format!(r#"2 0 PermissionChanged {holder} ["resolver_control"] resolver lease"#),
            format!(r#"0 0 PermissionChanged {wrapper} ["resolver_control"] resolver lease"#),
            format!("0 0 PermissionChanged {holder} [] resolver lease"),
        ]);
    }
    rows.extend([
        format!(r#"0 0 PermissionChanged {wrapper} ["registration_control"] registration lease"#),
        format!("0 0 PermissionChanged {holder} [] registration lease"),
        format!("0 2 PermissionChanged {holder} {wrapper_powers} registration lease"),
    ]);
    if resolver {
        rows.push(format!(
            r#"0 2 PermissionChanged {holder} ["resolver_control"] resolver lease"#
        ));
    }
    rows.push("0 2 PermissionScopeChanged  null - lease".to_owned());
    rows
}

/// The NameWrapper's own permission history: the name's history without the mint's rows, which
/// are the holder's wrapper powers and the fuse scope.
fn expected_wrapper_history(resolver: bool) -> Vec<String> {
    expected_history(resolver)
        .into_iter()
        .filter(|row| !row.starts_with("0 2 "))
        .collect()
}

/// The holder registers, approves the NameWrapper on the BaseRegistrar, then calls
/// `wrapETH2LD`: `transferFrom` to the NameWrapper, `reclaim`, the mint and `NameWrapped`, all
/// in one transaction.
#[tokio::test]
async fn wrapping_a_live_lease_serves_no_registry_only_permission_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let probe = name("wrapprobe");
    let start = LEASE - 200 * 86_400;
    publish(
        &database,
        vec![
            (start, registration(&probe)?),
            (start + 60, approval()?),
            (start + 120, move_into_wrapper(&probe, HOLDER)?),
        ],
    )
    .await?;
    let history = name_history(&database, "wrapprobe.eth").await?;
    assert_no_registry_only_rows(&database, "wrap", "wrapprobe.eth", probe.node).await?;
    assert_eq!(history, expected_history(false));
    let [holder, wrapper] = address_histories(&database, "wrapprobe.eth").await?;
    assert_eq!(holder, expected_history(false));
    assert_eq!(wrapper, expected_wrapper_history(false));
    assert_eq!(history.len(), 6);
    database.cleanup().await
}

/// The same wrap of a lease whose registry record already has a resolver. `resolver_control`
/// moves from the holder to the NameWrapper on the lease at the transfer, with no
/// registry-only row between.
#[tokio::test]
async fn wrapping_a_lease_with_a_resolver_moves_resolver_control_once() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let probe = name("wrapresolved");
    let start = LEASE - 200 * 86_400;
    publish(
        &database,
        vec![
            (start, registration(&probe)?),
            (start + 30, set_resolver(&probe)?),
            (start + 60, approval()?),
            (start + 120, move_into_wrapper(&probe, HOLDER)?),
        ],
    )
    .await?;
    let history = name_history(&database, "wrapresolved.eth").await?;
    assert_no_registry_only_rows(&database, "resolver", "wrapresolved.eth", probe.node).await?;
    assert_eq!(history, expected_history(true));
    let [holder, wrapper] = address_histories(&database, "wrapresolved.eth").await?;
    assert_eq!(holder, expected_history(true));
    assert_eq!(wrapper, expected_wrapper_history(true));
    database.cleanup().await
}

/// The holder sends the registrar token straight to the NameWrapper with `safeTransferFrom`.
/// `onERC721Received` reclaims and wraps, so the mint's operator is the BaseRegistrar.
#[tokio::test]
async fn a_direct_token_send_serves_no_registry_only_permission_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let probe = name("wrapsent");
    let start = LEASE - 200 * 86_400;
    publish(
        &database,
        vec![
            (start, registration(&probe)?),
            (start + 120, move_into_wrapper(&probe, BASE_REGISTRAR)?),
        ],
    )
    .await?;
    let history = name_history(&database, "wrapsent.eth").await?;
    assert_no_registry_only_rows(&database, "direct", "wrapsent.eth", probe.node).await?;
    let expected = expected_history_after(false, 1);
    assert_eq!(history, expected);
    let [holder, wrapper] = address_histories(&database, "wrapsent.eth").await?;
    assert_eq!(holder, expected);
    assert_eq!(
        wrapper,
        expected
            .iter()
            .filter(|row| !row.starts_with("0 2 "))
            .cloned()
            .collect::<Vec<_>>()
    );
    database.cleanup().await
}

/// Control: the wrapped controller's `registerAndWrapETH2LD` registers straight into the
/// NameWrapper, so the registry never names the holder.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L289-L304 @ ens_v1@91c966f)
#[tokio::test]
async fn register_and_wrap_serves_no_registry_only_permission_rows() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let probe = name("wrapprobectl");
    let mut logs = vec![
        token_transfer(
            &probe,
            "0x0000000000000000000000000000000000000000",
            NAME_WRAPPER,
        )?,
        new_owner(&probe, NAME_WRAPPER)?,
        name_registered(&probe, NAME_WRAPPER)?,
    ];
    logs.extend(wrap(&probe, WRAPPED_CONTROLLER)?);
    publish(&database, vec![(LEASE - 200 * 86_400, logs)]).await?;
    name_history(&database, "wrapprobectl.eth").await?;
    print_permission_events(&database, "control").await?;
    let registry_only = registry_only_changes(&database).await?;
    let served = permission_rows(&database, "wrapprobectl.eth").await?;
    let leaked = last_block_registry_only_rows(&served, &registry_only);
    assert!(
        leaked.is_empty(),
        "registry-only permission rows served in the wrap block: {leaked:#?}"
    );
    database.cleanup().await
}
