//! The original child's registry-only resource after its unwrap past the parent's locked
//! ENSv1→ENSv2 migration, through Engine, Project and the public permission routes. That
//! unwrap opens no successor, so GRANTEE holds the resource as the child's ENSv1 registry
//! owner with nothing selected. GRANTEE's next registry write moves the resource to the owner it
//! names, and a re-wrap moves it to the NameWrapper, so GRANTEE keeps no row on it either way.
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L16-L21 @ ens_v1@91c966f)
//! (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L69 @ ens_v1@91c966f)
use super::*;
use crate::tests::v2_registry_only_wrap_grants::{served, serves_only_at};
use crate::tests::v2_sepolia_redeploy::get;

const NEXT_OWNER: &str = "0x0000000000000000000000000000000000c0ff1e";

/// The original child's registry-only resource: the resource of its registry-only permission
/// rows. There is exactly one.
async fn registry_only_resource(database: &TestDatabase) -> Result<Uuid> {
    let original: B256 = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let resources: Vec<Uuid> = sqlx::query_scalar(
        "SELECT DISTINCT resource_id FROM normalized_events
         WHERE event_kind='PermissionChanged' AND logical_name_id=$1
           AND COALESCE(after_state->'grant_source', after_state->'revocation_source')
               ->>'authority_kind'='registry_only'",
    )
    .bind(format!("ens:{original:#x}"))
    .fetch_all(&database.pool)
    .await?;
    let [resource] = resources.as_slice() else {
        anyhow::bail!("the original child has one registry-only resource: {resources:?}");
    };
    Ok(*resource)
}

/// GRANTEE's `setOwner` of the original child to NEXT_OWNER after the locked unwrap. The
/// registry-only resource then serves only NEXT_OWNER, by registration and as the name's
/// current authority, and GRANTEE keeps no row on it.
#[tokio::test]
async fn a_registry_write_after_the_locked_unwrap_moves_the_registry_only_resource() -> Result<()> {
    let (database, _) = unwrapped_rebound().await?;
    let original: B256 = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let write = transaction(
        125,
        0,
        vec![(
            V1.parse()?,
            registry::Transfer {
                node: original,
                owner: NEXT_OWNER.parse()?,
            }
            .encode_log_data(),
        )],
    );
    seed_and_run_with(&database, &write, 125, 125, &[(125, 0, GRANTEE)], None).await?;
    let resource = registry_only_resource(&database).await?;
    let failures = serves_only_at(&database, CHILD, resource, NEXT_OWNER, &[GRANTEE]).await?;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    let event: Value = sqlx::query_scalar(
        "SELECT to_jsonb(event) FROM project_registry_owner_event event
         WHERE chain_id=$1 AND node=$2 AND source_family='ens_v1_registry_l1'
         ORDER BY block_number DESC, transaction_index DESC, log_index DESC LIMIT 1",
    )
    .bind(PATH_CHAIN)
    .bind(format!("{original:#x}"))
    .fetch_one(&database.pool)
    .await?;
    assert_eq!(
        event["owner_getter"], NEXT_OWNER,
        "the registry write names the child's registry owner: {event:#}"
    );
    database.cleanup().await
}

/// GRANTEE's `wrap` of the original child after the locked unwrap: the registry write to the
/// NameWrapper, the mint, then `NameWrapped` with fuses 0 and expiry 0. The registry-only
/// resource then serves only the NameWrapper, as an audit of that resource, GRANTEE keeps no
/// row on it, and the name serves GRANTEE as the wrapper token's holder only.
/// (upstream: .refs/ens_v1/contracts/wrapper/NameWrapper.sol:L364-L374 @ ens_v1@91c966f)
#[tokio::test]
async fn a_rewrap_after_the_locked_unwrap_moves_the_registry_only_resource_to_the_name_wrapper()
-> Result<()> {
    let (database, _) = unwrapped_rebound().await?;
    let original: B256 = bigname_lookup::ens_namehash_hex(CHILD)?.parse()?;
    let wrapper: Address = WRAPPER.parse()?;
    let holder: Address = GRANTEE.parse()?;
    let rewrap = transaction(
        125,
        0,
        vec![
            (
                V1.parse()?,
                registry::Transfer {
                    node: original,
                    owner: wrapper,
                }
                .encode_log_data(),
            ),
            (
                wrapper,
                TransferSingle {
                    operator: holder,
                    from: Address::ZERO,
                    to: holder,
                    id: U256::from_be_bytes(original.0),
                    value: U256::from(1),
                }
                .encode_log_data(),
            ),
            (
                wrapper,
                NameWrapped {
                    node: original,
                    name: bigname_domain::normalization::normalize_name(CHILD)?
                        .dns_encoded_name
                        .into(),
                    owner: holder,
                    fuses: 0,
                    expiry: 0,
                }
                .encode_log_data(),
            ),
        ],
    );
    seed_and_run_with(&database, &rewrap, 125, 125, &[(125, 0, GRANTEE)], None).await?;
    let resource = registry_only_resource(&database).await?;
    let mut failures = Vec::new();
    let (status, by_registration) = get(
        &database,
        &format!("/v1/permissions?registration_id={resource}"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{by_registration:#}");
    let by_registration_served = served(&by_registration);
    let audit = [(WRAPPER.to_owned(), "resource_audit".to_owned())]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    if by_registration_served != audit {
        failures.push(format!(
            "registration {resource} serves {by_registration_served:?}: {by_registration:#}"
        ));
    }
    let (status, by_address) =
        get(&database, &format!("/v1/permissions?address={GRANTEE}")).await?;
    assert_eq!(status, StatusCode::OK, "{by_address:#}");
    let on_resource = by_address["data"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter(|row| row["registration_id"] == resource.to_string())
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !on_resource.is_empty() {
        failures.push(format!(
            "{GRANTEE} keeps rows on registration {resource}: {on_resource:#?}"
        ));
    }
    let (status, by_name) = get(
        &database,
        &format!("/v1/permissions?name={CHILD}&namespace=ens"),
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{by_name:#}");
    let holders = by_name["data"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row["address"].as_str().map(str::to_owned))
                .collect::<std::collections::BTreeSet<_>>()
        })
        .unwrap_or_default();
    if holders != [GRANTEE.to_owned()].into_iter().collect() {
        failures.push(format!(
            "the re-wrapped name serves {holders:?}, not only its holder: {by_name:#}"
        ));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    database.cleanup().await
}
