//! `GET /v1/permissions?address=` pages of grants on resources that have no current name. For
//! each such row the route looks up the registrar lease that a registry-only authority stands
//! for, in one statement per page (`load_registry_permission_registration_map`). These tests
//! read pages of every size the route serves, over anchors of each input class, and check that
//! every row carries the registration handle that lookup implies.

use super::*;

const CHAIN: &str = "ethereum-mainnet";
const REGISTRY: &str = "0x00000000000000000000000000000000000000a1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000a2";
const OWNER: &str = V2_PERMISSIONS_SUBJECT;
/// Holds one anchor without a lease among 40 other nameless resources.
const SMALL_OWNER: &str = "0x0000000000000000000000000000000000000cc3";
const IDLE_ADDRESS: &str = "0x0000000000000000000000000000000000000cc4";
const FILLERS: u128 = 157;
const SMALL_FILLERS: u128 = 40;

/// One registry-only anchor: its lease, if any, how many token transfers the lease saw before
/// the registry authority, whether the lease was released, and whether the anchor also holds
/// registry-only authority on a second node.
struct Anchor {
    lease: bool,
    transfers: i64,
    released: bool,
    second_node: bool,
}

const ANCHORS: [Anchor; 10] = [
    Anchor {
        lease: false,
        transfers: 0,
        released: false,
        second_node: false,
    },
    Anchor {
        lease: true,
        transfers: 1,
        released: false,
        second_node: false,
    },
    Anchor {
        lease: true,
        transfers: 2,
        released: false,
        second_node: false,
    },
    Anchor {
        lease: true,
        transfers: 1,
        released: true,
        second_node: false,
    },
    Anchor {
        lease: true,
        transfers: 1,
        released: false,
        second_node: false,
    },
    Anchor {
        lease: true,
        transfers: 1,
        released: false,
        second_node: true,
    },
    Anchor {
        lease: false,
        transfers: 0,
        released: false,
        second_node: false,
    },
    Anchor {
        lease: false,
        transfers: 0,
        released: false,
        second_node: false,
    },
    Anchor {
        lease: false,
        transfers: 0,
        released: false,
        second_node: false,
    },
    Anchor {
        lease: false,
        transfers: 0,
        released: false,
        second_node: false,
    },
];

fn anchor(index: u128) -> Uuid {
    Uuid::from_u128(0xa000 + index)
}

fn lease(index: u128) -> Uuid {
    Uuid::from_u128(0xd000 + index)
}

fn filler(index: u128) -> Uuid {
    Uuid::from_u128(0xb000 + index)
}

fn small_anchor() -> Uuid {
    Uuid::from_u128(0xc000)
}

fn small_filler(index: u128) -> Uuid {
    Uuid::from_u128(0xc001 + index)
}

fn node(resource: Uuid) -> String {
    format!("0x{:064x}", resource.as_u128())
}

#[tokio::test]
async fn address_pages_carry_the_lease_of_each_nameless_registry_grant() -> Result<()> {
    let database = TestDatabase::new_migrated().await?;
    let result = check_address_pages(&database).await;
    database.cleanup().await?;
    result
}

async fn check_address_pages(database: &TestDatabase) -> Result<()> {
    let owned = (0..10)
        .map(anchor)
        .chain((0..FILLERS).map(filler))
        .collect::<Vec<_>>();
    let small = std::iter::once(small_anchor())
        .chain((0..SMALL_FILLERS).map(small_filler))
        .collect::<Vec<_>>();
    seed_grants(database, &owned, &small).await?;
    seed_registry_evidence(database).await?;

    let mut resources = owned.clone();
    resources.extend(&small);
    let names = bigname_storage::load_current_names_by_resource_ids(
        &database.pool,
        &resources,
        &[CHAIN.to_owned()],
    )
    .await?;
    assert!(
        names.is_empty(),
        "the page resources must have no current name: {:?}",
        names.keys().collect::<Vec<_>>()
    );

    // Rows are in resource order under one subject, anchors first. A mapped anchor's row
    // carries its lease; every other row carries its own resource.
    let handles = owned
        .iter()
        .enumerate()
        .map(|(index, resource)| {
            let index = index as u128;
            let mapped = ANCHORS
                .get(index as usize)
                .is_some_and(|anchor| anchor.lease && !anchor.released);
            (if mapped { lease(index) } else { *resource }).to_string()
        })
        .collect::<Vec<_>>();
    for page_size in [1_usize, 2, 5, 10, 50, 200] {
        let page = page_handles(database, OWNER, page_size).await?;
        let expected = &handles[..page_size.min(handles.len())];
        assert_eq!(page, expected, "address page of {page_size}");
    }
    let small_handles = small.iter().map(Uuid::to_string).collect::<Vec<_>>();
    assert_eq!(
        page_handles(database, SMALL_OWNER, 50).await?,
        small_handles
    );

    let idle = v2_permissions_payload_for_database(
        database,
        &format!("/v1/permissions?address={IDLE_ADDRESS}&page_size=50"),
    )
    .await?;
    assert_eq!(idle["data"], json!([]), "{idle}");
    Ok(())
}

async fn page_handles(
    database: &TestDatabase,
    address: &str,
    page_size: usize,
) -> Result<Vec<String>> {
    let payload = v2_permissions_payload_for_database(
        database,
        &format!("/v1/permissions?address={address}&page_size={page_size}"),
    )
    .await?;
    Ok(payload["data"]
        .as_array()
        .unwrap_or_else(|| panic!("no rows: {payload}"))
        .iter()
        .map(|row| {
            row["registration_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect())
}

/// Each page resource holds one retained registry-only resource grant to its owner, reduced by
/// Project as a nameless resource grant.
async fn seed_grants(database: &TestDatabase, owned: &[Uuid], small: &[Uuid]) -> Result<()> {
    let at = parse_rfc3339_utc_timestamp("2026-06-10T00:00:00Z")
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .unix_timestamp();
    let blocks = (100..=130)
        .map(|number| {
            let mut block = raw_block(
                CHAIN,
                &format!("0xhistory{number}"),
                None,
                number,
                at - 130 + number,
            );
            block.canonicality_state = CanonicalityState::Canonical;
            block
        })
        .collect::<Vec<_>>();
    upsert_phase_raw_blocks(&database.pool, &blocks).await?;
    database
        .seed_snapshot_selector_chain_positions(&json!({"ethereum":{"chain_id":CHAIN,
            "block_number":130,"block_hash":"0xhistory130","timestamp":"2026-06-10T00:00:00Z"}}))
        .await?;
    let leases = (0..10).map(lease);
    for resource in owned.iter().chain(small).copied().chain(leases) {
        sqlx::query(
            "INSERT INTO resources (resource_id, chain_id, block_number, block_hash, canonicality_state)
             VALUES ($1, $2, 100, '0xhistory100', 'canonical')",
        )
        .bind(resource)
        .bind(CHAIN)
        .execute(&database.pool)
        .await?;
    }
    let grants = owned
        .iter()
        .map(|resource| (OWNER, *resource))
        .chain(small.iter().map(|resource| (SMALL_OWNER, *resource)))
        .enumerate()
        .map(|(index, (subject, resource))| {
            let mut grant = v2_history_event(
                &format!("permission-map-grant-{resource}"),
                None,
                Some(resource),
                "PermissionChanged",
                101,
            );
            grant.source_family = "ens_v1_registry_l1".into();
            grant.log_index = Some(index as i64);
            grant.after_state = json!({"subject":subject,"scope":{"kind":"resource"},
                "effective_powers":["resource_control"], "grant_source":{"kind":"ens_v1_authority",
                    "authority_kind":"registry_only","source_event_kind":"AuthorityTransferred"},
                "revocation_source":null,"inheritance_path":[],
                "transfer_behavior":"replace_on_authority_change"});
            grant
        })
        .collect::<Vec<_>>();
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &grants).await?;
    rebuild_fixture_families(&database.pool, CHAIN, 130, "0xhistory130").await
}

/// An ENSRegistry `Transfer` is the registry's; every other kind here is the registrar's.
fn evidence(
    identity: String,
    resource: Uuid,
    kind: &str,
    block: i64,
    log: i64,
    state: Value,
) -> NormalizedEvent {
    let (family, emitter) = if kind == "AuthorityTransferred" {
        ("ens_v1_registry_l1", REGISTRY)
    } else {
        ("ens_v1_registrar_l1", REGISTRAR)
    };
    let mut event = v2_history_event(&identity, None, Some(resource), kind, block);
    event.source_family = family.to_owned();
    event.log_index = Some(log);
    event.raw_fact_ref = json!({"kind":"raw_log","chain_id":CHAIN,
        "block_hash":format!("0xhistory{block}"),"block_number":block,
        "transaction_hash":format!("0xtx{block}"),"transaction_index":0,"log_index":log,
        "emitting_address":emitter});
    event.after_state = state;
    event
}

fn registry_only(resource: Uuid, node: &str, block: i64, log: i64) -> NormalizedEvent {
    evidence(
        format!("permission-map-control-{resource}-{node}"),
        resource,
        "AuthorityTransferred",
        block,
        log,
        json!({"source_event":"Transfer","node":node,"owner":OWNER,"owner_getter":OWNER,
            "authority_kind":"registry_only",
            "authority_key":format!("registry-only:{CHAIN}:{node}")}),
    )
}

/// The registrar evidence of each anchor, then its registry-only authority, which stays the
/// latest authority on the node. Every anchor's events have log indexes of their own, so no
/// two anchors share a raw log.
async fn seed_registry_evidence(database: &TestDatabase) -> Result<()> {
    let mut events = Vec::new();
    for (index, spec) in (0_u128..).zip(ANCHORS.iter()) {
        let resource = anchor(index);
        let node = node(resource);
        let log = 10 * index as i64;
        if spec.lease {
            let lease = lease(index);
            let mut state = v2_history_after_state("RegistrationGranted");
            state["namehash"] = json!(node);
            events.push(evidence(
                format!("permission-map-lease-{lease}"),
                lease,
                "RegistrationGranted",
                102,
                log,
                state,
            ));
            for transfer in 0..spec.transfers {
                let block = 103 + transfer;
                for (kind, state) in [
                    (
                        "TokenControlTransferred",
                        json!({"source_event":"Transfer","namehash":node,"to":OWNER}),
                    ),
                    (
                        "AuthorityEpochChanged",
                        json!({"source_event":"Transfer","authority_kind":"registrar",
                            "authority_key":format!("registrar:{CHAIN}:{lease}"),
                            "registry_owner":OWNER}),
                    ),
                ] {
                    events.push(evidence(
                        format!("permission-map-{kind}-{lease}-{block}"),
                        lease,
                        kind,
                        block,
                        log,
                        state,
                    ));
                }
            }
            if spec.released {
                events.push(evidence(
                    format!("permission-map-release-{lease}"),
                    lease,
                    "RegistrationReleased",
                    105,
                    log,
                    json!({"namehash":node,"released_at":1_960_000_000_i64}),
                ));
            }
        }
        events.push(registry_only(resource, &node, 110, log));
        if spec.second_node {
            let second = format!("0x{:064x}", 0xe000 + index);
            events.push(registry_only(resource, &second, 111, log));
        }
    }
    let small = small_anchor();
    events.push(registry_only(small, &node(small), 110, 900));
    bigname_storage::insert_normalized_event_fixtures(&database.pool, &events).await?;
    Ok(())
}
