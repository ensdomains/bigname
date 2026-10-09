//! Overlapping mounts of a migrated wrapper registry. When the claimed entry expires, the
//! registry moves to its other live mount, `native105.eth`, where an ENSv1-held name has the
//! same namehash. An independently live physical route keeps serving the deeper ENSv1 names.
//! These are constructed admitted ABI logs through Interpret/Project, not EVM execution.
use super::*;
use std::collections::BTreeMap;

const ALT: &str = "native105.eth";
const LEAF: &str = "leaf.native105.eth";
const DEEP: &str = "deep.branch.native105.eth";
const RESET: &str = "reset.branch.native105.eth";
const USER: &str = "0x9bd8a88719068d09ecee662f36c0e3856708366a";
const X: &str = "0x0000000000000000000000000000000000001055";
const T: u64 = RESERVATION_EXPIRY;

fn reserve(
    registry: Address,
    label: &str,
    subregistry: Address,
    resolver: Address,
    expiry: u64,
    sender: Address,
) -> Vec<(Address, alloy_primitives::LogData)> {
    let id = label_token(label);
    let mut events = vec![(
        registry,
        LabelReserved {
            tokenId: id,
            labelHash: keccak256(label),
            label: label.into(),
            expiry,
            sender,
        }
        .encode_log_data(),
    )];
    if subregistry != Address::ZERO {
        events.push((
            registry,
            SubregistryUpdated {
                tokenId: id,
                subregistry,
                sender,
            }
            .encode_log_data(),
        ));
    }
    if resolver != Address::ZERO {
        events.push((
            registry,
            ResolverUpdated {
                tokenId: id,
                resolver,
                sender,
            }
            .encode_log_data(),
        ));
    }
    events
}

fn user_registry(owner: Address) -> Result<Vec<(Address, alloy_primitives::LogData)>> {
    let x = X.parse()?;
    let implementation = USER.parse()?;
    // UserRegistry.initialize accepts these root grants. In particular the reserved-entry
    // claim below has REGISTER_RESERVED, independently of W's expired canonical owner.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/UserRegistry.sol:L62-L70 @ ens_v2_sepolia_20261001@07e55a05)
    Ok(vec![
        (x, Upgraded { implementation }.encode_log_data()),
        (x, RegistryCreated {}.encode_log_data()),
        (
            x,
            EACRolesChanged {
                resource: U256::ZERO,
                account: owner,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: roles(&[0, 4, 8, 12, 16, 20, 24, 128, 132, 136, 140, 144, 148, 152]),
            }
            .encode_log_data(),
        ),
        (
            FACTORY.parse()?,
            ProxyDeployed {
                sender: owner,
                proxyAddress: x,
                salt: U256::from(1055),
                implementation,
            }
            .encode_log_data(),
        ),
    ])
}

fn native_registration(
    label: &str,
    version: u32,
    expiry: u64,
    reserved: bool,
) -> Result<Vec<(Address, alloy_primitives::LogData)>> {
    let x = X.parse()?;
    let owner = HOLDER.parse()?;
    let id = label_token(label) | U256::from(version);
    let mut events = Vec::new();
    if version > 0 {
        // Re-registering the expired, still-minted entry burns the old token and advances
        // both independent versions before minting; no old pointer is inherited.
        // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L486-L506 @ ens_v2_sepolia_20261001@07e55a05)
        events.push((
            x,
            TransferSingle {
                operator: owner,
                from: owner,
                to: Address::ZERO,
                id: label_token(label) | U256::from(version - 1),
                value: U256::from(1),
            }
            .encode_log_data(),
        ));
    }
    events.extend([
        (
            x,
            LabelRegistered {
                tokenId: id,
                labelHash: keccak256(label),
                label: label.into(),
                owner,
                expiry,
                sender: owner,
            }
            .encode_log_data(),
        ),
        (
            x,
            TransferSingle {
                operator: owner,
                from: Address::ZERO,
                to: owner,
                id,
                value: U256::from(1),
            }
            .encode_log_data(),
        ),
        (
            x,
            TokenResource {
                tokenId: id,
                resource: id,
            }
            .encode_log_data(),
        ),
        (
            x,
            EACRolesChanged {
                resource: id,
                account: owner,
                oldRoleBitmap: U256::ZERO,
                newRoleBitmap: roles(&[20, 24, 148, 152, 156])
                    | if reserved {
                        U256::from(1) << 32
                    } else {
                        U256::ZERO
                    },
            }
            .encode_log_data(),
        ),
    ]);
    Ok(events)
}

fn retained_names(resolver: Address) -> Result<Vec<RawLogInput>> {
    let v1 = V1.parse()?;
    let owner = HOLDER.parse()?;
    let mut logs = Vec::new();
    for (i, name) in [ALT, "branch.native105.eth", LEAF, DEEP, RESET]
        .into_iter()
        .enumerate()
    {
        let (label, parent) = name.split_once('.').context("child name")?;
        let node: B256 = bigname_lookup::ens_namehash_hex(name)?.parse()?;
        // ALT is an independently held ENSv1 parent. Its owner restates ownership with
        // setOwner before creating the children; this is not an EOA writing the eth node.
        // (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L63-L84 @ ens_v1@91c966f)
        let ownership = if name == ALT {
            registry::Transfer { node, owner }.encode_log_data()
        } else {
            NewOwner {
                node: bigname_lookup::ens_namehash_hex(parent)?.parse()?,
                label: keccak256(label),
                owner,
            }
            .encode_log_data()
        };
        let mut events = vec![(v1, ownership)];
        if [LEAF, DEEP, RESET].contains(&name) {
            events.push((v1, NewResolver { node, resolver }.encode_log_data()));
        }
        logs.extend(transaction(123, 10 + i as i64 * 2, events));
        let mut records = vec![(
            resolver,
            NameChanged {
                node,
                name: name.into(),
            }
            .encode_log_data(),
        )];
        if [LEAF, DEEP, RESET].contains(&name) {
            records.push((
                resolver,
                AddressChanged {
                    node,
                    coinType: U256::from(60),
                    newAddress: owner.to_vec().into(),
                }
                .encode_log_data(),
            ));
        }
        logs.extend(transaction(123, 11 + i as i64 * 2, records));
    }
    Ok(logs)
}

async fn physical_resource(database: &TestDatabase, registry: &str, label: &str) -> Result<Uuid> {
    sqlx::query_scalar(
        "SELECT resource_id FROM normalized_events WHERE chain_id=$1
         AND raw_fact_ref->>'emitting_address'=$2 AND after_state->>'token_id'=$3
         AND resource_id IS NOT NULL AND consumer_visibility='activated'
         AND event_kind IN ('TokenResourceLinked','ResolverChanged')
         ORDER BY block_number DESC, transaction_index DESC NULLS LAST, log_index DESC NULLS LAST LIMIT 1",
    ).bind(PATH_CHAIN).bind(registry).bind(format!("{:#066x}",label_token(label)))
        .fetch_one(&database.pool).await.map_err(Into::into)
}

async fn setup_native() -> Result<(TestDatabase, Address, BTreeMap<&'static str, Uuid>)> {
    let (database, resolver) = wrapped_setup().await?;
    let owner = HOLDER.parse()?;
    let w = REGISTRY.parse()?;
    let x = X.parse()?;
    // The migrated parent did not burn CANNOT_CREATE_SUBDOMAIN. Its current holder may
    // register fresh native labels in W; their own expiry is not capped to the parent.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/migration/LockedWrapperReceiver.sol:L217-L218 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/WrapperRegistry.sol:L170-L185 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L463-L513 @ ens_v2_sepolia_20261001@07e55a05)
    let mut logs = transaction(123, 0, user_registry(owner)?);
    // X's holder has root ROLE_SET_PARENT (bit 8) from initialization; setParent only
    // changes X's canonical parent, independently of every registry that mounts X.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L175-L181 @ ens_v2_sepolia_20261001@07e55a05)
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/libraries/RegistryRolesLib.sol:L18-L21 @ ens_v2_sepolia_20261001@07e55a05)
    logs.extend(transaction(
        123,
        1,
        vec![(
            x,
            ParentUpdated {
                parent: ETH.parse()?,
                label: "side105".into(),
                sender: owner,
            }
            .encode_log_data(),
        )],
    ));
    logs.extend(transaction(
        123,
        2,
        register(w, "leaf", resolver, Address::ZERO, T + 10, owner)?,
    ));
    logs.extend(transaction(
        123,
        3,
        register(w, "branch", Address::ZERO, x, T + 30, owner)?,
    ));
    logs.extend(transaction(
        123,
        4,
        register(x, "deep", resolver, Address::ZERO, T + 20, owner)?,
    ));
    logs.extend(transaction(
        123,
        5,
        reserve(x, "reset", Address::ZERO, resolver, T + 40, owner),
    ));
    // This independent reservation mounts W without changing W's canonical parent.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/BatchRegistrar.sol:L48-L71 @ ens_v2_sepolia_20261001@07e55a05)
    logs.extend(transaction(
        123,
        6,
        reserve(
            ETH.parse()?,
            "native105",
            w,
            Address::ZERO,
            T + 60,
            BATCH_REGISTRAR.parse()?,
        ),
    ));
    // X has its own live canonical mount outside W. A later X pointer change therefore
    // cannot select the alternate descendants merely by invalidating W's original name;
    // Project must traverse the physical W[branch] edge after W leaves its canonical name.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registrar/BatchRegistrar.sol:L48-L71 @ ens_v2_sepolia_20261001@07e55a05)
    logs.extend(transaction(
        123,
        7,
        reserve(
            ETH.parse()?,
            "side105",
            x,
            Address::ZERO,
            T + 60,
            BATCH_REGISTRAR.parse()?,
        ),
    ));
    logs.extend(retained_names(resolver)?);
    seed_and_run_with_targets(
        &database,
        &logs,
        123,
        123,
        &[(123, 6, DEPLOYER), (123, 7, DEPLOYER)],
        &[
            (123, 0, FACTORY),
            (123, 6, BATCH_REGISTRAR),
            (123, 7, BATCH_REGISTRAR),
        ],
        None,
    )
    .await?;
    let mut resources = BTreeMap::new();
    for (label, registry) in [
        ("leaf", REGISTRY),
        ("branch", REGISTRY),
        ("deep", X),
        ("reset", X),
    ] {
        resources.insert(label, physical_resource(&database, registry, label).await?);
    }
    for name in [LEAF, DEEP, RESET] {
        assert_name_consumers(&database, name, HOLDER, true, resolver, None).await?;
    }
    Ok((database, resolver, resources))
}

/// The claimed entry's expiry moves W's tokens from `NAME` to `native105.eth`. The old path is
/// released as expired and the new path is granted, both at the block boundary.
async fn assert_canonical_move(
    database: &TestDatabase,
    resources: &BTreeMap<&str, Uuid>,
    resolver: Address,
    immediate_state: bool,
) -> Result<()> {
    for (label, field) in [("leaf", "resolver"), ("branch", "subregistry")] {
        let kind = if field == "resolver" {
            "ResolverChanged"
        } else {
            "SubregistryChanged"
        };
        let retirement: Value = sqlx::query_scalar(
            "SELECT to_jsonb(event) FROM normalized_events event WHERE chain_id=$1 AND resource_id=$2
             AND block_number=$3 AND event_kind=$4 AND consumer_visibility='activated'
             AND after_state->>'source_event'='RegistryPathExpired'
             AND after_state->>'derived_from'='interpreter_state'
             AND after_state->>'terminal_reason'='registry_name_binding_expired'",
        ).bind(PATH_CHAIN).bind(resources[label]).bind(BASE+124).bind(kind)
            .fetch_one(&database.pool).await?;
        assert!(retirement["after_state"][field].is_null(), "{retirement:#}");
        let old = format!(
            "ens:{}",
            bigname_lookup::ens_namehash_hex(&format!("{label}.{NAME}"))?
        );
        assert_eq!(retirement["logical_name_id"], old, "{retirement:#}");
        if immediate_state {
            let expiry:String=sqlx::query_scalar("SELECT expiry::text FROM project_ens_v2_entry_owner WHERE chain_id=$1 AND resource_id=$2 AND status='registered'")
            .bind(PATH_CHAIN).bind(resources[label]).fetch_one(&database.pool).await?;
            assert!(
                expiry.parse::<u64>()? > T,
                "native own expiry must outlive canonical parent"
            );
        }
        let released: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM normalized_events WHERE resource_id=$1 AND block_number=$2
             AND event_kind='RegistrationReleased' AND after_state->>'source_event'='RegistryPathExpired'
             AND after_state->>'terminal_reason'='registry_name_binding_expired' AND consumer_visibility='activated')",
        ).bind(resources[label]).bind(BASE+124).fetch_one(&database.pool).await?;
        assert!(released, "canonical {label} registration must retire");
        // The same boundary grants the token under the mount and restates its pointer there.
        let moved = format!(
            "ens:{}",
            bigname_lookup::ens_namehash_hex(&format!("{label}.native105.eth"))?
        );
        let restated: Value = sqlx::query_scalar(
            "SELECT to_jsonb(event) FROM normalized_events event WHERE chain_id=$1 AND resource_id=$2
             AND block_number=$3 AND event_kind=$4 AND consumer_visibility='activated'
             AND logical_name_id=$5 AND transaction_index IS NULL",
        ).bind(PATH_CHAIN).bind(resources[label]).bind(BASE+124).bind(kind).bind(&moved)
            .fetch_one(&database.pool).await?;
        assert_eq!(
            restated["after_state"]["source_event"], "RegistryPathExpired",
            "{restated:#}"
        );
        assert!(
            restated["after_state"]["terminal_reason"].is_null(),
            "{restated:#}"
        );
        let granted: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM normalized_events WHERE resource_id=$1 AND block_number=$2
             AND event_kind='RegistrationGranted' AND logical_name_id=$3
             AND after_state->>'source_event'='RegistryPathExpired' AND consumer_visibility='activated')",
        ).bind(resources[label]).bind(BASE+124).bind(&moved).fetch_one(&database.pool).await?;
        assert!(granted, "{label} must be granted under its mount path");
        if immediate_state && field == "resolver" {
            let pointer: Value = sqlx::query_scalar("SELECT to_jsonb(row) FROM project_resource_pointer row WHERE chain_id=$1 AND resource_id=$2")
                .bind(PATH_CHAIN).bind(resources[label]).fetch_one(&database.pool).await?;
            assert_eq!(
                pointer["resolver_address"],
                format!("{resolver:#x}"),
                "{pointer:#}"
            );
            assert_eq!(
                pointer["namehash"],
                bigname_lookup::ens_namehash_hex(LEAF)?,
                "{pointer:#}"
            );
            assert_eq!(
                pointer["event_identity"], restated["event_identity"],
                "{pointer:#}"
            );
        }
    }
    let canonical = path_get(database, &format!("/v1/names/leaf.{NAME}")).await?;
    assert!(
        canonical["data"]["resolver"].is_null(),
        "canonical name still retired: {canonical:#}"
    );
    Ok(())
}

/// `leaf.native105.eth` after the move. The namehash also has an ENSv1 arm, the name the
/// ENSv1 registry holds. A name with an open ENSv2 binding selects the ENSv2 arm
/// (crates/storage/src/families/name/selection.rs:253-254), so the row serves W's `leaf` token.
/// The ENSv1 arm is carried as for a migrated name: no `ens_v1` object. No ENSv1→ENSv2
/// migration proved this name, so it has no `migrated_at`.
fn assert_moved_leaf(leaf: &Value, resolver: Address, resource: Uuid) {
    let data = &leaf["data"];
    assert_eq!(data["authority"], "ens_v2", "{leaf:#}");
    assert_eq!(data["status"], "active", "{leaf:#}");
    assert_eq!(data["owner"], HOLDER, "{leaf:#}");
    assert_eq!(data["registration_id"], resource.to_string(), "{leaf:#}");
    assert_eq!(
        data["resolver"]["address"],
        format!("{resolver:#x}"),
        "{leaf:#}"
    );
    assert_eq!(data["expires_at"], (T + 10).to_string(), "{leaf:#}");
    assert!(data.get("ens_v1").is_none(), "{leaf:#}");
    assert!(data.get("migrated_at").is_none(), "{leaf:#}");
}

/// `leaf.native105.eth` once W's `leaf` entry has expired. The row is a released ENSv2
/// registration with no resolver. Like any released ENSv2 row on main it keeps its serving
/// resource, with no record inventory, so the records route answers `inventory_not_available`
/// rather than `not_found` (crates/storage/src/name_current/row.rs:35-40, TYR-291).
async fn assert_released_leaf(
    database: &TestDatabase,
    address: &str,
    resource: Uuid,
) -> Result<()> {
    let detail = lookup_publication::assert_name_prepared_parity(database, LEAF).await?;
    assert_eq!(detail["authority"], "ens_v2", "{detail:#}");
    assert_eq!(detail["read_status"], "ok", "{detail:#}");
    assert_eq!(detail["status"], "released", "{detail:#}");
    assert_eq!(
        detail["registration_id"],
        resource.to_string(),
        "{detail:#}"
    );
    assert_eq!(
        detail["lapsed_registration"]["release_kind"], "expired",
        "{detail:#}"
    );
    assert_eq!(detail["lapsed_registration"]["owner"], HOLDER, "{detail:#}");
    assert!(detail.get("resolver").is_none(), "{detail:#}");
    assert!(detail.get("primary_address").is_none(), "{detail:#}");
    assert_eq!(
        detail["unsupported_fields"],
        json!(["primary_address"]),
        "{detail:#}"
    );
    let lookup = path_lookup(database, json!({"profile":"detail","inputs":[{"name":LEAF},{"address":address,"relation":"resolves_to"}]})).await?;
    assert_eq!(lookup["data"][0]["record"], detail, "{lookup:#}");
    let inverse = lookup["data"][1]["records"]
        .as_array()
        .context("inverse lookup records")?;
    assert!(!inverse.iter().any(|row| row["name"] == LEAF), "{lookup:#}");
    let records = path_get(
        database,
        &format!("/v1/names/{LEAF}/records?source=indexed&keys=addr:60"),
    )
    .await?;
    assert_eq!(
        records["data"]["records"]["addr:60"],
        json!({"status": "unsupported", "unsupported_reason": "inventory_not_available"}),
        "{records:#}"
    );
    let logical = format!("ens:{}", bigname_lookup::ens_namehash_hex(LEAF)?);
    let row = bigname_storage::families::name::load_family_name(&database.pool, &logical)
        .await?
        .context("leaf row")?;
    assert_eq!(row.record_serving_resource_id(), Some(resource));
    Ok(())
}

async fn assert_physical_deadlines(database: &TestDatabase) -> Result<()> {
    for (name, expiry) in [(LEAF, T + 10), (DEEP, T + 20)] {
        let deadline: Option<i64> = sqlx::query_scalar(
            "SELECT recompose_at FROM project_name_summary WHERE logical_name_id=$1",
        )
        .bind(format!("ens:{}", bigname_lookup::ens_namehash_hex(name)?))
        .fetch_one(&database.pool)
        .await?;
        assert_eq!(
            deadline,
            Some(expiry as i64),
            "physical deadline for {name}"
        );
    }
    Ok(())
}

async fn empty_clock(database: &TestDatabase, block: i64, clock: u64) -> Result<()> {
    seed_and_run_with(database, &[], block, block, &[], Some(clock as i64)).await
}

#[tokio::test]
async fn migrated_subname_canonical_expiry_moves_the_registry_to_its_live_mount() -> Result<()> {
    eprintln!(
        "TYR105 physical fingerprint {}",
        bigname_content_hash::INTERPRETER_CONTENT_HASH
    );
    let started = std::time::Instant::now();
    let (database, resolver, resources) = setup_native().await?;
    let before = replay::families(&database).await?;
    empty_clock(&database, 124, T).await?;
    assert_canonical_move(&database, &resources, resolver, true).await?;
    let leaf = path_get(&database, &format!("/v1/names/{LEAF}")).await?;
    assert_eq!(
        leaf["data"]["primary_address"], HOLDER,
        "Canonical parent expiry incorrectly withdrew the live alternate requested-node record: {leaf:#}"
    );
    assert_moved_leaf(&leaf, resolver, resources["leaf"]);
    for name in [LEAF, DEEP, RESET] {
        assert_name_consumers(&database, name, HOLDER, true, resolver, None).await?;
    }
    assert_physical_deadlines(&database).await?;
    let after = replay::families(&database).await?;
    assert_eq!(
        bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 123).await?,
        1
    );
    assert_eq!(
        replay::families(&database).await?,
        before,
        "canonical expiry undo"
    );
    for name in [LEAF, DEEP, RESET] {
        assert_name_consumers(&database, name, HOLDER, true, resolver, None).await?;
    }
    publish(&database, 124).await?;
    assert_eq!(
        replay::families(&database).await?,
        after,
        "canonical expiry reapply"
    );
    assert_canonical_move(&database, &resources, resolver, true).await?;
    for name in [LEAF, DEEP, RESET] {
        assert_name_consumers(&database, name, HOLDER, true, resolver, None).await?;
    }
    let owner = HOLDER.parse()?;
    let x = X.parse()?;
    let logical = format!("ens:{}", bigname_lookup::ens_namehash_hex(DEEP)?);
    assert_eq!(
        cutover_lookup(&database, PATH_CHAIN, &logical, GRANTEE).await?,
        bigname_lookup::LedgerAction::Written
    );
    let before_clear = replay::families(&database).await?;
    let clear = transaction(
        125,
        0,
        vec![(
            x,
            ResolverUpdated {
                tokenId: label_token("deep"),
                resolver: Address::ZERO,
                sender: owner,
            }
            .encode_log_data(),
        )],
    );
    seed_and_run_with(&database, &clear, 125, 125, &[], Some((T + 1) as i64)).await?;
    assert_name_consumers(&database, DEEP, HOLDER, false, resolver, None).await?;
    assert_name_consumers(&database, LEAF, HOLDER, true, resolver, None).await?;
    let active:i64=sqlx::query_scalar("SELECT count(*) FROM resolution_divergences WHERE logical_name_id=$1 AND cleared_at IS NULL")
        .bind(&logical).fetch_one(&database.pool).await?;
    assert_eq!(
        active, 0,
        "deeper physical change must select the alternate name for publication"
    );
    assert_eq!(
        bigname_project::families::undo_to(&database.pool, PATH_CHAIN, BASE + 124).await?,
        1
    );
    assert_eq!(
        replay::families(&database).await?,
        before_clear,
        "deeper change undo"
    );
    assert_name_consumers(&database, DEEP, HOLDER, true, resolver, None).await?;
    publish(&database, 125).await?;
    assert_name_consumers(&database, DEEP, HOLDER, false, resolver, None).await?;
    let restore = transaction(
        126,
        0,
        vec![(
            x,
            ResolverUpdated {
                tokenId: label_token("deep"),
                resolver,
                sender: owner,
            }
            .encode_log_data(),
        )],
    );
    seed_and_run_with(&database, &restore, 126, 126, &[], Some((T + 2) as i64)).await?;
    assert_name_consumers(&database, DEEP, HOLDER, true, resolver, None).await?;
    // A real reservation claim resets pointer storage, even though its zero arguments emit
    // no ResolverUpdated/SubregistryUpdated. This terminal clear must continue to win.
    // (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/registry/PermissionedRegistry.sol:L474-L513 @ ens_v2_sepolia_20261001@07e55a05)
    let claim = transaction(127, 0, native_registration("reset", 0, T + 40, true)?);
    seed_and_run_with(&database, &claim, 127, 127, &[], Some((T + 3) as i64)).await?;
    let reset:Value=sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE resource_id=$1 AND block_number=$2 AND event_kind='ResolverChanged' AND after_state->>'source_event'='LabelRegistered' AND consumer_visibility='activated'")
        .bind(resources["reset"]).bind(BASE+127).fetch_one(&database.pool).await?;
    assert!(reset["resolver"].is_null(), "{reset:#}");
    assert_eq!(
        physical_resource(&database, X, "reset").await?,
        resources["reset"],
        "reservation claim keeps this generation"
    );
    assert_name_consumers(&database, RESET, HOLDER, false, resolver, None).await?;
    assert_name_consumers(&database, DEEP, HOLDER, true, resolver, None).await?;
    empty_clock(&database, 128, T + 10).await?;
    assert_released_leaf(&database, HOLDER, resources["leaf"]).await?;
    assert_name_consumers(&database, DEEP, HOLDER, true, resolver, None).await?;
    empty_clock(&database, 129, T + 20).await?;
    assert_name_consumers(&database, DEEP, HOLDER, false, resolver, None).await?;
    let next = transaction(130, 0, native_registration("deep", 1, T + 50, false)?);
    seed_and_run_with(&database, &next, 130, 130, &[], Some((T + 21) as i64)).await?;
    let next_resource:Uuid=sqlx::query_scalar("SELECT resource_id FROM project_ens_v2_entry_owner WHERE chain_id=$1 AND registry=$2 AND entry_key=$3")
        .bind(PATH_CHAIN).bind(X).bind(format!("{:#066x}",label_token("deep"))).fetch_one(&database.pool).await?;
    assert_ne!(
        next_resource, resources["deep"],
        "new registration must use its new resource generation"
    );
    assert_name_consumers(&database, DEEP, HOLDER, false, resolver, None).await?;
    let record = transaction(
        131,
        0,
        vec![(
            resolver,
            AddressChanged {
                node: bigname_lookup::ens_namehash_hex(LEAF)?.parse()?,
                coinType: U256::from(60),
                newAddress: GRANTEE.parse::<Address>()?.to_vec().into(),
            }
            .encode_log_data(),
        )],
    );
    seed_and_run_with(&database, &record, 131, 131, &[], Some((T + 22) as i64)).await?;
    assert_released_leaf(&database, GRANTEE, resources["leaf"]).await?;
    replay::assert_rebuild(&database, 131).await?;
    assert_canonical_move(&database, &resources, resolver, false).await?;
    assert_released_leaf(&database, GRANTEE, resources["leaf"]).await?;
    for name in [DEEP, RESET] {
        assert_name_consumers(&database, name, HOLDER, false, resolver, None).await?;
    }
    let evidence = json!({"database":database.database_name,"fingerprint":bigname_content_hash::INTERPRETER_CONTENT_HASH,
        "total_ms":started.elapsed().as_millis(),"canonical_expiry":T,"resources":resources,"new_deep_resource":next_resource,
        "names":[LEAF,DEEP,RESET],"deep_logical_name_id":logical,"nested_clear_block":BASE+125,
        "generation_change_block":BASE+130,"record_only_block":BASE+131,"publication_block":BASE+131});
    eprintln!("TYR105 physical evidence {evidence}");
    if let Ok(path) = std::env::var("BIGNAME_TYR105_PHYSICAL_EVIDENCE") {
        std::fs::write(path, serde_json::to_vec_pretty(&evidence)?)?;
        database.pool.close().await;
        database.lookup_pool.close().await;
        Ok(())
    } else {
        database.cleanup().await
    }
}

#[path = "physical/classifier.rs"]
mod classifier;
