//! A synthetic ENSv2 path for the family tests. Every helper writes normalized events, name
//! surfaces or bindings of the kinds and source families the family reducers read. None is
//! taken from a chain, and no comment here says what a contract emits. The fixture holds an
//! admitted root registry whose `eth` entry row points at a `.eth` registry address, entry
//! events a test writes for that address, and names whose surfaces carry the label hashes of
//! their labels, so Project's registry walk places them under `.eth`.
use alloy_primitives::{B256, U256, keccak256};
use anyhow::Result;
use serde_json::json;

use super::{CHAIN, Event, Fixture, hash, uuid};

pub mod declared_path;

pub const OWNER: &str = "0x00000000000000000000000000000000000000aa";
pub const RESOLVER: &str = "0x00000000000000000000000000000000000000d1";
const V1_REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const V1_REGISTRY: &str = "0x00000000000000000000000000000000000000e5";

fn namehash(name: &str) -> B256 {
    name.rsplit('.').fold(B256::ZERO, |parent, label| {
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(parent.as_slice());
        input[32..].copy_from_slice(keccak256(label.as_bytes()).as_slice());
        keccak256(input)
    })
}

pub fn logical_name_id(name: &str) -> String {
    format!("ens:{:#x}", namehash(name))
}

/// Admit the root registry from block 0 and write the events of its `eth` entry. Returns the
/// `.eth` registry address the written `SubregistryChanged` event carries.
pub async fn admit(fixture: &Fixture) -> Result<String> {
    declared_path::root_eth_entry(fixture).await?;
    Ok(sqlx::query_scalar(
        "SELECT after_state ->> 'subregistry' FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'SubregistryChanged'
           AND source_family = 'ens_v2_root_l1'",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?)
}

/// The surface of `name` at block 0, with its real namehash and label hashes.
pub async fn surface(fixture: &Fixture, name: &str) -> Result<String> {
    let logical_name_id = logical_name_id(name);
    let normalized = bigname_domain::normalization::normalize_name(name)?;
    let labelhashes: Vec<String> = normalized
        .normalized_labels
        .iter()
        .map(|label| format!("{:#x}", keccak256(label.as_bytes())))
        .collect();
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
             dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
             chain_id, block_hash, block_number, canonicality_state)
         VALUES ($1, 'ens', $2, $3, $4, $5, $6, $7, 'active', $8, $9, 0, 'canonical')
         ON CONFLICT DO NOTHING",
    )
    .bind(&logical_name_id)
    .bind(normalized.normalized_name)
    .bind(normalized.normalized_labels)
    .bind(normalized.dns_encoded_name)
    .bind(logical_name_id.trim_start_matches("ens:"))
    .bind(labelhashes)
    .bind(bigname_domain::normalization::ENS_NORMALIZER_VERSION)
    .bind(CHAIN)
    .bind(hash(0))
    .execute(&fixture.pool)
    .await?;
    Ok(logical_name_id)
}

/// Write a `ResolverChanged` event of source family `ens_v1_registry_l1` for the name at
/// `block`, log `log`, with `RESOLVER` as its resolver.
pub async fn resolver(
    fixture: &Fixture,
    logical_name_id: &str,
    block: i64,
    log: i64,
) -> Result<()> {
    let identity = format!("resolver-{logical_name_id}-{block}");
    fixture
        .event(
            Event::new(
                &identity,
                block,
                log,
                "ResolverChanged",
                "ens_v1_registry_l1",
            )
            .name(logical_name_id)
            .after(json!({"resolver": RESOLVER,
                              "node": logical_name_id.trim_start_matches("ens:")}))
            .raw(json!({"emitting_address": V1_REGISTRY})),
        )
        .await?;
    Ok(())
}

/// A second-level name with lease resource `n`, written as events of source family
/// `ens_v1_registrar_l1`: a binding and a `SurfaceBound` at `block`, a `RegistrationGranted`
/// until `expiry` one block later, and the `resolver` event one block after that. Returns its
/// logical name id.
pub async fn leased(
    fixture: &Fixture,
    name: &str,
    n: u32,
    block: i64,
    expiry: u64,
) -> Result<String> {
    let logical_name_id = surface(fixture, name).await?;
    let lease = uuid(0x1000 + n);
    fixture
        .binding(
            &uuid(100 + n),
            &logical_name_id,
            &lease,
            "ens_v1",
            block,
            n.into(),
            None,
        )
        .await?;
    fixture
        .event(
            Event::new(
                &format!("bound-{name}"),
                block,
                n.into(),
                "SurfaceBound",
                "ens_v1_registrar_l1",
            )
            .name(&logical_name_id)
            .resource(&lease)
            .after(
                json!({"authority_kind": "registrar", "state_derived": false,
                          "registry_contract": V1_REGISTRY, "owner_getter": OWNER}),
            )
            .raw(json!({"emitting_address": V1_REGISTRAR})),
        )
        .await?;
    fixture
        .event(
            Event::new(
                &format!("granted-{name}"),
                block + 1,
                n.into(),
                "RegistrationGranted",
                "ens_v1_registrar_l1",
            )
            .name(&logical_name_id)
            .resource(&lease)
            .after(
                json!({"authority_kind": "registrar", "status": "registered",
                          "registrant": OWNER, "expiry": expiry}),
            )
            .raw(json!({"emitting_address": V1_REGISTRAR})),
        )
        .await?;
    resolver(fixture, &logical_name_id, block + 2, (100 + n).into()).await?;
    Ok(logical_name_id)
}

/// A subname with resource `n`, written as events of source family `ens_v1_registry_l1`
/// alone: a binding and an `AuthorityTransferred` to `OWNER` at `block`, and the `resolver`
/// event two blocks later. Returns its logical name id.
pub async fn subname(fixture: &Fixture, name: &str, n: u32, block: i64) -> Result<String> {
    let logical_name_id = surface(fixture, name).await?;
    let node = uuid(0x3000 + n);
    fixture
        .binding(
            &uuid(300 + n),
            &logical_name_id,
            &node,
            "ens_v1",
            block,
            (50 + n).into(),
            None,
        )
        .await?;
    fixture
        .event(
            Event::new(
                &format!("owned-{name}"),
                block,
                (50 + n).into(),
                "AuthorityTransferred",
                "ens_v1_registry_l1",
            )
            .name(&logical_name_id)
            .resource(&node)
            .after(json!({"authority_kind": "registry", "owner": OWNER,
                          "registry_owner": OWNER,
                          "node": logical_name_id.trim_start_matches("ens:")}))
            .raw(json!({"emitting_address": V1_REGISTRY})),
        )
        .await?;
    resolver(fixture, &logical_name_id, block + 2, (150 + n).into()).await?;
    Ok(logical_name_id)
}

/// The ids the fixture writes for `label` in `registry`: the label hash with its low 32 bits
/// cleared as the token id, a contract instance id derived from the address, and the resource
/// id storage derives from both.
fn entry(registry: &str, label: &str) -> (String, String, String) {
    let token = format!(
        "{:#066x}",
        U256::from_be_bytes(*keccak256(label)) >> 32 << 32
    );
    let mut instance = [0_u8; 16];
    instance.copy_from_slice(&keccak256(registry)[..16]);
    let instance = uuid::Uuid::from_bytes(instance);
    let resource =
        bigname_storage::ens_v2_registry_resource_id(CHAIN, instance, &token).to_string();
    (token, instance.to_string(), resource)
}

/// Write the two events of source family `ens_v2_registry_l1` the family reducers read as a
/// registration of `label` to `OWNER` until `expiry`, with `registry` as their emitting
/// address: a `RegistrationGranted` at `block`, log `log`, and a `TokenResourceLinked` at
/// `log + 1`. The events carry no name, so only the entry links them to the names under the
/// label.
pub async fn register(
    fixture: &Fixture,
    block: i64,
    log: i64,
    registry: &str,
    label: &str,
    expiry: u64,
) -> Result<()> {
    let (token, instance, resource) = entry(registry, label);
    fixture.resource(&resource).await?;
    for (offset, kind, resource_id, after) in [
        (
            0,
            "RegistrationGranted",
            None,
            json!({"source_event": "LabelRegistered", "token_id": token, "label": label,
                   "label_hash": format!("{:#x}", keccak256(label)),
                   "registry_contract_instance_id": instance, "resource_pending": true,
                   "sender": OWNER, "registrant": OWNER, "authority_kind": "ens_v2_registry",
                   "status": "registered", "expiry": expiry}),
        ),
        (
            1,
            "TokenResourceLinked",
            Some(resource.as_str()),
            json!({"source_event": "TokenResource", "token_id": token,
                   "upstream_resource": token, "resource_id": resource,
                   "registry_contract_instance_id": instance}),
        ),
    ] {
        let identity = format!("{kind}:{label}:{block}:{}", log + offset);
        let mut event = Event::new(&identity, block, log + offset, kind, "ens_v2_registry_l1")
            .after(after)
            .raw(json!({"emitting_address": registry}));
        event.resource = resource_id;
        fixture.event(event).await?;
    }
    Ok(())
}

/// The pointer event `point` writes for `label`'s entry in `registry`.
pub enum Pointer<'a> {
    Resolver(&'a str),
    Subregistry(&'a str),
}

/// Write a `ResolverChanged` or `SubregistryChanged` event of source family
/// `ens_v2_registry_l1` for the entry of `label` at `block`, log `log`, with `registry` as its
/// emitting address. The event carries no name.
pub async fn point(
    fixture: &Fixture,
    block: i64,
    log: i64,
    registry: &str,
    label: &str,
    pointer: Pointer<'_>,
) -> Result<()> {
    pointed(fixture, block, log, registry, label, pointer, None).await
}

/// `point`, with `logical_name_id` as the name the written event carries.
pub async fn point_named(
    fixture: &Fixture,
    block: i64,
    log: i64,
    registry: &str,
    label: &str,
    pointer: Pointer<'_>,
    logical_name_id: &str,
) -> Result<()> {
    pointed(
        fixture,
        block,
        log,
        registry,
        label,
        pointer,
        Some(logical_name_id),
    )
    .await
}

async fn pointed(
    fixture: &Fixture,
    block: i64,
    log: i64,
    registry: &str,
    label: &str,
    pointer: Pointer<'_>,
    logical_name_id: Option<&str>,
) -> Result<()> {
    let (token, instance, resource) = entry(registry, label);
    let (kind, source, field, target) = match pointer {
        Pointer::Resolver(resolver) => ("ResolverChanged", "ResolverUpdated", "resolver", resolver),
        Pointer::Subregistry(subregistry) => (
            "SubregistryChanged",
            "SubregistryUpdated",
            "subregistry",
            subregistry,
        ),
    };
    let identity = format!("{kind}:{label}:{block}:{log}");
    let mut event = Event::new(&identity, block, log, kind, "ens_v2_registry_l1")
        .resource(&resource)
        .after(
            json!({"source_event": source, "token_id": token, field: target,
                      "sender": OWNER, "registry_contract_instance_id": instance}),
        )
        .raw(json!({"emitting_address": registry}));
    event.name = logical_name_id;
    fixture.event(event).await?;
    Ok(())
}

/// Write a `ParentChanged` event of source family `ens_v2_registry_l1` at `block`, log `log`,
/// with `registry` as its emitting address, `parent` as the parent and `label` as the label.
pub async fn parent(
    fixture: &Fixture,
    block: i64,
    log: i64,
    registry: &str,
    parent: &str,
    label: &str,
) -> Result<()> {
    let identity = format!("ParentChanged:{label}:{block}:{log}");
    fixture
        .event(
            Event::new(&identity, block, log, "ParentChanged", "ens_v2_registry_l1")
                .after(json!({"source_event": "ParentUpdated", "parent": parent,
                              "raw_label_hex": alloy_primitives::hex::encode(label),
                              "sender": OWNER}))
                .raw(json!({"emitting_address": registry})),
        )
        .await?;
    Ok(())
}

/// Write a `RegistryCreated` event of source family `ens_v2_registry_l1` at `block`, log
/// `log`, that carries `registry`. Project selects every name it finds mounted below that
/// address in that block.
pub async fn created(fixture: &Fixture, block: i64, log: i64, registry: &str) -> Result<()> {
    let identity = format!("RegistryCreated:{registry}:{block}:{log}");
    fixture
        .event(
            Event::new(
                &identity,
                block,
                log,
                "RegistryCreated",
                "ens_v2_registry_l1",
            )
            .after(json!({"registry": registry})),
        )
        .await?;
    Ok(())
}

/// Write a `SubregistryChanged` event of source family `ens_v1_registry_l1` at `block`, log
/// `log`: the child edge of `child` under `parent`, with `OWNER` as the owner.
pub async fn edge(
    fixture: &Fixture,
    block: i64,
    log: i64,
    parent: &str,
    child: &str,
) -> Result<()> {
    let label = child.split('.').next().unwrap_or_default();
    let identity = format!("edge:{child}:{block}:{log}");
    fixture
        .event(
            Event::new(
                &identity,
                block,
                log,
                "SubregistryChanged",
                "ens_v1_registry_l1",
            )
            .after(json!({"source_event": "NewOwner",
                          "node": format!("{:#x}", namehash(parent)),
                          "child_node": format!("{:#x}", namehash(child)),
                          "labelhash": format!("{:#x}", keccak256(label)),
                          "owner": OWNER}))
            .raw(json!({"emitting_address": V1_REGISTRY})),
        )
        .await?;
    Ok(())
}

/// A block that carries family work and touches no name: an event no reducer keys.
pub async fn filler(fixture: &Fixture, block: i64) -> Result<()> {
    let identity = format!("path-filler:{block}");
    fixture
        .event(Event::new(
            &identity,
            block,
            90,
            "PreimageObserved",
            "ens_v1_registry_l1",
        ))
        .await?;
    Ok(())
}

/// The registry address the `bob` entry's `SubregistryChanged` event carries in `history`.
pub const USER_REGISTRY: &str = "0x00000000000000000000000000000000000000e9";

/// The logical name ids of the names in `history`.
pub struct Names {
    pub bob: String,
    pub sub: String,
    pub deep: String,
    pub carol: String,
    pub kid: String,
}

/// Block time `block` of the fixture lineage, in seconds.
pub fn block_time(block: i64) -> u64 {
    1_800_000_000 + 12 * block.unsigned_abs()
}

/// A history over blocks 0 to 12 in which ENSv2 entry and pointer events move names that no
/// event of their block names. Each line lists the synthetic events the helpers write:
///
/// - 0: the root registry is admitted, with the events of its `eth` entry.
/// - 1 to 4: `leased` writes `bob.eth` and `carol.eth`. `subname` writes `sub.bob.eth`,
///   `deep.sub.bob.eth` and `kid.carol.eth`. `edge` writes the child edge of `sub.bob.eth`
///   at block 3.
/// - 5: `register` writes `bob` for the `.eth` registry address, far into the future.
/// - 7: `point` writes `bob`'s resolver. `point_named` writes its subregistry,
///   `USER_REGISTRY`, in an event that names `bob.eth`. `parent` writes `USER_REGISTRY`'s
///   parent as `bob` in the `.eth` registry address.
/// - 8: `register` writes `sub` for `USER_REGISTRY`, and `carol` for the `.eth` registry
///   address until block 10's time. `point` writes `carol`'s resolver.
/// - 9: `created` writes `USER_REGISTRY` as a created registry.
/// - 10: `resolver` writes `deep.sub.bob.eth`'s resolver again, and the expiry written for
///   `carol`'s entry passes by the clock.
///
/// The expiry at block 10 is load-bearing. No event of block 10 names `carol.eth` or
/// `kid.carol.eth`, so their rows move only if the publication selects the names with a due
/// summary clock before it advances those clocks. The digest in `families_recorded_rows.rs`
/// is the guard on that order. Keep an expiry by the clock in this history.
///
/// Blocks 6, 11 and 12 carry family work that names nothing.
pub async fn history(fixture: &Fixture) -> Result<Names> {
    const FAR: u64 = 1_900_000_000;
    let eth = admit(fixture).await?;
    let names = Names {
        bob: leased(fixture, "bob.eth", 2, 1, FAR).await?,
        sub: subname(fixture, "sub.bob.eth", 1, 1).await?,
        deep: subname(fixture, "deep.sub.bob.eth", 2, 2).await?,
        carol: leased(fixture, "carol.eth", 3, 2, FAR).await?,
        kid: subname(fixture, "kid.carol.eth", 3, 2).await?,
    };
    edge(fixture, 3, 200, "bob.eth", "sub.bob.eth").await?;
    register(fixture, 5, 0, &eth, "bob", FAR).await?;
    point(fixture, 7, 0, &eth, "bob", Pointer::Resolver(RESOLVER)).await?;
    point_named(
        fixture,
        7,
        1,
        &eth,
        "bob",
        Pointer::Subregistry(USER_REGISTRY),
        &names.bob,
    )
    .await?;
    parent(fixture, 7, 2, USER_REGISTRY, &eth, "bob").await?;
    register(fixture, 8, 0, USER_REGISTRY, "sub", FAR).await?;
    register(fixture, 8, 2, &eth, "carol", block_time(10)).await?;
    point(fixture, 8, 4, &eth, "carol", Pointer::Resolver(RESOLVER)).await?;
    created(fixture, 9, 0, USER_REGISTRY).await?;
    resolver(fixture, &names.deep, 10, 0).await?;
    for block in [6, 11, 12] {
        filler(fixture, block).await?;
    }
    Ok(names)
}
