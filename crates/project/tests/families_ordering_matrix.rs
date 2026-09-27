//! The same-log ordering matrix: one case per order-sensitive family. Each case writes two facts
//! of one key at one (block, transaction, log) whose emission ordinal
//! (docs/glossary.md#emission-ordinal) and identity bytes disagree, inserted in the reverse of the
//! ordinal order so the generated ids disagree too. The canonical event order
//! (docs/glossary.md#canonical-event-order) decides by the ordinal, so the winning row, and any
//! position it stores inside a JSON column, is the higher-ordinal fact. Under the old rule
//! (identity bytes straight after the log index) the other fact would win: every pair checks that
//! its later fact has the byte-smaller identity. Identities follow the adapter's raw-log shape
//! `{derivation}:{manifest}:{chain}:{block hash}:{tx hash}:{log}:{suffix}:{ordinal}`
//! (adapters schema_v2/normalized.rs:118-131) with the derivation kind of common.rs:266-294.
//! Unless a case says otherwise the pair is constructed: two manifests (7 writes ordinal 2, 9
//! writes ordinal 1), or ordinals 9 and 10 of one batch. Every case then undoes its block and
//! equals a rebuild. This file holds F1 to F5; families_ordering_matrix_b.rs holds F6 to F13.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{CHAIN, Event, Fixture, hash, uuid};
use serde_json::{Value, json};

const BLOCK: i64 = 12;
const ALICE: &str = "0x00000000000000000000000000000000000000a1";
const BOB: &str = "0x00000000000000000000000000000000000000b2";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000e2";
const V2_REGISTRY: &str = "0x00000000000000000000000000000000000000e4";
const RESOLVER_1: &str = "0x00000000000000000000000000000000000000d1";
const RESOLVER_2: &str = "0x00000000000000000000000000000000000000d2";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

fn identity(derivation: &str, manifest: i64, log: i64, suffix: &str, ordinal: u32) -> String {
    format!(
        "{derivation}:{manifest}:{CHAIN}:{}:0xtx{BLOCK}_0:{log}:{suffix}:{ordinal}",
        hash(BLOCK)
    )
}

/// A stored position of block 12, transaction 0, as `Position::to_json` writes it.
fn position(log: i64, identity: &str) -> Value {
    json!({"block_number": BLOCK, "transaction_index": 0, "log_index": log,
           "event_identity": identity})
}

/// Two facts of one key at one log: `later` has the higher ordinal, `earlier` the lower one and
/// the byte-greater identity.
struct Pair {
    log: i64,
    later: String,
    earlier: String,
}

impl Pair {
    /// Two sources at one log: manifest 7 writes ordinal 2, manifest 9 writes ordinal 1.
    fn crossed(derivation: &str, log: i64, later: &str, earlier: &str) -> Self {
        Self::checked(Self {
            log,
            later: identity(derivation, 7, log, later, 2),
            earlier: identity(derivation, 9, log, earlier, 1),
        })
    }

    fn checked(pair: Self) -> Self {
        assert!(
            pair.later.as_bytes() < pair.earlier.as_bytes(),
            "the old identity-byte rule would pick the other fact"
        );
        pair
    }

    fn later_position(&self) -> Value {
        position(self.log, &self.later)
    }

    fn earlier_position(&self) -> Value {
        position(self.log, &self.earlier)
    }
}

/// One side of a pair: kind, source family, name, resource, payload and emitter.
struct Fact<'a> {
    kind: &'a str,
    family: &'a str,
    name: Option<&'a str>,
    resource: Option<&'a str>,
    after: Value,
    emitter: &'a str,
}

fn fact<'a>(kind: &'a str, family: &'a str, after: Value, emitter: &'a str) -> Fact<'a> {
    Fact {
        kind,
        family,
        name: None,
        resource: None,
        after,
        emitter,
    }
}

impl<'a> Fact<'a> {
    fn name(mut self, name: &'a str) -> Self {
        self.name = Some(name);
        self
    }

    fn resource(mut self, resource: &'a str) -> Self {
        self.resource = Some(resource);
        self
    }
}

async fn write(fixture: &Fixture, identity: &str, log: i64, fact: Fact<'_>) -> Result<i64> {
    if let Some(name) = fact.name {
        let namehash = name.split_once(':').map_or(name, |(_, hash)| hash);
        fixture.surface(name, namehash).await?;
    }
    if let Some(resource) = fact.resource {
        fixture.resource(resource).await?;
    }
    let mut event = Event::new(identity, BLOCK, log, fact.kind, fact.family)
        .after(fact.after)
        .raw(json!({"emitting_address": fact.emitter}));
    event.name = fact.name;
    event.resource = fact.resource;
    fixture.event(event).await
}

/// Write the pair, the higher ordinal first so its generated id is the lower one. Returns the
/// ids of the later and the earlier fact.
async fn pair(
    fixture: &Fixture,
    pair: &Pair,
    later: Fact<'_>,
    earlier: Fact<'_>,
) -> Result<(i64, i64)> {
    let later_id = write(fixture, &pair.later, pair.log, later).await?;
    let earlier_id = write(fixture, &pair.earlier, pair.log, earlier).await?;
    assert!(later_id < earlier_id, "the ids disagree with the ordinal");
    Ok((later_id, earlier_id))
}

fn pick(row: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).to_owned(), row[*name].clone()))
            .collect(),
    )
}

/// The table's single row.
async fn only(fixture: &Fixture, table: &str) -> Result<Value> {
    let rows = fixture.rows(table).await?;
    anyhow::ensure!(rows.len() == 1, "{table}: {rows:?}");
    Ok(rows[0].clone())
}

async fn settle(fixture: Fixture) -> Result<()> {
    fixture.assert_undo_restores(BLOCK).await?;
    fixture.assert_rebuild_equal(BLOCK).await?;
    fixture.cleanup().await
}

/// A surface binding row of block 12 opened at log 5, active from `from` to `to` microseconds
/// past the block time. Two bindings of one name and arm cannot share a start, so the second
/// starts a microsecond later while its provenance still names log 5.
async fn binding(
    fixture: &Fixture,
    id: &str,
    name: &str,
    resource: &str,
    from: i64,
    to: Option<i64>,
) -> Result<()> {
    let namehash = name.split_once(':').map_or(name, |(_, hash)| hash);
    fixture.surface(name, namehash).await?;
    fixture.resource(resource).await?;
    sqlx::query(
        "INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
             binding_kind, authority_arm, active_from, active_to, chain_id, block_hash,
             block_number, provenance, canonicality_state)
         VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v1',
                 to_timestamp(1800000000 + $6 * 12) + make_interval(secs => $7::float8 / 1e6),
                 to_timestamp(1800000000 + $6 * 12) + make_interval(secs => $8::float8 / 1e6),
                 $4, $5, $6, '{\"transaction_index\": 0, \"log_index\": 5}', 'canonical')",
    )
    .bind(id)
    .bind(name)
    .bind(resource)
    .bind(CHAIN)
    .bind(hash(BLOCK))
    .bind(BLOCK)
    .bind(from)
    .bind(to)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

// F1: two AuthorityEpochChanged of the ENSv1 arm at one log. The arm's start is the
// higher-ordinal epoch, with its kind, owner and resource.
#[tokio::test]
async fn f1_the_arm_start_is_the_higher_ordinal_epoch() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f1_epoch", 20).await?;
    let (lease, registry_only) = (uuid(1), uuid(2));
    let facts = Pair::crossed(
        "ens_v1_unwrapped_authority",
        5,
        "AuthorityEpochChanged:registry_only",
        "AuthorityEpochChanged:registrar",
    );
    let epoch = |kind: &str, owner: &str| json!({"authority_kind": kind, "owner": owner});
    let named = name(1);
    pair(
        &fixture,
        &facts,
        fact(
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            epoch("registry_only", BOB),
            REGISTRY,
        )
        .name(&named)
        .resource(&registry_only),
        fact(
            "AuthorityEpochChanged",
            "ens_v1_registrar_l1",
            epoch("registrar", ALICE),
            REGISTRAR,
        )
        .name(&named)
        .resource(&lease),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_name_state").await?;
    let mut start = facts.later_position();
    start["authority_kind"] = json!("registry_only");
    start["authority_key"] = Value::Null;
    start["owner"] = json!(BOB);
    start["resource_id"] = json!(registry_only);
    assert_eq!(row["authority_start_positions"], json!({"ens_v1": start}));
    assert_eq!(row["event_identity"], json!(facts.later));
    settle(fixture).await
}

// F1: a registry-only binding's predecessor is the latest earlier candidate of its name and arm.
// Two bindings opened at one log (a constructed shape: the store's no-overlap rule keeps two
// bindings of one name and arm from starting at one instant) order by their SurfaceBound
// ordinals, so the lower-ordinal binding is the predecessor; by identity bytes it would follow
// the registry-only binding and there would be none.
#[tokio::test]
async fn f1_the_predecessor_is_the_lower_ordinal_binding() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f1_predecessor", 20).await?;
    let (lease, registry_only) = (uuid(1), uuid(2));
    let (first, second) = (uuid(101), uuid(102));
    let named = name(2);
    binding(&fixture, &first, &named, &lease, 5, Some(6)).await?;
    binding(&fixture, &second, &named, &registry_only, 6, None).await?;
    let bound = Pair::crossed(
        "ens_v1_unwrapped_authority",
        5,
        "SurfaceBound:registry_only",
        "SurfaceBound:registrar",
    );
    let surface = |kind: &str| json!({"authority_kind": kind, "state_derived": false});
    pair(
        &fixture,
        &bound,
        fact(
            "SurfaceBound",
            "ens_v1_registry_l1",
            surface("registry_only"),
            REGISTRY,
        )
        .name(&named)
        .resource(&registry_only),
        fact(
            "SurfaceBound",
            "ens_v1_registrar_l1",
            surface("registrar"),
            REGISTRAR,
        )
        .name(&named)
        .resource(&lease),
    )
    .await?;
    let epoch = identity(
        "ens_v1_unwrapped_authority",
        7,
        6,
        "AuthorityEpochChanged:registry_only",
        0,
    );
    write(
        &fixture,
        &epoch,
        6,
        fact(
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            json!({"authority_kind": "registry_only"}),
            REGISTRY,
        )
        .name(&named)
        .resource(&registry_only),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let rows = fixture.rows("project_binding_candidate").await?;
    let columns = [
        "surface_binding_id",
        "event_identity",
        "registry_only",
        "predecessor_resource_id",
        "predecessor_position",
        "lease_resource_id",
        "lease_position",
    ];
    let mut rows: Vec<Value> = rows.iter().map(|row| pick(row, &columns)).collect();
    rows.sort_by_key(|row| row["surface_binding_id"].as_str().map(str::to_owned));
    assert_eq!(
        rows,
        vec![
            json!({"surface_binding_id": first, "event_identity": bound.earlier,
                   "registry_only": false, "predecessor_resource_id": null,
                   "predecessor_position": null, "lease_resource_id": null,
                   "lease_position": null}),
            json!({"surface_binding_id": second, "event_identity": bound.later,
                   "registry_only": true, "predecessor_resource_id": lease,
                   "predecessor_position": bound.earlier_position(),
                   "lease_resource_id": lease, "lease_position": bound.earlier_position()}),
        ]
    );
    settle(fixture).await
}

// F1: a registrar grant replaces a registry-only handoff's lease only when it follows the
// binding. The binding's SurfaceBound (ordinal 1) and the grant (ordinal 2) share a log, so the
// grant follows and becomes the lease; by identity bytes it would precede the binding and the
// predecessor's lease would stay. Constructed: the registry's binding and the registrar's grant
// come from different logs in the adapters.
#[tokio::test]
async fn f1_a_higher_ordinal_grant_at_the_binding_log_takes_the_lease() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f1_lease", 20).await?;
    let (predecessor, registry_only, successor) = (uuid(1), uuid(2), uuid(3));
    let named = name(3);
    fixture.surface(&named, &node(3)).await?;
    fixture.resource(&predecessor).await?;
    // The predecessor's binding from block 10 to the registry-only binding at block 12, log 5.
    sqlx::query(
        "INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
             binding_kind, authority_arm, active_from, active_to, chain_id, block_hash,
             block_number, provenance, canonicality_state)
         VALUES ($1::uuid, $2, $3::uuid, 'declared_registry_path', 'ens_v1',
                 to_timestamp(1800000000 + 120), to_timestamp(1800000000 + 144.000005),
                 $4, $5, 10, '{\"transaction_index\": 0, \"log_index\": 1}', 'canonical')",
    )
    .bind(uuid(101))
    .bind(&named)
    .bind(&predecessor)
    .bind(CHAIN)
    .bind(hash(10))
    .execute(&fixture.pool)
    .await?;
    fixture
        .event(
            Event::new("bound:10", 10, 1, "SurfaceBound", "ens_v1_registrar_l1")
                .name(&named)
                .resource(&predecessor)
                .after(json!({"authority_kind": "registrar", "state_derived": false})),
        )
        .await?;
    fixture
        .event(
            Event::new(
                "released:11",
                11,
                1,
                "RegistrationReleased",
                "ens_v1_registrar_l1",
            )
            .name(&named)
            .resource(&predecessor)
            .after(json!({"namehash": node(3)})),
        )
        .await?;
    binding(&fixture, &uuid(102), &named, &registry_only, 5, None).await?;
    let facts = Pair::crossed(
        "ens_v1_unwrapped_authority",
        5,
        "RegistrationGranted:successor",
        "SurfaceBound:registry_only",
    );
    pair(
        &fixture,
        &facts,
        fact(
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            json!({"namehash": node(3), "registrant": BOB, "authority_kind": "registrar"}),
            REGISTRAR,
        )
        .name(&named)
        .resource(&successor),
        fact(
            "SurfaceBound",
            "ens_v1_registry_l1",
            json!({"authority_kind": "registry_only", "state_derived": false}),
            REGISTRY,
        )
        .name(&named)
        .resource(&registry_only),
    )
    .await?;
    let epoch = identity(
        "ens_v1_unwrapped_authority",
        7,
        6,
        "AuthorityEpochChanged",
        0,
    );
    write(
        &fixture,
        &epoch,
        6,
        fact(
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            json!({"authority_kind": "registry_only"}),
            REGISTRY,
        )
        .name(&named)
        .resource(&registry_only),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let rows = fixture.rows("project_binding_candidate").await?;
    let handoff = rows
        .iter()
        .find(|row| row["surface_binding_id"] == json!(uuid(102)))
        .map(|row| {
            pick(
                row,
                &[
                    "event_identity",
                    "predecessor_resource_id",
                    "lease_resource_id",
                    "lease_position",
                ],
            )
        });
    assert_eq!(
        handoff,
        Some(
            json!({"event_identity": facts.earlier, "predecessor_resource_id": predecessor,
                    "lease_resource_id": successor, "lease_position": facts.later_position()})
        )
    );
    settle(fixture).await
}

// F2a: a reservation (ordinal 1) and a grant (ordinal 2) of one ENSv2 resource at one log. The
// grant is the resource's last active event, its triple's association and its registry's child
// row.
#[tokio::test]
async fn f2a_lifecycle_maxima_and_association_take_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f2a", 20).await?;
    let resource = uuid(1);
    let named = name(4);
    let facts = Pair::crossed(
        "ens_v2_registry_resource_surface",
        5,
        "RegistrationGranted:77",
        "RegistrationReserved:77",
    );
    let state = |registrant: &str, status: &str| {
        json!({"registry_contract_instance_id": "v2-registry", "token_id": "77",
               "registrant": registrant, "expiry": 2_000_000_000, "status": status})
    };
    pair(
        &fixture,
        &facts,
        fact(
            "RegistrationGranted",
            "ens_v2_registry_l1",
            state(BOB, "registered"),
            V2_REGISTRY,
        )
        .name(&named)
        .resource(&resource),
        fact(
            "RegistrationReserved",
            "ens_v2_registry_l1",
            state(ALICE, "reserved"),
            V2_REGISTRY,
        )
        .name(&named)
        .resource(&resource),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let key = only(&fixture, "project_lifecycle_key_state").await?;
    assert_eq!(
        (
            &key["event_identity"],
            &key["last_active"],
            &key["last_grant"]["position"],
            &key["last_grant"]["registrant"],
            &key["last_reservation"]["position"],
        ),
        (
            &json!(facts.later),
            &json!({"kind": "RegistrationGranted", "position": facts.later_position()}),
            &facts.later_position(),
            &json!(BOB),
            &facts.earlier_position(),
        )
    );
    let association = only(&fixture, "project_lifecycle_association").await?;
    assert_eq!(
        pick(
            &association,
            &["event_kind", "target_resource_id", "event_identity"]
        ),
        json!({"event_kind": "RegistrationGranted", "target_resource_id": resource,
               "event_identity": facts.later})
    );
    let child = only(&fixture, "project_child_registration_state").await?;
    assert_eq!(
        pick(
            &child,
            &["event_kind", "registrant", "exists", "event_identity"]
        ),
        json!({"event_kind": "RegistrationGranted", "registrant": BOB, "exists": true,
               "event_identity": facts.later})
    );
    settle(fixture).await
}

// F2c: two owner changes of one registry node at one log. The node keeps the higher-ordinal
// owner and its position; both stay in the owner history.
#[tokio::test]
async fn f2c_the_registry_owner_is_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f2c", 20).await?;
    let facts = Pair::crossed(
        "ens_v1_unwrapped_authority",
        5,
        "AuthorityTransferred:bob",
        "AuthorityTransferred:alice",
    );
    let owner = |owner: &str| json!({"node": node(5), "owner": owner, "emitter_role": "registry"});
    pair(
        &fixture,
        &facts,
        fact(
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            owner(BOB),
            REGISTRY,
        ),
        fact(
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            owner(ALICE),
            REGISTRY,
        ),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_registry_node_state").await?;
    assert_eq!(
        pick(&row, &["owner", "owner_position", "event_identity"]),
        json!({"owner": BOB, "owner_position": facts.later_position(),
               "event_identity": facts.later})
    );
    assert_eq!(fixture.rows("project_registry_owner_event").await?.len(), 2);
    settle(fixture).await
}

// F4: two ResolverChanged of one ENSv1 registry node at one log. The pointer is the
// higher-ordinal resolver, though its generated id is the lower one. The realistic cross-source
// case, NameWrapped, is families_ordering.rs `name_wrapped_pointer_keeps_the_wrapper_row`.
#[tokio::test]
async fn f4_the_registry_pointer_is_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f4", 20).await?;
    let facts = Pair::crossed(
        "ens_v1_unwrapped_authority",
        5,
        "ResolverChanged:second",
        "ResolverChanged:first",
    );
    let pointer = |resolver: &str| json!({"node": node(6), "resolver": resolver});
    let (later_id, _) = pair(
        &fixture,
        &facts,
        fact(
            "ResolverChanged",
            "ens_v1_registry_l1",
            pointer(RESOLVER_2),
            REGISTRY,
        ),
        fact(
            "ResolverChanged",
            "ens_v1_registry_l1",
            pointer(RESOLVER_1),
            REGISTRY,
        ),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_registry_pointer").await?;
    assert_eq!(
        pick(
            &row,
            &[
                "resolver_address",
                "event_identity",
                "log_index",
                "normalized_event_id"
            ]
        ),
        json!({"resolver_address": RESOLVER_2, "event_identity": facts.later, "log_index": 5,
               "normalized_event_id": later_id})
    );
    settle(fixture).await
}

// F5: a resource's pointer set (ordinal 1) and cleared (ordinal 2) at one log. The pointer and
// the version boundary are the clear; the last non-zero resolver stays the set, at its own
// position.
#[tokio::test]
async fn f5_the_resource_pointer_groups_take_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f5", 20).await?;
    let resource = uuid(1);
    let named = name(7);
    let facts = Pair::crossed(
        "ens_v2_registry_resource_surface",
        5,
        "ResolverChanged:clear",
        "ResolverChanged:set",
    );
    pair(
        &fixture,
        &facts,
        fact(
            "ResolverChanged",
            "ens_v2_registry_l1",
            json!({"resolver": ZERO}),
            V2_REGISTRY,
        )
        .name(&named)
        .resource(&resource),
        fact(
            "ResolverChanged",
            "ens_v2_registry_l1",
            json!({"resolver": RESOLVER_1}),
            V2_REGISTRY,
        )
        .name(&named)
        .resource(&resource),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_resource_pointer").await?;
    assert_eq!(
        pick(
            &row,
            &[
                "resolver_address",
                "pointer_position",
                "nonzero_resolver_address",
                "nonzero_position",
                "boundary_kind",
                "boundary_position",
                "namehash",
                "event_identity",
            ]
        ),
        json!({"resolver_address": ZERO, "pointer_position": facts.later_position(),
               "nonzero_resolver_address": RESOLVER_1,
               "nonzero_position": facts.earlier_position(),
               "boundary_kind": "ResolverChanged", "boundary_position": facts.later_position(),
               "namehash": node(7), "event_identity": facts.later})
    );
    settle(fixture).await
}
