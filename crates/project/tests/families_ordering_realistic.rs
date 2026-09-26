//! Realistic same-log shapes where the emission ordinal (docs/glossary.md#emission-ordinal) and
//! the old identity-byte order disagree, one fixture per adapter shape found by the TYR-36 D12
//! survey (outputs/tyr36/d12-ordinal-vs-bytes-realistic-cases-2026-09-26.md). Each fixture writes
//! the facts one adapter log emits, in the adapter's write order, with the adapter's own identity
//! `{derivation}:{manifest}:{chain}:{block hash}:{tx hash}:{log}:{suffix}:{ordinal}`
//! (adapters schema_v2/normalized.rs:118-131; derivation kinds common.rs:266-294). Facts the
//! adapter writes at the same log that no asserted family reads are left out, so some ordinals
//! are skipped; each case says which. Every case asserts the field the survey names takes the
//! higher-ordinal fact, checks that the identity bytes would have picked the other fact, then
//! undoes its block and equals a rebuild. The wrapper TransferSingle holder and delegate cases
//! and the NameWrapped pointer pair are families_ordering.rs; the constructed per-family pairs
//! are families_ordering_matrix.rs and _b.rs. This file holds F2a, F2b, F2c and the F4 handoff;
//! families_ordering_realistic_b.rs holds F8, F12, F13 and the cross-batch cases.
//! The small helpers (identity, position, facts, write, settle) repeat those of the other
//! ordering test files instead of living in families_support; moving them is left for later.
mod families_support;

use anyhow::Result;
use bigname_project::families::{FamilyMode, emission_ordinal};
use families_support::{CHAIN, Event, Fixture, hash, uuid};
use serde_json::{Value, json};

const BLOCK: i64 = 12;
const LOG: i64 = 5;
const ALICE: &str = "0x00000000000000000000000000000000000000a1";
const BOB: &str = "0x00000000000000000000000000000000000000b2";
const OWNER: &str = "0x00000000000000000000000000000000000000c3";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const WRAPPER: &str = "0x00000000000000000000000000000000000000e3";
const V2_REGISTRY: &str = "0x00000000000000000000000000000000000000e4";
const OLD_RESOLVER: &str = "0x00000000000000000000000000000000000000d1";
const ZERO: &str = "0x0000000000000000000000000000000000000000";
const V1_REGISTRY_FAMILY: &str = "ens_v1_registry_l1";
const WRAPPER_FAMILY: &str = "ens_v1_wrapper_l1";
const V2_REGISTRY_FAMILY: &str = "ens_v2_registry_l1";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// The derivation kind the adapter gives these families' events (adapters common.rs:266-294).
fn derivation(family: &str, kind: &str) -> &'static str {
    match family {
        V2_REGISTRY_FAMILY if kind == "PermissionChanged" => "ens_v2_permissions",
        V2_REGISTRY_FAMILY => "ens_v2_registry_resource_surface",
        _ => "ens_v1_unwrapped_authority",
    }
}

/// The adapter a fact comes from: its block, log, manifest, source family and emitter.
#[derive(Clone, Copy)]
struct Source {
    block: i64,
    log: i64,
    manifest: i64,
    family: &'static str,
    emitter: &'static str,
}

fn source(family: &'static str, emitter: &'static str) -> Source {
    Source {
        block: BLOCK,
        log: LOG,
        manifest: 7,
        family,
        emitter,
    }
}

impl Source {
    fn at(self, block: i64, log: i64) -> Self {
        Self { block, log, ..self }
    }

    fn fact(self, kind: &'static str, suffix: &str, ordinal: u32) -> Fact {
        let identity = format!(
            "{}:{}:{CHAIN}:{}:0xtx{}_0:{}:{suffix}:{ordinal}",
            derivation(self.family, kind),
            self.manifest,
            hash(self.block),
            self.block,
            self.log
        );
        Fact {
            source: self,
            identity,
            kind,
            name: None,
            resource: None,
            after: json!({}),
            before: json!({}),
        }
    }
}

struct Fact {
    source: Source,
    identity: String,
    kind: &'static str,
    name: Option<String>,
    resource: Option<String>,
    after: Value,
    before: Value,
}

impl Fact {
    fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_owned());
        self
    }

    fn resource(mut self, resource: &str) -> Self {
        self.resource = Some(resource.to_owned());
        self
    }

    fn after(mut self, after: Value) -> Self {
        self.after = after;
        self
    }

    fn before(mut self, before: Value) -> Self {
        self.before = before;
        self
    }

    /// The position a family stores for this fact, as `Position::to_json` writes it.
    fn position(&self) -> Value {
        json!({"block_number": self.source.block, "transaction_index": 0,
               "log_index": self.source.log, "event_identity": self.identity})
    }
}

/// Write the facts in the order given, which is the adapter's write order.
async fn write(fixture: &Fixture, facts: &[&Fact]) -> Result<()> {
    for fact in facts {
        if let Some(name) = &fact.name {
            let namehash = name.split_once(':').map_or(name.as_str(), |(_, hash)| hash);
            fixture.surface(name, namehash).await?;
        }
        if let Some(resource) = &fact.resource {
            fixture.resource(resource).await?;
        }
        let source = fact.source;
        let mut event = Event::new(
            &fact.identity,
            source.block,
            source.log,
            fact.kind,
            source.family,
        )
        .after(fact.after.clone())
        .before(fact.before.clone())
        .raw(json!({"emitting_address": source.emitter}));
        event.name = fact.name.as_deref();
        event.resource = fact.resource.as_deref();
        fixture.event(event).await?;
    }
    Ok(())
}

/// Under the old rule (identity bytes straight after the log index) `earlier`, the lower-ordinal
/// fact, would have been the later one.
fn old_rule_flips(earlier: &Fact, later: &Fact) {
    assert!(
        earlier.identity.as_bytes() > later.identity.as_bytes(),
        "the identity bytes would pick {} over {}",
        earlier.identity,
        later.identity
    );
    let ordinal = |fact: &Fact| emission_ordinal(Some(0), Some(fact.source.log), &fact.identity);
    assert!(ordinal(earlier) < ordinal(later), "the ordinals disagree");
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

/// The table's row whose `column` is `value`.
async fn row_where(fixture: &Fixture, table: &str, column: &str, value: &str) -> Result<Value> {
    let rows = fixture.rows(table).await?;
    rows.into_iter()
        .find(|row| row[column] == json!(value))
        .ok_or_else(|| anyhow::anyhow!("{table}: no row with {column} = {value}"))
}

async fn settle(fixture: Fixture) -> Result<()> {
    fixture.assert_undo_restores(BLOCK).await?;
    fixture.assert_rebuild_equal(BLOCK).await?;
    fixture.cleanup().await
}

// F2a, topology rebind: a ParentUpdated moves an ENSv2 token from one registry path to another.
// append_v2_name_transitions (adapters protocol/v2_registry/topology.rs:14-214, called from
// v2_registry.rs:268) writes, after ParentChanged at ordinal 0, SurfaceUnbound:topology (1) and
// RegistrationReleased:topology (2, :66-86) under the previous name, then SurfaceBound:topology
// (3, :178-197), RegistrationGranted:topology (4, :363-392), AuthorityTransferred:topology (5)
// and ExpiryChanged:topology (6) under the current name, all on the token's one resource.
// ParentChanged carries no name or resource and is left out. `Granted` sorts before `Released`,
// so by identity bytes the release was the resource's last event and the row took the previous
// name; in the adapter's order the grant follows the release and the row keeps the current name.
#[tokio::test]
async fn f2a_topology_rebind_leaves_the_resource_under_the_current_name() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f2a_rebind", 20).await?;
    let resource = uuid(1);
    let instance = uuid(0x901);
    let (previous, current) = (name(1), name(2));
    let token = node(0x77);
    let v2 = source(V2_REGISTRY_FAMILY, V2_REGISTRY);
    let tail = format!("topology:{V2_REGISTRY}:{token}");
    let rebind = |event: &str| {
        json!({"source_event": "ParentUpdated", "topology_rebind": true,
               "registry": V2_REGISTRY, "token_id": token, "previous_namehash": node(1),
               "current_namehash": node(2), "event": event})
    };
    let pointer = |extra: Value| {
        let mut state = json!({"source_event": "ParentUpdated", "token_id": token,
                               "current_token_id": token, "upstream_resource": "0x1"});
        state
            .as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        state
    };
    let unbound = v2
        .fact("SurfaceUnbound", &format!("SurfaceUnbound:{tail}"), 1)
        .name(&previous)
        .resource(&resource)
        .after(rebind("unbound"));
    let released = v2
        .fact(
            "RegistrationReleased",
            &format!("RegistrationReleased:{tail}"),
            2,
        )
        .name(&previous)
        .resource(&resource)
        .before(json!({"status": "registered"}))
        .after(json!({"source_event": "ParentUpdated",
                      "terminal_reason": "registry_name_binding_changed",
                      "status": "released", "token_id": token,
                      "registry_contract_instance_id": instance}));
    let bound = v2
        .fact("SurfaceBound", &format!("SurfaceBound:{tail}"), 3)
        .name(&current)
        .resource(&resource)
        .after(rebind("bound"));
    let granted = v2
        .fact(
            "RegistrationGranted",
            &format!("RegistrationGranted:{tail}"),
            4,
        )
        .name(&current)
        .resource(&resource)
        .after(pointer(json!({
            "authority_kind": "ens_v2_registry", "registrant": ALICE,
            "expiry": 2_000_000_000, "labelhash": node(0x78), "status": "registered",
            "registry_contract_instance_id": instance,
        })));
    let transferred = v2
        .fact(
            "AuthorityTransferred",
            &format!("AuthorityTransferred:{tail}"),
            5,
        )
        .name(&current)
        .resource(&resource)
        .after(pointer(json!({"owner": ALICE})));
    let expiry = v2
        .fact("ExpiryChanged", &format!("ExpiryChanged:{tail}"), 6)
        .name(&current)
        .resource(&resource)
        .after(pointer(json!({"expiry": 2_000_000_000})));
    old_rule_flips(&released, &granted);
    old_rule_flips(&released, &expiry);
    write(
        &fixture,
        &[&unbound, &released, &bound, &granted, &transferred, &expiry],
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_lifecycle_key_state").await?;
    assert_eq!(
        pick(
            &row,
            &[
                "logical_name_id",
                "event_identity",
                "last_active",
                "last_release_any"
            ]
        ),
        json!({"logical_name_id": current, "event_identity": expiry.identity,
               "last_active": {"kind": "RegistrationGranted", "position": granted.position()},
               "last_release_any": {"position": released.position()}}),
        "the resource's last events are the current name's grant and expiry, after the release"
    );
    let association = only(&fixture, "project_lifecycle_association").await?;
    assert_eq!(
        pick(
            &association,
            &["logical_name_id", "target_resource_id", "event_identity"]
        ),
        json!({"logical_name_id": current, "target_resource_id": resource,
               "event_identity": granted.identity})
    );
    settle(fixture).await
}

// F2a, ExpiryUpdated revival of an expired ENSv2 token (adapters protocol/v2_registry.rs:98-155):
// ExpiryChanged (0), RegistrationRenewed:{token} (1, :141-149), then the revived name's
// transition (topology.rs:178-392): SurfaceBound:topology (2), RegistrationGranted:topology (3),
// AuthorityTransferred:topology (4), ExpiryChanged:topology (5). Bytes put `Granted` before
// `Renewed`, so the renewal was the last registration event; now the grant follows it. Both carry
// the same expiry, so the survey's low-impact claim holds for the key row, whose stored grant
// and renewal positions do not change; what changes is the key row's last event and the
// registry's child row, which keeps the latest Granted, Renewed or Released and takes its
// registrant from it: the grant names the registrant, the renewal does not.
#[tokio::test]
async fn f2a_expiry_revival_folds_the_grant_after_the_renewal() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f2a_revival", 20).await?;
    let resource = uuid(1);
    let instance = uuid(0x901);
    let named = name(3);
    let token = node(0x79);
    let v2 = source(V2_REGISTRY_FAMILY, V2_REGISTRY);
    let tail = format!("topology:{V2_REGISTRY}:{token}");
    let expiry_state = json!({"source_event": "ExpiryUpdated", "token_id": token,
                              "expiry": 2_100_000_000, "sender": ALICE,
                              "revived_from_expiry": true, "labelhash": node(0x7a),
                              "registry_contract_instance_id": instance});
    let topology = |extra: Value| {
        let mut state = json!({"source_event": "ExpiryUpdated", "token_id": token,
                               "current_token_id": token, "upstream_resource": "0x1"});
        state
            .as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        state
    };
    let changed = v2
        .fact("ExpiryChanged", "ExpiryChanged", 0)
        .name(&named)
        .resource(&resource)
        .before(json!({"expiry": 1_000}))
        .after(expiry_state.clone());
    let renewed = v2
        .fact(
            "RegistrationRenewed",
            &format!("RegistrationRenewed:{token}"),
            1,
        )
        .name(&named)
        .resource(&resource)
        .before(json!({"expiry": 1_000}))
        .after(expiry_state);
    let bound = v2
        .fact("SurfaceBound", &format!("SurfaceBound:{tail}"), 2)
        .name(&named)
        .resource(&resource)
        .after(topology(
            json!({"topology_rebind": true, "registry": V2_REGISTRY}),
        ));
    let granted = v2
        .fact(
            "RegistrationGranted",
            &format!("RegistrationGranted:{tail}"),
            3,
        )
        .name(&named)
        .resource(&resource)
        .after(topology(json!({
            "authority_kind": "ens_v2_registry", "registrant": ALICE,
            "expiry": 2_100_000_000, "labelhash": node(0x7a), "status": "registered",
            "registry_contract_instance_id": instance,
        })));
    let transferred = v2
        .fact(
            "AuthorityTransferred",
            &format!("AuthorityTransferred:{tail}"),
            4,
        )
        .name(&named)
        .resource(&resource)
        .after(topology(json!({"owner": ALICE})));
    let restated = v2
        .fact("ExpiryChanged", &format!("ExpiryChanged:{tail}"), 5)
        .name(&named)
        .resource(&resource)
        .after(topology(json!({"expiry": 2_100_000_000})));
    old_rule_flips(&renewed, &granted);
    old_rule_flips(&renewed, &restated);
    write(
        &fixture,
        &[
            &changed,
            &renewed,
            &bound,
            &granted,
            &transferred,
            &restated,
        ],
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_lifecycle_key_state").await?;
    assert_eq!(
        (
            &row["event_identity"],
            &row["last_grant"]["position"],
            &row["last_renewal"]["position"],
            &row["last_expiry_changed"]["position"],
        ),
        (
            &json!(restated.identity),
            &granted.position(),
            &renewed.position(),
            &restated.position(),
        )
    );
    let child = only(&fixture, "project_child_registration_state").await?;
    assert_eq!(
        pick(
            &child,
            &["event_kind", "registrant", "exists", "event_identity"]
        ),
        json!({"event_kind": "RegistrationGranted", "registrant": ALICE, "exists": true,
               "event_identity": granted.identity}),
        "the child row keeps the grant, which follows the renewal"
    );
    settle(fixture).await
}

/// The NameWrapped `after` object the wrapper writes (adapters protocol/v1/wrapper.rs:179-180).
fn wrapped_state(named: &str, holder: &str) -> Value {
    json!({"source_event": "NameWrapped", "node": named.trim_start_matches("ens:"),
           "owner": holder, "fuses": 0, "wrapper_state": "wrapped", "expiry": 2_000_000_000,
           "authority_kind": "wrapper", "authority_key": "wrapper:key", "surface_known": true})
}

/// A wrapper holder PermissionChanged of the resource scope (adapters protocol/permissions.rs:
/// 196-236, via wrapper/permissions.rs:81-123).
fn holder(named: &str, subject: &str, grant: bool, event: &str) -> Value {
    let source = json!({"kind": "ens_v1_authority", "authority_kind": "wrapper",
                        "authority_key": "wrapper:key", "authority_contract": WRAPPER,
                        "relation_kind": "holder", "node": named.trim_start_matches("ens:"),
                        "source_event_kind": event});
    json!({
        "subject": subject,
        "scope": {"kind": "resource"},
        "effective_powers": if grant { json!(["resource_control", "transfer"]) } else { json!([]) },
        "grant_source": if grant { source.clone() } else { Value::Null },
        "revocation_source": if grant { Value::Null } else { source },
        "inheritance_path": [],
        "transfer_behavior": "replace_on_authority_change",
    })
}

// F2b, NameWrapped (adapters protocol/v1/wrapper.rs:179-240): TokenControlTransferred (0),
// ExpiryChanged (1), PermissionScopeChanged (2), then the authority transition's
// SurfaceBound:NameWrapped:{W} (3) and AuthorityEpochChanged (4) (authority_transition.rs:
// 443-488), then the holder grant PermissionChanged:NameWrapped:holder:0:grant:{owner} (5,
// wrapper/permissions.rs:120-123). No previous surface, so no SurfaceUnbound; no resolver, so
// no ResolverChanged or resolver-scope grant. The lifecycle is the mint or the holder grant,
// whichever is later: by bytes `PermissionChanged` sorts before `TokenControlTransferred`, so
// the mint was; now the holder grant is. The name stays wrapped either way (label only).
#[tokio::test]
async fn f2b_name_wrapped_lifecycle_ends_on_the_holder_grant() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f2b_wrapped", 20).await?;
    let wrapper = uuid(1);
    let named = name(4);
    let log = source(WRAPPER_FAMILY, WRAPPER);
    let state = wrapped_state(&named, OWNER);
    let mut minted_state = state.clone();
    minted_state["to"] = json!(OWNER);
    let minted = log
        .fact("TokenControlTransferred", "TokenControlTransferred", 0)
        .name(&named)
        .resource(&wrapper)
        .after(minted_state);
    let expiry = log
        .fact("ExpiryChanged", "ExpiryChanged", 1)
        .name(&named)
        .resource(&wrapper)
        .after(state.clone());
    let scope = log
        .fact("PermissionScopeChanged", "PermissionScopeChanged", 2)
        .name(&named)
        .resource(&wrapper)
        .after(state.clone());
    let bound = log
        .fact(
            "SurfaceBound",
            &format!("SurfaceBound:NameWrapped:{wrapper}"),
            3,
        )
        .name(&named)
        .resource(&wrapper)
        .after(state.clone());
    let epoch = log
        .fact(
            "AuthorityEpochChanged",
            &format!("AuthorityEpochChanged:NameWrapped:{named}"),
            4,
        )
        .name(&named)
        .resource(&wrapper)
        .after(state);
    let granted = log
        .fact(
            "PermissionChanged",
            &format!("PermissionChanged:NameWrapped:holder:0:grant:{OWNER}"),
            5,
        )
        .name(&named)
        .resource(&wrapper)
        .after(holder(&named, OWNER, true, "NameWrapped"));
    old_rule_flips(&minted, &granted);
    write(
        &fixture,
        &[&minted, &expiry, &scope, &bound, &epoch, &granted],
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_wrapper_state").await?;
    assert_eq!(
        pick(
            &row,
            &[
                "lifecycle_source",
                "lifecycle_unwrapped",
                "lifecycle_position",
                "event_identity"
            ]
        ),
        json!({"lifecycle_source": "holder_grant", "lifecycle_unwrapped": false,
               "lifecycle_position": granted.position(), "event_identity": granted.identity})
    );
    settle(fixture).await
}

// F2b, NameUnwrapped of a wrapped subname with no registrar to reactivate (adapters
// protocol/v1/wrapper.rs:275-340): the authority transition writes
// SurfaceUnbound:NameUnwrapped:{W} (0) and AuthorityEpochChanged:NameUnwrapped (1), both on the
// wrapper resource (authority_transition.rs:421-488; no linked authority, so no SurfaceBound or
// ResolverChanged), then the holder revoke PermissionChanged:NameUnwrapped:holder:0:revoke:
// {owner} (2, wrapper.rs:340). No resolver, so no resolver-scope revoke. By bytes the
// SurfaceUnbound was the last lifecycle event; now the holder revoke is. The name is unwrapped
// either way (label only); the unwrap position, the latest epoch close, also moves from the
// SurfaceUnbound to the AuthorityEpochChanged, which the survey does not mention.
#[tokio::test]
async fn f2b_name_unwrapped_lifecycle_ends_on_the_holder_revoke() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f2b_unwrapped", 20).await?;
    let wrapper = uuid(1);
    let named = name(5);
    let wrap = source(WRAPPER_FAMILY, WRAPPER).at(10, 1);
    let mut minted_state = wrapped_state(&named, OWNER);
    minted_state["to"] = json!(OWNER);
    let minted = wrap
        .fact("TokenControlTransferred", "TokenControlTransferred", 0)
        .name(&named)
        .resource(&wrapper)
        .after(minted_state);
    let wrapped = wrap
        .fact(
            "PermissionChanged",
            &format!("PermissionChanged:NameWrapped:holder:0:grant:{OWNER}"),
            5,
        )
        .name(&named)
        .resource(&wrapper)
        .after(holder(&named, OWNER, true, "NameWrapped"));
    let log = source(WRAPPER_FAMILY, WRAPPER);
    let unwrap = |extra: Value| {
        let mut state = json!({"source_event": "NameUnwrapped",
                               "node": node(5), "owner": BOB, "unwrapped_at": 1_800_000_144});
        state
            .as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        state
    };
    let unbound = log
        .fact(
            "SurfaceUnbound",
            &format!("SurfaceUnbound:NameUnwrapped:{wrapper}"),
            0,
        )
        .name(&named)
        .resource(&wrapper)
        .after(unwrap(json!({"authority_kind": "wrapper",
                             "authority_key": "wrapper:key", "active_to": 1_800_000_144})));
    let epoch = log
        .fact(
            "AuthorityEpochChanged",
            &format!("AuthorityEpochChanged:NameUnwrapped:{named}"),
            1,
        )
        .name(&named)
        .resource(&wrapper)
        .after(unwrap(
            json!({"authority_kind": null, "authority_key": null}),
        ));
    let revoked = log
        .fact(
            "PermissionChanged",
            &format!("PermissionChanged:NameUnwrapped:holder:0:revoke:{OWNER}"),
            2,
        )
        .name(&named)
        .resource(&wrapper)
        .after(holder(&named, OWNER, false, "NameUnwrapped"));
    old_rule_flips(&unbound, &revoked);
    old_rule_flips(&unbound, &epoch);
    write(&fixture, &[&minted, &wrapped, &unbound, &epoch, &revoked]).await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_wrapper_state").await?;
    assert_eq!(
        pick(
            &row,
            &[
                "lifecycle_source",
                "lifecycle_unwrapped",
                "lifecycle_position",
                "unwrapped_position",
                "event_identity"
            ]
        ),
        json!({"lifecycle_source": "holder_revoke", "lifecycle_unwrapped": true,
               "lifecycle_position": revoked.position(),
               "unwrapped_position": epoch.position(), "event_identity": revoked.identity})
    );
    settle(fixture).await
}

/// The NewOwner body the registry writes (adapters protocol/v1/registry.rs:58, :108-156,
/// :315-331).
fn new_owner(parent: &str, child: &str, owner: &str, extra: Value) -> Value {
    let mut body = json!({"source_event": "NewOwner", "node": parent, "child_node": child,
                          "labelhash": node(0x99), "owner": owner, "emitter_role": "registry"});
    body.as_object_mut()
        .expect("an object")
        .extend(extra.as_object().expect("an object").clone());
    body
}

// F2c, NewOwner of a node the registry has not seen named (adapters protocol/v1/registry.rs:
// 49-62, :232, :332): SubregistryChanged (0) then AuthorityTransferred (1), both on the child
// node and the registry-only resource. No surface, so no authority transition follows. The
// owner is the same; by bytes `AuthorityTransferred` sorted first, so the node's owner group
// named the SubregistryChanged, and now it names the AuthorityTransferred.
#[tokio::test]
async fn f2c_new_owner_node_row_keeps_the_authority_transfer() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f2c_new_owner", 20).await?;
    let registry_only = uuid(1);
    let (parent, child) = (node(6), node(7));
    let log = source(V1_REGISTRY_FAMILY, REGISTRY);
    let body = new_owner(
        &parent,
        &child,
        ALICE,
        json!({"owner_getter": ALICE, "authority_kind": "registry_only",
               "authority_key": format!("registry-only:{CHAIN}:{child}")}),
    );
    let subregistry = log
        .fact("SubregistryChanged", "SubregistryChanged", 0)
        .resource(&registry_only)
        .after(body.clone());
    let transferred = log
        .fact("AuthorityTransferred", "AuthorityTransferred", 1)
        .resource(&registry_only)
        .before(json!({"owner": null, "owner_getter": null}))
        .after(body);
    old_rule_flips(&subregistry, &transferred);
    write(&fixture, &[&subregistry, &transferred]).await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_registry_node_state").await?;
    assert_eq!(
        pick(
            &row,
            &[
                "node",
                "owner",
                "owner_event_kind",
                "owner_position",
                "owner_resource_id",
                "event_identity"
            ]
        ),
        json!({"node": child, "owner": ALICE, "owner_event_kind": "AuthorityTransferred",
               "owner_position": transferred.position(), "owner_resource_id": registry_only,
               "event_identity": transferred.identity})
    );
    assert_eq!(fixture.rows("project_registry_owner_event").await?.len(), 2);
    settle(fixture).await
}

// F2c, NewOwner to the zero address over a registrar-held node (adapters protocol/v1/registry.rs:
// 266-277, :381-394): the SubregistryChanged (0) keeps the registrar authority's resource, and
// the AuthorityTransferred (1) is moved to the registry read anchor's resource. By bytes the
// node's owner group took the registrar resource; now it takes the anchor's.
#[tokio::test]
async fn f2c_new_owner_to_zero_keeps_the_anchor_resource() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f2c_zero_owner", 20).await?;
    let (registrar, anchor) = (uuid(1), uuid(2));
    let (parent, child) = (node(8), node(9));
    let named = format!("ens:{child}");
    let log = source(V1_REGISTRY_FAMILY, REGISTRY);
    let body = new_owner(
        &parent,
        &child,
        ZERO,
        json!({"owner_getter": ZERO, "owner_getter_reason": "literal_zero",
               "authority_kind": "registrar", "authority_key": "registrar:key"}),
    );
    let subregistry = log
        .fact("SubregistryChanged", "SubregistryChanged", 0)
        .name(&named)
        .resource(&registrar)
        .after(body.clone());
    let transferred = log
        .fact("AuthorityTransferred", "AuthorityTransferred", 1)
        .name(&named)
        .resource(&anchor)
        .before(json!({"owner": ALICE, "owner_getter": ALICE}))
        .after(body);
    old_rule_flips(&subregistry, &transferred);
    write(&fixture, &[&subregistry, &transferred]).await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_registry_node_state").await?;
    assert_eq!(
        pick(
            &row,
            &[
                "owner",
                "owner_getter_reason",
                "owner_event_kind",
                "owner_position",
                "owner_resource_id"
            ]
        ),
        json!({"owner": ZERO, "owner_getter_reason": "literal_zero",
               "owner_event_kind": "AuthorityTransferred",
               "owner_position": transferred.position(), "owner_resource_id": anchor})
    );
    settle(fixture).await
}

// F2c binding observation (project families/registry.rs:223-241): a registry Transfer that
// moves a registrar-held name to the registry-only authority (adapters protocol/v1/registry.rs:
// 64-77, :285-303, :403-415) writes AuthorityTransferred (0), then
// SurfaceUnbound:Transfer:{REG} (1) and SurfaceBound:Transfer:{RO} (2), and
// AuthorityEpochChanged:Transfer (3) (authority_transition.rs:421-488). No resolver, so no
// ResolverChanged; the permission rows that follow (registry.rs:514-527) are left out. The
// observation of the name keeps its latest producer: by bytes `SurfaceUnbound` sorted last, so
// it was the inapplicable unbind of the registrar resource; now it is the SurfaceBound of the
// registry-only resource with the registry owner.
#[tokio::test]
async fn f2c_registry_transfer_observation_is_the_surface_bound() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f2c_observation", 20).await?;
    let (registrar, registry_only) = (uuid(1), uuid(2));
    let named = name(10);
    let log = source(V1_REGISTRY_FAMILY, REGISTRY);
    let body = json!({"source_event": "Transfer", "node": node(10), "owner": BOB,
                      "emitter_role": "registry", "owner_getter": BOB,
                      "authority_kind": "registry_only",
                      "authority_key": format!("registry-only:{CHAIN}:{}", node(10))});
    let merged = |extra: Value| {
        let mut state = body.clone();
        state
            .as_object_mut()
            .expect("an object")
            .extend(extra.as_object().expect("an object").clone());
        state
    };
    let transferred = log
        .fact("AuthorityTransferred", "AuthorityTransferred", 0)
        .name(&named)
        .resource(&registry_only)
        .after(body.clone());
    let unbound = log
        .fact(
            "SurfaceUnbound",
            &format!("SurfaceUnbound:Transfer:{registrar}"),
            1,
        )
        .name(&named)
        .resource(&registrar)
        .after(merged(json!({"authority_kind": "registrar",
                             "authority_key": "registrar:key", "active_to": 1_800_000_144})));
    let bound = log
        .fact(
            "SurfaceBound",
            &format!("SurfaceBound:Transfer:{registry_only}"),
            2,
        )
        .name(&named)
        .resource(&registry_only)
        .after(merged(json!({"registry_contract": REGISTRY,
                             "active_from": 1_800_000_144,
                             "binding_kind": "declared_registry_path"})));
    let epoch = log
        .fact(
            "AuthorityEpochChanged",
            &format!("AuthorityEpochChanged:Transfer:{named}"),
            3,
        )
        .name(&named)
        .resource(&registry_only)
        .after(body.clone());
    old_rule_flips(&unbound, &bound);
    write(&fixture, &[&transferred, &unbound, &bound, &epoch]).await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = row_where(
        &fixture,
        "project_registry_binding_observation",
        "observation_identity",
        &named,
    )
    .await?;
    assert_eq!(
        pick(
            &row,
            &[
                "event_kind",
                "resource_id",
                "target_resource_id",
                "registry_owner",
                "registry_contract",
                "applicable",
                "event_identity"
            ]
        ),
        json!({"event_kind": "SurfaceBound", "resource_id": registry_only,
               "target_resource_id": registry_only, "registry_owner": BOB,
               "registry_contract": REGISTRY, "applicable": true,
               "event_identity": bound.identity})
    );
    settle(fixture).await
}

// F4, the registry fallback handoff (adapters protocol/v1/authority_transition.rs:74-126): the
// first current-registry ownership event of a node that had an old-registry resolver clears
// every retired resolver link at that node, one ResolverChanged with the zero resolver per link,
// suffixed ResolverChanged:registry-fallback-handoff:{node}:{resource}:{resolver}. The links come
// in `mark_v1_migrated` order (state_surfaces.rs:303-367): the sorted linked resources first,
// then any authority resource not among them, so the name's authority resource can follow a
// uuid-greater linked one. Here a registry Transfer writes AuthorityTransferred (0, left out)
// and the handoffs for the linked resource (1) and the appended authority resource (2). All
// clear to the zero resolver, so only the pointer's resource differs: by bytes it was the
// uuid-greater linked resource, now it is the last retired link.
#[tokio::test]
async fn f4_fallback_handoff_pointer_keeps_the_last_retired_link() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f4_handoff", 20).await?;
    let (linked, authority) = (uuid(0xf0), uuid(0x10));
    let named = name(11);
    let log = source(V1_REGISTRY_FAMILY, REGISTRY);
    let handoff = |resource: &str, ordinal: u32| {
        log.fact(
            "ResolverChanged",
            &format!(
                "ResolverChanged:registry-fallback-handoff:{}:{resource}:{OLD_RESOLVER}",
                node(11)
            ),
            ordinal,
        )
        .name(&named)
        .resource(resource)
        .before(json!({"resolver": OLD_RESOLVER}))
        .after(
            json!({"source_event": "Transfer", "node": node(11), "owner": BOB,
                      "emitter_role": "registry", "state_derived": true,
                      "registry_fallback_handoff": true, "resolver": ZERO,
                      "previous_resolver": OLD_RESOLVER,
                      "pointer_reason": "current_registry_record_suppresses_old_fallback"}),
        )
    };
    let (first, last) = (handoff(&linked, 1), handoff(&authority, 2));
    old_rule_flips(&first, &last);
    write(&fixture, &[&first, &last]).await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_registry_pointer").await?;
    assert_eq!(
        pick(
            &row,
            &[
                "resolver_address",
                "resource_id",
                "source_family",
                "event_identity"
            ]
        ),
        json!({"resolver_address": ZERO, "resource_id": authority,
               "source_family": V1_REGISTRY_FAMILY, "event_identity": last.identity})
    );
    settle(fixture).await
}
