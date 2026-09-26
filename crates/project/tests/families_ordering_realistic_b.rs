//! Realistic same-log shapes, second half: F8, F12, F13 and the cross-batch cases. The first
//! half (families_ordering_realistic.rs) explains the shape: the facts one adapter log emits, in
//! the adapter's write order and with the adapter's own identities, where the emission ordinal
//! (docs/glossary.md#emission-ordinal) and the old identity-byte order disagree. Facts no
//! asserted family reads are left out, so some ordinals are skipped. A cross-batch case writes a
//! primary batch and a sourced batch (adapters schema_v2/session.rs:489-497, primary first); the
//! ordinals count from 0 per batch (normalized.rs:118-131, sourced_events.rs:57-71), so between
//! batches the old order was decided by the manifest ids. Each such case picks manifest ids that
//! make the identity bytes favour the lower-ordinal fact, so only the ordinal picks the other.
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
const DELEGATE: &str = "0x00000000000000000000000000000000000000c4";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000e2";
const WRAPPER: &str = "0x00000000000000000000000000000000000000e3";
const CONTROLLER: &str = "0x00000000000000000000000000000000000000e5";
const REVERSE: &str = "0x00000000000000000000000000000000000000f1";
const RESOLVER: &str = "0x00000000000000000000000000000000000000d1";
const REGISTRY_FAMILY: &str = "ens_v1_registry_l1";
const REGISTRAR_FAMILY: &str = "ens_v1_registrar_l1";
const WRAPPER_FAMILY: &str = "ens_v1_wrapper_l1";
const REVERSE_FAMILY: &str = "ens_v1_reverse_l1";
/// Manifest ids for the cross-batch cases: the sourced (lower-ordinal) batch takes the greater
/// id, so its identity is byte-greater.
const PRIMARY_MANIFEST: i64 = 6131;
const SOURCED_MANIFEST: i64 = 6133;

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

/// The derivation kind the adapter gives these families' events (adapters common.rs:266-294).
fn derivation(family: &str) -> &'static str {
    match family {
        REVERSE_FAMILY => "ens_v1_reverse_claim",
        _ => "ens_v1_unwrapped_authority",
    }
}

/// The adapter batch a fact comes from: its block, log, manifest, source family and the raw
/// log's emitter.
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

    fn manifest(self, manifest: i64) -> Self {
        Self { manifest, ..self }
    }

    fn fact(self, kind: &'static str, suffix: &str, ordinal: u32) -> Fact {
        let identity = format!(
            "{}:{}:{CHAIN}:{}:0xtx{}_0:{}:{suffix}:{ordinal}",
            derivation(self.family),
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

async fn settle(fixture: Fixture) -> Result<()> {
    fixture.assert_undo_restores(BLOCK).await?;
    fixture.assert_rebuild_equal(BLOCK).await?;
    fixture.cleanup().await
}

/// An ENSv1 registrar or registry PermissionChanged of the resource scope: a grant carries
/// `resource_control` and a grant source, a revoke no powers and a revocation source (adapters
/// protocol/permissions.rs:120-178, via protocol/v1/registry.rs:532-572).
fn v1_permission(subject: &str, grant: bool, kind: &str, key: &str, event: &str) -> Value {
    let source = json!({"kind": "ens_v1_authority", "authority_kind": kind,
                        "authority_key": key, "source_event_kind": event});
    json!({
        "subject": subject,
        "scope": {"kind": "resource"},
        "effective_powers": if grant { json!(["resource_control"]) } else { json!([]) },
        "grant_source": if grant { source.clone() } else { Value::Null },
        "revocation_source": if grant { Value::Null } else { source },
        "inheritance_path": [],
        "transfer_behavior": "replace_on_authority_change",
    })
}

/// A NameWrapper PermissionChanged of the resource scope (adapters protocol/permissions.rs:
/// 196-236, via wrapper/permissions.rs:81-123).
fn wrapper_permission(named: &str, relation: &str, subject: &str, grant: bool) -> Value {
    let source = json!({"kind": "ens_v1_authority", "authority_kind": "wrapper",
                        "authority_key": "wrapper:key", "authority_contract": WRAPPER,
                        "relation_kind": relation, "node": named.trim_start_matches("ens:"),
                        "source_event_kind": "TransferSingle"});
    let powers = match (relation, grant) {
        ("holder", true) => json!(["resource_control", "transfer"]),
        ("token_approval", true) => json!(["extend_subname_expiry"]),
        _ => json!([]),
    };
    json!({
        "subject": subject,
        "scope": {"kind": "resource"},
        "effective_powers": powers,
        "grant_source": if grant { source.clone() } else { Value::Null },
        "revocation_source": if grant { Value::Null } else { source },
        "inheritance_path": [],
        "transfer_behavior": if relation == "token_approval" {
            "cleared_on_transfer_unless_cannot_approve"
        } else {
            "replace_on_authority_change"
        },
    })
}

/// A registrar Transfer's permission rows (adapters protocol/v1/registrar/
/// transfer_permissions.rs:21-55): the token's revoke and grant on the registrar resource
/// (ordinals 1 and 2, after TokenControlTransferred at 0), then, when the authority moves, the
/// authority rows on the other resource (3). The authority transition that follows
/// (registrar.rs:257-267) touches no asserted family and is left out.
struct RegistrarTransfer {
    transferred: Fact,
    revoked: Fact,
    granted: Fact,
    authority: Fact,
}

fn registrar_transfer(
    named: &str,
    registrar: &str,
    registry_only: &str,
    from: &str,
    to: &str,
    authority: (&str, &str, bool),
) -> RegistrarTransfer {
    let log = source(REGISTRAR_FAMILY, REGISTRAR);
    let (subject, action, grant) = authority;
    let registry_key = format!("registry-only:{CHAIN}:{}", named.trim_start_matches("ens:"));
    RegistrarTransfer {
        transferred: log
            .fact("TokenControlTransferred", "TokenControlTransferred", 0)
            .name(named)
            .resource(registrar)
            .after(
                json!({"source_event": "Transfer", "to": to, "token_id": node(0x31),
                          "namehash": named.trim_start_matches("ens:")}),
            ),
        revoked: log
            .fact(
                "PermissionChanged",
                &format!("PermissionChanged:transfer-resource-revoke:{from}"),
                1,
            )
            .name(named)
            .resource(registrar)
            .after(v1_permission(
                from,
                false,
                "registrar",
                "registrar:key",
                "TokenControlTransferred",
            )),
        granted: log
            .fact(
                "PermissionChanged",
                &format!("PermissionChanged:transfer-resource-grant:{to}"),
                2,
            )
            .name(named)
            .resource(registrar)
            .after(v1_permission(
                to,
                true,
                "registrar",
                "registrar:key",
                "TokenControlTransferred",
            )),
        authority: log
            .fact(
                "PermissionChanged",
                &format!("PermissionChanged:transfer-authority-resource-{action}:{subject}"),
                3,
            )
            .name(named)
            .resource(registry_only)
            .after(v1_permission(
                subject,
                grant,
                "registry_only",
                &registry_key,
                "TokenControlTransferred",
            )),
    }
}

impl RegistrarTransfer {
    fn facts(&self) -> [&Fact; 4] {
        [
            &self.transferred,
            &self.revoked,
            &self.granted,
            &self.authority,
        ]
    }
}

// F8, a registrar self-transfer (from = to) that moves the authority from the registrar
// resource to the registry-only one (adapters protocol/v1/registrar/transfer_permissions.rs:
// 15-32, suffix registry.rs:572): transfer-resource-revoke:F (1) then transfer-resource-grant:F
// (2) on the registrar resource share one grant key. `grant` sorts before `revoke`, so by bytes
// the holder's control was revoked; in the adapter's order it stands.
#[tokio::test]
async fn f8_registrar_self_transfer_keeps_the_grant() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f8_self_transfer", 20).await?;
    let (registrar, registry_only) = (uuid(1), uuid(2));
    let named = name(20);
    let transfer = registrar_transfer(
        &named,
        &registrar,
        &registry_only,
        ALICE,
        ALICE,
        (ALICE, "grant", true),
    );
    old_rule_flips(&transfer.revoked, &transfer.granted);
    write(&fixture, &transfer.facts()).await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let rows = fixture.rows("project_grant").await?;
    let row = rows
        .iter()
        .find(|row| row["resource_id"] == json!(registrar))
        .map(|row| {
            pick(
                row,
                &["subject", "effective_powers", "revoked", "event_identity"],
            )
        });
    assert_eq!(
        row,
        Some(
            json!({"subject": ALICE, "effective_powers": ["resource_control"],
                    "revoked": false, "event_identity": transfer.granted.identity})
        )
    );
    settle(fixture).await
}

// F12, NameForAddrChanged (adapters protocol/v1/reverse.rs:27-70): ReverseChanged (0) and the
// direct claim RecordChanged (1) of one (address, coin type, namespace) tuple. The reverse and
// claim columns are separate and keep their own positions either way; the row's own last-writer
// position was the ReverseChanged by bytes (`Rec` < `Rev`) and is now the RecordChanged.
#[tokio::test]
async fn f12_name_for_addr_changed_row_ends_on_the_claim() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f12_name_for_addr", 20).await?;
    let log = source(REVERSE_FAMILY, REVERSE);
    let label = ALICE.trim_start_matches("0x");
    let reverse_node = node(0x60);
    let provenance = json!({"source_family": REVERSE_FAMILY, "contract_role": "reverse_registrar",
                            "contract_instance_id": uuid(0x902), "emitting_address": REVERSE});
    let reverse = log.fact("ReverseChanged", "ReverseChanged", 0).after(
        json!({"source_event": "NameForAddrChanged", "address": ALICE,
                      "coin_type": "60", "namespace": "ens", "reverse_namespace": "ens",
                      "reverse_label": label, "reverse_name": format!("{label}.addr.reverse"),
                      "reverse_node": reverse_node, "claim_provenance": provenance}),
    );
    let claim = log.fact("RecordChanged", "RecordChanged", 1).after(json!({
        "source_event": "NameForAddrChanged", "address": ALICE, "reverse_node": reverse_node,
        "record_key": "name", "record_family": "name", "selector_key": null,
        "raw_name": "alice.eth",
        "primary_claim_source": {"address": ALICE, "namespace": "ens", "coin_type": "60",
                                 "reverse_name": format!("{label}.addr.reverse"),
                                 "reverse_node": reverse_node, "claim_provenance": provenance},
    }));
    old_rule_flips(&reverse, &claim);
    write(&fixture, &[&reverse, &claim]).await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_reverse_tuple").await?;
    assert_eq!(
        pick(
            &row,
            &[
                "reverse_node",
                "reverse_position",
                "raw_name",
                "claim_position",
                "event_identity"
            ]
        ),
        json!({"reverse_node": reverse_node, "reverse_position": reverse.position(),
               "raw_name": "alice.eth", "claim_position": claim.position(),
               "event_identity": claim.identity})
    );
    settle(fixture).await
}

/// The name's fold row: controller, action, subject and position.
async fn controller(fixture: &Fixture) -> Result<Value> {
    Ok(pick(
        &only(fixture, "project_address_name_fold").await?,
        &[
            "controller",
            "controller_action",
            "controller_subject",
            "controller_position",
        ],
    ))
}

// F13, a wrapper TransferSingle to the approved delegate (adapters protocol/v1/wrapper/
// transfer.rs:132-169): TransferSingle:{node} (0), the delegate's token-approval revoke (1),
// the old holder's revoke (2) and the delegate's holder grant (3). No resolver, so no resolver
// scope rows. families_ordering.rs covers this log's grant rows and wrapper lifecycle; this is
// the name's controller fold. By bytes the token-approval revoke of the new holder came last
// and cleared the controller; now the holder grant does, and the delegate controls the name.
#[tokio::test]
async fn f13_transfer_to_the_delegate_leaves_it_the_controller() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f13_delegate", 20).await?;
    let wrapper = uuid(1);
    let named = name(21);
    let nh = node(21);
    let wrapped = source(WRAPPER_FAMILY, WRAPPER)
        .at(10, 1)
        .fact(
            "PermissionChanged",
            &format!("PermissionChanged:NameWrapped:holder:0:grant:{OWNER}"),
            5,
        )
        .name(&named)
        .resource(&wrapper)
        .after(wrapper_permission(&named, "holder", OWNER, true));
    let log = source(WRAPPER_FAMILY, WRAPPER);
    let transferred = log
        .fact(
            "TokenControlTransferred",
            &format!("TransferSingle:{nh}"),
            0,
        )
        .name(&named)
        .resource(&wrapper)
        .after(
            json!({"source_event": "TransferSingle", "operator": OWNER, "to": DELEGATE,
                      "id": nh, "namehash": nh, "value": "1"}),
        );
    let cleared = log
        .fact(
            "PermissionChanged",
            &format!("PermissionChanged:TransferSingle:{nh}:token_approval:0:revoke:{DELEGATE}"),
            1,
        )
        .name(&named)
        .resource(&wrapper)
        .after(wrapper_permission(
            &named,
            "token_approval",
            DELEGATE,
            false,
        ));
    let revoked = log
        .fact(
            "PermissionChanged",
            &format!("PermissionChanged:TransferSingle:{nh}:holder:0:revoke:{OWNER}"),
            2,
        )
        .name(&named)
        .resource(&wrapper)
        .after(wrapper_permission(&named, "holder", OWNER, false));
    let granted = log
        .fact(
            "PermissionChanged",
            &format!("PermissionChanged:TransferSingle:{nh}:holder:0:grant:{DELEGATE}"),
            3,
        )
        .name(&named)
        .resource(&wrapper)
        .after(wrapper_permission(&named, "holder", DELEGATE, true));
    old_rule_flips(&cleared, &granted);
    write(
        &fixture,
        &[&wrapped, &transferred, &cleared, &revoked, &granted],
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    assert_eq!(
        controller(&fixture).await?,
        json!({"controller": DELEGATE, "controller_action": "set",
               "controller_subject": DELEGATE, "controller_position": granted.position()})
    );
    settle(fixture).await
}

/// The registrant's grant at registration, which makes `subject` the controller at block 10.
fn registered(named: &str, registrar: &str, subject: &str) -> Fact {
    source(REGISTRAR_FAMILY, REGISTRAR)
        .at(10, 1)
        .fact("PermissionChanged", "PermissionChanged", 2)
        .name(named)
        .resource(registrar)
        .after(v1_permission(
            subject,
            true,
            "registrar",
            "registrar:key",
            "RegistrationGranted",
        ))
}

// F13, a registrar Transfer without reclaim from F to T that moves the authority from the
// registrar resource to the registry-only one F still owns (transfer_permissions.rs:21-55):
// transfer-resource-revoke:F (1) and transfer-resource-grant:T (2) on the registrar resource,
// then transfer-authority-resource-grant:F (3) on the registry-only resource. The unmasked fold
// ends on the authority grant: F. By bytes `transfer-authority` sorted before `transfer-resource`,
// so the fold ended on T's grant.
#[tokio::test]
async fn f13_transfer_without_reclaim_leaves_the_registry_owner_the_controller() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f13_no_reclaim", 20).await?;
    let (registrar, registry_only) = (uuid(1), uuid(2));
    let named = name(22);
    let prior = registered(&named, &registrar, ALICE);
    let transfer = registrar_transfer(
        &named,
        &registrar,
        &registry_only,
        ALICE,
        BOB,
        (ALICE, "grant", true),
    );
    old_rule_flips(&transfer.granted, &transfer.authority);
    let mut facts = vec![&prior];
    facts.extend(transfer.facts());
    write(&fixture, &facts).await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    assert_eq!(
        controller(&fixture).await?,
        json!({"controller": ALICE, "controller_action": "set", "controller_subject": ALICE,
               "controller_position": transfer.authority.position()})
    );
    settle(fixture).await
}

// F13, a registrar Transfer back to the registry owner X, which moves the authority from the
// registry-only resource back to the registrar resource (transfer_permissions.rs:21-55):
// transfer-resource-revoke:F (1), transfer-resource-grant:X (2) on the registrar resource, then
// transfer-authority-resource-revoke:X (3) on the registry-only resource. The unmasked fold now
// ends on that revoke and clears the controller; by bytes it ended on X's grant. A read that
// admits only the registrar resource's candidates (address_names.rs:245-278) still folds to X:
// each candidate is its own row, kept with its resource either way.
#[tokio::test]
async fn f13_transfer_back_to_the_registry_owner_clears_the_unmasked_controller() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f13_back", 20).await?;
    let (registrar, registry_only) = (uuid(1), uuid(2));
    let named = name(23);
    let prior = registered(&named, &registrar, ALICE);
    let transfer = registrar_transfer(
        &named,
        &registrar,
        &registry_only,
        ALICE,
        OWNER,
        (OWNER, "revoke", false),
    );
    old_rule_flips(&transfer.granted, &transfer.authority);
    let mut facts = vec![&prior];
    facts.extend(transfer.facts());
    write(&fixture, &facts).await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    assert_eq!(
        controller(&fixture).await?,
        json!({"controller": null, "controller_action": "revoke", "controller_subject": OWNER,
               "controller_position": transfer.authority.position()})
    );
    let candidates = fixture.rows("project_address_controller_candidate").await?;
    let registrar_candidates: Vec<Value> = candidates
        .iter()
        .filter(|row| row["resource_id"] == json!(registrar) && row["block_number"] == BLOCK)
        .map(|row| pick(row, &["action", "subject", "event_identity"]))
        .collect();
    assert_eq!(
        registrar_candidates.len(),
        2,
        "the registrar resource keeps its own revoke and grant: {registrar_candidates:?}"
    );
    settle(fixture).await
}

/// A surface binding row of block 12 with its provenance log, active from `from` to `to`
/// microseconds past the block time (two bindings of one name and arm cannot share a start).
async fn binding(
    fixture: &Fixture,
    id: &str,
    name: &str,
    resource: &str,
    log: i64,
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
                 $4, $5, $6, jsonb_build_object('transaction_index', 0, 'log_index', $9),
                 'canonical')",
    )
    .bind(id)
    .bind(name)
    .bind(resource)
    .bind(CHAIN)
    .bind(hash(BLOCK))
    .bind(BLOCK)
    .bind(from)
    .bind(to)
    .bind(log)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

// The cross-batch shapes below are read from the adapter code. Only the NameWrapped pointer
// pair has been produced by an adapter run (mirror_resolver/name_wrapped_sources.rs), and in
// that run the materialization sourced no SurfaceBound. An adapter run of a registrar
// NameRegistered after a registry owner and a resolver selection produced the primary batch
// modelled here (ordinals 0 to 6) but no sourced materialization. So these cases pin the
// families' result for shapes that are not yet demonstrated (docs/projections.md lists the
// four and states the F13 controller value difference).
/// The registry-only surface materialization a readable trigger sources to the registry
/// manifest (adapters protocol/v1/authority_transition.rs:233-284): SurfaceBound (0) and, with
/// a resolver, ResolverChanged (1), both on the registry-only resource.
fn materialization(
    named: &str,
    registry_only: &str,
    trigger: &str,
    emitter: &'static str,
) -> (Fact, Fact) {
    let node = named.trim_start_matches("ens:");
    let log = source(REGISTRY_FAMILY, emitter).manifest(SOURCED_MANIFEST);
    let common = json!({"state_derived": true, "surface_materialization": true,
                        "source_event": trigger, "node": node,
                        "authority_kind": "registry_only",
                        "authority_key": format!("registry-only:{CHAIN}:{node}"),
                        "owner": OWNER, "owner_getter": OWNER, "registry_contract": REGISTRY,
                        "binding_kind": "declared_registry_path",
                        "pointer_reason": "surface_materialization_current_resolver"});
    let mut bound_state = common.clone();
    bound_state["active_from"] = json!(1_800_000_144);
    let mut resolver_state = common;
    resolver_state["resolver"] = json!(RESOLVER);
    resolver_state["resolver_source_role"] = json!("registry");
    (
        log.fact(
            "SurfaceBound",
            &format!("SurfaceBound:surface-materialization:{node}:{registry_only}"),
            0,
        )
        .name(named)
        .resource(registry_only)
        .after(bound_state),
        log.fact(
            "ResolverChanged",
            &format!("ResolverChanged:surface-materialization:{node}:{registry_only}:{RESOLVER}"),
            1,
        )
        .name(named)
        .resource(registry_only)
        .after(resolver_state),
    )
}

// F1 across batches, NameWrapped over a registry-only authority whose surface was unknown: the
// wrapper's primary batch opens the wrapper binding with SurfaceBound:NameWrapped:{W} (3,
// authority_transition.rs:443-466; TokenControlTransferred, ExpiryChanged and
// PermissionScopeChanged at 0 to 2 are left out) and AuthorityEpochChanged (4), and the surface
// materialization (adapters protocol/v1.rs:30-78) sources SurfaceBound:surface-materialization
// (0) to the registry manifest, opening the registry-only binding at the same log. The same
// block then unwraps the name (log 6) and a registry Transfer makes the registry-only authority
// current again (log 7, registry.rs:285-303), an epoch that marks the name's registry-only
// candidates. Each registry-only candidate's predecessor is the latest earlier candidate of the
// name: in the ordinal order the wrapper binding follows the materialized one, so the log-7
// binding hands off from the wrapper and the materialized one has none. By bytes (the sourced
// manifest id is the greater) the materialized binding followed the wrapper's instead.
#[tokio::test]
async fn cross_batch_f1_the_wrapper_binding_follows_the_materialized_one() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f1_cross_batch", 20).await?;
    let (wrapper, registry_only) = (uuid(1), uuid(2));
    let (materialized, wrapped, returned) = (uuid(101), uuid(102), uuid(103));
    let named = name(24);
    binding(
        &fixture,
        &materialized,
        &named,
        &registry_only,
        5,
        5,
        Some(6),
    )
    .await?;
    binding(&fixture, &wrapped, &named, &wrapper, 5, 6, Some(7)).await?;
    binding(&fixture, &returned, &named, &registry_only, 7, 7, None).await?;
    let primary = source(WRAPPER_FAMILY, WRAPPER).manifest(PRIMARY_MANIFEST);
    let wrap_state = json!({"source_event": "NameWrapped", "node": node(24), "owner": BOB,
                            "authority_kind": "wrapper", "authority_key": "wrapper:key",
                            "wrapped_registrar_resource_id": null,
                            "binding_kind": "declared_registry_path"});
    let bound = primary
        .fact(
            "SurfaceBound",
            &format!("SurfaceBound:NameWrapped:{wrapper}"),
            3,
        )
        .name(&named)
        .resource(&wrapper)
        .after(wrap_state.clone());
    let epoch = primary
        .fact(
            "AuthorityEpochChanged",
            &format!("AuthorityEpochChanged:NameWrapped:{named}"),
            4,
        )
        .name(&named)
        .resource(&wrapper)
        .after(wrap_state);
    let (sourced, _) = materialization(&named, &registry_only, "NameWrapped", WRAPPER);
    old_rule_flips(&sourced, &bound);
    let unwrap = primary.at(BLOCK, 6);
    let unwrap_state = json!({"source_event": "NameUnwrapped", "node": node(24), "owner": OWNER});
    let unbound = unwrap
        .fact(
            "SurfaceUnbound",
            &format!("SurfaceUnbound:NameUnwrapped:{wrapper}"),
            0,
        )
        .name(&named)
        .resource(&wrapper)
        .after(unwrap_state.clone());
    let closed = unwrap
        .fact(
            "AuthorityEpochChanged",
            &format!("AuthorityEpochChanged:NameUnwrapped:{named}"),
            1,
        )
        .name(&named)
        .resource(&wrapper)
        .after(unwrap_state);
    let registry = source(REGISTRY_FAMILY, REGISTRY)
        .manifest(SOURCED_MANIFEST)
        .at(BLOCK, 7);
    let transfer_state = json!({"source_event": "Transfer", "node": node(24), "owner": OWNER,
                                "owner_getter": OWNER, "emitter_role": "registry",
                                "authority_kind": "registry_only",
                                "authority_key": format!("registry-only:{CHAIN}:{}", node(24))});
    let returned_bound = registry
        .fact(
            "SurfaceBound",
            &format!("SurfaceBound:Transfer:{registry_only}"),
            1,
        )
        .name(&named)
        .resource(&registry_only)
        .after(transfer_state.clone());
    let returned_epoch = registry
        .fact(
            "AuthorityEpochChanged",
            &format!("AuthorityEpochChanged:Transfer:{named}"),
            2,
        )
        .name(&named)
        .resource(&registry_only)
        .after(transfer_state);
    write(
        &fixture,
        &[
            &bound,
            &epoch,
            &sourced,
            &unbound,
            &closed,
            &returned_bound,
            &returned_epoch,
        ],
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let mut rows: Vec<Value> = fixture
        .rows("project_binding_candidate")
        .await?
        .iter()
        .map(|row| {
            pick(
                row,
                &[
                    "surface_binding_id",
                    "event_identity",
                    "registry_only",
                    "predecessor_resource_id",
                    "predecessor_position",
                ],
            )
        })
        .collect();
    rows.sort_by_key(|row| row["surface_binding_id"].as_str().map(str::to_owned));
    assert_eq!(
        rows,
        vec![
            json!({"surface_binding_id": materialized, "event_identity": sourced.identity,
                   "registry_only": true, "predecessor_resource_id": null,
                   "predecessor_position": null}),
            json!({"surface_binding_id": wrapped, "event_identity": bound.identity,
                   "registry_only": false, "predecessor_resource_id": null,
                   "predecessor_position": null}),
            json!({"surface_binding_id": returned, "event_identity": returned_bound.identity,
                   "registry_only": true, "predecessor_resource_id": wrapper,
                   "predecessor_position": bound.position()}),
        ]
    );
    settle(fixture).await
}

/// A registrar NameRegistered over a registry-only authority whose surface was unknown
/// (adapters protocol/v1/registrar.rs:433-565): the primary batch writes RegistrationGranted
/// (0), ExpiryChanged (1), the registrant's resource grant PermissionChanged (2, :497-517), the
/// resolver grant (3, :518-541), then the authority transition's SurfaceBound (4),
/// AuthorityEpochChanged (5) and ResolverChanged:authority:NameRegistered:{resolver} (6,
/// authority_transition.rs:489-508), all on the registrar resource; the surface materialization
/// (registrar.rs:452-460) sources SurfaceBound (0) and ResolverChanged (1) on the registry-only
/// resource to the registry manifest. RegistrationGranted, ExpiryChanged, the resolver grant,
/// the SurfaceBound and the epoch touch no asserted family and are left out.
struct NameRegistered {
    grant: Fact,
    pointer: Fact,
    sourced_bound: Fact,
    sourced_pointer: Fact,
}

fn name_registered(named: &str, registrar: &str, registry_only: &str) -> NameRegistered {
    let primary = source(REGISTRAR_FAMILY, REGISTRAR).manifest(PRIMARY_MANIFEST);
    let node = named.trim_start_matches("ens:");
    let (sourced_bound, sourced_pointer) =
        materialization(named, registry_only, "NameRegistered", REGISTRAR);
    NameRegistered {
        grant: primary
            .fact("PermissionChanged", "PermissionChanged", 2)
            .name(named)
            .resource(registrar)
            .after(v1_permission(
                ALICE,
                true,
                "registrar",
                "registrar:key",
                "RegistrationGranted",
            )),
        pointer: primary
            .fact(
                "ResolverChanged",
                &format!("ResolverChanged:authority:NameRegistered:{RESOLVER}"),
                6,
            )
            .name(named)
            .resource(registrar)
            .after(
                json!({"source_event": "AuthorityEpochChanged", "namehash": node,
                          "registrant": ALICE, "authority_kind": "registrar",
                          "authority_key": "registrar:key", "resolver": RESOLVER,
                          "resolver_source_role": "registry"}),
            ),
        sourced_bound,
        sourced_pointer,
    }
}

impl NameRegistered {
    async fn write(&self, fixture: &Fixture) -> Result<()> {
        write(
            fixture,
            &[
                &self.grant,
                &self.pointer,
                &self.sourced_bound,
                &self.sourced_pointer,
            ],
        )
        .await
    }
}

// F4 across batches: the NameRegistered log above writes the registry-node pointer twice with
// one resolver, the registrar's ResolverChanged:authority (6) and the sourced
// surface-materialization ResolverChanged (1). The registrar's is the later: the pointer names
// the registrar resource. By bytes the sourced row, with the greater manifest id, was.
#[tokio::test]
async fn cross_batch_f4_name_registered_pointer_keeps_the_registrar_row() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f4_name_registered", 20).await?;
    let (registrar, registry_only) = (uuid(1), uuid(2));
    let named = name(25);
    let log = name_registered(&named, &registrar, &registry_only);
    old_rule_flips(&log.sourced_pointer, &log.pointer);
    log.write(&fixture).await?;
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
        json!({"resolver_address": RESOLVER, "resource_id": registrar,
               "source_family": REGISTRAR_FAMILY, "event_identity": log.pointer.identity})
    );
    settle(fixture).await
}

// F13 across batches: in the same NameRegistered log the sourced state-derived registry-only
// SurfaceBound (0) sets the controller to the registry owner, and the registrant's resource
// grant (2) sets it to the registrant. The grant is the later: the unmasked controller is the
// registrant. By bytes the sourced SurfaceBound was, and the controller stayed the registry
// owner.
#[tokio::test]
async fn cross_batch_f13_name_registered_controller_is_the_registrant() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f13_name_registered", 20).await?;
    let (registrar, registry_only) = (uuid(1), uuid(2));
    let named = name(26);
    let log = name_registered(&named, &registrar, &registry_only);
    old_rule_flips(&log.sourced_bound, &log.grant);
    log.write(&fixture).await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    assert_eq!(
        controller(&fixture).await?,
        json!({"controller": ALICE, "controller_action": "set", "controller_subject": ALICE,
               "controller_position": log.grant.position()})
    );
    assert_eq!(
        fixture
            .rows("project_address_controller_candidate")
            .await?
            .len(),
        2
    );
    settle(fixture).await
}

// F4 across batches, a controller NameRegistered that names a surface the registry-only
// authority did not have while a proven registrar is retained. The enrichment (adapters
// protocol/v1/registrar/enrichment.rs:81-120) sources the resolver link replay,
// ResolverChanged:surface-materialization (0), to the registry-only authority's manifest; the
// registrar surface snapshot (registrar_surface.rs:98-155) sources SurfaceBound (0),
// RegistrationGranted (1), ExpiryChanged (2), ResolverChanged:registrar-surface:{node} (3) and
// its grants to the registrar manifest. Only the two ResolverChanged are written. The snapshot's
// is the later: the pointer names the registrar resource. By bytes the enrichment's, with the
// greater manifest id, was.
#[tokio::test]
async fn cross_batch_f4_registrar_surface_pointer_follows_the_enrichment() -> Result<()> {
    let fixture = Fixture::new("families_realistic_f4_registrar_surface", 20).await?;
    let (registrar, registry_only) = (uuid(1), uuid(2));
    let named = name(27);
    let node = node(27);
    let enrichment = source(REGISTRY_FAMILY, CONTROLLER)
        .manifest(SOURCED_MANIFEST)
        .fact(
            "ResolverChanged",
            &format!("ResolverChanged:surface-materialization:{node}:{registry_only}:{RESOLVER}"),
            0,
        )
        .name(&named)
        .resource(&registry_only)
        .after(
            json!({"state_derived": true, "surface_materialization": true,
                      "source_event": "NameRegistered", "node": node,
                      "authority_kind": "registry_only",
                      "authority_key": format!("registry-only:{CHAIN}:{node}"),
                      "binding_kind": "declared_registry_path",
                      "pointer_reason": "surface_materialization_current_resolver",
                      "resolver": RESOLVER, "resolver_source_role": "registry"}),
        );
    let snapshot = source(REGISTRAR_FAMILY, CONTROLLER)
        .manifest(PRIMARY_MANIFEST)
        .fact(
            "ResolverChanged",
            &format!("ResolverChanged:registrar-surface:{node}"),
            3,
        )
        .name(&named)
        .resource(&registrar)
        .after(
            json!({"state_derived": true, "surface_materialization": true,
                      "registrar_surface_snapshot": true,
                      "source_event": "ReadableNameObserved",
                      "readable_source_event": "NameRegistered", "node": node,
                      "namehash": node, "surface_known": true, "authority_kind": "registrar",
                      "authority_key": "registrar:key", "registrant": ALICE, "owner": ALICE,
                      "resolver": RESOLVER, "resolver_source_role": "registry",
                      "pointer_reason": "surface_materialization_current_resolver"}),
        );
    old_rule_flips(&enrichment, &snapshot);
    write(&fixture, &[&enrichment, &snapshot]).await?;
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
        json!({"resolver_address": RESOLVER, "resource_id": registrar,
               "source_family": REGISTRAR_FAMILY, "event_identity": snapshot.identity})
    );
    settle(fixture).await
}
