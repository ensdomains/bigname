//! The same-log ordering matrix, second half: F6, F7, F9, F10, F11, F12 and F13. The first half
//! (families_ordering_matrix.rs) explains the shape: two facts of one key at one (block,
//! transaction, log) whose emission ordinal (docs/glossary.md#emission-ordinal) and identity bytes
//! disagree, written in the reverse of the ordinal order; the higher ordinal wins, where the old
//! identity-byte rule would pick the other fact. Every case undoes its block and equals a rebuild.
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
const REVERSE: &str = "0x00000000000000000000000000000000000000f1";

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

    /// One batch past nine facts: ordinals 9 and 10 with the same prefix and suffix.
    fn tenth(derivation: &str, log: i64, suffix: &str) -> Self {
        Self::checked(Self {
            log,
            later: identity(derivation, 7, log, suffix, 10),
            earlier: identity(derivation, 7, log, suffix, 9),
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

// F6: two writes of one named record key at one log, ordinals 9 and 10 of one batch. The
// record and its partition are the tenth write.
#[tokio::test]
async fn f6_the_node_record_is_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f6", 20).await?;
    let named = name(8);
    let facts = Pair::tenth("ens_v1_unwrapped_authority", 5, "TextChanged:url");
    let text = |value: &str| {
        json!({"node": node(8), "record_key": "text:url", "record_family": "text",
               "value": value, "source_event": "TextChanged"})
    };
    pair(
        &fixture,
        &facts,
        fact(
            "RecordChanged",
            "ens_v1_resolver_l1",
            text("https://b"),
            RESOLVER_1,
        )
        .name(&named),
        fact(
            "RecordChanged",
            "ens_v1_resolver_l1",
            text("https://a"),
            RESOLVER_1,
        )
        .name(&named),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let value = only(&fixture, "project_node_record_value").await?;
    assert_eq!(
        pick(&value, &["value", "status", "event_identity"]),
        json!({"value": "https://b", "status": "success", "event_identity": facts.later})
    );
    let partition = only(&fixture, "project_node_record_partition").await?;
    assert_eq!(partition["event_identity"], json!(facts.later));
    settle(fixture).await
}

// F7: two writes of one record-id value at one log, and two links of one node at the next. The
// value and the link are the higher-ordinal facts.
#[tokio::test]
async fn f7_the_record_id_value_and_link_are_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f7", 20).await?;
    let values = Pair::crossed(
        "ens_v2_resolver",
        5,
        "TextChanged:7:avatar",
        "TextChanged:7:avatar",
    );
    let value = |value: &str| {
        json!({"resolver": RESOLVER_1, "resolver_record_id": "7", "record_key": "text:avatar",
               "record_family": "text", "storage_model": "resolver_record_id",
               "value": value, "source_event": "TextChanged"})
    };
    pair(
        &fixture,
        &values,
        fact(
            "RecordChanged",
            "ens_v2_resolver_l1",
            value("b"),
            RESOLVER_1,
        ),
        fact(
            "RecordChanged",
            "ens_v2_resolver_l1",
            value("a"),
            RESOLVER_1,
        ),
    )
    .await?;
    let links = Pair::crossed("ens_v2_resolver", 6, "Linked:9", "Linked:7");
    let link = |record: &str| {
        json!({"node": node(9), "resolver": RESOLVER_1, "resolver_record_id": record,
               "storage_model": "resolver_record_id", "source_event": "Linked"})
    };
    pair(
        &fixture,
        &links,
        fact(
            "ResolverRecordLinked",
            "ens_v2_resolver_l1",
            link("9"),
            RESOLVER_1,
        ),
        fact(
            "ResolverRecordLinked",
            "ens_v2_resolver_l1",
            link("7"),
            RESOLVER_1,
        ),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_record_id_value").await?;
    assert_eq!(
        pick(&row, &["value", "event_identity"]),
        json!({"value": "b", "event_identity": values.later})
    );
    let row = only(&fixture, "project_resolver_link").await?;
    assert_eq!(
        pick(&row, &["record_id", "event_identity"]),
        json!({"record_id": "9", "event_identity": links.later})
    );
    settle(fixture).await
}

// F9: an approval granted (ordinal 9) and revoked (ordinal 10) on one key at one log. The
// approval is the revoke.
#[tokio::test]
async fn f9_the_account_approval_is_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f9", 20).await?;
    let facts = Pair::tenth("standard_approval", 5, "ApprovalForAll:operator");
    let approval = |approved: bool| {
        json!({"subject": BOB, "relation_kind": "operator", "approved": approved,
               "scope": {"kind": "account", "authority_kind": "registry",
                         "authority_contract": REGISTRY, "owner": ALICE},
               "effective_powers": [], "inheritance_path": [],
               "transfer_behavior": {"mode": "owner_scoped"}})
    };
    pair(
        &fixture,
        &facts,
        fact(
            "AccountPermissionChanged",
            "ens_v1_registry_l1",
            approval(false),
            REGISTRY,
        ),
        fact(
            "AccountPermissionChanged",
            "ens_v1_registry_l1",
            approval(true),
            REGISTRY,
        ),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_account_approval").await?;
    assert_eq!(
        pick(&row, &["approved", "event_identity"]),
        json!({"approved": false, "event_identity": facts.later})
    );
    settle(fixture).await
}

// F10: an alias set (ordinal 9) and removed (ordinal 10) at one log. The name's and the
// resolver's alias rows are the removal.
#[tokio::test]
async fn f10_the_alias_is_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f10", 20).await?;
    let named = name(10);
    let facts = Pair::tenth("ens_v2_resolver", 5, "AliasChanged");
    let alias = |active: bool| {
        json!({"resolver": RESOLVER_1, "active": active,
               "alias_state": if active { "active" } else { "removed" },
               "to_logical_name_id": name(11)})
    };
    pair(
        &fixture,
        &facts,
        fact(
            "AliasChanged",
            "ens_v2_resolver_l1",
            alias(false),
            RESOLVER_1,
        )
        .name(&named),
        fact(
            "AliasChanged",
            "ens_v2_resolver_l1",
            alias(true),
            RESOLVER_1,
        )
        .name(&named),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    for table in ["project_name_alias", "project_resolver_alias"] {
        let row = only(&fixture, table).await?;
        assert_eq!(
            pick(&row, &["active", "alias_state", "event_identity"]),
            json!({"active": false, "alias_state": "removed", "event_identity": facts.later}),
            "{table}"
        );
    }
    settle(fixture).await
}

// F11: two NewOwner of one ENSv1 child edge at one log, and an ENSv2 subregistry set (ordinal 1)
// and cleared (ordinal 2) at the next. The edge keeps the higher-ordinal owner, the parent the
// clear.
#[tokio::test]
async fn f11_the_child_edge_and_parent_subregistry_are_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f11", 20).await?;
    let edges = Pair::crossed(
        "ens_v1_unwrapped_authority",
        5,
        "NewOwner:bob",
        "NewOwner:alice",
    );
    let edge = |owner: &str| {
        json!({"source_event": "NewOwner", "node": node(1), "child_node": node(12),
               "labelhash": node(99), "owner": owner})
    };
    pair(
        &fixture,
        &edges,
        fact(
            "SubregistryChanged",
            "ens_v1_registry_l1",
            edge(BOB),
            REGISTRY,
        ),
        fact(
            "SubregistryChanged",
            "ens_v1_registry_l1",
            edge(ALICE),
            REGISTRY,
        ),
    )
    .await?;
    let named = name(13);
    let parents = Pair::crossed(
        "ens_v2_registry_resource_surface",
        6,
        "SubregistryUpdate:clear",
        "SubregistryUpdate:set",
    );
    pair(
        &fixture,
        &parents,
        fact(
            "SubregistryChanged",
            "ens_v2_registry_l1",
            json!({"subregistry": null}),
            V2_REGISTRY,
        )
        .name(&named),
        fact(
            "SubregistryChanged",
            "ens_v2_registry_l1",
            json!({"subregistry": V2_REGISTRY}),
            V2_REGISTRY,
        )
        .name(&named),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let row = only(&fixture, "project_child_edge_candidate").await?;
    assert_eq!(
        pick(&row, &["owner", "event_identity"]),
        json!({"owner": BOB, "event_identity": edges.later})
    );
    let row = only(&fixture, "project_parent_subregistry").await?;
    assert_eq!(
        pick(&row, &["subregistry_address", "event_identity"]),
        json!({"subregistry_address": "", "event_identity": parents.later})
    );
    settle(fixture).await
}

// F12: two ReverseChanged of one tuple at one log, and two NameChanged claims of that tuple and
// node at the next (ordinals 9 and 10). The tuple keeps the higher-ordinal reverse node and
// claim with their positions; the node's claim row is the tenth.
#[tokio::test]
async fn f12_the_reverse_tuple_and_claim_are_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f12", 20).await?;
    let reverses = Pair::crossed(
        "ens_v1_reverse_claim",
        5,
        "ReverseClaimed:second",
        "ReverseClaimed:first",
    );
    let reverse = |reverse_node: &str| {
        json!({"address": ALICE, "coin_type": "60", "namespace": "ens",
               "reverse_node": reverse_node, "source_event": "ReverseClaimed",
               "claim_provenance": "reverse_registrar"})
    };
    pair(
        &fixture,
        &reverses,
        fact(
            "ReverseChanged",
            "ens_v1_reverse_l1",
            reverse(&node(15)),
            REVERSE,
        ),
        fact(
            "ReverseChanged",
            "ens_v1_reverse_l1",
            reverse(&node(14)),
            REVERSE,
        ),
    )
    .await?;
    let claims = Pair::tenth("ens_v1_unwrapped_authority", 6, "NameChanged");
    let claim = |raw_name: &str| {
        json!({"node": node(15), "record_key": "name", "source_event": "NameChanged",
               "resolver": RESOLVER_1, "raw_name": raw_name,
               "primary_claim_source": {"address": ALICE, "coin_type": "60",
                                        "namespace": "ens", "reverse_node": node(15)}})
    };
    pair(
        &fixture,
        &claims,
        fact(
            "RecordChanged",
            "ens_v1_resolver_l1",
            claim("b.eth"),
            RESOLVER_1,
        ),
        fact(
            "RecordChanged",
            "ens_v1_resolver_l1",
            claim("a.eth"),
            RESOLVER_1,
        ),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let tuple = only(&fixture, "project_reverse_tuple").await?;
    assert_eq!(
        pick(
            &tuple,
            &[
                "reverse_node",
                "reverse_position",
                "raw_name",
                "claim_event_identity",
                "claim_position"
            ]
        ),
        json!({"reverse_node": node(15), "reverse_position": reverses.later_position(),
               "raw_name": "b.eth", "claim_event_identity": claims.later,
               "claim_position": claims.later_position()})
    );
    let node_claim = only(&fixture, "project_reverse_node_claim").await?;
    assert_eq!(
        pick(&node_claim, &["raw_name", "event_identity"]),
        json!({"raw_name": "b.eth", "event_identity": claims.later})
    );
    settle(fixture).await
}

// F13: an AuthorityTransferred (ordinal 1) and a state-derived registry-only SurfaceBound
// (ordinal 2) of one name at one log. The controller is the SurfaceBound's owner at its
// position; both stay candidates.
#[tokio::test]
async fn f13_the_controller_is_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f13_controller", 20).await?;
    let resource = uuid(1);
    let named = name(16);
    let facts = Pair::crossed(
        "ens_v1_unwrapped_authority",
        5,
        "SurfaceBound:registry_only",
        "AuthorityTransferred:alice",
    );
    pair(
        &fixture,
        &facts,
        fact(
            "SurfaceBound",
            "ens_v1_registry_l1",
            json!({"state_derived": true, "authority_kind": "registry_only", "owner": BOB}),
            REGISTRY,
        )
        .name(&named)
        .resource(&resource),
        fact(
            "AuthorityTransferred",
            "ens_v1_registry_l1",
            json!({"node": node(16), "owner": ALICE}),
            REGISTRY,
        )
        .name(&named)
        .resource(&resource),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let fold = only(&fixture, "project_address_name_fold").await?;
    assert_eq!(
        pick(
            &fold,
            &["controller", "controller_action", "controller_position"]
        ),
        json!({"controller": BOB, "controller_action": "set",
               "controller_position": facts.later_position()})
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

// F13: a registrar grant to ALICE (ordinal 1) and a token transfer to BOB (ordinal 2) of one
// name at one log. The registrant and the token holder are BOB, at the transfer's position.
#[tokio::test]
async fn f13_the_registrant_and_token_holder_are_the_higher_ordinal() -> Result<()> {
    let fixture = Fixture::new("families_matrix_f13_registrant", 20).await?;
    let resource = uuid(1);
    let named = name(17);
    let facts = Pair::crossed(
        "ens_v1_unwrapped_authority",
        5,
        "Transfer:bob",
        "NameRegistered:alice",
    );
    pair(
        &fixture,
        &facts,
        fact(
            "TokenControlTransferred",
            "ens_v1_registrar_l1",
            json!({"to": BOB}),
            REGISTRAR,
        )
        .name(&named)
        .resource(&resource),
        fact(
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            json!({"registrant": ALICE, "namehash": node(17), "expiry": 2_000_000_000}),
            REGISTRAR,
        )
        .name(&named)
        .resource(&resource),
    )
    .await?;
    fixture.apply(BLOCK, FamilyMode::Normal).await;
    let fold = only(&fixture, "project_address_name_fold").await?;
    assert_eq!(
        pick(
            &fold,
            &[
                "registrant",
                "registrant_position",
                "token_holder",
                "token_holder_position"
            ]
        ),
        json!({"registrant": BOB, "registrant_position": facts.later_position(),
               "token_holder": BOB, "token_holder_position": facts.later_position()})
    );
    settle(fixture).await
}
