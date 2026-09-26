//! D12 as amended (Tate, 2026-09-26): several facts of one adapter emission batch at one log
//! fold in the order the adapter wrote them. Within one (block, transaction, log) the trailing
//! emission ordinal (docs/glossary.md#emission-ordinal) of the event identity decides before the
//! identity bytes. The wrapper transfer shapes are the adapter's
//! own (adapters schema_v2/protocol/v1/wrapper/transfer.rs:141-169, permissions.rs:47-123):
//! one TransferSingle writes the delegate approval clear, the old holder's revoke, the new
//! holder's grant and, for a retained delegate that was the old holder, its re-grant, in that
//! order. Every case states its expected rows directly, then checks undo and a rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{CHAIN, Event, Fixture, hash, uuid};
use serde_json::{Value, json};

const WRAPPER_FAMILY: &str = "ens_v1_wrapper_l1";
const OLD: &str = "0x00000000000000000000000000000000000000a1";
const NEW: &str = "0x00000000000000000000000000000000000000b2";
const DELEGATE: &str = "0x00000000000000000000000000000000000000c3";
const HOLDER_POWERS: [&str; 3] = ["resource_control", "set_resolver", "transfer"];
const DELEGATE_POWERS: [&str; 1] = ["extend_subname_expiry"];
const NODE: &str = "0x00000000000000000000000000000000000000000000000000000000000000aa";

fn name() -> String {
    format!("ens:{NODE}")
}

/// The adapter's raw-log identity: `{derivation}:{manifest}:{chain}:{block hash}:{tx hash}:
/// {log}:{suffix}:{ordinal}` (adapters schema_v2/normalized.rs:118-131), with the derivation
/// kind of a wrapper PermissionChanged (adapters schema_v2/common.rs:266-295).
fn identity(manifest: i64, block: i64, log: i64, suffix: &str, ordinal: &str) -> String {
    format!(
        "ens_v1_unwrapped_authority:{manifest}:{CHAIN}:{}:0xtx{block}_0:{log}:{suffix}:{ordinal}",
        hash(block)
    )
}

/// A wrapper PermissionChanged of the resource scope: a grant carries its powers and a grant
/// source, a revoke no powers and a revocation source (adapters protocol/permissions.rs:196-236).
fn permission(relation: &str, subject: &str, grant: bool, powers: &[&str]) -> Value {
    let source = json!({"kind": "ens_v1_authority", "authority_kind": "wrapper",
                        "relation_kind": relation, "node": NODE});
    json!({
        "subject": subject,
        "scope": {"kind": "resource"},
        "effective_powers": if grant { json!(powers) } else { json!([]) },
        "grant_source": if grant { source.clone() } else { Value::Null },
        "revocation_source": if grant { Value::Null } else { source },
        "inheritance_path": [],
        "transfer_behavior": "replace_on_authority_change",
    })
}

/// One fact of a TransferSingle at (block 12, tx 0, log 5).
struct Fact {
    relation: &'static str,
    subject: &'static str,
    grant: bool,
    powers: &'static [&'static str],
    ordinal: &'static str,
    manifest: i64,
}

fn fact(relation: &'static str, subject: &'static str, grant: bool, ordinal: &'static str) -> Fact {
    let powers: &'static [&'static str] = match (relation, grant) {
        ("holder", true) => &HOLDER_POWERS,
        ("token_approval", true) => &DELEGATE_POWERS,
        _ => &[],
    };
    Fact {
        relation,
        subject,
        grant,
        powers,
        ordinal,
        manifest: 7,
    }
}

/// A fixture with the name wrapped for OLD at block 10, then `facts` written at one log of
/// block 12 in the order given (which is also their generated-id order).
async fn transfer(prefix: &str, facts: &[Fact]) -> Result<Fixture> {
    let fixture = Fixture::new(prefix, 20).await?;
    let resource = uuid(1);
    fixture.surface(&name(), NODE).await?;
    fixture.resource(&resource).await?;
    let minted = identity(7, 10, 1, &format!("TransferSingle:{NODE}"), "0");
    fixture
        .event(
            Event::new(&minted, 10, 1, "TokenControlTransferred", WRAPPER_FAMILY)
                .name(&name())
                .resource(&resource)
                .after(json!({"source_event": "NameWrapped", "to": OLD})),
        )
        .await?;
    let granted = identity(7, 10, 1, "PermissionChanged:x:holder:0:grant:old", "1");
    fixture
        .event(
            Event::new(&granted, 10, 1, "PermissionChanged", WRAPPER_FAMILY)
                .name(&name())
                .resource(&resource)
                .after(permission("holder", OLD, true, &HOLDER_POWERS)),
        )
        .await?;
    for fact in facts {
        let action = if fact.grant { "grant" } else { "revoke" };
        let suffix = format!(
            "PermissionChanged:TransferSingle:{NODE}:{}:0:{action}:{}",
            fact.relation, fact.subject
        );
        let id = identity(fact.manifest, 12, 5, &suffix, fact.ordinal);
        fixture
            .event(
                Event::new(&id, 12, 5, "PermissionChanged", WRAPPER_FAMILY)
                    .name(&name())
                    .resource(&resource)
                    .after(permission(
                        fact.relation,
                        fact.subject,
                        fact.grant,
                        fact.powers,
                    ))
                    .at(0, 5),
            )
            .await?;
    }
    fixture.apply(12, FamilyMode::Normal).await;
    Ok(fixture)
}

/// Each grant row of the resource: subject, powers, revoked and the ordinal it ended on.
async fn grants(fixture: &Fixture) -> Result<Vec<Value>> {
    let mut rows: Vec<Value> = fixture
        .rows("project_grant")
        .await?
        .iter()
        .map(|row| {
            let identity = row["event_identity"].as_str().unwrap_or_default();
            json!({
                "subject": row["subject"],
                "powers": row["effective_powers"],
                "revoked": row["revoked"],
                "ordinal": identity.rsplit(':').next(),
            })
        })
        .collect();
    rows.sort_by_key(|row| row["subject"].as_str().unwrap_or_default().to_owned());
    Ok(rows)
}

async fn lifecycle(fixture: &Fixture) -> Result<Value> {
    let rows = fixture.rows("project_wrapper_state").await?;
    Ok(rows
        .first()
        .map(|row| {
            json!({"source": row["lifecycle_source"], "unwrapped": row["lifecycle_unwrapped"]})
        })
        .unwrap_or(Value::Null))
}

async fn settle(fixture: Fixture) -> Result<()> {
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Holder to holder: the old holder's revoke is written before the new holder's grant, so the
// wrapper lifecycle ends on the grant and the name stays wrapped with its restrictions.
#[tokio::test]
async fn a_holder_to_holder_transfer_keeps_the_name_wrapped() -> Result<()> {
    let fixture = transfer(
        "families_ordering_holder",
        &[
            fact("holder", OLD, false, "1"),
            fact("holder", NEW, true, "2"),
        ],
    )
    .await?;
    assert_eq!(
        lifecycle(&fixture).await?,
        json!({"source": "holder_grant", "unwrapped": false})
    );
    assert_eq!(
        grants(&fixture).await?,
        vec![
            json!({"subject": OLD, "powers": [], "revoked": true, "ordinal": "1"}),
            json!({"subject": NEW, "powers": HOLDER_POWERS, "revoked": false, "ordinal": "2"}),
        ]
    );
    settle(fixture).await
}

// To the approved delegate: the approval clear is written before the holder rows, so under the
// shared (resource, subject, scope) key the recipient's holder grant is the later row.
#[tokio::test]
async fn a_transfer_to_the_delegate_leaves_the_recipient_holder_powers() -> Result<()> {
    let fixture = transfer(
        "families_ordering_delegate",
        &[
            fact("token_approval", DELEGATE, false, "1"),
            fact("holder", OLD, false, "2"),
            fact("holder", DELEGATE, true, "3"),
        ],
    )
    .await?;
    assert_eq!(
        grants(&fixture).await?,
        vec![
            json!({"subject": OLD, "powers": [], "revoked": true, "ordinal": "2"}),
            json!({"subject": DELEGATE, "powers": HOLDER_POWERS, "revoked": false,
                   "ordinal": "3"}),
        ]
    );
    assert_eq!(
        lifecycle(&fixture).await?,
        json!({"source": "holder_grant", "unwrapped": false})
    );
    settle(fixture).await
}

// A delegate retained under CANNOT_APPROVE that was the old holder is re-granted after the
// holder revoke, so its newest row is the token-approval grant.
#[tokio::test]
async fn a_retained_delegate_is_re_granted_after_its_holder_revoke() -> Result<()> {
    let fixture = transfer(
        "families_ordering_retained",
        &[
            fact("holder", OLD, false, "1"),
            fact("holder", NEW, true, "2"),
            fact("token_approval", OLD, true, "3"),
        ],
    )
    .await?;
    assert_eq!(
        grants(&fixture).await?,
        vec![
            json!({"subject": OLD, "powers": DELEGATE_POWERS, "revoked": false, "ordinal": "3"}),
            json!({"subject": NEW, "powers": HOLDER_POWERS, "revoked": false, "ordinal": "2"}),
        ]
    );
    settle(fixture).await
}

// The delivery order, and with it the generated ids, does not decide: the delegate case written
// in reverse gives the same rows.
#[tokio::test]
async fn reversed_delivery_gives_the_same_rows() -> Result<()> {
    let fixture = transfer(
        "families_ordering_reversed",
        &[
            fact("holder", DELEGATE, true, "3"),
            fact("holder", OLD, false, "2"),
            fact("token_approval", DELEGATE, false, "1"),
        ],
    )
    .await?;
    assert_eq!(
        grants(&fixture).await?,
        vec![
            json!({"subject": OLD, "powers": [], "revoked": true, "ordinal": "2"}),
            json!({"subject": DELEGATE, "powers": HOLDER_POWERS, "revoked": false,
                   "ordinal": "3"}),
        ]
    );
    settle(fixture).await
}

// Ordinals compare as numbers: the tenth fact of a log follows the ninth. The two identities
// differ only in the ordinal, where bytes would put ":10" first.
#[tokio::test]
async fn ordinal_ten_follows_ordinal_nine() -> Result<()> {
    let ninth = Fact {
        powers: &DELEGATE_POWERS,
        ..fact("holder", NEW, true, "9")
    };
    let fixture = transfer(
        "families_ordering_ten",
        &[fact("holder", NEW, true, "10"), ninth],
    )
    .await?;
    let rows = grants(&fixture).await?;
    assert_eq!(
        rows.iter().find(|row| row["subject"] == json!(NEW)),
        Some(
            &json!({"subject": NEW, "powers": HOLDER_POWERS, "revoked": false,
                     "ordinal": "10"})
        )
    );
    settle(fixture).await
}

// A suffix that is not a u32 has no ordinal and sorts before every fact that has one at the same
// log, then by identity: here the valid ordinal 0 still follows it.
#[tokio::test]
async fn an_invalid_or_overflowing_suffix_sorts_before_an_ordinal() -> Result<()> {
    for (prefix, ordinal) in [
        ("families_ordering_overflow", "4294967296"),
        ("families_ordering_letters", "x1"),
    ] {
        let fixture = transfer(
            prefix,
            &[
                fact("holder", NEW, true, "0"),
                fact("holder", NEW, false, ordinal),
            ],
        )
        .await?;
        let rows = grants(&fixture).await?;
        assert_eq!(
            rows.iter().find(|row| row["subject"] == json!(NEW)),
            Some(
                &json!({"subject": NEW, "powers": HOLDER_POWERS, "revoked": false,
                         "ordinal": "0"})
            ),
            "{ordinal}"
        );
        settle(fixture).await?;
    }
    Ok(())
}

// Two sources writing one grant key at one log (a shape the adapter is not known to produce; the
// D12 precondition rules it out). The rule orders by ordinal first, so manifest 7's second fact
// follows manifest 9's first although its identity bytes are lower; at equal ordinals the
// identity bytes decide and manifest 9 follows manifest 7.
#[tokio::test]
async fn a_cross_source_collision_orders_by_ordinal_then_identity() -> Result<()> {
    let mut first = fact("holder", NEW, true, "2");
    first.manifest = 7;
    let mut second = fact("holder", NEW, false, "1");
    second.manifest = 9;
    let fixture = transfer("families_ordering_cross_ordinal", &[first, second]).await?;
    let rows = grants(&fixture).await?;
    assert_eq!(
        rows.iter().find(|row| row["subject"] == json!(NEW)),
        Some(
            &json!({"subject": NEW, "powers": HOLDER_POWERS, "revoked": false,
                     "ordinal": "2"})
        )
    );
    settle(fixture).await?;

    let mut first = fact("holder", NEW, true, "1");
    first.manifest = 9;
    let mut second = fact("holder", NEW, false, "1");
    second.manifest = 7;
    let fixture = transfer("families_ordering_cross_identity", &[first, second]).await?;
    let rows = grants(&fixture).await?;
    assert_eq!(
        rows.iter().find(|row| row["subject"] == json!(NEW)),
        Some(
            &json!({"subject": NEW, "powers": HOLDER_POWERS, "revoked": false,
                     "ordinal": "1"})
        ),
        "equal ordinals: the higher identity bytes (manifest 9) are the later fact"
    );
    settle(fixture).await
}

// Boundary facts carry no transaction or log, and their trailing number is not an emission
// index (adapters normalized.rs:141-144), so they keep the full-identity byte order: ":10"
// sorts before ":9".
#[tokio::test]
async fn boundary_facts_keep_identity_order() -> Result<()> {
    let fixture = Fixture::new("families_ordering_boundary", 20).await?;
    let resource = uuid(1);
    fixture.surface(&name(), NODE).await?;
    fixture.resource(&resource).await?;
    for (ordinal, grant) in [("9", true), ("10", false)] {
        let id = format!(
            "ens_v1_wrapper:7:{CHAIN}:{}:boundary:holder:{ordinal}",
            hash(12)
        );
        let action = permission("holder", NEW, grant, &HOLDER_POWERS);
        fixture
            .event(
                Event::new(&id, 12, 0, "PermissionChanged", WRAPPER_FAMILY)
                    .name(&name())
                    .resource(&resource)
                    .after(action)
                    .synthesised(),
            )
            .await?;
    }
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = grants(&fixture).await?;
    assert_eq!(
        rows,
        vec![json!({"subject": NEW, "powers": HOLDER_POWERS, "revoked": false, "ordinal": "9"})],
        "byte order puts :9 after :10, so the grant at :9 is the later boundary fact"
    );
    settle(fixture).await
}

// The one known cross-source pair at one log, the disclosed precondition of the amendment: a
// NameWrapped log writes the registry-node pointer twice with the same resolver. The wrapper's
// own ResolverChanged carries ordinal 4 or 5, depending on whether a SurfaceBound was emitted
// before it (adapters authority_transition.rs:489-497), and the registry-read surface
// materialization, a batch of its own sourced to the registry manifest, carries ordinal 0
// (authority_transition.rs:200-320). Ordinals count from 0 again per emission batch
// (normalized.rs:118-131, sourced_events.rs:57-71), so the families keep the wrapper row, the
// name's authority after NameWrapped. The registry row's manifest id is the higher one, so its
// identity is byte-greater and only the ordinal picks the wrapper row. The served read breaks
// the tie by normalized_event_id DESC (builders/record_inventory/mirror.rs:105-109) and keeps
// the registry row the adapter inserted last, so the two differ in provenance only.
#[tokio::test]
async fn name_wrapped_pointer_keeps_the_wrapper_row() -> Result<()> {
    const RESOLVER: &str = "0x00000000000000000000000000000000000000d4";
    let fixture = Fixture::new("families_ordering_name_wrapped_pointer", 20).await?;
    let wrapper = uuid(1);
    let anchor = uuid(2);
    fixture.surface(&name(), NODE).await?;
    fixture.resource(&wrapper).await?;
    fixture.resource(&anchor).await?;
    let prefix = |manifest: i64| {
        format!(
            "ens_v1_unwrapped_authority:{manifest}:{CHAIN}:{}:0xtx12_0:5",
            hash(12)
        )
    };
    let wrapper_identity = format!(
        "{}:ResolverChanged:authority:NameWrapped:{RESOLVER}:5",
        prefix(6131)
    );
    let registry_identity = format!(
        "{}:ResolverChanged:surface-materialization:{NODE}:{anchor}:{RESOLVER}:0",
        prefix(6133)
    );
    for (identity, family, resource) in [
        (&wrapper_identity, WRAPPER_FAMILY, &wrapper),
        (&registry_identity, "ens_v1_registry_l1", &anchor),
    ] {
        fixture
            .event(
                Event::new(identity, 12, 5, "ResolverChanged", family)
                    .name(&name())
                    .resource(resource)
                    .after(json!({"resolver": RESOLVER, "node": NODE})),
            )
            .await?;
    }
    assert!(
        registry_identity.as_bytes() > wrapper_identity.as_bytes(),
        "identity bytes alone would pick the registry row"
    );
    fixture.apply(12, FamilyMode::Normal).await;
    let pointers: Vec<(String, Option<String>, String, String)> = sqlx::query_as(
        "SELECT resolver_address, resource_id::text, source_family, event_identity
         FROM project_registry_pointer",
    )
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        pointers,
        vec![(
            RESOLVER.to_owned(),
            Some(wrapper.clone()),
            WRAPPER_FAMILY.to_owned(),
            wrapper_identity.clone()
        )],
        "the wrapper's ordinal beats the registry batch's 0: the wrapper row wins"
    );
    let served: String = sqlx::query_scalar(
        "SELECT source_family FROM normalized_events
         WHERE event_kind = 'ResolverChanged' ORDER BY normalized_event_id DESC LIMIT 1",
    )
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(
        served, "ens_v1_registry_l1",
        "the served tie-break keeps the other source's row, with the same resolver"
    );
    settle(fixture).await
}
