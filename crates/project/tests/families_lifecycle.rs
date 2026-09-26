//! F2a through the family loop: the decoder's resource and triple keys, the association that
//! moves a triple between resources without touching either side's state, the retained
//! per-kind events with their immutable original name, the membership maxima and the child row.
//! The fixtures assert stored rows only; the reads that consume them come with step 3. Each case
//! undoes its last block byte for byte and equals a rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{Event, Fixture, uuid};
use serde_json::{Value, json};

const REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const WRAPPER: &str = "0x00000000000000000000000000000000000000e4";
const REGISTRY: &str = "0x00000000000000000000000000000000000000e5";
const ALICE: &str = "0x00000000000000000000000000000000000000aa";
const BOB: &str = "0x00000000000000000000000000000000000000bb";
const CAROL: &str = "0x00000000000000000000000000000000000000cc";
const V1_REGISTRAR: &str = "ens_v1_registrar_l1";
const V1_WRAPPER: &str = "ens_v1_wrapper_l1";
const V2_REGISTRY: &str = "ens_v2_registry_l1";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

fn pick(row: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).to_owned(), row[*name].clone()))
            .collect(),
    )
}

/// The retained rows of one state key in the canonical order.
async fn retained(fixture: &Fixture, state_key: &str) -> Result<Vec<Value>> {
    let mut rows: Vec<Value> = fixture
        .rows("project_lifecycle_event")
        .await?
        .into_iter()
        .filter(|row| row["state_key"] == json!(state_key))
        .collect();
    rows.sort_by_key(|row| {
        (
            row["block_number"].as_i64(),
            row["transaction_index"].as_i64(),
            row["log_index"].as_i64(),
        )
    });
    Ok(rows)
}

async fn one(fixture: &Fixture, table: &str) -> Result<Vec<Value>> {
    fixture.rows(table).await
}

fn block_of(value: &Value) -> Value {
    value["position"]["block_number"].clone()
}

/// An ENSv2 registry lifecycle event of triple (name 1, registry R, token 7).
async fn v2(
    fixture: &Fixture,
    block: i64,
    kind: &str,
    resource: Option<&str>,
    after: Value,
) -> Result<i64> {
    let mut after = after;
    after["registry_contract_instance_id"] = json!("R");
    after["token_id"] = json!("7");
    fixture
        .write(
            block,
            1,
            kind,
            V2_REGISTRY,
            Some(&name(1)),
            resource,
            after,
            REGISTRY,
        )
        .await
}

#[tokio::test]
async fn five_position_history_moves_the_association_and_nothing_else() -> Result<()> {
    let fixture = Fixture::new("families_lifecycle_five", 20).await?;
    let (k1, k2) = (uuid(1), uuid(2));
    let path = json!({"source_event": "RegistryPathExpired", "derived_from": "interpreter_state",
                      "terminal_reason": "registry_name_binding_expired", "expiry": 100});
    v2(
        &fixture,
        10,
        "RegistrationGranted",
        Some(&k1),
        json!({"expiry": 200}),
    )
    .await?;
    v2(&fixture, 12, "RegistrationReleased", None, path).await?;
    v2(
        &fixture,
        14,
        "ExpiryChanged",
        Some(&k1),
        json!({"expiry": 300}),
    )
    .await?;
    fixture.apply(14, FamilyMode::Normal).await;

    let association = one(&fixture, "project_lifecycle_association").await?;
    assert_eq!(
        pick(
            &association[0],
            &[
                "target_resource_id",
                "registry_identifier",
                "token_id",
                "block_number"
            ]
        ),
        json!({"target_resource_id": k1, "registry_identifier": "R", "token_id": "7", "block_number": 10})
    );
    let summary = one(&fixture, "project_lifecycle_triple_summary").await?;
    assert_eq!(block_of(&summary[0]["last_path_expiry"]), json!(12));
    let key_state_at_14 = one(&fixture, "project_lifecycle_key_state").await?;

    v2(
        &fixture,
        16,
        "RegistrationGranted",
        Some(&k2),
        json!({"expiry": 400}),
    )
    .await?;
    v2(
        &fixture,
        18,
        "ExpiryChanged",
        Some(&k2),
        json!({"expiry": 500}),
    )
    .await?;
    fixture.apply(18, FamilyMode::Normal).await;
    let association = one(&fixture, "project_lifecycle_association").await?;
    assert_eq!(association[0]["target_resource_id"], json!(k2));
    assert_eq!(association[0]["block_number"], json!(16));
    let key_state = one(&fixture, "project_lifecycle_key_state").await?;
    let k1_row = |rows: &[Value]| {
        rows.iter()
            .find(|row| row["resource_id"] == json!(k1))
            .cloned()
    };
    assert_eq!(
        k1_row(&key_state),
        k1_row(&key_state_at_14),
        "K1 is unchanged since 14"
    );
    assert_eq!(
        one(&fixture, "project_lifecycle_triple_summary").await?,
        summary
    );

    fixture.assert_undo_restores(18).await?;
    fixture.assert_rebuild_equal(18).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn competing_grants_in_one_block_associate_the_later_transaction() -> Result<()> {
    let fixture = Fixture::new("families_lifecycle_competing", 20).await?;
    let (k1, k2) = (uuid(1), uuid(2));
    fixture.surface(&name(1), &node(1)).await?;
    for (resource, transaction, identity) in [(&k2, 2, "grant-b"), (&k1, 1, "grant-a")] {
        // Inserted in the order that gives the later transaction the lower generated id.
        fixture.resource(resource).await?;
        fixture
            .event(
                Event::new(identity, 10, 0, "RegistrationGranted", V2_REGISTRY)
                    .name(&name(1))
                    .resource(resource)
                    .after(json!({"registry_contract_instance_id": "R", "token_id": "7"}))
                    .at(transaction, 0),
            )
            .await?;
    }
    fixture.apply(10, FamilyMode::Normal).await;
    let association = one(&fixture, "project_lifecycle_association").await?;
    assert_eq!(association[0]["target_resource_id"], json!(k2));
    assert_eq!(association[0]["transaction_index"], json!(2));
    fixture.assert_undo_restores(10).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn registry_identifier_comes_from_instance_then_emitter_then_registry() -> Result<()> {
    let fixture = Fixture::new("families_lifecycle_registry_id", 20).await?;
    fixture.surface(&name(1), &node(1)).await?;
    let events = [
        (
            "with-instance",
            json!({"registry_contract_instance_id": "I", "registry": "0xreg"}),
            json!({"emitting_address": REGISTRY}),
        ),
        (
            "with-emitter",
            json!({"registry": "0xreg"}),
            json!({"emitting_address": REGISTRY}),
        ),
        ("with-registry", json!({"registry": "0xreg"}), json!({})),
    ];
    for (log, (identity, after, raw)) in (1..).zip(events) {
        fixture
            .event(
                Event::new(identity, 10, log, "ExpiryChanged", V2_REGISTRY)
                    .name(&name(1))
                    .after(after)
                    .raw(raw),
            )
            .await?;
    }
    fixture.apply(10, FamilyMode::Normal).await;
    let mut registries: Vec<Value> = one(&fixture, "project_lifecycle_triple_summary")
        .await?
        .iter()
        .map(|row| row["registry_identifier"].clone())
        .collect();
    registries.sort_by_key(Value::to_string);
    assert_eq!(
        registries,
        vec![json!(REGISTRY), json!("0xreg"), json!("I")]
    );
    fixture.assert_undo_restores(10).await?;
    fixture.cleanup().await
}

/// Round 7: a grant on P naming R0, a wrapper transfer on P to Alice or Carol, a registry-only
/// binding of Q, then a wrapper transfer on P to Bob.
async fn round_seven(holder: &str) -> Result<(Fixture, Vec<Value>)> {
    let fixture = Fixture::new("families_lifecycle_round7", 20).await?;
    let (p, q) = (uuid(1), uuid(2));
    fixture
        .write(
            5,
            1,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&p),
            json!({"registrant": "0x00000000000000000000000000000000000000d0",
                      "namehash": node(1), "expiry": 1000, "status": "registered"}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            10,
            1,
            "TokenControlTransferred",
            V1_WRAPPER,
            Some(&name(1)),
            Some(&p),
            json!({"to": holder, "namehash": node(1)}),
            WRAPPER,
        )
        .await?;
    fixture
        .binding(&uuid(102), &name(1), &q, "ens_v1", 15, 1, None)
        .await?;
    fixture
        .write(
            15,
            1,
            "SurfaceBound",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&q),
            json!({"authority_kind": "registry_only", "state_derived": true}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            18,
            1,
            "TokenControlTransferred",
            V1_WRAPPER,
            Some(&name(1)),
            Some(&p),
            json!({"to": BOB, "namehash": node(1)}),
            WRAPPER,
        )
        .await?;
    fixture.apply(18, FamilyMode::Normal).await;
    let rows = retained(&fixture, &p).await?;
    Ok((fixture, rows))
}

#[tokio::test]
async fn round_seven_alice_and_carol_histories_differ_before_bob() -> Result<()> {
    let (alice, alice_rows) = round_seven(ALICE).await?;
    let (carol, carol_rows) = round_seven(CAROL).await?;
    for rows in [&alice_rows, &carol_rows] {
        assert_eq!(rows.len(), 3, "the grant and both transfers are retained");
        assert_eq!(rows[1]["source_family"], json!(V1_WRAPPER));
        assert_eq!(rows[2]["source_family"], json!(V1_WRAPPER));
        // Each reader field is stored on its own row, asserted separately.
        assert_eq!(
            rows[0]["decoded_logical_name_id"],
            json!(name(1)),
            "candidate identity"
        );
        assert_eq!(
            rows[0]["registrant"],
            json!("0x00000000000000000000000000000000000000d0")
        );
        assert_eq!(rows[0]["status"], json!("registered"));
        assert_eq!(rows[0]["expiry_seconds"], json!(1000));
        assert_eq!(rows[2]["to_address"], json!(BOB), "holder");
    }
    assert_eq!(alice_rows[1]["to_address"], json!(ALICE));
    assert_eq!(carol_rows[1]["to_address"], json!(CAROL));
    assert_ne!(
        alice_rows, carol_rows,
        "the two histories keep different stored state"
    );
    let key_state = one(&alice, "project_lifecycle_key_state").await?;
    let grant = &key_state[0]["last_grant"];
    assert_eq!(
        grant["registered_at"],
        json!(1_800_000_000 + 5 * 12),
        "registered_at"
    );
    assert_eq!(
        block_of(&key_state[0]["last_active"]),
        json!(5),
        "five-kind grant"
    );
    alice.assert_undo_restores(18).await?;
    alice.assert_rebuild_equal(18).await?;
    alice.cleanup().await?;
    carol.cleanup().await
}

/// Round 8: a registrar grant on lease L at 10, unnamed in history U and named in history N,
/// then a wrapper W at 12 that records L.
async fn round_eight(named: bool) -> Result<(Fixture, Value)> {
    let fixture = Fixture::new("families_lifecycle_round8", 20).await?;
    let (lease, wrapper) = (uuid(1), uuid(2));
    let grant_name = named.then(|| name(1));
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            V1_REGISTRAR,
            grant_name.as_deref(),
            Some(&lease),
            json!({"registrant": ALICE, "namehash": node(1), "expiry": 900}),
            REGISTRAR,
        )
        .await?;
    fixture
        .binding(&uuid(101), &name(1), &wrapper, "ens_v1", 12, 1, None)
        .await?;
    fixture
        .write(
            12,
            1,
            "SurfaceBound",
            V1_WRAPPER,
            Some(&name(1)),
            Some(&wrapper),
            json!({"authority_kind": "wrapper", "wrapped_registrar_resource_id": lease,
                      "node": node(1)}),
            WRAPPER,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let grant = retained(&fixture, &lease).await?.remove(0);
    Ok((fixture, grant))
}

/// The read-side reproduction of the two staging passes over stored rows: a row the adapter
/// emitted unnamed that no binding of its resource names, but a wrapper candidate does.
async fn wrapper_linked(fixture: &Fixture, row: &Value) -> Result<bool> {
    if !row["original_logical_name_id"].is_null() {
        return Ok(false);
    }
    let candidates = fixture.rows("project_binding_candidate").await?;
    let namehash = row["namehash"].clone();
    let direct = candidates.iter().any(|candidate| {
        candidate["resource_id"] == row["resource_id"] && candidate["surface_namehash"] == namehash
    });
    let wrapped = candidates.iter().any(|candidate| {
        candidate["wrapped_registrar_resource_id"] == row["resource_id"]
            && candidate["node"] == namehash
    });
    Ok(!direct && wrapped)
}

#[tokio::test]
async fn round_eight_already_named_grant_is_not_wrapper_linked() -> Result<()> {
    let (unnamed, u) = round_eight(false).await?;
    let (named, n) = round_eight(true).await?;
    assert_eq!(u["original_logical_name_id"], Value::Null);
    assert_eq!(n["original_logical_name_id"], json!(name(1)));
    assert_eq!(u["decoded_logical_name_id"], json!(name(1)));
    assert_eq!(n["decoded_logical_name_id"], json!(name(1)));
    assert!(wrapper_linked(&unnamed, &u).await?, "U is wrapper-linked");
    assert!(!wrapper_linked(&named, &n).await?, "N enters neither pass");
    for row in [&u, &n] {
        assert_eq!(row["registrant"], json!(ALICE));
        assert_eq!(row["expiry_seconds"], json!(900));
        assert_eq!(row["event_kind"], json!("RegistrationGranted"));
    }
    unnamed.assert_undo_restores(12).await?;
    unnamed.assert_rebuild_equal(12).await?;
    unnamed.cleanup().await?;
    named.cleanup().await
}

#[tokio::test]
async fn round_eight_same_transaction_witness_keeps_both_grants() -> Result<()> {
    let fixture = Fixture::new("families_lifecycle_witness", 20).await?;
    let (lease, wrapper) = (uuid(1), uuid(2));
    let grant = |expiry: i64| json!({"registrant": ALICE, "namehash": node(1), "expiry": expiry});
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            V1_REGISTRAR,
            Some(&name(1)),
            Some(&lease),
            grant(500),
            REGISTRAR,
        )
        .await?;
    fixture
        .binding(&uuid(101), &name(1), &wrapper, "ens_v1", 10, 2, None)
        .await?;
    fixture
        .write(
            10,
            2,
            "SurfaceBound",
            V1_WRAPPER,
            Some(&name(1)),
            Some(&wrapper),
            json!({"authority_kind": "wrapper", "wrapped_registrar_resource_id": lease,
                      "node": node(1)}),
            WRAPPER,
        )
        .await?;
    fixture
        .event(
            Event::new("grant-2", 12, 1, "RegistrationGranted", V1_REGISTRAR)
                .name(&name(1))
                .resource(&lease)
                .after(grant(600))
                .raw(json!({"emitting_address": REGISTRAR}))
                .at(3, 1),
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = retained(&fixture, &lease).await?;
    assert_eq!(
        rows.iter()
            .map(|row| pick(row, &["transaction_hash", "expiry_seconds"]))
            .collect::<Vec<_>>(),
        vec![
            json!({"transaction_hash": "0xtx10_0", "expiry_seconds": 500}),
            json!({"transaction_hash": "0xtx12_3", "expiry_seconds": 600}),
        ],
        "a reducer that keeps one grant per class drops the witness"
    );
    let candidates = fixture.rows("project_binding_candidate").await?;
    assert_eq!(
        candidates[0]["transaction_hash"],
        rows[0]["transaction_hash"]
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn maxima_follow_the_reducer_table() -> Result<()> {
    let fixture = Fixture::new("families_lifecycle_maxima", 20).await?;
    let (k, r) = (uuid(1), uuid(2));
    let path = json!({"source_event": "RegistryPathExpired", "derived_from": "interpreter_state",
                      "terminal_reason": "registry_name_binding_expired", "expiry": 50});
    let at = |block, kind: &'static str, resource: &str, after: Value| {
        (block, kind, resource.to_owned(), after)
    };
    let events = [
        // Grant then reservation: the reservation is the latest active.
        at(10, "RegistrationGranted", &k, json!({"expiry": 100})),
        at(
            11,
            "RegistrationReserved",
            &k,
            json!({"expiry": 110, "status": "reserved"}),
        ),
        // Path expiry, revival, then an ordinary renewal.
        at(12, "RegistrationReleased", &k, path.clone()),
        at(
            13,
            "RegistrationRenewed",
            &k,
            json!({"expiry": 130, "revived_from_expiry": true}),
        ),
        at(14, "RegistrationRenewed", &k, json!({"expiry": 140})),
        // An explicit release keeps the registrant it removed.
        at(15, "RegistrationReleased", &r, json!({"released_at": 15})),
    ];
    for (block, kind, resource, after) in events {
        fixture
            .write(
                block,
                1,
                kind,
                V2_REGISTRY,
                Some(&name(1)),
                Some(&resource),
                after,
                REGISTRY,
            )
            .await?;
    }
    fixture
        .event(
            Event::new("release-r", 16, 1, "RegistrationReleased", V2_REGISTRY)
                .name(&name(1))
                .resource(&r)
                .before(json!({"registrant": BOB}))
                .after(json!({})),
        )
        .await?;
    // The mixed-null block: a synthesised path expiry, a positioned one and a grant.
    let one_name = name(1);
    let synthesised = Event::new("path-null", 17, 0, "RegistrationReleased", V2_REGISTRY)
        .name(&one_name)
        .resource(&k)
        .after(path.clone())
        .synthesised();
    fixture.event(synthesised).await?;
    for (log, kind, after) in [
        (1, "RegistrationReleased", path.clone()),
        (2, "RegistrationGranted", json!({})),
    ] {
        fixture
            .write(
                17,
                log,
                kind,
                V2_REGISTRY,
                Some(&name(1)),
                Some(&k),
                after,
                REGISTRY,
            )
            .await?;
    }
    fixture.apply(16, FamilyMode::Normal).await;
    let state = |rows: &[Value], resource: &str| {
        rows.iter()
            .find(|row| row["resource_id"] == json!(resource))
            .cloned()
            .unwrap()
    };
    let rows = one(&fixture, "project_lifecycle_key_state").await?;
    let key = state(&rows, &k);
    assert_eq!(key["last_active"]["kind"], json!("RegistrationReserved"));
    assert_eq!(block_of(&key["last_active"]), json!(11));
    assert_eq!(block_of(&key["last_grant"]), json!(10));
    assert_eq!(block_of(&key["last_path_expiry"]), json!(12));
    assert_eq!(block_of(&key["last_revival"]), json!(13));
    assert_eq!(block_of(&key["last_renewal"]), json!(14));
    assert_eq!(key["last_renewal"]["revived_from_expiry"], Value::Null);
    let release = retained(&fixture, &r).await?;
    assert_eq!(release[1]["before_registrant"], json!(BOB));
    assert_eq!(
        block_of(&state(&rows, &r)["last_explicit_release"]),
        json!(16)
    );

    fixture.apply(17, FamilyMode::Normal).await;
    let key = state(&one(&fixture, "project_lifecycle_key_state").await?, &k);
    assert_eq!(key["last_path_expiry"]["position"]["log_index"], json!(1));
    assert_eq!(key["last_active"]["kind"], json!("RegistrationGranted"));
    assert_eq!(
        key["last_active"]["position"]["log_index"],
        json!(2),
        "after both path expiries"
    );
    let children = one(&fixture, "project_child_registration_state").await?;
    assert_eq!(children.len(), 0, "no registry instance id, no child row");
    fixture.assert_undo_restores(17).await?;
    fixture.assert_rebuild_equal(17).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn child_rows_select_granted_renewed_released_and_count_reservations() -> Result<()> {
    let fixture = Fixture::new("families_lifecycle_child", 20).await?;
    let resource = uuid(1);
    let after =
        |registrant: &str| json!({"registry_contract_instance_id": "C", "registrant": registrant});
    fixture
        .write(
            10,
            1,
            "RegistrationReserved",
            V2_REGISTRY,
            Some(&name(1)),
            Some(&resource),
            after(ALICE),
            REGISTRY,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    let child = one(&fixture, "project_child_registration_state").await?;
    assert_eq!(
        pick(&child[0], &["event_kind", "exists"]),
        json!({"event_kind": null, "exists": true})
    );
    fixture
        .write(
            11,
            1,
            "RegistrationGranted",
            V2_REGISTRY,
            Some(&name(1)),
            Some(&resource),
            after(BOB),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "RegistrationReserved",
            V2_REGISTRY,
            Some(&name(1)),
            Some(&resource),
            after(CAROL),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            2,
            "RegistrationReleased",
            V2_REGISTRY,
            Some(&name(1)),
            Some(&resource),
            after(BOB),
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let child = one(&fixture, "project_child_registration_state").await?;
    assert_eq!(
        pick(
            &child[0],
            &[
                "event_kind",
                "registrant",
                "exists",
                "block_number",
                "log_index"
            ]
        ),
        json!({"event_kind": "RegistrationReleased", "registrant": null, "exists": true,
               "block_number": 12, "log_index": 2})
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}
