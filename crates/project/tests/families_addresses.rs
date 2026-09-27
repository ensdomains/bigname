//! F13 and F14 through the family loop: the per-name controller fold with its token holder, the
//! controller candidates, the candidate-complete (address, name, relation) index derived from
//! them and F2a, and the inverse record indexes derived from node and record-id `addr` values. Undo restores the index rows with
//! their base rows, and a rebuild derives the same rows.
mod families_support;

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, RunMode, families::FamilyMode};
use families_support::{CHAIN, Fixture, uuid};
use serde_json::{Value, json};

const R1: &str = "0x00000000000000000000000000000000000000a1";
const ALICE: &str = "0x00000000000000000000000000000000000000AA";
const BOB: &str = "0x00000000000000000000000000000000000000bb";
const CAROL: &str = "0x00000000000000000000000000000000000000cc";
const DAVE: &str = "0x00000000000000000000000000000000000000dd";

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

fn index(rows: &[Value], names: &[&str]) -> Vec<Value> {
    rows.iter().map(|row| pick(row, names)).collect()
}

#[tokio::test]
async fn names_fold_their_controller_and_index_every_relation() -> Result<()> {
    let fixture = Fixture::new("families_addresses_names", 20).await?;
    let resource = uuid(1);
    let (one, v1) = (name(1), "ens_v1_registry_l1");
    let permission = |subject: &str, powers: Value| json!({"subject": subject, "scope": {"kind": "resource"}, "effective_powers": powers});
    let events = [
        (10, 1, "AuthorityTransferred", json!({"owner": ALICE})),
        (10, 2, "RegistrationGranted", json!({"registrant": CAROL})),
        (
            11,
            1,
            "PermissionChanged",
            permission(BOB, json!(["resource_control"])),
        ),
        // A revoke from someone other than the controller changes nothing.
        (11, 2, "PermissionChanged", permission(ALICE, json!([]))),
        // A grant scoped to something other than the resource is not a controller event.
        (
            11,
            3,
            "PermissionChanged",
            json!({"subject": DAVE, "scope": {"kind": "registry"},
                   "effective_powers": ["resource_control"]}),
        ),
        (12, 1, "TokenControlTransferred", json!({"to": DAVE})),
        (
            12,
            2,
            "PermissionChanged",
            permission(BOB, json!(["set_resolver"])),
        ),
    ];
    for (block, log, kind, after) in events {
        fixture
            .write(block, log, kind, v1, Some(&one), Some(&resource), after, R1)
            .await?;
    }
    fixture.apply(12, FamilyMode::Normal).await;

    let folds = fixture.rows("project_address_name_fold").await?;
    assert_eq!(folds.len(), 1);
    assert_eq!(
        pick(
            &folds[0],
            &[
                "controller",
                "controller_action",
                "controller_subject",
                "token_holder",
                "registrant"
            ]
        ),
        json!({"controller": null, "controller_action": "revoke", "controller_subject": BOB,
               "token_holder": DAVE, "registrant": DAVE}),
        "the registrant is the latest retained F2a row that reports one: the transfer at 12"
    );
    assert_eq!(folds[0]["controller_position"]["log_index"], json!(2));
    assert_eq!(folds[0]["controller_position"]["block_number"], json!(12));
    let candidates = fixture.rows("project_address_controller_candidate").await?;
    let mut candidates = index(
        &candidates,
        &["block_number", "log_index", "action", "subject"],
    );
    candidates.sort_by_key(|row| (row["block_number"].as_i64(), row["log_index"].as_i64()));
    assert_eq!(
        candidates,
        vec![
            json!({"block_number": 10, "log_index": 1, "action": "set", "subject": ALICE.to_lowercase()}),
            json!({"block_number": 11, "log_index": 1, "action": "set", "subject": BOB}),
            json!({"block_number": 11, "log_index": 2, "action": "revoke", "subject": ALICE.to_lowercase()}),
            json!({"block_number": 12, "log_index": 2, "action": "revoke", "subject": BOB}),
        ],
        "every controller event stays a candidate with its own position"
    );
    let mut relations = index(
        &fixture.rows("project_address_name_index").await?,
        &["address", "relation"],
    );
    relations.sort_by_key(Value::to_string);
    let mut expected = vec![
        json!({"address": ALICE.to_lowercase(), "relation": "effective_controller"}),
        json!({"address": BOB, "relation": "effective_controller"}),
    ];
    for address in [CAROL, DAVE] {
        for relation in ["registrant", "token_holder", "effective_controller"] {
            expected.push(json!({"address": address, "relation": relation}));
        }
    }
    expected.sort_by_key(Value::to_string);
    assert_eq!(
        relations, expected,
        "the index holds every address a relation can take; the read-time fold and masks \
         narrow it (the registrant and the transfer recipient come from F2a)"
    );

    fixture.assert_undo_restores(12).await?;
    // Block 11's grant still stands after undoing 12, so Bob is the controller there.
    fixture.apply(11, FamilyMode::Rebuild).await;
    let controllers = fixture.rows("project_address_name_index").await?;
    assert!(
        controllers.contains(&json!({"address": BOB, "logical_name_id": one,
                                          "relation": "effective_controller"}))
    );
    fixture.apply(12, FamilyMode::Normal).await;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn a_masked_owner_word_clears_the_controller() -> Result<()> {
    let fixture = Fixture::new("families_addresses_masked", 20).await?;
    let resource = uuid(2);
    let v1 = "ens_v1_registry_l1";
    fixture
        .write(
            10,
            1,
            "AuthorityTransferred",
            v1,
            Some(&name(2)),
            Some(&resource),
            json!({"owner": ALICE}),
            R1,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "AuthorityTransferred",
            v1,
            Some(&name(2)),
            Some(&resource),
            json!({"owner": BOB, "owner_word_unmasked": true}),
            R1,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let folds = fixture.rows("project_address_name_fold").await?;
    assert_eq!(
        folds[0]["controller"],
        json!("0x0000000000000000000000000000000000000000")
    );
    assert_eq!(
        index(
            &fixture.rows("project_address_name_index").await?,
            &["address", "relation"]
        ),
        vec![json!({"address": ALICE.to_lowercase(), "relation": "effective_controller"})],
        "the earlier transfer stays a candidate; the masked owner word never indexes an address"
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn addr_values_index_their_address_past_a_version_change() -> Result<()> {
    let fixture = Fixture::new("families_addresses_records", 20).await?;
    let addr = |node_hex: String, coin: &str, value: Value| {
        json!({"node": node_hex, "resolver": R1, "record_key": format!("addr:{coin}"),
               "record_family": "addr", "selector_key": coin, "value": value,
               "source_event": "AddressChanged"})
    };
    let v1 = "ens_v1_resolver_l1";
    fixture
        .write(
            10,
            1,
            "RecordChanged",
            v1,
            None,
            None,
            addr(node(1), "60", json!(ALICE)),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            2,
            "RecordChanged",
            v1,
            None,
            None,
            addr(node(1), "2147483658", json!({"bytes": BOB})),
            R1,
        )
        .await?;
    // A zero address, a non-EVM value and a text record are not indexed.
    fixture
        .write(
            10,
            3,
            "RecordChanged",
            v1,
            None,
            None,
            addr(
                node(2),
                "60",
                json!("0x0000000000000000000000000000000000000000"),
            ),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            4,
            "RecordChanged",
            v1,
            None,
            None,
            addr(node(2), "0", json!("0x00aa")),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            5,
            "RecordChanged",
            "ens_v2_resolver_l1",
            None,
            None,
            json!({"resolver": R1, "resolver_record_id": "7",
                      "storage_model": "resolver_record_id", "record_key": "addr:60",
                      "record_family": "addr", "selector_key": "60", "value": CAROL,
                      "source_event": "AddressUpdated"}),
            R1,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    let lower = ALICE.to_lowercase();
    let node_one = vec![
        json!({"address": lower, "coin_type": "60", "resolver_address": R1, "node": node(1)}),
        json!({"address": BOB, "coin_type": "2147483658", "resolver_address": R1,
               "node": node(1)}),
    ];
    assert_eq!(
        index(
            &fixture.rows("project_address_record_node_index").await?,
            &["address", "coin_type", "resolver_address", "node"]
        ),
        node_one
    );
    assert_eq!(
        index(
            &fixture.rows("project_address_record_id_index").await?,
            &["address", "coin_type", "record_id"]
        ),
        vec![json!({"address": CAROL, "coin_type": "60", "record_id": "7"})]
    );

    // A version change at node 1 keeps its values in the index: a later link can keep a value
    // below the version served (record_inventory.rs, the combined boundary), so readers apply the
    // boundary and the index stays a superset.
    fixture
        .write(
            11,
            1,
            "RecordVersionChanged",
            v1,
            None,
            None,
            json!({"node": node(1), "resolver": R1, "version": "1"}),
            R1,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    assert_eq!(
        index(
            &fixture.rows("project_address_record_node_index").await?,
            &["address", "coin_type", "resolver_address", "node"]
        ),
        node_one
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// The served value of a coin-60 pair is its AddressChanged half (record_inventory.rs,
// `ranked_records`): when the two halves carry different addresses, only that one is indexed. A
// value carried as address_bytes_hex is indexed like a value payload, and a value ordered after
// the version change only by its event identity (two synthesised events of one block) is kept.
#[tokio::test]
async fn the_index_holds_the_served_half_of_a_pair_and_hex_payloads() -> Result<()> {
    let fixture = Fixture::new("families_addresses_pair", 20).await?;
    let v1 = "ens_v1_resolver_l1";
    let record = |node_hex: String, source: &str, payload: Value| {
        let mut after = json!({"node": node_hex, "resolver": R1, "record_key": "addr:60",
                               "record_family": "addr", "selector_key": "60",
                               "source_event": source});
        for (field, value) in payload.as_object().cloned().unwrap_or_default() {
            after[field] = value;
        }
        after
    };
    fixture
        .write(
            10,
            4,
            "RecordChanged",
            v1,
            None,
            None,
            record(
                node(1),
                "AddressChanged",
                json!({"value": {"bytes": ALICE}}),
            ),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            5,
            "RecordChanged",
            v1,
            None,
            None,
            record(node(1), "AddrChanged", json!({"value": BOB})),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            7,
            "RecordChanged",
            v1,
            None,
            None,
            record(
                node(2),
                "AddressChanged",
                json!({"address_bytes_hex": CAROL}),
            ),
            R1,
        )
        .await?;
    // Two synthesised events of block 11 at node 3: the version change sorts before the value
    // only by identity.
    fixture
        .event(
            families_support::Event::new("11:a-version", 11, 0, "RecordVersionChanged", v1)
                .synthesised()
                .after(json!({"node": node(3), "resolver": R1, "version": "1"}))
                .raw(json!({"emitting_address": R1})),
        )
        .await?;
    fixture
        .event(
            families_support::Event::new("11:b-value", 11, 0, "RecordChanged", v1)
                .synthesised()
                .after(record(node(3), "AddressChanged", json!({"value": DAVE})))
                .raw(json!({"emitting_address": R1})),
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let mut rows = index(
        &fixture.rows("project_address_record_node_index").await?,
        &["address", "node"],
    );
    rows.sort_by_key(Value::to_string);
    let mut expected = vec![
        json!({"address": ALICE.to_lowercase(), "node": node(1)}),
        json!({"address": CAROL, "node": node(2)}),
        json!({"address": DAVE, "node": node(3)}),
    ];
    expected.sort_by_key(Value::to_string);
    assert_eq!(
        rows, expected,
        "the AddrChanged half's own address is not indexed"
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// An unnamed registrar grant is named when a binding candidate of its lease arrives a block
// later; its registrant then enters the name's index, since the registrant is read from F2a.
#[tokio::test]
async fn a_grant_named_later_puts_its_registrant_in_the_index() -> Result<()> {
    let fixture = Fixture::new("families_addresses_decoded", 20).await?;
    let lease = uuid(5);
    let registrar = "ens_v1_registrar_l1";
    fixture
        .write(
            10,
            1,
            "RegistrationGranted",
            registrar,
            None,
            Some(&lease),
            json!({"namehash": node(4), "registrant": CAROL}),
            R1,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    assert_eq!(
        fixture.rows("project_address_name_index").await?,
        Vec::<Value>::new(),
        "an unnamed grant indexes nothing"
    );
    fixture
        .binding(&uuid(105), &name(4), &lease, "ens_v1", 11, 1, None)
        .await?;
    fixture
        .write(
            11,
            1,
            "SurfaceBound",
            registrar,
            Some(&name(4)),
            Some(&lease),
            json!({"authority_kind": "registrar"}),
            R1,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let mut rows = index(
        &fixture.rows("project_address_name_index").await?,
        &["address", "logical_name_id", "relation"],
    );
    rows.sort_by_key(Value::to_string);
    let mut expected: Vec<Value> = ["registrant", "token_holder", "effective_controller"]
        .into_iter()
        .map(|relation| json!({"address": CAROL, "logical_name_id": name(4), "relation": relation}))
        .collect();
    expected.sort_by_key(Value::to_string);
    assert_eq!(rows, expected);
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// A named write keeps the name it was written under on its index row, so the inverse read finds a
// value whose node is not the name's namehash by the name, as the forward inventory does.
#[tokio::test]
async fn a_named_value_indexes_the_name_it_was_written_under() -> Result<()> {
    let fixture = Fixture::new("families_addresses_named_node", 20).await?;
    let name = format!("ens:{}", node(7));
    fixture
        .write(
            10,
            1,
            "RecordChanged",
            "ens_v1_resolver_l1",
            Some(&name),
            None,
            json!({"node": node(8), "resolver": R1, "record_key": "addr:60",
                   "record_family": "addr", "selector_key": "60", "value": DAVE,
                   "source_event": "AddressChanged"}),
            R1,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    assert_eq!(
        index(
            &fixture.rows("project_address_record_node_index").await?,
            &["address", "coin_type", "node", "logical_name_id"]
        ),
        vec![json!({"address": DAVE, "coin_type": "60", "node": node(8),
                    "logical_name_id": name})]
    );
    fixture.assert_undo_restores(10).await?;
    fixture.assert_rebuild_equal(10).await?;
    fixture.cleanup().await
}

const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const OWNER: &str = "0x00000000000000000000000000000000000000c1";

/// The served address rows (`address_records_current`, the table the served resolves-to reader
/// reads) as (address, coin type, name).
async fn served_addresses(fixture: &Fixture, target: i64) -> Result<Vec<Value>> {
    Engine::new(fixture.pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: target,
            affected_from_block: 0,
            affected_to_block: target,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object('address', address, 'coin_type', coin_type,
                                   'logical_name_id', logical_name_id)
         FROM address_records_current ORDER BY address, coin_type, logical_name_id",
    )
    .fetch_all(&fixture.pool)
    .await?)
}

/// A name the served build publishes: bound at `block` to its own resource, owned through the
/// registry and pointing at R1, which an active manifest declares.
async fn served_name(fixture: &Fixture, n: u64, block: i64) -> Result<()> {
    let (name, resource) = (name(n), uuid(100 + n as u32));
    fixture
        .binding(
            &uuid(200 + n as u32),
            &name,
            &resource,
            "ens_v1",
            block,
            0,
            None,
        )
        .await?;
    let registry = "ens_v1_registry_l1";
    fixture
        .write(
            block,
            1,
            "AuthorityTransferred",
            registry,
            Some(&name),
            Some(&resource),
            json!({"node": node(n), "owner": OWNER, "owner_getter": OWNER,
                   "authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            block,
            2,
            "ResolverChanged",
            registry,
            Some(&name),
            Some(&resource),
            json!({"node": node(n), "resolver": R1}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

async fn declare_r1(fixture: &Fixture) -> Result<()> {
    let payload = json!({"deployment_epoch": "fixture", "contracts": [{
        "role": "resolver", "address": R1, "proxy_kind": "none", "start_block": 0,
        "read_features": []
    }]});
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (1, 'ens', 'ens_v1_resolver_l1', $1, 'fixture', 'active', 'fixture',
                 'fixture/resolver.yaml', $2)
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .bind(&payload)
    .fetch_one(&fixture.pool)
    .await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, derivation_kind,
             canonicality_state, after_state)
         VALUES ('manifest:resolver', 'ens', 'SourceManifestUpdated', 'ens_v1_resolver_l1', 1,
                 $1, $2, 'manifest_sync', 'canonical',
                 jsonb_build_object('rollout_status', 'active', 'manifest_payload', $3::jsonb))",
    )
    .bind(id)
    .bind(CHAIN)
    .bind(&payload)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

// A named write under A at node 8, whose own name B is bound a block later, then version changes
// at node 8 named under B, unnamed and named under A, and a later link there. Each step runs the
// served build and reads what it publishes for both names (`address_records_current`, the
// resolves-to reader's table): A serves the value until the version named under A, and B never
// does. The F14 node index keeps the value under A through every step, so it holds every served
// row; readers apply the version and link boundary.
#[tokio::test]
async fn a_named_write_keeps_its_name_through_a_rebinding_versions_and_a_link() -> Result<()> {
    let fixture = Fixture::new("families_addresses_combined", 20).await?;
    let (a, b) = (name(7), name(8));
    declare_r1(&fixture).await?;
    served_name(&fixture, 7, 10).await?;
    let v1 = "ens_v1_resolver_l1";
    let addr = |value: &str| {
        json!({"node": node(8), "resolver": R1, "record_key": "addr:60",
               "record_family": "addr", "selector_key": "60", "value": value,
               "source_event": "AddressChanged"})
    };
    fixture
        .write(11, 1, "RecordChanged", v1, Some(&a), None, addr(DAVE), R1)
        .await?;
    let mut steps = Vec::new();
    fixture.apply(11, FamilyMode::Normal).await;
    steps.push(("write under A", served_addresses(&fixture, 11).await?));
    served_name(&fixture, 8, 12).await?;
    fixture.apply(12, FamilyMode::Normal).await;
    steps.push(("B bound at node 8", served_addresses(&fixture, 12).await?));
    let version = |n: &str| json!({"node": node(8), "resolver": R1, "version": n});
    fixture
        .write(
            13,
            1,
            "RecordVersionChanged",
            v1,
            Some(&b),
            None,
            version("1"),
            R1,
        )
        .await?;
    fixture.apply(13, FamilyMode::Normal).await;
    steps.push(("named version", served_addresses(&fixture, 13).await?));
    fixture
        .write(
            14,
            1,
            "RecordVersionChanged",
            v1,
            None,
            None,
            version("2"),
            R1,
        )
        .await?;
    fixture.apply(14, FamilyMode::Normal).await;
    steps.push(("unnamed version", served_addresses(&fixture, 14).await?));
    fixture
        .write(
            15,
            1,
            "RecordVersionChanged",
            v1,
            Some(&a),
            None,
            version("3"),
            R1,
        )
        .await?;
    fixture.apply(15, FamilyMode::Normal).await;
    steps.push((
        "named version under A",
        served_addresses(&fixture, 15).await?,
    ));
    fixture
        .write(
            16,
            1,
            "ResolverRecordLinked",
            "ens_v2_resolver_l1",
            None,
            None,
            json!({"source_event": "Linked", "storage_model": "resolver_record_id",
                   "resolver": R1, "node": node(8), "resolver_record_id": "1",
                   "dns_encoded_name": "0x00"}),
            R1,
        )
        .await?;
    fixture.apply(16, FamilyMode::Normal).await;
    steps.push(("later link", served_addresses(&fixture, 16).await?));
    let indexed = index(
        &fixture.rows("project_address_record_node_index").await?,
        &["address", "coin_type", "node", "logical_name_id"],
    );
    // One value, indexed under A at node 8 through every step: B's binding at node 8 does not
    // relabel it, and no version change removes it.
    let dave = DAVE.to_lowercase();
    assert_eq!(
        indexed,
        vec![json!({"address": dave, "coin_type": "60", "node": node(8), "logical_name_id": a})]
    );
    // A serves it until a version change attributed to A; B, whose own node it is, never
    // serves it; a version named under B, an unnamed one, and the later link change neither.
    let served_a = vec![json!({"address": dave, "coin_type": "60", "logical_name_id": a})];
    let expected = [
        ("write under A", served_a.clone()),
        ("B bound at node 8", served_a.clone()),
        ("named version", served_a.clone()),
        ("unnamed version", served_a),
        ("named version under A", vec![]),
        ("later link", vec![]),
    ];
    for ((step, served), (expected_step, expected_rows)) in steps.iter().zip(&expected) {
        assert_eq!((step, served), (expected_step, expected_rows));
        // Every served row has its index row under the name it was written under.
        for row in served {
            assert!(
                indexed.iter().any(|indexed| {
                    indexed["address"] == row["address"]
                        && indexed["coin_type"] == row["coin_type"]
                        && indexed["logical_name_id"] == row["logical_name_id"]
                }),
                "{step}: {row} is served but not indexed"
            );
        }
    }
    fixture.assert_undo_restores(16).await?;
    fixture.assert_rebuild_equal(16).await?;
    fixture.cleanup().await
}

// A registrar transfer the adapter emitted unnamed reaches the name's fold only when a later
// binding names its row. The fold's token holder is the latest transfer's recipient, so the
// transfer named later replaces an earlier named one.
#[tokio::test]
async fn a_transfer_named_later_becomes_the_name_folds_token_holder() -> Result<()> {
    let fixture = Fixture::new("families_addresses_decoded_transfer", 20).await?;
    let lease = uuid(6);
    let registrar = "ens_v1_registrar_l1";
    fixture
        .write(
            9,
            1,
            "TokenControlTransferred",
            registrar,
            Some(&name(4)),
            Some(&lease),
            json!({"namehash": node(4), "to": CAROL}),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            1,
            "TokenControlTransferred",
            registrar,
            None,
            Some(&lease),
            json!({"namehash": node(4), "to": DAVE}),
            R1,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await;
    fixture
        .binding(&uuid(106), &name(4), &lease, "ens_v1", 11, 1, None)
        .await?;
    fixture
        .write(
            11,
            1,
            "SurfaceBound",
            registrar,
            Some(&name(4)),
            Some(&lease),
            json!({"authority_kind": "registrar"}),
            R1,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let fold = fixture.rows("project_address_name_fold").await?;
    let fold = fold
        .iter()
        .find(|row| row["logical_name_id"] == json!(name(4)))
        .expect("the name has a fold row");
    assert_eq!(
        (
            fold["token_holder"].clone(),
            fold["token_holder_position"]["block_number"].clone()
        ),
        (json!(DAVE), json!(10)),
        "the later transfer, named at 11, is the token holder"
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

/// Runs `transfers` (block, log, named, recipient) on one lease of name 4, applies to 10, binds
/// the name at 11 so the unnamed ones are named then, and returns the fold's token holder with
/// its block and log after the undo and rebuild checks.
async fn token_holder_after_naming(
    prefix: &str,
    transfers: &[(i64, i64, bool, &str)],
) -> Result<Value> {
    let fixture = Fixture::new(prefix, 20).await?;
    let lease = uuid(6);
    let registrar = "ens_v1_registrar_l1";
    for (block, log, named, to) in transfers {
        fixture
            .write(
                *block,
                *log,
                "TokenControlTransferred",
                registrar,
                named.then(|| name(4)).as_deref(),
                Some(&lease),
                json!({"namehash": node(4), "to": to}),
                R1,
            )
            .await?;
    }
    fixture.apply(10, FamilyMode::Normal).await;
    fixture
        .binding(&uuid(106), &name(4), &lease, "ens_v1", 11, 1, None)
        .await?;
    fixture
        .write(
            11,
            1,
            "SurfaceBound",
            registrar,
            Some(&name(4)),
            Some(&lease),
            json!({"authority_kind": "registrar"}),
            R1,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let fold = fixture.rows("project_address_name_fold").await?;
    let holder = fold
        .iter()
        .find(|row| row["logical_name_id"] == json!(name(4)))
        .map(|row| {
            json!([
                row["token_holder"],
                row["token_holder_position"]["block_number"],
                row["token_holder_position"]["log_index"]
            ])
        })
        .expect("the name has a fold row");
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await?;
    Ok(holder)
}

// A transfer named later that is older than the held one leaves the token holder alone
// (registrant.rs, the strict comparison with token_holder_position).
#[tokio::test]
async fn an_older_transfer_named_later_keeps_the_token_holder() -> Result<()> {
    let holder = token_holder_after_naming(
        "families_addresses_older_named_later",
        &[(9, 1, false, CAROL), (10, 1, true, DAVE)],
    )
    .await?;
    assert_eq!(holder, json!([DAVE, 10, 1]));
    Ok(())
}

// Two transfers in one block both named later: the later log is the token holder.
#[tokio::test]
async fn two_transfers_named_later_in_one_block_take_the_later_log() -> Result<()> {
    let holder = token_holder_after_naming(
        "families_addresses_two_named_later",
        &[(9, 1, false, CAROL), (9, 2, false, DAVE)],
    )
    .await?;
    assert_eq!(holder, json!([DAVE, 9, 2]));
    Ok(())
}
