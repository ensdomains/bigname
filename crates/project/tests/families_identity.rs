//! F1 and F2c through the family loop: name migration and authority epoch starts, binding
//! candidates with their SurfaceBound facts and registry-only predecessor, registry node
//! ownership with the old-registry fold, and registry-binding observations. Each case undoes its
//! last block byte for byte and equals a rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{CHAIN, Event, Fixture, uuid};
use serde_json::{Value, json};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const REGISTRAR: &str = "0x00000000000000000000000000000000000000e2";
const WRAPPER: &str = "0x00000000000000000000000000000000000000e3";
const OWNER: &str = "0x00000000000000000000000000000000000000a1";

fn node(n: u64) -> String {
    format!("0x{n:064x}")
}

fn name(n: u64) -> String {
    format!("ens:{}", node(n))
}

fn columns(row: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).to_owned(), row[*name].clone()))
            .collect(),
    )
}

#[tokio::test]
async fn a_name_keeps_its_migration_and_each_arms_epoch_start() -> Result<()> {
    let fixture = Fixture::new("families_identity_names", 20).await?;
    let resource = uuid(1);
    fixture
        .write(
            10,
            1,
            "AuthorityEpochChanged",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&resource),
            json!({"authority_kind": "registrar"}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&resource),
            json!({"authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "MigrationApplied",
            "ens_v2_migration_l1",
            Some(&name(1)),
            None,
            json!({"migration_path": "wrapper_backed", "evidence": [{"kind": "proof"}]}),
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = fixture.rows("project_name_state").await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        columns(
            &rows[0],
            &["migration_path", "migration_evidence", "migrated_at"]
        ),
        json!({"migration_path": "wrapper_backed", "migration_evidence": [{"kind": "proof"}],
               "migrated_at": "2027-01-15T08:02:24+00:00"})
    );
    let start = &rows[0]["authority_start_positions"]["ens_v1"];
    assert_eq!(
        columns(start, &["block_number", "authority_kind", "resource_id"]),
        json!({"block_number": 11, "authority_kind": "registry_only", "resource_id": resource}),
        "the arm keeps its latest epoch start"
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

/// F1 keys a name per chain, like every other family table, so one chain's run neither reads nor
/// overwrites another chain's row for the same logical name. Today a logical name's surface
/// belongs to one chain (name_surfaces keys on logical_name_id alone and normalized_events
/// references it by chain), so the test drops that reference to let a second chain carry the
/// name. Each chain then reads only its own epoch start.
#[tokio::test]
async fn each_chain_keeps_its_own_epoch_start_for_one_name() -> Result<()> {
    const OTHER: &str = "other-chain";
    let fixture = Fixture::new("families_identity_chain_key", 20).await?;
    fixture.lineage(OTHER, 20).await?;
    sqlx::query(
        "ALTER TABLE normalized_events
             DROP CONSTRAINT normalized_events_chain_id_logical_name_id_fkey",
    )
    .execute(&fixture.pool)
    .await?;
    fixture
        .write(
            10,
            1,
            "AuthorityEpochChanged",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            None,
            json!({"authority_kind": "registrar"}),
            REGISTRAR,
        )
        .await?;
    let named = name(1);
    fixture
        .event(
            Event::new(
                "other:AuthorityEpochChanged:11:1",
                11,
                1,
                "AuthorityEpochChanged",
                "ens_v1_registry_l1",
            )
            .on(OTHER)
            .name(&named)
            .after(json!({"authority_kind": "registry_only"}))
            .raw(json!({"emitting_address": REGISTRY})),
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    fixture.apply_on(OTHER, 12).await;
    let starts: Vec<(String, Value)> = sqlx::query_as(
        "SELECT chain_id, authority_start_positions -> 'ens_v1'
         FROM project_name_state WHERE logical_name_id = $1 ORDER BY chain_id",
    )
    .bind(&named)
    .fetch_all(&fixture.pool)
    .await?;
    let starts: Vec<Value> = starts
        .into_iter()
        .map(|(chain, start)| {
            json!({"chain": chain, "block_number": start["block_number"],
                   "authority_kind": start["authority_kind"]})
        })
        .collect();
    let mut expected = vec![
        json!({"chain": CHAIN, "block_number": 10, "authority_kind": "registrar"}),
        json!({"chain": OTHER, "block_number": 11, "authority_kind": "registry_only"}),
    ];
    expected.sort_by_key(|start| start["chain"].as_str().map(str::to_owned));
    assert_eq!(
        starts, expected,
        "each chain keeps its own row and epoch start"
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn binding_candidates_keep_their_bound_facts_and_registry_only_predecessor() -> Result<()> {
    let fixture = Fixture::new("families_identity_candidates", 20).await?;
    let (lease, wrapped, registry_only) = (uuid(1), uuid(2), uuid(3));
    let (first, second, third) = (uuid(101), uuid(102), uuid(103));
    fixture
        .binding(&first, &name(1), &lease, "ens_v1", 10, 1, Some(11))
        .await?;
    fixture
        .write(
            10,
            1,
            "SurfaceBound",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&lease),
            json!({"authority_kind": "registrar", "state_derived": false}),
            REGISTRAR,
        )
        .await?;
    fixture
        .binding(&second, &name(1), &wrapped, "ens_v1", 11, 2, Some(12))
        .await?;
    fixture
        .write(
            11,
            2,
            "SurfaceBound",
            "ens_v1_wrapper_l1",
            Some(&name(1)),
            Some(&wrapped),
            json!({"authority_kind": "wrapper", "wrapped_registrar_resource_id": lease,
                   "node": node(1).to_uppercase().replace("0X", "0x")}),
            WRAPPER,
        )
        .await?;
    fixture
        .binding(&third, &name(1), &registry_only, "ens_v1", 12, 3, None)
        .await?;
    fixture
        .write(
            12,
            3,
            "SurfaceBound",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&registry_only),
            json!({"authority_kind": "registry_only", "state_derived": true}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            4,
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&registry_only),
            json!({"authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;

    let mut candidates = fixture.rows("project_binding_candidate").await?;
    candidates.sort_by_key(|row| row["block_number"].as_i64());
    assert_eq!(candidates.len(), 3);
    assert_eq!(
        columns(
            &candidates[0],
            &[
                "surface_binding_id",
                "authority_kind",
                "state_derived",
                "registry_only",
                "log_index"
            ]
        ),
        json!({"surface_binding_id": first, "authority_kind": "registrar", "state_derived": false,
               "registry_only": false, "log_index": 1})
    );
    assert_eq!(
        columns(
            &candidates[1],
            &[
                "wrapped_registrar_resource_id",
                "node",
                "emitting_address",
                "transaction_hash"
            ]
        ),
        json!({"wrapped_registrar_resource_id": lease, "node": node(1), "emitting_address": WRAPPER,
               "transaction_hash": "0xtx11_0"})
    );
    assert_eq!(
        columns(
            &candidates[2],
            &[
                "registry_only",
                "predecessor_resource_id",
                "lease_resource_id"
            ]
        ),
        json!({"registry_only": true, "predecessor_resource_id": wrapped, "lease_resource_id": wrapped}),
        "the replaced binding is the latest earlier candidate of the arm"
    );
    assert_eq!(
        columns(
            &candidates[2],
            &[
                "predecessor_wrapped_registrar_resource_id",
                "predecessor_node"
            ]
        ),
        json!({"predecessor_wrapped_registrar_resource_id": lease, "predecessor_node": node(1)}),
        "a NameWrapper predecessor carries the registrar lease it recorded"
    );
    assert_eq!(
        candidates[2]["lease_position"], candidates[2]["predecessor_position"],
        "until a successor grant, the lease stands at the predecessor"
    );
    assert_eq!(
        columns(
            &candidates[1],
            &["event_identity", "surface_bound_position"]
        ),
        json!({"event_identity": "SurfaceBound:11:2",
               "surface_bound_position": {"block_number": 11, "transaction_index": 0,
                                          "log_index": 2, "event_identity": "SurfaceBound:11:2"}}),
        "a candidate carries the identity of the SurfaceBound that opened it"
    );
    assert_eq!(
        candidates[2]["predecessor_position"]["block_number"],
        json!(11)
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn registry_nodes_fold_the_old_registry_and_observations_keep_their_clear() -> Result<()> {
    let fixture = Fixture::new("families_identity_registry", 20).await?;
    let resource = uuid(4);
    fixture
        .write(10, 1, "AuthorityTransferred", "ens_v1_registry_l1", None, Some(&resource),
            json!({"node": node(7), "owner": OWNER, "owner_getter": OWNER, "emitter_role": "registry_old"}), REGISTRY)
        .await?;
    fixture
        .write(
            11,
            1,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"node": node(1), "child_node": node(7), "labelhash": node(70), "owner": OWNER,
                   "emitter_role": "registry"}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "SurfaceUnbound",
            "ens_v1_registrar_l1",
            None,
            Some(&resource),
            json!({"registry_contract": REGISTRY}),
            REGISTRAR,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let nodes = fixture.rows("project_registry_node_state").await?;
    assert_eq!(
        nodes
            .iter()
            .map(|row| columns(
                row,
                &[
                    "node",
                    "owner",
                    "has_old_record",
                    "first_current_record_block",
                    "emitter_role"
                ]
            ))
            .collect::<Vec<_>>(),
        vec![
            json!({"node": node(7), "owner": OWNER, "has_old_record": true,
                    "first_current_record_block": 11, "emitter_role": "registry"})
        ]
    );
    let observations = fixture.rows("project_registry_binding_observation").await?;
    assert_eq!(
        observations
            .iter()
            .map(|row| columns(
                row,
                &[
                    "attributed_via",
                    "event_kind",
                    "registry_owner",
                    "applicable",
                    "clear_event_identity"
                ]
            ))
            .collect::<Vec<_>>(),
        vec![
            json!({"attributed_via": "own", "event_kind": "SurfaceUnbound", "registry_owner": null,
                    "applicable": false, "clear_event_identity": "SurfaceUnbound:12:1"})
        ],
        "the unbinding clears the resource's registry owner and stays a row"
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// Two bindings of one name and resource opened by two logs of one block (one per arm, since a
// name's bindings of one arm cannot overlap) each carry their own SurfaceBound, and an epoch
// that arrives a block later turns both registry-only.
#[tokio::test]
async fn each_binding_keeps_its_own_surface_bound_and_a_later_epoch_makes_it_registry_only()
-> Result<()> {
    let fixture = Fixture::new("families_identity_own_event", 20).await?;
    let (lease, registry) = (uuid(1), uuid(2));
    let (first, second, third) = (uuid(101), uuid(102), uuid(103));
    fixture
        .binding(&first, &name(1), &lease, "ens_v1", 10, 1, Some(11))
        .await?;
    fixture
        .write(10, 1, "SurfaceBound", "ens_v1_registrar_l1", Some(&name(1)), Some(&lease),
            json!({"authority_kind": "registrar", "authority_key": "first", "state_derived": false}),
            REGISTRAR)
        .await?;
    fixture
        .binding(&second, &name(1), &registry, "ens_v1", 11, 1, None)
        .await?;
    fixture
        .write(11, 1, "SurfaceBound", "ens_v1_registry_l1", Some(&name(1)), Some(&registry),
            json!({"authority_kind": "registry_only", "authority_key": "second", "state_derived": true}),
            REGISTRY)
        .await?;
    fixture
        .binding(&third, &name(1), &registry, "ens_v2", 11, 3, None)
        .await?;
    fixture
        .write(11, 3, "SurfaceBound", "ens_v1_registry_l1", Some(&name(1)), Some(&registry),
            json!({"authority_kind": "registry_only", "authority_key": "third", "state_derived": true}),
            REGISTRY)
        .await?;
    fixture
        .write(
            12,
            1,
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&registry),
            json!({"authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let mut candidates = fixture.rows("project_binding_candidate").await?;
    candidates.sort_by_key(|row| {
        row["log_index"].as_i64().unwrap_or_default()
            + 100 * row["block_number"].as_i64().unwrap_or_default()
    });
    assert_eq!(
        candidates
            .iter()
            .map(|row| columns(row, &["event_identity", "authority_key", "registry_only"]))
            .collect::<Vec<_>>(),
        vec![
            json!({"event_identity": "SurfaceBound:10:1", "authority_key": "first", "registry_only": false}),
            json!({"event_identity": "SurfaceBound:11:1", "authority_key": "second", "registry_only": false}),
            json!({"event_identity": "SurfaceBound:11:3", "authority_key": "third", "registry_only": false}),
        ],
        "each binding is associated with the SurfaceBound at its own log"
    );

    fixture.apply(12, FamilyMode::Normal).await;
    let mut candidates = fixture.rows("project_binding_candidate").await?;
    candidates.sort_by_key(|row| {
        row["log_index"].as_i64().unwrap_or_default()
            + 100 * row["block_number"].as_i64().unwrap_or_default()
    });
    assert_eq!(
        candidates
            .iter()
            .map(|row| columns(row, &["registry_only", "predecessor_resource_id"]))
            .collect::<Vec<_>>(),
        vec![
            json!({"registry_only": false, "predecessor_resource_id": null}),
            json!({"registry_only": true, "predecessor_resource_id": lease}),
            json!({"registry_only": true, "predecessor_resource_id": null}),
        ],
        "the later epoch marks both bindings of the resource; each predecessor is the latest \
         earlier candidate of its own arm"
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

// A registry-only binding's lease moves to a later registrar grant of the name on another
// resource once the predecessor's lease was released before that grant (stage.rs:47-135).
#[tokio::test]
async fn a_successor_grant_after_a_release_becomes_the_handoff_lease() -> Result<()> {
    let fixture = Fixture::new("families_identity_successor", 20).await?;
    let (lease, registry, successor, stray) = (uuid(1), uuid(2), uuid(3), uuid(4));
    let namehash = node(1);
    fixture
        .binding(&uuid(101), &name(1), &lease, "ens_v1", 10, 1, Some(11))
        .await?;
    fixture
        .write(
            10,
            1,
            "SurfaceBound",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&lease),
            json!({"authority_kind": "registrar"}),
            REGISTRAR,
        )
        .await?;
    fixture
        .binding(&uuid(102), &name(1), &registry, "ens_v1", 11, 1, None)
        .await?;
    fixture
        .write(
            11,
            1,
            "SurfaceBound",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&registry),
            json!({"authority_kind": "registry_only", "state_derived": true,
                   "owner": OWNER.to_uppercase().replace("0X", "0x")}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            11,
            2,
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&registry),
            json!({"authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    // A grant before any release of the predecessor's lease does not move the lease.
    fixture
        .write(
            12,
            1,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&stray),
            json!({"namehash": namehash, "registrant": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            13,
            1,
            "RegistrationReleased",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&lease),
            json!({"namehash": namehash}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            13,
            2,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&successor),
            json!({"namehash": namehash, "registrant": OWNER, "authority_key": "registrar-key"}),
            REGISTRAR,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let owner = fixture
        .rows("project_binding_candidate")
        .await?
        .into_iter()
        .find(|row| row["registry_only"] == json!(true))
        .map(|row| columns(&row, &["bound_owner", "surface_bound_position"]));
    assert_eq!(
        owner,
        Some(
            json!({"bound_owner": OWNER, "surface_bound_position": {"block_number": 11,
                    "transaction_index": 0, "log_index": 1, "event_identity": "SurfaceBound:11:1"}})
        ),
        "the registry-only SurfaceBound keeps the owner the served control block reads"
    );
    let lease_of = |rows: &[Value]| {
        rows.iter()
            .find(|row| row["registry_only"] == json!(true))
            .map(|row| columns(row, &["lease_resource_id", "lease_position"]))
    };
    let rows = fixture.rows("project_binding_candidate").await?;
    assert_eq!(
        lease_of(&rows).map(|lease| lease["lease_resource_id"].clone()),
        Some(json!(lease)),
        "no release of the predecessor yet: the lease stands at the predecessor"
    );
    fixture.apply(13, FamilyMode::Normal).await;
    let rows = fixture.rows("project_binding_candidate").await?;
    assert_eq!(
        lease_of(&rows),
        Some(json!({"lease_resource_id": successor,
                    "lease_position": {"block_number": 13, "transaction_index": 0, "log_index": 2,
                                       "event_identity": "RegistrationGranted:13:2"}}))
    );
    let grants = fixture.rows("project_lifecycle_key_state").await?;
    let grant = grants
        .iter()
        .find(|row| row["resource_id"] == json!(successor))
        .expect("the successor lease has a state row");
    assert_eq!(grant["last_grant"]["authority_key"], json!("registrar-key"));
    fixture.assert_undo_restores(13).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

// The epoch can arrive after the successor grant: the binding opens, the predecessor's lease is
// released and a qualifying registrar grant follows, and only then does an AuthorityEpochChanged
// mark the binding registry-only. The served handoff reads the epoch without a position
// (stage.rs:128-135) and the grant after the binding and the release (stage.rs:84-127), so the
// converted candidate's lease is that earlier grant, not the predecessor.
#[tokio::test]
async fn an_epoch_after_the_successor_grant_takes_that_grant_as_the_lease() -> Result<()> {
    let fixture = Fixture::new("families_identity_late_epoch", 20).await?;
    let (lease, registry, successor, early) = (uuid(1), uuid(2), uuid(3), uuid(4));
    let namehash = node(1);
    fixture
        .binding(&uuid(101), &name(1), &lease, "ens_v1", 10, 1, Some(11))
        .await?;
    fixture
        .write(
            10,
            1,
            "SurfaceBound",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&lease),
            json!({"authority_kind": "registrar"}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            10,
            2,
            "RegistrationReleased",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&lease),
            json!({"namehash": namehash}),
            REGISTRAR,
        )
        .await?;
    // After the release but before the registry-only binding opens: never the lease.
    fixture
        .write(
            10,
            3,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&early),
            json!({"namehash": namehash, "registrant": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .binding(&uuid(102), &name(1), &registry, "ens_v1", 11, 1, None)
        .await?;
    fixture
        .write(
            11,
            1,
            "SurfaceBound",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&registry),
            json!({"authority_kind": "registry_only", "state_derived": true}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "RegistrationGranted",
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(&successor),
            json!({"namehash": namehash, "registrant": OWNER}),
            REGISTRAR,
        )
        .await?;
    fixture
        .write(
            13,
            1,
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&registry),
            json!({"authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = fixture.rows("project_binding_candidate").await?;
    assert!(
        rows.iter().all(|row| row["registry_only"] == json!(false)),
        "no epoch yet: no candidate is registry-only"
    );
    fixture.apply(13, FamilyMode::Normal).await;
    let rows = fixture.rows("project_binding_candidate").await?;
    let handoff = rows
        .iter()
        .find(|row| row["registry_only"] == json!(true))
        .map(|row| {
            columns(
                row,
                &[
                    "predecessor_resource_id",
                    "lease_resource_id",
                    "lease_position",
                ],
            )
        });
    assert_eq!(
        handoff,
        Some(
            json!({"predecessor_resource_id": lease, "lease_resource_id": successor,
                    "lease_position": {"block_number": 12, "transaction_index": 0,
                                       "log_index": 1,
                                       "event_identity": "RegistrationGranted:12:1"}})
        ),
        "the epoch converts the binding and the grant already retained becomes the lease"
    );
    fixture.assert_undo_restores(13).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

/// Opens an ens_v1 binding of name 1 on `resource` at `block`, log 1, with its SurfaceBound:
/// a registrar one for a lease, a registry one otherwise.
async fn bound(
    fixture: &Fixture,
    id: u32,
    resource: &str,
    block: i64,
    closed_at: Option<i64>,
) -> Result<()> {
    fixture
        .binding(&uuid(id), &name(1), resource, "ens_v1", block, 1, closed_at)
        .await?;
    let (family, kind, emitter) = if resource == uuid(2) {
        ("ens_v1_registry_l1", "registry_only", REGISTRY)
    } else {
        ("ens_v1_registrar_l1", "registrar", REGISTRAR)
    };
    fixture
        .write(
            block,
            1,
            "SurfaceBound",
            family,
            Some(&name(1)),
            Some(resource),
            json!({"authority_kind": kind}),
            emitter,
        )
        .await?;
    Ok(())
}

/// Writes a name-1 registrar event of `kind` on `resource` with the name's namehash.
async fn registrar_event(
    fixture: &Fixture,
    block: i64,
    log: i64,
    kind: &str,
    resource: &str,
    extra: Value,
) -> Result<()> {
    let mut after = json!({"namehash": node(1), "registrant": OWNER});
    if let (Value::Object(after), Value::Object(extra)) = (&mut after, extra) {
        after.extend(extra);
    }
    fixture
        .write(
            block,
            log,
            kind,
            "ens_v1_registrar_l1",
            Some(&name(1)),
            Some(resource),
            after,
            REGISTRAR,
        )
        .await?;
    Ok(())
}

async fn registry_only_epoch(
    fixture: &Fixture,
    block: i64,
    log: i64,
    registry: &str,
) -> Result<()> {
    fixture
        .write(
            block,
            log,
            "AuthorityEpochChanged",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(registry),
            json!({"authority_kind": "registry_only"}),
            REGISTRY,
        )
        .await?;
    Ok(())
}

/// The registry-only candidates' predecessor and lease, in binding order.
async fn handoffs(fixture: &Fixture) -> Result<Vec<Value>> {
    let mut rows: Vec<Value> = fixture
        .rows("project_binding_candidate")
        .await?
        .into_iter()
        .filter(|row| row["registry_only"] == json!(true))
        .collect();
    rows.sort_by_key(|row| row["block_number"].as_i64().unwrap_or_default());
    Ok(rows
        .iter()
        .map(|row| {
            json!({
                "predecessor": row["predecessor_resource_id"],
                "lease": row["lease_resource_id"],
                "lease_at": [row["lease_position"]["block_number"],
                             row["lease_position"]["log_index"]],
            })
        })
        .collect())
}

// A grant in the epoch's own block, before the epoch's log, is the lease: the served handoff
// reads the epoch at any position (stage.rs:128-135), and the lifecycle family runs after
// identity, so the grant meets the candidate already registry-only.
#[tokio::test]
async fn a_grant_in_the_epochs_own_block_becomes_the_lease() -> Result<()> {
    let fixture = Fixture::new("families_identity_epoch_block_grant", 20).await?;
    let (lease, registry, successor) = (uuid(1), uuid(2), uuid(3));
    bound(&fixture, 101, &lease, 10, Some(11)).await?;
    bound(&fixture, 102, &registry, 11, None).await?;
    registrar_event(&fixture, 12, 1, "RegistrationReleased", &lease, json!({})).await?;
    registrar_event(
        &fixture,
        13,
        1,
        "RegistrationGranted",
        &successor,
        json!({}),
    )
    .await?;
    registry_only_epoch(&fixture, 13, 2, &registry).await?;
    fixture.apply(13, FamilyMode::Normal).await;
    assert_eq!(
        handoffs(&fixture).await?,
        vec![json!({"predecessor": lease, "lease": successor, "lease_at": [13, 1]})]
    );
    fixture.assert_undo_restores(13).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

// The replay applies the served grant filter: a grant of another authority kind is not the
// lease (stage.rs:93-94), so the earlier registrar grant stays it.
#[tokio::test]
async fn the_grant_replay_skips_a_grant_of_another_authority_kind() -> Result<()> {
    let fixture = Fixture::new("families_identity_replay_kind", 20).await?;
    let (lease, registry, successor, other) = (uuid(1), uuid(2), uuid(3), uuid(4));
    bound(&fixture, 101, &lease, 10, Some(11)).await?;
    bound(&fixture, 102, &registry, 11, None).await?;
    registrar_event(&fixture, 12, 1, "RegistrationReleased", &lease, json!({})).await?;
    registrar_event(
        &fixture,
        12,
        2,
        "RegistrationGranted",
        &successor,
        json!({}),
    )
    .await?;
    registrar_event(
        &fixture,
        12,
        3,
        "RegistrationGranted",
        &other,
        json!({"authority_kind": "wrapper"}),
    )
    .await?;
    registry_only_epoch(&fixture, 13, 1, &registry).await?;
    fixture.apply(13, FamilyMode::Normal).await;
    assert_eq!(
        handoffs(&fixture).await?,
        vec![json!({"predecessor": lease, "lease": successor, "lease_at": [12, 2]})]
    );
    fixture.assert_undo_restores(13).await?;
    fixture.assert_rebuild_equal(13).await?;
    fixture.cleanup().await
}

// One epoch converts two earlier bindings of the same registry resource. Each takes its own
// predecessor and its own lease: the first a grant after its predecessor's release, the second,
// whose predecessor was never released, the predecessor.
#[tokio::test]
async fn one_epoch_converting_two_candidates_gives_each_its_own_lease() -> Result<()> {
    let fixture = Fixture::new("families_identity_two_conversions", 20).await?;
    let (lease, registry, successor, second_lease) = (uuid(1), uuid(2), uuid(3), uuid(4));
    bound(&fixture, 101, &lease, 10, Some(11)).await?;
    bound(&fixture, 102, &registry, 11, Some(14)).await?;
    registrar_event(&fixture, 12, 1, "RegistrationReleased", &lease, json!({})).await?;
    registrar_event(
        &fixture,
        12,
        2,
        "RegistrationGranted",
        &successor,
        json!({}),
    )
    .await?;
    bound(&fixture, 103, &second_lease, 14, Some(15)).await?;
    bound(&fixture, 104, &registry, 15, None).await?;
    registry_only_epoch(&fixture, 16, 1, &registry).await?;
    fixture.apply(16, FamilyMode::Normal).await;
    assert_eq!(
        handoffs(&fixture).await?,
        vec![
            json!({"predecessor": lease, "lease": successor, "lease_at": [12, 2]}),
            json!({"predecessor": second_lease, "lease": second_lease, "lease_at": [14, 1]}),
        ]
    );
    fixture.assert_undo_restores(16).await?;
    fixture.assert_rebuild_equal(16).await?;
    fixture.cleanup().await
}

// A binding whose block carries no activated event (its SurfaceBound dropped by the adapter's
// reconcile, the F1 pairing precondition failing) still gets its candidate row on the normal
// path, block by block, and a rebuild visits its block too, so both keep the row.
#[tokio::test]
async fn a_binding_in_a_block_without_events_survives_a_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_identity_binding_only_block", 20).await?;
    let (lease, registry) = (uuid(1), uuid(2));
    bound(&fixture, 101, &lease, 10, Some(11)).await?;
    fixture
        .binding(&uuid(102), &name(1), &registry, "ens_v1", 11, 1, None)
        .await?;
    registrar_event(&fixture, 12, 1, "RegistrationReleased", &lease, json!({})).await?;
    fixture.apply(10, FamilyMode::Normal).await;
    fixture.apply(11, FamilyMode::Normal).await;
    fixture.apply(12, FamilyMode::Normal).await;
    let rows = fixture.rows("project_binding_candidate").await?;
    let unpaired = rows
        .iter()
        .find(|row| row["surface_binding_id"] == json!(uuid(102)))
        .map(|row| columns(row, &["block_number", "event_identity"]));
    assert_eq!(
        unpaired,
        Some(json!({"block_number": 11, "event_identity": format!("binding:{}", uuid(102))})),
        "the normal path keeps the unpaired binding at its own position"
    );
    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}
