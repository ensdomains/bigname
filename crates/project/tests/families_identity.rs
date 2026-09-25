//! F1 and F2c through the family loop: name migration and authority epoch starts, binding
//! candidates with their SurfaceBound facts and registry-only predecessor, registry node
//! ownership with the old-registry fold, and registry-binding observations. Each case undoes its
//! last block byte for byte and equals a rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::{Fixture, uuid};
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
