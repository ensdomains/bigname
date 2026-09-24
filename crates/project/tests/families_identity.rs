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
