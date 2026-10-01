//! F11 through the family loop: ENSv1 child edges keyed by the child, the parent node and the
//! registry, and an ENSv2 parent's subregistry with its clear. Each case undoes its last block
//! byte for byte and equals a rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::Fixture;
use serde_json::{Value, json};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
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
async fn child_edges_and_the_v2_subregistry_keep_their_latest_clears_included() -> Result<()> {
    let fixture = Fixture::new("families_child_edges", 20).await?;
    let edge = |parent: u64, owner: &str| {
        json!({"source_event": "NewOwner", "node": node(parent), "child_node": node(9),
               "labelhash": node(99), "owner": owner})
    };
    // The same child under two parents keeps two candidates; the latest under parent 1 wins.
    fixture
        .write(
            10,
            1,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            None,
            None,
            edge(1, OWNER),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            2,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            None,
            None,
            edge(2, OWNER),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            None,
            None,
            edge(1, "0x0000000000000000000000000000000000000000"),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            10,
            3,
            "SubregistryChanged",
            "ens_v2_registry_l1",
            Some(&name(5)),
            None,
            json!({"subregistry": REGISTRY}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            11,
            2,
            "SubregistryChanged",
            "ens_v2_registry_l1",
            Some(&name(5)),
            None,
            json!({"subregistry": null}),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    let edges = fixture.rows("project_child_edge_candidate").await?;
    assert_eq!(
        edges
            .iter()
            .map(|row| columns(
                row,
                &[
                    "parent_node",
                    "child_node",
                    "authority_arm",
                    "owner",
                    "block_number"
                ]
            ))
            .collect::<Vec<_>>(),
        vec![
            json!({"parent_node": node(1), "child_node": node(9), "authority_arm": "ens_v1",
                   "owner": "0x0000000000000000000000000000000000000000", "block_number": 11}),
            json!({"parent_node": node(2), "child_node": node(9), "authority_arm": "ens_v1",
                   "owner": OWNER, "block_number": 10}),
        ]
    );
    let parents = fixture.rows("project_parent_subregistry").await?;
    assert_eq!(
        parents
            .iter()
            .map(|row| columns(
                row,
                &["logical_name_id", "subregistry_address", "block_number"]
            ))
            .collect::<Vec<_>>(),
        vec![json!({"logical_name_id": name(5), "subregistry_address": "", "block_number": 11})],
        "the clear stays a row"
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// The served children build lower-cases a child edge's owner (children.rs, `lower(COALESCE(
// owner_getter, owner))`), and the family columns are documented lower-cased, so a checksummed
// payload is stored lower-cased.
#[tokio::test]
async fn a_child_edge_stores_its_owner_and_owner_getter_lower_cased() -> Result<()> {
    let fixture = Fixture::new("families_child_edge_case", 20).await?;
    fixture
        .write(
            10,
            1,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"source_event": "NewOwner", "node": node(1), "child_node": node(9),
                   "labelhash": node(99),
                   "owner": "0x00000000000000000000000000000000000000Aa",
                   "owner_getter": "0x00000000000000000000000000000000000000Bb"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(10, FamilyMode::Normal).await?;
    let owners = || async {
        let edges = fixture.rows("project_child_edge_candidate").await?;
        anyhow::ensure!(edges.len() == 1, "one edge key: {edges:?}");
        anyhow::Ok(columns(
            &edges[0],
            &["owner", "owner_getter", "block_number"],
        ))
    };
    assert_eq!(
        owners().await?,
        json!({"owner": "0x00000000000000000000000000000000000000aa",
               "owner_getter": "0x00000000000000000000000000000000000000bb",
               "block_number": 10})
    );
    fixture.assert_undo_restores(10).await?;
    // The same key again with other mixed-case owners: the update lands lower-cased, its undo
    // restores block 10's row (checked inside assert_undo_restores), and the replay lower-cases
    // again.
    fixture
        .write(
            11,
            1,
            "SubregistryChanged",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"source_event": "NewOwner", "node": node(1), "child_node": node(9),
                   "labelhash": node(99),
                   "owner": "0x00000000000000000000000000000000000000cC",
                   "owner_getter": "0x00000000000000000000000000000000000000Dd"}),
            REGISTRY,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await?;
    let updated = json!({"owner": "0x00000000000000000000000000000000000000cc",
                         "owner_getter": "0x00000000000000000000000000000000000000dd",
                         "block_number": 11});
    assert_eq!(owners().await?, updated);
    fixture.assert_undo_restores(11).await?;
    assert_eq!(owners().await?, updated, "the replay lower-cases again");
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}
