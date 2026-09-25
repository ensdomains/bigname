//! F10 and F11 through the family loop: a name's alias and a resolver's aliases with an inactive
//! alias kept, ENSv1 child edges keyed by the child, the parent node and the registry, and an
//! ENSv2 parent's subregistry with its clear. Each case undoes its last block byte for byte and
//! equals a rebuild.
mod families_support;

use anyhow::Result;
use bigname_project::families::FamilyMode;
use families_support::Fixture;
use serde_json::{Value, json};

const RESOLVER: &str = "0x00000000000000000000000000000000000000c3";
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
async fn aliases_keep_the_latest_per_name_and_per_resolver_inactive_included() -> Result<()> {
    let fixture = Fixture::new("families_aliases", 20).await?;
    let alias = |active: bool| {
        json!({"resolver": RESOLVER, "active": active,
               "alias_state": if active { "active" } else { "removed" },
               "from_dns_encoded_name": "0x0161", "to_dns_encoded_name": "0x0162",
               "to_logical_name_id": name(2), "to_name": "b.eth"})
    };
    fixture
        .write(
            10,
            1,
            "AliasChanged",
            "ens_v2_resolver_l1",
            Some(&name(1)),
            None,
            alias(true),
            RESOLVER,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "AliasChanged",
            "ens_v2_resolver_l1",
            Some(&name(1)),
            None,
            alias(false),
            RESOLVER,
        )
        .await?;
    fixture.apply(11, FamilyMode::Normal).await;
    let names = fixture.rows("project_name_alias").await?;
    assert_eq!(
        names
            .iter()
            .map(|row| columns(
                row,
                &[
                    "logical_name_id",
                    "active",
                    "alias_state",
                    "to_logical_name_id",
                    "resolver_address"
                ]
            ))
            .collect::<Vec<_>>(),
        vec![
            json!({"logical_name_id": name(1), "active": false, "alias_state": "removed",
                    "to_logical_name_id": name(2), "resolver_address": RESOLVER})
        ]
    );
    let resolvers = fixture.rows("project_resolver_alias").await?;
    assert_eq!(
        resolvers
            .iter()
            .map(|row| columns(
                row,
                &[
                    "resolver_address",
                    "alias_identity",
                    "active",
                    "block_number"
                ]
            ))
            .collect::<Vec<_>>(),
        vec![
            json!({"resolver_address": RESOLVER, "alias_identity": name(1), "active": false, "block_number": 11})
        ]
    );
    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
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
    fixture.apply(11, FamilyMode::Normal).await;
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

#[tokio::test]
async fn an_alias_without_an_active_flag_is_stored_active() -> Result<()> {
    let fixture = Fixture::new("families_alias_default", 20).await?;
    fixture
        .write(
            10,
            1,
            "AliasChanged",
            "ens_v2_resolver_l1",
            Some(&name(3)),
            None,
            json!({"resolver": RESOLVER, "to_logical_name_id": name(2)}),
            RESOLVER,
        )
        .await?;
    let outcome = fixture.apply(10, FamilyMode::Normal).await;
    assert_eq!(outcome.skipped, None);
    for table in ["project_name_alias", "project_resolver_alias"] {
        let rows = fixture.rows(table).await?;
        assert_eq!(rows.len(), 1, "{table}");
        assert_eq!(rows[0]["active"], json!(true), "{table}");
    }
    fixture.cleanup().await
}
