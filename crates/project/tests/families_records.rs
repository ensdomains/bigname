//! F4 to F7 through the family loop: registry-node and resource pointers with their clears,
//! node-keyed records by admission arm with the coin-60 pair, record-id values and links with
//! the `0` clear. Every case undoes its last block byte for byte and equals a rebuild.
mod families_support;

use anyhow::Result;
use families_support::{Fixture, uuid};
use serde_json::{Value, json};

const REGISTRY: &str = "0x00000000000000000000000000000000000000e1";
const R1: &str = "0x00000000000000000000000000000000000000a1";
const ZERO: &str = "0x0000000000000000000000000000000000000000";

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
async fn pointers_keep_the_latest_resolver_per_node_and_resource_clears_included() -> Result<()> {
    let fixture = Fixture::new("families_records_pointers", 20).await?;
    let resource = uuid(1);
    fixture
        .write(
            10,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&resource),
            json!({"node": node(1), "resolver": R1}),
            REGISTRY,
        )
        .await?;
    // An unnamed pointer on another node owns its F4 key and writes no F5 row.
    fixture
        .write(
            10,
            2,
            "ResolverChanged",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"node": node(2), "resolver": R1}),
            REGISTRY,
        )
        .await?;
    fixture
        .write(
            12,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            Some(&name(1)),
            Some(&resource),
            json!({"node": node(1), "resolver": ZERO}),
            REGISTRY,
        )
        .await?;
    // A pointer without a resolver is a clear too.
    fixture
        .write(
            11,
            1,
            "ResolverChanged",
            "ens_v1_registry_l1",
            None,
            None,
            json!({"node": node(2), "resolver": null}),
            REGISTRY,
        )
        .await?;
    fixture
        .apply(12, bigname_project::families::FamilyMode::Normal)
        .await;

    let registry = fixture.rows("project_registry_pointer").await?;
    assert_eq!(
        registry
            .iter()
            .map(|row| columns(row, &["node", "resolver_address", "block_number"]))
            .collect::<Vec<_>>(),
        vec![
            json!({"node": node(1), "resolver_address": ZERO, "block_number": 12}),
            json!({"node": node(2), "resolver_address": "", "block_number": 11}),
        ],
        "the clear stays a row"
    );
    let resources = fixture.rows("project_resource_pointer").await?;
    assert_eq!(resources.len(), 1);
    assert_eq!(
        columns(
            &resources[0],
            &[
                "resolver_address",
                "nonzero_resolver_address",
                "boundary_kind",
                "namehash"
            ]
        ),
        json!({"resolver_address": ZERO, "nonzero_resolver_address": R1,
               "boundary_kind": "ResolverChanged", "namehash": node(1)}),
        "the current group takes the clear; the non-zero group keeps R1"
    );
    assert_eq!(resources[0]["nonzero_position"]["block_number"], json!(10));
    assert_eq!(
        resources[0]["boundary_block_timestamp"],
        json!("2027-01-15T08:02:24+00:00")
    );

    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn node_records_keep_a_partition_per_arm_and_the_coin_60_pair() -> Result<()> {
    let fixture = Fixture::new("families_records_nodes", 20).await?;
    let record = |key: &str, family: &str, selector: Value, value: Value, source: &str| {
        json!({"node": node(1), "record_key": key, "record_family": family,
               "selector_key": selector, "value": value, "source_event": source})
    };
    // A named write, an unnamed ENSv1 write and an unnamed ENSv2 write at the same node.
    fixture
        .write(
            10,
            1,
            "RecordChanged",
            "ens_v1_resolver_l1",
            Some(&name(1)),
            None,
            record(
                "text:avatar",
                "text",
                json!("avatar"),
                json!("a"),
                "TextChanged",
            ),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            2,
            "RecordChanged",
            "ens_v1_resolver_l1",
            None,
            None,
            record("text:url", "text", json!("url"), json!("u"), "TextChanged"),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            3,
            "RecordChanged",
            "ens_v2_resolver_l1",
            None,
            None,
            record("text:url", "text", json!("url"), json!("v2"), "TextUpdated"),
            R1,
        )
        .await?;
    // setAddr(node, 60, bytes): AddressChanged then its AddrChanged sibling one log later.
    fixture
        .write(
            11,
            4,
            "RecordChanged",
            "ens_v1_resolver_l1",
            None,
            None,
            record(
                "addr:60",
                "addr",
                json!("60"),
                json!({"bytes": "0xbb"}),
                "AddressChanged",
            ),
            R1,
        )
        .await?;
    fixture
        .write(
            11,
            5,
            "RecordChanged",
            "ens_v1_resolver_l1",
            None,
            None,
            record("addr:60", "addr", json!("60"), json!("0xbb"), "AddrChanged"),
            R1,
        )
        .await?;
    // A version change and a cleared contenthash.
    fixture
        .write(
            12,
            1,
            "RecordVersionChanged",
            "ens_v1_resolver_l1",
            None,
            None,
            json!({"node": node(1), "record_version": 2}),
            R1,
        )
        .await?;
    fixture
        .write(
            12,
            2,
            "RecordChanged",
            "ens_v1_resolver_l1",
            None,
            None,
            json!({"node": node(1), "record_key": "contenthash", "record_family": "contenthash",
                   "contenthash_hex": "0x", "source_event": "ContenthashChanged"}),
            R1,
        )
        .await?;
    fixture
        .apply(12, bigname_project::families::FamilyMode::Normal)
        .await;

    let partitions = fixture.rows("project_node_record_partition").await?;
    let arms: Vec<Value> = partitions
        .iter()
        .map(|row| columns(row, &["arm", "arm_identity"]))
        .collect();
    assert_eq!(arms.len(), 3, "{arms:?}");
    assert!(arms.contains(&json!({"arm": "named", "arm_identity": name(1)})));
    assert!(arms.contains(
        &json!({"arm": "native", "arm_identity": format!("{}|ens_v1_resolver_l1", node(1))})
    ));
    assert!(arms.contains(
        &json!({"arm": "guarded", "arm_identity": format!("{}|ens_v2_resolver_l1|ens|", node(1))})
    ));
    let native = partitions
        .iter()
        .find(|row| row["arm"] == "native")
        .expect("native");
    assert_eq!(native["version_position"]["block_number"], json!(12));

    let values = fixture.rows("project_node_record_value").await?;
    let addr = values
        .iter()
        .find(|row| row["record_key"] == "addr:60")
        .expect("addr:60");
    assert_eq!(
        columns(addr, &["source_event", "value", "sibling_value", "status"]),
        json!({"source_event": "AddrChanged", "value": "0xbb",
               "sibling_value": {"bytes": "0xbb"}, "status": "success"})
    );
    assert_eq!(addr["sibling_position"]["log_index"], json!(4));
    let cleared = values
        .iter()
        .find(|row| row["record_key"] == "contenthash")
        .expect("contenthash");
    assert_eq!(
        cleared["status"],
        json!("not_found"),
        "the clear stays a row"
    );
    assert_eq!(values.len(), 5);

    fixture.assert_undo_restores(12).await?;
    fixture.assert_rebuild_equal(12).await?;
    fixture.cleanup().await
}

#[tokio::test]
async fn record_id_values_and_links_keep_the_latest_with_the_zero_clear() -> Result<()> {
    let fixture = Fixture::new("families_records_links", 20).await?;
    let linked = |record_id: &str| {
        json!({"node": node(1), "resolver": R1, "resolver_record_id": record_id,
               "storage_model": "resolver_record_id", "source_event": "Linked"})
    };
    fixture
        .write(
            10,
            1,
            "ResolverRecordLinked",
            "ens_v2_resolver_l1",
            None,
            None,
            linked("7"),
            R1,
        )
        .await?;
    fixture
        .write(
            10,
            2,
            "RecordChanged",
            "ens_v2_resolver_l1",
            None,
            None,
            json!({"resolver": R1, "resolver_record_id": "7", "storage_model": "resolver_record_id",
                   "record_key": "text:avatar", "record_family": "text", "selector_key": "avatar",
                   "value": "a", "source_event": "TextUpdated"}),
            R1,
        )
        .await?;
    fixture
        .write(
            11,
            1,
            "ResolverRecordLinked",
            "ens_v2_resolver_l1",
            None,
            None,
            linked("0"),
            R1,
        )
        .await?;
    fixture
        .apply(11, bigname_project::families::FamilyMode::Normal)
        .await;

    let links = fixture.rows("project_resolver_link").await?;
    assert_eq!(
        links
            .iter()
            .map(|row| columns(row, &["node", "record_id", "block_number"]))
            .collect::<Vec<_>>(),
        vec![json!({"node": node(1), "record_id": "0", "block_number": 11})],
        "the zero link stays a row"
    );
    let values = fixture.rows("project_record_id_value").await?;
    assert_eq!(
        values
            .iter()
            .map(|row| columns(row, &["record_id", "record_key", "value", "status"]))
            .collect::<Vec<_>>(),
        vec![
            json!({"record_id": "7", "record_key": "text:avatar", "value": "a", "status": "success"})
        ]
    );
    assert!(
        fixture.rows("project_node_record_value").await?.is_empty(),
        "record-id writes are not node-keyed"
    );

    fixture.assert_undo_restores(11).await?;
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}
