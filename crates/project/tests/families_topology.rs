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
    fixture.apply(10, FamilyMode::Normal).await;
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
    fixture.apply(11, FamilyMode::Normal).await;
    let updated = json!({"owner": "0x00000000000000000000000000000000000000cc",
                         "owner_getter": "0x00000000000000000000000000000000000000dd",
                         "block_number": 11});
    assert_eq!(owners().await?, updated);
    fixture.assert_undo_restores(11).await?;
    assert_eq!(owners().await?, updated, "the replay lower-cases again");
    fixture.assert_rebuild_equal(11).await?;
    fixture.cleanup().await
}

// Both alias readers take `(after_state ->> 'active')::boolean`, so every spelling PostgreSQL
// reads as a boolean decides the flag: case-insensitive and trimmed, unique prefixes of true,
// false, yes, no, on and off, and 1 or 0, as text or as a JSON number.
#[tokio::test]
async fn an_alias_active_flag_reads_as_postgresql_reads_a_boolean() -> Result<()> {
    let fixture = Fixture::new("families_alias_boolean", 20).await?;
    let spellings = [
        (json!("off"), false),
        (json!(" No "), false),
        (json!("0"), false),
        (json!(0), false),
        (json!("fal"), false),
        (json!("n"), false),
        (json!("ON"), true),
        (json!("1"), true),
        (json!(1), true),
        (json!("ye"), true),
    ];
    for (n, (active, _)) in (1..).zip(&spellings) {
        fixture
            .write(
                10,
                n,
                "AliasChanged",
                "ens_v2_resolver_l1",
                Some(&name(u64::try_from(100 + n)?)),
                None,
                json!({"resolver": RESOLVER, "to_logical_name_id": name(2), "active": active}),
                RESOLVER,
            )
            .await?;
    }
    fixture.apply(10, FamilyMode::Normal).await;
    let rows = fixture.rows("project_name_alias").await?;
    let mut stored: Vec<(String, Option<bool>)> = rows
        .iter()
        .map(|row| (row["logical_name_id"].to_string(), row["active"].as_bool()))
        .collect();
    stored.sort();
    let mut expected: Vec<(String, Option<bool>)> = (1..)
        .zip(&spellings)
        .map(|(n, (_, flag))| (json!(name(100 + n)).to_string(), Some(*flag)))
        .collect();
    expected.sort();
    assert_eq!(stored, expected);
    fixture.cleanup().await
}

// A differential check against PostgreSQL itself: every value goes through the served readers'
// cast, `COALESCE((after_state ->> 'active')::boolean, true)`, and through the family reducer.
// Where the cast accepts the value, both alias tables hold its result; where it rejects it (the
// served batch would fail), both hold the documented active fallback.
#[tokio::test]
async fn an_alias_active_flag_matches_the_served_boolean_cast() -> Result<()> {
    let fixture = Fixture::new("families_alias_boolean_cast", 20).await?;
    let mut values: Vec<Value> = [
        "t",
        "tr",
        "tru",
        "true",
        "f",
        "fa",
        "fal",
        "fals",
        "false",
        "y",
        "ye",
        "yes",
        "n",
        "no",
        "on",
        "of",
        "off",
        "1",
        "0",
        "TrUe",
        "FALSE",
        "Off",
        "yEs",
        "o",
        "",
        "   ",
        "junk",
        "truex",
        "2",
        "01",
        "\u{a0}off\u{a0}",
        "\u{2003}on",
        "off\u{3000}",
        "\u{85}no",
    ]
    .into_iter()
    .map(|text| json!(text))
    .collect();
    for space in [' ', '\t', '\n', '\r', '\u{b}', '\u{c}'] {
        values.push(json!(format!("{space}off{space}")));
        values.push(json!(format!("{space}{space}yes")));
    }
    values.extend([
        json!(true),
        json!(false),
        Value::Null,
        json!(0),
        json!(1),
        json!(2),
        json!(1.0),
    ]);
    let mut expected = Vec::new();
    for (n, value) in (1..).zip(&values) {
        let cast: std::result::Result<Option<bool>, sqlx::Error> = sqlx::query_scalar(
            "SELECT COALESCE((jsonb_build_object('active', $1::jsonb) ->> 'active')::boolean, true)",
        )
        .bind(value)
        .fetch_one(&fixture.pool)
        .await;
        let flag = match cast {
            Ok(flag) => flag.expect("COALESCE is never null"),
            Err(sqlx::Error::Database(error)) => {
                assert_eq!(error.code().as_deref(), Some("22P02"), "{value:?}: {error}");
                true
            }
            Err(error) => return Err(error.into()),
        };
        let logical = name(u64::try_from(200 + n)?);
        fixture
            .write(
                10,
                n,
                "AliasChanged",
                "ens_v2_resolver_l1",
                Some(&logical),
                None,
                json!({"resolver": RESOLVER, "to_logical_name_id": name(2), "active": value}),
                RESOLVER,
            )
            .await?;
        expected.push((value.clone(), logical, flag));
    }
    fixture.apply(10, FamilyMode::Normal).await;
    for table in ["project_name_alias", "project_resolver_alias"] {
        let rows = fixture.rows(table).await?;
        assert_eq!(rows.len(), expected.len(), "{table}");
        for (value, logical, flag) in &expected {
            let row = rows
                .iter()
                .find(|row| {
                    row["logical_name_id"] == json!(logical)
                        || row["alias_identity"] == json!(logical)
                })
                .unwrap_or_else(|| panic!("{table}: no row for {value:?}"));
            assert_eq!(row["active"], json!(flag), "{table}: {value:?}");
        }
    }
    fixture.cleanup().await
}
