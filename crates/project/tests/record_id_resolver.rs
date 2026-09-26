#[path = "support/bounded_attribution.rs"]
mod bounded_attribution;
#[path = "support/family_shadow.rs"]
mod family_shadow;

use anyhow::Result;
use bigname_project::{BatchRequest, Engine, Marker, RunMode};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use family_shadow::{Expectations, ExpectedDifference};
use serde_json::{Value, json};
use sqlx::{PgPool, raw_sql};

const CHAIN: &str = "ethereum-sepolia";
const REGISTRY: &str = "0x4444444444444444444444444444444444444444";
const RESOLVER: &str = "0x1111111111111111111111111111111111111111";
const IMPLEMENTATION: &str = "0x2222222222222222222222222222222222222222";
fn hash(n: i64) -> String {
    format!("0x{n:064x}")
}
fn resource(n: i64) -> String {
    format!("66000000-0000-0000-0000-{n:012}")
}
fn node(n: i64) -> String {
    bigname_lookup::ens_namehash_hex(&format!("record{n}.eth")).unwrap()
}

// Exercise canonical normalized link/value facts through the production Project engine.
#[tokio::test]
async fn shared_records_relink_default_and_empty_values_survive_incremental_and_redo() -> Result<()>
{
    let (db, pool) = database("record_id_links").await?;
    seed(&pool).await?;
    run(&pool, 12, None, RunMode::Normal).await?;
    assert_text(&pool, 1, "one").await?;
    assert_text(&pool, 2, "one").await?;
    assert_text(&pool, 3, "default").await?;
    run(&pool, 13, Some(12), RunMode::Normal).await?;
    assert_text(&pool, 1, "shared").await?;
    assert_text(&pool, 2, "shared").await?;
    run(&pool, 14, Some(13), RunMode::Normal).await?;
    assert_text(&pool, 2, "default").await?;
    let relink = inventory(&pool, 2).await?;
    assert_eq!(relink["boundary"]["event_kind"], "ResolverRecordLinked");
    assert_eq!(relink["last_change"]["chain_position"]["block_number"], 14);
    run(&pool, 15, Some(14), RunMode::Normal).await?;
    assert_text(&pool, 1, "default").await?;
    run(&pool, 16, Some(15), RunMode::Normal).await?;
    assert_text(&pool, 1, "new default").await?;
    assert_text(&pool, 3, "new default").await?;
    assert_text(&pool, 2, "default").await?;
    run(&pool, 17, Some(16), RunMode::Normal).await?;
    assert_text(&pool, 2, "").await?;
    run(&pool, 18, Some(17), RunMode::Normal).await?;
    let empty = inventory(&pool, 2).await?;
    for key in ["addr:60", "contenthash"] {
        let item = empty["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["record_key"] == key)
            .unwrap();
        assert_eq!(item["status"], "not_found");
        assert!(item.get("value").is_none());
    }
    let incremental = snapshot(&pool).await?;
    run(&pool, 18, Some(17), RunMode::Normal).await?;
    assert_eq!(snapshot(&pool).await?, incremental, "repeat changed result");
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(snapshot(&pool).await?, incremental, "full rebuild drift");
    run(&pool, 18, Some(17), RunMode::Redo).await?;
    assert_eq!(snapshot(&pool).await?, incremental, "redo drift");
    let nameless: i64 = sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE event_kind = 'RecordChanged' AND (logical_name_id IS NOT NULL OR resource_id IS NOT NULL)").fetch_one(&pool).await?;
    assert_eq!(nameless, 0, "Project mutated immutable record attribution");
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn deleted_link_and_default_changes_rebuild_every_consumer() -> Result<()> {
    let (db, pool) = database("record_id_retracted").await?;
    seed(&pool).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    // Interpret redo can remove a normalized event; retained projection citations must scope it.
    sqlx::query(
        "DELETE FROM normalized_events WHERE event_identity IN ('link-b-two','default-three')",
    )
    .execute(&pool)
    .await?;
    run(&pool, 18, Some(18), RunMode::Redo).await?;
    assert_text(&pool, 2, "shared").await?;
    assert_text(&pool, 1, "").await?;
    assert_text(&pool, 3, "").await?;
    let replay = snapshot(&pool).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        snapshot(&pool).await?,
        replay,
        "retraction differs from clean rebuild"
    );
    db.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn record_history_survives_relinks_and_excludes_later_unselected_writes() -> Result<()> {
    let (db, pool) = database("record_id_history").await?;
    seed(&pool).await?;
    text(&pool, "unselected-later", 16, 4, 1, "unrelated").await?;
    run(&pool, 13, None, RunMode::Normal).await?;
    assert_history(&pool, 2, &["record-one", "shared-update"], &[]).await?;
    run(&pool, 14, Some(13), RunMode::Normal).await?;
    assert_history(
        &pool,
        2,
        &["record-one", "shared-update", "record-two"],
        &[],
    )
    .await?;
    run(&pool, 18, Some(14), RunMode::Normal).await?;
    let expected = ["record-one", "shared-update", "record-two", "empty-text"];
    assert_history(&pool, 2, &expected, &["unselected-later", "record-three"]).await?;
    assert_text(&pool, 2, "").await?;
    assert_history(
        &pool,
        1,
        &["record-one", "shared-update", "record-two", "record-three"],
        &["unselected-later", "empty-text"],
    )
    .await?;
    assert_history(
        &pool,
        3,
        &["record-two", "record-three"],
        &["record-one", "empty-text"],
    )
    .await?;
    let incremental = snapshot(&pool).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        snapshot(&pool).await?,
        incremental,
        "history full rebuild drift"
    );
    run(&pool, 18, Some(18), RunMode::Redo).await?;
    assert_eq!(snapshot(&pool).await?, incremental, "history redo drift");
    sqlx::query("DELETE FROM normalized_events WHERE event_identity = 'link-b-two'")
        .execute(&pool)
        .await?;
    run(&pool, 18, Some(18), RunMode::Redo).await?;
    assert_history(
        &pool,
        2,
        &["record-one", "shared-update", "unselected-later"],
        &["record-two", "empty-text"],
    )
    .await?;
    let replay = snapshot(&pool).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        snapshot(&pool).await?,
        replay,
        "history retraction rebuild drift"
    );
    event(
        &pool,
        "clear-pointer",
        18,
        5,
        "ResolverChanged",
        Some(1),
        json!({"resolver":"0x0000000000000000000000000000000000000000"}),
    )
    .await?;
    text(&pool, "after-clear", 18, 6, 3, "unrelated").await?;
    run(&pool, 18, Some(18), RunMode::Normal).await?;
    assert_history(
        &pool,
        1,
        &["record-one", "shared-update", "record-two", "record-three"],
        &["after-clear", "unselected-later", "empty-text"],
    )
    .await?;
    assert_eq!(inventory(&pool, 1).await?["support"], "unsupported");
    let cleared = snapshot(&pool).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        snapshot(&pool).await?,
        cleared,
        "cleared history rebuild drift"
    );
    db.cleanup().await?;
    Ok(())
}

// The combined version boundary as the family readers see it (TYR-36 step 4): a link after a
// version keeps a value written before both, a version after a link cuts it off, and a later
// write is served again. Every run also compares the family reads with today's reads.
#[tokio::test]
async fn link_and_version_boundaries_read_the_same_through_the_families() -> Result<()> {
    let (db, pool) = database("record_id_boundaries").await?;
    seed(&pool).await?;
    for n in 19..=23 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')").bind(CHAIN).bind(hash(n)).bind(n).execute(&pool).await?;
    }
    let version = |n: i64| json!({"source_event":"VersionChanged","node":node(n),"resolver":RESOLVER,"record_version":"1"});
    text(&pool, "record-four", 19, 0, 4, "four").await?;
    text(&pool, "record-five-early", 19, 1, 5, "five early").await?;
    event(
        &pool,
        "version-one",
        20,
        0,
        "RecordVersionChanged",
        Some(1),
        version(1),
    )
    .await?;
    link(&pool, "link-c-five", 20, 1, Some(3), 5).await?;
    link(&pool, "link-a-four", 21, 0, Some(1), 4).await?;
    event(
        &pool,
        "version-three",
        21,
        1,
        "RecordVersionChanged",
        Some(3),
        version(3),
    )
    .await?;
    run(&pool, 21, None, RunMode::Normal).await?;
    assert_text(&pool, 1, "four").await?;
    let cut = inventory(&pool, 3).await?;
    assert_eq!(
        cut["boundary"]["event_kind"], "RecordVersionChanged",
        "{cut}"
    );
    assert!(
        cut["entries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["record_key"] != "text:url"),
        "{cut}"
    );
    text(&pool, "record-five-late", 22, 0, 5, "five late").await?;
    run(&pool, 22, Some(21), RunMode::Normal).await?;
    assert_text(&pool, 3, "five late").await?;

    // Two synthesised writes of one record key in one block have no transaction or log position.
    // Today's reader breaks the tie by the generated event id, so the later insert (`tie-a`)
    // wins; the canonical order breaks it by event identity as bytes, so `tie-b` wins. This is
    // the disclosed canonical-order change, asserted as the answer.
    text(&pool, "tie-b", 23, 0, 4, "b").await?;
    text(&pool, "tie-a", 23, 1, 4, "a").await?;
    sqlx::query("UPDATE normalized_events SET transaction_hash = NULL, transaction_index = NULL, log_index = NULL WHERE event_identity IN ('tie-a', 'tie-b')").execute(&pool).await?;
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 23,
            affected_from_block: 23,
            affected_to_block: 23,
            resume_current: Some(Marker {
                number: 22,
                hash: hash(22),
            }),
            mode: RunMode::Normal,
        })
        .await?;
    assert_text(&pool, 1, "a").await?;
    let id = |identity: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT normalized_event_id FROM normalized_events WHERE event_identity = $1",
            )
            .bind(identity)
            .fetch_one(&pool)
            .await
        }
    };
    let (tie_a, tie_b) = (id("tie-a").await?, id("tie-b").await?);
    let today_ids: Value = sqlx::query_scalar(
        "SELECT provenance -> 'record_event_ids' FROM record_inventory_current
         WHERE resource_id = $1::uuid",
    )
    .bind(resource(1))
    .fetch_one(&pool)
    .await?;
    let family_ids = Value::Array(
        today_ids
            .as_array()
            .unwrap()
            .iter()
            .map(|id| {
                if *id == json!(tie_a) {
                    json!(tie_b)
                } else {
                    id.clone()
                }
            })
            .collect(),
    );
    let expected = Expectations {
        differences: vec![ExpectedDifference {
            target: 23,
            key: format!("record_inventory {}", resource(1)),
            fields: vec![
                (
                    "entries[text:url].value".into(),
                    Some(json!("a")),
                    Some(json!("b")),
                ),
                (
                    "last_change.normalized_event_id".into(),
                    Some(json!(tie_a)),
                    Some(json!(tie_b)),
                ),
                (
                    "provenance.record_event_ids".into(),
                    Some(today_ids),
                    Some(family_ids),
                ),
            ],
            times: 1,
        }],
        ..Expectations::none()
    };
    family_shadow::compare_family_reads_at(&pool, &outcome.current, &expected).await?;
    expected.finish()?;

    // Mutations on the accepted result must fail: another value for an expected field, named with
    // the mutated value, and a further field that differs.
    for (mutation, field, value) in [
        (
            "UPDATE record_inventory_current SET entries = (
             SELECT jsonb_agg(CASE WHEN entry ->> 'record_key' = 'text:url'
                                   THEN jsonb_set(entry, '{value}', '\"c\"') ELSE entry END)
             FROM jsonb_array_elements(entries) entry)
         WHERE resource_id = $1::uuid",
            "entries[text:url].value",
            Some("\"c\""),
        ),
        (
            "UPDATE record_inventory_current
         SET chain_positions = chain_positions || '{\"mutated\": true}'
         WHERE resource_id = $1::uuid",
            "chain_positions.mutated",
            None,
        ),
    ] {
        let (entries, positions): (Value, Value) = sqlx::query_as(
            "SELECT entries, chain_positions FROM record_inventory_current
             WHERE resource_id = $1::uuid",
        )
        .bind(resource(1))
        .fetch_one(&pool)
        .await?;
        sqlx::query(mutation)
            .bind(resource(1))
            .execute(&pool)
            .await?;
        let report = bigname_storage::families::records::compare_family_reads(
            &pool,
            CHAIN,
            Some((23, hash(23))),
            1,
        )
        .await?;
        let error = expected.check(23, &report).expect_err(mutation).to_string();
        assert!(
            error.starts_with(&format!(
                "the difference at 23 on record_inventory {} is not the expected one",
                resource(1)
            )) && error.contains(field)
                && value.is_none_or(|value| error.contains(value)),
            "{mutation}: {error}"
        );
        sqlx::query(
            "UPDATE record_inventory_current SET entries = $2, chain_positions = $3
             WHERE resource_id = $1::uuid",
        )
        .bind(resource(1))
        .bind(entries)
        .bind(positions)
        .execute(&pool)
        .await?;
    }
    // An expected difference that does not show fails, at once and in the final count.
    let report = bigname_storage::families::records::compare_family_reads(
        &pool,
        CHAIN,
        Some((23, hash(23))),
        1,
    )
    .await?;
    let mut missing = expected.differences.clone();
    missing.push(ExpectedDifference {
        target: 23,
        key: format!("record_inventory {}", resource(2)),
        fields: vec![(
            "entries[text:url].value".into(),
            Some(json!("x")),
            Some(json!("y")),
        )],
        times: 1,
    });
    let missing = Expectations {
        differences: missing,
        ..Expectations::none()
    };
    // The final count flags the phantom even once the real difference has been counted.
    let counted = Expectations {
        differences: missing.differences.clone(),
        seen: std::sync::Mutex::new(vec![1, 0]),
        ..Expectations::none()
    };
    let stated = format!(
        "the expected difference at 23 on record_inventory {}",
        resource(2)
    );
    assert_eq!(
        missing.check(23, &report).expect_err("missing").to_string(),
        format!("{stated} did not show")
    );
    assert_eq!(
        counted.finish().expect_err("missing").to_string(),
        format!("{stated} showed 0 times, not 1")
    );
    // A report naming one result twice fails rather than matching one expectation twice.
    let mut twice = bigname_storage::families::records::compare_family_reads(
        &pool,
        CHAIN,
        Some((23, hash(23))),
        1,
    )
    .await?;
    let first = twice.differences[0].clone();
    twice.differences.push(first);
    assert_eq!(
        expected.check(23, &twice).expect_err("twice").to_string(),
        format!(
            "the report at 23 names record_inventory {} more than once",
            resource(1)
        )
    );
    db.cleanup().await?;
    Ok(())
}

// A link and a version change at one position (both synthesised, so no transaction or log)
// resolve by event identity: when the link comes later, it is the boundary and a value written
// before both is served; when the version comes later, it cuts that value off. The events are
// inserted in identity order, so today's generated ids agree and every read compares equal.
#[tokio::test]
async fn a_link_and_version_at_one_position_follow_event_identity() -> Result<()> {
    let (db, pool) = database("record_id_boundary_tie").await?;
    seed(&pool).await?;
    for n in 19..=20 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')").bind(CHAIN).bind(hash(n)).bind(n).execute(&pool).await?;
    }
    let version = |n: i64| json!({"source_event":"VersionChanged","node":node(n),"resolver":RESOLVER,"record_version":"1"});
    text(&pool, "record-four", 19, 0, 4, "four").await?;
    text(&pool, "record-five", 19, 1, 5, "five").await?;
    event(
        &pool,
        "tie-a-version-one",
        20,
        0,
        "RecordVersionChanged",
        Some(1),
        version(1),
    )
    .await?;
    link(&pool, "tie-b-link-one", 20, 1, Some(1), 4).await?;
    link(&pool, "tie-c-link-three", 20, 2, Some(3), 5).await?;
    event(
        &pool,
        "tie-d-version-three",
        20,
        3,
        "RecordVersionChanged",
        Some(3),
        version(3),
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET transaction_hash = NULL, transaction_index = NULL, log_index = NULL WHERE event_identity LIKE 'tie-%'").execute(&pool).await?;
    run(&pool, 20, None, RunMode::Normal).await?;
    let linked = inventory(&pool, 1).await?;
    assert_eq!(
        linked["boundary"]["event_kind"], "ResolverRecordLinked",
        "{linked}"
    );
    assert_text(&pool, 1, "four").await?;
    let cut = inventory(&pool, 3).await?;
    assert_eq!(
        cut["boundary"]["event_kind"], "RecordVersionChanged",
        "{cut}"
    );
    assert!(
        cut["entries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["record_key"] != "text:url"),
        "{cut}"
    );
    db.cleanup().await?;
    Ok(())
}

// The same link-and-version ties at one log with both indexes present, decided by the emission
// ordinal (docs/glossary.md#emission-ordinal). Each pair shares block 20, transaction 0 and one
// log; the identities end in ordinals 10 and 2, whose byte order is the reverse of their ordinal
// order, and the ordinal-10 event is inserted first, so today's generated ids pick the other
// event. For resource 1 the link has ordinal 10: the families make it the boundary and serve the
// value written before both, where today makes the version the boundary and cuts that value off.
// For resource 3 the version has ordinal 10: the families cut the value off, where today makes
// the link the boundary and serves it. The index-less test above keeps the identity fallback.
#[tokio::test]
async fn a_link_and_version_at_one_log_follow_the_emission_ordinal() -> Result<()> {
    let (db, pool) = database("record_id_ordinal_tie").await?;
    seed(&pool).await?;
    for n in 19..=20 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')").bind(CHAIN).bind(hash(n)).bind(n).execute(&pool).await?;
    }
    let version = |n: i64| json!({"source_event":"VersionChanged","node":node(n),"resolver":RESOLVER,"record_version":"1"});
    text(&pool, "record-four", 19, 0, 4, "four").await?;
    text(&pool, "record-five", 19, 1, 5, "five").await?;
    // Resource 1, log 0: the link (ordinal 10) first, then the version (ordinal 2).
    link(&pool, "one:10", 20, 0, Some(1), 4).await?;
    event(
        &pool,
        "one:2",
        20,
        0,
        "RecordVersionChanged",
        Some(1),
        version(1),
    )
    .await?;
    // Resource 3, log 1: the version (ordinal 10) first, then the link (ordinal 2).
    event(
        &pool,
        "three:10",
        20,
        1,
        "RecordVersionChanged",
        Some(3),
        version(3),
    )
    .await?;
    link(&pool, "three:2", 20, 1, Some(3), 5).await?;
    let id = |identity: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT normalized_event_id FROM normalized_events WHERE event_identity = $1",
            )
            .bind(identity)
            .fetch_one(&pool)
            .await
        }
    };
    let (four, five) = (id("record-four").await?, id("record-five").await?);
    let (link_one, version_one) = (id("one:10").await?, id("one:2").await?);
    let (version_three, link_three) = (id("three:10").await?, id("three:2").await?);
    assert!(link_one < version_one && version_three < link_three);
    let text_url = json!({"record_family":"text","record_key":"text:url","selector_key":"url","status":"success"});
    let entry = |value: &str| {
        let mut entry = text_url.clone();
        entry["value"] = json!(value);
        entry
    };
    let selector = json!({"cacheable":true,"record_family":"text","record_key":"text:url","selector_key":"url"});
    let boundary = |today: (&str, i64), family: (&str, i64)| {
        [
            (
                "record_version_boundary.event_kind".to_owned(),
                Some(json!(today.0)),
                Some(json!(family.0)),
            ),
            (
                "record_version_boundary.normalized_event_id".to_owned(),
                Some(json!(today.1)),
                Some(json!(family.1)),
            ),
        ]
    };
    let mut linked_later = vec![
        ("entries[text:url]".to_owned(), None, Some(entry("four"))),
        (
            "provenance.record_event_ids".to_owned(),
            Some(json!([link_one])),
            Some(json!([four, link_one])),
        ),
        (
            "selectors[text:url]".to_owned(),
            None,
            Some(selector.clone()),
        ),
    ];
    linked_later.extend(boundary(
        ("RecordVersionChanged", version_one),
        ("ResolverRecordLinked", link_one),
    ));
    let mut version_later = vec![
        ("entries[text:url]".to_owned(), Some(entry("five")), None),
        (
            "provenance.record_event_ids".to_owned(),
            Some(json!([five, link_three])),
            Some(json!([link_three])),
        ),
        ("selectors[text:url]".to_owned(), Some(selector), None),
    ];
    version_later.extend(boundary(
        ("ResolverRecordLinked", link_three),
        ("RecordVersionChanged", version_three),
    ));
    let expected = Expectations {
        differences: vec![
            ExpectedDifference {
                target: 20,
                key: format!("record_inventory {}", resource(1)),
                fields: linked_later,
                times: 1,
            },
            ExpectedDifference {
                target: 20,
                key: format!("record_inventory {}", resource(3)),
                fields: version_later,
                times: 1,
            },
        ],
        ..Expectations::none()
    };
    run_expecting(&pool, 20, None, RunMode::Normal, &expected).await?;
    expected.finish()?;

    // Today's rows, decided by the generated ids.
    let today_one = inventory(&pool, 1).await?;
    assert_eq!(
        today_one["boundary"]["normalized_event_id"], version_one,
        "{today_one}"
    );
    assert_eq!(today_one["entries"], json!([]), "{today_one}");
    let today_three = inventory(&pool, 3).await?;
    assert_eq!(
        today_three["boundary"]["normalized_event_id"], link_three,
        "{today_three}"
    );
    assert_text(&pool, 3, "five").await?;
    // The family rows, decided by the ordinal: entries, boundary and last change.
    for (n, boundary_kind, boundary_id, entries) in [
        (1, "ResolverRecordLinked", link_one, json!([entry("four")])),
        (3, "RecordVersionChanged", version_three, json!([])),
    ] {
        let family = bigname_storage::families::records::load_family_record_inventory(
            &pool,
            CHAIN,
            resource(n).parse()?,
        )
        .await?
        .expect("a family row");
        let today = inventory(&pool, n).await?;
        assert_eq!(family.entries, entries, "resource{n}");
        assert_eq!(
            family.record_version_boundary["event_kind"], boundary_kind,
            "resource{n}"
        );
        assert_eq!(
            family.record_version_boundary["normalized_event_id"], boundary_id,
            "resource{n}"
        );
        // The latest contributing write or link is the selected link on both sides; a version
        // change is never the last change.
        let link = if n == 1 { link_one } else { link_three };
        let last_change = family.last_change.expect("a last change");
        assert_eq!(
            last_change["event_kind"], "ResolverRecordLinked",
            "resource{n}"
        );
        assert_eq!(last_change["normalized_event_id"], link, "resource{n}");
        assert_eq!(last_change, today["last_change"], "resource{n}");
    }
    db.cleanup().await?;
    Ok(())
}

// The inventory's last change at one log, decided by the emission ordinal
// (docs/glossary.md#emission-ordinal): the latest of the selected record id's writes and links
// (storage families/records/assemble.rs, `latest_link`). A write of record id 7 ("metadata:10")
// and the link that selects it for name 3 ("metadata:2") share block 20, transaction 0 and log
// 3. Compared as bytes "metadata:2" sorts last, and the write is inserted first, so identity order
// and today's generated ids both pick the link; the ordinal picks the write. Entries and boundary
// agree; only the last change differs.
#[tokio::test]
async fn the_last_change_of_a_linked_record_follows_the_emission_ordinal() -> Result<()> {
    let (db, pool) = database("record_id_latest_link").await?;
    seed(&pool).await?;
    lineage(&pool, 19..=20).await?;
    text(&pool, "metadata:10", 20, 3, 7, "seven").await?;
    link(&pool, "metadata:2", 20, 3, Some(3), 7).await?;
    let (write, link_id) = (
        event_id(&pool, "metadata:10").await?,
        event_id(&pool, "metadata:2").await?,
    );
    assert!(write < link_id);
    let expected = last_change_differs(vec![
        (
            "last_change.event_kind".into(),
            Some(json!("ResolverRecordLinked")),
            Some(json!("RecordChanged")),
        ),
        (
            "last_change.normalized_event_id".into(),
            Some(json!(link_id)),
            Some(json!(write)),
        ),
    ]);
    run_expecting(&pool, 20, None, RunMode::Normal, &expected).await?;
    expected.finish()?;
    let (today, family) = both_rows(&pool, 3).await?;
    assert_eq!(
        today["entries"],
        json!([{"record_family":"text","record_key":"text:url","selector_key":"url","status":"success","value":"seven"}])
    );
    assert_eq!(family.entries, today["entries"]);
    assert_eq!(today["boundary"]["normalized_event_id"], link_id);
    assert_eq!(family.record_version_boundary, today["boundary"]);
    assert_eq!(today["last_change"]["normalized_event_id"], link_id);
    let last_change = family.last_change.expect("a last change");
    assert_eq!(last_change["event_kind"], "RecordChanged");
    assert_eq!(last_change["normalized_event_id"], write);
    db.cleanup().await?;
    Ok(())
}

// The last change of an inventory with no link selection is its latest served record
// (assemble.rs, `latest_record`). The resolver's link events are removed, so name 3 reads only its
// named writes. Two writes of different record keys share block 20, transaction 0 and log 3:
// "records:10" (text:url, inserted first) and "records:2" (text:email). Identity order and
// today's generated ids pick the text:email write; the ordinal picks the text:url write. Both
// entries are served on both sides; only the last change's event id differs.
#[tokio::test]
async fn the_last_change_of_unlinked_records_follows_the_emission_ordinal() -> Result<()> {
    let (db, pool) = database("record_id_latest_record").await?;
    seed(&pool).await?;
    sqlx::query("DELETE FROM normalized_events WHERE event_kind = 'ResolverRecordLinked'")
        .execute(&pool)
        .await?;
    lineage(&pool, 19..=20).await?;
    let named = |key: &str, value: &str| json!({"source_event":"TextChanged","node":node(3),"resolver":RESOLVER,"record_key":format!("text:{key}"),"record_family":"text","selector_key":key,"value_retained":true,"value":value,"value_length":value.len()});
    event(
        &pool,
        "records:10",
        20,
        3,
        "RecordChanged",
        Some(3),
        named("url", "url"),
    )
    .await?;
    event(
        &pool,
        "records:2",
        20,
        3,
        "RecordChanged",
        Some(3),
        named("email", "email"),
    )
    .await?;
    let (url, email) = (
        event_id(&pool, "records:10").await?,
        event_id(&pool, "records:2").await?,
    );
    assert!(url < email);
    let expected = last_change_differs(vec![(
        "last_change.normalized_event_id".into(),
        Some(json!(email)),
        Some(json!(url)),
    )]);
    run_expecting(&pool, 20, None, RunMode::Normal, &expected).await?;
    expected.finish()?;
    let (today, family) = both_rows(&pool, 3).await?;
    let keys = |entries: &Value| -> Vec<String> {
        entries
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["record_key"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(keys(&today["entries"]), ["text:email", "text:url"]);
    assert_eq!(family.entries, today["entries"]);
    assert_eq!(family.record_version_boundary, today["boundary"]);
    assert_eq!(today["last_change"]["normalized_event_id"], email);
    let last_change = family.last_change.expect("a last change");
    assert_eq!(last_change["event_kind"], "RecordChanged");
    assert_eq!(last_change["normalized_event_id"], url);
    db.cleanup().await?;
    Ok(())
}

async fn lineage(pool: &PgPool, blocks: std::ops::RangeInclusive<i64>) -> Result<()> {
    for n in blocks {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')").bind(CHAIN).bind(hash(n)).bind(n).execute(pool).await?;
    }
    Ok(())
}

async fn event_id(pool: &PgPool, identity: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT normalized_event_id FROM normalized_events WHERE event_identity = $1",
    )
    .bind(identity)
    .fetch_one(pool)
    .await?)
}

/// Name 3's inventory differing only in `fields` at block 20, once.
fn last_change_differs(fields: Vec<(String, Option<Value>, Option<Value>)>) -> Expectations {
    Expectations {
        differences: vec![ExpectedDifference {
            target: 20,
            key: format!("record_inventory {}", resource(3)),
            fields,
            times: 1,
        }],
        ..Expectations::none()
    }
}

/// Today's inventory row of name `n` and the family reader's.
async fn both_rows(
    pool: &PgPool,
    n: i64,
) -> Result<(Value, bigname_storage::RecordInventoryCurrentRow)> {
    let family = bigname_storage::families::records::load_family_record_inventory(
        pool,
        CHAIN,
        resource(n).parse()?,
    )
    .await?
    .expect("a family row");
    Ok((inventory(pool, n).await?, family))
}

// Names resolving to an address are found from every retained address value, not only the
// derived address index, which used to drop a value positioned at or before its partition's
// version change. A link that outranks that version keeps the value served (value at 19, version at 20,
// link at 21 to a record with no address), and a value sharing the version's block, transaction
// and log position but later by event identity is served too; both names must be listed. Every
// run compares the family reads with today's reads, pages included.
#[tokio::test]
async fn inverse_address_reads_find_values_the_address_index_drops() -> Result<()> {
    let (db, pool) = database("record_id_inverse_address").await?;
    seed(&pool).await?;
    for n in 19..=22 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')").bind(CHAIN).bind(hash(n)).bind(n).execute(&pool).await?;
    }
    let address = |n: i64, value: &str| json!({"source_event":"AddressChanged","node":node(n),"resolver":RESOLVER,"record_key":"addr:60","record_family":"addr","selector_key":"60","coin_type":"60","value":value});
    let version = |n: i64| json!({"source_event":"VersionChanged","node":node(n),"resolver":RESOLVER,"record_version":"1"});
    event(
        &pool,
        "address-one",
        19,
        0,
        "RecordChanged",
        Some(1),
        address(1, INVERSE_A),
    )
    .await?;
    event(
        &pool,
        "version-one",
        20,
        0,
        "RecordVersionChanged",
        Some(1),
        version(1),
    )
    .await?;
    link(&pool, "link-a-three", 21, 0, Some(1), 3).await?;
    // Inserted in identity order, so today's generated ids agree with the canonical order.
    event(
        &pool,
        "tie-a-version",
        22,
        0,
        "RecordVersionChanged",
        Some(2),
        version(2),
    )
    .await?;
    event(
        &pool,
        "tie-b-value",
        22,
        1,
        "RecordChanged",
        Some(2),
        address(2, INVERSE_A),
    )
    .await?;
    sqlx::query("UPDATE normalized_events SET transaction_hash = NULL, transaction_index = NULL, log_index = NULL WHERE event_identity IN ('tie-a-version', 'tie-b-value')").execute(&pool).await?;
    // Step 2 now indexes both: the value that ties with the version by event identity
    // (resource 2) and resource 1's value before its partition's version change, which the later
    // link keeps served. So the index misses nothing here.
    let expected = Expectations::none();
    run_expecting(&pool, 22, None, RunMode::Normal, &expected).await?;
    for id in [1, 2] {
        let value = INVERSE_A;
        let row = inventory(&pool, id).await?;
        let served = row["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["record_key"] == "addr:60")
            .map(|entry| entry["value"].clone());
        assert_eq!(served, Some(json!(value)), "resource{id}: {row}");
        let listed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM address_records_current
             WHERE address = $1 AND coin_type = '60' AND record_resource_id = $2::uuid",
        )
        .bind(value)
        .bind(resource(id))
        .fetch_one(&pool)
        .await?;
        assert_eq!(listed, 1, "today lists resource{id} for {value}");
    }

    // Mutation: a difference on the second of the address's two pages (page size one) must fail
    // the comparison, and be named on that entry.
    sqlx::query(
        "UPDATE address_records_current SET provenance = provenance || '{\"mutated\": true}'
         WHERE address = $1 AND record_resource_id = $2::uuid",
    )
    .bind(INVERSE_A)
    .bind(resource(2))
    .execute(&pool)
    .await?;
    let target = Marker {
        number: 22,
        hash: hash(22),
    };
    let report = family_shadow::shadow_report_at(&pool, &target).await?;
    assert!(expected.check(22, &report).is_err(), "{report:#?}");
    let fields: Vec<&str> = report
        .differences
        .iter()
        .flat_map(|(_, differences)| differences.iter().map(|d| d.field.as_str()))
        .collect();
    assert_eq!(
        fields,
        [format!(
            "entries[ens:{}|{}|{}].provenance.mutated",
            node(2),
            resource(2),
            resource(102)
        )],
        "{report:#?}"
    );
    // With the index rows removed, the family read finds both names from the retained values.
    sqlx::query("DELETE FROM project_address_record_node_index")
        .execute(&pool)
        .await?;
    let page = bigname_storage::families::records::load_family_address_records_page(
        &pool,
        INVERSE_A,
        "60",
        None,
        bigname_storage::AddressNamesCurrentDedupe::Surface,
        None,
        None,
        bigname_storage::AddressNamesCurrentSort::Name,
        bigname_storage::AddressNamesCurrentOrder::Asc,
        None,
        10,
    )
    .await?;
    let mut resources: Vec<_> = page
        .entries
        .iter()
        .map(|entry| entry.record_resource_id.to_string())
        .collect();
    resources.sort();
    assert_eq!(resources, [resource(1), resource(2)]);
    let mut misses = index_misses_without_index(&pool, INVERSE_A).await?;
    misses.sort();
    assert_eq!(
        misses,
        [
            (resource(1), "addr:60".to_owned()),
            (resource(2), "addr:60".to_owned())
        ]
    );
    db.cleanup().await?;
    Ok(())
}

// A named write is admitted by its logical name alone, with no node test, so a named address
// write whose node is not the name's namehash is served forward and must be found inversely.
#[tokio::test]
async fn a_named_address_write_at_another_node_is_found_inversely() -> Result<()> {
    let (db, pool) = database("record_id_named_other_node").await?;
    seed(&pool).await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,19,to_timestamp(19::double precision),'canonical')").bind(CHAIN).bind(hash(19)).execute(&pool).await?;
    event(
        &pool,
        "named-other-node",
        19,
        0,
        "RecordChanged",
        Some(1),
        json!({"source_event":"AddressChanged","node":node(7),"resolver":RESOLVER,"record_key":"addr:60","record_family":"addr","selector_key":"60","coin_type":"60","value":INVERSE_A}),
    )
    .await?;
    let expected = Expectations {
        index_misses: vec![(
            19,
            format!(
                "resolves_to {INVERSE_A} coin 60 resource {} addr:60",
                resource(1)
            ),
        )],
        ..Expectations::none()
    };
    run_expecting(&pool, 19, None, RunMode::Normal, &expected).await?;
    let listed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM address_records_current
         WHERE address = $1 AND record_resource_id = $2::uuid",
    )
    .bind(INVERSE_A)
    .bind(resource(1))
    .fetch_one(&pool)
    .await?;
    assert_eq!(listed, 1, "today lists the named write");
    db.cleanup().await?;
    Ok(())
}

/// The index misses of `address`'s coin-60 names through the paged family read, as (record
/// resource, record key); call it with the index rows removed.
async fn index_misses_without_index(pool: &PgPool, address: &str) -> Result<Vec<(String, String)>> {
    let records =
        bigname_storage::families::records::load_family_address_records(pool, address, "60")
            .await?;
    let page = bigname_storage::families::records::page_family_address_records(
        pool,
        &records,
        None,
        bigname_storage::AddressNamesCurrentDedupe::Surface,
        None,
        None,
        bigname_storage::AddressNamesCurrentSort::Name,
        bigname_storage::AddressNamesCurrentOrder::Asc,
        None,
        10,
    )
    .await?;
    Ok(page
        .index_misses
        .into_iter()
        .map(|(resource, key)| (resource.to_string(), key))
        .collect())
}

const INVERSE_A: &str = "0x5555555555555555555555555555555555555555";
/// The raw-bytes address of the pair case, mixed case as a resolver may emit it.
const RAW_PAIR_MIXED: &str = "0x77777777777777777777777777777777777777Ab";

// A coin-60 pair whose `AddressChanged` half carries its address only as raw bytes (and a
// different `AddrChanged` value), before a version reset that a later link lifts: the forward read
// serves the raw-bytes address, which the value row keeps only as the pair's
// `sibling_address_bytes_hex`. Both inverse readers must list the name for it, from mixed-case
// input too. Step 2 now indexes values past a version change, so the index finds it; with the
// index rows removed, the family read must still find it from the retained values alone.
#[tokio::test]
async fn a_raw_bytes_pair_address_behind_a_lifted_version_is_found_inversely() -> Result<()> {
    let (db, pool) = database("record_id_raw_pair").await?;
    seed(&pool).await?;
    for n in 19..=21 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')").bind(CHAIN).bind(hash(n)).bind(n).execute(&pool).await?;
    }
    event(
        &pool,
        "raw-pair-address",
        19,
        0,
        "RecordChanged",
        Some(1),
        json!({"source_event":"AddressChanged","node":node(1),"resolver":RESOLVER,"record_key":"addr:60","record_family":"addr","selector_key":"60","coin_type":"60","value_retained":false,"address_bytes_hex":RAW_PAIR_MIXED}),
    )
    .await?;
    event(
        &pool,
        "raw-pair-addr",
        19,
        1,
        "RecordChanged",
        Some(1),
        json!({"source_event":"AddrChanged","node":node(1),"resolver":RESOLVER,"record_key":"addr:60","record_family":"addr","selector_key":"60","coin_type":"60","value":INVERSE_A}),
    )
    .await?;
    event(
        &pool,
        "raw-pair-version",
        20,
        0,
        "RecordVersionChanged",
        Some(1),
        json!({"source_event":"VersionChanged","node":node(1),"resolver":RESOLVER,"record_version":"1"}),
    )
    .await?;
    link(&pool, "raw-pair-link", 21, 0, Some(1), 3).await?;
    let lower = RAW_PAIR_MIXED.to_ascii_lowercase();
    run(&pool, 21, None, RunMode::Normal).await?;
    for (address, without_index) in [
        (lower.as_str(), false),
        (RAW_PAIR_MIXED, false),
        (lower.as_str(), true),
        (RAW_PAIR_MIXED, true),
    ] {
        if without_index {
            sqlx::query("DELETE FROM project_address_record_node_index")
                .execute(&pool)
                .await?;
        }
        let page = bigname_storage::families::records::load_family_address_records_page(
            &pool,
            address,
            "60",
            None,
            bigname_storage::AddressNamesCurrentDedupe::Surface,
            None,
            None,
            bigname_storage::AddressNamesCurrentSort::Name,
            bigname_storage::AddressNamesCurrentOrder::Asc,
            None,
            10,
        )
        .await?;
        let resources: Vec<_> = page
            .entries
            .iter()
            .map(|entry| entry.record_resource_id.to_string())
            .collect();
        assert_eq!(
            resources,
            [resource(1)],
            "{address} without index {without_index}"
        );
    }
    let listed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM address_records_current
         WHERE address = $1 AND record_resource_id = $2::uuid",
    )
    .bind(&lower)
    .bind(resource(1))
    .fetch_one(&pool)
    .await?;
    assert_eq!(listed, 1, "today lists the raw-bytes address");
    // With no index rows, the paged family read names the entry only the retained values found.
    assert_eq!(
        index_misses_without_index(&pool, &lower).await?,
        [(resource(1), "addr:60".to_owned())]
    );
    // With today's rows for the address removed as well, only the retained-value listing (the
    // pair's sibling raw bytes) puts the address in the comparison, which then reports that the
    // family lists a name today does not.
    sqlx::query("DELETE FROM address_records_current WHERE address = $1")
        .bind(&lower)
        .execute(&pool)
        .await?;
    let report = bigname_storage::families::records::compare_family_reads(
        &pool,
        CHAIN,
        Some((21, hash(21))),
        1,
    )
    .await?;
    let key = format!("resolves_to {lower} coin 60");
    let differences = report
        .differences
        .iter()
        .find(|(listed, _)| *listed == key)
        .map(|(_, differences)| differences.clone())
        .unwrap_or_default();
    assert!(
        differences
            .iter()
            .any(|d| d.field == "entries.count" && d.today == Some(json!(0))),
        "{report:#?}"
    );
    db.cleanup().await?;
    Ok(())
}

/// Today's linked arm admits a record-id write only with `storage_model = resolver_record_id` and
/// the resolver named in its payload (`linked_records.rs`, `project_linked_record_events`). A
/// linked `AddressChanged` value pairs with a linked `AddrChanged` one log later; the same record
/// id's `AddrChanged` without the storage model is attributed to nothing, so it pairs nothing.
///
/// Step 2's family pairs node-keyed writes only, so for the linked pair it serves the later
/// `AddrChanged` value where today serves the `AddressChanged` one; the harness names that
/// difference. The pinned record-id resolver emits only `AddressUpdated` for an address write, so
/// no such pair comes from that source.
/// (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L174 @ ens_v2@a971bd64)
#[tokio::test]
async fn a_record_id_pair_needs_the_sibling_admitted_by_the_linked_arm() -> Result<()> {
    let (db, pool) = database("record_id_pair_admission").await?;
    seed(&pool).await?;
    for n in 19..=20 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')").bind(CHAIN).bind(hash(n)).bind(n).execute(&pool).await?;
    }
    let address = |source: &str, value: &str, linked: bool| {
        let mut after = json!({"source_event":source,"resolver":RESOLVER,"resolver_record_id":"2","record_key":"addr:60","record_family":"addr","selector_key":"60","coin_type":"60","value_retained":true,"value":value});
        if linked {
            after["storage_model"] = json!("resolver_record_id");
        }
        after
    };
    let pairs = |pool: PgPool| async move {
        let family = bigname_storage::families::records::load_family_record_inventory_detail(
            &pool,
            CHAIN,
            resource(2).parse()?,
            bigname_storage::families::records::FamilyAttribution::Given(Default::default()),
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("no family row"))?;
        let served: Value = sqlx::query_scalar(
            "SELECT entry -> 'value' FROM record_inventory_current,
                    jsonb_array_elements(entries) entry
             WHERE resource_id = $1::uuid AND entry ->> 'record_key' = 'addr:60'",
        )
        .bind(resource(2))
        .fetch_one(&pool)
        .await?;
        anyhow::Ok((family.compatibility_pairs.len(), served))
    };
    let (value_a, sibling_a) = (INVERSE_A, "0x6666666666666666666666666666666666666666");
    event(
        &pool,
        "linked-value",
        19,
        0,
        "RecordChanged",
        None,
        address("AddressChanged", value_a, true),
    )
    .await?;
    event(
        &pool,
        "linked-sibling",
        19,
        1,
        "RecordChanged",
        None,
        address("AddrChanged", sibling_a, true),
    )
    .await?;
    let id = |identity: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT normalized_event_id FROM normalized_events WHERE event_identity = $1",
            )
            .bind(identity)
            .fetch_one(&pool)
            .await
        }
    };
    let (value, sibling) = (id("linked-value").await?, id("linked-sibling").await?);
    let position = |identity: &str, log: i64| {
        json!({"block_number": 19, "transaction_index": 0, "log_index": log,
               "event_identity": identity})
    };
    let mut linked_pair = Expectations {
        differences: vec![ExpectedDifference {
            target: 19,
            key: format!("record_inventory {}", resource(2)),
            fields: vec![
                (
                    "entries[addr:60].value".into(),
                    Some(json!(value_a)),
                    Some(json!(sibling_a)),
                ),
                (
                    "provenance.record_event_ids".into(),
                    Some(json!([
                        id("empty-text").await?,
                        id("empty-contenthash").await?,
                        value,
                        id("link-b-two").await?
                    ])),
                    Some(json!([
                        id("empty-text").await?,
                        id("empty-contenthash").await?,
                        sibling,
                        id("link-b-two").await?
                    ])),
                ),
                (
                    "compatibility_pairs[addr:60]".into(),
                    Some(json!({
                        "record_key": "addr:60",
                        "value_event_id": value, "value_position": position("linked-value", 0),
                        "sibling_event_id": sibling,
                        "sibling_position": position("linked-sibling", 1),
                    })),
                    None,
                ),
            ],
            times: 1,
        }],
        ..Expectations::default()
    };
    let link = id("link-b-two").await?;
    let logical_name_id = format!("ens:{}", node(2));
    let coverage = json!({"exhaustiveness": "not_asserted", "status": "projected"});
    let entry = |address: &str, event: i64| {
        json!({
            "address": address, "binding_kind": "DeclaredRegistryPath",
            "canonical_display_name": "record2.eth",
            "canonicality_summary": {"state": "canonical_lineage"},
            "chain_positions": {"block_hash": hash(19), "block_number": 19},
            "coin_type": "60", "coverage": coverage,
            "logical_name_id": logical_name_id, "namehash": node(2), "namespace": "ens",
            "normalized_name": "record2.eth",
            "provenance": {
                "chain_id": CHAIN, "coverage": coverage, "logical_name_id": logical_name_id,
                "normalized_event_id": event,
                "record_version_boundary_key": format!(
                    "70:{logical_name_id};36:{};{}:{link};20:ResolverRecordLinked;16:{CHAIN};2:14;66:{};25:1970-01-01T00:00:14+00:00;",
                    resource(2),
                    link.to_string().len(),
                    hash(14)
                ),
                "resolver_address": RESOLVER,
            },
            "record_key": "addr:60", "record_resource_id": resource(2),
            "resource_id": resource(2), "surface_binding_id": resource(102),
        })
    };
    let entry_key = format!(
        "entries[{logical_name_id}|{}|{}]",
        resource(2),
        resource(102)
    );
    for (address, today, family) in [
        (value_a, Some(entry(value_a, sibling)), None),
        (sibling_a, None, Some(entry(sibling_a, sibling))),
    ] {
        let count = |side: &Option<Value>| json!(usize::from(side.is_some()));
        linked_pair.differences.push(ExpectedDifference {
            target: 19,
            key: format!("resolves_to {address} coin 60"),
            fields: vec![
                (
                    "entries.count".into(),
                    Some(count(&today)),
                    Some(count(&family)),
                ),
                (entry_key.clone(), today, family),
            ],
            times: 1,
        });
    }
    run_expecting(&pool, 19, None, RunMode::Normal, &linked_pair).await?;
    linked_pair.finish()?;
    assert_eq!(pairs(pool.clone()).await?, (0, json!(value_a)));
    let (value_b, sibling_b) = (
        "0x7777777777777777777777777777777777777777",
        "0x8888888888888888888888888888888888888888",
    );
    event(
        &pool,
        "unlinked-value",
        20,
        0,
        "RecordChanged",
        None,
        address("AddressChanged", value_b, true),
    )
    .await?;
    event(
        &pool,
        "unlinked-sibling",
        20,
        1,
        "RecordChanged",
        None,
        address("AddrChanged", sibling_b, false),
    )
    .await?;
    run(&pool, 20, Some(19), RunMode::Normal).await?;
    assert_eq!(pairs(pool.clone()).await?, (0, json!(value_b)));
    db.cleanup().await?;
    Ok(())
}

/// For the root name, whose namehash is the default node, the exact link and the default link are
/// the same `Linked` event. When it links record id 0, today's selection (`linked_records.rs`,
/// `project_selected_records`: the exact record id unless it is `0`, else the default's) takes
/// record id 0 from that same link, so record id 0's writes serve the root name and a linked
/// `AddressChanged` value pairs with the linked `AddrChanged` one log later. The harness must
/// expect that pair.
#[tokio::test]
async fn a_root_name_holding_a_zero_link_pairs_record_id_zero() -> Result<()> {
    let (db, pool) = database("record_id_root_zero_link").await?;
    seed(&pool).await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,19,to_timestamp(19),'canonical')").bind(CHAIN).bind(hash(19)).execute(&pool).await?;
    event(
        &pool,
        "upgrade-other",
        10,
        8,
        "Upgraded",
        None,
        json!({"proxy_address":OTHER,"implementation":IMPLEMENTATION}),
    )
    .await?;
    let root = format!("ens:{}", hash(0));
    sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens','',ARRAY[]::text[],'\\x00'::bytea,$2,ARRAY[]::text[],'fixture','active',$3,$4,10,'canonical')")
        .bind(&root).bind(hash(0)).bind(CHAIN).bind(hash(10)).execute(&pool).await?;
    sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,10,'canonical')").bind(resource(50)).bind(CHAIN).bind(hash(10)).execute(&pool).await?;
    sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path','ens_v2',to_timestamp(10),$4,$5,10,'canonical')").bind(resource(150)).bind(&root).bind(resource(50)).bind(CHAIN).bind(hash(10)).execute(&pool).await?;
    let registry_manifest: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions WHERE source_family = 'ens_v2_registry_l1'",
    )
    .fetch_one(&pool)
    .await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref,source_manifest_id) VALUES ('root-pointer','ens',$1,$2::uuid,'ResolverChanged','ens_v2_registry_l1',1,$3,19,$4,$5,0,9,'ens_v2_registry_resource_surface','canonical',$6,$7,$8)")
        .bind(&root).bind(resource(50)).bind(CHAIN).bind(hash(19)).bind(hash(1900))
        .bind(json!({"source_event":"ResolverUpdated","resolver":OTHER,"sender":REGISTRY,"token_id":hash(0)}))
        .bind(json!({"emitting_address":REGISTRY})).bind(registry_manifest).execute(&pool).await?;
    let address = |source: &str, value: &str| json!({"source_event":source,"storage_model":"resolver_record_id","resolver":OTHER,"resolver_record_id":"0","record_key":"addr:60","record_family":"addr","selector_key":"60","coin_type":"60","value_retained":true,"value":value});
    event(&pool,"root-zero-link",19,0,"ResolverRecordLinked",None,json!({"source_event":"Linked","storage_model":"resolver_record_id","resolver":OTHER,"node":hash(0),"resolver_record_id":"0","dns_encoded_name":"0x00"})).await?;
    event(
        &pool,
        "root-zero-value",
        19,
        1,
        "RecordChanged",
        None,
        address("AddressChanged", INVERSE_A),
    )
    .await?;
    event(
        &pool,
        "root-zero-sibling",
        19,
        2,
        "RecordChanged",
        None,
        address("AddrChanged", "0x6666666666666666666666666666666666666666"),
    )
    .await?;
    let id = |identity: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT normalized_event_id FROM normalized_events WHERE event_identity = $1",
            )
            .bind(identity)
            .fetch_one(&pool)
            .await
        }
    };
    let (link, value, sibling) = (
        id("root-zero-link").await?,
        id("root-zero-value").await?,
        id("root-zero-sibling").await?,
    );
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 19,
            affected_from_block: 10,
            affected_to_block: 19,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    // Today serves record id 0's value for the root name through its zero link.
    let provenance: Value = sqlx::query_scalar(
        "SELECT provenance FROM record_inventory_current WHERE resource_id = $1::uuid",
    )
    .bind(resource(50))
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        provenance["record_link_event_ids"],
        json!([link]),
        "{provenance}"
    );
    let served: Value = sqlx::query_scalar(
        "SELECT entry -> 'value' FROM record_inventory_current,
                jsonb_array_elements(entries) entry
         WHERE resource_id = $1::uuid AND entry ->> 'record_key' = 'addr:60'",
    )
    .bind(resource(50))
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        served,
        json!(INVERSE_A),
        "the AddressChanged half is served"
    );
    assert!(
        provenance["record_event_ids"]
            .as_array()
            .is_some_and(|served| served.contains(&json!(value))),
        "{provenance}"
    );
    let report = family_shadow::shadow_report_at(&pool, &outcome.current).await?;
    let position = |identity: &str, log: i64| {
        json!({"block_number": 19, "transaction_index": 0, "log_index": log,
               "event_identity": identity})
    };
    let pair = json!({
        "record_key": "addr:60",
        "value_event_id": value, "value_position": position("root-zero-value", 1),
        "sibling_event_id": sibling, "sibling_position": position("root-zero-sibling", 2),
    });
    let differences = report
        .differences
        .iter()
        .find(|(key, _)| *key == format!("record_inventory {}", resource(50)))
        .map(|(_, differences)| differences.clone())
        .unwrap_or_default();
    assert!(
        differences.iter().any(|difference| {
            difference.field == "compatibility_pairs[addr:60]"
                && difference.today == Some(pair.clone())
        }),
        "{report:#?}"
    );
    db.cleanup().await?;
    Ok(())
}

/// Both address readers list every chain's names for an address. A name another chain lists
/// there is compared only if that chain's families stand at its served marker; otherwise the
/// comparison refuses rather than report or hide a difference from an incomparable snapshot.
#[tokio::test]
async fn an_address_listed_on_a_lagging_chain_stops_the_comparison() -> Result<()> {
    let (db, pool) = database("record_id_other_chain").await?;
    seed(&pool).await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,19,to_timestamp(19),'canonical')").bind(CHAIN).bind(hash(19)).execute(&pool).await?;
    event(&pool, "named-address", 19, 0, "RecordChanged", Some(1), json!({"source_event":"AddressChanged","node":node(1),"resolver":RESOLVER,"record_key":"addr:60","record_family":"addr","selector_key":"60","coin_type":"60","value":INVERSE_A})).await?;
    run(&pool, 19, None, RunMode::Normal).await?;
    let copied = sqlx::query(
        "INSERT INTO address_records_current
         SELECT (jsonb_populate_record(NULL::address_records_current, to_jsonb(row)
                 || jsonb_build_object('logical_name_id', $2::text, 'namehash', $3::text,
                                       'raw_name', 'record3.eth',
                                       'surface_binding_id', $4::uuid, 'resource_id', $5::uuid,
                                       'record_resource_id', $5::uuid,
                                       'provenance', row.provenance
                                           || '{\"chain_id\":\"base-sepolia\"}'))).*
         FROM address_records_current row WHERE row.address = $1",
    )
    .bind(INVERSE_A)
    .bind(format!("ens:{}", node(3)))
    .bind(node(3))
    .bind(resource(103))
    .bind(resource(3))
    .execute(&pool)
    .await?;
    assert_eq!(copied.rows_affected(), 1);
    // Today's reader serves a row only on its chain's canonical lineage.
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ('base-sepolia',$1,19,to_timestamp(19),'canonical')").bind(hash(19)).execute(&pool).await?;
    let error = bigname_storage::families::records::compare_family_reads(
        &pool,
        CHAIN,
        Some((19, hash(19))),
        10,
    )
    .await
    .expect_err("a lagging chain's names are not compared");
    let message = format!("{error:#}");
    assert!(
        message.contains("base-sepolia") && message.contains("served marker"),
        "{message}"
    );
    // Once that chain's families stand at its served marker, its names are compared like any
    // other: today lists the second chain's name, the family read (which has none) does not.
    let copied = sqlx::query(
        "INSERT INTO project_family_marker
         SELECT (jsonb_populate_record(NULL::project_family_marker,
                 to_jsonb(row) || '{\"chain_id\":\"base-sepolia\"}')).*
         FROM project_family_marker row WHERE row.chain_id = $1",
    )
    .bind(CHAIN)
    .execute(&pool)
    .await?;
    assert_eq!(copied.rows_affected(), 1);
    sqlx::query(
        "INSERT INTO chain_phase_state
             (chain_id, phase_name, current_block_number, current_block_hash)
         VALUES ('base-sepolia', 'project', 19, $1)",
    )
    .bind(hash(19))
    .execute(&pool)
    .await?;
    let report = bigname_storage::families::records::compare_family_reads(
        &pool,
        CHAIN,
        Some((19, hash(19))),
        10,
    )
    .await?;
    let key = format!("resolves_to {INVERSE_A} coin 60");
    let counted = report
        .differences
        .iter()
        .filter(|(listed, _)| *listed == key)
        .flat_map(|(_, differences)| differences.iter())
        .any(|d| {
            d.field == "entries.count" && d.today == Some(json!(2)) && d.family == Some(json!(1))
        });
    assert!(counted, "{report:#?}");
    db.cleanup().await?;
    Ok(())
}

// A normalized record whose `value` is an explicit JSON null, as
// crates/project/testdata/sql/stage/linked_records_fixture.sql sets up, is a success to today's
// builder (`after_state ? 'value'`). The family row keeps that status though its value column
// reads back empty, and the family entry must be today's entry. Today's own row reader rejects
// that entry (a success without a value), so the harness cannot run here; the entries are compared
// as stored.
#[tokio::test]
async fn an_explicit_null_value_is_served_as_today_serves_it() -> Result<()> {
    let (db, pool) = database("record_id_explicit_null").await?;
    seed(&pool).await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,19,to_timestamp(19::double precision),'canonical')").bind(CHAIN).bind(hash(19)).execute(&pool).await?;
    event(
        &pool,
        "explicit-null",
        19,
        0,
        "RecordChanged",
        Some(1),
        json!({"source_event":"TextChanged","node":node(1),"resolver":RESOLVER,"record_key":"text:url","record_family":"text","selector_key":"url","value":null}),
    )
    .await?;
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: 19,
            affected_from_block: 10,
            affected_to_block: 19,
            resume_current: None,
            mode: RunMode::Normal,
        })
        .await?;
    family_shadow::rebuild_families_at(&pool, &outcome.current).await?;
    let url = |entries: &Value| {
        entries
            .as_array()
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|entry| entry["record_key"] == "text:url")
            })
            .cloned()
    };
    let today = url(&inventory(&pool, 1).await?["entries"]);
    assert_eq!(
        today.as_ref().map(|entry| &entry["status"]),
        Some(&json!("success")),
        "{today:?}"
    );
    let family = bigname_storage::families::records::load_family_record_inventory_detail(
        &pool,
        CHAIN,
        resource(1).parse()?,
        bigname_storage::families::records::FamilyAttribution::Given(Default::default()),
    )
    .await?
    .expect("a family row");
    assert_eq!(url(&family.row.entries), today);
    db.cleanup().await?;
    Ok(())
}

// ABI content types come from the writes the selected record holds, so a write made before a
// name links to the record counts, and a name without an exact link reads the default record.
#[tokio::test]
async fn abi_content_types_follow_exact_links_and_the_default_record() -> Result<()> {
    let (db, pool) = database("record_id_abi").await?;
    seed(&pool).await?;
    abi(&pool, "abi-seven", 10, 8, 7, "8").await?;
    abi(&pool, "abi-two", 10, 9, 2, "4").await?;
    link(&pool, "link-c-seven", 15, 1, Some(3), 7).await?;
    run(&pool, 14, None, RunMode::Normal).await?;
    assert_eq!(abi_content_types(&pool, 1).await?, observed(&[]));
    assert_eq!(abi_content_types(&pool, 2).await?, observed(&["4"]));
    // No exact link: the default record (2) is selected.
    assert_eq!(abi_content_types(&pool, 3).await?, observed(&["4"]));
    run(&pool, 15, Some(14), RunMode::Normal).await?;
    // Record 7 was written before the link and is now selected.
    assert_eq!(abi_content_types(&pool, 3).await?, observed(&["8"]));
    // An unlinked name falls back to the default record.
    assert_eq!(abi_content_types(&pool, 1).await?, observed(&["4"]));
    run(&pool, 16, Some(15), RunMode::Normal).await?;
    assert_eq!(abi_content_types(&pool, 1).await?, observed(&[]));
    assert_eq!(abi_content_types(&pool, 3).await?, observed(&["8"]));
    run(&pool, 16, None, RunMode::Normal).await?;
    assert_eq!(abi_content_types(&pool, 3).await?, observed(&["8"]));
    db.cleanup().await?;
    Ok(())
}

fn observed(types: &[&str]) -> bigname_storage::AbiContentTypes {
    bigname_storage::AbiContentTypes::Observed(types.iter().map(|v| (*v).to_owned()).collect())
}

async fn abi_content_types(pool: &PgPool, id: i64) -> Result<bigname_storage::AbiContentTypes> {
    let (resource_id, boundary_key, support, provenance, positions, recomputed): (
        uuid::Uuid,
        String,
        String,
        Value,
        Value,
        time::OffsetDateTime,
    ) = sqlx::query_as(
        "SELECT resource_id, record_version_boundary_key, support_status, provenance, chain_positions, last_recomputed_at FROM record_inventory_current WHERE resource_id=$1::uuid",
    )
    .bind(resource(id))
    .fetch_one(pool)
    .await?;
    let mut answers = bigname_storage::load_record_inventory_abi_content_types(
        pool,
        &[bigname_storage::AbiContentTypesInput {
            authoritative: support == "supported",
            resource_id,
            record_version_boundary_key: &boundary_key,
            provenance: &provenance,
            chain_positions: &positions,
            last_recomputed_at: recomputed,
        }],
    )
    .await?;
    Ok(answers.remove(0))
}

async fn abi(
    pool: &PgPool,
    identity: &str,
    block: i64,
    log: i64,
    record: i64,
    content_type: &str,
) -> Result<()> {
    event(pool,identity,block,log,"RecordChanged",None,json!({"source_event":"ABIUpdated","storage_model":"resolver_record_id","resolver":RESOLVER,"resolver_record_id":record.to_string(),"record_key":format!("abi:{content_type}"),"record_family":"abi","selector_key":content_type,"content_type":content_type,"value_retained":false})).await
}

// The overview's link section follows the latest `Linked` per node: record 0 drops the node,
// the empty-name node is the default record, and a name is attached only when a surface knows it.
#[tokio::test]
async fn resolver_links_summary_follows_latest_link_per_node() -> Result<()> {
    let (db, pool) = database("record_id_link_summary").await?;
    seed(&pool).await?;
    run(&pool, 12, None, RunMode::Normal).await?;
    let links = links_summary(&pool, RESOLVER).await?;
    assert_eq!(links["status"], "supported", "{links}");
    assert_eq!(links["count"], 3);
    assert_eq!(links["record_count"], 2);
    let items = links["items"].as_array().unwrap();
    let mut first_record: Vec<_> = [node(1), node(2)].into();
    first_record.sort();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["record_id"], "1");
    assert_eq!(items[0]["namehash"], first_record[0]);
    assert_eq!(items[1]["record_id"], "1");
    assert_eq!(items[1]["namehash"], first_record[1]);
    for item in &items[..2] {
        let n = if item["namehash"] == node(1) { 1 } else { 2 };
        assert_eq!(item["name"], format!("record{n}.eth"));
        assert_eq!(item["logical_name_id"], format!("ens:{}", node(n)));
        assert_eq!(item["namespace"], "ens");
        assert_eq!(item["default"], false);
        assert_eq!(item["chain_position"]["block_number"], 11);
    }
    assert_eq!(
        items[2],
        json!({
            "record_id": "2", "namehash": hash(0), "default": true,
            "normalized_event_id": items[2]["normalized_event_id"],
            "chain_position": {
                "chain_id": CHAIN, "block_number": 12, "block_hash": hash(12),
                "transaction_hash": hash(1200), "log_index": 0,
                "timestamp": items[2]["chain_position"]["timestamp"],
            }
        })
    );
    assert!(items[2]["chain_position"]["timestamp"].is_string());

    run(&pool, 18, Some(12), RunMode::Normal).await?;
    let links = links_summary(&pool, RESOLVER).await?;
    assert_eq!(links["count"], 2, "{links}");
    assert_eq!(links["record_count"], 2);
    let items = links["items"].as_array().unwrap();
    assert_eq!(items[0]["record_id"], "2");
    assert_eq!(items[0]["namehash"], node(2));
    assert_eq!(items[0]["name"], "record2.eth");
    assert_eq!(items[0]["chain_position"]["block_number"], 14);
    assert_eq!(items[1]["record_id"], "3");
    assert_eq!(items[1]["default"], true);
    assert!(items[1].get("name").is_none());
    assert_eq!(items[1]["chain_position"]["block_number"], 16);
    let incremental = links.clone();
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        links_summary(&pool, RESOLVER).await?,
        incremental,
        "full rebuild drift"
    );
    run(&pool, 18, Some(18), RunMode::Redo).await?;
    assert_eq!(
        links_summary(&pool, RESOLVER).await?,
        incremental,
        "redo drift"
    );
    db.cleanup().await?;
    Ok(())
}

// Redo retracting the link of a node that no name and no resource consumes leaves
// nothing else to scope the resolver; the section's digest against the canonical
// link set is what rebuilds it. A second resolver isolates that: the redo window
// carries record events for the first resolver only.
#[tokio::test]
async fn resolver_links_summary_follows_a_retracted_link_through_redo() -> Result<()> {
    const OTHER: &str = "0x5555555555555555555555555555555555555555";
    let (db, pool) = database("record_id_link_retraction").await?;
    seed(&pool).await?;
    event(
        &pool,
        "upgrade-other",
        10,
        8,
        "Upgraded",
        None,
        json!({"proxy_address":OTHER,"implementation":IMPLEMENTATION}),
    )
    .await?;
    // Node 7 has no surface, no resource, and nothing reads its record.
    event(&pool,"link-orphan",17,9,"ResolverRecordLinked",None,json!({"source_event":"Linked","storage_model":"resolver_record_id","resolver":OTHER,"node":node(7),"resolver_record_id":"1","dns_encoded_name":"0x00"})).await?;
    run(&pool, 18, None, RunMode::Normal).await?;
    let before = links_summary(&pool, OTHER).await?;
    assert_eq!(before["count"], 1, "{before}");
    assert_eq!(before["items"][0]["namehash"], node(7));
    assert!(before["digest"].is_string());
    sqlx::query("DELETE FROM normalized_events WHERE event_identity = 'link-orphan'")
        .execute(&pool)
        .await?;
    run(&pool, 18, Some(18), RunMode::Redo).await?;
    let after = links_summary(&pool, OTHER).await?;
    assert_eq!(after["count"], 0, "{after}");
    assert_eq!(after["items"], json!([]));
    assert_ne!(after["digest"], before["digest"]);
    let redone = after.clone();
    run(&pool, 18, None, RunMode::Normal).await?;
    assert_eq!(
        links_summary(&pool, OTHER).await?,
        redone,
        "redo differs from rebuild"
    );
    db.cleanup().await?;
    Ok(())
}

// A node linked before its name was observed gains the name in the resolver's
// summary on the incremental run that discovers the surface -- on a resolver the
// run has no other reason to touch -- and a shadow surface never counts as a name.
#[tokio::test]
async fn resolver_links_summary_picks_up_a_name_discovered_later() -> Result<()> {
    let (db, pool) = database("record_id_link_late_name").await?;
    link_node_before_its_name(&pool).await?;
    assert_link_unnamed_at_12(&pool).await?;
    discover_late_name(&pool).await?;
    run(&pool, 13, Some(12), RunMode::Normal).await?;
    let links = links_summary(&pool, OTHER).await?;
    assert_eq!(late_item(&links)["name"], "record9.eth", "{links}");
    assert_eq!(
        late_item(&links)["logical_name_id"],
        format!("ens:{}", node(9))
    );
    // A shadow surface is not a name to show: demote it and rebuild.
    sqlx::query("UPDATE name_surfaces SET visibility_state = 'shadow', deactivation_reason = 'fixture', deactivated_at = now() WHERE logical_name_id = $1")
        .bind(format!("ens:{}", node(9))).execute(&pool).await?;
    run(&pool, 13, None, RunMode::Normal).await?;
    let links = links_summary(&pool, OTHER).await?;
    assert!(late_item(&links).get("name").is_none(), "{links}");
    db.cleanup().await?;
    Ok(())
}

// The same discovery on a run that also scopes a record inventory the resolver
// serves -- here record3.eth, pointed at the resolver and touched by an event of
// its own -- must still rebuild the summary. Such an inventory alone lets the run
// republish the resolver's old summary, since inventories do not change it, but a
// name for a linked node does.
#[tokio::test]
async fn resolver_links_summary_picks_up_a_name_discovered_beside_a_scoped_inventory() -> Result<()>
{
    let (db, pool) = database("record_id_link_late_name_inventory").await?;
    link_node_before_its_name(&pool).await?;
    event(
        &pool,
        "pointer3-other",
        11,
        6,
        "ResolverChanged",
        Some(3),
        json!({"resolver":OTHER,"node":node(3)}),
    )
    .await?;
    assert_link_unnamed_at_12(&pool).await?;
    let provenance = inventory(&pool, 3).await?["provenance"].clone();
    assert_eq!(provenance["resolver_address"], OTHER, "{provenance}");
    discover_late_name(&pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) SELECT 'preimage-three','ens',$1,'PreimageObserved','ens_v2_resolver_l1',1,manifest_id,$2,13,$3,$4,0,8,'ens_v2_resolver','canonical','{}'::jsonb,'{}'::jsonb FROM manifest_versions WHERE source_family='ens_v2_resolver_l1' AND chain_id=$2")
        .bind(format!("ens:{}", node(3))).bind(CHAIN).bind(hash(13)).bind(hash(1300)).execute(&pool).await?;
    run(&pool, 13, Some(12), RunMode::Normal).await?;
    let links = links_summary(&pool, OTHER).await?;
    assert_eq!(late_item(&links)["name"], "record9.eth", "{links}");
    db.cleanup().await?;
    Ok(())
}

const OTHER: &str = "0x5555555555555555555555555555555555555555";
/// The seed, a second record-ID resolver, and its link to node 9 before that node
/// has a surface.
async fn link_node_before_its_name(pool: &PgPool) -> Result<()> {
    seed(pool).await?;
    event(
        pool,
        "upgrade-other",
        10,
        8,
        "Upgraded",
        None,
        json!({"proxy_address":OTHER,"implementation":IMPLEMENTATION}),
    )
    .await?;
    event(pool,"link-late",11,5,"ResolverRecordLinked",None,json!({"source_event":"Linked","storage_model":"resolver_record_id","resolver":OTHER,"node":node(9),"resolver_record_id":"1","dns_encoded_name":"0x00"})).await?;
    // The root surface -- the empty name at the all-zero node -- exists on every ENS
    // chain; the default record's link must not pick it up as a name.
    sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens','',ARRAY[]::text[],'\\x00'::bytea,$2,ARRAY[]::text[],'fixture','active',$3,$4,10,'canonical')")
        .bind(format!("ens:{}", hash(0))).bind(hash(0)).bind(CHAIN).bind(hash(10)).execute(pool).await?;
    Ok(())
}

/// Node 9 has no surface yet, so its link is served by namehash alone.
async fn assert_link_unnamed_at_12(pool: &PgPool) -> Result<()> {
    run(pool, 12, None, RunMode::Normal).await?;
    let links = links_summary(pool, OTHER).await?;
    assert!(late_item(&links).get("name").is_none(), "{links}");
    let default = links_summary(pool, RESOLVER).await?;
    let default = default["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["default"] == true)
        .unwrap();
    assert!(default.get("name").is_none(), "{default}");
    Ok(())
}

/// Block 13 observes node 9's name: a surface plus the event that carries its logical name.
async fn discover_late_name(pool: &PgPool) -> Result<()> {
    let late = bigname_domain::normalization::normalize_name("record9.eth")?;
    let late_node = node(9);
    sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens','record9.eth',ARRAY['record9','eth'],$2,$3,ARRAY['a','b'],'fixture','active',$4,$5,13,'canonical')")
        .bind(format!("ens:{late_node}")).bind(late.dns_encoded_name.clone()).bind(&late_node).bind(CHAIN).bind(hash(13)).execute(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) SELECT 'preimage-late','ens',$1,'PreimageObserved','ens_v2_resolver_l1',1,manifest_id,$2,13,$3,$4,0,7,'ens_v2_resolver','canonical','{}'::jsonb,'{}'::jsonb FROM manifest_versions WHERE source_family='ens_v2_resolver_l1' AND chain_id=$2")
        .bind(format!("ens:{late_node}")).bind(CHAIN).bind(hash(13)).bind(hash(1300)).execute(pool).await?;
    Ok(())
}

fn late_item(links: &Value) -> Value {
    links["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["namehash"] == node(9))
        .cloned()
        .unwrap()
}

// A row built by an earlier deploy, before a section existed, is rebuilt on the
// next run even when nothing it cites changed and no event in the run's range
// names the resolver: the row's summary_version is what scopes it.
#[tokio::test]
async fn resolver_summary_reshaped_by_a_deploy_is_rebuilt_without_new_evidence() -> Result<()> {
    const OTHER: &str = "0x5555555555555555555555555555555555555555";
    let (db, pool) = database("record_id_summary_version").await?;
    seed(&pool).await?;
    event(
        &pool,
        "upgrade-other",
        10,
        8,
        "Upgraded",
        None,
        json!({"proxy_address":OTHER,"implementation":IMPLEMENTATION}),
    )
    .await?;
    run(&pool, 12, None, RunMode::Normal).await?;
    let built = summary(&pool, OTHER).await?;
    assert!(built["links"].is_object(), "{built}");
    assert_eq!(built["summary_version"], json!(1), "{built}");
    // The database an older deploy left behind: no links section, no version.
    sqlx::query("UPDATE resolver_current SET declared_summary = declared_summary - 'links' - 'summary_version' WHERE resolver_address = $1")
        .bind(OTHER).execute(&pool).await?;
    // Nothing in 13..=18 names OTHER.
    run(&pool, 18, Some(12), RunMode::Normal).await?;
    assert_eq!(
        summary(&pool, OTHER).await?,
        built,
        "stale row was not rebuilt"
    );
    // A row on the current version and untouched by the range is carried, not rebuilt.
    sqlx::query("UPDATE resolver_current SET declared_summary = declared_summary || '{\"fixture_marker\": true}' WHERE resolver_address = $1")
        .bind(OTHER).execute(&pool).await?;
    run(&pool, 18, Some(18), RunMode::Normal).await?;
    assert_eq!(
        summary(&pool, OTHER).await?["fixture_marker"],
        json!(true),
        "a current row was rebuilt without a reason"
    );
    db.cleanup().await?;
    Ok(())
}

// A grant scoped to a setter argument -- the resource is the keccak of the argument --
// keeps the interpreter's decoded selector on the permission row, so reads can say
// which record the resource is about; an argument the interpreter never saw leaves
// the scope alone.
// (upstream: .refs/ens_v2/contracts/src/resolver/PermissionedResolver.sol:L307-L338 @ ens_v2@a971bd64)
#[tokio::test]
async fn record_resolver_permission_rows_keep_the_decoded_argument_selector() -> Result<()> {
    let (db, pool) = database("record_id_permission_selector").await?;
    seed(&pool).await?;
    let grant = |resource: i64, selector: Value| {
        json!({
            "subject": "0x00000000000000000000000000000000000000ee",
            "scope": {"kind": "resolver", "chain_id": CHAIN, "resolver_address": RESOLVER},
            "effective_powers": ["set_text"],
            "grant_source": {"kind": "raw_log", "source_event": "EACRolesChanged",
                "upstream_resource": hash(resource), "root_resource": false,
                "changed_powers": ["set_text"]},
            "revocation_source": null, "inheritance_path": [], "transfer_behavior": {},
            "source_event": "EACRolesChanged", "upstream_resource": hash(resource),
            "resource": hash(resource), "root_resource": false, "selector": selector,
            "storage_model": "resolver_record_id", "resolver": RESOLVER,
            "resolver_record_id": "0", "record_key": "permission",
        })
    };
    for (identity, resource, selector) in [
        (
            "grant-text",
            501,
            json!({"kind": "text", "key": "url", "hash": hash(501)}),
        ),
        (
            "grant-unknown",
            502,
            json!({"kind": "resource", "key": null, "hash": null}),
        ),
        // A node-keyed named-resource selector hashes the key, not the resource.
        // (upstream: .refs/ens_v2_sepolia_20260629/contracts/src/resolver/PermissionedResolver.sol:L144-L153 @ ens_v2_sepolia_20260629@ccaeb58)
        (
            "grant-node-keyed",
            503,
            json!({"kind": "text", "key": "url", "hash": hash(777)}),
        ),
    ] {
        sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,10,'canonical')")
            .bind(resource_uuid(resource)).bind(CHAIN).bind(hash(10)).execute(&pool).await?;
        sqlx::query("INSERT INTO normalized_events (event_identity,namespace,resource_id,event_kind,source_family,manifest_version,source_manifest_id,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref) SELECT $1,'ens',$2::uuid,'PermissionChanged','ens_v2_resolver_l1',1,manifest_id,$3,11,$4,$5,0,$6,'ens_v2_permissions','canonical',$7,'{}'::jsonb FROM manifest_versions WHERE source_family='ens_v2_resolver_l1' AND chain_id=$3")
            .bind(identity).bind(resource_uuid(resource)).bind(CHAIN).bind(hash(11)).bind(hash(1100)).bind(resource).bind(grant(resource, selector)).execute(&pool).await?;
    }
    run(&pool, 12, None, RunMode::Normal).await?;
    let selector_of = |resource: i64| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, Value>(
                "SELECT scope_detail FROM permissions_current WHERE resource_id = $1::uuid",
            )
            .bind(resource_uuid(resource))
            .fetch_one(&pool)
            .await
        }
    };
    let described = selector_of(501).await?;
    assert_eq!(
        described["resource_selector"],
        json!({"kind": "text", "key": "url", "hash": hash(501)})
    );
    assert_eq!(described["kind"], "resolver");
    assert_eq!(described["resolver_address"], RESOLVER);
    let undescribed = selector_of(502).await?;
    assert!(
        undescribed.get("resource_selector").is_none(),
        "{undescribed}"
    );
    let node_keyed = selector_of(503).await?;
    assert!(
        node_keyed.get("resource_selector").is_none(),
        "{node_keyed}"
    );
    db.cleanup().await?;
    Ok(())
}

async fn summary(pool: &PgPool, resolver: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT declared_summary FROM resolver_current WHERE resolver_address = $1",
    )
    .bind(resolver)
    .fetch_one(pool)
    .await?)
}

async fn links_summary(pool: &PgPool, resolver: &str) -> Result<Value> {
    Ok(sqlx::query_scalar(
        "SELECT declared_summary -> 'links' FROM resolver_current WHERE resolver_address = $1",
    )
    .bind(resolver)
    .fetch_one(pool)
    .await?)
}

fn resource_uuid(n: i64) -> String {
    format!("77000000-0000-0000-0000-{n:012}")
}

async fn assert_history(pool: &PgPool, id: i64, present: &[&str], absent: &[&str]) -> Result<()> {
    let page = bigname_storage::load_name_history_page(
        pool,
        &format!("ens:{}", node(id)),
        &[resource(id).parse()?],
        bigname_storage::HistoryScope::Both,
        true,
        None,
        100,
        bigname_storage::HistorySummaryMode::None,
        &bigname_storage::HistoryPageOptions {
            event_kinds: vec!["RecordChanged".into()],
            ..Default::default()
        },
        None,
    )
    .await?;
    let identities: Vec<_> = page
        .rows
        .iter()
        .map(|event| event.event_identity.as_str())
        .collect();
    for identity in present {
        assert_eq!(
            identities.iter().filter(|value| *value == identity).count(),
            1,
            "expected exactly one {identity}: {identities:?}"
        );
    }
    for identity in absent {
        assert!(
            !identities.contains(identity),
            "unexpected {identity}: {identities:?}"
        );
    }
    Ok(())
}

async fn run(pool: &PgPool, target: i64, previous: Option<i64>, mode: RunMode) -> Result<()> {
    run_expecting(pool, target, previous, mode, &Expectations::none()).await
}

/// [`run`] whose family comparison expects `expected`.
async fn run_expecting(
    pool: &PgPool,
    target: i64,
    previous: Option<i64>,
    mode: RunMode,
    expected: &Expectations,
) -> Result<()> {
    let outcome = Engine::new(pool.clone())
        .run_batch(BatchRequest {
            chain_id: CHAIN.to_owned(),
            target_block: target,
            affected_from_block: previous.map_or(10, |p| (p + 1).min(target)),
            affected_to_block: target,
            resume_current: previous.map(|p| Marker {
                number: p,
                hash: hash(p),
            }),
            mode,
        })
        .await?;
    bounded_attribution::assert_bounded_record_attribution_matches_inventory(pool).await?;
    family_shadow::compare_family_reads_at(pool, &outcome.current, expected).await?;
    Ok(())
}

/// The address the ENSIP-19 case writes as `address_bytes_hex` only.
const ADDRESS_BYTES_ONLY: &str = "0x3333333333333333333333333333333333333333";
async fn inventory(pool: &PgPool, id: i64) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_build_object('entries',entries,'last_change',last_change,'boundary',record_version_boundary,'provenance',provenance,'support',support_status) FROM record_inventory_current WHERE resource_id=$1::uuid")
        .bind(resource(id)).fetch_one(pool).await?)
}
async fn assert_text(pool: &PgPool, id: i64, value: &str) -> Result<()> {
    let row = inventory(pool, id).await?;
    assert_eq!(row["support"], "supported", "{row}");
    let item = row["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["record_key"] == "text:url")
        .unwrap();
    assert_eq!(item["value"], value, "resource{id}: {row}");
    Ok(())
}
async fn snapshot(pool: &PgPool) -> Result<Value> {
    Ok(sqlx::query_scalar("SELECT jsonb_agg(jsonb_build_object('resource',resource_id,'entries',entries,'boundary',record_version_boundary,'last_change',last_change,'provenance',provenance,'positions',chain_positions) ORDER BY resource_id) FROM record_inventory_current").fetch_one(pool).await?)
}

async fn event(
    pool: &PgPool,
    identity: &str,
    block: i64,
    log: i64,
    kind: &str,
    name: Option<i64>,
    mut after: Value,
) -> Result<()> {
    let family = if kind == "ResolverChanged" {
        "ens_v2_registry_l1"
    } else {
        "ens_v2_resolver_l1"
    };
    let derivation = match kind {
        "ResolverChanged" => "ens_v2_registry_resource_surface",
        "Upgraded" => "proxy_upgrade",
        _ => "ens_v2_resolver",
    };
    let emitter = if kind == "ResolverChanged" {
        REGISTRY
    } else {
        after["resolver"]
            .as_str()
            .or_else(|| after["proxy_address"].as_str())
            .unwrap_or(RESOLVER)
    }
    .to_owned();
    let resolver_id = resource(if emitter == RESOLVER { 900 } else { 901 });
    if kind == "ResolverChanged" {
        after = json!({"source_event":"ResolverUpdated", "resolver":after["resolver"], "sender":REGISTRY, "token_id":hash(name.unwrap())});
    } else if kind == "Upgraded" {
        after["source_event"] = json!("Upgraded");
    } else {
        after["resolver_contract_instance_id"] = json!(resolver_id);
    }
    let scope = if after["storage_model"] == "resolver_record_id" {
        if kind == "ResolverRecordLinked" {
            format!("{resolver_id}:link:{}", after["node"].as_str().unwrap())
        } else {
            format!(
                "{resolver_id}:record:{}:{}",
                after["resolver_record_id"].as_str().unwrap(),
                after["record_key"].as_str().unwrap()
            )
        }
    } else {
        format!(
            "{emitter}:{}:{}:-:{}",
            after["node"].as_str().unwrap_or("-"),
            after["token_id"].as_str().unwrap_or("-"),
            after["record_key"]
                .as_str()
                .or_else(|| after["source_event"].as_str())
                .unwrap_or(kind)
        )
    };
    let facet = match kind {
        "RecordChanged" => "records",
        "ResolverChanged" => "resolver",
        _ => kind,
    };
    let state_key = format!(
        "ens:{family}:{}:{}:{facet}:{scope}",
        name.map(|n| format!("ens:{}", node(n)))
            .unwrap_or_else(|| "-".into()),
        name.map(resource).unwrap_or_else(|| "-".into())
    );
    let before: Value = sqlx::query_scalar("SELECT after_state FROM normalized_events WHERE raw_fact_ref->>'interpreter_state_key'=$1 ORDER BY block_number DESC, transaction_index DESC, log_index DESC LIMIT 1")
        .bind(&state_key).fetch_optional(pool).await?.unwrap_or_else(||json!({}));
    let manifest_id: i64 = sqlx::query_scalar(
        "SELECT manifest_id FROM manifest_versions WHERE source_family=$1 AND chain_id=$2",
    )
    .bind(family)
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    let source_ref = json!({"kind":"raw_log","chain_id":CHAIN,"block_hash":hash(block),"block_number":block,"transaction_hash":hash(block*100),"transaction_index":0,"log_index":log,"emitting_address":emitter,"state_scope":scope,"interpreter_state_key":state_key});
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,logical_name_id,resource_id,event_kind,source_family,manifest_version,chain_id,block_number,block_hash,transaction_hash,transaction_index,log_index,derivation_kind,canonicality_state,after_state,raw_fact_ref,before_state,source_manifest_id) VALUES ($1,'ens',$2,$3::uuid,$4,$5,1,$6,$7,$8,$9,0,$10,$13,'canonical',$11,$12,$14,$15)")
        .bind(identity).bind(name.map(|n|format!("ens:{}",node(n)))).bind(name.map(resource)).bind(kind).bind(family).bind(CHAIN).bind(block).bind(hash(block)).bind(hash(block*100)).bind(log).bind(after).bind(source_ref).bind(derivation).bind(before).bind(manifest_id).execute(pool).await?;
    Ok(())
}
async fn link(
    pool: &PgPool,
    identity: &str,
    block: i64,
    log: i64,
    n: Option<i64>,
    record: i64,
) -> Result<()> {
    event(pool,identity,block,log,"ResolverRecordLinked",None,json!({"source_event":"Linked","storage_model":"resolver_record_id","resolver":RESOLVER,"node":n.map_or_else(||hash(0),node),"resolver_record_id":record.to_string(),"dns_encoded_name":n.map_or_else(||"0x00".to_owned(),|n|format!("0x{}",alloy_primitives::hex::encode(bigname_domain::normalization::normalize_name(&format!("record{n}.eth")).unwrap().dns_encoded_name)))})).await
}
async fn text(
    pool: &PgPool,
    identity: &str,
    block: i64,
    log: i64,
    record: i64,
    value: &str,
) -> Result<()> {
    event(pool,identity,block,log,"RecordChanged",None,json!({"source_event":"TextUpdated","storage_model":"resolver_record_id","resolver":RESOLVER,"resolver_record_id":record.to_string(),"record_key":"text:url","record_family":"text","selector_key":"url","value_retained":true,"value":value,"value_length":value.len()})).await
}
async fn seed(pool: &PgPool) -> Result<()> {
    for n in 10..=18 {
        sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')").bind(CHAIN).bind(hash(n)).bind(n).execute(pool).await?;
    }
    let payload = json!({"deployment_epoch":"record_id_fixture","resolver_implementations":[{"role":"permissioned_resolver","address":IMPLEMENTATION}],"contracts":[],"capability_flags":{},"abi":{"events":[{"name":"Linked","fragment":"event Linked(uint256 indexed recordId, bytes32 indexed node, bytes name)","normalized_events":["ResolverRecordLinked","PreimageObserved"]}]}});
    let manifest: i64 = sqlx::query_scalar("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens','ens_v2_resolver_l1',$1,'record_id_fixture','active','fixture','fixture/record-id.toml',$2) RETURNING manifest_id").bind(CHAIN).bind(&payload).fetch_one(pool).await?;
    sqlx::query("INSERT INTO normalized_events (event_identity,namespace,event_kind,source_family,manifest_version,source_manifest_id,chain_id,derivation_kind,canonicality_state,after_state) VALUES ('manifest','ens','SourceManifestUpdated','ens_v2_resolver_l1',1,$1,$2,'manifest_sync','canonical',$3)").bind(manifest).bind(CHAIN).bind(json!({"rollout_status":"active","normalizer_version":"fixture","manifest_payload":payload})).execute(pool).await?;
    sqlx::query("INSERT INTO manifest_versions (manifest_version,namespace,source_family,chain_id,deployment_label,rollout_status,normalizer_version,file_path,manifest_payload) VALUES (1,'ens','ens_v2_registry_l1',$1,'record_id_fixture','active','fixture','fixture/registry.toml','{}')").bind(CHAIN).execute(pool).await?;
    for n in 1..=3 {
        let normalized = bigname_domain::normalization::normalize_name(&format!("record{n}.eth"))?;
        let labelhashes: Vec<_> = normalized
            .normalized_labels
            .iter()
            .map(|label| format!("{:#x}", alloy_primitives::keccak256(label)))
            .collect();
        sqlx::query("INSERT INTO name_surfaces (logical_name_id,namespace,raw_name,raw_labels,dns_encoded_name,namehash,labelhashes,normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state) VALUES ($1,'ens',$2,string_to_array($2,'.'),$6,$3,$7,'fixture','active',$4,$5,10,'canonical')").bind(format!("ens:{}",node(n))).bind(format!("record{n}.eth")).bind(node(n)).bind(CHAIN).bind(hash(10)).bind(normalized.dns_encoded_name).bind(labelhashes).execute(pool).await?;
        sqlx::query("INSERT INTO resources (resource_id,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3,10,'canonical')").bind(resource(n)).bind(CHAIN).bind(hash(10)).execute(pool).await?;
        sqlx::query("INSERT INTO surface_bindings (surface_binding_id,logical_name_id,resource_id,binding_kind,authority_arm,active_from,chain_id,block_hash,block_number,canonicality_state) VALUES ($1::uuid,$2,$3::uuid,'declared_registry_path','ens_v2',to_timestamp(10),$4,$5,10,'canonical')").bind(resource(100+n)).bind(format!("ens:{}",node(n))).bind(resource(n)).bind(CHAIN).bind(hash(10)).execute(pool).await?;
        event(
            pool,
            &format!("pointer{n}"),
            10,
            n,
            "ResolverChanged",
            Some(n),
            json!({"resolver":RESOLVER,"node":node(n)}),
        )
        .await?;
    }
    event(
        pool,
        "upgrade",
        10,
        4,
        "Upgraded",
        None,
        json!({"proxy_address":RESOLVER,"implementation":IMPLEMENTATION}),
    )
    .await?;
    // Writes precede later links and need not have name/resource attribution.
    text(pool, "record-one", 10, 5, 1, "one").await?;
    text(pool, "record-two", 10, 6, 2, "default").await?;
    text(pool, "record-three", 10, 7, 3, "new default").await?;
    link(pool, "link-a-one", 11, 0, Some(1), 1).await?;
    link(pool, "link-b-one", 11, 1, Some(2), 1).await?;
    link(pool, "default-two", 12, 0, None, 2).await?;
    text(pool, "shared-update", 13, 0, 1, "shared").await?;
    link(pool, "link-b-two", 14, 0, Some(2), 2).await?;
    link(pool, "unlink-a", 15, 0, Some(1), 0).await?;
    link(pool, "default-three", 16, 0, None, 3).await?;
    text(pool, "empty-text", 17, 0, 2, "").await?;
    for (log, family, key, extra) in [
        (
            0,
            "addr",
            "addr:60",
            json!({"selector_key":"60","coin_type":"60","address_bytes_hex":"0x"}),
        ),
        (
            1,
            "contenthash",
            "contenthash",
            json!({"selector_key":null,"contenthash_hex":"0x"}),
        ),
    ] {
        let mut after = json!({"source_event":if family == "addr" {"AddressUpdated"} else {"ContenthashUpdated"},"storage_model":"resolver_record_id","resolver":RESOLVER,"resolver_record_id":"2","record_family":family,"record_key":key,"value_retained":false});
        after
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        event(
            pool,
            &format!("empty-{family}"),
            18,
            log,
            "RecordChanged",
            None,
            after,
        )
        .await?;
    }
    Ok(())
}

async fn database(name: &str) -> Result<(TestDatabase, PgPool)> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name.to_string())).await?;
    let pool = database.pool().clone();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let mut transaction = pool.begin().await?;
    raw_sql(&format!("CREATE SCHEMA bigname_phase; ALTER DATABASE \"{}\" SET search_path TO bigname_phase, public; SET LOCAL search_path TO bigname_phase, public", database_name.replace('"', "\"\""))).execute(&mut *transaction).await?;
    for script in [
        include_str!("../../../schema-v2/baseline/01_chain.sql"),
        include_str!("../../../schema-v2/baseline/02_raw_facts.sql"),
        include_str!("../../../schema-v2/baseline/03_identity.sql"),
        include_str!("../../../schema-v2/baseline/04_manifests.sql"),
        include_str!("../../../schema-v2/baseline/05_normalized_events.sql"),
        include_str!("../../../schema-v2/baseline/06_projections.sql"),
        include_str!("../../../schema-v2/baseline/07_labels.sql"),
        include_str!("../../../schema-v2/baseline/08_heartbeats.sql"),
        include_str!("../../../schema-v2/baseline/09_divergence.sql"),
        include_str!("../../../schema-v2/baseline/10_phase_state.sql"),
    ] {
        raw_sql(script).execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    pool.set_connect_options(
        pool.connect_options()
            .as_ref()
            .clone()
            .options([("search_path", "bigname_phase,public")]),
    );
    let mut connections = Vec::new();
    for _ in 0..pool.options().get_max_connections() {
        connections.push(pool.acquire().await?);
    }
    for connection in &mut connections {
        sqlx::query("SET search_path TO bigname_phase, public")
            .execute(&mut **connection)
            .await?;
    }
    Ok((database, pool))
}

/// Optimism's coin (`0x80000000 | 10`), an ENSIP-19 fallback target the comparison does not add.
const OPTIMISM_COIN: &str = "2147483658";
/// An exact address for the fallback cases that differs from the default.
const EXACT_OTHER: &str = "0x8888888888888888888888888888888888888888";

/// Today's and the family's names for `address` and `coin`, which must agree; returns how many.
async fn probe(pool: &PgPool, address: &str, coin: &str) -> Result<usize> {
    let read = |family: bool| async move {
        let arguments = (
            None,
            bigname_storage::AddressNamesCurrentDedupe::Surface,
            None,
            None,
            bigname_storage::AddressNamesCurrentSort::Name,
            bigname_storage::AddressNamesCurrentOrder::Asc,
            None,
            50,
        );
        if family {
            bigname_storage::families::records::load_family_address_records_page(
                pool,
                address,
                coin,
                arguments.0,
                arguments.1,
                arguments.2,
                arguments.3,
                arguments.4,
                arguments.5,
                arguments.6,
                arguments.7,
            )
            .await
        } else {
            bigname_storage::load_address_records_current_page(
                pool,
                address,
                coin,
                arguments.0,
                arguments.1,
                arguments.2,
                arguments.3,
                arguments.4,
                arguments.5,
                arguments.6,
                arguments.7,
            )
            .await
        }
    };
    let (today, family) = (read(false).await?, read(true).await?);
    let differences = bigname_storage::families::records::compare_address_records(
        &today.entries,
        &family.entries,
    );
    assert!(
        differences.is_empty(),
        "{address} coin {coin}: {differences:#?}"
    );
    Ok(today.entries.len())
}

// ENSIP-19 fallback against exact records, table-driven over one name with a default address D.
// Each case adds exact records after the default and states how many names each probe finds:
// D for ETH, Base, Optimism and the ineligible coin 0, then the exact address where it differs.
// An exact record with an address shadows the default for its coin, and so does one whose value
// is not retained (unsupported: nothing shows its stored bytes are empty); a cleared exact record
// falls back to the default. The resolver falls back only when the coin's stored bytes are empty
// and the coin is an EVM coin.
// (upstream: .refs/ens_v2/contracts/src/resolver/AbstractRecordResolver.sol:L172-L178 @ ens_v2@a971bd64)
// Every run also compares every stored key, with coin 60 and Base added for D, and each probe
// requires both readers to list the same names.
#[tokio::test]
async fn exact_records_and_the_default_address_answer_alike_through_the_families() -> Result<()> {
    type Record = (&'static str, Value);
    type Case = (
        &'static str,
        Vec<Record>,
        [usize; 4],
        Option<(&'static str, &'static str, usize)>,
    );
    let exact = |coin: &'static str, payload: Value| -> Record { (coin, payload) };
    let d = ADDRESS_BYTES_ONLY;
    // (case, exact records, names for D at [60, Base, Optimism, 0], exact probe)
    let cases: Vec<Case> = vec![
        ("default_only", vec![], [1, 1, 1, 0], None),
        (
            "overlapping_eth",
            vec![exact("60", json!({"value": d}))],
            [1, 1, 1, 0],
            None,
        ),
        (
            "differing_eth",
            vec![exact("60", json!({"value": EXACT_OTHER}))],
            [0, 1, 1, 0],
            Some((EXACT_OTHER, "60", 1)),
        ),
        (
            "cleared_eth",
            vec![exact("60", json!({"value": "0x"}))],
            [1, 1, 1, 0],
            None,
        ),
        (
            "unsupported_eth",
            vec![exact("60", json!({"value_retained": false}))],
            [0, 1, 1, 0],
            None,
        ),
        (
            "differing_optimism",
            vec![exact(OPTIMISM_COIN, json!({"value": EXACT_OTHER}))],
            [1, 1, 0, 0],
            Some((EXACT_OTHER, OPTIMISM_COIN, 1)),
        ),
        (
            "ineligible_coin",
            vec![exact("0", json!({"value": "0x0014abcd"}))],
            [1, 1, 1, 0],
            None,
        ),
    ];
    for (case, records, names, exact_probe) in cases {
        let (db, pool, target, resolver) = public_default(&format!("ensip19_{case}")).await?;
        for (index, (coin, payload)) in records.iter().enumerate() {
            let mut after = json!({
                "source_event":"AddressChanged","resolver":resolver,"node":node(1),
                "record_family":"addr","record_key":format!("addr:{coin}"),
                "selector_key":coin,"coin_type":coin,
            });
            for (key, value) in payload.as_object().unwrap() {
                after[key] = value.clone();
            }
            event(
                &pool,
                &format!("exact-{index}"),
                target,
                i64::try_from(index)? + 2,
                "RecordChanged",
                None,
                after,
            )
            .await?;
        }
        run(&pool, target, None, RunMode::Normal).await?;
        let mut found = [0; 4];
        for (slot, coin) in ["60", "2147492101", OPTIMISM_COIN, "0"].iter().enumerate() {
            found[slot] = probe(&pool, d, coin).await?;
        }
        assert_eq!(found, names, "{case}");
        if let Some((address, coin, count)) = exact_probe {
            assert_eq!(probe(&pool, address, coin).await?, count, "{case}");
        }
        db.cleanup().await?;
    }
    Ok(())
}

/// A name pointing at the official Sepolia PublicResolverV2 with an ENSIP-19 default address
/// (`ADDRESS_BYTES_ONLY`, as raw bytes) written at the target block; returns the database, the
/// target block and the resolver address.
async fn public_default(name: &str) -> Result<(TestDatabase, PgPool, i64, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let repository = bigname_manifests::load_repository(root.join("manifests/sepolia"))?;
    let manifest = &repository
        .manifests()
        .iter()
        .find(|m| m.manifest.source_family == "ens_v2_resolver_l1")
        .unwrap()
        .manifest;
    let direct = manifest
        .contracts
        .iter()
        .find(|c| c.role == "public_resolver_v2")
        .unwrap();
    let target = i64::try_from(direct.start_block.unwrap())? + 1;
    let address = direct.address.to_ascii_lowercase();
    let payload = serde_json::to_value(manifest)?;
    let (db, pool) = database(name).await?;
    seed(&pool).await?;
    let _: i64 = sqlx::query_scalar("UPDATE manifest_versions SET manifest_payload=$1, normalizer_version=$2, deployment_label=$3 WHERE source_family='ens_v2_resolver_l1' RETURNING manifest_id")
        .bind(&payload).bind(&manifest.normalizer_version).bind(&manifest.deployment_epoch).fetch_one(&pool).await?;
    sqlx::query("UPDATE normalized_events SET after_state=$1 WHERE event_identity='manifest'")
        .bind(json!({"rollout_status":"active","normalizer_version":manifest.normalizer_version,"manifest_payload":payload})).execute(&pool).await?;
    sqlx::query("INSERT INTO chain_lineage (chain_id,block_hash,block_number,block_timestamp,canonicality_state) VALUES ($1,$2,$3,to_timestamp($3::double precision),'canonical')")
        .bind(CHAIN).bind(hash(target)).bind(target).execute(&pool).await?;
    event(
        &pool,
        "public-pointer",
        target,
        0,
        "ResolverChanged",
        Some(1),
        json!({"resolver":address,"node":node(1)}),
    )
    .await?;
    let value = ADDRESS_BYTES_ONLY;
    event(
        &pool,
        "public-default",
        target,
        1,
        "RecordChanged",
        None,
        json!({
            "source_event":"AddressChanged","resolver":address,"node":node(1),
            "record_family":"addr","record_key":"addr:2147483648","selector_key":"2147483648",
            "coin_type":"2147483648","value_retained":false,"address_bytes_hex":value
        }),
    )
    .await?;
    Ok((db, pool, target, address))
}

#[tokio::test]
async fn official_sepolia_direct_resolver_projects_ensip19_default_for_missing_eth_address()
-> Result<()> {
    use bigname_domain::resolver_read::{IndexedRecordStatus, evaluate_indexed_record};
    let (db, pool, target, address) = public_default("record_id_public_default").await?;
    let value = ADDRESS_BYTES_ONLY;
    // Step 2 indexes the address this `AddressChanged` carries only as `address_bytes_hex`, so
    // the comparison expects nothing.
    let expected = Expectations::none();
    run_expecting(&pool, target, None, RunMode::Normal, &expected).await?;
    // The only stored coin is the default one, so the comparison adds coin 60 and one other EVM
    // coin for the address: each answers through the default, and both readers list the name.
    let report = bigname_storage::families::records::compare_family_reads(
        &pool,
        CHAIN,
        Some((target, hash(target))),
        1,
    )
    .await?;
    assert_eq!(
        (report.address_pages, report.address_entries),
        (3, 3),
        "{report:#?}"
    );
    // A direct node-keyed declaration has no link state, so the section is unsupported
    // by kind rather than reported empty.
    assert_eq!(
        links_summary(&pool, &address).await?,
        json!({"status": "unsupported", "unsupported_reason": "record_links_not_applicable"})
    );
    let row = inventory(&pool, 1).await?;
    assert_eq!(row["support"], "supported", "{row}");
    assert_eq!(
        row["provenance"]["read_rules"],
        json!([{"kind":"ensip19_default_address","source_record_key":"addr:2147483648"}])
    );
    assert!(
        !row["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["record_key"] == "addr:60")
    );
    let answer = evaluate_indexed_record(
        &row["entries"],
        &row["provenance"],
        &json!({"status":"projected"}),
        "addr:60",
        "addr",
        Some("60"),
    );
    assert_eq!(answer.status, IndexedRecordStatus::Success);
    assert_eq!(answer.value, Some(json!(value)));
    // A supported direct PublicResolverV2 inventory still has no admitted ABI event: the list is
    // unavailable, not empty.
    assert_eq!(
        abi_content_types(&pool, 1).await?,
        bigname_storage::AbiContentTypes::Unavailable(
            bigname_storage::AbiContentTypesUnavailable::ObservationsNotSupported
        )
    );
    db.cleanup().await?;
    Ok(())
}
