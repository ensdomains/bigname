//! The served name of a surface with and without raw bytes: the pure functions, the SQL
//! expression against `label_preimages`, and the compositor reading a surface without bytes.
use alloy_primitives::{B256, hex, keccak256};
use anyhow::Result;
use bigname_domain::normalization::normalize_name;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgPool, raw_sql};

use super::{
    bracketed_label, composed_surface_sql, label_hash, parse, placeholder_label, placeholder_name,
    rendered_name_sql, rendered_name_sql_unqualified,
};
use crate::families::{
    control::lifecycle::ETH_LABELHASH,
    name::{
        CoverageShape, compose_name_summaries, load_composed_base, load_family_name, loaders,
        publication_on,
    },
};

const CHAIN: &str = "ethereum-sepolia";

fn labelhash(label: &[u8]) -> String {
    format!("{:#x}", keccak256(label))
}

fn bracket(labelhash: &str) -> String {
    placeholder_label(labelhash).expect("a labelhash")
}

/// The node of a label-hash path in name order.
fn node(labelhashes: &[String]) -> String {
    let node = labelhashes.iter().rev().fold(B256::ZERO, |parent, hash| {
        let hash: B256 = hash.parse().expect("a labelhash");
        keccak256([parent.as_slice(), hash.as_slice()].concat())
    });
    format!("{node:#x}")
}

#[test]
fn a_placeholder_is_the_lowercase_hash_in_brackets() {
    let hash = format!("0x{}", "AB".repeat(32));
    let lower = "ab".repeat(32);
    assert_eq!(placeholder_label(&hash), Some(format!("[{lower}]")));
    assert_eq!(placeholder_label(&lower), None);
    assert_eq!(placeholder_label("0xab"), None);
    assert_eq!(placeholder_label(&format!("0x{}", "zz".repeat(32))), None);
    assert_eq!(
        placeholder_name(&[hash.clone(), ETH_LABELHASH.to_owned()]).unwrap(),
        format!("[{lower}].[{}]", &ETH_LABELHASH[2..])
    );
    assert!(placeholder_name(&[]).is_err());
    assert!(placeholder_name(&[hash, "eth".to_owned()]).is_err());
}

#[test]
fn a_label_stands_for_its_bracketed_hash_or_the_hash_of_its_text() {
    let digits = "ab".repeat(32);
    assert_eq!(bracketed_label(&format!("[{digits}]")), Some(&*digits));
    assert_eq!(
        bracketed_label(&format!("[{}]", digits.to_uppercase())),
        Some(&*digits.to_uppercase())
    );
    for not_bracketed in ["[ab]", "alice", &digits, &format!("[{digits}"), "[]"] {
        assert_eq!(bracketed_label(not_bracketed), None, "{not_bracketed}");
    }
    assert_eq!(label_hash(&format!("[{digits}]")), [0xab; 32]);
    assert_eq!(label_hash("eth"), keccak256(b"eth").0);
    assert_eq!(
        format!("0x{}", hex::encode(label_hash("eth"))),
        ETH_LABELHASH
    );
    assert_eq!(label_hash("[ab]"), keccak256(b"[ab]").0);
}

#[test]
fn parsing_a_name_without_brackets_is_normalizing_it() {
    for name in [
        "eth",
        "alice.eth",
        "Alice.ETH",
        "sub.alice.base.eth",
        "\u{1f468}\u{200d}\u{1f4bb}.eth",
        "1\u{fe0f}\u{20e3}.eth",
        "xn--ls8h.eth",
        "a_b.eth",
        "_tcp.alice.eth",
        "\u{3a3}\u{3a3}.eth",
        "faß.eth",
        "",
        ".",
        "alice..eth",
        ".eth",
        "alice.eth.",
        "al ice.eth",
        "[ab].eth",
        "alice].eth",
        "a\u{200d}b.eth",
    ] {
        match (parse(name), normalize_name(name)) {
            (Ok(parsed), Ok(normalized)) => {
                assert_eq!(parsed.normalized_name, normalized.normalized_name, "{name}");
                assert_eq!(
                    parsed.canonical_display_name, normalized.canonical_display_name,
                    "{name}"
                );
                assert_eq!(parsed.labels, normalized.normalized_labels, "{name}");
                let hashes: Vec<[u8; 32]> = normalized
                    .normalized_labels
                    .iter()
                    .map(|label| keccak256(label.as_bytes()).0)
                    .collect();
                assert_eq!(parsed.labelhashes, hashes, "{name}");
            }
            (Err(parsed), Err(normalized)) => assert_eq!(parsed, normalized, "{name}"),
            (parsed, normalized) => panic!("{name}: {parsed:?} against {normalized:?}"),
        }
    }
}

#[test]
fn parsing_keeps_bracketed_labels_and_normalizes_the_others() {
    let digits = "ab".repeat(32);
    let parsed = parse(&format!("Sub.[{digits}].eth")).unwrap();
    assert_eq!(parsed.normalized_name, format!("sub.[{digits}].eth"));
    assert_eq!(parsed.canonical_display_name, parsed.normalized_name);
    assert_eq!(parsed.labels, ["sub", &format!("[{digits}]"), "eth"]);
    assert_eq!(
        parsed.labelhashes,
        [keccak256(b"sub").0, [0xab; 32], keccak256(b"eth").0]
    );
    assert_eq!(parse(&format!("[{digits}]")).unwrap().labels.len(), 1);
    assert!(parse(&format!("[{}].eth", digits.to_uppercase())).is_err());
    assert!(parse(&format!("[{digits}]..eth")).is_err());
    assert!(parse(&format!("al ice.[{digits}].eth")).is_err());
    assert!(parse(&format!("[ab].[{digits}].eth")).is_err());
}

/// The surfaces of the fixture. `alice.eth` and `named.eth` store their bytes; `named.eth`'s
/// label hashes do not spell its name, so an answer built from them would be visible. The others
/// store none.
struct Fixture {
    eth: String,
    alice: String,
    usable: String,
    failing: String,
    undecodable: String,
    missing: String,
    named: String,
    child: String,
    mixed: String,
    unknown: String,
}

impl Fixture {
    fn new() -> Self {
        let eth = labelhash(b"eth");
        let alice = labelhash(b"alice");
        let usable = labelhash(b"known");
        let failing = labelhash(b"Known");
        let undecodable = labelhash(&[0xff, 0xfe]);
        let missing = format!("0x{}", "ab".repeat(32));
        Self {
            named: format!("ens:{}", node(&[missing.clone(), eth.clone()])),
            child: format!(
                "ens:{}",
                node(&[usable.clone(), alice.clone(), eth.clone()])
            ),
            mixed: format!(
                "ens:{}",
                node(&[
                    usable.clone(),
                    failing.clone(),
                    undecodable.clone(),
                    missing.clone(),
                    eth.clone()
                ])
            ),
            unknown: format!("ens:{}", node(&[missing.clone(), missing.clone()])),
            eth,
            alice,
            usable,
            failing,
            undecodable,
            missing,
        }
    }

    fn id(&self, labelhashes: &[&String]) -> String {
        let path: Vec<String> = labelhashes.iter().map(|hash| (*hash).clone()).collect();
        format!("ens:{}", node(&path))
    }

    async fn install(&self, pool: &PgPool) -> Result<()> {
        for baseline in [
            include_str!("../../../schema/baseline/01_chain.sql"),
            include_str!("../../../schema/baseline/02_raw_facts.sql"),
            include_str!("../../../schema/baseline/03_identity.sql"),
            include_str!("../../../schema/baseline/04_manifests.sql"),
            include_str!("../../../schema/baseline/05_normalized_events.sql"),
            include_str!("../../../schema/baseline/06_projections.sql"),
            include_str!("../../../schema/baseline/07_labels.sql"),
            include_str!("../../../schema/baseline/08_heartbeats.sql"),
            include_str!("../../../schema/baseline/09_divergence.sql"),
            include_str!("../../../schema/baseline/10_phase_state.sql"),
        ] {
            raw_sql(baseline).execute(pool).await?;
        }
        let hash = bigname_content_hash::INTERPRETER_CONTENT_HASH;
        raw_sql(&format!(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
                 canonicality_state)
             SELECT '{CHAIN}', 'b' || n, n, to_timestamp(1700000000 + n), 'finalized'
             FROM generate_series(1, 3) n;
             INSERT INTO project_family_marker (chain_id, current_block_number,
                 current_block_hash, block_timestamp, input_content_hash, state)
             VALUES ('{CHAIN}', 2, 'b2', to_timestamp(1700000002), '{hash}', 'live');
             INSERT INTO label_preimages (labelhash, raw_label, decoded_label,
                 normalizer_version, normalized_under_version, normalization_error, source_kind,
                 source_priority)
             VALUES ('{eth}', 'eth', 'eth', 'v', true, NULL, 'fixture', 0),
                    ('{alice}', 'alice', 'alice', 'v', true, NULL, 'fixture', 0),
                    ('{usable}', 'known', 'known', 'v', true, NULL, 'fixture', 0),
                    ('{failing}', 'Known', 'Known', 'v', false, 'not normalized', 'fixture', 0),
                    ('{undecodable}', '\\xfffe', NULL, 'v', false, 'not text', 'fixture', 0)",
            eth = self.eth,
            alice = self.alice,
            usable = self.usable,
            failing = self.failing,
            undecodable = self.undecodable,
        ))
        .execute(pool)
        .await?;
        let with_bytes = [
            (
                self.id(&[&self.alice, &self.eth]),
                "alice.eth",
                vec![&self.alice, &self.eth],
                "active",
                1,
            ),
            (
                self.named.clone(),
                "named.eth",
                vec![&self.missing, &self.eth],
                "active",
                1,
            ),
            (
                self.id(&[&self.failing, &self.eth]),
                "Known.eth",
                vec![&self.failing, &self.eth],
                "shadow",
                1,
            ),
            (self.id(&[&self.eth]), "eth", vec![&self.eth], "active", 1),
            (format!("ens:{}", node(&[])), "", vec![], "active", 1),
        ];
        for (id, raw_name, labelhashes, visibility, block) in with_bytes {
            let labels: Vec<&str> = raw_name.split('.').filter(|l| !l.is_empty()).collect();
            let shadow = visibility == "shadow";
            sqlx::query(
                "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
                     dns_encoded_name, namehash, labelhashes, normalizer_version,
                     visibility_state, deactivation_reason, deactivated_at, chain_id, block_hash,
                     block_number, canonicality_state)
                 VALUES ($1, 'ens', $2, $3, '\\x00', split_part($1, ':', 2), $4, 'v', $5,
                     CASE WHEN $6 THEN 'invalid' END, CASE WHEN $6 THEN now() END, $7,
                     'b' || $8, $8, 'finalized')",
            )
            .bind(&id)
            .bind(raw_name)
            .bind(&labels)
            .bind(&labelhashes)
            .bind(visibility)
            .bind(shadow)
            .bind(CHAIN)
            .bind(block)
            .execute(pool)
            .await?;
        }
        let without_bytes = [
            (
                self.child.clone(),
                vec![&self.usable, &self.alice, &self.eth],
                2_i64,
            ),
            (
                self.mixed.clone(),
                vec![
                    &self.usable,
                    &self.failing,
                    &self.undecodable,
                    &self.missing,
                    &self.eth,
                ],
                2,
            ),
            (self.unknown.clone(), vec![&self.missing, &self.missing], 2),
            (
                self.id(&[&self.missing, &self.alice, &self.eth]),
                vec![&self.missing, &self.alice, &self.eth],
                3,
            ),
        ];
        for (id, labelhashes, block) in without_bytes {
            sqlx::query(
                "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
                     normalizer_version, visibility_state, chain_id, block_hash, block_number,
                     canonicality_state)
                 VALUES ($1, 'ens', split_part($1, ':', 2), $2, 'v', 'active', $3, 'b' || $4,
                     $4, 'finalized')",
            )
            .bind(&id)
            .bind(&labelhashes)
            .bind(CHAIN)
            .bind(block)
            .execute(pool)
            .await?;
        }
        Ok(())
    }
}

async fn with_fixture(
    name: &str,
    check: impl AsyncFnOnce(&PgPool, &Fixture) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(1)).await?;
    let fixture = Fixture::new();
    let result = async {
        database.create_phase_schema().await?;
        fixture.install(database.pool()).await?;
        check(database.pool(), &fixture).await
    }
    .await;
    database.cleanup().await?;
    result
}

#[tokio::test]
async fn the_sql_name_is_the_stored_name_or_each_label_by_its_preimage() -> Result<()> {
    with_fixture("rendered_name_sql", async |pool, fixture| {
        for expression in [
            rendered_name_sql("surface"),
            rendered_name_sql_unqualified("surface"),
        ] {
            let names: Vec<(String, String)> = sqlx::query_as(&format!(
                "SELECT surface.logical_name_id, {expression} FROM name_surfaces surface"
            ))
            .fetch_all(pool)
            .await?;
            let name_of = |id: &str| {
                names
                    .iter()
                    .find(|(found, _)| found == id)
                    .map(|(_, name)| name.as_str())
            };
            assert_eq!(name_of(&fixture.named), Some("named.eth"));
            assert_eq!(
                name_of(&fixture.id(&[&fixture.alice, &fixture.eth])),
                Some("alice.eth")
            );
            assert_eq!(name_of(&format!("ens:{}", node(&[]))), Some(""));
            assert_eq!(name_of(&fixture.child), Some("known.alice.eth"));
            assert_eq!(
                name_of(&fixture.mixed),
                Some(&*format!(
                    "known.{}.{}.{}.eth",
                    bracket(&fixture.failing),
                    bracket(&fixture.undecodable),
                    bracket(&fixture.missing)
                ))
            );
            assert_eq!(
                name_of(&fixture.unknown),
                Some(&*format!("{0}.{0}", bracket(&fixture.missing)))
            );
        }
        // A labelhash stored in upper case reads the same preimage and placeholder.
        let upper: String = sqlx::query_scalar(&format!(
            "SELECT {} FROM (SELECT NULL::text AS raw_name, $1::text[] AS labelhashes) surface",
            rendered_name_sql("surface")
        ))
        .bind(vec![
            fixture.usable.to_uppercase().replace("0X", "0x"),
            fixture.missing.to_uppercase().replace("0X", "0x"),
        ])
        .fetch_one(pool)
        .await?;
        assert_eq!(upper, format!("known.{}", bracket(&fixture.missing)));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn readers_agree_with_the_text_tests_for_surfaces_with_bytes() -> Result<()> {
    with_fixture("rendered_name_predicates", async |pool, _| {
        let rows: Vec<(String, bool, bool, bool, bool)> = sqlx::query_as(&format!(
            "SELECT surface.logical_name_id,
                    COALESCE({composed}, false),
                    surface.visibility_state = 'active' AND surface.raw_name <> '',
                    cardinality(surface.labelhashes) = 2
                        AND lower(surface.labelhashes[2]) = '{ETH_LABELHASH}',
                    cardinality(surface.labelhashes) = 2
                        AND split_part(surface.raw_name, '.', 2) = 'eth'
             FROM name_surfaces surface WHERE surface.raw_name IS NOT NULL",
            composed = composed_surface_sql("surface")
        ))
        .fetch_all(pool)
        .await?;
        assert_eq!(rows.len(), 5);
        for (id, composed, composed_by_text, eth_child, eth_child_by_text) in rows {
            assert_eq!(composed, composed_by_text, "{id}");
            assert_eq!(eth_child, eth_child_by_text, "{id}");
        }
        let without_bytes: Vec<bool> = sqlx::query_scalar(&format!(
            "SELECT {} FROM name_surfaces surface WHERE surface.raw_name IS NULL",
            composed_surface_sql("surface")
        ))
        .fetch_all(pool)
        .await?;
        assert_eq!(without_bytes, [true; 4]);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn the_compositor_reads_a_surface_without_bytes() -> Result<()> {
    with_fixture("rendered_name_compose", async |pool, fixture| {
        let root = format!("ens:{}", node(&[]));
        let shadow = fixture.id(&[&fixture.failing, &fixture.eth]);
        let ids = [
            fixture.named.clone(),
            fixture.child.clone(),
            fixture.mixed.clone(),
            fixture.unknown.clone(),
            root,
            shadow,
        ];
        let mut conn = pool.acquire().await?;
        let mut loaded: Vec<(String, Option<String>)> = loaders::surfaces(&mut conn, &ids)
            .await?
            .into_iter()
            .map(|surface| (surface.logical_name_id, surface.raw_name))
            .collect();
        loaded.sort();
        let mut expected = vec![
            (fixture.named.clone(), Some("named.eth".to_owned())),
            (fixture.child.clone(), None),
            (fixture.mixed.clone(), None),
            (fixture.unknown.clone(), None),
        ];
        expected.sort();
        assert_eq!(loaded, expected);

        // The composition Project's summary step shares reads no preimage.
        let base = load_composed_base(&mut conn, &ids, CoverageShape::Plain).await?;
        let child_path = [
            fixture.usable.clone(),
            fixture.alice.clone(),
            fixture.eth.clone(),
        ];
        assert_eq!(
            base[&fixture.child].normalized_name,
            placeholder_name(&child_path)?
        );
        assert_eq!(
            base[&fixture.child].canonical_display_name,
            base[&fixture.child].normalized_name
        );
        assert_eq!(base[&fixture.named].normalized_name, "named.eth");
        drop(conn);

        let child = load_family_name(pool, &fixture.child)
            .await?
            .expect("a surface without bytes has a composed row");
        assert_eq!(child.normalized_name, "known.alice.eth");
        assert_eq!(child.canonical_display_name, "known.alice.eth");
        assert_eq!(child.namehash, node(&child_path));
        assert_eq!(child.namespace, "ens");
        let mixed = load_family_name(pool, &fixture.mixed).await?.expect("row");
        assert_eq!(
            mixed.normalized_name,
            format!(
                "known.{}.{}.{}.eth",
                bracket(&fixture.failing),
                bracket(&fixture.undecodable),
                bracket(&fixture.missing)
            )
        );
        let named = load_family_name(pool, &fixture.named).await?.expect("row");
        assert_eq!(named.normalized_name, "named.eth");
        assert_eq!(named.canonical_display_name, "named.eth");

        // A surface written after the publication is not part of it, with or without bytes.
        let later = fixture.id(&[&fixture.missing, &fixture.alice, &fixture.eth]);
        assert!(load_family_name(pool, &later).await?.is_none());

        // The stored summary is the same whatever preimages exist.
        let mut conn = pool.acquire().await?;
        let publication = publication_on(&mut conn, CHAIN)
            .await?
            .expect("a servable publication");
        let with_preimages = compose_name_summaries(&mut conn, &publication, &ids).await?;
        assert!(with_preimages.contains_key(&fixture.child));
        assert!(with_preimages.contains_key(&fixture.unknown));
        sqlx::query("DELETE FROM label_preimages")
            .execute(&mut *conn)
            .await?;
        assert_eq!(
            compose_name_summaries(&mut conn, &publication, &ids).await?,
            with_preimages
        );
        drop(conn);
        let child = load_family_name(pool, &fixture.child).await?.expect("row");
        assert_eq!(child.normalized_name, placeholder_name(&child_path)?);
        Ok(())
    })
    .await
}
