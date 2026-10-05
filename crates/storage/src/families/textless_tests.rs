//! The serving readers over name surfaces that store no raw bytes: search, the children of a
//! parent without bytes, the registry-child fallback, resolver links and the identity read model.
//! The bound-name and reverse-page candidate walks and the mirror walks have their own tests
//! (`name_order_plan_tests`, `records::reverse_page`, `records::mirror`), on this fixture where
//! they need one.
//!
//! The fixture is `eth` and `alpha.eth` with bytes, and below `alpha.eth`:
//!
//! - `beta` and `zeta`, with bytes;
//! - `first`, without bytes and with no preimage, and below it `nested` (no bytes, no preimage),
//!   `known` (no bytes, a usable preimage), `undecodable` (no bytes, a preimage that is not
//!   text), and two children with no surface: `escaped` (a preimage that is not text) and
//!   `bare` (no preimage);
//! - `late`, whose surface without bytes is written after the publication.
use alloy_primitives::{B256, keccak256};
use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::{PgPool, raw_sql};
use uuid::Uuid;

use super::{
    name::{load_family_name, load_family_search_page, rendered::placeholder_label},
    topology::{
        count_children_shadow, load_children_shadow_page, load_name_topology_shadow,
        load_owned_registry_children, load_resolver_links_shadow, published_surface_exists,
    },
};
use crate::{
    ChildrenCurrentPageFilter, NameCurrentListFilter, NameQuery, load_name_surface,
    load_name_surfaces_by_logical_name_ids, load_subregistry_pointers_for_names,
};

pub(crate) const CHAIN: &str = "ethereum-sepolia";
pub(crate) const OWNER: &str = "0x00000000000000000000000000000000000000aa";
pub(crate) const RESOLVER: &str = "0x00000000000000000000000000000000000000cc";
/// The publication block. `late`'s surface is written at the next one.
pub(crate) const PUBLISHED: i64 = 3;

fn labelhash(label: &[u8]) -> String {
    format!("{:#x}", keccak256(label))
}

fn bracket(labelhash: &str) -> String {
    placeholder_label(labelhash).expect("a labelhash")
}

/// The node of a label-hash path in name order.
pub(crate) fn node(labelhashes: &[&String]) -> String {
    let node = labelhashes.iter().rev().fold(B256::ZERO, |parent, hash| {
        let hash: B256 = hash.parse().expect("a labelhash");
        keccak256([parent.as_slice(), hash.as_slice()].concat())
    });
    format!("{node:#x}")
}

/// The label hashes of the fixture's labels, and the names built from them.
pub(crate) struct Fixture {
    pub(crate) eth: String,
    pub(crate) alpha: String,
    pub(crate) beta: String,
    pub(crate) zeta: String,
    pub(crate) first: String,
    pub(crate) nested: String,
    pub(crate) known: String,
    pub(crate) undecodable: String,
    pub(crate) escaped: String,
    pub(crate) bare: String,
    pub(crate) late: String,
}

impl Fixture {
    pub(crate) fn new() -> Self {
        Self {
            eth: labelhash(b"eth"),
            alpha: labelhash(b"alpha"),
            beta: labelhash(b"beta"),
            zeta: labelhash(b"zeta"),
            first: format!("0x{}", "11".repeat(32)),
            nested: format!("0x{}", "22".repeat(32)),
            known: labelhash(b"known"),
            undecodable: labelhash(&[0xff, 0xfe]),
            escaped: labelhash(&[0xfe, 0xfe]),
            bare: format!("0x{}", "33".repeat(32)),
            late: format!("0x{}", "44".repeat(32)),
        }
    }

    pub(crate) fn id(&self, labelhashes: &[&String]) -> String {
        format!("ens:{}", node(labelhashes))
    }

    pub(crate) fn alpha_path(&self) -> [&String; 2] {
        [&self.alpha, &self.eth]
    }

    pub(crate) fn first_path(&self) -> [&String; 3] {
        [&self.first, &self.alpha, &self.eth]
    }

    /// The path of `label` below `first`.
    pub(crate) fn below_first<'a>(&'a self, label: &'a String) -> [&'a String; 4] {
        [label, &self.first, &self.alpha, &self.eth]
    }

    /// `first`'s served name: no label of its own, under the stored `alpha.eth`.
    pub(crate) fn first_name(&self) -> String {
        format!("{}.alpha.eth", bracket(&self.first))
    }

    pub(crate) async fn install(&self, pool: &PgPool) -> Result<()> {
        for baseline in [
            include_str!("../../schema/baseline/01_chain.sql"),
            include_str!("../../schema/baseline/02_raw_facts.sql"),
            include_str!("../../schema/baseline/03_identity.sql"),
            include_str!("../../schema/baseline/04_manifests.sql"),
            include_str!("../../schema/baseline/05_normalized_events.sql"),
            include_str!("../../schema/baseline/06_projections.sql"),
            include_str!("../../schema/baseline/07_labels.sql"),
            include_str!("../../schema/baseline/08_heartbeats.sql"),
            include_str!("../../schema/baseline/09_divergence.sql"),
            include_str!("../../schema/baseline/10_phase_state.sql"),
        ] {
            raw_sql(baseline).execute(pool).await?;
        }
        let hash = bigname_content_hash::INTERPRETER_CONTENT_HASH;
        raw_sql(&format!(
            "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp,
                 canonicality_state)
             SELECT '{CHAIN}', 'b' || n, n, to_timestamp(1700000000 + n), 'finalized'
             FROM generate_series(1, 5) n;
             INSERT INTO project_family_marker (chain_id, current_block_number,
                 current_block_hash, block_timestamp, input_content_hash, state)
             VALUES ('{CHAIN}', {PUBLISHED}, 'b{PUBLISHED}',
                     to_timestamp(1700000000 + {PUBLISHED}), '{hash}', 'live');
             INSERT INTO label_preimages (labelhash, raw_label, decoded_label,
                 normalizer_version, normalized_under_version, normalization_error, source_kind,
                 source_priority)
             VALUES ('{eth}', 'eth', 'eth', 'v', true, NULL, 'fixture', 0),
                    ('{alpha}', 'alpha', 'alpha', 'v', true, NULL, 'fixture', 0),
                    ('{beta}', 'beta', 'beta', 'v', true, NULL, 'fixture', 0),
                    ('{zeta}', 'zeta', 'zeta', 'v', true, NULL, 'fixture', 0),
                    ('{known}', 'known', 'known', 'v', true, NULL, 'fixture', 0),
                    ('{undecodable}', '\\xfffe', NULL, 'v', false, 'not text', 'fixture', 0),
                    ('{escaped}', '\\xfefe', NULL, 'v', false, 'not text', 'fixture', 0)",
            eth = self.eth,
            alpha = self.alpha,
            beta = self.beta,
            zeta = self.zeta,
            known = self.known,
            undecodable = self.undecodable,
            escaped = self.escaped,
        ))
        .execute(pool)
        .await?;

        let alpha = self.alpha_path();
        for (raw_name, path) in [
            ("eth", vec![&self.eth]),
            ("alpha.eth", alpha.to_vec()),
            ("beta.alpha.eth", vec![&self.beta, &self.alpha, &self.eth]),
            ("zeta.alpha.eth", vec![&self.zeta, &self.alpha, &self.eth]),
        ] {
            let labels: Vec<&str> = raw_name.split('.').collect();
            sqlx::query(
                "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
                     dns_encoded_name, namehash, labelhashes, normalizer_version,
                     visibility_state, chain_id, block_hash, block_number, canonicality_state)
                 VALUES ($1, 'ens', $2, $3, '\\x00', split_part($1, ':', 2), $4, 'v', 'active',
                     $5, 'b1', 1, 'finalized')",
            )
            .bind(self.id(&path))
            .bind(raw_name)
            .bind(&labels)
            .bind(&path)
            .bind(CHAIN)
            .execute(pool)
            .await?;
        }
        for (path, block) in [
            (self.first_path().to_vec(), 2_i64),
            (self.below_first(&self.nested).to_vec(), 2),
            (self.below_first(&self.known).to_vec(), 2),
            (self.below_first(&self.undecodable).to_vec(), 2),
            (vec![&self.late, &self.alpha, &self.eth], PUBLISHED + 1),
        ] {
            insert_textless_surface(pool, "ens", CHAIN, &path, block).await?;
        }

        let below_alpha = [&self.beta, &self.zeta, &self.first, &self.late];
        let below_first = [
            &self.nested,
            &self.known,
            &self.undecodable,
            &self.escaped,
            &self.bare,
        ];
        let first = self.first_path();
        let edges = below_alpha
            .iter()
            .map(|label| (alpha.to_vec(), *label))
            .chain(below_first.iter().map(|label| (first.to_vec(), *label)));
        for (index, (parent, label)) in edges.enumerate() {
            let child = [vec![label], parent.clone()].concat();
            let identity = format!("edge:{index}");
            // The NewOwner that creates the child: its edge, the node's registry state, and the
            // event the fallback finds the child's parent by.
            sqlx::query(
                "WITH edge AS (
                     INSERT INTO project_child_edge_candidate (chain_id, namespace, parent_node,
                         child_node, authority_arm, block_number, transaction_index, log_index,
                         event_identity, owner, labelhash, source_family)
                     VALUES ($1, 'ens', $2, $3, 'ens_v1', 2, 0, $4, $5, $6, $7,
                         'ens_v1_registry_l1')
                 ), state AS (
                     INSERT INTO project_registry_node_state (chain_id, namespace, node,
                         block_number, transaction_index, log_index, event_identity, owner,
                         owner_resource_id)
                     VALUES ($1, 'ens', $3, 2, 0, $4, $5, $6, $8)
                 )
                 INSERT INTO normalized_events (event_identity, namespace, event_kind,
                     source_family, manifest_version, chain_id, block_number, block_hash,
                     transaction_hash, transaction_index, log_index, derivation_kind,
                     canonicality_state, after_state)
                 VALUES ($5, 'ens', 'SubregistryChanged', 'ens_v1_registry_l1', 1, $1, 2, 'b2',
                     'tx', 0, $4, 'ens_v1_unwrapped_authority', 'finalized',
                     jsonb_build_object('node', $2, 'child_node', $3))",
            )
            .bind(CHAIN)
            .bind(node(&parent))
            .bind(node(&child))
            .bind(i64::try_from(index)?)
            .bind(identity)
            .bind(OWNER)
            .bind(label)
            .bind(Uuid::from_u128(index as u128 + 1))
            .execute(pool)
            .await?;
        }
        // A released registrar lease on each of these nodes. Only a child with no name row at
        // the publication reports it.
        for (index, path) in [
            self.first_path().to_vec(),
            vec![&self.late, &self.alpha, &self.eth],
        ]
        .iter()
        .enumerate()
        {
            sqlx::query(
                "INSERT INTO project_lifecycle_event (chain_id, state_kind, state_key,
                     block_number, event_identity, event_kind, source_family, namehash)
                 VALUES ($1, 'resource', $2, 2, $3, 'RegistrationReleased',
                     'ens_v1_registrar_l1', $4)",
            )
            .bind(CHAIN)
            .bind(Uuid::from_u128(100 + index as u128).to_string())
            .bind(format!("lease:{index}"))
            .bind(node(path))
            .execute(pool)
            .await?;
        }
        Ok(())
    }
}

/// Insert an active name surface that stores no raw bytes: only its node and label-hash path
/// (name order), at `block` of `chain_id`, whose lineage row `b<block>` must exist.
pub(crate) async fn insert_textless_surface(
    pool: &PgPool,
    namespace: &str,
    chain_id: &str,
    labelhashes: &[&String],
    block: i64,
) -> Result<String> {
    let id = format!("{namespace}:{}", node(labelhashes));
    sqlx::query(
        "INSERT INTO name_surfaces (logical_name_id, namespace, namehash, labelhashes,
             normalizer_version, visibility_state, chain_id, block_hash, block_number,
             canonicality_state)
         VALUES ($1, $2, split_part($1, ':', 2), $3, 'v', 'active', $4, 'b' || $5, $5,
             'finalized')",
    )
    .bind(&id)
    .bind(namespace)
    .bind(labelhashes)
    .bind(chain_id)
    .bind(block)
    .execute(pool)
    .await?;
    Ok(id)
}

pub(crate) async fn with_fixture(
    name: &str,
    check: impl AsyncFnOnce(&PgPool, &Fixture) -> Result<()>,
) -> Result<()> {
    let database =
        TestDatabase::create(TestDatabaseConfig::new(name).pool_max_connections(2)).await?;
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

async fn search(pool: &PgPool, filter: NameCurrentListFilter, page: u64) -> Result<Vec<String>> {
    let mut names = Vec::new();
    let mut cursor = None;
    loop {
        let loaded = load_family_search_page(pool, &filter, cursor.as_ref(), page).await?;
        names.extend(loaded.rows.into_iter().map(|row| row.row.normalized_name));
        cursor = loaded.next_cursor;
        if cursor.is_none() {
            return Ok(names);
        }
    }
}

#[tokio::test]
async fn search_finds_and_orders_names_without_bytes() -> Result<()> {
    with_fixture("textless_search", async |pool, fixture| {
        let first = fixture.first_name();
        let ens = || NameCurrentListFilter {
            namespace: Some("ens".to_owned()),
            ..NameCurrentListFilter::default()
        };
        // The database's order of every published name, by served name.
        let mut expected = vec![
            "alpha.eth".to_owned(),
            "beta.alpha.eth".to_owned(),
            "eth".to_owned(),
            "zeta.alpha.eth".to_owned(),
            first.clone(),
            format!("{}.{first}", bracket(&fixture.nested)),
            format!("known.{first}"),
            format!("{}.{first}", bracket(&fixture.undecodable)),
        ];
        let ordered: Vec<String> =
            sqlx::query_scalar("SELECT name FROM unnest($1::text[]) name ORDER BY name")
                .bind(&expected)
                .fetch_all(pool)
                .await?;
        expected = ordered;
        // One page, and pages that end inside and between the two kinds of surface.
        for page in [50, 1, 3] {
            assert_eq!(search(pool, ens(), page).await?, expected, "page {page}");
        }
        assert_eq!(
            search(pool, NameCurrentListFilter::default(), 2).await?,
            expected
        );

        // A readable label of a name without bytes finds it, as a prefix and inside the name.
        let known = format!("known.{first}");
        let by_prefix = NameCurrentListFilter {
            prefix: Some("known".to_owned()),
            ..ens()
        };
        assert_eq!(
            search(pool, by_prefix, 10).await?,
            std::slice::from_ref(&known)
        );
        let by_parent_label = NameCurrentListFilter {
            contains: Some(".alpha".to_owned()),
            ..ens()
        };
        let under_alpha: Vec<String> = expected
            .iter()
            .filter(|name| name.contains(".alpha"))
            .cloned()
            .collect();
        assert_eq!(under_alpha.len(), 6);
        assert_eq!(search(pool, by_parent_label, 2).await?, under_alpha);
        let exact = NameCurrentListFilter {
            name: Some(known.clone()),
            ..ens()
        };
        assert_eq!(search(pool, exact, 10).await?, [known]);
        let other_namespace = NameCurrentListFilter {
            namespace: Some("basenames".to_owned()),
            ..NameCurrentListFilter::default()
        };
        assert!(search(pool, other_namespace, 10).await?.is_empty());
        Ok(())
    })
    .await
}

async fn children(
    pool: &PgPool,
    parent: &str,
    filter: &ChildrenCurrentPageFilter<'_>,
) -> Result<(Vec<String>, u64)> {
    let page = load_children_shadow_page(pool, parent, filter, None, 50).await?;
    Ok((
        page.rows
            .into_iter()
            .map(|row| row.canonical_display_name)
            .collect(),
        page.total_count,
    ))
}

#[tokio::test]
async fn a_parent_without_bytes_lists_and_counts_its_children() -> Result<()> {
    with_fixture("textless_children", async |pool, fixture| {
        let alpha = fixture.id(&fixture.alpha_path());
        let first = fixture.id(&fixture.first_path());
        let first_name = fixture.first_name();
        let all = ChildrenCurrentPageFilter::default();

        // Below a parent with bytes: the children without them are named as their rows are.
        let (names, total) = children(pool, &alpha, &all).await?;
        assert_eq!(
            names,
            [
                first_name.clone(),
                format!("{}.alpha.eth", bracket(&fixture.late)),
                "beta.alpha.eth".to_owned(),
                "zeta.alpha.eth".to_owned(),
            ]
        );
        assert_eq!(total, 4);

        // Below a parent without bytes, every child's name ends in the parent's served name.
        // A child with a surface is named as its name row is, so its undecodable preimage is a
        // placeholder; a child with no surface keeps the escaped form.
        let (names, total) = children(pool, &first, &all).await?;
        let mut expected = vec![
            format!("{}.{first_name}", bracket(&fixture.nested)),
            format!("{}.{first_name}", bracket(&fixture.bare)),
            format!("known.{first_name}"),
            format!("{}.{first_name}", bracket(&fixture.undecodable)),
            format!("\\376\\376.{first_name}"),
        ];
        let ordered: Vec<String> =
            sqlx::query_scalar("SELECT name FROM unnest($1::text[]) name ORDER BY name")
                .bind(&expected)
                .fetch_all(pool)
                .await?;
        expected = ordered;
        assert_eq!(names, expected);
        assert_eq!(total, 5);
        for label in [&fixture.nested, &fixture.known, &fixture.undecodable] {
            let id = fixture.id(&fixture.below_first(label));
            let row = load_family_name(pool, &id).await?.expect("a name row");
            assert!(names.contains(&row.normalized_name), "{id}");
        }

        let by_label = ChildrenCurrentPageFilter {
            q: Some(NameQuery::prefix("known")),
            ..ChildrenCurrentPageFilter::default()
        };
        assert_eq!(
            children(pool, &first, &by_label).await?,
            (vec![format!("known.{first_name}")], 1)
        );

        assert_eq!(
            count_children_shadow(pool, &[alpha.clone(), first.clone()]).await?,
            [(alpha.clone(), 4), (first.clone(), 5)]
        );

        // A child with a name row at the publication reports no lease of its own; one whose
        // surface is not published yet still does.
        let page = load_children_shadow_page(pool, &alpha, &all, None, 50).await?;
        let released = |label: &String| {
            let id = fixture.id(&[label, &fixture.alpha, &fixture.eth]);
            page.rows
                .iter()
                .find(|row| row.child_logical_name_id == id)
                .map(|row| row.released_lease)
        };
        assert_eq!(released(&fixture.first), Some(false));
        assert_eq!(released(&fixture.late), Some(true));
        assert_eq!(released(&fixture.beta), Some(false));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn the_fallback_lists_a_child_only_until_its_surface_is_published() -> Result<()> {
    with_fixture("textless_fallback", async |pool, fixture| {
        let first = fixture.id(&fixture.first_path());
        let late = fixture.id(&[&fixture.late, &fixture.alpha, &fixture.eth]);
        let bare = fixture.id(&fixture.below_first(&fixture.bare));
        let beta = fixture.id(&[&fixture.beta, &fixture.alpha, &fixture.eth]);
        let ids = vec![first.clone(), late.clone(), bare.clone(), beta.clone()];
        let published = |block: i64| {
            let ids = ids.clone();
            async move {
                sqlx::query_scalar::<_, String>(&format!(
                    "SELECT id FROM unnest($1::text[]) id WHERE {} ORDER BY id",
                    published_surface_exists("id", "$2")
                ))
                .bind(ids)
                .bind(block)
                .fetch_all(pool)
                .await
            }
        };
        let mut both = vec![first.clone(), beta.clone()];
        both.sort();
        assert_eq!(published(PUBLISHED).await?, both);
        let mut all = vec![first.clone(), beta.clone(), late.clone()];
        all.sort();
        assert_eq!(published(PUBLISHED + 1).await?, all);
        assert!(published(0).await?.is_empty());

        // The name compositor serves `first`, so the fallback does not list it a second time.
        let mut conn = pool.acquire().await?;
        let listed = load_owned_registry_children(&mut conn, CHAIN, OWNER, &ids, PUBLISHED).await?;
        let mut listed: Vec<(String, String)> = listed
            .into_iter()
            .map(|child| (child.logical_name_id, child.display_name))
            .collect();
        listed.sort();
        let mut expected = vec![
            (
                late.clone(),
                format!("{}.alpha.eth", bracket(&fixture.late)),
            ),
            (
                bare.clone(),
                format!("{}.{}", bracket(&fixture.bare), fixture.first_name()),
            ),
        ];
        expected.sort();
        assert_eq!(listed, expected);
        drop(conn);
        assert!(load_family_name(pool, &first).await?.is_some());
        assert!(load_family_name(pool, &late).await?.is_none());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_resolver_link_names_a_node_without_bytes() -> Result<()> {
    with_fixture("textless_links", async |pool, fixture| {
        let known = fixture.below_first(&fixture.known);
        for (index, path) in [known.to_vec(), fixture.alpha_path().to_vec()]
            .iter()
            .enumerate()
        {
            sqlx::query(
                "INSERT INTO project_resolver_link (chain_id, resolver_address, node,
                     block_number, event_identity, record_id)
                 VALUES ($1, $2, $3, 2, $4, $5)",
            )
            .bind(CHAIN)
            .bind(RESOLVER)
            .bind(node(path))
            .bind(format!("link:{index}"))
            .bind((index + 1).to_string())
            .execute(pool)
            .await?;
        }
        let page = load_resolver_links_shadow(pool, CHAIN, RESOLVER, "ens", None, 10).await?;
        let names: Vec<Option<&str>> = page
            .rows
            .iter()
            .map(|(.., item)| item["name"].as_str())
            .collect();
        let known = format!("known.{}", fixture.first_name());
        assert_eq!(names, [Some(known.as_str()), Some("alpha.eth")]);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn the_identity_read_model_loads_a_surface_without_bytes() -> Result<()> {
    with_fixture("textless_identity", async |pool, fixture| {
        let known = fixture.id(&fixture.below_first(&fixture.known));
        let alpha = fixture.id(&fixture.alpha_path());
        let name = format!("known.{}", fixture.first_name());
        let surface = load_name_surface(pool, &known).await?.expect("a surface");
        assert_eq!(surface.normalized_name, name);
        assert_eq!(surface.canonical_display_name, name);
        assert_eq!(surface.input_name, name);
        assert_eq!(surface.dns_encoded_name, None);
        assert_eq!(surface.labelhashes.len(), 4);

        let both =
            load_name_surfaces_by_logical_name_ids(pool, &[known.clone(), alpha.clone()]).await?;
        assert_eq!(both[&known], surface);
        assert_eq!(both[&alpha].normalized_name, "alpha.eth");
        assert_eq!(both[&alpha].input_name, "alpha.eth");
        assert_eq!(both[&alpha].dns_encoded_name, Some(vec![0]));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_subregistry_pointer_names_a_surface_without_bytes() -> Result<()> {
    with_fixture("textless_subregistry", async |pool, fixture| {
        let known = fixture.id(&fixture.below_first(&fixture.known));
        let alpha = fixture.id(&fixture.alpha_path());
        for (index, id) in [&known, &alpha].into_iter().enumerate() {
            sqlx::query(
                "INSERT INTO normalized_events (event_identity, namespace, logical_name_id,
                     event_kind, source_family, manifest_version, chain_id, block_number,
                     block_hash, transaction_hash, transaction_index, log_index,
                     derivation_kind, canonicality_state, after_state)
                 VALUES ('subregistry:' || $1, 'ens', $1, 'SubregistryChanged',
                     'ens_v2_registry_l1', 1, $2, 2, 'b2', 'tx', 1, $3,
                     'ens_v2_registry_resource_surface', 'finalized',
                     jsonb_build_object('subregistry', '0x00000000000000000000000000000000000000dd'))",
            )
            .bind(id)
            .bind(CHAIN)
            .bind(i64::try_from(index)?)
            .execute(pool)
            .await?;
        }
        let pointers =
            load_subregistry_pointers_for_names(pool, &[known.clone(), alpha.clone()], None)
                .await?;
        assert_eq!(
            pointers[&known].display_name,
            format!("known.{}", fixture.first_name())
        );
        assert_eq!(pointers[&alpha].display_name, "alpha.eth");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn the_wildcard_topology_read_accepts_a_surface_without_bytes() -> Result<()> {
    with_fixture("textless_topology", async |pool, fixture| {
        // Neither name has a wildcard binding; the read must reach that answer for both.
        for path in [fixture.first_path().to_vec(), fixture.alpha_path().to_vec()] {
            assert_eq!(
                load_name_topology_shadow(pool, &fixture.id(&path)).await?,
                None
            );
        }
        Ok(())
    })
    .await
}
