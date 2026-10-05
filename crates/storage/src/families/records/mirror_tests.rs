//! The mirror walk over names and ancestors whose surfaces store no raw labels, and the walk's
//! order clause, `latest_registry_first`, run over an `unnest` relation of
//! positions. This proves the generated `ORDER BY` clause, not the walk's join over
//! `name_surfaces` and `project_registry_pointer`. In the walk the emission ordinal
//! (docs/glossary.md#emission-ordinal) term is inert: `project_registry_pointer` is keyed by
//! (chain_id, namespace, node), its primary key in crates/storage/schema/baseline/06_projections.sql, so
//! every row at one depth is the same pointer row.
use alloy_primitives::keccak256;
use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    super::{FamilyPosition, serving::ServingPointer},
    latest_registry_first, nearest, suffix_namehash,
};
use crate::families::{
    name::rendered::placeholder_label,
    textless_tests::{CHAIN, Fixture, insert_textless_surface, node, with_fixture},
};

/// Positions at one block, including an ordinal-bearing tie at one log (`e:2` against `e:10`),
/// facts without an ordinal at that log, boundary facts and the `u32` bound.
fn positions() -> Vec<FamilyPosition> {
    let at = |transaction: Option<i64>, log: Option<i64>, identity: &str| FamilyPosition {
        block_number: 5,
        transaction_index: transaction,
        log_index: log,
        event_identity: identity.to_owned(),
    };
    vec![
        at(Some(0), Some(0), "e:2"),
        at(Some(0), Some(0), "e:10"),
        at(Some(0), Some(0), "e:holder"),
        at(Some(0), Some(0), "e:007"),
        at(Some(0), Some(0), "t:4294967296"),
        at(Some(0), Some(0), "u:4294967295"),
        at(Some(0), Some(1), "a:0"),
        at(None, None, "p:10"),
        at(None, None, "p:9"),
    ]
}

/// The walk's order clause agrees with `FamilyPosition`'s: at one log, the higher emission
/// ordinal is the later fact, so `e:10` wins over `e:2`, where identity bytes alone pick `e:2`.
#[tokio::test]
async fn the_walk_order_follows_the_emission_ordinal() -> Result<()> {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("families_mirror_order").pool_max_connections(1),
    )
    .await?;
    let result = check_order(database.pool()).await;
    database.cleanup().await?;
    result
}

async fn check_order(pool: &sqlx::PgPool) -> Result<()> {
    let positions = positions();
    let sql = format!(
        "SELECT registry.event_identity
         FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::text[])
              registry (block_number, transaction_index, log_index, event_identity)
         ORDER BY {}",
        latest_registry_first("registry")
    );
    let identities: Vec<String> = sqlx::query_scalar(&sql)
        .bind(positions.iter().map(|p| p.block_number).collect::<Vec<_>>())
        .bind(
            positions
                .iter()
                .map(|p| p.transaction_index)
                .collect::<Vec<_>>(),
        )
        .bind(positions.iter().map(|p| p.log_index).collect::<Vec<_>>())
        .bind(
            positions
                .iter()
                .map(|p| p.event_identity.clone())
                .collect::<Vec<_>>(),
        )
        .fetch_all(pool)
        .await?;
    let mut expected = positions;
    expected.sort_by(|left, right| right.cmp(left));
    let expected: Vec<String> = expected.into_iter().map(|p| p.event_identity).collect();
    assert_eq!(identities, expected);
    let tie: Vec<&str> = identities
        .iter()
        .map(String::as_str)
        .filter(|identity| matches!(*identity, "e:2" | "e:10"))
        .collect();
    assert_eq!(tie, ["e:10", "e:2"], "the walk picks ordinal 10");
    Ok(())
}

async fn point(pool: &PgPool, path: &[&String], resolver: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO project_registry_pointer (chain_id, namespace, node, block_number,
             event_identity, resolver_address, source_family)
         VALUES ($1, 'ens', $2, 2, 'pointer:' || $2, $3, 'ens_v1_registry_l1')",
    )
    .bind(CHAIN)
    .bind(node(path))
    .bind(resolver)
    .execute(pool)
    .await?;
    Ok(())
}

/// The walk's answer for the name at `path`: the depth, name and resolver of its nearest node.
async fn walked(
    pool: &PgPool,
    fixture: &Fixture,
    path: &[&String],
) -> Result<Option<(i32, String, String)>> {
    let pointer = ServingPointer {
        resource_id: Uuid::nil(),
        logical_name_id: fixture.id(path),
        namespace: "ens".to_owned(),
        source_family: "ens_v2_registry_l1".to_owned(),
        namehash: node(path),
        resolver_address: "0xmirror".to_owned(),
        pointer_event_id: None,
        block_number: 2,
    };
    let mut conn = pool.acquire().await?;
    let nearest = nearest(&mut conn, CHAIN, &pointer).await?;
    Ok(nearest.map(|nearest| {
        assert_eq!(
            nearest.mirrored_node,
            node(&path[nearest.ancestor_depth as usize..])
        );
        (
            nearest.ancestor_depth,
            nearest.mirrored_name,
            nearest.mirrored_resolver_address,
        )
    }))
}

#[tokio::test]
async fn the_walk_follows_names_and_ancestors_without_raw_labels() -> Result<()> {
    with_fixture("families_mirror_textless", async |pool, fixture| {
        let found = |depth: i32, name: &str, resolver: &str| {
            Some((depth, name.to_owned(), resolver.to_owned()))
        };
        let first_name = fixture.first_name();
        let known = fixture.below_first(&fixture.known);
        let beta = [&fixture.beta, &fixture.alpha, &fixture.eth];
        assert_eq!(walked(pool, fixture, &known).await?, None);

        // An ancestor with raw labels, under a name without them and under one with them.
        point(pool, &fixture.alpha_path(), "0xa").await?;
        assert_eq!(
            walked(pool, fixture, &known).await?,
            found(2, "alpha.eth", "0xa")
        );
        assert_eq!(
            walked(pool, fixture, &beta).await?,
            found(1, "alpha.eth", "0xa")
        );

        // A nearer ancestor without raw labels, then the name itself.
        point(pool, &fixture.first_path(), "0xb").await?;
        assert_eq!(
            walked(pool, fixture, &known).await?,
            found(1, &first_name, "0xb")
        );
        point(pool, &known, "0xc").await?;
        assert_eq!(
            walked(pool, fixture, &known).await?,
            found(0, &format!("known.{first_name}"), "0xc")
        );

        // A name with raw labels below an ancestor without them.
        let mid = format!("{:#x}", keccak256(b"mid"));
        let deep = format!("{:#x}", keccak256(b"deep"));
        let mid_path = [&mid, &fixture.alpha, &fixture.eth];
        let deep_path = [&deep, &mid, &fixture.alpha, &fixture.eth];
        insert_textless_surface(pool, "ens", CHAIN, &mid_path, 2).await?;
        sqlx::query(
            "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
                 chain_id, block_hash, block_number, canonicality_state)
             VALUES ($1, 'ens', 'deep.mid.alpha.eth', ARRAY['deep', 'mid', 'alpha', 'eth'],
                 '\\x00', split_part($1, ':', 2), $2, 'v', 'active', $3, 'b2', 2, 'finalized')",
        )
        .bind(fixture.id(&deep_path))
        .bind(deep_path.to_vec())
        .bind(CHAIN)
        .execute(pool)
        .await?;
        assert_eq!(
            walked(pool, fixture, &deep_path).await?,
            found(2, "alpha.eth", "0xa")
        );
        point(pool, &mid_path, "0xd").await?;
        let mid_name = format!(
            "{}.alpha.eth",
            placeholder_label(&mid).expect("a labelhash")
        );
        assert_eq!(
            walked(pool, fixture, &deep_path).await?,
            found(1, &mid_name, "0xd")
        );
        Ok(())
    })
    .await
}

#[test]
fn a_suffix_without_raw_labels_is_hashed_from_its_labelhashes() {
    let fixture = Fixture::new();
    let path = [fixture.alpha.clone(), fixture.eth.clone()];
    let labels = ["alpha".to_owned(), "eth".to_owned()];
    let expected = node(&fixture.alpha_path());
    assert_eq!(suffix_namehash(None, &path).unwrap(), expected);
    assert_eq!(suffix_namehash(Some(&labels), &path).unwrap(), expected);
    // Labelhashes that are not hashes fall back to the raw labels, and without those fail.
    let malformed = ["0xalpha".to_owned(), "0xeth".to_owned()];
    assert_eq!(
        suffix_namehash(Some(&labels), &malformed).unwrap(),
        expected
    );
    assert!(suffix_namehash(None, &malformed).is_err());
}
