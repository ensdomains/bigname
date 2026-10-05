//! The reverse page's candidate walk over an address's names with and without raw bytes.
use anyhow::Result;
use serde_json::json;
use sqlx::PgPool;

use super::{NameKey, candidates_on};
use crate::{
    ReverseIdentityRoles, ReverseIdentityStorageInput,
    families::textless_tests::{CHAIN, Fixture, OWNER, with_fixture},
};

async fn candidates(
    pool: &PgPool,
    primary: &serde_json::Value,
    (is_primary, rank): (bool, i16),
    after: Option<&NameKey>,
    limit: i64,
) -> Result<Vec<NameKey>> {
    let input = ReverseIdentityStorageInput {
        address: OWNER.to_owned(),
        coin_type: "60".to_owned(),
        roles: ReverseIdentityRoles::Owned,
        page_size: limit,
        cursor: None,
    };
    let mut conn = pool.acquire().await?;
    let rows = candidates_on(
        &mut conn,
        &input,
        &["ens".to_owned()],
        primary,
        is_primary,
        rank,
        after,
        None,
        limit,
    )
    .await?;
    Ok(rows
        .into_iter()
        .map(|(_, name, namespace, namehash)| (name, namespace, namehash))
        .collect())
}

fn names(keys: &[NameKey]) -> Vec<&str> {
    keys.iter().map(|(name, ..)| name.as_str()).collect()
}

#[tokio::test]
async fn the_candidates_include_names_without_bytes_in_served_name_order() -> Result<()> {
    with_fixture(
        "families_reverse_textless",
        async |pool, fixture: &Fixture| {
            let first_name = fixture.first_name();
            let known_name = format!("known.{first_name}");
            let late = [&fixture.late, &fixture.alpha, &fixture.eth];
            // The address holds five names, one of them not published yet, and controls a sixth.
            for (path, relation) in [
                (fixture.alpha_path().to_vec(), "token_holder"),
                (
                    vec![&fixture.zeta, &fixture.alpha, &fixture.eth],
                    "token_holder",
                ),
                (fixture.first_path().to_vec(), "token_holder"),
                (fixture.below_first(&fixture.known).to_vec(), "token_holder"),
                (late.to_vec(), "token_holder"),
                (
                    fixture.below_first(&fixture.nested).to_vec(),
                    "effective_controller",
                ),
            ] {
                sqlx::query(
                    "INSERT INTO project_address_name_index (address, logical_name_id, relation,
                     chain_id)
                 VALUES ($1, $2, $3, $4)",
                )
                .bind(OWNER)
                .bind(fixture.id(&path))
                .bind(relation)
                .bind(CHAIN)
                .execute(pool)
                .await?;
            }
            let no_primary = json!({});
            let held: Vec<String> =
                sqlx::query_scalar("SELECT name FROM unnest($1::text[]) name ORDER BY name")
                    .bind(vec![
                        "alpha.eth".to_owned(),
                        "zeta.alpha.eth".to_owned(),
                        first_name.clone(),
                        known_name.clone(),
                    ])
                    .fetch_all(pool)
                    .await?;
            let all = candidates(pool, &no_primary, (false, 0), None, 10).await?;
            assert_eq!(names(&all), held);
            assert!(
                candidates(pool, &no_primary, (true, 0), None, 10)
                    .await?
                    .is_empty()
            );

            // One at a time, each batch continues after the last key of either kind of surface.
            let mut walked: Vec<NameKey> = Vec::new();
            loop {
                let batch = candidates(pool, &no_primary, (false, 0), walked.last(), 1).await?;
                if batch.is_empty() {
                    break;
                }
                walked.extend(batch);
            }
            assert_eq!(walked, all);

            // A primary name is matched on the served name of a surface without bytes.
            let primary = json!({"ens": known_name});
            assert_eq!(
                names(&candidates(pool, &primary, (true, 0), None, 10).await?),
                [known_name.as_str()]
            );
            let others = candidates(pool, &primary, (false, 0), None, 10).await?;
            assert_eq!(others.len(), 3);
            assert!(!names(&others).contains(&known_name.as_str()));

            let controlled = candidates(pool, &no_primary, (false, 1), None, 10).await?;
            assert_eq!(controlled.len(), 1);
            assert!(controlled[0].0.ends_with(&first_name));
            Ok(())
        },
    )
    .await
}
