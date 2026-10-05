//! The expiry walk's `parent` test over names with and without raw bytes.
use anyhow::Result;
use sqlx::PgPool;

use super::expiry_pairs;
use crate::{
    NameCurrentExpiringFilter, UnixSeconds,
    families::textless_tests::{CHAIN, Fixture, with_fixture},
};

async fn walked(pool: &PgPool, parent: Option<&str>) -> Result<Vec<String>> {
    let filter = NameCurrentExpiringFilter {
        namespace: "ens".to_owned(),
        expires_after: UnixSeconds::from_seconds(0),
        expires_before: None,
        authorities: None,
        parent: parent.map(str::to_owned),
    };
    let mut conn = pool.acquire().await?;
    let pairs = expiry_pairs(&mut conn, &filter, (Some(0), None), true, None, 50).await?;
    Ok(pairs.into_iter().map(|(_, name)| name).collect())
}

#[tokio::test]
async fn the_parent_test_matches_a_name_without_bytes_on_its_served_name() -> Result<()> {
    with_fixture(
        "families_expiry_textless",
        async |pool, fixture: &Fixture| {
            let beta = fixture.id(&[&fixture.beta, &fixture.alpha, &fixture.eth]);
            let first = fixture.id(&fixture.first_path());
            let nested = fixture.id(&fixture.below_first(&fixture.nested));
            // A lease on a name with bytes, on one without, and on a child of the one without.
            for (index, name) in [&beta, &first, &nested].into_iter().enumerate() {
                sqlx::query(
                    "INSERT INTO project_lifecycle_event (chain_id, state_kind, state_key,
                     block_number, event_identity, event_kind, source_family,
                     original_logical_name_id, expiry, expiry_seconds)
                 VALUES ($1, 'triple', $2, 2, 'expiry:' || $3, 'RegistrationGranted',
                     'ens_v1_registrar_l1', $3, to_jsonb($4::bigint), $4)",
                )
                .bind(CHAIN)
                .bind(format!("[\"{name}\"]"))
                .bind(name)
                .bind(1_800_000_000_i64 + i64::try_from(index)?)
                .execute(pool)
                .await?;
            }
            assert_eq!(
                walked(pool, None).await?,
                [beta.clone(), first.clone(), nested.clone()]
            );
            assert_eq!(walked(pool, Some("alpha.eth")).await?, [beta, first]);
            assert_eq!(walked(pool, Some(&fixture.first_name())).await?, [nested]);
            assert!(walked(pool, Some("eth")).await?.is_empty());
            Ok(())
        },
    )
    .await
}
