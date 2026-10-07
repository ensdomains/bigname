use anyhow::Result;
use serde_json::json;

use crate::{
    ReverseIdentityRoles, ReverseIdentityStorageInput,
    families::textless_tests::{CHAIN, with_fixture},
};

#[tokio::test]
async fn primary_batch_matches_individual_hydrated_fallback_and_missing_tuples() -> Result<()> {
    with_fixture("primary_batch_equivalence", async |pool, _| {
        let (block, hash): (i64, String) = sqlx::query_as(
            "SELECT block_number, block_hash FROM bigname_phase.chain_lineage
             WHERE chain_id = $1 AND canonicality_state IN ('canonical', 'safe', 'finalized')
             ORDER BY block_number LIMIT 1",
        )
        .bind(CHAIN)
        .fetch_one(pool)
        .await?;
        let addresses = (1..=4).map(|n| format!("0x{n:040x}")).collect::<Vec<_>>();
        for (index, coin, name) in [
            (0, "60", "direct.eth"),
            (0, super::DEFAULT_COIN_TYPE, "fallback.eth"),
            (1, super::DEFAULT_COIN_TYPE, "only-default.eth"),
            (2, "0", "invalid..eth"),
        ] {
            let identity = format!("primary-batch:{index}:{coin}");
            sqlx::query(
                "INSERT INTO bigname_phase.project_reverse_tuple
                 (address, coin_type, namespace, chain_id, block_number, event_identity,
                  reverse_position, hydrated_name, attempt_block, attempt_hash, baseline)
                 VALUES ($1, $2, 'ens', $3, $4, $5, $6, $7, $4, $8, '{}'::jsonb)",
            )
            .bind(&addresses[index])
            .bind(coin)
            .bind(CHAIN)
            .bind(block)
            .bind(&identity)
            .bind(json!({"block_number": block, "event_identity": identity}))
            .bind(name)
            .bind(&hash)
            .execute(pool)
            .await?;
        }
        let inputs = [
            (0, "60"),
            (1, "60"),
            (2, "0"),
            (3, "60"),
            (0, super::DEFAULT_COIN_TYPE),
            (0, "60"),
        ]
        .into_iter()
        .map(|(index, coin)| ReverseIdentityStorageInput {
            address: addresses[index].clone(),
            coin_type: coin.to_owned(),
            roles: ReverseIdentityRoles::Both,
            page_size: 10,
            cursor: None,
        })
        .collect::<Vec<_>>();
        let namespaces = vec!["ens".to_owned()];
        let chains = vec![CHAIN.to_owned()];
        for stale_attempt in [false, true] {
            if stale_attempt {
                sqlx::query(
                    "UPDATE bigname_phase.project_reverse_tuple SET attempt_hash = 'not-canonical'",
                )
                .execute(pool)
                .await?;
            }
            let mut snapshot = crate::families::read_snapshot(pool).await?;
            let batched =
                super::batch::load(&mut snapshot, &inputs, &namespaces, Some(&chains)).await?;
            for (input, actual) in inputs.iter().zip(batched) {
                let expected = super::load_family_primary_name_snapshots_on(
                    &mut snapshot,
                    &input.address,
                    &[("ens".to_owned(), input.coin_type.clone())],
                    Some(&chains),
                )
                .await?;
                assert_eq!(
                    actual, expected,
                    "{} {} stale={stale_attempt}",
                    input.address, input.coin_type
                );
            }
            snapshot.commit().await?;
        }
        Ok(())
    })
    .await
}
