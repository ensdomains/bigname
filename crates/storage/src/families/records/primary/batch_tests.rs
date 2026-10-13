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

const RESOLVER: &str = "0x00000000000000000000000000000000000000a1";

fn addr_reverse_node(address: &str) -> String {
    let label = address.trim_start_matches("0x");
    format!(
        "{:#x}",
        crate::ens_namehash_label_bytes(&[label.as_bytes(), b"addr", b"reverse"])
    )
}

// The batch and single readers agree, value by value, on every reverse-node resolver shape: a
// nonzero resolver answers alone, and the `default.reverse` name is served only without one.
#[tokio::test]
async fn primary_batch_matches_individual_across_reverse_node_resolvers() -> Result<()> {
    with_fixture("primary_batch_resolvers", async |pool, _| {
        let (block, hash): (i64, String) = sqlx::query_as(
            "SELECT block_number, block_hash FROM bigname_phase.chain_lineage
             WHERE chain_id = $1 AND canonicality_state IN ('canonical', 'safe', 'finalized')
             ORDER BY block_number LIMIT 1",
        )
        .bind(CHAIN)
        .fetch_one(pool)
        .await?;
        // (reverse node's resolver, addr.reverse tuple, its hydrated name, default name)
        let cases = [
            // A resolver the projection holds no name for.
            (Some(RESOLVER), true, None::<&str>, Some("default0.eth")),
            (Some(RESOLVER), true, Some(""), Some("default1.eth")),
            // Claimed through a registrar no manifest admits: only the registry pointer exists.
            (Some(RESOLVER), false, None, Some("default2.eth")),
            (Some(RESOLVER), true, Some("named.eth"), Some("default3.eth")),
            (Some(RESOLVER), true, Some(" "), Some("default4.eth")),
            (Some(RESOLVER), true, None, None),
            (Some(super::ZERO_ADDRESS), true, None, Some("default6.eth")),
            (None, true, None, Some("default7.eth")),
        ];
        let addresses = (1..=cases.len())
            .map(|n| format!("0x{n:040x}"))
            .collect::<Vec<_>>();
        for (index, (resolver, tuple, hydrated, default)) in cases.into_iter().enumerate() {
            let address = &addresses[index];
            let node = addr_reverse_node(address);
            if let Some(resolver) = resolver {
                sqlx::query(
                    "INSERT INTO bigname_phase.project_registry_pointer
                     (chain_id, namespace, node, block_number, event_identity, resolver_address,
                      source_family)
                     VALUES ($1, 'ens', $2, $3, $4, $5, 'ens_v1_registry_l1')",
                )
                .bind(CHAIN)
                .bind(&node)
                .bind(block)
                .bind(format!("primary-batch-pointer:{index}"))
                .bind(resolver)
                .execute(pool)
                .await?;
            }
            if tuple {
                let identity = format!("primary-batch-claim:{index}");
                sqlx::query(
                    "INSERT INTO bigname_phase.project_reverse_tuple
                     (address, coin_type, namespace, chain_id, block_number, event_identity,
                      reverse_position, reverse_node, source_event, hydrated_name, attempt_block,
                      attempt_hash, baseline)
                     VALUES ($1, '60', 'ens', $2, $3, $4, $5, $6, 'ReverseClaimed', $7, $3, $8, $9)",
                )
                .bind(address)
                .bind(CHAIN)
                .bind(block)
                .bind(&identity)
                .bind(json!({"block_number": block, "event_identity": identity}))
                .bind(&node)
                .bind(hydrated)
                .bind(&hash)
                .bind(json!({"reverse_node": node, "resolver_address": resolver}))
                .execute(pool)
                .await?;
            }
            if let Some(default) = default {
                let identity = format!("primary-batch-default:{index}");
                sqlx::query(
                    "INSERT INTO bigname_phase.project_reverse_tuple
                     (address, coin_type, namespace, chain_id, block_number, event_identity,
                      reverse_position, hydrated_name, attempt_block, attempt_hash, baseline)
                     VALUES ($1, $2, 'ens', $3, $4, $5, $6, $7, $4, $8, '{}'::jsonb)",
                )
                .bind(address)
                .bind(super::DEFAULT_COIN_TYPE)
                .bind(CHAIN)
                .bind(block)
                .bind(&identity)
                .bind(json!({"block_number": block, "event_identity": identity}))
                .bind(default)
                .bind(&hash)
                .execute(pool)
                .await?;
            }
        }
        let inputs = addresses
            .iter()
            .map(|address| ReverseIdentityStorageInput {
                address: address.clone(),
                coin_type: "60".to_owned(),
                roles: ReverseIdentityRoles::Both,
                page_size: 10,
                cursor: None,
            })
            .collect::<Vec<_>>();
        let namespaces = vec!["ens".to_owned()];
        let chains = vec![CHAIN.to_owned()];
        let mut snapshot = crate::families::read_snapshot(pool).await?;
        let batched =
            super::batch::load(&mut snapshot, &inputs, &namespaces, Some(&chains)).await?;
        let mut served = Vec::new();
        for (input, actual) in inputs.iter().zip(batched) {
            let expected = super::load_family_primary_name_snapshots_on(
                &mut snapshot,
                &input.address,
                &[("ens".to_owned(), "60".to_owned())],
                Some(&chains),
            )
            .await?;
            assert_eq!(actual, expected, "{}", input.address);
            served.push(actual.into_values().next().map(|claim| {
                (
                    claim.row.claim_status.as_str(),
                    claim.row.raw_claim_name,
                    claim.row.claim_provenance.get("reverse_node").cloned(),
                )
            }));
        }
        snapshot.commit().await?;
        let addr = |index: usize, status: &'static str, name: Option<&str>| {
            Some((
                status,
                name.map(str::to_owned),
                Some(json!(addr_reverse_node(&addresses[index]))),
            ))
        };
        let default = |name: &str| Some(("success", Some(name.to_owned()), None));
        assert_eq!(
            served,
            vec![
                addr(0, "not_found", None),
                addr(1, "not_found", None),
                None,
                addr(3, "success", Some("named.eth")),
                addr(4, "not_found", None),
                addr(5, "not_found", None),
                default("default6.eth"),
                default("default7.eth"),
            ]
        );
        Ok(())
    })
    .await
}
