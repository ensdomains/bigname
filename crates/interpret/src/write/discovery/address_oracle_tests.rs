//! The row-by-row address writer this module replaced, kept verbatim as the oracle. Every
//! input shape must leave the same rows and the same outcome under both writers.
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use serde_json::json;
use sqlx::types::Uuid;

use super::*;
use crate::ErrorKind;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type ActiveAddressEpoch = (i64, Option<i64>, Option<String>, Option<i64>, Option<i64>);

async fn reference_write(
    transaction: &mut Transaction<'_, Postgres>,
    addresses: &[ContractAddress],
) -> Result<()> {
    for address in addresses {
        let active: Option<ActiveAddressEpoch> = sqlx::query_as(
            "
            SELECT current.contract_instance_address_id,
                   current.active_from_block_number,
                   current.active_from_block_hash,
                   (
                       SELECT max(history.active_to_block_number)
                       FROM contract_instance_addresses history
                       WHERE history.contract_instance_id = current.contract_instance_id
                         AND history.chain_id = current.chain_id
                         AND lower(history.address) = lower(current.address)
                         AND history.contract_instance_address_id <>
                             current.contract_instance_address_id
                   ) AS prior_epoch_end,
                   (
                       SELECT max(history.active_to_block_number)
                       FROM contract_instance_addresses history
                       WHERE history.contract_instance_id = current.contract_instance_id
                         AND history.chain_id = current.chain_id
                         AND lower(history.address) = lower(current.address)
                         AND history.deactivated_at IS NOT NULL
                         AND history.provenance ->> 'source' IN
                             ('manifest_declaration', 'manifest_proxy_implementation')
                   ) AS manifest_retired_through
            FROM contract_instance_addresses current
            WHERE current.contract_instance_id = $1
              AND current.chain_id = $2
              AND lower(current.address) = lower($3)
              AND current.deactivated_at IS NULL
            ORDER BY current.admitted_at DESC
            LIMIT 1
            FOR UPDATE
            ",
        )
        .bind(address.contract_instance_id)
        .bind(&address.chain_id)
        .bind(&address.address)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to lock discovered address epoch", error)
        })?;
        if let Some((row_id, current_start, current_hash, prior_end, retired_through)) = active {
            if retired_through.is_some_and(|end| address.active_from_block_number <= end) {
                continue;
            }
            let bounded_start = bound_address_epoch(address.active_from_block_number, prior_end)?;
            let effective_start = current_start.map(|start| start.min(bounded_start));
            let effective_hash = if current_start == effective_start {
                current_hash
            } else if effective_start == Some(address.active_from_block_number) {
                Some(address.active_from_block_hash.clone())
            } else {
                None
            };
            sqlx::query(
                "
                UPDATE contract_instance_addresses
                SET active_from_block_number = $2,
                    active_from_block_hash = $3,
                    source_manifest_id = CASE
                        WHEN provenance ->> 'source' IN
                            ('manifest_declaration', 'manifest_proxy_implementation')
                        THEN source_manifest_id
                        ELSE $4
                    END,
                    provenance = CASE
                        WHEN provenance ->> 'source' IN
                            ('manifest_declaration', 'manifest_proxy_implementation')
                        THEN provenance
                        ELSE $5
                    END
                WHERE contract_instance_address_id = $1
                ",
            )
            .bind(row_id)
            .bind(effective_start)
            .bind(effective_hash)
            .bind(address.source_manifest_id)
            .bind(&address.provenance)
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                InterpretError::database("failed to backdate discovered address", error)
            })?;
            continue;
        }
        let (prior_end, manifest_retired_through): (Option<i64>, Option<i64>) = sqlx::query_as(
            "
            SELECT max(active_to_block_number),
                   max(active_to_block_number) FILTER (
                       WHERE deactivated_at IS NOT NULL
                         AND provenance ->> 'source' IN
                             ('manifest_declaration', 'manifest_proxy_implementation')
                   )
            FROM contract_instance_addresses
            WHERE contract_instance_id = $1
              AND chain_id = $2
              AND lower(address) = lower($3)
            ",
        )
        .bind(address.contract_instance_id)
        .bind(&address.chain_id)
        .bind(&address.address)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| {
            InterpretError::database("failed to bound new discovered address epoch", error)
        })?;
        if manifest_retired_through.is_some_and(|end| address.active_from_block_number <= end) {
            continue;
        }
        let bounded_start = bound_address_epoch(address.active_from_block_number, prior_end)?;
        let bounded_hash = (bounded_start == address.active_from_block_number)
            .then_some(&address.active_from_block_hash);
        sqlx::query(
            "
            INSERT INTO contract_instance_addresses (
                contract_instance_id, chain_id, address,
                active_from_block_number, active_from_block_hash,
                source_manifest_id, provenance
            )
            SELECT $1, $2, lower($3), $4, $5, $6, $7
            WHERE NOT EXISTS (
                SELECT 1
                FROM contract_instance_addresses existing
                WHERE existing.chain_id = $2
                  AND lower(existing.address) = lower($3)
                  AND existing.deactivated_at IS NULL
            )
            ",
        )
        .bind(address.contract_instance_id)
        .bind(&address.chain_id)
        .bind(&address.address)
        .bind(bounded_start)
        .bind(bounded_hash)
        .bind(address.source_manifest_id)
        .bind(&address.provenance)
        .execute(&mut **transaction)
        .await
        .map_err(|error| InterpretError::database("failed to write discovered address", error))?;
    }
    Ok(())
}

fn bound_address_epoch(event_block: i64, prior_end: Option<i64>) -> Result<i64> {
    let Some(prior_end) = prior_end else {
        return Ok(event_block);
    };
    let floor = prior_end.checked_add(1).ok_or_else(|| {
        InterpretError::data_integrity("discovered address epoch end overflowed BIGINT")
    })?;
    Ok(event_block.max(floor))
}

const LOWER: &str = "0x00000000000000000000000000000000000000ab";
const UPPER: &str = "0x00000000000000000000000000000000000000AB";
const OTHER: &str = "0x00000000000000000000000000000000000000cd";
const ACTIVE_20: &str = r#"INSERT INTO contract_instance_addresses (
    contract_instance_id, chain_id, address, active_from_block_number, active_from_block_hash,
    source_manifest_id, provenance
) VALUES ('{one}', '{chain}', '0x00000000000000000000000000000000000000ab', 20,
          'block-20-step-7', 7, '{"source":"raw_log","step":7}')"#;
const RETIRED_MANIFEST_2_9: &str = r#"INSERT INTO contract_instance_addresses (
    contract_instance_id, chain_id, address, active_from_block_number, active_to_block_number,
    provenance, admitted_at, deactivated_at
) VALUES ('{one}', '{chain}', '0x00000000000000000000000000000000000000ab', 2, 9,
          '{"source":"manifest_declaration"}', now() - interval '2 hours', now() - interval '1 hour')"#;

enum Step {
    Write(Vec<ContractAddress>),
    Redo(i64, i64),
}

struct Shape {
    name: &'static str,
    seed: Vec<String>,
    transactions: Vec<Vec<Step>>,
}

fn shape(name: &'static str, seed: &[&str], transactions: Vec<Vec<Step>>) -> Shape {
    Shape {
        name,
        seed: seed.iter().map(|sql| (*sql).to_owned()).collect(),
        transactions,
    }
}

/// Instance 1 or 2 of the shape, which the runner moves onto the shape's own chain.
fn seen(instance: u128, address: &str, block: i64, step: i64) -> ContractAddress {
    seen_with(
        instance,
        address,
        block,
        step,
        json!({"source":"raw_log","step":step}),
    )
}

fn seen_with(
    instance: u128,
    address: &str,
    block: i64,
    step: i64,
    provenance: Value,
) -> ContractAddress {
    ContractAddress {
        contract_instance_id: Uuid::from_u128(instance),
        chain_id: String::new(),
        address: address.to_owned(),
        active_from_block_number: block,
        active_from_block_hash: format!("block-{block}-step-{step}"),
        source_manifest_id: step,
        provenance,
    }
}

fn one_write(addresses: Vec<ContractAddress>) -> Vec<Vec<Step>> {
    vec![vec![Step::Write(addresses)]]
}

fn shapes() -> Vec<Shape> {
    let float = json!({"source":"raw_log","value":1.5});
    vec![
        shape("empty batch", &[ACTIVE_20], one_write(Vec::new())),
        shape(
            "single new address",
            &[],
            one_write(vec![seen(1, LOWER, 10, 1)]),
        ),
        shape(
            "single unchanged observation",
            &[ACTIVE_20],
            one_write(vec![seen(1, LOWER, 20, 7)]),
        ),
        shape(
            "unchanged observation repeated",
            &[ACTIVE_20],
            one_write(vec![seen(1, LOWER, 20, 7); 5]),
        ),
        shape(
            "repeats move the start both ways and change provenance",
            &[ACTIVE_20],
            one_write(vec![
                seen(1, LOWER, 15, 1),
                seen(1, LOWER, 25, 2),
                seen(1, LOWER, 10, 3),
                seen(1, LOWER, 10, 4),
                seen(1, LOWER, 30, 5),
            ]),
        ),
        shape(
            "two spellings of one address interleaved",
            &[ACTIVE_20],
            one_write(vec![
                seen(1, LOWER, 15, 1),
                seen(1, UPPER, 12, 2),
                seen(1, LOWER, 12, 3),
                seen(1, UPPER, 25, 4),
            ]),
        ),
        shape(
            "inserted row observed again",
            &[],
            one_write(vec![
                seen(1, LOWER, 20, 1),
                seen(1, LOWER, 10, 2),
                seen(1, LOWER, 10, 2),
                seen(1, UPPER, 5, 3),
            ]),
        ),
        shape(
            "two instances claim one address",
            &[],
            one_write(vec![
                seen(1, LOWER, 20, 1),
                seen(2, LOWER, 30, 2),
                seen(1, LOWER, 10, 3),
                seen(2, LOWER, 5, 4),
            ]),
        ),
        shape(
            "one instance at two addresses",
            &[],
            one_write(vec![seen(1, LOWER, 20, 1), seen(1, OTHER, 30, 2)]),
        ),
        shape(
            "observations around a manifest retirement with an active row",
            &[RETIRED_MANIFEST_2_9, ACTIVE_20],
            one_write(vec![
                seen(1, LOWER, 5, 1),
                seen(1, LOWER, 9, 2),
                seen(1, LOWER, 10, 3),
                seen(1, LOWER, 7, 4),
                seen(1, LOWER, 11, 5),
            ]),
        ),
        shape(
            "retired manifest history without an active row",
            &[RETIRED_MANIFEST_2_9],
            one_write(vec![
                seen(1, LOWER, 5, 1),
                seen(1, LOWER, 12, 2),
                seen(1, LOWER, 3, 3),
                seen(1, LOWER, 11, 4),
            ]),
        ),
        shape(
            "manifest-declared active row with an omitted start",
            &[r#"INSERT INTO contract_instance_addresses (
                contract_instance_id, chain_id, address, source_manifest_id, provenance
            ) VALUES ('{one}', '{chain}', '0x00000000000000000000000000000000000000ab', 99,
                      '{"source":"manifest_declaration","kept":true}')"#],
            one_write(vec![
                seen(1, LOWER, 20, 1),
                seen(1, LOWER, 5, 2),
                seen(1, LOWER, 20, 1),
            ]),
        ),
        shape(
            "an observation carrying manifest provenance freezes later metadata",
            &[ACTIVE_20],
            one_write(vec![
                seen(1, LOWER, 15, 1),
                seen_with(
                    1,
                    LOWER,
                    12,
                    2,
                    json!({"source":"manifest_proxy_implementation","step":2}),
                ),
                seen(1, LOWER, 11, 3),
                seen(1, LOWER, 11, 3),
            ]),
        ),
        shape(
            "repeated provenance that is not flat JSON",
            &[r#"INSERT INTO contract_instance_addresses (
                contract_instance_id, chain_id, address, active_from_block_number,
                active_from_block_hash, source_manifest_id, provenance
            ) VALUES ('{one}', '{chain}', '0x00000000000000000000000000000000000000ab', 20,
                      'block-20-step-7', 7, '{"source":"raw_log","value":1.5}')"#],
            one_write(vec![
                seen_with(1, LOWER, 20, 7, float.clone()),
                seen_with(1, LOWER, 20, 7, float),
                seen_with(
                    1,
                    LOWER,
                    20,
                    7,
                    json!({"source":"raw_log","nested":{"a":[1]}}),
                ),
                seen_with(
                    1,
                    LOWER,
                    20,
                    7,
                    json!({"source":"raw_log","nested":{"a":[1]}}),
                ),
            ]),
        ),
        shape(
            "invalid JSON after an unchanged observation",
            &[ACTIVE_20],
            one_write(vec![
                seen(1, LOWER, 20, 7),
                seen_with(
                    1,
                    LOWER,
                    20,
                    7,
                    json!({"source":"raw_log","bad":"nul\u{0}"}),
                ),
            ]),
        ),
        shape(
            "negative block after a cached row",
            &[ACTIVE_20],
            one_write(vec![seen(1, LOWER, 20, 7), seen(1, LOWER, -1, 8)]),
        ),
        shape(
            "an earlier non-manifest epoch bounds the start",
            &[r#"INSERT INTO contract_instance_addresses (
                contract_instance_id, chain_id, address, active_from_block_number,
                active_to_block_number, provenance, admitted_at, deactivated_at
            ) VALUES ('{one}', '{chain}', '0x00000000000000000000000000000000000000ab', 2, 15,
                      '{"source":"raw_log"}', now() - interval '2 hours', now() - interval '1 hour')"#],
            one_write(vec![
                seen(1, LOWER, 10, 1),
                seen(1, LOWER, 12, 2),
                seen(1, LOWER, 20, 3),
            ]),
        ),
        shape(
            "repeats across two writes in one transaction",
            &[ACTIVE_20],
            vec![vec![
                Step::Write(vec![seen(1, LOWER, 15, 1), seen(1, LOWER, 15, 1)]),
                Step::Write(vec![seen(1, LOWER, 12, 2), seen(1, UPPER, 12, 2)]),
            ]],
        ),
        shape(
            "repeats across transactions",
            &[ACTIVE_20],
            vec![
                vec![Step::Write(vec![seen(1, LOWER, 15, 1)])],
                vec![Step::Write(vec![
                    seen(1, LOWER, 15, 1),
                    seen(1, LOWER, 12, 2),
                ])],
            ],
        ),
        shape(
            "redo deletes the row before the batch re-observes it",
            &[ACTIVE_20],
            vec![vec![
                Step::Redo(15, 25),
                Step::Write(vec![
                    seen(1, LOWER, 22, 1),
                    seen(1, LOWER, 18, 2),
                    seen(1, LOWER, 22, 1),
                ]),
            ]],
        ),
        shape(
            "redo resumed in the next transaction",
            &[ACTIVE_20],
            vec![
                vec![Step::Redo(15, 25), Step::Write(vec![seen(1, LOWER, 22, 1)])],
                vec![Step::Write(vec![
                    seen(1, LOWER, 18, 2),
                    seen(1, UPPER, 18, 2),
                ])],
            ],
        ),
        shape(
            "a later transaction's redo deletes a row an earlier write read",
            &[ACTIVE_20],
            vec![
                vec![Step::Write(vec![seen(1, LOWER, 22, 1)])],
                vec![Step::Redo(15, 25), Step::Write(vec![seen(1, LOWER, 18, 2)])],
            ],
        ),
        shape(
            "a failed transaction is retried",
            &[],
            vec![
                vec![Step::Write(vec![
                    seen(1, LOWER, 20, 1),
                    seen(1, OTHER, 30, 2),
                ])],
                vec![Step::Write(vec![
                    seen(1, LOWER, 20, 1),
                    seen(1, LOWER, 20, 1),
                ])],
            ],
        ),
    ]
}

type Outcome = (&'static str, Vec<Option<ErrorKind>>, Vec<Value>);

async fn database(name: &str) -> TestResult<TestDatabase> {
    let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
    database.create_phase_schema().await?;
    for sql in [
        include_str!("../../../../storage/schema/baseline/01_chain.sql"),
        include_str!("../../../../storage/schema/baseline/03_identity.sql"),
        include_str!("../../../../storage/schema/baseline/04_manifests.sql"),
        include_str!("../../../../storage/schema/baseline/05_normalized_events.sql"),
    ] {
        sqlx::raw_sql(sql).execute(database.pool()).await?;
    }
    // No manifest row backs the observations' manifest ids. Both writers bind the same ids.
    sqlx::raw_sql(
        "ALTER TABLE contract_instance_addresses
         DROP CONSTRAINT contract_instance_addresses_manifest_fkey",
    )
    .execute(database.pool())
    .await?;
    Ok(database)
}

async fn run(database: &TestDatabase, reference: bool) -> TestResult<Vec<Outcome>> {
    let mut outcomes = Vec::new();
    for (index, shape) in shapes().into_iter().enumerate() {
        let chain = format!("oracle-{index}");
        let instances = [1, 2].map(|n| Uuid::from_u128(index as u128 * 2 + n));
        sqlx::query(
            "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
             VALUES ($1, $3, 'contract'), ($2, $3, 'contract')",
        )
        .bind(instances[0])
        .bind(instances[1])
        .bind(&chain)
        .execute(database.pool())
        .await?;
        for seed in &shape.seed {
            let seed = seed
                .replace("{chain}", &chain)
                .replace("{one}", &instances[0].to_string());
            sqlx::raw_sql(&seed).execute(database.pool()).await?;
        }
        let mut results = Vec::new();
        for steps in shape.transactions {
            let mut transaction = database.pool().begin().await?;
            let mut result = Ok(());
            for step in steps {
                result = match step {
                    Step::Write(addresses) => {
                        let addresses: Vec<_> = addresses
                            .into_iter()
                            .map(|mut address| {
                                address.chain_id.clone_from(&chain);
                                address.contract_instance_id =
                                    instances[address.contract_instance_id.as_u128() as usize - 1];
                                address
                            })
                            .collect();
                        if reference {
                            reference_write(&mut transaction, &addresses).await
                        } else {
                            write(&mut transaction, &addresses).await
                        }
                    }
                    Step::Redo(from, to) => {
                        crate::write::prepare_redo_range(&mut transaction, &chain, from, to).await
                    }
                };
                if result.is_err() {
                    break;
                }
            }
            match result {
                Ok(()) => {
                    transaction.commit().await?;
                    results.push(None);
                }
                Err(error) => {
                    transaction.rollback().await?;
                    results.push(Some(error.kind()));
                }
            }
        }
        let rows: Vec<Value> = sqlx::query_scalar(
            "SELECT to_jsonb(address) - 'admitted_at' - 'deactivated_at' - 'contract_instance_id'
                    || jsonb_build_object(
                        'instance', address.contract_instance_id = $2,
                        'admitted', dense_rank() OVER (ORDER BY admitted_at),
                        'deactivated', deactivated_at IS NOT NULL)
             FROM contract_instance_addresses address
             WHERE chain_id = $1
             ORDER BY contract_instance_address_id",
        )
        .bind(&chain)
        .bind(instances[0])
        .fetch_all(database.pool())
        .await?;
        outcomes.push((shape.name, results, rows));
    }
    Ok(outcomes)
}

#[tokio::test]
async fn every_shape_matches_the_row_by_row_writer() -> TestResult {
    let reference_db = database("interpret_address_oracle_reference").await?;
    let branch_db = database("interpret_address_oracle_cached").await?;
    let expected = run(&reference_db, true).await?;
    let actual = run(&branch_db, false).await?;
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(&expected) {
        assert_eq!(actual, expected, "shape: {}", expected.0);
    }
    assert!(
        expected
            .iter()
            .any(|(_, results, _)| results.contains(&Some(ErrorKind::DataIntegrity)))
            && expected
                .iter()
                .any(|(_, results, _)| results.contains(&Some(ErrorKind::Transient))),
        "the shapes cover both failure kinds"
    );
    reference_db.cleanup().await?;
    branch_db.cleanup().await?;
    Ok(())
}
