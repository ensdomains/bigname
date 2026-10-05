//! Reverse-name hydration fixtures shared by the head-rule and failure suites: tuples whose
//! resolver is the event-silent reverse resolver, so only an RPC read can name them.
#![allow(dead_code)]
use anyhow::Result;
use bigname_project::{
    Marker, ProjectError,
    families::{self, FamilyMode, FamilyOptions, FamilyOutcome},
};
use serde_json::{Value, json};

use super::{
    rpc,
    support::{CONTENT_HASH, Event, Fixture, marker},
};

pub const CHAIN: &str = "ethereum-mainnet";
pub const SILENT: &str = "0xa2c122be93b0074270ebee7f6b7292c7deb45047";

pub fn address(index: i64) -> String {
    format!("0x{index:040x}")
}

// ReverseClaimed is admitted only for namehash(<lowercase address>.addr.reverse), as
// crates/adapters/src/schema_v2/protocol/v1/reverse.rs:88-91 enforces.
pub fn node(index: i64) -> String {
    let labels = [
        format!("{index:040x}"),
        "addr".to_owned(),
        "reverse".to_owned(),
    ];
    let hash = labels
        .iter()
        .rev()
        .fold(alloy_primitives::B256::ZERO, |parent, label| {
            let label = alloy_primitives::keccak256(label.as_bytes());
            alloy_primitives::keccak256([parent.as_slice(), label.as_slice()].concat())
        });
    format!("{hash:#x}")
}

/// A reverse claim of address `index` in `block`, pointed at the event-silent resolver.
pub async fn seed(fixture: &Fixture, block: i64, index: i64) -> Result<()> {
    let node = node(index);
    fixture
        .event(
            Event::new(
                &format!("claim:{block}:{index}"),
                block,
                index * 2,
                "ReverseChanged",
                "ens_v1_reverse_l1",
            )
            .on(CHAIN)
            .after(json!({
                "source_event":"ReverseClaimed", "address":address(index), "coin_type":"60",
                "namespace":"ens", "reverse_node":node
            })),
        )
        .await?;
    fixture
        .event(
            Event::new(
                &format!("pointer:{block}:{index}"),
                block,
                index * 2 + 1,
                "ResolverChanged",
                "ens_v1_registry_l1",
            )
            .on(CHAIN)
            .after(json!({
                "source_event":"NewResolver", "node":node, "resolver":SILENT
            })),
        )
        .await?;
    Ok(())
}

pub fn options(rpc: &rpc::Rpc) -> FamilyOptions {
    FamilyOptions::new(CONTENT_HASH).with_hydration(rpc.urls())
}

/// One family run to `target`, whatever the readable head is.
pub async fn apply(
    fixture: &Fixture,
    target: &Marker,
    mode: FamilyMode,
    options: &FamilyOptions,
) -> (FamilyOutcome, Option<ProjectError>) {
    let token = families::input_token(&fixture.pool, CHAIN)
        .await
        .expect("input token");
    families::run(&fixture.pool, CHAIN, target, mode, &token, options).await
}

/// Make `block` the highest readable block, then run the families to it.
pub async fn run(
    fixture: &Fixture,
    block: i64,
    mode: FamilyMode,
    rpc: &rpc::Rpc,
) -> Result<FamilyOutcome> {
    rpc::head(&fixture.pool, block).await?;
    let (outcome, error) = apply(fixture, &marker(block), mode, &options(rpc)).await;
    match error {
        Some(error) => Err(error.into()),
        None => Ok(outcome),
    }
}

pub async fn tuple(fixture: &Fixture, index: i64) -> Result<Value> {
    Ok(
        sqlx::query_scalar("SELECT to_jsonb(t) FROM project_reverse_tuple t WHERE address=$1")
            .bind(address(index))
            .fetch_one(&fixture.pool)
            .await?,
    )
}

/// The primary-name claim of address `index` as the reader serves it.
pub async fn claim(
    fixture: &Fixture,
    index: i64,
) -> Result<bigname_storage::PrimaryNameCurrentSnapshot> {
    Ok(
        bigname_storage::families::records::load_family_primary_name_snapshot(
            &fixture.pool,
            &address(index),
            "ens",
            "60",
        )
        .await?
        .expect("reverse tuple"),
    )
}

/// Undo journal rows of `family` written by `block`: the rows that block changed.
pub async fn journalled(fixture: &Fixture, block: i64, family: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM project_family_undo
         WHERE chain_id = $1 AND block_number = $2 AND family = $3",
    )
    .bind(CHAIN)
    .bind(block)
    .bind(family)
    .fetch_one(&fixture.pool)
    .await?)
}

/// Tuples per `attempt_block`, the never-attempted ones under `None`.
pub async fn attempts(fixture: &Fixture) -> Result<Vec<(Option<i64>, i64)>> {
    Ok(sqlx::query_as(
        "SELECT attempt_block, count(*) FROM project_reverse_tuple
         GROUP BY attempt_block ORDER BY attempt_block NULLS FIRST",
    )
    .fetch_all(&fixture.pool)
    .await?)
}

/// A canonical block `number` off the fixture's own hashes, the child of `parent`.
pub async fn fork_block(fixture: &Fixture, number: i64, parent: &str) -> Result<Marker> {
    let hash = format!("0x{}{number:02x}", "f".repeat(62));
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
             block_timestamp, canonicality_state)
         VALUES ($1, $2, $3, $4, to_timestamp(1800000000 + $4 * 12), 'canonical')",
    )
    .bind(CHAIN)
    .bind(&hash)
    .bind(parent)
    .bind(number)
    .execute(&fixture.pool)
    .await?;
    Ok(Marker { number, hash })
}

/// The reverse work index beside what its source rows derive: every fixture tuple is eligible.
pub async fn work_index(fixture: &Fixture) -> Result<(Vec<Value>, Vec<Value>)> {
    let index = sqlx::query_scalar(
        "SELECT jsonb_build_array(address, attempt_ordinal, attempt_block, successful_at_block,
             attempt_failures)
         FROM project_reverse_hydration_work WHERE eligible ORDER BY address",
    )
    .fetch_all(&fixture.pool)
    .await?;
    let derived = sqlx::query_scalar(
        "SELECT jsonb_build_array(address, attempt_ordinal, attempt_block,
             CASE WHEN hydrated_name IS NOT NULL THEN attempt_block END, attempt_failures)
         FROM project_reverse_tuple ORDER BY address",
    )
    .fetch_all(&fixture.pool)
    .await?;
    Ok((index, derived))
}

/// Exercise the production queue selection at a candidate retry height without publishing
/// thousands of empty fixture blocks. Head eligibility remains covered by the Follow tests.
pub async fn selected_addresses(fixture: &Fixture, block: i64) -> Result<Vec<String>> {
    let sql = include_str!("../../src/families/hydrate/reverse.sql").replace(
        "{pointer_emission_ordinal}",
        &bigname_storage::families::position::emission_ordinal_sql(
            "p.event_identity",
            "p.transaction_index",
            "p.log_index",
        ),
    );
    let rows: Vec<Value> = sqlx::query_scalar(&sql)
        .bind(CHAIN)
        .bind(block)
        .bind(super::support::hash(block))
        .bind(json!([]))
        .bind(json!([]))
        .bind(json!([]))
        .bind(json!([]))
        .bind(json!([]))
        .bind(vec![SILENT])
        .bind(json!([]))
        .bind(false)
        .fetch_all(&fixture.pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| row["address"].as_str().unwrap().to_owned())
        .collect())
}
