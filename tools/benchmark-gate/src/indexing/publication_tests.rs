use super::*;
use bigname_test_support::{TestDatabase, TestDatabaseConfig};

#[tokio::test]
async fn published_head_reapply_refuses_a_copy_projected_only_through_head_minus_one() {
    let database = TestDatabase::create(
        TestDatabaseConfig::new("benchmark_head_reapply_publication").pool_max_connections(1),
    )
    .await
    .unwrap();
    sqlx::raw_sql(
        "CREATE SCHEMA bigname_phase;
         CREATE TABLE chain_lineage (
             chain_id text NOT NULL,
             block_number bigint NOT NULL,
             block_hash text NOT NULL,
             canonicality_state text NOT NULL
         );
         CREATE TABLE bigname_phase.chain_heads (
             chain_id text PRIMARY KEY,
             latest_block_number bigint NOT NULL,
             latest_block_hash text NOT NULL
         );
         CREATE TABLE bigname_phase.chain_phase_state (
             chain_id text NOT NULL,
             phase_name text NOT NULL,
             phase_status text NOT NULL,
             current_block_number bigint,
             current_block_hash text,
             input_content_hash text,
             PRIMARY KEY (chain_id, phase_name)
         );
         INSERT INTO chain_lineage
         SELECT 'ethereum-mainnet', block, 'block-' || block, 'canonical'
         FROM generate_series(1, 16) block;
         INSERT INTO bigname_phase.chain_heads
         VALUES ('ethereum-mainnet', 16, 'block-16');
         INSERT INTO bigname_phase.chain_phase_state
         VALUES ('ethereum-mainnet', 'project', 'completed', 15, 'block-15',
                 'keccak256:prior');",
    )
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET input_content_hash = $1",
    )
    .bind(bigname_content_hash::INTERPRETER_CONTENT_HASH)
    .execute(database.pool())
    .await
    .unwrap();
    let budgets = crate::budgets::BudgetsFile::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../benchmarks/release-gate.toml"),
    )
    .unwrap();
    let input = IndexingInput {
        chain_id: "ethereum-mainnet".to_owned(),
        head_block: 16,
        walk_from_block: 1,
        walk_to_block: 16,
        hydration_rpc_urls: None,
    };

    let error = validate_input(
        database.pool(),
        &input,
        budgets.profile(crate::budgets::BudgetProfile::Smoke),
    )
    .await
    .expect_err("a published-head re-apply requires Project already published at the head")
    .to_string();

    assert!(error.contains("already be a completed Project publication"));

    sqlx::query(
        "UPDATE bigname_phase.chain_phase_state
         SET current_block_number = 16,
             current_block_hash = 'block-16'",
    )
    .execute(database.pool())
    .await
    .unwrap();
    validate_input(
        database.pool(),
        &input,
        budgets.profile(crate::budgets::BudgetProfile::Smoke),
    )
    .await
    .expect("an already-published current-generation head must be accepted");
    database.cleanup().await.unwrap();
}
