use std::str::FromStr;

use anyhow::Result;
use bigname_test_support::{TestDatabase, TestDatabaseConfig, database_url_from_env};
use phase_runner::database::{RunnerDatabase, VerificationDatabase};
use sqlx::{Connection, PgConnection, postgres::PgConnectOptions};

const VERIFICATION_ROLE: &str = "bigname_phase_verification_reader_test";
const VERIFICATION_PASSWORD: &str = "bigname-phase-verification-reader-test";
const VERIFICATION_ROLE_LOCK: i64 = 7_312_026_073_000_004;

pub struct ScratchDatabase {
    database: TestDatabase,
    runner: RunnerDatabase,
}

impl ScratchDatabase {
    pub async fn create(prefix: &str) -> Result<Self> {
        let database = TestDatabase::create(
            TestDatabaseConfig::new(prefix)
                .pool_max_connections(10)
                .parse_context("failed to parse database URL for phase-runner tests")
                .admin_connect_context("failed to connect phase-runner test admin pool")
                .pool_connect_context("failed to connect phase-runner test pool"),
        )
        .await?;
        apply_schema(database.pool()).await?;
        let options = PgConnectOptions::from_str(&database_url_from_env())?
            .database(database.database_name());
        let runner = RunnerDatabase::connect_with_options(options, 10).await?;
        Ok(Self { database, runner })
    }

    pub fn runner(&self) -> RunnerDatabase {
        self.runner.clone()
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        self.runner.pool()
    }

    pub fn legacy_pool(&self) -> &sqlx::PgPool {
        self.database.pool()
    }

    pub fn writer_connect_options(&self) -> PgConnectOptions {
        self.runner.pool().connect_options().as_ref().clone()
    }

    pub async fn verification_database(
        &self,
        maximum_connections: u32,
    ) -> Result<VerificationDatabase> {
        Ok(VerificationDatabase::connect_with_options(
            self.verification_connect_options().await?,
            &self.runner,
            maximum_connections,
        )
        .await?)
    }

    pub async fn verification_connect_options(&self) -> Result<PgConnectOptions> {
        let mut transaction = self.pool().begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(VERIFICATION_ROLE_LOCK)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "DO $test_role$
             BEGIN
                 CREATE ROLE bigname_phase_verification_reader_test
                     LOGIN PASSWORD 'bigname-phase-verification-reader-test'
                     NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
                     NOREPLICATION NOBYPASSRLS;
             EXCEPTION
                 -- Concurrent CREATE ROLE can surface 23505 at the catalog index; either error means the role exists.
                 WHEN duplicate_object OR unique_violation THEN NULL;
             END
             $test_role$",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
            .execute(&mut *transaction)
            .await?;
        let database_identifier = self.database.database_name().replace('"', "\"\"");
        let database_privileges = format!(
            "REVOKE CREATE ON DATABASE \"{database_identifier}\" FROM PUBLIC;
             GRANT CONNECT ON DATABASE \"{database_identifier}\"
                 TO bigname_phase_verification_reader_test"
        );
        sqlx::raw_sql(&database_privileges)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system()
                 TO bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "REVOKE ALL PRIVILEGES ON SCHEMA bigname_phase
                 FROM bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "GRANT USAGE ON SCHEMA bigname_phase
                 TO bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA bigname_phase
                 FROM bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "GRANT SELECT ON ALL TABLES IN SCHEMA bigname_phase
                 TO bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bigname_phase
                 FROM bigname_phase_verification_reader_test",
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(PgConnectOptions::from_str(&database_url_from_env())?
            .database(self.database.database_name())
            .username(VERIFICATION_ROLE)
            .password(VERIFICATION_PASSWORD))
    }

    pub async fn writer_assuming_verification_role_options(&self) -> Result<PgConnectOptions> {
        self.verification_connect_options().await?;
        Ok(self
            .writer_connect_options()
            .options([("role", VERIFICATION_ROLE)]))
    }

    pub async fn cleanup(self) -> Result<()> {
        self.runner.pool().close().await;
        self.database.cleanup().await
    }
}

async fn apply_schema(pool: &sqlx::PgPool) -> Result<()> {
    phase_runner::schema::initialize_schema_v2(pool).await?;
    Ok(())
}

pub async fn assert_connection_hash_stamp(database: &RunnerDatabase) -> Result<()> {
    let stamp: String = sqlx::query_scalar("SELECT current_setting($1, true)")
        .bind(phase_runner::database::INTERPRETER_CONTENT_HASH_SETTING)
        .fetch_one(database.pool())
        .await?;
    assert_eq!(stamp, phase_runner::INTERPRETER_CONTENT_HASH);

    let mut dedicated = PgConnection::connect_with(&database_options(database)).await?;
    let dedicated_stamp: String = sqlx::query_scalar("SELECT current_setting($1, true)")
        .bind(phase_runner::database::INTERPRETER_CONTENT_HASH_SETTING)
        .fetch_one(&mut dedicated)
        .await?;
    assert_eq!(dedicated_stamp, phase_runner::INTERPRETER_CONTENT_HASH);
    dedicated.close().await?;
    Ok(())
}

fn database_options(database: &RunnerDatabase) -> PgConnectOptions {
    database.pool().connect_options().as_ref().clone()
}

pub async fn seed_lineage(pool: &sqlx::PgPool, chain_id: &str, through: i64) -> Result<()> {
    for number in 0..=through {
        let hash = format!("{chain_id}-block-{number}");
        let parent = (number > 0).then(|| format!("{chain_id}-block-{}", number - 1));
        sqlx::query(
            "
            INSERT INTO chain_lineage (
                chain_id,
                block_hash,
                parent_hash,
                block_number,
                block_timestamp,
                canonicality_state
            )
            VALUES ($1, $2, $3, $4, to_timestamp($4), 'observed')
            ",
        )
        .bind(chain_id)
        .bind(hash)
        .bind(parent)
        .bind(number)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Helpers every JSON-RPC chain double in these tests shares.
///
/// Ingest fetches a selected transaction directly rather than downloading its whole block,
/// and checks each stored log against the receipt that carries it and the header bloom that
/// commits to it. A double that answers only the block-shaped methods, reports an empty
/// `logsBloom`, or serves receipts without their logs is no longer answering as a node
/// would, so these build those three answers out of fixtures a double already has.
pub mod chain_double {
    use alloy_primitives::{Bloom, BloomInput, hex};
    use serde_json::{Value, json};

    /// The header `logsBloom` a block carrying `logs` would report.
    pub fn logs_bloom(logs: &[Value]) -> String {
        let mut bloom = Bloom::ZERO;
        for log in logs {
            if let Some(address) = log.get("address").and_then(Value::as_str)
                && let Ok(bytes) = hex::decode(address)
            {
                bloom.accrue(BloomInput::Raw(&bytes));
            }
            for topic in log
                .get("topics")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default()
            {
                if let Some(topic) = topic.as_str()
                    && let Ok(bytes) = hex::decode(topic)
                {
                    bloom.accrue(BloomInput::Raw(&bytes));
                }
            }
        }
        hex::encode_prefixed(bloom.as_slice())
    }

    /// Copies each receipt with the block logs that belong to its transaction.
    pub fn receipts_with_logs(receipts: &[Value], logs: &[Value]) -> Vec<Value> {
        receipts
            .iter()
            .map(|receipt| {
                let hash = receipt
                    .get("transactionHash")
                    .cloned()
                    .unwrap_or(Value::Null);
                let owned = logs
                    .iter()
                    .filter(|log| log.get("transactionHash") == Some(&hash))
                    .cloned()
                    .collect::<Vec<_>>();
                let mut receipt = receipt.clone();
                receipt["logs"] = json!(owned);
                receipt
            })
            .collect()
    }

    /// Wraps a block-shaped chain double so it answers as a node would.
    ///
    /// `respond` is the double's own responder. This fills the three gaps generically:
    /// it derives `eth_getTransactionReceipt` and `eth_getTransactionByHash` from the
    /// blocks and block receipts the double already serves, attaches a block's logs to the
    /// receipts it returns, and replaces a placeholder header bloom with the real one. The
    /// inner calls it makes are never per-transaction methods, so it does not recurse.
    ///
    /// Suitable for doubles that serve a handful of blocks: finding a transaction walks the
    /// chain from the head.
    pub fn node_shaped<F>(request: &Value, respond: &F) -> Value
    where
        F: Fn(&Value) -> Value,
    {
        let method = request["method"].as_str().unwrap_or_default();
        let id = request.get("id").cloned().unwrap_or(json!(1));
        if let Some(result) = derived_transaction_answer(method, request, respond) {
            return json!({"jsonrpc": "2.0", "id": id, "result": result});
        }
        let mut response = respond(request);
        match method {
            "eth_getBlockByNumber" | "eth_getBlockByHash" => {
                if let Some(hash) = response
                    .pointer("/result/hash")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                {
                    response["result"]["logsBloom"] =
                        json!(logs_bloom(&block_logs(&hash, respond)));
                }
            }
            "eth_getBlockReceipts" => {
                if let Some(receipts) = response
                    .pointer("/result")
                    .and_then(Value::as_array)
                    .cloned()
                {
                    let hash = request
                        .pointer("/params/0")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    response["result"] =
                        json!(receipts_with_logs(&receipts, &block_logs(&hash, respond)));
                }
            }
            _ => {}
        }
        response
    }

    fn call(method: &str, params: Value) -> Value {
        json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    }

    fn quantity(value: Option<&Value>) -> Option<i64> {
        i64::from_str_radix(value?.as_str()?.trim_start_matches("0x"), 16).ok()
    }

    fn block_logs<F>(block_hash: &str, respond: &F) -> Vec<Value>
    where
        F: Fn(&Value) -> Value,
    {
        respond(&call("eth_getLogs", json!([{"blockHash": block_hash}])))
            .pointer("/result")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    }

    /// The per-transaction answer, found by walking the double's chain from its head.
    ///
    /// The walk follows `parentHash` through `eth_getBlockByHash` rather than asking for
    /// blocks by number: a double that scripts its answers per by-number request would
    /// otherwise have its script consumed by this lookup.
    fn derived_transaction_answer<F>(method: &str, request: &Value, respond: &F) -> Option<Value>
    where
        F: Fn(&Value) -> Value,
    {
        if !matches!(
            method,
            "eth_getTransactionReceipt" | "eth_getTransactionByHash"
        ) {
            return None;
        }
        let hash = request.pointer("/params/0")?.as_str()?.to_owned();
        let mut block = respond(&call("eth_getBlockByNumber", json!(["latest", true])))
            .pointer("/result")
            .filter(|value| value.is_object())
            .cloned();
        for _ in 0..MAX_DOUBLE_CHAIN_WALK {
            let Some(current) = block.take() else { break };
            if let Some(answer) = transaction_answer(method, &hash, &current, respond) {
                return Some(answer);
            }
            let Some(parent) = current
                .get("parentHash")
                .and_then(Value::as_str)
                .filter(|parent| parent.trim_start_matches("0x").trim_matches('0').len() > 0)
                .map(str::to_owned)
            else {
                break;
            };
            block = respond(&call("eth_getBlockByHash", json!([parent, true])))
                .pointer("/result")
                .filter(|value| value.is_object())
                .cloned();
        }
        Some(Value::Null)
    }

    /// How far back a per-transaction lookup walks before giving up.
    const MAX_DOUBLE_CHAIN_WALK: usize = 64;

    fn transaction_answer<F>(method: &str, hash: &str, block: &Value, respond: &F) -> Option<Value>
    where
        F: Fn(&Value) -> Value,
    {
        if method == "eth_getTransactionByHash" {
            return block
                .get("transactions")
                .and_then(Value::as_array)?
                .iter()
                .find(|transaction| transaction.get("hash").and_then(Value::as_str) == Some(hash))
                .cloned();
        }
        let block_hash = block.get("hash").and_then(Value::as_str)?;
        let receipts = respond(&call("eth_getBlockReceipts", json!([block_hash])));
        let receipt = receipts
            .pointer("/result")
            .and_then(Value::as_array)?
            .iter()
            .find(|receipt| receipt.get("transactionHash").and_then(Value::as_str) == Some(hash))?
            .clone();
        Some(json!(
            receipts_with_logs(&[receipt], &block_logs(block_hash, respond))[0]
        ))
    }

    /// Answers `eth_getTransactionReceipt` and `eth_getTransactionByHash` from the receipts
    /// and full block bodies a double already serves.
    ///
    /// `None` means the method is not one of those two; `Some(result)` is the answer,
    /// including `Some(Value::Null)` for a transaction the double does not have.
    pub fn per_transaction_result(
        method: &str,
        params: &[Value],
        receipts: &[Value],
        blocks: &[Value],
    ) -> Option<Value> {
        let hash = params.first().and_then(Value::as_str)?;
        match method {
            "eth_getTransactionReceipt" => Some(
                receipts
                    .iter()
                    .find(|receipt| {
                        receipt.get("transactionHash").and_then(Value::as_str) == Some(hash)
                    })
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            "eth_getTransactionByHash" => Some(
                blocks
                    .iter()
                    .filter_map(|block| block.get("transactions").and_then(Value::as_array))
                    .flatten()
                    .find(|transaction| {
                        transaction.get("hash").and_then(Value::as_str) == Some(hash)
                    })
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            _ => None,
        }
    }
}
