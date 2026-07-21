use alloy_primitives::{Address, Bytes, U256, hex};
use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};

/// Minimal JSON-RPC client for driving a local anvil node. Transactions are
/// sent from anvil's unlocked dev accounts via `eth_sendTransaction`, so no
/// local signing is required.
pub struct RpcClient {
    http: reqwest::Client,
    url: String,
}

pub struct TxReceipt {
    pub contract_address: Option<Address>,
    pub block_number: u64,
    pub status_ok: bool,
    pub tx_hash: String,
}

impl RpcClient {
    pub fn new(url: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            url,
        }
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let response: Value = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("rpc transport failure for {method}"))?
            .json()
            .await
            .with_context(|| format!("rpc non-json response for {method}"))?;
        if let Some(error) = response.get("error").filter(|e| !e.is_null()) {
            bail!("rpc {method} error: {error}");
        }
        response
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow!("rpc {method} returned no result"))
    }

    pub async fn chain_id(&self) -> Result<u64> {
        parse_quantity(&self.call("eth_chainId", json!([])).await?)
    }

    pub async fn block_number(&self) -> Result<u64> {
        parse_quantity(&self.call("eth_blockNumber", json!([])).await?)
    }

    pub async fn block_timestamp(&self) -> Result<u128> {
        let raw = self
            .call("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let timestamp = raw
            .get("timestamp")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("latest block returned no timestamp"))?;
        Ok(u128::from_str_radix(
            timestamp.trim_start_matches("0x"),
            16,
        )?)
    }

    pub async fn block_hash(&self, block_number: u64) -> Result<String> {
        let raw = self
            .call(
                "eth_getBlockByNumber",
                json!([format!("{block_number:#x}"), false]),
            )
            .await?;
        raw.get("hash")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| anyhow!("eth_getBlockByNumber({block_number}) returned no hash"))
    }

    pub async fn accounts(&self) -> Result<Vec<Address>> {
        let raw = self.call("eth_accounts", json!([])).await?;
        serde_json::from_value(raw).context("eth_accounts decode")
    }

    /// Send a transaction from an unlocked account and wait for its receipt.
    /// Anvil auto-mines, so the receipt is available immediately after send.
    pub async fn send_transaction(
        &self,
        from: Address,
        to: Option<Address>,
        data: &[u8],
        value: U256,
    ) -> Result<TxReceipt> {
        let mut tx = json!({
            "from": from,
            "data": Bytes::copy_from_slice(data),
            "value": format!("{value:#x}"),
            "gas": "0x1c9c380",
        });
        if let Some(to) = to {
            tx["to"] = json!(to);
        }
        let tx_hash: String =
            serde_json::from_value(self.call("eth_sendTransaction", json!([tx])).await?)
                .context("eth_sendTransaction result decode")?;
        let mut receipt = Value::Null;
        for _ in 0..40 {
            receipt = self
                .call("eth_getTransactionReceipt", json!([&tx_hash]))
                .await?;
            if !receipt.is_null() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        if receipt.is_null() {
            bail!("no receipt for {tx_hash} after 2s; is anvil auto-mining?");
        }
        let contract_address = receipt
            .get("contractAddress")
            .filter(|v| !v.is_null())
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .context("receipt contractAddress decode")?;
        let status_ok = receipt.get("status").and_then(Value::as_str) == Some("0x1");
        let block_number = parse_quantity(
            receipt
                .get("blockNumber")
                .ok_or_else(|| anyhow!("receipt missing blockNumber"))?,
        )?;
        Ok(TxReceipt {
            contract_address,
            block_number,
            status_ok,
            tx_hash,
        })
    }

    /// Send a contract call and require a successful receipt. The caller
    /// supplies the operation description so failures identify both the
    /// scenario action and the target contract while retaining the receipt's
    /// transaction hash.
    pub async fn send_checked(
        &self,
        from: Address,
        to: Address,
        data: &[u8],
        value: U256,
        description: &str,
    ) -> Result<TxReceipt> {
        let receipt = self.send_transaction(from, Some(to), data, value).await?;
        if !receipt.status_ok {
            bail!("{description} reverted at {to:#x} (tx {})", receipt.tx_hash);
        }
        Ok(receipt)
    }

    pub async fn eth_call(&self, to: Address, data: &[u8]) -> Result<Vec<u8>> {
        let raw = self
            .call(
                "eth_call",
                json!([{"to": to, "data": Bytes::copy_from_slice(data)}, "latest"]),
            )
            .await?;
        let hex_str = raw
            .as_str()
            .ok_or_else(|| anyhow!("eth_call non-string result"))?;
        alloy_primitives::hex::decode(hex_str).context("eth_call hex decode")
    }

    pub async fn get_code(&self, address: Address) -> Result<Vec<u8>> {
        let raw = self.call("eth_getCode", json!([address, "latest"])).await?;
        let hex_str = raw
            .as_str()
            .ok_or_else(|| anyhow!("eth_getCode non-string result"))?;
        hex::decode(hex_str).context("eth_getCode hex decode")
    }

    pub async fn get_code_at_block_hash(
        &self,
        address: Address,
        block_hash: &str,
    ) -> Result<Vec<u8>> {
        let raw = self
            .call("eth_getCode", json!([address, {"blockHash": block_hash}]))
            .await?;
        let hex_str = raw
            .as_str()
            .ok_or_else(|| anyhow!("eth_getCode non-string result"))?;
        hex::decode(hex_str).context("historical eth_getCode hex decode")
    }

    pub async fn set_code(&self, address: Address, code: &[u8]) -> Result<()> {
        self.call(
            "anvil_setCode",
            json!([address, Bytes::copy_from_slice(code)]),
        )
        .await?;
        Ok(())
    }

    /// Warp chain time forward and mine one block so the new timestamp is observable.
    pub async fn increase_time(&self, seconds: u64) -> Result<()> {
        self.call("evm_increaseTime", json!([seconds])).await?;
        self.call("evm_mine", json!([])).await?;
        Ok(())
    }

    pub async fn mine(&self, blocks: u64) -> Result<()> {
        self.call("anvil_mine", json!([format!("{blocks:#x}")]))
            .await?;
        Ok(())
    }

    pub async fn evm_snapshot(&self) -> Result<String> {
        let raw = self.call("evm_snapshot", json!([])).await?;
        raw.as_str()
            .map(str::to_owned)
            .ok_or_else(|| anyhow!("evm_snapshot returned non-string result: {raw}"))
    }

    pub async fn evm_revert(&self, snapshot_id: &str) -> Result<()> {
        let raw = self.call("evm_revert", json!([snapshot_id])).await?;
        match raw.as_bool() {
            Some(true) => Ok(()),
            Some(false) => bail!("evm_revert({snapshot_id}) returned false"),
            None => bail!("evm_revert({snapshot_id}) returned non-bool result: {raw}"),
        }
    }
}

fn parse_quantity(value: &Value) -> Result<u64> {
    let s = value
        .as_str()
        .ok_or_else(|| anyhow!("expected hex quantity, got {value}"))?;
    u64::from_str_radix(s.trim_start_matches("0x"), 16).context("hex quantity parse")
}
