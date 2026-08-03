use bigname_execution::ChainRpcUrls;
use serde_json::{Map, Value, json};
use sqlx::PgPool;

use crate::{Marker, ProjectError, Result};

mod head;
mod reverse;
mod text;

const ETHEREUM: &str = "ethereum-mainnet";
pub(super) const HYDRATION_KEY: &str = "canonical_head_multicall_hydration";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HydrationOutcome {
    pub head: Marker,
    pub deferred_for_redo: bool,
    pub reverse_candidates: usize,
    pub text_candidates: usize,
    pub updated_rows: usize,
}

pub struct Hydrator {
    pool: PgPool,
    rpc_urls: ChainRpcUrls,
}

impl Hydrator {
    pub fn new(pool: PgPool, rpc_urls: ChainRpcUrls) -> Self {
        Self { pool, rpc_urls }
    }

    pub async fn hydrate_canonical_head(&self, chain_id: &str) -> Result<HydrationOutcome> {
        let head = head::load(&self.pool, chain_id).await?;
        self.hydrate_loaded_head(chain_id, head).await
    }

    pub async fn hydrate_if_canonical_head(
        &self,
        chain_id: &str,
        expected: &Marker,
    ) -> Result<Option<HydrationOutcome>> {
        let head = head::load(&self.pool, chain_id).await?;
        if &head != expected {
            return Ok(None);
        }
        self.hydrate_loaded_head(chain_id, head).await.map(Some)
    }

    async fn hydrate_loaded_head(&self, chain_id: &str, head: Marker) -> Result<HydrationOutcome> {
        if chain_id != ETHEREUM {
            return Ok(empty_outcome(head, false));
        }
        if head::interpret_redo_pending(&self.pool, chain_id).await? {
            return Ok(empty_outcome(head, true));
        }

        let reverse = reverse::load_candidates(&self.pool, &head).await?;
        let mut text_rows = text::load_candidates(&self.pool).await?;
        let text_candidates = text_rows.iter().map(|row| row.calls.len()).sum::<usize>();
        if reverse.is_empty() && text_candidates == 0 {
            return Ok(empty_outcome(head, false));
        }
        let missing_rpc = (reverse.needs_rpc() || text_candidates > 0)
            && self.rpc_urls.url_for(chain_id).is_none();

        let reverse_results = reverse.execute(&self.rpc_urls, &head).await;
        let text_failures = text::hydrate(&self.rpc_urls, &head, &mut text_rows).await?;
        let mut transaction = self.pool.begin().await.map_err(|error| {
            ProjectError::database(
                "failed to begin canonical-head hydration publication",
                error,
            )
        })?;
        head::require_same(&mut transaction, chain_id, &head).await?;
        let mut updated_rows = 0usize;
        let (reverse_updates, reverse_failures) = reverse
            .publish(&mut transaction, &head, reverse_results)
            .await?;
        updated_rows += reverse_updates;
        for row in text_rows.iter().filter(|row| row.changed) {
            let result = sqlx::query(
                "UPDATE record_inventory_current
                 SET entries = $3, last_recomputed_at = now()
                 WHERE resource_id::text = $1
                   AND record_version_boundary_key = $2",
            )
            .bind(&row.resource_id)
            .bind(&row.boundary_key)
            .bind(&row.entries)
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                ProjectError::database("failed to publish hydrated text records", error)
            })?;
            if result.rows_affected() != 1 {
                return Err(ProjectError::data_integrity(
                    "hydrated record-inventory row disappeared before publication",
                ));
            }
            updated_rows += 1;
        }
        transaction.commit().await.map_err(|error| {
            ProjectError::database("failed to commit canonical-head hydration", error)
        })?;
        if missing_rpc {
            return Err(ProjectError::configuration(format!(
                "canonical-head hydration candidates exist for {chain_id}, but no hydration RPC URL is configured"
            )));
        }
        let failed_calls = reverse_failures.saturating_add(text_failures);
        if failed_calls > 0 {
            return Err(ProjectError::transient(format!(
                "canonical-head hydration retracted stale values after {failed_calls} multicall failure(s); retry at the same head"
            )));
        }
        Ok(HydrationOutcome {
            head,
            deferred_for_redo: false,
            reverse_candidates: reverse.active_len(),
            text_candidates,
            updated_rows,
        })
    }
}

fn empty_outcome(head: Marker, deferred_for_redo: bool) -> HydrationOutcome {
    HydrationOutcome {
        head,
        deferred_for_redo,
        reverse_candidates: 0,
        text_candidates: 0,
        updated_rows: 0,
    }
}

pub(super) fn hydration_provenance(
    head: &Marker,
    resolver: Option<&str>,
    reverse_node: Option<&str>,
) -> Value {
    let mut value = Map::from_iter([
        ("source".to_owned(), json!("multicall_at_canonical_head")),
        ("chain_id".to_owned(), json!(ETHEREUM)),
        ("block_number".to_owned(), json!(head.number)),
        ("block_hash".to_owned(), json!(head.hash)),
    ]);
    if let Some(resolver) = resolver {
        value.insert("resolver_address".to_owned(), json!(resolver));
    }
    if let Some(reverse_node) = reverse_node {
        value.insert("reverse_node".to_owned(), json!(reverse_node));
    }
    Value::Object(value)
}
