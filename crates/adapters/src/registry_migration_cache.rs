use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

#[cfg(not(test))]
use std::sync::OnceLock;

use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct RegistryMigrationMarkerEmitter {
    address: String,
    effective_from_block: i64,
    effective_to_block: i64,
}

impl RegistryMigrationMarkerEmitter {
    pub(crate) fn new(
        address: impl Into<String>,
        effective_from_block: i64,
        effective_to_block: i64,
    ) -> Self {
        Self {
            address: address.into().to_ascii_lowercase(),
            effective_from_block,
            effective_to_block,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MigratedRegistryNodes {
    baseline: Arc<HashSet<String>>,
    delta: HashSet<String>,
}

impl MigratedRegistryNodes {
    pub(crate) fn empty() -> Self {
        Self {
            baseline: Arc::new(HashSet::new()),
            delta: HashSet::new(),
        }
    }

    pub(crate) fn from_delta(delta: HashSet<String>) -> Self {
        Self {
            baseline: Arc::new(HashSet::new()),
            delta,
        }
    }

    fn from_baseline(baseline: HashSet<String>) -> Self {
        Self {
            baseline: Arc::new(baseline),
            delta: HashSet::new(),
        }
    }

    pub(crate) fn contains(&self, node: &str) -> bool {
        self.delta.contains(node) || self.baseline.contains(node)
    }

    pub(crate) fn insert(&mut self, node: String) -> bool {
        self.delta.insert(node)
    }

    pub(crate) fn delta_nodes(&self) -> impl Iterator<Item = &String> {
        self.delta.iter()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RegistryMigrationMarkerCacheKey {
    chain: String,
    marker_topic0: String,
    emitters: Vec<RegistryMigrationMarkerEmitter>,
}

#[derive(Debug)]
struct RegistryMigrationMarkerCacheEntry {
    finalized_loaded_before_block: i64,
    finalized_nodes_by_block: BTreeMap<i64, HashSet<String>>,
    volatile_nodes: HashSet<String>,
}

#[cfg(not(test))]
static REGISTRY_MIGRATION_MARKER_CACHE: OnceLock<
    tokio::sync::Mutex<HashMap<RegistryMigrationMarkerCacheKey, RegistryMigrationMarkerCacheEntry>>,
> = OnceLock::new();

#[cfg(not(test))]
fn registry_migration_marker_cache() -> &'static tokio::sync::Mutex<
    HashMap<RegistryMigrationMarkerCacheKey, RegistryMigrationMarkerCacheEntry>,
> {
    REGISTRY_MIGRATION_MARKER_CACHE.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
}

pub(crate) async fn load_migrated_registry_nodes_before_block<F>(
    pool: &PgPool,
    chain: &str,
    emitters: &[RegistryMigrationMarkerEmitter],
    before_block: i64,
    marker_topic0: &str,
    decode_node: F,
) -> Result<MigratedRegistryNodes>
where
    F: Fn(&[String]) -> Result<String>,
{
    let mut emitters = emitters.to_vec();
    emitters.sort();
    emitters.dedup();
    if emitters.is_empty() {
        return Ok(MigratedRegistryNodes::empty());
    }

    #[cfg(test)]
    {
        let nodes = load_marker_nodes_between(
            pool,
            chain,
            &emitters,
            0,
            before_block,
            marker_topic0,
            &decode_node,
        )
        .await?;
        Ok(MigratedRegistryNodes::from_baseline(nodes))
    }

    #[cfg(not(test))]
    {
        let key = RegistryMigrationMarkerCacheKey {
            chain: chain.to_owned(),
            marker_topic0: marker_topic0.to_ascii_lowercase(),
            emitters,
        };
        let mut cache = registry_migration_marker_cache().lock().await;
        return load_migrated_registry_nodes_before_block_with_cache(
            pool,
            chain,
            key,
            before_block,
            marker_topic0,
            &decode_node,
            &mut cache,
        )
        .await;
    }
}

async fn load_migrated_registry_nodes_before_block_with_cache<F>(
    pool: &PgPool,
    chain: &str,
    key: RegistryMigrationMarkerCacheKey,
    before_block: i64,
    marker_topic0: &str,
    decode_node: &F,
    cache: &mut HashMap<RegistryMigrationMarkerCacheKey, RegistryMigrationMarkerCacheEntry>,
) -> Result<MigratedRegistryNodes>
where
    F: Fn(&[String]) -> Result<String>,
{
    let key_emitters = key.emitters.clone();
    let entry = cache
        .entry(key)
        .or_insert_with(|| RegistryMigrationMarkerCacheEntry {
            finalized_loaded_before_block: 0,
            finalized_nodes_by_block: BTreeMap::new(),
            volatile_nodes: HashSet::new(),
        });
    let finalized_watermark =
        load_finalized_watermark_before_block(pool, chain, before_block).await?;
    let stable_before_block = finalized_watermark.min(before_block);

    if stable_before_block > entry.finalized_loaded_before_block {
        let finalized_nodes = load_marker_node_rows_between(
            pool,
            chain,
            &key_emitters,
            entry.finalized_loaded_before_block,
            stable_before_block,
            marker_topic0,
            decode_node,
        )
        .await?;
        for (block_number, node) in finalized_nodes {
            entry
                .finalized_nodes_by_block
                .entry(block_number)
                .or_default()
                .insert(node);
        }
        entry.finalized_loaded_before_block = stable_before_block;
    }

    entry.volatile_nodes = load_marker_nodes_between(
        pool,
        chain,
        &key_emitters,
        stable_before_block,
        before_block,
        marker_topic0,
        decode_node,
    )
    .await?;

    let mut nodes = HashSet::new();
    for (_block_number, block_nodes) in entry.finalized_nodes_by_block.range(..before_block) {
        nodes.extend(block_nodes.iter().cloned());
    }
    nodes.extend(entry.volatile_nodes.iter().cloned());

    Ok(MigratedRegistryNodes::from_baseline(nodes))
}

async fn load_marker_nodes_between<F>(
    pool: &PgPool,
    chain: &str,
    emitters: &[RegistryMigrationMarkerEmitter],
    from_block: i64,
    before_block: i64,
    marker_topic0: &str,
    decode_node: &F,
) -> Result<HashSet<String>>
where
    F: Fn(&[String]) -> Result<String>,
{
    Ok(load_marker_node_rows_between(
        pool,
        chain,
        emitters,
        from_block,
        before_block,
        marker_topic0,
        decode_node,
    )
    .await?
    .into_iter()
    .map(|(_block_number, node)| node)
    .collect())
}

async fn load_marker_node_rows_between<F>(
    pool: &PgPool,
    chain: &str,
    emitters: &[RegistryMigrationMarkerEmitter],
    from_block: i64,
    before_block: i64,
    marker_topic0: &str,
    decode_node: &F,
) -> Result<Vec<(i64, String)>>
where
    F: Fn(&[String]) -> Result<String>,
{
    if from_block >= before_block {
        return Ok(Vec::new());
    }

    let addresses = emitters
        .iter()
        .map(|emitter| emitter.address.clone())
        .collect::<Vec<_>>();
    let from_blocks = emitters
        .iter()
        .map(|emitter| emitter.effective_from_block)
        .collect::<Vec<_>>();
    let to_blocks = emitters
        .iter()
        .map(|emitter| emitter.effective_to_block)
        .collect::<Vec<_>>();

    let rows = sqlx::query(
        r#"
        SELECT block_number, topics
        FROM raw_logs
        WHERE chain_id = $1
          AND emitting_address = ANY($2::TEXT[])
          AND block_number >= $7
          AND block_number < $3
          AND topics[1] = $4
          AND EXISTS (
              SELECT 1
              FROM unnest($2::TEXT[], $5::BIGINT[], $6::BIGINT[]) AS watched(
                  address,
                  effective_from_block,
                  effective_to_block
              )
              WHERE watched.address = emitting_address
                AND block_number BETWEEN watched.effective_from_block
                    AND watched.effective_to_block
          )
          AND canonicality_state IN (
              'canonical'::canonicality_state,
              'safe'::canonicality_state,
              'finalized'::canonicality_state
          )
        "#,
    )
    .bind(chain)
    .bind(&addresses)
    .bind(before_block)
    .bind(marker_topic0)
    .bind(&from_blocks)
    .bind(&to_blocks)
    .bind(from_block)
    .fetch_all(pool)
    .await
    .with_context(|| {
        format!(
            "failed to load ENSv1 registry migration markers for blocks {from_block}..{before_block}"
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let block_number = row
                .try_get::<i64, _>("block_number")
                .context("missing block_number")?;
            let topics = row
                .try_get::<Vec<String>, _>("topics")
                .context("missing topics")?;
            Ok((block_number, decode_node(&topics)?))
        })
        .collect()
}

async fn load_finalized_watermark_before_block(
    pool: &PgPool,
    chain: &str,
    before_block: i64,
) -> Result<i64> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COALESCE(MAX(block_number) + 1, 0)::BIGINT
        FROM chain_lineage
        WHERE chain_id = $1
          AND block_number < $2
          AND canonicality_state = 'finalized'::canonicality_state
        "#,
    )
    .bind(chain)
    .bind(before_block)
    .fetch_one(pool)
    .await
    .with_context(|| {
        format!("failed to load finalized lineage watermark for chain {chain} before block {before_block}")
    })
}

#[cfg(test)]
mod tests;
