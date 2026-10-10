//! The ENSv1 registry resolver pointer of nodes the response serves without a composed name row,
//! such as a registry child no label-bearing event named. The value is the composed row's
//! `declared_summary.ens_v1_resolver` (`serving::ens_v1_resolver`), read from the same
//! `project_registry_pointer` rows.
use std::collections::BTreeMap;

use anyhow::Result;
use serde_json::Value;

use crate::ReadDb;

/// The pointer of each `(namespace, node)` on `chain_id`, keyed by namespace and lower-cased
/// node, JSON null for a node with no pointer. One statement for every node.
pub async fn load_ens_v1_resolvers(
    db: impl Into<ReadDb<'_>>,
    chain_id: &str,
    nodes: &[(String, String)],
) -> Result<BTreeMap<(String, String), Value>> {
    let nodes: Vec<(String, String)> = nodes
        .iter()
        .map(|(namespace, node)| (namespace.clone(), node.to_ascii_lowercase()))
        .collect();
    if nodes.is_empty() {
        return Ok(BTreeMap::new());
    }
    super::seams::note_registry_pointer_read(nodes.len());
    let mut conn = db.into().acquire().await?;
    let pointers = super::loaders::node_pointers(&mut conn, chain_id, &nodes).await?;
    Ok(nodes
        .into_iter()
        .map(|node| {
            let value = super::serving::ens_v1_resolver(pointers.get(&node), chain_id);
            (node, value)
        })
        .collect())
}
