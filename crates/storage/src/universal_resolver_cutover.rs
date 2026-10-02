//! Per-chain Universal Resolver cutover state for reporting. Serving decides the cutover with
//! `families::control::cutover::cut_over`; this reads the same Project rows, takes the decision
//! from that function, and also returns the client-facing proxy chain as walked, so a reader can
//! tell an implementation the manifest does not admit from a proxy with no `Upgraded` yet. It sits
//! outside `families`, so it is not part of the interpreter content hash.
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use sqlx::FromRow;

use crate::families::control::cutover::{ProxyRow, cut_over};

const CLIENT_FACING_ROLE: &str = "universal_resolver";

/// One hop of the client-facing chain: a declared proxy's latest `Upgraded`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyHop {
    pub proxy_address: String,
    pub implementation: String,
    /// `admitted_universal_resolver`, `universal_resolver_proxy` or `other`.
    pub implementation_kind: String,
    pub block_number: i64,
}

/// A chain's cutover state at the current family publication. A chain with no proxy row has the
/// default: not cut over, empty chain.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UniversalResolverCutover {
    pub cut_over: bool,
    /// From the client-facing proxy, each hop until the chain ends: at an implementation that is
    /// not a declared proxy, at a proxy with no row, or where it would revisit a hop.
    pub chain: Vec<ProxyHop>,
}

impl UniversalResolverCutover {
    /// The last hop when the chain ends at an implementation the manifest neither admits nor
    /// declares as a proxy.
    pub fn unadmitted(&self) -> Option<&ProxyHop> {
        self.chain
            .last()
            .filter(|hop| hop.implementation_kind == "other")
    }

    fn from_rows(rows: &[CutoverRow]) -> Self {
        let proxies: Vec<ProxyRow> = rows.iter().map(|row| row.proxy.clone()).collect();
        let mut chain: Vec<ProxyHop> = Vec::new();
        let mut at = rows
            .iter()
            .find(|row| row.proxy.proxy_role.as_deref() == Some(CLIENT_FACING_ROLE));
        while let Some(row) = at {
            if chain
                .iter()
                .any(|hop| hop.proxy_address == row.proxy.proxy_address)
            {
                break;
            }
            chain.push(ProxyHop {
                proxy_address: row.proxy.proxy_address.clone(),
                implementation: row.proxy.implementation.clone(),
                implementation_kind: row.proxy.implementation_kind.clone(),
                block_number: row.block_number,
            });
            at = (row.proxy.implementation_kind == "universal_resolver_proxy")
                .then(|| {
                    rows.iter()
                        .find(|next| next.proxy.proxy_address == row.proxy.implementation)
                })
                .flatten();
        }
        Self {
            cut_over: cut_over(&proxies),
            chain,
        }
    }
}

#[derive(FromRow)]
struct CutoverRow {
    chain_id: String,
    #[sqlx(flatten)]
    proxy: ProxyRow,
    block_number: i64,
}

/// Every chain with a Universal Resolver proxy row, keyed by chain id.
pub async fn load_universal_resolver_cutovers<'e>(
    executor: impl sqlx::PgExecutor<'e>,
) -> Result<BTreeMap<String, UniversalResolverCutover>> {
    let rows: Vec<CutoverRow> = sqlx::query_as(
        "/* storage:universal_resolver_cutover */
         SELECT proxy.chain_id, proxy.proxy_address, proxy.proxy_role, proxy.implementation,
                proxy.implementation_kind, proxy.block_number
         FROM bigname_phase.project_universal_resolver_proxy proxy
         ORDER BY proxy.chain_id, proxy.proxy_address",
    )
    .fetch_all(executor)
    .await
    .context("failed to load the Universal Resolver proxies")?;
    let mut by_chain: BTreeMap<String, Vec<CutoverRow>> = BTreeMap::new();
    for row in rows {
        by_chain.entry(row.chain_id.clone()).or_default().push(row);
    }
    Ok(by_chain
        .into_iter()
        .map(|(chain, rows)| (chain, UniversalResolverCutover::from_rows(&rows)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(proxy: &str, role: &str, implementation: &str, kind: &str, block: i64) -> CutoverRow {
        CutoverRow {
            chain_id: "ethereum-sepolia".into(),
            proxy: ProxyRow {
                proxy_address: proxy.into(),
                proxy_role: Some(role.into()),
                implementation: implementation.into(),
                implementation_kind: kind.into(),
            },
            block_number: block,
        }
    }

    fn top() -> CutoverRow {
        row(
            "0xtop",
            "universal_resolver",
            "0xmanaged",
            "universal_resolver_proxy",
            10,
        )
    }

    fn managed(implementation: &str, kind: &str) -> CutoverRow {
        row(
            "0xmanaged",
            "universal_resolver_managed",
            implementation,
            kind,
            20,
        )
    }

    #[test]
    fn names_the_unadmitted_end_of_the_client_facing_chain() {
        let admitted = UniversalResolverCutover::from_rows(&[
            top(),
            managed("0xv2", "admitted_universal_resolver"),
        ]);
        assert!(admitted.cut_over);
        assert_eq!(admitted.chain.len(), 2);
        assert_eq!(admitted.unadmitted(), None);

        let unadmitted = UniversalResolverCutover::from_rows(&[top(), managed("0xnew", "other")]);
        assert!(!unadmitted.cut_over);
        assert_eq!(
            unadmitted.unadmitted(),
            Some(&ProxyHop {
                proxy_address: "0xmanaged".into(),
                implementation: "0xnew".into(),
                implementation_kind: "other".into(),
                block_number: 20,
            })
        );
    }

    #[test]
    fn a_chain_that_ends_without_an_implementation_is_not_unadmitted() {
        let none = UniversalResolverCutover::from_rows(&[]);
        assert_eq!(none, UniversalResolverCutover::default());

        let pending = UniversalResolverCutover::from_rows(&[top()]);
        assert!(!pending.cut_over);
        assert_eq!(pending.chain.len(), 1);
        assert_eq!(
            pending.unadmitted(),
            None,
            "the managed proxy has no row yet"
        );

        let managed_only = UniversalResolverCutover::from_rows(&[managed("0xnew", "other")]);
        assert!(
            managed_only.chain.is_empty(),
            "only the client-facing chain counts"
        );
        assert_eq!(managed_only.unadmitted(), None);

        let cycle = UniversalResolverCutover::from_rows(&[
            top(),
            managed("0xtop", "universal_resolver_proxy"),
        ]);
        assert!(!cycle.cut_over);
        assert_eq!(cycle.chain.len(), 2);
        assert_eq!(cycle.unadmitted(), None);
    }
}
