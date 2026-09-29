//! The Universal Resolver cutover: whether a chain resolves `.eth` names through ENSv2 at the
//! family publication. Clients resolve through the client-facing Universal Resolver proxy; it
//! forwards every call to its implementation, which can be another declared proxy (on Sepolia the
//! client-facing proxy points at a managed proxy whose admin swaps the implementation). A block is
//! cut over while that chain of implementations ends at a UniversalResolverV2 implementation the
//! `ens_execution` manifest admits (`universal_resolver_implementations`), whose resolution
//! starts at the admitted ENSv2 root registry. A rollback, an unadmitted implementation, or a
//! proxy with no `Upgraded` yet (its constructor sets the first implementation without an event)
//! is not cut over, so a chain with no admitted proxy events, such as Mainnet today, never is.
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L71-L75 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L83-L84 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UpgradableUniversalResolverProxy.sol:L111-L115 @ ens_v2_sepolia_20260916@366de741)
//! (upstream: .refs/ens_v2_sepolia_20260916/contracts/src/universalResolver/UniversalResolverV2.sol:L55-L62 @ ens_v2_sepolia_20260916@366de741)
use anyhow::{Context, Result};
use sqlx::PgConnection;

/// The manifest role of the client-facing proxy (`bigname_manifests::UNIVERSAL_RESOLVER_ROLE`).
const CLIENT_FACING_ROLE: &str = "universal_resolver";

/// One declared proxy's latest `Upgraded` (`project_universal_resolver_proxy`).
#[derive(Clone, Debug, Eq, PartialEq, sqlx::FromRow)]
pub struct ProxyRow {
    pub proxy_address: String,
    pub proxy_role: Option<String>,
    pub implementation: String,
    pub implementation_kind: String,
}

/// Whether the client-facing proxy's chain of implementations ends at an admitted
/// UniversalResolverV2 implementation. Each hop follows a declared proxy's own latest `Upgraded`;
/// a hop to a proxy with no row, and a cycle, end the chain without cutover.
pub fn cut_over(rows: &[ProxyRow]) -> bool {
    let Some(mut at) = rows
        .iter()
        .find(|row| row.proxy_role.as_deref() == Some(CLIENT_FACING_ROLE))
    else {
        return false;
    };
    for _ in 0..=rows.len() {
        match at.implementation_kind.as_str() {
            "admitted_universal_resolver" => return true,
            "universal_resolver_proxy" => {
                match rows
                    .iter()
                    .find(|row| row.proxy_address == at.implementation)
                {
                    Some(next) => at = next,
                    None => return false,
                }
            }
            _ => return false,
        }
    }
    false
}

/// Whether chain `chain_id` is cut over at the family publication `conn` reads.
pub async fn load_cut_over_on(conn: &mut PgConnection, chain_id: &str) -> Result<bool> {
    let rows: Vec<ProxyRow> = sqlx::query_as(
        "/* storage:families.control.cutover */
         SELECT proxy.proxy_address, proxy.proxy_role, proxy.implementation,
                proxy.implementation_kind
         FROM bigname_phase.project_universal_resolver_proxy proxy
         WHERE proxy.chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(conn)
    .await
    .context("failed to load the Universal Resolver proxies")?;
    Ok(cut_over(&rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(proxy: &str, role: &str, implementation: &str, kind: &str) -> ProxyRow {
        ProxyRow {
            proxy_address: proxy.into(),
            proxy_role: Some(role.into()),
            implementation: implementation.into(),
            implementation_kind: kind.into(),
        }
    }

    #[test]
    fn the_client_facing_chain_decides_the_cutover() {
        let top_to_managed = row(
            "0xtop",
            "universal_resolver",
            "0xmanaged",
            "universal_resolver_proxy",
        );
        let managed_admitted = row(
            "0xmanaged",
            "universal_resolver_managed",
            "0xv2",
            "admitted_universal_resolver",
        );
        let managed_other = row("0xmanaged", "universal_resolver_managed", "0xold", "other");
        assert!(!cut_over(&[]), "no proxy event is no cutover");
        assert!(
            !cut_over(std::slice::from_ref(&top_to_managed)),
            "a hop to a proxy with no event yet is no cutover"
        );
        assert!(cut_over(&[
            top_to_managed.clone(),
            managed_admitted.clone()
        ]));
        assert!(
            !cut_over(&[top_to_managed.clone(), managed_other]),
            "a rollback behind the hop is no cutover"
        );
        assert!(
            !cut_over(std::slice::from_ref(&managed_admitted)),
            "only the client-facing proxy's chain counts"
        );
        assert!(cut_over(&[row(
            "0xtop",
            "universal_resolver",
            "0xv2",
            "admitted_universal_resolver"
        )]));
        let cycle = [
            row(
                "0xtop",
                "universal_resolver",
                "0xmanaged",
                "universal_resolver_proxy",
            ),
            row(
                "0xmanaged",
                "universal_resolver_managed",
                "0xtop",
                "universal_resolver_proxy",
            ),
        ];
        assert!(!cut_over(&cycle));
    }
}
