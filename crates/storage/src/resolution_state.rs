//! Per-chain resolution protocol in force, for reporting. Serving decides the Universal Resolver
//! cutover with `families::control::cutover::cut_over`; this reads the same Project rows, takes
//! the decision from that function, and also names the row where the client-facing proxy chain
//! ends, so a reader can tell an implementation the manifest does not admit from a proxy with no
//! `Upgraded` yet. It sits outside `families`, so it is not part of the interpreter content hash.
use anyhow::{Context, Result};
use sqlx::{FromRow, PgConnection};

use crate::families::control::cutover::{ProxyRow, cut_over};

const CLIENT_FACING_ROLE: &str = "universal_resolver";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    EnsV1,
    EnsV2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolutionState {
    /// `EnsV2` while the client-facing chain ends at an admitted UniversalResolverV2.
    pub protocol: Protocol,
    /// The `Upgraded` block of the row that ended the walk.
    pub since_block: i64,
    /// The proxy and implementation of that row.
    pub proxy: String,
    pub implementation: String,
    /// The walk ended at an implementation the manifest neither admits nor declares as a proxy.
    /// False for a hop to a proxy with no `Upgraded` yet.
    pub unadmitted: bool,
}

#[derive(FromRow)]
struct StateRow {
    #[sqlx(flatten)]
    proxy: ProxyRow,
    block_number: i64,
}

/// `None` when the chain has no client-facing proxy row, as on a chain with no proxy `Upgraded`.
pub async fn load_resolution_state_on(
    conn: &mut PgConnection,
    chain_id: &str,
) -> Result<Option<ResolutionState>> {
    let rows: Vec<StateRow> = sqlx::query_as(
        "/* storage:resolution_state */
         SELECT proxy.proxy_address, proxy.proxy_role, proxy.implementation,
                proxy.implementation_kind, proxy.block_number
         FROM bigname_phase.project_universal_resolver_proxy proxy
         WHERE proxy.chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(conn)
    .await
    .context("failed to load the Universal Resolver proxies")?;
    Ok(state(&rows))
}

fn state(rows: &[StateRow]) -> Option<ResolutionState> {
    let mut at = rows
        .iter()
        .find(|row| row.proxy.proxy_role.as_deref() == Some(CLIENT_FACING_ROLE))?;
    let mut visited = vec![at.proxy.proxy_address.as_str()];
    while at.proxy.implementation_kind == "universal_resolver_proxy" {
        let Some(next) = rows
            .iter()
            .find(|row| row.proxy.proxy_address == at.proxy.implementation)
        else {
            break;
        };
        if visited.contains(&next.proxy.proxy_address.as_str()) {
            break;
        }
        visited.push(&next.proxy.proxy_address);
        at = next;
    }
    let proxies: Vec<ProxyRow> = rows.iter().map(|row| row.proxy.clone()).collect();
    Some(ResolutionState {
        protocol: if cut_over(&proxies) {
            Protocol::EnsV2
        } else {
            Protocol::EnsV1
        },
        since_block: at.block_number,
        proxy: at.proxy.proxy_address.clone(),
        implementation: at.proxy.implementation.clone(),
        unadmitted: at.proxy.implementation_kind == "other",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(proxy: &str, role: &str, implementation: &str, kind: &str, block: i64) -> StateRow {
        StateRow {
            proxy: ProxyRow {
                proxy_address: proxy.into(),
                proxy_role: Some(role.into()),
                implementation: implementation.into(),
                implementation_kind: kind.into(),
            },
            block_number: block,
        }
    }

    fn top() -> StateRow {
        row(
            "0xtop",
            "universal_resolver",
            "0xmanaged",
            "universal_resolver_proxy",
            10,
        )
    }

    fn managed(implementation: &str, kind: &str) -> StateRow {
        row(
            "0xmanaged",
            "universal_resolver_managed",
            implementation,
            kind,
            20,
        )
    }

    fn expect(
        protocol: Protocol,
        since: i64,
        proxy: &str,
        implementation: &str,
        unadmitted: bool,
    ) -> Option<ResolutionState> {
        Some(ResolutionState {
            protocol,
            since_block: since,
            proxy: proxy.into(),
            implementation: implementation.into(),
            unadmitted,
        })
    }

    #[test]
    fn the_walk_names_the_row_that_ends_the_client_facing_chain() {
        assert_eq!(
            state(&[top(), managed("0xv2", "admitted_universal_resolver")]),
            expect(Protocol::EnsV2, 20, "0xmanaged", "0xv2", false)
        );
        assert_eq!(
            state(&[top(), managed("0xnew", "other")]),
            expect(Protocol::EnsV1, 20, "0xmanaged", "0xnew", true)
        );
        assert_eq!(
            state(&[top()]),
            expect(Protocol::EnsV1, 10, "0xtop", "0xmanaged", false),
            "a hop to a proxy with no row yet is not unadmitted"
        );
        assert_eq!(state(&[]), None);
        assert_eq!(
            state(&[managed("0xnew", "other")]),
            None,
            "only the client-facing chain counts"
        );
        assert_eq!(
            state(&[top(), managed("0xtop", "universal_resolver_proxy")]),
            expect(Protocol::EnsV1, 20, "0xmanaged", "0xtop", false),
            "a cycle ends at its last hop"
        );
        assert_eq!(
            state(&[row("0xtop", "universal_resolver", "0xnew", "other", 30)]),
            expect(Protocol::EnsV1, 30, "0xtop", "0xnew", true),
            "the client-facing proxy itself can point at an unadmitted implementation"
        );
        // A retired declaration keeps its row with no role; Project classifies a hop to it as
        // `other`, and the row itself never starts the chain.
        let mut retired = managed("0xv2", "admitted_universal_resolver");
        retired.proxy.proxy_role = None;
        let top_to_retired = row("0xtop", "universal_resolver", "0xmanaged", "other", 10);
        assert_eq!(
            state(&[top_to_retired, retired]),
            expect(Protocol::EnsV1, 10, "0xtop", "0xmanaged", true)
        );
        let mut retired = managed("0xv2", "admitted_universal_resolver");
        retired.proxy.proxy_role = None;
        assert_eq!(state(&[retired]), None);
        assert_eq!(
            state(&[row(
                "0xtop",
                "universal_resolver",
                "0xv2",
                "admitted_universal_resolver",
                30
            )]),
            expect(Protocol::EnsV2, 30, "0xtop", "0xv2", false)
        );
        assert_eq!(
            state(&[row(
                "0xtop",
                "universal_resolver",
                "0xtop",
                "universal_resolver_proxy",
                30
            )]),
            expect(Protocol::EnsV1, 30, "0xtop", "0xtop", false),
            "a proxy pointing at itself ends the walk"
        );
    }
}
