//! The Universal Resolver cutover: whether bigname composes a chain's `.eth` names as the ENSv2
//! Universal Resolver reads them. A chain is cut over while its deployment profile admits an
//! ENSv2 root registry: an active `ens` manifest of source family `ens_v2_root_l1` that declares
//! a contract with the role `root_registry`. UniversalResolverV2 resolves from its immutable
//! root registry, so that declaration is the deployment the cutover follows.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/UniversalResolverV2.sol:L20 @ ens_v2_sepolia_20261001@07e55a05)
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/UniversalResolverV2.sol:L55-L63 @ ens_v2_sepolia_20261001@07e55a05)
//!
//! The flag has no block. Manifest sync carries no block position and a manifest change is
//! adopted by a full redo, so an admitted chain reads as cut over at every block. The
//! client-facing Universal Resolver proxy and its `Upgraded` events are not an input. Project
//! keeps them for monitoring only (`crate::resolution_state`).
use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::PgConnection;

const ROOT_REGISTRY_ROLE: &str = "root_registry";

/// The root registry declaration that cuts a chain over. Project derives it from the manifest
/// set a publication composes with and records it on the marker. Every reader takes the flag and
/// the root address from the publication, so the ENSv2 path walk starts at the admitted root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Admission {
    /// The admitted root registry's lower-cased address.
    pub root_registry: String,
    /// Its declared `start_block`.
    pub since_block: Option<i64>,
}

/// The admission the payloads of a chain's active `ens_v2_root_l1` manifests declare. A chain
/// has at most one active manifest per source family and a manifest's roles are unique, so more
/// than one declared root is a broken manifest set, not a choice to make.
pub fn admission(payloads: &[Value]) -> Result<Option<Admission>> {
    let roots: Vec<Admission> = payloads
        .iter()
        .flat_map(|payload| payload["contracts"].as_array().into_iter().flatten())
        .filter(|contract| contract["role"] == ROOT_REGISTRY_ROLE)
        .filter_map(|contract| {
            Some(Admission {
                root_registry: contract["address"].as_str()?.to_ascii_lowercase(),
                since_block: contract["start_block"].as_i64(),
            })
        })
        .collect();
    anyhow::ensure!(
        roots.len() <= 1,
        "the active ens_v2_root_l1 manifests declare {} root registries",
        roots.len()
    );
    Ok(roots.into_iter().next())
}

/// The admission that cuts chain `chain_id` over, `None` while it is not cut over.
pub async fn load_admission_on(
    conn: &mut PgConnection,
    chain_id: &str,
) -> Result<Option<Admission>> {
    let payloads: Vec<Value> = sqlx::query_scalar(
        "/* storage:families.control.cutover */
         SELECT manifest.manifest_payload
         FROM bigname_phase.manifest_versions manifest
         WHERE manifest.chain_id = $1 AND manifest.namespace = 'ens'
           AND manifest.source_family = 'ens_v2_root_l1'
           AND manifest.rollout_status = 'active'",
    )
    .bind(chain_id)
    .fetch_all(conn)
    .await
    .context("failed to load the ENSv2 root registry admission")?;
    admission(&payloads)
}

/// Whether chain `chain_id` is cut over.
pub async fn load_cut_over_on(conn: &mut PgConnection, chain_id: &str) -> Result<bool> {
    Ok(load_admission_on(conn, chain_id).await?.is_some())
}

#[cfg(test)]
mod tests {
    use bigname_test_support::{TestDatabase, TestDatabaseConfig};
    use serde_json::{Value, json};

    use super::*;

    const CHAIN: &str = "ethereum-sepolia";

    async fn database(name: &str) -> Result<TestDatabase> {
        let database = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
        sqlx::raw_sql(
            "CREATE SCHEMA bigname_phase;
             CREATE TABLE bigname_phase.manifest_versions (
                 manifest_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, chain_id text,
                 namespace text, source_family text, rollout_status text, manifest_payload jsonb);
             CREATE TABLE bigname_phase.project_universal_resolver_proxy (chain_id text,
                 proxy_address text, proxy_role text, implementation text,
                 implementation_kind text);",
        )
        .execute(database.pool())
        .await?;
        Ok(database)
    }

    async fn manifest(
        pool: &sqlx::PgPool,
        chain: &str,
        namespace: &str,
        family: &str,
        status: &str,
        contracts: Value,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO bigname_phase.manifest_versions
                 (chain_id, namespace, source_family, rollout_status, manifest_payload)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(chain)
        .bind(namespace)
        .bind(family)
        .bind(status)
        .bind(json!({"contracts": contracts}))
        .execute(pool)
        .await?;
        Ok(())
    }

    fn root(start_block: i64) -> Value {
        json!([{"role": "root_registry", "address": "0xR00T", "start_block": start_block}])
    }

    // R1. The client-facing proxy forwards to an implementation the `ens_execution` manifest
    // lists, on a chain whose profile admits no ENSv2 root registry.
    #[tokio::test]
    async fn an_admitted_universal_resolver_without_an_ens_v2_root_manifest_is_not_cut_over()
    -> Result<()> {
        let database = database("cutover_proxy_only").await?;
        let pool = database.pool();
        manifest(
            pool,
            CHAIN,
            "ens",
            "ens_execution",
            "active",
            json!([{"role": "universal_resolver", "address": "0xtop", "start_block": 0}]),
        )
        .await?;
        sqlx::query(
            "INSERT INTO bigname_phase.project_universal_resolver_proxy VALUES
             ($1, '0xtop', 'universal_resolver', '0xv2', 'admitted_universal_resolver')",
        )
        .bind(CHAIN)
        .execute(pool)
        .await?;
        // Another chain's root registry admits nothing here.
        manifest(
            pool,
            "other-chain",
            "ens",
            "ens_v2_root_l1",
            "active",
            root(5),
        )
        .await?;
        let mut conn = pool.acquire().await?;
        let cut_over = load_cut_over_on(&mut conn, CHAIN).await;
        drop(conn);
        database.cleanup().await?;
        assert!(
            !cut_over?,
            "a proxy upgrade alone must not cut the chain over"
        );
        Ok(())
    }

    // R15, and the rest of the admission predicate: only an active `ens` `ens_v2_root_l1`
    // manifest that declares `root_registry` cuts the chain over.
    #[tokio::test]
    async fn a_shadow_ens_v2_root_manifest_does_not_cut_over() -> Result<()> {
        let database = database("cutover_rollout").await?;
        let pool = database.pool();
        for status in ["shadow", "draft", "deprecated"] {
            manifest(pool, CHAIN, "ens", "ens_v2_root_l1", status, root(1)).await?;
        }
        manifest(
            pool,
            CHAIN,
            "ens",
            "ens_v2_registry_l1",
            "active",
            json!([{"role": "registry", "address": "0xeth", "start_block": 2}]),
        )
        .await?;
        manifest(
            pool,
            CHAIN,
            "basenames",
            "ens_v2_root_l1",
            "active",
            root(3),
        )
        .await?;
        let mut conn = pool.acquire().await?;
        let before = load_cut_over_on(&mut conn, CHAIN).await;
        sqlx::query(
            "UPDATE bigname_phase.manifest_versions SET rollout_status = 'active'
             WHERE namespace = 'ens' AND source_family = 'ens_v2_root_l1'
               AND rollout_status = 'shadow'",
        )
        .execute(&mut *conn)
        .await?;
        let after = load_cut_over_on(&mut conn, CHAIN).await;
        let admitted = load_admission_on(&mut conn, CHAIN).await;
        drop(conn);
        database.cleanup().await?;
        assert!(!before?, "only an active root manifest admits the chain");
        assert!(
            after?,
            "the active root registry declaration cuts the chain over"
        );
        assert_eq!(
            admitted?,
            Some(Admission {
                root_registry: "0xr00t".into(),
                since_block: Some(1)
            })
        );
        Ok(())
    }

    #[test]
    fn the_root_registry_declaration_names_and_dates_the_admission() -> Result<()> {
        let other = json!({"contracts": [{"role": "registry", "address": "0xeth",
            "start_block": 4}]});
        assert_eq!(admission(&[])?, None);
        assert_eq!(admission(&[json!({}), other.clone()])?, None);
        assert_eq!(
            admission(&[other, json!({"contracts": root(9)})])?,
            Some(Admission {
                root_registry: "0xr00t".into(),
                since_block: Some(9)
            })
        );
        assert_eq!(
            admission(&[json!({"contracts": [{"role": "root_registry"}]})])?,
            None,
            "a declaration with no address names no root to walk from"
        );
        assert_eq!(
            admission(&[json!({"contracts": [{"role": "root_registry", "address": "0xR00T"}]})])?,
            Some(Admission {
                root_registry: "0xr00t".into(),
                since_block: None
            }),
            "a root with no declared start block still admits, undated"
        );
        let two = admission(&[json!({"contracts": root(1)}), json!({"contracts": root(2)})]);
        assert!(format!("{:#}", two.expect_err("two roots")).contains("declare 2 root registries"));
        Ok(())
    }

    #[tokio::test]
    async fn an_unreadable_manifest_table_is_an_error_not_a_missing_admission() -> Result<()> {
        let database = TestDatabase::create(TestDatabaseConfig::new("cutover_no_table")).await?;
        let mut conn = database.pool().acquire().await?;
        let loaded = load_cut_over_on(&mut conn, CHAIN).await;
        drop(conn);
        database.cleanup().await?;
        let error = loaded.expect_err("a missing table must not read as not cut over");
        assert!(
            format!("{error:#}").contains("failed to load the ENSv2 root registry admission"),
            "{error:#}"
        );
        Ok(())
    }
}
