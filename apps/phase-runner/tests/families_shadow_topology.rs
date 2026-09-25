//! TYR-36 step 5: the alias arm of name topology (F10 with F5) against
//! `name_current.declared_summary.topology` at one publication. The same publications feed the
//! resolver comparison, so the alias resolvers' `/aliases` pages and bound names are compared
//! too. The wildcard arm has no fixture: no interpreter producer writes an
//! `observed_wildcard_path` binding (crates/project/src/scope.rs, the wildcard scope arm), so
//! there is no operating path to test until one is wired.
#[allow(dead_code)]
#[path = "project_end_to_end/shadow.rs"]
mod shadow;
#[allow(dead_code)]
#[path = "project_end_to_end/shadow_fixture.rs"]
mod shadow_fixture;
#[allow(dead_code)]
mod support;

use anyhow::{Result, ensure};
use bigname_storage::families::topology::load_name_topology_shadow;
use serde_json::{Value, json};
use shadow_fixture::{Fixture, ZERO_ADDRESS, address, extra_not_active, unexpected, uuid, word};

const REGISTRY: &str = "ens_v2_registry_l1";
const RESOLVER: &str = "ens_v2_resolver_l1";
const ETH: u64 = 0xe7;

struct Name {
    logical: String,
    resource: String,
}

/// A second-level `.eth` name bound to its own resource with `binding_kind`.
async fn name(fixture: &Fixture, n: u64, label: &str, binding_kind: &str) -> Result<Name> {
    let logical = fixture
        .surface(
            "ens",
            &word(0x1000 + n),
            &format!("{label}.eth"),
            &[word(0x2000 + n), word(ETH)],
            1,
        )
        .await?;
    let resource = uuid(0xa000 + n);
    fixture
        .binding(
            &uuid(0xb000 + n),
            &logical,
            &resource,
            binding_kind,
            "ens_v2",
            1,
        )
        .await?;
    Ok(Name { logical, resource })
}

async fn point(
    fixture: &Fixture,
    identity: &str,
    name: &Name,
    resolver: &str,
    block: i64,
) -> Result<()> {
    fixture
        .event(
            identity,
            Some(&name.logical),
            Some(&name.resource),
            REGISTRY,
            "ResolverChanged",
            block,
            json!({"resolver": resolver}),
            &address(0xe3),
        )
        .await?;
    Ok(())
}

async fn alias(
    fixture: &Fixture,
    identity: &str,
    from: &Name,
    resolver: &str,
    target: &str,
    block: i64,
) -> Result<()> {
    fixture
        .event(
            identity,
            Some(&from.logical),
            None,
            RESOLVER,
            "AliasChanged",
            block,
            json!({"resolver": resolver, "active": true, "alias_state": "active",
                   "from_dns_encoded_name": "0x0161", "to_dns_encoded_name": "0x0162",
                   "to_logical_name_id": target, "to_name": "target.eth"}),
            resolver,
        )
        .await?;
    Ok(())
}

async fn served_topology(fixture: &Fixture, logical: &str) -> Result<Option<Value>> {
    Ok(sqlx::query_scalar(
        "SELECT declared_summary -> 'topology' FROM name_current WHERE logical_name_id = $1",
    )
    .bind(logical)
    .fetch_optional(fixture.pool())
    .await?
    .flatten())
}

// An alias whose source resolver changes with no alias event follows the new resolver; an alias
// whose source pointer is cleared with no alias event has no topology and exposes no older
// pointer.
#[tokio::test]
async fn alias_topology_matches_the_served_names() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_topology", 12).await?;
    let first_resolver = address(0xc1);
    let second_resolver = address(0xc2);
    let target = name(&fixture, 1, "target", "declared_registry_path").await?;
    let followed = name(&fixture, 2, "followed", "resolver_alias_path").await?;
    let cleared = name(&fixture, 3, "cleared", "resolver_alias_path").await?;

    for source in [&followed, &cleared] {
        point(
            &fixture,
            &format!("point-{}", source.logical),
            source,
            &first_resolver,
            2,
        )
        .await?;
        alias(
            &fixture,
            &format!("alias-{}", source.logical),
            source,
            &first_resolver,
            &target.logical,
            2,
        )
        .await?;
    }
    fixture.publish(4).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(report.topology_names >= 2, "{}", report.line());
    // The resolvers are undeclared: F3 keeps a resolver_manifest_not_active row for each one a
    // name points at, which the served build does not write (step 2's declared approximation).
    extra_not_active(&report, &[&first_resolver])?;
    for logical in [&followed.logical, &cleared.logical] {
        ensure!(
            served_topology(&fixture, logical).await?.is_some()
                && load_name_topology_shadow(fixture.pool(), logical)
                    .await?
                    .is_some(),
            "{logical} has no topology"
        );
    }

    point(&fixture, "followed-moves", &followed, &second_resolver, 6).await?;
    point(&fixture, "cleared-zero", &cleared, ZERO_ADDRESS, 6).await?;
    fixture.publish(8).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    fixture.rebuild().await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    let followed_topology = load_name_topology_shadow(fixture.pool(), &followed.logical)
        .await?
        .unwrap_or_default();
    ensure!(
        followed_topology.pointer("/resolver_path/0/address") == Some(&json!(second_resolver)),
        "{followed_topology}"
    );
    ensure!(
        load_name_topology_shadow(fixture.pool(), &cleared.logical)
            .await?
            .is_none()
    );
    fixture.cleanup().await
}
