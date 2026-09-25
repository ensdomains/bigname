//! TYR-36 step 5: the alias and wildcard arms of name topology (F10 with F5) against
//! `name_current.declared_summary.topology` at one publication. The same publications feed the
//! resolver comparison, so the alias resolvers' `/aliases` pages and bound names are compared
//! too.
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
use shadow_fixture::{Fixture, ZERO_ADDRESS, address, unexpected, uuid, word};

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
// pointer; a wildcard name reads its ancestor's historical non-zero pointer and an independent
// version boundary, which moves to the ancestor's later zero pointer.
#[tokio::test]
async fn alias_and_wildcard_topology_match_the_served_names() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_topology", 12).await?;
    let first_resolver = address(0xc1);
    let second_resolver = address(0xc2);
    let wildcard_resolver = address(0xc3);
    let target = name(&fixture, 1, "target", "declared_registry_path").await?;
    let followed = name(&fixture, 2, "followed", "resolver_alias_path").await?;
    let cleared = name(&fixture, 3, "cleared", "resolver_alias_path").await?;
    let ancestor = name(&fixture, 4, "wild", "declared_registry_path").await?;
    let wildcard_logical = fixture
        .surface(
            "ens",
            &word(0x1005),
            "sub.wild.eth",
            &[word(0x2005), word(0x2004), word(ETH)],
            1,
        )
        .await?;
    let wildcard = Name {
        resource: uuid(0xa005),
        logical: wildcard_logical,
    };
    fixture
        .binding(
            &uuid(0xb005),
            &wildcard.logical,
            &wildcard.resource,
            "observed_wildcard_path",
            "ens_v2",
            1,
        )
        .await?;

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
    point(&fixture, "wild-point", &ancestor, &wildcard_resolver, 2).await?;
    fixture
        .event(
            "wild-version",
            Some(&ancestor.logical),
            Some(&ancestor.resource),
            RESOLVER,
            "RecordVersionChanged",
            3,
            json!({"resolver": wildcard_resolver, "node": word(0x1004), "version": 1}),
            &wildcard_resolver,
        )
        .await?;
    fixture.publish(4).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(report.topology_names >= 3, "{}", report.line());
    for logical in [&followed.logical, &cleared.logical, &wildcard.logical] {
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
    point(&fixture, "wild-zero", &ancestor, ZERO_ADDRESS, 7).await?;
    fixture.publish(8).await?;
    // Expected difference: the served incremental batch keeps the wildcard name's version
    // boundary at block 3. The alias and wildcard scope adds a scoped wildcard name's ancestors
    // (crates/project/src/scope.rs:373-400), never a changed ancestor's wildcard descendants, so
    // the ancestor's zero pointer at block 7 does not restage `sub.wild.eth`; a rebuild at the
    // same block moves the boundary to block 7, as the shadow does. No interpreter producer
    // writes observed_wildcard_path today (scope.rs:389-390).
    let report = fixture.compare(1).await?;
    unexpected(&report, &[&format!("topology of {}", wildcard.logical)])?;
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
    let wildcard_topology = load_name_topology_shadow(fixture.pool(), &wildcard.logical)
        .await?
        .unwrap_or_default();
    ensure!(
        wildcard_topology.pointer("/resolver_path/0/address") == Some(&json!(wildcard_resolver))
            && wildcard_topology.pointer(
                "/version_boundaries/topology_version_boundary/chain_position/block_number"
            ) == Some(&json!(7)),
        "{wildcard_topology}"
    );
    fixture.cleanup().await
}
