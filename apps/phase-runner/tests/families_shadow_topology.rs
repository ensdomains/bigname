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
    let version_event = fixture
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
    // The resolvers are undeclared: F3 keeps a resolver_manifest_not_active row for each one a
    // name points at, which the served build does not write (step 2's declared approximation).
    extra_not_active(&report, &[&first_resolver, &wildcard_resolver])?;
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
    // The difference is exactly the boundary: the served topology still bounds at block 3, the
    // shadow at block 7, and with the shadow's boundaries put in, the served topology is the
    // shadow's.
    // The difference, pinned in full: the served boundaries are the version event at block 3 and
    // the shadow's the zero pointer at block 7, each exactly, and nothing else differs.
    let pin = WildcardPin {
        logical: &wildcard.logical,
        ancestor: &ancestor,
        version_event,
    };
    pin.check(&fixture).await?;
    // A second difference inside the exempted boundary still fails: a changed served block hash
    // leaves the known difference in place but breaks the pin.
    sqlx::query(
        "UPDATE name_current SET declared_summary = jsonb_set(declared_summary,
             '{topology,version_boundaries,record_version_boundary,chain_position,block_hash}',
             '\"another-hash\"')
         WHERE logical_name_id = $1",
    )
    .bind(&wildcard.logical)
    .execute(fixture.pool())
    .await?;
    ensure!(
        pin.check(&fixture).await.is_err(),
        "a changed boundary hash passed the pin"
    );
    unexpected(
        &fixture.compare(1).await?,
        &[format!("topology of {}", wildcard.logical)],
    )?;
    unexpected(&report, &[format!("topology of {}", wildcard.logical)])?;
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

/// A name `raw_name` under `.eth` with node `0x1<n>` and the given labelhashes, bound to its own
/// resource with `binding_kind`.
async fn deep_name(
    fixture: &Fixture,
    n: u64,
    raw_name: &str,
    labels: &[String],
    binding_kind: &str,
) -> Result<Name> {
    let logical = fixture
        .surface("ens", &word(0x1000 + n), raw_name, labels, 1)
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

// A wildcard name's resolver hop from a mixed-case ResolverChanged: the builder copies the
// payload's `resolver` as spelled (crates/project/src/builders/name_topology.rs), and
// `serialize_projected_topologies` (name_topology/serialization.rs) then rewrites the stored
// topology through `ResolutionTopology`, whose addresses serialize in canonical lowercase. So the
// served spelling is lowercase, which is what the shadow serves from the lowercased pointer row.
// The longest ancestor with a binding and a non-zero pointer is the source: a longer ancestor
// with a binding but no pointer is passed over.
#[tokio::test]
async fn wildcard_path_keeps_the_served_resolver_spelling_across_ancestors() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_topology_casing", 12).await?;
    let outer_resolver = "0xAbCdEf0000000000000000000000000000000C11";
    let inner_resolver = "0x00000000000000000000000000000000DeAdBeEf";
    let eth = word(ETH);
    let wild = deep_name(
        &fixture,
        11,
        "wild.eth",
        &[word(0x2011), eth.clone()],
        "declared_registry_path",
    )
    .await?;
    let inner = deep_name(
        &fixture,
        12,
        "b.wild.eth",
        &[word(0x2012), word(0x2011), eth.clone()],
        "declared_registry_path",
    )
    .await?;
    // Bound, never pointed: not a source.
    deep_name(
        &fixture,
        13,
        "y.wild.eth",
        &[word(0x2013), word(0x2011), eth.clone()],
        "declared_registry_path",
    )
    .await?;
    let through_inner = deep_name(
        &fixture,
        14,
        "a.b.wild.eth",
        &[word(0x2014), word(0x2012), word(0x2011), eth.clone()],
        "observed_wildcard_path",
    )
    .await?;
    let through_outer = deep_name(
        &fixture,
        15,
        "x.y.wild.eth",
        &[word(0x2015), word(0x2013), word(0x2011), eth],
        "observed_wildcard_path",
    )
    .await?;
    point(&fixture, "outer-point", &wild, outer_resolver, 2).await?;
    point(&fixture, "inner-point", &inner, inner_resolver, 3).await?;
    fixture.publish(4).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    extra_not_active(
        &report,
        &[
            &inner_resolver.to_ascii_lowercase(),
            &outer_resolver.to_ascii_lowercase(),
        ],
    )?;
    for (name, source, resolver) in [
        (&through_inner, &inner, inner_resolver),
        (&through_outer, &wild, outer_resolver),
    ] {
        let served = served_topology(&fixture, &name.logical)
            .await?
            .unwrap_or_default();
        let topology = load_name_topology_shadow(fixture.pool(), &name.logical)
            .await?
            .unwrap_or_default();
        let canonical = json!(resolver.to_ascii_lowercase());
        ensure!(
            served == topology
                && served.pointer("/resolver_path/0/address") == Some(&canonical)
                && served.pointer("/wildcard/source/logical_name_id")
                    == Some(&json!(source.logical)),
            "served {served}, shadow {topology}"
        );
    }
    fixture.cleanup().await
}

/// The wildcard boundary difference of `alias_and_wildcard_topology_match_the_served_names`.
struct WildcardPin<'a> {
    logical: &'a str,
    ancestor: &'a Name,
    version_event: i64,
}

impl WildcardPin<'_> {
    /// Both boundary objects exactly as expected, and the two topologies equal once the
    /// boundaries are set aside.
    async fn check(&self, fixture: &Fixture) -> Result<()> {
        let mut served = served_topology(fixture, self.logical)
            .await?
            .unwrap_or_default();
        let mut shadowed = load_name_topology_shadow(fixture.pool(), self.logical)
            .await?
            .unwrap_or_default();
        let boundary = |block: i64, event_id: Value, event_kind: Value| async move {
            let timestamp: Value = sqlx::query_scalar("SELECT to_jsonb(to_timestamp($1::bigint))")
                .bind(shadow_fixture::EPOCH + block)
                .fetch_one(fixture.pool())
                .await?;
            let one = json!({
                "logical_name_id": self.ancestor.logical,
                "resource_id": self.ancestor.resource,
                "normalized_event_id": event_id,
                "event_kind": event_kind,
                "chain_position": {
                    "chain_id": shadow_fixture::CHAIN,
                    "block_number": block,
                    "block_hash": shadow_fixture::hash(block),
                    "timestamp": timestamp,
                },
            });
            anyhow::Ok(json!({
                "topology_version_boundary": one.clone(),
                "record_version_boundary": one,
            }))
        };
        let served_boundaries =
            boundary(3, json!(self.version_event), json!("RecordVersionChanged")).await?;
        let shadow_boundaries = boundary(7, Value::Null, Value::Null).await?;
        ensure!(
            served["version_boundaries"] == served_boundaries,
            "served {}",
            served["version_boundaries"]
        );
        ensure!(
            shadowed["version_boundaries"] == shadow_boundaries,
            "shadow {}",
            shadowed["version_boundaries"]
        );
        served["version_boundaries"] = Value::Null;
        shadowed["version_boundaries"] = Value::Null;
        ensure!(served == shadowed, "served {served}, shadow {shadowed}");
        Ok(())
    }
}
