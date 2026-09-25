//! TYR-36 step 5: the resolver readers over the families against the served ones at one
//! publication: `bound_names` over the F5 resolver index, `/aliases` (F10 per-resolver rows and
//! the alias-path binding arm over F5), `/links` (F7) and `/roles` (F8), each paged at size one
//! so every page and its total are compared.
#[allow(dead_code)]
#[path = "project_end_to_end/shadow.rs"]
mod shadow;
#[allow(dead_code)]
#[path = "project_end_to_end/shadow_fixture.rs"]
mod shadow_fixture;
#[allow(dead_code)]
mod support;

use anyhow::{Result, ensure};
use bigname_storage::families::topology::{FamilyCollectionPage, load_resolver_links_shadow};
use serde_json::{Value, json};
use shadow_fixture::{CHAIN, Fixture, ZERO_ADDRESS, ZERO_NODE, address, unexpected, uuid, word};

const REGISTRY: &str = "ens_v2_registry_l1";
const RESOLVER: &str = "ens_v2_resolver_l1";
const ETH: u64 = 0xe7;

struct Name {
    logical: String,
    node: String,
    resource: String,
}

async fn name(fixture: &Fixture, n: u64, label: &str, binding_kind: &str) -> Result<Name> {
    let node = word(0x1000 + n);
    let logical = fixture
        .surface(
            "ens",
            &node,
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
    fixture
        .event(
            &format!("granted-{label}"),
            Some(&logical),
            Some(&resource),
            REGISTRY,
            "RegistrationGranted",
            1,
            json!({"registry_contract_instance_id": uuid(0xf1), "status": "registered",
                   "registrant": address(0xa000 + n), "owner": address(0xa000 + n),
                   "expiry": 4_000_000_000_i64, "authority_kind": "registrar"}),
            &address(0xf1),
        )
        .await?;
    Ok(Name {
        logical,
        node,
        resource,
    })
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
            &address(0xf1),
        )
        .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn alias(
    fixture: &Fixture,
    identity: &str,
    from: &Name,
    resolver: &str,
    active: bool,
    target: &Name,
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
            json!({"resolver": resolver, "active": active,
                   "alias_state": if active { "active" } else { "removed" },
                   "from_dns_encoded_name": "0x0161", "to_dns_encoded_name": "0x0162",
                   "to_logical_name_id": target.logical, "to_name": "one.eth"}),
            resolver,
        )
        .await?;
    Ok(())
}

async fn link(
    fixture: &Fixture,
    identity: &str,
    resolver: &str,
    node: &str,
    record_id: &str,
    emitter: &str,
    block: i64,
) -> Result<()> {
    fixture
        .event(
            identity,
            None,
            None,
            RESOLVER,
            "ResolverRecordLinked",
            block,
            json!({"resolver": resolver, "node": node, "resolver_record_id": record_id,
                   "storage_model": "resolver_record_id"}),
            emitter,
        )
        .await?;
    Ok(())
}

async fn grant(
    fixture: &Fixture,
    identity: &str,
    resolver: &str,
    subject: &str,
    resource: u64,
    powers: Value,
    block: i64,
) -> Result<()> {
    let resource_id = uuid(0xd000 + resource);
    fixture.resource(&resource_id, block).await?;
    let revoked = powers.as_array().is_some_and(Vec::is_empty);
    fixture
        .event(
            identity,
            None,
            Some(&resource_id),
            RESOLVER,
            "PermissionChanged",
            block,
            json!({
                "subject": subject,
                "scope": {"kind": "resolver", "chain_id": CHAIN, "resolver_address": resolver},
                "effective_powers": powers,
                "grant_source": {"kind": "raw_log", "source_event": "EACRolesChanged",
                    "upstream_resource": word(resource), "root_resource": false,
                    "changed_powers": ["set_text"]},
                "revocation_source": if revoked {
                    json!({"kind": "raw_log", "source_event": "EACRolesChanged"})
                } else {
                    Value::Null
                },
                "inheritance_path": [], "transfer_behavior": {},
                "source_event": "EACRolesChanged", "upstream_resource": word(resource),
                "resource": word(resource), "root_resource": false,
                "storage_model": "resolver_record_id", "resolver": resolver,
                "resolver_record_id": "0", "record_key": "permission",
            }),
            resolver,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn resolver_collections_and_bound_names_match_the_served_readers() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_resolver", 12).await?;
    let first = address(0xd1);
    let second = address(0xd2);
    let stranger = address(0xd9);
    // Both resolvers are declared, so the overview's classification is compared for each.
    fixture
        .declare_resolvers(RESOLVER, &[&first, &second])
        .await?;
    let one = name(&fixture, 1, "one", "declared_registry_path").await?;
    let two = name(&fixture, 2, "two", "declared_registry_path").await?;
    let three = name(&fixture, 3, "three", "declared_registry_path").await?;
    let four = name(&fixture, 4, "four", "declared_registry_path").await?;
    let path = name(&fixture, 5, "path", "resolver_alias_path").await?;
    let aliased = name(&fixture, 6, "aliased", "declared_registry_path").await?;

    for (identity, target, resolver) in [
        ("one-first", &one, &first),
        ("two-first", &two, &first),
        ("three-first", &three, &first),
        ("four-first", &four, &first),
        ("path-first", &path, &first),
    ] {
        point(&fixture, identity, target, resolver, 2).await?;
    }
    // Aliases at two resolvers, one of them removed, and the alias-path binding at the first.
    alias(
        &fixture,
        "alias-aliased-first",
        &aliased,
        &first,
        true,
        &one,
        2,
    )
    .await?;
    alias(&fixture, "alias-two-second", &two, &second, true, &one, 2).await?;
    alias(
        &fixture,
        "alias-three-second",
        &three,
        &second,
        true,
        &one,
        2,
    )
    .await?;
    alias(
        &fixture,
        "alias-three-removed",
        &three,
        &second,
        false,
        &one,
        3,
    )
    .await?;
    alias(&fixture, "alias-path", &path, &first, true, &one, 2).await?;
    // Links: a named node, the default node, one cleared by record 0, and one whose later
    // record replaces an earlier one.
    link(&fixture, "link-one", &first, &one.node, "5", &first, 2).await?;
    link(&fixture, "link-default", &first, ZERO_NODE, "7", &first, 2).await?;
    link(&fixture, "link-three", &first, &three.node, "9", &first, 2).await?;
    link(
        &fixture,
        "link-three-clear",
        &first,
        &three.node,
        "0",
        &first,
        3,
    )
    .await?;
    link(&fixture, "link-four", &first, &four.node, "11", &first, 2).await?;
    link(
        &fixture,
        "link-four-again",
        &first,
        &four.node,
        "12",
        &first,
        3,
    )
    .await?;
    // Roles: two holders, an empty-effective grant and a revoked one, which are excluded.
    grant(
        &fixture,
        "grant-a",
        &first,
        &address(0xee1),
        1,
        json!(["set_text"]),
        2,
    )
    .await?;
    grant(
        &fixture,
        "grant-b",
        &first,
        &address(0xee2),
        2,
        json!(["set_addr", "set_text"]),
        2,
    )
    .await?;
    grant(
        &fixture,
        "grant-empty",
        &first,
        &address(0xee3),
        3,
        json!([]),
        2,
    )
    .await?;
    grant(
        &fixture,
        "grant-c",
        &first,
        &address(0xee4),
        4,
        json!(["set_text"]),
        2,
    )
    .await?;
    grant(
        &fixture,
        "grant-c-revoked",
        &first,
        &address(0xee4),
        4,
        json!([]),
        3,
    )
    .await?;
    fixture.publish(4).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(
        report.bound_names == 5 && report.aliases >= 4 && report.links >= 3 && report.roles >= 2,
        "{}",
        report.line()
    );
    // project_resolver_classification is not filled yet, so both resolvers are classified from
    // their declaration. When the table is filled this turns red: then the classification rows
    // are compared in full, support status included, and this assertion goes.
    ensure!(report.f3_unfilled > 0, "{}", report.line());

    // Block 6: the second name moves to the second resolver, the fourth clears its pointer, and a
    // stranger emits a Linked naming the first resolver, which today's reader ignores.
    point(&fixture, "two-second", &two, &second, 6).await?;
    point(&fixture, "four-zero", &four, ZERO_ADDRESS, 6).await?;
    link(
        &fixture,
        "link-stranger",
        &first,
        &one.node,
        "13",
        &stranger,
        6,
    )
    .await?;
    fixture.publish(7).await?;
    let report = fixture.compare(1).await?;
    // Expected difference: project_resolver_link keeps the latest ResolverRecordLinked per
    // (resolver, node) from
    // any emitter (crates/project/src/families/records.rs:167-175, :302-322), while today's
    // /links reads only the resolver's own logs
    // (apps/api/src/v2/resolvers/collections/links.sql:14-17). The stranger's link replaces
    // record 5 at the first resolver in the family and not in the served reader.
    // The difference is exactly that row: the served collection keeps record 5 at the first
    // name's node, the shadow serves the stranger's record 13 there, and every other row and the
    // total agree.
    let (height, _) = shadow::publication(fixture.pool(), CHAIN).await?;
    let served =
        shadow::served_collection(fixture.pool(), CHAIN, &first, "links", height, None, 1_000)
            .await?;
    let shadowed =
        load_resolver_links_shadow(fixture.pool(), CHAIN, &first, "ens", None, 1_000).await?;
    let records_at = |page: &FamilyCollectionPage| -> Vec<Value> {
        page.rows
            .iter()
            .filter(|(_, node, _)| node == &one.node)
            .map(|(_, _, item)| item["record_id"].clone())
            .collect()
    };
    let others = |page: &FamilyCollectionPage| -> Vec<(String, String, Value)> {
        page.rows
            .iter()
            .filter(|(_, node, _)| node != &one.node)
            .cloned()
            .collect()
    };
    ensure!(
        records_at(&served) == [json!("5")]
            && records_at(&shadowed) == [json!("13")]
            && others(&served) == others(&shadowed)
            && served.total_count == shadowed.total_count,
        "served {served:?}, shadow {shadowed:?}"
    );
    unexpected(&report, &[format!("links of {first}")])?;
    ensure!(report.bound_names == 4, "{}", report.line());
    fixture.cleanup().await
}
