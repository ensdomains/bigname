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

use anyhow::{Context, Result, ensure};
use bigname_storage::families::topology::{
    FamilyCollectionPage, load_resolver_links_shadow, load_resolver_shadow,
};
use serde_json::{Value, json};
use shadow_fixture::{CHAIN, Fixture, ZERO_ADDRESS, ZERO_NODE, address, unexpected, uuid, word};
use sqlx::PgPool;

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
    // project_resolver_classification is not filled yet, so each declared resolver is classified
    // from its declaration, a partial comparison of the mirror only. A classification row for
    // either one turns this red; the row is then compared in full and this assertion changes.
    let sources: Vec<(&str, &str)> = report
        .classification_sources
        .iter()
        .map(|(address, source)| (address.as_str(), *source))
        .collect();
    ensure!(
        sources
            == [
                (first.as_str(), "declaration"),
                (second.as_str(), "declaration")
            ],
        "{sources:?}"
    );

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
    // The difference is exactly that row, pinned in full: the served collection keeps the first
    // resolver's own link of record 5 at the first name's node, the shadow serves the stranger's
    // record 13 there, each row whole with its ordering keys, and every other row and the total
    // agree.
    let pin = LinkPin {
        resolver: &first,
        node: &one.node,
        name: &one.logical,
        served: ("link-one", "5"),
        shadow: ("link-stranger", "13"),
    };
    pin.check(fixture.pool()).await?;
    // A second difference on the exempted row still fails: a changed position of the family's
    // link leaves the known difference in place but breaks the pin.
    sqlx::query(
        "UPDATE project_resolver_link SET log_index = log_index + 50
         WHERE chain_id = $1 AND resolver_address = $2 AND node = $3",
    )
    .bind(CHAIN)
    .bind(&first)
    .bind(&one.node)
    .execute(fixture.pool())
    .await?;
    ensure!(
        pin.check(fixture.pool()).await.is_err(),
        "a changed link position passed the pin"
    );
    unexpected(&fixture.compare(1).await?, &[format!("links of {first}")])?;
    unexpected(&report, &[format!("links of {first}")])?;
    ensure!(report.bound_names == 4, "{}", report.line());
    fixture.cleanup().await
}

// The classification comparison: a declared ENSv1 mirror resolver's fallback serves the same
// non-null mirror as the overview; a classification row copied from the served row compares
// equal in full; changing any one of its fields, or adding a row for a resolver with no served
// row, is a mismatch on that resolver's key.
#[tokio::test]
async fn classification_rows_are_compared_in_full() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_classification", 12).await?;
    let plain = address(0xd1);
    let mirror = address(0xd3);
    let registry = address(0xd4);
    let orphan = address(0xd5);
    fixture
        .declare_contracts(
            RESOLVER,
            &[(&plain, "resolver"), (&mirror, "ensv1_mirror_resolver")],
            Some(&registry),
        )
        .await?;
    let one = name(&fixture, 1, "one", "declared_registry_path").await?;
    let two = name(&fixture, 2, "two", "declared_registry_path").await?;
    point(&fixture, "one-plain", &one, &plain, 2).await?;
    point(&fixture, "two-mirror", &two, &mirror, 2).await?;
    fixture.publish(4).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(
        report.classification_sources.get(&plain) == Some(&"declaration")
            && report.classification_sources.get(&mirror) == Some(&"declaration"),
        "{:?}",
        report.classification_sources
    );
    let fallback = load_resolver_shadow(fixture.pool(), CHAIN, &mirror)
        .await?
        .context("the mirror resolver is declared")?;
    ensure!(
        fallback.mirrored_registry_address() == Some(registry.clone()),
        "{fallback:?}"
    );

    // Seed the classification row from the served row: equal in full.
    sqlx::query(
        "INSERT INTO project_resolver_classification (chain_id, resolver_address, block_number,
             event_identity, classification, support_status, unsupported_reason, manifest_id,
             manifest_event_id, admission_namespace, summary_version)
         SELECT chain_id, lower(resolver_address), 1, 'fixture:classification',
                declared_summary -> 'classification', support_status, unsupported_reason,
                (provenance ->> 'manifest_id')::bigint,
                (provenance ->> 'manifest_event_id')::bigint,
                provenance ->> 'classification_admission_namespace',
                declared_summary ->> 'summary_version'
         FROM resolver_current WHERE chain_id = $1 AND lower(resolver_address) = $2",
    )
    .bind(CHAIN)
    .bind(&plain)
    .execute(fixture.pool())
    .await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(
        report.classification_sources.get(&plain) == Some(&"family"),
        "{:?}",
        report.classification_sources
    );

    // Each field on its own is compared.
    for change in [
        "classification = classification || '{\"role\": \"other\"}'",
        "support_status = CASE WHEN support_status = 'supported' THEN 'unsupported'
                          ELSE 'supported' END",
        "unsupported_reason = 'fixture_reason'",
        "manifest_id = COALESCE(manifest_id, 0) + 1000",
        "manifest_event_id = COALESCE(manifest_event_id, 0) + 1000",
        "admission_namespace = 'basenames'",
        "summary_version = 'fixture'",
    ] {
        let mut transaction = fixture.pool().begin().await?;
        sqlx::query(&format!(
            "UPDATE project_resolver_classification SET {change}
             WHERE chain_id = $1 AND resolver_address = $2"
        ))
        .bind(CHAIN)
        .bind(&plain)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        let report = fixture.compare(1).await?;
        let keys: Vec<&str> = report
            .mismatches
            .iter()
            .map(|mismatch| mismatch.key.as_str())
            .collect();
        ensure!(
            keys == [format!("resolver {plain}").as_str()],
            "{change}: {keys:?}"
        );
        sqlx::query(
            "DELETE FROM project_resolver_classification
             WHERE chain_id = $1 AND resolver_address = $2",
        )
        .bind(CHAIN)
        .bind(&plain)
        .execute(fixture.pool())
        .await?;
        sqlx::query(
            "INSERT INTO project_resolver_classification (chain_id, resolver_address,
                 block_number, event_identity, classification, support_status,
                 unsupported_reason, manifest_id, manifest_event_id, admission_namespace,
                 summary_version)
             SELECT chain_id, lower(resolver_address), 1, 'fixture:classification',
                    declared_summary -> 'classification', support_status, unsupported_reason,
                    (provenance ->> 'manifest_id')::bigint,
                    (provenance ->> 'manifest_event_id')::bigint,
                    provenance ->> 'classification_admission_namespace',
                    declared_summary ->> 'summary_version'
             FROM resolver_current WHERE chain_id = $1 AND lower(resolver_address) = $2",
        )
        .bind(CHAIN)
        .bind(&plain)
        .execute(fixture.pool())
        .await?;
    }

    // A classification row for a resolver with no served row is examined and fails.
    sqlx::query(
        "INSERT INTO project_resolver_classification (chain_id, resolver_address, block_number,
             event_identity, support_status)
         VALUES ($1, $2, 1, 'fixture:orphan', 'supported')",
    )
    .bind(CHAIN)
    .bind(&orphan)
    .execute(fixture.pool())
    .await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[format!("resolver {orphan}")])?;
    fixture.cleanup().await
}

/// The `/links` difference at one node: the served collection serves the link event `served`
/// and the shadow the link event `shadow`, each as `(event identity, record id)`.
struct LinkPin<'a> {
    resolver: &'a str,
    node: &'a str,
    name: &'a str,
    served: (&'a str, &'a str),
    shadow: (&'a str, &'a str),
}

impl LinkPin<'_> {
    /// The whole collection row a link event makes at this node: its ordering keys and its item,
    /// built from the interpreted event itself.
    async fn row(
        &self,
        pool: &PgPool,
        identity: &str,
        record: &str,
    ) -> Result<(String, String, Value)> {
        let item: Value = sqlx::query_scalar(
            "SELECT jsonb_build_object(
                 'record_id', $2::text, 'namehash', $3::text, 'default', false,
                 'logical_name_id', $4::text, 'name', surface.raw_name, 'namespace', 'ens',
                 'normalized_event_id', event.normalized_event_id,
                 'chain_position', jsonb_build_object(
                     'chain_id', event.chain_id, 'block_number', event.block_number,
                     'block_hash', event.block_hash, 'transaction_hash', event.transaction_hash,
                     'log_index', event.log_index,
                     'timestamp', to_char(lineage.block_timestamp AT TIME ZONE 'UTC',
                                          'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')))
             FROM normalized_events event
             JOIN chain_lineage lineage
               ON lineage.chain_id = event.chain_id AND lineage.block_hash = event.block_hash
             JOIN name_surfaces surface ON surface.logical_name_id = $4
             WHERE event.event_identity = $1",
        )
        .bind(identity)
        .bind(record)
        .bind(self.node)
        .bind(self.name)
        .fetch_one(pool)
        .await?;
        Ok((format!("{record:0>78}"), self.node.to_owned(), item))
    }

    async fn check(&self, pool: &PgPool) -> Result<()> {
        let (height, _) = shadow::publication(pool, CHAIN).await?;
        let served =
            shadow::served_collection(pool, CHAIN, self.resolver, "links", height, None, 1_000)
                .await?;
        let shadowed =
            load_resolver_links_shadow(pool, CHAIN, self.resolver, "ens", None, 1_000).await?;
        let at_node = |page: &FamilyCollectionPage| -> Vec<(String, String, Value)> {
            page.rows
                .iter()
                .filter(|(_, node, _)| node == self.node)
                .cloned()
                .collect()
        };
        let others = |page: &FamilyCollectionPage| -> Vec<(String, String, Value)> {
            page.rows
                .iter()
                .filter(|(_, node, _)| node != self.node)
                .cloned()
                .collect()
        };
        let expected_served = self.row(pool, self.served.0, self.served.1).await?;
        let expected_shadow = self.row(pool, self.shadow.0, self.shadow.1).await?;
        ensure!(
            at_node(&served) == [expected_served.clone()],
            "served {:?}, expected {expected_served:?}",
            at_node(&served)
        );
        ensure!(
            at_node(&shadowed) == [expected_shadow.clone()],
            "shadow {:?}, expected {expected_shadow:?}",
            at_node(&shadowed)
        );
        ensure!(
            others(&served) == others(&shadowed) && served.total_count == shadowed.total_count,
            "served {served:?}, shadow {shadowed:?}"
        );
        Ok(())
    }
}
