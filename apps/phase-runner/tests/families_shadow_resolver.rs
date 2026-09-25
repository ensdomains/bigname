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
    FamilyCollectionPage, load_family_link_selection, load_resolver_links_shadow,
    load_resolver_shadow,
};
use serde_json::{Value, json};
use shadow_fixture::{
    CHAIN, Fixture, ZERO_ADDRESS, ZERO_NODE, address, extra_not_active, unexpected, uuid, word,
};
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
    link_of_model(
        fixture,
        identity,
        resolver,
        node,
        record_id,
        "resolver_record_id",
        emitter,
        block,
    )
    .await
}

/// A `ResolverRecordLinked` of the given storage model.
#[allow(clippy::too_many_arguments)]
async fn link_of_model(
    fixture: &Fixture,
    identity: &str,
    resolver: &str,
    node: &str,
    record_id: &str,
    storage_model: &str,
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
                   "storage_model": storage_model}),
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
    // Step 2 fills project_resolver_classification block by block, so each declared resolver is
    // read from its classification row and compared in full, not from the declaration fallback.
    let sources: Vec<(&str, &str)> = report
        .classification_sources
        .iter()
        .map(|(address, source)| (address.as_str(), *source))
        .collect();
    ensure!(
        sources == [(first.as_str(), "family"), (second.as_str(), "family")],
        "{sources:?}"
    );

    // Block 6: the second name moves to the second resolver, the fourth clears its pointer, and a
    // stranger emits a Linked naming the first resolver. Today's reader reads only the
    // resolver's own logs, and step 2's F7 reducer now takes a Linked only from the resolver it
    // names, so both keep the first resolver's own record 5 at the first name's node.
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
    unexpected(&report, &[])?;
    let own = LinkRow {
        resolver: &first,
        node: &one.node,
        name: &one.logical,
        event: ("link-one", "5"),
    };
    own.check(fixture.pool()).await?;
    // The check sees a change to that row: a moved family link fails it and the comparison.
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
        own.check(fixture.pool()).await.is_err(),
        "a changed link position passed the check"
    );
    unexpected(&fixture.compare(1).await?, &[format!("links of {first}")])?;
    ensure!(report.bound_names == 4, "{}", report.line());
    fixture.cleanup().await
}

// The classification comparison. Step 2 fills project_resolver_classification block by block,
// so both declared resolvers are read from their rows and compared in full. With the plain
// resolver's row removed the reader falls back to the declaration, a partial comparison of the
// mirror only; a rebuild of the families writes the row back and the full comparison returns.
// Changing any one field of a row, or adding a row with no served row, is a mismatch on that
// resolver's key, except a `resolver_manifest_not_active` row, step 2's declared approximation,
// which is counted by address.
#[tokio::test]
async fn classification_rows_are_compared_in_full() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_classification", 12).await?;
    let plain = address(0xd1);
    let mirror = address(0xd3);
    let registry = address(0xd4);
    let orphan = address(0xd5);
    let inactive = address(0xd6);
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
    let sources = |report: &shadow::Report| {
        (
            report.classification_sources.get(&plain).copied(),
            report.classification_sources.get(&mirror).copied(),
        )
    };
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    extra_not_active(&report, &[])?;
    ensure!(
        sources(&report) == (Some("family"), Some("family")) && report.f3_unfilled == 0,
        "{:?}",
        report.classification_sources
    );
    let row = load_resolver_shadow(fixture.pool(), CHAIN, &mirror)
        .await?
        .context("the mirror resolver is classified")?;
    ensure!(
        row.mirrored_registry_address() == Some(registry.clone()),
        "{row:?}"
    );

    // The switch: without its row the plain resolver falls back to the declaration, compared
    // for the mirror only; a rebuild writes the row back.
    sqlx::query(
        "CREATE TABLE fixture_classification AS SELECT * FROM project_resolver_classification
         WHERE chain_id = $1 AND resolver_address = $2",
    )
    .bind(CHAIN)
    .bind(&plain)
    .execute(fixture.pool())
    .await?;
    let restore = "DELETE FROM project_resolver_classification
                   WHERE chain_id = $1 AND resolver_address = $2";
    sqlx::query(restore)
        .bind(CHAIN)
        .bind(&plain)
        .execute(fixture.pool())
        .await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(
        sources(&report) == (Some("declaration"), Some("family"))
            && report.f3_unfilled == 1
            && report.f3_unfilled_mirror_differs == 0,
        "{}: {:?}",
        report.line(),
        report.classification_sources
    );
    fixture.rebuild().await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(
        sources(&report) == (Some("family"), Some("family")),
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
        sqlx::query(&format!(
            "UPDATE project_resolver_classification SET {change}
             WHERE chain_id = $1 AND resolver_address = $2"
        ))
        .bind(CHAIN)
        .bind(&plain)
        .execute(fixture.pool())
        .await?;
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
        sqlx::query(restore)
            .bind(CHAIN)
            .bind(&plain)
            .execute(fixture.pool())
            .await?;
        sqlx::query(
            "INSERT INTO project_resolver_classification SELECT * FROM fixture_classification",
        )
        .execute(fixture.pool())
        .await?;
    }
    unexpected(&fixture.compare(1).await?, &[])?;

    // A row with no served row: counted when it reads resolver_manifest_not_active, a mismatch
    // otherwise.
    for (resolver, reason) in [
        (&inactive, Some("resolver_manifest_not_active")),
        (&orphan, None),
    ] {
        sqlx::query(
            "INSERT INTO project_resolver_classification (chain_id, resolver_address,
                 block_number, event_identity, support_status, unsupported_reason)
             VALUES ($1, $2, 1, 'fixture:extra', 'unsupported', $3)",
        )
        .bind(CHAIN)
        .bind(resolver)
        .bind(reason)
        .execute(fixture.pool())
        .await?;
    }
    let report = fixture.compare(1).await?;
    unexpected(&report, &[format!("resolver {orphan}")])?;
    extra_not_active(&report, &[&inactive])?;
    fixture.cleanup().await
}

// A later pointer on a resource that is not the name's selected one does not move the name:
// today's name row takes its resolver from the selected authority's events
// (crates/project/src/builders/name_current/build.sql, the `resolver` lateral), so `one.eth`
// stays bound to the first resolver and is not bound to the second.
#[tokio::test]
async fn bound_names_follow_the_selected_resource_not_the_newest_pointer() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_bound_selected", 12).await?;
    let first = address(0xd1);
    let second = address(0xd2);
    fixture
        .declare_resolvers(RESOLVER, &[&first, &second])
        .await?;
    let one = name(&fixture, 1, "one", "declared_registry_path").await?;
    point(&fixture, "one-first", &one, &first, 2).await?;
    let other = uuid(0xa101);
    fixture.resource(&other, 3).await?;
    fixture
        .event(
            "one-other-resource-second",
            Some(&one.logical),
            Some(&other),
            REGISTRY,
            "ResolverChanged",
            3,
            json!({"resolver": second}),
            &address(0xf1),
        )
        .await?;
    fixture.publish(4).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(report.bound_names == 1, "{}", report.line());
    fixture.cleanup().await
}

// The declaration fallback reads a manifest's latest update before asking whether it is active,
// as today's manifest staging does (crates/project/src/stage.rs, `create_manifests`): a manifest
// whose newest update retires it declares nothing, even though an older update was active.
#[tokio::test]
async fn the_declaration_fallback_ignores_a_retired_manifest() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_retired_manifest", 12).await?;
    let retired = address(0xd7);
    fixture.declare_resolvers(RESOLVER, &[&retired]).await?;
    fixture.publish(2).await?;
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, derivation_kind,
             canonicality_state, after_state)
         SELECT 'fixture:manifest-retired:' || manifest_id, 'ens', 'SourceManifestUpdated',
                source_family, 1, manifest_id, chain_id, 'manifest_sync',
                'canonical'::canonicality_state,
                jsonb_build_object('rollout_status', 'deprecated', 'normalizer_version',
                    'fixture', 'manifest_payload', manifest_payload)
         FROM manifest_versions WHERE chain_id = $1 AND source_family = $2",
    )
    .bind(CHAIN)
    .bind(RESOLVER)
    .execute(fixture.pool())
    .await?;
    sqlx::query(
        "DELETE FROM project_resolver_classification
         WHERE chain_id = $1 AND resolver_address = $2",
    )
    .bind(CHAIN)
    .bind(&retired)
    .execute(fixture.pool())
    .await?;
    let fallback = load_resolver_shadow(fixture.pool(), CHAIN, &retired).await?;
    ensure!(fallback.is_none(), "{fallback:?}");
    fixture.cleanup().await
}

// A grant whose resource stops being readable leaves `/roles`: today's statement applies the
// permissions read filter, whose resource and lineage predicates drop it, and the shadow must too.
#[tokio::test]
async fn roles_leave_out_a_grant_whose_resource_is_not_readable() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_roles_readable", 12).await?;
    let first = address(0xd1);
    fixture.declare_resolvers(RESOLVER, &[&first]).await?;
    for (identity, subject, resource) in [("grant-a", 0xee1, 1), ("grant-b", 0xee2, 2)] {
        grant(
            &fixture,
            identity,
            &first,
            &address(subject),
            resource,
            json!(["set_text"]),
            2,
        )
        .await?;
    }
    fixture.publish(4).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(report.roles == 2, "{}", report.line());
    sqlx::query(
        "UPDATE resources SET canonicality_state = 'orphaned' WHERE resource_id = $1::uuid",
    )
    .bind(uuid(0xd000 + 2))
    .execute(fixture.pool())
    .await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(report.roles == 1, "{}", report.line());
    fixture.cleanup().await
}

// A record-ID link followed, at the same resolver and node, by a link of another storage model.
// Today's `/links` keeps only record-ID links, so it still serves record 5 there. The newest link
// per (resolver, node) wins whatever its storage model (Tate, 2026-09-26; the F7 design, "latest
// link per (resolver, node)"), and the F7 reducer keeps that one row
// (crates/project/src/families/records.rs, `link`). The later link is no record-ID link, so the
// shadow serves no link at that node, and the name's link selection reads that link and falls
// back to the default record, never to record 5. Expected difference by that ruling, until the
// served read switches to these readers.
#[tokio::test]
async fn a_link_of_another_storage_model_hides_the_record_id_link() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_link_model", 12).await?;
    let first = address(0xd1);
    fixture.declare_resolvers(RESOLVER, &[&first]).await?;
    let one = name(&fixture, 1, "one", "declared_registry_path").await?;
    let two = name(&fixture, 2, "two", "declared_registry_path").await?;
    point(&fixture, "one-first", &one, &first, 2).await?;
    point(&fixture, "two-first", &two, &first, 2).await?;
    link(&fixture, "link-one", &first, &one.node, "5", &first, 2).await?;
    link(&fixture, "link-two", &first, &two.node, "6", &first, 2).await?;
    link(&fixture, "link-default", &first, ZERO_NODE, "7", &first, 2).await?;
    fixture.publish(4).await?;
    unexpected(&fixture.compare(1).await?, &[])?;

    link_of_model(
        &fixture,
        "link-one-other-model",
        &first,
        &one.node,
        "9",
        "resolver_node",
        &first,
        5,
    )
    .await?;
    fixture.publish(6).await?;
    let report = fixture.compare(1).await?;
    let hidden = HiddenLink {
        resolver: &first,
        node: &one.node,
        name: &one.logical,
        served: ("link-one", "5"),
    };
    hidden.check(fixture.pool()).await?;
    let selection = load_family_link_selection(fixture.pool(), CHAIN, &first, &one.node)
        .await?
        .context("the default link remains")?;
    ensure!(
        selection
            .exact
            .as_ref()
            .is_some_and(|link| link.record_id == "9"
                && link.storage_model.as_deref() == Some("resolver_node"))
            && selection.selected().map(|link| link.record_id.as_str()) == Some("7"),
        "{selection:?}"
    );
    // The check sees a change: with the family row turned back into a record-ID link, the
    // shadow serves the other model's record 9 at that node and the check fails.
    let set_model = "UPDATE project_resolver_link SET storage_model = $4
                     WHERE chain_id = $1 AND resolver_address = $2 AND node = $3";
    sqlx::query(set_model)
        .bind(CHAIN)
        .bind(&first)
        .bind(&one.node)
        .bind("resolver_record_id")
        .execute(fixture.pool())
        .await?;
    ensure!(
        hidden.check(fixture.pool()).await.is_err(),
        "a record-ID family row passed the check"
    );
    sqlx::query(set_model)
        .bind(CHAIN)
        .bind(&first)
        .bind(&one.node)
        .bind("resolver_node")
        .execute(fixture.pool())
        .await?;
    unexpected(&report, &[format!("links of {first}")])?;
    fixture.cleanup().await
}

/// A `/links` row today's reader serves and the shadow does not: at `node` the served collection
/// serves exactly the row the link event `served` makes, the shadow serves nothing, every other
/// row agrees and the served total is one higher.
struct HiddenLink<'a> {
    resolver: &'a str,
    node: &'a str,
    name: &'a str,
    served: (&'a str, &'a str),
}

impl HiddenLink<'_> {
    async fn check(&self, pool: &PgPool) -> Result<()> {
        let (height, _) = shadow::publication(pool, CHAIN).await?;
        let served =
            shadow::served_collection(pool, CHAIN, self.resolver, "links", height, None, 1_000)
                .await?;
        let shadowed =
            load_resolver_links_shadow(pool, CHAIN, self.resolver, "ens", None, 1_000).await?;
        let expected = LinkRow {
            resolver: self.resolver,
            node: self.node,
            name: self.name,
            event: self.served,
        }
        .row(pool, self.served.0, self.served.1)
        .await?;
        let split = |page: &FamilyCollectionPage| -> (Vec<_>, Vec<_>) {
            page.rows
                .iter()
                .cloned()
                .partition(|(_, node, _)| node == self.node)
        };
        let (served_at, served_rest) = split(&served);
        let (shadow_at, shadow_rest) = split(&shadowed);
        ensure!(
            served_at == [expected.clone()],
            "served {served_at:?}, expected {expected:?}"
        );
        ensure!(shadow_at.is_empty(), "shadow {shadow_at:?}");
        ensure!(
            served_rest == shadow_rest && served.total_count == shadowed.total_count + 1,
            "served {served:?}, shadow {shadowed:?}"
        );
        Ok(())
    }
}

/// The `/links` row at one node, which both readers must serve exactly: the row the link event
/// `event`, as `(event identity, record id)`, makes.
struct LinkRow<'a> {
    resolver: &'a str,
    node: &'a str,
    name: &'a str,
    event: (&'a str, &'a str),
}

impl LinkRow<'_> {
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
        let expected = self.row(pool, self.event.0, self.event.1).await?;
        ensure!(
            at_node(&served) == [expected.clone()],
            "served {:?}, expected {expected:?}",
            at_node(&served)
        );
        ensure!(
            at_node(&shadowed) == [expected.clone()],
            "shadow {:?}, expected {expected:?}",
            at_node(&shadowed)
        );
        ensure!(
            others(&served) == others(&shadowed) && served.total_count == shadowed.total_count,
            "served {served:?}, shadow {shadowed:?}"
        );
        Ok(())
    }
}
