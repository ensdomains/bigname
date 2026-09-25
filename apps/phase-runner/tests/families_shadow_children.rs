//! TYR-36 step 5: the child-edge shadow reader (F11 with F2a, F2b and F2c) against
//! `children_current` at one publication, over small pages and every sort and expiry filter.
//! Each case seeds interpreted events, publishes through the runner's Project phase with the
//! families on, and runs the shadow comparison of `project_end_to_end/shadow.rs`.
#[allow(dead_code)]
#[path = "project_end_to_end/shadow.rs"]
mod shadow;
#[allow(dead_code)]
#[path = "project_end_to_end/shadow_fixture.rs"]
mod shadow_fixture;
#[allow(dead_code)]
mod support;

use anyhow::{Context, Result, ensure};
use bigname_storage::{
    ChildrenCurrentOrder, ChildrenCurrentPageFilter, ChildrenCurrentSort,
    families::topology::{FamilyChildRow, load_children_shadow_page},
    load_children_current_page_filtered,
};
use serde_json::json;
use shadow_fixture::{CHAIN, Fixture, ZERO_ADDRESS, address, unexpected, uuid, word};
use sqlx::types::time::OffsetDateTime;

const V1_REGISTRY: &str = "ens_v1_registry_l1";
const V2_REGISTRY: &str = "ens_v2_registry_l1";
const ETH: u64 = 0xe7;

fn owner(n: u64) -> String {
    address(0xa000 + n)
}

/// A second-level `.eth` parent: namehash `0x1<n>` and labelhash `0x2<n>`.
async fn parent(fixture: &Fixture, n: u64, label: &str) -> Result<(String, String, Vec<String>)> {
    let node = word(0x1000 + n);
    let labels = vec![word(0x2000 + n), word(ETH)];
    let logical = fixture
        .surface("ens", &node, &format!("{label}.eth"), &labels, 1)
        .await?;
    Ok((logical, node, labels))
}

/// A NewOwner edge under `parent_node` for child `n`.
#[allow(clippy::too_many_arguments)]
async fn edge(
    fixture: &Fixture,
    identity: &str,
    parent_node: &str,
    child: u64,
    edge_owner: &str,
    block: i64,
) -> Result<()> {
    fixture
        .event(
            identity,
            None,
            None,
            V1_REGISTRY,
            "SubregistryChanged",
            block,
            json!({"source_event": "NewOwner", "node": parent_node, "child_node": word(child),
                   "labelhash": word(0x5000 + child), "owner": edge_owner}),
            &address(0xe1),
        )
        .await?;
    Ok(())
}

/// An ENSv1 registry Transfer of `node` to `new_owner`, attributed to no name or resource.
async fn transfer(
    fixture: &Fixture,
    identity: &str,
    child: u64,
    new_owner: &str,
    block: i64,
) -> Result<()> {
    attributed_transfer(fixture, identity, child, new_owner, block, None, None).await
}

/// An ENSv1 registry Transfer of `node` to `new_owner`, carrying the name and resource the adapter
/// attributed it to.
async fn attributed_transfer(
    fixture: &Fixture,
    identity: &str,
    child: u64,
    new_owner: &str,
    block: i64,
    logical: Option<&str>,
    resource: Option<&str>,
) -> Result<()> {
    fixture
        .event(
            identity,
            logical,
            resource,
            V1_REGISTRY,
            "AuthorityTransferred",
            block,
            json!({"source_event": "Transfer", "node": word(child), "owner": new_owner,
                   "owner_getter": new_owner, "emitter_role": "registry"}),
            &address(0xe1),
        )
        .await?;
    Ok(())
}

// ENSv1 edges: a live child, one whose edge moved to another parent, one zeroed by its edge, one
// with a name surface zeroed by a later Transfer, one without a surface transferred to zero (today
// keys the override by name, so it stays listed), one with a later non-zero Transfer (the edge's
// owner is still served), a label that fails normalization, a child with no label at all, and
// eight children for sort, count and page agreement at page size 2. A migrated `unwrapped` parent
// hides its edges.
#[tokio::test]
async fn ens_v1_edges_match_the_served_children() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_children_v1", 12).await?;
    let (first, first_node, _) = parent(&fixture, 1, "first").await?;
    let (second, second_node, _) = parent(&fixture, 2, "second").await?;
    let (migrated, migrated_node, _) = parent(&fixture, 3, "migrated").await?;
    for (child, label, normalized) in [
        (1, "live", true),
        (2, "moved", true),
        (3, "zeroed", true),
        (4, "transferred", true),
        (5, "kept", true),
        (6, "Bad Label", false),
        (8, "surfaced", true),
    ] {
        fixture
            .label(&word(0x5000 + child), label, normalized)
            .await?;
    }
    for child in 10..18 {
        fixture
            .label(&word(0x5000 + child), &format!("many{child}"), true)
            .await?;
    }
    edge(&fixture, "live", &first_node, 1, &owner(1), 2).await?;
    edge(&fixture, "moved-a", &first_node, 2, &owner(2), 2).await?;
    edge(&fixture, "zeroed-a", &first_node, 3, &owner(3), 2).await?;
    edge(&fixture, "transferred", &first_node, 4, &owner(4), 2).await?;
    edge(&fixture, "kept", &first_node, 5, &owner(5), 2).await?;
    edge(&fixture, "bad-label", &first_node, 6, &owner(6), 2).await?;
    edge(&fixture, "unlabelled", &first_node, 7, &owner(7), 2).await?;
    fixture
        .surface(
            "ens",
            &word(8),
            "surfaced.first.eth",
            &[word(0x5008), word(0x2001), word(ETH)],
            1,
        )
        .await?;
    edge(&fixture, "surfaced", &first_node, 8, &owner(8), 2).await?;
    for child in 10..18 {
        edge(
            &fixture,
            &format!("many-{child}"),
            &second_node,
            child,
            &owner(child),
            3 + i64::try_from(child % 3)?,
        )
        .await?;
    }
    // Exact-position duplicates: the same child edge twice at one position; the greater event
    // identity wins, whatever the generated ids (the lower id is the winner here).
    fixture.label(&word(0x5000 + 30), "duplicate", true).await?;
    for (identity, edge_owner) in [("dup-z", owner(31)), ("dup-a", owner(30))] {
        fixture
            .event_at(
                identity,
                None,
                None,
                V1_REGISTRY,
                "SubregistryChanged",
                3,
                0,
                900,
                json!({"source_event": "NewOwner", "node": second_node, "child_node": word(30),
                       "labelhash": word(0x5000 + 30), "owner": edge_owner}),
                &address(0xe1),
            )
            .await?;
    }
    edge(
        &fixture,
        "migrated-child",
        &migrated_node,
        20,
        &owner(20),
        2,
    )
    .await?;
    fixture
        .event(
            "migrated-parent",
            Some(&migrated),
            None,
            "ens_v2_migration_l1",
            "MigrationApplied",
            3,
            json!({"migration_path": "unwrapped", "evidence": []}),
            &address(0xe2),
        )
        .await?;
    fixture.publish(6).await?;
    let report = fixture.compare(2).await?;
    unexpected(&report, &[])?;
    ensure!(
        shadow::shadow_children(fixture.pool(), &first).await?.len() == 8,
        "{:#}",
        shadow::describe(&report)
    );
    let duplicate: Option<String> =
        sqlx::query_scalar("SELECT owner FROM children_current WHERE child_logical_name_id = $1")
            .bind(format!("ens:{}", word(30)))
            .fetch_one(fixture.pool())
            .await?;
    ensure!(duplicate == Some(owner(31)), "{duplicate:?}");
    // A page turn across a publication: the cursor of the second parent's first page at block 6
    // is continued at block 9 by both readers.
    let turn = load_children_current_page_filtered(
        fixture.pool(),
        &second,
        &ChildrenCurrentPageFilter::default(),
        None,
        2,
    )
    .await?
    .next_cursor
    .context("the second parent has more than one page")?;

    // Block 8: child 2 moves to the second parent, child 3's edge zeroes, child 4 is transferred
    // to the zero address, child 5 to another owner.
    edge(&fixture, "moved-b", &second_node, 2, &owner(22), 8).await?;
    edge(&fixture, "zeroed-b", &first_node, 3, ZERO_ADDRESS, 8).await?;
    transfer(&fixture, "transferred-zero", 4, ZERO_ADDRESS, 8).await?;
    transfer(&fixture, "kept-transfer", 5, &owner(55), 8).await?;
    transfer(&fixture, "surfaced-zero", 8, ZERO_ADDRESS, 8).await?;
    fixture.publish(9).await?;
    // The zero Transfer at block 8 removes the surfaced child from both readers. The served
    // incremental batch used to keep it until a rebuild, because the children scope took only an
    // event's `child_node`; main's #958 restages transferred child edges, so the served page now
    // drops it at once and matches the shadow under every filter.
    let report = fixture.compare(2).await?;
    unexpected(&report, &[])?;
    let surfaced = format!("ens:{}", word(8));
    let (_, clock) = shadow::publication(fixture.pool(), CHAIN).await?;
    for (index, filter) in shadow::child_filters(clock, true, &[]).iter().enumerate() {
        let (served_total, served, shadow_total, shadowed) =
            shadow::walk_children(fixture.pool(), &first, filter, 2).await?;
        ensure!(
            served == shadowed
                && served_total == shadow_total
                && !served
                    .iter()
                    .any(|row| row.child_logical_name_id == surfaced),
            "filter {index}: served {served_total} {served:?}, shadow {shadow_total} {shadowed:?}"
        );
    }
    for filter in [
        ChildrenCurrentPageFilter::default(),
        ChildrenCurrentPageFilter {
            include_expired: false,
            evaluated_at: Some(OffsetDateTime::from_unix_timestamp(
                shadow_fixture::EPOCH + 9,
            )?),
            ..ChildrenCurrentPageFilter::default()
        },
    ] {
        let served =
            load_children_current_page_filtered(fixture.pool(), &second, &filter, Some(&turn), 2)
                .await?;
        let shadowed =
            load_children_shadow_page(fixture.pool(), &second, &filter, Some(&turn), 2).await?;
        ensure!(
            served.total_count == shadowed.total_count
                && served.next_cursor == shadowed.next_cursor
                && served
                    .rows
                    .iter()
                    .map(|row| &row.child_logical_name_id)
                    .eq(shadowed.rows.iter().map(|row| &row.child_logical_name_id)),
            "page turn: served {served:?}, shadow {shadowed:?}"
        );
    }
    fixture.rebuild().await?;
    let report = fixture.compare(2).await?;
    unexpected(&report, &[])?;
    let first_children = shadow::shadow_children(fixture.pool(), &first).await?;
    ensure!(
        first_children.len() == 5
            && !first_children.contains(&format!("ens:{}", word(2)))
            && !first_children.contains(&format!("ens:{}", word(8)))
            && first_children.contains(&format!("ens:{}", word(4)))
            && shadow::shadow_children(fixture.pool(), &second)
                .await?
                .contains(&format!("ens:{}", word(2))),
        "{first_children:?}"
    );
    ensure!(
        shadow::shadow_children(fixture.pool(), &migrated)
            .await?
            .is_empty()
    );
    ensure!(
        report.child_rows >= 14 && report.parents >= 3,
        "{}",
        report.line()
    );
    let _ = CHAIN;
    fixture.cleanup().await
}

/// An ENSv2 parent's subregistry and its registrations.
async fn registration(
    fixture: &Fixture,
    identity: &str,
    logical: &str,
    kind: &str,
    registry: &str,
    registrant: &str,
    block: i64,
) -> Result<()> {
    let status = match kind {
        "RegistrationReleased" => "released",
        "RegistrationReserved" => "reserved",
        _ => "registered",
    };
    fixture
        .event(
            identity,
            Some(logical),
            None,
            V2_REGISTRY,
            kind,
            block,
            json!({"registry_contract_instance_id": registry, "status": status,
                   "registrant": registrant, "expiry": 4_000_000_000_i64}),
            &address(0xe3),
        )
        .await?;
    Ok(())
}

// ENSv2: a granted child, a reserved-only child, a child registered in two registries, a released
// child, and a parent whose subregistry changes with no child event.
#[tokio::test]
async fn ens_v2_subregistry_children_match_the_served_children() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_children_v2", 12).await?;
    let (parent_id, _, parent_labels) = parent(&fixture, 1, "vtwo").await?;
    let first_registry = uuid(0xf1);
    let second_registry = uuid(0xf2);
    fixture.contract(&first_registry, &address(0xf1), 1).await?;
    fixture
        .contract(&second_registry, &address(0xf2), 1)
        .await?;
    let mut children = Vec::new();
    for (n, label) in [
        (1, "granted"),
        (2, "reserved"),
        (3, "both"),
        (4, "released"),
        (5, "second"),
    ] {
        let labelhash = word(0x6000 + n);
        fixture.label(&labelhash, label, true).await?;
        let mut labels = vec![labelhash];
        labels.extend(parent_labels.iter().cloned());
        let logical = fixture
            .surface(
                "ens",
                &word(0x7000 + n),
                &format!("{label}.vtwo.eth"),
                &labels,
                1,
            )
            .await?;
        children.push(logical);
    }
    fixture
        .event(
            "subregistry-first",
            Some(&parent_id),
            None,
            V2_REGISTRY,
            "SubregistryChanged",
            2,
            json!({"subregistry": address(0xf1)}),
            &address(0xe3),
        )
        .await?;
    registration(
        &fixture,
        "granted",
        &children[0],
        "RegistrationGranted",
        &first_registry,
        &owner(1),
        2,
    )
    .await?;
    registration(
        &fixture,
        "reserved",
        &children[1],
        "RegistrationReserved",
        &first_registry,
        &owner(2),
        2,
    )
    .await?;
    registration(
        &fixture,
        "both-first",
        &children[2],
        "RegistrationGranted",
        &first_registry,
        &owner(3),
        2,
    )
    .await?;
    registration(
        &fixture,
        "both-second",
        &children[2],
        "RegistrationGranted",
        &second_registry,
        &owner(33),
        3,
    )
    .await?;
    registration(
        &fixture,
        "released-grant",
        &children[3],
        "RegistrationGranted",
        &first_registry,
        &owner(4),
        2,
    )
    .await?;
    registration(
        &fixture,
        "released",
        &children[3],
        "RegistrationReleased",
        &first_registry,
        &owner(4),
        3,
    )
    .await?;
    registration(
        &fixture,
        "second",
        &children[4],
        "RegistrationGranted",
        &second_registry,
        &owner(5),
        3,
    )
    .await?;
    fixture.publish(4).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    let served_first = shadow::shadow_children(fixture.pool(), &parent_id).await?;
    ensure!(
        served_first.contains(&children[0]) && served_first.contains(&children[2]),
        "{served_first:?}"
    );

    // The parent's subregistry moves to the second registry with no child event.
    fixture
        .event(
            "subregistry-second",
            Some(&parent_id),
            None,
            V2_REGISTRY,
            "SubregistryChanged",
            6,
            json!({"subregistry": address(0xf2)}),
            &address(0xe3),
        )
        .await?;
    fixture.publish(7).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    let served_second = shadow::shadow_children(fixture.pool(), &parent_id).await?;
    ensure!(
        served_second.contains(&children[2])
            && served_second.contains(&children[4])
            && !served_second.contains(&children[0]),
        "{served_second:?}"
    );

    // Cleared: no children.
    fixture
        .event(
            "subregistry-clear",
            Some(&parent_id),
            None,
            V2_REGISTRY,
            "SubregistryChanged",
            9,
            json!({"subregistry": null}),
            &address(0xe3),
        )
        .await?;
    fixture.publish(10).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(
        shadow::shadow_children(fixture.pool(), &parent_id)
            .await?
            .is_empty()
    );
    fixture.cleanup().await
}

/// The parent's migration registry, announced and associated the way the served gate requires
/// (children.rs:93-135): a manifest, a registry announcement edge and the association row whose
/// evidence the migration event carries.
/// Which migration evidence a locked parent's registry gets: all of it, no registry
/// announcement edge, or a migration whose evidence does not contain the association's.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Evidence {
    Valid,
    NoAnnouncement,
    UnmatchedMigration,
}

async fn migration_registry(
    fixture: &Fixture,
    parent_id: &str,
    registry: &str,
    registry_address: &str,
    block: i64,
    evidence: Evidence,
) -> Result<()> {
    let pool = fixture.pool();
    fixture.contract(registry, registry_address, block).await?;
    let manifest_id: i64 = sqlx::query_scalar(
        "INSERT INTO manifest_versions (manifest_version, namespace, source_family, chain_id,
             deployment_label, rollout_status, normalizer_version, file_path, manifest_payload)
         VALUES (12, 'ens', 'ens_v2_registry_l1', $1, 'fixture', 'active', 'fixture',
             'fixture-migration.toml', '{}')
         RETURNING manifest_id",
    )
    .bind(CHAIN)
    .fetch_one(pool)
    .await?;
    if evidence != Evidence::NoAnnouncement {
        sqlx::query(
            "INSERT INTO discovery_edges (chain_id, edge_kind, from_contract_instance_id,
                 to_contract_instance_id, discovery_source, admission_basis, source_manifest_id,
                 active_from_block_number, active_from_block_hash, canonicality_state, provenance)
             VALUES ($1, 'registry_announcement', $2::uuid, $2::uuid, 'fixture', 'fixture', $3,
                 $4, $5, 'canonical', '{\"transaction_index\":0,\"log_index\":0}')",
        )
        .bind(CHAIN)
        .bind(registry)
        .bind(manifest_id)
        .bind(block)
        .bind(shadow_fixture::hash(block))
        .execute(pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO migration_discovery_associations (logical_edge_identity,
             migration_correlation_id, correlation_kind, registry_contract_instance_id,
             registry_address, source_manifest_id, evidence_refs, chain_id, block_number,
             block_hash, transaction_hash, transaction_index, log_index, canonicality_state,
             consumer_visibility, interpreter_content_hash)
         VALUES ('edge-locked', 'registry-locked', 'migration_registry_creation', $1::uuid, $2,
             $3, '[{\"event_identity\":\"migration-registry-proof\"}]', $4, $5, $6, $7, 0, 0,
             'canonical', 'candidate', 'fixture')",
    )
    .bind(registry)
    .bind(registry_address)
    .bind(manifest_id)
    .bind(CHAIN)
    .bind(block)
    .bind(shadow_fixture::hash(block))
    .bind(word(0xbeef))
    .execute(pool)
    .await?;
    fixture
        .event(
            "locked-subregistry",
            Some(parent_id),
            None,
            V2_REGISTRY,
            "SubregistryChanged",
            block,
            json!({"subregistry": registry_address}),
            &address(0xe3),
        )
        .await?;
    fixture
        .event(
            "locked-migration",
            Some(parent_id),
            None,
            "ens_v2_migration_l1",
            "MigrationApplied",
            block,
            json!({"migration_path": "locked_wrapped",
            "successor_registry_contract_instance_id": registry,
            "evidence": [{"event_identity": if evidence == Evidence::UnmatchedMigration {
                "another-proof"
            } else {
                "migration-registry-proof"
            }}]}),
            &address(0xe2),
        )
        .await?;
    Ok(())
}

// A locked parent keeps exactly its migratable children: parent and child fuse states differ per
// child, and the child's wrapper expiry is read at the block clock, so a child whose wrapper
// expires between publications leaves the page with no event of its own.
#[tokio::test]
async fn a_locked_parent_keeps_its_migratable_children() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_children_locked", 12).await?;
    let (parent_id, parent_node, parent_labels) = parent(&fixture, 1, "locked").await?;
    let registry = uuid(0xf9);
    migration_registry(
        &fixture,
        &parent_id,
        &registry,
        &address(0xf9),
        2,
        Evidence::Valid,
    )
    .await?;
    let far = shadow_fixture::EPOCH + 1_000_000;
    let soon = shadow_fixture::EPOCH + 7;
    // (child, label, fuses, expiry, expiry from the registrar's NameRenewed, registered in the
    // migration registry)
    let cases: [(u64, &str, i64, i64, bool, bool); 6] = [
        (1, "migratable", 65_536, far, false, false),
        (2, "unlocked", 0, far, false, false),
        (3, "expiring", 65_536, soon, false, false),
        (4, "renewed", 65_536, far, true, false),
        (5, "dotted", 65_536 + 131_072, far, false, false),
        (6, "registered", 65_536, far, false, true),
    ];
    for (child, label, fuses, expiry, renewed, registered) in cases {
        let labelhash = word(0x5000 + child);
        fixture.label(&labelhash, label, true).await?;
        let mut labels = vec![labelhash];
        labels.extend(parent_labels.iter().cloned());
        let logical = fixture
            .surface(
                "ens",
                &word(child),
                &format!("{label}.locked.eth"),
                &labels,
                1,
            )
            .await?;
        let wrapper = uuid(0xc000 + child);
        fixture.resource(&wrapper, 1).await?;
        edge(
            &fixture,
            &format!("edge-{label}"),
            &parent_node,
            child,
            &owner(child),
            2,
        )
        .await?;
        fixture
            .event(
                &format!("fuses-{label}"),
                Some(&logical),
                Some(&wrapper),
                "ens_v1_wrapper_l1",
                "PermissionScopeChanged",
                2,
                json!({"fuses": fuses, "wrapper_state": "emancipated"}),
                &address(0xe4),
            )
            .await?;
        let (family, after) = if renewed {
            (
                "ens_v1_registrar_l1",
                json!({"expiry": expiry, "source_event": "NameRenewed", "authority_kind": "wrapper"}),
            )
        } else {
            ("ens_v1_wrapper_l1", json!({"expiry": expiry}))
        };
        fixture
            .event(
                &format!("expiry-{label}"),
                Some(&logical),
                Some(&wrapper),
                family,
                "ExpiryChanged",
                2,
                after,
                &address(0xe4),
            )
            .await?;
        if registered {
            registration(
                &fixture,
                &format!("history-{label}"),
                &logical,
                "RegistrationGranted",
                &registry,
                &owner(child),
                2,
            )
            .await?;
        }
    }
    fixture.publish(4).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    let visible = shadow::shadow_children(fixture.pool(), &parent_id).await?;
    let expected: std::collections::BTreeSet<String> = [1, 3, 4]
        .iter()
        .map(|child| format!("ens:{}", word(*child)))
        .collect();
    ensure!(visible == expected, "{visible:?}");

    // Block 9's clock is past the expiring child's wrapper expiry; nothing else happens.
    fixture.publish(9).await?;
    let report = fixture.compare(1).await?;
    unexpected(&report, &[])?;
    ensure!(
        !shadow::shadow_children(fixture.pool(), &parent_id)
            .await?
            .contains(&format!("ens:{}", word(3)))
    );
    fixture.cleanup().await
}

/// A child `n` of `parent_node` with a name surface under `parent_labels`, returning its id.
async fn child_surface(
    fixture: &Fixture,
    child: u64,
    name: &str,
    parent_labels: &[String],
) -> Result<String> {
    let mut labels = vec![word(0x5000 + child)];
    labels.extend(parent_labels.iter().cloned());
    fixture.surface("ens", &word(child), name, &labels, 1).await
}

// A registry Transfer to zero overrides a child's edge owner only when today's stage attributes
// it to that child (crates/project/src/builders/name_authority/stage.rs,
// `project_latest_registry_owner`): by the name it carries, else by the latest named event of
// its resource and family, else by an active surface at its node. An inactive surface at the
// node attributes nothing, while a name or resource attributes whatever node the Transfer
// carries. The phase schema's `normalized_events_chain_id_logical_name_id_fkey`
// (schema-v2/baseline/05_normalized_events.sql) requires an event's name to have a surface, so a
// Transfer naming a child with no surface at all cannot be stored; the name and resource cases use
// inactive surfaces and Transfers of another node instead. (The legacy public schema under
// migrations/ has no such key, but the phase readers and this fixture run on the phase schema.)
#[tokio::test]
async fn zero_owner_attribution_follows_the_served_precedence() -> Result<()> {
    let mut fixture = Fixture::new("families_shadow_children_owner", 12).await?;
    let (parent_id, parent_node, parent_labels) = parent(&fixture, 1, "owners").await?;
    for (child, label) in [
        (41, "inactive"),
        (42, "direct"),
        (43, "resource"),
        (44, "surfaced"),
        (45, "kept"),
    ] {
        fixture.label(&word(0x5000 + child), label, true).await?;
        edge(
            &fixture,
            &format!("edge-{label}"),
            &parent_node,
            child,
            &owner(child),
            2,
        )
        .await?;
    }
    for (child, name) in [
        (41, "inactive.owners.eth"),
        (42, "direct.owners.eth"),
        (43, "resource.owners.eth"),
    ] {
        let logical = child_surface(&fixture, child, name, &parent_labels).await?;
        sqlx::query(
            "UPDATE name_surfaces SET visibility_state = 'shadow',
                 deactivation_reason = 'fixture', deactivated_at = now()
             WHERE logical_name_id = $1",
        )
        .bind(&logical)
        .execute(fixture.pool())
        .await?;
    }
    // 41: an unattributed zero Transfer at its node, which has only an inactive surface.
    transfer(&fixture, "zero-inactive", 41, ZERO_ADDRESS, 3).await?;
    // 42: a zero Transfer of another node that names the child.
    let direct = format!("ens:{}", word(42));
    attributed_transfer(
        &fixture,
        "zero-direct",
        142,
        ZERO_ADDRESS,
        3,
        Some(&direct),
        None,
    )
    .await?;
    // 43: a named non-zero Transfer of a resource, then an unnamed zero Transfer of the same
    // resource at another node.
    let resourced = format!("ens:{}", word(43));
    let resource = uuid(0xd043);
    fixture.resource(&resource, 1).await?;
    attributed_transfer(
        &fixture,
        "named-resource",
        43,
        &owner(143),
        2,
        Some(&resourced),
        Some(&resource),
    )
    .await?;
    attributed_transfer(
        &fixture,
        "zero-resource",
        143,
        ZERO_ADDRESS,
        3,
        None,
        Some(&resource),
    )
    .await?;
    // 44: an active surface, then an unattributed zero Transfer at its node.
    child_surface(&fixture, 44, "surfaced.owners.eth", &parent_labels).await?;
    transfer(&fixture, "zero-surfaced", 44, ZERO_ADDRESS, 3).await?;
    fixture.publish(4).await?;
    let report = fixture.compare(2).await?;
    unexpected(&report, &[])?;
    let visible = shadow::shadow_children(fixture.pool(), &parent_id).await?;
    let expected: std::collections::BTreeSet<String> = [41, 45]
        .iter()
        .map(|child| format!("ens:{}", word(*child)))
        .collect();
    ensure!(visible == expected, "{visible:?}");
    fixture.cleanup().await
}

// A locked parent whose migration registry evidence is rejected, by a missing registry
// announcement or by a migration whose evidence does not contain the association's, serves none
// of its ENSv1 children, even a migratable one.
#[tokio::test]
async fn rejected_migration_evidence_hides_a_locked_parents_children() -> Result<()> {
    for (evidence, prefix) in [
        (
            Evidence::NoAnnouncement,
            "families_shadow_children_no_announcement",
        ),
        (
            Evidence::UnmatchedMigration,
            "families_shadow_children_unmatched",
        ),
    ] {
        let mut fixture = Fixture::new(prefix, 12).await?;
        let (parent_id, parent_node, parent_labels) = parent(&fixture, 1, "locked").await?;
        let registry = uuid(0xf9);
        migration_registry(&fixture, &parent_id, &registry, &address(0xf9), 2, evidence).await?;
        fixture.label(&word(0x5000 + 1), "migratable", true).await?;
        let logical = child_surface(&fixture, 1, "migratable.locked.eth", &parent_labels).await?;
        let wrapper = uuid(0xc001);
        fixture.resource(&wrapper, 1).await?;
        edge(&fixture, "edge-migratable", &parent_node, 1, &owner(1), 2).await?;
        fixture
            .event(
                "fuses-migratable",
                Some(&logical),
                Some(&wrapper),
                "ens_v1_wrapper_l1",
                "PermissionScopeChanged",
                2,
                json!({"fuses": 65_536, "wrapper_state": "emancipated"}),
                &address(0xe4),
            )
            .await?;
        fixture
            .event(
                "expiry-migratable",
                Some(&logical),
                Some(&wrapper),
                "ens_v1_wrapper_l1",
                "ExpiryChanged",
                2,
                json!({"expiry": shadow_fixture::EPOCH + 1_000_000}),
                &address(0xe4),
            )
            .await?;
        fixture.publish(4).await?;
        let report = fixture.compare(1).await?;
        unexpected(&report, &[])?;
        ensure!(
            shadow::shadow_children(fixture.pool(), &parent_id)
                .await?
                .is_empty(),
            "{evidence:?}: {}",
            report.line()
        );
        fixture.cleanup().await?;
    }
    Ok(())
}

/// An ENSv2 registration granted in `registry` with the given expiry, or none.
async fn expiring_registration(
    fixture: &Fixture,
    identity: &str,
    logical: &str,
    registry: &str,
    block: i64,
    expiry: Option<i64>,
) -> Result<()> {
    let mut after = json!({"registry_contract_instance_id": registry, "status": "registered",
                           "registrant": owner(1)});
    if let Some(expiry) = expiry {
        after["expiry"] = json!(expiry);
    }
    fixture
        .event(
            identity,
            Some(logical),
            None,
            V2_REGISTRY,
            "RegistrationGranted",
            block,
            after,
            &address(0xe3),
        )
        .await?;
    Ok(())
}

fn display_names(rows: &[FamilyChildRow]) -> Vec<&str> {
    rows.iter()
        .map(|row| row.canonical_display_name.as_str())
        .collect()
}

// The subnames filters at the block clock. ENSv2 children have mixed null and non-null expiries
// and registration times (two are bound, so their grant gives them one), labels a prefix must
// match literally (`_` and `%`), and an expiry exactly at the publication's time, which the
// fence keeps. An expiry-sorted, fenced page is then continued from one publication into the
// next at the new block's time, after a child on the remaining pages expired in between. Every
// page's rows, total and cursor are compared.
#[tokio::test]
async fn child_filters_sort_page_and_expire_at_the_block_clock() -> Result<()> {
    const PREFIXES: &[&str] = &["a_", "a%"];
    let mut fixture = Fixture::new("families_shadow_children_clock", 12).await?;
    let (parent_id, _, parent_labels) = parent(&fixture, 1, "clock").await?;
    let registry = uuid(0xf1);
    fixture.contract(&registry, &address(0xf1), 1).await?;
    fixture
        .event(
            "subregistry-clock",
            Some(&parent_id),
            None,
            V2_REGISTRY,
            "SubregistryChanged",
            2,
            json!({"subregistry": address(0xf1)}),
            &address(0xe3),
        )
        .await?;
    let epoch = shadow_fixture::EPOCH;
    let cases: [(u64, &str, Option<i64>, i64); 6] = [
        (1, "a_one", Some(epoch + 6), 2),
        (2, "a%two", Some(epoch + 1_000_000), 2),
        (3, "bthree", Some(epoch + 8), 3),
        (4, "anull", None, 3),
        (5, "zeta", Some(epoch + 1_000_000), 4),
        (6, "a_b", Some(epoch + 7), 4),
    ];
    for (n, label, expiry, block) in cases {
        let labelhash = word(0x6000 + n);
        fixture.label(&labelhash, label, true).await?;
        let mut labels = vec![labelhash];
        labels.extend(parent_labels.iter().cloned());
        let logical = fixture
            .surface(
                "ens",
                &word(0x7000 + n),
                &format!("{label}.clock.eth"),
                &labels,
                1,
            )
            .await?;
        // A registration time comes from the grant of the name's selected authority, so only
        // the two children with an ENSv2 binding have one; the others sort as null.
        if matches!(n, 2 | 5) {
            fixture
                .binding(
                    &uuid(0xc000 + n),
                    &logical,
                    &uuid(0xa000 + n),
                    "declared_registry_path",
                    "ens_v2",
                    block,
                )
                .await?;
        }
        expiring_registration(
            &fixture,
            &format!("granted-{label}"),
            &logical,
            &registry,
            block,
            expiry,
        )
        .await?;
    }
    fixture.publish(6).await?;
    let report = fixture.compare_with_prefixes(1, PREFIXES).await?;
    unexpected(&report, &[])?;
    let pool = fixture.pool();
    let (_, clock) = shadow::publication(pool, CHAIN).await?;
    ensure!(clock.unix_timestamp() == epoch + 6, "{clock}");
    let registered: Vec<(String, bool)> = sqlx::query_as(
        "SELECT logical_name_id,
                declared_summary #>> '{registration,registered_at}' IS NOT NULL
         FROM name_current WHERE logical_name_id = ANY($1) ORDER BY 1",
    )
    .bind(
        (1..=6)
            .map(|n| format!("ens:{}", word(0x7000 + n)))
            .collect::<Vec<_>>(),
    )
    .fetch_all(pool)
    .await?;
    let with_time: Vec<bool> = registered.iter().map(|(_, set)| *set).collect();
    ensure!(
        with_time == [false, true, false, false, true, false],
        "{registered:?}"
    );
    // Registration time in both directions, fenced and not, over pages of one row: two children
    // with a time and four without.
    for order in [ChildrenCurrentOrder::Asc, ChildrenCurrentOrder::Desc] {
        for include_expired in [true, false] {
            let filter = ChildrenCurrentPageFilter {
                sort: ChildrenCurrentSort::RegisteredAt,
                order,
                include_expired,
                evaluated_at: (!include_expired).then_some(clock),
                ..ChildrenCurrentPageFilter::default()
            };
            let (served_total, served, shadow_total, shadowed) =
                shadow::walk_children(pool, &parent_id, &filter, 1).await?;
            ensure!(
                served == shadowed && served_total == 6 && shadow_total == 6,
                "{order:?} {include_expired}: served {served:?}, shadow {shadowed:?}"
            );
        }
    }

    // The fence at block 6 keeps the child whose expiry is exactly block 6's time.
    let fenced = ChildrenCurrentPageFilter {
        include_expired: false,
        evaluated_at: Some(clock),
        ..ChildrenCurrentPageFilter::default()
    };
    let (served_total, served, shadow_total, shadowed) =
        shadow::walk_children(pool, &parent_id, &fenced, 10).await?;
    ensure!(
        served == shadowed
            && served_total == 6
            && shadow_total == 6
            && display_names(&served).contains(&"a_one.clock.eth"),
        "{served:?}"
    );
    // Prefixes match literally: `_` and `%` are not wildcards.
    for (prefix, expected) in [
        ("a_", vec!["a_b.clock.eth", "a_one.clock.eth"]),
        ("a%", vec!["a%two.clock.eth"]),
    ] {
        let filter = ChildrenCurrentPageFilter {
            q: Some(prefix),
            ..fenced
        };
        let (served_total, served, shadow_total, shadowed) =
            shadow::walk_children(pool, &parent_id, &filter, 1).await?;
        ensure!(
            display_names(&served) == expected
                && served == shadowed
                && served_total == expected.len() as u64
                && shadow_total == served_total,
            "{prefix}: served {served:?}, shadow {shadowed:?}"
        );
    }

    // The first expiry-sorted, fenced page at block 6, then its continuation at block 9's time.
    let by_expiry = |at| ChildrenCurrentPageFilter {
        sort: ChildrenCurrentSort::ExpiresAt,
        include_expired: false,
        evaluated_at: Some(at),
        ..ChildrenCurrentPageFilter::default()
    };
    let served =
        load_children_current_page_filtered(pool, &parent_id, &by_expiry(clock), None, 2).await?;
    let shadowed = load_children_shadow_page(pool, &parent_id, &by_expiry(clock), None, 2).await?;
    let served_rows: Vec<FamilyChildRow> = served.rows.iter().map(shadow::wire).collect();
    ensure!(
        served_rows == shadowed.rows
            && served.total_count == shadowed.total_count
            && served.next_cursor == shadowed.next_cursor
            && display_names(&served_rows) == ["a_one.clock.eth", "a_b.clock.eth"],
        "served {served:?}, shadow {shadowed:?}"
    );
    let mut served_cursor = served
        .next_cursor
        .context("more than one page at block 6")?;
    let mut shadow_cursor = served_cursor.clone();
    fixture.publish(9).await?;
    let pool = fixture.pool();
    let (_, later) = shadow::publication(pool, CHAIN).await?;
    ensure!(later.unix_timestamp() == epoch + 9, "{later}");
    let mut continued = Vec::new();
    loop {
        let served = load_children_current_page_filtered(
            pool,
            &parent_id,
            &by_expiry(later),
            Some(&served_cursor),
            2,
        )
        .await?;
        let shadowed =
            load_children_shadow_page(pool, &parent_id, &by_expiry(later), Some(&shadow_cursor), 2)
                .await?;
        let served_rows: Vec<FamilyChildRow> = served.rows.iter().map(shadow::wire).collect();
        ensure!(
            served_rows == shadowed.rows
                && served.total_count == shadowed.total_count
                && served.next_cursor == shadowed.next_cursor,
            "served {served:?}, shadow {shadowed:?}"
        );
        // Block 9's fence counts only the children not expired by then.
        ensure!(served.total_count == 3, "{}", served.total_count);
        continued.extend(served_rows);
        match (served.next_cursor, shadowed.next_cursor) {
            (Some(served_next), Some(shadow_next)) => {
                served_cursor = served_next;
                shadow_cursor = shadow_next;
            }
            _ => break,
        }
    }
    ensure!(
        display_names(&continued) == ["a%two.clock.eth", "zeta.clock.eth", "anull.clock.eth"],
        "{continued:?}"
    );
    let report = fixture.compare_with_prefixes(1, PREFIXES).await?;
    unexpected(&report, &[])?;
    fixture.cleanup().await
}

// Backslash is the escape character of both readers' prefix patterns, so a prefix holding one
// must still match literally: `a\` alone, `a\%` and `a\_`. Each decoy would match only if a
// backslash in the prefix escaped the character after it (`a%four` for `a\%` and `a\`,
// `a_five` for `a\_`). Kept apart from the clock fixture so its totals and orders stay as they
// are; the prefixes run through the whole filter matrix and are then checked by name.
#[tokio::test]
async fn child_prefixes_match_backslashes_literally() -> Result<()> {
    const PREFIXES: &[&str] = &["a\\", "a\\%", "a\\_"];
    let mut fixture = Fixture::new("families_shadow_children_backslash", 12).await?;
    let (parent_id, _, parent_labels) = parent(&fixture, 1, "slash").await?;
    let registry = uuid(0xf2);
    fixture.contract(&registry, &address(0xf2), 1).await?;
    fixture
        .event(
            "subregistry-slash",
            Some(&parent_id),
            None,
            V2_REGISTRY,
            "SubregistryChanged",
            2,
            json!({"subregistry": address(0xf2)}),
            &address(0xe3),
        )
        .await?;
    for (n, label) in [
        (1, "a\\one"),
        (2, "a\\%two"),
        (3, "a\\_three"),
        (4, "a%four"),
        (5, "a_five"),
        (6, "ab"),
    ] {
        let labelhash = word(0x6100 + n);
        fixture.label(&labelhash, label, true).await?;
        let mut labels = vec![labelhash];
        labels.extend(parent_labels.iter().cloned());
        let logical = fixture
            .surface(
                "ens",
                &word(0x7100 + n),
                &format!("{label}.slash.eth"),
                &labels,
                1,
            )
            .await?;
        expiring_registration(
            &fixture,
            &format!("granted-{n}"),
            &logical,
            &registry,
            2,
            None,
        )
        .await?;
    }
    fixture.publish(4).await?;
    let report = fixture.compare_with_prefixes(1, PREFIXES).await?;
    unexpected(&report, &[])?;
    let pool = fixture.pool();
    for (prefix, expected) in [
        (
            "a\\",
            vec![
                "a\\%two.slash.eth",
                "a\\_three.slash.eth",
                "a\\one.slash.eth",
            ],
        ),
        ("a\\%", vec!["a\\%two.slash.eth"]),
        ("a\\_", vec!["a\\_three.slash.eth"]),
    ] {
        let filter = ChildrenCurrentPageFilter {
            q: Some(prefix),
            ..ChildrenCurrentPageFilter::default()
        };
        let (served_total, served, shadow_total, shadowed) =
            shadow::walk_children(pool, &parent_id, &filter, 1).await?;
        let mut names = display_names(&served);
        names.sort_unstable();
        ensure!(
            names == expected
                && served == shadowed
                && served_total == expected.len() as u64
                && shadow_total == served_total,
            "{prefix}: served {served:?}, shadow {shadowed:?}"
        );
    }
    fixture.cleanup().await
}
