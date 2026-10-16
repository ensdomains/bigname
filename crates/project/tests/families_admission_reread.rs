//! A change of the chain's ENSv2 root registry admission (docs/glossary.md, "Universal Resolver
//! cutover") reaches every name at the next publication. Manifest sync writes its
//! SourceManifestUpdated events with no block, so the set they form applies at every block, and
//! a genesis rebuild under it composes every name as cut over or not from the start. A followed
//! publication that sees the admission change composes every surface again, and each case here
//! requires the families it leaves to equal a genesis rebuild under the same set at the same head
//! block: every family table except the two hydration work tables (docs/projections.md,
//! "Validation boundary"), and the marker without its generation.
mod families_support;

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Result, ensure};
use bigname_project::families::{FamilyMode, FamilyOptions, RebuildRanges};
use families_support::{CHAIN, CONTENT_HASH, Event, Fixture, ens_v2_path, uuid};
use serde_json::{Value, json};

const HYDRATION_WORK: [&str; 2] = [
    "project_text_hydration_work",
    "project_reverse_hydration_work",
];
const ROOT: &str = "0x00000000000000000000000000000000000000b4";
const REPLACEMENT_ROOT: &str = "0x00000000000000000000000000000000000000b5";
const FAR: u64 = 1_900_000_000;
const NO_ENTRY: Option<&str> = Some("no_live_ens_v2_entry");

static SYNC: AtomicU64 = AtomicU64::new(0);

/// What manifest sync writes for one manifest: the row on a first sync, its rollout status on a
/// later one, and a blockless finalized SourceManifestUpdated event carrying the payload
/// (crates/manifests/src/schema_v2_event_history.rs). Returns the manifest id.
async fn sync(
    fixture: &Fixture,
    manifest_id: Option<i64>,
    family: &str,
    status: &str,
    contracts: Value,
) -> Result<i64> {
    let payload = json!({
        "manifest_version": 1, "namespace": "ens", "source_family": family, "chain": CHAIN,
        "rollout_status": status, "contracts": contracts,
    });
    let id = match manifest_id {
        Some(id) => {
            sqlx::query(
                "UPDATE manifest_versions SET rollout_status = $2, manifest_payload = $3
                 WHERE manifest_id = $1",
            )
            .bind(id)
            .bind(status)
            .bind(&payload)
            .execute(&fixture.pool)
            .await?;
            id
        }
        None => {
            sqlx::query_scalar(
                "INSERT INTO manifest_versions (manifest_version, namespace, source_family,
                     chain_id, deployment_label, rollout_status, normalizer_version, file_path,
                     manifest_payload)
                 VALUES (1, 'ens', $1, $2, $6, $3, 'fixture', $4, $5)
                 RETURNING manifest_id",
            )
            .bind(family)
            .bind(CHAIN)
            .bind(status)
            .bind(format!(
                "fixture/{family}-{}.toml",
                SYNC.fetch_add(1, Ordering::Relaxed)
            ))
            .bind(&payload)
            .bind(format!("sync-{}", SYNC.fetch_add(1, Ordering::Relaxed)))
            .fetch_one(&fixture.pool)
            .await?
        }
    };
    let n = SYNC.fetch_add(1, Ordering::Relaxed);
    sqlx::query(
        "INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
             manifest_version, source_manifest_id, chain_id, derivation_kind,
             canonicality_state, after_state)
         VALUES ($1, 'ens', 'SourceManifestUpdated', $2, 1, $3, $4, 'manifest_sync',
                 'finalized', jsonb_build_object('rollout_status', $5::text,
                     'manifest_payload', $6::jsonb))",
    )
    .bind(format!("sync:{family}:{id}:{n}"))
    .bind(family)
    .bind(id)
    .bind(CHAIN)
    .bind(status)
    .bind(&payload)
    .execute(&fixture.pool)
    .await?;
    Ok(id)
}

fn root(address: &str) -> Value {
    json!([{"role": "root_registry", "address": address, "proxy_kind": "none",
        "start_block": 0}])
}

/// The id of the fixture path's root manifest (`ens_v2_path::declared_path`).
async fn path_root_manifest(fixture: &Fixture) -> Result<(i64, Value)> {
    Ok(sqlx::query_as(
        "SELECT manifest_id, manifest_payload -> 'contracts' FROM manifest_versions
         WHERE chain_id = $1 AND source_family = 'ens_v2_root_l1'",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?)
}

async fn follow(fixture: &Fixture, first: i64, last: i64) -> Result<()> {
    for block in first..=last {
        fixture.apply(block, FamilyMode::Normal).await?;
    }
    Ok(())
}

async fn compared(fixture: &Fixture) -> Result<Vec<(String, String)>> {
    Ok(fixture
        .exact()
        .await?
        .into_iter()
        .filter(|(table, _)| !HYDRATION_WORK.contains(&table.as_str()))
        .collect())
}

fn rows(text: &str) -> Vec<Value> {
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Array(rows)) => rows,
        Ok(other) => vec![other],
        Err(_) => vec![Value::String(text.to_owned())],
    }
}

fn shown(row: &Value) -> String {
    let text = row.to_string();
    match text.char_indices().nth(600) {
        Some((cut, _)) => format!("{}...", &text[..cut]),
        None => text,
    }
}

/// The tables `followed` and `rebuilt` disagree on, each with up to two rows only one side holds.
fn differences(followed: &[(String, String)], rebuilt: &[(String, String)]) -> Vec<String> {
    followed
        .iter()
        .zip(rebuilt)
        .filter(|((_, was), (_, now))| was != now)
        .map(|((table, was), (_, now))| {
            let (was, now) = (rows(was), rows(now));
            let only = |side: &[Value], other: &[Value]| -> (usize, Vec<String>) {
                let rows: Vec<&Value> = side.iter().filter(|row| !other.contains(row)).collect();
                (
                    rows.len(),
                    rows.iter().take(2).map(|row| shown(row)).collect(),
                )
            };
            let (followed_count, followed_rows) = only(&was, &now);
            let (rebuilt_count, rebuilt_rows) = only(&now, &was);
            format!(
                "{table}: {followed_count} rows only followed {followed_rows:?}; \
                 {rebuilt_count} rows only rebuilt {rebuilt_rows:?}"
            )
        })
        .collect()
}

/// The followed families against a genesis rebuild at `head`, in ranges and block by block.
/// The per-block rebuild runs last, so the fixture is left as it leaves it.
async fn assert_pass_equals_rebuild(fixture: &Fixture, head: i64) -> Result<()> {
    let followed = compared(fixture).await?;
    let mut failures = Vec::new();
    for (label, ranges) in [
        ("in ranges", RebuildRanges::Through(head)),
        ("block by block", RebuildRanges::Off),
    ] {
        let options = FamilyOptions::new(CONTENT_HASH).with_rebuild_ranges(ranges);
        fixture
            .apply_with(head, FamilyMode::Rebuild, &options)
            .await
            .map_err(|error| anyhow::anyhow!("rebuild {label}: {error}"))?;
        let differing = differences(&followed, &compared(fixture).await?);
        if !differing.is_empty() {
            failures.push(format!(
                "a rebuild {label} at {head} differs from the followed families in {} tables:\n{}",
                differing.len(),
                differing.join("\n")
            ));
        }
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// The names of `names` whose stored summary and lookup rows do not compose as `cut_over` says:
/// on a cut-over chain an ENSv1 `.eth` name with no live ENSv2 entry serves no resolver and the
/// reason `no_live_ens_v2_entry` (crates/storage/src/families/name/resolvability.rs), otherwise
/// it serves its resolver, if it has one, and no reason. `names` are (label, id, has a resolver).
async fn composed_otherwise(
    fixture: &Fixture,
    names: &[(&str, &str, bool)],
    cut_over: bool,
) -> Result<Vec<String>> {
    let ids: Vec<&str> = names.iter().map(|(_, id, _)| *id).collect();
    let stored: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT name.id,
             (SELECT summary.resolver_address FROM project_name_summary summary
              WHERE summary.chain_id = $1 AND summary.logical_name_id = name.id),
             (SELECT lookup.core -> 'declared_summary' ->> 'unresolvable_reason'
              FROM project_lookup_name lookup
              WHERE lookup.chain_id = $1 AND lookup.logical_name_id = name.id)
         FROM unnest($2::text[]) name (id) ORDER BY name.id",
    )
    .bind(CHAIN)
    .bind(&ids)
    .fetch_all(&fixture.pool)
    .await?;
    Ok(stored
        .into_iter()
        .filter_map(|(id, resolver, reason)| {
            let (label, _, has_resolver) = names.iter().find(|(_, name, _)| *name == id)?;
            let expected = match (cut_over, has_resolver) {
                (true, _) => (None, NO_ENTRY),
                (false, true) => (Some(ens_v2_path::RESOLVER), None),
                (false, false) => (None, None),
            };
            ((resolver.as_deref(), reason.as_deref()) != expected).then(|| {
                format!("{label}: summary resolver {resolver:?}, lookup reason {reason:?}")
            })
        })
        .collect())
}

/// Both checks of a case: the names that compose otherwise, and the rebuild comparison.
fn verdict(stale: Vec<String>, what: &str, rebuilt: Result<()>) -> Result<()> {
    let mut failures = Vec::new();
    if !stale.is_empty() {
        failures.push(format!("{what}: {stale:?}"));
    }
    if let Err(error) = rebuilt {
        failures.push(format!("{error:#}"));
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

const SENTINEL: &str = "0x000000000000000000000000000000000000dead";

/// Overwrite the stored owner of `name`'s summary, which any composition of the name replaces.
async fn plant_sentinel(fixture: &Fixture, name: &str) -> Result<()> {
    let planted = sqlx::query(
        "UPDATE project_name_summary SET owner = $3
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(name)
    .bind(SENTINEL)
    .execute(&fixture.pool)
    .await?
    .rows_affected();
    ensure!(planted == 1, "no summary of {name} to plant on");
    Ok(())
}

/// The families block `block` journalled, with their row counts.
async fn journal_families(fixture: &Fixture, block: i64) -> Result<Vec<(String, i64)>> {
    Ok(sqlx::query_as(
        "SELECT family, count(*) FROM project_family_undo
         WHERE chain_id = $1 AND block_number = $2 GROUP BY family ORDER BY family",
    )
    .bind(CHAIN)
    .bind(block)
    .fetch_all(&fixture.pool)
    .await?)
}

/// Whether `name`'s summary still holds the planted owner: no publication composed it again.
async fn sentinel_kept(fixture: &Fixture, name: &str) -> Result<bool> {
    let owner: Option<String> = sqlx::query_scalar(
        "SELECT owner FROM project_name_summary WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(name)
    .fetch_one(&fixture.pool)
    .await?;
    Ok(owner.as_deref() == Some(SENTINEL))
}

const V1_REGISTRAR: &str = "0x00000000000000000000000000000000000000e3";
const V1_REGISTRY: &str = "0x00000000000000000000000000000000000000e5";

/// `ens_v2_path::leased` without the resolver: a leased `.eth` name no resolver points at, so
/// no resolver classification reaches it.
async fn quiet(fixture: &Fixture, name: &str, n: u32, block: i64) -> Result<String> {
    let logical_name_id = ens_v2_path::surface(fixture, name).await?;
    let lease = uuid(0x1000 + n);
    fixture
        .binding(
            &uuid(100 + n),
            &logical_name_id,
            &lease,
            "ens_v1",
            block,
            n.into(),
            None,
        )
        .await?;
    for (kind, at, after) in [
        (
            "SurfaceBound",
            block,
            json!({"authority_kind": "registrar", "state_derived": false,
                "registry_contract": V1_REGISTRY, "owner_getter": ens_v2_path::OWNER}),
        ),
        (
            "RegistrationGranted",
            block + 1,
            json!({"authority_kind": "registrar", "status": "registered",
                "registrant": ens_v2_path::OWNER, "expiry": FAR}),
        ),
    ] {
        fixture
            .event(
                Event::new(
                    &format!("{kind}-{name}"),
                    at,
                    n.into(),
                    kind,
                    "ens_v1_registrar_l1",
                )
                .name(&logical_name_id)
                .resource(&lease)
                .after(after)
                .raw(json!({"emitting_address": V1_REGISTRAR})),
            )
            .await?;
    }
    Ok(logical_name_id)
}

/// ENSv1 `.eth` names only, written over blocks 1 to 4, on a chain whose manifest set admits
/// no ENSv2 root registry: `bob.eth` and `carol.eth` leased, `sub.bob.eth` under `bob.eth`, and
/// `quiet.eth` leased with no resolver.
struct V1Names {
    bob: String,
    carol: String,
    quiet: String,
}

async fn v1_names(fixture: &Fixture) -> Result<V1Names> {
    let bob = ens_v2_path::leased(fixture, "bob.eth", 2, 1, FAR).await?;
    let carol = ens_v2_path::leased(fixture, "carol.eth", 3, 2, FAR).await?;
    ens_v2_path::subname(fixture, "sub.bob.eth", 1, 1).await?;
    let quiet = quiet(fixture, "quiet.eth", 6, 2).await?;
    Ok(V1Names { bob, carol, quiet })
}

impl V1Names {
    /// The `.eth` second-level names, whose cut-over composition the checks spell out. The
    /// rebuild comparison covers `sub.bob.eth`.
    fn all(&self) -> [(&str, &str, bool); 3] {
        [
            ("bob.eth", &self.bob, true),
            ("carol.eth", &self.carol, true),
            ("quiet.eth", &self.quiet, false),
        ]
    }
}

// Row 1, with row 9 and the loss half of row 16 (the history catalogue and the lookup inventory
// tables are among the compared families). The chain follows to block 6 with no ENSv2 root registry
// admitted, manifest sync then admits one, and block 7 names none of the names. A `.eth` name ENSv1
// decides with no live ENSv2 entry resolves to nothing on a cut-over chain
// (crates/storage/src/families/name/resolvability.rs), so every name serves `no_live_ens_v2_entry`
// after block 7, as a rebuild under the new set composes them. The lease of `quiet.eth` then serves
// no name, so its inventory and dependency rows go. `late.eth` is a surface Interpret wrote at
// block 8, above the block that sees the admission: the publication at 7 composes nothing for it,
// as the rebuild at 7 does not.
#[tokio::test]
async fn an_admitted_root_recomposes_every_untouched_name_at_the_next_block() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_admit", 9).await?;
    let names = v1_names(&fixture).await?;
    let late = ens_v2_path::surface_at(&fixture, "late.eth", 8).await?;
    follow(&fixture, 0, 6).await?;
    sync(&fixture, None, "ens_v2_root_l1", "active", root(ROOT)).await?;
    follow(&fixture, 7, 7).await?;
    let mut stale = composed_otherwise(&fixture, &names.all(), true).await?;
    let early = stored_name_rows(&fixture, &late).await?;
    if early != (0, 0) {
        stale.push(format!(
            "late.eth, whose surface is at block 8: (summary rows, lookup rows) {early:?} at 7"
        ));
    }
    let rebuilt = assert_pass_equals_rebuild(&fixture, 7).await;
    fixture.cleanup().await?;
    verdict(
        stale,
        "after the admission, names still compose as not cut over",
        rebuilt,
    )
}

// Row 2. The ENSv2 path history is cut over under root A from block 0. Manifest sync deprecates A
// and admits a replacement root with no entries, and block 13 names none of the names: every path
// the walk took from A is gone. The names of the history point at a classified resolver, so the
// resolver classification that every manifest sync runs again already composes them and their
// subtrees again (crates/project/src/families/resolution_paths/selection.rs). `quiet.lone.eth` is
// the name only the admission reaches: an ENSv1 subname with no resolver under `lone.eth`, which
// has none either. Its walk from A leaves a `recompose_at` on its summary that the walk from the
// replacement root, which reads no entry, does not leave
// (crates/storage/src/families/name/resolution_path.rs).
#[tokio::test]
async fn a_replaced_root_moves_every_path_to_the_new_root() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_replace", 14).await?;
    ens_v2_path::history(&fixture).await?;
    quiet(&fixture, "lone.eth", 8, 1).await?;
    let walked = unresolved_subname(&fixture, "quiet.lone.eth", 9, 2).await?;
    follow(&fixture, 0, 12).await?;
    let before = recompose_at(&fixture, &walked).await?;
    let (old, contracts) = path_root_manifest(&fixture).await?;
    sync(
        &fixture,
        Some(old),
        "ens_v2_root_l1",
        "deprecated",
        contracts,
    )
    .await?;
    sync(
        &fixture,
        None,
        "ens_v2_root_l1",
        "active",
        root(REPLACEMENT_ROOT),
    )
    .await?;
    follow(&fixture, 13, 13).await?;
    let after = recompose_at(&fixture, &walked).await?;
    let rebuilt = assert_pass_equals_rebuild(&fixture, 13).await;
    fixture.cleanup().await?;
    let stale = match before {
        None => vec!["quiet.lone.eth: the walk from the old root stored no recompose_at".into()],
        Some(_) if after == before => {
            vec![format!(
                "quiet.lone.eth: recompose_at {after:?} from the old root's walk"
            )]
        }
        Some(_) => Vec::new(),
    };
    verdict(
        stale,
        "after the replacement, a path keeps the old root's walk",
        rebuilt,
    )
}

/// An ENSv1 subname with node resource `n` and no resolver, written as a binding and an
/// `AuthorityTransferred` to `OWNER` of source family `ens_v1_registry_l1` at `block`. Returns
/// its logical name id.
async fn unresolved_subname(fixture: &Fixture, name: &str, n: u32, block: i64) -> Result<String> {
    let logical_name_id = ens_v2_path::surface(fixture, name).await?;
    let node = uuid(0x3000 + n);
    fixture
        .binding(
            &uuid(300 + n),
            &logical_name_id,
            &node,
            "ens_v1",
            block,
            (50 + n).into(),
            None,
        )
        .await?;
    fixture
        .event(
            Event::new(
                &format!("owned-{name}"),
                block,
                (50 + n).into(),
                "AuthorityTransferred",
                "ens_v1_registry_l1",
            )
            .name(&logical_name_id)
            .resource(&node)
            .after(
                json!({"authority_kind": "registry", "owner": ens_v2_path::OWNER,
                          "registry_owner": ens_v2_path::OWNER,
                          "node": logical_name_id.trim_start_matches("ens:")}),
            )
            .raw(json!({"emitting_address": V1_REGISTRY})),
        )
        .await?;
    Ok(logical_name_id)
}

/// How many name summary rows and lookup name rows the chain stores for `logical_name_id`.
async fn stored_name_rows(fixture: &Fixture, logical_name_id: &str) -> Result<(i64, i64)> {
    Ok(sqlx::query_as(
        "SELECT (SELECT count(*) FROM project_name_summary
                 WHERE chain_id = $1 AND logical_name_id = $2),
                (SELECT count(*) FROM project_lookup_name
                 WHERE chain_id = $1 AND logical_name_id = $2)",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_one(&fixture.pool)
    .await?)
}

/// The stored next recomposition time of the name summary of `logical_name_id`.
async fn recompose_at(fixture: &Fixture, logical_name_id: &str) -> Result<Option<i64>> {
    Ok(sqlx::query_scalar(
        "SELECT recompose_at FROM project_name_summary
         WHERE chain_id = $1 AND logical_name_id = $2",
    )
    .bind(CHAIN)
    .bind(logical_name_id)
    .fetch_optional(&fixture.pool)
    .await?
    .flatten())
}

// Row 3, with the gain half of row 16. The history of row 2 and `quiet.eth`, an ENSv1 lease with
// an entry in the `.eth` registry and no resolver, and manifest sync deprecates the root with no
// replacement: the chain is no longer cut over and every name composes through ENSv1 again. The
// lease of `quiet.eth` serves its name again, so it gains its inventory and dependency rows.
#[tokio::test]
async fn a_withdrawn_root_composes_every_name_as_ens_v1_again() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_withdraw", 14).await?;
    ens_v2_path::history(&fixture).await?;
    quiet(&fixture, "quiet.eth", 7, 1).await?;
    v2_registration(&fixture, 6, "quiet").await?;
    follow(&fixture, 0, 12).await?;
    let (old, contracts) = path_root_manifest(&fixture).await?;
    sync(
        &fixture,
        Some(old),
        "ens_v2_root_l1",
        "deprecated",
        contracts,
    )
    .await?;
    follow(&fixture, 13, 13).await?;
    let rebuilt = assert_pass_equals_rebuild(&fixture, 13).await;
    fixture.cleanup().await?;
    rebuilt
}

// Row 5. Undo through the block that saw the admission change restores the families as they
// were before it, and the replayed block sees the change again.
#[tokio::test]
async fn undo_through_the_pass_restores_and_replay_sweeps_again() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_undo", 8).await?;
    let names = v1_names(&fixture).await?;
    follow(&fixture, 0, 6).await?;
    sync(&fixture, None, "ens_v2_root_l1", "active", root(ROOT)).await?;
    fixture.assert_undo_restores(7).await?;
    let stale = composed_otherwise(&fixture, &names.all(), true).await?;
    let rebuilt = assert_pass_equals_rebuild(&fixture, 7).await;
    fixture.cleanup().await?;
    verdict(
        stale,
        "after the replay, names still compose as not cut over",
        rebuilt,
    )
}

// Row 6. A root admission written at block 6, with filler work blocks around it so a rebuild in
// ranges folds block 6 inside a range. Production manifest sync writes no block, so this shape
// exists only in fixtures. It is one guard on the range path, which publishes once for its
// blocks.
#[tokio::test]
async fn an_admission_change_inside_a_range_equals_block_by_block() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_range", 10).await?;
    let names = v1_names(&fixture).await?;
    for block in [5, 7, 8] {
        ens_v2_path::filler(&fixture, block).await?;
    }
    let id = sync(&fixture, None, "ens_v2_root_l1", "active", root(ROOT)).await?;
    sqlx::query(
        "UPDATE normalized_events SET block_number = 6, block_hash = $2
         WHERE source_manifest_id = $1 AND event_kind = 'SourceManifestUpdated'",
    )
    .bind(id)
    .bind(families_support::hash(6))
    .execute(&fixture.pool)
    .await?;
    follow(&fixture, 0, 9).await?;
    let stale = composed_otherwise(&fixture, &names.all(), true).await?;
    let rebuilt = assert_pass_equals_rebuild(&fixture, 9).await;
    fixture.cleanup().await?;
    verdict(
        stale,
        "after the admission at block 6, names still compose as not cut over at 9",
        rebuilt,
    )
}

// Row 7. The commit of the block that saw the admission change fails once: nothing of it is
// published, the marker stays on its predecessor, and the next run publishes the block again.
#[tokio::test]
async fn a_failed_pass_publishes_nothing_and_the_next_run_repeats_it() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_crash", 8).await?;
    let names = v1_names(&fixture).await?;
    follow(&fixture, 0, 6).await?;
    sync(&fixture, None, "ens_v2_root_l1", "active", root(ROOT)).await?;
    let before = fixture.exact().await?;
    sqlx::raw_sql(
        "CREATE SEQUENCE refuse_commit_once;
         CREATE FUNCTION refuse_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF nextval('refuse_commit_once') = 1 THEN
                 RAISE EXCEPTION 'injected commit failure';
             END IF;
             RETURN NEW;
         END $$;
         CREATE CONSTRAINT TRIGGER refuse_commit AFTER UPDATE ON project_family_marker
         DEFERRABLE INITIALLY DEFERRED
         FOR EACH ROW WHEN (NEW.current_block_number = 7)
         EXECUTE FUNCTION refuse_commit();",
    )
    .execute(&fixture.pool)
    .await?;
    let error = fixture
        .apply(7, FamilyMode::Normal)
        .await
        .expect_err("the commit of block 7 fails");
    ensure!(
        error.to_string().contains("injected commit failure"),
        "{error}"
    );
    ensure!(
        fixture.marker().await?.0 == Some(6),
        "the marker left block 6"
    );
    ensure!(
        fixture.exact().await? == before,
        "the failed block published rows"
    );
    follow(&fixture, 7, 7).await?;
    let stale = composed_otherwise(&fixture, &names.all(), true).await?;
    let rebuilt = assert_pass_equals_rebuild(&fixture, 7).await;
    fixture.cleanup().await?;
    verdict(
        stale,
        "after the repeated block, names still compose as not cut over",
        rebuilt,
    )
}

// Row 8. The ENSv2 path history is followed to block 8 before its root is admitted: its events
// are retained, but the walk has no root. Manifest sync admits the root, and blocks 9 to 12
// follow. `carol.eth`'s ENSv2 entry expires by the clock at block 10, which no event names, so
// its summary moves only if the admission's publication stored the clock boundary the rebuild
// stores.
#[tokio::test]
async fn a_clock_after_the_pass_recomposes_as_the_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_clock", 14).await?;
    ens_v2_path::history(&fixture).await?;
    quiet(&fixture, "quiet.eth", 7, 1).await?;
    let (id, contracts) = path_root_manifest(&fixture).await?;
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE source_manifest_id = $1 AND event_kind = 'SourceManifestUpdated'",
    )
    .bind(id)
    .execute(&fixture.pool)
    .await?;
    follow(&fixture, 0, 8).await?;
    sync(&fixture, Some(id), "ens_v2_root_l1", "active", contracts).await?;
    follow(&fixture, 9, 12).await?;
    let rebuilt = assert_pass_equals_rebuild(&fixture, 12).await;
    fixture.cleanup().await?;
    rebuilt
}

// Row 15. Names the `.eth` arm of the path selection never reads: an ENSv1 name under another
// top-level name, and a `.eth` surface that a normalizer change withdrew to shadow.
#[tokio::test]
async fn a_non_eth_name_and_a_shadow_surface_compose_as_the_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_other", 8).await?;
    v1_names(&fixture).await?;
    ens_v2_path::subname(&fixture, "alice.box", 4, 2).await?;
    let hidden = ens_v2_path::leased(&fixture, "hidden.eth", 5, 2, FAR).await?;
    sqlx::query(
        "UPDATE name_surfaces SET visibility_state = 'shadow',
             deactivation_reason = 'fixture_normalizer_change',
             deactivated_at = to_timestamp(1800000024)
         WHERE logical_name_id = $1",
    )
    .bind(&hidden)
    .execute(&fixture.pool)
    .await?;
    follow(&fixture, 0, 6).await?;
    sync(&fixture, None, "ens_v2_root_l1", "active", root(ROOT)).await?;
    follow(&fixture, 7, 7).await?;
    let rebuilt = assert_pass_equals_rebuild(&fixture, 7).await;
    fixture.cleanup().await?;
    rebuilt
}

// Row 17. Two manifest syncs a block apart: the root is admitted before block 7, in which
// `quiet.eth`'s lease is granted again, and deprecated again before block 8, which names
// nothing. Block 8 must compose `quiet.eth` as not cut over again.
#[tokio::test]
async fn two_admission_changes_sweep_at_each_block() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_twice", 9).await?;
    let names = v1_names(&fixture).await?;
    follow(&fixture, 0, 6).await?;
    let id = sync(&fixture, None, "ens_v2_root_l1", "active", root(ROOT)).await?;
    fixture
        .event(
            Event::new(
                "regranted-quiet.eth",
                7,
                6,
                "RegistrationGranted",
                "ens_v1_registrar_l1",
            )
            .name(&names.quiet)
            .resource(&uuid(0x1006))
            .after(
                json!({"authority_kind": "registrar", "status": "registered",
                    "registrant": ens_v2_path::OWNER, "expiry": FAR + 1}),
            )
            .raw(json!({"emitting_address": V1_REGISTRAR})),
        )
        .await?;
    follow(&fixture, 7, 7).await?;
    sync(
        &fixture,
        Some(id),
        "ens_v2_root_l1",
        "deprecated",
        root(ROOT),
    )
    .await?;
    follow(&fixture, 8, 8).await?;
    let stale = composed_otherwise(&fixture, &names.all(), false).await?;
    let rebuilt = assert_pass_equals_rebuild(&fixture, 8).await;
    fixture.cleanup().await?;
    verdict(
        stale,
        "after the withdrawal, names still compose as cut over",
        rebuilt,
    )
}

/// `ens_v2_path::register` of `label` in the `.eth` registry the fixture path's root points at.
async fn v2_registration(fixture: &Fixture, block: i64, label: &str) -> Result<()> {
    let registry: String = sqlx::query_scalar(
        "SELECT after_state ->> 'subregistry' FROM normalized_events
         WHERE chain_id = $1 AND event_kind = 'SubregistryChanged'
           AND source_family = 'ens_v2_root_l1'",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    ens_v2_path::register(fixture, block, 300, &registry, label, FAR).await
}

const HISTORY_CATALOGUE: [&str; 4] = [
    "project_history_source",
    "project_history_source_edge",
    "project_address_history_anchor",
    "project_history_catalogue_marker",
];

// Row 9. The withdrawal of row 3 moves names decided through the ENSv2 arm back to ENSv1
// (crates/storage/src/families/control/lifecycle/canonical.rs), and with them the relations the
// history catalogue indexes: `quiet.eth` is leased through ENSv1 and registered in the `.eth`
// registry, with no resolver to reach it. The followed block refreshes the catalogue for every name
// with the marker's block before it as its window, where the rebuild's last publication uses no
// window at all. The stored catalogue rows must still be the rebuild's.
#[tokio::test]
async fn the_history_catalogue_after_the_pass_equals_the_rebuild() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_catalogue", 14).await?;
    ens_v2_path::history(&fixture).await?;
    quiet(&fixture, "quiet.eth", 7, 1).await?;
    v2_registration(&fixture, 6, "quiet").await?;
    follow(&fixture, 0, 12).await?;
    let (old, contracts) = path_root_manifest(&fixture).await?;
    sync(
        &fixture,
        Some(old),
        "ens_v2_root_l1",
        "deprecated",
        contracts,
    )
    .await?;
    follow(&fixture, 13, 13).await?;
    let catalogue = |tables: Vec<(String, String)>| -> Vec<(String, String)> {
        tables
            .into_iter()
            .filter(|(table, _)| HISTORY_CATALOGUE.contains(&table.as_str()))
            .collect()
    };
    let followed = catalogue(fixture.exact().await?);
    let others = compared(&fixture).await?;
    let mut failures = Vec::new();
    for (label, ranges) in [
        ("in ranges", RebuildRanges::Through(13)),
        ("block by block", RebuildRanges::Off),
    ] {
        let options = FamilyOptions::new(CONTENT_HASH).with_rebuild_ranges(ranges);
        fixture
            .apply_with(13, FamilyMode::Rebuild, &options)
            .await?;
        let differing = differences(&followed, &catalogue(fixture.exact().await?));
        if !differing.is_empty() {
            let rest = differences(&others, &compared(&fixture).await?);
            failures.push(format!(
                "the catalogue of a rebuild {label} differs:\n{}\nall differing tables:\n{}",
                differing.join("\n"),
                rest.join("\n")
            ));
        }
    }
    fixture.cleanup().await?;
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// Row 4, with the no-sweep half of row 13. A manifest sync that leaves the admission as it was
// changes the set key, so the resolver classification runs again
// (crates/project/src/families/classification.rs), but no name the block does not touch is
// composed again for the admission: the owner planted on the summary of `quiet.eth`, which no
// resolver reaches, survives the block. The chain admits no root before or after the sync, so
// both markers record no admission. The other half of row 13, the first publication of a
// rebuild after a reset, is in every `assert_pass_equals_rebuild`.
#[tokio::test]
async fn an_unrelated_manifest_edit_does_not_sweep() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_unrelated", 8).await?;
    let names = v1_names(&fixture).await?;
    follow(&fixture, 0, 6).await?;
    plant_sentinel(&fixture, &names.quiet).await?;
    sync(&fixture, None, "ens_v2_migration_l1", "active", json!([])).await?;
    follow(&fixture, 7, 7).await?;
    let kept = sentinel_kept(&fixture, &names.quiet).await?;
    let journal = journal_families(&fixture, 7).await?;
    fixture.cleanup().await?;
    ensure!(
        kept,
        "a manifest edit that keeps the admission composed quiet.eth again; block 7 journalled \
         {journal:?}"
    );
    Ok(())
}

// Row 14. A correction of the admitted root's declared start block alone changes the
// admission's `since_block`, which no composition reads, but the admission is still a different
// one: the block composes every name again, and the marker records the new start.
#[tokio::test]
async fn a_start_block_correction_sweeps_and_republishes_since_block() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_since", 8).await?;
    let names = v1_names(&fixture).await?;
    let id = sync(&fixture, None, "ens_v2_root_l1", "active", root(ROOT)).await?;
    follow(&fixture, 0, 6).await?;
    plant_sentinel(&fixture, &names.quiet).await?;
    let corrected = json!([{"role": "root_registry", "address": ROOT, "proxy_kind": "none",
        "start_block": 3}]);
    sync(&fixture, Some(id), "ens_v2_root_l1", "active", corrected).await?;
    follow(&fixture, 7, 7).await?;
    let kept = sentinel_kept(&fixture, &names.quiet).await?;
    let since: Option<i64> =
        sqlx::query_scalar("SELECT since_block FROM project_family_marker WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(&fixture.pool)
            .await?;
    let rebuilt = assert_pass_equals_rebuild(&fixture, 7).await;
    fixture.cleanup().await?;
    ensure!(since == Some(3), "the marker records since_block {since:?}");
    ensure!(
        !kept,
        "the start block correction left quiet.eth's summary uncomposed"
    );
    rebuilt
}

// Row 11. Reads composed at request time take the admission from the marker
// (crates/storage/src/families/name/batch.rs), as names bound without a surface do. After the
// block that saw the admission, the marker names the root and the request-time reader serves
// `bob.eth` as cut over.
#[tokio::test]
async fn request_time_reads_follow_the_marker_after_the_pass() -> Result<()> {
    let fixture = Fixture::new("families_admission_reread_request", 8).await?;
    let names = v1_names(&fixture).await?;
    follow(&fixture, 0, 6).await?;
    sync(&fixture, None, "ens_v2_root_l1", "active", root(ROOT)).await?;
    follow(&fixture, 7, 7).await?;
    let marker: (Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT root_registry, since_block FROM project_family_marker WHERE chain_id = $1",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    let row = bigname_storage::families::name::load_family_name(&fixture.pool, &names.bob).await;
    fixture.cleanup().await?;
    ensure!(
        marker == (Some(ROOT.to_owned()), Some(0)),
        "the marker records {marker:?}"
    );
    let row = row?.ok_or_else(|| anyhow::anyhow!("bob.eth is not served"))?;
    ensure!(
        row.declared_summary["unresolvable_reason"] == json!("no_live_ens_v2_entry"),
        "bob.eth serves {}",
        row.declared_summary
    );
    Ok(())
}
