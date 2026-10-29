//! Nothing serves the lookup facts while a rebuild's names stream is open (docs/projections.md,
//! "Owned key families"). A names chunk writes a lookup name with no serving resource, and the
//! lookup inventories stream sets it later, so between the two streams `project_lookup_name` is
//! partly rebuilt, as the serial pass leaves it between chunks. Every reader of the table, the
//! lookup records, the reverse pages and the family names, refuses with the publication
//! unavailable (the API's 409) from the rebuild's reset to its completing block. Two rebuilds
//! hold a names chunk at its commit and read: an operator redo, and a live chain whose reorg
//! orphans a block at or below the pass block. The history is `families_support/compose.rs`.
mod families_support;

use std::time::Duration;

use anyhow::{Result, ensure};
use bigname_project::{
    Marker,
    families::{self, FamilyMode, FamilyOptions, RebuildRanges},
};
use bigname_storage::{
    ReverseIdentityRoles, ReverseIdentityStorageInput,
    families::{
        lookup::load_lookup_records,
        name::{is_publication_unavailable, load_family_names_by_logical_name_ids},
        records::load_family_reverse_identity_groups,
    },
};
use families_support::{
    CHAIN, CONTENT_HASH, Fixture,
    compose::seeded_with,
    hash,
    workers::{FIRST_LOOKED_UP, held, hold, side, stream, until},
};
use sqlx::PgPool;

const TARGET: i64 = 300;
/// The longest a case waits for the held chunk.
const BOUND: u64 = 60;

fn options() -> FamilyOptions {
    FamilyOptions::new(CONTENT_HASH)
        .with_max_blocks_per_run(u64::MAX)
        .with_rebuild_ranges(RebuildRanges::Through(TARGET))
        .with_compose_chunk_keys(1)
        .with_compose_workers(3)
}

fn sibling(block: i64) -> String {
    format!("0x{:064x}", block + 4096)
}

/// The Project row, in the redo `redo` when given, with both phases' input hashes, so the lookup
/// readers admit the chain whenever its marker is servable.
async fn admitted(fixture: &Fixture, attempt: i64, redo: Option<(i64, i64, &str)>) -> Result<()> {
    fixture
        .interpret_row(bigname_test_support::INTERPRETER_CONTENT_HASH, 1, false)
        .await?;
    fixture.project_row(attempt, redo).await?;
    sqlx::query(
        "UPDATE chain_phase_state SET input_content_hash = $2
         WHERE chain_id = $1 AND phase_name = 'project'",
    )
    .bind(CHAIN)
    .bind(CONTENT_HASH)
    .execute(&fixture.pool)
    .await?;
    Ok(())
}

/// What the readers of the lookup facts are asked: every lookup name, and an address with lookup
/// relations in its names' namespace.
struct Reads {
    names: Vec<String>,
    reverse: ReverseIdentityStorageInput,
    namespaces: Vec<String>,
}

async fn reads(fixture: &Fixture) -> Result<Reads> {
    let names = sqlx::query_scalar(
        "SELECT logical_name_id FROM project_lookup_name WHERE chain_id = $1
         ORDER BY logical_name_id COLLATE \"C\"",
    )
    .bind(CHAIN)
    .fetch_all(&fixture.pool)
    .await?;
    let (address, namespace): (String, String) = sqlx::query_as(
        "SELECT relation.address, surface.namespace FROM project_lookup_relation relation
         JOIN name_surfaces surface ON surface.chain_id = relation.chain_id
             AND surface.logical_name_id = relation.logical_name_id
         JOIN project_lookup_name stored ON stored.chain_id = relation.chain_id
             AND stored.logical_name_id = relation.logical_name_id
         WHERE relation.chain_id = $1 AND stored.supported
             AND relation.relation IN ('token_holder', 'effective_controller')
         ORDER BY relation.address COLLATE \"C\" LIMIT 1",
    )
    .bind(CHAIN)
    .fetch_one(&fixture.pool)
    .await?;
    Ok(Reads {
        names,
        reverse: ReverseIdentityStorageInput {
            address,
            coin_type: "60".to_owned(),
            roles: ReverseIdentityRoles::Both,
            page_size: 10,
            cursor: None,
        },
        namespaces: vec![namespace],
    })
}

/// Each reader's answer: the rows it served, or `None` when it refused with the publication
/// unavailable. Any other error fails the case.
async fn served(pool: &PgPool, reads: &Reads) -> Result<[Option<usize>; 3]> {
    let chains = [CHAIN.to_owned()];
    let chains = &chains[..];
    let refused = |error: anyhow::Error| -> Result<Option<usize>> {
        if is_publication_unavailable(&error) {
            Ok(None)
        } else {
            Err(error)
        }
    };
    let lookup = match load_lookup_records(pool, &reads.names, true, Some(chains)).await {
        Ok(rows) => Some(rows.len()),
        Err(error) => refused(error)?,
    };
    let reverse = match load_family_reverse_identity_groups(
        pool,
        std::slice::from_ref(&reads.reverse),
        &reads.namespaces,
        Some(chains),
        true,
        true,
    )
    .await
    {
        Ok(groups) => Some(groups.iter().map(|group| group.entries.len()).sum()),
        Err(error) => refused(error)?,
    };
    let names = match load_family_names_by_logical_name_ids(pool, &reads.names).await {
        Ok(rows) => Some(rows.len()),
        Err(error) => refused(error)?,
    };
    Ok([lookup, reverse, names])
}

async fn require_served(pool: &PgPool, reads: &Reads, label: &str) -> Result<()> {
    let answers = served(pool, reads).await?;
    let [lookup, reverse, names] = answers;
    ensure!(
        lookup.is_some_and(|rows| rows > 0)
            && reverse.is_some()
            && names.is_some_and(|rows| rows > 0),
        "{label}: every reader serves (lookup, reverse, names): {answers:?}"
    );
    Ok(())
}

/// Run `target` under `mode` while a names chunk is held at its commit, read every reader at the
/// hold, and return what they answered and the stream the repair record stood in.
async fn read_at_hold(
    fixture: &Fixture,
    side: &PgPool,
    key: i64,
    target: &Marker,
    mode: FamilyMode,
    reads: &Reads,
) -> Result<([Option<usize>; 3], Option<i16>)> {
    let held_chunk = hold(fixture, side, key, FIRST_LOOKED_UP, false).await?;
    let run = async {
        let token = families::input_token(&fixture.pool, CHAIN).await?;
        tokio::time::timeout(
            Duration::from_secs(600),
            families::apply(&fixture.pool, CHAIN, target, mode, &token, &options()),
        )
        .await
        .unwrap_or_else(|_| {
            Err(bigname_project::ProjectError::transient(
                "the pass did not return in ten minutes",
            ))
        })
    };
    let control = async {
        let observed = async {
            let formed = until(BOUND, || held(side)).await?;
            anyhow::Ok(if formed {
                Some((served(side, reads).await?, stream(side).await?.0))
            } else {
                None
            })
        }
        .await;
        held_chunk.release().await?;
        observed?.ok_or_else(|| anyhow::anyhow!("no names chunk reached its hold"))
    };
    let (outcome, answers) = tokio::join!(run, control);
    let outcome = outcome?;
    ensure!(outcome.reset, "the run rebuilds: {outcome:?}");
    answers
}

// Pin row (not red-first). A redo from block 0 of a published chain, its names stream held at a
// chunk's commit on three workers: the lookup records, the reverse page and the family names all
// refuse with the publication unavailable. Before the redo and once it completes and its redo
// row clears, they serve.
#[tokio::test]
async fn nothing_serves_the_lookup_names_during_a_redo_names_stream() -> Result<()> {
    let fixture = seeded_with("families_unserved_redo", 4).await?;
    admitted(&fixture, 1, None).await?;
    fixture
        .apply_with(TARGET, FamilyMode::Rebuild, &options())
        .await?;
    let reads = reads(&fixture).await?;
    let side = side(&fixture).await?;
    require_served(&side, &reads, "published").await?;

    admitted(&fixture, 2, Some((0, TARGET, "operator redo"))).await?;
    let (answers, at) = read_at_hold(
        &fixture,
        &side,
        81,
        &families_support::marker(TARGET),
        FamilyMode::Redo {
            from: 0,
            to: TARGET,
        },
        &reads,
    )
    .await?;
    ensure!(
        at == Some(0) && answers == [None, None, None],
        "in the names stream ({at:?}) every reader refuses: {answers:?}"
    );
    ensure!(
        fixture.marker().await?.0 == Some(TARGET),
        "the redo completes"
    );
    admitted(&fixture, 2, None).await?;
    require_served(&side, &reads, "after the redo").await?;
    fixture.cleanup().await
}

// Pin row (not red-first). A live chain published by a rebuild in ranges, then a reorg that
// orphans the pass block: the normal run that follows the new branch rebuilds, and while its
// names stream is held at a chunk's commit on three workers, the lookup records, the reverse
// page and the family names all refuse with the publication unavailable. Before the reorg and
// once the new branch is published, they serve.
#[tokio::test]
async fn nothing_serves_the_lookup_names_during_a_reorg_rebuilds_names_stream() -> Result<()> {
    let fixture = seeded_with("families_unserved_reorg", 4).await?;
    admitted(&fixture, 1, None).await?;
    fixture
        .apply_with(TARGET, FamilyMode::Rebuild, &options())
        .await?;
    let reads = reads(&fixture).await?;
    let side = side(&fixture).await?;
    require_served(&side, &reads, "published").await?;

    let pass_block: Option<i64> =
        sqlx::query_scalar("SELECT composed_block FROM project_repair_record WHERE chain_id = $1")
            .bind(CHAIN)
            .fetch_one(&fixture.pool)
            .await?;
    let orphaned = pass_block.ok_or_else(|| anyhow::anyhow!("the rebuild ran no pass"))?;
    ensure!(
        0 < orphaned && orphaned < TARGET,
        "the pass block {orphaned}"
    );
    sqlx::query(
        "UPDATE chain_lineage SET canonicality_state = 'orphaned'
         WHERE chain_id = $1 AND block_number >= $2",
    )
    .bind(CHAIN)
    .bind(orphaned)
    .execute(&fixture.pool)
    .await?;
    for block in orphaned..=TARGET {
        let parent = if block == orphaned {
            hash(block - 1)
        } else {
            sibling(block - 1)
        };
        sqlx::query(
            "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                 block_timestamp, canonicality_state)
             VALUES ($1, $2, $3, $4, to_timestamp(1800000000 + $4 * 12), 'canonical')",
        )
        .bind(CHAIN)
        .bind(sibling(block))
        .bind(parent)
        .bind(block)
        .execute(&fixture.pool)
        .await?;
    }
    let target = Marker {
        number: TARGET,
        hash: sibling(TARGET),
    };
    let (answers, at) =
        read_at_hold(&fixture, &side, 82, &target, FamilyMode::Normal, &reads).await?;
    ensure!(
        at == Some(0) && answers == [None, None, None],
        "in the names stream ({at:?}) every reader refuses: {answers:?}"
    );
    ensure!(
        fixture.marker().await?.1.as_deref() == Some(sibling(TARGET).as_str()),
        "on the new branch"
    );
    let after = served(&side, &reads).await?;
    ensure!(
        after.iter().all(Option::is_some),
        "the new branch is published: {after:?}"
    );
    fixture.cleanup().await
}
