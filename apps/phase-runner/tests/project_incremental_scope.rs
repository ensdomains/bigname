//! Incremental Project scope against a full rebuild. Each case seeds the same interpreted events
//! into two scratch chains: the first publishes in two Normal batches (a full rebuild, then an
//! incremental batch that sees only the later events), the second publishes once from scratch at
//! the same block. The served rows the later events decide must agree between the two.
#[allow(dead_code)]
mod support;

use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context, Result, ensure};
use phase_runner::{
    INTERPRETER_CONTENT_HASH,
    heads::{BlockMarker, HeadMarkers, publish_heads},
    phase::{Phase, PhaseContext, PhaseName, PhaseResume, RunMode},
    project_phase::ProjectPhase,
    state::PhaseStore,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use support::ScratchDatabase;

const CHAIN: &str = "ethereum-sepolia";
const EPOCH: i64 = 1_800_000_000;
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const ETH: u64 = 0xe7;

fn hash(block: i64) -> String {
    format!("{CHAIN}-block-{block}")
}

fn word(n: u64) -> String {
    format!("0x{n:064x}")
}

fn address(n: u64) -> String {
    format!("0x{n:040x}")
}

fn uuid(n: u64) -> String {
    format!("00000000-0000-0000-0000-{n:012x}")
}

/// A scratch chain with canonical blocks `0..=blocks` and Ingest and Interpret complete, whose
/// Project publications run as the runner runs them.
struct Chain {
    scratch: ScratchDatabase,
    project: ProjectPhase,
    published: Option<BlockMarker>,
    log: AtomicI64,
}

impl Chain {
    async fn new(prefix: &str, blocks: i64) -> Result<Self> {
        let scratch = ScratchDatabase::create(prefix).await?;
        let pool = scratch.pool().clone();
        for number in 0..=blocks {
            sqlx::query(
                "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                     block_timestamp, canonicality_state)
                 VALUES ($1, $2, $3, $4, to_timestamp($5::bigint + $4), 'canonical')",
            )
            .bind(CHAIN)
            .bind(hash(number))
            .bind((number > 0).then(|| hash(number - 1)))
            .bind(number)
            .bind(EPOCH)
            .execute(&pool)
            .await?;
        }
        let store = PhaseStore::new(pool.clone());
        store.initialize_chain(CHAIN).await?;
        sqlx::query(
            "UPDATE chain_phase_state
             SET phase_status = 'completed', current_block_number = $2, current_block_hash = $3,
                 target_block_number = $2, target_block_hash = $3, input_content_hash = $4,
                 started_at = now(), finished_at = now(), updated_at = now()
             WHERE chain_id = $1 AND phase_name IN ('ingest', 'interpret')",
        )
        .bind(CHAIN)
        .bind(blocks)
        .bind(hash(blocks))
        .bind(INTERPRETER_CONTENT_HASH)
        .execute(&pool)
        .await?;
        store
            .start_phase(CHAIN, PhaseName::Project, &RunMode::Normal)
            .await?;
        Ok(Self {
            project: ProjectPhase::new(pool),
            scratch,
            published: None,
            log: AtomicI64::new(0),
        })
    }

    fn pool(&self) -> &PgPool {
        self.scratch.pool()
    }

    /// An active name surface. `labelhashes` run leaf first, as the interpreter stores them.
    async fn surface(
        &self,
        namehash: &str,
        raw_name: &str,
        labelhashes: &[String],
        block: i64,
    ) -> Result<String> {
        let logical = format!("ens:{namehash}");
        let labels: Vec<&str> = raw_name.split('.').collect();
        sqlx::query(
            "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
                 chain_id, block_hash, block_number, canonicality_state)
             VALUES ($1, 'ens', $2, $3, '\\x00', $4, $5, 'ensip15', 'active', $6, $7, $8,
                 'canonical')",
        )
        .bind(&logical)
        .bind(raw_name)
        .bind(&labels)
        .bind(namehash)
        .bind(labelhashes)
        .bind(CHAIN)
        .bind(hash(block))
        .bind(block)
        .execute(self.pool())
        .await
        .with_context(|| format!("surface {raw_name}"))?;
        Ok(logical)
    }

    /// A surface binding of a new resource to the name, with the SurfaceBound event the
    /// interpreter writes beside it.
    async fn binding(
        &self,
        binding_id: &str,
        logical: &str,
        resource_id: &str,
        binding_kind: &str,
        block: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO resources (resource_id, chain_id, block_hash, block_number,
                 canonicality_state)
             VALUES ($1::uuid, $2, $3, $4, 'canonical') ON CONFLICT DO NOTHING",
        )
        .bind(resource_id)
        .bind(CHAIN)
        .bind(hash(block))
        .bind(block)
        .execute(self.pool())
        .await?;
        sqlx::query(
            "INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
                 binding_kind, authority_arm, active_from, chain_id, block_hash, block_number,
                 provenance, canonicality_state)
             VALUES ($1::uuid, $2, $3::uuid, $4, 'ens_v2', to_timestamp($5::bigint + $8), $6,
                 $7, $8, '{\"transaction_index\":0,\"log_index\":0}', 'canonical')",
        )
        .bind(binding_id)
        .bind(logical)
        .bind(resource_id)
        .bind(binding_kind)
        .bind(EPOCH)
        .bind(CHAIN)
        .bind(hash(block))
        .bind(block)
        .execute(self.pool())
        .await
        .with_context(|| format!("binding {binding_id}"))?;
        self.event(
            &format!("bound-{binding_id}"),
            Some(logical),
            Some(resource_id),
            "ens_v2_registry_l1",
            "SurfaceBound",
            block,
            json!({"binding_kind": binding_kind}),
            &address(0xe0),
        )
        .await
    }

    /// A proof-checked, normalized label.
    async fn label(&self, labelhash: &str, label: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
                 normalized_under_version, normalization_error, source_kind, source_priority)
             VALUES ($1, convert_to($2, 'UTF8'), $2, 'ensip15', true, NULL, 'fixture', 0)",
        )
        .bind(labelhash)
        .bind(label)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// One interpreted event at `(block, log)`; log indexes are unique per chain.
    #[allow(clippy::too_many_arguments)]
    async fn event(
        &self,
        identity: &str,
        logical: Option<&str>,
        resource: Option<&str>,
        family: &str,
        kind: &str,
        block: i64,
        after: Value,
        emitter: &str,
    ) -> Result<()> {
        let log = self.log.fetch_add(1, Ordering::Relaxed);
        let derivation = match family {
            "ens_v1_registry_l1" => "ens_v1_unwrapped_authority",
            family if family.contains("resolver") => "ens_v2_resolver",
            _ => "ens_v2_registry_resource_surface",
        };
        sqlx::query(
            "INSERT INTO normalized_events (event_identity, namespace, logical_name_id,
                 resource_id, event_kind, source_family, manifest_version, chain_id, block_number,
                 block_hash, transaction_hash, transaction_index, log_index, derivation_kind,
                 canonicality_state, after_state, raw_fact_ref)
             VALUES ($1, 'ens', $2, $3::uuid, $4, $5, 1, $6, $7, $8, $9, 0, $10, $11,
                 'canonical', $12, jsonb_build_object('emitting_address', $13::text,
                     'event_identity', $1::text))",
        )
        .bind(identity)
        .bind(logical)
        .bind(resource)
        .bind(kind)
        .bind(family)
        .bind(CHAIN)
        .bind(block)
        .bind(hash(block))
        .bind(word(u64::try_from(block * 1_000)?))
        .bind(log)
        .bind(derivation)
        .bind(after)
        .bind(emitter)
        .execute(self.pool())
        .await
        .with_context(|| format!("event {identity}"))?;
        Ok(())
    }

    /// Publishes `number` as the runner does: the head moves there, one committed Normal batch
    /// resumes from the last publication (a full rebuild when there is none), and its progress
    /// is recorded.
    async fn publish(&mut self, number: i64) -> Result<()> {
        let pool = self.pool().clone();
        let target = BlockMarker::new(number, hash(number))?;
        let heads = HeadMarkers {
            latest: target.clone(),
            safe: None,
            finalized: None,
        };
        publish_heads(&pool, CHAIN, &heads).await?;
        let outcome = self
            .project
            .run_batch(PhaseContext {
                chain_id: CHAIN.to_owned(),
                phase: PhaseName::Project,
                mode: RunMode::Normal,
                redo_attempt: None,
                sources: Vec::new().into(),
                available_heads: Some(heads),
                live_handoff: None,
                resume: PhaseResume {
                    current: self.published.clone(),
                    ..PhaseResume::default()
                },
            })
            .await?;
        PhaseStore::new(pool)
            .record_progress(
                CHAIN,
                PhaseName::Project,
                &RunMode::Normal,
                None,
                outcome.progress(),
            )
            .await?;
        self.published = Some(target);
        Ok(())
    }

    /// Every `children_current` row, without its write timestamps and publication target: a row
    /// outside an incremental batch's scope keeps the target of the batch that wrote it.
    async fn children(&self) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT to_jsonb(row) - 'last_recomputed_at' - 'inserted_at'
                    || jsonb_build_object(
                        'canonicality_summary',
                        canonicality_summary - 'target_block_hash' - 'target_block_number',
                        'chain_positions',
                        chain_positions - 'target_block_hash' - 'target_block_number'
                    )
             FROM children_current row
             ORDER BY parent_logical_name_id, child_logical_name_id",
        )
        .fetch_all(self.pool())
        .await?)
    }

    async fn topology(&self, logical: &str) -> Result<Option<Value>> {
        Ok(sqlx::query_scalar(
            "SELECT declared_summary -> 'topology' FROM name_current WHERE logical_name_id = $1",
        )
        .bind(logical)
        .fetch_optional(self.pool())
        .await?
        .flatten())
    }

    async fn cleanup(self) -> Result<()> {
        self.scratch.cleanup().await
    }
}

/// `first.eth` with two child edges from the ENSv1 registry: `live` and `surfaced`, whose name
/// surface is already known. Neither event carries a logical name, as the adapter writes a
/// NewOwner for a child it has not attributed yet.
async fn children_before(chain: &Chain) -> Result<(String, String)> {
    let first_node = word(0x1001);
    let first = chain
        .surface(&first_node, "first.eth", &[word(0x2001), word(ETH)], 1)
        .await?;
    chain.label(&word(0x5001), "live").await?;
    chain.label(&word(0x5008), "surfaced").await?;
    let surfaced = chain
        .surface(
            &word(8),
            "surfaced.first.eth",
            &[word(0x5008), word(0x2001), word(ETH)],
            1,
        )
        .await?;
    for (identity, child) in [("live", 1), ("surfaced", 8)] {
        chain
            .event(
                identity,
                None,
                None,
                "ens_v1_registry_l1",
                "SubregistryChanged",
                2,
                json!({"source_event": "NewOwner", "node": first_node, "child_node": word(child),
                       "labelhash": word(0x5000 + child), "owner": address(0xa000 + child)}),
                &address(0xe1),
            )
            .await?;
    }
    Ok((first, surfaced))
}

/// A registry Transfer of the surfaced child to the zero address. The event carries `node` and
/// no logical name, the shape the adapter writes when it has not linked the node's authority.
async fn children_after(chain: &Chain) -> Result<()> {
    chain
        .event(
            "surfaced-zero",
            None,
            None,
            "ens_v1_registry_l1",
            "AuthorityTransferred",
            8,
            json!({"source_event": "Transfer", "node": word(8), "owner": ZERO_ADDRESS,
                   "owner_getter": ZERO_ADDRESS, "emitter_role": "registry"}),
            &address(0xe1),
        )
        .await
}

// A child edge whose node is transferred to the zero address, by an event without a logical
// name, leaves the served children in the incremental batch as it does in a rebuild.
#[tokio::test]
async fn zero_transfer_of_a_surfaced_child_matches_a_rebuild() -> Result<()> {
    let mut incremental = Chain::new("scope_children_incremental", 12).await?;
    let mut rebuilt = Chain::new("scope_children_rebuild", 12).await?;
    let (first, surfaced) = children_before(&incremental).await?;
    children_before(&rebuilt).await?;
    incremental.publish(6).await?;
    let before: Vec<String> = sqlx::query_scalar(
        "SELECT child_logical_name_id FROM children_current
         WHERE parent_logical_name_id = $1 ORDER BY 1",
    )
    .bind(&first)
    .fetch_all(incremental.pool())
    .await?;
    ensure!(
        before.contains(&surfaced) && before.len() == 2,
        "{before:?}"
    );

    children_after(&incremental).await?;
    children_after(&rebuilt).await?;
    incremental.publish(9).await?;
    rebuilt.publish(9).await?;
    let served = incremental.children().await?;
    let expected = rebuilt.children().await?;
    ensure!(
        !expected
            .iter()
            .any(|row| row["child_logical_name_id"] == json!(surfaced)),
        "the rebuild keeps the zero-transferred child: {expected:#?}"
    );
    ensure!(
        served == expected,
        "incremental children differ from the rebuild:\nincremental {served:#?}\nrebuild {expected:#?}"
    );
    incremental.cleanup().await?;
    rebuilt.cleanup().await
}

/// `wild.eth`, bound through the registry, points at a resolver at block 2 and bumps its record
/// version at block 3; `sub.wild.eth` is reached only as its wildcard descendant.
async fn wildcard_before(chain: &Chain, resolver: &str) -> Result<(String, String)> {
    let ancestor = chain
        .surface(&word(0x1004), "wild.eth", &[word(0x2004), word(ETH)], 1)
        .await?;
    let ancestor_resource = uuid(0xa004);
    chain
        .binding(
            &uuid(0xb004),
            &ancestor,
            &ancestor_resource,
            "declared_registry_path",
            1,
        )
        .await?;
    let wildcard = chain
        .surface(
            &word(0x1005),
            "sub.wild.eth",
            &[word(0x2005), word(0x2004), word(ETH)],
            1,
        )
        .await?;
    chain
        .binding(
            &uuid(0xb005),
            &wildcard,
            &uuid(0xa005),
            "observed_wildcard_path",
            1,
        )
        .await?;
    chain
        .event(
            "wild-point",
            Some(&ancestor),
            Some(&ancestor_resource),
            "ens_v2_registry_l1",
            "ResolverChanged",
            2,
            json!({"resolver": resolver}),
            &address(0xe3),
        )
        .await?;
    chain
        .event(
            "wild-version",
            Some(&ancestor),
            Some(&ancestor_resource),
            "ens_v2_resolver_l1",
            "RecordVersionChanged",
            3,
            json!({"resolver": resolver, "node": word(0x1004), "version": 1}),
            resolver,
        )
        .await?;
    Ok((ancestor, wildcard))
}

/// The ancestor's pointer is cleared at block 7; nothing names the wildcard descendant.
async fn wildcard_after(chain: &Chain, ancestor: &str) -> Result<()> {
    chain
        .event(
            "wild-zero",
            Some(ancestor),
            Some(&uuid(0xa004)),
            "ens_v2_registry_l1",
            "ResolverChanged",
            7,
            json!({"resolver": ZERO_ADDRESS}),
            &address(0xe3),
        )
        .await
}

const BOUNDARY_BLOCK: &str =
    "/version_boundaries/topology_version_boundary/chain_position/block_number";

// A wildcard name's version boundary follows its ancestor's later zero pointer in the
// incremental batch as it does in a rebuild, and keeps the ancestor's last non-zero resolver.
#[tokio::test]
async fn wildcard_boundary_follows_an_ancestor_pointer_change_like_a_rebuild() -> Result<()> {
    let resolver = address(0xc3);
    let mut incremental = Chain::new("scope_wildcard_incremental", 12).await?;
    let mut rebuilt = Chain::new("scope_wildcard_rebuild", 12).await?;
    let (ancestor, wildcard) = wildcard_before(&incremental, &resolver).await?;
    wildcard_before(&rebuilt, &resolver).await?;
    incremental.publish(4).await?;
    let before = incremental
        .topology(&wildcard)
        .await?
        .context("the wildcard name has no topology at block 4")?;
    ensure!(
        before.pointer(BOUNDARY_BLOCK) == Some(&json!(3)),
        "{before}"
    );

    wildcard_after(&incremental, &ancestor).await?;
    wildcard_after(&rebuilt, &ancestor).await?;
    incremental.publish(8).await?;
    rebuilt.publish(8).await?;
    let expected = rebuilt
        .topology(&wildcard)
        .await?
        .context("the rebuild serves no wildcard topology")?;
    ensure!(
        expected.pointer(BOUNDARY_BLOCK) == Some(&json!(7))
            && expected.pointer("/resolver_path/0/address") == Some(&json!(resolver)),
        "{expected}"
    );
    let served = incremental.topology(&wildcard).await?;
    ensure!(
        served.as_ref() == Some(&expected),
        "incremental wildcard topology differs from the rebuild:\nincremental {served:#?}\nrebuild {expected:#}"
    );
    incremental.cleanup().await?;
    rebuilt.cleanup().await
}
