//! A scratch chain for the step 5 shadow fixtures: identity rows and interpreted events are
//! seeded directly, Ingest and Interpret are marked complete, and every publication runs as the
//! runner runs it: one committed Normal Project batch, its progress recorded, then the owned key
//! families. [`Fixture::compare`] then runs the shadow comparison at that publication.
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

use crate::{shadow, support::ScratchDatabase};

pub const CHAIN: &str = "ethereum-sepolia";
/// Lineage timestamps start here, so expiry fences at the block clock are in the future of 2026.
pub const EPOCH: i64 = 1_800_000_000;
pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
pub const ZERO_NODE: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

pub fn hash(block: i64) -> String {
    format!("{CHAIN}-block-{block}")
}

/// A 32-byte hex word from a number, for nodes, labelhashes and transaction hashes.
pub fn word(n: u64) -> String {
    format!("0x{n:064x}")
}

pub fn address(n: u64) -> String {
    format!("0x{n:040x}")
}

pub fn uuid(n: u64) -> String {
    format!("00000000-0000-0000-0000-{n:012x}")
}

pub struct Fixture {
    scratch: ScratchDatabase,
    published: Option<BlockMarker>,
    project: ProjectPhase,
    log: AtomicI64,
}

impl Fixture {
    /// A chain with observed blocks `0..=blocks` that each publication promotes along its path,
    /// and Ingest and Interpret complete through the last.
    pub async fn new(prefix: &str, blocks: i64) -> Result<Self> {
        let scratch = ScratchDatabase::create(prefix).await?;
        let pool = scratch.pool().clone();
        // Observed, not canonical: each publication promotes its own path. A canonical block
        // above the published head would be orphaned by the publication, which stamps an
        // Interpret redo, and the families do not apply while Interpret is in redo.
        for number in 0..=blocks {
            sqlx::query(
                "INSERT INTO chain_lineage (chain_id, block_hash, parent_hash, block_number,
                     block_timestamp, canonicality_state)
                 VALUES ($1, $2, $3, $4, to_timestamp($5::bigint + $4), 'observed')",
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

    pub fn pool(&self) -> &PgPool {
        self.scratch.pool()
    }

    /// An active name surface. `labelhashes` run leaf first, as the interpreter stores them.
    pub async fn surface(
        &self,
        namespace: &str,
        namehash: &str,
        raw_name: &str,
        labelhashes: &[String],
        block: i64,
    ) -> Result<String> {
        let logical = format!("{namespace}:{namehash}");
        let labels: Vec<&str> = if raw_name.is_empty() {
            Vec::new()
        } else {
            raw_name.split('.').collect()
        };
        sqlx::query(
            "INSERT INTO name_surfaces (logical_name_id, namespace, raw_name, raw_labels,
                 dns_encoded_name, namehash, labelhashes, normalizer_version, visibility_state,
                 chain_id, block_hash, block_number, canonicality_state)
             VALUES ($1, $2, $3, $4, '\\x00', $5, $6, 'ensip15', 'active', $7, $8, $9,
                 'canonical')",
        )
        .bind(&logical)
        .bind(namespace)
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

    pub async fn resource(&self, resource_id: &str, block: i64) -> Result<()> {
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
        Ok(())
    }

    /// A surface binding of `resource_id` to the name.
    pub async fn binding(
        &self,
        binding_id: &str,
        logical: &str,
        resource_id: &str,
        binding_kind: &str,
        authority_arm: &str,
        block: i64,
    ) -> Result<()> {
        self.resource(resource_id, block).await?;
        sqlx::query(
            "INSERT INTO surface_bindings (surface_binding_id, logical_name_id, resource_id,
                 binding_kind, authority_arm, active_from, chain_id, block_hash, block_number,
                 provenance, canonicality_state)
             VALUES ($1::uuid, $2, $3::uuid, $4, $5, to_timestamp($6::bigint + $9), $7, $8, $9,
                 '{\"transaction_index\":0,\"log_index\":0}', 'canonical')",
        )
        .bind(binding_id)
        .bind(logical)
        .bind(resource_id)
        .bind(binding_kind)
        .bind(authority_arm)
        .bind(EPOCH)
        .bind(CHAIN)
        .bind(hash(block))
        .bind(block)
        .execute(self.pool())
        .await
        .with_context(|| format!("binding {binding_id}"))?;
        // The interpreter writes a binding with its SurfaceBound; the families read a block's
        // bindings only when the block has events.
        let family = match authority_arm {
            "ens_v2" => "ens_v2_registry_l1",
            _ => "ens_v1_registry_l1",
        };
        self.event(
            &format!("bound-{binding_id}"),
            Some(logical),
            Some(resource_id),
            family,
            "SurfaceBound",
            block,
            serde_json::json!({"binding_kind": binding_kind}),
            &address(0xe0),
        )
        .await?;
        Ok(())
    }

    /// A contract instance with one address active from `block`.
    pub async fn contract(&self, instance_id: &str, contract: &str, block: i64) -> Result<()> {
        sqlx::query(
            "INSERT INTO contract_instances (contract_instance_id, chain_id, contract_kind)
             VALUES ($1::uuid, $2, 'contract') ON CONFLICT DO NOTHING",
        )
        .bind(instance_id)
        .bind(CHAIN)
        .execute(self.pool())
        .await?;
        sqlx::query(
            "INSERT INTO contract_instance_addresses (contract_instance_id, chain_id, address,
                 active_from_block_number)
             VALUES ($1::uuid, $2, $3, $4)",
        )
        .bind(instance_id)
        .bind(CHAIN)
        .bind(contract)
        .bind(block)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// An active manifest declaring `addresses` as resolvers of `source_family`, with its
    /// SourceManifestUpdated event, as the rebuild-performance seed declares its resolvers. The
    /// resolver builder then serves a `resolver_current` row for each address.
    pub async fn declare_resolvers(&self, source_family: &str, addresses: &[&str]) -> Result<()> {
        let contracts: Vec<(&str, &str)> = addresses
            .iter()
            .map(|address| (*address, "resolver"))
            .collect();
        self.declare_contracts(source_family, &contracts, None)
            .await
    }

    /// An active manifest declaring `(address, role)` contracts of `source_family`, with the ENSv1
    /// registry a mirror resolver reads when `mirrored_registry` is given.
    pub async fn declare_contracts(
        &self,
        source_family: &str,
        contracts: &[(&str, &str)],
        mirrored_registry: Option<&str>,
    ) -> Result<()> {
        let contracts: Vec<Value> = contracts
            .iter()
            .map(|(address, role)| {
                json!({"role": role, "address": address, "proxy_kind": "none",
                       "start_block": 0})
            })
            .collect();
        let mut payload = json!({"deployment_epoch": "fixture", "contracts": contracts});
        if let Some(registry) = mirrored_registry {
            payload["correlation_addresses"] = json!({"ens_v1_registry": registry});
        }
        sqlx::query(
            "WITH manifest AS (
                 INSERT INTO manifest_versions (manifest_version, namespace, source_family,
                     chain_id, deployment_label, rollout_status, normalizer_version, file_path,
                     manifest_payload)
                 VALUES (1, 'ens', $2, $1, 'fixture', 'active', 'fixture',
                     'fixture/shadow-resolvers.toml', $3)
                 RETURNING manifest_id, manifest_payload)
             INSERT INTO normalized_events (event_identity, namespace, event_kind, source_family,
                 manifest_version, source_manifest_id, chain_id, derivation_kind,
                 canonicality_state, after_state)
             SELECT 'fixture:manifest:' || manifest_id, 'ens', 'SourceManifestUpdated', $2, 1,
                    manifest_id,
                    $1, 'manifest_sync', 'canonical'::canonicality_state,
                    jsonb_build_object('rollout_status', 'active', 'normalizer_version', 'fixture',
                        'manifest_payload', manifest_payload)
             FROM manifest",
        )
        .bind(CHAIN)
        .bind(source_family)
        .bind(payload)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// A proof-checked label; `normalized` false stores a normalization error.
    pub async fn label(&self, labelhash: &str, label: &str, normalized: bool) -> Result<()> {
        sqlx::query(
            "INSERT INTO label_preimages (labelhash, raw_label, decoded_label, normalizer_version,
                 normalized_under_version, normalization_error, source_kind, source_priority)
             VALUES ($1, convert_to($2, 'UTF8'), $2, 'ensip15', $3,
                 CASE WHEN $3 THEN NULL ELSE 'disallowed character' END, 'fixture', 0)",
        )
        .bind(labelhash)
        .bind(label)
        .bind(normalized)
        .execute(self.pool())
        .await?;
        Ok(())
    }

    /// One interpreted event at `(block, log)`, transaction index 0 unless `transaction` says
    /// otherwise. Log indexes are unique per fixture unless given.
    #[allow(clippy::too_many_arguments)]
    pub async fn event(
        &self,
        identity: &str,
        logical: Option<&str>,
        resource: Option<&str>,
        family: &str,
        kind: &str,
        block: i64,
        after: Value,
        emitter: &str,
    ) -> Result<i64> {
        let log = self.log.fetch_add(1, Ordering::Relaxed);
        self.event_at(
            identity, logical, resource, family, kind, block, 0, log, after, emitter,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn event_at(
        &self,
        identity: &str,
        logical: Option<&str>,
        resource: Option<&str>,
        family: &str,
        kind: &str,
        block: i64,
        transaction: i64,
        log: i64,
        after: Value,
        emitter: &str,
    ) -> Result<i64> {
        let derivation = match family {
            "ens_v1_registry_l1" | "ens_v1_wrapper_l1" => "ens_v1_unwrapped_authority",
            "ens_v2_migration_l1" => "ens_v2_migration",
            family if family.contains("resolver") => "ens_v2_resolver",
            family if family.contains("permissions") => "ens_v2_permissions",
            _ => "ens_v2_registry_resource_surface",
        };
        sqlx::query_scalar(
            "INSERT INTO normalized_events (event_identity, namespace, logical_name_id,
                 resource_id, event_kind, source_family, manifest_version, chain_id, block_number,
                 block_hash, transaction_hash, transaction_index, log_index, derivation_kind,
                 canonicality_state, after_state, raw_fact_ref)
             VALUES ($1, 'ens', $2, $3::uuid, $4, $5, 1, $6, $7, $8, $9, $10, $11, $12,
                 'canonical', $13, jsonb_build_object('emitting_address', $14::text,
                     'event_identity', $1::text))
             RETURNING normalized_event_id",
        )
        .bind(identity)
        .bind(logical)
        .bind(resource)
        .bind(kind)
        .bind(family)
        .bind(CHAIN)
        .bind(block)
        .bind(hash(block))
        .bind(word(u64::try_from(block * 1_000 + transaction)?))
        .bind(transaction)
        .bind(log)
        .bind(derivation)
        .bind(after)
        .bind(emitter)
        .fetch_one(self.pool())
        .await
        .with_context(|| format!("event {identity}"))
    }

    /// Publishes `number` as the runner does: the head moves there, one committed Normal batch
    /// resumes from the last publication, its progress is recorded, then the families follow.
    pub async fn publish(&mut self, number: i64) -> Result<()> {
        let pool = self.pool().clone();
        let target = BlockMarker::new(number, hash(number))?;
        publish_heads(
            &pool,
            CHAIN,
            &HeadMarkers {
                latest: target.clone(),
                safe: None,
                finalized: None,
            },
        )
        .await?;
        let outcome = self
            .project
            .run_batch(PhaseContext {
                chain_id: CHAIN.to_owned(),
                phase: PhaseName::Project,
                mode: RunMode::Normal,
                redo_attempt: None,
                sources: Vec::new().into(),
                available_heads: Some(HeadMarkers {
                    latest: target.clone(),
                    safe: None,
                    finalized: None,
                }),
                live_handoff: None,
                resume: PhaseResume {
                    current: self.published.clone(),
                    ..PhaseResume::default()
                },
            })
            .await?;
        PhaseStore::new(pool.clone())
            .record_progress(
                CHAIN,
                PhaseName::Project,
                &RunMode::Normal,
                None,
                outcome.progress(),
            )
            .await?;
        self.project.after_progress_recorded(CHAIN).await;
        let family: Option<i64> = sqlx::query_scalar(
            "SELECT current_block_number FROM project_family_marker WHERE chain_id = $1",
        )
        .bind(CHAIN)
        .fetch_optional(&pool)
        .await?
        .flatten();
        ensure!(
            family == Some(number),
            "the families stopped at {family:?}, not at {number}"
        );
        self.published = Some(target);
        Ok(())
    }

    /// Rebuilds the current publication from scratch, as the benchmark's rebuild comparison does.
    pub async fn rebuild(&mut self) -> Result<()> {
        let published = self.published.take().context("nothing is published")?;
        let result = self.publish(published.number).await;
        if result.is_err() {
            self.published = Some(published);
        }
        result
    }

    /// The shadow comparison at the current publication, over small pages and every filter.
    pub async fn compare(&self, page: u64) -> Result<shadow::Report> {
        self.compare_with_prefixes(page, &[]).await
    }

    /// The comparison with a fenced name-ordered page per prefix too.
    pub async fn compare_with_prefixes(
        &self,
        page: u64,
        prefixes: &'static [&'static str],
    ) -> Result<shadow::Report> {
        let report = shadow::compare(
            self.pool(),
            CHAIN,
            shadow::Settings {
                children_page: page,
                collection_page: page,
                every_child_filter: true,
                prefixes,
            },
        )
        .await?;
        eprintln!("{}", report.line());
        Ok(report)
    }

    pub async fn cleanup(self) -> Result<()> {
        self.scratch.cleanup().await
    }
}

/// Mismatches other than the named expected differences. Each name is an exact mismatch key, and
/// every named key must still differ, so a fixed reducer or reader turns the test red until the
/// name is removed. A key only says which read differs, and the comparison stops at a key's first
/// mismatch, so this check alone does not see a second regression on the same key. A fixture that
/// names an expected product difference must also check the whole read itself, as
/// `DifferingLink` does for the one such difference (families_shadow_resolver.rs). The mutation
/// checks that name a key only need it to show that the comparison notices a change.
/// A declaration-manifest classification whose mirror disagrees with the served one always fails.
pub fn unexpected(report: &shadow::Report, expected: &[String]) -> Result<()> {
    ensure!(
        report.f3_unfilled_mirror_differs == 0,
        "the declaration fallback serves a different mirror: {:#}",
        shadow::describe(report)
    );
    for key in expected {
        ensure!(
            report
                .mismatches
                .iter()
                .any(|mismatch| &mismatch.key == key),
            "expected difference {key:?} is gone: {:#}",
            shadow::describe(report)
        );
    }
    let unexpected: Vec<String> = report
        .mismatches
        .iter()
        .filter(|mismatch| !expected.contains(&mismatch.key))
        .map(ToString::to_string)
        .collect();
    ensure!(
        unexpected.is_empty(),
        "the family readers differ from the served readers: {unexpected:#?}"
    );
    Ok(())
}

/// The resolvers whose classification row has no served row and reads
/// `resolver_manifest_not_active`, step 2's declared F3 approximation, are exactly `expected`.
pub fn extra_not_active(report: &shadow::Report, expected: &[&str]) -> Result<()> {
    let seen: Vec<&str> = report
        .f3_extra_not_active
        .iter()
        .map(String::as_str)
        .collect();
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    ensure!(
        seen == expected,
        "extra resolver_manifest_not_active rows {seen:?}, expected {expected:?}"
    );
    Ok(())
}
