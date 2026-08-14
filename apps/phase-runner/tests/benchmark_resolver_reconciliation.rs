#[allow(dead_code)]
mod support;

use anyhow::Result;
use bigname_manifests::{load_repository, sync_schema_v2_repository};
use phase_runner::{INTERPRETER_CONTENT_HASH, state::PhaseStore};

use support::ScratchDatabase;

// Exercise the benchmark gate's exact coverage query from this integration test so the
// fixture can drive the real manifest synchronization path without adding a benchmark-crate
// dependency edge or changing Cargo.lock.
#[allow(dead_code)]
mod api_load {
    #[derive(Clone, Debug)]
    pub struct ResolverManifestCoverage {
        pub chain_id: String,
        pub source_family: String,
        pub declared_addresses: usize,
        pub applicable_addresses: usize,
        pub exercised_addresses: usize,
    }

    pub mod workload {
        #[derive(Clone, Debug)]
        pub struct ResolverTarget {
            pub chain_id: String,
            pub source_family: String,
            pub resolver_address: String,
        }
    }

    mod resolver_coverage {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../tools/benchmark-gate/src/api_load/corpus/resolver_coverage.rs"
        ));
    }

    pub async fn resolver_coverage_failures(pool: &sqlx::PgPool) -> anyhow::Result<Vec<String>> {
        Ok(resolver_coverage::load(pool).await?.failures)
    }
}

#[tokio::test]
async fn swallowed_second_deprecation_is_rejected_by_resolver_coverage() -> Result<()> {
    let scratch = ScratchDatabase::create("benchmark_resolver_manifest_cycle").await?;
    sync_resolver_manifest_cycle(&scratch, true).await?;
    publish_project_heads(&scratch).await?;
    seed_healthy_basenames_resolver(&scratch).await?;

    let failures = api_load::resolver_coverage_failures(scratch.pool()).await?;

    assert!(
        failures.iter().any(|failure| {
            failure.contains("Project admits \"ens_v1_resolver_l1\"")
                && failure.contains("stored manifest row")
                && failure.contains("not active")
                && failure.contains("stored version 1")
                && failure.contains("latest event version 1")
        }),
        "resolver family admitted only by the latest event was silently omitted: {failures:?}"
    );
    scratch.cleanup().await
}

#[tokio::test]
async fn resolver_family_deprecated_on_both_sides_stays_outside_coverage() -> Result<()> {
    let scratch = ScratchDatabase::create("benchmark_resolver_manifest_deprecated").await?;
    sync_resolver_manifest_cycle(&scratch, false).await?;
    publish_project_heads(&scratch).await?;
    seed_healthy_basenames_resolver(&scratch).await?;

    let failures = api_load::resolver_coverage_failures(scratch.pool()).await?;

    assert!(failures.is_empty(), "{failures:?}");
    scratch.cleanup().await
}

async fn sync_resolver_manifest_cycle(
    scratch: &ScratchDatabase,
    swallow_second_deprecation: bool,
) -> Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let full_repository = load_repository(&root)?;
    let base_repository = load_repository(root.join("base"))?;

    sync_schema_v2_repository(scratch.pool(), &full_repository).await?;
    seed_chain_head(scratch.pool(), "ethereum-mainnet", 30_000_000).await?;
    seed_chain_head(scratch.pool(), "base-mainnet", 30_000_000).await?;
    sync_schema_v2_repository(scratch.pool(), &base_repository).await?;

    if swallow_second_deprecation {
        sync_schema_v2_repository(scratch.pool(), &full_repository).await?;
        advance_chain_head(scratch.pool(), "ethereum-mainnet", 30_000_001).await?;
        sync_schema_v2_repository(scratch.pool(), &base_repository).await?;

        let split: (String, String) = sqlx::query_as(
            "SELECT manifest.rollout_status,
                    event.after_state ->> 'rollout_status'
             FROM manifest_versions manifest
             JOIN LATERAL (
                 SELECT after_state
                 FROM normalized_events
                 WHERE source_manifest_id = manifest.manifest_id
                   AND event_kind = 'SourceManifestUpdated'
                 ORDER BY normalized_event_id DESC
                 LIMIT 1
             ) event ON TRUE
             WHERE manifest.chain_id = 'ethereum-mainnet'
               AND manifest.source_family = 'ens_v1_resolver_l1'",
        )
        .fetch_one(scratch.pool())
        .await?;
        assert_eq!(split, ("deprecated".to_owned(), "active".to_owned()));
    }
    Ok(())
}

async fn publish_project_heads(scratch: &ScratchDatabase) -> Result<()> {
    let store = PhaseStore::new(scratch.pool().clone());
    for chain_id in ["ethereum-mainnet", "base-mainnet"] {
        store.initialize_chain(chain_id).await?;
        sqlx::query(
            "UPDATE chain_phase_state project
             SET phase_status = 'completed',
                 current_block_number = head.latest_block_number,
                 current_block_hash = head.latest_block_hash,
                 input_content_hash = $2,
                 started_at = now(),
                 finished_at = now()
             FROM chain_heads head
             WHERE project.chain_id = $1
               AND project.phase_name = 'project'
               AND head.chain_id = project.chain_id",
        )
        .bind(chain_id)
        .bind(INTERPRETER_CONTENT_HASH)
        .execute(scratch.pool())
        .await?;
    }
    Ok(())
}

async fn seed_healthy_basenames_resolver(scratch: &ScratchDatabase) -> Result<()> {
    let (manifest_id, manifest_version, resolver_address, manifest_event_id): (
        i64,
        i64,
        String,
        i64,
    ) = sqlx::query_as(
        "SELECT manifest.manifest_id,
                manifest.manifest_version,
                lower(contract ->> 'address'),
                max(event.normalized_event_id)
         FROM manifest_versions manifest
         CROSS JOIN LATERAL jsonb_array_elements(manifest.manifest_payload -> 'contracts') contract
         JOIN normalized_events event
           ON event.source_manifest_id = manifest.manifest_id
          AND event.event_kind = 'SourceManifestUpdated'
         WHERE manifest.chain_id = 'base-mainnet'
           AND manifest.source_family = 'basenames_base_resolver'
           AND manifest.rollout_status = 'active'
         GROUP BY manifest.manifest_id, manifest.manifest_version, contract ->> 'address'
         ORDER BY contract ->> 'address'
         LIMIT 1",
    )
    .fetch_one(scratch.pool())
    .await?;
    let (target_block_number, target_block_hash): (i64, String) = sqlx::query_as(
        "SELECT latest_block_number, latest_block_hash
         FROM chain_heads
         WHERE chain_id = 'base-mainnet'",
    )
    .fetch_one(scratch.pool())
    .await?;

    sqlx::query(
        "INSERT INTO resolver_current (
             chain_id, resolver_address, support_status, chain_positions,
             canonicality_summary, provenance, manifest_version
         ) VALUES (
             'base-mainnet', $1, 'supported',
             jsonb_build_object(
                 'target_block_number', $2::bigint,
                 'target_block_hash', $3::text
             ),
             '{\"state\":\"canonical_lineage\"}',
             jsonb_build_object(
                 'manifest_id', $4::bigint,
                 'manifest_event_id', $5::bigint
             ),
             $6
         )",
    )
    .bind(resolver_address)
    .bind(target_block_number)
    .bind(target_block_hash)
    .bind(manifest_id)
    .bind(manifest_event_id)
    .bind(manifest_version)
    .execute(scratch.pool())
    .await?;
    Ok(())
}

async fn seed_chain_head(pool: &sqlx::PgPool, chain_id: &str, number: i64) -> Result<()> {
    let hash = format!("{chain_id}-benchmark-manifest-head-{number}");
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, to_timestamp($3), 'canonical')",
    )
    .bind(chain_id)
    .bind(&hash)
    .bind(number)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO chain_heads (chain_id, latest_block_hash, latest_block_number)
         VALUES ($1, $2, $3)",
    )
    .bind(chain_id)
    .bind(hash)
    .bind(number)
    .execute(pool)
    .await?;
    Ok(())
}

async fn advance_chain_head(pool: &sqlx::PgPool, chain_id: &str, number: i64) -> Result<()> {
    let hash = format!("{chain_id}-benchmark-manifest-head-{number}");
    sqlx::query(
        "INSERT INTO chain_lineage (
             chain_id, block_hash, block_number, block_timestamp, canonicality_state
         ) VALUES ($1, $2, $3, to_timestamp($3), 'canonical')",
    )
    .bind(chain_id)
    .bind(&hash)
    .bind(number)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE chain_heads
         SET latest_block_hash = $2, latest_block_number = $3
         WHERE chain_id = $1",
    )
    .bind(chain_id)
    .bind(hash)
    .bind(number)
    .execute(pool)
    .await?;
    Ok(())
}
