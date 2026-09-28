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
async fn repeated_deprecation_stays_aligned_and_resolver_coverage_stays_clean() -> Result<()> {
    let scratch = ScratchDatabase::create("benchmark_resolver_manifest_cycle").await?;
    sync_resolver_manifest_cycle(&scratch).await?;
    assert_eq!(
        resolver_manifest_statuses(&scratch).await?,
        ("deprecated".to_owned(), "deprecated".to_owned())
    );
    publish_project_heads(&scratch).await?;

    let failures = api_load::resolver_coverage_failures(scratch.pool()).await?;

    assert!(failures.is_empty(), "{failures:?}");
    scratch.cleanup().await
}

#[tokio::test]
async fn manufactured_swallowed_deprecation_is_rejected_by_resolver_coverage() -> Result<()> {
    let scratch = ScratchDatabase::create("benchmark_resolver_manifest_deprecated").await?;
    sync_resolver_manifest_cycle(&scratch).await?;
    sqlx::query(
        "DELETE FROM normalized_events
         WHERE normalized_event_id = (
             SELECT max(event.normalized_event_id)
             FROM normalized_events event
             JOIN manifest_versions manifest
               ON manifest.manifest_id = event.source_manifest_id
             WHERE manifest.chain_id = 'ethereum-mainnet'
               AND manifest.source_family = 'ens_v1_resolver_l1'
               AND event.event_kind = 'SourceManifestUpdated'
         )",
    )
    .execute(scratch.pool())
    .await?;
    assert_eq!(
        resolver_manifest_statuses(&scratch).await?,
        ("deprecated".to_owned(), "active".to_owned())
    );
    publish_project_heads(&scratch).await?;

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

async fn sync_resolver_manifest_cycle(scratch: &ScratchDatabase) -> Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("manifests/mainnet");
    let full_repository = load_repository(&root)?;
    let base_repository = load_repository(root.join("base"))?;

    sync_schema_v2_repository(scratch.pool(), &full_repository).await?;
    seed_chain_head(scratch.pool(), "ethereum-mainnet", 30_000_000).await?;
    seed_chain_head(scratch.pool(), "base-mainnet", 30_000_000).await?;
    sync_schema_v2_repository(scratch.pool(), &base_repository).await?;
    sync_schema_v2_repository(scratch.pool(), &full_repository).await?;
    advance_chain_head(scratch.pool(), "ethereum-mainnet", 30_000_001).await?;
    sync_schema_v2_repository(scratch.pool(), &base_repository).await?;
    // Basenames classification is reached by a registry pointer; the declaration alone
    // is intentionally not a resolver-discovery edge.
    sqlx::query(
        "INSERT INTO normalized_events (
             event_identity, namespace, event_kind, source_family, manifest_version,
             chain_id, block_number, block_hash, derivation_kind, canonicality_state, after_state
         ) SELECT 'benchmark-base-pointer', 'basenames', 'ResolverChanged',
                  'basenames_base_registry', 1, head.chain_id, head.latest_block_number,
                  head.latest_block_hash, 'ens_v1_unwrapped_authority', 'canonical',
                  jsonb_build_object('resolver', lower(contract ->> 'address'))
           FROM manifest_versions manifest
           JOIN chain_heads head ON head.chain_id = manifest.chain_id
           CROSS JOIN LATERAL jsonb_array_elements(manifest.manifest_payload -> 'contracts') contract
           WHERE manifest.source_family = 'basenames_base_resolver'
           LIMIT 1",
    ).execute(scratch.pool()).await?;
    Ok(())
}

async fn resolver_manifest_statuses(scratch: &ScratchDatabase) -> Result<(String, String)> {
    Ok(sqlx::query_as(
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
    .await?)
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
        let (number, hash) = sqlx::query_as::<_, (i64, String)>(
            "SELECT latest_block_number, latest_block_hash FROM chain_heads WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_one(scratch.pool())
        .await?;
        let target = bigname_project::Marker { number, hash };
        let token = bigname_project::families::input_token(scratch.pool(), chain_id).await?;
        let outcome = bigname_project::families::apply(
            scratch.pool(),
            chain_id,
            &target,
            bigname_project::families::FamilyMode::Rebuild,
            &token,
            &bigname_project::families::FamilyOptions::new(INTERPRETER_CONTENT_HASH),
        )
        .await?;
        assert_eq!(outcome.marker.as_ref(), Some(&target));
        assert!(!outcome.budget_exhausted);
    }
    Ok(())
}

async fn seed_chain_head(pool: &sqlx::PgPool, chain_id: &str, number: i64) -> Result<()> {
    let hash = format!("{chain_id}-benchmark-manifest-head-{number}");
    sqlx::query(
        "INSERT INTO chain_lineage (chain_id, block_hash, block_number, block_timestamp, canonicality_state)
         VALUES ($1, $2, 0, to_timestamp(0), 'canonical') ON CONFLICT DO NOTHING",
    )
    .bind(chain_id).bind(format!("{chain_id}-genesis")).execute(pool).await?;
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
