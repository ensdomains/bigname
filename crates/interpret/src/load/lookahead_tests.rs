use std::{io::Read, path::Path, time::Instant};

use bigname_adapters::schema_v2::{StateCacheCapacity, prepare_schema_v2_batch_lookahead};

fn manifest(source_family: &str) -> bigname_adapters::schema_v2::ManifestInput {
    bigname_adapters::schema_v2::ManifestInput {
        manifest_id: 1,
        manifest_version: 1,
        namespace: "ens".to_owned(),
        source_family: source_family.to_owned(),
        chain_id: "ethereum-mainnet".to_owned(),
        deployment_label: "test".to_owned(),
        normalizer_version: "test".to_owned(),
        payload_json: "{}".to_owned(),
    }
}

#[test]
fn any_uncovered_manifest_family_requires_the_full_state_loader() {
    use crate::FullStateReason::UnsupportedSourceFamily;
    let mainnet: Vec<_> = [
        "basenames_execution",
        "basenames_l1_compat",
        "ens_v1_registrar_l1",
        "ens_v1_registry_l1",
        "ens_v1_resolver_l1",
        "ens_v1_reverse_l1",
        "ens_v1_wrapper_l1",
    ]
    .map(manifest)
    .into();
    assert_eq!(super::full_state_reason(&mainnet, &mainnet), None);

    let mut sepolia = mainnet.clone();
    sepolia.push(manifest("ens_v2_registry_l1"));
    assert_eq!(
        super::full_state_reason(&sepolia, &sepolia),
        Some(UnsupportedSourceFamily {
            source_family: "ens_v2_registry_l1".to_owned(),
            rollout_status: "active",
        })
    );
    // The full-state loader restores a deprecated manifest's retained events; lookahead
    // reads none of them, so a deprecated uncovered family also requires the full-state loader.
    assert_eq!(
        super::full_state_reason(&mainnet, &sepolia),
        Some(UnsupportedSourceFamily {
            source_family: "ens_v2_registry_l1".to_owned(),
            rollout_status: "deprecated",
        })
    );
    let base = [manifest("basenames_base_registry")];
    assert!(super::full_state_reason(&base, &base).is_some());
}

/// Opt-in operator probe. All connections enforce read-only mode; no writer is called.
///
/// `BIGNAME_LOOKAHEAD_PROBE_OUTPUT` names a directory that receives one complete
/// `BatchOutput` file per batch, named by the batch's first block. With
/// `BIGNAME_LOOKAHEAD_PROBE_LOADER=full-state` the same batches run through the full-state
/// loader, carrying its session between batches as the engine does, which produces the
/// baseline directory. With `BIGNAME_LOOKAHEAD_PROBE_BASELINE` set, every batch, not only
/// the first, must equal its baseline file; a missing baseline file fails the probe.
#[tokio::test]
#[ignore = "requires a read-only mainnet database and optional saved full-state baselines"]
async fn readonly_mainnet_batches() -> anyhow::Result<()> {
    let url = std::env::var("BIGNAME_LOOKAHEAD_PROBE_DATABASE_URL")?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET default_transaction_read_only = on")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET search_path = bigname_phase, public")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await?;
    let first = std::env::var("BIGNAME_LOOKAHEAD_PROBE_FROM")
        .unwrap_or_else(|_| "14684500".to_owned())
        .parse::<i64>()?;
    let batches = std::env::var("BIGNAME_LOOKAHEAD_PROBE_BATCHES")
        .unwrap_or_else(|_| "1".to_owned())
        .parse::<i64>()?;
    anyhow::ensure!(
        (1..=32).contains(&batches),
        "probe batch count must be 1..=32"
    );
    let full_state = match std::env::var("BIGNAME_LOOKAHEAD_PROBE_LOADER").as_deref() {
        Ok("full-state") => true,
        Ok("lookahead") | Err(_) => false,
        Ok(other) => anyhow::bail!("unknown probe loader {other}"),
    };
    let capacity = StateCacheCapacity::Entries(65_536);
    let mut carried = None;
    for batch in 0..batches {
        let started = Instant::now();
        let from = first + batch * 500;
        let loaded = if full_state {
            crate::load::batch_input(
                &pool,
                "ethereum-mainnet",
                from,
                from + 499,
                None,
                carried.take(),
                capacity,
            )
            .await?
        } else {
            match super::batch_input(
                &pool,
                "ethereum-mainnet",
                from,
                from + 499,
                None,
                capacity,
                None,
            )
            .await?
            {
                super::Attempt::Loaded(loaded) => *loaded,
                super::Attempt::FullStateRequired(choice) => {
                    anyhow::bail!("lookahead is not chosen for this database: {choice:?}")
                }
            }
        };
        let load_ms = started.elapsed().as_millis();
        let count = loaded.restored_event_count;
        let raw = loaded.input.raw_logs.len();
        let session = loaded
            .adapter_session
            .expect("both loaders restore a session");
        let prepared = match &loaded.lookahead_nodes {
            Some(nodes) => prepare_schema_v2_batch_lookahead(
                loaded.input,
                loaded.provenance_manifests,
                session,
                nodes,
                capacity,
            )?,
            None => bigname_adapters::prepare_schema_v2_batch_incremental_with_provenance(
                loaded.input,
                loaded.provenance_manifests,
                Some(session),
                capacity,
            )?,
        };
        let values = crate::load::prior_state_values(
            &pool,
            "ethereum-mainnet",
            from,
            prepared.state_value_requests(),
        )
        .await?;
        let (output, session) = prepared.finish(values)?;
        eprintln!(
            "probe loader={} from={from} nodes={} prior_events={count} raw_logs={raw} normalized_events={} load_ms={load_ms} total_ms={} rss_kib={}",
            if full_state {
                "full-state"
            } else {
                "lookahead"
            },
            loaded
                .lookahead_nodes
                .as_ref()
                .map_or(0, |nodes| nodes.len()),
            output.normalized_events.len(),
            started.elapsed().as_millis(),
            rss_kib()
        );
        if let Ok(directory) = std::env::var("BIGNAME_LOOKAHEAD_PROBE_OUTPUT") {
            let path = Path::new(&directory).join(format!("{from}.txt"));
            std::fs::write(&path, format!("{output:?}"))?;
            if let Ok(baseline) = std::env::var("BIGNAME_LOOKAHEAD_PROBE_BASELINE") {
                anyhow::ensure!(
                    same_file(&path, &Path::new(&baseline).join(format!("{from}.txt")))?,
                    "complete BatchOutput for the batch at {from} differs from its baseline"
                );
                eprintln!("probe from={from} complete_output_identical=true");
            }
        }
        if full_state {
            let cache =
                crate::load::fold_prior_cache(loaded.prior_cache, &output.normalized_events);
            carried = Some(crate::load::CachedPrior {
                cache,
                adapter_session: session,
            });
        }
        drop(output);
        eprintln!("probe released from={from} rss_kib={}", rss_kib());
    }
    Ok(())
}

fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .unwrap_or(0)
}

fn same_file(left: &Path, right: &Path) -> std::io::Result<bool> {
    if std::fs::metadata(left)?.len() != std::fs::metadata(right)?.len() {
        return Ok(false);
    }
    let mut left = std::fs::File::open(left)?;
    let mut right = std::fs::File::open(right)?;
    let mut a = [0; 65536];
    let mut b = [0; 65536];
    loop {
        let n = left.read(&mut a)?;
        if n == 0 {
            return Ok(true);
        }
        right.read_exact(&mut b[..n])?;
        if a[..n] != b[..n] {
            return Ok(false);
        }
    }
}
