use std::{
    io::Read,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::Context as _;
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
    let baseline = std::env::var_os("BIGNAME_LOOKAHEAD_PROBE_BASELINE").map(PathBuf::from);
    let output_directory = std::env::var_os("BIGNAME_LOOKAHEAD_PROBE_OUTPUT").map(PathBuf::from);
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
        check_batch_output(
            from,
            &format!("{output:?}"),
            baseline.as_deref(),
            output_directory.as_deref(),
        )?;
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

/// Checks one batch's rendered `BatchOutput` against its baseline file, then saves a copy.
///
/// The comparison runs before any write so an output directory that aliases the baseline
/// directory cannot replace the expected file before it is read. A missing or unreadable
/// baseline file and a mismatch all fail the probe.
fn check_batch_output(
    from: i64,
    rendered: &str,
    baseline: Option<&Path>,
    output: Option<&Path>,
) -> anyhow::Result<()> {
    let name = format!("{from}.txt");
    if let Some(baseline) = baseline {
        let path = baseline.join(&name);
        let identical = same_content(rendered.as_bytes(), &path)
            .with_context(|| format!("reading baseline {}", path.display()))?;
        anyhow::ensure!(
            identical,
            "complete BatchOutput for the batch at {from} differs from its baseline {}",
            path.display()
        );
        eprintln!("probe from={from} complete_output_identical=true");
    }
    if let Some(output) = output {
        let path = output.join(&name);
        std::fs::write(&path, rendered)
            .with_context(|| format!("writing output {}", path.display()))?;
    }
    Ok(())
}

/// Reads `path` in chunks and reports whether it holds exactly `expected`.
fn same_content(expected: &[u8], path: &Path) -> std::io::Result<bool> {
    if std::fs::metadata(path)?.len() != expected.len() as u64 {
        return Ok(false);
    }
    let mut file = std::fs::File::open(path)?;
    let mut buffer = [0; 65536];
    let mut offset = 0;
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            return Ok(offset == expected.len());
        }
        let Some(chunk) = expected.get(offset..offset + n) else {
            return Ok(false);
        };
        if buffer[..n] != *chunk {
            return Ok(false);
        }
        offset += n;
    }
}

fn probe_directory(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "bigname-lookahead-probe-{name}-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).expect("create probe directory");
    directory
}

#[test]
fn probe_fails_when_the_baseline_file_is_missing_and_no_output_is_requested() {
    let baseline = probe_directory("missing");
    let result = check_batch_output(100, "batch", Some(&baseline), None);
    let error = result.expect_err("a missing baseline must fail the probe");
    assert!(
        error.to_string().contains("100.txt"),
        "error names the baseline file: {error:#}"
    );
    std::fs::remove_dir_all(baseline).ok();
}

#[test]
fn probe_fails_when_the_baseline_differs_and_no_output_is_requested() {
    let baseline = probe_directory("mismatch");
    std::fs::write(baseline.join("100.txt"), "expected").unwrap();
    let error = check_batch_output(100, "actual", Some(&baseline), None)
        .expect_err("a differing baseline must fail the probe");
    assert!(
        error.to_string().contains("differs from its baseline"),
        "{error:#}"
    );
    let same_length = check_batch_output(100, "expectee", Some(&baseline), None)
        .expect_err("a same-length differing baseline must fail the probe");
    assert!(
        same_length
            .to_string()
            .contains("differs from its baseline"),
        "{same_length:#}"
    );
    std::fs::remove_dir_all(baseline).ok();
}

#[test]
fn probe_passes_when_the_baseline_matches_and_saves_a_copy_only_when_requested() {
    let baseline = probe_directory("match-baseline");
    let output = probe_directory("match-output");
    let rendered = "x".repeat(70_000);
    std::fs::write(baseline.join("100.txt"), &rendered).unwrap();
    check_batch_output(100, &rendered, Some(&baseline), None).expect("matching baseline passes");
    assert!(std::fs::read_dir(&output).unwrap().next().is_none());
    check_batch_output(100, &rendered, Some(&baseline), Some(&output))
        .expect("matching baseline passes with an output copy");
    assert_eq!(
        std::fs::read_to_string(output.join("100.txt")).unwrap(),
        rendered
    );
    check_batch_output(600, &rendered, None, Some(&output)).expect("no baseline means no check");
    assert_eq!(
        std::fs::read_to_string(output.join("600.txt")).unwrap(),
        rendered
    );
    std::fs::remove_dir_all(baseline).ok();
    std::fs::remove_dir_all(output).ok();
}

#[test]
fn probe_fails_and_keeps_the_baseline_when_output_aliases_the_baseline_directory() {
    let directory = probe_directory("aliased");
    std::fs::write(directory.join("100.txt"), "expected").unwrap();
    let error = check_batch_output(100, "actual", Some(&directory), Some(&directory))
        .expect_err("a differing batch must fail even when output aliases the baseline");
    assert!(
        error.to_string().contains("differs from its baseline"),
        "{error:#}"
    );
    assert_eq!(
        std::fs::read_to_string(directory.join("100.txt")).unwrap(),
        "expected",
        "the baseline file must survive a failed probe"
    );
    std::fs::remove_dir_all(directory).ok();
}

#[test]
fn probe_fails_on_a_second_batch_mismatch() {
    let baseline = probe_directory("second-batch");
    std::fs::write(baseline.join("100.txt"), "first").unwrap();
    std::fs::write(baseline.join("600.txt"), "second").unwrap();
    check_batch_output(100, "first", Some(&baseline), None).expect("the first batch matches");
    let error = check_batch_output(600, "changed", Some(&baseline), None)
        .expect_err("the second batch differs and must fail the probe");
    assert!(
        error.to_string().contains("600"),
        "error names the second batch: {error:#}"
    );
    std::fs::remove_dir_all(baseline).ok();
}
