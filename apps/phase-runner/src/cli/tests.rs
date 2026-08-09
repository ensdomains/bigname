use super::*;

#[test]
fn redo_cli_carries_canonical_head_hydration_rpc() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--chain",
        "ethereum-mainnet",
        "--phase",
        "project",
        "--from-block",
        "42",
        "--to-block",
        "42",
        "--hydration-rpc",
        "ethereum-mainnet=http://hydration.invalid",
    ])
    .expect("redo hydration RPC option must parse")
    .resolve()
    .expect("redo hydration RPC option must resolve");

    match command {
        ResolvedCommand::Redo {
            hydration_rpc_urls, ..
        } => assert_eq!(
            hydration_rpc_urls.url_for("ethereum-mainnet"),
            Some("http://hydration.invalid")
        ),
        _ => panic!("expected redo command"),
    }
}

#[test]
fn redo_cli_carries_watch_set_coverage_attestation() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--chain",
        "ethereum-mainnet",
        "--phase",
        "interpret",
        "--from-block",
        "42",
        "--to-block",
        "42",
        "--attest-watch-set-coverage",
        "reviewed-generation-token",
    ])
    .expect("redo attestation option must parse")
    .resolve()
    .expect("redo attestation option must resolve");

    let ResolvedCommand::Redo {
        watch_set_coverage_attestations,
        ..
    } = command
    else {
        panic!("expected redo command");
    };
    assert_eq!(
        watch_set_coverage_attestations,
        BTreeMap::from([(
            "ethereum-mainnet".to_owned(),
            "reviewed-generation-token".to_owned()
        )])
    );
}

#[test]
fn multi_chain_redo_requires_and_carries_per_chain_attestations() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--chain",
        "ethereum-mainnet,base-mainnet",
        "--phase",
        "interpret",
        "--from-block",
        "42",
        "--to-block",
        "42",
        "--attest-watch-set-coverage",
        "ethereum-mainnet=ethereum-token",
        "--attest-watch-set-coverage",
        "base-mainnet=base-token",
    ])
    .expect("per-chain redo attestations must parse")
    .resolve()
    .expect("per-chain redo attestations must resolve");

    let ResolvedCommand::Redo {
        watch_set_coverage_attestations,
        ..
    } = command
    else {
        panic!("expected redo command");
    };
    assert_eq!(
        watch_set_coverage_attestations,
        BTreeMap::from([
            ("base-mainnet".to_owned(), "base-token".to_owned()),
            ("ethereum-mainnet".to_owned(), "ethereum-token".to_owned()),
        ])
    );
}

#[test]
fn all_chains_redo_rejects_one_bare_attestation() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--all-chains",
        "--phase",
        "interpret",
        "--from-block",
        "42",
        "--to-block",
        "42",
        "--attest-watch-set-coverage",
        "one-token-for-every-chain",
    ])
    .expect("a bare all-chains attestation parses before semantic validation");
    let error = match command.resolve() {
        Ok(_) => panic!("one bare token must not attest multiple chains"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(error.to_string().contains("CHAIN=TOKEN"));
}

#[test]
fn verify_redo_requires_a_separate_verification_database_url() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--chain",
        "ethereum-mainnet",
        "--phase",
        "verify",
        "--from-block",
        "42",
        "--to-block",
        "42",
    ])
    .expect("verify redo without the reader URL must parse before semantic validation");
    let error = match command.resolve() {
        Ok(_) => panic!("verify redo must reject a missing verification database URL"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(error.to_string().contains("SELECT-only role"));
}

#[test]
fn all_phase_redo_requires_ingest_sources_before_dispatch() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--verification-database-url",
        "postgres://phase-runner.invalid/verification",
        "--chain",
        "ethereum-mainnet",
        "--phase",
        "all",
        "--from-block",
        "42",
        "--to-block",
        "43",
    ])
    .expect("all-phase redo must parse before source validation");
    let error = match command.resolve() {
        Ok(_) => panic!("all-phase redo must require ingest source configuration"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(error.to_string().contains("all-phase redo requires"));
}

#[test]
fn recompute_flags_accepts_the_all_chains_selector_without_sources() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--all-chains",
        "--phase",
        "recompute-flags",
        "--from-block",
        "42",
        "--to-block",
        "43",
    ])
    .expect("all-chains recompute must parse")
    .resolve()
    .expect("all-chains recompute must resolve");
    assert!(matches!(
        command,
        ResolvedCommand::Redo {
            chains: RedoChains::All { .. },
            phase: RedoPhase::RecomputeFlags,
            ..
        }
    ));
}

#[test]
fn label_preimages_import_resolves_batch_and_limit_options() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "label-preimages",
        "import-ens-rainbow",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--batch-size",
        "500",
        "--limit",
        "1000",
    ])
    .expect("rainbow import command must parse")
    .resolve()
    .expect("rainbow import command must resolve");

    match command {
        ResolvedCommand::LabelPreimagesImportEnsRainbow {
            database_url,
            batch_size,
            limit,
        } => {
            assert_eq!(database_url, "postgres://phase-runner.invalid/fresh");
            assert_eq!(batch_size, Some(500));
            assert_eq!(limit, Some(1000));
        }
        _ => panic!("expected rainbow import command"),
    }
}

#[test]
fn label_preimages_import_defaults_to_unbounded_full_table_batches() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "label-preimages",
        "import-ens-rainbow",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
    ])
    .expect("rainbow import command must parse")
    .resolve()
    .expect("rainbow import command must resolve");

    match command {
        ResolvedCommand::LabelPreimagesImportEnsRainbow {
            batch_size, limit, ..
        } => {
            assert_eq!(batch_size, None);
            assert_eq!(limit, None);
        }
        _ => panic!("expected rainbow import command"),
    }
}

#[test]
fn label_preimages_import_rejects_a_zero_batch_size() {
    Cli::try_parse_from([
        "phase-runner",
        "label-preimages",
        "import-ens-rainbow",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--batch-size",
        "0",
    ])
    .expect_err("a zero batch size would loop the import without making progress");
}

#[test]
fn label_preimages_import_rejects_a_negative_limit() {
    Cli::try_parse_from([
        "phase-runner",
        "label-preimages",
        "import-ens-rainbow",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--limit=-1",
    ])
    .expect_err("a negative limit is not a bounded import");
}

#[test]
fn inspect_cli_resolves_each_kept_schema_v2_window() {
    for (window, expected) in [
        (
            "block-canonicality",
            crate::inspect::InspectionKind::BlockCanonicality,
        ),
        (
            "stored-lineage",
            crate::inspect::InspectionKind::StoredLineage,
        ),
        ("raw-events", crate::inspect::InspectionKind::RawEvents),
    ] {
        let command = Cli::try_parse_from([
            "phase-runner",
            "inspect",
            "--database-url",
            "postgres://phase-runner.invalid/fresh",
            window,
            "--chain",
            "ethereum-mainnet",
            "--from-block",
            "42",
            "--to-block",
            "43",
        ])
        .expect("inspection window must parse")
        .resolve()
        .expect("inspection window must resolve");
        match command {
            ResolvedCommand::Inspect { request, .. } => {
                assert_eq!(request.kind, expected);
                assert_eq!(request.chain_id, "ethereum-mainnet");
                assert_eq!(request.range, BlockRange { from: 42, to: 43 });
            }
            _ => panic!("expected inspect command"),
        }
    }
}
