use super::*;
use crate::config::{SeedBasis, SourceRole};

const REDO_SOURCE_ENV: &str = "BIGNAME_TEST_REDO_SOURCE_ENDPOINT";
const REDO_SOURCE: &str =
    "ethereum-mainnet:reth:reth_db:ethereum_head:0=BIGNAME_TEST_REDO_SOURCE_ENDPOINT";

fn configure_redo_source_endpoint() {
    // SAFETY: every caller writes the same value to this test-only environment key.
    unsafe { std::env::set_var(REDO_SOURCE_ENV, "/tmp/bigname-test-reth") };
}

#[test]
fn source_descriptor_roles_are_backward_compatible_and_validated() {
    const ENDPOINT_ENV: &str = "BIGNAME_TEST_SOURCE_ROLE_ENDPOINT";
    // SAFETY: this test owns a unique environment key and never mutates it after parsing starts.
    unsafe { std::env::set_var(ENDPOINT_ENV, "https://source-role.invalid") };
    for (suffix, expected) in [
        ("", SourceRole::Both),
        (":intake", SourceRole::Intake),
        (":verification-only", SourceRole::VerificationOnly),
        (":both", SourceRole::Both),
    ] {
        let source = parse_source(&format!(
            "ethereum-sepolia:source:drpc:ethereum_head:0{suffix}={ENDPOINT_ENV}"
        ))
        .expect("supported source role must parse");
        assert_eq!(source.role, expected);
    }
    for role in ["", "reader", "verification_only"] {
        let error = parse_source(&format!(
            "ethereum-sepolia:source:drpc:ethereum_head:0:{role}={ENDPOINT_ENV}"
        ))
        .expect_err("empty and unknown source roles must fail");
        assert_eq!(error.kind(), ErrorKind::Configuration);
        assert!(error.to_string().contains("ethereum-sepolia:source"));
        assert!(!error.to_string().contains("https://"), "{error}");
    }
    const EMPTY_ENDPOINT_ENV: &str = "BIGNAME_TEST_SOURCE_ROLE_EMPTY_ENDPOINT";
    // SAFETY: this test owns a unique environment key and never mutates it after parsing starts.
    unsafe { std::env::set_var(EMPTY_ENDPOINT_ENV, "") };
    let error = parse_source(&format!(
        "ethereum-sepolia:empty-source:drpc:ethereum_head:0={EMPTY_ENDPOINT_ENV}"
    ))
    .expect_err("an empty resolved endpoint must fail");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(error.to_string().contains("ethereum-sepolia:empty-source"));
}
#[test]
fn runnable_and_redo_source_role_validation_matrix() {
    let source = |key, role| {
        SourceConfig::new_with_role(
            "ethereum-sepolia",
            key,
            "drpc",
            SeedBasis::EthereumHead,
            0,
            role,
            format!("https://{key}.invalid"),
        )
        .unwrap()
    };
    let verify_only = source("verify", SourceRole::VerificationOnly);
    let verify_only_chain =
        ChainConfig::new("ethereum-sepolia", vec![verify_only.clone()], false).unwrap();
    let error = RuntimeConfig::new(
        "role-matrix",
        vec![verify_only_chain],
        CapacityConfig::default(),
        TimingConfig::default(),
    )
    .expect_err("a runnable chain with only verification sources must fail");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    for (phase, needs_intake) in [
        (RedoPhase::Phase(PhaseName::Ingest), true),
        (RedoPhase::Phase(PhaseName::Verify), true),
        (RedoPhase::All, true),
        (RedoPhase::Phase(PhaseName::Interpret), true),
        (RedoPhase::Phase(PhaseName::Project), true),
        (RedoPhase::RecomputeFlags, false),
    ] {
        assert_eq!(phase.requires_intake_sources(), needs_intake);
        let sources = if needs_intake {
            vec![verify_only.clone()]
        } else {
            Vec::new()
        };
        let result = resolve_explicit_redo_chains(
            vec!["ethereum-sepolia".to_owned()],
            sources,
            needs_intake,
        );
        if phase == RedoPhase::Phase(PhaseName::Verify) {
            assert!(result.as_ref().unwrap_err().to_string().contains("verify"));
        }
        assert_eq!(result.is_err(), needs_intake);
    }

    let intake = source("intake", SourceRole::Intake);
    RuntimeConfig::new(
        "role-matrix",
        vec![
            ChainConfig::new("ethereum-sepolia", vec![intake.clone(), verify_only], false).unwrap(),
        ],
        CapacityConfig::default(),
        TimingConfig::default(),
    )
    .expect("a runnable mixed-role chain has intake");
    ChainConfig::new(
        "ethereum-sepolia",
        vec![intake, source("intake", SourceRole::VerificationOnly)],
        false,
    )
    .expect_err("duplicate source keys remain invalid across roles");
}
#[test]
fn run_cli_accepts_a_metrics_listener_address() {
    let cli = Cli::try_parse_from([
        "phase-runner",
        "run",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--verification-database-url",
        "postgres://phase-runner.invalid/verification",
        "--chain",
        "ethereum-mainnet",
        "--metrics-bind-addr",
        "0.0.0.0:19465",
        "--heartbeat-stale-after-secs",
        "1200",
    ])
    .expect("run metrics listener option must parse");

    let Command::Run(args) = cli.command else {
        panic!("expected run command");
    };
    assert_eq!(args.metrics_bind_addr, "0.0.0.0:19465".parse().unwrap());
    assert_eq!(args.heartbeat_stale_after_secs, 1200);
}

#[test]
fn run_cli_rejects_a_nonpositive_heartbeat_threshold() {
    let command = Cli::try_parse_from([
        "phase-runner",
        "run",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--verification-database-url",
        "postgres://phase-runner.invalid/verification",
        "--chain",
        "ethereum-mainnet",
        "--heartbeat-stale-after-secs",
        "0",
    ])
    .expect("zero threshold parses before semantic validation");
    let error = command
        .resolve()
        .err()
        .expect("zero threshold must be rejected");
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(error.to_string().contains("threshold must be positive"));
}

#[test]
fn redo_cli_carries_canonical_head_hydration_rpc() {
    configure_redo_source_endpoint();
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--chain",
        "ethereum-mainnet",
        "--source",
        REDO_SOURCE,
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
    configure_redo_source_endpoint();
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--chain",
        "ethereum-mainnet",
        "--source",
        REDO_SOURCE,
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
fn single_chain_redo_accepts_a_matching_chain_token_attestation() {
    configure_redo_source_endpoint();
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--chain",
        "ethereum-mainnet",
        "--source",
        REDO_SOURCE,
        "--phase",
        "interpret",
        "--from-block",
        "42",
        "--to-block",
        "42",
        "--attest-watch-set-coverage",
        "ethereum-mainnet=reviewed-generation-token",
    ])
    .expect("chain-qualified single-chain attestation must parse")
    .resolve()
    .expect("a matching chain-qualified single-chain attestation must resolve");

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
fn single_chain_redo_rejects_a_mismatched_chain_token_attestation() {
    configure_redo_source_endpoint();
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--chain",
        "ethereum-mainnet",
        "--source",
        REDO_SOURCE,
        "--phase",
        "interpret",
        "--from-block",
        "42",
        "--to-block",
        "42",
        "--attest-watch-set-coverage",
        "base-mainnet=reviewed-generation-token",
    ])
    .expect("mismatched chain-qualified attestation parses before semantic validation");
    let error = match command.resolve() {
        Ok(_) => panic!("a different chain must not be authorized by a single-chain redo"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(error.to_string().contains("base-mainnet"));
    assert!(error.to_string().contains("ethereum-mainnet"));
}

#[test]
fn multi_chain_redo_requires_and_carries_per_chain_attestations() {
    configure_redo_source_endpoint();
    let command = Cli::try_parse_from([
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://phase-runner.invalid/fresh",
        "--chain",
        "ethereum-mainnet,base-mainnet",
        "--source",
        REDO_SOURCE,
        "--source",
        "base-mainnet:coinbase:coinbase:ethereum_head:0=BIGNAME_TEST_REDO_SOURCE_ENDPOINT",
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
        "--source",
        "ethereum-mainnet:reth:reth_db:ethereum_head:0=PATH",
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
fn verify_redo_names_its_intake_source_requirement() {
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
        "verify",
        "--from-block",
        "42",
        "--to-block",
        "43",
    ])
    .expect("verify redo must parse before source validation");
    let error = match command.resolve() {
        Ok(_) => panic!("verify redo must require intake source configuration"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::Configuration);
    assert!(error.to_string().contains("verify"));
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
