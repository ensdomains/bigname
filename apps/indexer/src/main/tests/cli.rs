#[test]
fn run_cli_parses_per_chain_code_fallback_urls() {
    let cli = Cli::try_parse_from([
        "bigname-indexer",
        "run",
        "--chain-rpc-code-fallback-url",
        "ethereum-mainnet=https://archive.example/ethereum,base-mainnet=https://archive.example/base",
    ])
    .expect("run CLI must accept comma-delimited code fallback URLs");

    let Command::Run(args) = cli.command else {
        panic!("run command must parse as Command::Run");
    };
    assert_eq!(
        args.chain_rpc_code_fallback_urls,
        vec![
            "ethereum-mainnet=https://archive.example/ethereum",
            "base-mainnet=https://archive.example/base",
        ]
    );
}

#[test]
fn replay_normalized_events_cli_exposes_stateless_only_authority() {
    let command = <Cli as clap::CommandFactory>::command();
    let replay_command = command
        .get_subcommands()
        .find(|command| command.get_name() == "replay")
        .expect("CLI must expose the replay command");
    let normalized_events_command = replay_command
        .get_subcommands()
        .find(|command| command.get_name() == "normalized-events")
        .expect("replay CLI must expose normalized-events");
    let stateless_only_arg = normalized_events_command
        .get_arguments()
        .find(|arg| arg.get_id() == "stateless_only")
        .expect("normalized-events replay must expose --stateless-only");
    assert_eq!(
        stateless_only_arg.get_env(),
        Some(std::ffi::OsStr::new(
            "BIGNAME_INDEXER_REPLAY_NORMALIZED_EVENTS_STATELESS_ONLY"
        ))
    );

    let cli = Cli::try_parse_from([
        "bigname-indexer",
        "replay",
        "normalized-events",
        "--deployment-profile",
        "mainnet",
        "--chain",
        "ethereum-mainnet",
        "--block-hash",
        "0xabc",
        "--stateless-only",
    ])
    .expect("normalized-events replay CLI must accept --stateless-only");
    let Command::Replay(replay) = cli.command else {
        panic!("replay command must parse as Command::Replay");
    };
    let ReplayCommand::NormalizedEvents(args) = replay.command;
    assert!(args.stateless_only);
    assert_eq!(args.block_hashes, vec!["0xabc"]);
}

#[test]
fn coverage_recovery_terminal_failure_has_an_operator_rearm_command() {
    let cli = Cli::try_parse_from([
        "bigname-indexer",
        "repair",
        "coverage-recovery-rearm",
        "--deployment-profile",
        "mainnet",
        "--chain",
        "ethereum-mainnet",
        "--raw-log-retention-generation",
        "1",
        "--source-family",
        "ens_v1_wrapper_l1",
        "--address",
        "0x0000000000000000000000000000000000000133",
        "--from-block",
        "33",
        "--to-block",
        "33",
    ]);
    assert!(
        cli.is_ok(),
        "repair CLI must expose an exact generation/window coverage-recovery re-arm: {cli:?}"
    );
}

#[test]
fn run_cli_parses_startup_discovery_page_logs() {
    let command = <Cli as clap::CommandFactory>::command();
    let run_command = command
        .get_subcommands()
        .find(|command| command.get_name() == "run")
        .expect("CLI must expose the run command");
    let page_logs_arg = run_command
        .get_arguments()
        .find(|arg| arg.get_id() == "startup_discovery_page_logs")
        .expect("run CLI must expose the startup discovery page-log limit");
    assert_eq!(
        page_logs_arg.get_env(),
        Some(std::ffi::OsStr::new(
            "BIGNAME_INDEXER_STARTUP_DISCOVERY_PAGE_LOGS"
        ))
    );
    assert_eq!(
        page_logs_arg.get_default_values(),
        &[std::ffi::OsStr::new("1000")]
    );

    let cli = Cli::try_parse_from([
        "bigname-indexer",
        "run",
        "--startup-discovery-page-logs",
        "123456",
    ])
    .expect("run CLI must accept a startup discovery page-log limit");

    let Command::Run(args) = cli.command else {
        panic!("run command must parse as Command::Run");
    };
    assert_eq!(args.startup_discovery_page_logs, 123_456);

    let zero = Cli::try_parse_from([
        "bigname-indexer",
        "run",
        "--startup-discovery-page-logs",
        "0",
    ]);
    assert!(
        zero.is_err(),
        "run CLI must reject a zero startup discovery page-log limit"
    );

    let maximum = (i64::MAX - 1).to_string();
    let maximum_cli = Cli::try_parse_from([
        "bigname-indexer",
        "run",
        "--startup-discovery-page-logs",
        maximum.as_str(),
    ])
    .expect("run CLI must accept the largest SQL-safe page-log limit");
    let Command::Run(maximum_args) = maximum_cli.command else {
        panic!("run command must parse as Command::Run");
    };
    assert_eq!(
        maximum_args.startup_discovery_page_logs,
        usize::try_from(i64::MAX - 1).expect("test target must represent i64 in usize")
    );

    let sql_overflow = i64::MAX.to_string();
    let sql_overflow = Cli::try_parse_from([
        "bigname-indexer",
        "run",
        "--startup-discovery-page-logs",
        sql_overflow.as_str(),
    ]);
    assert!(
        sql_overflow.is_err(),
        "run CLI must reject a page-log limit whose SQL lookahead overflows"
    );
}

#[test]
fn run_cli_parses_coverage_recovery_max_attempts_per_iteration() {
    let command = <Cli as clap::CommandFactory>::command();
    let run_command = command
        .get_subcommands()
        .find(|command| command.get_name() == "run")
        .expect("CLI must expose the run command");
    let attempt_cap_arg = run_command
        .get_arguments()
        .find(|arg| arg.get_id() == "coverage_recovery_max_attempts_per_iteration")
        .expect("run CLI must expose the coverage-recovery iteration attempt cap");
    assert_eq!(
        attempt_cap_arg.get_env(),
        Some(std::ffi::OsStr::new(
            "BIGNAME_INDEXER_COVERAGE_RECOVERY_MAX_ATTEMPTS_PER_ITERATION"
        ))
    );
    assert_eq!(
        attempt_cap_arg.get_default_values(),
        &[std::ffi::OsStr::new("4")]
    );

    let Cli {
        command: Command::Run(defaults),
    } = Cli::try_parse_from(["bigname-indexer", "run"])
        .expect("run CLI must default the coverage-recovery iteration attempt cap")
    else {
        panic!("run command must parse as Command::Run");
    };
    assert_eq!(defaults.coverage_recovery_max_attempts_per_iteration, 4);

    let Cli {
        command: Command::Run(custom),
    } = Cli::try_parse_from([
        "bigname-indexer",
        "run",
        "--coverage-recovery-max-attempts-per-iteration",
        "9",
    ])
    .expect("run CLI must accept a custom coverage-recovery iteration attempt cap")
    else {
        panic!("run command must parse as Command::Run");
    };
    assert_eq!(custom.coverage_recovery_max_attempts_per_iteration, 9);

    for invalid in ["0", "not-a-number"] {
        let result = Cli::try_parse_from([
            "bigname-indexer",
            "run",
            "--coverage-recovery-max-attempts-per-iteration",
            invalid,
        ]);
        assert!(
            result.is_err(),
            "run CLI must reject invalid coverage-recovery iteration attempt cap {invalid:?}"
        );
    }
}
