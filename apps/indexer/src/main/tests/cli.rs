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
        &[std::ffi::OsStr::new("100000")]
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
