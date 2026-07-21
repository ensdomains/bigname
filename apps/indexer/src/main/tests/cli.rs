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
