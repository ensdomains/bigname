use bigname_interpret::AdaptiveBatchConfig;
use clap::{CommandFactory, Parser};

use crate::cli::{Cli, ResolvedCommand};

const SOURCE_ENV: &str = "BIGNAME_TEST_ADAPTIVE_REDO_SOURCE_ENDPOINT";

fn redo_arguments(phase: &str, extra: &[&str]) -> Vec<String> {
    // SAFETY: every caller writes the same value to this test-only environment key.
    unsafe { std::env::set_var(SOURCE_ENV, "https://adaptive-redo-source.invalid") };
    let source = format!("ethereum-mainnet:rpc:drpc:ethereum_head:0={SOURCE_ENV}");
    let mut args: Vec<String> = [
        "phase-runner",
        "redo",
        "--database-url",
        "postgres://fixture.invalid/target",
        "--verification-database-url",
        "postgres://fixture.invalid/verification",
        "--chain",
        "ethereum-mainnet",
        "--source",
        &source,
        "--phase",
        phase,
        "--from-block",
        "0",
        "--to-block",
        "10",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    args.extend(extra.iter().map(|value| (*value).to_owned()));
    args
}

fn arguments(extra: &[&str]) -> Vec<String> {
    redo_arguments("interpret", extra)
}

fn resolved(extra: &[&str]) -> ResolvedCommand {
    Cli::try_parse_from(arguments(extra))
        .unwrap()
        .resolve()
        .unwrap()
}

fn adaptive(extra: &[&str]) -> Option<AdaptiveBatchConfig> {
    let ResolvedCommand::Redo {
        interpret_adaptive, ..
    } = resolved(extra)
    else {
        panic!("redo")
    };
    interpret_adaptive
}

fn resolve_error(extra: &[&str]) -> String {
    match Cli::try_parse_from(arguments(extra)).unwrap().resolve() {
        Ok(_) => panic!("invalid adaptive configuration accepted"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn adaptive_redo_is_opt_in_and_has_bounded_defaults() {
    assert!(adaptive(&[]).is_none());
    let config = adaptive(&["--interpret-adaptive"]).unwrap();
    assert_eq!(config, AdaptiveBatchConfig::default());
    assert_eq!(config.target_logs.get(), 200_000);
    assert_eq!(config.target_input_bytes.get(), 128 * 1024 * 1024);
    assert_eq!(config.max_pending_input_bytes.get(), 1024 * 1024 * 1024);
    assert_eq!(config.max_blocks.get(), 131_072);
}

#[test]
fn adaptive_overrides_reach_redo_and_respect_both_block_caps() {
    let config = adaptive(&[
        "--interpret-adaptive",
        "--interpret-target-logs",
        "12345",
        "--interpret-target-input-mib",
        "256",
        "--interpret-adaptive-max-blocks",
        "10000",
        "--interpret-blocks-per-batch",
        "2000",
    ])
    .unwrap();
    assert_eq!(config.target_logs.get(), 12_345);
    assert_eq!(config.target_input_bytes.get(), 256 * 1024 * 1024);
    assert_eq!(config.max_blocks.get(), 2000);
    for (flags, expected) in [
        (
            &[
                "--interpret-adaptive-max-blocks",
                "2000",
                "--interpret-blocks-per-batch",
                "10000",
            ][..],
            2000,
        ),
        (&["--interpret-blocks-per-batch", "500"][..], 500),
        (&["--interpret-adaptive-max-blocks", "7"][..], 7),
    ] {
        let mut extra = vec!["--interpret-adaptive"];
        extra.extend_from_slice(flags);
        assert_eq!(adaptive(&extra).unwrap().max_blocks.get(), expected);
    }
}

#[test]
fn a_target_above_the_default_pending_budget_is_honoured() {
    let config = adaptive(&[
        "--interpret-adaptive",
        "--interpret-target-input-mib",
        "4096",
    ])
    .unwrap();
    assert_eq!(config.target_input_bytes.get(), 4096 * 1024 * 1024);
    assert_eq!(config.max_pending_input_bytes, config.target_input_bytes);
}

#[test]
fn adaptive_knobs_require_opt_in_and_positive_values() {
    for flag in [
        "--interpret-target-logs",
        "--interpret-target-input-mib",
        "--interpret-adaptive-max-blocks",
    ] {
        assert!(Cli::try_parse_from(arguments(&[flag, "1"])).is_err());
        for value in ["0", "-1", "invalid", "18446744073709551616"] {
            assert!(
                Cli::try_parse_from(arguments(&["--interpret-adaptive", flag, value])).is_err()
            );
        }
    }
    assert!(
        resolve_error(&[
            "--interpret-adaptive",
            "--interpret-target-input-mib",
            "18446744073709551615",
        ])
        .contains("too large to express in bytes")
    );
}

#[test]
fn adaptive_batching_requires_a_redo_that_runs_interpret() {
    for phase in ["interpret", "all"] {
        assert!(
            Cli::try_parse_from(redo_arguments(phase, &["--interpret-adaptive"]))
                .unwrap()
                .resolve()
                .is_ok(),
            "{phase}"
        );
    }
    for phase in ["ingest", "project", "verify", "live", "recompute-flags"] {
        let error = match Cli::try_parse_from(redo_arguments(phase, &["--interpret-adaptive"]))
            .unwrap()
            .resolve()
        {
            Ok(_) => panic!("{phase} accepted adaptive batching"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("only to an interpret or all-phase redo"),
            "{phase}: {error}"
        );
        assert!(
            Cli::try_parse_from(redo_arguments(phase, &[]))
                .unwrap()
                .resolve()
                .is_ok(),
            "{phase} without the flag"
        );
    }
}

#[test]
fn adaptive_flags_are_not_accepted_by_supervised_run() {
    for flag in [
        "--interpret-adaptive",
        "--interpret-target-logs",
        "--interpret-adaptive-max-blocks",
    ] {
        let mut args = vec![
            "phase-runner",
            "run",
            "--database-url",
            "postgres://fixture.invalid/target",
            "--verification-database-url",
            "postgres://fixture.invalid/verification",
            "--chain",
            "ethereum-mainnet",
            flag,
        ];
        if flag != "--interpret-adaptive" {
            args.push("1");
        }
        let error = Cli::try_parse_from(args).expect_err("run has no adaptive batching");
        assert!(error.to_string().contains(flag), "{error}");
    }
}

#[test]
fn adaptive_help_explains_defaults_and_estimated_memory_scope() {
    let mut command = Cli::command();
    let help = command
        .find_subcommand_mut("redo")
        .unwrap()
        .render_long_help()
        .to_string();
    for expected in [
        "--interpret-adaptive",
        "--interpret-target-logs",
        "--interpret-target-input-mib",
        "--interpret-adaptive-max-blocks",
        "200000",
        "128",
        "131072",
        "not an RSS cap",
        "default: 500",
    ] {
        assert!(help.contains(expected), "help missing {expected}");
    }
}

#[test]
fn explicit_environment_block_limit_caps_adaptive_without_changing_fixed_default() {
    const CHILD: &str = "BIGNAME_TEST_ADAPTIVE_ENV_CHILD";
    if std::env::var_os(CHILD).is_some() {
        assert_eq!(
            adaptive(&["--interpret-adaptive"])
                .unwrap()
                .max_blocks
                .get(),
            701
        );
        assert_eq!(
            adaptive(&[
                "--interpret-adaptive",
                "--interpret-blocks-per-batch",
                "702",
            ])
            .unwrap()
            .max_blocks
            .get(),
            702
        );
        let ResolvedCommand::Redo {
            capacity,
            interpret_adaptive,
            ..
        } = resolved(&[])
        else {
            panic!("redo")
        };
        assert_eq!(capacity.interpret_blocks_per_batch.get(), 701);
        assert!(interpret_adaptive.is_none());
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "cli::adaptive::tests::explicit_environment_block_limit_caps_adaptive_without_changing_fixed_default",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("BIGNAME_INTERPRET_BLOCKS_PER_BATCH", "701")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "the child must run this test"
    );
}

#[test]
fn the_fixed_default_does_not_cap_adaptive_batches() {
    let ResolvedCommand::Redo {
        capacity,
        interpret_adaptive,
        ..
    } = resolved(&["--interpret-adaptive"])
    else {
        panic!("redo")
    };
    assert_eq!(capacity.interpret_blocks_per_batch.get(), 500);
    assert_eq!(interpret_adaptive.unwrap().max_blocks.get(), 131_072);
}
