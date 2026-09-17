use super::super::VerificationLevel;
use super::*;
use crate::config::SourceRole;
#[test]
fn base_provider_trust_selects_drpc_and_quick_synced() -> RunnerResult<()> {
    let source = |key, kind, start| {
        SourceConfig::new(
            "base-mainnet",
            key,
            kind,
            SeedBasis::BaseSeam,
            start,
            if kind == "drpc" {
                "https://intake.invalid"
            } else {
                "https://coinbase.invalid"
            },
        )
    };
    let coinbase = source("coinbase-history", "coinbase_sql", 0)?;
    let drpc = source("drpc-intake", "drpc", BASE_COINBASE_SEAM_BLOCK)?;
    let intake = [&coinbase, &drpc];
    let selected = provider_trusted_source("base-mainnet", &intake)?;
    assert_eq!(selected.source_key, "drpc-intake");
    let plan = super::super::verification_plan("base-mainnet", &[coinbase.clone(), drpc.clone()])?;
    assert_eq!(plan.verification_level(), VerificationLevel::QuickSynced);
    let chain = crate::config::ChainConfig::new(
        "base-mainnet",
        vec![coinbase.clone(), drpc.clone()],
        false,
    )?;
    assert!(crate::runner::PhaseRunner::verify_before_live(&chain)?);
    let reference = SourceConfig::new_with_role(
        "base-mainnet",
        "drpc-reference",
        "drpc",
        SeedBasis::BaseSeam,
        BASE_COINBASE_SEAM_BLOCK,
        crate::config::SourceRole::VerificationOnly,
        "https://reference.invalid",
    )?;
    let compared =
        crate::config::ChainConfig::new("base-mainnet", vec![coinbase, drpc, reference], false)?;
    assert!(!crate::runner::PhaseRunner::verify_before_live(&compared)?);
    Ok(())
}

#[test]
fn reference_less_mainnet_provider_trust_is_serialized() -> RunnerResult<()> {
    let reth = SourceConfig::new(
        "ethereum-mainnet",
        "reth-intake",
        "reth_db",
        SeedBasis::EthereumHead,
        0,
        "/fixture/reth",
    )?;
    let chain = crate::config::ChainConfig::new("ethereum-mainnet", vec![reth], false)?;
    assert!(super::super::provider_trusted_verify_required(
        &chain.chain_id,
        &chain.sources,
    )?);
    assert!(crate::runner::PhaseRunner::verify_before_live(&chain)?);
    Ok(())
}

#[cfg(unix)]
#[test]
fn existing_filesystem_identity_does_not_depend_on_canonical_spelling() -> RunnerResult<()> {
    let root = std::env::temp_dir().join(format!("bigname-reth-identity-{}", uuid::Uuid::new_v4()));
    let datadir = root.join("reth");
    let alias = root.join("reth-link");
    fs::create_dir_all(&datadir).map_err(|error| {
        RunnerError::data_integrity(format!("create reth identity fixture: {error}"))
    })?;
    std::os::unix::fs::symlink(&datadir, &alias).map_err(|error| {
        RunnerError::data_integrity(format!("create reth identity alias: {error}"))
    })?;

    let same = same_reth_paths_with_fallback(&datadir, &alias, lexical_path_identity);
    fs::remove_dir_all(root).map_err(|error| {
        RunnerError::data_integrity(format!("remove reth identity fixture: {error}"))
    })?;
    assert!(
        same,
        "filesystem identity must catch aliases without canonical spelling"
    );
    Ok(())
}

#[test]
fn nonexistent_reth_paths_use_spelling_fallback() {
    let root = std::env::temp_dir().join(format!("bigname-reth-missing-{}", uuid::Uuid::new_v4()));
    let path = root.join("reth");
    let parent_alias = root.join("unused/../reth");
    let distinct = root.join("other");

    assert!(same_reth_paths_with_fallback(
        &path,
        &parent_alias,
        lexical_path_identity,
    ));
    assert!(!same_reth_paths_with_fallback(
        &path,
        &distinct,
        lexical_path_identity,
    ));
}

#[test]
fn nested_reth_opened_object_is_not_independent() -> RunnerResult<()> {
    let root = std::env::temp_dir().join(format!("bigname-reth-nested-{}", uuid::Uuid::new_v4()));
    let intake = root.join("reth");
    let reference = intake.join("db");
    fs::create_dir_all(&reference).map_err(fixture_error)?;
    let intake_source = reth_role_source("intake", &intake, SourceRole::Intake)?;
    let reference_source = reth_role_source("reference", &reference, SourceRole::VerificationOnly)?;

    let conflict = same_reth_path_identity(&intake_source, &reference_source)?;
    let conflict = conflict
        .expect("a reference datadir nested at an intake storage child must fail independence");
    assert_eq!(conflict.left_object, Some("db"));
    assert_eq!(conflict.right_object, Some("configured datadir"));
    assert_eq!(
        verification_plan_error(&[intake_source, reference_source]),
        "verification-only source ethereum-mainnet:reference opened object configured \
         datadir resolves to the same provider location as intake source \
         ethereum-mainnet:intake opened object db"
    );
    fs::remove_dir_all(root).map_err(fixture_error)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn cross_named_reth_storage_child_alias_is_not_independent() -> RunnerResult<()> {
    let root =
        std::env::temp_dir().join(format!("bigname-reth-cross-child-{}", uuid::Uuid::new_v4()));
    let intake = root.join("intake");
    let reference = root.join("reference");
    let intake_db = intake.join("db");
    fs::create_dir_all(&intake_db).map_err(fixture_error)?;
    fs::create_dir_all(&reference).map_err(fixture_error)?;
    std::os::unix::fs::symlink(&intake_db, reference.join("static_files"))
        .map_err(fixture_error)?;
    let intake_source = reth_role_source("intake", &intake, SourceRole::Intake)?;
    let reference_source = reth_role_source("reference", &reference, SourceRole::VerificationOnly)?;

    let conflict = same_reth_path_identity(&intake_source, &reference_source)?;
    let conflict = conflict
        .expect("differently named opened storage children must be compared for shared identity");
    assert_eq!(conflict.left_object, Some("db"));
    assert_eq!(conflict.right_object, Some("static_files"));
    assert_eq!(
        verification_plan_error(&[intake_source, reference_source]),
        "verification-only source ethereum-mainnet:reference opened object static_files \
         resolves to the same provider location as intake source ethereum-mainnet:intake \
         opened object db"
    );
    fs::remove_dir_all(root).map_err(fixture_error)?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn shared_reth_storage_children_are_not_independent() -> RunnerResult<()> {
    let root = std::env::temp_dir().join(format!(
        "bigname-reth-shared-children-{}",
        uuid::Uuid::new_v4()
    ));
    let shared = root.join("shared");
    let left = root.join("left");
    let right = root.join("right");
    fs::create_dir_all(&shared).map_err(fixture_error)?;
    fs::create_dir_all(&left).map_err(fixture_error)?;
    fs::create_dir_all(&right).map_err(fixture_error)?;
    for child in RETH_DB_OPENED_STORAGE_CHILDREN {
        let target = shared.join(child);
        fs::create_dir_all(&target).map_err(fixture_error)?;
        std::os::unix::fs::symlink(&target, left.join(child)).map_err(fixture_error)?;
        std::os::unix::fs::symlink(&target, right.join(child)).map_err(fixture_error)?;
    }
    let left_source = reth_source("left", &left)?;
    let right_source = reth_source("right", &right)?;

    let same = same_reth_path_identity(&left_source, &right_source)?;
    fs::remove_dir_all(root).map_err(fixture_error)?;
    assert!(
        same.is_some(),
        "shared opened storage children must fail independence"
    );
    Ok(())
}

#[test]
fn distinct_reth_storage_children_remain_independent() -> RunnerResult<()> {
    let root = std::env::temp_dir().join(format!(
        "bigname-reth-distinct-children-{}",
        uuid::Uuid::new_v4()
    ));
    let left = root.join("left");
    let right = root.join("right");
    for wrapper in [&left, &right] {
        for child in RETH_DB_OPENED_STORAGE_CHILDREN {
            fs::create_dir_all(wrapper.join(child)).map_err(fixture_error)?;
        }
    }
    let left_source = reth_source("left", &left)?;
    let right_source = reth_source("right", &right)?;

    let same = same_reth_path_identity(&left_source, &right_source)?;
    fs::remove_dir_all(root).map_err(fixture_error)?;
    assert!(
        same.is_none(),
        "distinct opened storage children remain independent"
    );
    Ok(())
}

fn reth_source(key: &str, datadir: &Path) -> RunnerResult<SourceConfig> {
    reth_role_source(key, datadir, SourceRole::Both)
}

fn reth_role_source(key: &str, datadir: &Path, role: SourceRole) -> RunnerResult<SourceConfig> {
    SourceConfig::new_with_role(
        "ethereum-mainnet",
        key,
        "reth_db",
        SeedBasis::EthereumHead,
        0,
        role,
        datadir
            .to_str()
            .ok_or_else(|| RunnerError::data_integrity("non-UTF-8 test datadir"))?,
    )
}

fn verification_plan_error(sources: &[SourceConfig]) -> String {
    match super::super::verification_plan("ethereum-mainnet", sources) {
        Ok(_) => panic!("shared reth opened objects must fail verification planning"),
        Err(error) => error.to_string(),
    }
}

fn fixture_error(error: std::io::Error) -> RunnerError {
    RunnerError::data_integrity(format!("reth identity fixture: {error}"))
}

#[test]
fn sepolia_direct_intake_keeps_rpc_reference_and_provider_trust() -> RunnerResult<()> {
    for kind in ["reth", "reth_db", "drpc"] {
        let intake = SourceConfig::new_with_role(
            "ethereum-sepolia",
            "sepolia-node",
            kind,
            SeedBasis::EthereumHead,
            0,
            SourceRole::Intake,
            if kind == "drpc" {
                "http://node.invalid"
            } else {
                "/reader/reth"
            },
        )?;
        validate_intake_shape("ethereum-sepolia", &[&intake])?;
        assert_eq!(
            provider_trusted_source("ethereum-sepolia", &[&intake])?.source_kind,
            kind
        );
        let trusted =
            super::super::verification_plan("ethereum-sepolia", std::slice::from_ref(&intake))?;
        assert_eq!(trusted.verification_level(), VerificationLevel::QuickSynced);
        let reference = SourceConfig::new_with_role(
            "ethereum-sepolia",
            "archive-reference",
            "drpc",
            SeedBasis::EthereumHead,
            0,
            SourceRole::VerificationOnly,
            "https://independent.invalid",
        )?;
        let compared = super::super::verification_plan("ethereum-sepolia", &[intake, reference])?;
        assert_eq!(
            compared.verification_level(),
            VerificationLevel::CrossChecked
        );
    }
    Ok(())
}
