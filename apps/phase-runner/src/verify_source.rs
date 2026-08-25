use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use crate::{
    config::{SeedBasis, SourceConfig, normalized_source_kind},
    error::{ErrorKind, RunnerError, RunnerResult},
};
use bigname_ingest::{BASE_COINBASE_SEAM_BLOCK, RETH_DB_OPENED_STORAGE_CHILDREN};
use url::Url;
pub(crate) fn production_verify_chain(id: &str) -> bool {
    matches!(id, "base-mainnet" | "ethereum-mainnet" | "ethereum-sepolia")
}
pub(super) fn provider_configuration_error(source: &super::VerificationSource) -> RunnerError {
    let key = source.source_key();
    RunnerError::new(ErrorKind::Configuration, format!("source {key} is invalid"))
}
pub(super) fn validate_intake_shape(chain_id: &str, sources: &[&SourceConfig]) -> RunnerResult<()> {
    let kind = |source: &&SourceConfig| normalized_source_kind(&source.source_kind);
    let valid = match chain_id {
        "base-mainnet" => {
            sources.len() == 2
                && sources.iter().any(|source| {
                    kind(source) == "coinbase_sql"
                        && source.seed_basis == SeedBasis::BaseSeam
                        && source.start_block_number <= BASE_COINBASE_SEAM_BLOCK
                })
                && sources.iter().any(|source| {
                    kind(source) == "drpc"
                        && source.seed_basis == SeedBasis::BaseSeam
                        && source.start_block_number == BASE_COINBASE_SEAM_BLOCK
                })
        }
        "ethereum-mainnet" => {
            sources.len() == 1
                && matches!(kind(&sources[0]), kind if kind == "reth" || kind == "reth_db")
                && sources[0].seed_basis == SeedBasis::EthereumHead
                && sources[0].start_block_number == 0
        }
        "ethereum-sepolia" => valid_sepolia_drpc_shape(sources),
        _ => true,
    };
    if !valid {
        let message = match chain_id {
            "ethereum-sepolia" => {
                let descriptors = sources
                    .iter()
                    .map(|source| format!("{chain_id}:{}", source.source_key))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "chain {chain_id} intake descriptors [{descriptors}] violate the required \
                     shape: exactly one dRPC intake-capable source with ethereum_head seed basis \
                     and start block 0"
                )
            }
            _ => format!("chain {chain_id} has an unsupported production intake shape"),
        };
        return Err(RunnerError::new(ErrorKind::Configuration, message));
    }
    Ok(())
}

pub(super) fn validate_sepolia_verification_shape(
    chain_id: &str,
    sources: &[&SourceConfig],
) -> RunnerResult<()> {
    if valid_sepolia_drpc_shape(sources) {
        return Ok(());
    }
    let descriptors = sources
        .iter()
        .map(|source| format!("{chain_id}:{}", source.source_key))
        .collect::<Vec<_>>()
        .join(", ");
    Err(RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "chain {chain_id} verification-only descriptors [{descriptors}] violate the required \
             shape: exactly one dRPC verification-only source with ethereum_head seed basis and \
             start block 0"
        ),
    ))
}

fn valid_sepolia_drpc_shape(sources: &[&SourceConfig]) -> bool {
    sources.len() == 1
        && normalized_source_kind(&sources[0].source_kind) == "drpc"
        && sources[0].seed_basis == SeedBasis::EthereumHead
        && sources[0].start_block_number == 0
}

pub(super) fn same_source_identity(
    left: &SourceConfig,
    right: &SourceConfig,
) -> RunnerResult<bool> {
    let left_kind = normalized_source_kind(&left.source_kind);
    let right_kind = normalized_source_kind(&right.source_kind);
    if left_kind == "drpc" && right_kind == "drpc" {
        return Ok(rpc_endpoint_identity(left)? == rpc_endpoint_identity(right)?);
    }
    if matches!(left_kind.as_str(), "reth" | "reth_db")
        && matches!(right_kind.as_str(), "reth" | "reth_db")
    {
        return same_reth_path_identity(left, right);
    }
    Ok(left.endpoint() == right.endpoint())
}

fn same_reth_path_identity(left: &SourceConfig, right: &SourceConfig) -> RunnerResult<bool> {
    let left = absolute_reth_path(left)?;
    let right = absolute_reth_path(right)?;
    if same_reth_paths_with_fallback(&left, &right, reth_path_spelling_identity) {
        return Ok(true);
    }
    Ok(RETH_DB_OPENED_STORAGE_CHILDREN.into_iter().any(|child| {
        same_reth_paths_with_fallback(
            &left.join(child),
            &right.join(child),
            reth_path_spelling_identity,
        )
    }))
}

fn absolute_reth_path(source: &SourceConfig) -> RunnerResult<PathBuf> {
    let configured = Path::new(source.endpoint());
    if configured.is_absolute() {
        Ok(configured.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|current| current.join(configured))
            .map_err(|_| unresolved_reth_path(source))
    }
}

fn same_reth_paths_with_fallback(
    left: &Path,
    right: &Path,
    fallback: impl Fn(&Path) -> PathBuf,
) -> bool {
    same_existing_filesystem_object(left, right)
        .unwrap_or_else(|| fallback(left) == fallback(right))
}

#[cfg(unix)]
fn same_existing_filesystem_object(left: &Path, right: &Path) -> Option<bool> {
    use std::os::unix::fs::MetadataExt;

    let left = fs::metadata(left).ok()?;
    let right = fs::metadata(right).ok()?;
    Some(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(not(unix))]
fn same_existing_filesystem_object(_left: &Path, _right: &Path) -> Option<bool> {
    None
}

fn reth_path_spelling_identity(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| lexical_path_identity(path))
}

fn lexical_path_identity(absolute: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

fn unresolved_reth_path(source: &SourceConfig) -> RunnerError {
    RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "source descriptor {}:{} has a reth path that cannot be resolved",
            source.chain_id, source.source_key
        ),
    )
}

#[derive(Eq, PartialEq)]
struct RpcEndpointIdentity {
    scheme: String,
    username: String,
    password: Option<String>,
    host: String,
    port: Option<u16>,
    path: String,
    query: Option<String>,
}

fn rpc_endpoint_identity(source: &SourceConfig) -> RunnerResult<RpcEndpointIdentity> {
    let endpoint = Url::parse(source.endpoint()).map_err(|_| invalid_rpc_endpoint(source))?;
    if !matches!(endpoint.scheme(), "http" | "https") {
        return Err(invalid_rpc_endpoint(source));
    }
    let host = endpoint
        .host_str()
        .ok_or_else(|| invalid_rpc_endpoint(source))?;
    Ok(RpcEndpointIdentity {
        scheme: endpoint.scheme().to_owned(),
        username: normalize_percent_encoding(endpoint.username()),
        password: endpoint.password().map(normalize_percent_encoding),
        host: host.to_owned(),
        port: endpoint.port_or_known_default(),
        path: normalize_percent_encoding(endpoint.path()),
        query: endpoint.query().map(normalize_percent_encoding),
    })
}

fn invalid_rpc_endpoint(source: &SourceConfig) -> RunnerError {
    RunnerError::new(
        ErrorKind::Configuration,
        format!(
            "source descriptor {}:{} has an invalid RPC endpoint",
            source.chain_id, source.source_key
        ),
    )
}

fn normalize_percent_encoding(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut normalized = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let (Some(high), Some(low)) =
                (hex_value(bytes[index + 1]), hex_value(bytes[index + 2]))
        {
            let decoded = high * 16 + low;
            if decoded.is_ascii_alphanumeric() || matches!(decoded, b'-' | b'.' | b'_' | b'~') {
                normalized.push(decoded);
            } else {
                normalized.extend_from_slice(&[b'%', upper_hex(high), upper_hex(low)]);
            }
            index += 3;
            continue;
        }
        normalized.push(bytes[index]);
        index += 1;
    }
    String::from_utf8(normalized).expect("parsed URL components remain valid UTF-8")
}

const fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

const fn upper_hex(value: u8) -> u8 {
    if value < 10 {
        b'0' + value
    } else {
        b'A' + value - 10
    }
}
pub(super) fn provider_trusted_source<'a>(
    chain_id: &str,
    intake: &'a [&SourceConfig],
) -> RunnerResult<&'a SourceConfig> {
    let target_kind = match chain_id {
        "base-mainnet" | "ethereum-sepolia" => "drpc",
        "ethereum-mainnet" => "reth_db",
        _ => "",
    };
    let mut candidates = intake.iter().copied().filter(|source| {
        let kind = normalized_source_kind(&source.source_kind);
        kind == target_kind || (target_kind == "reth_db" && kind == "reth")
    });
    if let (Some(source), None) = (candidates.next(), candidates.next()) {
        return Ok(source);
    }
    let message = format!("chain {chain_id} requires one provider-trusted intake for Verify");
    Err(RunnerError::new(ErrorKind::Configuration, message))
}
#[cfg(test)]
mod tests {
    use super::super::VerificationLevel;
    use super::*;
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
        let plan =
            super::super::verification_plan("base-mainnet", &[coinbase.clone(), drpc.clone()])?;
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
        let compared = crate::config::ChainConfig::new(
            "base-mainnet",
            vec![coinbase, drpc, reference],
            false,
        )?;
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
        let root =
            std::env::temp_dir().join(format!("bigname-reth-identity-{}", uuid::Uuid::new_v4()));
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
        let root =
            std::env::temp_dir().join(format!("bigname-reth-missing-{}", uuid::Uuid::new_v4()));
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
            same,
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
        assert!(!same, "distinct opened storage children remain independent");
        Ok(())
    }

    fn reth_source(key: &str, datadir: &Path) -> RunnerResult<SourceConfig> {
        SourceConfig::new(
            "ethereum-mainnet",
            key,
            "reth_db",
            SeedBasis::EthereumHead,
            0,
            datadir
                .to_str()
                .ok_or_else(|| RunnerError::data_integrity("non-UTF-8 test datadir"))?,
        )
    }

    fn fixture_error(error: std::io::Error) -> RunnerError {
        RunnerError::data_integrity(format!("reth identity fixture: {error}"))
    }
}
