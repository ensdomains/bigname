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
        "ethereum-sepolia" => valid_sepolia_intake_shape(sources),
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
                     shape: exactly one dRPC or Reth DB intake-capable source with ethereum_head seed basis \
                     and start block 0 or the admitted hackathon deployment start"
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
             start block 0 or the admitted hackathon deployment start"
        ),
    ))
}

fn valid_sepolia_intake_shape(sources: &[&SourceConfig]) -> bool {
    sources.len() == 1
        && matches!(
            normalized_source_kind(&sources[0].source_kind).as_str(),
            "drpc" | "reth" | "reth_db"
        )
        && sources[0].seed_basis == SeedBasis::EthereumHead
        && sources[0].sepolia_start_is_admitted()
}

fn valid_sepolia_drpc_shape(sources: &[&SourceConfig]) -> bool {
    sources.len() == 1
        && normalized_source_kind(&sources[0].source_kind) == "drpc"
        && sources[0].seed_basis == SeedBasis::EthereumHead
        && sources[0].sepolia_start_is_admitted()
}

pub(super) fn same_source_identity(
    left: &SourceConfig,
    right: &SourceConfig,
) -> RunnerResult<Option<SourceIdentityConflict>> {
    let left_kind = normalized_source_kind(&left.source_kind);
    let right_kind = normalized_source_kind(&right.source_kind);
    if left_kind == "drpc" && right_kind == "drpc" {
        return Ok(
            (rpc_endpoint_identity(left)? == rpc_endpoint_identity(right)?)
                .then(SourceIdentityConflict::default),
        );
    }
    if matches!(left_kind.as_str(), "reth" | "reth_db")
        && matches!(right_kind.as_str(), "reth" | "reth_db")
    {
        return same_reth_path_identity(left, right);
    }
    Ok((left.endpoint() == right.endpoint()).then(SourceIdentityConflict::default))
}

#[derive(Default)]
pub(super) struct SourceIdentityConflict {
    pub(super) left_object: Option<&'static str>,
    pub(super) right_object: Option<&'static str>,
}

struct RethOpenedObject {
    name: &'static str,
    path: PathBuf,
}

fn same_reth_path_identity(
    left: &SourceConfig,
    right: &SourceConfig,
) -> RunnerResult<Option<SourceIdentityConflict>> {
    let left = reth_opened_objects(absolute_reth_path(left)?);
    let right = reth_opened_objects(absolute_reth_path(right)?);
    for left_object in &left {
        for right_object in &right {
            if same_reth_paths_with_fallback(
                &left_object.path,
                &right_object.path,
                reth_path_spelling_identity,
            ) {
                return Ok(Some(SourceIdentityConflict {
                    left_object: Some(left_object.name),
                    right_object: Some(right_object.name),
                }));
            }
        }
    }
    Ok(None)
}

fn reth_opened_objects(datadir: PathBuf) -> Vec<RethOpenedObject> {
    let mut objects = Vec::with_capacity(RETH_DB_OPENED_STORAGE_CHILDREN.len() + 1);
    objects.push(RethOpenedObject {
        name: "configured datadir",
        path: datadir.clone(),
    });
    objects.extend(
        RETH_DB_OPENED_STORAGE_CHILDREN
            .into_iter()
            .map(|child| RethOpenedObject {
                name: child,
                path: datadir.join(child),
            }),
    );
    objects
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
        path: normalize_rpc_path(endpoint.path()),
        query: endpoint.query().map(normalize_percent_encoding),
    })
}

fn normalize_rpc_path(path: &str) -> String {
    let mut normalized = normalize_percent_encoding(path);
    if normalized.ends_with('/') && normalized.bytes().any(|byte| byte != b'/') {
        normalized.pop();
    }
    normalized
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
        "base-mainnet" => "drpc",
        "ethereum-sepolia" => {
            validate_intake_shape(chain_id, intake)?;
            return Ok(intake[0]);
        }
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
#[path = "verify_source_tests.rs"]
mod tests;
