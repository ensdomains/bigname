//! Manifest loading for the binary's start-up: hashed off the runtime so a stop
//! is never held by the filesystem, then synchronized into the database.

use anyhow::{Context, Result, bail, ensure};
use phase_runner::config::{COMPILED_CHAIN_NAMESPACES, validate_deployment_table_set};

/// Hash the manifest tree on a detached OS thread and await the result.
///
/// The work is synchronous filesystem I/O, so inline it never yields and a stop
/// cannot be observed until it finishes. `spawn_blocking` is not enough either:
/// those tasks cannot be aborted and the runtime joins its blocking pool when it
/// is dropped, so a caller that abandons the await still waits for the hash. A
/// detached thread is not joined at exit, so abandoning the await lets the
/// process leave immediately.
pub(super) fn hash_manifests_off_runtime(
    root: std::path::PathBuf,
) -> tokio::sync::oneshot::Receiver<Result<(bigname_manifests::ManifestRepository, &'static str)>> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let _ = sender.send(load_hashed_manifest_repository(&root));
    });
    receiver
}

pub(super) fn load_hashed_manifest_repository(
    root: &std::path::Path,
) -> Result<(bigname_manifests::ManifestRepository, &'static str)> {
    let before = bigname_content_hash::manifest_profile_hash(root)
        .with_context(|| format!("failed to fingerprint manifest profile {}", root.display()))?;
    let Some((profile, _)) = bigname_content_hash::HASHED_MANIFEST_PROFILES
        .iter()
        .find(|(_, expected)| *expected == before)
    else {
        bail!(
            "runtime manifest profile {} has fingerprint {before}, which is not covered by this binary's interpreter content hash {}",
            root.display(),
            bigname_content_hash::INTERPRETER_CONTENT_HASH
        );
    };

    let repository = bigname_manifests::load_repository(root)?;
    let after = bigname_content_hash::manifest_profile_hash(root).with_context(|| {
        format!(
            "failed to re-fingerprint manifest profile {}",
            root.display()
        )
    })?;
    ensure!(
        before == after,
        "runtime manifest profile {} changed while it was being loaded",
        root.display()
    );
    Ok((repository, profile))
}

/// Check the configured start blocks and validate the deployment table set.
pub(super) fn bind_runtime_manifests(
    _repository: &bigname_manifests::ManifestRepository,
    _profile: &'static str,
    chains: &mut [phase_runner::config::ChainConfig],
) -> Result<()> {
    if chains.iter().any(|chain| {
        chain.chain_id == "ethereum-sepolia"
            && chain
                .sources
                .iter()
                .any(|source| source.start_block_number != 0)
    }) {
        bail!("Sepolia intake must start at block zero");
    }
    validate_deployment_table_set(chains, COMPILED_CHAIN_NAMESPACES.iter().copied())?;
    Ok(())
}

/// The synchronous form, for callers that are not racing a stop.
#[cfg(test)]
pub(super) fn prepare_runtime_manifests(
    root: &std::path::Path,
    chains: &mut [phase_runner::config::ChainConfig],
) -> Result<(bigname_manifests::ManifestRepository, &'static str)> {
    let (repository, profile) = load_hashed_manifest_repository(root)?;
    bind_runtime_manifests(&repository, profile, chains)?;
    Ok((repository, profile))
}
