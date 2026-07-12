use alloy_primitives::{hex, keccak256};

pub(crate) const SOURCE_FAMILY_BASENAMES_BASE_REGISTRY: &str = "basenames_base_registry";

/// The Basenames registry family's full manifest ABI event set. The
/// Basenames L2 registry inherits the canonical ENS registry event surface
/// unchanged (upstream: .refs/basenames/src/L2/Registry.sol:14 `contract
/// Registry is ENS` @ basenames@1809bbc; event declarations:
/// .refs/ens_v1/contracts/registry/ENS.sol:L6-L15 @ ens_v1@91c966f). The
/// hash-pinned scan-all fetches these topic0s across all emitters, so the set
/// must stay equal to the family's active manifest ABI events — a corpus
/// parity test asserts this, and jobs persist the set verbatim so promotion's
/// topic-drift guard fails closed if the manifest ever grows past it.
const BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES: &[&str] = &[
    "NewOwner(bytes32,bytes32,address)",
    "NewResolver(bytes32,address)",
    "NewTTL(bytes32,uint64)",
    "Transfer(bytes32,address)",
];

pub(crate) fn basenames_registry_scan_all_event_signatures() -> Vec<String> {
    BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES
        .iter()
        .map(|signature| (*signature).to_owned())
        .collect()
}

pub(crate) fn basenames_registry_scan_all_topic0s() -> Vec<String> {
    BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES
        .iter()
        .map(|signature| format!("0x{}", hex::encode(keccak256(signature.as_bytes()))))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::PathBuf;

    use anyhow::{Context, Result};
    use bigname_manifests::{RolloutStatus, load_repository};

    use super::*;

    /// The scan-all constants must stay in corpus parity with the checked-in
    /// manifests: the topic0 set derived from every active
    /// basenames_base_registry manifest ABI must equal the hardcoded set.
    /// Promotion's topic-drift guard fails closed at runtime if the deployed
    /// manifests diverge from a persisted job identity; this test fails the
    /// build first, when the divergence is introduced in-repo.
    #[test]
    fn scan_all_topic0s_match_checked_in_active_registry_manifests() -> Result<()> {
        let expected = basenames_registry_scan_all_topic0s()
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            expected.len(),
            BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES.len()
        );

        let mut active_manifest_count = 0usize;
        let mut loaded_manifests = Vec::new();
        for profile_root in ["manifests/mainnet", "manifests/sepolia"] {
            let manifests_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../..")
                .join(profile_root);
            let repository = load_repository(&manifests_root)
                .with_context(|| format!("failed to load {}", manifests_root.display()))?;
            loaded_manifests.extend(repository.manifests().to_vec());
        }
        for loaded in &loaded_manifests {
            if loaded.manifest.source_family != SOURCE_FAMILY_BASENAMES_BASE_REGISTRY
                || loaded.manifest.rollout_status != RolloutStatus::Active
            {
                continue;
            }
            active_manifest_count += 1;
            let manifest_topic0s = loaded
                .manifest
                .abi
                .event_topic0s()
                .with_context(|| format!("failed to derive topic0s for {}", loaded.path.display()))?
                .into_iter()
                .map(|topic0| topic0.to_ascii_lowercase())
                .collect::<BTreeSet<String>>();
            assert_eq!(
                manifest_topic0s,
                expected,
                "active manifest {} ABI event topic0s diverged from the hash-pinned scan-all \
                 constants; update BASENAMES_REGISTRY_SCAN_ALL_EVENT_SIGNATURES (completed jobs \
                 persist their fetched set verbatim, so promotion re-checks old jobs on its own)",
                loaded.relative_path.display()
            );
        }
        assert!(
            active_manifest_count > 0,
            "no active basenames_base_registry manifest found in the checked-in profile roots; \
             the corpus parity guard would be vacuous"
        );
        Ok(())
    }
}
