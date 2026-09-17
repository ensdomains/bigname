//! Resolver admission announced by the resolver itself or its factory rather than by a registry
//! pointer: an ERC-1967 `Upgraded(implementation)` or a `VerifiableFactory` `ProxyDeployed`
//! naming an implementation the same-deployment `ens_v2_resolver_l1` manifest declares in
//! `resolver_implementations`.

use super::{Catalog, ManifestEvent, ManifestSource, RawLogInput};

pub(in crate::schema_v2) const ANNOUNCEMENT_ADMISSION_BASIS: &str =
    "declared_resolver_implementation";

/// The `Upgraded` selection scope is every emitter, narrowed by `topic1`: the log is selected
/// only when the indexed implementation is declared by this manifest.
pub(super) fn announces_declared_implementation(
    source: &ManifestSource,
    event: &ManifestEvent,
    raw: &RawLogInput,
) -> bool {
    source.source_family == "ens_v2_resolver_l1"
        && event.name == "Upgraded"
        && raw
            .topics
            .get(1)
            .and_then(|topic| topic_address(topic))
            .is_some_and(|implementation| source.resolver_implementations.contains(&implementation))
}

fn topic_address(topic: &str) -> Option<String> {
    let hex = topic.strip_prefix("0x")?;
    (hex.len() == 64 && hex[..24].bytes().all(|byte| byte == b'0'))
        .then(|| format!("0x{}", hex[24..].to_ascii_lowercase()))
}

impl Catalog {
    /// The same-namespace, same-chain, same-deployment resolver manifest declaring
    /// `implementation`; it is the admitting authority for an announced proxy.
    pub(in crate::schema_v2) fn resolver_implementation_authority(
        &self,
        source: &ManifestSource,
        implementation: &str,
    ) -> Option<&ManifestSource> {
        let implementation = implementation.to_ascii_lowercase();
        self.manifests.iter().find(|candidate| {
            candidate.source_family == "ens_v2_resolver_l1"
                && candidate.namespace == source.namespace
                && candidate.chain_id == source.chain_id
                && candidate.deployment_label == source.deployment_label
                && candidate.resolver_implementations.contains(&implementation)
        })
    }

    /// The block of the earliest discovery admission for `raw`'s emitter that opens after
    /// `raw`'s block, with the source family the emitter interprets under. Admissions are loaded
    /// only when they opened before the batch, so such an admission was opened inside this batch
    /// and the log preceded it.
    pub(in crate::schema_v2) fn later_discovery_admission(
        &self,
        raw: &RawLogInput,
    ) -> Option<(i64, String)> {
        self.admissions
            .for_address(&raw.emitting_address)
            .filter(|admission| {
                admission.discovery_edge_kind.is_some()
                    && admission
                        .address
                        .eq_ignore_ascii_case(&raw.emitting_address)
                    && admission
                        .active_from_block
                        .is_some_and(|from| from > raw.block_number)
            })
            .min_by_key(|admission| admission.active_from_block)
            .and_then(|admission| {
                let source = self.source(admission.source_manifest_id?)?;
                let family = super::inferred_family(
                    &source.source_family,
                    admission.discovery_edge_kind.as_deref(),
                )
                .unwrap_or(&source.source_family);
                Some((admission.active_from_block?, family.to_owned()))
            })
    }
}
