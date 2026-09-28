use super::types::PrimaryNameClaimStatus;

/// The projection stores the raw claim spelling plus a marker for whether those bytes were already
/// normalized; it stores no normalized column. When the marker is set the stored bytes are the
/// normalized form and are returned as published, so a later normalizer revision cannot silently
/// restate an already-published name. Otherwise a successful claim normalizes by construction — the
/// projection classifies anything else `invalid_name` — so derive it once here rather than leaving
/// each reader to repair a null or re-derive the rule.
///
/// A successful claim that no longer normalizes is possible only while a normalizer revision is
/// mid-re-derivation, and it is one row's defect. It reads as "no normalized name" so a single row
/// cannot fail a whole page; readers that must state a status report it as `invalid_name`.
pub fn normalized_claim_name(
    claim_status: PrimaryNameClaimStatus,
    claim_name_is_normalized: bool,
    raw_claim_name: Option<&str>,
) -> Option<String> {
    if claim_status != PrimaryNameClaimStatus::Success {
        return None;
    }
    if claim_name_is_normalized {
        return raw_claim_name.map(str::to_owned);
    }
    raw_claim_name
        .and_then(|raw_claim_name| {
            bigname_domain::normalization::normalize_name(raw_claim_name).ok()
        })
        .map(|normalized| normalized.normalized_name)
}
