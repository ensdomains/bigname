use alloy_primitives::{B256, keccak256};
use anyhow::{Context, ensure};
use bigname_domain::normalization::ENS_NORMALIZER_VERSION;
use serde_json::{Value, json};

use crate::schema_v2::{
    catalog::Selected,
    model::{BatchOutput, NameSurface, RawLogInput},
    state::State,
};

/// A name identity proved by its label-hash path, without the raw bytes of every label.
/// Materializing one emits no preimage observation and no label preimage.
#[derive(Clone, Debug)]
pub(in crate::schema_v2) struct NodeIdentityDraft {
    /// Label hashes leaf first, as `name_surfaces.labelhashes` stores them.
    pub labelhashes: Vec<String>,
    pub namehash: String,
}

pub(super) fn materialize(
    selected: &Selected,
    raw: &RawLogInput,
    drafts: &[NodeIdentityDraft],
    provenance: &Value,
    state: &mut State,
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    let namespace = &selected.source.namespace;
    let v1_surface = selected.source.source_family.starts_with("ens_v1_")
        || selected.source.source_family.starts_with("basenames_");
    for draft in drafts {
        ensure!(
            !draft.labelhashes.is_empty(),
            "node identity {namespace}:{} has no label hashes",
            draft.namehash
        );
        let node = draft
            .labelhashes
            .iter()
            .rev()
            .try_fold(B256::ZERO, |node, labelhash| {
                let labelhash = labelhash_word(labelhash)?;
                let mut input = [0u8; 64];
                input[..32].copy_from_slice(node.as_slice());
                input[32..].copy_from_slice(labelhash.as_slice());
                anyhow::Ok(keccak256(input))
            })
            .with_context(|| format!("node identity {namespace}:{}", draft.namehash))?;
        ensure!(
            format!("{node:#x}") == draft.namehash,
            "node identity {namespace}:{} does not hash from its label-hash path",
            draft.namehash
        );
        if v1_surface {
            state.materialize_v1_surface(namespace, &draft.namehash, true, false);
        }
        output.name_surfaces.push(NameSurface {
            logical_name_id: format!("{namespace}:{}", draft.namehash),
            namespace: namespace.clone(),
            raw: None,
            namehash: draft.namehash.clone(),
            labelhashes: draft.labelhashes.clone(),
            normalizer_version: ENS_NORMALIZER_VERSION.to_owned(),
            visibility_state: "active".to_owned(),
            normalization_errors: json!([]),
            deactivation_reason: None,
            deactivated_at: None,
            chain_id: raw.chain_id.clone(),
            block_hash: raw.block_hash.clone(),
            block_number: raw.block_number,
            provenance: provenance.clone(),
            canonicality_state: raw.canonicality_state.clone(),
        });
    }
    Ok(())
}

fn labelhash_word(labelhash: &str) -> anyhow::Result<B256> {
    let digits = labelhash
        .strip_prefix("0x")
        .filter(|digits| {
            digits.len() == 64
                && digits
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .with_context(|| format!("label hash {labelhash} is not lowercase 32-byte hex"))?;
    digits
        .parse::<B256>()
        .with_context(|| format!("label hash {labelhash} is not a 32-byte word"))
}
