//! Which content hash each source input feeds (`docs/storage.md` § Interpretation replay).
//!
//! A file joins the interpreter set when any path Interpret's persistence or manifest sync
//! executes can be changed by it. Otherwise it joins the projection set when Project's persisted
//! output depends on it. A doubtful file is labelled interpret: a wrong "project" label leaves
//! interpreted rows stale with no redo, while a wrong "interpret" label costs one unneeded
//! Interpret redo.

/// The hash a source input feeds. `Both` feeds the interpreter hash only. The projection hash
/// covers it through the interpreter hash it embeds.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum Class {
    Interpret,
    Project,
    Both,
}

impl Class {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Interpret => "interpret",
            Self::Project => "project",
            Self::Both => "both",
        }
    }

    pub(crate) const fn feeds_interpreter_hash(self) -> bool {
        !matches!(self, Self::Project)
    }
}

/// A hashed root or file with its class and the reason for it.
pub(crate) struct Classified {
    pub(crate) path: &'static str,
    pub(crate) class: Class,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) reason: &'static str,
}

const fn interpret(path: &'static str, reason: &'static str) -> Classified {
    Classified {
        path,
        class: Class::Interpret,
        reason,
    }
}

pub(crate) const fn project(path: &'static str, reason: &'static str) -> Classified {
    Classified {
        path,
        class: Class::Project,
        reason,
    }
}

pub(crate) const fn both(path: &'static str, reason: &'static str) -> Classified {
    Classified {
        path,
        class: Class::Both,
        reason,
    }
}

pub(crate) const ADAPTER_SOURCE_ROOT: &str = "crates/adapters/src";
pub(crate) const MANIFEST_AUTHORITY_SOURCE_ROOT: &str = "crates/manifests/src";
pub(crate) const PROJECT_SOURCE_ROOT: &str = "crates/project/src";
pub(crate) const INTERPRET_WRITE_SOURCE_ROOT: &str = "crates/interpret/src/write";

/// Whole source trees. The crate graph proves each label for every file under the root
/// (`src/tests/classes.rs`): `bigname-project` is no dependency of an interpret-side crate, and the
/// adapter, manifest and interpret crates are no dependency of `bigname-project`.
pub(crate) const ROOTS: &[Classified] = &[
    interpret(
        ADAPTER_SOURCE_ROOT,
        "event decoding and interpretation behaviour; bigname-project does not depend on adapters",
    ),
    both(
        MANIFEST_AUTHORITY_SOURCE_ROOT,
        "Interpret's authority and decoding; manifest sync also writes the manifest rows Project \
         reads",
    ),
    project(
        PROJECT_SOURCE_ROOT,
        "Project only, its SQL included; no interpret-side crate depends on bigname-project",
    ),
    // Which interpreted row wins a conflict, how a redo range reopens and reanchors bindings, and
    // which surfaces a normalizer-version recompute activates.
    interpret(INTERPRET_WRITE_SOURCE_ROOT, "Interpret's persistence stage"),
];

/// Manifest ABI event blocks, keyed `manifest-event:`.
const MANIFEST_EVENT_REASON: &str = "ABI decoding of raw logs";
/// Files a hashed file compiles in with `include_str!` or `include_bytes!`.
const DATA_READ_REASON: &str = "data a hashed file compiles in, classed by the files that read it";
/// Lockfile entries, keyed `semantic-crate:`.
const SEMANTIC_CRATE_REASON: &str =
    "alloy decodes logs and hydration results; serde parses manifests and serializes topology";

const HYDRATION: &str =
    "Project hydration multicalls before rows persist; Interpret does not depend on bigname-lookup";
const COMPOSITION: &str =
    "reached from Project's summary, catalogue or lookup composition; no interpret-side caller";
const SEARCH: &str = "Interpret prepares and refreshes identity search documents through it; \
                      Project reads the rows";

/// Sources outside the roots that decide persisted interpretation or projection output. Without
/// them a change to, say, ENS normalization would alter projected primary-name rows while leaving
/// both hashes, and the redo the phase guard demands, unchanged.
///
/// This is a file list rather than a crate root on purpose. `bigname-lookup` also holds the
/// request-scoped serving engine, CCIP-Read, storage and RPC transport, none of which decide a
/// persisted row. Watching the whole crate would rotate a hash, and force a re-derivation, for
/// serving-only edits. A missing entry is a hard error, so moving one of these modules fails the
/// build instead of silently narrowing a hash.
pub(crate) const SEMANTIC_SOURCE_FILES: &[Classified] = &[
    both(
        "crates/domain/src/normalization.rs",
        "ENS normalization decides adapter identity rows and Project's primary-name claims",
    ),
    project(
        "crates/domain/src/resolution_topology.rs",
        "final serializer of projected resolution topology; only storage topology uses it",
    ),
    both(
        "crates/domain/src/vocabulary.rs",
        "closed vocabularies the manifest repository and projected topology both use",
    ),
    both(
        "crates/domain/src/resolver_read.rs",
        "resolver-read features the manifest model re-exports and manifest sync persists",
    ),
    project(
        "crates/domain/src/chain_identity.rs",
        "the numeric chain id a stored ens_v1 resolver summary carries",
    ),
    project("crates/lookup/src/abi.rs", HYDRATION),
    // Deliberately not `crates/lookup/src/types.rs`, the request-scoped lookup response shape.
    project("crates/lookup/src/record_selector.rs", HYDRATION),
    project("crates/lookup/src/reverse_names.rs", HYDRATION),
    project("crates/lookup/src/text_records.rs", HYDRATION),
    // Which provider response hydration accepts as an answer. Deliberately not the rest of
    // `crates/lookup/src/rpc.rs`: client construction, timeouts and endpoint configuration abort
    // a request rather than reshape an answer.
    project("crates/lookup/src/json_rpc_envelope.rs", HYDRATION),
    interpret(
        "crates/interpret/src/write.rs",
        "redo-range preparation that drives Interpret's persistence stage",
    ),
    interpret(
        "crates/interpret/src/recompute.rs",
        "the normalizer-version recompute of interpreted surfaces",
    ),
    interpret(
        "crates/interpret/src/recompute/search.rs",
        "search spelling the normalizer-version recompute writes",
    ),
    project("crates/storage/src/address_names/query.rs", COMPOSITION),
    project(
        "crates/storage/src/address_names/query/timestamps.rs",
        COMPOSITION,
    ),
    project("crates/storage/src/address_names/types.rs", COMPOSITION),
    project(
        "crates/storage/src/history/address_evidence.rs",
        COMPOSITION,
    ),
    project(
        "crates/storage/src/history/catalogue_contract.rs",
        COMPOSITION,
    ),
    project("crates/storage/src/history/source.rs", COMPOSITION),
    both("crates/storage/src/identity_search.rs", SEARCH),
    both("crates/storage/src/identity_search/documents.rs", SEARCH),
    both("crates/storage/src/identity_search/tokens.rs", SEARCH),
    both(
        "crates/storage/src/label_preimages.rs",
        "Interpret's recompute reads its rainbow source kind and its import refreshes search; \
         Project reads the preimage rows",
    ),
    project(
        "crates/storage/src/name_current/wrapper_expiry.rs",
        COMPOSITION,
    ),
    // `families/name/rendered.rs` names `NameCurrentRow` only in a function Interpret never
    // calls.
    project("crates/storage/src/name_current/row.rs", COMPOSITION),
    project(
        "crates/storage/src/name_current/public_authority.rs",
        COMPOSITION,
    ),
    project("crates/storage/src/identity/ids.rs", COMPOSITION),
    project("crates/storage/src/identity/types.rs", COMPOSITION),
    project(
        "crates/storage/src/public_name_fields/ens_v1.rs",
        COMPOSITION,
    ),
    project("crates/storage/src/public_name_fields/mod.rs", COMPOSITION),
    project(
        "crates/storage/src/public_name_fields/registration.rs",
        COMPOSITION,
    ),
    project(
        "crates/storage/src/public_name_fields/types.rs",
        COMPOSITION,
    ),
    project(
        "crates/storage/src/public_name_fields/values.rs",
        COMPOSITION,
    ),
    project(
        "crates/storage/src/public_name_fields/wrapper.rs",
        COMPOSITION,
    ),
    project(
        "crates/storage/src/public_name_fields/wrapper_fuses.rs",
        COMPOSITION,
    ),
    project(
        "crates/storage/src/public_name_fields/wrapper_state.rs",
        COMPOSITION,
    ),
    project(
        "crates/storage/src/record_inventory/boundary_key.rs",
        COMPOSITION,
    ),
    project(
        "crates/storage/src/record_inventory/row_decode.rs",
        COMPOSITION,
    ),
    project("crates/storage/src/unix_seconds.rs", COMPOSITION),
    project("crates/storage/src/expiry.rs", COMPOSITION),
];

/// The class and reason of a root, file or composition entry that holds `relative_path`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn listed(relative_path: &str) -> Option<&'static Classified> {
    SEMANTIC_SOURCE_FILES
        .iter()
        .chain(crate::storage_families::COMPOSITION_FILES)
        .find(|entry| entry.path == relative_path)
        .or_else(|| {
            ROOTS.iter().find(|root| {
                relative_path
                    .strip_prefix(root.path)
                    .is_some_and(|rest| rest.starts_with('/'))
            })
        })
}

/// The class reason of any input key. A `source:` key that no entry lists is a data read, which
/// takes its class from the hashed files that read it.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn reason(key: &str) -> &'static str {
    if let Some(path) = key.strip_prefix("source:") {
        listed(path).map_or(DATA_READ_REASON, |entry| entry.reason)
    } else if key.starts_with("manifest-event:") {
        MANIFEST_EVENT_REASON
    } else {
        SEMANTIC_CRATE_REASON
    }
}
