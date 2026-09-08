use sqlx::types::time::OffsetDateTime;

/// How the first observation of a registry contract was established.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryCreationBasis {
    /// The contract emitted `RegistryCreated`.
    Announcement,
    /// The earliest ENSv2 `SubregistryUpdated` pointing at the contract.
    SubregistryPointer,
    /// A manifest-declared registry address with no observed on-chain creation.
    Declared,
}

impl RegistryCreationBasis {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Announcement => "announcement",
            Self::SubregistryPointer => "subregistry_pointer",
            Self::Declared => "declared",
        }
    }
}

/// First-observation evidence for one registry contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryCreation {
    pub basis: RegistryCreationBasis,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub block_timestamp: Option<OffsetDateTime>,
}

/// One known registry contract on one chain, keyed by lowercase address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryContractRow {
    pub chain_id: String,
    pub address: String,
    pub created: RegistryCreation,
}

/// The current ENSv2 subregistry pointer of one name: the latest canonical
/// `SubregistryChanged` for the name, with the registry that emitted it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubregistryPointer {
    pub logical_name_id: String,
    pub namespace: String,
    pub display_name: String,
    pub namehash: String,
    pub chain_id: String,
    /// Lowercase target registry address; `None` when the pointer was cleared.
    pub subregistry: Option<String>,
    /// Lowercase address of the registry holding the label (the event emitter).
    pub registry: Option<String>,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub transaction_hash: Option<String>,
    pub block_timestamp: Option<OffsetDateTime>,
}

/// Storage-local keyset cursor for names referencing one registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryReferenceKeysetCursor {
    pub display_name: String,
    pub logical_name_id: String,
}

impl From<&SubregistryPointer> for RegistryReferenceKeysetCursor {
    fn from(row: &SubregistryPointer) -> Self {
        Self {
            display_name: row.display_name.clone(),
            logical_name_id: row.logical_name_id.clone(),
        }
    }
}

/// Bounded page of names whose current subregistry pointer targets one registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryReferencePage {
    pub rows: Vec<SubregistryPointer>,
    pub next_cursor: Option<RegistryReferenceKeysetCursor>,
}
