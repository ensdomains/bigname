use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A fully evaluated name, including the explicit no-row outcome and its next clock boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct LookupNamePublication {
    pub core: Option<LookupNameCore>,
    pub relations: Vec<LookupRelation>,
    pub recompose_at: Option<i64>,
}

/// Publication-independent fields used by lookup. Names, diagnostic history, execution
/// topology, current block timestamps and current positions are deliberately absent.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LookupNameCore {
    pub surface_binding_id: Option<Uuid>,
    pub resource_id: Option<Uuid>,
    pub serving_resource_id: Option<Uuid>,
    pub record_serving_resource_id: Option<Uuid>,
    pub binding_kind: Option<String>,
    pub declared_summary: Value,
    pub provenance: Value,
    pub coverage: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct LookupRelation {
    pub address: String,
    pub relation: String,
}

/// Narrow input keys consulted by inventory selection. Missing keys are retained too; a later
/// insertion can change an inventory whose current selection is empty.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LookupInventoryDependency {
    ResourcePointer {
        resource_id: Uuid,
    },
    Identity {
        logical_name_id: String,
    },
    Classification {
        resolver_address: String,
    },
    RegistryNode {
        namespace: String,
        node: String,
    },
    Partition {
        resolver_address: String,
        arm: String,
        arm_identity: String,
    },
    Link {
        resolver_address: String,
        node: String,
    },
    RecordId {
        resolver_address: String,
        record_id: String,
    },
}

/// One selected key's payload and evidence, kept together so an ordinary value change updates
/// just this key. Selector and absence arrays and record-event provenance are assembled at read.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LookupRecordEntry {
    pub entries: Vec<Value>,
    pub selectors: Vec<Value>,
    pub normalized_event_id: Option<i64>,
    pub zero_address_absent: bool,
    pub unsupported_family: Option<String>,
}

/// One resource evaluated by the shared inventory plan. `None` is a known absence, not missing
/// preparation. The transient full inventory is also returned to ordinary composed readers.
#[derive(Clone, Debug)]
pub struct LookupInventoryPublication {
    pub inventory: Option<super::super::records::FamilyRecordInventory>,
    pub records: BTreeMap<String, LookupRecordEntry>,
    pub dependencies: BTreeSet<LookupInventoryDependency>,
}

impl LookupInventoryPublication {
    pub(crate) fn absent(resource_id: Uuid) -> Self {
        Self {
            inventory: None,
            records: BTreeMap::new(),
            dependencies: BTreeSet::from([LookupInventoryDependency::ResourcePointer {
                resource_id,
            }]),
        }
    }
}
