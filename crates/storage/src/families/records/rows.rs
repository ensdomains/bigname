//! The F6 and F7 rows one serving pointer admits (record_inventory.rs, `attributed_events`), read
//! as record candidates, and the partition version events that are boundary candidates.
use anyhow::{Context, Result};
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Row, postgres::PgRow};

use super::{FamilyPosition, facts::ResolverClassification, serving::ServingPointer};

const V1_POINTER_FAMILIES: [&str; 3] = [
    "ens_v1_registry_l1",
    "ens_v1_registrar_l1",
    "ens_v1_wrapper_l1",
];
const V2_POINTER_FAMILIES: [&str; 2] = ["ens_v2_registry_l1", "ens_v2_root_l1"];

/// The F6 partitions (arm, arm identity) the four attribution arms admit for `pointer`: the named
/// arm by the pointer's logical name alone; the native arm by the pointer family's paired resolver
/// family; the guarded ENSv2-origin arm when the resolver's classification is supported, declared
/// in the pointer's namespace, and of a family the arm admits. The fourth arm, the linked records,
/// is read from F7 through the link selection.
pub(crate) fn admitted_partitions(
    pointer: &ServingPointer,
    classification: Option<&ResolverClassification>,
) -> Vec<(&'static str, String)> {
    let node = &pointer.namehash;
    let mut partitions = vec![("named", pointer.logical_name_id.clone())];
    let family = pointer.source_family.as_str();
    if V1_POINTER_FAMILIES.contains(&family) {
        partitions.push(("native", format!("{node}|ens_v1_resolver_l1")));
    }
    if family == "basenames_base_registry" {
        partitions.push(("native", format!("{node}|basenames_base_resolver")));
    }
    let guarded = classification.filter(|classification| {
        V2_POINTER_FAMILIES.contains(&family)
            && classification.supported()
            && classification.field("basis") == Some("manifest_declared_address")
            && classification.declared_in(&pointer.namespace)
    });
    if let Some(classification) = guarded {
        match (
            classification.field("source_family"),
            classification.field("role"),
        ) {
            // An unnamed ENSv1 resolver write is kept in its native partition, which the guarded
            // arm reads without a namespace or manifest test.
            (Some("ens_v1_resolver_l1"), _) => {
                partitions.push(("native", format!("{node}|ens_v1_resolver_l1")));
            }
            (Some("ens_v2_resolver_l1"), Some("public_resolver_v2")) => partitions.push((
                "guarded",
                format!(
                    "{node}|ens_v2_resolver_l1|{}|{}",
                    pointer.namespace,
                    classification
                        .manifest_id
                        .map(|id| id.to_string())
                        .unwrap_or_default()
                ),
            )),
            _ => {}
        }
    }
    partitions
}

/// A retained record write: the latest write of one record key in one partition or record id.
#[derive(Clone, Debug)]
pub(crate) struct RecordCandidate {
    pub(crate) record_key: String,
    pub(crate) position: FamilyPosition,
    pub(crate) normalized_event_id: Option<i64>,
    pub(crate) source_family: String,
    pub(crate) status: String,
    /// The write's payload rebuilt from the row's columns.
    pub(crate) payload: Value,
    /// For the `AddrChanged` half of a coin-60 pair, the `AddressChanged` half one log earlier.
    pub(crate) sibling_position: Option<FamilyPosition>,
}

impl RecordCandidate {
    fn from_row(row: &PgRow, with_sibling: bool) -> Result<Self> {
        let mut payload = Map::new();
        for column in [
            "record_key",
            "record_family",
            "selector_key",
            "source_event",
        ] {
            if let Some(text) = row.try_get::<Option<String>, _>(column)? {
                payload.insert(column.to_owned(), json!(text));
            }
        }
        if let Some(value) = row.try_get::<Option<Value>, _>("value")?
            && !value.is_null()
        {
            payload.insert("value".to_owned(), value);
        }
        for column in ["contenthash_hex", "address_bytes_hex"] {
            if let Some(text) = row.try_get::<Option<String>, _>(column)? {
                payload.insert(column.to_owned(), json!(text));
            }
        }
        let sibling_position = if with_sibling {
            row.try_get::<Option<Value>, _>("sibling_position")?
                .as_ref()
                .and_then(FamilyPosition::from_json)
        } else {
            None
        };
        Ok(Self {
            record_key: row.try_get("record_key")?,
            position: FamilyPosition::from_row(row)?,
            normalized_event_id: row.try_get("normalized_event_id")?,
            source_family: row.try_get("source_family")?,
            status: row.try_get("status")?,
            payload: Value::Object(payload),
            sibling_position,
        })
    }

    /// Whether this is the `AddrChanged` half of a coin-60 pair.
    pub(crate) fn pair_sibling(&self) -> Option<&FamilyPosition> {
        (self.payload.get("source_event").and_then(Value::as_str) == Some("AddrChanged"))
            .then_some(self.sibling_position.as_ref())
            .flatten()
    }
}

const VALUE_COLUMNS: &str =
    "record_key, block_number, transaction_index, log_index, event_identity,
    normalized_event_id, source_family, status, value, record_family, selector_key,
    contenthash_hex, address_bytes_hex, source_event";

/// The admitted partitions' version events and their retained writes.
pub(crate) async fn load_partitions(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    partitions: &[(&'static str, String)],
) -> Result<(Vec<FamilyPosition>, Vec<RecordCandidate>)> {
    let arms: Vec<&str> = partitions.iter().map(|(arm, _)| *arm).collect();
    let identities: Vec<&str> = partitions.iter().map(|(_, id)| id.as_str()).collect();
    let versions = sqlx::query(
        "SELECT DISTINCT partition.version_position
         FROM bigname_phase.project_node_record_partition partition
         JOIN unnest($3::text[], $4::text[]) admitted (arm, arm_identity)
           ON admitted.arm = partition.arm AND admitted.arm_identity = partition.arm_identity
         WHERE partition.chain_id = $1 AND partition.resolver_address = $2
           AND partition.version_position IS NOT NULL",
    )
    .bind(chain_id)
    .bind(resolver_address)
    .bind(&arms)
    .bind(&identities)
    .fetch_all(pool)
    .await
    .context("failed to load the admitted record partitions")?
    .into_iter()
    .filter_map(|row| {
        row.try_get::<Value, _>("version_position")
            .ok()
            .as_ref()
            .and_then(FamilyPosition::from_json)
    })
    .collect();
    let values = sqlx::query(&format!(
        "SELECT {VALUE_COLUMNS}, sibling_position
         FROM bigname_phase.project_node_record_value value
         JOIN unnest($3::text[], $4::text[]) admitted (arm, arm_identity)
           ON admitted.arm = value.arm AND admitted.arm_identity = value.arm_identity
         WHERE value.chain_id = $1 AND value.resolver_address = $2"
    ))
    .bind(chain_id)
    .bind(resolver_address)
    .bind(&arms)
    .bind(&identities)
    .fetch_all(pool)
    .await
    .context("failed to load the admitted record values")?
    .iter()
    .map(|row| RecordCandidate::from_row(row, true))
    .collect::<Result<Vec<_>>>()?;
    Ok((versions, values))
}

/// The retained writes of one record id at a resolver.
pub(crate) async fn load_record_id_values(
    pool: &PgPool,
    chain_id: &str,
    resolver_address: &str,
    record_id: &str,
) -> Result<Vec<RecordCandidate>> {
    sqlx::query(&format!(
        "SELECT {VALUE_COLUMNS}
         FROM bigname_phase.project_record_id_value
         WHERE chain_id = $1 AND resolver_address = $2 AND record_id = $3"
    ))
    .bind(chain_id)
    .bind(resolver_address)
    .bind(record_id)
    .fetch_all(pool)
    .await
    .context("failed to load the linked record values")?
    .iter()
    .map(|row| RecordCandidate::from_row(row, false))
    .collect()
}
