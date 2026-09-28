//! The F6 and F7 rows one serving pointer admits (record_inventory.rs, `attributed_events`), read
//! as record candidates, and the partition version events that are boundary candidates.
use std::collections::{BTreeSet, HashMap};

use anyhow::{Context, Result};
use serde_json::{Map, Value, json};
use sqlx::{PgConnection, Row, postgres::PgRow};

use super::{FamilyPosition, facts::ResolverClassification, serving::ServingPointer};

#[path = "text_hydration.rs"]
pub(super) mod text_hydration;

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
        if with_sibling && let Some(overlay) = row.try_get::<Option<Value>, _>("text_hydration")? {
            payload.insert(text_hydration::KEY.to_owned(), overlay);
        }
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

/// One admitted F6 partition of a resolver: (resolver address, arm, arm identity).
pub(crate) type PartitionKey = (String, &'static str, String);

/// The partitions' version events and retained writes read for many serving pointers at once.
#[derive(Default)]
pub(crate) struct PartitionRows {
    versions: HashMap<(String, String, String), Vec<FamilyPosition>>,
    values: HashMap<(String, String, String), Vec<RecordCandidate>>,
}

impl PartitionRows {
    /// The version events and retained writes of `partitions`, as one read of just those
    /// partitions returns them: the version events distinct, one write per partition row.
    pub(crate) fn of(
        &self,
        partitions: &[PartitionKey],
    ) -> (Vec<FamilyPosition>, Vec<RecordCandidate>) {
        let mut versions: Vec<FamilyPosition> = Vec::new();
        let mut values = Vec::new();
        for (resolver, arm, identity) in partitions {
            let key = (resolver.clone(), (*arm).to_owned(), identity.clone());
            for version in self.versions.get(&key).into_iter().flatten() {
                if !versions.contains(version) {
                    versions.push(version.clone());
                }
            }
            values.extend(self.values.get(&key).into_iter().flatten().cloned());
        }
        (versions, values)
    }
}

/// The version events and retained writes of every partition in `partitions` on `chain_id`, in
/// two statements.
pub(crate) async fn load_partitions(
    conn: &mut PgConnection,
    chain_id: &str,
    partitions: &[PartitionKey],
) -> Result<PartitionRows> {
    let unique: BTreeSet<&PartitionKey> = partitions.iter().collect();
    if unique.is_empty() {
        return Ok(PartitionRows::default());
    }
    let resolvers: Vec<&str> = unique
        .iter()
        .map(|(resolver, _, _)| resolver.as_str())
        .collect();
    let arms: Vec<&str> = unique.iter().map(|(_, arm, _)| *arm).collect();
    let identities: Vec<&str> = unique.iter().map(|(_, _, id)| id.as_str()).collect();
    let mut rows = PartitionRows::default();
    let versions = sqlx::query(
        "SELECT DISTINCT partition.resolver_address, partition.arm, partition.arm_identity,
                partition.version_position
         FROM bigname_phase.project_node_record_partition partition
         JOIN unnest($2::text[], $3::text[], $4::text[]) admitted (resolver_address, arm, arm_identity)
           ON admitted.resolver_address = partition.resolver_address
          AND admitted.arm = partition.arm AND admitted.arm_identity = partition.arm_identity
         WHERE partition.chain_id = $1 AND partition.version_position IS NOT NULL",
    )
    .bind(chain_id)
    .bind(&resolvers)
    .bind(&arms)
    .bind(&identities)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the admitted record partitions")?;
    for row in versions {
        let Some(position) = row
            .try_get::<Value, _>("version_position")
            .ok()
            .as_ref()
            .and_then(FamilyPosition::from_json)
        else {
            continue;
        };
        rows.versions
            .entry(partition_key(&row)?)
            .or_default()
            .push(position);
    }
    let columns = VALUE_COLUMNS
        .split(',')
        .map(|column| format!("value.{}", column.trim()))
        .collect::<Vec<_>>()
        .join(", ");
    let values = sqlx::query(&format!(
        "SELECT {columns}, value.resolver_address, value.arm, value.arm_identity,
                value.sibling_position, {}
         FROM bigname_phase.project_node_record_value value
         JOIN unnest($2::text[], $3::text[], $4::text[]) admitted (resolver_address, arm, arm_identity)
           ON admitted.resolver_address = value.resolver_address
          AND admitted.arm = value.arm AND admitted.arm_identity = value.arm_identity
         LEFT JOIN bigname_phase.project_node_record_partition partition
           ON (partition.chain_id, partition.resolver_address, partition.arm, partition.arm_identity) =
              (value.chain_id, value.resolver_address, value.arm, value.arm_identity)
         LEFT JOIN bigname_phase.project_resolver_classification classification
           ON classification.chain_id = value.chain_id
          AND classification.resolver_address = value.resolver_address
         LEFT JOIN bigname_phase.name_surfaces surface
           ON surface.logical_name_id = value.logical_name_id
         WHERE value.chain_id = $1",
        text_hydration::COLUMNS
    ))
    .bind(chain_id)
    .bind(&resolvers)
    .bind(&arms)
    .bind(&identities)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the admitted record values")?;
    for row in &values {
        rows.values
            .entry(partition_key(row)?)
            .or_default()
            .push(RecordCandidate::from_row(row, true)?);
    }
    Ok(rows)
}

fn partition_key(row: &PgRow) -> Result<(String, String, String)> {
    Ok((
        row.try_get("resolver_address")?,
        row.try_get("arm")?,
        row.try_get("arm_identity")?,
    ))
}

/// The retained writes of each (resolver address, record id) in `record_ids` on `chain_id`, in
/// one statement.
pub(crate) async fn load_record_id_values(
    conn: &mut PgConnection,
    chain_id: &str,
    record_ids: &[(String, String)],
) -> Result<HashMap<(String, String), Vec<RecordCandidate>>> {
    let unique: BTreeSet<&(String, String)> = record_ids.iter().collect();
    let mut out: HashMap<(String, String), Vec<RecordCandidate>> = HashMap::new();
    if unique.is_empty() {
        return Ok(out);
    }
    let resolvers: Vec<&str> = unique
        .iter()
        .map(|(resolver, _)| resolver.as_str())
        .collect();
    let ids: Vec<&str> = unique.iter().map(|(_, id)| id.as_str()).collect();
    let rows = sqlx::query(&format!(
        "SELECT {VALUE_COLUMNS}, resolver_address, record_id
         FROM bigname_phase.project_record_id_value
         WHERE chain_id = $1
           AND (resolver_address, record_id) IN (
               SELECT requested.resolver_address, requested.record_id
               FROM unnest($2::text[], $3::text[]) requested (resolver_address, record_id))"
    ))
    .bind(chain_id)
    .bind(&resolvers)
    .bind(&ids)
    .fetch_all(&mut *conn)
    .await
    .context("failed to load the linked record values")?;
    for row in &rows {
        out.entry((row.try_get("resolver_address")?, row.try_get("record_id")?))
            .or_default()
            .push(RecordCandidate::from_row(row, false)?);
    }
    Ok(out)
}
