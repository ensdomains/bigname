use std::collections::{BTreeSet, HashMap};

use serde_json::Value;
use uuid::Uuid;

use super::event_index::Position;
use crate::schema_v2::model::BatchOutput;

pub(super) struct BindingIndex {
    pub(super) active: Vec<bool>,
    by_resource_position: HashMap<(Uuid, Option<Position>), Vec<usize>>,
}

impl BindingIndex {
    pub(super) fn new(output: &BatchOutput) -> Self {
        let mut by_resource_position = HashMap::new();
        for (index, binding) in output.surface_bindings.iter().enumerate() {
            let position = binding
                .provenance
                .get("transaction_index")
                .and_then(Value::as_i64)
                .zip(binding.provenance.get("log_index").and_then(Value::as_i64))
                .map(|(transaction_index, log_index)| {
                    (binding.block_number, transaction_index, log_index)
                });
            by_resource_position
                .entry((binding.resource_id, position))
                .or_insert_with(Vec::new)
                .push(index);
        }
        Self {
            active: vec![true; output.surface_bindings.len()],
            by_resource_position,
        }
    }

    pub(super) fn remove(&mut self, resources: &BTreeSet<Uuid>, positions: &BTreeSet<Position>) {
        for resource_id in resources {
            for position in positions {
                for index in self
                    .by_resource_position
                    .get(&(*resource_id, Some(*position)))
                    .into_iter()
                    .flatten()
                {
                    self.active[*index] = false;
                }
            }
        }
    }
}

pub(super) struct ClosureIndex {
    pub(super) active: Vec<bool>,
    by_name_position: HashMap<(String, Position), Vec<usize>>,
}

impl ClosureIndex {
    pub(super) fn new(output: &BatchOutput) -> Self {
        let mut by_name_position = HashMap::new();
        for (index, closure) in output.binding_closures.iter().enumerate() {
            by_name_position
                .entry((
                    closure.logical_name_id.clone(),
                    (
                        closure.block_number,
                        closure.transaction_index,
                        closure.log_index,
                    ),
                ))
                .or_insert_with(Vec::new)
                .push(index);
        }
        Self {
            active: vec![true; output.binding_closures.len()],
            by_name_position,
        }
    }

    pub(super) fn remove(&mut self, logical_name_id: &str, positions: &BTreeSet<Position>) {
        for position in positions {
            for index in self
                .by_name_position
                .get(&(logical_name_id.to_owned(), *position))
                .into_iter()
                .flatten()
            {
                self.active[*index] = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use time::OffsetDateTime;

    use super::*;
    use crate::schema_v2::model::SurfaceBinding;

    const BLOCK_NUMBER: i64 = 42;

    fn binding(id: u128, resource_id: Uuid, provenance: Value) -> SurfaceBinding {
        SurfaceBinding {
            surface_binding_id: Uuid::from_u128(id),
            logical_name_id: "ens:node".to_owned(),
            resource_id,
            binding_kind: "declared_registry_path".to_owned(),
            authority_arm: "ens_v1".to_owned(),
            active_from: OffsetDateTime::UNIX_EPOCH,
            chain_id: "ethereum-mainnet".to_owned(),
            block_hash: "block".to_owned(),
            block_number: BLOCK_NUMBER,
            provenance,
            canonicality_state: "canonical".to_owned(),
        }
    }

    #[test]
    fn missing_opening_position_does_not_match_any_real_position() {
        let resource_id = Uuid::from_u128(1);
        let output = BatchOutput {
            surface_bindings: vec![
                binding(
                    10,
                    resource_id,
                    json!({"kind":"raw_block","block_number":BLOCK_NUMBER}),
                ),
                binding(
                    11,
                    resource_id,
                    json!({"kind":"incomplete","transaction_index":0}),
                ),
                binding(
                    12,
                    resource_id,
                    json!({"kind":"known","transaction_index":-1,"log_index":-1}),
                ),
                binding(
                    13,
                    resource_id,
                    json!({"kind":"known","transaction_index":0,"log_index":0}),
                ),
            ],
            ..BatchOutput::default()
        };
        let mut index = BindingIndex::new(&output);

        index.remove(
            &BTreeSet::from([resource_id]),
            &BTreeSet::from([(BLOCK_NUMBER, -1, -1), (BLOCK_NUMBER, 0, 0)]),
        );

        assert_eq!(
            index.active,
            vec![true, true, false, false],
            "unknown or incomplete provenance must not collapse onto either real position"
        );
    }

    #[test]
    fn known_opening_position_requires_an_exact_match() {
        let resource_id = Uuid::from_u128(2);
        let output = BatchOutput {
            surface_bindings: vec![
                binding(
                    20,
                    resource_id,
                    json!({"transaction_index":3,"log_index":4}),
                ),
                binding(
                    21,
                    resource_id,
                    json!({"transaction_index":3,"log_index":5}),
                ),
            ],
            ..BatchOutput::default()
        };
        let mut index = BindingIndex::new(&output);

        index.remove(
            &BTreeSet::from([resource_id]),
            &BTreeSet::from([(BLOCK_NUMBER, 3, 4)]),
        );

        assert_eq!(index.active, vec![false, true]);
    }
}
