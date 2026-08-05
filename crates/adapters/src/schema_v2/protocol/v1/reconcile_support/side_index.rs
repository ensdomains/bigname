use std::collections::{BTreeSet, HashMap};

use serde_json::Value;
use uuid::Uuid;

use super::event_index::Position;
use crate::schema_v2::model::BatchOutput;

pub(super) struct BindingIndex {
    pub(super) active: Vec<bool>,
    by_resource_position: HashMap<(Uuid, Position), Vec<usize>>,
}

impl BindingIndex {
    pub(super) fn new(output: &BatchOutput) -> Self {
        let mut by_resource_position = HashMap::new();
        for (index, binding) in output.surface_bindings.iter().enumerate() {
            let position = (
                binding.block_number,
                binding
                    .provenance
                    .get("transaction_index")
                    .and_then(Value::as_i64)
                    .unwrap_or_default(),
                binding
                    .provenance
                    .get("log_index")
                    .and_then(Value::as_i64)
                    .unwrap_or_default(),
            );
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
                    .get(&(*resource_id, *position))
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
