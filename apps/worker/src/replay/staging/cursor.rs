use serde_json::Value;
use sqlx::types::Uuid;

#[derive(Clone, Copy)]
enum CursorValueShape {
    String,
    StringTuple(usize),
    Uuid,
    StringUuidTuple,
}

pub(super) fn source_key_is_valid(projection: &str, source_key: &Value) -> bool {
    let Some((_, value_shape)) = cursor_contract(projection) else {
        return false;
    };
    match value_shape {
        CursorValueShape::String => nonempty_string(source_key),
        CursorValueShape::StringTuple(expected_len) => {
            source_key.as_array().is_some_and(|values| {
                values.len() == expected_len && values.iter().all(nonempty_string)
            })
        }
        CursorValueShape::Uuid => source_key
            .as_str()
            .is_some_and(|value| Uuid::parse_str(value).is_ok()),
        CursorValueShape::StringUuidTuple => source_key.as_array().is_some_and(|values| {
            values.len() == 2
                && nonempty_string(&values[0])
                && values[1]
                    .as_str()
                    .is_some_and(|value| Uuid::parse_str(value).is_ok())
        }),
    }
}

#[cfg(test)]
pub(super) fn shape_tag(projection: &str) -> Option<&'static str> {
    cursor_contract(projection).map(|(tag, _)| tag)
}

fn cursor_contract(projection: &str) -> Option<(&'static str, CursorValueShape)> {
    let contract = match projection {
        "name_current" => ("logical_name_id:string", CursorValueShape::String),
        "children_current" => (
            "(parent_logical_name_id,canonical_display_name,child_logical_name_id):string_tuple",
            CursorValueShape::StringTuple(3),
        ),
        "permissions_current" | "record_inventory_current" => {
            ("resource_id:uuid", CursorValueShape::Uuid)
        }
        "resolver_current" => (
            "(chain_id,resolver_address):string_tuple",
            CursorValueShape::StringTuple(2),
        ),
        "address_names_current" => (
            "(logical_name_id,surface_binding_id):string_uuid_tuple",
            CursorValueShape::StringUuidTuple,
        ),
        "primary_names_current" => (
            "(address,namespace,coin_type):string_tuple",
            CursorValueShape::StringTuple(3),
        ),
        _ => return None,
    };
    Some(contract)
}

fn nonempty_string(value: &Value) -> bool {
    value.as_str().is_some_and(|value| !value.is_empty())
}
