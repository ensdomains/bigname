use super::manifest_queries::MANIFEST_CURRENT_INVALIDATIONS_PREFIX;

mod address_names;
mod children;
mod name_current;
mod permissions;
mod primary_names;
mod record_inventory;
mod resolver;

use address_names::ADDRESS_NAMES_CURRENT_INVALIDATIONS_PREFIX;
use children::CHILDREN_CURRENT_INVALIDATIONS_PREFIX;
use name_current::NAME_CURRENT_INVALIDATIONS_PREFIX;
use permissions::PERMISSIONS_CURRENT_INVALIDATIONS_PREFIX;
use primary_names::PRIMARY_NAMES_CURRENT_INVALIDATIONS_PREFIX;
use record_inventory::RECORD_INVENTORY_CURRENT_INVALIDATIONS_PREFIX;
use resolver::RESOLVER_CURRENT_INVALIDATIONS_PREFIX;

pub(super) const UPSERT_SUFFIX: &str = r#"
INSERT INTO projection_invalidations (
    projection,
    projection_key,
    key_payload,
    first_change_id,
    last_change_id,
    first_normalized_event_id,
    last_normalized_event_id,
    last_changed_at,
    invalidated_at
)
SELECT
    projection,
    projection_key,
    key_payload,
    MIN(change_id),
    MAX(change_id),
    MIN(normalized_event_id),
    MAX(normalized_event_id),
    MAX(changed_at),
    now()
FROM candidate_keys
WHERE projection_key IS NOT NULL
  AND btrim(projection_key) <> ''
GROUP BY projection, projection_key, key_payload
ON CONFLICT (projection, projection_key)
DO UPDATE SET
    key_payload = EXCLUDED.key_payload,
    generation = projection_invalidations.generation + 1,
    first_change_id = LEAST(
        projection_invalidations.first_change_id,
        EXCLUDED.first_change_id
    ),
    last_change_id = GREATEST(
        projection_invalidations.last_change_id,
        EXCLUDED.last_change_id
    ),
    first_normalized_event_id = LEAST(
        projection_invalidations.first_normalized_event_id,
        EXCLUDED.first_normalized_event_id
    ),
    last_normalized_event_id = GREATEST(
        projection_invalidations.last_normalized_event_id,
        EXCLUDED.last_normalized_event_id
    ),
    last_changed_at = GREATEST(
        projection_invalidations.last_changed_at,
        EXCLUDED.last_changed_at
    ),
    invalidated_at = EXCLUDED.invalidated_at,
    claim_token = NULL,
    claimed_at = NULL,
    last_failure_reason = NULL,
    last_failure_at = NULL
	"#;

pub(super) const INVALIDATION_QUERY_PREFIXES: &[&str] = &[
    NAME_CURRENT_INVALIDATIONS_PREFIX,
    CHILDREN_CURRENT_INVALIDATIONS_PREFIX,
    PERMISSIONS_CURRENT_INVALIDATIONS_PREFIX,
    RECORD_INVENTORY_CURRENT_INVALIDATIONS_PREFIX,
    RESOLVER_CURRENT_INVALIDATIONS_PREFIX,
    ADDRESS_NAMES_CURRENT_INVALIDATIONS_PREFIX,
    PRIMARY_NAMES_CURRENT_INVALIDATIONS_PREFIX,
    MANIFEST_CURRENT_INVALIDATIONS_PREFIX,
];

const NAME_CURRENT_COMPLETED_CHANGE_PREFIXES: &[&str] = &[
    NAME_CURRENT_INVALIDATIONS_PREFIX,
    MANIFEST_CURRENT_INVALIDATIONS_PREFIX,
];
const CHILDREN_CURRENT_COMPLETED_CHANGE_PREFIXES: &[&str] =
    &[CHILDREN_CURRENT_INVALIDATIONS_PREFIX];
const PERMISSIONS_CURRENT_COMPLETED_CHANGE_PREFIXES: &[&str] =
    &[PERMISSIONS_CURRENT_INVALIDATIONS_PREFIX];
const RECORD_INVENTORY_CURRENT_COMPLETED_CHANGE_PREFIXES: &[&str] = &[
    RECORD_INVENTORY_CURRENT_INVALIDATIONS_PREFIX,
    MANIFEST_CURRENT_INVALIDATIONS_PREFIX,
];
const RESOLVER_CURRENT_COMPLETED_CHANGE_PREFIXES: &[&str] = &[
    RESOLVER_CURRENT_INVALIDATIONS_PREFIX,
    MANIFEST_CURRENT_INVALIDATIONS_PREFIX,
];
const ADDRESS_NAMES_CURRENT_COMPLETED_CHANGE_PREFIXES: &[&str] =
    &[ADDRESS_NAMES_CURRENT_INVALIDATIONS_PREFIX];
const PRIMARY_NAMES_CURRENT_COMPLETED_CHANGE_PREFIXES: &[&str] =
    &[PRIMARY_NAMES_CURRENT_INVALIDATIONS_PREFIX];

pub(super) fn current_projection_invalidation_prefixes(
    projection: &str,
) -> Option<&'static [&'static str]> {
    match projection {
        "name_current" => Some(NAME_CURRENT_COMPLETED_CHANGE_PREFIXES),
        "children_current" => Some(CHILDREN_CURRENT_COMPLETED_CHANGE_PREFIXES),
        "permissions_current" => Some(PERMISSIONS_CURRENT_COMPLETED_CHANGE_PREFIXES),
        "record_inventory_current" => Some(RECORD_INVENTORY_CURRENT_COMPLETED_CHANGE_PREFIXES),
        "resolver_current" => Some(RESOLVER_CURRENT_COMPLETED_CHANGE_PREFIXES),
        "address_names_current" => Some(ADDRESS_NAMES_CURRENT_COMPLETED_CHANGE_PREFIXES),
        "primary_names_current" => Some(PRIMARY_NAMES_CURRENT_COMPLETED_CHANGE_PREFIXES),
        _ => None,
    }
}

#[cfg(test)]
pub(crate) fn projection_staging_input_channel_tags(projection: &str) -> Option<Vec<&'static str>> {
    let prefixes = current_projection_invalidation_prefixes(projection)?;
    let mut channels = Vec::with_capacity(prefixes.len() + 3);
    for prefix in prefixes {
        let channel = if *prefix == MANIFEST_CURRENT_INVALIDATIONS_PREFIX {
            "manifest_current"
        } else {
            "normalized_event"
        };
        if !channels.contains(&channel) {
            channels.push(channel);
        }
    }
    if projection == "children_current" {
        channels.push("parent_changed_full_restage");
    }
    channels.push("direct_invalidation_generation");
    if projection == "permissions_current" {
        channels.push("permissions_resource_input_revision");
    }
    Some(channels)
}
