//! The rows of the address listing family (`project_address_listing`, F18): one row per
//! (address, name) that `GET /v1/addresses/{address}/names` lists, derived at the publication
//! Project writes from the same composed rows and the same relation fold the route composes at
//! read (`address_names.rs`, `address_relations.rs`), so a page can walk an index of the
//! address's rows instead of composing every candidate name.
//!
//! A composed name gives a row to every address `relations` relates to it, with the three
//! relations as flags and the cited block of each, and to its lapsed registration's owner as a
//! former owner (`former_owners.rs`). The surface-less ENSv1 registry children come from the
//! children relation instead (`topology::registry_child_listing_statement`). The route lists a
//! name for an address only when the address index names the address on it, or the address holds
//! a role on it, so the stored row is kept under the same rule.
//!
//! The name is the chain-data spelling: a surface with no raw bytes is spelled from its
//! chain-observed preimages only (`topology::stored_spelling`), so an imported preimage changes
//! no stored row without a block, and the served name applies it at read. The sort keys are the
//! route's own SQL over the composed row (`address_names::query`), a missing one stored as
//! `MISSING_TIMESTAMP`, at canonical scale so undo restores them byte for byte.
use std::collections::BTreeMap;

use serde_json::{Value, json};
use sqlx::{Execute, Postgres, QueryBuilder, postgres::PgArguments};

use super::{
    address_names::{name_row, relation_resource},
    address_relations::{
        EFFECTIVE_CONTROLLER, NameRelationsInput, ROLE_HOLDER, TOKEN_HOLDER, relation_position,
        relations,
    },
    former_owners::{lapsed_owner, served_expiry},
};
use crate::{
    address_names::{
        push_created_at_timestamp_expr, push_expires_at_timestamp_expr, push_migration_proof,
        push_registered_at_timestamp_expr,
    },
    families::{
        name::{FamilyPublication, public_authority, rendered::bracketed_label},
        topology::{DISPLAY_KEY_BYTES, DISPLAY_KEY_CHARS, MISSING_TIMESTAMP, stored_spelling},
    },
};

/// The family's columns, in table order.
pub const ADDRESS_LISTING_COLUMNS: &str = "chain_id, address, logical_name_id, namespace, \
    display_key, display_name, normalized_name, parent_normalized_name, namehash, resource_id, \
    token_holder, effective_controller, role_holder, listed, former_owner, registry_child, \
    registry_parent_logical_name_id, registration_lead, public_authority, migrated, expires_key, \
    registered_key, created_key, former_expires_key, token_holder_block, \
    effective_controller_block, role_holder_block, unhealed_labelhashes";

/// The listing rows of a batch of composed names, before the statement that shapes them
/// ([`address_listing_statement`]): one entry per name that relates to some address, with the
/// composed fields the keys read, and one per (address, name) with the relations.
#[derive(Clone, Debug, Default)]
pub struct AddressListingRows {
    names: Vec<Value>,
    relations: Vec<Value>,
    composed: Vec<String>,
}

#[derive(Default)]
struct Related {
    token_holder: Option<Option<i64>>,
    effective_controller: Option<Option<i64>>,
    role_holder: Option<Option<i64>>,
    former_owner: bool,
}

impl AddressListingRows {
    /// Whether no name relates to any address.
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
    }

    /// Every name the batch composed a row for, whether or not it relates to an address: such a
    /// name is never a surface-less registry child.
    pub fn composed(&self) -> &[String] {
        &self.composed
    }

    pub fn extend(&mut self, other: Self) {
        self.names.extend(other.names);
        self.relations.extend(other.relations);
        self.composed.extend(other.composed);
    }

    /// Add the rows of one composed name at `publication`.
    pub(super) fn push(&mut self, input: &NameRelationsInput<'_>, publication: &FamilyPublication) {
        let row = input.row;
        self.composed.push(row.logical_name_id.clone());
        let mut related: BTreeMap<String, Related> = BTreeMap::new();
        for (address, relation) in relations(input) {
            // The cited acquisition event at or below the publication, as the history read keeps it
            // (`address_publication.rs`).
            let block = relation_position(input, &address, relation)
                .filter(|position| position.block_number <= publication.block_number)
                .map(|position| position.block_number);
            let entry = related.entry(address).or_default();
            let slot = match relation {
                TOKEN_HOLDER => &mut entry.token_holder,
                EFFECTIVE_CONTROLLER => &mut entry.effective_controller,
                ROLE_HOLDER => &mut entry.role_holder,
                _ => continue,
            };
            slot.get_or_insert(block);
        }
        if let Some(owner) = lapsed_owner(row) {
            related.entry(owner).or_default().former_owner = true;
        }
        if related.is_empty() {
            return;
        }
        let expiry = served_expiry(row).map(|expiry| expiry.to_string());
        for (address, related) in related {
            self.relations.push(json!({
                "address": address,
                "logical_name_id": row.logical_name_id,
                "token_holder": related.token_holder.is_some(),
                "effective_controller": related.effective_controller.is_some(),
                "role_holder": related.role_holder.is_some(),
                "former_owner": related.former_owner,
                "former_expires_key": related.former_owner.then(|| expiry.clone()).flatten(),
                "token_holder_block": related.token_holder.flatten(),
                "effective_controller_block": related.effective_controller.flatten(),
                "role_holder_block": related.role_holder.flatten(),
            }));
        }
        let slim = name_row(row);
        self.names.push(json!({
            "logical_name_id": row.logical_name_id,
            "namespace": row.namespace,
            "display_name": row.canonical_display_name,
            "normalized_name": row.normalized_name,
            "bracketed": row
                .normalized_name
                .split('.')
                .any(|label| bracketed_label(label).is_some()),
            "namehash": row.namehash,
            "resource_id": relation_resource(row),
            "public_authority": public_authority(&row.provenance),
            "declared_summary": slim["declared_summary"],
            "provenance": slim["provenance"],
        }));
    }
}

/// `head` (an `INSERT INTO <table> (ADDRESS_LISTING_COLUMNS)`, or the start of a statement that
/// reads the rows as a subquery) followed by the statement that shapes `rows` of chain
/// `chain_id` into listing rows, each column in [`ADDRESS_LISTING_COLUMNS`] order, as owned SQL
/// and arguments: the Project writer's future must be `Send` (`topology::listing_statement`).
pub fn address_listing_statement(
    head: &str,
    chain_id: &str,
    rows: &AddressListingRows,
) -> Result<(String, PgArguments), sqlx::Error> {
    let mut builder = QueryBuilder::<Postgres>::new(format!(
        "/* storage:families.records.address_listing_rows */ {head} SELECT CAST("
    ));
    builder.push_bind(chain_id.to_owned());
    builder.push(format!(
        " AS text), rel.address, nc.logical_name_id, nc.namespace,
           CASE WHEN octet_length(spelled.name) <= {DISPLAY_KEY_BYTES} THEN spelled.name
                ELSE left(spelled.name, {DISPLAY_KEY_CHARS}) END,
           spelled.name, spelled.normalized,
           CASE WHEN strpos(spelled.normalized, '.') > 1
                THEN substr(spelled.normalized, strpos(spelled.normalized, '.') + 1) END,
           nc.namehash, nc.resource_id,
           kept.listing AND rel.token_holder, kept.listing AND rel.effective_controller,
           kept.listing AND rel.role_holder,
           kept.listing AND (rel.token_holder OR rel.effective_controller OR rel.role_holder),
           kept.former, FALSE, NULL::text, FALSE, nc.public_authority,
           EXISTS (SELECT 1 FROM (SELECT nc.logical_name_id, nc.provenance) migration_nc"
    ));
    push_migration_proof(&mut builder, "migration_nc", "nc.logical_name_id");
    builder.push("), trim_scale(COALESCE(");
    push_expires_at_timestamp_expr(&mut builder);
    builder.push(format!(
        ", {MISSING_TIMESTAMP})), trim_scale(COALESCE(EXTRACT(EPOCH FROM "
    ));
    push_registered_at_timestamp_expr(&mut builder);
    builder.push(format!(
        "), {MISSING_TIMESTAMP})), trim_scale(COALESCE(EXTRACT(EPOCH FROM "
    ));
    push_created_at_timestamp_expr(&mut builder);
    builder.push(format!(
        "), {MISSING_TIMESTAMP})),
           trim_scale(CASE WHEN kept.former
                           THEN COALESCE(rel.former_expires_key, {MISSING_TIMESTAMP})
                           ELSE {MISSING_TIMESTAMP} END),
           CASE WHEN kept.listing THEN rel.token_holder_block END,
           CASE WHEN kept.listing THEN rel.effective_controller_block END,
           CASE WHEN kept.listing THEN rel.role_holder_block END,
           spelled.unhealed
         FROM jsonb_to_recordset("
    ));
    builder.push_bind(Value::Array(rows.names.clone()));
    builder.push(
        ") AS nc(logical_name_id text, namespace text, display_name text, normalized_name text,
                 bracketed boolean, namehash text, resource_id uuid, public_authority text,
                 declared_summary jsonb, provenance jsonb)
         JOIN jsonb_to_recordset(",
    );
    builder.push_bind(Value::Array(rows.relations.clone()));
    builder.push(format!(
        ") AS rel(address text, logical_name_id text, token_holder boolean,
                  effective_controller boolean, role_holder boolean, former_owner boolean,
                  former_expires_key numeric, token_holder_block bigint,
                  effective_controller_block bigint, role_holder_block bigint)
           ON rel.logical_name_id = nc.logical_name_id
         -- A name the publication reads without raw bytes is composed under its placeholder
         -- labels, and served under each label the preimages spell (`name::rendered`,
         -- `enrich`), though Interpret may since have filled the surface's raw bytes from a
         -- later observation (`name::loaders::surfaces_at`). The stored spelling reads the
         -- chain-observed preimages only.
         LEFT JOIN bigname_phase.name_surfaces surface
           ON nc.bracketed AND surface.logical_name_id = nc.logical_name_id{spelling}
         CROSS JOIN LATERAL (
             SELECT COALESCE(spelling.name, nc.display_name) AS name,
                    COALESCE(spelling.name, nc.normalized_name) AS normalized,
                    COALESCE(spelling.unhealed, '{{}}'::text[]) AS unhealed
         ) spelled
         -- The route composes a name for an address the address index names on it on the
         -- name's chain, or that holds a registry role on it (`address_name_candidates`), and
         -- lists a former owner through the index only (`former_owners.rs`). Its read filter
         -- joins each listed row's resource, so a name with none lists no relation.
         CROSS JOIN LATERAL (
             SELECT EXISTS (SELECT 1 FROM bigname_phase.project_address_name_index indexed
                            WHERE indexed.address = rel.address
                              AND indexed.logical_name_id = nc.logical_name_id
                              AND indexed.chain_id = ",
        spelling = stored_spelling("surface", "spelling"),
    ));
    builder.push_bind(chain_id.to_owned());
    builder.push(
        ") AS indexed
         ) gate
         CROSS JOIN LATERAL (
             SELECT (gate.indexed OR rel.role_holder) AND nc.resource_id IS NOT NULL
                        AS listing,
                    gate.indexed AND rel.former_owner AS former
         ) kept
         WHERE (kept.listing AND (rel.token_holder OR rel.effective_controller OR rel.role_holder))
            OR kept.former",
    );
    let arguments = builder
        .build()
        .take_arguments()
        .map_err(sqlx::Error::Encode)?
        .unwrap_or_default();
    Ok((builder.into_sql(), arguments))
}

/// A statement that spells again each row of the subquery `target` (rows of
/// `project_address_listing` whose name was spelled from label hashes, as a publication read the
/// surface without raw bytes) from that surface's chain-observed preimages, and yields `(address, logical_name_id, before_image,
/// after_image)` for each row it spells further: fewer labels left as `[labelhash]`. Only the
/// name and the columns read from it change; a preimage observation can do nothing else to a
/// stored row. `target` is read first and on its own, so its own index bounds the read.
pub fn address_listing_respell_sql(target: &str) -> String {
    format!(
        "/* storage:families.records.address_listing_respell */
         WITH target AS MATERIALIZED ({target})
         SELECT target.address, target.logical_name_id, to_jsonb(target),
                to_jsonb(target) || jsonb_build_object(
                    'display_key',
                    CASE WHEN octet_length(spelling.name) <= {DISPLAY_KEY_BYTES} THEN spelling.name
                         ELSE left(spelling.name, {DISPLAY_KEY_CHARS}) END,
                    'display_name', spelling.name,
                    'normalized_name', spelling.name,
                    'parent_normalized_name',
                    CASE WHEN strpos(spelling.name, '.') > 1
                         THEN substr(spelling.name, strpos(spelling.name, '.') + 1) END,
                    'unhealed_labelhashes', to_jsonb(spelling.unhealed))
         FROM target
         JOIN bigname_phase.name_surfaces surface
           ON surface.logical_name_id = target.logical_name_id{spelling}
         WHERE spelling.name IS NOT NULL
           AND target.unhealed_labelhashes @> spelling.unhealed
           AND target.unhealed_labelhashes <> spelling.unhealed",
        spelling = stored_spelling("surface", "spelling"),
    )
}
