//! Exact current address relations produced with the name-summary publication.
//! Membership uses the same relation fold and permission masks as address reads. Project
//! validates the cited position before retaining the exact masks and selected resource.
use anyhow::Result;
use sqlx::PgConnection;
use uuid::Uuid;

use super::{
    FamilyPosition,
    address_relation_inputs::ChainInputs,
    address_relations::{relation_position, relations},
    address_roles::RoleHolderLoad,
};
use crate::{NameCurrentRow, families::name::FamilyPublication};

/// One current relation and the selected name/resource that proves it. The history catalogue
/// packs relation masks after this evidence is validated; name rows retain the selected
/// resource so untouched aliases still contribute their independent current masks.
#[derive(Clone, Debug)]
pub struct CurrentHistoryRelation {
    pub address: String,
    pub logical_name_id: String,
    pub namespace: String,
    pub resource_id: Uuid,
    pub surface_binding_id: Uuid,
    pub token_lineage_id: Option<Uuid>,
    pub relation: &'static str,
    pub position: FamilyPosition,
}

pub(crate) async fn publication_relations(
    conn: &mut PgConnection,
    publication: &FamilyPublication,
    composed: &[&NameCurrentRow],
) -> Result<Vec<CurrentHistoryRelation>> {
    let clock = publication.timestamp_seconds();
    let inputs = ChainInputs::load(
        conn,
        &publication.chain_id,
        composed.iter().copied(),
        clock,
        RoleHolderLoad::All,
    )
    .await?;
    let mut out = Vec::new();
    for row in composed {
        let (Some(resource_id), Some(surface_binding_id)) =
            (row.resource_id, row.surface_binding_id)
        else {
            continue;
        };
        let input = inputs.input(row, clock);
        for (address, relation) in relations(&input) {
            // A normal bounded history read requires a cited acquisition event at or below
            // the publication. Missing evidence does not gain membership through the index.
            let Some(position) = relation_position(&input, &address, relation)
                .filter(|position| position.block_number <= publication.block_number)
            else {
                continue;
            };
            out.push(CurrentHistoryRelation {
                address,
                logical_name_id: row.logical_name_id.clone(),
                namespace: row.namespace.clone(),
                resource_id,
                surface_binding_id,
                token_lineage_id: row.token_lineage_id,
                relation,
                position,
            });
        }
    }
    Ok(out)
}
