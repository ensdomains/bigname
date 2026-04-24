use super::{
    model::{CurrentBindingSeed, ProjectedRelations, RelevantEvent},
    util::{json_str, normalize_address},
};

pub(super) fn project_relations(
    binding: &CurrentBindingSeed,
    events: &[RelevantEvent],
) -> ProjectedRelations {
    let mut registrant = None;
    let mut token_holder = None;
    let mut registry_owner = None;

    for event in events {
        match event.event_kind.as_str() {
            "RegistrationGranted" => {
                registrant = json_str(&event.after_state, &["registrant"]).map(normalize_address);
            }
            "TokenControlTransferred" => {
                let transferred_to = json_str(&event.after_state, &["to"]).map(normalize_address);
                registrant = transferred_to.clone();
                token_holder = transferred_to;
            }
            "AuthorityTransferred" => {
                registry_owner = json_str(&event.after_state, &["owner"]).map(normalize_address);
            }
            "AuthorityEpochChanged" | "TokenRegenerated" => {}
            _ => {}
        }
    }

    if binding.token_lineage_id.is_some() {
        let token_holder = token_holder.or_else(|| registrant.clone());
        let effective_controller = registry_owner
            .or_else(|| token_holder.clone())
            .or_else(|| registrant.clone());
        ProjectedRelations {
            registrant,
            token_holder,
            effective_controller,
        }
    } else {
        ProjectedRelations {
            registrant: None,
            token_holder: None,
            effective_controller: registry_owner,
        }
    }
}
