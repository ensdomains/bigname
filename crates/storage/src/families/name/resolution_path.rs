//! Publication-scoped ENSv2 registry walk for retained ENSv1 descendants.
//! Resolver selection precedes ENSIP-10 validation; a deeper non-extended resolver hides an
//! ancestor wildcard. Retained records require the same resolver and requested node.
//! (upstream: .refs/ens_v2_sepolia_20261001/contracts/src/universalResolver/libraries/LibResolution.sol:L22-L85 @ ens_v2_sepolia_20261001@07e55a05)
mod facts;
mod physical;
pub use physical::PHYSICAL_POINTER_EVENT_SQL;
mod wrapper;

use super::{FamilyPublication, compose::Surface};
use crate::families::{
    control::permissions::registry_support::{self, Declarations, Model, SupportedRegistry},
    records::{
        facts::{ResolverClassification, load_classifications_at},
        is_cleared,
        mirror::evaluate_family_mirror_at,
        serving::ServingPointer,
    },
};
use anyhow::Result;
use sqlx::PgConnection;
use std::collections::BTreeMap;

const ROOT: &str = "0xb458d6a3a77919449d03e7a6903c26827c1ec43f";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum Decision {
    #[default]
    Retained,
    NoLiveEntry,
    Absent,
    Different,
    Unknown,
}
impl Decision {
    pub(super) fn withheld(self) -> bool {
        self != Self::Retained
    }
    pub(super) fn absent_reason(self) -> Option<&'static str> {
        match self {
            Self::NoLiveEntry => Some("no_live_ens_v2_entry"),
            Self::Absent => Some("ens_v2_path_no_resolver"),
            _ => None,
        }
    }
    pub(super) fn unsupported_reason(self) -> Option<&'static str> {
        match self {
            Self::Different => Some("ens_v2_path_target_not_projected"),
            Self::Unknown => Some("ens_v2_path_not_projected"),
            _ => None,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub(super) struct Outcome {
    pub decision: Decision,
    pub deadline: Option<i64>,
}

pub(super) struct Walk {
    declarations: Declarations,
    models: BTreeMap<String, Option<SupportedRegistry>>,
    entries: BTreeMap<(String, String), facts::Entry>,
    classes: BTreeMap<String, Option<ResolverClassification>>,
    wrapper_nodes: BTreeMap<String, Option<String>>,
}
impl Walk {
    pub(super) async fn new(
        conn: &mut PgConnection,
        publication: &FamilyPublication,
    ) -> Result<Self> {
        Ok(Self {
            declarations: Declarations::load(conn, publication).await?,
            models: BTreeMap::new(),
            entries: BTreeMap::new(),
            classes: BTreeMap::new(),
            wrapper_nodes: BTreeMap::new(),
        })
    }
    async fn classification(
        &mut self,
        conn: &mut PgConnection,
        publication: &FamilyPublication,
        address: &str,
    ) -> Result<Option<ResolverClassification>> {
        if !self.classes.contains_key(address) {
            let class = load_classifications_at(
                conn,
                &publication.chain_id,
                &[address.to_owned()],
                Some(publication.block_number),
            )
            .await?
            .remove(address);
            self.classes.insert(address.to_owned(), class);
        }
        Ok(self.classes[address].clone())
    }
    pub(super) async fn evaluate(
        &mut self,
        conn: &mut PgConnection,
        publication: &FamilyPublication,
        surface: &Surface,
        retained_resolver: Option<&str>,
    ) -> Result<Outcome> {
        let mut registry = ROOT.to_owned();
        let mut nearest: Option<(String, usize)> = None;
        let mut outcome = Outcome::default();
        for depth in (0..surface.labelhashes.len()).rev() {
            let label = &surface.labelhashes[depth];
            let key = (registry.clone(), label.clone());
            if !self.entries.contains_key(&key) {
                self.entries.insert(
                    key.clone(),
                    facts::load_entry(conn, publication, &registry, label).await?,
                );
            }
            let entry = self.entries[&key].clone();
            if !self.models.contains_key(&registry) {
                // An absent entry has no instance key, but an exact non-proxy declaration
                // still proves its zero getters. The placeholder root is never used as an ID.
                let declared = self.declarations.declared(
                    publication,
                    &registry,
                    entry.instance.unwrap_or(uuid::Uuid::nil()),
                );
                let model = match declared {
                    Some(declared) => Some(declared),
                    None => {
                        registry_support::load(
                            conn,
                            publication,
                            &self.declarations,
                            &registry,
                            entry.instance,
                        )
                        .await?
                    }
                };
                self.models.insert(registry.clone(), model);
            }
            let Some(model) = self.models[&registry].as_ref() else {
                outcome.decision = Decision::Unknown;
                return Ok(outcome);
            };
            if model.model != Model::Declared
                && entry.instance.is_some_and(|instance| {
                    crate::identity::ens_v2_registry_root_resource_id(
                        &publication.chain_id,
                        instance,
                    ) != model.root
                })
            {
                outcome.decision = Decision::Unknown;
                return Ok(outcome);
            }
            let model = model.model;
            outcome.deadline = [
                outcome.deadline,
                entry.deadline(publication.timestamp_seconds()),
            ]
            .into_iter()
            .flatten()
            .min();
            if model == Model::Wrapper && (entry.expiry == Some(0) || entry.missing) {
                if !self.wrapper_nodes.contains_key(&registry) {
                    self.wrapper_nodes.insert(
                        registry.clone(),
                        wrapper::original_node(conn, publication, &registry).await?,
                    );
                }
                let Some(node) = self.wrapper_nodes[&registry].as_deref() else {
                    outcome.decision = Decision::Unknown;
                    return Ok(outcome);
                };
                let eligibility = wrapper::eligible(conn, publication, node, label).await?;
                outcome.deadline = [outcome.deadline, eligibility.deadline]
                    .into_iter()
                    .flatten()
                    .min();
                match eligibility.eligible {
                    Some(true) => {
                        nearest = Some((wrapper::MIRROR.into(), depth));
                        break;
                    }
                    None => {
                        outcome.decision = Decision::Unknown;
                        return Ok(outcome);
                    }
                    Some(false) => {}
                }
            }
            if entry.missing || entry.expired(publication.timestamp_seconds()) {
                break;
            }
            if !entry.known {
                outcome.decision = Decision::Unknown;
                return Ok(outcome);
            }
            if !is_cleared(entry.resolver.as_deref()) {
                nearest = entry.resolver.clone().map(|resolver| (resolver, depth));
            }
            if is_cleared(entry.subregistry.as_deref()) {
                break;
            }
            registry = entry.subregistry.unwrap_or_default();
        }
        let Some((address, depth)) = nearest else {
            outcome.decision = Decision::Absent;
            return Ok(outcome);
        };
        let Some(class) = self.classification(conn, publication, &address).await? else {
            outcome.decision = Decision::Unknown;
            return Ok(outcome);
        };
        if !class.supported() || !class.declared_in(&surface.namespace) {
            outcome.decision = Decision::Unknown;
            return Ok(outcome);
        }
        if depth > 0
            && class.field("role") != Some("ensv1_mirror_resolver")
            && !class.has_read_feature("ensip10_extended_resolver")
        {
            outcome.decision = Decision::Absent;
            return Ok(outcome);
        }
        if class.field("role") != Some("ensv1_mirror_resolver") {
            // Only the admitted ENSv1 node-based model proves an equivalent target. A token
            // or resource keyed resolver at this same address would not prove the record ID.
            outcome.decision = if depth == 0
                && class.field("source_family") == Some("ens_v1_resolver_l1")
                && !class.has_read_feature("ensip10_extended_resolver")
                && retained_resolver == Some(address.as_str())
            {
                Decision::Retained
            } else {
                Decision::Different
            };
            return Ok(outcome);
        }
        let pointer = ServingPointer {
            resource_id: uuid::Uuid::nil(),
            logical_name_id: surface.logical_name_id.clone(),
            namespace: surface.namespace.clone(),
            source_family: "ens_v2_registry_l1".into(),
            namehash: surface.namehash.clone(),
            resolver_address: address,
            pointer_event_id: None,
            block_number: publication.block_number,
        };
        let mirror = evaluate_family_mirror_at(
            conn,
            &publication.chain_id,
            &pointer,
            class,
            Some(publication.block_number),
        )
        .await?;
        outcome.decision = if mirror
            .nearest
            .as_ref()
            .is_some_and(|nearest| !nearest.mirrored_classification_supported)
        {
            // A structural mirror reason can precede support/namespace checks. It does not
            // prove a known target, even when the address matches the retained pointer.
            Decision::Unknown
        } else if mirror
            .substituted(&pointer)
            .is_some_and(|target| retained_resolver == Some(target.resolver_address.as_str()))
        {
            Decision::Retained
        } else if mirror.nearest.as_ref().is_none_or(|nearest| {
            nearest.mirrored_unsupported_reason.as_deref() == Some("ancestor_resolver_not_extended")
        }) {
            Decision::Absent
        } else {
            Decision::Different
        };
        Ok(outcome)
    }
}
