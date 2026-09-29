//! Address relation vocabulary and canonical relation sets.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Relation {
    Owner,
    Manager,
    Registrant,
    /// The address holds an ENSv2 registry role on the name; not exclusive, and not the manager.
    RoleHolder,
    /// The address is the value of the name's current `addr:<coin_type>` resolver record. A
    /// resolver-record relation, not an authority relation: it is coin-type scoped, never part
    /// of `any`, and never combined with the authority relations in one set.
    ResolvesTo,
    /// The address was the last registrant of the name's registration when it ended: an ENSv1
    /// lease that lapsed past grace, or an ENSv2 registration that expired or was unregistered
    /// (`lapsed_registration.registrant`). Never current authority, never part of `any`, and never
    /// combined with another relation in one set.
    FormerRegistrant,
}

impl Relation {
    /// The authority relations `any` expands to. `resolves_to` is deliberately outside this set.
    pub(crate) const ALL: [Self; 4] = [
        Self::Owner,
        Self::Manager,
        Self::Registrant,
        Self::RoleHolder,
    ];

    /// The relations reverse lookup serves, and what `any` expands to there.
    pub(crate) const LOOKUP: [Self; 3] = [Self::Owner, Self::Manager, Self::Registrant];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Manager => "manager",
            Self::Registrant => "registrant",
            Self::RoleHolder => "role_holder",
            Self::ResolvesTo => "resolves_to",
            Self::FormerRegistrant => "former_registrant",
        }
    }

    pub(crate) fn from_wire(value: &str) -> Option<Self> {
        match value {
            "owner" => Some(Self::Owner),
            "manager" => Some(Self::Manager),
            "registrant" => Some(Self::Registrant),
            "role_holder" => Some(Self::RoleHolder),
            "resolves_to" => Some(Self::ResolvesTo),
            "former_registrant" => Some(Self::FormerRegistrant),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelationSet {
    relations: Vec<Relation>,
}

impl RelationSet {
    pub(crate) fn all() -> Self {
        Self {
            relations: Relation::ALL.to_vec(),
        }
    }

    /// Canonicalizes a requested set. `resolves_to` is only valid on its own: a set that mixes it
    /// with an authority relation has no canonical form and returns `None`.
    pub(crate) fn from_relations(relations: impl IntoIterator<Item = Relation>) -> Option<Self> {
        let requested = relations.into_iter().collect::<Vec<_>>();
        for exclusive in [Relation::ResolvesTo, Relation::FormerRegistrant] {
            if requested.contains(&exclusive) {
                return requested
                    .iter()
                    .all(|relation| *relation == exclusive)
                    .then(|| Self {
                        relations: vec![exclusive],
                    });
            }
        }
        let mut normalized = Vec::new();
        for candidate in Relation::ALL {
            if requested.contains(&candidate) && !normalized.contains(&candidate) {
                normalized.push(candidate);
            }
        }
        (!normalized.is_empty()).then_some(Self {
            relations: normalized,
        })
    }

    /// The reverse lookup form of `any`: the three relations reverse lookup serves.
    pub(crate) fn lookup_all() -> Self {
        Self {
            relations: Relation::LOOKUP.to_vec(),
        }
    }

    pub(crate) fn as_slice(&self) -> &[Relation] {
        &self.relations
    }

    pub(crate) fn canonical_value(&self) -> String {
        self.relations
            .iter()
            .map(|relation| relation.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(crate) fn is_all(&self) -> bool {
        self.relations == Relation::ALL
    }

    pub(crate) fn is_exact_manager(&self) -> bool {
        self.relations == [Relation::Manager]
    }

    pub(crate) fn is_resolves_to(&self) -> bool {
        self.relations == [Relation::ResolvesTo]
    }

    pub(crate) fn is_former_registrant(&self) -> bool {
        self.relations == [Relation::FormerRegistrant]
    }

    pub(crate) fn is_exact_owner_and_registrant(&self) -> bool {
        self.relations == [Relation::Owner, Relation::Registrant]
    }
}

impl From<Relation> for RelationSet {
    fn from(value: Relation) -> Self {
        Self {
            relations: vec![value],
        }
    }
}
