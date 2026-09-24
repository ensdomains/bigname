use serde::{Deserialize, Serialize};

/// Where the chain reads a name's current registration and control from: the selected
/// [authority epoch](../../../../../docs/glossary.md#authority-epoch) arm, with the ENSv1 arm
/// split by [registry generation](../../../../../docs/glossary.md#registry-generation) into
/// `ens_v0` (still read from the 2017 registry, because the current registry holds no record for
/// the node yet) and `ens_v1`
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistryWithFallback.sol:L18-L46 @ ens_v1@91c966f)
/// (upstream: .refs/ens_v1/contracts/registry/ENSRegistry.sol:L150-L157 @ ens_v1@91c966f).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Authority {
    EnsV0,
    EnsV1,
    EnsV2,
}

impl Authority {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::EnsV0 => "ens_v0",
            Self::EnsV1 => "ens_v1",
            Self::EnsV2 => "ens_v2",
        }
    }

    pub(crate) fn from_wire(value: &str) -> Option<Self> {
        match value {
            "ens_v0" => Some(Self::EnsV0),
            "ens_v1" => Some(Self::EnsV1),
            "ens_v2" => Some(Self::EnsV2),
            _ => None,
        }
    }

    /// The value a current name row serves. Every response builder reads it here, so Basenames
    /// rows, unresolved selections and ownerless registry rows yield `None` alike.
    pub(crate) fn from_provenance(provenance: &serde_json::Value) -> Option<Self> {
        bigname_storage::name_current_public_authority(provenance).and_then(Self::from_wire)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::Authority;

    #[test]
    fn authority_wire_values_round_trip() {
        for authority in [Authority::EnsV0, Authority::EnsV1, Authority::EnsV2] {
            assert_eq!(Authority::from_wire(authority.as_str()), Some(authority));
            assert_eq!(
                serde_json::to_value(authority).unwrap(),
                json!(authority.as_str())
            );
        }
        assert_eq!(Authority::from_wire("basenames"), None);
    }

    #[test]
    fn authority_from_provenance_maps_generation_and_omits_ownerless_rows() {
        let provenance = |selection| json!({"authority_selection": selection});
        assert_eq!(
            Authority::from_provenance(&provenance(
                json!({"authority_arm": "ens_v1", "registry_generation": "old"})
            )),
            Some(Authority::EnsV0)
        );
        assert_eq!(
            Authority::from_provenance(&provenance(json!({"authority_arm": "ens_v1"}))),
            Some(Authority::EnsV1)
        );
        assert_eq!(
            Authority::from_provenance(&provenance(json!({
                "authority_arm": "ens_v1", "registry_generation": "old",
                "ownerless_registry": true
            }))),
            None
        );
    }
}
