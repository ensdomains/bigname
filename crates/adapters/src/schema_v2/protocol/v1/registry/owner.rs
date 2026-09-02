use super::ZERO_ADDRESS;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RegistryOwnerView {
    Authentic { owner: String },
    ZeroEquivalent { reason: RegistryOwnerZeroReason },
    UnavailableUnmasked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RegistryOwnerZeroReason {
    LiteralZero,
    RegistrySelf,
}

impl RegistryOwnerZeroReason {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::LiteralZero => "literal_zero",
            Self::RegistrySelf => "registry_self",
        }
    }
}

pub(super) fn classify(
    owner_word: &str,
    registry_address: &str,
    body_has_unmasked_owner_word: bool,
    registry_self_is_zero: bool,
) -> RegistryOwnerView {
    if body_has_unmasked_owner_word {
        RegistryOwnerView::UnavailableUnmasked
    } else if owner_word.eq_ignore_ascii_case(ZERO_ADDRESS) {
        RegistryOwnerView::ZeroEquivalent {
            reason: RegistryOwnerZeroReason::LiteralZero,
        }
    } else if registry_self_is_zero && owner_word.eq_ignore_ascii_case(registry_address) {
        RegistryOwnerView::ZeroEquivalent {
            reason: RegistryOwnerZeroReason::RegistrySelf,
        }
    } else {
        RegistryOwnerView::Authentic {
            owner: owner_word.to_owned(),
        }
    }
}
