//! Lifecycle membership read from the F2a maxima (design, note "F2a lifecycle key" and "F2a
//! reader summaries and reducer state"). A resource's lifecycle is the field-wise merge of its
//! key state with the summary of every triple whose association currently targets it; the
//! candidate of a key and the retirement of a resource are computed from positions alone, so a
//! reassociation changes them without any write to the events it moves.
use crate::families::control::{
    position::Position,
    rows::{Mark, Maxima},
};

/// The merged view of one lifecycle key.
#[derive(Clone, Debug, Default)]
pub struct MergedView {
    pub last_active: Option<Mark>,
    pub last_release_any: Option<Mark>,
    pub last_path_expiry: Option<Mark>,
    pub last_explicit_release: Option<Mark>,
    pub last_grant: Option<Mark>,
    pub last_reservation: Option<Mark>,
    pub last_renewal: Option<Mark>,
    pub last_expiry_changed: Option<Mark>,
}

fn later(left: &Option<Mark>, right: &Option<Mark>) -> Option<Mark> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if right.position > left.position {
            right.clone()
        } else {
            left.clone()
        }),
        (Some(mark), None) | (None, Some(mark)) => Some(mark.clone()),
        (None, None) => None,
    }
}

/// Per field the later of the stored values under the canonical order (design:40). The key
/// state's own events and the triples' null-resource events are disjoint sets, so the later
/// maximum is the maximum of the union. `last_revival` never merges: retirement reads the key
/// state alone.
pub fn merged_view<'a>(
    key_state: Option<&'a Maxima>,
    triples: impl IntoIterator<Item = &'a Maxima>,
) -> MergedView {
    let mut view = MergedView::default();
    for maxima in key_state.into_iter().chain(triples) {
        view.last_active = later(&view.last_active, &maxima.last_active);
        view.last_release_any = later(&view.last_release_any, &maxima.last_release_any);
        view.last_path_expiry = later(&view.last_path_expiry, &maxima.last_path_expiry);
        view.last_explicit_release =
            later(&view.last_explicit_release, &maxima.last_explicit_release);
        view.last_grant = later(&view.last_grant, &maxima.last_grant);
        view.last_reservation = later(&view.last_reservation, &maxima.last_reservation);
        view.last_renewal = later(&view.last_renewal, &maxima.last_renewal);
        view.last_expiry_changed = later(&view.last_expiry_changed, &maxima.last_expiry_changed);
    }
    view
}

/// Which kind of registration candidate a key serves.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateKind {
    /// The latest grant or reservation, with no release after it.
    Active,
    /// The latest path-expiry release, with no grant or reservation after it.
    PathExpiry,
    /// The latest explicit release, witnessed by an earlier grant or reservation with no path
    /// expiry between them.
    Explicit,
}

/// One key's registration candidate: its kind and the event that is the candidate.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub kind: CandidateKind,
    pub position: Position,
    /// The event kind: RegistrationGranted or RegistrationReserved for an active candidate,
    /// RegistrationReleased otherwise.
    pub event_kind: String,
}

/// The candidate of one key: candidate_active, candidate_path and candidate_explicit with the
/// witness rule, latest of the three (design:63, restating build.sql:322-334 under D12).
pub fn candidate(view: &MergedView) -> Option<Candidate> {
    let active = view.last_active.as_ref();
    let mut found: Vec<Candidate> = Vec::new();
    if let Some(active) = active {
        let released_after = view
            .last_release_any
            .as_ref()
            .is_some_and(|release| release.position > active.position);
        if !released_after {
            found.push(Candidate {
                kind: CandidateKind::Active,
                position: active.position.clone(),
                event_kind: active.kind().unwrap_or("RegistrationGranted").to_owned(),
            });
        }
    }
    if let Some(path) = &view.last_path_expiry
        && active.is_none_or(|active| active.position < path.position)
    {
        found.push(Candidate {
            kind: CandidateKind::PathExpiry,
            position: path.position.clone(),
            event_kind: "RegistrationReleased".into(),
        });
    }
    if let (Some(explicit), Some(active)) = (&view.last_explicit_release, active)
        && active.position < explicit.position
    {
        let path_between = view.last_path_expiry.as_ref().is_some_and(|path| {
            active.position < path.position && path.position < explicit.position
        });
        if !path_between {
            found.push(Candidate {
                kind: CandidateKind::Explicit,
                position: explicit.position.clone(),
                event_kind: "RegistrationReleased".into(),
            });
        }
    }
    found
        .into_iter()
        .max_by(|left, right| left.position.cmp(&right.position))
}

/// The cross-key preference of build.sql:336-340 over one candidate per key: the binding
/// resource's non-released candidate, then any non-released one, then the binding key's, then
/// the latest. `keys` pairs each key with its candidate; the result is the index of the winner.
pub fn preferred<'a>(
    keys: impl IntoIterator<Item = (&'a str, &'a Candidate)>,
    binding_resource: Option<&str>,
) -> Option<usize> {
    keys.into_iter()
        .enumerate()
        .max_by(|(_, (left_key, left)), (_, (right_key, right))| {
            let rank = |key: &str, candidate: &Candidate| {
                let binding = binding_resource == Some(key);
                let released = candidate.event_kind == "RegistrationReleased";
                (binding && !released, !released, binding)
            };
            rank(left_key, left)
                .cmp(&rank(right_key, right))
                .then_with(|| left.position.cmp(&right.position))
        })
        .map(|(index, _)| index)
}

/// Expiry retirement of one resource, from its key state alone (design:63, restating
/// expiry_retirement.rs:44-93): the latest path-expiry release is retired unless a grant,
/// reservation or qualifying revival of the same key state is positioned after it. A later
/// explicit release restores nothing.
pub fn retirement(key_state: &Maxima) -> Option<Position> {
    let path = key_state.last_path_expiry.as_ref()?;
    let restored = [&key_state.last_active, &key_state.last_revival]
        .into_iter()
        .flatten()
        .any(|mark| mark.position > path.position);
    (!restored).then(|| path.position.clone())
}

/// The permissions builder's drop rule (permissions.rs:111-133, :391-398) read from the key
/// state: a resource whose latest ENSv2 registration event (grant, reservation, qualifying
/// revival or path-expiry release) is a path-expiry release serves no grants.
pub fn registration_lapsed(key_state: &Maxima) -> bool {
    let Some(path) = &key_state.last_path_expiry else {
        return false;
    };
    ![&key_state.last_active, &key_state.last_revival]
        .into_iter()
        .flatten()
        .any(|mark| mark.position > path.position)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn mark(block: i64, kind: Option<&str>) -> Option<Mark> {
        Some(Mark {
            position: Position {
                block_number: block,
                transaction_index: Some(0),
                log_index: Some(0),
                event_identity: format!("e{block}"),
            },
            detail: json!({"kind": kind}),
        })
    }

    #[test]
    fn history_a_grant_path_expiry_explicit_release_serves_the_path_expiry() {
        // A: G, P, E. P lies between G and E, so E is not witnessed; P is the candidate.
        let view = MergedView {
            last_active: mark(10, Some("RegistrationGranted")),
            last_release_any: mark(30, None),
            last_path_expiry: mark(20, None),
            last_explicit_release: mark(30, None),
            ..MergedView::default()
        };
        let chosen = candidate(&view).expect("a candidate");
        assert_eq!(chosen.kind, CandidateKind::PathExpiry);
        assert_eq!(chosen.position.block_number, 20);
    }

    #[test]
    fn history_b_grant_explicit_release_serves_the_witnessed_release() {
        let view = MergedView {
            last_active: mark(10, Some("RegistrationGranted")),
            last_release_any: mark(30, None),
            last_explicit_release: mark(30, None),
            ..MergedView::default()
        };
        let chosen = candidate(&view).expect("a candidate");
        assert_eq!(chosen.kind, CandidateKind::Explicit);
    }

    #[test]
    fn a_revival_after_a_path_expiry_restores_it_and_a_renewal_alone_does_not() {
        let revived = Maxima {
            last_path_expiry: mark(10, None),
            last_revival: mark(20, None),
            last_renewal: mark(30, None),
            ..Maxima::default()
        };
        assert_eq!(retirement(&revived), None);
        assert!(!registration_lapsed(&revived));
        let renewed = Maxima {
            last_path_expiry: mark(10, None),
            last_renewal: mark(30, None),
            ..Maxima::default()
        };
        assert_eq!(retirement(&renewed).map(|at| at.block_number), Some(10));
        assert!(registration_lapsed(&renewed));
    }

    #[test]
    fn the_preference_puts_the_binding_keys_live_candidate_first() {
        let live = Candidate {
            kind: CandidateKind::Active,
            position: mark(30, None).unwrap().position,
            event_kind: "RegistrationGranted".into(),
        };
        let released = Candidate {
            kind: CandidateKind::Explicit,
            position: mark(50, None).unwrap().position,
            event_kind: "RegistrationReleased".into(),
        };
        let keys = [("k1", &released), ("k2", &live)];
        assert_eq!(
            preferred(keys, Some("k1")),
            Some(1),
            "a live key beats a released binding key"
        );
        let both_released = [("k1", &released), ("k2", &released)];
        assert_eq!(preferred(both_released, Some("k2")), Some(1));
    }
}
