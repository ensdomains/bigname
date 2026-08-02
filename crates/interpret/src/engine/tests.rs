use std::collections::{BTreeMap, HashMap};

use super::{RunMode, SessionKey, update_prior_sessions};
use crate::load::PriorSnapshot;

fn empty_snapshot() -> PriorSnapshot {
    PriorSnapshot {
        events: Vec::new(),
        dependencies: BTreeMap::new(),
    }
}

#[test]
fn completed_redo_sessions_are_evicted_without_dropping_normal_sessions() {
    let mut sessions = HashMap::new();
    let redo = SessionKey {
        chain_id: "chain".to_owned(),
        from_block: 100,
        mode: RunMode::Redo,
    };
    update_prior_sessions(&mut sessions, redo.clone(), 200, empty_snapshot(), false);
    assert_eq!(sessions.len(), 1);

    update_prior_sessions(&mut sessions, redo, 201, empty_snapshot(), true);
    assert!(sessions.is_empty());

    let normal = SessionKey {
        chain_id: "chain".to_owned(),
        from_block: 100,
        mode: RunMode::Normal,
    };
    update_prior_sessions(&mut sessions, normal, 201, empty_snapshot(), true);
    assert_eq!(sessions.len(), 1);
}
