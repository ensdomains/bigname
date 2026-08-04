use std::collections::{BTreeMap, HashMap};

use super::{RunMode, SessionKey, update_prior_sessions};
use crate::load::PriorSnapshot;

fn empty_snapshot() -> PriorSnapshot {
    PriorSnapshot {
        events: Vec::new(),
        dependencies: BTreeMap::new(),
        validated_orphaning_epoch: 0,
        pending_dependencies: Default::default(),
    }
}

#[test]
fn completed_redo_evicts_every_session_for_its_chain() {
    let mut sessions = HashMap::new();
    let normal = SessionKey {
        chain_id: "chain".to_owned(),
        from_block: 0,
        mode: RunMode::Normal,
    };
    let other_chain = SessionKey {
        chain_id: "other-chain".to_owned(),
        from_block: 0,
        mode: RunMode::Normal,
    };
    let redo = SessionKey {
        chain_id: "chain".to_owned(),
        from_block: 100,
        mode: RunMode::Redo,
    };
    update_prior_sessions(&mut sessions, normal, 500, empty_snapshot(), false);
    update_prior_sessions(
        &mut sessions,
        other_chain.clone(),
        500,
        empty_snapshot(),
        false,
    );
    update_prior_sessions(&mut sessions, redo.clone(), 200, empty_snapshot(), false);
    assert_eq!(sessions.len(), 3);

    update_prior_sessions(&mut sessions, redo, 201, empty_snapshot(), true);
    assert_eq!(sessions.len(), 1);
    assert!(sessions.contains_key(&other_chain));
}
