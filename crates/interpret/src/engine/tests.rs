use std::collections::HashMap;

use super::update_prior_sessions;

#[test]
fn a_new_completed_normal_session_replaces_the_chain_slot() {
    let mut sessions = HashMap::new();
    update_prior_sessions(&mut sessions, "chain".to_owned(), Some("normal-from-0"));
    update_prior_sessions(
        &mut sessions,
        "other-chain".to_owned(),
        Some("other-normal"),
    );
    update_prior_sessions(&mut sessions, "chain".to_owned(), Some("normal-from-100"));

    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions.get("chain"), Some(&"normal-from-100"));
}

#[test]
fn completed_redo_evicts_the_chain_slot() {
    let mut sessions = HashMap::new();
    update_prior_sessions(&mut sessions, "chain".to_owned(), Some("normal"));
    update_prior_sessions(
        &mut sessions,
        "other-chain".to_owned(),
        Some("other-normal"),
    );
    update_prior_sessions(&mut sessions, "chain".to_owned(), None);

    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions.get("other-chain"), Some(&"other-normal"));
}
