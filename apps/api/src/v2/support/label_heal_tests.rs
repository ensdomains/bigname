use super::*;

fn bracket(label: &str) -> String {
    format!(
        "[{}]",
        alloy_primitives::hex::encode(alloy_primitives::keccak256(label.as_bytes()))
    )
}

fn hash(label: &str) -> [u8; 32] {
    alloy_primitives::keccak256(label.as_bytes()).0
}

fn healed(heal: &mut LabelHeal, name: &str) -> String {
    let mut name = name.to_owned();
    heal.heal(&mut name);
    name
}

#[test]
fn a_plain_label_heals_its_placeholder_at_every_position() {
    let mut heal = LabelHeal::from_names(["first.alpha.eth"]);
    assert_eq!(
        healed(
            &mut heal,
            &format!(
                "{}.{}.{}",
                bracket("first"),
                bracket("alpha"),
                bracket("eth")
            )
        ),
        "first.alpha.eth"
    );
    assert_eq!(
        heal.candidates(),
        BTreeMap::from([
            (hash("alpha"), "alpha".to_owned()),
            (hash("eth"), "eth".to_owned()),
            (hash("first"), "first".to_owned()),
        ])
    );
}

#[test]
fn a_placeholder_the_request_does_not_prove_is_kept() {
    let unknown = format!("[{}]", "22".repeat(32));
    let mut heal = LabelHeal::from_names(["first.alpha.eth"]);
    assert_eq!(
        healed(
            &mut heal,
            &format!("{unknown}.{}.alpha.eth", bracket("first"))
        ),
        format!("{unknown}.first.alpha.eth")
    );
    assert_eq!(
        heal.candidates(),
        BTreeMap::from([(hash("first"), "first".to_owned())])
    );
}

#[test]
fn a_bracketed_request_label_proves_nothing() {
    let mut heal = LabelHeal::from_names([format!("{}.alpha.eth", bracket("first")).as_str()]);
    let served = format!("{}.{}.eth", bracket("first"), bracket("alpha"));
    assert_eq!(
        healed(&mut heal, &served),
        format!("{}.alpha.eth", bracket("first"))
    );
    assert_eq!(
        heal.candidates(),
        BTreeMap::from([(hash("alpha"), "alpha".to_owned())])
    );
}

#[test]
fn the_same_label_at_two_positions_heals_both_and_is_one_candidate() {
    let mut heal = LabelHeal::from_names(["x.x.eth"]);
    assert_eq!(
        healed(&mut heal, &format!("{}.{}.eth", bracket("x"), bracket("x"))),
        "x.x.eth"
    );
    assert_eq!(
        heal.candidates(),
        BTreeMap::from([(hash("x"), "x".to_owned())])
    );
}

#[test]
fn a_name_without_placeholders_is_unchanged_and_stages_nothing() {
    let mut heal = LabelHeal::from_names(["first.alpha.eth"]);
    assert_eq!(healed(&mut heal, "first.alpha.eth"), "first.alpha.eth");
    assert_eq!(healed(&mut heal, "other.eth"), "other.eth");
    assert!(heal.candidates().is_empty());
}

#[test]
fn a_batch_proves_every_label_of_every_name_input() {
    // A keccak match is a proof wherever the hash appears, an address input's row included.
    let mut heal = LabelHeal::from_names(["first.alpha.eth", "second.eth"]);
    assert_eq!(
        healed(
            &mut heal,
            &format!("{}.{}.eth", bracket("second"), bracket("first"))
        ),
        "second.first.eth"
    );
    assert_eq!(heal.candidates().len(), 2);
}

#[test]
fn an_observed_name_is_staged_from_but_not_changed() {
    let mut heal = LabelHeal::from_names(["first.alpha.eth"]);
    heal.observe(&format!("{}.alpha.eth", bracket("first")));
    assert_eq!(
        heal.candidates(),
        BTreeMap::from([(hash("first"), "first".to_owned())])
    );
    // The alias path's canonical name heals the same way as any served name.
    let mut canonical = format!("{}.alpha.eth", bracket("first"));
    heal.heal(&mut canonical);
    assert_eq!(canonical, "first.alpha.eth");
}

#[test]
fn a_label_that_only_looks_like_a_placeholder_is_not_replaced() {
    let mut heal = LabelHeal::from_names(["first.alpha.eth"]);
    let short = format!("[{}]", &bracket("first")[1..64]);
    let served = format!("{short}.alpha.eth");
    assert_eq!(healed(&mut heal, &served), served);
    assert!(heal.candidates().is_empty());
}
