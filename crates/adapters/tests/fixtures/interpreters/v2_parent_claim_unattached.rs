//! Runs `v2-parent-claim-unattached.json`: a user registry whose parent claim does not point
//! back at it keeps the name its parent token gives it. Each case pins the grants and releases
//! of the registry's `child` token, in every lane `interpret_physical_batches` compares and in
//! one whole batch.
use super::*;

const FIXTURE: &str = include_str!("v2-parent-claim-unattached.json");

#[derive(Deserialize)]
struct Suite {
    cases: Vec<MountCase>,
}

#[derive(Deserialize)]
struct MountCase {
    case: Case,
    batches: Vec<BlockRange>,
    child_token_id: String,
    #[serde(default)]
    expected_child_lifecycle: Vec<Value>,
    /// Name surfaces a block must write, by name and visibility.
    #[serde(default)]
    expected_surfaces: Vec<Value>,
}

fn child_lifecycle(outputs: &[BatchOutput], token_id: &str, names: &[&str]) -> Vec<Value> {
    outputs
        .iter()
        .flat_map(|output| &output.normalized_events)
        .filter(|event| {
            matches!(
                event.event_kind.as_str(),
                "RegistrationGranted" | "RegistrationReleased" | "RegistrationReserved"
            ) && event.after_state["token_id"] == token_id
        })
        .map(|event| {
            let name = event.logical_name_id.as_deref().map(|id| {
                names
                    .iter()
                    .find(|name| namehash_logical_id("ens", name) == id)
                    .map_or(id, |name| *name)
                    .to_owned()
            });
            let mut row = serde_json::json!({
                "block_number": event.block_number,
                "log_index": event.log_index,
                "event_kind": event.event_kind,
                "name": name,
                "source_event": event.after_state["source_event"],
            });
            if let Some(reason) = event.after_state.get("terminal_reason") {
                row["terminal_reason"] = reason.clone();
            }
            row
        })
        .collect()
}

fn run(case_id: &str) -> Result<()> {
    let suite: Suite = serde_json::from_str(FIXTURE)?;
    let fixture = suite
        .cases
        .into_iter()
        .find(|fixture| fixture.case.id == case_id)
        .with_context(|| format!("no fixture case {case_id}"))?;
    let gate = ExpectedCase {
        id: fixture.case.id.clone(),
        normalized_events: fixture
            .case
            .blocks
            .iter()
            .map(|block| serde_json::json!({"block_hash": block.hash}))
            .collect(),
        name_surfaces: Vec::new(),
        surface_bindings: Vec::new(),
        resources: Vec::new(),
        token_lineages: Vec::new(),
    };
    let input = batch_input(&fixture.case, &gate, &checked_in_manifests()?)?;
    let names = fixture
        .expected_child_lifecycle
        .iter()
        .filter_map(|row| row["name"].as_str())
        .collect::<Vec<_>>();
    let split = interpret_physical_batches(case_id, input.clone(), &fixture.batches)?;
    assert_eq!(
        child_lifecycle(&split, &fixture.child_token_id, &names),
        fixture.expected_child_lifecycle,
        "{case_id}: one batch per block"
    );
    let whole = [interpret_schema_v2_batch(input)?];
    assert_eq!(
        child_lifecycle(&whole, &fixture.child_token_id, &names),
        fixture.expected_child_lifecycle,
        "{case_id}: one batch"
    );
    for outputs in [&split[..], &whole[..]] {
        for expected in &fixture.expected_surfaces {
            let id = namehash_logical_id("ens", expected["name"].as_str().context("name")?);
            assert!(
                outputs
                    .iter()
                    .flat_map(|output| &output.name_surfaces)
                    .any(|surface| surface.logical_name_id == id
                        && surface.block_number == expected["block_number"]
                        && surface.visibility_state == expected["visibility_state"]),
                "{case_id}: no surface {expected}"
            );
        }
    }
    Ok(())
}

#[test]
fn claim_moves_to_a_label_with_no_token_then_clears() -> Result<()> {
    run("claim_moves_to_a_label_with_no_token_then_clears")
}

#[test]
fn mount_then_unattached_claim_in_one_block() -> Result<()> {
    run("mount_then_unattached_claim_in_one_block")
}

#[test]
fn unattached_claim_then_mount_in_one_block() -> Result<()> {
    run("unattached_claim_then_mount_in_one_block")
}

#[test]
fn mount_with_no_claim() -> Result<()> {
    run("mount_with_no_claim")
}

#[test]
fn unmount_then_remount_under_an_unattached_claim() -> Result<()> {
    run("unmount_then_remount_under_an_unattached_claim")
}

#[test]
fn expired_mount_renewed_under_an_unattached_claim() -> Result<()> {
    run("expired_mount_renewed_under_an_unattached_claim")
}

#[test]
fn expired_mount_moves_the_name_to_the_other_mount() -> Result<()> {
    run("expired_mount_moves_the_name_to_the_other_mount")
}

#[test]
fn expired_claimed_mount_moves_the_name_to_the_other_mount() -> Result<()> {
    run("expired_claimed_mount_moves_the_name_to_the_other_mount")
}

// The cases below move or reveal a name at a block boundary, where an expiry has no log.
#[test]
fn expiry_reveals_a_mount_path() -> Result<()> {
    run("expiry_reveals_a_mount_path")
}

#[test]
fn expired_mount_moves_the_name_to_a_shadow_path() -> Result<()> {
    run("expired_mount_moves_the_name_to_a_shadow_path")
}

#[test]
fn expired_shadow_mount_moves_the_name_to_a_normal_path() -> Result<()> {
    run("expired_shadow_mount_moves_the_name_to_a_normal_path")
}

#[test]
fn expired_shadow_mount_moves_the_name_to_another_shadow_path() -> Result<()> {
    run("expired_shadow_mount_moves_the_name_to_another_shadow_path")
}

#[test]
fn expired_mount_moves_a_reserved_label_with_no_resource() -> Result<()> {
    run("expired_mount_moves_a_reserved_label_with_no_resource")
}

#[test]
fn expired_mount_moves_a_reserved_label_with_a_resource() -> Result<()> {
    run("expired_mount_moves_a_reserved_label_with_a_resource")
}

#[test]
fn expired_mount_moves_an_unlinked_token() -> Result<()> {
    run("expired_mount_moves_an_unlinked_token")
}

#[test]
fn a_third_party_shadow_mount_expires_back_to_the_owner_mount() -> Result<()> {
    run("a_third_party_shadow_mount_expires_back_to_the_owner_mount")
}

#[test]
fn claim_made_valid_moves_the_name_to_the_claimed_path() -> Result<()> {
    run("claim_made_valid_moves_the_name_to_the_claimed_path")
}

#[test]
fn claim_set_before_the_mount_exists() -> Result<()> {
    run("claim_set_before_the_mount_exists")
}
