use super::*;

pub(super) fn correlate_renewals(
    observations: &[&MigrationObservation],
    output: &mut BatchOutput,
) -> anyhow::Result<()> {
    let bridges = observations.iter().copied().filter(|observation| {
        observation.event_name == "NameRenewed"
            && observation.emitter_role.as_deref() == Some("ens_v1_renewal_bridge")
    });
    for bridge in bridges {
        let token_id = value_str(&bridge.decoded, "token_id")?;
        let base_token_id = value_str(&bridge.decoded, "base_token_id")?;
        let logical_name_id = logical_name_from_decoded(&bridge.decoded)?;
        let Some(bridge_expiry) = bridge.decoded.get("expiry").and_then(Value::as_u64) else {
            continue;
        };
        let v2_events = matching_events(output, &bridge.raw, |event| {
            event.source_family == "ens_v2_registry_l1"
                && event.logical_name_id.as_deref() == Some(logical_name_id.as_str())
                && event.after_state.get("token_id").and_then(Value::as_str) == Some(token_id)
                && matches!(
                    event.event_kind.as_str(),
                    "ExpiryChanged" | "RegistrationRenewed"
                )
                && event.after_state.get("expiry").and_then(Value::as_u64) == Some(bridge_expiry)
                && event
                    .log_index
                    .is_some_and(|index| index < bridge.raw.log_index)
        });
        if !v2_events
            .iter()
            .any(|event| event.event_kind == "ExpiryChanged")
        {
            continue;
        }
        let Some(v2_position) = v2_events.iter().filter_map(|event| event.log_index).max() else {
            continue;
        };
        let Some(base) = observations
            .iter()
            .copied()
            .filter(|observation| {
                observation.event_name == "NameRenewed"
                    && is_v1_registrar_observation(observation)
                    && observation.decoded.get("token_id").and_then(Value::as_str)
                        == Some(base_token_id)
                    && observation.raw.log_index > v2_position
                    && observation.raw.log_index < bridge.raw.log_index
            })
            .max_by_key(|observation| observation.raw.log_index)
        else {
            continue;
        };
        let mut resources = v2_events
            .iter()
            .filter_map(|event| event.resource_id)
            .collect::<BTreeSet<_>>();
        if resources.len() > 1 {
            continue;
        }
        let successor_resource = resources.pop_first();
        let mut evidence = vec![observation_evidence(bridge), observation_evidence(base)];
        evidence.extend(v2_events.iter().map(event_evidence));
        let correlation_id =
            correlation_id("synchronized_renewal", Some(&logical_name_id), &evidence);
        mark_direct_position(output, &bridge.raw, &correlation_id);
        if let Some(successor_resource) = successor_resource {
            anchor_direct_position(output, &bridge.raw, successor_resource);
        }
        mark_direct_position(output, &base.raw, &correlation_id);
        for event in &v2_events {
            associate_event(
                output,
                &event.event_identity,
                &correlation_id,
                "synchronized_renewal",
                evidence.clone(),
            )?;
        }
    }
    Ok(())
}
