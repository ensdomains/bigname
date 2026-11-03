//! The failure message: one block per unmatched log, naming the test, the log, what the batch
//! admits and declares for it, and how to accept it on purpose.

use std::fmt::Write as _;

use super::{Admitted, DeclaredEvent, Unmatched, Why};

pub(super) fn test_name() -> String {
    std::thread::current()
        .name()
        .unwrap_or("<unnamed thread>")
        .to_owned()
}

pub(super) fn render(unmatched: &[&Unmatched], database: Option<&str>) -> String {
    let test = test_name();
    let mut out = format!(
        "{} fixture log(s) matched no catalog candidate; production drops such a log silently, \
         so the test exercises a smaller batch than it builds",
        unmatched.len()
    );
    for item in unmatched {
        let log = &item.log;
        let topic0 = log.topics.first().map_or("none", String::as_str);
        let _ = writeln!(out);
        let _ = writeln!(out, "  test:     {test}");
        if let Some(database) = database {
            let _ = writeln!(out, "  database: {database}");
        }
        let _ = writeln!(
            out,
            "  log:      block {} tx {} log_index {}",
            log.block_number, log.transaction_hash, log.log_index
        );
        let _ = writeln!(out, "  emitter:  {}", log.emitting_address);
        let _ = writeln!(out, "  topic0:   {topic0}");
        let _ = writeln!(out, "  why:      {}", why(&item.why));
        let accept = if database.is_some() {
            "allow_unmatched_in(pool, &[(\"<topic0, signature or emitter>\", \"<reason>\")]).await"
        } else {
            "allow_unmatched(&[(\"<topic0, signature or emitter>\", \"<reason>\")])"
        };
        let _ = writeln!(out, "  to accept on purpose: {accept}");
    }
    out
}

fn why(why: &Why) -> String {
    match why {
        Why::NoTopic0 => "the log has no topics".to_owned(),
        Why::NoAdmission { declared_by } if declared_by.is_empty() => {
            "no admission covers the emitter at this block, and no manifest in the batch declares \
             this topic0"
                .to_owned()
        }
        Why::NoAdmission { declared_by } => format!(
            "no admission covers the emitter at this block; the topic0 is declared as {}",
            events(declared_by)
        ),
        Why::NoTopic { admitted, declared } => format!(
            "no topic: the emitter is admitted as {}, and those manifests declare {}, not this \
             topic0. A renamed sol! struct or a missing retopic encodes a topic0 the manifest \
             never declares",
            admissions(admitted),
            events(declared)
        ),
        Why::RoleMismatch { admitted, declared } => format!(
            "role mismatch: the topic0 is declared as {}, and the emitter is admitted as {}",
            events(declared),
            admissions(admitted)
        ),
        Why::Refused { admitted } => format!(
            "refused: the emitter is admitted as {}, and a PublicResolverV2 admission refuses an \
             event outside the public resolver interface",
            admissions(admitted)
        ),
        Why::PreAdmissionUnrecorded { admitted_at } => format!(
            "the emitter is admitted by discovery at block {admitted_at}, after this log, but no \
             pre-admission decode skip was recorded"
        ),
    }
}

fn admissions(admitted: &[Admitted]) -> String {
    admitted
        .iter()
        .map(|admission| {
            let role = admission.role.as_deref().unwrap_or("none");
            match &admission.discovery_edge_kind {
                Some(kind) => format!(
                    "role {role} via a {kind} edge of manifest {} ({})",
                    admission.manifest_id, admission.source_family
                ),
                None => format!(
                    "role {role} by manifest {} ({})",
                    admission.manifest_id, admission.source_family
                ),
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn events(declared: &[DeclaredEvent]) -> String {
    if declared.is_empty() {
        return "no events".to_owned();
    }
    declared
        .iter()
        .map(|event| {
            let roles = if event.emitter_roles.is_empty() {
                "any role".to_owned()
            } else {
                format!("roles [{}]", event.emitter_roles.join(", "))
            };
            format!(
                "{} {} in manifest {} ({}) for {roles}",
                event.signature, event.topic0, event.manifest_id, event.source_family
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}
