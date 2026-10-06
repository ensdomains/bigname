//! Physical pointer events shared by the path reader and Project's dependency walk.
//!
//! Canonical-name retirement leaves physical pointer storage intact. This predicate is
//! deliberately limited to the exact synthetic retirement tuple; missing metadata and real
//! explicit zero or registration-reset events remain candidates. The caller aliases the
//! normalized event row as `event` and applies chain/resource/publication bounds separately.
pub const PHYSICAL_POINTER_EVENT_SQL: &str = "(
    event.after_state ->> 'source_event' = 'RegistryPathExpired'
    AND event.after_state ->> 'derived_from' = 'interpreter_state'
    AND event.after_state ->> 'terminal_reason' = 'registry_name_binding_expired'
) IS NOT TRUE";
