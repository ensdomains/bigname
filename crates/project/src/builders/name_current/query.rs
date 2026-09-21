pub(in crate::builders) const BUILD_NAME_CURRENT: &str =
    concat!(include_str!("build.sql"), "        ");

pub(in crate::builders) const STAGE_V2_LIFECYCLE_EVENTS: [&str; 4] = [
    include_str!("v2_lifecycle_events.sql"),
    "ALTER TABLE project_v2_lifecycle_events ADD PRIMARY KEY (normalized_event_id)",
    "CREATE INDEX ON project_v2_lifecycle_events (logical_name_id, lifecycle_key)",
    "ANALYZE project_v2_lifecycle_events",
];
