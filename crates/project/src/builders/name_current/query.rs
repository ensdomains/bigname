pub(in crate::builders) const BUILD_NAME_CURRENT: &str =
    concat!(include_str!("build.sql"), "        ");

pub(in crate::builders) const STAGE_V2_LIFECYCLE_EVENTS: [&str; 4] = [
    include_str!("v2_lifecycle_events.sql"),
    "/* project:builders.name_current.query.key_v2_lifecycle_events */ ALTER TABLE project_v2_lifecycle_events ADD PRIMARY KEY (normalized_event_id)",
    "/* project:builders.name_current.query.index_v2_lifecycle_events_logical_name_id */ CREATE INDEX ON project_v2_lifecycle_events (logical_name_id, lifecycle_key)",
    "/* project:builders.name_current.query.analyze_v2_lifecycle_events */ ANALYZE project_v2_lifecycle_events",
];
