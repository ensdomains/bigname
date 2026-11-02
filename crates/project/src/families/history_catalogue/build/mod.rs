//! The last stream of a rebuild's composition pass: the address history catalogue, built once at
//! the pass block from set-based statements (docs/projections.md, "Owned key families"). The
//! names stream left every listed name marked composed, with the current relations of each
//! composed name on its work row. The build writes `project_history_source`,
//! `project_history_source_edge` and `project_address_history_anchor` in steps, each split into
//! partitions, and each partition is one transaction the driver fenced, which deletes its own
//! range and inserts it again:
//!
//! 1. `keys`: the names and resources the work list reaches (spans.rs).
//! 2. `spans`: one scan of the events per block span, each key's partial aggregate (spans.rs).
//! 3. `edges`: resolver source discovery per resource partition (edges.rs).
//! 4. `sources`: each source key's partials merged, per key partition (sources.rs).
//! 5. `edge_bounds`: the edges' bounds from their finished sources (edges.rs).
//! 6. `name_anchors` and 7. `resource_anchors`: the address memberships (anchors.rs).
//! 8. `done`.
//!
//! A step reads the rows of earlier steps only once every partition of them committed, and the
//! repair record's cursor, never a composed row, says how far the build got. Key partitions are
//! `hashtext(key) & (partitions - 1)`. `hashtext` is stable within one Postgres server version,
//! so the cursor records the partition counts and the server version, and a build resumed under
//! others is refused.
mod anchors;
mod edges;
mod sources;
mod spans;

use bigname_storage::families::records::CurrentHistoryRelation;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::{
    ProjectError, Result,
    families::{MAX_COMPOSE_BUILDER_BLOCK_SPANS, MAX_COMPOSE_BUILDER_KEY_PARTITIONS},
};

/// The pass-scoped work table of the build, cleared with the name work list.
pub(crate) const WORK: &str = "project_compose_history_work";

/// One current relation of a composed name as the names stream persists it on the name's work
/// row, the fields the catalogue reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PersistedRelation {
    pub(crate) address: String,
    pub(crate) namespace: String,
    pub(crate) resource_id: Uuid,
    pub(crate) relation: String,
}

impl PersistedRelation {
    pub(crate) fn of(relation: &CurrentHistoryRelation) -> Self {
        Self {
            address: relation.address.clone(),
            namespace: relation.namespace.clone(),
            resource_id: relation.resource_id,
            relation: relation.relation.to_owned(),
        }
    }

    pub(crate) fn to_json(&self) -> Value {
        json!({"address": self.address, "namespace": self.namespace,
               "resource_id": self.resource_id, "relation": self.relation})
    }

    #[cfg(test)]
    pub(crate) fn from_json(value: &Value) -> Option<Self> {
        Some(Self {
            address: value["address"].as_str()?.to_owned(),
            namespace: value["namespace"].as_str()?.to_owned(),
            resource_id: value["resource_id"].as_str()?.parse().ok()?,
            relation: value["relation"].as_str()?.to_owned(),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Step {
    Keys,
    Spans,
    Edges,
    Sources,
    EdgeBounds,
    NameAnchors,
    ResourceAnchors,
    Done,
}

/// Every step, in the order the build runs them. A cursor's step name parses back through it.
const STEPS: [Step; 8] = [
    Step::Keys,
    Step::Spans,
    Step::Edges,
    Step::Sources,
    Step::EdgeBounds,
    Step::NameAnchors,
    Step::ResourceAnchors,
    Step::Done,
];

impl Step {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Keys => "keys",
            Self::Spans => "spans",
            Self::Edges => "edges",
            Self::Sources => "sources",
            Self::EdgeBounds => "edge_bounds",
            Self::NameAnchors => "name_anchors",
            Self::ResourceAnchors => "resource_anchors",
            Self::Done => "done",
        }
    }

    fn parts(self, layout: Layout) -> u16 {
        match self {
            Self::Keys | Self::Done => 1,
            Self::Spans => layout.spans,
            _ => layout.partitions,
        }
    }

    fn after(self) -> Self {
        match self {
            Self::Keys => Self::Spans,
            Self::Spans => Self::Edges,
            Self::Edges => Self::Sources,
            Self::Sources => Self::EdgeBounds,
            Self::EdgeBounds => Self::NameAnchors,
            Self::NameAnchors => Self::ResourceAnchors,
            Self::ResourceAnchors | Self::Done => Self::Done,
        }
    }
}

/// How the build splits its work: block spans of the span scan and key partitions of the keyed
/// steps, a power of two.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Layout {
    pub(crate) spans: u16,
    pub(crate) partitions: u16,
}

/// The last partition the build committed, as the repair record's `compose_cursor` holds it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Cursor {
    pub(crate) step: Step,
    pub(crate) part: u16,
    pub(crate) layout: Layout,
    pub(crate) server_version_num: i32,
}

impl Cursor {
    pub(crate) fn text(&self) -> String {
        json!({"step": self.step.name(), "part": self.part, "spans": self.layout.spans,
               "partitions": self.layout.partitions,
               "server_version_num": self.server_version_num})
        .to_string()
    }

    fn parse(text: &str) -> Result<Self> {
        let invalid = || ProjectError::data_integrity(format!("catalogue build cursor {text}"));
        let value: Value = serde_json::from_str(text).map_err(|_| invalid())?;
        let number = |field: &str| {
            value[field]
                .as_i64()
                .and_then(|number| u16::try_from(number).ok())
                .ok_or_else(invalid)
        };
        let step = STEPS
            .into_iter()
            .find(|step| Some(step.name()) == value["step"].as_str())
            .ok_or_else(invalid)?;
        Ok(Self {
            step,
            part: number("part")?,
            layout: Layout {
                spans: number("spans")?,
                partitions: number("partitions")?,
            },
            server_version_num: value["server_version_num"]
                .as_i64()
                .and_then(|version| i32::try_from(version).ok())
                .ok_or_else(invalid)?,
        })
    }

    fn mask(&self) -> i32 {
        i32::from(self.layout.partitions) - 1
    }
}

/// Whether the cursor of the build's stream says the build ended.
pub(crate) fn is_done(cursor: Option<&str>) -> Result<bool> {
    cursor.map_or(Ok(false), |text| {
        Cursor::parse(text).map(|cursor| cursor.step == Step::Done)
    })
}

/// The partition after the one `cursor` names, the first with no cursor. A build the cursor
/// says was split otherwise, or on another server version, is refused: its committed partitions
/// were hashed so. A first partition is refused for a split past the bounds, before any partial
/// is written.
pub(crate) fn next(
    cursor: Option<&str>,
    layout: Layout,
    server_version_num: i32,
) -> Result<Cursor> {
    let Some(text) = cursor else {
        let spans = 1..=MAX_COMPOSE_BUILDER_BLOCK_SPANS;
        let partitions = 1..=MAX_COMPOSE_BUILDER_KEY_PARTITIONS;
        if !spans.contains(&layout.spans)
            || !partitions.contains(&layout.partitions)
            || !layout.partitions.is_power_of_two()
        {
            return Err(ProjectError::data_integrity(format!(
                "the catalogue build cannot split as (spans, partitions) = ({}, {}): spans run \
                 from 1 to {MAX_COMPOSE_BUILDER_BLOCK_SPANS} and partitions are a power of two \
                 from 1 to {MAX_COMPOSE_BUILDER_KEY_PARTITIONS}",
                layout.spans, layout.partitions
            )));
        }
        return Ok(Cursor {
            step: Step::Keys,
            part: 0,
            layout,
            server_version_num,
        });
    };
    let last = Cursor::parse(text)?;
    if last.layout != layout || last.server_version_num != server_version_num {
        return Err(ProjectError::data_integrity(format!(
            "the catalogue build committed partitions as (spans, partitions) = ({}, {}) on server \
             version {}, and this run has ({}, {}) on server version {}",
            last.layout.spans,
            last.layout.partitions,
            last.server_version_num,
            layout.spans,
            layout.partitions,
            server_version_num
        )));
    }
    let (step, part) = if last.part + 1 < last.step.parts(layout) {
        (last.step, last.part + 1)
    } else {
        (last.step.after(), 0)
    };
    Ok(Cursor { step, part, ..last })
}

pub(crate) async fn server_version(pool: &PgPool) -> Result<i32> {
    let version: String = sqlx::query_scalar(
        "/* project:history.build.server_version */ SELECT current_setting('server_version_num')",
    )
    .fetch_one(pool)
    .await
    .map_err(|error| ProjectError::database("failed to read the server version", error))?;
    version.parse().map_err(|error| {
        ProjectError::data_integrity(format!("server_version_num {version}: {error}"))
    })
}

/// Run the partition `cursor` names inside the fenced `transaction`, after the gate: no name of
/// the work list is left uncomposed. Returns the table it wrote and the rows it wrote there.
pub(crate) async fn run(
    transaction: &mut Transaction<'_, Postgres>,
    chain: &str,
    pass: i64,
    cursor: &Cursor,
    work_mem: Option<&str>,
) -> Result<(&'static str, u64)> {
    if let Some(work_mem) = work_mem {
        sqlx::query("/* project:history.build.work_mem */ SELECT set_config('work_mem', $1, true)")
            .bind(work_mem)
            .execute(&mut **transaction)
            .await
            .map_err(|error| ProjectError::database("failed to set the build's work_mem", error))?;
    }
    crate::families::compose::require_composed(transaction, chain, "catalogue build").await?;
    let (part, mask) = (i32::from(cursor.part), cursor.mask());
    match cursor.step {
        Step::Keys => spans::keys(transaction, chain, mask).await,
        Step::Spans => {
            spans::span(transaction, chain, pass, cursor.part, cursor.layout, mask).await
        }
        Step::Edges => edges::discover(transaction, chain, pass, part, mask).await,
        Step::Sources => sources::merge(transaction, chain, part, mask).await,
        Step::EdgeBounds => edges::bounds(transaction, chain, part, mask).await,
        Step::NameAnchors => anchors::names(transaction, chain, pass, part, mask).await,
        Step::ResourceAnchors => anchors::resources(transaction, chain, pass, part, mask).await,
        Step::Done => Ok((WORK, 0)),
    }
}

/// The tables whose planner statistics the build refreshes once `cursor`'s partition committed:
/// a step's output, after its last partition, before the next step reads it.
pub(crate) fn analyzed(cursor: &Cursor) -> &'static [&'static str] {
    if cursor.part + 1 < cursor.step.parts(cursor.layout) {
        return &[];
    }
    match cursor.step {
        Step::Spans => &[WORK],
        Step::Edges | Step::EdgeBounds => &["project_history_source_edge"],
        Step::Sources => &["project_history_source"],
        Step::ResourceAnchors => &["project_address_history_anchor"],
        _ => &[],
    }
}

fn database(what: &'static str) -> impl FnOnce(sqlx::Error) -> ProjectError {
    move |error| ProjectError::database(what, error)
}

#[cfg(test)]
#[path = "build_tests.rs"]
mod build_tests;
