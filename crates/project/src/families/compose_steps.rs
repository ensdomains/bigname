//! The cursor of a pass stream built in steps of partitions, as the catalogue build keeps it
//! (history_catalogue/build): the last partition committed, with the split its keys were hashed
//! under and the server version that hashed them. Key partitions are
//! `hashtext(key) & (partitions - 1)` and `hashtext` is stable within one Postgres server
//! version, so a stream resumed under another split or server version is refused. A stream's last
//! partition moves the pass to the next stream, so a cursor never names it.
use serde_json::{Value, json};
use sqlx::{Postgres, Transaction};

use crate::{
    ProjectError, Result,
    families::{
        MAX_COMPOSE_BUILDER_BLOCK_SPANS, MAX_COMPOSE_BUILDER_KEY_PARTITIONS,
        history_catalogue::build::Layout,
    },
};

/// How a step splits: into the layout's block spans or its key partitions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Parts {
    Spans,
    Partitions,
}

/// A stream's steps in the order it runs them, each with its name and split.
pub(crate) type Steps = &'static [(&'static str, Parts)];

/// One partition of a stream's step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Step {
    pub(crate) index: usize,
    pub(crate) part: u16,
    pub(crate) layout: Layout,
    pub(crate) server_version_num: i32,
}

impl Step {
    pub(crate) fn name(&self, steps: Steps) -> &'static str {
        steps[self.index].0
    }

    /// Whether this is the stream's last partition, whose commit moves the pass to the next
    /// stream.
    pub(crate) fn last(&self, steps: Steps) -> bool {
        self.index + 1 == steps.len() && self.part + 1 >= parts(steps, self.index, self.layout)
    }

    pub(crate) fn text(&self, steps: Steps) -> String {
        json!({"step": self.name(steps), "part": self.part, "spans": self.layout.spans,
               "partitions": self.layout.partitions,
               "server_version_num": self.server_version_num})
        .to_string()
    }

    pub(crate) fn mask(&self) -> i32 {
        i32::from(self.layout.partitions) - 1
    }
}

fn parts(steps: Steps, index: usize, layout: Layout) -> u16 {
    match steps[index].1 {
        Parts::Spans => layout.spans,
        Parts::Partitions => layout.partitions,
    }
}

fn parse(text: &str, steps: Steps, what: &str) -> Result<Step> {
    let invalid = || ProjectError::data_integrity(format!("{what} cursor {text}"));
    let value: Value = serde_json::from_str(text).map_err(|_| invalid())?;
    let number = |field: &str| {
        value[field]
            .as_i64()
            .and_then(|number| u16::try_from(number).ok())
            .ok_or_else(invalid)
    };
    let index = steps
        .iter()
        .position(|(name, _)| Some(*name) == value["step"].as_str())
        .ok_or_else(invalid)?;
    Ok(Step {
        index,
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

/// The partition of `what` after the one `cursor` names, the first with no cursor. A stream the
/// cursor says was split otherwise, or on another server version, is refused, and a first
/// partition is refused for a split past the bounds, before anything is written.
pub(crate) fn next(
    cursor: Option<&str>,
    steps: Steps,
    layout: Layout,
    server_version_num: i32,
    what: &str,
) -> Result<Step> {
    let Some(text) = cursor else {
        let spans = 1..=MAX_COMPOSE_BUILDER_BLOCK_SPANS;
        let partitions = 1..=MAX_COMPOSE_BUILDER_KEY_PARTITIONS;
        if !spans.contains(&layout.spans)
            || !partitions.contains(&layout.partitions)
            || !layout.partitions.is_power_of_two()
        {
            return Err(ProjectError::data_integrity(format!(
                "the {what} cannot split as (spans, partitions) = ({}, {}): spans run from 1 to \
                 {MAX_COMPOSE_BUILDER_BLOCK_SPANS} and partitions are a power of two from 1 to \
                 {MAX_COMPOSE_BUILDER_KEY_PARTITIONS}",
                layout.spans, layout.partitions
            )));
        }
        return Ok(Step {
            index: 0,
            part: 0,
            layout,
            server_version_num,
        });
    };
    let last = parse(text, steps, what)?;
    if last.layout != layout || last.server_version_num != server_version_num {
        return Err(ProjectError::data_integrity(format!(
            "the {what} committed partitions as (spans, partitions) = ({}, {}) on server version \
             {}, and this run has ({}, {}) on server version {}",
            last.layout.spans,
            last.layout.partitions,
            last.server_version_num,
            layout.spans,
            layout.partitions,
            server_version_num
        )));
    }
    if last.last(steps) {
        return Err(ProjectError::data_integrity(format!(
            "the {what} cursor {text} names the stream's last partition"
        )));
    }
    if last.part + 1 < parts(steps, last.index, layout) {
        Ok(Step {
            part: last.part + 1,
            ..last
        })
    } else {
        Ok(Step {
            index: last.index + 1,
            part: 0,
            ..last
        })
    }
}

/// Set `work_mem` for the rest of `transaction`, when the run configures one.
pub(crate) async fn set_work_mem(
    transaction: &mut Transaction<'_, Postgres>,
    work_mem: Option<&str>,
) -> Result<()> {
    if let Some(work_mem) = work_mem {
        sqlx::query(
            "/* project:families.compose.work_mem */ SELECT set_config('work_mem', $1, true)",
        )
        .bind(work_mem)
        .execute(&mut **transaction)
        .await
        .map_err(|error| ProjectError::database("failed to set a pass step's work_mem", error))?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "compose_steps_tests.rs"]
mod compose_steps_tests;
