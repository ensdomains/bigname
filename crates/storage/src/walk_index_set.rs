//! The `normalized_events` indexes `ops/walk-index-set/drop.sql` drops for a from-zero walk or
//! a full-history Interpret redo (`docs/storage.md` § Walk index set), and one catalog read of
//! their state. The phase runner rebuilds them before Project runs. The advisory indexes are
//! outside that set: an operator builds them by hand, the runner never builds them, and the API
//! serves without them. The `project_grant` advisory indexes follow the same rule on that table,
//! which no walk drops.
use sqlx::{Connection, PgConnection};

mod constants;
pub use constants::{ADVISORY, DROP_LIST, KEPT, PROJECT_GRANT_ADVISORY};

pub const TABLE: &str = "bigname_phase.normalized_events";
pub const PROJECT_GRANT_TABLE: &str = "bigname_phase.project_grant";

/// One index `drop.sql` drops, or one advisory index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DroppedIndex {
    pub name: &'static str,
    /// The statement `ops/walk-index-set/install.sql` builds the index with, or for an advisory
    /// index the statement of its own install script.
    pub create: &'static str,
    /// The definition `install.sql` checks, as `pg_get_indexdef` prints it with `search_path`
    /// set to `pg_catalog` and `quote_all_identifiers` off.
    pub definition: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexState {
    /// A valid, ready index on the table, with the definition PostgreSQL prints for it.
    Present {
        definition: String,
    },
    Missing,
    /// An index on the table that is not valid or not ready, such as the leftover of an
    /// interrupted concurrent build.
    Invalid,
    /// Another relation holds the name: `held_by` is, for example, "a table" or "an index on
    /// bigname_phase.chain_lineage".
    Foreign {
        held_by: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexStatus {
    pub index: &'static DroppedIndex,
    pub state: IndexState,
}

impl IndexStatus {
    /// The definition found, when the index is in place with a definition other than the one
    /// `install.sql` checks.
    pub fn other_definition(&self) -> Option<&str> {
        match &self.state {
            IndexState::Present { definition } if definition != self.index.definition => {
                Some(definition)
            }
            _ => None,
        }
    }
}

const STATE: &str = "
SELECT relation.relkind::text,
       ix.indrelid = to_regclass($2),
       ix.indisvalid AND ix.indisready AND ix.indislive,
       CASE WHEN relation.relkind = 'i' THEN pg_get_indexdef(relation.oid) END,
       ix.indrelid::regclass::text
FROM unnest($1::text[]) WITH ORDINALITY AS listed(name, position)
LEFT JOIN pg_class relation ON relation.oid = to_regclass('bigname_phase.' || listed.name)
LEFT JOIN pg_index ix ON ix.indexrelid = relation.oid
ORDER BY listed.position";

type StateRow = (
    Option<String>,
    Option<bool>,
    Option<bool>,
    Option<String>,
    Option<String>,
);

/// The state of every index in [`DROP_LIST`], in its order, read in one transaction that
/// writes nothing, or `None` when [`TABLE`] does not exist. Definitions print as `install.sql`
/// checks them, whatever the session's `search_path`. It takes a connection rather than any
/// `Acquire`, so a caller's future stays `Send`.
pub async fn load_drop_list_state(
    connection: &mut PgConnection,
) -> Result<Option<Vec<IndexStatus>>, sqlx::Error> {
    load_state(connection, DROP_LIST, TABLE).await
}

/// The state of every index in [`ADVISORY`], read as [`load_drop_list_state`] reads the drop
/// list.
pub async fn load_advisory_state(
    connection: &mut PgConnection,
) -> Result<Option<Vec<IndexStatus>>, sqlx::Error> {
    load_state(connection, ADVISORY, TABLE).await
}

/// The state of every index in [`PROJECT_GRANT_ADVISORY`], read as [`load_drop_list_state`]
/// reads the drop list, or `None` when [`PROJECT_GRANT_TABLE`] does not exist.
pub async fn load_project_grant_advisory_state(
    connection: &mut PgConnection,
) -> Result<Option<Vec<IndexStatus>>, sqlx::Error> {
    load_state(connection, PROJECT_GRANT_ADVISORY, PROJECT_GRANT_TABLE).await
}

async fn load_state(
    connection: &mut PgConnection,
    listed: &'static [DroppedIndex],
    table: &str,
) -> Result<Option<Vec<IndexStatus>>, sqlx::Error> {
    let mut transaction = connection.begin().await?;
    sqlx::query(
        "SELECT set_config('search_path', 'pg_catalog', true),
                set_config('quote_all_identifiers', 'off', true)",
    )
    .execute(&mut *transaction)
    .await?;
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(table)
        .fetch_one(&mut *transaction)
        .await?;
    if !exists {
        transaction.commit().await?;
        return Ok(None);
    }
    let names: Vec<&str> = listed.iter().map(|index| index.name).collect();
    let rows: Vec<StateRow> = sqlx::query_as(STATE)
        .bind(names)
        .bind(table)
        .fetch_all(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(Some(
        listed
            .iter()
            .zip(rows)
            .map(|(index, row)| IndexStatus {
                index,
                state: state(row),
            })
            .collect(),
    ))
}

fn state((kind, on_table, usable, definition, indexed_table): StateRow) -> IndexState {
    let Some(kind) = kind else {
        return IndexState::Missing;
    };
    if kind != "i" {
        return IndexState::Foreign {
            held_by: relation_kind(&kind),
        };
    }
    if on_table != Some(true) {
        return IndexState::Foreign {
            held_by: format!(
                "an index on {}",
                indexed_table.as_deref().unwrap_or("another table")
            ),
        };
    }
    match (usable, definition) {
        (Some(true), Some(definition)) => IndexState::Present { definition },
        _ => IndexState::Invalid,
    }
}

/// The relation kinds `install.sql` names.
fn relation_kind(relkind: &str) -> String {
    match relkind {
        "I" => "a partitioned index".to_owned(),
        "r" => "a table".to_owned(),
        "p" => "a partitioned table".to_owned(),
        "v" => "a view".to_owned(),
        "m" => "a materialized view".to_owned(),
        "S" => "a sequence".to_owned(),
        "f" => "a foreign table".to_owned(),
        "c" => "a composite type".to_owned(),
        other => format!("a relation of kind {other}"),
    }
}

#[cfg(test)]
mod tests;
