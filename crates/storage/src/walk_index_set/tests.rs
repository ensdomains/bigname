use std::collections::BTreeSet;

use super::{ADVISORY, DROP_LIST, KEPT};

const DROP_SQL: &str = include_str!("../../../../ops/walk-index-set/drop.sql");
const INSTALL_SQL: &str = include_str!("../../../../ops/walk-index-set/install.sql");
const README: &str = include_str!("../../../../ops/walk-index-set/README.md");
const STORAGE_MD: &str = include_str!("../../../../docs/storage.md");
const DEPLOYMENT_MD: &str = include_str!("../../../../docs/deployment.md");
const RECORD_HISTORY_INSTALL_SQL: &str =
    include_str!("../../../../ops/record-history-indexes/install.sql");

/// The index names of every line of `script` that starts with `statement`, in script order.
fn listed(script: &str, statement: &str) -> Vec<String> {
    script
        .lines()
        .filter_map(|line| line.trim().strip_prefix(statement))
        .map(|rest| {
            rest.trim_start_matches("bigname_phase.")
                .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .next()
                .expect("index name")
                .to_owned()
        })
        .collect()
}

fn dropped() -> Vec<String> {
    listed(DROP_SQL, "DROP INDEX CONCURRENTLY IF EXISTS ")
}

fn set(names: &[String]) -> BTreeSet<String> {
    names.iter().cloned().collect()
}

/// `drop.sql`'s checked array, one quoted name per line.
fn drop_checked() -> Vec<String> {
    DROP_SQL
        .lines()
        .filter_map(|line| line.trim().strip_prefix('\''))
        .filter(|rest| rest.starts_with("normalized_events_"))
        .map(|rest| rest.trim_end_matches(['\'', ',']).to_owned())
        .collect()
}

/// `install.sql`'s reviewed-definition rows, by name.
fn install_checked() -> Vec<String> {
    INSTALL_SQL
        .split("('")
        .skip(1)
        .filter_map(|rest| rest.split_once("',"))
        .map(|(name, _)| name.to_owned())
        .filter(|name| name.starts_with("normalized_events_"))
        .collect()
}

/// `install.sql`'s receipt, which prints the flags and size of every index it builds.
fn install_receipt() -> Vec<String> {
    listed(INSTALL_SQL, "to_regclass('bigname_phase.")
}

/// `install.sql`'s build statements by name, each without its closing semicolon.
fn install_statements() -> Vec<(String, String)> {
    let mut statements = Vec::new();
    let mut lines = INSTALL_SQL.lines();
    while let Some(line) = lines.next() {
        let Some(rest) = line.strip_prefix("CREATE INDEX CONCURRENTLY IF NOT EXISTS ") else {
            continue;
        };
        let name = rest
            .split_whitespace()
            .next()
            .expect("index name")
            .to_owned();
        let mut statement = line.to_owned();
        while !statement.ends_with(';') {
            statement.push('\n');
            statement.push_str(lines.next().expect("statement end"));
        }
        statement.pop();
        statements.push((name, statement));
    }
    statements
}

/// `install.sql`'s reviewed definition of `name`.
fn install_definition(name: &str) -> &'static str {
    let (_, rest) = INSTALL_SQL
        .split_once(&format!("('{name}',"))
        .unwrap_or_else(|| panic!("install.sql checks no definition of {name}"));
    let (_, rest) = rest.split_once("$def$").expect("definition start");
    rest.split_once("$def$").expect("definition end").0
}

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Every list in the two scripts names the same indexes once each, and the drop list holds
/// `drop.sql`'s order, `install.sql`'s statements and its reviewed definitions.
#[test]
fn scripts_and_module_agree() {
    let dropped = dropped();
    let names = set(&dropped);
    assert_eq!(names.len(), dropped.len(), "drop.sql drops a name twice");
    assert_eq!(
        DROP_LIST.iter().map(|index| index.name).collect::<Vec<_>>(),
        dropped,
        "DROP_LIST must list what drop.sql drops, in its order"
    );
    let statements = install_statements();
    for index in DROP_LIST {
        let built: Vec<_> = statements
            .iter()
            .filter(|(name, _)| name == index.name)
            .map(|(_, statement)| statement.as_str())
            .collect();
        assert_eq!(
            built,
            [index.create],
            "{}: install.sql's statement",
            index.name
        );
        assert_eq!(
            index.definition,
            install_definition(index.name),
            "{}: install.sql's reviewed definition",
            index.name
        );
    }
    let kept: BTreeSet<&str> = KEPT.iter().copied().collect();
    assert_eq!(kept.len(), KEPT.len(), "KEPT repeats a name");
    assert!(
        DROP_LIST.iter().all(|index| !kept.contains(index.name)),
        "an index is both kept and dropped"
    );
    for (what, listed) in [
        ("drop.sql's checked names", drop_checked()),
        (
            "install.sql's builds",
            listed(INSTALL_SQL, "CREATE INDEX CONCURRENTLY IF NOT EXISTS "),
        ),
        ("install.sql's reviewed definitions", install_checked()),
        ("install.sql's receipt", install_receipt()),
    ] {
        assert_eq!(listed.len(), set(&listed).len(), "{what} repeat a name");
        assert_eq!(
            set(&listed),
            names,
            "{what} must be exactly what drop.sql drops; missing {:?}",
            names.difference(&set(&listed)).collect::<Vec<_>>()
        );
    }
}

/// The scripts' README, `docs/storage.md` and the `docs/deployment.md` entry that added the
/// scripts state how many indexes `drop.sql` drops and how many the table has without the drop.
#[test]
fn docs_state_the_drop_list_size() {
    let dropped = DROP_LIST.len();
    assert_eq!(dropped, self::dropped().len());
    let kept = KEPT.len();
    let total = kept + dropped;
    for (file, text, sentence) in [
        (
            "ops/walk-index-set/README.md",
            README,
            format!("is the {kept} indexes Interpret keeps. `drop.sql` drops the other {dropped} "),
        ),
        (
            "ops/walk-index-set/README.md",
            README,
            format!("Allow for all {dropped} drops"),
        ),
        (
            "ops/walk-index-set/drop.sql",
            DROP_SQL,
            format!("allow for all {dropped} before"),
        ),
        (
            "docs/storage.md",
            STORAGE_MD,
            format!("The {kept} kept indexes"),
        ),
        (
            "docs/storage.md",
            STORAGE_MD,
            format!("The other {dropped} serve only Project"),
        ),
        (
            "docs/deployment.md",
            DEPLOYMENT_MD,
            format!(
                "`drop.sql` drops the {dropped} `normalized_events` indexes Interpret does not \
                 read, so Interpret maintains {kept} indexes on the table instead of {total},"
            ),
        ),
    ] {
        assert!(
            one_line(text).contains(&sentence),
            "{file} must say {sentence:?}"
        );
    }
}

/// Every statement builds its own index on `normalized_events` concurrently, so a build never
/// blocks Interpret's writes and never runs inside a transaction.
#[test]
fn every_create_is_concurrent() {
    for index in DROP_LIST {
        let head = format!("CREATE INDEX CONCURRENTLY IF NOT EXISTS {}\n", index.name);
        assert!(index.create.starts_with(&head), "{}", index.name);
        assert!(
            one_line(index.create).contains(" ON bigname_phase.normalized_events "),
            "{}",
            index.name
        );
        assert!(!index.create.contains(';'), "{}", index.name);
        assert!(
            index.definition.starts_with(&format!(
                "CREATE INDEX {} ON bigname_phase.normalized_events ",
                index.name
            )),
            "{}",
            index.name
        );
    }
}

/// The advisory indexes are outside the walk index set: neither script names them and neither
/// list holds them. Each one's statement and definition are those of its own install script,
/// and `docs/storage.md` names it in the walk index set section.
#[test]
fn advisory_indexes_are_outside_the_walk_index_set() {
    let walk_scripts = format!("{DROP_SQL}{INSTALL_SQL}");
    let walk_section = STORAGE_MD
        .split_once("### Walk index set")
        .and_then(|(_, rest)| rest.split_once("\n## "))
        .map(|(section, _)| section)
        .expect("the walk index set section");
    for index in ADVISORY {
        assert!(!walk_scripts.contains(index.name), "{}", index.name);
        assert!(!KEPT.contains(&index.name), "{}", index.name);
        assert!(
            DROP_LIST.iter().all(|dropped| dropped.name != index.name),
            "{}",
            index.name
        );
        assert!(
            RECORD_HISTORY_INSTALL_SQL.contains(&format!("{};", index.create)),
            "{}: install.sql's statement",
            index.name
        );
        assert!(
            RECORD_HISTORY_INSTALL_SQL.contains(&format!("$def${}$def$", index.definition)),
            "{}: install.sql's reviewed definition",
            index.name
        );
        assert!(
            walk_section.contains(&format!("`{}`", index.name)),
            "docs/storage.md's walk index set section must name {}",
            index.name
        );
    }
}
