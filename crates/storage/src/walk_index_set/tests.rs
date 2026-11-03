use std::collections::BTreeSet;

const DROP_SQL: &str = include_str!("../../../../ops/walk-index-set/drop.sql");
const INSTALL_SQL: &str = include_str!("../../../../ops/walk-index-set/install.sql");
const README: &str = include_str!("../../../../ops/walk-index-set/README.md");
const STORAGE_MD: &str = include_str!("../../../../docs/storage.md");
const DEPLOYMENT_MD: &str = include_str!("../../../../docs/deployment.md");

/// The indexes Interpret keeps, as `docs/storage.md` § Walk index set counts them.
const KEPT: usize = 17;

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

fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Every list in the two scripts names the same indexes once each.
#[test]
fn scripts_and_module_agree() {
    let dropped = dropped();
    let names = set(&dropped);
    assert_eq!(names.len(), dropped.len(), "drop.sql drops a name twice");
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
    let dropped = dropped().len();
    let total = KEPT + dropped;
    for (file, text, sentence) in [
        (
            "ops/walk-index-set/README.md",
            README,
            format!("is the {KEPT} indexes Interpret keeps. `drop.sql` drops the other {dropped} "),
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
            format!("The {KEPT} kept indexes"),
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
                 read, so Interpret maintains {KEPT} indexes on the table instead of {total},"
            ),
        ),
    ] {
        assert!(
            one_line(text).contains(&sentence),
            "{file} must say {sentence:?}"
        );
    }
}
