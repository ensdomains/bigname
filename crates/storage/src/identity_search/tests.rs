//! Page shapes of the search refresh. Each test replays the previous one-statement page query
//! on the same rows as its oracle, then checks what the refresh really sent to PostgreSQL.
//!
//! | Row | Shape | Test |
//! | --- | --- | --- |
//! | a | no labels, names over more than two pages | `names_over_two_pages_page_by_cursor_only` |
//! | b | labels only, no names | `labels_without_names_stage_their_matches_once` |
//! | c | a row matched by a name and by a label | `row_matched_by_name_and_label_is_refreshed_once` |
//! | d | fewer names than one page | `fewer_names_than_one_page_send_one_page_statement` |
//! | e | exactly one page | `exactly_one_page_ends_on_an_empty_page` |
//! | f | no names and no labels | `no_names_and_no_labels_send_nothing` |
//! | g | names without a `name_surfaces` row | `names_without_surfaces_neither_end_nor_skip_pages` |
//! | h | two transactions on one connection | `later_transactions_on_one_connection_stage_again` |
//! | i | prepare, source write, then refresh | `prepare_then_refresh_reads_the_uncommitted_write` |
//! | j | mixed fixture against the previous query | `mixed_fixture_matches_the_previous_query` |
//! | k | two refreshes in one transaction | `second_refresh_forgets_the_first_name_set` |
//! | l | more names than one staging statement | `names_beyond_one_staging_statement_are_bound_once` |
//! | m | staging rolled back to a savepoint | `refresh_after_a_savepoint_rollback_stages_again` |
//! | n | no open transaction | `refresh_outside_a_transaction_is_refused` |
//! | o | wrong isolation level | `repeatable_read_is_refused_before_staging` |
//! | p | role may not create temporary tables | `role_without_temporary_tables_is_refused` |
//! | q | table created in a transaction that rolls back | `table_created_in_a_rolled_back_transaction_is_created_again` |
//! | r | rows staged by an earlier commit or rollback | `later_refresh_never_reads_rows_staged_before` |
//! | s | 1,000 small transactions on one connection | `small_refreshes_after_the_first_write_no_catalog_row` |
//! | t | staged set at and over the statistics threshold | `names_beyond_one_staging_statement_are_bound_once` |
//! | u | the page statement is not kept prepared | `page_statement_is_not_kept_prepared` |
mod wire;

use super::{documents, prepare, refresh};
use anyhow::{Context, Result, ensure};
use bigname_test_support::{TestDatabase, TestDatabaseConfig};
use futures_util::FutureExt;
use sqlx::{
    ConnectOptions, Connection, PgConnection,
    postgres::{PgConnectOptions, PgSslMode},
};
use std::{
    panic::AssertUnwindSafe,
    time::{Duration, Instant},
};
use wire::{Sent, Wire};

const NORMALIZER: &str = "test-normalizer";
/// A cursor is one logical name ID: a namespace, a colon and a 32-byte hex hash.
const CURSOR_BYTES: usize = 128;
const SHARED: u64 = 0x00ab_cdef;
const ETH: u64 = 0x00ab_cdf0;

struct Fixture {
    db: TestDatabase,
    wire: Wire,
    options: PgConnectOptions,
}

impl Fixture {
    async fn create(name: &str) -> Result<Self> {
        let db = TestDatabase::create(TestDatabaseConfig::new(name)).await?;
        for source in [
            include_str!("../../schema/baseline/01_chain.sql"),
            include_str!("../../schema/baseline/03_identity.sql"),
            include_str!("../../schema/baseline/07_labels.sql"),
        ] {
            sqlx::raw_sql(source).execute(db.pool()).await?;
        }
        sqlx::query("INSERT INTO chain_lineage(chain_id,block_hash,block_number,block_timestamp,canonicality_state)
            VALUES ('search-test','block-1',1,to_timestamp(1),'canonical')").execute(db.pool()).await?;
        let direct = db.pool().connect_options();
        let wire = Wire::start(direct.get_host(), direct.get_port())?;
        let options = direct
            .as_ref()
            .clone()
            .host("127.0.0.1")
            .port(wire.port)
            .ssl_mode(PgSslMode::Disable);
        Ok(Self { db, wire, options })
    }

    /// One physical connection whose traffic the relay records.
    async fn connect(&self) -> Result<PgConnection> {
        Ok(self.options.connect().await?)
    }

    /// Surfaces without raw bytes, each under its own label and the shared label.
    async fn structural(&self, numbers: impl IntoIterator<Item = u64>) -> Result<()> {
        let numbers: Vec<i64> = numbers.into_iter().map(|n| n as i64).collect();
        sqlx::query("INSERT INTO name_surfaces(logical_name_id,namespace,namehash,labelhashes,
                normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state)
            SELECT 'ens:0x'||lpad(to_hex(n),64,'0'),'ens','0x'||lpad(to_hex(n),64,'0'),
                ARRAY['0x'||lpad(to_hex(n+1000000),64,'0'),$2],$3,'active','search-test','block-1',1,'canonical'
            FROM unnest($1::bigint[]) n")
            .bind(numbers).bind(label(SHARED)).bind(NORMALIZER).execute(self.db.pool()).await?;
        Ok(())
    }

    /// Surfaces with raw bytes, named `raw<n>.eth`. `shared` puts the shared label in the path.
    async fn raw(&self, numbers: impl IntoIterator<Item = u64>, shared: bool) -> Result<()> {
        let numbers: Vec<i64> = numbers.into_iter().map(|n| n as i64).collect();
        sqlx::query("INSERT INTO name_surfaces(logical_name_id,namespace,namehash,labelhashes,raw_name,
                raw_labels,dns_encoded_name,preimage_event_identity,
                normalizer_version,visibility_state,chain_id,block_hash,block_number,canonicality_state)
            SELECT 'ens:0x'||lpad(to_hex(n),64,'0'),'ens','0x'||lpad(to_hex(n),64,'0'),
                ARRAY['0x'||lpad(to_hex(n+1000000),64,'0'),$2],'raw'||n||'.eth',
                ARRAY['raw'||n,'eth'],convert_to('raw'||n||'.eth','UTF8'),'witness-'||n,
                $3,'active','search-test','block-1',1,'canonical'
            FROM unnest($1::bigint[]) n")
            .bind(numbers).bind(label(if shared { SHARED } else { ETH })).bind(NORMALIZER)
            .execute(self.db.pool()).await?;
        Ok(())
    }

    async fn documents(&self) -> Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT count(*) FROM name_search_documents")
                .fetch_one(self.db.pool())
                .await?,
        )
    }

    async fn spelling(&self, number: u64) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT name FROM name_search_documents WHERE logical_name_id=$1")
                .bind(id(number))
                .fetch_optional(self.db.pool())
                .await?,
        )
    }
}

fn id(number: u64) -> String {
    format!("ens:0x{number:064x}")
}

fn ids(numbers: impl IntoIterator<Item = u64>) -> Vec<String> {
    numbers.into_iter().map(id).collect()
}

fn label(number: u64) -> String {
    format!("0x{number:064x}")
}

/// The label of surface `number` that no other surface carries.
fn own_label(number: u64) -> String {
    label(number + 1_000_000)
}

async fn reveal(conn: &mut PgConnection, labelhash: &str, text: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO label_preimages(labelhash,raw_label,decoded_label,normalizer_version,
            normalized_under_version,source_kind,source_priority)
         VALUES ($1,convert_to($2,'UTF8'),$2,$3,true,'test',1)",
    )
    .bind(labelhash)
    .bind(text)
    .bind(NORMALIZER)
    .execute(conn)
    .await?;
    Ok(())
}

/// The page query this change replaces, kept verbatim as the oracle for every shape.
async fn previous_refresh(
    conn: &mut PgConnection,
    names: &[String],
    labels: &[String],
) -> Result<()> {
    if names.is_empty() && labels.is_empty() {
        return Ok(());
    }
    let labels: Vec<String> = labels.iter().map(|l| l.to_ascii_lowercase()).collect();
    let mut after = String::new();
    loop {
        let rows: Vec<documents::Source> = sqlx::query_as(&format!(
            "/* test:previous.spellings */
             SELECT surface.logical_name_id, surface.chain_id, surface.namespace, surface.namehash,
                    surface.raw_name, surface.visibility_state, {rendered} AS name
             FROM name_surfaces surface WHERE (logical_name_id=ANY($1) OR
                (raw_name IS NULL AND labelhashes && $2::text[])) AND logical_name_id > $3
             ORDER BY logical_name_id LIMIT 100",
            rendered = crate::families::name::rendered::rendered_name_sql_unqualified("surface"),
        ))
        .bind(names)
        .bind(&labels)
        .bind(&after)
        .fetch_all(&mut *conn)
        .await?;
        let Some(last) = rows.last() else {
            return Ok(());
        };
        after = last.logical_name_id.clone();
        documents::replace(conn, rows).await?;
    }
}

/// Documents and postings. `keys` keeps the generated document keys, which only two databases
/// that ran the same statements share.
async fn stored(conn: &mut PgConnection, keys: bool) -> Result<serde_json::Value> {
    Ok(sqlx::query_scalar(
        "SELECT jsonb_build_object(
            'documents',(SELECT jsonb_agg(CASE WHEN $1 THEN to_jsonb(d) ELSE to_jsonb(d)-'search_id' END
                ORDER BY logical_name_id) FROM name_search_documents d),
            'postings',(SELECT jsonb_agg(jsonb_build_array(d.logical_name_id,
                    CASE WHEN $1 THEN p.search_id END,p.namespace,p.spelling_class,p.token_kind,
                    p.token_length,encode(p.token_bytes,'hex'))
                ORDER BY d.logical_name_id,p.token_kind,p.token_length,p.token_bytes)
                FROM name_search_postings p JOIN name_search_documents d USING(search_id)))",
    )
    .bind(keys)
    .fetch_one(conn)
    .await?)
}

/// What one refresh sent: the statements before the first page, then each page's statements.
struct Trace {
    staging: Vec<Sent>,
    pages: Vec<Vec<Sent>>,
}

impl Trace {
    fn new(sent: Vec<Sent>) -> Self {
        let mut trace = Self {
            staging: Vec::new(),
            pages: Vec::new(),
        };
        for statement in sent {
            if statement.tag() == "spellings" {
                trace.pages.push(Vec::new());
            }
            match trace.pages.last_mut() {
                Some(page) => page.push(statement),
                None => trace.staging.push(statement),
            }
        }
        trace
    }

    fn staging_tags(&self) -> Vec<&str> {
        self.staging.iter().map(Sent::tag).collect()
    }

    fn page_tags(&self) -> Vec<Vec<&str>> {
        self.pages
            .iter()
            .map(|page| page.iter().map(Sent::tag).collect())
            .collect()
    }

    fn staged_bytes(&self, tag: &str) -> usize {
        self.staging
            .iter()
            .filter(|statement| statement.tag() == tag)
            .map(Sent::bound_bytes)
            .sum()
    }

    /// No page statement carries the name set or the labels: a page binds its cursor only.
    fn pages_bind_only_a_cursor(&self) -> Result<()> {
        for page in &self.pages {
            ensure!(
                page[0].parameters.len() == 1 && page[0].parameters[0] <= CURSOR_BYTES,
                "a page bound parameters of {:?} bytes",
                page[0].parameters
            );
            ensure!(
                page.iter()
                    .all(|statement| !statement.tag().contains("stage")),
                "a page staged names again: {:?}",
                page.iter().map(Sent::tag).collect::<Vec<_>>()
            );
        }
        Ok(())
    }
}

const NEW_DOCUMENTS: [&str; 4] = ["spellings", "documents", "remove_postings", "add_postings"];

/// A staged set small enough to need no statistics.
fn staging(names: bool, labels: bool) -> Vec<&'static str> {
    let mut tags = vec![
        "SHOW transaction_isolation",
        "names_table",
        "open_transaction",
    ];
    if names {
        tags.push("stage_names");
    }
    if labels {
        tags.push("stage_labels");
    }
    tags
}

fn payload(values: &[String]) -> usize {
    values.iter().map(String::len).sum()
}

/// Inside the caller's transaction: run the previous query under a savepoint, undo it, run
/// the refresh, and require the same documents and postings and cursor-only pages.
async fn refresh_like_previous(
    wire: &Wire,
    conn: &mut PgConnection,
    names: &[String],
    labels: &[String],
) -> Result<Trace> {
    sqlx::query("SAVEPOINT previous")
        .execute(&mut *conn)
        .await?;
    previous_refresh(conn, names, labels).await?;
    let expected = stored(conn, false).await?;
    sqlx::query("ROLLBACK TO SAVEPOINT previous")
        .execute(&mut *conn)
        .await?;
    wire.take();
    refresh(conn, names, labels).await?;
    let trace = Trace::new(wire.take());
    ensure!(
        stored(conn, false).await? == expected,
        "refresh and the previous query stored different documents or postings"
    );
    trace.pages_bind_only_a_cursor()?;
    Ok(trace)
}

#[tokio::test]
async fn names_over_two_pages_page_by_cursor_only() -> Result<()> {
    let fixture = Fixture::create("search_pages_names").await?;
    fixture.structural(1..=250).await?;
    let names = ids(1..=250);
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &names, &[]).await?;
    tx.commit().await?;
    ensure!(fixture.documents().await? == 250);
    ensure!(trace.staging_tags() == staging(true, false));
    // The name set crosses the wire once, before any page.
    let bound = trace.staged_bytes("stage_names");
    ensure!(
        bound >= payload(&names) && bound < payload(&names) * 2,
        "bound {bound}"
    );
    // Three pages, each the same four statements. A short last page needs no empty page after.
    ensure!(
        trace.page_tags() == vec![NEW_DOCUMENTS.to_vec(); 3],
        "{:?}",
        trace.page_tags()
    );
    fixture.db.cleanup().await
}

#[tokio::test]
async fn labels_without_names_stage_their_matches_once() -> Result<()> {
    let fixture = Fixture::create("search_pages_labels").await?;
    fixture.structural(1..=230).await?;
    // The shared label is also in the path of surfaces with raw bytes, which it cannot rename.
    fixture.raw(231..=240, true).await?;
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    reveal(&mut tx, &label(SHARED), "shared").await?;
    let labels = [label(SHARED).to_ascii_uppercase().replace("0X", "0x")];
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &[], &labels).await?;
    tx.commit().await?;
    ensure!(fixture.documents().await? == 230);
    ensure!(
        fixture.spelling(7).await?.as_deref()
            == Some(format!("[{}].shared", &own_label(7)[2..]).as_str())
    );
    ensure!(trace.staging_tags() == staging(false, true));
    ensure!(
        trace.page_tags() == vec![NEW_DOCUMENTS.to_vec(); 3],
        "{:?}",
        trace.page_tags()
    );
    fixture.db.cleanup().await
}

#[tokio::test]
async fn row_matched_by_name_and_label_is_refreshed_once() -> Result<()> {
    let fixture = Fixture::create("search_pages_both").await?;
    fixture.structural(1..=120).await?;
    fixture.raw(121..=180, false).await?;
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    reveal(&mut tx, &label(SHARED), "shared").await?;
    // Names 100..=150 overlap the label's matches 1..=120 on 100..=120.
    let names = ids(100..=150);
    let labels = [label(SHARED)];
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &names, &labels).await?;
    tx.commit().await?;
    ensure!(fixture.documents().await? == 150);
    ensure!(fixture.spelling(150).await?.as_deref() == Some("raw150.eth"));
    ensure!(fixture.spelling(151).await?.is_none());
    ensure!(trace.staging_tags() == staging(true, true));
    // 150 distinct rows are two pages. A doubled row would spill into a third.
    ensure!(
        trace.page_tags() == vec![NEW_DOCUMENTS.to_vec(); 2],
        "{:?}",
        trace.page_tags()
    );
    fixture.db.cleanup().await
}

#[tokio::test]
async fn fewer_names_than_one_page_send_one_page_statement() -> Result<()> {
    let fixture = Fixture::create("search_pages_short").await?;
    fixture.structural(1..=20).await?;
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &ids(1..=7), &[]).await?;
    tx.commit().await?;
    ensure!(fixture.documents().await? == 7);
    ensure!(trace.staging_tags() == staging(true, false));
    ensure!(trace.page_tags() == vec![NEW_DOCUMENTS.to_vec()]);
    fixture.db.cleanup().await
}

#[tokio::test]
async fn exactly_one_page_ends_on_an_empty_page() -> Result<()> {
    let fixture = Fixture::create("search_pages_exact").await?;
    fixture.structural(1..=120).await?;
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &ids(1..=100), &[]).await?;
    tx.commit().await?;
    ensure!(fixture.documents().await? == 100);
    ensure!(
        trace.page_tags() == vec![NEW_DOCUMENTS.to_vec(), vec!["spellings"]],
        "{:?}",
        trace.page_tags()
    );
    fixture.db.cleanup().await
}

#[tokio::test]
async fn no_names_and_no_labels_send_nothing() -> Result<()> {
    let fixture = Fixture::create("search_pages_empty").await?;
    fixture.structural(1..=3).await?;
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &[], &[]).await?;
    tx.commit().await?;
    ensure!(fixture.documents().await? == 0);
    ensure!(trace.staging_tags() == ["SHOW transaction_isolation"] && trace.pages.is_empty());
    fixture.db.cleanup().await
}

#[tokio::test]
async fn names_without_surfaces_neither_end_nor_skip_pages() -> Result<()> {
    let fixture = Fixture::create("search_pages_missing").await?;
    // 1..=150 and every even number after have no surface. A whole page of absent names
    // comes before the first stored one.
    fixture
        .structural((151..=400).filter(|number| number % 2 == 1))
        .await?;
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &ids(1..=400), &[]).await?;
    tx.commit().await?;
    ensure!(fixture.documents().await? == 125);
    ensure!(fixture.spelling(399).await?.is_some());
    // 400 staged names are four full pages and one empty page. The first page stores nothing.
    let mut expected = vec![vec!["spellings"]];
    expected.extend(vec![NEW_DOCUMENTS.to_vec(); 3]);
    expected.push(vec!["spellings"]);
    ensure!(trace.page_tags() == expected, "{:?}", trace.page_tags());
    fixture.db.cleanup().await
}

#[tokio::test]
async fn later_transactions_on_one_connection_stage_again() -> Result<()> {
    let fixture = Fixture::create("search_pages_reconnect").await?;
    fixture.structural(1..=260).await?;
    let mut conn = fixture.connect().await?;
    // The staging table is dropped at each commit and rollback. The connection's cached
    // statements must find the new one.
    let mut first = conn.begin().await?;
    refresh_like_previous(&fixture.wire, &mut first, &ids(1..=120), &[]).await?;
    first.commit().await?;
    ensure!(fixture.documents().await? == 120);
    let mut undone = conn.begin().await?;
    refresh_like_previous(&fixture.wire, &mut undone, &ids(121..=130), &[]).await?;
    undone.rollback().await?;
    ensure!(fixture.documents().await? == 120);
    let mut third = conn.begin().await?;
    let trace = refresh_like_previous(&fixture.wire, &mut third, &ids(131..=260), &[]).await?;
    third.commit().await?;
    ensure!(fixture.documents().await? == 250);
    ensure!(fixture.spelling(125).await?.is_none());
    ensure!(trace.staging_tags() == staging(true, false));
    ensure!(trace.page_tags() == vec![NEW_DOCUMENTS.to_vec(); 2]);
    fixture.db.cleanup().await
}

#[tokio::test]
async fn prepare_then_refresh_reads_the_uncommitted_write() -> Result<()> {
    let fixture = Fixture::create("search_pages_prepare").await?;
    fixture.structural(1..=150).await?;
    let names = ids(1..=150);
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    let changed = [own_label(3)];
    prepare(&mut tx, &changed, &[], &names).await?;
    reveal(&mut tx, &changed[0], "third").await?;
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &names, &changed).await?;
    tx.commit().await?;
    ensure!(fixture.documents().await? == 150);
    ensure!(
        fixture.spelling(3).await?.as_deref()
            == Some(format!("third.[{}]", &label(SHARED)[2..]).as_str())
    );
    ensure!(trace.staging_tags() == staging(true, true));
    ensure!(trace.page_tags() == vec![NEW_DOCUMENTS.to_vec(); 2]);
    fixture.db.cleanup().await
}

/// Every kind of row the writer distinguishes, with stale documents to update and remove.
async fn mixed(name: &str) -> Result<(Fixture, Vec<String>, Vec<String>)> {
    let fixture = Fixture::create(name).await?;
    fixture.structural(1..=260).await?;
    fixture.raw(261..=400, false).await?;
    fixture.raw(401..=420, true).await?;
    let pool = fixture.db.pool();
    // A structural name too long for the short class.
    sqlx::query("UPDATE name_surfaces SET labelhashes=array_fill(labelhashes[1],ARRAY[40]) WHERE logical_name_id=$1")
        .bind(id(50)).execute(pool).await?;
    let mut tx = pool.begin().await?;
    previous_refresh(&mut tx, &ids(1..=420), &[]).await?;
    tx.commit().await?;
    // Now make stored documents stale in every way: renamed, deactivated, revealed, emptied.
    let mut setup = pool.acquire().await?;
    reveal(&mut setup, &label(SHARED), "shared").await?;
    reveal(&mut setup, &own_label(9), "nine").await?;
    reveal(&mut setup, &own_label(50), "l").await?;
    sqlx::query(
        "UPDATE name_surfaces SET visibility_state='shadow', deactivation_reason='test',
            deactivated_at=now(), normalization_errors='[\"test\"]'
         WHERE logical_name_id=ANY($1)",
    )
    .bind(ids((261..=420).filter(|number| number % 7 == 0)))
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE name_surfaces SET raw_name='Revealed'||right(logical_name_id,2)||'.eth',
            raw_labels=ARRAY['Revealed'||right(logical_name_id,2),'eth'],
            dns_encoded_name='\\x00', preimage_event_identity='late'
         WHERE logical_name_id=ANY($1)",
    )
    .bind(ids([20, 21]))
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE name_surfaces SET raw_name='', raw_labels=ARRAY[]::text[],
            visibility_state='shadow', deactivation_reason='test', deactivated_at=now()
         WHERE logical_name_id=$1",
    )
    .bind(id(300))
    .execute(pool)
    .await?;
    // Every third name, some names with no surface, a duplicate, and both changed labels.
    let mut names = ids((1..=420).filter(|number| number % 3 == 0));
    names.extend(ids([20, 21, 50, 300, 9_999, 0, 3, 3]));
    let labels = vec![label(SHARED), own_label(9), label(123_456)];
    Ok((fixture, names, labels))
}

#[tokio::test]
async fn mixed_fixture_matches_the_previous_query() -> Result<()> {
    let (previous, names, labels) = mixed("search_pages_mixed_previous").await?;
    let (current, _, _) = mixed("search_pages_mixed_current").await?;
    let mut tx = previous.db.pool().begin().await?;
    previous_refresh(&mut tx, &names, &labels).await?;
    tx.commit().await?;
    let mut conn = current.connect().await?;
    let mut tx = conn.begin().await?;
    current.wire.take();
    refresh(&mut tx, &names, &labels).await?;
    let trace = Trace::new(current.wire.take());
    tx.commit().await?;
    trace.pages_bind_only_a_cursor()?;
    // Same rows, same generated keys, same postings: the documents were written in one order.
    let expected = stored(&mut *previous.db.pool().acquire().await?, true).await?;
    let actual = stored(&mut *current.db.pool().acquire().await?, true).await?;
    ensure!(actual == expected, "stored search state differs");
    ensure!(current.spelling(9).await?.as_deref() == Some("nine.shared"));
    ensure!(
        current
            .spelling(50)
            .await?
            .is_some_and(|name| name.len() == 79)
    );
    ensure!(current.spelling(20).await?.as_deref() == Some("revealed14.eth"));
    ensure!(current.spelling(399).await?.is_none() && current.spelling(300).await?.is_none());
    ensure!(current.spelling(262).await?.as_deref() == Some("raw262.eth"));
    previous.db.cleanup().await?;
    current.db.cleanup().await
}

#[tokio::test]
async fn second_refresh_forgets_the_first_name_set() -> Result<()> {
    let fixture = Fixture::create("search_pages_twice").await?;
    fixture.structural(1..=130).await?;
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    refresh_like_previous(&fixture.wire, &mut tx, &ids(1..=120), &[]).await?;
    // Name 5 changes after its refresh. The second refresh does not name it.
    reveal(&mut tx, &own_label(5), "five").await?;
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &ids(121..=130), &[]).await?;
    tx.commit().await?;
    ensure!(fixture.documents().await? == 130);
    ensure!(
        fixture
            .spelling(5)
            .await?
            .is_some_and(|name| name.starts_with('['))
    );
    ensure!(trace.staging_tags() == staging(true, false));
    ensure!(trace.page_tags() == vec![NEW_DOCUMENTS.to_vec()]);
    fixture.db.cleanup().await
}

#[tokio::test]
async fn names_beyond_one_staging_statement_are_bound_once() -> Result<()> {
    let fixture = Fixture::create("search_pages_chunks").await?;
    let most = documents::ANALYZE_ABOVE;
    fixture.structural([1, 20_500, most + 1]).await?;
    let mut conn = fixture.connect().await?;
    // At the threshold: three staging statements and no statistics.
    let mut tx = conn.begin().await?;
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &ids(1..=most), &[]).await?;
    tx.commit().await?;
    let mut expected = staging(false, false);
    expected.extend(["stage_names"; 3]);
    ensure!(
        trace.staging_tags() == expected,
        "{:?}",
        trace.staging_tags()
    );
    // One name over it, with names repeated within and across statements.
    let mut names = ids(1..=most + 1);
    names.extend(ids([1, 20_500, most + 1, most + 1]));
    let mut tx = conn.begin().await?;
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &names, &[]).await?;
    tx.commit().await?;
    ensure!(fixture.documents().await? == 3);
    expected.push("names_stats");
    ensure!(
        trace.staging_tags() == expected,
        "{:?}",
        trace.staging_tags()
    );
    let bound = trace.staged_bytes("stage_names");
    ensure!(
        bound >= payload(&names) && bound < payload(&names) * 2,
        "bound {bound}"
    );
    // 50,001 distinct names are 500 full pages and one short page.
    ensure!(trace.pages.len() == 501, "{} pages", trace.pages.len());
    fixture.db.cleanup().await
}

#[tokio::test]
async fn refresh_after_a_savepoint_rollback_stages_again() -> Result<()> {
    let fixture = Fixture::create("search_pages_savepoint").await?;
    fixture.structural(1..=30).await?;
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    // The first savepoint creates the table, so its rollback removes the table too.
    let mut inner = tx.begin().await?;
    refresh(&mut inner, &ids(1..=10), &[]).await?;
    inner.rollback().await?;
    ensure!(!staging_table_exists(&mut tx).await?);
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &ids(11..=20), &[]).await?;
    ensure!(trace.staging_tags() == staging(true, false));
    // The second savepoint stages into the existing table. Its rollback removes the rows only.
    let mut inner = tx.begin().await?;
    refresh(&mut inner, &ids(1..=10), &[]).await?;
    inner.rollback().await?;
    ensure!(staging_table_exists(&mut tx).await?);
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &ids(21..=30), &[]).await?;
    tx.commit().await?;
    ensure!(trace.page_tags() == vec![NEW_DOCUMENTS.to_vec()]);
    ensure!(fixture.documents().await? == 20);
    ensure!(fixture.spelling(1).await?.is_none());
    fixture.db.cleanup().await
}

#[tokio::test]
async fn table_created_in_a_rolled_back_transaction_is_created_again() -> Result<()> {
    let fixture = Fixture::create("search_pages_recreate").await?;
    fixture.structural(1..=30).await?;
    let mut conn = fixture.connect().await?;
    let mut undone = conn.begin().await?;
    refresh_like_previous(&fixture.wire, &mut undone, &ids(1..=10), &[]).await?;
    ensure!(staging_table_exists(&mut undone).await?);
    undone.rollback().await?;
    ensure!(!staging_table_exists(&mut conn).await?);
    // The connection has cached every statement against the table that no longer exists.
    let mut tx = conn.begin().await?;
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &ids(11..=30), &[]).await?;
    tx.commit().await?;
    ensure!(staging_table_exists(&mut conn).await?);
    ensure!(fixture.documents().await? == 20);
    ensure!(trace.page_tags() == vec![NEW_DOCUMENTS.to_vec()]);
    fixture.db.cleanup().await
}

#[tokio::test]
async fn later_refresh_never_reads_rows_staged_before() -> Result<()> {
    let fixture = Fixture::create("search_pages_stale").await?;
    fixture.structural(1..=140).await?;
    let mut conn = fixture.connect().await?;
    let mut committed = conn.begin().await?;
    refresh(&mut committed, &ids(1..=120), &[]).await?;
    committed.commit().await?;
    ensure!(staged_rows(&mut conn).await? == 0);
    // A rolled-back refresh leaves its rows in the table file, invisible.
    let mut undone = conn.begin().await?;
    refresh(&mut undone, &ids(121..=130), &[]).await?;
    ensure!(staged_rows(&mut undone).await? == 10);
    undone.rollback().await?;
    ensure!(staged_rows(&mut conn).await? == 0);
    // Names 5 and 125 change now. Neither is named by the next refresh.
    let mut setup = fixture.db.pool().acquire().await?;
    reveal(&mut setup, &own_label(5), "five").await?;
    reveal(&mut setup, &own_label(125), "later").await?;
    let mut tx = conn.begin().await?;
    let trace = refresh_like_previous(&fixture.wire, &mut tx, &ids(131..=140), &[]).await?;
    ensure!(staged_rows(&mut tx).await? == 10);
    tx.commit().await?;
    ensure!(trace.page_tags() == vec![NEW_DOCUMENTS.to_vec()]);
    ensure!(fixture.documents().await? == 130);
    ensure!(
        fixture
            .spelling(5)
            .await?
            .is_some_and(|name| name.starts_with('['))
    );
    ensure!(fixture.spelling(125).await?.is_none());
    fixture.db.cleanup().await
}

#[tokio::test]
async fn refresh_outside_a_transaction_is_refused() -> Result<()> {
    let fixture = Fixture::create("search_pages_autocommit").await?;
    fixture.structural(1..=3).await?;
    let mut conn = fixture.connect().await?;
    for (names, labels) in [(ids(1..=3), vec![]), (vec![], vec![label(SHARED)])] {
        let error = refresh(&mut conn, &names, &labels)
            .await
            .err()
            .context("a refresh with no open transaction must fail")?;
        ensure!(
            format!("{error:#}").contains("open transaction"),
            "{error:#}"
        );
    }
    ensure!(fixture.documents().await? == 0);
    fixture.db.cleanup().await
}

#[tokio::test]
async fn repeatable_read_is_refused_before_staging() -> Result<()> {
    let fixture = Fixture::create("search_pages_isolation").await?;
    fixture.structural(1..=3).await?;
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    fixture.wire.take();
    let error = refresh(&mut tx, &ids(1..=3), &[]).await.unwrap_err();
    ensure!(error.to_string().contains("READ COMMITTED"));
    let sent = fixture.wire.take();
    ensure!(sent.len() == 1 && sent[0].tag() == "SHOW transaction_isolation");
    tx.rollback().await?;
    fixture.db.cleanup().await
}

#[tokio::test]
async fn role_without_temporary_tables_is_refused() -> Result<()> {
    let fixture = Fixture::create("search_pages_privilege").await?;
    fixture.structural(1..=3).await?;
    let pool = fixture.db.pool();
    // A crashed run can leave its role behind. The name is this process's own, and a role
    // left under the same name is dropped first, so no earlier run can block this one.
    let role = format!("search_pages_no_temp_{}", std::process::id());
    sqlx::query(&format!("DROP ROLE IF EXISTS {role}"))
        .execute(pool)
        .await?;
    sqlx::query(&format!("CREATE ROLE {role}"))
        .execute(pool)
        .await?;
    // Every failure inside this block still reaches the DROP ROLE below.
    let outcome = async {
        sqlx::query(&format!(
            "REVOKE TEMPORARY ON DATABASE \"{}\" FROM PUBLIC",
            fixture.db.database_name()
        ))
        .execute(pool)
        .await?;
        let mut conn = fixture.connect().await?;
        let mut tx = conn.begin().await?;
        sqlx::query(&format!("SET LOCAL ROLE {role}"))
            .execute(&mut *tx)
            .await?;
        let outcome = refresh(&mut tx, &ids(1..=3), &[]).await;
        tx.rollback().await?;
        anyhow::Ok(outcome)
    }
    .await;
    sqlx::query(&format!("DROP ROLE {role}"))
        .execute(pool)
        .await?;
    let error = outcome?
        .err()
        .context("a role that may not create temporary tables must fail")?;
    ensure!(format!("{error:#}").contains("staging table"), "{error:#}");
    ensure!(fixture.documents().await? == 0);
    fixture.db.cleanup().await
}

async fn staging_table_exists(conn: &mut PgConnection) -> Result<bool> {
    Ok(
        sqlx::query_scalar("SELECT to_regclass('pg_temp.identity_search_names') IS NOT NULL")
            .fetch_one(conn)
            .await?,
    )
}

async fn staged_rows(conn: &mut PgConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT count(*) FROM pg_temp.identity_search_names")
            .fetch_one(conn)
            .await?,
    )
}

/// Rows written to four system catalogs of this database so far. `pg_statistic` is left out
/// because autovacuum may analyze the fixture's catalogs at any moment.
async fn catalog_writes(conn: &mut PgConnection) -> Result<Vec<(String, i64)>> {
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(&mut *conn)
        .await?;
    Ok(sqlx::query_as(
        "SELECT relname::text, (n_tup_ins + n_tup_upd + n_tup_del)::bigint
         FROM pg_stat_sys_tables
         WHERE relname IN ('pg_class','pg_attribute','pg_type','pg_depend')
         ORDER BY relname",
    )
    .fetch_all(conn)
    .await?)
}

#[tokio::test]
async fn small_refreshes_after_the_first_write_no_catalog_row() -> Result<()> {
    let fixture = Fixture::create("search_pages_catalogs").await?;
    // The database is dropped on every way out of the measurement: success, error or panic.
    let outcome = AssertUnwindSafe(catalog_rows_stay_flat(&fixture))
        .catch_unwind()
        .await;
    // The measurement's own failure is reported before any failure to clean up.
    let cleaned = fixture.db.cleanup().await;
    outcome.unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
    cleaned
}

async fn catalog_rows_stay_flat(fixture: &Fixture) -> Result<()> {
    fixture.structural(1..=3).await?;
    // A backend reports its counters when it exits. End the fixture's own backends so that
    // their catalog writes are counted before the first reading. The pool reconnects later.
    let mut conn = fixture.connect().await?;
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
         WHERE datname = current_database() AND pid <> pg_backend_pid()",
    )
    .execute(&mut conn)
    .await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
         WHERE datname = current_database() AND pid <> pg_backend_pid())",
    )
    .fetch_one(&mut conn)
    .await?
    {
        ensure!(
            Instant::now() < deadline,
            "the fixture's other backends were still connected 10 seconds after termination"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let start = catalog_writes(&mut conn).await?;
    let mut first = Vec::new();
    for round in 0..1_000 {
        let mut tx = conn.begin().await?;
        refresh(&mut tx, &ids(1..=3), &[]).await?;
        // A second refresh in the same transaction clears the first one's names.
        refresh(&mut tx, &ids(2..=3), &[label(SHARED)]).await?;
        tx.commit().await?;
        if round == 0 {
            first = catalog_writes(&mut conn).await?;
        }
    }
    let last = catalog_writes(&mut conn).await?;
    ensure!(first != start, "the first refresh creates the table");
    ensure!(
        last == first,
        "refreshes after the first wrote catalog rows: {first:?} became {last:?}"
    );
    let statistics: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_statistic
         WHERE starelid = to_regclass('pg_temp.identity_search_names')",
    )
    .fetch_one(&mut conn)
    .await?;
    ensure!(statistics == 0, "a small refresh gathered statistics");
    ensure!(fixture.documents().await? == 3);
    Ok(())
}

#[tokio::test]
async fn page_statement_is_not_kept_prepared() -> Result<()> {
    let fixture = Fixture::create("search_pages_plan").await?;
    fixture.structural(1..=250).await?;
    let mut conn = fixture.connect().await?;
    let mut tx = conn.begin().await?;
    refresh(&mut tx, &ids(1..=3), &[]).await?;
    refresh_like_previous(&fixture.wire, &mut tx, &ids(1..=250), &[]).await?;
    // A prepared page statement could keep the plan it got for three names.
    let prepared: Vec<String> = sqlx::query_scalar("SELECT statement FROM pg_prepared_statements")
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    ensure!(
        prepared
            .iter()
            .any(|statement| statement.contains("identity_search.stage_names")),
        "the connection's prepared statements were not read: {prepared:?}"
    );
    ensure!(
        !prepared
            .iter()
            .any(|statement| statement.contains("identity_search.spellings")),
        "the page statement is prepared and can keep a stale plan"
    );
    fixture.db.cleanup().await
}
