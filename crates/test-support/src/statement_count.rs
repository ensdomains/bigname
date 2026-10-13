//! Count the statements a test sends to PostgreSQL.
//!
//! [`StatementCounter`] opens a pool on the same database as a test's pool, with a relay
//! between the pool and the server. The relay copies the bytes both ways and reads the
//! messages the pool sends. It counts each `Query` message and each `Execute` message, which is
//! each time the server is asked to run a statement. A statement prepared once and run again
//! counts on every run.
//!
//! The relay speaks plain TCP, so the test server must accept a connection without TLS, as the
//! test harness's server does.
use std::{
    collections::{BTreeMap, HashMap},
    io,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use sqlx::{
    PgPool,
    postgres::{PgPoolOptions, PgSslMode},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    task::{JoinHandle, JoinSet},
};

/// The protocol version a startup message carries. An encryption request carries another code
/// and is followed by a second startup message.
const PROTOCOL_3: u32 = 196_608;
const CANCEL_REQUEST: u32 = 80_877_102;

type Sent = Arc<Mutex<Vec<String>>>;

/// A pool whose statements are counted on their way to the server.
///
/// A request is counted from the first poll of its future to its completion. Two things can
/// put another request's statement in a count:
///
/// - Two requests running on one counter at the same time. Count one at a time.
/// - A transaction dropped without a commit or rollback. sqlx queues its `ROLLBACK` and sends it
///   with the connection's next statement, which may belong to the next request. To keep it
///   out, count each request on its own counter, which has its own connections.
pub struct StatementCounter {
    pool: PgPool,
    sent: Sent,
    relay: JoinHandle<()>,
}

impl StatementCounter {
    /// Open a counted pool on the database of `pool`, with its options and size.
    pub async fn connect(pool: &PgPool) -> Result<Self> {
        let options = pool.connect_options();
        ensure!(
            options.get_socket().is_none() && !options.get_host().starts_with('/'),
            "the statement counter relays TCP connections only"
        );
        let upstream = (options.get_host().to_owned(), options.get_port());
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .context("failed to bind the statement counter's relay")?;
        let port = listener.local_addr()?.port();
        let sent = Sent::default();
        let relay = tokio::spawn(accept(listener, upstream, Arc::clone(&sent)));
        let pool = PgPoolOptions::new()
            .max_connections(pool.options().get_max_connections())
            .connect_with(
                options
                    .as_ref()
                    .clone()
                    .host("127.0.0.1")
                    .port(port)
                    .ssl_mode(PgSslMode::Disable),
            )
            .await
            .context("failed to connect through the statement counter's relay")?;
        Ok(Self { pool, sent, relay })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Run `request` and return its output with the statements sent while it ran.
    pub async fn count<T>(&self, request: impl Future<Output = T>) -> (T, StatementCounts) {
        self.sent.lock().expect("statement log poisoned").clear();
        let output = request.await;
        let statements = std::mem::take(&mut *self.sent.lock().expect("statement log poisoned"));
        (output, StatementCounts { statements })
    }

    /// Close the pool and stop the relay.
    pub async fn close(self) {
        self.pool.close().await;
    }
}

impl Drop for StatementCounter {
    fn drop(&mut self) {
        self.relay.abort();
    }
}

/// The statements one request sent, in order.
#[derive(Clone, Debug, Default)]
pub struct StatementCounts {
    statements: Vec<String>,
}

impl StatementCounts {
    /// Every statement sent, transaction control included.
    pub fn total(&self) -> usize {
        self.statements.len()
    }

    /// The statements sent, leaving out transaction control: `BEGIN`, `START TRANSACTION`,
    /// `SET TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT` and `RELEASE`.
    pub fn without_transaction_control(&self) -> usize {
        self.statements
            .iter()
            .filter(|statement| !is_transaction_control(statement))
            .count()
    }

    /// The statements whose text carries the comment `/* storage:<tag> */`, anywhere in it.
    pub fn tagged(&self, tag: &str) -> usize {
        self.statements
            .iter()
            .filter(|statement| storage_tag(statement) == Some(tag))
            .count()
    }

    /// The count of each `/* storage:<tag> */`. Statements without one are under the empty tag.
    pub fn by_tag(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for statement in &self.statements {
            *counts
                .entry(storage_tag(statement).unwrap_or_default().to_owned())
                .or_default() += 1;
        }
        counts
    }

    /// The text of every statement sent, for a failure message.
    pub fn statements(&self) -> &[String] {
        &self.statements
    }
}

fn storage_tag(statement: &str) -> Option<&str> {
    let (_, rest) = statement.split_once("/* storage:")?;
    let (tag, _) = rest.split_once("*/")?;
    Some(tag.trim())
}

fn is_transaction_control(statement: &str) -> bool {
    let statement = statement.trim_start().to_ascii_uppercase();
    [
        "BEGIN",
        "START TRANSACTION",
        "SET TRANSACTION",
        "COMMIT",
        "END",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
    ]
    .iter()
    .any(|keyword| {
        statement.strip_prefix(keyword).is_some_and(|rest| {
            !rest.starts_with(|next: char| next.is_ascii_alphanumeric() || next == '_')
        })
    })
}

/// Relay every connection made to `listener`. Dropping the task ends the connections with it.
async fn accept(listener: TcpListener, upstream: (String, u16), sent: Sent) {
    let mut sessions = JoinSet::new();
    while let Ok((client, _)) = listener.accept().await {
        let (upstream, sent) = (upstream.clone(), Arc::clone(&sent));
        sessions.spawn(async move {
            // A connection that fails here fails in the pool as well, which reports it.
            let _ = session(client, upstream, sent).await;
        });
    }
}

async fn session(client: TcpStream, upstream: (String, u16), sent: Sent) -> io::Result<()> {
    let server = TcpStream::connect((upstream.0.as_str(), upstream.1)).await?;
    client.set_nodelay(true)?;
    server.set_nodelay(true)?;
    let (mut from_client, mut to_client) = client.into_split();
    let (mut from_server, mut to_server) = server.into_split();
    tokio::select! {
        result = forward(&mut from_client, &mut to_server, &sent) => result,
        result = tokio::io::copy(&mut from_server, &mut to_client) => result.map(|_| ()),
    }
}

/// Copy the pool's messages to the server, recording each statement before it is passed on.
async fn forward(
    client: &mut OwnedReadHalf,
    server: &mut OwnedWriteHalf,
    sent: &Mutex<Vec<String>>,
) -> io::Result<()> {
    // Until the session starts, a message has no type byte: a length, a code and a body.
    loop {
        let length = client.read_u32().await?;
        let mut frame = length.to_be_bytes().to_vec();
        frame.resize(message_size(length)?, 0);
        client.read_exact(&mut frame[4..]).await?;
        server.write_all(&frame).await?;
        match frame
            .get(4..8)
            .map(|code| u32::from_be_bytes(code.try_into().expect("four bytes")))
        {
            Some(PROTOCOL_3) => break,
            Some(CANCEL_REQUEST) | None => return Ok(()),
            Some(_) => {}
        }
    }
    // The text of each prepared statement by name, and of each bound portal by name.
    let mut prepared: HashMap<Vec<u8>, String> = HashMap::new();
    let mut portals: HashMap<Vec<u8>, String> = HashMap::new();
    loop {
        let kind = match client.read_u8().await {
            Ok(kind) => kind,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        };
        let length = client.read_u32().await?;
        let mut frame = vec![kind];
        frame.extend_from_slice(&length.to_be_bytes());
        frame.resize(1 + message_size(length)?, 0);
        client.read_exact(&mut frame[5..]).await?;
        let body = &frame[5..];
        match kind {
            b'Q' => record(sent, text(c_string(body).0)),
            b'P' => {
                let (name, rest) = c_string(body);
                prepared.insert(name.to_vec(), text(c_string(rest).0));
            }
            b'B' => {
                let (portal, rest) = c_string(body);
                let statement = prepared.get(c_string(rest).0).cloned().unwrap_or_default();
                portals.insert(portal.to_vec(), statement);
            }
            b'E' => record(
                sent,
                portals.get(c_string(body).0).cloned().unwrap_or_default(),
            ),
            _ => {}
        }
        server.write_all(&frame).await?;
    }
}

/// The size of a message from its length field, which counts itself.
fn message_size(length: u32) -> io::Result<usize> {
    if length < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a PostgreSQL message shorter than its length field",
        ));
    }
    Ok(length as usize)
}

/// A zero-terminated string at the start of `bytes`, and what follows it.
fn c_string(bytes: &[u8]) -> (&[u8], &[u8]) {
    match bytes.iter().position(|byte| *byte == 0) {
        Some(end) => (&bytes[..end], &bytes[end + 1..]),
        None => (bytes, &[]),
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn record(sent: &Mutex<Vec<String>>, statement: String) {
    sent.lock().expect("statement log poisoned").push(statement);
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::StatementCounter;
    use crate::{TestDatabase, TestDatabaseConfig};

    #[tokio::test]
    async fn counter_reports_each_statement_sent() -> Result<()> {
        let database = TestDatabase::create(TestDatabaseConfig::new("statement_count")).await?;
        let counter = StatementCounter::connect(database.pool()).await?;
        let (result, sent) = counter
            .count(async {
                let mut transaction = counter.pool().begin().await?;
                // The same prepared statement run three times counts three times.
                for value in 1..=3_i32 {
                    sqlx::query("/* storage:test.bound */ SELECT $1::int")
                        .bind(value)
                        .execute(&mut *transaction)
                        .await?;
                }
                sqlx::query("SELECT 2 /* storage:test.trailing */")
                    .persistent(false)
                    .execute(&mut *transaction)
                    .await?;
                // One simple-protocol message holds this statement.
                sqlx::raw_sql("SELECT 3").execute(&mut *transaction).await?;
                transaction.commit().await?;
                anyhow::Ok(())
            })
            .await;
        result?;
        assert_eq!(sent.total(), 7, "{:?}", sent.statements());
        assert_eq!(sent.without_transaction_control(), 5);
        assert_eq!(sent.tagged("test.bound"), 3);
        assert_eq!(sent.tagged("test.trailing"), 1);
        assert_eq!(sent.by_tag()[""], 3);

        // A second count starts from nothing.
        let (result, sent) = counter
            .count(sqlx::query("SELECT 4").execute(counter.pool()))
            .await;
        result?;
        assert_eq!(sent.total(), 1, "{:?}", sent.statements());

        counter.close().await;
        database.cleanup().await
    }
}
