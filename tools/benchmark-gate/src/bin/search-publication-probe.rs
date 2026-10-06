//! The explicitly invoked static Gate 1 preparer; never part of API request handling.
#[path = "../search_publication_probe/mod.rs"]
mod probe;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    probe::run().await
}
