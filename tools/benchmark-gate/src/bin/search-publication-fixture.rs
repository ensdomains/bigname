//! Small normal identity/normalized-input → Project fixture for Gate 1 HTTP parity.
#[path = "../search_publication_fixture/mod.rs"]
mod fixture;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fixture::run().await
}
