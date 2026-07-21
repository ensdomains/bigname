pub(super) fn format_provider_error(error: &anyhow::Error) -> String {
    let mut rendered = format!("{error:#}");
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<reqwest::Error>() {
            redact_reqwest_url(&mut rendered, error);
        }
    }
    rendered
}

pub(super) fn format_provider_transport_error(error: &reqwest::Error) -> String {
    let mut rendered = error.to_string();
    redact_reqwest_url(&mut rendered, error);
    rendered
}

pub(super) fn redact_provider_transport_error_url(error: &mut reqwest::Error) {
    let Some(url) = error.url_mut() else {
        return;
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    let _ = url.set_port(None);
    url.set_path("");
    url.set_query(None);
    url.set_fragment(None);
}

fn redact_reqwest_url(rendered: &mut String, error: &reqwest::Error) {
    let Some(url) = error.url() else {
        return;
    };
    let redacted_url = match url.host_str() {
        Some(host) => format!("{}://{host}", url.scheme()),
        None => format!("{}://<redacted-host>", url.scheme()),
    };
    *rendered = rendered.replace(url.as_str(), &redacted_url);
}

#[cfg(test)]
mod tests {
    use std::{future::pending, time::Duration};

    use anyhow::{Context, Result};
    use tokio::net::TcpListener;

    use super::{format_provider_error, format_provider_transport_error};
    use crate::provider::{JsonRpcProvider, request::is_retryable_provider_error};

    const SECRET_PATH: &str = "/provider-key-secret/v1";
    const SECRET_QUERY: &str = "api_key=query-secret";

    #[tokio::test]
    async fn transport_log_error_keeps_host_and_redacts_path_and_query() -> Result<()> {
        let (error, host) = url_bearing_timeout_error().await?;

        let rendered = format_provider_transport_error(&error);

        assert!(rendered.contains(&format!("for url (http://{host})")));
        assert!(!rendered.contains(SECRET_PATH));
        assert!(!rendered.contains(SECRET_QUERY));
        Ok(())
    }

    #[tokio::test]
    async fn retry_warning_error_is_redacted_and_remains_retryable() -> Result<()> {
        let (error, host) = url_bearing_timeout_error().await?;
        let error = anyhow::Error::new(error).context("failed to send JSON-RPC request for test");

        let rendered = format_provider_error(&error);

        assert!(rendered.contains(&format!("for url (http://{host})")));
        assert!(!rendered.contains(SECRET_PATH));
        assert!(!rendered.contains(SECRET_QUERY));
        assert!(rendered.to_ascii_lowercase().contains("timed out"));
        assert!(is_retryable_provider_error(&error));
        assert!(is_retryable_provider_error(&anyhow::anyhow!(rendered)));
        Ok(())
    }

    #[tokio::test]
    async fn retry_exhaustion_error_is_safe_for_debug_log_sinks() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to bind retry exhaustion test server")?;
        let address = listener
            .local_addr()
            .context("failed to read retry exhaustion test server address")?;
        let server = tokio::spawn(async move {
            while let Ok((connection, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let _connection = connection;
                    pending::<()>().await;
                });
            }
        });
        let endpoint = format!("http://{address}{SECRET_PATH}?{SECRET_QUERY}");
        let provider =
            JsonRpcProvider::new_with_request_timeout(&endpoint, Duration::from_millis(25))?;

        let error = provider
            .fetch_json_rpc_result("eth_chainId", Vec::new())
            .await
            .expect_err("the provider request must exhaust its timeout retries");
        server.abort();
        let rendered = format!("{error:?}");

        assert!(rendered.contains(&format!("http://{}", address.ip())));
        assert!(!rendered.contains(SECRET_PATH));
        assert!(!rendered.contains(SECRET_QUERY));
        assert!(is_retryable_provider_error(&error));
        Ok(())
    }

    async fn url_bearing_timeout_error() -> Result<(reqwest::Error, String)> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("failed to bind timeout test server")?;
        let address = listener
            .local_addr()
            .context("failed to read timeout test server address")?;
        let server = tokio::spawn(async move {
            let _connection = listener.accept().await;
            pending::<()>().await;
        });
        let endpoint = format!("http://{address}{SECRET_PATH}?{SECRET_QUERY}");
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .context("failed to build timeout test client")?;

        let error = client
            .get(&endpoint)
            .send()
            .await
            .expect_err("the server must hold the response until the request times out");
        server.abort();
        assert!(error.is_timeout(), "expected timeout error: {error}");
        assert_eq!(
            error.url().map(reqwest::Url::as_str),
            Some(endpoint.as_str())
        );

        Ok((error, address.ip().to_string()))
    }
}
