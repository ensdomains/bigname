use anyhow::{Context, Result, ensure};
use reqwest::Url;

pub(in crate::api_load) fn normalized_base_url(value: &str) -> Result<Url> {
    let mut url = Url::parse(value).context("failed to parse API base URL")?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "API base URL must use HTTP or HTTPS"
    );
    ensure!(
        percent_decodes_to_utf8(url.username())
            && url.password().is_none_or(percent_decodes_to_utf8),
        "API base URL userinfo must percent-decode to valid UTF-8"
    );
    url.set_query(None);
    url.set_fragment(None);
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

pub(crate) fn report_base_url(value: &str) -> Result<String> {
    let mut url = normalized_base_url(value)?;
    ensure!(
        url.set_username("").is_ok() && url.set_password(None).is_ok(),
        "API base URL credentials could not be removed for the benchmark report"
    );
    Ok(url.to_string())
}

fn percent_decodes_to_utf8(value: &str) -> bool {
    let encoded = value.as_bytes();
    let mut decoded = Vec::with_capacity(encoded.len());
    let mut index = 0;
    while index < encoded.len() {
        if encoded[index] == b'%'
            && let (Some(high), Some(low)) = (encoded.get(index + 1), encoded.get(index + 2))
            && let (Some(high), Some(low)) = (hex_value(*high), hex_value(*low))
        {
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(encoded[index]);
            index += 1;
        }
    }
    std::str::from_utf8(&decoded).is_ok()
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}
