//! Minimal HTTP GET abstraction for the REST side of the connector, so tests can run without a network.

use std::time::Duration;

use futures_util::future::BoxFuture;

/// What the connector needs from an HTTP response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// Status code.
    pub status: u16,
    /// `Retry-After` in seconds, when sent (429 and 418).
    pub retry_after: Option<Duration>,
    /// `X-MBX-USED-WEIGHT-1M`: request weight used by this IP in the current minute.
    pub used_weight_1m: Option<u32>,
    /// Body as text.
    pub body: String,
}

/// Transport-level failure (no HTTP response).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HttpError {
    /// The request did not finish in time.
    #[error("request timed out")]
    Timeout,
    /// DNS, TCP, TLS or protocol failure.
    #[error("network error: {0}")]
    Network(String),
}

/// A client bound to one base URL. `path_and_query` starts with `/` and is already percent-encoded.
pub trait HttpClient: Send + Sync + 'static {
    /// Performs `GET base + path_and_query`.
    fn get<'a>(&'a self, path_and_query: &'a str)
    -> BoxFuture<'a, Result<HttpResponse, HttpError>>;
}

/// Percent-encodes a query value: every byte of the UTF-8 form that is not an unreserved character
/// (`A-Z a-z 0-9 - . _ ~`) becomes `%XX`. Symbols such as `币安人生USDT` are sent this way.
pub fn encode_query_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Production client: `reqwest` over rustls (ring provider, bundled web PKI roots like the WebSocket side).
pub struct ReqwestClient {
    client: reqwest::Client,
    base: String,
}

impl ReqwestClient {
    /// Builds a client for `base_url` (for example `https://fapi.binance.com`).
    pub fn new(base_url: &str) -> Result<Self, String> {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let client = reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .connect_timeout(Duration::from_secs(5))
            // Backstop only; the callers apply their own, shorter deadlines.
            .timeout(Duration::from_secs(60))
            .pool_idle_timeout(Duration::from_secs(30))
            .user_agent(concat!("vulture-server/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            base: base_url.trim_end_matches('/').to_string(),
        })
    }
}

impl HttpClient for ReqwestClient {
    fn get<'a>(
        &'a self,
        path_and_query: &'a str,
    ) -> BoxFuture<'a, Result<HttpResponse, HttpError>> {
        Box::pin(async move {
            let url = format!("{}{}", self.base, path_and_query);
            let response = self.client.get(&url).send().await.map_err(map_error)?;
            let status = response.status().as_u16();
            let header = |name: &str| {
                response
                    .headers()
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.trim().to_string())
            };
            let retry_after = header("retry-after")
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs);
            let used_weight_1m = header("x-mbx-used-weight-1m").and_then(|v| v.parse::<u32>().ok());
            let body = response.text().await.map_err(map_error)?;
            Ok(HttpResponse {
                status,
                retry_after,
                used_weight_1m,
                body,
            })
        })
    }
}

fn map_error(e: reqwest::Error) -> HttpError {
    if e.is_timeout() {
        HttpError::Timeout
    } else {
        HttpError::Network(e.without_url().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_values_are_percent_encoded_as_utf8() {
        assert_eq!(encode_query_value("BTCUSDT"), "BTCUSDT");
        assert_eq!(encode_query_value("1000PEPEUSDT"), "1000PEPEUSDT");
        assert_eq!(
            encode_query_value("币安人生USDT"),
            "%E5%B8%81%E5%AE%89%E4%BA%BA%E7%94%9FUSDT"
        );
        assert_eq!(encode_query_value("a b&c"), "a%20b%26c");
    }
}
