// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Bounded public market discovery through the shared Nautilus HTTP transport.

use std::{
    collections::{BTreeSet, HashMap},
    num::{NonZeroU16, NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use nautilus_network::{
    http::{HttpClient, HttpClientError, HttpRedirectPolicy, HttpResponse, Method, Url},
    ratelimiter::quota::Quota,
    retry::{RetryConfig, RetryError, RetryManager},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    KalshiMarketMetadata, KalshiMetadataError, common::validate_ticker, decode_market_response,
    decode_markets_response,
};

const HTTP_QUOTA_KEY: &str = "kalshi-public-metadata";

/// Operator budgets for public REST metadata requests.
///
/// Clone a client to share its connection pool and request quota. The spacing is an operator
/// budget, not an assertion about the venue's account tier or other traffic on the same IP.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KalshiHttpConfig {
    /// The API root, including `/trade-api/v2` (without query parameters or credentials).
    pub base_url: String,
    /// Maximum wall time per attempt, including its quota wait; capped by the operation budget.
    pub request_timeout_ms: NonZeroU32,
    /// Maximum wall time for a complete operation, including pages, quota waits and retries.
    pub operation_timeout_ms: NonZeroU32,
    /// Maximum bytes per response, enforced while reading the body.
    pub max_response_bytes: NonZeroUsize,
    /// Maximum pages in one complete discovery operation.
    pub max_pages: NonZeroUsize,
    /// Maximum markets in one complete discovery operation.
    pub max_markets: NonZeroUsize,
    /// Maximum requested and accepted markets per page, from 1 through 1000.
    pub page_size: NonZeroU16,
    /// Minimum interval between requests; the shared quota permits a burst of one.
    pub request_spacing_ms: NonZeroU32,
    /// Additional attempts for a transient HTTP status or timeout.
    pub max_retries: u32,
}

/// A market status filter in the REST query vocabulary.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KalshiMarketFilterStatus {
    /// Markets which have not opened.
    Unopened,
    /// Markets open for trading.
    Open,
    /// Markets closed for trading.
    Closed,
    /// Markets which have settled.
    Settled,
}

/// Optional discovery filters; absent fields are omitted from the request.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KalshiMarketFilter {
    /// Source event identity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_ticker: Option<String>,
    /// Source series identity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series_ticker: Option<String>,
    /// REST status filter, distinct from the market definition's lifecycle status.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<KalshiMarketFilterStatus>,
}

/// Failures which prevent a complete metadata result.
#[derive(Debug, Error)]
pub enum KalshiHttpError {
    /// Invalid local configuration or request identity.
    #[error("Invalid Kalshi HTTP request configuration: {0}")]
    InvalidConfiguration(&'static str),
    /// Shared transport failure.
    #[error(transparent)]
    Transport(#[from] HttpClientError),
    /// Non-success response, with the original bounded body retained for diagnostics.
    #[error("Kalshi HTTP status {status}")]
    HttpStatus {
        /// Source HTTP status.
        status: u16,
        /// Original response body.
        body: Vec<u8>,
        /// Minimum venue delay. An unrecognized header disables automatic retries.
        retry_after: Option<Duration>,
        /// Whether the venue's delay header can be honored.
        retry_after_valid: bool,
    },
    /// Source metadata failed validation.
    #[error(transparent)]
    Metadata(#[from] KalshiMetadataError),
    /// The complete operation exceeded its wall-time budget.
    #[error("Kalshi HTTP operation exceeded its elapsed-time budget")]
    DeadlineExceeded,
    /// Shared retry machinery terminated the request.
    #[error(transparent)]
    Retry(#[from] RetryError),
    /// Discovery cannot be completed within the configured bounds.
    #[error("Incomplete Kalshi discovery: {0}")]
    IncompleteDiscovery(&'static str),
}

impl KalshiHttpError {
    fn should_retry(&self) -> bool {
        match self {
            Self::HttpStatus {
                status,
                retry_after_valid,
                ..
            } => *retry_after_valid && matches!(status, 408 | 429 | 500 | 502 | 503 | 504),
            Self::Transport(HttpClientError::TimeoutError(_))
            | Self::Retry(RetryError::OperationTimeout { .. }) => true,
            _ => false,
        }
    }

    fn retry_delay(&self) -> Option<Duration> {
        match self {
            Self::HttpStatus { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// Public, read-only Kalshi metadata client.
///
/// Uses no credentials and follows no redirects. Dropping a request future cancels its work.
/// A failed discovery returns no partial result and does not retain a local market catalog.
#[derive(Clone, Debug)]
pub struct KalshiHttpClient {
    client: HttpClient,
    base_url: Url,
    config: KalshiHttpConfig,
    retry: Arc<RetryManager<KalshiHttpError>>,
}

impl KalshiHttpClient {
    /// Creates a client with explicit resource budgets.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid API root, a page size above 1000, or a transport build error.
    pub fn new(config: KalshiHttpConfig) -> Result<Self, KalshiHttpError> {
        let mut base_url = Url::parse(&config.base_url)
            .map_err(|_| KalshiHttpError::InvalidConfiguration("invalid API root URL"))?;

        if !matches!(base_url.scheme(), "https" | "http")
            || base_url.host_str().is_none()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
        {
            return Err(KalshiHttpError::InvalidConfiguration(
                "invalid API root URL",
            ));
        }

        if config.page_size.get() > 1000 {
            return Err(KalshiHttpError::InvalidConfiguration(
                "page size exceeds 1000",
            ));
        }
        // Normalize once so relative endpoint joins preserve the caller's API path prefix.
        base_url.set_path(&format!("{}/", base_url.path().trim_end_matches('/')));
        let spacing = Duration::from_millis(u64::from(config.request_spacing_ms.get()));
        let quota = Quota::with_period(spacing).ok_or(KalshiHttpError::InvalidConfiguration(
            "invalid request spacing",
        ))?;
        let client = HttpClient::builder()
            .keyed_quotas(vec![(HTTP_QUOTA_KEY.to_string(), quota)])
            .header_keys(vec!["retry-after".to_string()])
            .redirect_policy(HttpRedirectPolicy::Reject)
            .max_response_bytes(config.max_response_bytes)
            .use_system_proxy(false)
            .build()?;
        let budget_ms = u64::from(config.operation_timeout_ms.get());
        let retry = Arc::new(RetryManager::new(RetryConfig {
            max_retries: config.max_retries,
            operation_timeout_ms: Some(u64::from(config.request_timeout_ms.get())),
            max_elapsed_ms: Some(budget_ms),
            ..RetryConfig::default()
        }));
        Ok(Self {
            client,
            base_url,
            config,
            retry,
        })
    }

    /// Fetches a market and binds the decoded identity to the requested ticker.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid ticker, failed request, exceeded budget or invalid metadata.
    pub async fn get_market(&self, ticker: &str) -> Result<KalshiMarketMetadata, KalshiHttpError> {
        validate_ticker(ticker)
            .map_err(|_| KalshiHttpError::InvalidConfiguration("invalid market ticker"))?;
        // Dot segments are normalized by URL libraries and cannot name a market endpoint.
        if matches!(ticker, "." | "..") {
            return Err(KalshiHttpError::InvalidConfiguration(
                "invalid market ticker",
            ));
        }
        let mut url = self.endpoint("markets/")?;
        url.path_segments_mut()
            .map_err(|()| KalshiHttpError::InvalidConfiguration("invalid API root URL"))?
            .pop_if_empty()
            .push(ticker);
        let response = self.get(url).await?;
        Ok(decode_market_response(
            &response.body,
            ticker,
            self.config.max_response_bytes,
        )?)
    }

    /// Discovers all matching markets within the configured budgets.
    ///
    /// # Errors
    ///
    /// Returns an error for a failed page, repeated cursor or market, or exhausted budget.
    /// An empty page is terminal only when the venue also returns an empty cursor.
    pub async fn discover_markets(
        &self,
        filter: &KalshiMarketFilter,
    ) -> Result<Vec<KalshiMarketMetadata>, KalshiHttpError> {
        for ticker in [&filter.event_ticker, &filter.series_ticker]
            .into_iter()
            .flatten()
        {
            validate_ticker(ticker)
                .map_err(|_| KalshiHttpError::InvalidConfiguration("invalid filter ticker"))?;
        }
        tokio::time::timeout(self.timeout(), self.discover_pages(filter))
            .await
            .map_err(|_| KalshiHttpError::DeadlineExceeded)?
    }

    async fn discover_pages(
        &self,
        filter: &KalshiMarketFilter,
    ) -> Result<Vec<KalshiMarketMetadata>, KalshiHttpError> {
        #[derive(Serialize)]
        struct Query<'a> {
            #[serde(flatten)]
            filter: &'a KalshiMarketFilter,
            limit: usize,
            #[serde(skip_serializing_if = "str::is_empty")]
            cursor: &'a str,
        }

        let mut markets = Vec::new();
        let mut seen_tickers = BTreeSet::new();
        let mut seen_cursors = BTreeSet::new();
        let mut cursor = String::new();

        for _ in 0..self.config.max_pages.get() {
            let remaining = self.config.max_markets.get() - markets.len();
            let limit = NonZeroUsize::new(remaining.min(usize::from(self.config.page_size.get())))
                .ok_or(KalshiHttpError::IncompleteDiscovery(
                    "market count limit reached",
                ))?;
            let mut url = self.endpoint("markets")?;
            url.set_query(Some(
                &serde_urlencoded::to_string(Query {
                    filter,
                    limit: limit.get(),
                    cursor: &cursor,
                })
                .map_err(|_| KalshiHttpError::InvalidConfiguration("invalid query"))?,
            ));
            let response = self.get(url).await?;
            let page =
                decode_markets_response(&response.body, self.config.max_response_bytes, limit)?;

            for market in page.markets {
                if !seen_tickers.insert(market.ticker.clone()) {
                    return Err(KalshiHttpError::IncompleteDiscovery(
                        "duplicate market across pages",
                    ));
                }
                markets.push(market);
            }

            if page.cursor.is_empty() {
                return Ok(markets);
            }

            if !seen_cursors.insert(page.cursor.clone()) {
                return Err(KalshiHttpError::IncompleteDiscovery("repeated cursor"));
            }
            cursor = page.cursor;
        }
        Err(KalshiHttpError::IncompleteDiscovery(
            "page count limit reached",
        ))
    }

    fn endpoint(&self, path: &str) -> Result<Url, KalshiHttpError> {
        self.base_url
            .join(path)
            .map_err(|_| KalshiHttpError::InvalidConfiguration("invalid endpoint URL"))
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(u64::from(self.config.operation_timeout_ms.get()))
    }

    async fn get(&self, url: Url) -> Result<HttpResponse, KalshiHttpError> {
        self.retry
            .execute_with_retry_with_delay(
                "Kalshi public metadata GET",
                || async {
                    let response = self
                        .client
                        .request_with_url_redacted(
                            Method::GET,
                            url.to_string(),
                            None,
                            None,
                            None,
                            None,
                            Some(vec![HTTP_QUOTA_KEY.to_string()]),
                        )
                        .await?;

                    if response.status.as_u16() != 200 {
                        let (retry_after, retry_after_valid) = retry_after(&response.headers);
                        return Err(KalshiHttpError::HttpStatus {
                            status: response.status.as_u16(),
                            body: response.body.to_vec(),
                            retry_after,
                            retry_after_valid,
                        });
                    }
                    Ok(response)
                },
                KalshiHttpError::should_retry,
                KalshiHttpError::retry_delay,
                KalshiHttpError::Retry,
            )
            .await
    }
}

fn retry_after(headers: &HashMap<String, String>) -> (Option<Duration>, bool) {
    let Some(value) = headers.get("retry-after") else {
        return (None, true);
    };
    // A delay we cannot interpret never permits an earlier automatic retry.
    let delay = value.trim().parse::<u64>().ok().map(Duration::from_secs);
    (delay, delay.is_some())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[tokio::test]
    async fn cloned_clients_charge_the_same_quota_before_transport() {
        let client = KalshiHttpClient::new(KalshiHttpConfig {
            base_url: "https://example.test/trade-api/v2".to_string(),
            request_timeout_ms: 100.try_into().unwrap(),
            operation_timeout_ms: 100.try_into().unwrap(),
            max_response_bytes: 1024.try_into().unwrap(),
            max_pages: 1.try_into().unwrap(),
            max_markets: 1.try_into().unwrap(),
            page_size: 1.try_into().unwrap(),
            request_spacing_ms: 5_000.try_into().unwrap(),
            max_retries: 0,
        })
        .unwrap();
        // The unsupported scheme fails before I/O, after acquiring any request quota.
        let url = Url::parse("about:blank").unwrap();
        assert!(matches!(
            client.get(url.clone()).await,
            Err(KalshiHttpError::Transport(HttpClientError::Error(_)))
        ));
        let result = client.clone().get(url).await;
        assert!(
            matches!(result, Err(KalshiHttpError::Retry(_))),
            "{result:?}"
        );
    }
}
