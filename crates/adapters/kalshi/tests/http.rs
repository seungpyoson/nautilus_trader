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

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use nautilus_kalshi::{
    KalshiHttpClient, KalshiHttpConfig, KalshiHttpError, KalshiMarketFilter,
    KalshiMarketFilterStatus, KalshiMetadataError,
};
use nautilus_network::http::Url;
use rstest::rstest;
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle};

fn config(base_url: String) -> KalshiHttpConfig {
    KalshiHttpConfig {
        base_url,
        request_timeout_ms: 1_000.try_into().unwrap(),
        operation_timeout_ms: 5_000.try_into().unwrap(),
        max_response_bytes: 65_536.try_into().unwrap(),
        max_pages: 3.try_into().unwrap(),
        max_markets: 3.try_into().unwrap(),
        page_size: 2.try_into().unwrap(),
        request_spacing_ms: 1.try_into().unwrap(),
        max_retries: 1,
    }
}

fn market(index: usize) -> Value {
    let source: Value =
        serde_json::from_slice(include_bytes!("../test_data/markets.json")).unwrap();
    source["markets"][index].clone()
}

fn page(indices: &[usize], cursor: &str) -> Value {
    json!({"markets": indices.iter().map(|i| market(*i)).collect::<Vec<_>>(), "cursor": cursor})
}

struct Reply {
    status: StatusCode,
    body: Value,
    headers: HeaderMap,
    delay: Duration,
}

impl Reply {
    fn json(body: Value) -> Self {
        Self {
            status: StatusCode::OK,
            body,
            headers: HeaderMap::new(),
            delay: Duration::ZERO,
        }
    }

    fn status(status: u16) -> Self {
        Self {
            status: StatusCode::from_u16(status).unwrap(),
            ..Self::json(json!({"error": "fixture"}))
        }
    }
}

#[derive(Clone)]
struct ServerState {
    replies: Arc<Mutex<VecDeque<Reply>>>,
    requests: Arc<Mutex<Vec<(String, String)>>>,
}

struct Server {
    state: ServerState,
    base_url: String,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start(replies: Vec<Reply>) -> Self {
        async fn handler(State(state): State<ServerState>, request: Request) -> Response {
            state
                .requests
                .lock()
                .unwrap()
                .push((request.method().to_string(), request.uri().to_string()));
            let reply = state
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected extra request");
            tokio::time::sleep(reply.delay).await;
            (reply.status, reply.headers, axum::Json(reply.body)).into_response()
        }
        let state = ServerState {
            replies: Arc::new(Mutex::new(replies.into())),
            requests: Arc::default(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}/trade-api/v2", listener.local_addr().unwrap());
        let app = Router::new().fallback(handler).with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            state,
            base_url,
            task,
        }
    }

    fn requests(&self) -> Vec<(String, String)> {
        self.state.requests.lock().unwrap().clone()
    }
    fn client(&self) -> KalshiHttpClient {
        KalshiHttpClient::new(config(self.base_url.clone())).unwrap()
    }
}

#[rstest]
#[case("https://example.test/trade-api/v2?token=invalid")]
#[case("https://example.test/trade-api/v2#fragment")]
#[case("https://user:password@example.test/trade-api/v2")]
#[case("file:///trade-api/v2")]
#[case("not a URL")]
fn invalid_api_root_is_rejected_without_io(#[case] base_url: &str) {
    assert!(matches!(
        KalshiHttpClient::new(config(base_url.to_string())),
        Err(KalshiHttpError::InvalidConfiguration(_))
    ));
}

#[rstest]
fn page_size_above_venue_limit_is_rejected_without_io() {
    let mut config = config("https://example.test/trade-api/v2".to_string());
    config.page_size = 1_001.try_into().unwrap();
    assert!(matches!(
        KalshiHttpClient::new(config),
        Err(KalshiHttpError::InvalidConfiguration(_))
    ));
}

#[rstest]
fn config_rejects_zero_budgets_and_unknown_fields_without_io() {
    let base =
        serde_json::to_value(config("https://example.test/trade-api/v2".to_string())).unwrap();

    for key in [
        "request_timeout_ms",
        "operation_timeout_ms",
        "max_response_bytes",
        "max_pages",
        "max_markets",
        "page_size",
        "request_spacing_ms",
    ] {
        let mut value = base.clone();
        value[key] = json!(0);
        assert!(
            serde_json::from_value::<KalshiHttpConfig>(value).is_err(),
            "{key}"
        );
    }
    let mut value = base;
    value["unexpected"] = json!(true);
    assert!(serde_json::from_value::<KalshiHttpConfig>(value).is_err());
}

#[rstest]
#[case::source_ticker(None)]
#[case::encoded_segment(Some("OTHER/SHARD?x=1#fragment"))]
#[tokio::test]
async fn get_market_encodes_one_segment_and_binds_the_response(#[case] ticker: Option<&str>) {
    let mut source = market(0);
    if let Some(ticker) = ticker {
        source["ticker"] = json!(ticker);
    }
    let ticker = source["ticker"].as_str().unwrap().to_string();
    let server = Server::start(vec![Reply::json(json!({"market": source}))]).await;
    let actual = server.client().get_market(&ticker).await.unwrap();
    assert_eq!(actual.ticker, ticker);
    let request = &server.requests()[0];
    assert_eq!(request.0, "GET");
    assert!(request.1.starts_with("/trade-api/v2/markets/"));
    let url = Url::parse(&format!("http://example.test{}", request.1)).unwrap();
    assert!(url.query().is_none());
    assert!(url.fragment().is_none());
    assert_eq!(url.path_segments().unwrap().count(), 4);
}

#[rstest]
#[tokio::test]
async fn mismatched_market_is_rejected() {
    let server = Server::start(vec![Reply::json(json!({"market": market(0)}))]).await;
    assert!(matches!(
        server.client().get_market("OTHER").await,
        Err(KalshiHttpError::Metadata(
            KalshiMetadataError::MarketMismatch
        ))
    ));
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[tokio::test]
async fn discovery_preserves_filters_and_opaque_cursor_across_pages() {
    let cursor = "page/+?=&\u{96ea}";
    let server = Server::start(vec![
        Reply::json(page(&[0], cursor)),
        Reply::json(page(&[1], "")),
    ])
    .await;
    let filter = KalshiMarketFilter {
        event_ticker: Some("EVENT/A&B".to_string()),
        series_ticker: None,
        status: Some(KalshiMarketFilterStatus::Open),
    };
    let actual = server.client().discover_markets(&filter).await.unwrap();
    assert_eq!(actual.len(), 2);
    assert_eq!(actual[0].ticker, market(0)["ticker"]);
    assert_eq!(actual[1].ticker, market(1)["ticker"]);
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    for (index, (method, uri)) in requests.iter().enumerate() {
        assert_eq!(method, "GET");
        let url = Url::parse(&format!("http://example.test{uri}")).unwrap();
        assert_eq!(url.path(), "/trade-api/v2/markets");
        let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
        assert_eq!(query["event_ticker"], "EVENT/A&B");
        assert_eq!(query["status"], "open");
        assert_eq!(query["limit"], "2");
        assert!(!query.contains_key("series_ticker"));
        assert_eq!(
            query.get("cursor").map(|v| v.as_ref()),
            (index == 1).then_some(cursor)
        );
    }
}

#[rstest]
#[case::duplicate_market(vec![page(&[0], "next"), page(&[0], "")])]
#[case::repeated_cursor(vec![page(&[0], "next"), page(&[1], "next")])]
#[case::cycling_empty_pages(vec![page(&[], "next"), page(&[], "next")])]
#[case::page_budget(vec![page(&[], "one"), page(&[], "two"), page(&[], "three")])]
#[tokio::test]
async fn incomplete_discovery_returns_an_error(#[case] pages: Vec<Value>) {
    let count = pages.len();
    let server = Server::start(pages.into_iter().map(Reply::json).collect()).await;
    assert!(matches!(
        server
            .client()
            .discover_markets(&KalshiMarketFilter::default())
            .await,
        Err(KalshiHttpError::IncompleteDiscovery(_))
    ));
    assert_eq!(server.requests().len(), count);
}

#[rstest]
#[tokio::test]
async fn market_budget_stops_before_an_extra_page() {
    let server = Server::start(vec![Reply::json(page(&[0, 1], "more"))]).await;
    let mut config = config(server.base_url.clone());
    config.max_markets = 2.try_into().unwrap();
    let result = KalshiHttpClient::new(config)
        .unwrap()
        .discover_markets(&KalshiMarketFilter::default())
        .await;
    assert!(matches!(
        result,
        Err(KalshiHttpError::IncompleteDiscovery(
            "market count limit reached"
        ))
    ));
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[tokio::test]
async fn an_empty_page_with_a_new_cursor_does_not_truncate_discovery() {
    let server = Server::start(vec![
        Reply::json(page(&[], "next")),
        Reply::json(page(&[1], "")),
    ])
    .await;
    assert_eq!(
        server
            .client()
            .discover_markets(&KalshiMarketFilter::default())
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(server.requests().len(), 2);
}

#[rstest]
#[case(301)]
#[case(401)]
#[case(403)]
#[case(404)]
#[tokio::test]
async fn permanent_statuses_are_preserved_without_retry_or_redirect(#[case] status: u16) {
    let mut reply = Reply::status(status);
    reply
        .headers
        .insert("location", "/redirected".parse().unwrap());
    let server = Server::start(vec![reply]).await;
    let result = server.client().get_market("ANY").await;
    match result {
        Err(KalshiHttpError::HttpStatus {
            status: actual,
            body,
            ..
        }) => {
            assert_eq!(actual, status);
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap(),
                json!({"error": "fixture"})
            );
        }
        other => panic!("unexpected result: {other:?}"),
    }
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[case(429)]
#[case(503)]
#[tokio::test]
async fn transient_statuses_retry_the_same_get(#[case] status: u16) {
    let server = Server::start(vec![
        Reply::status(status),
        Reply::json(json!({"market": market(0)})),
    ])
    .await;
    server
        .client()
        .get_market(market(0)["ticker"].as_str().unwrap())
        .await
        .unwrap();
    let requests = server.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
}

#[rstest]
#[case::cannot_fit_budget("60")]
#[case::unrecognized_hint("not a delay")]
#[tokio::test]
async fn venue_retry_delay_never_causes_an_early_retry(#[case] hint: &str) {
    let mut reply = Reply::status(429);
    reply.headers.insert("retry-after", hint.parse().unwrap());
    let server = Server::start(vec![reply]).await;
    assert!(matches!(
        server.client().get_market("ANY").await,
        Err(KalshiHttpError::HttpStatus { status: 429, .. })
    ));
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[tokio::test]
async fn malformed_metadata_is_not_retried() {
    let server = Server::start(vec![Reply::json(json!({"market": {}}))]).await;
    assert!(matches!(
        server.client().get_market("ANY").await,
        Err(KalshiHttpError::Metadata(_))
    ));
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[tokio::test]
async fn response_byte_budget_is_enforced_by_transport() {
    let server = Server::start(vec![Reply::json(json!({"market": market(0)}))]).await;
    let mut config = config(server.base_url.clone());
    config.max_response_bytes = 32.try_into().unwrap();
    assert!(matches!(
        KalshiHttpClient::new(config)
            .unwrap()
            .get_market("ANY")
            .await,
        Err(KalshiHttpError::Transport(_))
    ));
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[tokio::test]
async fn discovery_deadline_includes_in_flight_requests() {
    let mut reply = Reply::json(page(&[], ""));
    reply.delay = Duration::from_secs(5);
    let server = Server::start(vec![reply]).await;
    let mut config = config(server.base_url.clone());
    config.operation_timeout_ms = 100.try_into().unwrap();
    let client = KalshiHttpClient::new(config).unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        client.discover_markets(&KalshiMarketFilter::default()),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        Err(KalshiHttpError::DeadlineExceeded | KalshiHttpError::Retry(_))
    ));
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[tokio::test]
async fn cloned_client_shares_quota_and_wait_is_inside_the_deadline() {
    let server = Server::start(vec![Reply::json(json!({"market": market(0)}))]).await;
    let mut config = config(server.base_url.clone());
    config.request_spacing_ms = 5_000.try_into().unwrap();
    config.operation_timeout_ms = 100.try_into().unwrap();
    let client = KalshiHttpClient::new(config).unwrap();
    client
        .get_market(market(0)["ticker"].as_str().unwrap())
        .await
        .unwrap();
    assert!(matches!(
        client.clone().get_market("ANY").await,
        Err(KalshiHttpError::Retry(_))
    ));
    assert_eq!(server.requests().len(), 1);
}

#[rstest]
#[tokio::test]
async fn timed_out_get_retries_within_the_operation_budget() {
    let source = json!({"market": market(0)});
    let mut slow = Reply::json(source.clone());
    slow.delay = Duration::from_secs(5);
    let server = Server::start(vec![slow, Reply::json(source)]).await;
    let mut config = config(server.base_url.clone());
    config.request_timeout_ms = 100.try_into().unwrap();
    KalshiHttpClient::new(config)
        .unwrap()
        .get_market(market(0)["ticker"].as_str().unwrap())
        .await
        .unwrap();
    assert_eq!(server.requests().len(), 2);
}

#[rstest]
#[tokio::test]
async fn dropping_the_operation_cancels_pending_retries() {
    let server = Server::start(vec![Reply::status(503)]).await;
    let client = server.client();
    let canceled = tokio::time::timeout(Duration::from_millis(200), client.get_market("ANY")).await;
    assert!(canceled.is_err());
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert_eq!(server.requests().len(), 1);
}
