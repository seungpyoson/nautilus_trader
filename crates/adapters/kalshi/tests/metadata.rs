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

use std::num::{NonZeroU64, NonZeroUsize};

use nautilus_kalshi::{
    KalshiMarketStatus, KalshiMarketType, KalshiMarketsPage, KalshiMetadataError,
    KalshiOrderbookStream, KalshiStreamState, decode_market_response, decode_markets_response,
};
use rstest::rstest;
use rust_decimal::Decimal;
use serde_json::{Value, json};

const CAPTURE: &[u8] = include_bytes!("../test_data/markets.json");

fn page() -> Value {
    serde_json::from_slice(CAPTURE).unwrap()
}

fn decode(value: &Value) -> Result<KalshiMarketsPage, KalshiMetadataError> {
    decode_markets_response(
        &serde_json::to_vec(value).unwrap(),
        NonZeroUsize::new(65536).unwrap(),
        NonZeroUsize::new(2).unwrap(),
    )
}

#[rstest]
fn public_capture_preserves_identity_routing_dates_and_all_price_bands() {
    let decoded = decode_markets_response(
        CAPTURE,
        NonZeroUsize::new(CAPTURE.len()).unwrap(),
        NonZeroUsize::new(2).unwrap(),
    )
    .unwrap();
    assert_eq!(decoded.markets.len(), 2);
    assert!(!decoded.cursor.is_empty());
    assert_ne!(decoded.markets[0].ticker, decoded.markets[1].ticker);
    assert_ne!(
        decoded.markets[0].event_ticker,
        decoded.markets[1].event_ticker
    );
    assert_ne!(decoded.markets[0].close_time, decoded.markets[1].close_time);

    for market in decoded.markets {
        assert_eq!(market.market_type, KalshiMarketType::Binary);
        assert_eq!(market.status, KalshiMarketStatus::Active);
        assert_eq!(market.exchange_index, 1);
        assert_eq!(market.notional_value_dollars, Decimal::ONE);
        assert_eq!(market.price_ranges.len(), 3);
        assert_eq!(market.price_ranges[0].step, Decimal::new(1, 4));
        assert_eq!(market.price_ranges[1].start, Decimal::new(1, 2));
        assert_eq!(market.price_ranges[1].end, Decimal::new(99, 2));
        assert_eq!(market.price_ranges[1].step, Decimal::new(1, 3));
        assert_eq!(market.price_ranges[2].step, Decimal::new(1, 4));
        assert_eq!(market.price_ranges[2].end, Decimal::ONE);
        let source: Value = serde_json::from_str(market.raw.get()).unwrap();
        assert!(source.get("market_id").is_none());
        assert_eq!(source["ticker"], market.ticker);
    }
}

#[rstest]
fn grid_labels_and_ticker_spelling_do_not_select_rules_or_routing() {
    let expected = decode(&page()).unwrap();
    let mut changed = page();
    changed["markets"][0]["price_level_structure"] = json!("a-future-grid-name");
    changed["markets"][0]["exchange_index"] = json!(19);
    let actual = decode(&changed).unwrap();
    assert_eq!(
        actual.markets[0].price_ranges,
        expected.markets[0].price_ranges
    );
    assert_eq!(actual.markets[0].ticker, expected.markets[0].ticker);
    assert_eq!(actual.markets[0].exchange_index, 19);
}

#[rstest]
#[case("ticker")]
#[case("event_ticker")]
#[case("market_type")]
#[case("status")]
#[case("open_time")]
#[case("close_time")]
#[case("latest_expiration_time")]
#[case("updated_time")]
#[case("notional_value_dollars")]
#[case("exchange_index")]
#[case("price_level_structure")]
#[case("price_ranges")]
#[case("rules_primary")]
#[case("rules_secondary")]
fn missing_or_null_required_metadata_cannot_return_a_partial_page(#[case] field: &str) {
    for null in [false, true] {
        let mut value = page();
        let last = value["markets"][1].as_object_mut().unwrap();

        if null {
            last.insert(field.to_string(), Value::Null);
        } else {
            last.remove(field);
        }

        assert!(decode(&value).is_err());
    }
}

#[rstest]
#[case(json!(-1))]
#[case(json!(4294967296_u64))]
#[case(json!("1"))]
#[case(json!(1.5))]
fn routing_requires_a_representable_nonnegative_integer(#[case] routing: Value) {
    let mut value = page();
    value["markets"][0]["exchange_index"] = routing;
    assert!(decode(&value).is_err());
}

#[rstest]
fn zero_is_a_valid_explicit_exchange_index() {
    let mut value = page();
    value["markets"][0]["exchange_index"] = json!(0);
    assert_eq!(decode(&value).unwrap().markets[0].exchange_index, 0);
}

#[rstest]
#[case(json!([]))]
#[case(json!([{"start":"0", "end":"1", "step":"0"}]))]
#[case(json!([{"start":"0", "end":"1", "step":"-0.01"}]))]
#[case(json!([{"start":"-0.1", "end":"1", "step":"0.01"}]))]
#[case(json!([{"start":"0", "end":"1.1", "step":"0.01"}]))]
#[case(json!([{"start":"1", "end":"0", "step":"0.01"}]))]
#[case(json!([{"start":"0", "end":"1", "step":"2"}]))]
#[case(json!([{"start":"0", "end":"1", "step":0.01}]))]
#[case(json!([{"start":"0", "end":"1", "step":"1e-2"}]))]
#[case(json!([{"start":"0", "end":"1", "step":"0.00000000000000000000000000001"}]))]
#[case(json!([{"start":"0", "end":"1"}]))]
#[case(json!([{"start":"0", "end":"0.5", "step":"0.01"}, {"start":"0.4", "end":"1", "step":"0.01"}]))]
fn invalid_price_bands_are_rejected_without_rounding_or_default_ticks(#[case] bands: Value) {
    let mut value = page();
    value["markets"][0]["price_ranges"] = bands;
    assert!(decode(&value).is_err());
}

#[rstest]
#[case("not-a-date")]
#[case("1969-01-01T00:00:00Z")]
#[case("2028-01-01T00:00:00Z")]
fn malformed_or_inconsistent_opening_times_are_rejected(#[case] timestamp: &str) {
    let mut value = page();
    value["markets"][0]["open_time"] = json!(timestamp);
    assert!(decode(&value).is_err());
}

#[rstest]
fn source_time_preserves_nanoseconds_and_normalizes_the_offset() {
    let mut value = page();
    value["markets"][0]["updated_time"] = json!("2026-09-05T23:58:06.123456789+09:00");
    let timestamp = decode(&value).unwrap().markets[0].updated_time;
    assert_eq!(timestamp.to_string(), "2026-09-05T14:58:06.123456789Z");
}

#[rstest]
fn unknown_classifications_are_explicit_and_preserved_in_raw_metadata() {
    let mut value = page();
    value["markets"][0]["market_type"] = json!("future-payout-type");
    value["markets"][0]["status"] = json!("future-lifecycle-state");
    let decoded = decode(&value).unwrap();
    assert_eq!(decoded.markets[0].market_type, KalshiMarketType::Unknown);
    assert_eq!(decoded.markets[0].status, KalshiMarketStatus::Unknown);
    assert!(decoded.markets[0].raw.get().contains("future-payout-type"));
}

#[rstest]
fn duplicate_tickers_and_exceeded_page_bounds_are_rejected() {
    let mut value = page();
    value["markets"][1] = value["markets"][0].clone();
    assert!(matches!(
        decode(&value),
        Err(KalshiMetadataError::DuplicateMarket)
    ));
    assert!(matches!(
        decode_markets_response(
            CAPTURE,
            NonZeroUsize::new(CAPTURE.len()).unwrap(),
            NonZeroUsize::new(1).unwrap()
        ),
        Err(KalshiMetadataError::TooManyMarkets)
    ));
    assert!(matches!(
        decode_markets_response(
            b"not-json",
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap()
        ),
        Err(KalshiMetadataError::ResponseTooLarge)
    ));
}

#[rstest]
fn empty_final_page_preserves_the_cursor_contract() {
    assert!(
        decode(&json!({"markets":[], "cursor":""}))
            .unwrap()
            .markets
            .is_empty()
    );
    assert!(decode(&json!({"markets":[]})).is_err());
    assert!(decode(&json!({"markets":[], "cursor":null})).is_err());
}

#[rstest]
fn single_market_response_binds_to_the_request_and_retains_raw_numbers() {
    let decoded = decode(&page()).unwrap();
    let first = &decoded.markets[0];
    let raw =
        first
            .raw
            .get()
            .replacen('{', "{\"future_number\":0.1234567890123456789012345678,", 1);
    let response = format!("{{\"market\":{raw}}}");
    let max_bytes = NonZeroUsize::new(response.len()).unwrap();
    let market = decode_market_response(response.as_bytes(), &first.ticker, max_bytes).unwrap();
    assert_eq!(market.raw.get(), raw);
    assert!(matches!(
        decode_market_response(response.as_bytes(), "ANOTHER-MARKET", max_bytes),
        Err(KalshiMetadataError::MarketMismatch)
    ));
}

#[rstest]
fn public_metadata_selects_stream_tickers_without_an_invented_uuid() {
    let page = decode(&page()).unwrap();
    let tickers = page.markets.iter().map(|m| m.ticker.clone()).collect();
    let stream = KalshiOrderbookStream::new(
        NonZeroU64::new(1).unwrap(),
        tickers,
        NonZeroUsize::new(4096).unwrap(),
    )
    .unwrap();
    assert_eq!(stream.state(), KalshiStreamState::AwaitingSnapshot);
}
