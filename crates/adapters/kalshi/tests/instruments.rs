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

use std::num::NonZeroUsize;

use nautilus_kalshi::{
    KalshiInstrumentError, KalshiMarketMetadata, decode_market_response, parse_instrument,
};
use nautilus_model::{
    instruments::{Instrument, InstrumentAny},
    types::{Currency, Price},
};
use rstest::rstest;
use rust_decimal::Decimal;
use serde_json::{Value, json};

const CAPTURE: &[u8] = include_bytes!("../test_data/markets.json");

fn source_market() -> Value {
    serde_json::from_slice::<Value>(CAPTURE).unwrap()["markets"][0].clone()
}

fn decode(market: &Value) -> KalshiMarketMetadata {
    let ticker = market["ticker"].as_str().unwrap().to_owned();
    decode_market_response(
        &serde_json::to_vec(&json!({"market": market})).unwrap(),
        &ticker,
        NonZeroUsize::new(65536).unwrap(),
    )
    .unwrap()
}

#[rstest]
fn captured_metadata_creates_native_instrument_with_exact_grid() {
    let market = decode(&source_market());
    let instrument = parse_instrument(&market, 42.into()).unwrap();
    assert_eq!(
        instrument.id.to_string(),
        format!("{}.KALSHI", market.ticker)
    );
    assert_eq!(instrument.raw_symbol.as_str(), market.ticker);
    assert_eq!(instrument.currency, Currency::USD());
    assert_eq!(
        instrument.activation_ns.as_u64() as i128,
        market.open_time.as_nanosecond()
    );
    assert_eq!(
        instrument.expiration_ns.as_u64() as i128,
        market.close_time.as_nanosecond()
    );
    assert_eq!(
        instrument.ts_event.as_u64() as i128,
        market.updated_time.as_nanosecond()
    );
    assert_eq!(instrument.ts_init.as_u64(), 42);
    assert_eq!(instrument.price_precision, 4);
    assert_eq!(instrument.price_increment.to_string(), "0.0001");
    assert_eq!(instrument.size_increment.to_string(), "0.01");
    let info = instrument.info.as_ref().unwrap();
    assert_eq!(info["exchange_index"], 1);
    assert_eq!(info["kalshi_market_json"].as_str(), Some(market.raw.get()));
    let grid = instrument.price_grid.as_ref().unwrap();
    assert_eq!(grid.ranges().len(), 3);
    assert_eq!(
        grid.price_from_decimal(Decimal::new(11, 3)),
        Some("0.0110".into())
    );
    assert_eq!(grid.price_from_decimal(Decimal::new(105, 4)), None);
    assert_eq!(grid.price_from_decimal(Decimal::new(1100001, 8)), None);
    assert_eq!(grid.price_from_decimal(Decimal::ZERO), None);
    assert_eq!(grid.price_from_decimal(Decimal::ONE), None);
    assert_eq!(instrument.next_bid_price(0.99, 1), Some("0.9890".into()));
    assert_eq!(instrument.next_ask_price(0.99, 1), Some("0.9901".into()));
    let serialized = serde_json::to_string(&InstrumentAny::BinaryOption(instrument)).unwrap();
    let restored: InstrumentAny = serde_json::from_str(&serialized).unwrap();
    assert_eq!(restored.next_bid_price(0.01, 1), Some("0.0099".into()));
}

#[rstest]
fn routing_and_pricing_ignore_ticker_and_grid_labels() {
    let mut source = source_market();
    source["ticker"] = json!("ARBITRARY-SHARD99-NAME");
    source["exchange_index"] = json!(7);
    source["price_level_structure"] = json!("a-future-label");
    source["price_ranges"] = json!([
        {"start":"0.000", "end":"0.100", "step":"0.002"},
        {"start":"0.100", "end":"0.900", "step":"0.005"},
        {"start":"0.900", "end":"1.000", "step":"0.002"}
    ]);
    let instrument = parse_instrument(&decode(&source), 0.into()).unwrap();
    assert_eq!(instrument.id.to_string(), "ARBITRARY-SHARD99-NAME.KALSHI");
    assert_eq!(instrument.info.as_ref().unwrap()["exchange_index"], 7);
    assert_eq!(instrument.price_precision, 3);
    assert_eq!(instrument.price_increment, Price::from("0.002"));
    assert_eq!(instrument.next_bid_price(0.1, 1), Some("0.098".into()));
    assert_eq!(instrument.next_ask_price(0.1, 1), Some("0.105".into()));
}

#[rstest]
#[case("market_type", json!("scalar"))]
#[case("market_type", json!("future-type"))]
#[case("notional_value_dollars", json!("0.5"))]
fn unsupported_payouts_are_rejected(#[case] field: &str, #[case] value: Value) {
    let mut source = source_market();
    source[field] = value;
    if field == "notional_value_dollars" {
        source["price_ranges"] = json!([{"start":"0.0", "end":"0.5", "step":"0.1"}]);
    }
    assert!(matches!(
        parse_instrument(&decode(&source), 0.into()),
        Err(KalshiInstrumentError::UnsupportedProduct)
    ));
}

#[rstest]
fn unknown_lifecycle_is_not_mapped_to_active() {
    let mut source = source_market();
    source["status"] = json!("future-state");
    assert!(matches!(
        parse_instrument(&decode(&source), 0.into()),
        Err(KalshiInstrumentError::UnknownStatus)
    ));
}

#[rstest]
#[case(json!([{"start":"0", "end":"1", "step":"0.003"}]))]
#[case(json!([{"start":"0.1", "end":"1", "step":"0.1"}]))]
#[case(json!([{"start":"0", "end":"0.9", "step":"0.1"}]))]
#[case(json!([{"start":"0", "end":"1", "step":"1"}]))]
#[case(json!([{"start":"0", "end":"1", "step":"0.0000000000000000001"}]))]
#[case(json!([
    {"start":"0", "end":"0.1", "step":"0.01"},
    {"start":"0.2", "end":"1", "step":"0.01"}
]))]
fn unsupported_grid_is_not_flattened_or_rounded(#[case] ranges: Value) {
    let mut source = source_market();
    source["price_ranges"] = ranges;
    assert!(matches!(
        parse_instrument(&decode(&source), 0.into()),
        Err(KalshiInstrumentError::UnsupportedPriceGrid)
    ));
}
