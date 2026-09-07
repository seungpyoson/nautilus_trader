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

use nautilus_kalshi::{KalshiMarketSide, KalshiOrderbookMessage, decode_orderbook_message};
use rstest::rstest;
use rust_decimal::Decimal;
use serde_json::{Value, json};

fn snapshot() -> Value {
    json!({
        "type": "orderbook_snapshot",
        "sid": 2,
        "seq": 42,
        "msg": {
            "market_ticker": "SAMPLE-EVENT",
            "market_id": "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1",
            "yes_dollars_fp": [["0.0800", "300.01"]],
            "no_dollars_fp": [["0.5600", "146.00"]]
        }
    })
}

fn delta() -> Value {
    json!({
        "type": "orderbook_delta",
        "sid": 2,
        "seq": 43,
        "msg": {
            "market_ticker": "SAMPLE-EVENT",
            "market_id": "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1",
            "price_dollars": "0.0800",
            "delta_fp": "-54.01",
            "side": "yes",
            "ts_ms": 1669149841000_u64
        }
    })
}

fn decode(value: &Value) -> Result<KalshiOrderbookMessage, serde_json::Error> {
    decode_orderbook_message(&serde_json::to_vec(value).unwrap())
}

#[rstest]
fn snapshot_preserves_subscription_identity_and_fractional_quantity() {
    let KalshiOrderbookMessage::Snapshot { sid, seq, snapshot } = decode(&snapshot()).unwrap()
    else {
        panic!("expected snapshot");
    };
    assert_eq!((sid.get(), seq.get()), (2, 42));
    assert_eq!(snapshot.market_ticker, "SAMPLE-EVENT");
    assert_eq!(snapshot.yes[0].price_dollars, Decimal::new(800, 4));
    assert_eq!(snapshot.yes[0].quantity, Decimal::new(30001, 2));
    assert_eq!(snapshot.no[0].price_dollars, Decimal::new(5600, 4));
}

#[rstest]
#[case(true, false)]
#[case(false, true)]
#[case(true, true)]
fn every_omitted_side_is_equivalent_to_an_explicit_empty_side(
    #[case] omit_yes: bool,
    #[case] omit_no: bool,
) {
    let mut omitted = snapshot();
    let mut explicit = snapshot();

    for (field, omit) in [("yes_dollars_fp", omit_yes), ("no_dollars_fp", omit_no)] {
        if omit {
            omitted["msg"].as_object_mut().unwrap().remove(field);
            explicit["msg"][field] = json!([]);
        }
    }
    assert_eq!(decode(&omitted).unwrap(), decode(&explicit).unwrap());
}

#[rstest]
#[case(json!(null))]
#[case(json!({}))]
#[case(json!([["0.08"]]))]
#[case(json!([["0.08", "1.00", "extra"]]))]
#[case(json!([[0.08, "1.00"]]))]
#[case(json!([["0.08", 1]]))]
#[case(json!([["-0.08", "1.00"]]))]
#[case(json!([["0.08", "-1.00"]]))]
fn malformed_present_sides_are_not_treated_as_empty(#[case] levels: Value) {
    let mut frame = snapshot();
    frame["msg"]["yes_dollars_fp"] = levels;
    assert!(decode(&frame).is_err());
}

#[rstest]
#[case("")]
#[case("NaN")]
#[case("inf")]
#[case("1e-2")]
#[case(" 0.10")]
#[case("+0.10")]
#[case("0.1.0")]
#[case(".10")]
#[case("0.")]
#[case("0.00000000000000000000000000001")]
#[case("79228162514264337593543950336")]
fn invalid_or_inexact_decimal_strings_are_rejected(#[case] price: &str) {
    let mut frame = snapshot();
    frame["msg"]["yes_dollars_fp"][0][0] = json!(price);
    assert!(decode(&frame).is_err(), "accepted {price}");
}

#[rstest]
#[case("0.0000000000000000000000000001")]
#[case("79228162514264337593543950335")]
fn representable_decimal_boundaries_remain_exact(#[case] quantity: &str) {
    let mut frame = snapshot();
    frame["msg"]["yes_dollars_fp"][0][1] = json!(quantity);
    let KalshiOrderbookMessage::Snapshot { snapshot, .. } = decode(&frame).unwrap() else {
        panic!("expected snapshot");
    };
    assert_eq!(
        snapshot.yes[0].quantity,
        Decimal::from_str_exact(quantity).unwrap()
    );
}

#[rstest]
#[case("", "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1")]
#[case(" SAMPLE", "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1")]
#[case("SAMPLE\nEVENT", "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1")]
#[case("SAMPLE", "not-a-uuid")]
#[case("SAMPLE", "00000000-0000-0000-0000-000000000000")]
fn invalid_market_identities_are_rejected(#[case] ticker: &str, #[case] market_id: &str) {
    for mut frame in [snapshot(), delta()] {
        frame["msg"]["market_ticker"] = json!(ticker);
        frame["msg"]["market_id"] = json!(market_id);
        assert!(decode(&frame).is_err());
    }
}

#[rstest]
#[case("market_ticker")]
#[case("market_id")]
fn missing_market_identity_is_rejected(#[case] field: &str) {
    for mut frame in [snapshot(), delta()] {
        frame["msg"].as_object_mut().unwrap().remove(field);
        assert!(decode(&frame).is_err());
    }
}

#[rstest]
#[case("sid")]
#[case("seq")]
#[case("type")]
#[case("msg")]
fn missing_frame_identity_is_rejected(#[case] field: &str) {
    for mut frame in [snapshot(), delta()] {
        frame.as_object_mut().unwrap().remove(field);
        assert!(decode(&frame).is_err());
    }
}

#[rstest]
#[case(json!(0))]
#[case(json!(-1))]
#[case(json!("1"))]
fn invalid_subscription_ids_and_sequences_are_rejected(#[case] value: Value) {
    for field in ["sid", "seq"] {
        for mut frame in [snapshot(), delta()] {
            frame[field] = value.clone();
            assert!(decode(&frame).is_err());
        }
    }
}

#[rstest]
fn delta_retains_signed_quantity_and_source_time() {
    let KalshiOrderbookMessage::Delta { sid, seq, delta } = decode(&delta()).unwrap() else {
        panic!("expected delta");
    };
    assert_eq!((sid.get(), seq.get()), (2, 43));
    assert_eq!(delta.delta, Decimal::new(-5401, 2));
    assert_eq!(delta.side, KalshiMarketSide::Yes);
    assert_eq!(delta.ts_ms, Some(1669149841000));
}

#[rstest]
fn missing_current_timestamp_is_not_inferred_from_deprecated_time() {
    let mut frame = delta();
    frame["msg"].as_object_mut().unwrap().remove("ts_ms");
    frame["msg"]["ts"] = json!("2022-11-22T20:44:01Z");
    let KalshiOrderbookMessage::Delta { delta, .. } = decode(&frame).unwrap() else {
        panic!("expected delta");
    };
    assert_eq!(delta.ts_ms, None);
}

#[rstest]
#[case("ts_ms", json!(null))]
#[case("ts_ms", json!(-1))]
#[case("ts_ms", json!("1669149841000"))]
#[case("side", json!("YES"))]
#[case("side", json!("bid"))]
#[case("delta_fp", json!(54))]
#[case("price_dollars", json!("-0.01"))]
fn malformed_delta_fields_are_rejected(#[case] field: &str, #[case] value: Value) {
    let mut frame = delta();
    frame["msg"][field] = value;
    assert!(decode(&frame).is_err());
}

#[rstest]
fn documented_optional_order_metadata_does_not_change_the_book_update() {
    let baseline = delta();
    let mut frame = baseline.clone();
    frame["msg"]["client_order_id"] = json!("example-order");
    frame["msg"]["subaccount"] = json!(1);
    assert_eq!(decode(&baseline).unwrap(), decode(&frame).unwrap());
}

#[rstest]
fn retired_side_fields_cannot_be_mistaken_for_an_empty_book() {
    let mut frame = snapshot();
    let msg = frame["msg"].as_object_mut().unwrap();
    msg.remove("yes_dollars_fp");
    msg.remove("no_dollars_fp");
    msg.insert("yes".to_string(), json!([[8, 300]]));
    assert!(decode(&frame).is_err());
}

#[rstest]
fn truncated_and_trailing_frames_fail_without_retaining_partial_state() {
    let bytes = serde_json::to_vec(&snapshot()).unwrap();
    for end in 0..bytes.len() {
        assert!(decode_orderbook_message(&bytes[..end]).is_err());
    }
    let mut concatenated = bytes.clone();
    concatenated.extend_from_slice(&bytes);
    assert!(decode_orderbook_message(&concatenated).is_err());
    assert!(decode_orderbook_message(&bytes).is_ok());
}

#[rstest]
fn unrelated_message_types_are_rejected() {
    let mut frame = snapshot();
    frame["type"] = json!("ticker");
    assert!(decode(&frame).is_err());
}
