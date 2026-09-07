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
    KalshiOrderbookMessage, KalshiOrderbookStream, KalshiStreamError, KalshiStreamState,
};
use rstest::rstest;
use rust_decimal::Decimal;
use serde_json::{Value, json};

const TICKERS: [&str; 2] = ["SAMPLE-A", "SAMPLE-B"];
const IDS: [&str; 2] = [
    "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1",
    "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a2",
];

fn markets() -> Vec<String> {
    TICKERS.into_iter().map(str::to_string).collect()
}

fn stream() -> KalshiOrderbookStream {
    KalshiOrderbookStream::new(
        NonZeroU64::new(2).unwrap(),
        markets(),
        NonZeroUsize::new(4096).unwrap(),
    )
    .unwrap()
}

fn snapshot(market: usize, sequence: u64) -> Value {
    json!({
        "type": "orderbook_snapshot", "sid": 2, "seq": sequence,
        "msg": {"market_ticker": TICKERS[market], "market_id": IDS[market]}
    })
}

fn delta(market: usize, sequence: u64) -> Value {
    json!({
        "type": "orderbook_delta", "sid": 2, "seq": sequence,
        "msg": {
            "market_ticker": TICKERS[market], "market_id": IDS[market],
            "price_dollars": "0.1234", "delta_fp": "-0.01", "side": "no"
        }
    })
}

fn push(
    stream: &mut KalshiOrderbookStream,
    frame: &Value,
) -> Result<Option<KalshiOrderbookMessage>, KalshiStreamError> {
    stream.handle_frame(&serde_json::to_vec(frame).unwrap())
}

fn streaming() -> KalshiOrderbookStream {
    let mut stream = stream();
    assert!(push(&mut stream, &snapshot(0, 40)).unwrap().is_some());
    assert!(push(&mut stream, &snapshot(1, 41)).unwrap().is_some());
    assert_eq!(stream.state(), KalshiStreamState::Streaming);
    stream
}

fn assert_invalidated(stream: &mut KalshiOrderbookStream) {
    assert_eq!(stream.state(), KalshiStreamState::Invalidated);

    for frame in [snapshot(0, 1), snapshot(1, 42), delta(0, 43)] {
        assert!(matches!(
            push(stream, &frame),
            Err(KalshiStreamError::Inactive(KalshiStreamState::Invalidated))
        ));
        assert_eq!(stream.state(), KalshiStreamState::Invalidated);
    }
}

#[rstest]
fn interleaved_markets_share_sequence_but_require_individual_snapshots() {
    let mut stream = stream();
    assert_eq!(stream.state(), KalshiStreamState::AwaitingSnapshot);
    assert!(push(&mut stream, &snapshot(0, 40)).unwrap().is_some());
    assert!(push(&mut stream, &delta(0, 41)).unwrap().is_some());
    assert_eq!(stream.state(), KalshiStreamState::AwaitingSnapshot);
    assert!(push(&mut stream, &snapshot(1, 42)).unwrap().is_some());
    assert_eq!(stream.state(), KalshiStreamState::Streaming);

    let Some(KalshiOrderbookMessage::Delta { delta, .. }) =
        push(&mut stream, &delta(1, 43)).unwrap()
    else {
        panic!("expected delta");
    };
    assert_eq!(delta.price_dollars, Decimal::new(1234, 4));
    assert_eq!(delta.delta, Decimal::new(-1, 2));
    assert_eq!(delta.ts_ms, None);
}

#[rstest]
#[case(json!({"market_tickers": ["SAMPLE-B", "SAMPLE-A"]}))]
#[case(json!({"market_ids": [IDS[1], IDS[0]]}))]
#[case(json!({}))]
fn sequenced_ok_between_book_frames_preserves_continuity(#[case] membership: Value) {
    let mut stream = streaming();
    assert!(
        push(
            &mut stream,
            &json!({"type": "ok", "id": 123, "sid": 2, "seq": 42, "msg": membership})
        )
        .unwrap()
        .is_none()
    );
    assert!(push(&mut stream, &delta(0, 43)).unwrap().is_some());
    assert_eq!(stream.state(), KalshiStreamState::Streaming);
}

#[rstest]
fn reduced_ok_can_precede_initial_snapshot_without_establishing_book_state() {
    let mut stream = stream();
    assert!(
        push(&mut stream, &json!({"type": "ok", "sid": 2, "seq": 100}))
            .unwrap()
            .is_none()
    );
    assert_eq!(stream.state(), KalshiStreamState::AwaitingSnapshot);
    assert!(push(&mut stream, &snapshot(1, 101)).unwrap().is_some());
    assert_eq!(stream.state(), KalshiStreamState::AwaitingSnapshot);
    assert!(push(&mut stream, &snapshot(0, 102)).unwrap().is_some());
    assert_eq!(stream.state(), KalshiStreamState::Streaming);
}

#[rstest]
fn one_markets_snapshot_does_not_allow_another_markets_delta() {
    let mut stream = stream();
    push(&mut stream, &snapshot(0, 1)).unwrap();
    assert!(matches!(
        push(&mut stream, &delta(1, 2)),
        Err(KalshiStreamError::DeltaBeforeSnapshot)
    ));
    assert_invalidated(&mut stream);
}

#[rstest]
fn delta_cannot_initialize_a_subscription() {
    let mut stream = stream();
    assert!(matches!(
        push(&mut stream, &delta(0, 1)),
        Err(KalshiStreamError::DeltaBeforeSnapshot)
    ));
    assert_invalidated(&mut stream);
}

#[rstest]
#[case(40)]
#[case(41)]
#[case(43)]
fn gaps_duplicates_and_reordering_invalidate_all_markets(#[case] sequence: u64) {
    for frame in [
        delta(0, sequence),
        snapshot(1, sequence),
        json!({"type": "ok", "sid": 2, "seq": sequence}),
        json!({"type": "unsubscribed", "sid": 2, "seq": sequence}),
    ] {
        let mut stream = streaming();
        assert!(matches!(
            push(&mut stream, &frame),
            Err(KalshiStreamError::SequenceMismatch { expected: 42, received })
                if received == sequence
        ));
        assert_invalidated(&mut stream);
    }
}

#[rstest]
fn contiguous_refresh_snapshot_is_accepted() {
    let mut stream = streaming();
    assert!(push(&mut stream, &snapshot(0, 42)).unwrap().is_some());
    assert!(push(&mut stream, &delta(1, 43)).unwrap().is_some());
    assert_eq!(stream.state(), KalshiStreamState::Streaming);
}

#[rstest]
fn wrong_subscription_never_advances_the_stream() {
    for mut frame in [
        snapshot(0, 42),
        delta(0, 42),
        json!({"type": "ok", "sid": 2, "seq": 42}),
        json!({"type": "unsubscribed", "sid": 2, "seq": 42}),
        json!({"type": "error", "sid": 2, "seq": 42, "msg": {"code": 25, "msg": "overflow"}}),
    ] {
        let mut stream = streaming();
        frame["sid"] = json!(3);
        assert!(matches!(
            push(&mut stream, &frame),
            Err(KalshiStreamError::SubscriptionMismatch)
        ));
        assert_invalidated(&mut stream);
    }
}

#[rstest]
#[case("SAMPLE-C", IDS[0])]
#[case(TICKERS[0], IDS[1])]
#[case(TICKERS[1], IDS[0])]
fn market_ticker_and_uuid_must_match_the_same_binding(#[case] ticker: &str, #[case] id: &str) {
    for mut frame in [snapshot(0, 42), delta(0, 42)] {
        let mut stream = streaming();
        frame["msg"]["market_ticker"] = json!(ticker);
        frame["msg"]["market_id"] = json!(id);
        assert!(matches!(
            push(&mut stream, &frame),
            Err(KalshiStreamError::MarketMismatch)
        ));
        assert_invalidated(&mut stream);
    }
}

#[rstest]
#[case(json!({"market_tickers": []}))]
#[case(json!({"market_tickers": [TICKERS[0]]}))]
#[case(json!({"market_tickers": [TICKERS[0], TICKERS[0]]}))]
#[case(json!({"market_tickers": [TICKERS[0], TICKERS[1], "SAMPLE-C"]}))]
#[case(json!({"market_ids": [IDS[0]]}))]
#[case(json!({"market_ids": [IDS[0], IDS[0]]}))]
#[case(json!({"market_tickers": TICKERS, "market_ids": [IDS[1], IDS[1]]}))]
fn changed_or_ambiguous_membership_invalidates_the_fixed_subscription(#[case] membership: Value) {
    let mut stream = streaming();
    assert!(matches!(
        push(
            &mut stream,
            &json!({"type": "ok", "sid": 2, "seq": 42, "msg": membership})
        ),
        Err(KalshiStreamError::MembershipChanged)
    ));
    assert_invalidated(&mut stream);
}

#[rstest]
#[case(json!({"type": "ok", "sid": 2}))]
#[case(json!({"type": "ok", "seq": 42}))]
#[case(json!({"type": "ok", "sid": null, "seq": 42}))]
#[case(json!({"type": "ok", "sid": 2, "seq": null}))]
#[case(json!({"type": "ok", "sid": 2, "seq": 42, "msg": null}))]
#[case(json!({"type": "ok", "sid": 2, "seq": 42, "msg": {"market_tickers": null}}))]
#[case(json!({"type": "ok", "sid": 2, "seq": 42, "msg": {"market_ids": ["invalid"]}}))]
#[case(json!({"type": "ok", "sid": 2, "seq": 42, "unexpected": true}))]
#[case(json!({"type": "unsubscribed", "sid": 2}))]
#[case(json!({"type": "orderbook_delta", "sid": 2, "seq": 42, "msg": {}}))]
#[case(json!({"type": "new_message_type", "sid": 2, "seq": 42}))]
fn unsupported_or_malformed_frames_cannot_be_skipped(#[case] frame: Value) {
    let mut stream = streaming();
    assert!(push(&mut stream, &frame).is_err());
    assert_invalidated(&mut stream);
}

#[rstest]
#[case(9)]
#[case(25)]
#[case(999)]
fn scoped_and_connection_errors_invalidate_existing_snapshots(#[case] code: i64) {
    for scope in [json!({}), json!({"sid": 2}), json!({"sid": 2, "seq": 42})] {
        let mut stream = streaming();
        let mut frame = json!({"type": "error", "msg": {"code": code, "msg": "venue failure"}});
        frame
            .as_object_mut()
            .unwrap()
            .extend(scope.as_object().unwrap().clone());
        assert!(matches!(
            push(&mut stream, &frame),
            Err(KalshiStreamError::Venue { code: received }) if received == code
        ));
        assert_invalidated(&mut stream);
    }
}

#[rstest]
fn unsubscribed_is_terminal_and_repeated_shutdown_is_safe() {
    let mut stream = streaming();
    assert!(
        push(
            &mut stream,
            &json!({"type": "unsubscribed", "id": 102, "sid": 2, "seq": 42})
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(stream.state(), KalshiStreamState::Stopped);
    stream.stop();
    stream.invalidate();

    for frame in [snapshot(0, 1), delta(1, 43)] {
        assert!(matches!(
            push(&mut stream, &frame),
            Err(KalshiStreamError::Inactive(KalshiStreamState::Stopped))
        ));
        assert_eq!(stream.state(), KalshiStreamState::Stopped);
    }
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(2)]
fn disconnect_and_stop_discard_all_progress(#[case] snapshot_count: usize) {
    for stop in [false, true] {
        let mut stream = stream();

        for market in 0..snapshot_count {
            push(&mut stream, &snapshot(market, 40 + market as u64)).unwrap();
        }

        if stop {
            stream.stop();
            assert_eq!(stream.state(), KalshiStreamState::Stopped);
            assert!(push(&mut stream, &snapshot(0, 42)).is_err());
        } else {
            stream.invalidate();
            stream.invalidate();
            assert_invalidated(&mut stream);
        }
    }
}

#[rstest]
fn replacement_subscription_requires_fresh_snapshots_even_when_id_is_reused() {
    let mut previous = streaming();
    previous.invalidate();
    let mut replacement = stream();
    assert_eq!(replacement.state(), KalshiStreamState::AwaitingSnapshot);
    assert_invalidated(&mut previous);
    push(&mut replacement, &snapshot(1, 1)).unwrap();
    assert_eq!(replacement.state(), KalshiStreamState::AwaitingSnapshot);
    push(&mut replacement, &snapshot(0, 2)).unwrap();
    assert_eq!(replacement.state(), KalshiStreamState::Streaming);
}

#[rstest]
fn sequence_exhaustion_cannot_wrap() {
    let mut stream = stream();
    push(&mut stream, &snapshot(0, u64::MAX - 1)).unwrap();
    push(&mut stream, &snapshot(1, u64::MAX)).unwrap();
    assert!(matches!(
        push(&mut stream, &delta(0, 1)),
        Err(KalshiStreamError::SequenceExhausted)
    ));
    assert_invalidated(&mut stream);
}

#[rstest]
fn frame_limit_is_checked_before_json_decoding() {
    let mut stream = streaming();
    assert!(matches!(
        stream.handle_frame(&[b'x'; 4097]),
        Err(KalshiStreamError::FrameTooLarge {
            length: 4097,
            limit: 4096
        })
    ));
    assert_invalidated(&mut stream);
}

#[rstest]
fn frame_exactly_at_the_limit_is_accepted() {
    let bytes = serde_json::to_vec(&snapshot(0, 1)).unwrap();
    let mut stream = KalshiOrderbookStream::new(
        NonZeroU64::new(2).unwrap(),
        markets(),
        NonZeroUsize::new(bytes.len()).unwrap(),
    )
    .unwrap();
    assert!(stream.handle_frame(&bytes).unwrap().is_some());
}

#[rstest]
#[case(b"{\"type\":\"ok\",\"sid\":2,\"seq\":42,\"seq\":43}")]
#[case(b"{\"type\":\"ok\",\"sid\":2,\"seq\":42}{}")]
#[case(b"{\"type\":\"ok\",\"sid\":2,\"seq\":")]
fn duplicate_fields_trailing_frames_and_truncation_invalidate(#[case] bytes: &[u8]) {
    let mut stream = streaming();
    assert!(matches!(
        stream.handle_frame(bytes),
        Err(KalshiStreamError::Decode(_))
    ));
    assert_invalidated(&mut stream);
}

#[rstest]
#[case("empty")]
#[case("ticker")]
#[case("blank")]
#[case("alias")]
#[case("control")]
fn ambiguous_or_invalid_selections_are_rejected(#[case] invalid: &str) {
    let mut selection = markets();

    match invalid {
        "empty" => selection.clear(),
        "ticker" => selection[1] = selection[0].clone(),
        "blank" => selection[0] = String::new(),
        "alias" => selection[0] = " SAMPLE-A".to_string(),
        "control" => selection[0] = "SAMPLE\nA".to_string(),
        _ => panic!("unexpected invalid selection"),
    }

    assert!(matches!(
        KalshiOrderbookStream::new(
            NonZeroU64::new(2).unwrap(),
            selection,
            NonZeroUsize::new(4096).unwrap()
        ),
        Err(KalshiStreamError::InvalidConfiguration(_))
    ));
}

#[rstest]
fn initial_snapshot_cannot_reuse_another_selected_markets_uuid() {
    let mut stream = stream();
    push(&mut stream, &snapshot(0, 1)).unwrap();
    let mut second = snapshot(1, 2);
    second["msg"]["market_id"] = json!(IDS[0]);
    assert!(matches!(
        push(&mut stream, &second),
        Err(KalshiStreamError::MarketMismatch)
    ));
    assert_invalidated(&mut stream);
}

#[rstest]
fn first_snapshot_must_still_have_a_non_nil_uuid() {
    let mut stream = stream();
    let mut first = snapshot(0, 1);
    first["msg"]["market_id"] = json!("00000000-0000-0000-0000-000000000000");
    assert!(push(&mut stream, &first).is_err());
    assert_invalidated(&mut stream);
}

#[rstest]
fn membership_response_cannot_establish_snapshot_uuid_bindings() {
    let mut stream = stream();
    assert!(matches!(
        push(
            &mut stream,
            &json!({"type": "ok", "sid": 2, "seq": 1, "msg": {"market_ids": IDS}})
        ),
        Err(KalshiStreamError::MembershipChanged)
    ));
    assert_invalidated(&mut stream);
}
