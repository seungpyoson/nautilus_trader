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

//! Native book scenarios derive from the captured metadata and documented WebSocket examples.
//! Tickers, quantities, sequences and prices are mutated explicitly to exercise the boundary.

mod engine;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use engine::{Books, Publication};
use nautilus_common::messages::DataEvent;
use nautilus_model::{
    data::{Data, OrderBookDeltas},
    enums::{BookAction, BookType, OrderSide, RecordFlag},
    instruments::{BinaryOption, Instrument, PriceGrid},
    orderbook::OrderBook,
    types::{Price, Quantity},
};
use rstest::rstest;
use serde_json::{Value, json};

use crate::{
    KalshiOrderbookMessage, KalshiWebSocketEvent, decode_market_response, decode_orderbook_message,
    parse_instrument,
};

const TICKER: &str = "FED-23DEC-T3.00";

fn instrument(ticker: &str) -> BinaryOption {
    let mut market: Value = serde_json::from_slice::<Value>(include_bytes!(
        "../test_data/markets.json"
    ))
    .unwrap()["markets"][0]
        .clone();
    market["ticker"] = json!(ticker);
    let metadata = decode_market_response(
        &serde_json::to_vec(&json!({"market":market})).unwrap(),
        ticker,
        NonZeroUsize::new(65536).unwrap(),
    )
    .unwrap();
    parse_instrument(&metadata, 1.into()).unwrap()
}

fn snapshot(ticker: &str) -> Value {
    let mut frame: Value =
        serde_json::from_slice(include_bytes!("../test_data/ws_orderbook_snapshot.json")).unwrap();
    frame["msg"]["market_ticker"] = json!(ticker);

    // The documented example uses legacy NO prices; the client requests YES-leg prices
    frame["msg"]["no_dollars_fp"][0][0] = json!("0.4600");
    frame["msg"]["no_dollars_fp"][1][0] = json!("0.4400");
    frame
}

fn delta(price: &str, change: &str, side: &str) -> Value {
    let mut frame: Value =
        serde_json::from_slice(include_bytes!("../test_data/ws_orderbook_delta.json")).unwrap();
    frame["msg"]["price_dollars"] = json!(price);
    frame["msg"]["delta_fp"] = json!(change);
    frame["msg"]["side"] = json!(side);
    frame
}

fn decode(frame: &Value) -> KalshiOrderbookMessage {
    decode_orderbook_message(&serde_json::to_vec(frame).unwrap()).unwrap()
}

fn event(frame: &Value) -> KalshiWebSocketEvent {
    KalshiWebSocketEvent::Book {
        connection_epoch: 0,
        message: decode(frame),
    }
}

fn take_deltas(receiver: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>) -> OrderBookDeltas {
    match receiver.try_recv().unwrap() {
        DataEvent::Data(Data::Deltas(deltas)) => (*deltas).clone(),
        other => panic!("Unexpected event: {other:?}"),
    }
}

#[rstest]
fn native_snapshot_preserves_exact_levels_sides_identity_and_event_boundary() {
    let instrument = instrument(TICKER);
    let mut books = Books::new(std::slice::from_ref(&instrument)).unwrap();
    let deltas = books.apply(&decode(&snapshot(TICKER)), 100.into()).unwrap();
    assert!(books.is_ready());
    assert_eq!(deltas.instrument_id, instrument.id);
    assert_eq!(deltas.deltas.len(), 5);
    assert_eq!(deltas.deltas[0].action, BookAction::Clear);

    for (index, delta) in deltas.deltas.iter().enumerate() {
        assert_eq!(delta.instrument_id, instrument.id);
        assert_eq!(delta.sequence, 2);
        assert_eq!(delta.ts_event.as_u64(), 100);
        assert_eq!(delta.ts_init.as_u64(), 100);
        assert_eq!(
            delta.flags,
            RecordFlag::F_SNAPSHOT as u8
                | if index == 4 {
                    RecordFlag::F_LAST as u8
                } else {
                    0
                }
        );
    }
    let mut downstream = OrderBook::new(instrument.id, BookType::L2_MBP);
    downstream.apply_deltas(&deltas).unwrap();
    assert_eq!(downstream.best_bid_price(), Some(Price::from("0.2200")));
    assert_eq!(downstream.best_bid_size(), Some(Quantity::from("333.00")));
    assert_eq!(downstream.best_ask_price(), Some(Price::from("0.4400")));
    assert_eq!(downstream.best_ask_size(), Some(Quantity::from("146.00")));
}

#[rstest]
#[case("yes", "0.2200", "333.00", OrderSide::Buy)]
#[case("no", "0.4400", "146.00", OrderSide::Sell)]
fn additive_changes_use_the_correct_native_side_and_absolute_quantities(
    #[case] side: &str,
    #[case] price: &str,
    #[case] initial: &str,
    #[case] order_side: OrderSide,
) {
    let instrument = instrument(TICKER);
    let mut books = Books::new(std::slice::from_ref(&instrument)).unwrap();
    books.apply(&decode(&snapshot(TICKER)), 100.into()).unwrap();
    let mut downstream = OrderBook::new(instrument.id, BookType::L2_MBP);
    downstream
        .apply_deltas(&books.snapshot(instrument.id, 101.into()).unwrap())
        .unwrap();
    let changed = books
        .apply(&decode(&delta(price, "0.25", side)), 200.into())
        .unwrap();
    let expected =
        initial.parse::<rust_decimal::Decimal>().unwrap() + rust_decimal::Decimal::new(25, 2);
    assert_eq!(changed.deltas[0].order.size.as_decimal(), expected);
    assert_eq!(changed.deltas[0].order.side, order_side);
    assert_eq!(changed.deltas[0].action, BookAction::Update);
    assert_eq!(changed.deltas[0].flags, RecordFlag::F_LAST as u8);
    assert_eq!(changed.ts_event.as_u64(), 1_669_149_841_000_000_000);
    assert_eq!(changed.ts_init.as_u64(), 200);
    downstream.apply_deltas(&changed).unwrap();

    let removed = books
        .apply(
            &decode(&delta(price, &(-expected).to_string(), side)),
            201.into(),
        )
        .unwrap();
    assert_eq!(removed.deltas[0].action, BookAction::Delete);
    assert!(removed.deltas[0].order.size.is_zero());
    downstream.apply_deltas(&removed).unwrap();
    let added = books
        .apply(&decode(&delta(price, "1.25", side)), 202.into())
        .unwrap();
    assert_eq!(added.deltas[0].action, BookAction::Add);
    assert_eq!(added.deltas[0].order.size, Quantity::from("1.25"));
    downstream.apply_deltas(&added).unwrap();
    let opposite = order_side.as_specified().opposite().as_order_side();
    assert_eq!(
        downstream.get_quantity_at_level(Price::from(price), opposite, 2),
        Quantity::from("1.25")
    );
}

#[rstest]
#[case("0.0001", true)]
#[case("0.0099", true)]
#[case("0.0100", true)]
#[case("0.0105", false)]
#[case("0.9890", true)]
#[case("0.9901", true)]
#[case("0.9999", true)]
#[case("0.0000", false)]
#[case("1.0000", false)]
#[case("0.22001", false)]
fn native_prices_follow_all_metadata_bands_without_rounding(
    #[case] price: &str,
    #[case] accepted: bool,
) {
    let mut frame = snapshot(TICKER);
    frame["msg"]["yes_dollars_fp"] = json!([[price, "1.00"]]);
    frame["msg"]["no_dollars_fp"] = json!([]);
    let mut books = Books::new(&[instrument(TICKER)]).unwrap();
    assert_eq!(books.apply(&decode(&frame), 1.into()).is_ok(), accepted);
    assert_eq!(books.is_ready(), accepted);
}

#[rstest]
#[case("0.2200", "-333.01", "yes")]
#[case("0.1000", "-0.01", "yes")]
#[case("0.4400", "-146.01", "no")]
#[case("0.2200", "0.001", "yes")]
#[case("0.0105", "1.00", "yes")]
#[case("0.9900", "1.00", "yes")]
#[case("0.0010", "1.00", "no")]
#[case("0.2200", "79228162514264337593543950335", "yes")]
fn invalid_delta_invalidates_every_native_book(
    #[case] price: &str,
    #[case] change: &str,
    #[case] side: &str,
) {
    let a = instrument(TICKER);
    let b = instrument("SECOND");
    let mut books = Books::new(&[a.clone(), b.clone()]).unwrap();
    books.apply(&decode(&snapshot(TICKER)), 1.into()).unwrap();
    books.apply(&decode(&snapshot("SECOND")), 1.into()).unwrap();
    assert!(books.is_ready());
    assert!(
        books
            .apply(&decode(&delta(price, change, side)), 2.into())
            .is_err()
    );
    assert!(!books.is_ready());
    assert!(books.snapshot(a.id, 3.into()).is_none());
    assert!(books.snapshot(b.id, 3.into()).is_none());
    assert!(
        books
            .apply(&decode(&delta("0.2200", "1.00", "yes")), 4.into())
            .is_err()
    );
}

#[rstest]
fn duplicate_snapshot_levels_cannot_replace_or_double_count_liquidity() {
    let mut books = Books::new(&[instrument(TICKER)]).unwrap();
    books.apply(&decode(&snapshot(TICKER)), 1.into()).unwrap();
    let mut duplicate = snapshot(TICKER);
    let level = duplicate["msg"]["yes_dollars_fp"][0].clone();
    duplicate["msg"]["yes_dollars_fp"]
        .as_array_mut()
        .unwrap()
        .push(level);
    assert!(books.apply(&decode(&duplicate), 2.into()).is_err());
    assert!(!books.is_ready());
}

#[rstest]
fn empty_and_zero_liquidity_snapshots_end_with_a_snapshot_clear() {
    let mut books = Books::new(&[instrument(TICKER)]).unwrap();
    let mut frame = snapshot(TICKER);
    frame["msg"]["yes_dollars_fp"] = json!([["0.0800", "0.00"]]);
    frame["msg"]["no_dollars_fp"] = json!([]);
    let deltas = books.apply(&decode(&frame), 99.into()).unwrap();
    assert!(books.is_ready());
    assert_eq!(deltas.deltas.len(), 1);
    assert_eq!(deltas.deltas[0].action, BookAction::Clear);
    assert_eq!(
        deltas.flags,
        RecordFlag::F_SNAPSHOT as u8 | RecordFlag::F_LAST as u8
    );
}

#[rstest]
#[case::consumer_before_feed(true)]
#[case::consumer_after_feed(false)]
fn engine_cache_applies_each_frame_once_and_replays_without_mutation(
    #[case] subscribe_first: bool,
) {
    use nautilus_common::messages::data::{
        SubscribeBookDeltas, SubscribeCommand, UnsubscribeBookDeltas, UnsubscribeCommand,
    };
    use nautilus_model::identifiers::{ClientId, Venue};

    let instrument = instrument(TICKER);
    let id = instrument.id;
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let ready = Arc::new(AtomicBool::new(false));
    let mut publication = Publication::new(&[instrument], sender, ready).unwrap();
    let subscribe = SubscribeBookDeltas {
        instrument_id: id,
        book_type: BookType::L2_MBP,
        client_id: Some(ClientId::from("KALSHI")),
        venue: Some(Venue::new("KALSHI")),
        command_id: Default::default(),
        ts_init: 1.into(),
        depth: None,
        managed: true,
        correlation_id: None,
        params: None,
    };

    if subscribe_first {
        publication
            .engine
            .execute_subscribe(SubscribeCommand::BookDeltas(subscribe.clone()))
            .unwrap();
        publication.subscribe(id, 1.into()).unwrap();
    }
    publication
        .handle(event(&snapshot(TICKER)), 2.into())
        .unwrap();
    assert_eq!(
        publication
            .cache
            .borrow()
            .order_book(&id)
            .unwrap()
            .update_count,
        5
    );

    if !subscribe_first {
        assert!(receiver.try_recv().is_err());
        publication
            .engine
            .execute_subscribe(SubscribeCommand::BookDeltas(subscribe.clone()))
            .unwrap();
        publication.subscribe(id, 3.into()).unwrap();
    }
    take_deltas(&mut receiver);
    assert_eq!(
        publication
            .cache
            .borrow()
            .order_book(&id)
            .unwrap()
            .update_count,
        5
    );
    publication
        .handle(event(&delta("0.2200", "0.25", "yes")), 4.into())
        .unwrap();
    assert_eq!(
        take_deltas(&mut receiver).deltas[0].order.size,
        Quantity::from("333.25")
    );
    assert_eq!(
        publication
            .cache
            .borrow()
            .order_book(&id)
            .unwrap()
            .update_count,
        6
    );
    publication
        .engine
        .execute_unsubscribe(&UnsubscribeCommand::BookDeltas(UnsubscribeBookDeltas {
            instrument_id: id,
            client_id: subscribe.client_id,
            venue: subscribe.venue,
            command_id: Default::default(),
            ts_init: 5.into(),
            correlation_id: None,
            params: None,
        }))
        .unwrap();
    publication.unsubscribe(id);
    publication
        .handle(event(&delta("0.2200", "0.25", "yes")), 6.into())
        .unwrap();
    assert!(receiver.try_recv().is_err());
    assert_eq!(
        publication
            .cache
            .borrow()
            .order_book(&id)
            .unwrap()
            .best_bid_size(),
        Some(Quantity::from("333.50"))
    );
    publication
        .engine
        .execute_subscribe(SubscribeCommand::BookDeltas(subscribe))
        .unwrap();
    publication.subscribe(id, 7.into()).unwrap();
    let replay = take_deltas(&mut receiver);
    assert!(RecordFlag::F_SNAPSHOT.matches(replay.flags));
    assert_eq!(
        publication
            .cache
            .borrow()
            .order_book(&id)
            .unwrap()
            .update_count,
        7
    );
    publication
        .handle(event(&delta("0.2200", "0.25", "yes")), 8.into())
        .unwrap();
    assert_eq!(
        take_deltas(&mut receiver).deltas[0].order.size,
        Quantity::from("333.75")
    );
    assert_eq!(
        publication
            .cache
            .borrow()
            .order_book(&id)
            .unwrap()
            .update_count,
        8
    );
}

#[rstest]
fn invalidation_fences_queued_old_frames_and_old_close_cannot_clear_replacement() {
    let instrument = instrument(TICKER);
    let id = instrument.id;
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let ready = Arc::new(AtomicBool::new(false));
    let mut publication = Publication::new(&[instrument], sender, Arc::clone(&ready)).unwrap();
    publication.subscribe(id, 1.into()).unwrap();
    publication
        .handle(event(&snapshot(TICKER)), 2.into())
        .unwrap();
    take_deltas(&mut receiver);
    publication
        .inner
        .handle(event(&delta("0.2200", "999.00", "yes")), 3.into())
        .unwrap();
    publication.inner.invalidate(4.into()).unwrap();
    let DataEvent::BookFeed(old_frame) = publication.receiver.try_recv().unwrap() else {
        panic!("Expected frame")
    };
    let DataEvent::BookFeed(old_close) = publication.receiver.try_recv().unwrap() else {
        panic!("Expected close")
    };
    publication.engine.process(&old_frame);
    assert_eq!(
        take_deltas(&mut receiver).deltas[0].action,
        BookAction::Clear
    );
    assert!(!ready.load(Ordering::Acquire));
    let mut replacement = snapshot(TICKER);
    replacement["msg"]["yes_dollars_fp"][1][1] = json!("7.00");
    publication.handle(event(&replacement), 5.into()).unwrap();
    take_deltas(&mut receiver);
    publication.engine.process(&old_close);
    publication.engine.process(&old_frame);
    assert!(receiver.try_recv().is_err());
    assert!(ready.load(Ordering::Acquire));
    assert_eq!(
        publication
            .cache
            .borrow()
            .order_book(&id)
            .unwrap()
            .best_bid_size(),
        Some(Quantity::from("7.00"))
    );
}

#[rstest]
fn engine_backlog_overflow_clears_once_and_requires_a_new_snapshot() {
    let instrument = instrument(TICKER);
    let id = instrument.id;
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let ready = Arc::new(AtomicBool::new(false));
    let mut publication = Publication::new(&[instrument], sender, Arc::clone(&ready)).unwrap();
    publication.subscribe(id, 1.into()).unwrap();
    publication
        .handle(event(&snapshot(TICKER)), 2.into())
        .unwrap();
    take_deltas(&mut receiver);

    for _ in 0..66 {
        publication
            .inner
            .handle(event(&delta("0.2200", "0.25", "yes")), 3.into())
            .unwrap();
    }
    assert!(
        publication
            .inner
            .handle(event(&delta("0.2200", "0.25", "yes")), 3.into())
            .is_err()
    );
    assert!(!ready.load(Ordering::Acquire));
    publication.invalidate(4.into()).unwrap();
    assert_eq!(
        take_deltas(&mut receiver).deltas[0].action,
        BookAction::Clear
    );
    assert!(receiver.try_recv().is_err());
    assert!(
        !publication
            .cache
            .borrow()
            .order_book(&id)
            .unwrap()
            .has_bid()
    );
    assert!(
        publication
            .handle(event(&delta("0.2200", "0.25", "yes")), 5.into())
            .is_err()
    );
    assert!(receiver.try_recv().is_err());
    publication
        .handle(event(&snapshot(TICKER)), 6.into())
        .unwrap();
    assert!(RecordFlag::F_SNAPSHOT.matches(take_deltas(&mut receiver).flags));
    assert!(ready.load(Ordering::Acquire));
    assert_eq!(
        publication
            .cache
            .borrow()
            .order_book(&id)
            .unwrap()
            .best_bid_size(),
        Some(Quantity::from("333.00"))
    );
}

#[rstest]
fn timestamps_preserve_source_or_explicitly_use_receipt_time_and_reject_overflow() {
    let mut books = Books::new(&[instrument(TICKER)]).unwrap();
    books.apply(&decode(&snapshot(TICKER)), 1.into()).unwrap();
    let mut frame = delta("0.2200", "0.01", "yes");
    frame["msg"].as_object_mut().unwrap().remove("ts_ms");
    let deltas = books.apply(&decode(&frame), 123.into()).unwrap();
    assert_eq!(deltas.ts_event.as_u64(), 123);
    assert_eq!(deltas.ts_init.as_u64(), 123);
    frame["msg"]["ts_ms"] = json!(u64::MAX);
    assert!(books.apply(&decode(&frame), 124.into()).is_err());
    assert!(!books.is_ready());
}

#[rstest]
fn publication_replays_current_state_and_clears_once_before_recovery() {
    let instrument = instrument(TICKER);
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let ready = Arc::new(AtomicBool::new(false));
    let mut publication = Publication::new(
        std::slice::from_ref(&instrument),
        sender,
        Arc::clone(&ready),
    )
    .unwrap();
    publication
        .handle(event(&snapshot(TICKER)), 1.into())
        .unwrap();
    publication
        .handle(event(&delta("0.2200", "0.25", "yes")), 2.into())
        .unwrap();
    assert!(receiver.try_recv().is_err());
    publication.subscribe(instrument.id, 3.into()).unwrap();
    let current = take_deltas(&mut receiver);
    let mut downstream = OrderBook::new(instrument.id, BookType::L2_MBP);
    downstream.apply_deltas(&current).unwrap();
    assert_eq!(downstream.best_bid_size(), Some(Quantity::from("333.25")));
    publication.subscribe(instrument.id, 4.into()).unwrap();
    assert!(receiver.try_recv().is_err());

    publication.invalidate(5.into()).unwrap();
    let clear = take_deltas(&mut receiver);
    assert_eq!(clear.deltas.len(), 1);
    assert_eq!(clear.flags, RecordFlag::F_LAST as u8);
    assert_eq!(clear.sequence, 0);
    downstream.apply_deltas(&clear).unwrap();
    assert!(!downstream.has_bid());
    assert!(!downstream.has_ask());
    publication.invalidate(6.into()).unwrap();
    assert!(receiver.try_recv().is_err());
    assert!(!publication.is_ready());
    assert!(!ready.load(Ordering::Acquire));
    publication
        .handle(event(&snapshot(TICKER)), 7.into())
        .unwrap();
    assert_eq!(take_deltas(&mut receiver).deltas.len(), 5);
    publication.unsubscribe(instrument.id);
    publication
        .handle(event(&delta("0.2200", "0.25", "yes")), 8.into())
        .unwrap();
    assert!(receiver.try_recv().is_err());
}

#[rstest]
fn rejected_book_update_publishes_only_invalidation_then_requires_snapshot() {
    let instrument = instrument(TICKER);
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let ready = Arc::new(AtomicBool::new(false));
    let mut publication = Publication::new(
        std::slice::from_ref(&instrument),
        sender,
        Arc::clone(&ready),
    )
    .unwrap();
    publication.subscribe(instrument.id, 1.into()).unwrap();
    publication
        .handle(event(&snapshot(TICKER)), 2.into())
        .unwrap();
    take_deltas(&mut receiver);
    assert!(
        publication
            .handle(event(&delta("0.2200", "-334.00", "yes")), 3.into())
            .is_err()
    );
    let clear = take_deltas(&mut receiver);
    assert_eq!(clear.deltas[0].action, BookAction::Clear);
    assert!(!publication.is_ready());
    assert!(!ready.load(Ordering::Acquire));
    assert!(
        publication
            .handle(event(&delta("0.2200", "1.00", "yes")), 4.into())
            .is_err()
    );
    assert!(receiver.try_recv().is_err());
}

#[rstest]
#[case::descriptive(false, false)]
#[case::activation(true, false)]
#[case::expiration(false, true)]
fn metadata_refresh_keeps_liquidity_for_unchanged_book_rules(
    #[case] activation: bool,
    #[case] expiration: bool,
) {
    let original = instrument(TICKER);
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let ready = Arc::new(AtomicBool::new(false));
    let mut publication =
        Publication::new(std::slice::from_ref(&original), sender, Arc::clone(&ready)).unwrap();
    let definitions = publication.definitions();
    publication.subscribe(original.id, 1.into()).unwrap();
    publication
        .handle(event(&snapshot(TICKER)), 2.into())
        .unwrap();
    let mut downstream = OrderBook::new(original.id, BookType::L2_MBP);
    downstream
        .apply_deltas(&take_deltas(&mut receiver))
        .unwrap();

    let mut fresh = original;
    fresh.ts_init = 3.into();
    fresh.ts_event = 4.into();
    fresh.description = Some("Updated description".into());
    if activation {
        fresh.activation_ns = (fresh.activation_ns.as_u64() + 1).into();
    }

    if expiration {
        fresh.expiration_ns = (fresh.expiration_ns.as_u64() + 1).into();
    }
    fresh
        .info
        .as_mut()
        .unwrap()
        .insert("kalshi_market_json".into(), json!("fresh venue metadata"));
    assert!(!publication.refresh(&[fresh.clone()], 5.into()).unwrap());
    assert!(ready.load(Ordering::Acquire));
    let DataEvent::Instrument(published) = receiver.try_recv().unwrap() else {
        panic!("Expected refreshed instrument");
    };
    assert_eq!(published.ts_init().as_u64(), 3);
    assert_eq!(definitions.borrow()[0].activation_ns, fresh.activation_ns);
    assert_eq!(definitions.borrow()[0].expiration_ns, fresh.expiration_ns);
    assert_eq!(definitions.borrow()[0].ts_init.as_u64(), 3);
    publication
        .handle(event(&delta("0.2200", "0.25", "yes")), 6.into())
        .unwrap();
    let update = take_deltas(&mut receiver);
    assert_eq!(update.deltas[0].action, BookAction::Update);
    downstream.apply_deltas(&update).unwrap();
    assert_eq!(downstream.best_bid_size(), Some(Quantity::from("333.25")));
    assert!(receiver.try_recv().is_err());
}

#[derive(Clone, Copy, Debug)]
enum DefinitionChange {
    Grid,
    PricePrecision,
    SizeIncrement,
    SizePrecision,
    ExchangeIndex,
}

#[rstest]
#[case(DefinitionChange::Grid)]
#[case(DefinitionChange::PricePrecision)]
#[case(DefinitionChange::SizeIncrement)]
#[case(DefinitionChange::SizePrecision)]
#[case(DefinitionChange::ExchangeIndex)]
fn metadata_rule_change_clears_all_books_before_definition_and_fresh_snapshots(
    #[case] change: DefinitionChange,
) {
    let mut first = instrument(TICKER);
    let second = instrument("OTHER");
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let ready = Arc::new(AtomicBool::new(false));
    let mut publication =
        Publication::new(&[first.clone(), second.clone()], sender, Arc::clone(&ready)).unwrap();

    for definition in [&first, &second] {
        publication.subscribe(definition.id, 1.into()).unwrap();
        publication
            .handle(event(&snapshot(definition.raw_symbol.as_str())), 2.into())
            .unwrap();
        take_deltas(&mut receiver);
    }

    match change {
        DefinitionChange::Grid => {
            first.price_grid = Some(
                PriceGrid::new(vec![(
                    Price::from("0.02"),
                    Price::from("0.98"),
                    Price::from("0.02"),
                )])
                .unwrap(),
            );
            first.price_precision = 2;
            first.price_increment = Price::from("0.02");
            first.min_price = Some(Price::from("0.02"));
            first.max_price = Some(Price::from("0.98"));
        }
        DefinitionChange::PricePrecision => {
            let precision = first.price_precision + 1;
            let grid = PriceGrid::new(
                first
                    .price_grid
                    .as_ref()
                    .unwrap()
                    .ranges()
                    .iter()
                    .map(|&(first, last, step)| {
                        let rescale = |price: Price| {
                            Price::from_decimal_dp(price.as_decimal(), precision).unwrap()
                        };
                        (rescale(first), rescale(last), rescale(step))
                    })
                    .collect(),
            )
            .unwrap();
            assert_eq!(first.price_grid.as_ref(), Some(&grid));
            first.price_precision = precision;
            first.price_increment = grid.min_increment();
            first.min_price = Some(grid.min_price());
            first.max_price = Some(grid.max_price());
            first.price_grid = Some(grid);
        }
        DefinitionChange::SizeIncrement => first.size_increment = Quantity::from("0.05"),
        DefinitionChange::SizePrecision => first.size_precision = 3,
        DefinitionChange::ExchangeIndex => {
            first
                .info
                .as_mut()
                .unwrap()
                .insert("exchange_index".into(), json!(u64::MAX));
        }
    }
    assert!(publication.refresh(&[first.clone()], 3.into()).unwrap());
    assert!(!publication.is_ready());
    assert!(!ready.load(Ordering::Acquire));
    let mut cleared = Vec::new();

    for _ in 0..2 {
        let clear = take_deltas(&mut receiver);
        assert_eq!(clear.deltas.len(), 1);
        assert_eq!(clear.deltas[0].action, BookAction::Clear);
        assert_eq!(clear.flags, RecordFlag::F_LAST as u8);
        cleared.push(clear.instrument_id);
    }
    assert!(cleared.contains(&first.id));
    assert!(cleared.contains(&second.id));
    assert!(matches!(
        receiver.try_recv().unwrap(),
        DataEvent::Instrument(_)
    ));
    assert!(
        publication
            .handle(event(&delta("0.2200", "0.25", "yes")), 4.into())
            .is_err()
    );
    assert!(receiver.try_recv().is_err());

    for definition in [&first, &second] {
        publication
            .handle(event(&snapshot(definition.raw_symbol.as_str())), 5.into())
            .unwrap();
        let snapshot = take_deltas(&mut receiver);
        assert!(RecordFlag::F_SNAPSHOT.matches(snapshot.flags));
        assert_eq!(
            snapshot.deltas[1].order.price.precision,
            definition.price_precision
        );
        assert_eq!(
            snapshot.deltas[1].order.size.precision,
            definition.size_precision
        );
    }
    assert!(ready.load(Ordering::Acquire));

    match change {
        DefinitionChange::Grid => assert!(
            publication
                .handle(event(&delta("0.2300", "1.00", "yes")), 6.into())
                .is_err()
        ),
        DefinitionChange::SizeIncrement => assert!(
            publication
                .handle(event(&delta("0.2200", "0.01", "yes")), 6.into())
                .is_err()
        ),
        _ => {}
    }
}

#[rstest]
fn instrument_requests_validate_selection_routing_history_and_parameters_without_io() {
    use nautilus_common::messages::data::{RequestInstrument, RequestInstruments};
    use nautilus_core::Params;
    use nautilus_model::identifiers::{ClientId, InstrumentId, Venue};

    use crate::requests::InstrumentRequest;

    let client_id = ClientId::from("KALSHI");
    let selection = vec![TICKER.to_string()];
    let valid = RequestInstrument::new(
        InstrumentId::from("FED-23DEC-T3.00.KALSHI"),
        None,
        None,
        Some(client_id),
        Default::default(),
        1.into(),
        None,
    );
    assert!(
        InstrumentRequest::Instrument(valid.clone())
            .validate(client_id, &selection)
            .is_ok()
    );
    let mut invalid = Vec::new();
    let mut request = valid.clone();
    request.instrument_id = InstrumentId::from("OTHER.KALSHI");
    invalid.push(request);
    let mut request = valid.clone();
    request.instrument_id = InstrumentId::from("FED-23DEC-T3.00.OTHER");
    invalid.push(request);
    let mut request = valid.clone();
    request.client_id = Some(ClientId::from("OTHER"));
    invalid.push(request);
    let mut request = valid.clone();
    request.start = Some(jiff::Timestamp::UNIX_EPOCH);
    invalid.push(request);
    let mut request = valid.clone();
    request.end = Some(jiff::Timestamp::UNIX_EPOCH);
    invalid.push(request);

    for (key, value) in [
        ("status", json!("open")),
        ("force_instrument_update", json!("true")),
        ("update_catalog", json!(1)),
        ("only_last", json!(false)),
    ] {
        let mut request = valid.clone();
        let mut params = Params::new();
        params.insert(key.into(), value);
        request.params = Some(params);
        invalid.push(request);
    }

    for request in invalid {
        assert!(
            InstrumentRequest::Instrument(request)
                .validate(client_id, &selection)
                .is_err()
        );
    }

    let mut params = Params::new();
    params.insert("only_last".into(), json!(true));
    params.insert("force_instrument_update".into(), json!(true));
    params.insert("update_catalog".into(), json!(false));
    let mut all = RequestInstruments::new(
        None,
        None,
        None,
        Some(Venue::new("KALSHI")),
        Default::default(),
        1.into(),
        Some(params),
    );
    assert!(
        InstrumentRequest::Instruments(all.clone())
            .validate(client_id, &selection)
            .is_ok()
    );
    all.venue = Some(Venue::new("OTHER"));
    assert!(
        InstrumentRequest::Instruments(all)
            .validate(client_id, &selection)
            .is_err()
    );
}
