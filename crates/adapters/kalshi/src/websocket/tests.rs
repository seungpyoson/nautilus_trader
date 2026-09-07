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

//! Lifecycle scenarios mutate documented IDs, tickers and sequences to exercise ownership rules.

use rstest::rstest;
use serde_json::{Value, json};

use super::*;

const TICKER: &str = "FED-23DEC-T3.00";

fn session() -> Session {
    Session::new(
        vec![TICKER.to_string()],
        NonZeroUsize::new(4096).unwrap(),
        Duration::from_secs(1),
    )
    .unwrap()
}

fn acknowledgement(id: u64) -> Value {
    let mut value: Value =
        serde_json::from_str(include_str!("../../test_data/ws_subscribed.json")).unwrap();
    value["id"] = json!(id);
    value["msg"]["sid"] = json!(2);
    value
}

fn snapshot() -> Value {
    serde_json::from_str(include_str!("../../test_data/ws_orderbook_snapshot.json")).unwrap()
}

fn push(
    session: &mut Session,
    epoch: u64,
    frame: &Value,
    now: tokio::time::Instant,
) -> Result<Option<KalshiWebSocketEvent>, KalshiWebSocketError> {
    session.handle_frame(epoch, &serde_json::to_vec(frame).unwrap(), now)
}

#[rstest]
#[case::identified(false)]
#[case::sole_request_without_id(true)]
fn subscribe_intent_requires_acknowledgement_then_snapshot(#[case] omit_id: bool) {
    let mut session = session();
    let now = tokio::time::Instant::now();
    let (command, deadline) = session.begin(0, now).unwrap();
    let command: Value = serde_json::from_str(&command).unwrap();
    assert_eq!(
        command,
        json!({"id":1,"cmd":"subscribe","params":{"channels":[CHANNEL],"market_tickers":[TICKER],"use_yes_price":true}})
    );
    assert_eq!(session.subscriptions.len(), 0);
    assert_eq!(
        session.subscriptions.pending_subscribe_topics(),
        vec![CHANNEL]
    );
    let mut ack = acknowledgement(1);

    if omit_id {
        ack.as_object_mut().unwrap().remove("id");
    }
    assert!(
        matches!(push(&mut session, 0, &ack, now).unwrap(), Some(KalshiWebSocketEvent::Subscribed {connection_epoch: 0, subscription_id}) if subscription_id.get() == 2)
    );
    assert_eq!(session.subscriptions.len(), 1);
    assert_eq!(session.deadline(), Some(deadline));
    assert!(matches!(
        push(&mut session, 0, &snapshot(), now).unwrap(),
        Some(KalshiWebSocketEvent::Book {
            connection_epoch: 0,
            ..
        })
    ));
    assert_eq!(session.deadline(), None);
}

#[rstest]
#[case(json!({"id":2,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":2}}))]
#[case(json!({"id":1,"type":"subscribed","msg":{"channel":"trade","sid":2}}))]
#[case(json!({"id":null,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":2}}))]
#[case(json!({"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":0}}))]
#[case(json!({"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta"}}))]
#[case(json!({"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":2,"unexpected":true}}))]
fn malformed_or_mismatched_acknowledgements_cannot_confirm(#[case] frame: Value) {
    let mut session = session();
    let now = tokio::time::Instant::now();
    session.begin(0, now).unwrap();
    assert!(push(&mut session, 0, &frame, now).is_err());
    assert_eq!(session.subscriptions.len(), 0);
    assert_eq!(
        session.subscriptions.pending_subscribe_topics(),
        vec![CHANNEL]
    );
    assert!(push(&mut session, 0, &acknowledgement(1), now).is_err());
}

#[rstest]
fn book_cannot_establish_an_unacknowledged_subscription() {
    let mut session = session();
    let now = tokio::time::Instant::now();
    session.begin(0, now).unwrap();
    assert!(push(&mut session, 0, &snapshot(), now).is_err());
    assert_eq!(session.subscriptions.len(), 0);
}

#[rstest]
fn reconnect_ignores_old_frames_and_requires_fresh_snapshot_when_sid_is_reused() {
    let mut session = session();
    let now = tokio::time::Instant::now();
    session.begin(0, now).unwrap();
    push(&mut session, 0, &acknowledgement(1), now).unwrap();
    push(&mut session, 0, &snapshot(), now).unwrap();
    session.invalidate();
    let (command, _) = session.begin(1, now).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&command).unwrap()["id"], 2);
    assert!(session.handle_frame(0, &[0; 4097], now).unwrap().is_none());
    assert!(
        push(&mut session, 0, &acknowledgement(1), now)
            .unwrap()
            .is_none()
    );
    assert_eq!(session.subscriptions.len(), 0);
    push(&mut session, 1, &acknowledgement(2), now).unwrap();
    let delta: Value =
        serde_json::from_str(include_str!("../../test_data/ws_orderbook_delta.json")).unwrap();
    assert!(matches!(
        push(&mut session, 1, &delta, now),
        Err(KalshiWebSocketError::Stream(
            KalshiStreamError::DeltaBeforeSnapshot
        ))
    ));
    assert_eq!(session.subscriptions.len(), 0);
    session.begin(2, now).unwrap();
    push(&mut session, 2, &acknowledgement(3), now).unwrap();
    let mut replacement = snapshot();
    replacement["msg"]["market_id"] = json!("9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a2");
    assert!(push(&mut session, 2, &replacement, now).unwrap().is_some());
}

#[rstest]
fn bootstrap_deadline_includes_acknowledgement_and_every_selected_market() {
    let mut session = Session::new(
        vec![TICKER.to_string(), "SECOND".to_string()],
        NonZeroUsize::new(4096).unwrap(),
        Duration::from_secs(1),
    )
    .unwrap();
    let now = tokio::time::Instant::now();
    let (_, deadline) = session.begin(0, now).unwrap();
    push(&mut session, 0, &acknowledgement(1), now).unwrap();
    push(&mut session, 0, &snapshot(), now).unwrap();
    assert_eq!(session.deadline(), Some(deadline));
    let mut second = snapshot();
    second["seq"] = json!(3);
    second["msg"]["market_ticker"] = json!("SECOND");
    second["msg"]["market_id"] = json!("9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a2");
    assert!(matches!(
        push(&mut session, 0, &second, deadline),
        Err(KalshiWebSocketError::BootstrapTimeout)
    ));
    assert_eq!(session.subscriptions.len(), 0);
}

#[rstest]
fn stale_epoch_and_duplicate_acknowledgement_do_not_replace_active_progress() {
    let mut session = session();
    let now = tokio::time::Instant::now();
    session.begin(1, now).unwrap();
    push(&mut session, 1, &acknowledgement(1), now).unwrap();
    assert!(session.begin(0, now).is_err());
    assert!(session.begin(1, now).is_err());
    assert_eq!(session.subscriptions.len(), 1);
    assert!(push(&mut session, 1, &acknowledgement(1), now).is_err());
    assert_eq!(session.subscriptions.len(), 0);
}

#[rstest]
fn stopped_session_cannot_replay_or_accept_late_acknowledgements() {
    let mut session = session();
    let now = tokio::time::Instant::now();
    session.begin(0, now).unwrap();
    session.stop();
    session.stop();
    session.invalidate();
    assert!(session.subscriptions.is_empty());
    assert!(push(&mut session, 0, &acknowledgement(1), now).is_err());
    assert!(session.begin(1, now).is_err());
}

#[rstest]
fn bootstrap_bounds_apply_before_decoding_and_do_not_echo_venue_text() {
    let mut session = session();
    let now = tokio::time::Instant::now();
    session.begin(0, now).unwrap();
    assert!(matches!(
        session.handle_frame(0, &[0; 4097], now),
        Err(KalshiWebSocketError::Stream(
            KalshiStreamError::FrameTooLarge { .. }
        ))
    ));
    session.begin(1, now).unwrap();
    let error = push(
        &mut session,
        1,
        &json!({"type":"error","msg":{"code":9,"msg":"private venue text"}}),
        now,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        KalshiWebSocketError::Stream(KalshiStreamError::Venue { code: 9 })
    ));
    assert!(!error.to_string().contains("private venue text"));
}
