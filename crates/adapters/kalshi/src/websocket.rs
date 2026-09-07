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

//! Authenticated, fixed-selection orderbook subscriptions over the shared NT transport.

mod ingress;
#[cfg(test)]
mod tests;

use std::{
    fmt::Debug,
    future::Future,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use nautilus_network::{
    RECONNECTED, SocketState, SocketStateSink,
    error::SendError,
    http::Url,
    mode::ReconnectRequestOutcome,
    ratelimiter::{RateLimiter, quota::Quota},
    transport::{Message, TransportError},
    websocket::{SubscriptionState, WebSocketClient, WebSocketConfig},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ustr::Ustr;

use self::ingress::Ingress;
use crate::{
    KalshiAuthError, KalshiCredential, KalshiOrderbookMessage, KalshiOrderbookStream,
    KalshiStreamError, KalshiStreamState,
    orderbook::{StreamMessage, decode_stream_message},
    stream::validate_market_selection,
};

const CHANNEL: &str = "orderbook_delta";
const CONNECTION_KEY: &str = "kalshi-ws-connection";
const COMMAND_KEY: &str = "kalshi-ws-command";

/// Explicit transport, protocol and per-client quota budgets for a fixed selection.
///
/// Frames are bounded before retention, with a fixed queue capacity. Overflow invalidates the
/// connection. Quotas apply to this client, not an account-wide venue tier.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct KalshiWebSocketConfig {
    /// Shared NT transport policy; headers must be empty. No attempt limit enables ongoing recovery.
    pub transport: WebSocketConfig,
    /// Total initial connection deadline, including its connection quota wait.
    pub connect_deadline_ms: NonZeroU32,
    /// Time from subscribe intent through acknowledgement and every market's initial snapshot.
    pub bootstrap_timeout_ms: NonZeroU32,
    /// Maximum complete application frame size accepted for decoding.
    pub max_frame_bytes: NonZeroUsize,
    /// Maximum number of pending frames; retained payloads are bounded by this times frame size.
    pub max_pending_frames: NonZeroUsize,
    /// Minimum spacing between connection attempts, with a burst of one.
    pub connection_spacing_ms: NonZeroU32,
    /// Minimum spacing between subscription commands, with a burst of one.
    pub command_spacing_ms: NonZeroU32,
}

impl KalshiWebSocketConfig {
    pub(crate) fn validate(&self) -> Result<(), KalshiWebSocketError> {
        self.transport
            .validate()
            .map_err(|_| KalshiWebSocketError::Configuration("invalid transport policy"))?;
        let url = Url::parse(&self.transport.url)
            .map_err(|_| KalshiWebSocketError::Configuration("invalid WebSocket URL"))?;

        if self.transport.proxy_url.is_some() && url.scheme() != "wss" {
            return Err(KalshiWebSocketError::Configuration(
                "authenticated WebSocket proxies require TLS to the target",
            ));
        }

        if self.transport.heartbeat_interval_secs.is_none()
            && self.transport.heartbeat_timeout_secs.is_none()
            && self.transport.idle_timeout_ms.is_none()
        {
            return Err(KalshiWebSocketError::Configuration(
                "a finite heartbeat or idle timeout is required",
            ));
        }
        Ok(())
    }
}

/// Failures invalidate protocol progress before any further book messages are returned.
#[derive(Debug, Error)]
pub enum KalshiWebSocketError {
    /// The selected transport or protocol configuration is invalid.
    #[error("Invalid Kalshi WebSocket configuration: {0}")]
    Configuration(&'static str),
    /// The injected credential or signing target is invalid.
    #[error(transparent)]
    Authentication(#[from] KalshiAuthError),
    /// The shared transport could not establish a connection.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// An epoch-bound subscription send failed.
    #[error(transparent)]
    Send(#[from] SendError),
    /// The initial connection deadline expired.
    #[error("Kalshi WebSocket connection deadline expired")]
    ConnectTimeout,
    /// A subscription did not acknowledge and supply all initial snapshots in time.
    #[error("Kalshi orderbook bootstrap deadline expired")]
    BootstrapTimeout,
    /// Incoming frames exceeded the retention budget before the consumer caught up.
    #[error("Kalshi incoming frame backlog exceeded {limit} frames")]
    BacklogOverflow {
        /// The configured number of frames retained at most.
        limit: usize,
    },
    /// A command or message does not satisfy the supported wire contract.
    #[error("Invalid Kalshi WebSocket JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// A response cannot be correlated to the one outstanding subscription request.
    #[error("Invalid Kalshi subscription response: {0}")]
    Protocol(&'static str),
    /// A book or sequenced control frame failed continuity checks.
    #[error(transparent)]
    Stream(#[from] KalshiStreamError),
}

/// Protocol events carrying the ownership epoch supplied by the NT transport.
#[derive(Debug)]
pub enum KalshiWebSocketEvent {
    /// The venue acknowledged the selection; initial snapshots are still required.
    Subscribed {
        /// The owning transport connection.
        connection_epoch: u64,
        /// The server-assigned subscription ID.
        subscription_id: NonZeroU64,
    },
    /// A validated snapshot or additive delta, still requiring grid and native book processing.
    Book {
        /// The owning transport connection.
        connection_epoch: u64,
        /// The validated wire message.
        message: KalshiOrderbookMessage,
    },
    /// All progress from this connection must be discarded before recovery.
    Disconnected {
        /// The invalidated transport connection.
        connection_epoch: u64,
    },
}

#[derive(Debug)]
enum Phase {
    Disconnected,
    Subscribing {
        request_id: u64,
        deadline: tokio::time::Instant,
    },
    Subscribed {
        stream: KalshiOrderbookStream,
        deadline: Option<tokio::time::Instant>,
    },
    Stopped,
}

#[derive(Debug)]
struct Session {
    markets: Vec<String>,
    max_frame_bytes: NonZeroUsize,
    bootstrap_timeout: Duration,
    subscriptions: SubscriptionState,
    epoch: Option<u64>,
    last_request_id: u64,
    phase: Phase,
}

#[derive(Serialize)]
struct Subscribe<'a> {
    id: u64,
    cmd: &'static str,
    params: SubscribeParams<'a>,
}

#[derive(Serialize)]
struct SubscribeParams<'a> {
    channels: [&'static str; 1],
    market_tickers: &'a [String],
    use_yes_price: bool,
}

impl Session {
    fn new(
        markets: Vec<String>,
        max_frame_bytes: NonZeroUsize,
        bootstrap_timeout: Duration,
    ) -> Result<Self, KalshiWebSocketError> {
        validate_market_selection(&markets)?;
        let subscriptions = SubscriptionState::new(':');
        subscriptions.mark_subscribe(CHANNEL);
        Ok(Self {
            markets,
            max_frame_bytes,
            bootstrap_timeout,
            subscriptions,
            epoch: None,
            last_request_id: 0,
            phase: Phase::Disconnected,
        })
    }

    fn begin(
        &mut self,
        epoch: u64,
        now: tokio::time::Instant,
    ) -> Result<(String, tokio::time::Instant), KalshiWebSocketError> {
        if matches!(self.phase, Phase::Stopped) {
            return Err(KalshiWebSocketError::Protocol("session is stopped"));
        }

        if self.epoch.is_some_and(|previous| epoch <= previous) {
            return Err(KalshiWebSocketError::Protocol(
                "replacement epoch did not advance",
            ));
        }
        let request_id = self
            .last_request_id
            .checked_add(1)
            .ok_or(KalshiWebSocketError::Protocol("command ID exhausted"))?;
        let request = serde_json::to_string(&Subscribe {
            id: request_id,
            cmd: "subscribe",
            params: SubscribeParams {
                channels: [CHANNEL],
                market_tickers: &self.markets,
                use_yes_price: true,
            },
        })?;
        let deadline = now + self.bootstrap_timeout;
        self.subscriptions.reset_after_reconnect();
        self.epoch = Some(epoch);
        self.last_request_id = request_id;
        self.phase = Phase::Subscribing {
            request_id,
            deadline,
        };
        Ok((request, deadline))
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        match &self.phase {
            Phase::Subscribing { deadline, .. } => Some(*deadline),
            Phase::Subscribed { deadline, .. } => *deadline,
            Phase::Disconnected | Phase::Stopped => None,
        }
    }

    fn invalidate(&mut self) {
        if !matches!(self.phase, Phase::Stopped) {
            self.phase = Phase::Disconnected;
            self.subscriptions.mark_failure(CHANNEL);
        }
    }

    fn stop(&mut self) {
        self.phase = Phase::Stopped;
        self.subscriptions.clear();
    }

    fn handle_frame(
        &mut self,
        epoch: u64,
        bytes: &[u8],
        now: tokio::time::Instant,
    ) -> Result<Option<KalshiWebSocketEvent>, KalshiWebSocketError> {
        if self.epoch != Some(epoch) {
            return Ok(None);
        }
        let result = self.apply_frame(epoch, bytes, now);

        if result.is_err() {
            self.invalidate();
        }
        result
    }

    fn apply_frame(
        &mut self,
        epoch: u64,
        bytes: &[u8],
        now: tokio::time::Instant,
    ) -> Result<Option<KalshiWebSocketEvent>, KalshiWebSocketError> {
        if self.deadline().is_some_and(|deadline| now >= deadline) {
            return Err(KalshiWebSocketError::BootstrapTimeout);
        }

        if bytes.len() > self.max_frame_bytes.get() {
            return Err(KalshiStreamError::FrameTooLarge {
                length: bytes.len(),
                limit: self.max_frame_bytes.get(),
            }
            .into());
        }
        let message = decode_stream_message(bytes)?;

        match (&mut self.phase, message) {
            (
                Phase::Subscribing {
                    request_id,
                    deadline,
                },
                StreamMessage::Subscribed { id, channel, sid },
            ) => {
                if channel != CHANNEL || id.is_some_and(|id| id != *request_id) {
                    return Err(KalshiWebSocketError::Protocol(
                        "acknowledgement does not match request",
                    ));
                }
                let stream =
                    KalshiOrderbookStream::new(sid, self.markets.clone(), self.max_frame_bytes)?;
                self.phase = Phase::Subscribed {
                    stream,
                    deadline: Some(*deadline),
                };
                self.subscriptions.confirm_subscribe(CHANNEL);
                Ok(Some(KalshiWebSocketEvent::Subscribed {
                    connection_epoch: epoch,
                    subscription_id: sid,
                }))
            }
            (Phase::Subscribed { stream, deadline }, message) => {
                let message = stream.handle_message(message)?;

                match stream.state() {
                    KalshiStreamState::Streaming => *deadline = None,
                    KalshiStreamState::Stopped => {
                        return Err(KalshiWebSocketError::Protocol("venue ended subscription"));
                    }
                    _ => {}
                }
                Ok(message.map(|message| KalshiWebSocketEvent::Book {
                    connection_epoch: epoch,
                    message,
                }))
            }
            (_, StreamMessage::Error { code, .. }) => Err(KalshiStreamError::Venue { code }.into()),
            _ => Err(KalshiWebSocketError::Protocol(
                "frame arrived outside its subscription phase",
            )),
        }
    }
}

#[derive(Debug)]
enum Incoming {
    Message { epoch: u64, message: Message },
    Lost,
    Failed(KalshiWebSocketError),
}

struct PendingSubscription(Pin<Box<dyn Future<Output = Result<(), KalshiWebSocketError>> + Send>>);

impl Debug for PendingSubscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PendingSubscription")
    }
}

/// Owns one authenticated orderbook selection and drives recovery while polled.
///
/// Authentication completes in the HTTP upgrade; there is no separate application auth exchange.
/// The connected constructor queues its epoch-bound subscribe before publishing this object.
/// Acknowledgements confirm intent through NT's shared tracker, while book continuity remains in
/// `KalshiOrderbookStream`. Reconnects always obtain a fresh acknowledgement and snapshots.
///
/// Call `next_event` continuously. This object does not own book levels or publish NT data.
/// Dropping it stops the shared transport; `disconnect` additionally waits for bounded shutdown.
#[derive(Debug)]
pub struct KalshiWebSocketClient {
    client: Arc<WebSocketClient>,
    receiver: Option<Arc<Ingress>>,
    session: Session,
    command_keys: [Ustr; 1],
    pending_subscription: Option<PendingSubscription>,
}

impl KalshiWebSocketClient {
    /// Connects with injected credentials and sends one fixed-selection subscription.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, authentication, connection or subscription send.
    /// A successful return does not confirm the subscription or establish a book.
    pub async fn connect(
        config: KalshiWebSocketConfig,
        markets: Vec<String>,
        credential: &KalshiCredential,
    ) -> Result<Self, KalshiWebSocketError> {
        config.validate()?;
        let session = Session::new(
            markets,
            config.max_frame_bytes,
            Duration::from_millis(config.bootstrap_timeout_ms.get().into()),
        )?;
        let url = Url::parse(&config.transport.url)
            .map_err(|_| KalshiWebSocketError::Configuration("invalid WebSocket URL"))?;
        let provider = credential.websocket_headers_provider(url)?;
        let connection_quota = Quota::with_period(Duration::from_millis(
            config.connection_spacing_ms.get().into(),
        ))
        .ok_or(KalshiWebSocketError::Configuration(
            "invalid connection quota",
        ))?;
        let command_quota = Quota::with_period(Duration::from_millis(
            config.command_spacing_ms.get().into(),
        ))
        .ok_or(KalshiWebSocketError::Configuration("invalid command quota"))?;
        let connection_keys: Arc<[Ustr]> = Arc::from([Ustr::from(CONNECTION_KEY)]);
        let limiter = Arc::new(RateLimiter::new_with_quota(
            None,
            vec![(connection_keys[0], connection_quota)],
        ));
        let receiver = Arc::new(Ingress::new(
            config.max_pending_frames,
            config.max_frame_bytes,
        ));
        let sender = Arc::clone(&receiver);
        let loss_sender = Arc::clone(&receiver);
        let client = tokio::time::timeout(
            Duration::from_millis(config.connect_deadline_ms.get().into()),
            WebSocketClient::epoch_builder()
                .config(config.transport)
                .headers_provider(provider)
                .connection_rate_limiter(limiter)
                .connection_rate_keys(connection_keys)
                .keyed_quotas(vec![(COMMAND_KEY.to_string(), command_quota)])
                .state_sink(SocketStateSink::new(move |state| {
                    if state == SocketState::Disconnected {
                        loss_sender.disconnected();
                    }
                }))
                .epoch_handler(Arc::new(move |epoch, message| {
                    sender.push(epoch, message);
                }))
                .connect(),
        )
        .await
        .map_err(|_| KalshiWebSocketError::ConnectTimeout)??;
        let mut connected = Self {
            client: Arc::new(client),
            receiver: Some(receiver),
            session,
            command_keys: [Ustr::from(COMMAND_KEY)],
            pending_subscription: None,
        };
        connected.begin_subscription(0)?;
        connected.complete_subscription().await?;
        Ok(connected)
    }

    fn begin_subscription(&mut self, epoch: u64) -> Result<(), KalshiWebSocketError> {
        let (request, deadline) = self.session.begin(epoch, tokio::time::Instant::now())?;
        let client = Arc::clone(&self.client);
        let keys = self.command_keys;
        self.pending_subscription = Some(PendingSubscription(Box::pin(async move {
            tokio::time::timeout_at(
                deadline,
                client.send_text_on_connection(request, Some(&keys), epoch),
            )
            .await
            .map_err(|_| KalshiWebSocketError::BootstrapTimeout)??;
            Ok(())
        })));
        Ok(())
    }

    async fn complete_subscription(&mut self) -> Result<(), KalshiWebSocketError> {
        if let Some(pending) = &mut self.pending_subscription {
            // Retain the same future across cancellation, including its writer acknowledgement
            let result = pending.0.as_mut().await;
            self.pending_subscription.take();
            result?;
        }
        Ok(())
    }

    /// Returns the next subscription, book or disconnect event, or `None` after terminal shutdown.
    ///
    /// Old connection messages are discarded before parsing. Missing acknowledgements or snapshots,
    /// malformed frames and continuity failures invalidate progress and request NT transport recovery.
    /// Discard downstream book state on a `Disconnected` event or end of stream (`Ok(None)`).
    ///
    /// # Errors
    ///
    /// Returns an error on protocol or subscription-send failure. Discard downstream book state on
    /// every error before polling again; the next connection must bootstrap from fresh snapshots.
    pub async fn next_event(
        &mut self,
    ) -> Result<Option<KalshiWebSocketEvent>, KalshiWebSocketError> {
        loop {
            if let Err(e) = self.complete_subscription().await {
                let _ = self.invalidate();
                return Err(e);
            }
            let Some(receiver) = &mut self.receiver else {
                return Ok(None);
            };
            let deadline = self.session.deadline();
            let received = tokio::select! {
                biased;
                () = self.client.wait_until_closed() => Ok(None),
                result = async {
                    match deadline {
                        Some(deadline) => tokio::time::timeout_at(deadline, receiver.recv())
                            .await.map(Some).map_err(|_| KalshiWebSocketError::BootstrapTimeout),
                        None => Ok(Some(receiver.recv().await)),
                    }
                } => result,
            };
            let incoming = match received {
                Ok(Some(incoming)) => incoming,
                Ok(None) => {
                    self.session.stop();
                    self.receiver.take();
                    return Ok(None);
                }
                Err(e) => {
                    let _ = self.invalidate();
                    return Err(e);
                }
            };

            match incoming {
                Incoming::Failed(e) => {
                    let _ = self.invalidate();
                    return Err(e);
                }
                Incoming::Lost => {
                    self.session.invalidate();

                    if let Some(epoch) = self.session.epoch {
                        return Ok(Some(KalshiWebSocketEvent::Disconnected {
                            connection_epoch: epoch,
                        }));
                    }
                }
                Incoming::Message { epoch, message } => {
                    if epoch != self.client.connection_epoch() || !self.client.is_active() {
                        continue;
                    }

                    match message {
                        Message::Text(bytes) if bytes.as_ref() == RECONNECTED.as_bytes() => {
                            if let Err(e) = self.begin_subscription(epoch) {
                                let _ = self.invalidate();
                                return Err(e);
                            }
                        }
                        Message::Text(bytes) => {
                            match self.session.handle_frame(
                                epoch,
                                &bytes,
                                tokio::time::Instant::now(),
                            ) {
                                Ok(Some(event)) => return Ok(Some(event)),
                                Ok(None) => {}
                                Err(e) => {
                                    let _ = self.invalidate();
                                    return Err(e);
                                }
                            }
                        }
                        Message::Ping(_) | Message::Pong(_) | Message::Close(_) => {}
                        _ => {
                            let _ = self.invalidate();
                            return Err(KalshiWebSocketError::Protocol(
                                "expected a text application frame",
                            ));
                        }
                    }
                }
            }
        }
    }

    /// Discards protocol progress and requests recovery through the shared NT controller.
    ///
    /// Call this when downstream grid or native book processing rejects an accepted wire message.
    #[must_use]
    pub fn invalidate(&mut self) -> ReconnectRequestOutcome {
        self.pending_subscription.take();
        self.session.invalidate();
        self.client.reconnect_handle().request_reconnect()
    }

    /// Clears intent and pending frames, then waits for bounded transport shutdown.
    ///
    /// Repeated calls are safe; a stopped client cannot resubscribe.
    pub async fn disconnect(&mut self) {
        self.pending_subscription.take();
        self.session.stop();
        self.receiver.take();
        self.client.disconnect().await;
    }
}
