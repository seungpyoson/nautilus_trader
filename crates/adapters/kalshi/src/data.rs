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

#[cfg(test)]
mod tests;

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    live::{get_runtime, runner::try_get_data_event_sender},
    messages::{
        DataEvent,
        book::BookFeedBudget,
        data::{
            self, RequestInstrument, RequestInstruments, SubscribeBookDeltas, UnsubscribeBookDeltas,
        },
    },
};
use nautilus_core::time::{AtomicTime, get_atomic_clock_realtime};
use nautilus_model::{
    enums::BookType,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{BinaryOption, InstrumentAny},
};
use tokio_util::sync::CancellationToken;

use crate::{
    KALSHI_VENUE, KalshiCredential, KalshiDataClientConfig, KalshiHttpClient,
    KalshiWebSocketClient,
    publication::Publication,
    requests::{InstrumentRequest, Requests, load_instruments},
    stream::validate_market_selection,
};

#[derive(Debug)]
enum Command {
    Subscribe(InstrumentId),
    Unsubscribe(InstrumentId),
}

/// Native NT data client for a fixed selection of binary Kalshi instruments.
///
/// Supports full L2 delta subscriptions and fresh requests for the configured instruments.
/// Historical instrument definitions are unsupported. The shared real-time atomic clock
/// supplies receipt timestamps, as for other native live adapters.
#[derive(Debug)]
pub struct KalshiDataClient {
    client_id: ClientId,
    config: KalshiDataClientConfig,
    credential: KalshiCredential,
    http: KalshiHttpClient,
    cache: CacheView,
    expected_instruments: Option<tokio::sync::watch::Receiver<Vec<BinaryOption>>>,
    sender: Option<tokio::sync::mpsc::UnboundedSender<DataEvent>>,
    commands: Option<tokio::sync::mpsc::UnboundedSender<Command>>,
    requests: Option<tokio::sync::mpsc::UnboundedSender<InstrumentRequest>>,
    task: Option<tokio::task::JoinHandle<()>>,
    cancellation: CancellationToken,
    connected: Arc<AtomicBool>,
    book_budget: Arc<BookFeedBudget>,
    clock: &'static AtomicTime,
}

/// Drop also clears published books when a task is aborted.
struct StreamOwner {
    publication: Publication,
    clock: &'static AtomicTime,
}

impl StreamOwner {
    fn recover(&mut self, websocket: &mut KalshiWebSocketClient) -> anyhow::Result<()> {
        self.publication.invalidate(self.clock.get_time_ns())?;
        let _ = websocket.invalidate();
        Ok(())
    }
}

impl Drop for StreamOwner {
    fn drop(&mut self) {
        if let Err(e) = self.publication.invalidate(self.clock.get_time_ns()) {
            log::debug!("Kalshi stream teardown: {e}");
        }
    }
}

impl KalshiDataClient {
    /// Creates a client without starting network work or reading credentials.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, duplicate or malformed selection, or invalid HTTP policy.
    pub fn new(
        client_id: ClientId,
        config: KalshiDataClientConfig,
        credential: KalshiCredential,
        cache: CacheView,
    ) -> anyhow::Result<Self> {
        validate_market_selection(&config.market_tickers)?;
        config.websocket.validate()?;
        // Reserve bootstrap snapshots and subscription intent without resetting the
        // budget on reconnect; retained inputs still belong to this client.
        let book_limit = config
            .market_tickers
            .len()
            .checked_mul(2)
            .and_then(|bootstrap| bootstrap.checked_add(config.websocket.max_pending_frames.get()))
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| anyhow::anyhow!("Kalshi engine backlog budget overflow"))?;
        let http = KalshiHttpClient::new(config.http.clone())?;
        Ok(Self {
            client_id,
            config,
            credential,
            http,
            cache,
            expected_instruments: None,
            sender: None,
            commands: None,
            requests: None,
            task: None,
            cancellation: CancellationToken::new(),
            connected: Arc::new(AtomicBool::new(false)),
            book_budget: BookFeedBudget::new(book_limit),
            clock: get_atomic_clock_realtime(),
        })
    }

    fn stop_local(&mut self) {
        self.connected.store(false, Ordering::Release);
        self.cancellation.cancel();
        self.commands.take();
        self.requests.take();
    }

    fn validate_instrument(&self, id: InstrumentId) -> anyhow::Result<()> {
        anyhow::ensure!(
            id.venue == *KALSHI_VENUE
                && self
                    .config
                    .market_tickers
                    .iter()
                    .any(|ticker| ticker == id.symbol.as_str()),
            "Instrument is outside the configured Kalshi selection"
        );
        Ok(())
    }

    fn send_command(&self, command: Command) -> anyhow::Result<()> {
        self.commands
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Kalshi data client is not connected"))?
            .send(command)
            .map_err(|_| anyhow::anyhow!("Kalshi data task stopped"))
    }

    fn send_request(&self, request: InstrumentRequest) -> anyhow::Result<()> {
        request.validate(self.client_id, &self.config.market_tickers)?;
        self.requests
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Kalshi data client is not connected"))?
            .send(request)
            .map_err(|_| anyhow::anyhow!("Kalshi data task stopped"))
    }

    async fn bootstrap(
        &self,
        sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    ) -> anyhow::Result<(KalshiWebSocketClient, StreamOwner)> {
        let instruments =
            load_instruments(&self.http, &self.config.market_tickers, self.clock).await?;
        let publication = Publication::new(
            &instruments,
            sender.clone(),
            Arc::clone(&self.connected),
            Arc::clone(&self.book_budget),
        )?;
        publication.publish_instruments(&instruments)?;

        let mut websocket = KalshiWebSocketClient::connect(
            self.config.websocket.clone(),
            self.config.market_tickers.clone(),
            &self.credential,
        )
        .await?;
        let mut owner = StreamOwner {
            publication,
            clock: self.clock,
        };

        while !owner.publication.is_ready() {
            let event = websocket
                .next_event()
                .await?
                .ok_or_else(|| anyhow::anyhow!("Kalshi stream closed during bootstrap"))?;
            owner.publication.handle(event, self.clock.get_time_ns())?;
        }
        anyhow::ensure!(
            !sender.is_closed(),
            "NT data receiver closed during bootstrap"
        );
        Ok((websocket, owner))
    }

    async fn run_stream(
        client_id: ClientId,
        mut websocket: KalshiWebSocketClient,
        mut owner: StreamOwner,
        mut commands: tokio::sync::mpsc::UnboundedReceiver<Command>,
        cancellation: CancellationToken,
        mut requests: Requests,
    ) -> anyhow::Result<()> {
        let closed = owner.publication.receiver_closed();
        tokio::pin!(closed);

        'stream: loop {
            // Retain the request future while processing book frames and local commands
            let request = requests.next();
            tokio::pin!(request);

            loop {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break 'stream,
                    () = &mut closed => break 'stream,
                    () = owner.publication.failed() => {
                        owner.recover(&mut websocket)?;
                        log::warn!("Kalshi engine book application failed; requiring fresh snapshots");
                    },
                    command = commands.recv() => {
                        let ts_init = owner.clock.get_time_ns();
                        let result = match command {
                            Some(Command::Subscribe(id)) => owner.publication.subscribe(id, ts_init),
                            Some(Command::Unsubscribe(id)) => owner.publication.unsubscribe(id, ts_init),
                            None => break 'stream,
                        };

                        if let Err(e) = result {
                            owner.recover(&mut websocket)?;
                            log::warn!("Kalshi subscription publication failed; requiring fresh snapshots: {e}");
                        }
                    },
                    result = &mut request => {
                        let Some(update) = result else { break 'stream };
                        let update = match update {
                            Ok(update) => update,
                            Err(e) => {
                                log::error!("Kalshi instrument refresh failed for client {client_id}: {e}");
                                break;
                            }
                        };
                        let ts_init = owner.clock.get_time_ns();

                        if owner.publication.refresh(&update.instruments, ts_init)? {
                            let _ = websocket.invalidate();
                        }
                        owner.publication.respond(update.response)?;
                        break;
                    },
                    result = websocket.next_event() => {
                        let ts_init = owner.clock.get_time_ns();

                        match result {
                            Ok(Some(event)) => {
                                if let Err(e) = owner.publication.handle(event, ts_init) {
                                    owner.recover(&mut websocket)?;
                                    log::warn!("Kalshi book invalidated: {e}");
                                }
                            }
                            Ok(None) => anyhow::bail!("Kalshi transport recovery exhausted"),
                            Err(e) => {
                                owner.publication.invalidate(ts_init)?;
                                log::warn!("Kalshi stream invalidated: {e}");
                            }
                        }
                    }
                }
            }
        }
        owner.publication.invalidate(owner.clock.get_time_ns())?;
        websocket.disconnect().await;
        Ok(())
    }
}

impl Drop for KalshiDataClient {
    fn drop(&mut self) {
        self.stop_local();

        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[async_trait(?Send)]
impl DataClient for KalshiDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*KALSHI_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.sender = Some(try_get_data_event_sender().ok_or_else(|| {
            anyhow::anyhow!("NT data event sender must be installed before start")
        })?);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.stop_local();
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop_local();
        self.sender.take();
        self.expected_instruments.take();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.reset()
    }

    fn is_connected(&self) -> bool {
        if self.cancellation.is_cancelled() || !self.connected.load(Ordering::Acquire) {
            return false;
        }

        // LiveNode drains queued definitions after connect returns; readiness observes that drain
        let cache = self.cache.borrow();
        self.expected_instruments.as_ref().is_some_and(|expected| {
            expected.borrow().iter().all(|definition| {
                cache.instrument(&definition.id).is_some_and(|instrument| {
                    let expected = InstrumentAny::BinaryOption(definition.clone());

                    match (
                        serde_json::to_vec(instrument),
                        serde_json::to_vec(&expected),
                    ) {
                        (Ok(cached), Ok(expected)) => cached == expected,
                        _ => false,
                    }
                })
            })
        })
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }
        self.disconnect().await?;
        let sender = self
            .sender
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Start Kalshi data client before connecting"))?;
        self.cancellation = CancellationToken::new();
        let timeout = Duration::from_millis(self.config.bootstrap_timeout_ms.get().into());
        let (websocket, owner) = tokio::time::timeout(timeout, self.bootstrap(sender))
            .await
            .context("Kalshi data bootstrap deadline expired")??;
        self.expected_instruments = Some(owner.publication.definitions());
        let (commands, receiver) = tokio::sync::mpsc::unbounded_channel();
        self.commands = Some(commands);
        let (requests, request_receiver) = tokio::sync::mpsc::unbounded_channel();
        self.requests = Some(requests);
        let requests = Requests::new(
            self.client_id,
            &self.config,
            self.http.clone(),
            request_receiver,
        );
        let cancellation = self.cancellation.clone();
        let client_id = self.client_id;
        let selection = self.config.market_tickers.clone();

        self.task = Some(get_runtime().spawn(async move {
            if let Err(e) = Self::run_stream(
                client_id,
                websocket,
                owner,
                receiver,
                cancellation,
                requests,
            )
            .await
            {
                log::error!(
                    "Kalshi data task stopped for client {client_id}, selection {selection:?}: {e}"
                );
            }
        }));
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop_local();

        if let Some(task) = self.task.as_mut() {
            let timeout = Duration::from_millis(self.config.shutdown_timeout_ms.get().into());

            if tokio::time::timeout(timeout, &mut *task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
            self.task.take();
        }
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        self.validate_instrument(cmd.instrument_id)?;
        anyhow::ensure!(
            cmd.book_type == BookType::L2_MBP
                && cmd.depth.is_none()
                && cmd.params.as_ref().is_none_or(|params| params.is_empty()),
            "Kalshi supports full L2_MBP deltas without additional parameters"
        );
        anyhow::ensure!(
            cmd.client_id.is_none_or(|id| id == self.client_id)
                && cmd.venue.is_none_or(|venue| Some(venue) == self.venue()),
            "Kalshi subscription routing mismatch"
        );
        self.send_command(Command::Subscribe(cmd.instrument_id))
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        self.validate_instrument(cmd.instrument_id)?;
        anyhow::ensure!(
            cmd.client_id.is_none_or(|id| id == self.client_id)
                && cmd.venue.is_none_or(|venue| Some(venue) == self.venue())
                && cmd.params.as_ref().is_none_or(|params| params.is_empty()),
            "Kalshi unsubscribe routing or parameters mismatch"
        );
        self.send_command(Command::Unsubscribe(cmd.instrument_id))
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        self.send_request(InstrumentRequest::Instrument(request))
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        self.send_request(InstrumentRequest::Instruments(request))
    }

    fn subscribe(&mut self, _command: data::SubscribeCustomData) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe")
    }

    fn subscribe_instruments(
        &mut self,
        _command: data::SubscribeInstruments,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_instruments")
    }

    fn subscribe_instrument(&mut self, _command: data::SubscribeInstrument) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_instrument")
    }

    fn subscribe_book_depth10(
        &mut self,
        _command: data::SubscribeBookDepth10,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_book_depth10")
    }

    fn subscribe_quotes(&mut self, _command: data::SubscribeQuotes) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_quotes")
    }

    fn subscribe_trades(&mut self, _command: data::SubscribeTrades) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_trades")
    }

    fn subscribe_mark_prices(&mut self, _command: data::SubscribeMarkPrices) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_mark_prices")
    }

    fn subscribe_index_prices(
        &mut self,
        _command: data::SubscribeIndexPrices,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_index_prices")
    }

    fn subscribe_funding_rates(
        &mut self,
        _command: data::SubscribeFundingRates,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_funding_rates")
    }

    fn subscribe_bars(&mut self, _command: data::SubscribeBars) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_bars")
    }

    fn subscribe_instrument_status(
        &mut self,
        _command: data::SubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_instrument_status")
    }

    fn subscribe_instrument_close(
        &mut self,
        _command: data::SubscribeInstrumentClose,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_instrument_close")
    }

    fn subscribe_option_greeks(
        &mut self,
        _command: data::SubscribeOptionGreeks,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support subscribe_option_greeks")
    }

    fn unsubscribe(&mut self, _command: &data::UnsubscribeCustomData) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe")
    }

    fn unsubscribe_instruments(
        &mut self,
        _command: &data::UnsubscribeInstruments,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_instruments")
    }

    fn unsubscribe_instrument(
        &mut self,
        _command: &data::UnsubscribeInstrument,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_instrument")
    }

    fn unsubscribe_book_depth10(
        &mut self,
        _command: &data::UnsubscribeBookDepth10,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_book_depth10")
    }

    fn unsubscribe_quotes(&mut self, _command: &data::UnsubscribeQuotes) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_quotes")
    }

    fn unsubscribe_trades(&mut self, _command: &data::UnsubscribeTrades) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_trades")
    }

    fn unsubscribe_mark_prices(
        &mut self,
        _command: &data::UnsubscribeMarkPrices,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_mark_prices")
    }

    fn unsubscribe_index_prices(
        &mut self,
        _command: &data::UnsubscribeIndexPrices,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_index_prices")
    }

    fn unsubscribe_funding_rates(
        &mut self,
        _command: &data::UnsubscribeFundingRates,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_funding_rates")
    }

    fn unsubscribe_bars(&mut self, _command: &data::UnsubscribeBars) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_bars")
    }

    fn unsubscribe_instrument_status(
        &mut self,
        _command: &data::UnsubscribeInstrumentStatus,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_instrument_status")
    }

    fn unsubscribe_instrument_close(
        &mut self,
        _command: &data::UnsubscribeInstrumentClose,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_instrument_close")
    }

    fn unsubscribe_option_greeks(
        &mut self,
        _command: &data::UnsubscribeOptionGreeks,
    ) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support unsubscribe_option_greeks")
    }

    fn request_data(&self, _command: data::RequestCustomData) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support request_data")
    }

    fn request_book_snapshot(&self, _command: data::RequestBookSnapshot) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support request_book_snapshot")
    }

    fn request_quotes(&self, _command: data::RequestQuotes) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support request_quotes")
    }

    fn request_trades(&self, _command: data::RequestTrades) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support request_trades")
    }

    fn request_funding_rates(&self, _command: data::RequestFundingRates) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support request_funding_rates")
    }

    fn request_forward_prices(&self, _command: data::RequestForwardPrices) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support request_forward_prices")
    }

    fn request_bars(&self, _command: data::RequestBars) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support request_bars")
    }

    fn request_book_depth(&self, _command: data::RequestBookDepth) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support request_book_depth")
    }

    fn request_book_deltas(&self, _command: data::RequestBookDeltas) -> anyhow::Result<()> {
        anyhow::bail!("Kalshi does not support request_book_deltas")
    }
}
