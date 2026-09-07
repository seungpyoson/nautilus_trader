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

use anyhow::Context;
use nautilus_common::messages::data::{
    DataResponse, InstrumentResponse, InstrumentsResponse, RequestInstrument, RequestInstruments,
};
use nautilus_core::time::{AtomicTime, get_atomic_clock_realtime};
use nautilus_model::{
    identifiers::ClientId,
    instruments::{BinaryOption, InstrumentAny},
};

use crate::{KALSHI_VENUE, KalshiDataClientConfig, KalshiHttpClient, parse_instrument};

#[derive(Debug)]
pub(crate) enum InstrumentRequest {
    Instrument(RequestInstrument),
    Instruments(RequestInstruments),
}

impl InstrumentRequest {
    pub(crate) fn validate(&self, client_id: ClientId, selection: &[String]) -> anyhow::Result<()> {
        let (id, venue, routed_client, start, end, params) = match self {
            Self::Instrument(request) => (
                Some(request.instrument_id),
                Some(request.instrument_id.venue),
                request.client_id,
                request.start,
                request.end,
                request.params.as_ref(),
            ),
            Self::Instruments(request) => (
                None,
                request.venue,
                request.client_id,
                request.start,
                request.end,
                request.params.as_ref(),
            ),
        };
        anyhow::ensure!(
            routed_client.is_none_or(|id| id == client_id)
                && venue.is_none_or(|venue| venue == *KALSHI_VENUE),
            "Kalshi instrument request routing mismatch"
        );
        anyhow::ensure!(
            id.is_none_or(|id| selection.iter().any(|ticker| ticker == id.symbol.as_str())),
            "Instrument is outside the configured Kalshi selection"
        );
        anyhow::ensure!(
            start.is_none() && end.is_none(),
            "Kalshi supports only current instrument definitions"
        );
        anyhow::ensure!(
            params.is_none_or(
                |params| params.iter().all(|(key, value)| match key.as_str() {
                    "force_instrument_update" | "update_catalog" => value.is_boolean(),
                    "only_last" => value.as_bool() == Some(true),
                    _ => false,
                })
            ),
            "Unsupported Kalshi instrument request parameters"
        );
        Ok(())
    }

    async fn fetch(
        self,
        http: &KalshiHttpClient,
        selection: &[String],
        client_id: ClientId,
        clock: &'static AtomicTime,
    ) -> anyhow::Result<InstrumentUpdate> {
        let tickers = match &self {
            Self::Instrument(request) => vec![request.instrument_id.symbol.to_string()],
            Self::Instruments(_) => selection.to_vec(),
        };
        let instruments = load_instruments(http, &tickers, clock).await?;
        let ts_init = clock.get_time_ns();
        let response = match self {
            Self::Instrument(request) => {
                let instrument = instruments
                    .first()
                    .context("Missing requested Kalshi instrument")?;
                DataResponse::Instrument(Box::new(InstrumentResponse::new(
                    request.request_id,
                    client_id,
                    request.instrument_id,
                    InstrumentAny::BinaryOption(instrument.clone()),
                    None,
                    None,
                    ts_init,
                    request.params,
                )))
            }
            Self::Instruments(request) => DataResponse::Instruments(InstrumentsResponse::new(
                request.request_id,
                client_id,
                *KALSHI_VENUE,
                instruments
                    .iter()
                    .cloned()
                    .map(InstrumentAny::BinaryOption)
                    .collect(),
                None,
                None,
                ts_init,
                request.params,
            )),
        };
        Ok(InstrumentUpdate {
            instruments,
            response,
        })
    }
}

pub(crate) struct InstrumentUpdate {
    pub(crate) instruments: Vec<BinaryOption>,
    pub(crate) response: DataResponse,
}

/// The stream owner polls one request at a time, preserving request and definition order.
pub(crate) struct Requests {
    client_id: ClientId,
    http: KalshiHttpClient,
    selection: Vec<String>,
    receiver: tokio::sync::mpsc::UnboundedReceiver<InstrumentRequest>,
}

impl Requests {
    pub(crate) fn new(
        client_id: ClientId,
        config: &KalshiDataClientConfig,
        http: KalshiHttpClient,
        receiver: tokio::sync::mpsc::UnboundedReceiver<InstrumentRequest>,
    ) -> Self {
        Self {
            client_id,
            http,
            selection: config.market_tickers.clone(),
            receiver,
        }
    }

    pub(crate) async fn next(&mut self) -> Option<anyhow::Result<InstrumentUpdate>> {
        let request = self.receiver.recv().await?;
        Some(
            request
                .fetch(
                    &self.http,
                    &self.selection,
                    self.client_id,
                    get_atomic_clock_realtime(),
                )
                .await,
        )
    }
}

pub(crate) async fn load_instruments(
    http: &KalshiHttpClient,
    tickers: &[String],
    clock: &'static AtomicTime,
) -> anyhow::Result<Vec<BinaryOption>> {
    let mut instruments = Vec::with_capacity(tickers.len());

    for ticker in tickers {
        // Each selected market owns its HTTP operation budget, including quota and retries.
        let market = http.get_market(ticker).await?;
        instruments.push(parse_instrument(&market, clock.get_time_ns())?);
    }
    Ok(instruments)
}
