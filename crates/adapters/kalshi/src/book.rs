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

use ahash::AHashMap;
use nautilus_common::messages::book::{BookFeedAction, L2BookUpdate};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::OrderSideSpecified,
    identifiers::InstrumentId,
    instruments::{BinaryOption, PriceGrid},
};
use rust_decimal::Decimal;

use crate::{KalshiMarketSide, KalshiOrderbookMessage};

/// Immutable interpretation rules; mutable liquidity belongs to the data engine.
#[derive(Debug)]
pub(crate) struct BookDefinitions {
    markets: AHashMap<String, BookDefinition>,
}

#[derive(Debug, PartialEq)]
struct BookDefinition {
    instrument_id: InstrumentId,
    exchange_index: u64,
    grid: PriceGrid,
    price_precision: u8,
    size_precision: u8,
    size_increment: Decimal,
}

impl BookDefinitions {
    pub(crate) fn new(instruments: &[BinaryOption]) -> anyhow::Result<Self> {
        anyhow::ensure!(!instruments.is_empty(), "Kalshi books require instruments");
        let mut markets = AHashMap::new();

        for instrument in instruments {
            let definition = BookDefinition {
                instrument_id: instrument.id,
                exchange_index: instrument
                    .info
                    .as_ref()
                    .and_then(|info| info.get("exchange_index"))
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| anyhow::anyhow!("Kalshi book requires the exchange index"))?,
                grid: instrument.price_grid.clone().ok_or_else(|| {
                    anyhow::anyhow!("Kalshi book requires the instrument price grid")
                })?,
                price_precision: instrument.price_precision,
                size_precision: instrument.size_precision,
                size_increment: instrument.size_increment.as_decimal(),
            };
            anyhow::ensure!(
                markets
                    .insert(instrument.raw_symbol.to_string(), definition)
                    .is_none(),
                "Duplicate Kalshi book instrument"
            );
        }
        Ok(Self { markets })
    }

    pub(crate) fn same_definitions(&self, other: &Self) -> bool {
        self.markets == other.markets
    }

    pub(crate) fn prepare(
        &self,
        message: &KalshiOrderbookMessage,
        ts_init: UnixNanos,
    ) -> anyhow::Result<BookFeedAction> {
        let ticker = match message {
            KalshiOrderbookMessage::Snapshot { snapshot, .. } => &snapshot.market_ticker,
            KalshiOrderbookMessage::Delta { delta, .. } => &delta.market_ticker,
        };
        let definition = self
            .markets
            .get(ticker)
            .ok_or_else(|| anyhow::anyhow!("Unselected Kalshi book market"))?;
        let (update, sequence, ts_event) = match message {
            KalshiOrderbookMessage::Snapshot { seq, snapshot, .. } => (
                L2BookUpdate::Snapshot {
                    bids: snapshot
                        .yes
                        .iter()
                        .map(|level| (level.price_dollars, level.quantity))
                        .collect(),
                    asks: snapshot
                        .no
                        .iter()
                        .map(|level| (level.price_dollars, level.quantity))
                        .collect(),
                },
                seq.get(),
                ts_init,
            ),
            KalshiOrderbookMessage::Delta { seq, delta, .. } => (
                L2BookUpdate::Change {
                    side: match delta.side {
                        KalshiMarketSide::Yes => OrderSideSpecified::Buy,
                        KalshiMarketSide::No => OrderSideSpecified::Sell,
                    },
                    price: delta.price_dollars,
                    quantity_change: delta.delta,
                },
                seq.get(),
                match delta.ts_ms {
                    Some(ms) => UnixNanos::from(
                        ms.checked_mul(1_000_000)
                            .ok_or_else(|| anyhow::anyhow!("Kalshi source timestamp overflow"))?,
                    ),
                    None => ts_init,
                },
            ),
        };
        Ok(BookFeedAction::Update {
            instrument_id: definition.instrument_id,
            update,
            sequence,
            ts_event,
        })
    }
}
