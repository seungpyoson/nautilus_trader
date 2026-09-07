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

//! Bounded L2 reader using an explicit JSON config and caller-supplied local credentials.
//!
//! `inspect CONFIG.json` validates current public metadata without reading credentials.
//! `read CONFIG.json` uses KALSHI_KEY_ID and KALSHI_PRIVATE_KEY_FILE, then stops automatically.

use std::{
    cell::RefCell, collections::BTreeMap, fs::File, io::Read, num::NonZeroU16, path::Path, rc::Rc,
    time::Duration,
};

use anyhow::Context;
use nautilus_common::{
    actor::{DataActor, DataActorCore, data_actor::DataActorConfig},
    enums::Environment,
    live::get_runtime,
    logging::logger::LoggerConfig,
    nautilus_actor,
};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_kalshi::{
    KALSHI_CLIENT_ID, KALSHI_VENUE, KalshiCredential, KalshiDataClientConfig,
    KalshiDataClientFactory, KalshiHttpClient, KalshiMarketStatus, parse_instrument,
};
use nautilus_live::node::{LiveNode, LiveNodeHandle, NodeState};
use nautilus_model::{
    data::OrderBookDeltas,
    enums::{BookAction, BookType, RecordFlag},
    identifiers::{InstrumentId, Symbol, TraderId},
};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReaderConfig {
    client: KalshiDataClientConfig,
    observation_seconds: NonZeroU16,
}

#[derive(Debug, Default, Serialize)]
struct Observation {
    snapshots: u64,
    updates: u64,
    invalidations: u64,
}

#[derive(Debug, Default, Serialize)]
struct Report {
    books: BTreeMap<InstrumentId, Observation>,
    callback_failed: bool,
    stopped: bool,
}

#[derive(Debug)]
struct BookReader {
    core: DataActorCore,
    report: Rc<RefCell<Report>>,
    started: Option<tokio::sync::oneshot::Sender<()>>,
    handle: LiveNodeHandle,
}

nautilus_actor!(BookReader);

impl DataActor for BookReader {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let ids: Vec<_> = self.report.borrow().books.keys().copied().collect();

        for id in ids {
            self.subscribe_book_deltas(
                id,
                BookType::L2_MBP,
                None,
                Some(*KALSHI_CLIENT_ID),
                true,
                None,
            );
        }
        self.started
            .take()
            .context("Reader already started")?
            .send(())
            .map_err(|()| anyhow::anyhow!("Reader timer stopped before startup"))
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        let result = self.observe(deltas);

        if result.is_err() {
            self.report.borrow_mut().callback_failed = true;
            self.handle.stop();
        }
        result
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.report.borrow_mut().stopped = true;
        Ok(())
    }
}

impl BookReader {
    fn observe(&self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        anyhow::ensure!(
            RecordFlag::F_LAST.matches(deltas.flags),
            "Incomplete native book event"
        );
        let book = self
            .cache()
            .order_book(&deltas.instrument_id)
            .context("Managed book is missing from the engine cache")?;
        anyhow::ensure!(
            book.update_count > 0,
            "Managed book did not receive the event"
        );
        let mut report = self.report.borrow_mut();
        let observation = report
            .books
            .get_mut(&deltas.instrument_id)
            .context("Received an unselected instrument")?;

        if RecordFlag::F_SNAPSHOT.matches(deltas.flags) {
            observation.snapshots += 1;
        } else if deltas.deltas.len() == 1 && deltas.deltas[0].action == BookAction::Clear {
            observation.invalidations += 1;
        } else {
            observation.updates += 1;
        }
        Ok(())
    }
}

async fn inspect(config: &KalshiDataClientConfig) -> anyhow::Result<()> {
    let http = KalshiHttpClient::new(config.http.clone())?;

    for ticker in &config.market_tickers {
        let metadata = http.get_market(ticker).await?;
        anyhow::ensure!(
            metadata.status == KalshiMarketStatus::Active,
            "Selected market is not active"
        );
        let now = get_atomic_clock_realtime().get_time_ns();
        let instrument = parse_instrument(&metadata, now)?;
        anyhow::ensure!(
            instrument.activation_ns <= now && now < instrument.expiration_ns,
            "Selected market is outside its trading interval"
        );
        println!(
            "{}",
            serde_json::json!({
                "instrument_id": instrument.id.to_string(),
                "status": "active",
                "close_time": metadata.close_time.to_string(),
                "price_precision": instrument.price_precision,
                "size_precision": instrument.size_precision,
                "price_increment": instrument.price_increment.to_string(),
            })
        );
    }
    Ok(())
}

fn read_bounded(path: &Path) -> anyhow::Result<Zeroizing<Vec<u8>>> {
    let mut bytes = Zeroizing::new(Vec::new());
    File::open(path)
        .context("Cannot open the supplied local input file")?
        .take(65_537)
        .read_to_end(&mut bytes)
        .context("Cannot read the supplied local input file")?;
    anyhow::ensure!(bytes.len() <= 65_536, "Local input exceeds 64 KiB");
    Ok(bytes)
}

async fn read(config: ReaderConfig) -> anyhow::Result<()> {
    let key_id = Zeroizing::new(
        std::env::var("KALSHI_KEY_ID").map_err(|_| anyhow::anyhow!("Set KALSHI_KEY_ID locally"))?,
    );
    let key_path = std::env::var_os("KALSHI_PRIVATE_KEY_FILE")
        .context("Set KALSHI_PRIVATE_KEY_FILE to the local PEM file")?;
    let pem = read_bounded(Path::new(&key_path))?;
    let factory = KalshiDataClientFactory::new(KalshiCredential::from_pem(&key_id, &pem)?);
    drop(pem);
    drop(key_id);
    let report = Rc::new(RefCell::new(Report::default()));

    for ticker in &config.client.market_tickers {
        let id = InstrumentId::new(Symbol::new_checked(ticker)?, *KALSHI_VENUE);
        report.borrow_mut().books.insert(id, Observation::default());
    }
    let startup_secs = u64::from(config.client.bootstrap_timeout_ms.get()).div_ceil(1000) + 5;
    let shutdown_secs = u64::from(config.client.shutdown_timeout_ms.get()).div_ceil(1000) + 5;
    let observation_secs = u64::from(config.observation_seconds.get());
    let mut node = LiveNode::builder(TraderId::from("READER-001"), Environment::Live)?
        .with_name("KALSHI-DATA-READER")
        .with_reconciliation(false)
        .with_load_state(false)
        .with_save_state(false)
        .with_timeout_connection(startup_secs)
        .with_timeout_disconnection_secs(shutdown_secs)
        .with_delay_post_stop_secs(0)
        .with_logging(LoggerConfig {
            bypass_logging: true,
            ..Default::default()
        })
        .add_data_client(None, Box::new(factory), Box::new(config.client))?
        .build()?;
    let handle = node.handle();
    let (started, startup) = tokio::sync::oneshot::channel();
    node.add_actor(BookReader {
        core: DataActorCore::new(DataActorConfig::default()),
        report: Rc::clone(&report),
        started: Some(started),
        handle: handle.clone(),
    })?;
    let timer_handle = handle.clone();

    let timer = get_runtime().spawn(async move {
        startup.await.context("Reader stopped before startup")?;
        tokio::time::sleep(Duration::from_secs(observation_secs)).await;
        timer_handle.stop();
        Ok::<_, anyhow::Error>(())
    });
    let deadline = Duration::from_secs(startup_secs + observation_secs + shutdown_secs);
    let result = tokio::time::timeout(deadline, node.run()).await;
    timer.abort();
    let timer_result = timer.await;
    let disconnected = node.kernel().data_engine().check_disconnected();
    let stopped = handle.state() == NodeState::Stopped;
    node.dispose();
    let report = report.borrow();
    println!("{}", serde_json::to_string_pretty(&*report)?);
    result.context("Reader exceeded its total deadline")??;
    timer_result.context("Reader ended before the observation period completed")??;
    anyhow::ensure!(stopped && disconnected, "Node shutdown incomplete");
    anyhow::ensure!(
        !report.callback_failed && report.stopped,
        "Reader callback failed"
    );
    anyhow::ensure!(
        !report.books.is_empty()
            && report
                .books
                .values()
                .all(|book| book.snapshots > 0 && book.updates > 0),
        "Observation incomplete: every market requires a snapshot and a subsequent update"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let [command, path] = args.as_slice() else {
        anyhow::bail!("Usage: kalshi-data-reader <inspect|read> CONFIG.json");
    };
    let config: ReaderConfig = serde_json::from_slice(&read_bounded(Path::new(path))?)
        .context("Invalid reader configuration")?;
    anyhow::ensure!(
        !config.client.market_tickers.is_empty() && config.observation_seconds.get() <= 300,
        "Select at least one market and an observation period of 1 to 300 seconds"
    );

    match command.to_str() {
        Some("inspect") => inspect(&config.client).await,
        Some("read") => read(config).await,
        _ => anyhow::bail!("Usage: kalshi-data-reader <inspect|read> CONFIG.json"),
    }
}
