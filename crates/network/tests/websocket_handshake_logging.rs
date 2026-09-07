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

//! Exercise the locked handshake implementation with a logger accepting every level.

use std::sync::Mutex;

use log::{LevelFilter, Log, Metadata, Record};
use rstest::rstest;
use tokio_tungstenite::{accept_async, client_async, tungstenite::client::IntoClientRequest};

#[derive(Debug)]
struct Capture(Mutex<Vec<String>>);

impl Log for Capture {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        self.0.lock().unwrap().push(record.args().to_string());
    }

    fn flush(&self) {}
}

static LOG: Capture = Capture(Mutex::new(Vec::new()));

#[rstest]
#[tokio::test]
async fn authenticated_handshake_does_not_log_header_values() {
    log::set_logger(&LOG).unwrap();
    log::set_max_level(LevelFilter::Trace);
    let (client_io, server_io) = tokio::io::duplex(8192);
    let mut request = "ws://127.0.0.1/fixture".into_client_request().unwrap();
    let sentinels = [
        ("kalshi-access-key", "synthetic-sensitive-key-id"),
        ("kalshi-access-timestamp", "synthetic-sensitive-timestamp"),
        ("kalshi-access-signature", "synthetic-sensitive-signature"),
    ];

    for (name, value) in sentinels {
        request.headers_mut().insert(name, value.parse().unwrap());
    }
    let (client, server) = tokio::join!(client_async(request, client_io), accept_async(server_io));
    assert!(client.is_ok());
    assert!(server.is_ok());
    let records = LOG.0.lock().unwrap();
    assert!(
        !records.is_empty(),
        "the handshake logger must be exercised"
    );

    for (_, value) in sentinels {
        assert!(
            records.iter().all(|record| !record.contains(value)),
            "authenticated header value appeared in a handshake log"
        );
    }
}
