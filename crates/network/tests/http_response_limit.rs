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

use std::{num::NonZeroUsize, time::Duration};

use nautilus_network::http::HttpClient;
use rstest::rstest;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[rstest]
#[case::exact_length("Content-Length: 4\r\n", "data", true)]
#[case::oversized_length("Content-Length: 5\r\n", "extra", false)]
#[case::exact_chunked(
    "Transfer-Encoding: chunked\r\n",
    "2\r\nda\r\n2\r\nta\r\n0\r\n\r\n",
    true
)]
#[case::oversized_chunked(
    "Transfer-Encoding: chunked\r\n",
    "2\r\nda\r\n3\r\nta!\r\n0\r\n\r\n",
    false
)]
#[tokio::test]
async fn public_builder_enforces_response_limit(
    #[case] headers: &str,
    #[case] body: &str,
    #[case] accepted: bool,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let response = format!("HTTP/1.1 200 OK\r\nConnection: close\r\n{headers}\r\n{body}");

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let mut length = 0;

        while !request[..length]
            .windows(4)
            .any(|bytes| bytes == b"\r\n\r\n")
        {
            let count = stream.read(&mut request[length..]).await.unwrap();
            assert!(count > 0, "request headers must fit in the fixture buffer");
            length += count;
        }
        stream.write_all(response.as_bytes()).await.unwrap();
    });
    let client = HttpClient::builder()
        .max_response_bytes(NonZeroUsize::new(4).unwrap())
        .use_system_proxy(false)
        .build()
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        client.get(format!("http://{address}"), None, None, None, None),
    )
    .await
    .unwrap();
    server.await.unwrap();

    if accepted {
        assert_eq!(result.unwrap().body.as_ref(), b"data");
    } else {
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }
}
