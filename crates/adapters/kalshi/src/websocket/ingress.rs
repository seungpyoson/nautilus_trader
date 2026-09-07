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

use std::{collections::VecDeque, num::NonZeroUsize, sync::Mutex};

use nautilus_network::transport::Message;

use super::{Incoming, KalshiStreamError, KalshiWebSocketError};

#[derive(Debug, Default)]
struct Buffer {
    frames: VecDeque<(u64, Message)>,
    failure: Option<KalshiWebSocketError>,
    blocked_epoch: Option<u64>,
    lost: bool,
}

/// Failure and disconnect signals do not compete with data for queue capacity.
#[derive(Debug)]
pub(super) struct Ingress {
    buffer: Mutex<Buffer>,
    ready: tokio::sync::Notify,
    max_frames: usize,
    max_bytes: usize,
}

impl Ingress {
    pub(super) fn new(max_frames: NonZeroUsize, max_bytes: NonZeroUsize) -> Self {
        Self {
            buffer: Mutex::new(Buffer::default()),
            ready: tokio::sync::Notify::new(),
            max_frames: max_frames.get(),
            max_bytes: max_bytes.get(),
        }
    }

    pub(super) fn push(&self, epoch: u64, message: Message) {
        // Control frames are handled by the transport and cannot grow the application backlog
        if message.is_control() {
            return;
        }
        let mut buffer = self.buffer.lock().expect("Kalshi ingress lock poisoned");
        if buffer.blocked_epoch.is_some_and(|blocked| epoch <= blocked) {
            return;
        }
        let failure = if message.as_bytes().len() > self.max_bytes {
            Some(
                KalshiStreamError::FrameTooLarge {
                    length: message.as_bytes().len(),
                    limit: self.max_bytes,
                }
                .into(),
            )
        } else if buffer.frames.len() >= self.max_frames {
            Some(KalshiWebSocketError::BacklogOverflow {
                limit: self.max_frames,
            })
        } else {
            None
        };

        if let Some(failure) = failure {
            buffer.frames.clear();
            buffer.blocked_epoch = Some(epoch);
            buffer.failure.get_or_insert(failure);
        } else {
            buffer.frames.push_back((epoch, message));
        }
        drop(buffer);
        self.ready.notify_one();
    }

    pub(super) fn disconnected(&self) {
        let mut buffer = self.buffer.lock().expect("Kalshi ingress lock poisoned");
        buffer.frames.clear();
        buffer.lost = true;
        drop(buffer);
        self.ready.notify_one();
    }

    pub(super) async fn recv(&self) -> Incoming {
        loop {
            let notified = self.ready.notified();
            {
                let mut buffer = self.buffer.lock().expect("Kalshi ingress lock poisoned");
                if let Some(failure) = buffer.failure.take() {
                    return Incoming::Failed(failure);
                }

                if std::mem::take(&mut buffer.lost) {
                    return Incoming::Lost;
                }

                if let Some((epoch, message)) = buffer.frames.pop_front() {
                    return Incoming::Message { epoch, message };
                }
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[tokio::test]
    async fn overflow_discards_all_queued_frames_and_blocks_the_damaged_epoch() {
        let ingress = Ingress::new(2.try_into().unwrap(), 8.try_into().unwrap());
        ingress.push(0, Message::text("first"));
        ingress.push(0, Message::text("second"));
        assert_eq!(ingress.buffer.lock().unwrap().frames.len(), 2);
        ingress.push(0, Message::text("overflow"));
        for _ in 0..10000 {
            ingress.push(0, Message::text("late"));
        }
        assert!(ingress.buffer.lock().unwrap().frames.is_empty());
        ingress.disconnected();
        ingress.push(1, Message::text("fresh"));
        assert!(matches!(
            ingress.recv().await,
            Incoming::Failed(KalshiWebSocketError::BacklogOverflow { limit: 2 })
        ));
        assert!(matches!(ingress.recv().await, Incoming::Lost));
        assert!(
            matches!(ingress.recv().await, Incoming::Message { epoch: 1, message } if message.as_text() == Some("fresh"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn oversized_frame_is_rejected_before_retention_and_failure_survives_cancellation() {
        let ingress = Ingress::new(2.try_into().unwrap(), 8.try_into().unwrap());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(1), ingress.recv())
                .await
                .is_err()
        );
        ingress.push(0, Message::text("123456789"));
        assert!(ingress.buffer.lock().unwrap().frames.is_empty());
        assert!(matches!(
            ingress.recv().await,
            Incoming::Failed(KalshiWebSocketError::Stream(
                KalshiStreamError::FrameTooLarge {
                    length: 9,
                    limit: 8
                }
            ))
        ));
        ingress.push(1, Message::text("12345678"));
        assert!(matches!(
            ingress.recv().await,
            Incoming::Message { epoch: 1, .. }
        ));
    }
}
