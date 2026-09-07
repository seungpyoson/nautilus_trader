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

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{KALSHI, KalshiCredential, KalshiDataClient, KalshiDataClientConfig};

impl ClientConfig for KalshiDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Creates native data clients using an injected credential, without credential-store access.
#[derive(Clone, Debug)]
pub struct KalshiDataClientFactory {
    credential: KalshiCredential,
}

impl KalshiDataClientFactory {
    /// Creates a factory with the credential supplied by its owner.
    #[must_use]
    pub fn new(credential: KalshiCredential) -> Self {
        Self { credential }
    }
}

impl DataClientFactory for KalshiDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<KalshiDataClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("Expected KalshiDataClientConfig"))?;
        Ok(Box::new(KalshiDataClient::new(
            ClientId::new_checked(name)?,
            config.clone(),
            self.credential.clone(),
            cache,
        )?))
    }

    fn name(&self) -> &'static str {
        KALSHI
    }

    fn config_type(&self) -> &'static str {
        stringify!(KalshiDataClientConfig)
    }
}
