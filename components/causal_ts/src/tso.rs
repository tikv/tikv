// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use pd_client::PdClient;
use std::sync::Arc;

use crate::errors::Result;
use crate::CausalTsProvider;
use txn_types::TimeStamp;

/// A simple implementation acquiring TSO on every request.
/// For test purpose only. Do not use in production.
pub struct TsoSimpleProvider {
    pd_client: Arc<dyn PdClient>,
}

impl TsoSimpleProvider {
    pub fn new(pd_client: Arc<dyn PdClient>) -> TsoSimpleProvider {
        TsoSimpleProvider { pd_client }
    }
}

impl CausalTsProvider for TsoSimpleProvider {
    fn get_ts(&self) -> Result<TimeStamp> {
        // TODO: async
        let ts = block_on(self.pd_client.get_tso())?;
        debug!("TsoSimpleProvider::get_ts"; "ts" => ?ts);
        Ok(ts)
    }

    fn advance(&self, _ts: TimeStamp) -> Result<()> {
        Ok(())
    }
}
