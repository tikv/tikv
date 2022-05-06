// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::resource_usage_agent::ResourceUsageRecord;

use crate::error::Result;

/// This trait abstracts the interface to communicate with the remote.
/// We can simply mock this interface to test without RPC.
pub trait DataSink: Send {
    // `try_send` pushes a report data into the sink, which will later be sent to a target
    // by the sink. If the sink is kept full, or the sink is closed, an error will be returned.
    fn try_send(&mut self, records: Arc<Vec<ResourceUsageRecord>>) -> Result<()>;
}
