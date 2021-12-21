// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::error::Result;
use crate::model::Records;

/// This trait abstracts the interface to communicate with the remote.
/// We can simply mock this interface to test without RPC.
pub trait DataSink {
    // `try_send` pushes a report data into the sink, which will later be sent to a target
    // by the sink. A deadline can be specified to control how late it should be sent.
    // If the sink is kept full and cannot schedule a send within the specified deadline,
    // or the sink is closed, an error will be returned.
    fn try_send(&mut self, records: Records) -> Result<()>;

    // `on_reporter_closing` notifies DataSink that the reporter is closing.
    fn on_reporter_closing() {}
}
