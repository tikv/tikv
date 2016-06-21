// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::net::{ToSocketAddrs, SocketAddr, UdpSocket};

use cadence::prelude::*;
use cadence::{MetricSink, MetricResult, ErrorKind};

#[macro_use]
pub mod macros;
use util::SlowTimer;

static mut CLIENT: Option<*const Metric> = None;
// IS_INITIALIZED indicates the state of CLIENT,
// `false` for uninitialized, `true` for initialized.
static IS_INITIALIZED: AtomicBool = AtomicBool::new(false);

pub trait Metric: Counted + Gauged + Metered + Timed {}

impl<T: Counted + Gauged + Metered + Timed> Metric for T {}

/// The type returned by `set_metric_client` if `set_metric_client` has already been called.
#[derive(Debug)]
pub struct SetMetricError;

impl fmt::Display for SetMetricError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt,
               "attempted to set a metric client after the metric system was already initialized")
    }
}

impl error::Error for SetMetricError {
    fn description(&self) -> &str {
        "set_metric_client() called multiple times"
    }
}

pub fn set_metric_client(client: Box<Metric + Send + Sync>) -> Result<(), SetMetricError> {
    unsafe {
        if IS_INITIALIZED.compare_and_swap(false, true, Ordering::SeqCst) != false {
            return Err(SetMetricError);
        }

        CLIENT = Some(Box::into_raw(client));
        Ok(())
    }
}

#[doc(hidden)]
pub fn client() -> Option<&'static Metric> {
    if IS_INITIALIZED.load(Ordering::SeqCst) != true {
        return None;
    }

    unsafe { CLIENT.map(|c| &*c) }
}

/// Implementation of a `MetricSink` that emits metrics over UDP with nonblocking mode.
// TODO: add buffer.
pub struct NonblockUdpMetricSink {
    sink_addr: SocketAddr,
    socket: UdpSocket,
}

impl NonblockUdpMetricSink {
    pub fn from<A>(sink_addr: A, socket: UdpSocket) -> MetricResult<NonblockUdpMetricSink>
        where A: ToSocketAddrs
    {
        let mut addr_iter = try!(sink_addr.to_socket_addrs());
        let addr = try!(addr_iter.next()
            .ok_or((ErrorKind::InvalidInput, "No socket addresses yielded")));

        // Moves this UDP stream into nonblocking mode.
        try!(socket.set_nonblocking(true));

        Ok(NonblockUdpMetricSink {
            sink_addr: addr,
            socket: socket,
        })
    }
}

impl MetricSink for NonblockUdpMetricSink {
    fn emit(&self, metric: &str) -> io::Result<usize> {
        let t = SlowTimer::from_millis(500);
        let r = self.socket.send_to(metric.as_bytes(), &self.sink_addr);
        slow_log!(t, "send metric too slow, takes {:?}", t.elapsed());
        r
    }
}
