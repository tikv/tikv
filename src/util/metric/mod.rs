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
use std::sync::Mutex;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, Ordering};
use std::net::{ToSocketAddrs, SocketAddr, UdpSocket};

use super::worker::{Worker, Runnable};

use cadence::prelude::*;
use cadence::{MetricSink, MetricResult, ErrorKind};
use metrics::registry::StdRegistry;

#[macro_use]
pub mod macros;

// Prometheus Metric
static mut COLLECTOR: Option<*const StdRegistry<'static>> = None;
static IS_COLLECTOR_INITIALIZED: AtomicBool = AtomicBool::new(false);

pub fn init_collector() -> Result<(), SetMetricError> {
    unsafe {
        if IS_COLLECTOR_INITIALIZED.compare_and_swap(false, true, Ordering::SeqCst) != false {
            return Err(SetMetricError);
        }

        let c = Box::new(StdRegistry::<'static>::new());
        COLLECTOR = Some(Box::into_raw(c));
        Ok(())
    }
}

pub fn collector() -> Option<&'static StdRegistry<'static>> {
    if IS_COLLECTOR_INITIALIZED.load(Ordering::SeqCst) != true {
        return None;
    }

    unsafe { COLLECTOR.map(|c| &*c) }
}



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

const BUFFER_SIZE: usize = 1024;

struct MetricBuffer(Vec<u8>);

impl fmt::Display for MetricBuffer {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "MetricBuffer")
    }
}

struct BufferSender {
    sink_addr: SocketAddr,
    socket: UdpSocket,
}

impl Runnable<MetricBuffer> for BufferSender {
    fn run(&mut self, buffer: MetricBuffer) {
        if let Err(e) = self.socket.send_to(buffer.0.as_slice(), &self.sink_addr) {
            warn!("send metric failed {:?}", e);
        }
    }
}

struct MutexBundle {
    buffer: MetricBuffer,
    last_flush_time: Instant,
    worker: Worker<MetricBuffer>,
}

pub struct BufferedUdpMetricSink {
    buffer: Mutex<MutexBundle>,
    flush_period: Duration,
}

impl BufferedUdpMetricSink {
    pub fn from<A>(sink_addr: A,
                   socket: UdpSocket,
                   flush_period: Duration)
                   -> MetricResult<BufferedUdpMetricSink>
        where A: ToSocketAddrs
    {
        let mut addr_iter = try!(sink_addr.to_socket_addrs());
        let addr = try!(addr_iter.next()
            .ok_or((ErrorKind::InvalidInput, "No socket addresses yielded")));

        let sender = BufferSender {
            sink_addr: addr,
            socket: socket,
        };

        let mut worker = Worker::new("mteric-worker");
        worker.start(sender).unwrap();

        let mb = MutexBundle {
            buffer: MetricBuffer(Vec::with_capacity(BUFFER_SIZE)),
            last_flush_time: Instant::now(),
            worker: worker,
        };

        Ok(BufferedUdpMetricSink {
            buffer: Mutex::new(mb),
            flush_period: flush_period,
        })
    }

    fn append_to_buffer(&self, metric: &str) -> io::Result<usize> {
        let mut mb = self.buffer.lock().unwrap();

        // +1 for '\n'
        if (mb.buffer.0.len() + metric.len() + 1) > mb.buffer.0.capacity() ||
           (mb.last_flush_time.elapsed() >= self.flush_period) {
            mb.last_flush_time = Instant::now();
            mb.worker.schedule(MetricBuffer(mb.buffer.0.clone())).unwrap();
            mb.buffer.0.clear()
        }

        mb.buffer.0.extend_from_slice(metric.as_bytes());
        mb.buffer.0.push(b'\n');
        Ok(metric.len())
    }
}

impl MetricSink for BufferedUdpMetricSink {
    fn emit(&self, metric: &str) -> io::Result<usize> {
        self.append_to_buffer(metric)
    }
}
