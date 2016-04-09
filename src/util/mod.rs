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

use std::ops::Deref;
use std::ops::DerefMut;
use std::io::{self, Write};
use std::net::{ToSocketAddrs, TcpStream};
use time;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rand::{self, ThreadRng};
use protobuf::Message;

pub use log::LogLevelFilter;
use log::{self, Log, LogMetadata, LogRecord, SetLoggerError};

#[macro_use]
pub mod macros;
pub mod logger;
// TODO: add #[cfg(test)] if we get it compiled.
pub mod panic_hook;
pub mod codec;

pub fn init_log(level: LogLevelFilter) -> Result<(), SetLoggerError> {
    log::set_logger(|filter| {
        filter.set(level);
        Box::new(DefaultLogger { level: level })
    })
}

struct DefaultLogger {
    level: LogLevelFilter,
}

impl Log for DefaultLogger {
    fn enabled(&self, meta: &LogMetadata) -> bool {
        meta.level() <= self.level
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            let t = time::now();
            // TODO allow formatter to be configurable.
            let _ = write!(io::stderr(),
                           "{},{:03} {}:{} - {:5} - {}\n",
                           time::strftime("%Y-%m-%d %H:%M:%S", &t).unwrap(),
                           t.tm_nsec / 1000_000,
                           record.location().file().rsplit('/').nth(0).unwrap(),
                           record.location().line(),
                           record.level(),
                           record.args());
        }
    }
}

pub fn limit_size<T: Message + Clone>(entries: &[T], max: u64) -> Vec<T> {
    if entries.len() == 0 {
        return entries.to_vec();
    }

    let mut size = Message::compute_size(&entries[0]) as u64;
    let mut limit = 1usize;
    while limit < entries.len() {
        size += Message::compute_size(&entries[limit]) as u64;
        if size > max {
            break;
        }
        limit += 1;
    }
    entries[..limit].to_vec()
}

pub struct DefaultRng {
    rng: ThreadRng,
}

impl DefaultRng {
    fn new() -> DefaultRng {
        DefaultRng { rng: rand::thread_rng() }
    }
}

impl Default for DefaultRng {
    fn default() -> DefaultRng {
        DefaultRng::new()
    }
}

impl Deref for DefaultRng {
    type Target = ThreadRng;

    fn deref(&self) -> &ThreadRng {
        &self.rng
    }
}

impl DerefMut for DefaultRng {
    fn deref_mut(&mut self) -> &mut ThreadRng {
        &mut self.rng
    }
}

/// A handy shortcut to replace `RwLock` write/read().unwrap() pattern to
/// shortcut wl and rl.
pub trait HandyRwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<T>;
    fn rl(&self) -> RwLockReadGuard<T>;
}

impl<T> HandyRwLock<T> for RwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<T> {
        self.write().unwrap()
    }

    fn rl(&self) -> RwLockReadGuard<T> {
        self.read().unwrap()
    }
}


pub fn make_std_tcp_conn<A: ToSocketAddrs>(addr: A) -> io::Result<TcpStream> {
    let stream = try!(TcpStream::connect(addr));
    try!(stream.set_nodelay(true));
    Ok(stream)
}
