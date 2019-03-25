// Copyright 2019 PingCAP, Inc.
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

//! Utilities to work with tests.

use std::io;
use std::sync;

/// A buffer which can be served as a logging destination while being able to access its content.
#[derive(Clone)]
pub struct SyncLoggerBuffer(sync::Arc<sync::Mutex<Vec<u8>>>);

impl SyncLoggerBuffer {
    /// Creates a new instance.
    pub fn new() -> SyncLoggerBuffer {
        let inner = Vec::new();
        SyncLoggerBuffer(sync::Arc::new(sync::Mutex::new(inner)))
    }

    /// Builds a `slog::Logger` over this buffer which uses compact format and always output `TIME`
    /// in the time field.
    pub fn build_logger(&self) -> slog::Logger {
        use slog::Drain;

        let decorator = slog_term::PlainDecorator::new(self.clone());
        let drain = slog_term::CompactFormat::new(decorator)
            .use_custom_timestamp(|w: &mut dyn io::Write| w.write(b"TIME").map(|_| ()))
            .build();
        let drain = sync::Mutex::new(drain).fuse();
        slog::Logger::root(drain, o!())
    }

    fn lock(&self) -> sync::MutexGuard<'_, Vec<u8>> {
        self.0.lock().unwrap()
    }

    /// Clones the buffer and creates a String.
    ///
    /// Panics if the buffer is not a valid UTF-8 string.
    pub fn as_string(&self) -> String {
        let inner = self.lock();
        String::from_utf8(inner.clone()).unwrap()
    }

    /// Clears the buffer.
    pub fn clear(&self) {
        self.lock().clear();
    }
}

impl io::Write for SyncLoggerBuffer {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let mut guard = self.0.lock().unwrap();
        io::Write::write(&mut *guard, data)
    }
    fn flush(&mut self) -> io::Result<()> {
        let mut guard = self.0.lock().unwrap();
        io::Write::flush(&mut *guard)
    }
}
