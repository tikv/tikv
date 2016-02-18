use std::ops::Deref;
use std::ops::DerefMut;
use rand::{self, ThreadRng};
use protobuf::Message;

pub use log::LogLevelFilter;
use log::{self, Log, LogMetadata, LogRecord, SetLoggerError};

pub mod codec;
pub mod macros;

pub fn init_log(level: LogLevelFilter) -> Result<(), SetLoggerError> {
    log::set_logger(|filter| {
        filter.set(level);
        Box::new(StdOutLogger { level: level })
    })
}

struct StdOutLogger {
    level: LogLevelFilter,
}

impl Log for StdOutLogger {
    fn enabled(&self, meta: &LogMetadata) -> bool {
        meta.level() <= self.level
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            println!("[{}] {}", record.level(), record.args());
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
