use std::cell::UnsafeCell;
use std::ops::Deref;

pub use log::LogLevelFilter;
use log::{self, Log, LogMetadata, LogRecord, SetLoggerError};

pub mod codec;

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

/// A simple general wrapper for struct that is thread-safe for interior mutability already.
pub struct ThreadSafeCell<T: Sync>(UnsafeCell<T>);

impl<T: Sync> ThreadSafeCell<T> {
    pub fn new(t: T) -> ThreadSafeCell<T> {
        ThreadSafeCell(UnsafeCell::new(t))
    }

    pub fn into_inner(self) -> T {
        let ThreadSafeCell(c) = self;
        unsafe { c.into_inner() }
    }

    pub fn borrow_mut(&self) -> &mut T {
        let &ThreadSafeCell(ref c) = self;
        unsafe { &mut *c.get() }
    }
}

impl<T: Sync> Deref for ThreadSafeCell<T> {
    type Target = T;

    fn deref(&self) -> &T {
        let &ThreadSafeCell(ref c) = self;
        unsafe { &*c.get() }
    }
}

unsafe impl<T: Sync> Sync for ThreadSafeCell<T> {}
