use std::error;
use std::result;
use std::fmt::{self, Display, Formatter};
use self::engine::Engine;
pub use self::engine::Dsn;

mod engine;
mod mvcc;

pub struct Storage {
    engine: Box<Engine>,
}

impl Storage {
    pub fn new(desc: Dsn) -> Result<Storage> {
        engine::new_engine(desc).map(|e| Storage { engine: e }).map_err(|e| Error::from(e))
    }

    pub fn get(&self, key: &[u8], version: u64) -> Result<Option<Vec<u8>>> {
        trace!("storage: get {:?}@{}", key, version);
        // TODO (disksing)
        self.engine.get(key).map_err(|e| Error::from(e))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8], version: u64) -> Result<()> {
        trace!("storage: put {:?}@{}", key, version);
        // TODO (disksing)
        self.engine.put(key, value).map_err(|e| Error::from(e))
    }
}

#[derive(Debug)]
pub enum Error {
    Engine(engine::Error),
    Mvcc(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Error::Engine(ref e) => Display::fmt(e, f),
            Error::Mvcc(ref s) => Display::fmt(s, f),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Engine(ref e) => e.description(),
            Error::Mvcc(..) => "mvcc",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            &Error::Engine(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<engine::Error> for Error {
    fn from(err: engine::Error) -> Error {
        Error::Engine(err)
    }
}

pub type Result<T> = result::Result<T, Error>;
