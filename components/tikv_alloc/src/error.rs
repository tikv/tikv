use std::error;
use std::fmt;

#[derive(Debug)]
pub enum ProfError {
    MemProfilingNotEnabled,
    IOError(std::io::Error),
    JemallocError(i32),
}

pub type ProfResult<T> = std::result::Result<T, ProfError>;

impl fmt::Display for ProfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProfError::MemProfilingNotEnabled => write!(f, "mem-profiling was not enabled"),
            ProfError::IOError(e) => write!(f, "io error occurred {:?}", e),
            ProfError::JemallocError(e) => write!(f, "jemalloc error {}", e),
        }
    }
}

impl From<std::io::Error> for ProfError {
    fn from(e: std::io::Error) -> Self {
        ProfError::IOError(e)
    }
}

impl error::Error for ProfError {}
